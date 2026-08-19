"""Transcribe the non-overlapping audio interval represented by a video frame."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from typing import Any, Dict, List, Optional, Tuple

from describe_media.tasks.media import output_relative_path, split_media_item
from describe_media.tasks.task import Task
from describe_media.tasks.transcribe_video.gpus.backend import RemoteAudioTranscriptionBackend


TRANSCRIPT_SCHEMA_VERSION = 1


class TranscribeVideoTask(Task[Tuple[str, Dict[str, Any]], Tuple[str, Dict[str, Any]]]):
    """Extract a frame's audio slice and transcribe it on the remote GPU host."""

    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str,
        enabled: bool,
        backend_name: str,
        model_name: str,
        language: str,
        remote_api_base: Optional[str] = None,
        remote_api_token: Optional[str] = None,
        remote_timeout_seconds: float = 600.0,
        retry: bool = False,
        retry_failed: bool = False,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir = output_dir
        self.enabled = enabled
        self.backend_name = backend_name
        self.model_name = model_name
        self.language = language.strip()
        self.retry = retry
        self.retry_failed = retry_failed
        self.transcriber: Optional[RemoteAudioTranscriptionBackend] = (
            RemoteAudioTranscriptionBackend(remote_api_base, remote_api_token or "", remote_timeout_seconds)
            if enabled and remote_api_base
            else None
        )

    def load(self) -> None:
        if not self.enabled:
            return
        if self.transcriber is None:
            raise RuntimeError(
                "Video transcription requires GPU_API_BASE and GPU_API_TOKEN for the remote Faster-Whisper worker."
            )
        self.transcriber.load()

    def unload(self) -> None:
        return None

    def execute(self, item: Tuple[str, Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
        frame_path, metadata = split_media_item(item)
        metadata = dict(metadata)
        if "_source_video_path" not in metadata:
            return frame_path, metadata

        if not self.enabled:
            self.record_skip()
            return self._result(frame_path, metadata, "")

        source_path = str(metadata["_source_video_path"])
        start, end = self._interval(metadata)
        transcript_path = self._transcript_path(frame_path, metadata)
        error_path = transcript_path.replace(".json", ".error.json")
        cached = self._read_cached(transcript_path, source_path, start, end)
        if cached is not None and not self.retry:
            self.record_skip()
            return self._finish(frame_path, metadata, cached)
        if not self.retry and not self.retry_failed and os.path.exists(error_path):
            self.record_skip()
            return self._finish(frame_path, metadata, "")

        try:
            audio_path = self._extract_audio(source_path, start, end)
            try:
                text = self._transcribe(audio_path)
            finally:
                self._remove(audio_path)
            self._write_json(transcript_path, self._record(source_path, start, end, text))
            self._remove(error_path)
            return self._finish(frame_path, metadata, text)
        except Exception as error:
            self._write_json(error_path, {"schema_version": TRANSCRIPT_SCHEMA_VERSION, "error": str(error)})
            # Audio context enriches a visual caption; an unavailable
            # transcription endpoint must not strand the frame at the LLM gate.
            return self._finish(frame_path, metadata, "")

    def _finish(self, frame_path: str, metadata: Dict[str, Any], text: str) -> Tuple[str, Dict[str, Any]]:
        self._update_video_transcript(str(metadata["_source_video_path"]))
        return self._result(frame_path, metadata, text)

    @staticmethod
    def _interval(metadata: Dict[str, Any]) -> Tuple[float, float]:
        start = float(metadata["_frame_audio_start_seconds"])
        end = float(metadata["_frame_audio_end_seconds"])
        if start < 0 or end <= start:
            raise ValueError(f"Invalid video-frame transcription interval: {start}..{end}")
        return start, end

    def _transcript_path(self, frame_path: str, metadata: Dict[str, Any]) -> str:
        relative = output_relative_path(frame_path, self.input_dir or frame_path, metadata)
        return os.path.join(self.output_dir, relative + ".transcript.json")

    def _video_transcript_path(self, source_path: str) -> str:
        relative = os.path.relpath(source_path, self.input_dir or source_path)
        return os.path.join(self.output_dir, relative + ".transcript.json")

    def _video_vtt_path(self, source_path: str) -> str:
        relative = os.path.relpath(source_path, self.input_dir or source_path)
        return os.path.join(self.output_dir, relative + ".transcript.vtt")

    def _update_video_transcript(self, source_path: str) -> None:
        """Build the whole-video transcript from the frame-slice cache files."""
        manifest_path = os.path.join(
            self.output_dir,
            os.path.relpath(source_path, self.input_dir or source_path) + ".frames.json",
        )
        try:
            with open(manifest_path, encoding="utf-8") as handle:
                frame_manifest = json.load(handle)
            frames = frame_manifest["frames"]
            if not isinstance(frames, list):
                return
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
            return

        entries = []
        complete = True
        has_failures = False
        texts = []
        for frame in frames:
            if not isinstance(frame, dict) or not isinstance(frame.get("path"), str):
                return
            frame_transcript_path = frame["path"] + ".transcript.json"
            error_path = frame_transcript_path.replace(".json", ".error.json")
            entry = {
                "number": frame.get("number"),
                "timestamp_seconds": frame.get("timestamp_seconds"),
                "interval_seconds": {
                    "start": frame.get("audio_start_seconds"),
                    "end": frame.get("audio_end_seconds"),
                },
            }
            try:
                with open(frame_transcript_path, encoding="utf-8") as handle:
                    frame_record = json.load(handle)
                text = frame_record["transcription"]["text"]
                if not isinstance(text, str):
                    raise ValueError("Transcript text is missing")
                entry.update({"status": "transcribed", "text": text})
                if text:
                    texts.append(text)
            except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
                if os.path.exists(error_path):
                    entry["status"] = "failed"
                    has_failures = True
                else:
                    entry["status"] = "pending"
                    complete = False
            entries.append(entry)

        whole_transcript = {
            "schema_version": TRANSCRIPT_SCHEMA_VERSION,
            "source": frame_manifest.get("source", {}),
            "transcription": {"backend": self.backend_name, "model": self.model_name, "language": self.language},
            "complete": complete,
            "has_failures": has_failures,
            "frames": entries,
            "text": "\n".join(texts),
        }
        self._write_json(self._video_transcript_path(source_path), whole_transcript)
        self._write_vtt(self._video_vtt_path(source_path), entries)

    @staticmethod
    def _signature(path: str) -> Dict[str, int]:
        stat = os.stat(path)
        return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}

    def _record(self, source_path: str, start: float, end: float, text: str) -> Dict[str, Any]:
        return {
            "schema_version": TRANSCRIPT_SCHEMA_VERSION,
            "source": {"path": source_path, "signature": self._signature(source_path)},
            "interval_seconds": {"start": start, "end": end},
            "transcription": {"backend": self.backend_name, "model": self.model_name, "language": self.language, "text": text},
        }

    def _read_cached(self, path: str, source_path: str, start: float, end: float) -> Optional[str]:
        try:
            with open(path, encoding="utf-8") as handle:
                record = json.load(handle)
            if record.get("schema_version") != TRANSCRIPT_SCHEMA_VERSION:
                return None
            if record.get("source", {}).get("signature") != self._signature(source_path):
                return None
            if record.get("interval_seconds") != {"start": start, "end": end}:
                return None
            transcription = record.get("transcription", {})
            if (
                transcription.get("backend") != self.backend_name
                or transcription.get("model") != self.model_name
                or transcription.get("language") != self.language
            ):
                return None
            return transcription["text"] if isinstance(transcription.get("text"), str) else None
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return None

    def _extract_audio(self, source_path: str, start: float, end: float) -> str:
        descriptor, path = tempfile.mkstemp(prefix=".transcript-", suffix=".m4a")
        os.close(descriptor)
        command = [
            os.getenv("FFMPEG_BINARY", "ffmpeg"), "-hide_banner", "-loglevel", "error", "-i", source_path,
            "-ss", f"{start:.3f}", "-t", f"{end - start:.3f}", "-map", "0:a:0", "-vn",
            "-ac", "1", "-ar", "16000", "-c:a", "aac", "-y", path,
        ]
        try:
            completed = subprocess.run(command, capture_output=True, text=True, check=False)
        except FileNotFoundError as error:
            self._remove(path)
            raise RuntimeError("Video transcription requires ffmpeg; install it or set FFMPEG_BINARY.") from error
        if completed.returncode != 0:
            self._remove(path)
            detail = completed.stderr.strip()
            if "matches no streams" in detail or "does not contain any stream" in detail:
                # Silence is valid context for a visual frame, not a pipeline failure.
                descriptor, empty_path = tempfile.mkstemp(prefix=".transcript-empty-", suffix=".m4a")
                os.close(descriptor)
                return empty_path
            raise RuntimeError(f"ffmpeg could not extract audio from {source_path}: {detail}")
        return path

    def _transcribe(self, audio_path: str) -> str:
        if os.path.getsize(audio_path) == 0:
            return ""
        if self.transcriber is None:
            raise RuntimeError("Remote Faster-Whisper backend is not configured")
        return self.transcriber.transcribe(audio_path, self.backend_name, self.model_name, self.language)

    @staticmethod
    def _write_json(path: str, value: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        descriptor, temporary_path = tempfile.mkstemp(prefix=".transcript-", suffix=".json", dir=os.path.dirname(path))
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(temporary_path, path)
        except Exception:
            TranscribeVideoTask._remove(temporary_path)
            raise

    @staticmethod
    def _write_vtt(path: str, entries: List[Dict[str, Any]]) -> None:
        """Write completed audio slices as standard WebVTT cues."""
        lines = ["WEBVTT", ""]
        for entry in entries:
            text = entry.get("text")
            interval = entry.get("interval_seconds")
            if not isinstance(text, str) or not text or not isinstance(interval, dict):
                continue
            start = interval.get("start")
            end = interval.get("end")
            if not isinstance(start, (int, float)) or not isinstance(end, (int, float)) or end <= start:
                continue
            lines.extend([
                f"{TranscribeVideoTask._vtt_timestamp(float(start))} --> {TranscribeVideoTask._vtt_timestamp(float(end))}",
                text.replace("\r\n", "\n").replace("\r", "\n"),
                "",
            ])
        TranscribeVideoTask._write_text(path, "\n".join(lines))

    @staticmethod
    def _vtt_timestamp(seconds: float) -> str:
        milliseconds = max(0, round(seconds * 1000))
        hours, remainder = divmod(milliseconds, 3_600_000)
        minutes, remainder = divmod(remainder, 60_000)
        seconds_value, milliseconds = divmod(remainder, 1000)
        return f"{hours:02d}:{minutes:02d}:{seconds_value:02d}.{milliseconds:03d}"

    @staticmethod
    def _write_text(path: str, text: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        descriptor, temporary_path = tempfile.mkstemp(prefix=".transcript-", suffix=".vtt", dir=os.path.dirname(path))
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(text)
            os.replace(temporary_path, path)
        except Exception:
            TranscribeVideoTask._remove(temporary_path)
            raise

    @staticmethod
    def _remove(path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    @staticmethod
    def _result(frame_path: str, metadata: Dict[str, Any], text: str) -> Tuple[str, Dict[str, Any]]:
        metadata["_transcript"] = text
        metadata["_stage"] = "transcript"
        return frame_path, metadata
