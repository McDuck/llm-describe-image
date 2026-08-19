import json
import os
import struct
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from describe_media.tasks.task import Task
from describe_media.tasks.extract_video.gpus.api.backend import RemoteVideoFrameBackend
from describe_media.tasks.extract_video.gpus.base import VideoFrame, VideoFrameBackend
from describe_media.tasks.extract_video.gpus.direct.backend import DirectVideoFrameBackend


MANIFEST_SCHEMA_VERSION = 2
_ISOBMFF_EXTENSIONS = {".3g2", ".3gp", ".m4a", ".mj2", ".mov", ".mp4"}


class ExtractVideoTask(Task[str, List[Tuple[str, Dict[str, Any]]]]):
    """Reuse a video's valid frame manifest or decode sampled JPEG frames."""

    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str,
        frame_interval_seconds: float,
        max_frames: int,
        retry: bool = False,
        retry_failed: bool = False,
        remote_api_base: Optional[str] = None,
        remote_api_token: Optional[str] = None,
        remote_timeout_seconds: float = 120.0,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir = output_dir
        self.frame_interval_seconds = float(frame_interval_seconds)
        self.max_frames = int(max_frames)
        self.retry = retry
        self.retry_failed = retry_failed
        self.remote_api_base = remote_api_base
        self.remote_api_token = remote_api_token
        self.remote_timeout_seconds = float(remote_timeout_seconds)
        self.frame_backend: VideoFrameBackend = (
            RemoteVideoFrameBackend(remote_api_base, remote_api_token or "", self.remote_timeout_seconds)
            if remote_api_base
            else DirectVideoFrameBackend()
        )

    def load(self) -> None:
        self.frame_backend.load()

    def execute(self, input_path: str) -> List[Tuple[str, Dict[str, Any]]]:
        error_path = os.path.join(self.output_dir, os.path.relpath(input_path, self.input_dir) + ".frames.error.txt")
        if not self.retry and not self.retry_failed and os.path.exists(error_path):
            self.record_skip()
            return []
        manifest_path = self._manifest_path(input_path)
        manifest = self._read_valid_manifest(input_path, manifest_path)
        if manifest is not None:
            self.record_skip()
        if manifest is None:
            manifest = self._extract(input_path, manifest_path)
        self._remove_error_marker(input_path)
        return self._items_from_manifest(input_path, manifest)

    def _manifest_path(self, input_path: str) -> str:
        return os.path.join(self.output_dir, os.path.relpath(input_path, self.input_dir) + ".frames.json")

    def _source_signature(self, input_path: str) -> Dict[str, int]:
        stat = os.stat(input_path)
        return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}

    def _read_valid_manifest(self, input_path: str, manifest_path: str) -> Optional[Dict[str, Any]]:
        if self.retry or not os.path.exists(manifest_path):
            return None
        try:
            with open(manifest_path, "r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
                return None
            if manifest.get("source", {}).get("signature") != self._source_signature(input_path):
                return None
            settings = manifest.get("sampling", {})
            if settings.get("frame_interval_seconds") != self.frame_interval_seconds or settings.get("max_frames") != self.max_frames:
                return None
            frames = manifest.get("frames")
            if not isinstance(frames, list) or not frames or not all(os.path.exists(frame.get("path", "")) for frame in frames):
                return None
            return manifest
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return None

    def _extract(self, input_path: str, manifest_path: str) -> Dict[str, Any]:
        self._validate_container_index(input_path)
        duration, frames = self.frame_backend.extract(
            input_path,
            self.frame_interval_seconds,
            self.max_frames,
        )
        return self._persist_extracted_frames(input_path, manifest_path, duration, frames)

    def _persist_extracted_frames(
        self,
        input_path: str,
        manifest_path: str,
        duration: float,
        extracted_frames: List[VideoFrame],
    ) -> Dict[str, Any]:
        """Persist frames from either direct or shared-GPU extraction."""
        relative_video = os.path.relpath(input_path, self.input_dir)
        destination_dir = os.path.join(self.output_dir, os.path.dirname(relative_video))
        os.makedirs(destination_dir, exist_ok=True)
        frames: List[Dict[str, Any]] = []
        timestamps = [frame.timestamp_seconds for frame in extracted_frames]
        for index, frame in enumerate(extracted_frames):
            frame_name = f"{os.path.basename(input_path)}.frame-{frame.number:04d}-t{frame.timestamp_seconds:08.3f}.jpg"
            frame_path = os.path.join(destination_dir, frame_name)
            self._write_bytes_atomically(frame_path, frame.jpeg_bytes)
            # Adjacent midpoint boundaries give every frame one non-overlapping
            # audio slice, covering the source video exactly once.
            start = 0.0 if index == 0 else round((timestamps[index - 1] + frame.timestamp_seconds) / 2, 6)
            end = duration if index == len(extracted_frames) - 1 else round((frame.timestamp_seconds + timestamps[index + 1]) / 2, 6)
            frames.append({
                "number": frame.number,
                "timestamp_seconds": frame.timestamp_seconds,
                "audio_start_seconds": start,
                "audio_end_seconds": end,
                "path": frame_path,
            })
        manifest = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "source": {
                "relative_path": os.path.relpath(input_path, self.input_dir).replace("\\", "/"),
                "signature": self._source_signature(input_path),
                "duration_seconds": duration,
            },
            "sampling": {
                "frame_interval_seconds": self.frame_interval_seconds,
                "max_frames": self.max_frames,
            },
            "frames": frames,
        }
        self._write_manifest(manifest_path, manifest)
        return manifest

    @staticmethod
    def _validate_container_index(input_path: str) -> None:
        """Fail clearly before OpenCV invokes FFmpeg for an incomplete MP4/MOV.

        An ISO base media file must include a top-level ``moov`` atom to be
        seekable.  Checking it here prevents FFmpeg's noisy low-level error and
        gives the caller an actionable, retryable failure marker instead.
        """
        if os.path.splitext(input_path)[1].lower() not in _ISOBMFF_EXTENSIONS:
            return

        try:
            file_size = os.path.getsize(input_path)
            offset = 0
            has_movie_atom = False
            with open(input_path, "rb") as handle:
                while offset + 8 <= file_size:
                    handle.seek(offset)
                    header = handle.read(8)
                    atom_size, atom_type = struct.unpack(">I4s", header)
                    header_size = 8
                    if atom_size == 1:
                        extended_size = handle.read(8)
                        if len(extended_size) != 8:
                            break
                        atom_size = struct.unpack(">Q", extended_size)[0]
                        header_size = 16
                    elif atom_size == 0:
                        atom_size = file_size - offset

                    if atom_size < header_size or offset + atom_size > file_size:
                        break
                    if atom_type == b"moov":
                        has_movie_atom = True
                        break
                    offset += atom_size
        except OSError as error:
            raise RuntimeError(f"Could not read video file: {input_path}") from error

        if not has_movie_atom:
            raise RuntimeError(
                f"Video is incomplete or corrupt (missing moov atom): {input_path}. "
                "Wait for the copy/export to finish, then replace the source file and rerun with --retry-failed."
            )

    def _remove_error_marker(self, input_path: str) -> None:
        error_path = os.path.join(self.output_dir, os.path.relpath(input_path, self.input_dir) + ".frames.error.txt")
        try:
            os.remove(error_path)
        except FileNotFoundError:
            pass

    def _items_from_manifest(self, input_path: str, manifest: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
        source_relative = os.path.relpath(input_path, self.input_dir)
        result: List[Tuple[str, Dict[str, Any]]] = []
        for frame in manifest["frames"]:
            frame_path = frame["path"]
            frame_relative = os.path.join(os.path.dirname(source_relative), os.path.basename(frame_path))
            result.append((frame_path, {
                "_output_relative_path": frame_relative,
                "_source_video_path": input_path,
                "_frame_number": frame["number"],
                "_frame_timestamp_seconds": frame["timestamp_seconds"],
                "_frame_audio_start_seconds": frame["audio_start_seconds"],
                "_frame_audio_end_seconds": frame["audio_end_seconds"],
            }))
        return result

    @staticmethod
    def _write_manifest(path: str, manifest: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        descriptor, temporary_path = tempfile.mkstemp(prefix=".frames-", suffix=".json", dir=os.path.dirname(path))
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(manifest, handle, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(temporary_path, path)
        except Exception:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
            raise

    @staticmethod
    def _write_bytes_atomically(path: str, data: bytes) -> None:
        descriptor, temporary_path = tempfile.mkstemp(prefix=".frame-", suffix=".jpg", dir=os.path.dirname(path))
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
            os.replace(temporary_path, path)
        except Exception:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
            raise
