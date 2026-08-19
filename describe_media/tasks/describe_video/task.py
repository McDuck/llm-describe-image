"""Describe a complete video from its frame captions and transcript, without an image."""

import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "llm"))

from describe_media.tasks.media import split_media_item
from describe_media.tasks.task import Task
from describe_media.tasks.write.task import WriteTask
from llms import get_backend
from llms.base import LLMBackend


class DescribeVideoTask(Task[Tuple[str, Dict[str, Any]], Optional[str]]):
    """Wait for all frame captions, then generate one text-only video description."""

    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str,
        model_name: str,
        prompt: str,
        backend_name: Optional[str],
        output_format: str,
        retry: bool = False,
        retry_failed: bool = False,
        sync_api_timeout_s: int = 600,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir = output_dir
        self.model_name = model_name
        self.prompt = prompt
        self.backend_name = backend_name
        self.retry = retry
        self.retry_failed = retry_failed
        self.sync_api_timeout_s = sync_api_timeout_s
        self.backend: Optional[LLMBackend] = None
        self.model: Any = None
        self.server_started_by_script = False
        self.model_loaded_by_script = False
        self.writer = WriteTask(
            maximum=maximum,
            input_dir=input_dir,
            output_dir=output_dir,
            output_format=output_format,
        )

    def load(self) -> None:
        self.backend = get_backend(self.backend_name)
        if self.backend:
            self.server_started_by_script = self.backend.bootstrap_server(
                auto_start=True,
                sync_api_timeout_s=self.sync_api_timeout_s,
            )
            self.model = self.backend.load_model(self.model_name, allow_cli_install=False)
            self.model_loaded_by_script = self.model is not None
        if self.model is None:
            raise RuntimeError(f"Failed to load video-description model: {self.model_name}")

    def unload(self) -> None:
        if self.backend and hasattr(self.backend, "cleanup"):
            self.backend.cleanup(
                model_loaded_by_script=self.model_loaded_by_script,
                model_name=self.model_name,
                server_started_by_script=self.server_started_by_script,
            )
        self.model = None
        self.backend = None

    def execute(self, item: Tuple[str, Dict[str, Any]]) -> Optional[str]:
        _, metadata = split_media_item(item)
        source_path = metadata.get("_source_video_path")
        if not isinstance(source_path, str):
            return None

        output_path = os.path.join(self.output_dir, os.path.relpath(source_path, self.input_dir) + ".txt")
        error_path = output_path.replace(".txt", ".error.txt")
        if not self.retry and os.path.exists(output_path):
            self.record_skip()
            return output_path
        if not self.retry and not self.retry_failed and os.path.exists(error_path):
            self.record_skip()
            return None

        frame_manifest = self._read_frame_manifest(source_path)
        if frame_manifest is None:
            return None
        captions = self._read_frame_captions(frame_manifest)
        if captions is None:
            return None
        transcript = self._read_whole_transcript(source_path)
        if transcript is None:
            return None

        try:
            if self.backend is None or self.model is None:
                raise RuntimeError("Video-description backend or model is not configured")
            prompt = self._build_prompt(captions, transcript)
            content = self.backend.respond(self.model, prompt, image_handle=None)
            self.writer.execute((source_path, content, {"filename": os.path.basename(source_path)}))
            return output_path
        except Exception as error:
            self.writer.execute((source_path, error, {"filename": os.path.basename(source_path)}))
            return None

    def _read_frame_manifest(self, source_path: str) -> Optional[Dict[str, Any]]:
        path = os.path.join(self.output_dir, os.path.relpath(source_path, self.input_dir) + ".frames.json")
        try:
            with open(path, encoding="utf-8") as handle:
                manifest = json.load(handle)
            return manifest if isinstance(manifest.get("frames"), list) else None
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return None

    @staticmethod
    def _read_frame_captions(frame_manifest: Dict[str, Any]) -> Optional[List[Tuple[Any, str]]]:
        captions: List[Tuple[Any, str]] = []
        for frame in frame_manifest["frames"]:
            if not isinstance(frame, dict) or not isinstance(frame.get("path"), str):
                return None
            try:
                with open(frame["path"] + ".txt", encoding="utf-8") as handle:
                    captions.append((frame.get("timestamp_seconds"), handle.read().strip()))
            except OSError:
                return None
        return captions

    def _read_whole_transcript(self, source_path: str) -> Optional[str]:
        path = os.path.join(self.output_dir, os.path.relpath(source_path, self.input_dir) + ".transcript.json")
        if not os.path.exists(path):
            # Transcription is optional, so no file is valid empty context.
            return ""
        try:
            with open(path, encoding="utf-8") as handle:
                transcript = json.load(handle)
            if not transcript.get("complete"):
                return None
            text = transcript.get("text", "")
            return text if isinstance(text, str) else None
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return None

    def _build_prompt(self, captions: List[Tuple[Any, str]], transcript: str) -> str:
        frame_section = "\n\n".join(
            f"Frame at {timestamp if isinstance(timestamp, (int, float)) else 'unknown'} seconds:\n{caption}"
            for timestamp, caption in captions
        )
        transcript_section = transcript or "(No spoken audio was transcribed.)"
        return (
            f"{self.prompt}\n\n"
            f"Frame descriptions:\n{frame_section}\n\n"
            f"Full video transcript:\n{transcript_section}"
        )
