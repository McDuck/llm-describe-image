"""Local Faster-Whisper inference backend for the remote GPU host."""

from __future__ import annotations

import os
from typing import Any, Optional

from ..base import TranscriptionBackend


class FasterWhisperBackend(TranscriptionBackend):
    """Reuse one CTranslate2 model instance until its requested name changes."""

    def __init__(self, device: Optional[str] = None, compute_type: Optional[str] = None) -> None:
        self.device = device or os.getenv("TRANSCRIPTION_DEVICE", "auto")
        self.compute_type = compute_type or os.getenv("TRANSCRIPTION_COMPUTE_TYPE", "int8")
        self.model_name: Optional[str] = None
        self.model: Any = None

    def transcribe(self, audio_path: str, model_name: str, language: str) -> str:
        if self.model is None or self.model_name != model_name:
            self._load(model_name)
        segments, _ = self.model.transcribe(
            audio_path,
            language=language or None,
            beam_size=5,
            vad_filter=True,
            condition_on_previous_text=False,
        )
        return " ".join(segment.text.strip() for segment in segments if segment.text.strip())

    def _load(self, model_name: str) -> None:
        try:
            from faster_whisper import WhisperModel
        except ImportError as error:
            raise RuntimeError("Faster-Whisper backend requires faster-whisper; install the worker requirements.") from error
        self.model = WhisperModel(model_name, device=self.device, compute_type=self.compute_type)
        self.model_name = model_name
