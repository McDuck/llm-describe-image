"""Factory for pluggable remote transcription backends."""

from __future__ import annotations

import os

from .base import TranscriptionBackend
from .faster_whisper.backend import FasterWhisperBackend


def get_backend(name: str = "") -> TranscriptionBackend:
    """Return the named transcription backend; future engines register here."""
    backend = (name or os.getenv("TRANSCRIPTION_BACKEND", "faster-whisper")).strip().lower()
    if backend in {"faster-whisper", "faster_whisper", "whisper"}:
        return FasterWhisperBackend()
    raise ValueError(f"Unsupported transcription backend: {backend}")


__all__ = ["TranscriptionBackend", "get_backend"]
