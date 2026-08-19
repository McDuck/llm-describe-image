"""Backward-compatible import for the Faster-Whisper backend."""

from .models.faster_whisper.backend import FasterWhisperBackend

FasterWhisperTranscriber = FasterWhisperBackend

__all__ = ["FasterWhisperTranscriber"]
