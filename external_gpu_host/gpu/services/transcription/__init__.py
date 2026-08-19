"""Remote pluggable audio-transcription service."""

from .models import TranscriptionBackend, get_backend
from .models.faster_whisper import FasterWhisperBackend

__all__ = ["TranscriptionBackend", "FasterWhisperBackend", "get_backend"]
