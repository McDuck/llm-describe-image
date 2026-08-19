"""Contract for remote speech-to-text model backends."""

from __future__ import annotations

import abc


class TranscriptionBackend(abc.ABC):
    """Transcribe one uploaded audio interval on the GPU worker."""

    @abc.abstractmethod
    def transcribe(self, audio_path: str, model_name: str, language: str) -> str:
        raise NotImplementedError
