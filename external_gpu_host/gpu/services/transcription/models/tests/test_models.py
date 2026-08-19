import pytest

from external_gpu_host.gpu.services.transcription.models import get_backend
from external_gpu_host.gpu.services.transcription.models.faster_whisper import FasterWhisperBackend


def test_faster_whisper_backend_is_selected_by_name() -> None:
    assert isinstance(get_backend("faster-whisper"), FasterWhisperBackend)
    assert isinstance(get_backend("whisper"), FasterWhisperBackend)


def test_unknown_transcription_backend_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported transcription backend"):
        get_backend("unknown")
