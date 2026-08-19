import pytest

from describe_media.recognition.gpus.api.backend import RemoteRecognitionBackend


def test_remote_recognition_requires_a_token() -> None:
    with pytest.raises(ValueError, match="token"):
        RemoteRecognitionBackend("http://127.0.0.1:5002/v1", "")


def test_remote_recognition_requires_an_http_url() -> None:
    with pytest.raises(ValueError, match="http"):
        RemoteRecognitionBackend("worker:5002/v1", "test-token")
