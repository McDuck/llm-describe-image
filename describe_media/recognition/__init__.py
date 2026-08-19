"""Recognition client, local backend, and curated identity index."""

from describe_media.recognition.gpus.api.backend import RemoteRecognitionBackend
from describe_media.recognition.index import RecognitionIndex

__all__ = ["RecognitionIndex", "RemoteRecognitionBackend"]
