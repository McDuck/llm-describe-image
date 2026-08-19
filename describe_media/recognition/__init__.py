"""Recognition client, local backend, and curated identity index."""

from describe_media.recognition.client import RemoteRecognitionBackend
from describe_media.recognition.index import RecognitionIndex

__all__ = ["RecognitionIndex", "RemoteRecognitionBackend"]
