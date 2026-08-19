from http import HTTPStatus
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from external_gpu_host.recognition.worker.face_backend import FaceDetection
from external_gpu_host.recognition.worker.server import RecognitionService, build_handler


class FakeBackend:
    def detect_bytes(self, image_bytes: bytes):
        assert image_bytes == b"image-bytes"
        return [
            FaceDetection(
                bbox={"x": 1, "y": 2, "width": 3, "height": 4},
                confidence=0.9,
                embedding=[1.0, 0.0],
                image_size=(10, 20),
            )
        ]


def test_service_requires_exact_bearer_token() -> None:
    service = RecognitionService(FakeBackend(), "test-token")

    assert service.authorised("Bearer test-token")
    assert not service.authorised("Bearer wrong-token")
    assert not service.authorised("")


def test_handler_rejects_unauthenticated_health_requests() -> None:
    handler = build_handler(RecognitionService(FakeBackend(), "test-token"))

    assert handler is not None
