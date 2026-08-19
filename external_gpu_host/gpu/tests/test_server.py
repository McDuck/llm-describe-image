from pathlib import Path
from http import HTTPStatus
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from external_gpu_host.gpu.services.recognition.face_backend import FaceDetection
from external_gpu_host.gpu.server import GpuApiService, build_handler
from external_gpu_host.gpu.services.video_frames.video_frames import ExtractedVideoFrame


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
    service = GpuApiService(FakeBackend(), "test-token")

    assert service.authorised("Bearer test-token")
    assert not service.authorised("Bearer wrong-token")
    assert not service.authorised("")


def test_handler_rejects_unauthenticated_health_requests() -> None:
    handler = build_handler(GpuApiService(FakeBackend(), "test-token"))

    assert handler is not None


def test_service_serialises_video_frame_extraction(tmp_path) -> None:
    seen = []

    def extract(path: str, interval: float, maximum: int):
        seen.append((Path(path).read_bytes(), interval, maximum))
        return 1.5, [ExtractedVideoFrame(1, 0.0, b"jpeg")]

    service = GpuApiService(FakeBackend(), "test-token", video_extractor=extract)
    video_path = tmp_path / "clip.mp4"
    video_path.write_bytes(b"video")

    duration, frames = service.extract_video_frames(str(video_path), 5.0, 24)

    assert seen == [(b"video", 5.0, 24)]
    assert duration == 1.5
    assert frames[0].jpeg_bytes == b"jpeg"
