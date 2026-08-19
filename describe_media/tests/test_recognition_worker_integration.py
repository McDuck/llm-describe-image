import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from describe_media.recognition.gpus.api.backend import RemoteRecognitionBackend
from describe_media.tasks.extract_video.gpus.api.backend import RemoteVideoFrameBackend
from describe_media.tasks.transcribe_video.gpus.backend import RemoteAudioTranscriptionBackend
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


def test_remote_recognition_client_round_trip(tmp_path) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(GpuApiService(FakeBackend(), "test-token")))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    image_path = tmp_path / "source.jpg"
    image_path.write_bytes(b"image-bytes")
    try:
        backend = RemoteRecognitionBackend(f"http://127.0.0.1:{server.server_port}/v1", "test-token")
        backend.load()
        detections = backend.detect(str(image_path))
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    assert len(detections) == 1
    assert detections[0].bbox == {"x": 1, "y": 2, "width": 3, "height": 4}
    assert detections[0].embedding == [1.0, 0.0]


def test_remote_recognition_rejects_missing_token() -> None:
    try:
        RemoteRecognitionBackend("http://127.0.0.1:5002/v1", "")
    except ValueError as error:
        assert "token" in str(error).lower()
    else:
        raise AssertionError("A remote recognition token must be required")


def test_remote_video_frame_client_round_trip(tmp_path) -> None:
    def extract(video_path: str, interval: float, maximum: int):
        assert Path(video_path).read_bytes() == b"video-bytes"
        assert interval == 2.5
        assert maximum == 3
        return 12.0, [ExtractedVideoFrame(1, 0.0, b"jpeg-bytes")]

    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        build_handler(GpuApiService(FakeBackend(), "test-token", video_extractor=extract)),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    video_path = tmp_path / "source.mp4"
    video_path.write_bytes(b"video-bytes")
    try:
        backend = RemoteVideoFrameBackend(f"http://127.0.0.1:{server.server_port}/v1", "test-token")
        backend.load()
        duration, frames = backend.extract(str(video_path), 2.5, 3)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    assert duration == 12.0
    assert [(frame.number, frame.timestamp_seconds, frame.jpeg_bytes) for frame in frames] == [(1, 0.0, b"jpeg-bytes")]


def test_remote_audio_transcription_client_round_trip(tmp_path) -> None:
    def transcribe(audio_path: str, model_name: str, language: str) -> str:
        assert Path(audio_path).read_bytes() == b"audio-bytes"
        assert model_name == "small"
        assert language == "nl"
        return "Hallo wereld"

    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        build_handler(GpuApiService(FakeBackend(), "test-token", audio_transcriber=transcribe)),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    audio_path = tmp_path / "clip.m4a"
    audio_path.write_bytes(b"audio-bytes")
    try:
        backend = RemoteAudioTranscriptionBackend(f"http://127.0.0.1:{server.server_port}/v1", "test-token")
        backend.load()
        text = backend.transcribe(str(audio_path), "faster-whisper", "small", "nl")
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    assert text == "Hallo wereld"
