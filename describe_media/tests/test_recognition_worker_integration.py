import threading
from http.server import ThreadingHTTPServer

from describe_media.recognition.client import RemoteRecognitionBackend
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


def test_remote_recognition_client_round_trip(tmp_path) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(RecognitionService(FakeBackend(), "test-token")))
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
