"""Run the authenticated shared GPU API."""

from __future__ import annotations

import argparse
import hmac
import json
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, List, Optional, Tuple



def _load_environment_file(path: Path) -> None:
    """Load simple KEY=VALUE settings without overriding the process environment."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip("\"'")


_load_environment_file(Path(__file__).resolve().parent / ".env")

try:  # Support both ``python server.py`` and package execution.
    from external_gpu_host.gpu.services.recognition.face_backend import FaceDetection, InsightFaceBackend
except ImportError:  # pragma: no cover - exercised by the standalone entry point
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from external_gpu_host.gpu.services.recognition.face_backend import FaceDetection, InsightFaceBackend

from external_gpu_host.gpu.services.video_frames.http_api import serve_video_frames_request
from external_gpu_host.gpu.services.video_frames.video_frames import ExtractedVideoFrame, extract_video_file
from external_gpu_host.gpu.services.transcription.http_api import serve_audio_transcription_request
from external_gpu_host.gpu.services.transcription.models import get_backend as get_transcription_backend


MAX_IMAGE_BYTES = 32 * 1024 * 1024


def _serialise_detection(detection: FaceDetection) -> dict:
    return {"bbox": detection.bbox, "confidence": detection.confidence, "embedding": detection.embedding, "image_size": list(detection.image_size)}


class GpuApiService:
    def __init__(
        self,
        backend: InsightFaceBackend,
        token: str,
        video_extractor: Callable[[str, float, int], Tuple[float, List[ExtractedVideoFrame]]] = extract_video_file,
        audio_transcriber: Optional[Callable[[str, str, str], str]] = None,
    ) -> None:
        self.backend = backend
        self.token = token
        self.video_extractor = video_extractor
        self.audio_transcriber = audio_transcriber
        self.transcription_backends = {}
        # DirectML sessions permit one Run call at a time; serialising also bounds
        # concurrent decoder/VRAM use while a video request is being handled.
        self.inference_lock = threading.Lock()

    def detect(self, image_bytes: bytes) -> List[FaceDetection]:
        with self.inference_lock:
            return self.backend.detect_bytes(image_bytes)

    def extract_video_frames(self, video_path: str, frame_interval_seconds: float, max_frames: int) -> Tuple[float, List[ExtractedVideoFrame]]:
        with self.inference_lock:
            return self.video_extractor(video_path, frame_interval_seconds, max_frames)

    def transcribe_audio(self, audio_path: str, backend_name: str, model_name: str, language: str) -> str:
        with self.inference_lock:
            if self.audio_transcriber is not None:
                return self.audio_transcriber(audio_path, model_name, language)
            backend = self.transcription_backends.get(backend_name)
            if backend is None:
                backend = get_transcription_backend(backend_name)
                self.transcription_backends[backend_name] = backend
            return backend.transcribe(audio_path, model_name, language)

    def authorised(self, header: str) -> bool:
        return hmac.compare_digest(header, f"Bearer {self.token}")


def build_handler(service: GpuApiService):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            if not self._authorise():
                return
            if self.path != "/v1/health":
                self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return
            self._json(HTTPStatus.OK, {"status": "ok"})

        def do_POST(self) -> None:
            if not self._authorise():
                return
            if self.path == "/v1/recognition":
                self._recognition()
                return
            if self.path == "/v1/video-frames":
                serve_video_frames_request(self, service.extract_video_frames)
                return
            if self.path == "/v1/audio-transcriptions":
                serve_audio_transcription_request(self, service.transcribe_audio)
                return
            self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def _recognition(self) -> None:
            length = self._content_length(MAX_IMAGE_BYTES, "image")
            if length is None:
                return
            try:
                detections = service.detect(self.rfile.read(length))
            except Exception as error:
                self._json(HTTPStatus.UNPROCESSABLE_ENTITY, {"error": str(error)})
                return
            self._json(HTTPStatus.OK, {"detections": [_serialise_detection(item) for item in detections]})

        def _content_length(self, maximum: int, label: str) -> Optional[int]:
            try:
                length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                self._json(HTTPStatus.BAD_REQUEST, {"error": "Content-Length is required"})
                return None
            if length < 1 or length > maximum:
                self._json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": f"{label} exceeds size limit"})
                return None
            return length

        def _authorise(self) -> bool:
            if service.authorised(self.headers.get("Authorization", "")):
                return True
            self._json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorised"})
            return False

        def _json(self, status: HTTPStatus, payload: dict) -> None:
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            print("gpu-api-server:", format % args)

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve authenticated recognition and video-frame GPU APIs")
    parser.add_argument(
        "--host",
        default=os.getenv("GPU_API_HOST", "127.0.0.1"),
        help="Listen address (default: GPU_API_HOST or loopback only)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("GPU_API_PORT", "5002")),
        help="Listen port (default: GPU_API_PORT or 5002)",
    )
    parser.add_argument("--model", default=os.getenv("RECOGNITION_MODEL", "buffalo_l"))
    parser.add_argument("--provider", default=os.getenv("RECOGNITION_PROVIDER", "DmlExecutionProvider"))
    parser.add_argument("--token", default=os.getenv("GPU_API_TOKEN"))
    args = parser.parse_args()
    if not args.token:
        parser.error("--token or GPU_API_TOKEN is required")

    backend = InsightFaceBackend(model_name=args.model, providers=[args.provider, "CPUExecutionProvider"])
    backend.load()
    server = ThreadingHTTPServer((args.host, args.port), build_handler(GpuApiService(backend, args.token)))
    print(f"GPU API server listening on http://{args.host}:{args.port}/v1 using {args.provider}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping GPU API server")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
