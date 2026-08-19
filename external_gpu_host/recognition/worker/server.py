"""Run an authenticated GPU-capable InsightFace recognition worker."""

from __future__ import annotations

import argparse
import hmac
import json
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import List

try:  # Support both ``python server.py`` and package execution.
    from .face_backend import FaceDetection, InsightFaceBackend
except ImportError:  # pragma: no cover - exercised by the standalone entry point
    from face_backend import FaceDetection, InsightFaceBackend


MAX_IMAGE_BYTES = 32 * 1024 * 1024


def _serialise_detection(detection: FaceDetection) -> dict:
    return {"bbox": detection.bbox, "confidence": detection.confidence, "embedding": detection.embedding, "image_size": list(detection.image_size)}


class RecognitionService:
    def __init__(self, backend: InsightFaceBackend, token: str) -> None:
        self.backend = backend
        self.token = token
        # DirectML sessions permit one Run call at a time. This also bounds VRAM use.
        self.inference_lock = threading.Lock()

    def detect(self, image_bytes: bytes) -> List[FaceDetection]:
        with self.inference_lock:
            return self.backend.detect_bytes(image_bytes)

    def authorised(self, header: str) -> bool:
        return hmac.compare_digest(header, f"Bearer {self.token}")


def build_handler(service: RecognitionService):
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
            if self.path != "/v1/recognition":
                self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                self._json(HTTPStatus.BAD_REQUEST, {"error": "Content-Length is required"})
                return
            if length < 1 or length > MAX_IMAGE_BYTES:
                self._json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "image exceeds size limit"})
                return
            try:
                detections = service.detect(self.rfile.read(length))
            except Exception as error:
                self._json(HTTPStatus.UNPROCESSABLE_ENTITY, {"error": str(error)})
                return
            self._json(HTTPStatus.OK, {"detections": [_serialise_detection(item) for item in detections]})

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
            print("recognition-server:", format % args)

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve InsightFace detections over an authenticated local HTTP API")
    parser.add_argument("--host", default="127.0.0.1", help="Listen address (default: loopback only)")
    parser.add_argument("--port", type=int, default=5002, help="Listen port (default: 5002)")
    parser.add_argument("--model", default=os.getenv("RECOGNITION_MODEL", "buffalo_l"))
    parser.add_argument("--provider", default=os.getenv("RECOGNITION_PROVIDER", "DmlExecutionProvider"))
    parser.add_argument("--token", default=os.getenv("RECOGNITION_API_TOKEN"))
    args = parser.parse_args()
    if not args.token:
        parser.error("--token or RECOGNITION_API_TOKEN is required")

    backend = InsightFaceBackend(model_name=args.model, providers=[args.provider, "CPUExecutionProvider"])
    backend.load()
    server = ThreadingHTTPServer((args.host, args.port), build_handler(RecognitionService(backend, args.token)))
    print(f"Recognition server listening on http://{args.host}:{args.port}/v1 using {args.provider}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping recognition server")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
