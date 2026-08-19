"""Authenticated HTTP client for a remote InsightFace inference worker."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from describe_media.recognition.gpus.base import FaceDetection, RecognitionBackend


class RemoteRecognitionBackend(RecognitionBackend):
    """Send an image to a trusted recognition worker and decode its detections."""

    backend_name = "remote"

    def __init__(self, api_base: str, token: str, timeout_seconds: float = 120.0) -> None:
        if not api_base.startswith(("http://", "https://")):
            raise ValueError("Recognition API base must be an http(s) URL")
        if not token:
            raise ValueError("Recognition API token is required")
        self.api_base = api_base.rstrip("/")
        self.token = token
        self.timeout_seconds = float(timeout_seconds)
        self.model_name = "remote"

    def load(self) -> None:
        """Check that the authenticated worker is reachable before processing."""
        self._request("GET", "/health")

    def detect(self, image_path: str) -> List[FaceDetection]:
        with Path(image_path).open("rb") as handle:
            body = handle.read()
        payload = self._request("POST", "/recognition", body)
        detections = payload.get("detections")
        if not isinstance(detections, list):
            raise RuntimeError("Recognition worker returned an invalid response")
        return [self._decode_detection(item) for item in detections]

    def _request(self, method: str, path: str, body: Optional[bytes] = None) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.token}"}
        if body is not None:
            headers["Content-Type"] = "application/octet-stream"
        request = Request(f"{self.api_base}{path}", data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"Recognition worker returned HTTP {error.code}: {detail}") from error
        except (URLError, OSError, ValueError) as error:
            raise RuntimeError(f"Recognition worker request failed: {error}") from error
        if not isinstance(payload, dict):
            raise RuntimeError("Recognition worker returned an invalid JSON object")
        return payload

    @staticmethod
    def _decode_detection(value: Any) -> FaceDetection:
        if not isinstance(value, dict):
            raise RuntimeError("Recognition worker returned an invalid detection")
        bbox = value.get("bbox")
        image_size = value.get("image_size")
        embedding = value.get("embedding")
        if (
            not isinstance(bbox, dict)
            or not all(isinstance(bbox.get(key), int) for key in ("x", "y", "width", "height"))
            or not isinstance(image_size, list)
            or len(image_size) != 2
            or not all(isinstance(item, int) for item in image_size)
            or not isinstance(embedding, list)
            or not all(isinstance(item, (int, float)) for item in embedding)
            or not isinstance(value.get("confidence"), (int, float))
        ):
            raise RuntimeError("Recognition worker returned an invalid detection")
        return FaceDetection(
            bbox={key: bbox[key] for key in ("x", "y", "width", "height")},
            confidence=float(value["confidence"]),
            embedding=[float(item) for item in embedding],
            image_size=(image_size[0], image_size[1]),
        )
