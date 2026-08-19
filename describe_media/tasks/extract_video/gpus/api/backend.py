"""Authenticated client for the remote video-frame API."""

from __future__ import annotations

import base64
import json
import math
from http.client import HTTPConnection, HTTPSConnection
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import urlsplit

from describe_media.tasks.extract_video.gpus.base import VideoFrame, VideoFrameBackend


class RemoteVideoFrameBackend(VideoFrameBackend):
    """Upload a video to the shared GPU worker and receive sampled JPEG frames."""

    def __init__(self, api_base: str, token: str, timeout_seconds: float = 120.0) -> None:
        if not api_base.startswith(("http://", "https://")):
            raise ValueError("Video-frame API base must be an http(s) URL")
        if not token:
            raise ValueError("Video-frame API token is required")
        self.api_base = api_base.rstrip("/")
        self.token = token
        self.timeout_seconds = float(timeout_seconds)

    def load(self) -> None:
        self._request("GET", "/health")

    def extract(self, video_path: str, frame_interval_seconds: float, max_frames: int) -> Tuple[float, List[VideoFrame]]:
        payload = self._upload_video(video_path, frame_interval_seconds, max_frames)
        duration = payload.get("duration_seconds")
        frames = payload.get("frames")
        if not isinstance(duration, (int, float)) or not math.isfinite(float(duration)) or float(duration) < 0:
            raise RuntimeError("Video-frame worker returned an invalid duration")
        if not isinstance(frames, list) or not frames:
            raise RuntimeError("Video-frame worker returned no frames")
        return float(duration), [self._decode_frame(item) for item in frames]

    def _upload_video(self, video_path: str, frame_interval_seconds: float, max_frames: int) -> Dict[str, Any]:
        """Stream an upload so large source videos never occupy process memory."""
        parsed = urlsplit(self.api_base)
        connection_type = HTTPSConnection if parsed.scheme == "https" else HTTPConnection
        connection = connection_type(parsed.hostname, parsed.port, timeout=self.timeout_seconds)
        endpoint = (parsed.path.rstrip("/") + "/video-frames") or "/video-frames"
        if parsed.query:
            endpoint = f"{endpoint}?{parsed.query}"
        size = Path(video_path).stat().st_size
        try:
            connection.putrequest("POST", endpoint)
            connection.putheader("Authorization", f"Bearer {self.token}")
            connection.putheader("Content-Type", "application/octet-stream")
            connection.putheader("Content-Length", str(size))
            connection.putheader("X-Frame-Interval-Seconds", str(frame_interval_seconds))
            connection.putheader("X-Max-Frames", str(max_frames))
            connection.endheaders()
            with Path(video_path).open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    connection.send(chunk)
            response = connection.getresponse()
            raw = response.read()
        except (OSError, ValueError) as error:
            raise RuntimeError(f"Video-frame worker request failed: {error}") from error
        finally:
            connection.close()
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as error:
            raise RuntimeError("Video-frame worker returned invalid JSON") from error
        if response.status >= 400:
            detail = raw.decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"Video-frame worker returned HTTP {response.status}: {detail}")
        if not isinstance(payload, dict):
            raise RuntimeError("Video-frame worker returned an invalid JSON object")
        return payload

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[bytes] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.token}"}
        if body is not None:
            headers["Content-Type"] = "application/octet-stream"
        if extra_headers:
            headers.update(extra_headers)
        request = Request(f"{self.api_base}{path}", data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"Video-frame worker returned HTTP {error.code}: {detail}") from error
        except (URLError, OSError, ValueError) as error:
            raise RuntimeError(f"Video-frame worker request failed: {error}") from error
        if not isinstance(payload, dict):
            raise RuntimeError("Video-frame worker returned an invalid JSON object")
        return payload

    @staticmethod
    def _decode_frame(value: Any) -> VideoFrame:
        if not isinstance(value, dict):
            raise RuntimeError("Video-frame worker returned an invalid frame")
        number = value.get("number")
        timestamp = value.get("timestamp_seconds")
        encoded = value.get("jpeg_base64")
        if not isinstance(number, int) or number < 1 or not isinstance(timestamp, (int, float)) or not math.isfinite(float(timestamp)) or float(timestamp) < 0 or not isinstance(encoded, str):
            raise RuntimeError("Video-frame worker returned an invalid frame")
        try:
            jpeg_bytes = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError) as error:
            raise RuntimeError("Video-frame worker returned an invalid JPEG payload") from error
        if not jpeg_bytes:
            raise RuntimeError("Video-frame worker returned an empty JPEG payload")
        return VideoFrame(number, float(timestamp), jpeg_bytes)
