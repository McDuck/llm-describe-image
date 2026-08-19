"""HTTP endpoint implementation for remote video-frame extraction."""

from __future__ import annotations

import base64
import math
import os
import tempfile
from http import HTTPStatus
from typing import Any, Callable, List, Optional, Tuple

from .video_frames import ExtractedVideoFrame


MAX_VIDEO_BYTES = int(os.getenv("VIDEO_FRAME_API_MAX_UPLOAD_BYTES", str(8 * 1024 * 1024 * 1024)))
MAX_VIDEO_FRAMES = 1_000


def serve_video_frames_request(
    handler: Any,
    extract: Callable[[str, float, int], Tuple[float, List[ExtractedVideoFrame]]],
) -> None:
    """Process a binary upload and emit sampled JPEGs through a shared handler."""
    length = _content_length(handler)
    if length is None:
        return
    try:
        interval = float(handler.headers.get("X-Frame-Interval-Seconds", ""))
        max_frames = int(handler.headers.get("X-Max-Frames", ""))
    except ValueError:
        handler._json(HTTPStatus.BAD_REQUEST, {"error": "valid X-Frame-Interval-Seconds and X-Max-Frames headers are required"})
        return
    if not math.isfinite(interval) or interval <= 0 or not 1 <= max_frames <= MAX_VIDEO_FRAMES:
        handler._json(HTTPStatus.BAD_REQUEST, {"error": "video sampling settings are out of range"})
        return
    descriptor, temporary_path = tempfile.mkstemp(prefix="video-frame-", suffix=".mp4")
    try:
        with os.fdopen(descriptor, "wb") as handle:
            remaining = length
            while remaining:
                chunk = handler.rfile.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise ValueError("request body ended before Content-Length")
                handle.write(chunk)
                remaining -= len(chunk)
        duration, frames = extract(temporary_path, interval, max_frames)
    except Exception as error:
        handler._json(HTTPStatus.UNPROCESSABLE_ENTITY, {"error": str(error)})
        return
    finally:
        try:
            os.unlink(temporary_path)
        except OSError:
            pass
    handler._json(HTTPStatus.OK, {
        "duration_seconds": duration,
        "frames": [
            {
                "number": frame.number,
                "timestamp_seconds": frame.timestamp_seconds,
                "jpeg_base64": base64.b64encode(frame.jpeg_bytes).decode("ascii"),
            }
            for frame in frames
        ],
    })


def _content_length(handler: Any) -> Optional[int]:
    try:
        length = int(handler.headers.get("Content-Length", ""))
    except ValueError:
        handler._json(HTTPStatus.BAD_REQUEST, {"error": "Content-Length is required"})
        return None
    if length < 1 or length > MAX_VIDEO_BYTES:
        handler._json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "video exceeds size limit"})
        return None
    return length
