"""Video-frame extraction used by the shared remote GPU API."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class ExtractedVideoFrame:
    """A sampled frame encoded as a JPEG for HTTP transport."""

    number: int
    timestamp_seconds: float
    jpeg_bytes: bytes


def extract_video_file(
    video_path: str,
    frame_interval_seconds: float,
    max_frames: int,
) -> Tuple[float, List[ExtractedVideoFrame]]:
    """Decode evenly-spaced video frames and encode them as JPEG bytes."""
    try:
        import cv2  # type: ignore
    except ImportError as error:
        raise RuntimeError(
            "Video-frame extraction requires opencv-python-headless. "
            "Install the shared GPU worker requirements."
        ) from error

    capture = cv2.VideoCapture(video_path)
    if not capture.isOpened():
        raise RuntimeError(f"Could not open uploaded video: {video_path}")
    try:
        fps = float(capture.get(cv2.CAP_PROP_FPS))
        frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
        if fps <= 0 or frame_count <= 0:
            raise RuntimeError("Could not determine uploaded video duration")
        duration = frame_count / fps
        timestamps = sample_timestamps(duration, fps, frame_interval_seconds, max_frames)
        frames: List[ExtractedVideoFrame] = []
        for number, timestamp in enumerate(timestamps, start=1):
            capture.set(cv2.CAP_PROP_POS_MSEC, timestamp * 1000.0)
            success, frame = capture.read()
            if not success:
                raise RuntimeError(f"Could not extract frame at {timestamp:.3f} seconds")
            encoded, jpeg = cv2.imencode(".jpg", frame)
            if not encoded:
                raise RuntimeError(f"Could not encode frame at {timestamp:.3f} seconds")
            frames.append(ExtractedVideoFrame(number, timestamp, jpeg.tobytes()))
        return duration, frames
    finally:
        capture.release()


def sample_timestamps(
    duration: float,
    fps: float,
    frame_interval_seconds: float,
    max_frames: int,
) -> List[float]:
    """Match Describe Media's local frame sampling contract."""
    if not math.isfinite(frame_interval_seconds) or frame_interval_seconds <= 0:
        raise ValueError("frame_interval_seconds must be positive")
    if max_frames < 1:
        raise ValueError("max_frames must be positive")
    requested = max(1, int(math.ceil(duration / frame_interval_seconds)))
    count = min(requested, max_frames)
    if count == 1:
        return [0.0]
    latest = max(0.0, duration - (1.0 / fps))
    return [round(latest * index / (count - 1), 3) for index in range(count)]
