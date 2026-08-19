"""Direct OpenCV implementation of video-frame extraction."""

from __future__ import annotations

import math
from typing import List, Tuple

from describe_media.tasks.extract_video.gpus.base import VideoFrame, VideoFrameBackend


class DirectVideoFrameBackend(VideoFrameBackend):
    """Decode source videos on the pipeline host with OpenCV."""

    def extract(
        self,
        video_path: str,
        frame_interval_seconds: float,
        max_frames: int,
    ) -> Tuple[float, List[VideoFrame]]:
        try:
            import cv2  # type: ignore
        except ImportError as error:
            raise RuntimeError(
                "Video support requires 'opencv-python-headless' "
                "(install with pip install -e \"describe_media[video]\")"
            ) from error

        capture = cv2.VideoCapture(video_path)
        if not capture.isOpened():
            raise RuntimeError(f"Could not open video file: {video_path}")
        try:
            fps = float(capture.get(cv2.CAP_PROP_FPS))
            frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
            if fps <= 0 or frame_count <= 0:
                raise RuntimeError(f"Could not determine video duration: {video_path}")
            duration = frame_count / fps
            timestamps = self._sample_timestamps(duration, fps, frame_interval_seconds, max_frames)
            frames: List[VideoFrame] = []
            for number, timestamp in enumerate(timestamps, start=1):
                capture.set(cv2.CAP_PROP_POS_MSEC, timestamp * 1000.0)
                success, frame = capture.read()
                if not success:
                    raise RuntimeError(f"Could not extract frame at {timestamp:.3f} seconds from {video_path}")
                encoded, jpeg = cv2.imencode(".jpg", frame)
                if not encoded:
                    raise RuntimeError(f"Could not encode frame at {timestamp:.3f} seconds from {video_path}")
                frames.append(VideoFrame(number, timestamp, jpeg.tobytes()))
            return duration, frames
        finally:
            capture.release()

    @staticmethod
    def _sample_timestamps(
        duration: float,
        fps: float,
        frame_interval_seconds: float,
        max_frames: int,
    ) -> List[float]:
        requested = max(1, int(math.ceil(duration / frame_interval_seconds)))
        count = min(requested, max_frames)
        if count == 1:
            return [0.0]
        latest = max(0.0, duration - (1.0 / fps))
        return [round(latest * index / (count - 1), 3) for index in range(count)]
