"""Common interface for video-frame extraction backends."""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class VideoFrame:
    number: int
    timestamp_seconds: float
    jpeg_bytes: bytes


class VideoFrameBackend(abc.ABC):
    """Extract sampled JPEG frames from a source video."""

    def load(self) -> None:
        """Prepare the backend before processing work."""

    @abc.abstractmethod
    def extract(
        self,
        video_path: str,
        frame_interval_seconds: float,
        max_frames: int,
    ) -> Tuple[float, List[VideoFrame]]:
        raise NotImplementedError
