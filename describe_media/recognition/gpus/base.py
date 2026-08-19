"""Common recognition backend types and embedding utilities."""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple


@dataclass
class FaceDetection:
    bbox: Dict[str, int]
    confidence: float
    embedding: List[float]
    image_size: Tuple[int, int]


class RecognitionBackend(abc.ABC):
    """Detect and encode faces from source images."""

    @abc.abstractmethod
    def load(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def detect(self, image_path: str) -> List[FaceDetection]:
        raise NotImplementedError


def normalise_embedding(values: Sequence[float]) -> List[float]:
    magnitude = sum(value * value for value in values) ** 0.5
    if magnitude == 0:
        raise ValueError("Face embedding cannot have zero magnitude")
    return [float(value) / magnitude for value in values]


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right):
        raise ValueError("Face embeddings have different dimensions")
    return sum(a * b for a, b in zip(normalise_embedding(left), normalise_embedding(right)))
