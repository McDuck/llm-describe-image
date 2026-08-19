"""InsightFace adapter used only by the independently deployed worker."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass
class FaceDetection:
    bbox: Dict[str, int]
    confidence: float
    embedding: List[float]
    image_size: Tuple[int, int]


def normalise_embedding(values: Sequence[float]) -> List[float]:
    magnitude = sum(value * value for value in values) ** 0.5
    if magnitude == 0:
        raise ValueError("Face embedding cannot have zero magnitude")
    return [float(value) / magnitude for value in values]


class InsightFaceBackend:
    """Load InsightFace lazily so server configuration errors stay readable."""

    def __init__(self, model_name: str = "buffalo_l", providers: Optional[List[str]] = None) -> None:
        self.model_name = model_name
        self.providers = providers or ["CPUExecutionProvider"]
        self._app = None

    def load(self) -> None:
        try:
            from insightface.app import FaceAnalysis
        except ImportError as error:
            raise RuntimeError(
                "The recognition worker requires its own dependencies. "
                "Install external_gpu_host/recognition/worker/requirements.txt."
            ) from error
        self._app = FaceAnalysis(name=self.model_name, providers=self.providers)
        self._app.prepare(ctx_id=0 if "CUDAExecutionProvider" in self.providers else -1, det_size=(640, 640))

    def detect_bytes(self, image_bytes: bytes) -> List[FaceDetection]:
        if self._app is None:
            raise RuntimeError("InsightFace backend has not been loaded")
        from PIL import Image

        with Image.open(BytesIO(image_bytes)) as source:
            return self._detect_image(source)

    def detect(self, image_path: str) -> List[FaceDetection]:
        if self._app is None:
            raise RuntimeError("InsightFace backend has not been loaded")
        from PIL import Image

        with Image.open(Path(image_path)) as source:
            return self._detect_image(source)

    def _detect_image(self, source: object) -> List[FaceDetection]:
        import numpy as np
        from PIL import ImageOps

        image = ImageOps.exif_transpose(source).convert("RGB")
        width, height = image.size
        pixels = np.asarray(image)[:, :, ::-1]
        detections: List[FaceDetection] = []
        for face in self._app.get(pixels):
            left, top, right, bottom = [int(round(value)) for value in face.bbox]
            left, top = max(0, left), max(0, top)
            right, bottom = min(width, right), min(height, bottom)
            if right <= left or bottom <= top:
                continue
            detections.append(
                FaceDetection(
                    bbox={"x": left, "y": top, "width": right - left, "height": bottom - top},
                    confidence=float(face.det_score),
                    embedding=normalise_embedding(face.embedding.tolist()),
                    image_size=(width, height),
                )
            )
        return detections
