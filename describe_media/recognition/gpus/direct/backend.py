"""Small adapter around InsightFace that keeps all recognition local."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import List, Optional

from describe_media.recognition.gpus.base import FaceDetection, RecognitionBackend, normalise_embedding


class InsightFaceBackend(RecognitionBackend):
    """Lazy InsightFace loader so importing the normal describe pipeline is cheap."""

    backend_name = "insightface"

    def __init__(self, model_name: str = "buffalo_l", providers: Optional[List[str]] = None) -> None:
        self.model_name = model_name
        self.providers = providers or ["CPUExecutionProvider"]
        self._app = None

    def load(self) -> None:
        try:
            from insightface.app import FaceAnalysis
        except ImportError as error:
            raise RuntimeError(
                "Local recognition requires the recognition dependencies. "
                "The Docker image must be rebuilt after this change."
            ) from error
        self._app = FaceAnalysis(name=self.model_name, providers=self.providers)
        self._app.prepare(ctx_id=0 if "CUDAExecutionProvider" in self.providers else -1, det_size=(640, 640))

    def detect(self, image_path: str) -> List[FaceDetection]:
        if self._app is None:
            raise RuntimeError("InsightFace backend has not been loaded")
        from PIL import Image

        with Image.open(Path(image_path)) as source:
            return self._detect_image(source)

    def detect_bytes(self, image_bytes: bytes) -> List[FaceDetection]:
        """Detect faces in a network payload without writing it to disk."""
        if self._app is None:
            raise RuntimeError("InsightFace backend has not been loaded")
        from PIL import Image

        with Image.open(BytesIO(image_bytes)) as source:
            return self._detect_image(source)

    def _detect_image(self, source: object) -> List[FaceDetection]:
        import numpy as np
        from PIL import ImageOps

        image = ImageOps.exif_transpose(source).convert("RGB")
        width, height = image.size
        # InsightFace expects BGR, while Pillow gives us RGB.
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
