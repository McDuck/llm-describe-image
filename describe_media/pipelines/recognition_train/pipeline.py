"""Pipeline for training the reviewed face-recognition identity index."""

from __future__ import annotations

import os
from typing import Optional

from describe_media.config_loader import DEFAULT_RECOGNITION_MATCH_THRESHOLD, DEFAULT_RECOGNITION_MODEL
from describe_media.pipelines.pipeline import Pipeline
from describe_media.recognition.gpus.api.backend import RemoteRecognitionBackend
from describe_media.gpu_api import remote_gpu_api_config
from describe_media.recognition.index import train_recognition_index


class RecognitionTrainingPipeline(Pipeline):
    """Build an identity index from folder-based reviewer curation."""

    def __init__(self) -> None:
        super().__init__(
            name="recognition-train",
            description="Builds a local identity index from reviewed recognition clusters",
        )

    def run(
        self,
        input_dir: str,
        output_dir: Optional[str] = None,
        verbose: bool = False,
        status_interval: float = 5.0,
        **kwargs: object,
    ) -> None:
        remote_api_base, remote_api_token, remote_timeout_seconds = remote_gpu_api_config()
        backend = None
        if remote_api_base:
            backend = RemoteRecognitionBackend(
                remote_api_base,
                remote_api_token or "",
                timeout_seconds=remote_timeout_seconds,
            )
        index_path = train_recognition_index(
            input_dir=os.path.abspath(input_dir),
            output_dir=os.path.abspath(output_dir or input_dir),
            model_name=os.getenv("RECOGNITION_MODEL", DEFAULT_RECOGNITION_MODEL),
            match_threshold=float(os.getenv("RECOGNITION_MATCH_THRESHOLD", DEFAULT_RECOGNITION_MATCH_THRESHOLD)),
            backend=backend,
        )
        print(f"Built local recognition index: {index_path}")
