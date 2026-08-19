"""Face-recognition clustering pipeline for later human review."""

from __future__ import annotations

import os
import json
import random
import re
from pathlib import Path
from typing import List, Optional, Set

from describe_media.config_loader import (
    DEFAULT_IMAGE_EXTENSIONS,
    DEFAULT_NUM_DISCOVER_THREADS,
    DEFAULT_NUM_DOWNLOAD_THREADS,
    DEFAULT_RECOGNITION_CLUSTER_THRESHOLD,
    DEFAULT_RECOGNITION_DETECTION_THRESHOLD,
    DEFAULT_RECOGNITION_MODEL,
    DEFAULT_SORT_ORDER,
)
from describe_media.gpu_api import remote_gpu_api_config
from describe_media.pipelines.pipeline import Pipeline


def _recognition_preparation_kwargs(pipeline: "RecognitionClusterPipeline") -> dict:
    remote_api_base, remote_api_token, remote_timeout_seconds = remote_gpu_api_config()
    return {
        "maximum": pipeline.num_recognition_threads,
        "input_dir": pipeline.input_dir,
        "output_dir": pipeline.output_dir,
        "model_name": os.getenv("RECOGNITION_MODEL", DEFAULT_RECOGNITION_MODEL),
        "detection_threshold": float(
            os.getenv("RECOGNITION_DETECTION_THRESHOLD", DEFAULT_RECOGNITION_DETECTION_THRESHOLD)
        ),
        "cluster_threshold": float(
            os.getenv("RECOGNITION_CLUSTER_THRESHOLD", DEFAULT_RECOGNITION_CLUSTER_THRESHOLD)
        ),
        "retry": pipeline.retry,
        "remote_api_base": remote_api_base,
        "remote_api_token": remote_api_token,
        "remote_timeout_seconds": remote_timeout_seconds,
    }


class RecognitionClusterPipeline(Pipeline):
    """Discover â†’ Download metadata â†’ Recognition review artifacts."""

    PIPELINE_CONFIG = [
        {
            "name": "Discover",
            "class_name": "DiscoverTask",
            "dir": "discover",
            "kwargs_builder": lambda self: {
                "maximum": self.num_discover_threads,
                "input_dir": self.input_dir,
                "image_extensions": DEFAULT_IMAGE_EXTENSIONS,
                "sort_order": os.getenv("SORT_ORDER", DEFAULT_SORT_ORDER),
            },
            "task": "Discover",
            "num_threads": 1,
            "next_task": "Download",
            "has_pending_queue": True,
        },
        {
            "name": "Download",
            "class_name": "DownloadTask",
            "dir": "download",
            "kwargs_builder": lambda self: {
                "maximum": self.num_download_threads,
                "input_dir": self.input_dir,
            },
            "task": "Download",
            "num_threads_getter": "num_download_threads",
            "next_task": "Recognize",
        },
        {
            "name": "Recognize",
            "class_name": "RecognitionPreparationTask",
            "dir": "recognition",
            "kwargs_builder": _recognition_preparation_kwargs,
            "task": "Recognize",
            "num_threads_getter": "num_recognition_threads",
            "next_task": None,
        },
    ]

    def __init__(self) -> None:
        super().__init__(name="recognition-cluster", description="Creates face-recognition clusters for later review")
        self.input_dir: Optional[str] = None
        self.output_dir: Optional[str] = None
        self.num_discover_threads = DEFAULT_NUM_DISCOVER_THREADS
        self.num_download_threads = DEFAULT_NUM_DOWNLOAD_THREADS
        # Provisional clustering mutates one shared centroid state; serialise it.
        self.num_recognition_threads = 1
        self.retry = False

    def run(
        self,
        input_dir: str,
        output_dir: Optional[str] = None,
        verbose: bool = False,
        status_interval: float = 5.0,
        subdirectory: Optional[str] = None,
        manifest_path: Optional[str] = None,
        random_sample_size: Optional[int] = None,
        **kwargs: object,
    ) -> None:
        self.input_dir = os.path.abspath(input_dir)
        self.output_dir = os.path.abspath(output_dir or input_dir)
        self.verbose = verbose
        selected_sources = sum(value is not None for value in (subdirectory, manifest_path, random_sample_size))
        if selected_sources > 1:
            raise ValueError("Use one recognition-cluster subdirectory, manifest, or random sample")
        self._load_tasks_from_config()
        if manifest_path or random_sample_size is not None:
            download_task = self.get_task("Download")
            if download_task is None:
                raise RuntimeError("Recognition pipeline has no download task")
            source_paths = (
                self._load_manifest(manifest_path)
                if manifest_path
                else self._select_random_images(int(random_sample_size))
            )
            print(f"Selected {len(source_paths)} source images for recognition clustering")
            for source_path in source_paths:
                download_task.add(source_path)
        else:
            discovery_dir = self._resolve_discovery_dir(subdirectory)
            first_task = self.get_task("Discover")
            if first_task is None:
                raise RuntimeError("Recognition pipeline has no discovery task")
            first_task.add(discovery_dir)
        self._run_pipeline(status_interval=status_interval)

    def _resolve_discovery_dir(self, subdirectory: Optional[str]) -> str:
        if not subdirectory:
            return self.input_dir or ""
        candidate = os.path.abspath(os.path.join(self.input_dir or "", subdirectory))
        if os.path.commonpath([self.input_dir or "", candidate]) != (self.input_dir or ""):
            raise ValueError("Recognition subdirectory must remain below INPUT_DIR")
        if not os.path.isdir(candidate):
            raise ValueError(f"Recognition subdirectory does not exist: {candidate}")
        return candidate

    def _load_manifest(self, manifest_path: str) -> List[str]:
        source_root = self.input_dir or ""
        manifest = Path(manifest_path)
        if not manifest.is_file():
            raise ValueError(f"Recognition manifest does not exist: {manifest}")
        paths: List[str] = []
        seen: Set[str] = set()
        for raw_line in manifest.read_text(encoding="utf-8").splitlines():
            relative_path = raw_line.strip().replace("/", os.sep)
            if not relative_path or relative_path.startswith("#"):
                continue
            candidate = os.path.abspath(os.path.join(source_root, relative_path))
            try:
                is_within_source = os.path.commonpath([source_root, candidate]) == source_root
            except ValueError:
                is_within_source = False
            if not is_within_source:
                raise ValueError(f"Manifest path must remain below INPUT_DIR: {raw_line}")
            if not os.path.isfile(candidate):
                raise ValueError(f"Manifest image does not exist: {raw_line}")
            if os.path.splitext(candidate)[1].lower() not in DEFAULT_IMAGE_EXTENSIONS:
                raise ValueError(f"Manifest file is not a configured image type: {raw_line}")
            if candidate not in seen:
                seen.add(candidate)
                paths.append(candidate)
        if not paths:
            raise ValueError("Recognition manifest did not contain any image paths")
        return paths

    def _select_random_images(self, count: int) -> List[str]:
        """Select a distinct random batch without scanning the complete tree."""
        if count <= 0:
            raise ValueError("Random recognition-cluster sample count must be positive")
        source_root = Path(self.input_dir or "")
        year_pattern = re.compile(r"^(19|20)\d{2}$")
        month_pattern = re.compile(r"^(19|20)\d{2}-\d{2}$")
        day_pattern = re.compile(r"^(19|20)\d{2}-\d{2}-\d{2}$")
        years = self._child_directories(source_root, year_pattern)
        if not years:
            raise ValueError(f"No year directories found below INPUT_DIR: {source_root}")

        excluded = self._reviewed_source_paths()
        selected: List[str] = []
        seen: Set[str] = set(excluded)
        month_cache = {}
        day_cache = {}
        image_cache = {}
        attempts = 0
        max_attempts = count * 500
        while len(selected) < count and attempts < max_attempts:
            attempts += 1
            year = random.choice(years)
            months = month_cache.get(year)
            if months is None:
                months = self._child_directories(year, month_pattern)
                month_cache[year] = months
            if not months:
                continue
            month = random.choice(months)
            days = day_cache.get(month)
            if days is None:
                days = self._child_directories(month, day_pattern)
                day_cache[month] = days
            if not days:
                continue
            day = random.choice(days)
            images = image_cache.get(day)
            if images is None:
                images = self._image_files(day)
                image_cache[day] = images
            if not images:
                continue
            image = random.choice(images)
            relative_path = image.relative_to(source_root).as_posix()
            if relative_path in seen:
                continue
            seen.add(relative_path)
            selected.append(str(image))
        if len(selected) != count:
            raise RuntimeError(
                f"Only found {len(selected)} unreviewed random source images after {attempts} attempts; requested {count}"
            )
        return selected

    @staticmethod
    def _child_directories(parent: Path, pattern: re.Pattern) -> List[Path]:
        try:
            return [entry for entry in parent.iterdir() if entry.is_dir() and pattern.fullmatch(entry.name)]
        except OSError:
            return []

    @staticmethod
    def _image_files(parent: Path) -> List[Path]:
        try:
            return [
                entry
                for entry in parent.iterdir()
                if entry.is_file() and entry.suffix.lower() in DEFAULT_IMAGE_EXTENSIONS
            ]
        except OSError:
            return []

    def _reviewed_source_paths(self) -> Set[str]:
        recognition_root = Path(self.output_dir or "") / "recognition"
        if not recognition_root.exists():
            return set()
        sources: Set[str] = set()
        for record_path in recognition_root.rglob("*.json"):
            if record_path.name == "index.json":
                continue
            try:
                payload = json.loads(record_path.read_text(encoding="utf-8"))
                if payload.get("kind") != "recognition-review":
                    continue
                relative_path = payload.get("source", {}).get("relative_path")
                if isinstance(relative_path, str):
                    sources.add(relative_path.replace("\\", "/"))
            except (OSError, ValueError, json.JSONDecodeError):
                continue
        return sources
