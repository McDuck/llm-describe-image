"""Integrated media description graph with images and extracted video frames."""

import os
from typing import Any, Optional

from describe_media.config_loader import (
    DEFAULT_CONTEXT_MODEL_NAME,
    DEFAULT_CONTEXT_WINDOW_DAYS,
    DEFAULT_ENHANCEMENT_PROMPT,
    DEFAULT_IMAGE_EXTENSIONS,
    DEFAULT_LMSTUDIO_SYNC_API_TIMEOUT_S,
    DEFAULT_MODEL_NAME,
    DEFAULT_NUM_DISCOVER_THREADS,
    DEFAULT_NUM_DOWNLOAD_THREADS,
    DEFAULT_NUM_ENHANCE_THREADS,
    DEFAULT_NUM_LLM_THREADS,
    DEFAULT_NUM_RESIZE_THREADS,
    DEFAULT_NUM_SKIP_CHECKER_THREADS,
    DEFAULT_NUM_WRITE_THREADS,
    DEFAULT_OUTPUT_FORMAT,
    DEFAULT_PROMPT,
    DEFAULT_RECOGNITION_DETECTION_THRESHOLD,
    DEFAULT_RECOGNITION_COPY_MATCHES_TO_REVIEW_CLUSTERS,
    DEFAULT_RECOGNITION_ENABLED,
    DEFAULT_RECOGNITION_MODEL,
    DEFAULT_RECOGNITION_THREADS,
    DEFAULT_REVERSE_GEOCODE_GPS,
    DEFAULT_SORT_ORDER,
    DEFAULT_VIDEO_EXTENSIONS,
    DEFAULT_VIDEO_FRAME_INTERVAL_SECONDS,
    DEFAULT_VIDEO_MAX_FRAMES,
    DEFAULT_MAX_CONTEXT_ITEMS,
)
from describe_media.pipelines.pipeline import Pipeline


def route_discovered_media(path: str) -> str | list[str]:
    """Send each source video to both extraction and shortcut creation."""
    if os.path.splitext(path)[1].lower() in DEFAULT_IMAGE_EXTENSIONS:
        return "ImageRouter"
    return ["ExtractVideo", "Shortcut"]


def route_image_targets(item: Any) -> list[str]:
    if not isinstance(item, tuple) or len(item) != 2 or not isinstance(item[1], dict):
        return []
    targets = ["Resize", "Metadata", "Geolocate"]
    if item[1].get("_source_image"):
        targets.append("Shortcut")
    return targets


def route_resize_targets(item: Any) -> list[str]:
    """Keep resized artifacts connected to their original media."""
    targets = ["Recognition", "LLM"]
    if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], dict):
        if item[1].get("_shortcut_output_relative_path"):
            targets.append("Shortcut")
    return targets


def route_llm_result(result: Any) -> Optional[str]:
    if isinstance(result, dict):
        return result.get("route")
    return None


def unwrap_result(result: Any) -> Any:
    return result.get("item") if isinstance(result, dict) else None


class DescribePipeline(Pipeline):
    """Discover â†’ route images/frames â†’ enrich â†’ LLM â†’ enhance."""

    PIPELINE_CONFIG = [
        {
            "name": "Discover", "class_name": "DiscoverTask", "dir": "discover",
            "kwargs_builder": lambda self: {"maximum": self.num_discover_threads, "input_dir": self.input_dir, "image_extensions": DEFAULT_IMAGE_EXTENSIONS, "video_extensions": DEFAULT_VIDEO_EXTENSIONS, "sort_order": os.getenv("SORT_ORDER", DEFAULT_SORT_ORDER)},
            "task": "Discover", "num_threads": 1, "next_task": None, "route": route_discovered_media, "route_targets": ["ImageRouter", "ExtractVideo", "Shortcut"], "has_pending_queue": True, "priority": 5,
        },
        {
            "name": "ExtractVideo", "class_name": "ExtractVideoTask", "dir": "extract_video",
            "kwargs_builder": lambda self: {"maximum": self.num_download_threads, "input_dir": self.input_dir, "output_dir": self.output_dir, "frame_interval_seconds": float(os.getenv("VIDEO_FRAME_INTERVAL_SECONDS", DEFAULT_VIDEO_FRAME_INTERVAL_SECONDS)), "max_frames": int(os.getenv("VIDEO_MAX_FRAMES", DEFAULT_VIDEO_MAX_FRAMES)), "retry": self.retry, "retry_failed": self.retry_failed},
            "task": "ExtractVideo", "num_threads_getter": "num_download_threads", "next_task": "ImageRouter", "error_task": "VideoError", "priority": 4,
        },
        {
            "name": "ImageRouter", "class_name": "ImageRouterTask", "dir": "image_router",
            "kwargs_builder": lambda self: {"maximum": self.num_download_threads, "input_dir": self.input_dir},
            "task": "ImageRouter", "num_threads_getter": "num_download_threads", "next_task": None, "route": route_image_targets, "route_targets": ["Resize", "Metadata", "Geolocate", "Shortcut"], "priority": 3,
        },
        {
            "name": "Resize", "class_name": "ResizeTask", "dir": "resize",
            "kwargs_builder": lambda self: {"maximum": self.num_resize_threads, "input_dir": self.input_dir, "output_dir": self.output_dir, "retry": self.retry, "retry_failed": self.retry_failed},
            "task": "Resize", "num_threads_getter": "num_resize_threads", "next_task": None, "route": route_resize_targets, "route_targets": ["Recognition", "LLM", "Shortcut"], "priority": 2,
        },
        {
            "name": "Metadata", "class_name": "MetadataTask", "dir": "download",
            "kwargs_builder": lambda self: {"maximum": self.num_download_threads, "input_dir": self.input_dir, "output_dir": self.output_dir, "retry": self.retry, "retry_failed": self.retry_failed},
            "task": "Metadata", "num_threads_getter": "num_download_threads", "next_task": "LLM", "priority": 2,
        },
        {
            "name": "Geolocate", "class_name": "GeolocateEnrichedTask", "dir": "geolocate_enriched",
            "kwargs_builder": lambda self: {"maximum": 1, "input_dir": self.input_dir, "output_dir": self.output_dir, "retry": self.retry, "retry_failed": self.retry_failed, "enabled": os.getenv("REVERSE_GEOCODE_GPS", str(DEFAULT_REVERSE_GEOCODE_GPS)).lower() in {"1", "true", "yes", "on"}, "timeout_seconds": int(os.getenv("GEOLOCATE_TIMEOUT_S", "30"))},
            "task": "Geolocate", "num_threads": 1, "next_task": "LLM", "priority": 2,
        },
        {
            "name": "Recognition", "class_name": "RecognitionTask", "dir": "recognition",
            "kwargs_builder": lambda self: {"maximum": self.num_recognition_threads, "input_dir": self.input_dir, "output_dir": self.output_dir, "model_name": os.getenv("RECOGNITION_MODEL", DEFAULT_RECOGNITION_MODEL), "enabled": os.getenv("RECOGNITION_ENABLED", str(DEFAULT_RECOGNITION_ENABLED)).lower() in {"1", "true", "yes", "on"}, "detection_threshold": float(os.getenv("RECOGNITION_DETECTION_THRESHOLD", DEFAULT_RECOGNITION_DETECTION_THRESHOLD)), "copy_matches_to_review_clusters": os.getenv("RECOGNITION_COPY_MATCHES_TO_REVIEW_CLUSTERS", str(DEFAULT_RECOGNITION_COPY_MATCHES_TO_REVIEW_CLUSTERS)).lower() in {"1", "true", "yes", "on"}, "retry": self.retry, "retry_failed": self.retry_failed, "remote_api_base": os.getenv("RECOGNITION_API_BASE"), "remote_api_token": os.getenv("RECOGNITION_API_TOKEN"), "remote_timeout_seconds": float(os.getenv("RECOGNITION_API_TIMEOUT_S", "120"))},
            "task": "Recognition", "num_threads_getter": "num_recognition_threads", "next_task": "LLM", "priority": 2,
        },
        {
            "name": "LLM", "class_name": "LLMTask", "dir": "llm",
            "kwargs_builder": lambda self: {"maximum": self.num_llm_threads, "model_name": os.getenv("MODEL_NAME", DEFAULT_MODEL_NAME), "prompt": os.getenv("PROMPT", DEFAULT_PROMPT), "backend_name": os.getenv("BACKEND"), "input_dir": self.input_dir, "output_dir": self.output_dir, "output_format": os.getenv("OUTPUT_FORMAT", DEFAULT_OUTPUT_FORMAT), "retry": self.retry, "retry_failed": self.retry_failed, "sync_api_timeout_s": int(os.getenv("LMSTUDIO_SYNC_API_TIMEOUT_S", DEFAULT_LMSTUDIO_SYNC_API_TIMEOUT_S))},
            "task": "LLM", "num_threads_getter": "num_llm_threads", "next_task": None, "route": route_llm_result, "route_targets": ["Enhance", "FixJPEG"], "transform": unwrap_result, "priority": 1,
        },
        {
            "name": "Enhance", "class_name": "IntegratedEnhanceTask", "dir": "enhance_integrated",
            "kwargs_builder": lambda self: {"maximum": self.num_enhance_threads, "input_dir": self.input_dir, "output_dir": self.output_dir, "model_name": os.getenv("CONTEXT_MODEL_NAME", DEFAULT_CONTEXT_MODEL_NAME), "prompt": os.getenv("ENHANCEMENT_PROMPT", DEFAULT_ENHANCEMENT_PROMPT), "backend_name": os.getenv("BACKEND"), "context_window_days": DEFAULT_CONTEXT_WINDOW_DAYS, "max_context_items": DEFAULT_MAX_CONTEXT_ITEMS, "retry": self.retry, "retry_failed": self.retry_failed},
            "task": "Enhance", "num_threads_getter": "num_enhance_threads", "next_task": None, "priority": 0,
        },
        {
            "name": "Shortcut", "class_name": "ShortcutTask", "dir": "shortcut",
            "kwargs_builder": lambda self: {"maximum": 10, "input_dir": self.input_dir, "output_dir": self.output_dir},
            "task": "Shortcut", "num_threads": 10, "next_task": None, "priority": 0,
        },
        {
            "name": "FixJPEG", "class_name": "FixJpegTask", "dir": "fix_jpeg",
            "kwargs_builder": lambda self: {"maximum": 1, "input_dir": self.input_dir, "output_dir": self.output_dir},
            "task": "FixJPEG", "num_threads": 1, "next_task": "ImageRouter", "priority": 2,
        },
        {
            "name": "VideoError", "class_name": "VideoErrorTask", "dir": "video_error",
            "kwargs_builder": lambda self: {"maximum": self.num_write_threads, "input_dir": self.input_dir, "output_dir": self.output_dir},
            "task": "VideoError", "num_threads_getter": "num_write_threads", "next_task": None, "priority": 0,
        },
    ]

    def __init__(self) -> None:
        super().__init__(name="describe", description="Integrated image/video description graph")
        self.input_dir: Optional[str] = None
        self.output_dir: Optional[str] = None
        self.num_discover_threads = DEFAULT_NUM_DISCOVER_THREADS
        self.num_download_threads = DEFAULT_NUM_DOWNLOAD_THREADS
        self.num_resize_threads = DEFAULT_NUM_RESIZE_THREADS
        self.num_recognition_threads = DEFAULT_RECOGNITION_THREADS
        self.num_llm_threads = DEFAULT_NUM_LLM_THREADS
        self.num_enhance_threads = DEFAULT_NUM_ENHANCE_THREADS
        self.num_write_threads = DEFAULT_NUM_WRITE_THREADS
        self.num_skip_checker_threads = DEFAULT_NUM_SKIP_CHECKER_THREADS
        self.retry = False
        self.retry_failed = False
