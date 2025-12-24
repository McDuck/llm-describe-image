"""Describe Image Pipeline: Discover → SkipCheck → Download → LLM → Write"""

import os
from typing import Optional

from pipelines.pipeline import Pipeline
from config_loader import (
    DEFAULT_MODEL_NAME,
    DEFAULT_PROMPT,
    DEFAULT_SORT_ORDER,
    DEFAULT_NUM_DISCOVER_THREADS,
    DEFAULT_NUM_SKIP_CHECKER_THREADS,
    DEFAULT_NUM_DOWNLOAD_THREADS,
    DEFAULT_NUM_LLM_THREADS,
    DEFAULT_NUM_WRITE_THREADS,
    DEFAULT_BACKPRESSURE_MULTIPLIER,
    DEFAULT_RETRY_FAILED,
    DEFAULT_OUTPUT_FORMAT,
    DEFAULT_IMAGE_EXTENSIONS,
)


class DescribePipeline(Pipeline):
    """Pipeline for describing images using LLM."""
    
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
            "next_task": "SkipCheck",
            "transform": None,
            "check_rejection": None,
            "has_pending_queue": True,
        },
        {
            "name": "SkipCheck",
            "class_name": "SkipCheckTask",
            "dir": "skip_check",
            "kwargs_builder": lambda self: {
                "maximum": self.num_skip_checker_threads,
                "input_dir": self.input_dir,
                "output_dir": self.output_dir,
                "output_suffix_pattern": ".txt",
                "retry_failed": self.retry_failed,
                "retry": self.retry
            },
            "task": "SkipCheck",
            "num_threads_getter": "num_skip_checker_threads",
            "next_task": "Download",
            "transform": lambda result: result[1],  # Extract path from (bool, path)
            "check_rejection": lambda result: result[0],  # True if should skip
            "has_pending_queue": False,
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
            "next_task": "LLM",
            "transform": None,
            "check_rejection": None,
            "has_pending_queue": False,
        },
        {
            "name": "LLM",
            "class_name": "LLMTask",
            "dir": "llm",
            "kwargs_builder": lambda self: {
                "maximum": self.num_llm_threads,
                "model_name": os.getenv("MODEL_NAME", DEFAULT_MODEL_NAME),
                "prompt": os.getenv("PROMPT", DEFAULT_PROMPT),
                "backend_name": os.getenv("BACKEND"),
                "input_dir": self.input_dir,
            },
            "task": "LLM",
            "num_threads_getter": "num_llm_threads",
            "next_task": "Write",
            "transform": None,
            "check_rejection": None,
            "has_pending_queue": False,
        },
        {
            "name": "Write",
            "class_name": "WriteTask",
            "dir": "write",
            "kwargs_builder": lambda self: {
                "maximum": self.num_write_threads,
                "input_dir": self.input_dir,
                "output_dir": self.output_dir,
                "output_format": os.getenv("OUTPUT_FORMAT", DEFAULT_OUTPUT_FORMAT),
            },
            "task": "Write",
            "num_threads_getter": "num_write_threads",
            "next_task": None,
            "transform": None,
            "check_rejection": None,
            "has_pending_queue": False,
        },
    ]
    
    def __init__(self) -> None:
        super().__init__(
            name="describe",
            description="Describes images using LLM (Discover → SkipCheck → Download → LLM → Write)"
        )
        
        # Configuration (will be set in run())
        self.input_dir: Optional[str] = None
        self.output_dir: Optional[str] = None
        
        # Thread counts
        self.num_discover_threads: int = DEFAULT_NUM_DISCOVER_THREADS
        self.num_skip_checker_threads: int = DEFAULT_NUM_SKIP_CHECKER_THREADS
        self.num_download_threads: int = DEFAULT_NUM_DOWNLOAD_THREADS
        self.num_llm_threads: int = DEFAULT_NUM_LLM_THREADS
        self.num_write_threads: int = DEFAULT_NUM_WRITE_THREADS
        
        # Set backpressure multiplier
        self.backpressure_multiplier = DEFAULT_BACKPRESSURE_MULTIPLIER
        
        # Skip checking (retry all items)
        self.retry: bool = False
        
        # Retry failed items
        self.retry_failed: bool = False
