"""Describe Image Pipeline: Discover → SkipCheck → Download → LLM → Write"""

import os
from typing import Optional

from pipelines.pipeline import Pipeline
from config_loader import (
    DEFAULT_SORT_ORDER,
    DEFAULT_NUM_DISCOVER_THREADS,
    DEFAULT_NUM_SKIP_CHECKER_THREADS,
)

class FixJpegPipeline(Pipeline):
    """Pipeline for fixing corrupted JPEG files."""
    
    PIPELINE_CONFIG = [
        {
            "name": "Discover",
            "class_name": "DiscoverTask",
            "dir": "discover",
            "kwargs_builder": lambda self: {
                "maximum": self.num_discover_threads,
                "input_dir": self.input_dir,
                "image_extensions": {".jpg", ".jpeg"},  # Only JPEG files
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
                "output_suffix_pattern": ".fixed{ext}",
                "retry_failed": self.retry_failed,
                "retry": self.retry
            },
            "task": "SkipCheck",
            "num_threads_getter": "num_skip_checker_threads",
            "next_task": "FixJpeg",
            "transform": lambda result: result[1],  # Extract path from (bool, path)
            "check_rejection": lambda result: result[0],  # Skip if already processed
            "has_pending_queue": False,
        },
        {
            "name": "FixJpeg",
            "class_name": "FixJpegTask",
            "dir": "fix_jpeg",
            "kwargs_builder": lambda self: {
                "maximum": self.num_fix_threads,
                "input_dir": self.input_dir,
                "output_dir": self.output_dir
            },
            "task": "FixJpeg",
            "num_threads_getter": "num_fix_threads",
            "next_task": None
        }
    ]
    
    def __init__(self) -> None:
        """Initialize the fix-jpeg pipeline."""
        super().__init__(
            name="fix-jpeg",
            description="Fix corrupted JPEG files by re-encoding them to output directory"
        )
        
        # Configuration (will be set in run())
        self.input_dir: Optional[str] = None
        self.output_dir: Optional[str] = None
        
        # Thread counts
        self.num_discover_threads: int = DEFAULT_NUM_DISCOVER_THREADS
        self.num_skip_checker_threads: int = DEFAULT_NUM_SKIP_CHECKER_THREADS
        self.num_fix_threads: int = 4  # I/O bound, can use more threads
        
        # Set from CLI arguments
        self.retry: bool = False
        self.retry_failed: bool = False
