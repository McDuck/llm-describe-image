"""Shortcut pipeline for creating file links to original images."""

import os
from pipelines.pipeline import Pipeline
from config_loader import (
    DEFAULT_NUM_DISCOVER_THREADS,
    DEFAULT_SORT_ORDER,
    DEFAULT_IMAGE_EXTENSIONS,
)


class ShortcutPipeline(Pipeline):
    """Pipeline for creating Windows shortcuts linking to original images."""
    
    def __init__(self) -> None:
        """Initialize the shortcut pipeline."""
        super().__init__(name="shortcut", description="Create shortcuts linking to original images")
        
        self.num_discover_threads: int = DEFAULT_NUM_DISCOVER_THREADS
        self.num_skip_checker_threads: int = 100
        self.num_shortcut_threads: int = 10
        
        # Skip and retry configuration
        self.retry_failed: bool = False
        self.retry: bool = False
    
    PIPELINE_CONFIG = [
        {
            "name": "Discover",
            "class_name": "DiscoverTask",
            "dir": "discover",
            "kwargs_builder": lambda self: {
                "maximum": self.num_discover_threads,
                "input_dir": self.input_dir,
                "image_extensions": DEFAULT_IMAGE_EXTENSIONS | {".lnk"},
                "sort_order": os.getenv("SORT_ORDER", DEFAULT_SORT_ORDER)
            },
            "task": "Discover",
            "num_threads": 1,
            "next_task": "SkipCheck",
            "has_pending_queue": True
        },
        {
            "name": "SkipCheck",
            "class_name": "SkipCheckTask",
            "dir": "skip_check",
            "kwargs_builder": lambda self: {
                "maximum": self.num_skip_checker_threads,
                "input_dir": self.input_dir,
                "output_dir": self.output_dir,
                "output_dir_output_suffix": ".lnk",
                "retry_failed": self.retry_failed,
                "retry": self.retry
            },
            "task": "SkipCheck",
            "num_threads_getter": "num_skip_checker_threads",
            "next_task": "Shortcut",
            "transform": lambda result: result[1],  # Extract path from (should_skip, path)
            "check_rejection": lambda result: result[0]  # Check should_skip flag
        },
        {
            "name": "Shortcut",
            "class_name": "ShortcutTask",
            "dir": "shortcut",
            "kwargs_builder": lambda self: {
                "maximum": self.num_shortcut_threads,
                "input_dir": self.input_dir,
                "output_dir": self.output_dir
            },
            "task": "Shortcut",
            "num_threads_getter": "num_shortcut_threads",
            "next_task": None
        }
    ]
