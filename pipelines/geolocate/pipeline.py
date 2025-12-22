"""Geolocation Pipeline: Discover → Geolocate → Write"""

import os
from pipelines.pipeline import Pipeline
from config_loader import (
    DEFAULT_NUM_DISCOVER_THREADS,
    DEFAULT_SORT_ORDER,
    DEFAULT_IMAGE_EXTENSIONS,
)


class GeolocationPipeline(Pipeline):
    """Pipeline for reverse geocoding GPS coordinates to human-readable locations."""
    
    def __init__(self) -> None:
        """Initialize the geolocation pipeline."""
        super().__init__(name="geolocate", description="Reverse geocode GPS coordinates to human-readable locations")
        
        self.num_discover_threads: int = DEFAULT_NUM_DISCOVER_THREADS
        self.num_skip_checker_threads: int = 100  # High parallelism for file checking
        self.num_geolocate_threads: int = 2  # Network-bound, keep low
        self.num_write_threads: int = 1
        
        # Skip and retry configuration
        self.skip_all: bool = False
        self.retry_failed: bool = False
    
    PIPELINE_CONFIG = [
        {
            "name": "Discover",
            "class_name": "DiscoverTask",
            "dir": "discover",
            "kwargs_builder": lambda self: {
                "input_dir": self.input_dir,
                "image_extensions": DEFAULT_IMAGE_EXTENSIONS,
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
                "input_dir": self.input_dir,
                "output_dir": self.output_dir,
                "skip_all": self.skip_all,
                "retry_failed": self.retry_failed,
                "output_suffix": ".geocode.txt",
                "check_input_exists": False
            },
            "task": "SkipCheck",
            "num_threads_getter": "num_skip_checker_threads",
            "next_task": "Geolocate",
            "transform": lambda result: result[1],  # Extract path from (should_skip, path)
            "check_rejection": lambda result: result[0]  # Check should_skip flag
        },
        {
            "name": "Geolocate",
            "class_name": "GeolocationTask",
            "dir": "geolocate",
            "kwargs_builder": lambda self: {
                "input_dir": self.input_dir,
                "output_dir": self.output_dir,
                "initial_wait_seconds": 1,
                "max_retries": 5
            },
            "task": "Geolocate",
            "num_threads_getter": "num_geolocate_threads",
            "next_task": "Write"
        },
        {
            "name": "Write",
            "class_name": "GeolocationWriteTask",
            "dir": "geolocate",
            "kwargs_builder": lambda self: {
                "input_dir": self.input_dir,
                "output_dir": self.output_dir,
            },
            "task": "Write",
            "num_threads_getter": "num_write_threads",
            "next_task": None
        }
    ]
