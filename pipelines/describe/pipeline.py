"""Describe-image pipeline implementation."""

import os
import threading
from typing import Dict, List, Tuple, Optional, Any
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pipelines.pipeline import Pipeline, TaskDefinition
from tasks.task import Task


class DescribeImagePipeline(Pipeline):
    """Pipeline for generating image descriptions using LLM vision models."""
    
    def print_configuration(self) -> None:
        """Print pipeline configuration."""
        from config_loader import DEFAULT_PROMPT
        
        # Load prompt and store for task creation
        self.prompt = self.load_prompt(DEFAULT_PROMPT)
        
        # Print common configuration
        self.print_base_configuration(self.model_name, self.prompt_source)
        
        # Print thread counts
        self.print_thread_counts(self.get_task_definitions())
    
    def get_task_definitions(self) -> List[TaskDefinition]:
        """Return task definitions for this pipeline."""
        from llm_describe_directory import (
            NUM_DISCOVER_THREADS, NUM_SKIP_CHECKER_THREADS,
            NUM_DOWNLOAD_THREADS, NUM_LLM_THREADS, NUM_WRITE_THREADS
        )
        from config_loader import DEFAULT_OUTPUT_FORMAT
        
        # Skip check transform/reject functions
        def check_skip_rejection(result: Tuple[bool, str]) -> bool:
            should_skip, path = result
            return should_skip
        
        def skip_transform(result: Tuple[bool, str]) -> str:
            should_skip, path = result
            return path
        
        return [
            TaskDefinition(
                name="Discover",
                task_dir="discover",
                task_class="DiscoverTask",
                num_workers=NUM_DISCOVER_THREADS,
                next_task="SkipCheck",
                has_pending_queue=True,
                kwargs={
                    "input_dir": self.input_dir,
                    "image_extensions": self.image_extensions,
                    "sort_order": self.sort_order
                }
            ),
            TaskDefinition(
                name="SkipCheck",
                task_dir="skip_check",
                task_class="SkipCheckTask",
                num_workers=NUM_SKIP_CHECKER_THREADS,
                next_task="Download",
                transform=skip_transform,
                check_rejection=check_skip_rejection,
                kwargs={
                    "input_dir": self.input_dir,
                    "output_dir": self.output_dir,
                    "retry_failed": self.retry_failed
                }
            ),
            TaskDefinition(
                name="Download",
                task_dir="download",
                task_class="DownloadTask",
                num_workers=NUM_DOWNLOAD_THREADS,
                next_task="LLM",
                kwargs={
                    "backend_name": os.getenv("BACKEND"),
                    "input_dir": self.input_dir
                }
            ),
            TaskDefinition(
                name="LLM",
                task_dir="llm",
                task_class="LLMTask",
                num_workers=NUM_LLM_THREADS,
                next_task="Write",
                kwargs={
                    "model_name": self.model_name,
                    "prompt": self.prompt,
                    "backend_name": os.getenv("BACKEND"),
                    "input_dir": self.input_dir
                }
            ),
            TaskDefinition(
                name="Write",
                task_dir="write",
                task_class="WriteTask",
                num_workers=NUM_WRITE_THREADS,
                kwargs={
                    "input_dir": self.input_dir,
                    "output_dir": self.output_dir,
                    "output_format": DEFAULT_OUTPUT_FORMAT
                }
            )
        ]
    

