"""Context enhancement pipeline implementation."""

import os
import threading
from typing import Dict, List, Tuple, Optional, Any
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pipelines.pipeline import Pipeline, TaskDefinition
from tasks.task import Task


class EnhanceByContextPipeline(Pipeline):
    """Pipeline for enhancing descriptions using context from nearby images."""
    
    def print_configuration(self) -> None:
        """Print pipeline configuration."""
        from config_loader import DEFAULT_ENHANCEMENT_PROMPT, DEFAULT_CONTEXT_WINDOW_DAYS, DEFAULT_CONTEXT_MODEL_NAME, DEFAULT_MAX_CONTEXT_ITEMS
        
        # Get context enhancement configuration
        self.context_window_days: int = self.args.context_window_days or int(os.getenv("CONTEXT_WINDOW_DAYS", str(DEFAULT_CONTEXT_WINDOW_DAYS)))
        self.context_model: str = self.args.context_model or os.getenv("CONTEXT_MODEL", DEFAULT_CONTEXT_MODEL_NAME)
        self.max_context_items: int = int(os.getenv("MAX_CONTEXT_ITEMS", str(DEFAULT_MAX_CONTEXT_ITEMS)))
        
        # Load enhancement prompt and store for task creation
        self.prompt = self.load_prompt(DEFAULT_ENHANCEMENT_PROMPT, "ENHANCEMENT_PROMPT")
        
        # Print common configuration
        self.print_base_configuration(self.context_model, self.prompt_source)
        
        # Print pipeline-specific configuration
        print(f"Context window: ±{self.context_window_days} days, max {self.max_context_items} items")
        
        # Print thread counts
        self.print_thread_counts(self.get_task_definitions())
    
    def get_task_definitions(self) -> List[TaskDefinition]:
        """Return task definitions for this pipeline."""
        from llm_describe_directory import (
            NUM_DISCOVER_THREADS, NUM_SKIP_CHECKER_THREADS,
            NUM_LLM_THREADS, NUM_WRITE_THREADS
        )
        from config_loader import DEFAULT_ENHANCEMENT_OUTPUT_FORMAT
        
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
                num_workers=1,
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
                next_task="Context",
                transform=skip_transform,
                check_rejection=check_skip_rejection,
                kwargs={
                    "input_dir": self.input_dir,
                    "output_dir": self.output_dir,
                    "retry_failed": self.retry_failed,
                    "output_suffix": ".context-enhanced.txt"
                }
            ),
            TaskDefinition(
                name="Context",
                task_dir="context",
                task_class="ContextTask",
                num_workers=NUM_SKIP_CHECKER_THREADS,
                next_task="Enhance",
                kwargs={
                    "input_dir": self.input_dir,
                    "output_dir": self.output_dir,
                    "context_window_days": self.context_window_days,
                    "max_context_items": self.max_context_items
                }
            ),
            TaskDefinition(
                name="Enhance",
                task_dir="enhance",
                task_class="EnhanceTask",
                num_workers=NUM_LLM_THREADS,
                next_task="Write",
                kwargs={
                    "model_name": self.context_model,
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
                    "output_format": DEFAULT_ENHANCEMENT_OUTPUT_FORMAT,
                    "output_suffix": ".context-enhanced.txt",
                    "error_suffix": ".context-enhanced.error.txt"
                }
            )
        ]
    

