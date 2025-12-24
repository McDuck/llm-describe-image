"""Context enhancement pipeline implementation."""

import os
from pipelines.pipeline import Pipeline
from config_loader import (
    DEFAULT_NUM_DISCOVER_THREADS,
    DEFAULT_NUM_SKIP_CHECKER_THREADS,
    DEFAULT_NUM_CONTEXT_THREADS,
    DEFAULT_NUM_ENHANCE_THREADS,
    DEFAULT_NUM_WRITE_THREADS,
    DEFAULT_CONTEXT_MODEL_NAME,
    DEFAULT_ENHANCEMENT_PROMPT,
    DEFAULT_ENHANCEMENT_OUTPUT_FORMAT,
    DEFAULT_CONTEXT_TEMPLATE,
    DEFAULT_CONTEXT_ITEM_TEMPLATE,
    DEFAULT_CONTEXT_ITEM_MAX_LENGTH,
    DEFAULT_MAX_CONTEXT_IN_PROMPT,
    DEFAULT_CONTEXT_WINDOW_DAYS,
    DEFAULT_MAX_CONTEXT_ITEMS,
    DEFAULT_MAX_CONTEXT_LENGTH,
    DEFAULT_MODEL_CONTEXT_LENGTH,
    DEFAULT_SORT_ORDER,
)


class EnhanceByContextPipeline(Pipeline):
    """Pipeline for enhancing descriptions using context from nearby images."""
    
    def __init__(self) -> None:
        """Initialize the enhance pipeline."""
        super().__init__(name="enhance", description="Enhances descriptions using context from nearby images")
        
        self.num_discover_threads: int = DEFAULT_NUM_DISCOVER_THREADS
        self.num_skip_checker_threads: int = DEFAULT_NUM_SKIP_CHECKER_THREADS
        self.num_context_threads: int = DEFAULT_NUM_CONTEXT_THREADS
        self.num_enhance_threads: int = DEFAULT_NUM_ENHANCE_THREADS
        self.num_write_threads: int = DEFAULT_NUM_WRITE_THREADS
        
        # Model and prompt configuration
        self.enhance_model: str = DEFAULT_CONTEXT_MODEL_NAME
        self.enhance_prompt: str = DEFAULT_ENHANCEMENT_PROMPT
        self.output_format: str = DEFAULT_ENHANCEMENT_OUTPUT_FORMAT
        self.context_template: str = DEFAULT_CONTEXT_TEMPLATE
        self.context_item_template: str = DEFAULT_CONTEXT_ITEM_TEMPLATE
        self.context_item_max_length: int = DEFAULT_CONTEXT_ITEM_MAX_LENGTH
        self.max_context_in_prompt: int = DEFAULT_MAX_CONTEXT_IN_PROMPT
        self.context_window_days: int = DEFAULT_CONTEXT_WINDOW_DAYS
        self.max_context_items: int = DEFAULT_MAX_CONTEXT_ITEMS
        self.max_context_length: int = DEFAULT_MAX_CONTEXT_LENGTH  # Max chars for context to avoid token limit
        self.model_context_length: int = DEFAULT_MODEL_CONTEXT_LENGTH  # Model context window size
        
        # Debug mode (separate from verbose logging)
        self.debug: bool = False
        
        # Skip checking (retry all items)
        self.retry: bool = False
        
        # Retry failed items
        self.retry_failed: bool = False
    
    PIPELINE_CONFIG = [
        {
            "name": "Discover",
            "class_name": "DiscoverTask",
            "dir": "discover",
            "kwargs_builder": lambda self: {
                "input_dir": self.input_dir,
                "image_extensions": {".jpg", ".jpeg", ".png", ".webp"},
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
                "maximum": 100,
                "input_dir": self.input_dir,
                "output_dir": self.output_dir,
                "output_suffix_pattern": ".enhanced.txt",
                "retry_failed": self.retry_failed,
                "retry": self.retry
            },
            "task": "SkipCheck",
            "num_threads_getter": "num_skip_checker_threads",
            "next_task": "Context",
            "transform": lambda result: result[1],  # Extract path from (should_skip, path)
            "check_rejection": lambda result: result[0]  # Check should_skip flag
        },
        {
            "name": "Context",
            "class_name": "ContextTask",
            "dir": "context",
            "kwargs_builder": lambda self: {
                "input_dir": self.input_dir,
                "output_dir": self.output_dir,
                "context_window_days": getattr(self, 'context_window_days', 10),
                "max_context_items": getattr(self, 'max_context_items', 20)
            },
            "task": "Context",
            "num_threads_getter": "num_context_threads",
            "next_task": "Enhance"
        },
        {
            "name": "Enhance",
            "class_name": "EnhanceTask",
            "dir": "enhance",
            "kwargs_builder": lambda self: {
                "model_name": getattr(self, 'enhance_model', None),
                "prompt": getattr(self, 'enhance_prompt', None),
                "backend_name": os.getenv("BACKEND"),
                "input_dir": self.input_dir,
                "output_dir": self.output_dir,
                "context_template": getattr(self, 'context_template', None),
                "context_item_template": getattr(self, 'context_item_template', None),
                "context_item_max_length": getattr(self, 'context_item_max_length', None),
                "max_context_in_prompt": getattr(self, 'max_context_in_prompt', None),
                "max_context_length": getattr(self, 'max_context_length', 8000),
                "model_context_length": getattr(self, 'model_context_length', 32768),
                "debug": getattr(self, 'debug', False)
            },
            "task": "Enhance",
            "num_threads_getter": "num_enhance_threads",
            "next_task": "Write"
        },
        {
            "name": "Write",
            "class_name": "WriteTask",
            "dir": "write",
            "kwargs_builder": lambda self: {
                "input_dir": self.input_dir,
                "output_dir": self.output_dir,
                "output_format": getattr(self, 'output_format', ""),
                "output_suffix": ".enhanced.txt",
                "error_suffix": ".enhanced.error.txt"
            },
            "task": "Write",
            "num_threads_getter": "num_write_threads",
            "next_task": None
        }
    ]


