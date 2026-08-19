import os
import sys
from typing import Any, Tuple
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from describe_media.tasks.task import Task
from describe_media.tasks.media import output_relative_path, split_media_item


class SkipCheckTask(Task[Any, Tuple[bool, Any]]):
    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str,
        output_suffix_pattern: str,
        retry_failed: bool,
        retry: bool
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: str = output_dir
        self.retry_failed: bool = retry_failed
        self.retry: bool = retry
        self.output_suffix_pattern: str = output_suffix_pattern

    def execute(self, item: Any) -> Tuple[bool, Any]:
        """
        Check if file should be skipped.
        
        If skip_all=True: Never skip (process everything)
        If check_input_exists=True: Skip if INPUT description doesn't exist (for enhance pipeline)
        If check_input_exists=False: Skip if OUTPUT already exists (for describe pipeline)
        
        Returns: (should_skip, input_path)
        - True if file should be skipped
        - False if file needs processing
        """
        
        input_path, metadata = split_media_item(item)
        try:
            # Generate suffix from pattern by substituting {ext} with file extension
            base, ext = os.path.splitext(input_path)
            suffix = self.output_suffix_pattern.format(ext=ext)

            # Calculate file paths
            relative_path = output_relative_path(input_path, self.input_dir, metadata)
            relative_path_base = os.path.splitext(relative_path)[0]
            
            output_output_path = os.path.join(self.output_dir, relative_path_base + suffix)
            output_output_error_path = os.path.join(self.output_dir, relative_path_base + ext + ".error.txt")
            
            # Skip if already processed
            if not self.retry and os.path.exists(output_output_path):
                return (True, item)  # Skip - already processed
            
            # Check if error file exists and retry_failed is False
            if not self.retry_failed and os.path.exists(output_output_error_path):
                return (True, item)  # Skip - previously failed
            
            return (False, item)  # Process
                
        except Exception as e:
            # Show relative path in error
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            try:
                print(f"Error checking {rel_path}: {e}")
            except:
                pass  # Ignore print errors during shutdown
            return (False, item)  # Process on error
