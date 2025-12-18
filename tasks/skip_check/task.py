import os
import sys
from typing import Optional, Tuple
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class SkipCheckTask(Task[str, Tuple[bool, str]]):
    def __init__(
        self,
        maximum: int = 100,
        input_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        retry_failed: bool = False,
        output_suffix: str = ".txt"
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: Optional[str] = output_dir
        self.retry_failed: bool = retry_failed
        self.output_suffix: str = output_suffix

    def execute(self, input_path: str) -> Tuple[bool, str]:
        """
        Check if output file already exists or if file previously failed.
        Returns: (should_skip, input_path)
        - True if file should be skipped (already exists or previously failed)
        - False if file needs processing
        """
        try:
            # Calculate output path
            if self.input_dir and self.output_dir:
                relative = os.path.relpath(input_path, self.input_dir)
                output_file = os.path.join(self.output_dir, relative + self.output_suffix)
                error_file = os.path.join(self.output_dir, relative + self.output_suffix.replace(".txt", ".error.txt"))
            else:
                output_file = input_path + self.output_suffix
                error_file = input_path + self.output_suffix.replace(".txt", ".error.txt")
            
            # Check if output exists
            if os.path.exists(output_file):
                return (True, input_path)  # Skip - already processed
            
            # Check if error file exists and retry_failed is False
            if not self.retry_failed and os.path.exists(error_file):
                return (True, input_path)  # Skip - previously failed
            
            return (False, input_path)  # Process
                
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
            return (False, input_path)  # Process on error
