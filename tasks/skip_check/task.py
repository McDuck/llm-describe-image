import os
import sys
from typing import Optional, Tuple
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class SkipCheckTask(Task[str, Tuple[bool, str]]):
    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str,
        output_dir_output_suffix: str,
        retry_failed: bool,
        retry: bool
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: str = output_dir
        self.retry_failed: bool = retry_failed
        self.retry: bool = retry
        self.output_dir_output_suffix: str = output_dir_output_suffix

    def execute(self, input_path: str) -> Tuple[bool, str]:
        """
        Check if file should be skipped.
        
        If skip_all=True: Never skip (process everything)
        If check_input_exists=True: Skip if INPUT description doesn't exist (for enhance pipeline)
        If check_input_exists=False: Skip if OUTPUT already exists (for describe pipeline)
        
        Returns: (should_skip, input_path)
        - True if file should be skipped
        - False if file needs processing
        """
        
        try:
            # Calculate file paths
            relative_path = os.path.relpath(input_path, self.input_dir)
            output_output_path = os.path.join(self.output_dir, relative_path + self.output_dir_output_suffix)
            output_output_error_path = os.path.join(self.output_dir, relative_path + ".error" + self.output_dir_output_suffix)
            
            # Skip if already processed
            if not self.retry and os.path.exists(output_output_path):
                return (True, input_path)  # Skip - already processed
            
            # Check if error file exists and retry_failed is False
            if not self.retry_failed and os.path.exists(output_output_error_path):
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
