import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class SkipCheckTask(Task):
    def __init__(self, maximum=100, input_dir=None, output_dir=None):
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir = output_dir

    def execute(self, input_path):
        """
        Check if output file already exists.
        Returns: (should_skip, input_path)
        - True if file should be skipped (already exists)
        - False if file needs processing
        """
        try:
            # Calculate output path
            if self.input_dir and self.output_dir:
                relative = os.path.relpath(input_path, self.input_dir)
                output_file = os.path.join(self.output_dir, relative + ".txt")
            else:
                output_file = input_path + ".txt"
            
            # Check if output exists
            if os.path.exists(output_file):
                return (True, input_path)  # Skip
            else:
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
