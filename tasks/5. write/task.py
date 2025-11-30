import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class WriteTask(Task):
    def __init__(self, maximum=1, input_dir=None, output_dir=None):
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir = output_dir

    def execute(self, item):
        """
        Write LLM output to file.
        Args: (input_path, content)
        Returns: output_path
        """
        input_path, content = item
        
        try:
            # Calculate output path
            if self.input_dir and self.output_dir:
                relative = os.path.relpath(input_path, self.input_dir)
                output_file = os.path.join(self.output_dir, relative + ".txt")
            else:
                output_file = input_path + ".txt"
            
            # Create directory if needed
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Write content
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(content)
            
            return output_file
            
        except Exception as e:
            # Show relative path in error
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            try:
                print(f"Error writing {rel_path}: {e}")
            except:
                pass  # Ignore print errors during shutdown
            raise
