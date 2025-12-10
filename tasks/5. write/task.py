import os
import sys
from typing import Optional, Tuple, Union
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class WriteTask(Task[Tuple[str, Union[str, Exception]], str]):
    def __init__(self, maximum: int = 1, input_dir: Optional[str] = None, output_dir: Optional[str] = None) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: Optional[str] = output_dir

    def execute(self, item: Tuple[str, Union[str, Exception]]) -> str:
        """
        Write LLM output or error to file.
        Args: (input_path, content_or_error)
        Returns: output_path
        """
        input_path, content_or_error = item
        
        # Calculate output paths
        if self.input_dir and self.output_dir:
            relative = os.path.relpath(input_path, self.input_dir)
            output_file = os.path.join(self.output_dir, relative + ".txt")
            error_file = os.path.join(self.output_dir, relative + ".error.txt")
        else:
            output_file = input_path + ".txt"
            error_file = input_path + ".error.txt"
        
        # Check if this is an error or success
        if isinstance(content_or_error, Exception):
            # Write error file
            try:
                os.makedirs(os.path.dirname(error_file), exist_ok=True)
                with open(error_file, "w", encoding="utf-8") as f:
                    f.write(f"Error processing {input_path}\n")
                    f.write(f"Error: {str(content_or_error)}\n")
                return error_file
            except Exception as e:
                # Show relative path in error
                rel_path = input_path
                if self.input_dir and input_path.startswith(self.input_dir):
                    try:
                        rel_path = os.path.relpath(input_path, self.input_dir)
                    except (ValueError, TypeError):
                        pass
                try:
                    print(f"Error writing error file for {rel_path}: {e}")
                except:
                    pass  # Ignore print errors during shutdown
                raise
        else:
            # Write success output
            try:
                # Create directory if needed
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                
                # Write content
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(content_or_error)
                
                # Remove error file if it exists (successful retry)
                if os.path.exists(error_file):
                    try:
                        os.remove(error_file)
                    except Exception:
                        pass  # Ignore errors removing error file
                
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
