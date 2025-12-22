import os
import sys
from typing import Optional, Tuple, Union
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class GeolocationWriteTask(Task[Tuple[str, Optional[str]], str]):
    """
    Write geolocation data to files.
    
    Input: (image_path, location_string)
    Output: image_path
    
    Success: writes to .geocode.txt
    Failure: writes to .geocode.error.txt
    """
    
    def __init__(
        self,
        maximum: int = 1,
        input_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: Optional[str] = output_dir
    
    def execute(self, item: Tuple[str, Union[str, Exception]]) -> str:
        """
        Write geolocation to file.
        Args: (input_path, location_string_or_error)
        Returns: output_path
        """
        # Handle both success (str) and error (Exception) cases
        if len(item) == 2:
            input_path, content_or_error = item
        else:
            input_path = item
            content_or_error = None
        
        # Calculate output paths
        if self.input_dir and self.output_dir:
            relative = os.path.relpath(input_path, self.input_dir)
            output_file = os.path.join(self.output_dir, relative + ".geocode.txt")
            error_file = os.path.join(self.output_dir, relative + ".geocode.error.txt")
        else:
            output_file = input_path + ".geocode.txt"
            error_file = input_path + ".geocode.error.txt"
        
        # Check if this is an error or success
        if isinstance(content_or_error, Exception):
            # Write error file
            try:
                os.makedirs(os.path.dirname(error_file), exist_ok=True)
                with open(error_file, "w", encoding="utf-8") as f:
                    f.write(f"{str(content_or_error)}\n")
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
                    print(f"Error writing geolocation error file for {rel_path}: {e}")
                except:
                    pass
                raise
        else:
            # Write success output
            if content_or_error is None:
                # No GPS data - just write empty file or skip
                return input_path
            
            try:
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(content_or_error)
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
                    print(f"Error writing geolocation for {rel_path}: {e}")
                except:
                    pass
                raise
