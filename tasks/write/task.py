import os
import sys
from typing import Optional, Tuple, Union, Dict, Any
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class WriteTask(Task[Tuple[str, Union[str, Exception], Optional[Dict[str, Any]]], str]):
    def __init__(
        self,
        maximum: int = 1,
        input_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        output_format: Optional[str] = None,
        output_suffix: str = ".txt",
        error_suffix: str = ".error.txt"
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: Optional[str] = output_dir
        self.output_format: str = output_format or "Date time: {datetime}\nLocation: {location}\nDescription:\n{content}"
        self.output_suffix: str = output_suffix
        self.error_suffix: str = error_suffix

    def execute(self, item: Tuple[str, Union[str, Exception], Optional[Dict[str, Any]]]) -> str:
        """
        Write LLM output or error to file with metadata.
        Args: (input_path, content_or_error, metadata)
        Returns: output_path
        """
        # Handle both 2-tuple (error case) and 3-tuple (success case) formats
        if len(item) == 2:
            input_path, content_or_error = item
            metadata = None
        else:
            input_path, content_or_error, metadata = item
        
        # Calculate output paths
        if self.input_dir and self.output_dir:
            relative = os.path.relpath(input_path, self.input_dir)
            output_file = os.path.join(self.output_dir, relative + self.output_suffix)
            error_file = os.path.join(self.output_dir, relative + self.error_suffix)
        else:
            output_file = input_path + self.output_suffix
            error_file = input_path + self.error_suffix
        
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
                    print(f"Error writing error file for {rel_path}: {e}")
                except:
                    pass  # Ignore print errors during shutdown
                raise
        else:
            # Write success output with metadata
            try:
                # Create directory if needed
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                
                # Prepare format values
                datetime_value = "Unknown"
                if metadata and metadata.get('datetime'):
                    dt = metadata['datetime']
                    datetime_value = dt.strftime("%Y-%m-%d %H:%M:%S") if dt.hour or dt.minute or dt.second else dt.strftime("%Y-%m-%d")
                
                location_value = "Unknown"
                if metadata and metadata.get('location_str'):
                    location_value = metadata['location_str']
                
                filename_value = ""
                if metadata and metadata.get('filename'):
                    filename_value = metadata['filename']
                
                # Format content using template
                # Try to format with all available placeholders, falling back gracefully
                try:
                    formatted_content = self.output_format.format(
                        datetime=datetime_value,
                        location=location_value,
                        content=content_or_error,
                        filename=filename_value
                    )
                except KeyError:
                    # If template doesn't have all placeholders, try with just content
                    try:
                        formatted_content = self.output_format.format(content=content_or_error)
                    except KeyError:
                        # If that fails too, just use the content as-is
                        formatted_content = content_or_error
                
                # Write formatted content
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(formatted_content)
                
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
