import os
import sys
from typing import Optional, Tuple, Dict, Any
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task
from tasks.metadata_extractor import get_image_metadata


class DownloadTask(Task[str, Tuple[str, Dict[str, Any]]]):
    def __init__(self, maximum: int = 2, input_dir: Optional[str] = None) -> None:
        super().__init__(maximum, input_dir=input_dir)

    def execute(self, input_path: str) -> Tuple[str, Dict[str, Any]]:
        """
        Extract metadata from image (no longer downloads/prepares with backend).
        Returns: (input_path, metadata)
        Raises: Exception on error (caught by worker_thread and passed to WriteTask)
        """
        try:
            # Extract metadata from image only
            metadata: Dict[str, Any] = get_image_metadata(input_path)
            return (input_path, metadata)
            
        except Exception as e:
            # Show relative path in error
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            try:
                print(f"Error extracting metadata {rel_path}: {e}")
            except:
                pass  # Ignore print errors during shutdown
            raise
