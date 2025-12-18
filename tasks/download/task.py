import os
import sys
from typing import Optional, Tuple, TYPE_CHECKING, Dict, Any
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task
from tasks.metadata_extractor import get_image_metadata

# Import from LLM task's llms directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'llm'))
from llms import get_backend
from llms.base import LLMBackend

if TYPE_CHECKING:
    from lmstudio import FileHandle
else:
    try:
        from lmstudio import FileHandle
    except ImportError:
        from typing import Any as FileHandle  # Fallback if lmstudio not installed

class DownloadTask(Task[str, Tuple[str, FileHandle, Dict[str, Any]]]):
    def __init__(self, maximum: int = 2, backend_name: Optional[str] = None, input_dir: Optional[str] = None) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.backend: LLMBackend = get_backend(backend_name)

    def execute(self, input_path: str) -> Tuple[str, FileHandle, Dict[str, Any]]:
        """
        Prepare/download image for LLM processing and extract metadata.
        Returns: (input_path, image_handle, metadata)
        """
        try:
            if not self.backend:
                raise Exception("Backend not configured")
            
            # Extract metadata from image
            metadata: Dict[str, Any] = get_image_metadata(input_path)
            
            # Use backend to prepare image
            image_handle: FileHandle = self.backend.prepare_image(input_path)
            return (input_path, image_handle, metadata)
            
        except Exception as e:
            # Show relative path in error
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            try:
                print(f"Error preparing {rel_path}: {e}")
            except:
                pass  # Ignore print errors during shutdown
            raise
