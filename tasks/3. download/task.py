import os
import sys
from typing import Optional, Tuple, TYPE_CHECKING
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task

# Import from LLM task's llms directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '4. llm'))
from llms import get_backend
from llms.base import LLMBackend

if TYPE_CHECKING:
    from lmstudio import FileHandle
else:
    try:
        from lmstudio import FileHandle
    except ImportError:
        from typing import Any as FileHandle  # Fallback if lmstudio not installed

class DownloadTask(Task[str, Tuple[str, FileHandle]]):
    def __init__(self, maximum: int = 2, backend_name: Optional[str] = None, input_dir: Optional[str] = None) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.backend: LLMBackend = get_backend(backend_name)

    def execute(self, input_path: str) -> Tuple[str, FileHandle]:
        """
        Prepare/download image for LLM processing.
        Returns: (input_path, image_handle)
        """
        try:
            if not self.backend:
                raise Exception("Backend not configured")
            
            # Use backend to prepare image
            image_handle: FileHandle = self.backend.prepare_image(input_path)
            return (input_path, image_handle)
            
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
