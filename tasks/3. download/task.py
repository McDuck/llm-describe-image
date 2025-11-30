import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task

# Import from LLM task's llms directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '4. llm'))
from llms import get_backend

class DownloadTask(Task):
    def __init__(self, maximum=2, backend_name=None, input_dir=None):
        super().__init__(maximum, input_dir=input_dir)
        self.backend = get_backend(backend_name)

    def execute(self, input_path):
        """
        Prepare/download image for LLM processing.
        Returns: (input_path, image_handle)
        """
        try:
            if not self.backend:
                raise Exception("Backend not configured")
            
            # Use backend to prepare image
            image_handle = self.backend.prepare_image(input_path)
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
