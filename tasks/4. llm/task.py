import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# Add local llms directory to path
sys.path.insert(0, os.path.dirname(__file__))

from tasks.task import Task
from llms import get_backend


class LLMTask(Task):
    def __init__(self, maximum=1, model_name=None, prompt=None, backend_name=None, input_dir=None):
        super().__init__(maximum, input_dir=input_dir)
        self.backend = get_backend(backend_name)
        self.model = self.backend.load_model(model_name, allow_cli_install=False) if self.backend else None
        self.prompt = prompt
        
        if not self.model:
            raise Exception(f"Failed to load model: {model_name}")

    def execute(self, item):
        """
        Run LLM inference on image.
        Args: (input_path, image_handle)
        Returns: (input_path, content)
        """
        input_path, image_handle = item
        
        try:
            if not self.backend or not self.model:
                raise Exception("Backend or model not configured")
            
            # Run LLM inference
            content = self.backend.respond(self.model, self.prompt, image_handle)
            return (input_path, content)
            
        except Exception as e:
            # Show relative path in error
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            try:
                print(f"Failed LLM {rel_path}: {e}")
            except:
                pass  # Ignore print errors during shutdown
            raise
