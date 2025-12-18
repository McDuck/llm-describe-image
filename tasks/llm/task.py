import os
import sys
from typing import Optional, Tuple, TYPE_CHECKING, Any, Dict
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# Add local llms directory to path
sys.path.insert(0, os.path.dirname(__file__))

from tasks.task import Task
from llms import get_backend
from llms.base import LLMBackend

if TYPE_CHECKING:
    from lmstudio import FileHandle
else:
    try:
        from lmstudio import FileHandle
    except ImportError:
        from typing import Any as FileHandle  # Fallback if lmstudio not installed


class LLMTask(Task[Tuple[str, FileHandle, Dict[str, Any]], Tuple[str, str, Dict[str, Any]]]):
    def __init__(self, maximum: int = 1, model_name: Optional[str] = None, prompt: Optional[str] = None, backend_name: Optional[str] = None, input_dir: Optional[str] = None) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.model_name: Optional[str] = model_name
        self.backend_name: Optional[str] = backend_name
        self.backend: Optional[LLMBackend] = None
        self.model: Any = None
        self.prompt: Optional[str] = prompt

    def load(self) -> None:
        """Load the model and backend. Called by worker thread at start."""
        self.backend = get_backend(self.backend_name)
        if self.backend:
            self.model = self.backend.load_model(self.model_name, allow_cli_install=False)
        
        if not self.model:
            raise Exception(f"Failed to load model: {self.model_name}")
    
    def unload(self) -> None:
        """Unload the model. Called by worker thread at end."""
        if self.backend and hasattr(self.backend, 'cleanup'):
            # Assuming model was loaded by script, not preloaded
            self.backend.cleanup(
                model_loaded_by_script=True,
                model_name=self.model_name,
                server_started_by_script=False
            )
        self.model = None
        self.backend = None

    def execute(self, item: Tuple[str, FileHandle, Dict[str, Any]]) -> Tuple[str, str, Dict[str, Any]]:
        """
        Run LLM inference on image with metadata context.
        Args: (input_path, image_handle, metadata)
        Returns: (input_path, content, metadata)
        """
        input_path, image_handle, metadata = item
        
        try:
            if not self.backend or not self.model:
                raise Exception("Backend or model not configured")
            
            # Prepare format values for prompt template
            dt = metadata.get('datetime')
            datetime_value = ""
            if dt:
                datetime_value = dt.strftime("%Y-%m-%d %H:%M:%S") if dt.hour or dt.minute or dt.second else dt.strftime("%Y-%m-%d")
            
            location_value = metadata.get('location_str', "")
            camera_value = metadata.get('camera', "")
            focal_length_value = metadata.get('focal_length', "")
            aperture_value = metadata.get('aperture', "")
            iso_value = metadata.get('iso', "")
            shutter_speed_value = metadata.get('shutter_speed', "")
            filename_value = metadata.get('filename', "")
            
            # Format prompt with metadata placeholders
            try:
                enhanced_prompt = self.prompt.format(
                    datetime=datetime_value,
                    location=location_value,
                    camera=camera_value,
                    focal_length=focal_length_value,
                    aperture=aperture_value,
                    iso=iso_value,
                    shutter_speed=shutter_speed_value,
                    filename=filename_value
                )
            except (KeyError, ValueError):
                # If template has no placeholders or formatting fails, use as-is
                enhanced_prompt = self.prompt
            
            # Run LLM inference
            content = self.backend.respond(self.model, enhanced_prompt, image_handle)
            return (input_path, content, metadata)
            
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
