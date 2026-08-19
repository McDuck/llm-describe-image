from __future__ import annotations
import abc
from typing import Any, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from lmstudio import FileHandle
else:
    try:
        from lmstudio import FileHandle
    except ImportError:
        FileHandle = Any  # Fallback if lmstudio not installed

# Generic image handle type for different backends
ImageHandle = Union[FileHandle, Any]

class LLMBackend(abc.ABC):
    """Abstract interface for LLM backends used by the pipeline."""

    @abc.abstractmethod
    def bootstrap_server(self, auto_start: bool, sync_api_timeout_s: int = 600) -> bool:
        """Ensure server is running. Optionally auto-start. Return True if running."""
        raise NotImplementedError

    @abc.abstractmethod
    def load_model(self, model_name: str, allow_cli_install: bool, context_size: int = 0) -> Any:
        """Load or attach to a model and return a model handle.
        Args:
            model_name: Name of the model to load
            allow_cli_install: Whether to allow CLI installation
            context_size: Desired context window size (0 = use model default)
        Return None on failure.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def prepare_image(self, path: str) -> ImageHandle:
        """Prepare image handle for inference."""
        raise NotImplementedError

    @abc.abstractmethod
    def respond(self, model: Any, prompt: str, image_handle: Optional[ImageHandle] = None) -> str:
        """Run inference and return string content. Image is optional."""
        raise NotImplementedError

    @abc.abstractmethod
    def cleanup(self, model_loaded_by_script: bool, model_name: Optional[str], server_started_by_script: bool) -> None:
        """Unload model and stop server if owned by script."""
        raise NotImplementedError
