from __future__ import annotations
import abc
from typing import Any, Optional

class LLMBackend(abc.ABC):
    """Abstract interface for LLM backends used by the pipeline."""

    @abc.abstractmethod
    def bootstrap_server(self, auto_start: bool) -> bool:
        """Ensure server is running. Optionally auto-start. Return True if running."""
        raise NotImplementedError

    @abc.abstractmethod
    def load_model(self, model_name: str, allow_cli_install: bool) -> Any:
        """Load or attach to a model and return a model handle.
        Return None on failure.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def prepare_image(self, path: str) -> Any:
        """Prepare image handle for inference."""
        raise NotImplementedError

    @abc.abstractmethod
    def respond(self, model: Any, prompt: str, image_handle: Any) -> str:
        """Run inference and return string content."""
        raise NotImplementedError

    @abc.abstractmethod
    def cleanup(self, model_loaded_by_script: bool, model_name: Optional[str], server_started_by_script: bool) -> None:
        """Unload model and stop server if owned by script."""
        raise NotImplementedError
