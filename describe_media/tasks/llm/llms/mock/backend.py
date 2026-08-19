from __future__ import annotations

import os
from typing import Any, Optional

from llms.base import LLMBackend


class MockBackend(LLMBackend):
    """Deterministic backend for end-to-end tests and local dry runs."""

    def bootstrap_server(self, auto_start: bool, sync_api_timeout_s: int = 600) -> bool:
        return False

    def load_model(self, model_name: str, allow_cli_install: bool, context_size: int = 0) -> Any:
        if os.getenv("MOCK_LLM_LOAD_ERROR"):
            return None
        return {
            "model_name": model_name,
            "context_size": context_size,
        }

    def prepare_image(self, path: str) -> str:
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        return path

    def respond(self, model: Any, prompt: str, image_handle: Optional[str] = None) -> str:
        error_message = os.getenv("MOCK_LLM_RESPONSE_ERROR")
        if error_message:
            raise RuntimeError(error_message)

        template = os.getenv("MOCK_LLM_RESPONSE_TEMPLATE", "mock description")
        image_path = image_handle or ""
        image_name = os.path.basename(image_path) if image_path else ""
        return template.format(
            model_name=model.get("model_name", "") if isinstance(model, dict) else "",
            prompt=prompt,
            image_path=image_path,
            image_name=image_name,
        )

    def cleanup(self, model_loaded_by_script: bool, model_name: Optional[str], server_started_by_script: bool) -> None:
        return None
