from __future__ import annotations

import base64
import json
import mimetypes
import os
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from describe_media.config_loader import _config
from llms.base import ImageHandle, LLMBackend


class OpenAIBackend(LLMBackend):
    """Call the OpenAI Chat Completions API."""

    def __init__(self) -> None:
        config = _config.get("openai", {})
        self.api_base = os.getenv(
            "OPENAI_API_BASE", config.get("api_base", "https://api.openai.com/v1")
        ).rstrip("/")
        self.timeout_s = int(os.getenv("OPENAI_TIMEOUT_S", config.get("timeout_s", 600)))
        self.api_key = os.getenv("OPENAI_API_KEY", "")

    def _request(self, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.api_base}/{path.lstrip('/')}"
        headers = {"Accept": "application/json"}
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        request = Request(url, data=data, headers=headers, method="POST" if data else "GET")
        try:
            with urlopen(request, timeout=self.timeout_s) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"OpenAI API returned HTTP {error.code}: {detail}") from error
        except URLError as error:
            raise RuntimeError(f"Cannot reach OpenAI API at {self.api_base}: {error.reason}") from error

    def bootstrap_server(self, auto_start: bool, sync_api_timeout_s: int = 600) -> bool:
        """Confirm the OpenAI API is reachable."""
        try:
            self._request("models")
            print(f"OpenAI API endpoint is reachable at {self.api_base}.")
        except RuntimeError as error:
            print(error)
        return False

    def load_model(self, model_name: str, allow_cli_install: bool, context_size: int = 0) -> str:
        """OpenAI selects the model for each request."""
        if not model_name:
            raise ValueError("A model name is required for the OpenAI backend")
        return model_name

    def prepare_image(self, path: str) -> ImageHandle:
        image_path = Path(path)
        if not image_path.is_file():
            raise FileNotFoundError(path)
        mime_type = mimetypes.guess_type(image_path.name)[0] or "application/octet-stream"
        encoded_image = base64.b64encode(image_path.read_bytes()).decode("ascii")
        return f"data:{mime_type};base64,{encoded_image}"

    def respond(self, model: Any, prompt: str, image_handle: Optional[ImageHandle] = None) -> str:
        content = [{"type": "text", "text": prompt}]
        if image_handle is not None:
            content.append({"type": "image_url", "image_url": {"url": image_handle}})

        response = self._request(
            "chat/completions",
            {
                "model": str(model),
                "messages": [{"role": "user", "content": content}],
            },
        )
        try:
            message_content = response["choices"][0]["message"]["content"]
        except (IndexError, KeyError, TypeError) as error:
            raise RuntimeError(f"Unexpected OpenAI chat completion response: {response}") from error
        if not isinstance(message_content, str):
            raise RuntimeError(f"Unexpected OpenAI message content: {message_content!r}")
        return message_content

    def cleanup(self, model_loaded_by_script: bool, model_name: Optional[str], server_started_by_script: bool) -> None:
        """The OpenAI API does not require local model or server cleanup."""
        return None
