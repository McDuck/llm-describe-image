from __future__ import annotations
import os
import sys
from typing import Optional

# Import default backend configuration
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from describe_media.config_loader import DEFAULT_BACKEND

from llms.base import LLMBackend
from llms.openai.backend import OpenAIBackend

def get_backend(name: Optional[str] = None) -> LLMBackend:
    """Factory for LLM backends."""
    backend = (name or os.getenv("BACKEND", DEFAULT_BACKEND)).strip().lower()
    if backend in ("openai", "open-ai"):
        return OpenAIBackend()
    if backend in ("lmstudio", "lm-studio", "lms"):
        from llms.lmstudio.backend import LMStudioBackend
        return LMStudioBackend()
    elif backend in ("mock", "test", "fake"):
        from llms.mock.backend import MockBackend
        return MockBackend()
    elif backend in ("huggingface", "hf", "transformers"):
        # Lazy import to avoid requiring transformers when not using HF backend
        try:
            from llms.huggingface.backend import HuggingFaceBackend
            return HuggingFaceBackend()
        except ImportError as e:
            raise ImportError(f"Hugging Face backend requires transformers library. Install with: pip install torch transformers accelerate bitsandbytes. Error: {e}")
    return OpenAIBackend()
