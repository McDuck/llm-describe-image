from __future__ import annotations
import os
from typing import Optional

from llms.base import LLMBackend
from llms.lmstudio.backend import LMStudioBackend

def get_backend(name: Optional[str] = None) -> LLMBackend:
    """Factory for LLM backends. Currently supports 'lmstudio' only."""
    backend = (name or os.getenv("BACKEND", "lmstudio")).strip().lower()
    if backend in ("lmstudio", "lm-studio", "lms"):
        return LMStudioBackend()
    # Default
    return LMStudioBackend()
