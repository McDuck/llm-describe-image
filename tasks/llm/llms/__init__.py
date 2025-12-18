from __future__ import annotations
import os
import sys
from typing import Optional

# Import default backend configuration
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from config_loader import DEFAULT_BACKEND

from llms.base import LLMBackend
from llms.lmstudio.backend import LMStudioBackend

def get_backend(name: Optional[str] = None) -> LLMBackend:
    """Factory for LLM backends. Currently supports 'lmstudio' only."""
    backend = (name or os.getenv("BACKEND", DEFAULT_BACKEND)).strip().lower()
    if backend in ("lmstudio", "lm-studio", "lms"):
        return LMStudioBackend()
    # Default
    return LMStudioBackend()
