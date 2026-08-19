"""Configuration shared by remote GPU-host pipeline stages."""

from __future__ import annotations

import os
from typing import Optional, Tuple


def remote_gpu_api_config() -> Tuple[Optional[str], Optional[str], float]:
    """Return the common remote GPU API connection settings."""
    return (
        os.getenv("GPU_API_BASE"),
        os.getenv("GPU_API_TOKEN"),
        float(os.getenv("GPU_API_TIMEOUT_S", "120")),
    )
