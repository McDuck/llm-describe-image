"""
Configuration loader for llm-describe-image.
Loads default configuration from config.defaults.toml
"""

import os
from typing import Set, Any, Dict

def load_defaults() -> Dict[str, Any]:
    """Load default configuration from TOML file."""
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            raise ImportError(
                "TOML library not found. Install tomli for Python < 3.11: pip install tomli"
            )
    
    config_file: str = os.path.join(os.path.dirname(__file__), "config.defaults.toml")
    
    with open(config_file, "rb") as f:
        config: Dict[str, Any] = tomllib.load(f)
    return config


# Load configuration at module import
_config: Dict[str, Any] = load_defaults()

# Export individual defaults for convenience
DEFAULT_MODEL_NAME: str = _config["model"]["name"]
DEFAULT_BACKEND: str = _config["model"]["backend"]
DEFAULT_PROMPT: str = _config["model"]["prompt"]
DEFAULT_SORT_ORDER: str = _config["sorting"]["order"]
DEFAULT_NUM_DISCOVER_THREADS: int = _config["threads"]["discover"]
DEFAULT_NUM_SKIP_CHECKER_THREADS: int = _config["threads"]["skip_checker"]
DEFAULT_NUM_DOWNLOAD_THREADS: int = _config["threads"]["download"]
DEFAULT_NUM_LLM_THREADS: int = _config["threads"]["llm"]
DEFAULT_NUM_WRITE_THREADS: int = _config["threads"]["write"]
DEFAULT_BACKPRESSURE_MULTIPLIER: float = _config["performance"]["backpressure_multiplier"]
DEFAULT_RETRY_FAILED: bool = _config["retry"]["failed"]
DEFAULT_OUTPUT_FORMAT: str = _config["output"]["format"]
DEFAULT_IMAGE_EXTENSIONS: Set[str] = set(_config["image"]["extensions"])
