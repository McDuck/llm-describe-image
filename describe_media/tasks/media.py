"""Shared helpers for media work items flowing through image tasks."""

import os
from typing import Any, Dict, Tuple


def split_media_item(item: Any) -> Tuple[str, Dict[str, Any]]:
    """Return a physical image path and optional pipeline metadata."""
    if isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str) and isinstance(item[1], dict):
        return item[0], dict(item[1])
    if isinstance(item, str):
        return item, {}
    raise TypeError(f"Expected an image path or (path, metadata), got {type(item).__name__}")


def output_relative_path(input_path: str, input_dir: str, metadata: Dict[str, Any]) -> str:
    """Return the stable output identity for an image or extracted video frame."""
    relative = metadata.get("_output_relative_path")
    if isinstance(relative, str) and relative:
        return relative
    return os.path.relpath(input_path, input_dir)
