"""Normalize source images and extracted frames into one image event stream."""

from typing import Any, Dict, Tuple

from describe_media.tasks.media import split_media_item
from describe_media.tasks.task import Task


class ImageRouterTask(Task[Any, Tuple[str, Dict[str, Any]]]):
    """Attach source/frame identity before broadcasting work to image stages."""

    def execute(self, item: Any) -> Tuple[str, Dict[str, Any]]:
        image_path, metadata = split_media_item(item)
        metadata.setdefault("_source_image", "_source_video_path" not in metadata)
        return image_path, metadata
