import os
import sys
from typing import Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from describe_media.tasks.task import Task


class VideoErrorTask(Task[Tuple[str, Exception], str]):
    """Persist extraction errors where SkipVideoCheckTask can honour retry flags."""

    def __init__(self, maximum: int, input_dir: str, output_dir: str) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir = output_dir

    def execute(self, item: Tuple[str, Exception]) -> str:
        input_path, error = item
        relative = os.path.relpath(input_path, self.input_dir)
        error_path = os.path.join(self.output_dir, relative + ".frames.error.txt")
        os.makedirs(os.path.dirname(error_path), exist_ok=True)
        with open(error_path, "w", encoding="utf-8") as handle:
            handle.write(f"{error}\n")
        return error_path
