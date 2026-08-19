import json
import os
import sys
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from describe_media.tasks.task import Task


class SkipVideoCheckTask(Task[str, Optional[str]]):
    """Skip known failed videos unless retry was explicitly requested.

    A valid extraction manifest is intentionally allowed through: ExtractVideoTask
    will read its listed frames rather than decoding the video again.
    """

    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str,
        retry_failed: bool,
        retry: bool,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir = output_dir
        self.retry_failed = retry_failed
        self.retry = retry

    def execute(self, input_path: str) -> Optional[str]:
        relative = os.path.relpath(input_path, self.input_dir)
        error_path = os.path.join(self.output_dir, relative + ".frames.error.txt")
        if not self.retry and not self.retry_failed and os.path.exists(error_path):
            return None
        return input_path
