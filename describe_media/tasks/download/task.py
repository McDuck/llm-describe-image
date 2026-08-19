import json
import os
import sys
import tempfile
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

# Add tasks root to path for metadata_extractor in same directory
sys.path.insert(0, os.path.dirname(__file__))

from describe_media.tasks.task import Task
from metadata_extractor import get_image_metadata
from describe_media.tasks.media import output_relative_path, split_media_item


class DownloadTask(Task[Any, Tuple[str, Dict[str, Any]]]):
    def __init__(self, maximum: int = 2, input_dir: Optional[str] = None) -> None:
        super().__init__(maximum, input_dir=input_dir)

    def execute(self, item: Any) -> Tuple[str, Dict[str, Any]]:
        """
        Extract metadata from image (no longer downloads/prepares with backend).
        Uses .fixed image if available from fix_jpeg task.
        Returns: (input_path, metadata)
        Raises: Exception on error (caught by worker_thread and passed to WriteTask)
        """
        try:
            input_path, inherited_metadata = split_media_item(item)
            # Use .fixed image if available (from fix_jpeg task)
            image_path = self.get_preferred_image_path(input_path)
            
            # Extract metadata from image only
            metadata: Dict[str, Any] = get_image_metadata(image_path)
            metadata.update(inherited_metadata)
            return (input_path, metadata)
            
        except Exception as e:
            # Show relative path in error
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            try:
                print(f"Error extracting metadata {rel_path}: {e}")
            except:
                pass  # Ignore print errors during shutdown
            raise


class MetadataTask(DownloadTask):
    """Persist image metadata and emit it as one LLM dependency event."""

    def __init__(
        self,
        maximum: int = 2,
        input_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        retry: bool = False,
        retry_failed: bool = False,
    ) -> None:
        super().__init__(maximum=maximum, input_dir=input_dir)
        self.output_dir = output_dir
        self.retry = retry
        self.retry_failed = retry_failed

    def execute(self, item: Any) -> Optional[Tuple[str, Dict[str, Any]]]:
        input_path, inherited = split_media_item(item)
        if not self.input_dir or not self.output_dir:
            raise RuntimeError("MetadataTask requires input_dir and output_dir")
        relative = output_relative_path(input_path, self.input_dir, inherited)
        output_path = os.path.join(self.output_dir, relative + ".metadata.json")
        error_path = os.path.join(self.output_dir, relative + ".metadata.error.json")

        if not self.retry and os.path.exists(output_path):
            try:
                metadata = self._read(output_path)
                metadata.update(inherited)
                metadata["_stage"] = "metadata"
                return input_path, metadata
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                pass
        if not self.retry and not self.retry_failed and os.path.exists(error_path):
            return None

        try:
            # EXIF belongs to the source image; resized copies are only for model input.
            metadata = get_image_metadata(input_path)
            metadata.update(inherited)
            self._write(output_path, relative, metadata)
            try:
                os.remove(error_path)
            except FileNotFoundError:
                pass
            metadata["_stage"] = "metadata"
            return input_path, metadata
        except Exception as error:
            self._write_error(error_path, relative, error)
            raise

    @staticmethod
    def _serialise(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, tuple):
            return list(value)
        if isinstance(value, dict):
            return {key: MetadataTask._serialise(item) for key, item in value.items() if not key.startswith("_")}
        if isinstance(value, list):
            return [MetadataTask._serialise(item) for item in value]
        return value

    @staticmethod
    def _deserialise(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: MetadataTask._deserialise(item) for key, item in value.items()}
        if isinstance(value, list):
            return [MetadataTask._deserialise(item) for item in value]
        return value

    def _read(self, path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        metadata = self._deserialise(payload["metadata"])
        datetime_value = metadata.get("datetime")
        if isinstance(datetime_value, str):
            metadata["datetime"] = datetime.fromisoformat(datetime_value)
        return metadata

    def _write(self, path: str, relative: str, metadata: Dict[str, Any]) -> None:
        self._write_json(path, {"schema_version": 1, "source": {"relative_path": relative.replace("\\", "/")}, "metadata": self._serialise(metadata)})

    def _write_error(self, path: str, relative: str, error: Exception) -> None:
        self._write_json(path, {"schema_version": 1, "status": "error", "source": {"relative_path": relative.replace("\\", "/")}, "error": str(error)})

    @staticmethod
    def _write_json(path: str, payload: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        descriptor, temporary_path = tempfile.mkstemp(prefix=".metadata-", suffix=".json", dir=os.path.dirname(path))
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(temporary_path, path)
        except Exception:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
            raise
