import json
import math
import os
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from describe_media.tasks.task import Task


MANIFEST_SCHEMA_VERSION = 1


class ExtractVideoTask(Task[str, List[Tuple[str, Dict[str, Any]]]]):
    """Reuse a video's valid frame manifest or decode sampled JPEG frames."""

    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str,
        frame_interval_seconds: float,
        max_frames: int,
        retry: bool = False,
        retry_failed: bool = False,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir = output_dir
        self.frame_interval_seconds = float(frame_interval_seconds)
        self.max_frames = int(max_frames)
        self.retry = retry
        self.retry_failed = retry_failed

    def execute(self, input_path: str) -> List[Tuple[str, Dict[str, Any]]]:
        error_path = os.path.join(self.output_dir, os.path.relpath(input_path, self.input_dir) + ".frames.error.txt")
        if not self.retry and not self.retry_failed and os.path.exists(error_path):
            return []
        manifest_path = self._manifest_path(input_path)
        manifest = self._read_valid_manifest(input_path, manifest_path)
        if manifest is None:
            manifest = self._extract(input_path, manifest_path)
        self._remove_error_marker(input_path)
        return self._items_from_manifest(input_path, manifest)

    def _manifest_path(self, input_path: str) -> str:
        return os.path.join(self.output_dir, os.path.relpath(input_path, self.input_dir) + ".frames.json")

    def _source_signature(self, input_path: str) -> Dict[str, int]:
        stat = os.stat(input_path)
        return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}

    def _read_valid_manifest(self, input_path: str, manifest_path: str) -> Optional[Dict[str, Any]]:
        if self.retry or not os.path.exists(manifest_path):
            return None
        try:
            with open(manifest_path, "r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
                return None
            if manifest.get("source", {}).get("signature") != self._source_signature(input_path):
                return None
            settings = manifest.get("sampling", {})
            if settings.get("frame_interval_seconds") != self.frame_interval_seconds or settings.get("max_frames") != self.max_frames:
                return None
            frames = manifest.get("frames")
            if not isinstance(frames, list) or not frames or not all(os.path.exists(frame.get("path", "")) for frame in frames):
                return None
            return manifest
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return None

    def _extract(self, input_path: str, manifest_path: str) -> Dict[str, Any]:
        try:
            import cv2  # type: ignore
        except ImportError as error:
            raise RuntimeError("Video support requires 'opencv-python-headless' (install with pip install -e \"describe_media[video]\")") from error

        capture = cv2.VideoCapture(input_path)
        if not capture.isOpened():
            raise RuntimeError(f"Could not open video file: {input_path}")
        try:
            fps = float(capture.get(cv2.CAP_PROP_FPS))
            frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
            if fps <= 0 or frame_count <= 0:
                raise RuntimeError(f"Could not determine video duration: {input_path}")
            duration = frame_count / fps
            timestamps = self._sample_timestamps(duration, fps)
            relative_video = os.path.relpath(input_path, self.input_dir)
            destination_dir = os.path.join(self.output_dir, os.path.dirname(relative_video))
            os.makedirs(destination_dir, exist_ok=True)
            frames: List[Dict[str, Any]] = []
            for number, timestamp in enumerate(timestamps, start=1):
                capture.set(cv2.CAP_PROP_POS_MSEC, timestamp * 1000.0)
                success, frame = capture.read()
                if not success:
                    raise RuntimeError(f"Could not extract frame at {timestamp:.3f} seconds from {input_path}")
                frame_name = f"{os.path.basename(input_path)}.frame-{number:04d}-t{timestamp:08.3f}.jpg"
                frame_path = os.path.join(destination_dir, frame_name)
                if not cv2.imwrite(frame_path, frame):
                    raise RuntimeError(f"Could not write extracted frame: {frame_path}")
                frames.append({"number": number, "timestamp_seconds": timestamp, "path": frame_path})
        finally:
            capture.release()

        manifest = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "source": {
                "relative_path": os.path.relpath(input_path, self.input_dir).replace("\\", "/"),
                "signature": self._source_signature(input_path),
                "duration_seconds": duration,
            },
            "sampling": {
                "frame_interval_seconds": self.frame_interval_seconds,
                "max_frames": self.max_frames,
            },
            "frames": frames,
        }
        self._write_manifest(manifest_path, manifest)
        return manifest

    def _sample_timestamps(self, duration: float, fps: float) -> List[float]:
        requested = max(1, int(math.ceil(duration / self.frame_interval_seconds)))
        count = min(requested, self.max_frames)
        if count == 1:
            return [0.0]
        # The final duration timestamp points immediately after the final frame.
        latest = max(0.0, duration - (1.0 / fps))
        return [round(latest * index / (count - 1), 3) for index in range(count)]

    def _remove_error_marker(self, input_path: str) -> None:
        error_path = os.path.join(self.output_dir, os.path.relpath(input_path, self.input_dir) + ".frames.error.txt")
        try:
            os.remove(error_path)
        except FileNotFoundError:
            pass

    def _items_from_manifest(self, input_path: str, manifest: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
        source_relative = os.path.relpath(input_path, self.input_dir)
        result: List[Tuple[str, Dict[str, Any]]] = []
        for frame in manifest["frames"]:
            frame_path = frame["path"]
            frame_relative = os.path.join(os.path.dirname(source_relative), os.path.basename(frame_path))
            result.append((frame_path, {
                "_output_relative_path": frame_relative,
                "_source_video_path": input_path,
                "_frame_number": frame["number"],
                "_frame_timestamp_seconds": frame["timestamp_seconds"],
            }))
        return result

    @staticmethod
    def _write_manifest(path: str, manifest: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        descriptor, temporary_path = tempfile.mkstemp(prefix=".frames-", suffix=".json", dir=os.path.dirname(path))
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(manifest, handle, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(temporary_path, path)
        except Exception:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
            raise
