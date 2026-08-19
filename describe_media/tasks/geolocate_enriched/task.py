"""Persist geocoding as an image dependency for the description LLM."""

import json
import os
from typing import Any, Dict, Optional, Tuple

from describe_media.tasks.geolocate.task import GeolocationTask
from describe_media.tasks.media import output_relative_path, split_media_item


class GeolocateEnrichedTask(GeolocationTask):
    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str,
        retry: bool = False,
        retry_failed: bool = False,
        enabled: bool = True,
        initial_wait_seconds: int = 1,
        max_retries: int = 5,
        timeout_seconds: int = 30,
    ) -> None:
        super().__init__(maximum, input_dir, output_dir, initial_wait_seconds, max_retries, timeout_seconds)
        self.retry = retry
        self.retry_failed = retry_failed
        self.enabled = enabled

    def execute(self, item: Any) -> Optional[Tuple[str, Dict[str, Any]]]:
        input_path, metadata = split_media_item(item)
        relative = output_relative_path(input_path, self.input_dir or input_path, metadata)
        output_path = os.path.join(self.output_dir or "", relative + ".geocode.txt")
        error_path = os.path.join(self.output_dir or "", relative + ".geocode.error.json")
        if not self.enabled:
            self._write_text(output_path, "N/A")
            metadata["_geocode"] = "N/A"
            metadata["_stage"] = "geolocation"
            return input_path, metadata
        if not self.retry and os.path.exists(output_path):
            location = self._read(output_path)
            metadata["_geocode"] = location
            if location and location != "N/A":
                metadata["location_str"] = location
            metadata["_stage"] = "geolocation"
            return input_path, metadata
        if not self.retry and not self.retry_failed and os.path.exists(error_path):
            return None

        try:
            _, location = super().execute(input_path)
            self._write_text(output_path, location or "N/A")
            try:
                os.remove(error_path)
            except FileNotFoundError:
                pass
            metadata["_geocode"] = location or "N/A"
            if location:
                metadata["location_str"] = location
            metadata["_stage"] = "geolocation"
            return input_path, metadata
        except Exception as error:
            self._write_json(error_path, {"schema_version": 1, "status": "error", "error": str(error)})
            raise

    @staticmethod
    def _read(path: str) -> str:
        with open(path, "r", encoding="utf-8") as handle:
            return handle.read().strip()

    @staticmethod
    def _write_text(path: str, value: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(value + "\n")

    @staticmethod
    def _write_json(path: str, payload: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
