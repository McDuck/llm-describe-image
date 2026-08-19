"""Enhance base captions and persist the exact context used."""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "llm"))

from llms import get_backend
from llms.base import LLMBackend
from describe_media.tasks.media import output_relative_path
from describe_media.tasks.task import Task


class IntegratedEnhanceTask(Task[Tuple[str, Dict[str, Any]], str]):
    def __init__(
        self, maximum: int, input_dir: str, output_dir: str, model_name: Optional[str],
        prompt: str, backend_name: Optional[str], context_window_days: int,
        max_context_items: int, retry: bool = False, retry_failed: bool = False,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir = output_dir
        self.model_name = model_name
        self.prompt = prompt
        self.backend_name = backend_name
        self.context_window_days = context_window_days
        self.max_context_items = max_context_items
        self.retry = retry
        self.retry_failed = retry_failed
        self.backend: Optional[LLMBackend] = None
        self.model: Any = None

    def load(self) -> None:
        self.backend = get_backend(self.backend_name)
        if self.backend:
            self.model = self.backend.load_model(self.model_name, allow_cli_install=False)
        if not self.model:
            raise RuntimeError(f"Failed to load enhancement model: {self.model_name}")

    def unload(self) -> None:
        if self.backend and hasattr(self.backend, "cleanup"):
            self.backend.cleanup(model_loaded_by_script=True, model_name=self.model_name, server_started_by_script=False)
        self.backend = None
        self.model = None

    def execute(self, item: Tuple[str, Dict[str, Any]]) -> Optional[str]:
        input_path, metadata = item
        relative = output_relative_path(input_path, self.input_dir or input_path, metadata)
        output_path = os.path.join(self.output_dir, relative + ".enhanced.txt")
        provenance_path = os.path.join(self.output_dir, relative + ".enhanced.json")
        error_path = os.path.join(self.output_dir, relative + ".enhanced.error.json")
        if not self.retry and os.path.exists(output_path) and os.path.exists(provenance_path):
            self.record_skip()
            return output_path
        if not self.retry and not self.retry_failed and os.path.exists(error_path):
            self.record_skip()
            return None

        base_caption_path = os.path.join(self.output_dir, relative + ".txt")
        if not os.path.exists(base_caption_path):
            return self._missing(error_path, relative, [], [relative + ".txt"])
        with open(base_caption_path, "r", encoding="utf-8") as handle:
            base_caption = handle.read().strip()

        contexts = self._context_files(relative, metadata)
        missing = [candidate["caption"] for candidate in contexts if not os.path.exists(os.path.join(self.output_dir, candidate["caption"]))]
        if missing:
            return self._missing(error_path, relative, contexts, missing)

        context_text = []
        for candidate in contexts:
            caption_path = os.path.join(self.output_dir, candidate["caption"])
            with open(caption_path, "r", encoding="utf-8") as handle:
                context_text.append(handle.read().strip())
        prompt = self.prompt.format(context_section="\n\n".join(context_text), original_description=base_caption)
        if not self.backend or not self.model:
            raise RuntimeError("Enhancement model is not configured")
        try:
            content = self.backend.respond(self.model, prompt).strip()
            self._write_text(output_path, content)
            self._write_json(provenance_path, {
                "schema_version": 1,
                "status": "complete",
                "target": relative.replace("\\", "/"),
                "base_caption": relative.replace("\\", "/") + ".txt",
                "context": contexts,
                "output": relative.replace("\\", "/") + ".enhanced.txt",
            })
            try:
                os.remove(error_path)
            except FileNotFoundError:
                pass
            return output_path
        except Exception as error:
            self._write_json(error_path, {"schema_version": 1, "status": "error", "target": relative.replace("\\", "/"), "error": str(error)})
            raise

    def _context_files(self, relative: str, metadata: Dict[str, Any]) -> List[Dict[str, str]]:
        target_time = metadata.get("datetime")
        if isinstance(target_time, str):
            target_time = datetime.fromisoformat(target_time)
        if not isinstance(target_time, datetime):
            return []
        candidates: List[Tuple[float, str]] = []
        for metadata_path in Path(self.output_dir).rglob("*.metadata.json"):
            try:
                with metadata_path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                source = payload.get("source", {}).get("relative_path")
                value = payload.get("metadata", {}).get("datetime")
                if not isinstance(source, str) or source == relative.replace("\\", "/") or not isinstance(value, str):
                    continue
                candidate_time = datetime.fromisoformat(value)
                distance = abs((candidate_time - target_time).total_seconds())
                if distance <= self.context_window_days * 86400:
                    candidates.append((distance, source))
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                continue
        candidates.sort(key=lambda candidate: (candidate[0], candidate[1]))
        return [{"image": source, "caption": source + ".txt"} for _, source in candidates[:self.max_context_items]]

    def _missing(self, error_path: str, relative: str, contexts: List[Dict[str, str]], missing: List[str]) -> None:
        self._write_json(error_path, {
            "schema_version": 1,
            "status": "error",
            "reason": "missing_context_captions",
            "target": relative.replace("\\", "/"),
            "required_context": len(contexts),
            "available_context": len(contexts) - len(missing),
            "missing": missing,
        })
        raise RuntimeError(f"Enhancement context is incomplete for {relative}: {', '.join(missing)}")

    @staticmethod
    def _write_text(path: str, content: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)

    @staticmethod
    def _write_json(path: str, payload: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
