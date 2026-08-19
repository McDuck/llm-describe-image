import os
import sys
import json
import threading
from typing import Optional, Tuple, TYPE_CHECKING, Any, Dict
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# Add local llms directory to path
sys.path.insert(0, os.path.dirname(__file__))

from describe_media.tasks.media import output_relative_path
from describe_media.tasks.task import Task
from describe_media.tasks.write.task import WriteTask
from llms import get_backend
from llms.base import LLMBackend

if TYPE_CHECKING:
    from lmstudio import FileHandle
else:
    try:
        from lmstudio import FileHandle
    except ImportError:
        from typing import Any as FileHandle  # Fallback if lmstudio not installed


class LLMTask(Task[Tuple[str, Dict[str, Any]], Dict[str, Any]]):
    """Join persisted image dependencies, describe, and write the base caption."""

    # Reverse geocoding is best-effort: public providers can rate-limit or fail
    # independently of the inputs needed to describe an image.
    REQUIRED_STAGES = ("metadata", "resize", "recognition")
    OPTIONAL_STAGES = ("geolocation",)

    def __init__(
        self, maximum: int = 1, model_name: Optional[str] = None,
        prompt: Optional[str] = None, backend_name: Optional[str] = None,
        input_dir: Optional[str] = None, output_dir: Optional[str] = None,
        output_format: Optional[str] = None, retry: bool = False,
        retry_failed: bool = False, sync_api_timeout_s: int = 600000,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.model_name: Optional[str] = model_name
        self.backend_name: Optional[str] = backend_name
        self.backend: Optional[LLMBackend] = None
        self.model: Any = None
        self.prompt: Optional[str] = prompt
        self.server_started_by_script: bool = False
        self.model_loaded_by_script: bool = False
        self.sync_api_timeout_s: int = sync_api_timeout_s
        self.output_dir = output_dir
        self.retry = retry
        self.retry_failed = retry_failed
        self.writer = WriteTask(
            maximum=maximum,
            input_dir=input_dir,
            output_dir=output_dir,
            output_format=output_format,
        )
        self._parts: Dict[str, Dict[str, Tuple[str, Dict[str, Any]]]] = {}
        self._emitted: set[str] = set()
        # Dependency events arrive on worker threads while the pipeline status
        # thread reads this state.  Keep those operations atomic so rendering a
        # progress line cannot fail with "dictionary changed size during
        # iteration".
        self._parts_lock = threading.Lock()
        self._written = 0
        self._errors = 0

    def load(self) -> None:
        """Load the model and backend. Called by worker thread at start."""
        self.backend = get_backend(self.backend_name)
        if self.backend:
            # Bootstrap the server to initialize SDK connection
            self.server_started_by_script = self.backend.bootstrap_server(auto_start=True, sync_api_timeout_s=self.sync_api_timeout_s)
            self.model = self.backend.load_model(self.model_name, allow_cli_install=False)
            if self.model:
                self.model_loaded_by_script = True
        
        if not self.model:
            raise Exception(f"Failed to load model: {self.model_name}")
    
    def unload(self) -> None:
        """Unload the model. Called by worker thread at end."""
        if self.backend and hasattr(self.backend, 'cleanup'):
            self.backend.cleanup(
                model_loaded_by_script=self.model_loaded_by_script,
                model_name=self.model_name,
                server_started_by_script=self.server_started_by_script
            )
        self.model = None
        self.backend = None

    def execute(self, item: Tuple[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Accept one dependency event and run when the required inputs are available."""
        input_path, metadata = item
        if not isinstance(metadata, dict):
            raise metadata
        stage = metadata.get("_stage")
        if stage not in (*self.REQUIRED_STAGES, *self.OPTIONAL_STAGES):
            return None
        key = output_relative_path(input_path, self.input_dir or input_path, metadata)
        with self._parts_lock:
            self._parts.setdefault(key, {})[stage] = (input_path, dict(metadata))
            parts = self._parts[key]
            if key in self._emitted or not all(required in parts for required in self.REQUIRED_STAGES):
                return None

            merged = self._merged_metadata(parts)
            self._emitted.add(key)
        return self._describe(input_path, merged)

    def _merged_metadata(self, parts: Dict[str, Tuple[str, Dict[str, Any]]]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        # Metadata owns EXIF/path values; later stages add only their stage fields.
        # Geolocation contributes only when it completed before the LLM started.
        for stage in (*self.REQUIRED_STAGES, *self.OPTIONAL_STAGES):
            if stage in parts:
                merged.update(parts[stage][1])
        merged.pop("_stage", None)
        return merged

    def _describe(self, input_path: str, metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        relative = output_relative_path(input_path, self.input_dir or input_path, metadata)
        output_path = os.path.join(self.output_dir or "", relative + ".txt")
        error_path = os.path.join(self.output_dir or "", relative + ".error.txt")
        if not self.retry and os.path.exists(output_path):
            self._written += 1
            return {"route": "enhance", "item": (input_path, metadata)}
        if not self.retry and not self.retry_failed and os.path.exists(error_path):
            return None

        try:
            if not self.backend or not self.model:
                raise Exception("Backend or model not configured")
            
            # Prefer an explicit prepared path from upstream tasks, then fall back to local variants.
            image_path = metadata.get('_prepared_image_path') or self.get_preferred_image_path(input_path)
            
            # Prepare image with backend (now done in LLM task)
            image_handle: FileHandle = self.backend.prepare_image(image_path)
            
            # Prepare format values for prompt template
            dt = metadata.get('datetime')
            datetime_value = ""
            if dt:
                datetime_value = dt.strftime("%Y-%m-%d %H:%M:%S") if dt.hour or dt.minute or dt.second else dt.strftime("%Y-%m-%d")
            
            location_value = metadata.get('location_str', "")
            camera_value = metadata.get('camera', "")
            filename_value = metadata.get('filename', "")
            
            # Format prompt with metadata placeholders
            try:
                enhanced_prompt = self.prompt.format(
                    datetime=datetime_value,
                    location=location_value,
                    camera=camera_value,
                    filename=filename_value
                )
            except (KeyError, ValueError):
                # If template has no placeholders or formatting fails, use as-is
                enhanced_prompt = self.prompt

            recognition = self._recognition_for_llm(metadata.get("_recognition"))
            if recognition is not None:
                recognition_json = json.dumps(recognition, ensure_ascii=False, separators=(",", ":"))
                enhanced_prompt += (
                    "\n\nGeverifieerde lokale persoonsherkenning (JSON):\n"
                    f"{recognition_json}\n"
                    "Gebruik uitsluitend een label uit een item met status 'matched' als naam voor een persoon, "
                    "en alleen voor de persoon op de opgegeven face_bbox. Verzin nooit een identiteit. "
                    "Beschrijf per genoemde persoon alleen zichtbare handelingen in deze foto."
                )
            
            # Run LLM inference
            content = self.backend.respond(self.model, enhanced_prompt, image_handle)
            self.writer.execute((input_path, content, metadata))
            self._written += 1
            return {"route": "enhance", "item": (input_path, metadata)}
            
        except Exception as e:
            # Show relative path in error
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            try:
                print(f"Failed LLM {rel_path}: {e}")
            except:
                pass  # Ignore print errors during shutdown
            self.writer.execute((input_path, e, metadata))
            self._errors += 1
            if "Invalid SOS parameters" in str(e) or "Invalid image detected" in str(e):
                return {"route": "fix", "item": (input_path, metadata)}
            return None

    @staticmethod
    def _recognition_for_llm(recognition: Any) -> Optional[Dict[str, Any]]:
        """Return only threshold-approved identities; keep unknown faces on disk."""
        if not isinstance(recognition, dict):
            return None
        people = recognition.get("people")
        if not isinstance(people, list):
            return None
        matched_people = [
            person
            for person in people
            if (
                isinstance(person, dict)
                and person.get("status") == "matched"
                and isinstance(person.get("label"), str)
                and person["label"]
                and isinstance(person.get("match_confidence"), (int, float))
            )
        ]
        return {"people": matched_people} if matched_people else None

    def format_status(self, name: str) -> str:
        with self._parts_lock:
            waiting = [parts for key, parts in self._parts.items() if key not in self._emitted]
            missing = {
                stage: sum(1 for parts in waiting if stage not in parts)
                for stage in ("resize", "recognition")
            }
            ready = sum(1 for parts in waiting if all(stage in parts for stage in self.REQUIRED_STAGES))
        return (
            f"{name}: {len(waiting)}Q "
            f"[p{missing['resize']} r{missing['recognition']} ready{ready}] "
            f"{len(self.active)}A {self._errors}F ->{self._written}t"
        )
