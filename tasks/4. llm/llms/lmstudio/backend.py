from __future__ import annotations
import shutil
import subprocess
import time
from typing import Any, Optional

import lmstudio as lms
from lmstudio import FileHandle

from llms.base import LLMBackend

class LMStudioBackend(LLMBackend):
    def __init__(self):
        self._model_was_preloaded = False

    def _cli(self) -> Optional[str]:
        return shutil.which("lms")

    def bootstrap_server(self, auto_start: bool) -> bool:
        # Try SDK bootstrap
        try:
            lms.bootstrap()
            return True
        except Exception:
            pass
        cli = self._cli()
        if not cli:
            return False
        if auto_start:
            try:
                subprocess.Popen([cli, "server", "start"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(2)
                try:
                    lms.bootstrap()
                    return True
                except Exception:
                    return False
            except Exception:
                return False
        # Fallback: check status commands
        try:
            out = subprocess.check_output([cli, "server", "status"], stderr=subprocess.STDOUT, timeout=2, encoding="utf-8")
            return bool(out.strip())
        except Exception:
            return False

    def load_model(self, model_name: str, allow_cli_install: bool) -> Any:
        cli = self._cli()
        # detect preloaded
        if cli:
            try:
                result = subprocess.run([cli, "ps"], capture_output=True, text=True, timeout=2)
                if result.returncode == 0 and result.stdout.strip():
                    lines = result.stdout.strip().split('\n')
                    if len(lines) > 1:
                        self._model_was_preloaded = True
            except Exception:
                pass
        try:
            model = lms.llm(model_name)
            return model
        except Exception:
            if not allow_cli_install or not cli:
                return None
            # Prompting handled by caller; here we try CLI load unprompted if allowed
            try:
                proc = subprocess.Popen([cli, "load", model_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                while proc.poll() is None:
                    time.sleep(2)
                if proc.returncode == 0:
                    try:
                        return lms.llm(model_name)
                    except Exception:
                        return None
            except Exception:
                return None
        return None

    def prepare_image(self, path: str) -> FileHandle:
        return lms.prepare_image(path)

    def respond(self, model: Any, prompt: str, image_handle: FileHandle) -> str:
        chat = lms.Chat()
        chat.add_user_message(prompt, images=[image_handle])
        result = model.respond(chat)
        content = getattr(result, "content", None)
        if content is None:
            content = str(result)
        return content

    def cleanup(self, model_loaded_by_script: bool, model_name: Optional[str], server_started_by_script: bool) -> None:
        cli = self._cli()
        if cli and model_loaded_by_script and not self._model_was_preloaded and model_name:
            try:
                subprocess.run([cli, "unload", model_name], check=False, timeout=10)
            except Exception:
                pass
        if cli and server_started_by_script:
            try:
                subprocess.run([cli, "server", "stop"], check=False, timeout=5)
            except Exception:
                pass
