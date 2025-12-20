from __future__ import annotations
import shutil
import subprocess
import sys
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

    def _run_cli_command(self, cmd_args: list[str], capture_output: bool = False) -> tuple[bool, Optional[str]]:
        """Run a CLI command and return (success, output)."""
        try:
            if capture_output:
                proc = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
                output, _ = proc.communicate()
                return proc.returncode == 0, output
            else:
                proc = subprocess.Popen(cmd_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                proc.wait()
                return proc.returncode == 0, None
        except Exception:
            return False, None

    def _run_cli_command_with_messages(self, cmd_args: list[str], start_msg: str, success_msg: str, error_msg: str, capture_output: bool = False) -> bool:
        """Run a CLI command with status messages."""
        if start_msg:
            print(start_msg)
        success, _ = self._run_cli_command(cmd_args, capture_output=capture_output)
        if error_msg or success_msg:
            print(success_msg if success else error_msg)
        return success

    def bootstrap_server(self, auto_start: bool) -> bool:
        # Try SDK bootstrap - if it works, server is already running
        try:
            lms.bootstrap()
            print("LM Studio server is running.")
            return False  # Didn't start it, was already running
        except Exception:
            pass
        cli = self._cli()
        if not cli:
            return False
        if auto_start:
            try:
                return self._run_cli_command_with_messages(
                    [cli, "server", "start"],
                    "LM Studio server starting...",
                    "LM Studio server started.",
                    "LM Studio server starting failed!"
                )
            except Exception as e:
                print(f"Failed to start LM Studio server: {e}")
                return False
        # Fallback: check status commands
        success, _ = self._run_cli_command([cli, "server", "status"], capture_output=True)
        return False  # Server exists but we didn't start it

    def load_model(self, model_name: str, allow_cli_install: bool, context_size: int = 0) -> Any:
        cli = self._cli()
        preloaded_model_name = None
        preloaded_context_size = None
        
        # detect preloaded model
        if cli:
            success, output = self._run_cli_command([cli, "ps"], capture_output=True)
            if success and output and output.strip():
                lines = output.strip().split('\n')
                if len(lines) > 1:
                    self._model_was_preloaded = True
                    # Extract model name and context size from ps output
                    parts = lines[1].split()
                    preloaded_model_name = parts[0] if len(parts) > 0 else None
                    # Context is usually in a column, try to find it
                    if len(parts) > 4:
                        try:
                            preloaded_context_size = int(parts[4])
                        except (ValueError, IndexError):
                            pass
                    print(f"Model already loaded: {preloaded_model_name} (context: {preloaded_context_size})")
        
        # If we need a specific context size and model is loaded with different context, unload it first
        if context_size > 0 and self._model_was_preloaded and (preloaded_model_name != model_name or preloaded_context_size != context_size) and cli:
            print(f"Unloading preloaded model (context {preloaded_context_size}) to load {model_name} with required context size {context_size}...")
            self._run_cli_command([cli, "unload"])
            self._model_was_preloaded = False
        
        if not self._model_was_preloaded:
            print("Loading model...")
        
        try:
            # Try loading with context size using CLI if specified and not preloaded
            if context_size > 0 and cli and not self._model_was_preloaded:
                # Use CLI to load with custom context length
                cli_cmd = [cli, "load", model_name, "--context-length", str(context_size), "-y"]
                cli_result = self._run_cli_command(cli_cmd)
                if cli_result[0]:
                    model = lms.llm(model_name)
                    if model:
                        print(f"Model loaded.")
                        return model
                # Fall through to standard load if CLI fails
            
            # Fall back to standard load
            model = lms.llm(model_name)
            if not self._model_was_preloaded:
                print(f"Model loaded.")
            return model
        except Exception:
            if not allow_cli_install or not cli:
                return None
            # Prompting handled by caller; here we try CLI load unprompted if allowed
            if self._run_cli_command([cli, "load", model_name])[0]:
                try:
                    model = lms.llm(model_name)
                    print(f"Model loaded.")
                    return model
                except Exception:
                    return None
        return None

    def prepare_image(self, path: str) -> FileHandle:
        # Retry logic for SDK connection issues
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return lms.prepare_image(path)
            except Exception as e:
                error_msg = str(e)
                if "Client unexpectedly disconnected." == error_msg.lower():
                    if attempt < max_retries - 1:
                        # Try to reconnect
                        time.sleep(1)
                        try:
                            lms.bootstrap()
                        except Exception:
                            pass
                        continue
                # Re-raise if not a connection issue or final attempt
                raise

    def respond(self, model: Any, prompt: str, image_handle: Optional[FileHandle] = None) -> str:
        chat = lms.Chat()
        if image_handle is not None:
            chat.add_user_message(prompt, images=[image_handle])
        else:
            chat.add_user_message(prompt)
        result = model.respond(chat)
        content = getattr(result, "content", None)
        if content is None:
            content = str(result)
        return content

    def cleanup(self, model_loaded_by_script: bool, model_name: Optional[str], server_started_by_script: bool) -> None:
        cli = self._cli()
        if cli and model_loaded_by_script and not self._model_was_preloaded and model_name:
            self._run_cli_command_with_messages(
                [cli, "unload", model_name],
                "Model unloading...",
                "Model unloaded.",
                "Model unloading failed!"
            )
        if cli and server_started_by_script:
            try:
                self._run_cli_command_with_messages(
                    [cli, "server", "stop"],
                    "LMStudio server stopping...",
                    "LMStudio server stopped.",
                    "LMStudio server stopping failed!"
                )
            except Exception as e:
                print(f"Error closing server: {e}")
        
        # Report server status if we didn't start it
        if cli and not server_started_by_script:
            self._run_cli_command_with_messages(
                [cli, "server", "status"],
                "",
                "LMStudio server is still running (was not started by script).",
                "",
                capture_output=True
            )
