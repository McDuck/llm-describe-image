"""Client for the remote Faster-Whisper audio-transcription endpoint."""

from __future__ import annotations

import json
from http.client import HTTPConnection, HTTPSConnection
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


class RemoteAudioTranscriptionBackend:
    """Stream small extracted-audio segments to the shared GPU worker."""

    def __init__(self, api_base: str, token: str, timeout_seconds: float = 600.0) -> None:
        if not api_base.startswith(("http://", "https://")):
            raise ValueError("GPU API base must be an http(s) URL for video transcription")
        if not token:
            raise ValueError("GPU API token is required for video transcription")
        self.api_base = api_base.rstrip("/")
        self.token = token
        self.timeout_seconds = float(timeout_seconds)

    def load(self) -> None:
        self._request("GET", "/health")

    def transcribe(self, audio_path: str, backend_name: str, model_name: str, language: str) -> str:
        payload = self._upload_audio(audio_path, backend_name, model_name, language)
        text = payload.get("text")
        if not isinstance(text, str):
            raise RuntimeError("GPU transcription worker returned an invalid text response")
        return text

    def _upload_audio(self, audio_path: str, backend_name: str, model_name: str, language: str) -> Dict[str, Any]:
        parsed = urlsplit(self.api_base)
        connection_type = HTTPSConnection if parsed.scheme == "https" else HTTPConnection
        connection = connection_type(parsed.hostname, parsed.port, timeout=self.timeout_seconds)
        endpoint = (parsed.path.rstrip("/") + "/audio-transcriptions") or "/audio-transcriptions"
        if parsed.query:
            endpoint = f"{endpoint}?{parsed.query}"
        size = Path(audio_path).stat().st_size
        try:
            connection.putrequest("POST", endpoint)
            connection.putheader("Authorization", f"Bearer {self.token}")
            connection.putheader("Content-Type", "audio/mp4")
            connection.putheader("Content-Length", str(size))
            connection.putheader("X-Transcription-Backend", backend_name)
            connection.putheader("X-Transcription-Model", model_name)
            connection.putheader("X-Transcription-Language", language)
            connection.endheaders()
            with Path(audio_path).open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    connection.send(chunk)
            response = connection.getresponse()
            raw = response.read()
        except (OSError, ValueError) as error:
            raise RuntimeError(f"GPU transcription worker request failed: {error}") from error
        finally:
            connection.close()
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as error:
            raise RuntimeError("GPU transcription worker returned invalid JSON") from error
        if response.status >= 400:
            detail = raw.decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"GPU transcription worker returned HTTP {response.status}: {detail}")
        if not isinstance(payload, dict):
            raise RuntimeError("GPU transcription worker returned an invalid JSON object")
        return payload

    def _request(self, method: str, path: str) -> Dict[str, Any]:
        request = Request(
            f"{self.api_base}{path}",
            headers={"Authorization": f"Bearer {self.token}"},
            method=method,
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"GPU transcription worker returned HTTP {error.code}: {detail}") from error
        except (URLError, OSError, ValueError) as error:
            raise RuntimeError(f"GPU transcription worker request failed: {error}") from error
        if not isinstance(payload, dict):
            raise RuntimeError("GPU transcription worker returned an invalid JSON object")
        return payload
