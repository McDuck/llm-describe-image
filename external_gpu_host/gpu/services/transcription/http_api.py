"""Authenticated binary-audio endpoint for remote Faster-Whisper inference."""

from __future__ import annotations

import os
import tempfile
from http import HTTPStatus
from typing import Any, Callable, Optional


MAX_AUDIO_BYTES = int(os.getenv("AUDIO_TRANSCRIPTION_API_MAX_UPLOAD_BYTES", str(512 * 1024 * 1024)))


def serve_audio_transcription_request(handler: Any, transcribe: Callable[[str, str, str, str], str]) -> None:
    """Receive an audio segment and return only its local transcription text."""
    length = _content_length(handler)
    if length is None:
        return
    backend_name = handler.headers.get("X-Transcription-Backend", "").strip()
    model_name = handler.headers.get("X-Transcription-Model", os.getenv("TRANSCRIPTION_MODEL", "turbo")).strip()
    language = handler.headers.get("X-Transcription-Language", "").strip()
    if not model_name or len(model_name) > 256:
        handler._json(HTTPStatus.BAD_REQUEST, {"error": "a valid X-Transcription-Model header is required"})
        return

    descriptor, temporary_path = tempfile.mkstemp(prefix="audio-transcription-", suffix=".m4a")
    try:
        with os.fdopen(descriptor, "wb") as handle:
            _stream_body(handler, handle, length)
        text = transcribe(temporary_path, backend_name, model_name, language)
    except Exception as error:
        print(f"gpu-api-server: audio transcription failed: {error!r}")
        handler._json(HTTPStatus.UNPROCESSABLE_ENTITY, {"error": str(error)})
        return
    finally:
        try:
            os.unlink(temporary_path)
        except OSError:
            pass
    handler._json(HTTPStatus.OK, {"text": text})


def _content_length(handler: Any) -> Optional[int]:
    try:
        length = int(handler.headers.get("Content-Length", ""))
    except ValueError:
        handler._json(HTTPStatus.BAD_REQUEST, {"error": "Content-Length is required"})
        return None
    if length < 1 or length > MAX_AUDIO_BYTES:
        handler._json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "audio exceeds size limit"})
        return None
    return length


def _stream_body(handler: Any, handle: Any, length: int) -> None:
    remaining = length
    while remaining:
        chunk = handler.rfile.read(min(1024 * 1024, remaining))
        if not chunk:
            raise ValueError("request body ended before Content-Length")
        handle.write(chunk)
        remaining -= len(chunk)
