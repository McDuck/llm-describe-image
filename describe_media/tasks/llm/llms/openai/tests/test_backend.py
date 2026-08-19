import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[6]
LLM_ROOT = REPO_ROOT / "describe_media" / "tasks" / "llm"
if str(LLM_ROOT) not in sys.path:
    sys.path.insert(0, str(LLM_ROOT))

from llms.openai.backend import OpenAIBackend
from llms import get_backend


class OpenAIBackendTests(unittest.TestCase):
    def test_openai_is_the_default_backend(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertIsInstance(get_backend(), OpenAIBackend)

    def test_uses_openai_vision_request(self) -> None:
        request_log = {}

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b'{"choices": [{"message": {"content": "description"}}]}'

        def fake_urlopen(request, timeout):
            request_log["url"] = request.full_url
            request_log["payload"] = json.loads(request.data.decode("utf-8"))
            request_log["timeout"] = timeout
            return FakeResponse()

        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory) / "image.jpg"
            image.write_bytes(b"jpeg-bytes")
            with patch.dict("os.environ", {"OPENAI_API_BASE": "https://api.openai.com/v1/"}):
                with patch("llms.openai.backend.urlopen", fake_urlopen):
                    backend = OpenAIBackend()
                    result = backend.respond("gpt-5", "Describe this image", backend.prepare_image(str(image)))

        self.assertEqual(result, "description")
        self.assertEqual(request_log["url"], "https://api.openai.com/v1/chat/completions")
        self.assertEqual(request_log["timeout"], 600)
        self.assertEqual(request_log["payload"]["model"], "gpt-5")
        content = request_log["payload"]["messages"][0]["content"]
        self.assertEqual(content[0], {"type": "text", "text": "Describe this image"})
        self.assertEqual(content[1]["type"], "image_url")
        self.assertEqual(content[1]["image_url"]["url"], "data:image/jpeg;base64,anBlZy1ieXRlcw==")

    def test_reachability_log_uses_generic_openai_endpoint_name(self) -> None:
        with patch.dict("os.environ", {"OPENAI_API_BASE": "http://127.0.0.1:15002/v1"}):
            backend = OpenAIBackend()
        with patch.object(backend, "_request"), patch("builtins.print") as print_mock:
            backend.bootstrap_server(auto_start=True)

        print_mock.assert_called_once_with(
            "OpenAI API endpoint is reachable at http://127.0.0.1:15002/v1."
        )
