from __future__ import annotations

import sys
from typing import Dict, List

import describe_media.llm_describe_directory as llm_describe_directory
import describe_media.pipelines as pipelines


class _PipelineStub:
    name = "describe"
    debug = False
    retry = False
    retry_failed = False


def test_cli_uses_describe_for_plain_input_and_output_paths(monkeypatch) -> None:
    calls: List[Dict] = []
    requested_names: List[str] = []
    target = _PipelineStub()

    monkeypatch.setattr(llm_describe_directory, "list_pipelines", lambda: {"describe": "describe"})
    monkeypatch.setattr(
        llm_describe_directory,
        "get_pipeline",
        lambda name: requested_names.append(name) or target,
    )
    monkeypatch.setattr(sys, "argv", ["describe_media.py", "input", "output"])
    monkeypatch.setattr(target, "run", lambda **kwargs: calls.append(kwargs), raising=False)

    llm_describe_directory.main()

    assert calls == [
        {
            "input_dir": "input",
            "output_dir": "output",
            "verbose": False,
            "status_interval": 5.0,
            "subdirectory": None,
            "manifest_path": None,
            "random_sample_size": None,
        }
    ]
    assert requested_names == ["describe"]


def test_registry_keeps_only_supported_pipeline_commands() -> None:
    assert set(pipelines.list_pipelines()) == {
        "describe",
        "enhance",
        "geolocate",
        "recognition-cluster",
        "recognition-train",
    }
