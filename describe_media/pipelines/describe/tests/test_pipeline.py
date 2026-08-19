import json
import threading

import pytest

from describe_media.pipelines.describe.pipeline import DescribePipeline, route_discovered_media, route_extracted_video_frame, route_llm_result, route_resize_targets
from describe_media.tasks.enhance_integrated.task import IntegratedEnhanceTask
from describe_media.tasks.geolocate_enriched.task import GeolocateEnrichedTask
from describe_media.tasks.llm.task import LLMTask
from describe_media.tasks.resize.task import ResizeTask
from describe_media.tasks.shortcut.task import ShortcutTask
from describe_media.tasks.task import Task


def test_describe_graph_has_no_skip_or_write_nodes():
    names = [item["name"] for item in DescribePipeline.PIPELINE_CONFIG]

    assert "SkipImageCheck" not in names
    assert "SkipVideoCheck" not in names
    assert "Write" not in names
    assert {"ImageRouter", "Metadata", "Geolocate", "Enhance", "FixJPEG"} <= set(names)
    geolocate = next(item for item in DescribePipeline.PIPELINE_CONFIG if item["name"] == "Geolocate")
    assert geolocate["num_threads"] == 1


def test_describe_routes_videos_to_shortcut_and_frame_extraction():
    assert route_discovered_media("/input/photo.jpg") == "ImageRouter"
    assert route_discovered_media("/input/clip.mp4") == ["ExtractVideo", "Shortcut"]
    assert route_extracted_video_frame(("/output/clip.frame.jpg", {})) == ["ImageRouter", "Transcribe"]
    assert route_llm_result({"route": "enhance", "item": ("/output/clip.frame.jpg", {"_source_video_path": "/input/clip.mp4"})}) == ["enhance", "DescribeVideo"]


def test_describe_passes_shared_gpu_api_settings_to_recognition_and_extraction(monkeypatch):
    monkeypatch.setenv("GPU_API_BASE", "http://gpu-host.example:5002/v1")
    monkeypatch.setenv("GPU_API_TOKEN", "test-token")
    monkeypatch.setenv("GPU_API_TIMEOUT_S", "30")
    pipeline = DescribePipeline()
    extraction_config = next(item for item in pipeline.PIPELINE_CONFIG if item["name"] == "ExtractVideo")
    recognition_config = next(item for item in pipeline.PIPELINE_CONFIG if item["name"] == "Recognition")
    transcription_config = next(item for item in pipeline.PIPELINE_CONFIG if item["name"] == "Transcribe")

    extraction_settings = extraction_config["kwargs_builder"](pipeline)
    recognition_settings = recognition_config["kwargs_builder"](pipeline)

    for settings in (extraction_settings, recognition_settings):
        assert settings["remote_api_base"] == "http://gpu-host.example:5002/v1"
        assert settings["remote_api_token"] == "test-token"
        assert settings["remote_timeout_seconds"] == 30.0

    transcription_settings = transcription_config["kwargs_builder"](pipeline)
    assert transcription_settings["remote_api_base"] == "http://gpu-host.example:5002/v1"
    assert transcription_settings["remote_api_token"] == "test-token"
    assert transcription_settings["remote_timeout_seconds"] == 30.0


def test_describe_routes_resized_images_to_a_matching_shortcut():
    assert route_resize_targets(("/input/photo.jpg", {})) == ["Recognition", "LLM"]
    assert route_resize_targets(("/input/photo.jpg", {"_shortcut_output_relative_path": "photo.resized.jpg"})) == [
        "Recognition", "LLM", "Shortcut"
    ]


def test_describe_status_uses_queue_skip_active_processed_failure_order():
    pipeline = DescribePipeline()
    task = Task(maximum=1)
    task.queue.append("queued")
    task.active.append("active")
    for _ in range(5):
        task.total.finish()
    task.total.skip()
    task.total.skip()
    task.total.fail()

    assert pipeline._format_task_status("Resize", task) == "Resize: 1Q 2S 1A 3P 1F ->5p"


def test_resized_image_shortcut_uses_the_resized_output_name(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    source_path = input_dir / "album" / "photo.jpg"
    resized_path = output_dir / "album" / "photo.resized.jpg"
    metadata = {}
    resize = ResizeTask(maximum=1, input_dir=str(input_dir), output_dir=str(output_dir))

    resize._set_prepared_image_metadata(metadata, "album/photo.jpg", str(resized_path))

    assert metadata["_shortcut_output_relative_path"] == "album/photo.resized.jpg"
    shortcut = ShortcutTask(maximum=1, input_dir=str(input_dir), output_dir=str(output_dir))
    assert shortcut._shortcut_output_relative_path(str(source_path), metadata) == "album/photo.resized.jpg"


def test_portable_shortcut_uses_a_relative_windows_target(tmp_path):
    source_path = tmp_path / "albums" / "album" / "photo.jpg"
    shortcut_path = tmp_path / "output" / "album" / "photo.jpg.lnk"
    source_path.parent.mkdir(parents=True)
    shortcut_path.parent.mkdir(parents=True)

    ShortcutTask._create_portable_windows_shortcut(str(shortcut_path), str(source_path))

    contents = shortcut_path.read_bytes()
    expected = "..\\..\\albums\\album\\photo.jpg".encode("utf-16-le")
    assert contents[:4] == (0x4C).to_bytes(4, "little")
    assert contents[20:24] == (0x00000088).to_bytes(4, "little")
    assert contents.endswith((len(expected) // 2).to_bytes(2, "little") + expected)


def test_llm_does_not_wait_for_geolocation():
    task = LLMTask(maximum=1, input_dir="/input", output_dir="/output")
    task._describe = lambda path, metadata: {"route": "enhance", "item": (path, metadata)}
    common = {"_output_relative_path": "photo.jpg"}

    assert task.execute(("/input/photo.jpg", {**common, "_stage": "metadata"})) is None
    assert task.execute(("/input/photo.jpg", {**common, "_stage": "resize", "_prepared_image_path": "/input/photo.jpg"})) is None
    result = task.execute(("/input/photo.jpg", {**common, "_stage": "recognition", "_recognition": {}}))

    assert result["route"] == "enhance"


def test_llm_uses_geolocation_when_it_arrives_before_required_inputs():
    task = LLMTask(maximum=1, input_dir="/input", output_dir="/output")
    task._describe = lambda path, metadata: {"route": "enhance", "item": (path, metadata)}
    common = {"_output_relative_path": "photo.jpg"}

    assert task.execute(("/input/photo.jpg", {**common, "_stage": "geolocation", "location_str": "Amsterdam"})) is None
    assert task.execute(("/input/photo.jpg", {**common, "_stage": "metadata"})) is None
    assert task.execute(("/input/photo.jpg", {**common, "_stage": "resize", "_prepared_image_path": "/input/photo.jpg"})) is None
    result = task.execute(("/input/photo.jpg", {**common, "_stage": "recognition", "_recognition": {}}))

    assert result["item"][1]["location_str"] == "Amsterdam"


def test_llm_waits_for_transcript_only_for_video_frames():
    task = LLMTask(maximum=1, input_dir="/input", output_dir="/output")
    task._describe = lambda path, metadata: {"route": "enhance", "item": (path, metadata)}
    common = {"_output_relative_path": "clip.frame.jpg", "_source_video_path": "/input/clip.mp4"}

    assert task.execute(("/output/clip.frame.jpg", {**common, "_stage": "metadata"})) is None
    assert task.execute(("/output/clip.frame.jpg", {**common, "_stage": "resize"})) is None
    assert task.execute(("/output/clip.frame.jpg", {**common, "_stage": "recognition"})) is None
    result = task.execute(("/output/clip.frame.jpg", {**common, "_stage": "transcript", "_transcript": "spoken words"}))

    assert result["item"][1]["_transcript"] == "spoken words"


def test_llm_status_can_render_while_dependency_events_arrive():
    task = LLMTask(maximum=1, input_dir="/input", output_dir="/output")
    errors = []
    started = threading.Event()

    def add_dependencies():
        started.set()
        for index in range(1_000):
            task.execute((
                f"/input/photo-{index}.jpg",
                {"_output_relative_path": f"photo-{index}.jpg", "_stage": "metadata"},
            ))

    worker = threading.Thread(target=add_dependencies)
    worker.start()
    assert started.wait(timeout=1)
    while worker.is_alive():
        try:
            task.format_status("LLM")
        except Exception as error:
            errors.append(error)
    worker.join()

    assert errors == []


def test_llm_recognition_context_excludes_unknown_and_unverified_people():
    recognition = {
        "people": [
            {"label": "PersonAlpha", "status": "matched", "match_confidence": 0.95},
            {"label": None, "status": "unknown", "cluster_id": "cluster-00001"},
            {"label": "Maybe", "status": "matched"},
        ]
    }

    assert LLMTask._recognition_for_llm(recognition) == {
        "people": [{"label": "PersonAlpha", "status": "matched", "match_confidence": 0.95}]
    }


def test_cached_llm_caption_still_routes_to_enhance(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    (output_dir / "photo.jpg.txt").write_text("Existing caption", encoding="utf-8")
    task = LLMTask(maximum=1, input_dir=str(input_dir), output_dir=str(output_dir))
    common = {"_output_relative_path": "photo.jpg"}

    assert task.execute((str(input_dir / "photo.jpg"), {**common, "_stage": "metadata"})) is None
    assert task.execute((str(input_dir / "photo.jpg"), {**common, "_stage": "resize", "_prepared_image_path": str(input_dir / "photo.jpg")})) is None
    result = task.execute((str(input_dir / "photo.jpg"), {**common, "_stage": "recognition", "_recognition": {}}))

    assert result["route"] == "enhance"
    assert task.total.skipped == 1
    assert "1S" in task.format_status("LLM")


def test_disabled_geolocation_completes_the_llm_dependency_without_network(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    image_path = input_dir / "photo.jpg"
    task = GeolocateEnrichedTask(
        maximum=1,
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        enabled=False,
    )

    result = task.execute((str(image_path), {"_output_relative_path": "photo.jpg"}))

    assert result == (str(image_path), {"_output_relative_path": "photo.jpg", "_geocode": "N/A", "_stage": "geolocation"})
    assert (output_dir / "photo.jpg.geocode.txt").read_text(encoding="utf-8") == "N/A\n"
    assert task.total.skipped == 1


def test_geolocation_failure_is_recorded_and_propagated(tmp_path, monkeypatch):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    image_path = input_dir / "photo.jpg"
    task = GeolocateEnrichedTask(maximum=1, input_dir=str(input_dir), output_dir=str(output_dir))

    monkeypatch.setattr("describe_media.tasks.geolocate_enriched.task.GeolocationTask.execute", lambda *_: (_ for _ in ()).throw(RuntimeError("offline")))
    with pytest.raises(RuntimeError, match="offline"):
        task.execute((str(image_path), {"_output_relative_path": "photo.jpg"}))

    assert not (output_dir / "photo.jpg.geocode.txt").exists()
    error = json.loads((output_dir / "photo.jpg.geocode.error.json").read_text(encoding="utf-8"))
    assert error["error"] == "offline"


def test_enhance_writes_missing_context_error_with_paths(tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    target = "target.jpg"
    (output_dir / f"{target}.txt").write_text("Base caption", encoding="utf-8")
    (output_dir / "neighbor.jpg.metadata.json").write_text(
        json.dumps({"source": {"relative_path": "neighbor.jpg"}, "metadata": {"datetime": "2026-08-08T10:00:00"}}),
        encoding="utf-8",
    )
    task = IntegratedEnhanceTask(
        maximum=1,
        input_dir=str(tmp_path),
        output_dir=str(output_dir),
        model_name=None,
        prompt="{original_description}\n{context_section}",
        backend_name=None,
        context_window_days=1,
        max_context_items=5,
    )

    with pytest.raises(RuntimeError, match="Enhancement context is incomplete"):
        task.execute((str(tmp_path / target), {"datetime": "2026-08-08T10:00:00"}))

    error = json.loads((output_dir / f"{target}.enhanced.error.json").read_text(encoding="utf-8"))
    assert error["reason"] == "missing_context_captions"
    assert error["missing"] == ["neighbor.jpg.txt"]
