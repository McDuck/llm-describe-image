import json
from pathlib import Path

from describe_media.tasks.describe_video.task import DescribeVideoTask


class FakeBackend:
    def __init__(self) -> None:
        self.request = None

    def respond(self, model, prompt, image_handle=None):
        self.request = {"model": model, "prompt": prompt, "image_handle": image_handle}
        return "A complete video description."


def test_describe_video_combines_all_frame_captions_and_whole_transcript(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    source = input_dir / "album" / "clip.mp4"
    frame_one = output_dir / "album" / "clip.mp4.frame-0001-t0000.000.jpg"
    frame_two = output_dir / "album" / "clip.mp4.frame-0002-t0005.000.jpg"
    source.parent.mkdir(parents=True)
    frame_one.parent.mkdir(parents=True)
    source.write_bytes(b"video")
    frame_one.write_bytes(b"frame")
    frame_two.write_bytes(b"frame")
    (Path(str(frame_one) + ".txt")).write_text("First frame description", encoding="utf-8")
    (Path(str(frame_two) + ".txt")).write_text("Second frame description", encoding="utf-8")
    (output_dir / "album" / "clip.mp4.frames.json").write_text(json.dumps({"frames": [
        {"path": str(frame_one), "timestamp_seconds": 0.0},
        {"path": str(frame_two), "timestamp_seconds": 5.0},
    ]}), encoding="utf-8")
    (output_dir / "album" / "clip.mp4.transcript.json").write_text(
        json.dumps({"complete": True, "text": "A speaker says hello."}), encoding="utf-8"
    )
    task = DescribeVideoTask(
        maximum=1, input_dir=str(input_dir), output_dir=str(output_dir), model_name="test-model",
        prompt="Describe the video.", backend_name=None, output_format="{content}",
    )
    backend = FakeBackend()
    task.backend = backend
    task.model = "test-model"

    output = task.execute((str(frame_one), {"_source_video_path": str(source)}))

    assert output == str(output_dir / "album" / "clip.mp4.txt")
    assert Path(output).read_text(encoding="utf-8") == "A complete video description."
    assert backend.request["image_handle"] is None
    assert "First frame description" in backend.request["prompt"]
    assert "Second frame description" in backend.request["prompt"]
    assert "A speaker says hello." in backend.request["prompt"]
