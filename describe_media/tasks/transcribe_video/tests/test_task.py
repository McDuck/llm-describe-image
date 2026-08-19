import json
from pathlib import Path

from describe_media.tasks.transcribe_video.task import TranscribeVideoTask


def _task(input_dir: Path, output_dir: Path) -> TranscribeVideoTask:
    return TranscribeVideoTask(
        maximum=1,
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        enabled=True,
        backend_name="faster-whisper",
        model_name="test-transcriber",
        language="nl",
        remote_api_base="http://gpu.example.test/v1",
        remote_api_token="test-token",
    )


def test_transcribe_video_writes_frame_adjacent_cache_and_reuses_it(tmp_path: Path, monkeypatch) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    source = input_dir / "album" / "clip.mp4"
    frame = output_dir / "album" / "clip.mp4.frame-0002-t0005.000.jpg"
    source.parent.mkdir(parents=True)
    frame.parent.mkdir(parents=True)
    source.write_bytes(b"source-video")
    frame.write_bytes(b"frame")
    (output_dir / "album" / "clip.mp4.frames.json").write_text(json.dumps({
        "source": {"relative_path": "album/clip.mp4"},
        "frames": [{
            "number": 2,
            "timestamp_seconds": 5.0,
            "audio_start_seconds": 2.5,
            "audio_end_seconds": 7.5,
            "path": str(frame),
        }],
    }), encoding="utf-8")
    task = _task(input_dir, output_dir)
    monkeypatch.setattr(task, "_extract_audio", lambda *_: str(tmp_path / "audio.m4a"))
    (tmp_path / "audio.m4a").write_bytes(b"audio")
    monkeypatch.setattr(task, "_transcribe", lambda _: "Hallo wereld")
    item = (str(frame), {
        "_output_relative_path": "album/clip.mp4.frame-0002-t0005.000.jpg",
        "_source_video_path": str(source),
        "_frame_audio_start_seconds": 2.5,
        "_frame_audio_end_seconds": 7.5,
    })

    result = task.execute(item)

    path = output_dir / "album" / "clip.mp4.frame-0002-t0005.000.jpg.transcript.json"
    assert result[1]["_stage"] == "transcript"
    assert result[1]["_transcript"] == "Hallo wereld"
    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["interval_seconds"] == {"start": 2.5, "end": 7.5}
    assert record["transcription"]["text"] == "Hallo wereld"
    whole_video = json.loads((output_dir / "album" / "clip.mp4.transcript.json").read_text(encoding="utf-8"))
    assert whole_video["complete"] is True
    assert whole_video["text"] == "Hallo wereld"
    assert (output_dir / "album" / "clip.mp4.transcript.vtt").read_text(encoding="utf-8") == (
        "WEBVTT\n\n00:00:02.500 --> 00:00:07.500\nHallo wereld\n"
    )

    monkeypatch.setattr(task, "_extract_audio", lambda *_: (_ for _ in ()).throw(AssertionError("cache miss")))
    assert task.execute(item)[1]["_transcript"] == "Hallo wereld"


def test_disabled_transcription_still_unblocks_video_caption(tmp_path: Path) -> None:
    task = TranscribeVideoTask(
        maximum=1, input_dir=str(tmp_path), output_dir=str(tmp_path / "output"), enabled=False,
        backend_name="faster-whisper", model_name="unused", language="", remote_api_base=None, remote_api_token=None,
    )

    result = task.execute((str(tmp_path / "frame.jpg"), {"_source_video_path": "clip.mp4"}))

    assert result[1] == {"_source_video_path": "clip.mp4", "_transcript": "", "_stage": "transcript"}
