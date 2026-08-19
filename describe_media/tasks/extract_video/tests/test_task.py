import json
from pathlib import Path

import pytest

from describe_media.tasks.extract_video.task import ExtractVideoTask
from describe_media.tasks.skip_check.task import SkipCheckTask


def _create_video(path: Path) -> None:
    cv2 = pytest.importorskip("cv2")
    numpy = pytest.importorskip("numpy")
    writer = cv2.VideoWriter(str(path), cv2.VideoWriter_fourcc(*"mp4v"), 5, (32, 24))
    assert writer.isOpened()
    for value in range(15):
        writer.write(numpy.full((24, 32, 3), value * 10, dtype=numpy.uint8))
    writer.release()


def test_extract_video_emits_stable_frame_paths_and_reuses_manifest(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    input_root.mkdir()
    video_path = input_root / "clip.mp4"
    _create_video(video_path)

    task = ExtractVideoTask(
        maximum=1,
        input_dir=str(input_root),
        output_dir=str(output_root),
        frame_interval_seconds=1,
        max_frames=3,
    )
    first = task.execute(str(video_path))
    second = task.execute(str(video_path))

    assert [Path(path).name for path, _ in first] == [
        "clip.mp4.frame-0001-t0000.000.jpg",
        "clip.mp4.frame-0002-t0001.400.jpg",
        "clip.mp4.frame-0003-t0002.800.jpg",
    ]
    assert [path for path, _ in second] == [path for path, _ in first]
    assert all(Path(path).is_file() for path, _ in first)
    assert task.total.skipped == 1

    manifest_path = output_root / "clip.mp4.frames.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["source"]["relative_path"] == "clip.mp4"
    assert len(manifest["frames"]) == 3


def test_extract_video_reports_a_missing_moov_atom_without_starting_ffmpeg(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    input_root.mkdir()
    video_path = input_root / "partial.mp4"
    video_path.write_bytes(b"\x00\x00\x00\x18ftypisom\x00\x00\x00\x00isomiso2")

    task = ExtractVideoTask(
        maximum=1,
        input_dir=str(input_root),
        output_dir=str(output_root),
        frame_interval_seconds=1,
        max_frames=3,
    )

    with pytest.raises(RuntimeError, match="missing moov atom"):
        task.execute(str(video_path))


def test_frame_skip_check_uses_the_logical_output_path(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    input_root.mkdir()
    frame_path = output_root / "clip.mp4.frame-0001-t0000.000.jpg"
    frame_path.parent.mkdir(parents=True)
    frame_path.write_bytes(b"frame")
    (output_root / "clip.mp4.frame-0001-t0000.000.jpg.txt").write_text("done", encoding="utf-8")

    task = SkipCheckTask(1, str(input_root), str(output_root), "{ext}.txt", retry_failed=False, retry=False)
    should_skip, returned = task.execute((str(frame_path), {"_output_relative_path": frame_path.name}))

    assert should_skip is True
    assert returned[0] == str(frame_path)
