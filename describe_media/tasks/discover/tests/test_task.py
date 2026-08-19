from describe_media.tasks.discover.task import DiscoverTask


def test_discover_status_separates_directories_images_and_videos(tmp_path):
    (tmp_path / "nested").mkdir()
    (tmp_path / "photo.jpg").touch()
    (tmp_path / "clip.mp4").touch()

    task = DiscoverTask(
        maximum=1,
        input_dir=str(tmp_path),
        image_extensions={".jpg"},
        video_extensions={".mp4"},
    )

    files, directories = task.execute(str(tmp_path))
    task.finish(str(tmp_path), len(files) + len(directories))

    assert task.format_status("Discover") == "Discover: 0Q 0S 0A 1P 0F ->1d1i1v"
