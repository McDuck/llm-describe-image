from describe_media.tasks.task import TaskStats


def test_task_stats_formats_skipped_work_separately() -> None:
    stats = TaskStats()

    stats.skip()

    assert stats.format() == "1S0D"
    stats.reset()
    assert stats.format() == "0D"
