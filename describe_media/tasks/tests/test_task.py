from describe_media.tasks.task import TaskStats


def test_task_stats_formats_skipped_work_separately() -> None:
    stats = TaskStats()

    stats.skip()

    assert stats.format() == "1S0D"
    assert stats.processed == 0
    stats.reset()
    assert stats.format() == "0D"

    stats.finish()
    stats.finish()
    stats.skip()
    assert stats.processed == 1
