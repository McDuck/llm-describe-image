from describe_media.pipelines.pipeline import Pipeline
from describe_media.pipelines.describe.pipeline import DescribePipeline
from describe_media.tasks.task import Task


class _SchedulerPipeline(Pipeline):
    PIPELINE_CONFIG = [
        {"name": "InputA", "task": "InputA", "next_task": "Output"},
        {"name": "InputB", "task": "InputB", "next_task": "Output"},
        {"name": "Output", "task": "Output", "next_task": None},
    ]

    def __init__(self):
        super().__init__("scheduler-test", "test scheduler")
        for name in ("InputA", "InputB", "Output"):
            self.add_task(name, Task(maximum=1))


def test_serial_scheduler_prefers_downstream_work():
    pipeline = _SchedulerPipeline()
    configs = pipeline.PIPELINE_CONFIG
    distances = pipeline._task_distances_to_output(configs)
    pipeline.get_task("InputA").add("upstream")
    pipeline.get_task("Output").add("downstream")

    config, item = pipeline._next_serial_item(configs, distances)

    assert (config["name"], item) == ("Output", "downstream")


def test_serial_scheduler_round_robins_equal_depth_inputs():
    pipeline = _SchedulerPipeline()
    configs = pipeline.PIPELINE_CONFIG
    distances = pipeline._task_distances_to_output(configs)
    input_a = pipeline.get_task("InputA")
    input_b = pipeline.get_task("InputB")
    input_a.add("a")
    input_b.add("b")

    first_config, first_item = pipeline._next_serial_item(configs, distances)
    pipeline.get_task(first_config["task"]).finish(first_item)
    second_config, second_item = pipeline._next_serial_item(configs, distances)

    assert (first_config["name"], first_item) == ("InputA", "a")
    assert (second_config["name"], second_item) == ("InputB", "b")


def test_explicit_priority_prevents_a_terminal_side_branch_from_elevating_a_router():
    pipeline = Pipeline("priority-test", "test explicit priorities")
    configs = [
        {"name": "Router", "task": "Router", "route_targets": ["Shortcut", "Required"], "priority": 3},
        {"name": "Required", "task": "Required", "next_task": "Output", "priority": 2},
        {"name": "Output", "task": "Output", "next_task": None, "priority": 1},
        {"name": "Shortcut", "task": "Shortcut", "next_task": None, "priority": 0},
    ]

    assert pipeline._task_distances_to_output(configs) == {
        "Router": 3,
        "Required": 2,
        "Output": 1,
        "Shortcut": 0,
    }


def test_describe_priority_groups_llm_inputs_before_the_image_router():
    pipeline = DescribePipeline()
    priorities = pipeline._task_distances_to_output(pipeline.PIPELINE_CONFIG)

    assert priorities["LLM"] == 1
    assert {priorities[name] for name in ("Resize", "Metadata", "Geolocate", "Recognition")} == {2}
    assert priorities["ImageRouter"] == 3


def test_single_task_setting_creates_only_the_scheduler_thread():
    pipeline = _SchedulerPipeline()
    pipeline.stop_event.set()

    workers = pipeline._create_worker_threads()
    for worker in workers:
        worker.join(timeout=1)

    assert [worker.name for worker in workers] == ["Scheduler"]
