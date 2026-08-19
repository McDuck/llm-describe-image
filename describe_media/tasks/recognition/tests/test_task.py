import json
from pathlib import Path

from PIL import Image

from describe_media.pipelines.recognition_cluster import RecognitionClusterPipeline
from describe_media.recognition.gpus.base import FaceDetection
from describe_media.recognition.index import RecognitionIndex, train_recognition_index
from recognition_review.api.server import ReviewStore
from describe_media.tasks.recognition.task import RecognitionPreparationTask, RecognitionTask


class FakeFaceBackend:
    backend_name = "fake"
    model_name = "fake-model"

    def load(self) -> None:
        return None

    def detect(self, image_path: str):
        return [
            FaceDetection(
                bbox={"x": 1, "y": 2, "width": 10, "height": 12},
                confidence=0.99,
                embedding=[1.0, 0.0, 0.0],
                image_size=(20, 20),
            )
        ]


class UnknownFaceBackend(FakeFaceBackend):
    def detect(self, image_path: str):
        return [
            FaceDetection(
                bbox={"x": 1, "y": 2, "width": 10, "height": 12},
                confidence=0.99,
                embedding=[0.0, 1.0, 0.0],
                image_size=(20, 20),
            )
        ]


class RecordingFaceBackend(FakeFaceBackend):
    def __init__(self) -> None:
        self.detected_paths = []

    def detect(self, image_path: str):
        self.detected_paths.append(image_path)
        return super().detect(image_path)


def _create_source_image(root: Path) -> str:
    relative = "2026/2026-07/2026-07-27/IMG_001.jpg"
    path = root / relative
    path.parent.mkdir(parents=True)
    Image.new("RGB", (20, 20), "white").save(path)
    return relative


def _write_review_record(output_root: Path, identity: str, relative_path: str) -> None:
    path = output_root / "recognition" / identity / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    record_path = path.with_name(path.name + ".json")
    record_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "recognition-review",
                "source": {"relative_path": relative_path},
                "face_bbox": {"x": 1, "y": 2, "width": 10, "height": 12},
            }
        ),
        encoding="utf-8",
    )


def test_training_uses_renamed_folders_and_ignores_provisional_clusters(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    relative_path = _create_source_image(input_root)
    _write_review_record(output_root, "john", relative_path)
    _write_review_record(output_root, "cluster-00001", relative_path)

    index_path = train_recognition_index(
        str(input_root),
        str(output_root),
        backend=FakeFaceBackend(),
        match_threshold=0.9,
    )

    index = RecognitionIndex.load(index_path)
    assert set(index.identities) == {"john"}
    assert index.match([1.0, 0.0, 0.0]).identity_id == "john"
    assert index.match([0.0, 1.0, 0.0]) is None
    assert (output_root / "recognition" / "john" / "model" / "identity.json").is_file()


def test_describe_recognition_writes_an_empty_result_without_a_trained_index(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    relative_path = _create_source_image(input_root)
    source_path = input_root / relative_path
    task = RecognitionTask(1, str(input_root), str(output_root))

    task.load()
    _, metadata = task.execute((str(source_path), {}))

    assert metadata["_recognition"]["people"] == []
    result_path = output_root / f"{relative_path}.cut-out.json"
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    assert payload["index_available"] is False
    assert payload["recognition_input"] == "original"


def test_describe_recognition_writes_named_crop_artifacts_and_manifest(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    relative_path = _create_source_image(input_root)
    source_path = input_root / relative_path
    task = RecognitionTask(1, str(input_root), str(output_root))
    task.index = RecognitionIndex({"PersonAlpha": [1.0, 0.0, 0.0]}, {"match_threshold": 0.9})
    task.backend = FakeFaceBackend()

    _, metadata = task.execute((str(source_path), {}))

    person = metadata["_recognition"]["people"][0]
    assert person["label"] == "PersonAlpha"
    assert person["status"] == "matched"
    output_directory = output_root / Path(relative_path).parent
    crop_path = output_directory / "IMG_001.jpg.cut-out.PersonAlpha.jpg"
    crop_json_path = output_directory / "IMG_001.jpg.cut-out.PersonAlpha.jpg.json"
    manifest_path = output_directory / "IMG_001.jpg.cut-out.json"
    assert crop_path.is_file()
    assert crop_json_path.is_file()
    assert not (output_directory / "IMG_001.jpg.cut-out.PersonAlpha.lnk").exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["people"][0]["crop"] == crop_path.name


def test_describe_recognition_copies_matches_to_a_provisional_review_cluster(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    relative_path = _create_source_image(input_root)
    source_path = input_root / relative_path
    task = RecognitionTask(1, str(input_root), str(output_root))
    task.index = RecognitionIndex({"PersonAlpha": [1.0, 0.0, 0.0]}, {"match_threshold": 0.9})
    task.backend = FakeFaceBackend()

    task.execute((str(source_path), {}))

    cluster_dir = output_root / "recognition" / "cluster-00001" / Path(relative_path).parent
    copied_crop = cluster_dir / "IMG_001.jpg.cut-out.PersonAlpha.jpg"
    record = json.loads((cluster_dir / "IMG_001.jpg.cut-out.PersonAlpha.jpg.json").read_text(encoding="utf-8"))
    assert copied_crop.is_file()
    assert record["kind"] == "recognition-review"
    assert record["suggested_identity"] == "PersonAlpha"
    assert record["source"]["relative_path"] == relative_path


def test_cached_describe_matches_are_backfilled_into_the_review_cluster(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    relative_path = _create_source_image(input_root)
    source_path = input_root / relative_path
    index = RecognitionIndex({"PersonAlpha": [1.0, 0.0, 0.0]}, {"match_threshold": 0.9})
    original = RecognitionTask(1, str(input_root), str(output_root), copy_matches_to_review_clusters=False)
    original.index = index
    original.backend = FakeFaceBackend()
    original.execute((str(source_path), {}))

    rerun = RecognitionTask(1, str(input_root), str(output_root))
    rerun.index = index
    rerun.backend = FakeFaceBackend()
    rerun.execute((str(source_path), {}))

    copied_crop = output_root / "recognition" / "cluster-00001" / Path(relative_path).parent / "IMG_001.jpg.cut-out.PersonAlpha.jpg"
    assert copied_crop.is_file()


def test_training_excludes_describe_matches_until_they_are_reviewed(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    relative_path = _create_source_image(input_root)
    source_path = input_root / relative_path
    task = RecognitionTask(1, str(input_root), str(output_root))
    task.index = RecognitionIndex({"PersonAlpha": [1.0, 0.0, 0.0]}, {"match_threshold": 0.9})
    task.backend = FakeFaceBackend()
    task.execute((str(source_path), {}))

    index = RecognitionIndex.load(train_recognition_index(str(input_root), str(output_root), backend=FakeFaceBackend()))

    copied_crop = output_root / "recognition" / "cluster-00001" / Path(relative_path).parent / "IMG_001.jpg.cut-out.PersonAlpha.jpg"
    assert copied_crop.is_file()
    assert index.metadata["sample_counts"] == {}


def test_training_can_reuse_a_matched_video_frame_from_the_output_tree(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    frame_relative = "2026/clip.mp4.frame-0001-t0000.000.jpg"
    frame_path = output_root / frame_relative
    frame_path.parent.mkdir(parents=True)
    Image.new("RGB", (20, 20), "white").save(frame_path)
    task = RecognitionTask(1, str(input_root), str(output_root))
    task.index = RecognitionIndex({"PersonAlpha": [1.0, 0.0, 0.0]}, {"match_threshold": 0.9})
    task.backend = FakeFaceBackend()

    task.execute((str(frame_path), {
        "_output_relative_path": frame_relative,
        "_source_video_path": str(input_root / "2026" / "clip.mp4"),
        "_frame_number": 1,
        "_frame_timestamp_seconds": 0.0,
    }))
    ReviewStore(output_root / "recognition").move_cluster("cluster-00001", "PersonAlpha")
    index_path = train_recognition_index(str(input_root), str(output_root), backend=FakeFaceBackend())
    index = RecognitionIndex.load(index_path)
    record_path = output_root / "recognition" / "PersonAlpha" / "2026" / "clip.mp4.frame-0001-t0000.000.jpg.cut-out.PersonAlpha.jpg.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))

    assert index.metadata["sample_counts"] == {"PersonAlpha": 1}
    assert record["source"]["video_relative_path"] == "2026/clip.mp4"
    assert record["source"]["frame_timestamp_seconds"] == 0.0


def test_describe_recognition_uses_original_not_resized_image(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    relative_path = _create_source_image(input_root)
    source_path = input_root / relative_path
    resized_path = output_root / "2026/2026-07/2026-07-27/IMG_001.resized.jpg"
    resized_path.parent.mkdir(parents=True)
    Image.new("RGB", (10, 10), "white").save(resized_path)
    backend = RecordingFaceBackend()
    task = RecognitionTask(1, str(input_root), str(output_root))
    task.index = RecognitionIndex({"PersonAlpha": [1.0, 0.0, 0.0]}, {"match_threshold": 0.9})
    task.backend = backend

    task.execute((str(source_path), {"_prepared_image_path": str(resized_path)}))

    assert backend.detected_paths == [str(source_path)]


def test_describe_recognition_writes_unknown_cluster_crop_artifacts(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    relative_path = _create_source_image(input_root)
    source_path = input_root / relative_path
    task = RecognitionTask(1, str(input_root), str(output_root))
    task.index = RecognitionIndex({"PersonAlpha": [1.0, 0.0, 0.0]}, {"match_threshold": 0.9})
    task.backend = UnknownFaceBackend()

    _, metadata = task.execute((str(source_path), {}))

    person = metadata["_recognition"]["people"][0]
    output_directory = output_root / Path(relative_path).parent
    crop_path = output_directory / "IMG_001.jpg.cut-out.cluster-00001.jpg"
    assert person["status"] == "unknown"
    assert person["label"] is None
    assert person["crop"] == crop_path.name
    assert crop_path.is_file()
    assert (output_directory / "IMG_001.jpg.cut-out.cluster-00001.jpg.json").is_file()
    assert not (output_directory / "IMG_001.jpg.cut-out.cluster-00001.lnk").exists()


def test_new_preparation_excludes_clusters_that_were_renamed_by_the_reviewer(tmp_path: Path) -> None:
    output_root = tmp_path / "output"
    recognition_root = output_root / "recognition"
    (recognition_root / "PersonBeta").mkdir(parents=True)
    (recognition_root / "cluster-00002").mkdir(parents=True)
    state_path = recognition_root / ".state" / "provisional-clusters.json"
    state_path.parent.mkdir()
    state_path.write_text(
        json.dumps(
            {
                "clusters": {
                    "cluster-00001": {"centroid": [1.0, 0.0], "sample_count": 2},
                    "cluster-00002": {"centroid": [0.0, 1.0], "sample_count": 1},
                }
            }
        ),
        encoding="utf-8",
    )
    task = RecognitionPreparationTask(1, str(tmp_path / "input"), str(output_root))

    task._clusters = task._load_clusters()

    assert set(task._clusters) == {"cluster-00002"}
    assert task._next_cluster_id() == "cluster-00003"


def test_manifest_accepts_unique_source_relative_image_paths(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    relative_path = _create_source_image(input_root)
    manifest = tmp_path / "sample.txt"
    manifest.write_text(f"# random batch\n{relative_path}\n{relative_path}\n", encoding="utf-8")
    pipeline = RecognitionClusterPipeline()
    pipeline.input_dir = str(input_root)

    paths = pipeline._load_manifest(str(manifest))

    assert paths == [str(input_root / relative_path)]


def test_random_sampling_returns_distinct_unreviewed_source_images(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    for image_name in ("one.jpg", "two.jpg", "three.jpg"):
        path = input_root / "2018" / "2018-06" / "2018-06-21" / image_name
        path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (20, 20), "white").save(path)
    pipeline = RecognitionClusterPipeline()
    pipeline.input_dir = str(input_root)
    pipeline.output_dir = str(tmp_path / "output")

    paths = pipeline._select_random_images(3)

    assert len(paths) == 3
    assert len(set(paths)) == 3
