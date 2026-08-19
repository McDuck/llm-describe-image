import json
from pathlib import Path

from recognition_review.api.server import ReviewStore


def _review_record(root: Path, cluster: str, name: str) -> Path:
    directory = root / "recognition" / cluster / "2026" / "day"
    directory.mkdir(parents=True, exist_ok=True)
    record = directory / f"{name}.jpg.json"
    crop = directory / f"{name}.jpg.cut-out.jpg"
    shortcut = directory / f"{name}.jpg.lnk"
    crop.write_bytes(b"crop")
    shortcut.write_bytes(b"shortcut")
    record.write_text(
        json.dumps(
            {
                "kind": "recognition-review",
                "source": {"filename": f"{name}.jpg"},
                "cut_out": {"relative_path": crop.relative_to(root).as_posix()},
                "shortcut": {"relative_path": shortcut.relative_to(root).as_posix()},
            }
        ),
        encoding="utf-8",
    )
    return record


def test_review_store_moves_a_cluster_into_a_named_label(tmp_path: Path) -> None:
    _review_record(tmp_path, "cluster-00001", "one")
    _review_record(tmp_path, "cluster-00001", "two")
    store = ReviewStore(tmp_path / "recognition")

    assert store.move_cluster("cluster-00001", "Ada") == 2

    assert not (tmp_path / "recognition" / "cluster-00001").exists()
    assert len(list((tmp_path / "recognition" / "Ada").rglob("*.json"))) == 2
    assert store.list_labels() == ["Ada"]


def test_review_store_moves_one_item_to_a_dedicated_false_positive_cluster(tmp_path: Path) -> None:
    record = _review_record(tmp_path, "cluster-00001", "one")
    store = ReviewStore(tmp_path / "recognition")

    destination = store.move_item("cluster-00001", record.relative_to(record.parents[2]).as_posix(), false_positive=True)

    assert destination == "cluster-false-positive-00001"
    moved = tmp_path / "recognition" / destination / "2026" / "day" / "one.jpg.json"
    assert moved.is_file()
    assert json.loads(moved.read_text(encoding="utf-8"))["review_status"] == "false_positive"
    assert store.list_clusters()[0]["false_positive"] is True


def test_review_store_merges_a_cluster_into_an_existing_label_without_a_nested_cluster(tmp_path: Path) -> None:
    _review_record(tmp_path, "cluster-00001", "one")
    (tmp_path / "recognition" / "Ada").mkdir(parents=True)
    store = ReviewStore(tmp_path / "recognition")

    assert store.move_cluster("cluster-00001", "Ada") == 1

    assert (tmp_path / "recognition" / "Ada" / "2026" / "day" / "one.jpg.json").is_file()
    assert not (tmp_path / "recognition" / "Ada" / "cluster-00001").exists()


def test_review_store_keeps_existing_label_artifacts_when_a_cluster_has_the_same_path(tmp_path: Path) -> None:
    _review_record(tmp_path, "cluster-00001", "one")
    _review_record(tmp_path, "Ada", "one")
    existing_crop = tmp_path / "recognition" / "Ada" / "2026" / "day" / "one.jpg.cut-out.jpg"
    existing_crop.write_bytes(b"curated")
    store = ReviewStore(tmp_path / "recognition")

    assert store.move_cluster("cluster-00001", "Ada") == 1

    assert existing_crop.read_bytes() == b"curated"
    assert not (tmp_path / "recognition" / "cluster-00001").exists()
