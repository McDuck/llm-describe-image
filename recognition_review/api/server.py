"""Local web reviewer for provisional face-recognition clusters.

The review data remains folder based. This service only moves the existing
JSON/crop/shortcut artifact sets, so the normal trainer needs no database.
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import shutil
import tempfile
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlparse

CLUSTER_PREFIX = "cluster-"
FALSE_POSITIVE_PREFIX = "cluster-false-positive-"
LABEL_PATTERN = re.compile(r"^[^\\/:*?\"<>|][^\\/:*?\"<>|]*$")


class ReviewError(ValueError):
    """A request could not be safely applied to the review folders."""


def write_json_atomically(path: Path, payload: Dict[str, Any]) -> None:
    """Persist review metadata without requiring the recognition ML runtime."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


class ReviewStore:
    def __init__(self, recognition_root: Path) -> None:
        self.root = recognition_root.resolve()

    def list_labels(self) -> List[str]:
        if not self.root.is_dir():
            return []
        return sorted(entry.name for entry in self.root.iterdir() if entry.is_dir() and not entry.name.startswith(CLUSTER_PREFIX) and not entry.name.startswith("."))

    def list_clusters(self) -> List[Dict[str, Any]]:
        if not self.root.is_dir():
            return []
        clusters = []
        for directory in self.root.iterdir():
            if not directory.is_dir() or not directory.name.startswith(CLUSTER_PREFIX):
                continue
            items = list(self._items(directory))
            if items:
                clusters.append({"id": directory.name, "false_positive": directory.name.startswith(FALSE_POSITIVE_PREFIX), "items": items, "count": len(items)})
        return sorted(clusters, key=lambda cluster: (-cluster["count"], cluster["id"]))

    def move_cluster(self, cluster_id: str, label: str) -> int:
        directory = self._cluster_directory(cluster_id)
        records = [item["record_path"] for item in self._items(directory)]
        if not records:
            raise ReviewError("Cluster has no review records")
        destination_name = self._validate_label(label)
        destination = self.root / destination_name
        # A reviewed cluster becomes part of the label, never a nested
        # ``label/cluster-*`` directory. Moving whole unused year trees keeps
        # large cluster labelling fast while preserving every review artifact.
        if not destination.exists():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(directory), str(destination))
        elif not destination.is_dir():
            raise ReviewError(f"Label destination is not a directory: {destination_name}")
        else:
            self._assert_directory_mergeable(directory, destination)
            self._merge_directory(directory, destination)
            self._remove_empty_directories(directory)
        return len(records)

    def mark_cluster_false_positive(self, cluster_id: str) -> int:
        directory = self._cluster_directory(cluster_id)
        records = [item["record_path"] for item in self._items(directory)]
        if not records:
            raise ReviewError("Cluster has no review records")
        for record_path in records:
            self.move_item(cluster_id, record_path, false_positive=True)
        self._remove_empty_directories(directory)
        return len(records)

    def move_item(self, cluster_id: str, record_path: str, label: Optional[str] = None, false_positive: bool = False) -> str:
        if bool(label) == bool(false_positive):
            raise ReviewError("Choose either a label or false positive")
        source_cluster = self._cluster_directory(cluster_id)
        source_record, payload = self._record_at(source_cluster, record_path)
        destination_name = self._next_false_positive_cluster() if false_positive else self._validate_label(label or "")
        destination = self.root / destination_name
        target_record = destination / source_record.relative_to(source_cluster)
        for source in self._artifact_paths(source_record, payload):
            self._move_file(source, destination / source.relative_to(source_cluster))
        self._update_artifact_references(payload, target_record, destination_name, false_positive)
        write_json_atomically(target_record, payload)
        self._remove_empty_directories(source_cluster)
        return destination_name

    def _items(self, directory: Path) -> Iterable[Dict[str, Any]]:
        for record_path in sorted(directory.rglob("*.json")):
            payload = self._read_record(record_path)
            if payload is None:
                continue
            yield {"record_path": record_path.relative_to(directory).as_posix(), "source": payload.get("source", {}), "detection_confidence": payload.get("detection_confidence"), "crop_url": self._media_url(payload.get("cut_out", {}).get("relative_path"))}

    def _record_at(self, cluster: Path, record_path: str) -> Tuple[Path, Dict[str, Any]]:
        candidate = (cluster / Path(record_path)).resolve()
        if cluster not in candidate.parents or candidate.suffix != ".json":
            raise ReviewError("Invalid review record")
        payload = self._read_record(candidate)
        if payload is None:
            raise ReviewError("Review record was not found")
        return candidate, payload

    def _cluster_directory(self, cluster_id: str) -> Path:
        if not cluster_id.startswith(CLUSTER_PREFIX) or Path(cluster_id).name != cluster_id:
            raise ReviewError("Invalid cluster")
        directory = (self.root / cluster_id).resolve()
        if self.root not in directory.parents or not directory.is_dir():
            raise ReviewError("Cluster was not found")
        return directory

    def _artifact_paths(self, record_path: Path, payload: Dict[str, Any]) -> List[Path]:
        paths = [record_path]
        for key in ("cut_out", "shortcut"):
            relative = payload.get(key, {}).get("relative_path")
            if isinstance(relative, str):
                artifact = (self.root.parent / Path(relative)).resolve()
                if artifact.is_file() and record_path.parent in artifact.parents:
                    paths.append(artifact)
        return paths

    @staticmethod
    def _move_file(source: Path, target: Path) -> None:
        if source == target:
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise ReviewError(f"Destination already contains {target.name}")
        shutil.move(str(source), str(target))

    @classmethod
    def _assert_directory_mergeable(cls, source: Path, destination: Path) -> None:
        """Ensure directory/file shape conflicts are found before moving files."""
        for entry in source.iterdir():
            target = destination / entry.name
            if entry.is_dir():
                if target.exists() and not target.is_dir():
                    raise ReviewError(f"Label already contains a file at {entry.relative_to(source)}")
                if target.is_dir():
                    cls._assert_directory_mergeable(entry, target)
            elif target.exists() and not target.is_file():
                raise ReviewError(f"Label already contains a directory at {entry.relative_to(source)}")

    @classmethod
    def _merge_directory(cls, source: Path, destination: Path) -> None:
        for entry in source.iterdir():
            target = destination / entry.name
            if entry.is_dir() and target.is_dir():
                cls._merge_directory(entry, target)
                entry.rmdir()
            elif target.exists():
                # The label's existing artifact is authoritative; discard the
                # incoming duplicate rather than overwriting reviewed data.
                entry.unlink()
            else:
                shutil.move(str(entry), str(target))

    def _update_artifact_references(self, payload: Dict[str, Any], record_path: Path, destination_name: str, false_positive: bool) -> None:
        relative_parent = record_path.relative_to(self.root / destination_name).parent
        for key in ("cut_out", "shortcut"):
            relative = payload.get(key, {}).get("relative_path")
            if isinstance(relative, str):
                payload[key]["relative_path"] = (Path("recognition") / destination_name / relative_parent / Path(relative).name).as_posix()
        if false_positive:
            payload["review_status"] = "false_positive"
        else:
            payload.pop("review_status", None)

    def _next_false_positive_cluster(self) -> str:
        self.root.mkdir(parents=True, exist_ok=True)
        numbers = [int(match.group(1)) for entry in self.root.iterdir() for match in [re.fullmatch(r"cluster-false-positive-(\d+)", entry.name)] if match]
        return f"{FALSE_POSITIVE_PREFIX}{max(numbers, default=0) + 1:05d}"

    @staticmethod
    def _validate_label(label: str) -> str:
        label = label.strip()
        if not LABEL_PATTERN.fullmatch(label) or label.startswith(CLUSTER_PREFIX) or label.startswith("."):
            raise ReviewError("Enter a valid label that does not start with 'cluster-'")
        return label

    @staticmethod
    def _read_record(path: Path) -> Optional[Dict[str, Any]]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            return None
        return payload if payload.get("kind") == "recognition-review" else None

    @staticmethod
    def _remove_empty_directories(directory: Path) -> None:
        for candidate in sorted(directory.rglob("*"), reverse=True):
            if candidate.is_dir():
                try:
                    candidate.rmdir()
                except OSError:
                    pass
        try:
            directory.rmdir()
        except OSError:
            pass

    @staticmethod
    def _media_url(relative_path: Any) -> Optional[str]:
        if not isinstance(relative_path, str):
            return None
        return "/media/" + "/".join(part for part in Path(relative_path).parts if part not in {".", ".."})


class ReviewHandler(BaseHTTPRequestHandler):
    store: ReviewStore

    def do_GET(self) -> None:  # noqa: N802
        path = unquote(urlparse(self.path).path)
        if path == "/api/review":
            self._json({"clusters": self.store.list_clusters(), "labels": self.store.list_labels()})
        elif path.startswith("/media/"):
            self._file(self.store.root.parent / path.removeprefix("/media/"))
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        try:
            payload = self._body()
            if self.path == "/api/clusters/label":
                moved = self.store.move_cluster(payload["cluster_id"], payload["label"])
                message = f"Moved {moved} images to {payload['label']}."
            elif self.path == "/api/clusters/false-positive":
                moved = self.store.mark_cluster_false_positive(payload["cluster_id"])
                message = f"Split {moved} images into false-positive clusters."
            elif self.path == "/api/items/label":
                label = self.store.move_item(payload["cluster_id"], payload["record_path"], label=payload["label"])
                message = f"Moved image to {label}."
            elif self.path == "/api/items/false-positive":
                label = self.store.move_item(payload["cluster_id"], payload["record_path"], false_positive=True)
                message = f"Moved image to {label}."
            else:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            self._json({"message": message})
        except (KeyError, TypeError, ValueError, OSError, ReviewError) as error:
            self._json({"error": str(error)}, HTTPStatus.BAD_REQUEST)

    def _body(self) -> Dict[str, Any]:
        return json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8"))

    def _file(self, requested: Path) -> None:
        try:
            path = requested.resolve()
            if self.store.root.parent not in path.parents or not path.is_file():
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            data = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", mimetypes.guess_type(str(path))[0] or "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND)

    def _json(self, payload: Dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            self.wfile.write(data)
        except BrokenPipeError:
            # A proxy/client may disconnect after a slow mutation; the completed
            # filesystem move remains valid and must not crash the request thread.
            pass

    def log_message(self, format: str, *args: object) -> None:
        print(f"review-ui: {format % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the local recognition review UI")
    parser.add_argument("--root", default=os.getenv("RECOGNITION_ROOT", "/data/output/recognition"))
    parser.add_argument("--host", default=os.getenv("REVIEW_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("REVIEW_PORT", "8080")))
    args = parser.parse_args()
    ReviewHandler.store = ReviewStore(Path(args.root))
    print(f"Recognition review UI: http://localhost:{args.port}")
    ThreadingHTTPServer((args.host, args.port), ReviewHandler).serve_forever()


if __name__ == "__main__":
    main()
