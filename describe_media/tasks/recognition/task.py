"""Tasks for producing review data and matching curated local identities."""

from __future__ import annotations

import json
import os
import re
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from PIL import Image, ImageOps

from describe_media.recognition.gpus.api.backend import RemoteRecognitionBackend
from describe_media.recognition.gpus.base import FaceDetection, RecognitionBackend, cosine_similarity, normalise_embedding
from describe_media.recognition.gpus.direct.backend import InsightFaceBackend
from describe_media.recognition.index import INDEX_FILENAME, RecognitionIndex, SCHEMA_VERSION, write_json_atomically
from describe_media.tasks.media import output_relative_path
from describe_media.tasks.task import Task


class RecognitionPreparationTask(Task[Tuple[str, Dict[str, Any]], str]):
    """Detect faces, group them provisionally, and create human-review files."""

    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str,
        model_name: str = "buffalo_l",
        providers: Optional[List[str]] = None,
        detection_threshold: float = 0.60,
        cluster_threshold: float = 0.45,
        retry: bool = False,
        remote_api_base: Optional[str] = None,
        remote_api_token: Optional[str] = None,
        remote_timeout_seconds: float = 120.0,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir = Path(output_dir)
        self.recognition_root = self.output_dir / "recognition"
        self.model_name = model_name
        self.providers = providers
        self.detection_threshold = float(detection_threshold)
        self.cluster_threshold = float(cluster_threshold)
        self.retry = retry
        self.remote_api_base = remote_api_base
        self.remote_api_token = remote_api_token
        self.remote_timeout_seconds = float(remote_timeout_seconds)
        self.backend = (
            RemoteRecognitionBackend(
                remote_api_base,
                remote_api_token or "",
                self.remote_timeout_seconds,
            )
            if remote_api_base
            else InsightFaceBackend(model_name=model_name, providers=providers)
        )
        self._clusters: Dict[str, Dict[str, Any]] = {}
        self._next_cluster_number = 1
        self._processed_sources: Set[str] = set()
        self._cluster_lock = threading.Lock()

    def load(self) -> None:
        self.backend.load()
        self._clusters = self._load_clusters()
        self._processed_sources = self._load_existing_sources()

    def execute(self, item: Tuple[str, Dict[str, Any]]) -> str:
        input_path, metadata = item
        if not isinstance(metadata, dict):
            raise metadata
        relative_path = self._relative_source_path(input_path)
        # A reviewed record is authoritative. Never recreate it merely because a
        # preparation command is repeated; the reviewer can explicitly remove
        # the complete source's review records to request fresh preparation.
        if relative_path in self._processed_sources:
            return input_path

        detections = [
            detection
            for detection in self.backend.detect(input_path)
            if detection.confidence >= self.detection_threshold
        ]
        for detection_number, detection in enumerate(detections, start=1):
            cluster_id = self._assign_cluster(detection.embedding)
            self._write_review_artifacts(
                input_path=input_path,
                relative_path=relative_path,
                metadata=metadata,
                detection=detection,
                detection_number=detection_number,
                cluster_id=cluster_id,
            )
        self._processed_sources.add(relative_path)
        return input_path

    def _relative_source_path(self, input_path: str) -> str:
        return Path(os.path.relpath(input_path, self.input_dir or input_path)).as_posix()

    def _load_existing_sources(self) -> Set[str]:
        if not self.recognition_root.exists():
            return set()
        sources: Set[str] = set()
        for path in self.recognition_root.rglob("*.json"):
            if path.name == INDEX_FILENAME:
                continue
            try:
                with path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                if payload.get("kind") == "recognition-review":
                    relative_path = payload.get("source", {}).get("relative_path")
                    if isinstance(relative_path, str):
                        sources.add(relative_path)
            except (OSError, ValueError, json.JSONDecodeError):
                continue
        return sources

    def _cluster_state_path(self) -> Path:
        return self.recognition_root / ".state" / "provisional-clusters.json"

    def _load_clusters(self) -> Dict[str, Dict[str, Any]]:
        path = self._cluster_state_path()
        if not path.exists():
            return {}
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            clusters = payload.get("clusters", {})
            if not isinstance(clusters, dict):
                return {}
            known_numbers = [
                int(match.group(1))
                for cluster_id in clusters
                for match in [re.fullmatch(r"cluster-(\d+)", cluster_id)]
                if match
            ]
            self._next_cluster_number = max(
                int(payload.get("next_cluster_number", 1)),
                max(known_numbers, default=0) + 1,
            )
            return {
                cluster_id: {
                    "centroid": normalise_embedding(cluster["centroid"]),
                    "sample_count": int(cluster.get("sample_count", 1)),
                }
                for cluster_id, cluster in clusters.items()
                if (
                    isinstance(cluster_id, str)
                    and isinstance(cluster, dict)
                    and cluster.get("centroid")
                    and (self.recognition_root / cluster_id).is_dir()
                )
            }
        except (OSError, ValueError, json.JSONDecodeError):
            return {}

    def _assign_cluster(self, embedding: List[float]) -> str:
        normalised = normalise_embedding(embedding)
        with self._cluster_lock:
            best_cluster_id: Optional[str] = None
            best_similarity = -1.0
            for cluster_id, cluster in self._clusters.items():
                similarity = cosine_similarity(normalised, cluster["centroid"])
                if similarity > best_similarity:
                    best_cluster_id = cluster_id
                    best_similarity = similarity

            if best_cluster_id is None or best_similarity < self.cluster_threshold:
                cluster_id = self._next_cluster_id()
                self._clusters[cluster_id] = {"centroid": normalised, "sample_count": 1}
            else:
                cluster_id = best_cluster_id
                cluster = self._clusters[cluster_id]
                sample_count = cluster["sample_count"]
                centroid = [
                    (value * sample_count + new_value) / (sample_count + 1)
                    for value, new_value in zip(cluster["centroid"], normalised)
                ]
                cluster["centroid"] = normalise_embedding(centroid)
                cluster["sample_count"] = sample_count + 1
            self._save_clusters()
            return cluster_id

    def _next_cluster_id(self) -> str:
        while True:
            cluster_id = f"cluster-{self._next_cluster_number:05d}"
            self._next_cluster_number += 1
            if cluster_id not in self._clusters and not (self.recognition_root / cluster_id).exists():
                return cluster_id

    def _save_clusters(self) -> None:
        write_json_atomically(
            self._cluster_state_path(),
            {
                "schema_version": SCHEMA_VERSION,
                "model_name": self.model_name,
                "cluster_threshold": self.cluster_threshold,
                "next_cluster_number": self._next_cluster_number,
                "clusters": self._clusters,
            },
        )

    def _write_review_artifacts(
        self,
        input_path: str,
        relative_path: str,
        metadata: Dict[str, Any],
        detection: FaceDetection,
        detection_number: int,
        cluster_id: str,
    ) -> None:
        source_relative = Path(relative_path)
        output_directory = self.recognition_root / cluster_id / source_relative.parent
        file_stem = source_relative.name
        suffix = "" if detection_number == 1 else f".person-{detection_number:02d}"
        record_path = output_directory / f"{file_stem}{suffix}.json"
        shortcut_path = output_directory / f"{file_stem}{suffix}.lnk"
        extension = source_relative.suffix or ".jpg"
        crop_path = output_directory / f"{file_stem}{suffix}.cut-out{extension}"
        crop_bbox = _person_review_crop(detection.bbox, detection.image_size)

        output_directory.mkdir(parents=True, exist_ok=True)
        _save_crop(input_path, crop_path, crop_bbox)
        _create_windows_shortcut(shortcut_path, Path(input_path))
        payload = {
            "schema_version": SCHEMA_VERSION,
            "kind": "recognition-review",
            "cluster_id": cluster_id,
            "source": {
                "relative_path": relative_path,
                "filename": source_relative.name,
                "captured_at": _serialise_datetime(metadata.get("datetime")),
                "captured_at_source": metadata.get("datetime_source"),
                "image_size": {"width": detection.image_size[0], "height": detection.image_size[1]},
                "coordinate_space": "exif-orientation-corrected source image pixels",
            },
            "face_bbox": detection.bbox,
            "person_crop_bbox": crop_bbox,
            "detection_confidence": round(detection.confidence, 6),
            "cut_out": {"relative_path": str(crop_path.relative_to(self.output_dir)).replace("\\", "/")},
            "shortcut": {"relative_path": str(shortcut_path.relative_to(self.output_dir)).replace("\\", "/")},
        }
        source_video = metadata.get("_source_video_path")
        if isinstance(source_video, str):
            try:
                payload["source"]["video_relative_path"] = self._relative_source_path(source_video)
            except ValueError:
                pass
        for field, metadata_key in (
            ("frame_number", "_frame_number"),
            ("frame_timestamp_seconds", "_frame_timestamp_seconds"),
        ):
            value = metadata.get(metadata_key)
            if isinstance(value, (int, float)):
                payload["source"][field] = value
        write_json_atomically(record_path, payload)


class RecognitionTask(Task[Tuple[str, Dict[str, Any]], Tuple[str, Dict[str, Any]]]):
    """Add only threshold-approved curated identities to an image's metadata."""

    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str,
        model_name: str = "buffalo_l",
        providers: Optional[List[str]] = None,
        enabled: bool = True,
        detection_threshold: float = 0.60,
        copy_matches_to_review_clusters: bool = True,
        retry: bool = False,
        retry_failed: bool = False,
        remote_api_base: Optional[str] = None,
        remote_api_token: Optional[str] = None,
        remote_timeout_seconds: float = 120.0,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir = Path(output_dir)
        self.model_name = model_name
        self.providers = providers
        self.enabled = enabled
        self.detection_threshold = float(detection_threshold)
        self.copy_matches_to_review_clusters = copy_matches_to_review_clusters
        self.retry = retry
        self.retry_failed = retry_failed
        self.remote_api_base = remote_api_base
        self.remote_api_token = remote_api_token
        self.remote_timeout_seconds = float(remote_timeout_seconds)
        self.index: Optional[RecognitionIndex] = None
        self.backend: Optional[RecognitionBackend] = None
        self._candidate_clusters: Dict[str, str] = {}
        self._candidate_clusters_loaded = False
        self._candidate_cluster_lock = threading.Lock()

    def load(self) -> None:
        if not self.enabled:
            return
        index_path = self.output_dir / "recognition" / INDEX_FILENAME
        if not index_path.exists():
            return
        self.index = RecognitionIndex.load(index_path)
        index_model_name = self.index.metadata.get("model_name", self.model_name)
        if self.remote_api_base:
            self.backend = RemoteRecognitionBackend(
                self.remote_api_base,
                self.remote_api_token or "",
                self.remote_timeout_seconds,
            )
        else:
            self.backend = InsightFaceBackend(model_name=index_model_name, providers=self.providers)
        self.backend.load()

    def execute(self, item: Tuple[str, Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
        input_path, metadata = item
        if not isinstance(metadata, dict):
            raise metadata
        metadata = dict(metadata)
        relative_path = Path(output_relative_path(input_path, self.input_dir or input_path, metadata)).as_posix()
        output_path = self.output_dir / f"{relative_path}.cut-out.json"
        error_path = self.output_dir / f"{relative_path}.cut-out.error.json"
        if not self.retry and output_path.exists():
            try:
                with output_path.open("r", encoding="utf-8") as handle:
                    cached_payload = json.load(handle)
                if cached_payload.get("recognition_input") == "original":
                    self.record_skip()
                    self._sync_cached_review_artifacts(relative_path, metadata, cached_payload)
                    metadata["_recognition"] = cached_payload
                    metadata["_stage"] = "recognition"
                    return input_path, metadata
            except (OSError, ValueError, json.JSONDecodeError):
                pass
        if not self.retry and not self.retry_failed and error_path.exists():
            self.record_skip()
            return input_path, {**metadata, "_stage": "recognition-blocked"}
        people: List[Dict[str, Any]] = []
        if not self.enabled:
            self.record_skip()
        # Recognition needs the full-resolution source: ResizeTask retains the
        # original path as the task item and stores its LLM-sized derivative in
        # ``_prepared_image_path``.  Do not use that derivative for detection
        # or crops, otherwise small faces can be lost before recognition.
        source_path = input_path
        image_size = _source_image_size(source_path)
        if self.index is not None and self.backend is not None:
            matched_labels: Dict[str, int] = {}
            unknown_count = 0
            for detection in self.backend.detect(source_path):
                if detection.confidence < self.detection_threshold:
                    continue
                image_size = {"width": detection.image_size[0], "height": detection.image_size[1]}
                match = self.index.match(detection.embedding)
                if match is not None:
                    matched_labels[match.identity_id] = matched_labels.get(match.identity_id, 0) + 1
                    people.append(
                        self._write_match_artifacts(
                            source_path,
                            relative_path,
                            detection,
                            match.identity_id,
                            match.confidence,
                            matched_labels[match.identity_id],
                            metadata,
                        )
                    )
                else:
                    unknown_count += 1
                    people.append(
                        self._write_unknown_artifacts(
                            source_path,
                            relative_path,
                            detection,
                            unknown_count,
                        )
                    )
        payload = {
            "schema_version": SCHEMA_VERSION,
            "kind": "recognition-result",
            "source": {
                "relative_path": relative_path,
                "output_relative_path": relative_path,
                "image_size": image_size,
                "coordinate_space": "exif-orientation-corrected source image pixels",
            },
            "index_available": self.index is not None,
            "recognition_input": "original",
            "people": people,
        }
        source_video = metadata.get("_source_video_path")
        if isinstance(source_video, str):
            try:
                payload["source"]["video_relative_path"] = Path(
                    os.path.relpath(source_video, self.input_dir or source_video)
                ).as_posix()
            except ValueError:
                pass
        for field, metadata_key in (
            ("frame_number", "_frame_number"),
            ("frame_timestamp_seconds", "_frame_timestamp_seconds"),
        ):
            value = metadata.get(metadata_key)
            if isinstance(value, (int, float)):
                payload["source"][field] = value
        write_json_atomically(output_path, payload)
        metadata["_recognition"] = payload
        metadata["_stage"] = "recognition"
        try:
            error_path.unlink()
        except FileNotFoundError:
            pass
        return input_path, metadata

    def _write_match_artifacts(
        self,
        input_path: str,
        relative_path: str,
        detection: FaceDetection,
        label: str,
        confidence: float,
        sequence: int,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        source_relative = Path(relative_path)
        safe_label = re.sub(r"[^A-Za-z0-9._-]+", "_", label).strip("._") or "person"
        suffix = f".{safe_label}" if sequence == 1 else f".{safe_label}-{sequence:02d}"
        extension = source_relative.suffix or ".jpg"
        destination_dir = self.output_dir / source_relative.parent
        crop_path = destination_dir / f"{source_relative.name}.cut-out{suffix}{extension}"
        json_path = Path(f"{crop_path}.json")
        crop_bbox = _person_review_crop(detection.bbox, detection.image_size)
        destination_dir.mkdir(parents=True, exist_ok=True)
        _save_crop(input_path, crop_path, crop_bbox)
        person = {
            "label": label,
            "status": "matched",
            "match_confidence": round(confidence, 6),
            "face_bbox": detection.bbox,
            "person_crop_bbox": crop_bbox,
            "detection_confidence": round(detection.confidence, 6),
            "crop": crop_path.name,
        }
        write_json_atomically(json_path, {
            "schema_version": SCHEMA_VERSION,
            "kind": "recognition-cut-out",
            "source": {"relative_path": relative_path, "coordinate_space": "exif-orientation-corrected source image pixels"},
            **person,
        })
        self._copy_match_to_review_cluster(
            crop_path=crop_path,
            relative_path=relative_path,
            label=label,
            person=person,
            metadata=metadata,
        )
        return person

    def _sync_cached_review_artifacts(
        self,
        relative_path: str,
        metadata: Dict[str, Any],
        payload: Dict[str, Any],
    ) -> None:
        """Backfill provisional review clusters from cached describe results."""
        if not self.copy_matches_to_review_clusters:
            return
        source_relative = Path(relative_path)
        source_directory = self.output_dir / source_relative.parent
        people = payload.get("people")
        if not isinstance(people, list):
            return
        for person in people:
            if not isinstance(person, dict) or person.get("status") != "matched":
                continue
            label = person.get("label")
            crop_name = person.get("crop")
            if not isinstance(label, str) or not isinstance(crop_name, str):
                continue
            crop_path = source_directory / crop_name
            if crop_path.is_file():
                self._copy_match_to_review_cluster(crop_path, relative_path, label, person, metadata)

    def _copy_match_to_review_cluster(
        self,
        crop_path: Path,
        relative_path: str,
        label: str,
        person: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> None:
        """Queue a matched crop for human review without changing any identity."""
        if not self.copy_matches_to_review_clusters:
            return
        cluster_id = self._review_cluster_for_label(label)
        source_relative = Path(relative_path)
        destination_dir = self.output_dir / "recognition" / cluster_id / source_relative.parent
        destination_dir.mkdir(parents=True, exist_ok=True)
        copied_crop = destination_dir / crop_path.name
        shutil.copy2(crop_path, copied_crop)

        source_payload: Dict[str, Any] = {
            "relative_path": relative_path,
            "output_relative_path": relative_path,
            "coordinate_space": "exif-orientation-corrected source image pixels",
        }
        source_video = metadata.get("_source_video_path")
        if isinstance(source_video, str):
            try:
                source_payload["video_relative_path"] = Path(
                    os.path.relpath(source_video, self.input_dir or source_video)
                ).as_posix()
            except ValueError:
                pass
        for field, metadata_key in (
            ("frame_number", "_frame_number"),
            ("frame_timestamp_seconds", "_frame_timestamp_seconds"),
        ):
            value = metadata.get(metadata_key)
            if isinstance(value, (int, float)):
                source_payload[field] = value

        record_path = destination_dir / f"{crop_path.name}.json"
        payload = {
            "schema_version": SCHEMA_VERSION,
            "kind": "recognition-review",
            "cluster_id": cluster_id,
            "suggested_identity": label,
            "source": source_payload,
            "face_bbox": person.get("face_bbox"),
            "person_crop_bbox": person.get("person_crop_bbox"),
            "detection_confidence": person.get("detection_confidence"),
            "match_confidence": person.get("match_confidence"),
            "cut_out": {
                "relative_path": str(copied_crop.relative_to(self.output_dir)).replace("\\", "/"),
            },
            "created_by": "describe-recognition-candidate",
        }
        write_json_atomically(record_path, payload)

    def _review_cluster_for_label(self, label: str) -> str:
        """Return a stable, provisional cluster for one model-suggested label."""
        candidate = Path(label)
        if candidate.name != label or label in {"", ".", ".."}:
            raise ValueError(f"Invalid recognition identity label: {label!r}")
        with self._candidate_cluster_lock:
            if not self._candidate_clusters_loaded:
                self._candidate_clusters = self._load_candidate_clusters()
                self._candidate_clusters_loaded = True
            existing = self._candidate_clusters.get(label)
            if existing:
                return existing
            cluster_id = self._next_review_cluster_id()
            self._candidate_clusters[label] = cluster_id
            write_json_atomically(
                self.output_dir / "recognition" / ".state" / "describe-match-clusters.json",
                {
                    "schema_version": SCHEMA_VERSION,
                    "kind": "recognition-candidate-clusters",
                    "clusters": self._candidate_clusters,
                },
            )
            return cluster_id

    def _load_candidate_clusters(self) -> Dict[str, str]:
        path = self.output_dir / "recognition" / ".state" / "describe-match-clusters.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            clusters = payload.get("clusters", {})
            if not isinstance(clusters, dict):
                return {}
            return {
                label: cluster_id
                for label, cluster_id in clusters.items()
                if (
                    isinstance(label, str)
                    and isinstance(cluster_id, str)
                    and re.fullmatch(r"cluster-\d+", cluster_id)
                )
            }
        except (OSError, ValueError, json.JSONDecodeError):
            return {}

    def _next_review_cluster_id(self) -> str:
        recognition_root = self.output_dir / "recognition"
        known_numbers = []
        try:
            entries = list(recognition_root.iterdir())
        except OSError:
            entries = []
        for entry in entries:
            match = re.fullmatch(r"cluster-(\d+)", entry.name)
            if match:
                known_numbers.append(int(match.group(1)))
        return f"cluster-{max(known_numbers, default=0) + 1:05d}"

    def _write_unknown_artifacts(
        self,
        input_path: str,
        relative_path: str,
        detection: FaceDetection,
        sequence: int,
    ) -> Dict[str, Any]:
        """Persist an unmatched face without making it an LLM identity."""
        source_relative = Path(relative_path)
        cluster_id = f"cluster-{sequence:05d}"
        extension = source_relative.suffix or ".jpg"
        destination_dir = self.output_dir / source_relative.parent
        crop_path = destination_dir / f"{source_relative.name}.cut-out.{cluster_id}{extension}"
        json_path = Path(f"{crop_path}.json")
        crop_bbox = _person_review_crop(detection.bbox, detection.image_size)
        destination_dir.mkdir(parents=True, exist_ok=True)
        _save_crop(input_path, crop_path, crop_bbox)
        person = {
            "label": None,
            "status": "unknown",
            "cluster_id": cluster_id,
            "face_bbox": detection.bbox,
            "person_crop_bbox": crop_bbox,
            "detection_confidence": round(detection.confidence, 6),
            "crop": crop_path.name,
        }
        write_json_atomically(json_path, {
            "schema_version": SCHEMA_VERSION,
            "kind": "recognition-cut-out",
            "source": {"relative_path": relative_path, "coordinate_space": "exif-orientation-corrected source image pixels"},
            **person,
        })
        return person


def _person_review_crop(face_bbox: Dict[str, int], image_size: Tuple[int, int]) -> Dict[str, int]:
    """Create a face-focused crop that excludes other people as far as possible."""
    image_width, image_height = image_size
    face_width = face_bbox["width"]
    face_height = face_bbox["height"]
    crop_width = max(face_width, int(round(face_width * 1.6)))
    crop_height = max(face_height, int(round(face_height * 2.0)))
    centre_x = face_bbox["x"] + face_width / 2
    centre_y = face_bbox["y"] + face_height * 0.65
    left = max(0, min(image_width - crop_width, int(round(centre_x - crop_width / 2))))
    top = max(0, min(image_height - crop_height, int(round(centre_y - crop_height / 2))))
    right = min(image_width, left + crop_width)
    bottom = min(image_height, top + crop_height)
    return {"x": left, "y": top, "width": right - left, "height": bottom - top}


def _save_crop(source_path: str, crop_path: Path, bbox: Dict[str, int]) -> None:
    with Image.open(source_path) as original:
        image = ImageOps.exif_transpose(original)
        crop = image.crop((bbox["x"], bbox["y"], bbox["x"] + bbox["width"], bbox["y"] + bbox["height"]))
        suffix = crop_path.suffix.lower()
        if suffix in {".jpg", ".jpeg"}:
            crop.convert("RGB").save(crop_path, "JPEG", quality=95)
        elif suffix == ".png":
            crop.save(crop_path, "PNG")
        elif suffix == ".webp":
            crop.save(crop_path, "WEBP", quality=95)
        else:
            crop.convert("RGB").save(crop_path, "JPEG", quality=95)


def _create_windows_shortcut(shortcut_path: Path, relative_target: str) -> None:
    """Write a portable Windows Shell Link containing a relative UTF-16 target.

    A relative target lets a link written by Linux Docker resolve correctly when
    opened later from the mounted storage share in Windows.
    """
    flags = 0x00000008 | 0x00000080  # HasRelativePath | IsUnicode
    # File attributes, timestamps, file size and icon index are zero. The link
    # itself opens normally (SW_SHOWNORMAL = 1) when double-clicked in Explorer.
    header_tail = b"\x00" * 36 + (1).to_bytes(4, "little") + b"\x00" * 12
    header = (
        (0x4C).to_bytes(4, "little")
        + bytes.fromhex("0114020000000000c000000000000046")
        + flags.to_bytes(4, "little")
        + header_tail
    )
    target = relative_target.encode("utf-16-le")
    shortcut_path.write_bytes(header + (len(target) // 2).to_bytes(2, "little") + target)


def _source_image_size(source_path: str) -> Optional[Dict[str, int]]:
    try:
        with Image.open(source_path) as image:
            corrected = ImageOps.exif_transpose(image)
            return {"width": corrected.width, "height": corrected.height}
    except OSError:
        return None


def _serialise_datetime(value: Any) -> Optional[str]:
    return value.isoformat() if isinstance(value, datetime) else None
