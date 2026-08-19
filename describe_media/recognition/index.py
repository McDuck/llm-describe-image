"""Persisted local identity templates built from reviewer-approved folders."""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from describe_media.recognition.gpus.base import RecognitionBackend, cosine_similarity, normalise_embedding
from describe_media.recognition.gpus.direct.backend import InsightFaceBackend

SCHEMA_VERSION = 2
INDEX_FILENAME = "model-manifest.json"


def write_json_atomically(path: Path, payload: Dict[str, Any]) -> None:
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


@dataclass
class RecognitionMatch:
    identity_id: str
    confidence: float


class RecognitionIndex:
    def __init__(self, identities: Dict[str, Sequence[float]], metadata: Optional[Dict[str, Any]] = None) -> None:
        self.identities = {identity: normalise_embedding(embedding) for identity, embedding in identities.items()}
        self.metadata = metadata or {}
        self.match_threshold = float(self.metadata.get("match_threshold", 0.55))

    def match(self, embedding: Sequence[float]) -> Optional[RecognitionMatch]:
        if not self.identities:
            return None
        normalised = normalise_embedding(embedding)
        identity_id, confidence = max(
            ((identity, cosine_similarity(normalised, centroid)) for identity, centroid in self.identities.items()),
            key=lambda item: item[1],
        )
        if confidence < self.match_threshold:
            return None
        return RecognitionMatch(identity_id=identity_id, confidence=confidence)

    @classmethod
    def load(cls, path: Path) -> "RecognitionIndex":
        payload = json.loads(path.read_text(encoding="utf-8"))
        identities = payload.get("identities", {})
        if not isinstance(identities, dict):
            raise ValueError(f"Invalid recognition index: {path}")
        return cls(identities=identities, metadata=payload.get("metadata", {}))


def train_recognition_index(
    input_dir: str,
    output_dir: str,
    model_name: str = "buffalo_l",
    match_threshold: float = 0.55,
    backend: Optional[RecognitionBackend] = None,
) -> Path:
    """Build one reusable local index, with an inspectable model file per identity."""
    source_root = Path(input_dir)
    recognition_root = Path(output_dir) / "recognition"
    print("Recognition training: importing prior describe matches into review clusters", flush=True)
    imported_matches = import_describe_matches_to_review_clusters(Path(output_dir))
    print(f"Recognition training: imported {imported_matches} prior describe matches", flush=True)
    runtime = backend or InsightFaceBackend(model_name=model_name)
    print("Recognition training: checking recognition worker", flush=True)
    runtime.load()
    centroids: Dict[str, List[float]] = {}
    sample_counts: Dict[str, int] = {}

    if recognition_root.exists():
        identity_dirs = [
            identity_dir
            for identity_dir in sorted(recognition_root.iterdir())
            if identity_dir.is_dir() and not identity_dir.name.startswith(("cluster-", "."))
        ]
        print(f"Recognition training: {len(identity_dirs)} reviewed identity directories")
        for identity_dir in identity_dirs:
            review_record_count = sum(
                1
                for record_path in identity_dir.rglob("*.json")
                if record_path.parent.name != "model"
            )
            print(
                f"Recognition training: {identity_dir.name} "
                f"({review_record_count} review records)",
                flush=True,
            )
            samples = _identity_embeddings(identity_dir, source_root, recognition_root, runtime)
            if not samples:
                print(f"Recognition training: {identity_dir.name}: no usable detections", flush=True)
                continue
            centroid = normalise_embedding([sum(values) / len(samples) for values in zip(*samples)])
            centroids[identity_dir.name] = centroid
            sample_counts[identity_dir.name] = len(samples)
            write_json_atomically(
                identity_dir / "model" / "identity.json",
                {
                    "schema_version": SCHEMA_VERSION,
                    "kind": "recognition-identity-model",
                    "identity": identity_dir.name,
                    "model_name": model_name,
                    "sample_count": len(samples),
                    "embedding_centroid": centroid,
                },
            )
            print(
                f"Recognition training: {identity_dir.name}: indexed {len(samples)} samples",
                flush=True,
            )

    index_path = recognition_root / INDEX_FILENAME
    write_json_atomically(
        index_path,
        {
            "schema_version": SCHEMA_VERSION,
            "kind": "recognition-model-manifest",
            "identities": centroids,
            "metadata": {
                "model_name": model_name,
                "match_threshold": float(match_threshold),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "sample_counts": sample_counts,
            },
        },
    )
    print(f"Recognition training: wrote {index_path}", flush=True)
    return index_path


def import_describe_matches_to_review_clusters(output_root: Path) -> int:
    """Queue prior describe matches as review candidates, never as identities."""
    recognition_root = output_root / "recognition"
    candidate_clusters = _load_candidate_clusters(recognition_root)
    imported = 0
    for manifest_path in output_root.rglob("*.cut-out.json"):
        try:
            manifest_path.relative_to(recognition_root)
            continue
        except ValueError:
            pass
        try:
            result = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if result.get("kind") != "recognition-result":
            continue
        source = result.get("source")
        people = result.get("people")
        if not isinstance(source, dict) or not isinstance(people, list):
            continue
        relative_path = source.get("relative_path")
        if not isinstance(relative_path, str):
            continue
        source_relative = Path(relative_path)
        for person in people:
            if not isinstance(person, dict) or person.get("status") != "matched":
                continue
            label = person.get("label")
            crop_name = person.get("crop")
            if not isinstance(label, str) or not isinstance(crop_name, str):
                continue
            label_path = Path(label)
            if label_path.name != label or label in {"", ".", ".."}:
                continue
            crop_path = manifest_path.parent / crop_name
            if not crop_path.is_file():
                continue
            cluster_id = candidate_clusters.get(label)
            if cluster_id is None:
                cluster_id = _next_candidate_cluster_id(recognition_root)
                candidate_clusters[label] = cluster_id
                _save_candidate_clusters(recognition_root, candidate_clusters)
            destination_dir = recognition_root / cluster_id / source_relative.parent
            destination_dir.mkdir(parents=True, exist_ok=True)
            copied_crop = destination_dir / crop_name
            shutil.copy2(crop_path, copied_crop)
            write_json_atomically(
                destination_dir / f"{crop_name}.json",
                {
                    "schema_version": SCHEMA_VERSION,
                    "kind": "recognition-review",
                    "cluster_id": cluster_id,
                    "suggested_identity": label,
                    "source": {
                        **source,
                        "output_relative_path": source.get("output_relative_path", relative_path),
                    },
                    "face_bbox": person.get("face_bbox"),
                    "person_crop_bbox": person.get("person_crop_bbox"),
                    "detection_confidence": person.get("detection_confidence"),
                    "match_confidence": person.get("match_confidence"),
                    "cut_out": {
                        "relative_path": str(copied_crop.relative_to(output_root)).replace("\\", "/"),
                    },
                    "created_by": "describe-recognition-import",
                },
            )
            imported += 1
    return imported


def _load_candidate_clusters(recognition_root: Path) -> Dict[str, str]:
    path = recognition_root / ".state" / "describe-match-clusters.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        clusters = payload.get("clusters", {})
        if not isinstance(clusters, dict):
            return {}
        return {
            label: cluster_id
            for label, cluster_id in clusters.items()
            if isinstance(label, str) and isinstance(cluster_id, str) and re.fullmatch(r"cluster-\d+", cluster_id)
        }
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def _save_candidate_clusters(recognition_root: Path, candidate_clusters: Dict[str, str]) -> None:
    write_json_atomically(
        recognition_root / ".state" / "describe-match-clusters.json",
        {
            "schema_version": SCHEMA_VERSION,
            "kind": "recognition-candidate-clusters",
            "clusters": candidate_clusters,
        },
    )


def _next_candidate_cluster_id(recognition_root: Path) -> str:
    try:
        entries = list(recognition_root.iterdir())
    except OSError:
        entries = []
    numbers = [
        int(match.group(1))
        for entry in entries
        for match in [re.fullmatch(r"cluster-(\d+)", entry.name)]
        if match
    ]
    return f"cluster-{max(numbers, default=0) + 1:05d}"


def _identity_embeddings(
    identity_dir: Path,
    source_root: Path,
    recognition_root: Path,
    backend: Any,
) -> List[List[float]]:
    """Embed reviewer-approved source images."""
    embeddings: List[List[float]] = []
    for record_path in identity_dir.rglob("*.json"):
        if record_path.parent.name == "model":
            continue
        try:
            record = json.loads(record_path.read_text(encoding="utf-8"))
            if record.get("kind") != "recognition-review":
                continue
            source = record.get("source", {})
            if not isinstance(source, dict):
                source = {}
            candidates: List[Tuple[Path, Any]] = []
            relative_path = source.get("relative_path")
            if isinstance(relative_path, str):
                candidates.append((source_root / Path(relative_path), record.get("face_bbox")))
            output_relative_path = source.get("output_relative_path")
            if isinstance(output_relative_path, str):
                candidates.append((recognition_root.parent / Path(output_relative_path), record.get("face_bbox")))
            cut_out = record.get("cut_out", {})
            cut_out_relative_path = cut_out.get("relative_path") if isinstance(cut_out, dict) else None
            if isinstance(cut_out_relative_path, str):
                # A crop has its own coordinate system, so match its most
                # prominent detection instead of applying the original bbox.
                candidates.append((recognition_root.parent / Path(cut_out_relative_path), None))
            detection = None
            for candidate_path, bbox in candidates:
                if not candidate_path.is_file():
                    continue
                try:
                    detection = _closest_detection(backend.detect(str(candidate_path)), bbox)
                except (OSError, RuntimeError, ValueError):
                    # A remote worker can reject an overlarge or malformed
                    # source image. Try the review crop before excluding this
                    # reviewed sample from the model.
                    continue
                if detection is not None:
                    break
            if detection is not None:
                embeddings.append(normalise_embedding(detection.embedding))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    return embeddings


def _closest_detection(detections: Sequence[Any], bbox: Any) -> Optional[Any]:
    if not detections:
        return None
    if not isinstance(bbox, dict):
        return detections[0]
    def distance(detection: Any) -> float:
        candidate = detection.bbox
        return abs(candidate["x"] - bbox.get("x", 0)) + abs(candidate["y"] - bbox.get("y", 0))
    return min(detections, key=distance)
