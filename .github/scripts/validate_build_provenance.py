#!/usr/bin/env python3
"""Strict canonical contract for a published Seforim build provenance."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re


SHA40 = re.compile(r"[0-9a-f]{40}")
SHA64 = re.compile(r"[0-9a-f]{64}")
TAG = re.compile(r"[A-Za-z0-9._-]{1,150}")
V1_KEYS = {
    "schema_version", "correlation_id", "source_commit", "sefaria_tag",
    "sefaria_release_metadata_sha256", "sefaria_archive_sha256", "otzaria_tag",
    "otzaria_asset_sha256", "expected_links_commit", "otzaria_target_commit",
    "lineage_sha256",
    "config_sha256", "source_links_tree_sha256", "packaged_links_tree_sha256", "assets",
}
V2_KEYS = V1_KEYS | {
    "fordb_archive_sha256", "fordb_tag", "linker_payload_sha256",
    "linker_engine_fingerprint", "linker_relink_run_id", "linker_commit",
    "linker_relink_run_attempt", "linker_relink_request_id",
}
V3_KEYS = V2_KEYS | {"phase2_implementation_commit"}


def load(path: Path) -> dict:
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError(f"duplicate key {key!r}")
            value[key] = item
        return value

    raw = path.read_bytes()
    value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise ValueError("build provenance must be an object")
    version = value.get("schema_version")
    if type(version) is not int or version not in (1, 2, 3):
        raise ValueError("schema_version must be integer 1, 2 or 3")
    expected_keys = {1: V1_KEYS, 2: V2_KEYS, 3: V3_KEYS}[version]
    if set(value) != expected_keys:
        raise ValueError("unknown build provenance key set")
    canonical = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode() + b"\n"
    if raw != canonical:
        raise ValueError("build provenance is not canonical JSON with one trailing LF")
    return value


def validate(value: dict) -> None:
    version = value["schema_version"]
    correlation = value["correlation_id"]
    if not isinstance(correlation, str):
        raise ValueError("correlation_id must be a string")
    match = re.fullmatch(
        r"sefaria:([1-9][0-9]*):([1-9][0-9]*):([A-Za-z0-9._-]{1,100}):([0-9a-f]{64})",
        correlation,
    )
    if not match:
        raise ValueError("invalid correlation_id")
    for field in ("source_commit", "expected_links_commit", "otzaria_target_commit"):
        if not isinstance(value[field], str) or not SHA40.fullmatch(value[field]):
            raise ValueError(f"invalid {field}")
    for field in (
        "sefaria_release_metadata_sha256", "sefaria_archive_sha256", "otzaria_asset_sha256",
        "lineage_sha256", "config_sha256", "source_links_tree_sha256",
        "packaged_links_tree_sha256",
    ):
        if not isinstance(value[field], str) or not SHA64.fullmatch(value[field]):
            raise ValueError(f"invalid {field}")
    for field in ("sefaria_tag", "otzaria_tag"):
        if not isinstance(value[field], str) or not TAG.fullmatch(value[field]):
            raise ValueError(f"invalid {field}")
    if value["sefaria_tag"] != match.group(3) or value["sefaria_release_metadata_sha256"] != match.group(4):
        raise ValueError("correlation_id disagrees with pinned Sefaria fields")
    if value["expected_links_commit"] != value["otzaria_target_commit"]:
        raise ValueError("Otzaria target differs from expected links commit")
    if version >= 2:
        if not isinstance(value["linker_commit"], str) or not SHA40.fullmatch(value["linker_commit"]):
            raise ValueError("invalid linker_commit")
        for field in (
            "fordb_archive_sha256", "linker_payload_sha256", "linker_relink_request_id",
        ):
            if not isinstance(value[field], str) or not SHA64.fullmatch(value[field]):
                raise ValueError(f"invalid {field}")
        if value["fordb_tag"] != "fordb-sha256-" + value["fordb_archive_sha256"]:
            raise ValueError("ForDB tag does not match archive digest")
        for field in ("linker_relink_run_id", "linker_relink_run_attempt"):
            if type(value[field]) is not int or value[field] < 1:
                raise ValueError(f"{field} must be a positive integer")
        fingerprint = value["linker_engine_fingerprint"]
        if not isinstance(fingerprint, str) or not re.fullmatch(r"[\x20-\x7e]{1,4096}", fingerprint):
            raise ValueError("invalid linker_engine_fingerprint")
    if version >= 3:
        phase2_commit = value["phase2_implementation_commit"]
        if not isinstance(phase2_commit, str) or not SHA40.fullmatch(phase2_commit):
            raise ValueError("invalid phase2_implementation_commit")
    assets = value["assets"]
    if not isinstance(assets, list) or not assets:
        raise ValueError("assets must be a non-empty array")
    names = []
    for index, asset in enumerate(assets):
        if not isinstance(asset, dict) or set(asset) != {"name", "size", "sha256"}:
            raise ValueError(f"invalid asset descriptor {index}")
        name = asset["name"]
        if not isinstance(name, str) or not name or Path(name).name != name:
            raise ValueError(f"invalid asset name {index}")
        if type(asset["size"]) is not int or asset["size"] < 1:
            raise ValueError(f"invalid asset size {index}")
        if not isinstance(asset["sha256"], str) or not SHA64.fullmatch(asset["sha256"]):
            raise ValueError(f"invalid asset digest {index}")
        names.append(name)
    if names != sorted(names, key=lambda item: item.encode("utf-8")) or len(names) != len(set(names)):
        raise ValueError("asset names must be unique and bytewise sorted")
    required = {"seforim.db.zst", "seforim.db.buildstate", "lines_snapshot.db.zst"}
    if not required.issubset(names):
        raise ValueError("required build assets are missing")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    args = parser.parse_args()
    try:
        validate(load(Path(args.path)))
        return 0
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"build provenance contract error: {exc}", file=__import__("sys").stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
