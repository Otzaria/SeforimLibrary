#!/usr/bin/env python3
"""Strict boundary validator for an otzaria-library release provenance."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys


SHA40 = re.compile(r"^[0-9a-f]{40}$")
SHA64 = re.compile(r"^[0-9a-f]{64}$")
TAG = re.compile(r"^library-links-[A-Za-z0-9._-]+$")
KEYS = {
    "schema_version", "correlation_id", "target_commit", "tag", "asset",
    "auxiliary_assets", "packaging_runtime", "packaging_toolchain",
    "config_sha256", "source_links_tree_sha256", "packaged_links_tree_sha256",
    "lineage_sha256",
}


def load(path: Path) -> dict:
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError(f"duplicate provenance key {key!r}")
            value[key] = item
        return value

    raw = path.read_bytes()
    value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
    canonical = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode() + b"\n"
    if raw != canonical:
        raise ValueError("Otzaria provenance is not canonical JSON with one trailing LF")
    if not isinstance(value, dict) or set(value) != KEYS:
        raise ValueError("Otzaria provenance has an unexpected key set")
    return value


def descriptor(value: object, field: str) -> dict:
    if not isinstance(value, dict) or set(value) != {"name", "size", "sha256"}:
        raise ValueError(f"{field} is not an exact file descriptor")
    if (
        not isinstance(value["name"], str)
        or not value["name"]
        or Path(value["name"]).name != value["name"]
        or type(value["size"]) is not int
        or value["size"] < 0
        or not isinstance(value["sha256"], str)
        or not SHA64.fullmatch(value["sha256"])
    ):
        raise ValueError(f"{field} contains an invalid descriptor value")
    return value


def validate(value: dict, expected_target: str, expected_tag: str, expected_asset_sha: str) -> None:
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("schema_version must be integer 1")
    if not isinstance(value["correlation_id"], str) or not value["correlation_id"]:
        raise ValueError("correlation_id must be a non-empty string")
    if not isinstance(value["target_commit"], str) or not SHA40.fullmatch(value["target_commit"]):
        raise ValueError("target_commit must be a full Git SHA")
    if not isinstance(value["tag"], str) or not TAG.fullmatch(value["tag"]):
        raise ValueError("tag does not match the immutable Otzaria namespace")
    primary = descriptor(value["asset"], "asset")
    auxiliary = value["auxiliary_assets"]
    if not isinstance(auxiliary, list):
        raise ValueError("auxiliary_assets must be an array")
    for index, item in enumerate(auxiliary):
        descriptor(item, f"auxiliary_assets[{index}]")
    names = [item["name"] for item in auxiliary]
    if names != sorted(names, key=os.fsencode) or len(names) != len(set(names)):
        raise ValueError("auxiliary assets must have unique bytewise-sorted names")
    if primary["name"] != "otzaria_latest.zip" or names != [
        "otzaria_dicta_latest.zip", "talmud_bavli_latest.tar.zst"
    ]:
        raise ValueError("Otzaria release asset names differ from the exact contract")
    runtime = value["packaging_runtime"]
    if not isinstance(runtime, dict) or set(runtime) != {"python", "zlib", "zip_compression"}:
        raise ValueError("packaging_runtime has an unexpected schema")
    toolchain = value["packaging_toolchain"]
    if not isinstance(toolchain, dict) or set(toolchain) != {
        "schema_version", "python", "zlib_build", "zlib_runtime", "gnu_tar", "zstd"
    } or type(toolchain["schema_version"]) is not int or toolchain["schema_version"] != 1:
        raise ValueError("packaging_toolchain has an unexpected schema")
    for field, item in {**runtime, **{key: toolchain[key] for key in toolchain if key != "schema_version"}}.items():
        if not isinstance(item, str) or not item:
            raise ValueError(f"runtime/toolchain {field} must be a non-empty string")
    for field in (
        "config_sha256", "source_links_tree_sha256", "packaged_links_tree_sha256",
        "lineage_sha256",
    ):
        if not isinstance(value[field], str) or not SHA64.fullmatch(value[field]):
            raise ValueError(f"invalid {field}")
    if value["target_commit"] != expected_target:
        raise ValueError("provenance target differs from the pinned Otzaria commit")
    if value["tag"] != expected_tag:
        raise ValueError("provenance tag differs from the pinned Otzaria release")
    if primary["sha256"] != expected_asset_sha:
        raise ValueError("provenance asset digest differs from the pinned archive")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("--expected-target", required=True)
    parser.add_argument("--expected-tag", required=True)
    parser.add_argument("--expected-asset-sha256", required=True)
    args = parser.parse_args()
    try:
        value = load(args.path)
        validate(value, args.expected_target, args.expected_tag, args.expected_asset_sha256)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"Otzaria provenance contract error: {exc}", file=sys.stderr)
        return 2
    # Positive evidence, one line: a green step must not look the same as a
    # validator that was never wired up.
    print(
        f"ok: otzaria_provenance v{value['schema_version']}, {len(value)} fields, "
        f"asset {value['asset']['name']} + {len(value['auxiliary_assets'])} auxiliary, "
        f"tag={value['tag']} target={value['target_commit'][:12]}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
