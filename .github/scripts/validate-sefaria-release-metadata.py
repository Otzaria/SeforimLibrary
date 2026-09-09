#!/usr/bin/env python3
"""Validate the immutable Sefaria archive descriptor used by pinned workflows."""

import json
import pathlib
import re
import sys


def validate(path: pathlib.Path, expected_tag: str, expected_archive_sha256: str) -> dict:
    metadata = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(metadata, dict):
        raise ValueError("release metadata must be a JSON object")
    if metadata.get("tag") != expected_tag:
        raise ValueError("metadata tag does not match the pinned release tag")
    for name in ("run_id", "run_attempt"):
        value = metadata.get(name)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"metadata {name} must be a positive integer")
    archive = metadata.get("archive")
    if not isinstance(archive, dict):
        raise ValueError("metadata archive descriptor is missing")
    if archive.get("sha256") != expected_archive_sha256 or re.fullmatch(r"[0-9a-f]{64}", expected_archive_sha256) is None:
        raise ValueError("metadata archive digest does not match the pinned digest")
    archive_size = archive.get("size")
    if not isinstance(archive_size, int) or isinstance(archive_size, bool) or archive_size <= 0:
        raise ValueError("metadata archive size is invalid")
    parts = archive.get("parts")
    if not isinstance(parts, list) or not parts:
        raise ValueError("metadata archive.parts must be non-empty")
    names: list[str] = []
    total = 0
    for part in parts:
        if not isinstance(part, dict):
            raise ValueError("metadata archive part must be an object")
        name, size, digest = part.get("name"), part.get("size"), part.get("sha256")
        if not isinstance(name, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name) is None:
            raise ValueError("metadata archive part name is invalid")
        if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
            raise ValueError("metadata archive part size is invalid")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError("metadata archive part digest is invalid")
        names.append(name)
        total += size
    if len(set(names)) != len(names) or names != sorted(names, key=lambda item: item.encode("utf-8")):
        raise ValueError("metadata archive parts must have unique UTF-8-sorted names")
    if total != archive_size:
        raise ValueError("metadata archive part sizes do not sum to archive.size")
    return metadata


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("usage: validate-sefaria-release-metadata.py PATH TAG ARCHIVE_SHA256")
    try:
        checked = validate(pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3])
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(str(error)) from error
    # Positive evidence, one line: this validator is the only thing standing
    # between a pinned digest and a silently different Sefaria archive, so a
    # pass has to say so out loud.
    print(
        f"ok: sefaria_release_metadata tag={checked['tag']}, "
        f"archive {checked['archive']['size']} bytes in "
        f"{len(checked['archive']['parts'])} parts, "
        f"sha256={checked['archive']['sha256'][:12]}"
    )
