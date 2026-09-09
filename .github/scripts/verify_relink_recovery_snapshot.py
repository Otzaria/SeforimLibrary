#!/usr/bin/env python3
"""Fail-closed semantic check for reusing a Linker recovery on a rebuilt DB.

The normal serial pipeline requires a byte-identical lines snapshot. A recovery
has to rebuild the DB, however, and Sefaria image embedding can legitimately
change an image ``src`` from its remote ``textimages.sefaria.org`` URL to an
inline data URI when a previously transient download succeeds. Such a change
cannot affect Linker output, but every other source change must still fail.

Two entry points share one implementation of the payload-metadata checks:

``--preflight --payload-meta META``
    Input-only. Validates the Linker's published ``meta.json`` alone, with no
    database, so the dispatch-time preflight in manual-generate-release.yml can
    reject a drifted Linker payload in seconds. Build 34021998271 spent 48
    minutes — DB generation, snapshot dump and a 987 MiB upload — before Phase-2
    rejected a ``meta.json`` that was downloadable at second 0.

the full comparison (all five paths)
    Everything above plus the semantic snapshot comparison, run in Phase-2.

Both call :func:`check_payload_meta`, so the accepted schema set cannot drift
between the cheap gate and the expensive one.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import sqlite3
from pathlib import Path


IMAGE_TAG = re.compile(
    r"(?P<prefix><img\s+[^>]*?\bsrc\s*=\s*)(?P<quote>[\"'])"
    r"(?P<src>[^\"']+)(?P=quote)(?P<suffix>[^>]*?/?>)",
    re.IGNORECASE,
)
TEXTIMAGE_PREFIX = "https://textimages.sefaria.org/"
DATA_IMAGE = re.compile(
    r"data:(image/(?:png|jpeg|gif|svg\+xml|webp));base64,"
    r"(?P<payload>[A-Za-z0-9+/]*={0,2})\Z",
    re.IGNORECASE,
)
MAX_IMAGE_BYTES = 5 * 1024 * 1024
SNAPSHOT_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
# The single source of truth for which Linker payload metadata this pipeline can
# consume. The file is written by LinkerToOtzaria ``src/incremental.py`` in
# ``write_meta`` (``"schema_version": 3`` at line 280 as of a9ae2d4, 2026-08-31);
# only ``snapshot.sha256`` is read here, and schema 3 left that field untouched.
# Add a version ONLY after observing the Linker's real writer and confirming that
# ``snapshot.sha256`` still means the sha256 of the uncompressed lines snapshot.
# The tripwire for the next bump is not this constant: it is the dispatch-time
# ``--preflight`` run in manual-generate-release.yml, which feeds the Linker's
# actually published meta.json to this exact check before the build spends a
# minute (SeforimLibrary .github/scripts/test_verify_relink_recovery_snapshot.py
# pins the fixture to the writer's real key layout).
PAYLOAD_META_SCHEMAS = frozenset({2, 3})
BASELINE_SCHEMA = 2


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def check_payload_meta(path: Path) -> tuple[int, str]:
    """Validate the input-only fields of the Linker's payload ``meta.json``.

    Returns ``(schema_version, snapshot_sha256)``. Needs no database, so the
    dispatch-time preflight and Phase-2 run byte-identical logic.
    """
    meta = _json(path)
    if not isinstance(meta, dict):
        raise SystemExit(f"{path}: payload meta must be a JSON object, got {type(meta).__name__}")
    schema = meta.get("schema_version")
    if schema not in PAYLOAD_META_SCHEMAS:
        raise SystemExit(
            f"{path}: schema_version {schema!r} is not an accepted Linker meta schema "
            f"{sorted(PAYLOAD_META_SCHEMAS)}"
        )
    snapshot = meta.get("snapshot")
    sha = snapshot.get("sha256") if isinstance(snapshot, dict) else None
    if not isinstance(sha, str) or not SNAPSHOT_SHA256.fullmatch(sha):
        raise SystemExit(
            f"{path}: snapshot.sha256 {sha!r} is not a 64-character lowercase sha256"
        )
    return schema, sha


def check_baseline_manifest(path: Path) -> object:
    """Validate the line baseline manifest schema; return its snapshot sha256."""
    baseline = _json(path)
    if not isinstance(baseline, dict):
        raise SystemExit(f"{path}: baseline manifest must be a JSON object")
    if baseline.get("schema_version") != BASELINE_SCHEMA:
        raise SystemExit(
            f"{path}: schema_version {baseline.get('schema_version')!r} is not the accepted "
            f"line baseline schema {BASELINE_SCHEMA}"
        )
    return baseline.get("snapshot_sha256")


def _remote_inline_equivalent(src_a: str, src_b: str) -> bool:
    if src_a.startswith(TEXTIMAGE_PREFIX):
        remote, inline = src_a, src_b
    elif src_b.startswith(TEXTIMAGE_PREFIX):
        remote, inline = src_b, src_a
    else:
        return False
    if not remote[len(TEXTIMAGE_PREFIX) :] or any(c.isspace() for c in remote):
        return False
    match = DATA_IMAGE.fullmatch(inline)
    if match is None:
        return False
    try:
        decoded = base64.b64decode(match.group("payload"), validate=True)
    except ValueError:
        return False
    return 0 < len(decoded) <= MAX_IMAGE_BYTES


def _split_image_sources(value: str) -> tuple[list[str], list[str]]:
    """Return exact non-src spans and ordered img src values."""
    spans: list[str] = []
    sources: list[str] = []
    cursor = 0
    for match in IMAGE_TAG.finditer(value):
        spans.append(value[cursor : match.start("src")])
        sources.append(match.group("src"))
        cursor = match.end("src")
    spans.append(value[cursor:])
    return spans, sources


def _image_src_equivalent(left: str, right: str) -> bool:
    left_spans, left_sources = _split_image_sources(left)
    right_spans, right_sources = _split_image_sources(right)
    if not left_sources or left_spans != right_spans:
        return False
    return all(
        src_a == src_b or _remote_inline_equivalent(src_a, src_b)
        for src_a, src_b in zip(left_sources, right_sources, strict=True)
    )


def _schema(connection: sqlite3.Connection) -> list[tuple]:
    return connection.execute(
        "SELECT type,name,sql FROM sqlite_master "
        "WHERE type IN ('table','index','view','trigger') ORDER BY type,name"
    ).fetchall()


def _meta(connection: sqlite3.Connection) -> list[tuple]:
    return connection.execute(
        "SELECT key,value FROM lines_snapshot_meta ORDER BY key"
    ).fetchall()


def verify(
    original: Path,
    rebuilt: Path,
    artifacts: Path,
    payload_meta: Path,
    baseline_manifest: Path,
) -> tuple[int, int]:
    # Schema first, digests second: both metadata files are input-only, so a
    # drifted schema must never cost the sha256 of a 5 GiB snapshot. The Linker
    # bumped meta.json to schema 3 in a9ae2d4 (2026-08-31) without changing
    # ``snapshot.sha256`` — the only field read here. The line baseline manifest
    # is still schema 2 (LinkerToOtzaria src/line_baseline.py SCHEMA_VERSION).
    _, expected_sha = check_payload_meta(payload_meta)
    baseline_sha = check_baseline_manifest(baseline_manifest)
    original_sha = _sha256(original)
    if original_sha != expected_sha:
        raise SystemExit("original raw snapshot SHA does not match Linker payload metadata")
    if baseline_sha != original_sha:
        raise SystemExit("line baseline is not bound to the original raw snapshot")

    connections = []
    for path in (original, rebuilt):
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA mmap_size=4294967296")
        connection.execute("PRAGMA cache_size=-524288")
        connections.append(connection)
    old, new = connections
    if _schema(old) != _schema(new):
        raise SystemExit("rebuilt snapshot schema differs from the linked snapshot")
    if _meta(old) != _meta(new):
        raise SystemExit("rebuilt snapshot metadata differs from the linked snapshot")

    query = (
        "SELECT source_name,canonical_he_title,line_index,content,context_ref "
        "FROM lines_snapshot ORDER BY source_name,canonical_he_title,line_index"
    )
    old_rows, new_rows = old.execute(query), new.execute(query)
    safe_differences: set[tuple[str, str, int]] = set()
    count = 0
    while True:
        before, after = old_rows.fetchone(), new_rows.fetchone()
        if before is None or after is None:
            if before is not None or after is not None:
                raise SystemExit("rebuilt snapshot row count differs from linked snapshot")
            break
        count += 1
        if before[:3] != after[:3]:
            raise SystemExit(f"rebuilt snapshot line identity differs at row {count}")
        if before[4] != after[4]:
            raise SystemExit(f"rebuilt snapshot context_ref differs at {before[:3]!r}")
        if before[3] != after[3]:
            if not _image_src_equivalent(before[3], after[3]):
                raise SystemExit(
                    f"rebuilt snapshot text differs outside a safe image src at {before[:3]!r}"
                )
            safe_differences.add((before[0], before[1], before[2]))
            if len(safe_differences) > 10_000:
                raise SystemExit("too many image-src differences for a bounded recovery")

    if safe_differences:
        for artifact in artifacts.rglob("*.jsonl"):
            with artifact.open(encoding="utf-8") as stream:
                for number, line in enumerate(stream, 1):
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    book = record.get("book_key", {})
                    key = (
                        book.get("source_name"),
                        book.get("canonical_he_title"),
                        record.get("line_index"),
                    )
                    if key in safe_differences:
                        raise SystemExit(
                            f"image-src changed line has a Linker record: {key!r} "
                            f"({artifact}:{number})"
                        )
    return count, len(safe_differences)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="validate only the Linker payload meta.json (no database needed)",
    )
    parser.add_argument("--original", type=Path)
    parser.add_argument("--rebuilt", type=Path)
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--payload-meta", required=True, type=Path)
    parser.add_argument("--baseline-manifest", type=Path)
    args = parser.parse_args()
    comparison = (
        ("--original", args.original),
        ("--rebuilt", args.rebuilt),
        ("--artifacts", args.artifacts),
        ("--baseline-manifest", args.baseline_manifest),
    )
    if args.preflight:
        # Fail closed rather than silently downgrading: --preflight next to the
        # comparison flags would otherwise turn Phase-2's 5 GiB check into a
        # 759-byte schema read and still exit 0.
        supplied = [flag for flag, value in comparison if value is not None]
        if supplied:
            raise SystemExit(
                "--preflight validates the payload meta.json alone and cannot be combined with "
                + ", ".join(supplied)
            )
        schema, snapshot_sha = check_payload_meta(args.payload_meta)
        print(f"RECOVERY_META_PREFLIGHT_OK schema={schema} snapshot_sha256={snapshot_sha}")
        return
    # Nothing about the full comparison is optional: the flags are declared
    # optional only so --preflight can omit them, and every one is required here.
    missing = [flag for flag, value in comparison if value is None]
    if missing:
        raise SystemExit(
            "the full recovery comparison requires " + ", ".join(missing) + " (or --preflight)"
        )
    rows, differences = verify(
        args.original,
        args.rebuilt,
        args.artifacts,
        args.payload_meta,
        args.baseline_manifest,
    )
    print(
        f"RECOVERY_SNAPSHOT_SEMANTIC_OK rows={rows} "
        f"unlinked_image_src_differences={differences}"
    )


if __name__ == "__main__":
    main()
