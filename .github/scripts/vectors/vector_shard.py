"""The external embedding interface, version 2, as otzaria-semantic-search defines it.

The contract is the sidecar's docs/VECTOR_BUILD.md §2 (origin/store-v2 @ 95fc54e):

  input   <plan>/embed.jsonl          {"key","embedding_text_sha256","embedding_text"} per line
          <plan>/embed-manifest.json  format "otzaria-embed-plan", version 2
  output  <shard>/vectors.f32         records x dim little-endian f32, plan order
          <shard>/keys.bin            records x 32 bytes: each record's embedding_text_sha256, raw
          <shard>/shard-manifest.json format "otzaria-embed-shard", version 2 — written last

Standard library only: this module is what the CPU tests exercise, and what any worker
(GPU or not) uses to read a window and write a shard.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import struct
import sys
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple

EMBED_PLAN_FILE = "embed.jsonl"
EMBED_MANIFEST_FILE = "embed-manifest.json"
EMBED_PLAN_FORMAT = "otzaria-embed-plan"
EMBED_PLAN_VERSION = 2

VECTORS_FILE = "vectors.f32"
KEYS_FILE = "keys.bin"
SHARD_MANIFEST_FILE = "shard-manifest.json"
PARITY_DOCUMENT_FILE = "parity-certificate.json"
SHARD_FORMAT = "otzaria-embed-shard"
SHARD_FORMAT_VERSION = 2

PARITY_MIN_COSINE = 0.999
PARITY_MIN_SAMPLES = 1000
UNIT_NORM_TOLERANCE = 1e-3

MODEL_FIELDS = ("family_id", "tokenizer_checksum", "embedding_dim", "pooling", "max_tokens",
                "embedding_text_version", "normalization_version", "chunking_identity",
                "query_packages")


class ContractError(Exception):
    """The plan, the package or the output directory is not one this worker may use."""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path, chunk: int = 1 << 24) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _is_hex(text: str, length: int) -> bool:
    return isinstance(text, str) and len(text) == length and all(c in "0123456789abcdef" for c in text)


def read_embed_manifest(plan_dir: Path) -> dict:
    """embed-manifest.json, checked the way the sidecar's EmbedManifest::read checks it."""
    path = Path(plan_dir) / EMBED_MANIFEST_FILE
    with open(path, encoding="utf-8") as f:
        manifest = json.load(f)
    if manifest.get("format") != EMBED_PLAN_FORMAT or manifest.get("version") != EMBED_PLAN_VERSION:
        raise ContractError(f"{path} is {manifest.get('format')} version {manifest.get('version')}; "
                            f"this worker reads {EMBED_PLAN_FORMAT} version {EMBED_PLAN_VERSION}")
    model = manifest.get("model")
    if not isinstance(model, dict) or any(k not in model for k in MODEL_FIELDS):
        raise ContractError(f"{path}: the model block lacks one of {MODEL_FIELDS}")
    package = manifest.get("passage_package")
    if not isinstance(package, dict) or package not in model["query_packages"]:
        raise ContractError(f"{path}: the passage package is not one of the family's query packages")
    if not _is_hex(manifest.get("plan_sha256", ""), 64):
        raise ContractError(f"{path}: plan_sha256 is not 64 lowercase hex digits")
    if not isinstance(manifest.get("records"), int) or manifest["records"] < 0:
        raise ContractError(f"{path}: records is not a count")
    return manifest


class Record:
    __slots__ = ("position", "digest", "text")

    def __init__(self, position: int, digest: bytes, text: str):
        self.position, self.digest, self.text = position, digest, text


def check_record(position: int, record: dict) -> Record:
    """EmbedRecord::check: the text hashes to its digest, and the key is the digest's prefix."""
    text = record.get("embedding_text")
    declared = record.get("embedding_text_sha256")
    if not isinstance(text, str) or not isinstance(declared, str):
        raise ContractError(f"record {position} is not an embed record")
    actual = hashlib.sha256(text.encode("utf-8")).digest()
    if actual.hex() != declared:
        raise ContractError(f"record {position}: its text hashes to {actual.hex()} and the plan "
                            f"declares {declared} — the plan was damaged on its way; stop")
    if record.get("key") != actual[:16].hex():
        raise ContractError(f"record {position}'s key is {record.get('key')} and its text's digest "
                            f"begins {actual[:16].hex()}")
    return Record(position, actual, text)


def read_window(plan_dir: Path, skip: int, take: int) -> Iterator[Record]:
    """Records skip..skip+take of embed.jsonl, each checked before it is handed out."""
    path = Path(plan_dir) / EMBED_PLAN_FILE
    end = skip + take
    with open(path, "rb") as f:
        for position, line in enumerate(f):
            if position < skip:
                continue
            if position >= end:
                break
            try:
                record = json.loads(line)
            except ValueError as error:
                raise ContractError(f"{path} line {position + 1} is not an embed record: {error}")
            yield check_record(position, record)


def owed(manifest: dict, skip: int, take: int) -> int:
    return max(0, min(take, manifest["records"] - skip))


def ensure_output_dir(out: Path) -> None:
    """A finished shard (all three files) is refused, never overwritten; leftovers are fine."""
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    names = (VECTORS_FILE, KEYS_FILE, SHARD_MANIFEST_FILE)
    if all(os.path.lexists(out / name) for name in names):
        raise ContractError(f"{out} holds a finished shard; embed into another directory, "
                            f"or remove it to re-run this window")
    # A manifest from an unfinished earlier attempt would make this directory look finished
    # the moment the data files are renamed; it is never valid without them.
    if os.path.lexists(out / SHARD_MANIFEST_FILE):
        os.remove(out / SHARD_MANIFEST_FILE)


def partial(path: Path) -> Path:
    return Path(str(path) + ".partial")


class HashingWriter:
    """A file written under .partial, hashed as it goes, flushed, fsynced and renamed when finished."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.file = open(partial(self.path), "wb")
        self.hasher = hashlib.sha256()
        self.bytes = 0

    def write(self, data: bytes) -> None:
        self.hasher.update(data)
        self.file.write(data)
        self.bytes += len(data)

    def finish(self) -> str:
        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()
        os.replace(partial(self.path), self.path)
        return self.hasher.hexdigest()


def vector_bytes(vector: Sequence[float], dim: int) -> bytes:
    """One vector as little-endian f32, refused unless finite, `dim` wide and of unit norm."""
    if hasattr(vector, "astype"):  # numpy, without importing it here
        values = vector.astype("<f4")
        if values.shape != (dim,):
            raise ContractError(f"a vector came back {values.shape}, and each must be {dim} wide")
        data = values.tobytes()
        floats = struct.unpack("<%df" % dim, data)
    else:
        floats = tuple(float(x) for x in vector)
        if len(floats) != dim:
            raise ContractError(f"a vector came back {len(floats)} wide, and each must be {dim} wide")
        data = struct.pack("<%df" % dim, *floats)
        floats = struct.unpack("<%df" % dim, data)
    if not all(math.isfinite(x) for x in floats):
        raise ContractError("a vector is not finite")
    norm = math.sqrt(sum(x * x for x in floats))
    if abs(norm - 1.0) > UNIT_NORM_TOLERANCE:
        raise ContractError(f"a vector's norm is {norm}, not 1 within {UNIT_NORM_TOLERANCE}")
    return data


def matrix_bytes(matrix, dim: int) -> bytes:
    """A numpy [n, dim] matrix as little-endian f32 rows, under vector_bytes' rules."""
    import numpy as np
    m = np.asarray(matrix, dtype="<f4")
    if m.ndim != 2 or m.shape[1] != dim:
        raise ContractError(f"vectors came back {m.shape}, and each must be {dim} wide")
    if not np.isfinite(m).all():
        raise ContractError("a vector is not finite")
    norms = np.sqrt((m.astype(np.float64) ** 2).sum(1))
    bad = np.abs(norms - 1.0) > UNIT_NORM_TOLERANCE
    if bad.any():
        raise ContractError(f"a vector's norm is {float(norms[bad][0])}, not 1 within {UNIT_NORM_TOLERANCE}")
    return m.tobytes()


def write_json_atomic(path: Path, value: dict) -> None:
    tmp = partial(path)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(value, f, indent=1, ensure_ascii=False)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def shard_manifest(plan: dict, skip: int, take: int, records: int, vectors_sha256: str,
                   keys_sha256: str, worker: dict, parity: Optional[dict]) -> dict:
    manifest = {
        "format": SHARD_FORMAT, "version": SHARD_FORMAT_VERSION,
        "plan_sha256": plan["plan_sha256"],
        "skip": skip, "take": take, "records": records,
        "dim": plan["model"]["embedding_dim"],
        "vectors_sha256": vectors_sha256, "keys_sha256": keys_sha256,
        "model": plan["model"],
        "passage_package": plan["passage_package"],
        "worker": worker,
    }
    if parity is not None:
        manifest["parity"] = parity
    return manifest


def check_parity(parity: dict) -> None:
    """The acceptance rule of ShardManifest::check, applied before the manifest is written."""
    if parity["samples"] < PARITY_MIN_SAMPLES:
        raise ContractError(f"the parity certificate covers {parity['samples']} texts, fewer than "
                            f"{PARITY_MIN_SAMPLES}")
    if not (PARITY_MIN_COSINE <= parity["min_cosine"] <= 1.0 + 1e-9):
        raise ContractError(f"the parity certificate's lowest cosine is {parity['min_cosine']}, "
                            f"below {PARITY_MIN_COSINE}")


def sample_positions(plan_sha256: str, records: int, count: int) -> List[int]:
    """Plan positions spread across the whole plan, the same for every shard of one plan."""
    count = min(count, records)
    if count <= 0:
        return []
    seed = int(plan_sha256[:16], 16)
    # one position per equal stretch of the plan, offset inside it by a hash of (seed, stretch)
    out = []
    for i in range(count):
        lo = i * records // count
        hi = max(lo + 1, (i + 1) * records // count)
        h = int.from_bytes(hashlib.sha256(f"{seed}:{i}".encode()).digest()[:8], "little")
        out.append(lo + h % (hi - lo))
    return out


def read_positions(plan_dir: Path, positions: Iterable[int]) -> List[Record]:
    """The checked records at `positions` (sorted, any count), in one pass over embed.jsonl."""
    wanted = sorted(set(positions))
    out, i = [], 0
    if not wanted:
        return out
    with open(Path(plan_dir) / EMBED_PLAN_FILE, "rb") as f:
        for position, line in enumerate(f):
            if position == wanted[i]:
                out.append(check_record(position, json.loads(line)))
                i += 1
                if i == len(wanted):
                    break
    if i != len(wanted):
        raise ContractError(f"the plan ends before position {wanted[i]}")
    return out


def write_shard(plan_dir: Path, out: Path, skip: int, take: int, embed_batch, worker: dict,
                parity_fn=None, chunk: int = 65536, log=None) -> dict:
    """Embed records skip..skip+take of the plan into a shard at `out`.

    `embed_batch(texts) -> sequence of vectors` embeds a list of texts in order.
    `parity_fn(plan) -> (certificate dict, document dict)` runs after the data files are
    finished and before the manifest is written; it is required unless the worker is ONNX
    Runtime on a CPU.
    """
    plan = read_embed_manifest(plan_dir)
    out = Path(out)
    ensure_output_dir(out)
    dim = plan["model"]["embedding_dim"]
    vectors, keys = HashingWriter(out / VECTORS_FILE), HashingWriter(out / KEYS_FILE)
    records = 0
    batch: List[Record] = []

    def flush():
        nonlocal records
        if not batch:
            return
        embedded = embed_batch([r.text for r in batch])
        if len(embedded) != len(batch):
            raise ContractError(f"{len(embedded)} vector(s) came back for {len(batch)} text(s)")
        if hasattr(embedded, "shape"):  # a numpy matrix: checked and written in bulk
            vectors.write(matrix_bytes(embedded, dim))
        else:
            for v in embedded:
                vectors.write(vector_bytes(v, dim))
        keys.write(b"".join(r.digest for r in batch))
        records += len(batch)
        if log:
            log(f"{records} record(s) embedded")
        batch.clear()

    for record in read_window(plan_dir, skip, take):
        batch.append(record)
        if len(batch) >= chunk:
            flush()
    flush()
    if records != owed(plan, skip, take):
        raise ContractError(f"the plan held {records} record(s) from {skip}, and its manifest "
                            f"promises {owed(plan, skip, take)}")
    vectors_sha256, keys_sha256 = vectors.finish(), keys.finish()
    reference = worker.get("ep") == "cpu" and worker.get("mode") == "onnxruntime"
    parity = None
    if parity_fn is not None:
        parity, document = parity_fn(plan)
        if document is not None:
            write_json_atomic(out / PARITY_DOCUMENT_FILE, document)
            parity["document_sha256"] = sha256_file(out / PARITY_DOCUMENT_FILE)
        check_parity(parity)
    elif not reference:
        raise ContractError("a worker that is not ONNX Runtime on a CPU must carry a parity certificate")
    manifest = shard_manifest(plan, skip, take, records, vectors_sha256, keys_sha256, worker, parity)
    write_json_atomic(out / SHARD_MANIFEST_FILE, manifest)
    return manifest


def write_embed_plan(plan_dir: Path, texts: Iterable[str], model: dict, passage_package: dict) -> dict:
    """An embed.jsonl + embed-manifest.json for `texts` (each once, in order) — for tests and bridges."""
    plan_dir = Path(plan_dir)
    plan_dir.mkdir(parents=True, exist_ok=True)
    h, n = hashlib.sha256(), 0
    with open(plan_dir / EMBED_PLAN_FILE, "wb") as f:
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
            line = json.dumps({"key": digest[:32], "embedding_text_sha256": digest, "embedding_text": text},
                              ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
            f.write(line)
            h.update(line)
            n += 1
    manifest = {"format": EMBED_PLAN_FORMAT, "version": EMBED_PLAN_VERSION, "records": n,
                "plan_sha256": h.hexdigest(), "model": model,
                "chunking_identity": model["chunking_identity"], "passage_package": passage_package}
    write_json_atomic(plan_dir / EMBED_MANIFEST_FILE, manifest)
    return manifest
