#!/usr/bin/env python3
"""Present a whole-library run as a v2 shard, for the warehouse's bulk import.

    bridge_whole_library.py --run <run dir> --unique <unique.jsonl> --family <model.json>
                            --out <shard dir> [--plan-out <plan dir>]

<run dir> is a finished run of this worker over a whole library (the v30 run on winpc:
vectors.f32, keys.sha256, certificate.json, manifest.json). <unique.jsonl> holds the texts it
embedded, one {"k": sha256 hex, "t": text} per row in row order — the order of first
appearance in the plan. <model.json> is the family's identity (the sidecar's
config/models/<family>/model.json).

The shard is one window over the whole plan (skip 0, take = records = the rows):
  vectors.f32  hard link to the run's vectors.f32 (no copy)
  keys.bin     hard link to the run's keys.sha256 — the same layout, 32 raw bytes per row
  parity-certificate.json  the run's certificate, whose SHA-256 the manifest names
  shard-manifest.json      version 2, written last
Its plan_sha256 is the SHA-256 of the v2 embed.jsonl the texts make — each row's key, digest
and text, in row order — which --plan-out also writes, with its embed-manifest.json, so the
shards can be checked against a plan too. Every row's key is checked against its text and
against the run's keys file before anything is written.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import vector_shard as vs  # noqa: E402

WORKER = {"name": "seforim-gpu-worker", "version": "1.0", "ep": "rocm", "mode": "torch-mixed"}


def link(src: Path, dst: Path) -> None:
    if os.path.lexists(dst):
        os.remove(dst)
    try:
        os.link(src, dst)
    except OSError as error:
        raise vs.ContractError(f"cannot hard-link {src} to {dst} ({error}); put the shard on the "
                               f"run's filesystem rather than copying gigabytes") from None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--run", required=True, type=Path)
    ap.add_argument("--unique", required=True, type=Path)
    ap.add_argument("--family", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--plan-out", type=Path, default=None)
    a = ap.parse_args(argv)
    run_manifest = json.load(open(a.run / "manifest.json", encoding="utf-8"))
    cert = json.load(open(a.run / "certificate.json", encoding="utf-8"))
    family = json.load(open(a.family, encoding="utf-8"))
    package = {"checksum": run_manifest["model"]["passage_package"]["package_checksum"],
               "quantization": run_manifest["model"]["passage_package"]["variant"]}
    if package not in family["query_packages"]:
        raise vs.ContractError(f"the run's package {package} is not one of the family's query packages")
    dim = family["embedding_dim"]
    rows = run_manifest["vectors"]["count"]
    if os.path.getsize(a.run / "vectors.f32") != rows * dim * 4 or os.path.getsize(a.run / "keys.sha256") != rows * 32:
        raise vs.ContractError("the run's files are not the length its count implies")
    if not cert.get("pass"):
        raise vs.ContractError("the run's certificate did not pass")
    vs.ensure_output_dir(a.out)

    # one pass: the v2 plan's bytes (hashed, optionally written) and every key checked twice
    plan_hash = hashlib.sha256()
    plan_file = None
    if a.plan_out is not None:
        a.plan_out.mkdir(parents=True, exist_ok=True)
        plan_file = open(vs.partial(a.plan_out / vs.EMBED_PLAN_FILE), "wb")
    n = 0
    with open(a.unique, "rb") as texts, open(a.run / "keys.sha256", "rb") as keys:
        for line in texts:
            row = json.loads(line)
            text = row["t"]
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            if digest.hex() != row["k"] or keys.read(32) != digest:
                raise vs.ContractError(f"row {n}: the text, its recorded key and the run's keys file disagree")
            out = json.dumps({"key": digest[:16].hex(), "embedding_text_sha256": digest.hex(),
                              "embedding_text": text}, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
            plan_hash.update(out)
            if plan_file:
                plan_file.write(out)
            n += 1
        if keys.read(1):
            raise vs.ContractError("the run's keys file holds more rows than the texts")
    if n != rows:
        raise vs.ContractError(f"{n} texts for {rows} rows")
    plan_sha256 = plan_hash.hexdigest()
    embed_manifest = {"format": vs.EMBED_PLAN_FORMAT, "version": vs.EMBED_PLAN_VERSION, "records": n,
                      "plan_sha256": plan_sha256, "model": family, "chunking_identity": family["chunking_identity"],
                      "passage_package": package}
    if plan_file:
        plan_file.flush(); os.fsync(plan_file.fileno()); plan_file.close()
        os.replace(vs.partial(a.plan_out / vs.EMBED_PLAN_FILE), a.plan_out / vs.EMBED_PLAN_FILE)
        vs.write_json_atomic(a.plan_out / vs.EMBED_MANIFEST_FILE, embed_manifest)

    link(a.run / "vectors.f32", a.out / vs.VECTORS_FILE)
    link(a.run / "keys.sha256", a.out / vs.KEYS_FILE)
    doc = (a.run / "certificate.json").read_bytes()
    with open(a.out / vs.PARITY_DOCUMENT_FILE, "wb") as f:
        f.write(doc)
    ort, gold = cert["ort_crosscheck"], cert["goldens"]
    samples = ort["n"] + gold["cases"]
    parity = {"reference": ort["ort"].split(",")[0].replace("CPU fp32 graph", "cpu fp32").strip(),
              "samples": samples, "min_cosine": min(ort["min"], gold["min_cos"]),
              "mean_cosine": (ort["mean"] * ort["n"] + gold["mean_cos"] * gold["cases"]) / samples,
              "document_sha256": hashlib.sha256(doc).hexdigest()}
    vs.check_parity(parity)
    worker = dict(WORKER, device=run_manifest["worker"]["device"])
    manifest = vs.shard_manifest(embed_manifest, 0, n, n, vs.sha256_file(a.out / vs.VECTORS_FILE),
                                 vs.sha256_file(a.out / vs.KEYS_FILE), worker, parity)
    files = run_manifest.get("files", {})
    for name, key in (("vectors.f32", "vectors_sha256"), ("keys.sha256", "keys_sha256")):
        if name in files and files[name]["sha256"] != manifest[key]:
            raise vs.ContractError(f"{name} no longer hashes to what the run's manifest recorded")
    vs.write_json_atomic(a.out / vs.SHARD_MANIFEST_FILE, manifest)
    print(json.dumps({"out": str(a.out), "records": n, "plan_sha256": plan_sha256,
                      "vectors_sha256": manifest["vectors_sha256"], "keys_sha256": manifest["keys_sha256"],
                      "parity": parity}))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except vs.ContractError as error:
        print(f"bridge: {error}", file=sys.stderr)
        sys.exit(2)
