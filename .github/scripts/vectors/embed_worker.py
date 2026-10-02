#!/usr/bin/env python3
"""seforim-gpu-worker: embed a window of a vector plan into a v2 shard, on a GPU.

    embed_worker.py --plan <dir> --out <dir> [--skip N] [--take N]
                    [--model-dir <package dir> | --cache <dir> [--repo R] [--revision REV]]

The plan is the sidecar's external embedding interface v2 (embed.jsonl + embed-manifest.json),
the output a v2 shard (vectors.f32, keys.bin, shard-manifest.json; see vector_shard.py). The
model is the plan's passage package, which must be the family's fp32 package: this worker is
a re-implementation of the fp32 graph (torch_bert.py, mode "mixed"), so every shard carries a
parity certificate against ONNX Runtime on a CPU — the 41 golden texts plus plan texts sampled
across the whole plan, at least 1,000 in all, lowest cosine at least 0.999.

The package is either a directory given with --model-dir or fetched into --cache (default
$OTZARIA_VECTOR_MODEL_CACHE, else /opt/otzaria-cache/vector-models) and verified against the
plan's package checksum. The fetch authenticates with the token in $OTZARIA_HF_TOKEN (see
--token-env) when it is set; the token is never printed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import model_package  # noqa: E402
import vector_shard  # noqa: E402

WORKER_NAME = "seforim-gpu-worker"
WORKER_VERSION = "1.0"
MAX_TOKENS = 256          # the cap the worker truncates to, held to the family's
POOLING = "in-graph"      # the graph pools; so does the re-implementation
GOLDEN_TEXTS = HERE / "parity_golden_texts.json"


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", file=sys.stderr, flush=True)


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--plan", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--skip", type=int, default=0)
    ap.add_argument("--take", type=int, default=None, help="default: all that remain")
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--model-dir", type=Path, help="a package directory: the graph and tokenizer.json")
    src.add_argument("--cache", type=Path, default=None,
                     help="fetch the plan's package into this cache (default $OTZARIA_VECTOR_MODEL_CACHE "
                          "or /opt/otzaria-cache/vector-models)")
    ap.add_argument("--repo", default=model_package.DEFAULT_REPO)
    ap.add_argument("--revision", default=model_package.DEFAULT_REVISION)
    ap.add_argument("--token-env", default=model_package.DEFAULT_TOKEN_ENV)
    ap.add_argument("--graph", default="seforim-embed-round2-fp32.onnx")
    ap.add_argument("--mode", default="mixed", choices=("mixed", "fp32"))
    ap.add_argument("--device", default="cuda")
    ap.add_argument("--token-budget", type=int, default=32768)
    ap.add_argument("--max-batch", type=int, default=1024)
    ap.add_argument("--chunk", type=int, default=65536, help="records read, embedded and written at a time")
    ap.add_argument("--parity-samples", type=int, default=1000,
                    help=f"texts in the parity certificate, golden texts included (at least {vector_shard.PARITY_MIN_SAMPLES})")
    ap.add_argument("--parity-procs", type=int, default=0, help="ONNX Runtime reference processes (default: CPUs, at most 16)")
    a = ap.parse_args(argv)
    if a.skip < 0 or (a.take is not None and a.take < 0):
        ap.error("--skip and --take are counts")
    if a.parity_samples < vector_shard.PARITY_MIN_SAMPLES:
        ap.error(f"--parity-samples must be at least {vector_shard.PARITY_MIN_SAMPLES}")
    return a


def resolve_package(a, plan) -> Path:
    if a.model_dir is not None:
        return a.model_dir
    cache = a.cache or Path(os.environ.get("OTZARIA_VECTOR_MODEL_CACHE") or "/opt/otzaria-cache/vector-models")
    return model_package.fetch_package(cache, plan["passage_package"]["checksum"], a.graph, repo=a.repo,
                                       revision=a.revision, token_env=a.token_env, log=log)


def main(argv=None) -> int:
    a = parse_args(argv)
    t0 = time.time()
    plan = vector_shard.read_embed_manifest(a.plan)
    package = plan["passage_package"]
    if package["quantization"] != "fp32":
        raise vector_shard.ContractError(f"the plan's passage package is {package['quantization']}; this worker "
                                         f"re-implements the fp32 graph only")
    take = plan["records"] - a.skip if a.take is None else a.take
    take = max(0, take)
    vector_shard.ensure_output_dir(a.out)         # refuse a finished shard before any work
    root = resolve_package(a, plan)
    graph = str(root / a.graph)

    import numpy as np
    import torch
    import reference
    import torch_bert
    torch.backends.cuda.matmul.allow_tf32 = False
    tok = reference.load_tokenizer(str(root / model_package.TOKENIZER_FILE), MAX_TOKENS)
    model = torch_bert.MeivinTorch(torch_bert.load_weights(graph), mode=a.mode, device=a.device)
    model_package.check_family(root, a.graph, plan["model"], package, model.dim, POOLING, MAX_TOKENS)
    log(f"package {package['checksum'][:12]} ({package['quantization']}) holds the family's tokenizer, "
        f"width {model.dim}, pooling {POOLING}, cap {MAX_TOKENS}")
    on_gpu = a.device.startswith("cuda")
    worker = {"name": WORKER_NAME, "version": WORKER_VERSION,
              "device": (torch.cuda.get_device_name(0) if on_gpu else "cpu"),
              "ep": ("rocm" if getattr(torch.version, "hip", None) else "cuda") if on_gpu else "cpu",
              "mode": f"torch-{a.mode}"}

    def embed(texts):
        ids = [e.ids for e in tok.encode_batch(texts, add_special_tokens=True)]
        return torch_bert.embed_id_lists(model, ids, a.token_budget, a.max_batch)

    timing = {}

    def parity(plan_):
        tp = time.time()
        golden = json.load(open(GOLDEN_TEXTS, encoding="utf-8"))["cases"]
        n_plan = a.parity_samples - len(golden)
        positions = vector_shard.sample_positions(plan_["plan_sha256"], plan_["records"], n_plan)
        sampled = vector_shard.read_positions(a.plan, positions)
        texts = [c["input"] for c in golden] + [r.text for r in sampled]
        ids = [e.ids for e in tok.encode_batch(texts, add_special_tokens=True)]
        mine = torch_bert.embed_id_lists(model, ids, a.token_budget, a.max_batch)
        log(f"parity: {len(texts)} texts through ONNX Runtime {reference.version()} on the CPU")
        ref = reference.embed_reference(graph, ids, a.parity_procs)
        cos = reference.cosines(mine, ref)
        cert = {"reference": f"onnxruntime {reference.version()} cpu {package['quantization']}",
                "samples": int(len(texts)), "min_cosine": float(cos.min()), "mean_cosine": float(cos.mean())}
        doc = {"format": "seforim-gpu-worker-parity", "version": 1, "plan_sha256": plan_["plan_sha256"],
               "passage_package": package, "worker": worker, "reference": cert["reference"],
               "golden_texts": {"source": json.load(open(GOLDEN_TEXTS, encoding="utf-8"))["source"],
                                "cases": [{"name": c["name"], "input_utf8_sha256": c["input_utf8_sha256"],
                                           "cosine": float(x)} for c, x in zip(golden, cos[:len(golden)])]},
               "plan_samples": [{"position": r.position, "embedding_text_sha256": r.digest.hex(), "cosine": float(x)}
                                for r, x in zip(sampled, cos[len(golden):])],
               "min_cosine": cert["min_cosine"], "mean_cosine": cert["mean_cosine"],
               "quantiles": {str(q): float(np.quantile(cos, q)) for q in (0.001, 0.01, 0.5)}}
        log(f"parity: min cosine {cert['min_cosine']:.9f}, mean {cert['mean_cosine']:.9f}")
        timing["parity_s"] = time.time() - tp
        return cert, doc

    t1 = time.time()
    manifest = vector_shard.write_shard(a.plan, a.out, a.skip, take, embed, worker, parity_fn=parity,
                                        chunk=a.chunk, log=log)
    t2 = time.time()
    summary = {"out": str(a.out), "skip": a.skip, "take": take, "records": manifest["records"],
               "vectors_sha256": manifest["vectors_sha256"], "keys_sha256": manifest["keys_sha256"],
               "parity": manifest["parity"], "worker": worker,
               "load_s": round(t1 - t0, 1), "parity_s": round(timing.get("parity_s", 0.0), 1),
               "embed_s": round(t2 - t1 - timing.get("parity_s", 0.0), 1),
               "vec_per_s": round(manifest["records"] / max(t2 - t1 - timing.get("parity_s", 0.0), 1e-9), 1)}
    print(json.dumps(summary), flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (vector_shard.ContractError, model_package.PackageError) as error:
        print(f"seforim-gpu-worker: {error}", file=sys.stderr)
        sys.exit(2)
