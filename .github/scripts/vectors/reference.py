"""The parity reference: the shipped ONNX graph on ONNX Runtime's CPU execution provider,
configured as the sidecar's OnnxBackend configures it (one text per run, input_ids and an
all-ones attention_mask as int64 [1, len], graph optimization ALL, one inter-op thread,
sequential execution, no spinning), with the tokenizer configured as it is (padding off,
truncation to max_tokens, special tokens added)."""

from __future__ import annotations

import multiprocessing as mp
import os
from typing import List, Sequence

import numpy as np


def load_tokenizer(path: str, max_tokens: int):
    from tokenizers import Tokenizer
    tok = Tokenizer.from_file(path)
    tok.no_padding()
    tok.enable_truncation(max_length=max_tokens, stride=0, strategy="longest_first", direction="right")
    if tok.encode_special_tokens:
        raise ValueError("tokenizer.json turns encode_special_tokens on; the role prefixes would be spelled out")
    return tok


def session(graph: str, threads: int = 1):
    import onnxruntime as ort
    so = ort.SessionOptions()
    so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    so.intra_op_num_threads = threads
    so.inter_op_num_threads = 1
    so.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
    so.add_session_config_entry("session.intra_op.allow_spinning", "0")
    return ort.InferenceSession(graph, sess_options=so, providers=["CPUExecutionProvider"])


def run_ids(sess, ids: Sequence[int]) -> np.ndarray:
    a = np.asarray(ids, dtype=np.int64)[None, :]
    return sess.run(None, {"input_ids": a, "attention_mask": np.ones_like(a)})[0][0]


_STATE = {}


def _init(graph):
    _STATE["sess"] = session(graph, 1)


def _one(ids):
    return run_ids(_STATE["sess"], ids)


def embed_reference(graph: str, id_lists: List[List[int]], procs: int = 0) -> np.ndarray:
    """ONNX Runtime CPU vectors for already-tokenized texts, `procs` single-thread sessions."""
    procs = procs or min(16, os.cpu_count() or 1)
    if procs <= 1 or len(id_lists) < 64:
        s = session(graph, 1)
        return np.stack([run_ids(s, ids) for ids in id_lists]).astype(np.float32)
    ctx = mp.get_context("fork")
    with ctx.Pool(procs, initializer=_init, initargs=(graph,)) as pool:
        return np.stack(pool.map(_one, id_lists, chunksize=8)).astype(np.float32)


def version() -> str:
    import onnxruntime as ort
    return ort.__version__


def cosines(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    a = a.astype(np.float64); b = b.astype(np.float64)
    return (a * b).sum(1) / (np.linalg.norm(a, axis=1) * np.linalg.norm(b, axis=1))
