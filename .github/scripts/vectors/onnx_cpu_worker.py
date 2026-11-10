"""Persistent bounded CPU sessions; the shipped graph runs one text at a time.

Spawn avoids forking an already-threaded ONNX/tokenizer process. Each child's
provider, graph optimizations and thread settings match the application backend.
No Torch import, GPU calls, weight conversion or cross-text activation ranges.
"""
import multiprocessing as mp
import numpy as np
import reference

_SESSION = None


def initialize(graph, threads):
    global _SESSION
    _SESSION = reference.session(graph, threads)
    if _SESSION.get_providers() != ['CPUExecutionProvider']:
        raise RuntimeError('Exact worker must use CPUExecutionProvider only')


def encode_one(ids):
    return reference.run_ids(_SESSION, ids)


class ExactOnnxPool:
    def __init__(self, graph, processes=8, threads=2):
        if processes < 1 or threads < 1:
            raise ValueError('Positive process/thread counts required')
        self.graph, self.processes, self.threads = graph, processes, threads
        self.pool = None

    def __enter__(self):
        self.pool = mp.get_context('spawn').Pool(self.processes, initializer=initialize,
                                               initargs=(self.graph,self.threads))
        return self

    def embed(self, ids):
        if not ids:
            return np.empty((0,384),dtype=np.float32)
        return np.stack(self.pool.map(encode_one, ids, chunksize=8)).astype(np.float32)

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self.pool.terminate()
        else:
            self.pool.close()
        self.pool.join()
