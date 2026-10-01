#!/usr/bin/env python3
"""Trains the line-text zstd dictionary (generator/common/src/jvmMain/resources/zstd/line_content.zdict).

The dictionary is frozen: retrain it only together with a full-rebase schema bump,
then update LineContentCompression.DICT_SHA256 and the frozen-frames test.

Requires `pip install zstandard` (bundles zstd 1.5.7, the version zstd-jni 1.5.7-7 ships).

    python train_line_content_dict.py path/to/seforim.db out.zdict

Measured on v30 (schema 6), held-out 50K rows at level 19: no dictionary 2.54x,
64K 3.44x, 256K 3.58x, 1M 3.72x, 2M 3.80x; level 22 adds nothing over 19.
"""
import random
import sqlite3
import sys

import zstandard

DICT_SIZE = 2 * 1024 * 1024
TRAIN_ROWS = 200_000
SEED = 1


def main(db_path: str, out_path: str) -> None:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.text_factory = bytes
    max_id = conn.execute("SELECT MAX(id) FROM line_content").fetchone()[0]
    rnd = random.Random(SEED)
    ids = sorted({rnd.randint(1, max_id) for _ in range(int(TRAIN_ROWS * 1.3))})
    samples = []
    for i in range(0, len(ids), 900):
        chunk = ids[i:i + 900]
        sql = "SELECT CAST(content AS BLOB) FROM line_content WHERE typeof(content) = 'text' AND id IN (%s)"
        samples += [r[0] for r in conn.execute(sql % ",".join("?" * len(chunk)), chunk)]
    rnd.shuffle(samples)
    samples = samples[:TRAIN_ROWS]
    d = zstandard.train_dictionary(DICT_SIZE, samples, k=2000, d=8, f=20, accel=1, threads=-1, level=19)
    with open(out_path, "wb") as f:
        f.write(d.as_bytes())
    print(f"{len(samples)} samples -> {out_path} (dict id {d.dict_id()})")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
