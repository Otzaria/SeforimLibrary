#!/usr/bin/env python3
"""Trains the line-text zstd dictionary (generator/common/src/jvmMain/resources/zstd/line_content.zdict).

The dictionary is frozen: retrain it only together with a full-rebase schema bump,
then update LineContentCompression.DICT_SHA256 and the frozen-frames test.

Requires `pip install zstandard` (bundles zstd 1.5.7, the version zstd-jni 1.5.7-7 ships).

    python train_line_content_dict.py path/to/seforim.db out.zdict

Measured on v30 (schema 6), held-out 50K rows at level 19: no dictionary 2.54x,
64K 3.44x, 256K 3.58x, 1M 3.72x, 2M 3.80x; level 22 adds nothing over 19.

Provenance of the frozen line_content.zdict (dict id 32768 = 0x8000,
SHA-256 7b08b7c3...a396). Recorded, in the commit that added it (1b991830) and in
SL PR #62: 2MB, fastcover k=2000 d=8, 200K random line_content rows of a v30
(schema 6) seforim.db, level 19. Not recorded anywhere in the repo: which v30 file
(build), the zstandard / Python versions, and f, accel, seed or sample selection.

This script does NOT reproduce it. Run 2026-10-02 on a local v30 (db_version 30,
db_schema_version 6, 6,908,090 TEXT rows) with python-zstandard 0.25.0 (zstd 1.5.7)
on Python 3.14.4, Windows x64: 198,177 samples, dict id 2129175477, SHA-256
f3eb1b97a74d9c604d3ae9d6fdc0382fbc7f8374df1271e0813661bc69c06d62. Treat the
committed file as the source of truth; this script is the method, not a rebuild.

Dict id re-issued before the format froze: the trained file had id 908519771
(0x3626e95b, SHA-256 1e5c9d66...3f6c). Only header bytes 4-7 (the little-endian id)
were rewritten to 32768; the other 2,097,148 bytes are unchanged, and so is every
frame body. An id in 32768..65535 takes a 2-byte Dictionary_ID field in each frame
header instead of 4 (<=32767 and >=2^31 are reserved by zdict.h): 2 bytes per row,
~14.6MB over 7.3M frames. DICT_ID below makes a retrain use the same id.
"""
import random
import sqlite3
import sys

import zstandard

DICT_SIZE = 2 * 1024 * 1024
TRAIN_ROWS = 200_000
SEED = 1
DICT_ID = 32768


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
    d = zstandard.train_dictionary(DICT_SIZE, samples, k=2000, d=8, f=20, accel=1, dict_id=DICT_ID, threads=-1, level=19)
    with open(out_path, "wb") as f:
        f.write(d.as_bytes())
    print(f"{len(samples)} samples -> {out_path} (dict id {d.dict_id()})")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
