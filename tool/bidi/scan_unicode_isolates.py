#!/usr/bin/env python3
"""Count UAX #9 isolate controls in a released Otzaria/Seforim SQLite snapshot.

The main text, edition overrides and bundled notes may use zstd frames with a
dictionary embedded in the snapshot. Count controls only after decoding those
frames. The metadata population covers selected bibliographic fields and TOC
labels. This scans stored codepoints, including those in HTML markup.
"""
from __future__ import annotations

import argparse
import collections
from contextlib import closing
import ctypes
import ctypes.util
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import time

ISOLATES = {
    "\u2066".encode(): "LRI",
    "\u2067".encode(): "RLI",
    "\u2068".encode(): "FSI",
    "\u2069".encode(): "PDI",
}
OTHER = {
    "\u202a".encode(): "LRE",
    "\u202b".encode(): "RLE",
    "\u202c".encode(): "PDF",
    "\u202d".encode(): "LRO",
    "\u202e".encode(): "RLO",
    "\u200e".encode(): "LRM",
    "\u200f".encode(): "RLM",
}
BIDI_CONTROL = re.compile(b"|".join((*ISOLATES, *OTHER)))
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
METADATA_TABLES = (
    "book", "book_version", "source", "category", "author", "topic",
    "pub_place", "pub_date", "banner", "tocEntry", "tocText", "alt_toc_structure",
)


def load_zstd():
    name = os.environ.get("LIBZSTD_PATH") or ctypes.util.find_library("zstd")
    if not name and sys.platform == "darwin":
        name = next(
            (str(p) for p in (
                Path("/opt/homebrew/lib/libzstd.dylib"),
                Path("/usr/local/lib/libzstd.dylib"),
            ) if p.is_file()),
            "libzstd.dylib",
        )
    try:
        return ctypes.CDLL(name or ("libzstd.dll" if sys.platform == "win32" else "libzstd.so.1"))
    except OSError as exc:
        raise RuntimeError("Install libzstd or set LIBZSTD_PATH to its path") from exc


class Decoder:
    """Reuse a zstd context and a prepared dictionary across all frames."""

    def __init__(self, dictionary: bytes | None):
        self.lib = load_zstd()
        pointer = ctypes.c_void_p
        size = ctypes.c_size_t
        self.lib.ZSTD_createDCtx.argtypes = []
        self.lib.ZSTD_createDCtx.restype = pointer
        self.lib.ZSTD_freeDCtx.argtypes = [pointer]
        self.lib.ZSTD_freeDCtx.restype = size
        self.lib.ZSTD_createDDict.argtypes = [pointer, size]
        self.lib.ZSTD_createDDict.restype = pointer
        self.lib.ZSTD_freeDDict.argtypes = [pointer]
        self.lib.ZSTD_freeDDict.restype = size
        self.lib.ZSTD_getFrameContentSize.argtypes = [pointer, size]
        self.lib.ZSTD_getFrameContentSize.restype = ctypes.c_ulonglong
        self.lib.ZSTD_decompressDCtx.argtypes = [pointer, pointer, size, pointer, size]
        self.lib.ZSTD_decompressDCtx.restype = size
        self.lib.ZSTD_decompress_usingDDict.argtypes = [
            pointer, pointer, size, pointer, size, pointer,
        ]
        self.lib.ZSTD_decompress_usingDDict.restype = size
        self.lib.ZSTD_isError.argtypes = [size]
        self.lib.ZSTD_isError.restype = ctypes.c_uint
        self.lib.ZSTD_getErrorName.argtypes = [size]
        self.lib.ZSTD_getErrorName.restype = ctypes.c_char_p

        self.ddict = None
        self.ctx = self.lib.ZSTD_createDCtx()
        if not self.ctx:
            raise RuntimeError("ZSTD_createDCtx failed")
        if dictionary:
            self.ddict = self.lib.ZSTD_createDDict(dictionary, len(dictionary))
            if not self.ddict:
                self.close()
                raise RuntimeError("ZSTD_createDDict failed")
        self.frames = 0

    def close(self):
        if self.ctx:
            self.lib.ZSTD_freeDCtx(self.ctx)
            self.ctx = None
        if self.ddict:
            self.lib.ZSTD_freeDDict(self.ddict)
            self.ddict = None

    def decode(self, value: str | bytes) -> bytes:
        if isinstance(value, str):
            return value.encode("utf-8")
        if not value.startswith(ZSTD_MAGIC):
            return value
        uncompressed = self.lib.ZSTD_getFrameContentSize(value, len(value))
        if uncompressed > 16 * 1024 * 1024:
            raise ValueError(f"Invalid/unknown zstd decoded size: {uncompressed}")
        dst = ctypes.create_string_buffer(max(1, uncompressed))
        if self.ddict:
            res = self.lib.ZSTD_decompress_usingDDict(
                self.ctx, dst, uncompressed, value, len(value), self.ddict,
            )
        else:
            res = self.lib.ZSTD_decompressDCtx(self.ctx, dst, uncompressed, value, len(value))
        if self.lib.ZSTD_isError(res):
            raise RuntimeError(f"zstd decompression failed: {self.lib.ZSTD_getErrorName(res).decode()}")
        if res != uncompressed:
            raise RuntimeError(f"zstd decoded {res} bytes, expected {uncompressed}")
        self.frames += 1
        return dst.raw[:res]


def make_stats():
    return {
        "rows_examined": 0,
        "text_values_examined": 0,
        "rows_with_any_isolate": 0,
        "utf8_bytes_examined": 0,
        "character_occurrences": collections.Counter(),
        "other_bidi_controls": collections.Counter(),
    }


def tally_row(stats, values, decoder):
    stats["rows_examined"] += 1
    found = False
    for value in values:
        if value is None:
            continue
        raw = decoder.decode(value)
        stats["text_values_examined"] += 1
        stats["utf8_bytes_examined"] += len(raw)
        for encoded in BIDI_CONTROL.findall(raw):
            if encoded in ISOLATES:
                stats["character_occurrences"][ISOLATES[encoded]] += 1
                found = True
            else:
                stats["other_bidi_controls"][OTHER[encoded]] += 1
    if found:
        stats["rows_with_any_isolate"] += 1
    return found


def scan_text(conn, query, name, decoder, positive_lines, progress, started):
    stats = make_stats()
    for line_id, content in conn.execute(query):
        if tally_row(stats, (content,), decoder):
            positive_lines[line_id] += 1
        if progress and stats["rows_examined"] % progress == 0:
            print(f"{name}: {stats['rows_examined']:,} rows, "
                  f"{stats['rows_with_any_isolate']:,} with isolates, "
                  f"elapsed={time.monotonic()-started:.1f}s", flush=True)
    return stats


def scan_metadata(conn, tables, decoder):
    metadata = make_stats()
    counts = {}
    for table in METADATA_TABLES:
        if table not in tables:
            continue
        names = [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')
                 if "TEXT" in (r[2] or "").upper() and r[1] != "content"]
        if not names:
            continue
        columns = ", ".join('"' + name.replace('"', '""') + '"' for name in names)
        stats = make_stats()
        for row in conn.execute(f'SELECT {columns} FROM "{table}"'):
            tally_row(stats, row, decoder)
        counts[table] = stats
        for key in ("rows_examined", "text_values_examined", "rows_with_any_isolate",
                    "utf8_bytes_examined"):
            metadata[key] += stats[key]
        for key in ("character_occurrences", "other_bidi_controls"):
            metadata[key].update(stats[key])
    return metadata, counts


def summarize_books(conn, positive_lines):
    book_hits = collections.Counter()
    line_ids = list(positive_lines)
    for offset in range(0, len(line_ids), 500):
        batch = line_ids[offset:offset + 500]
        placeholders = ", ".join("?" for _ in batch)
        for line_id, book_id in conn.execute(
            f"SELECT id, bookId FROM line WHERE id IN ({placeholders})", batch,
        ):
            book_hits[book_id] += positive_lines[line_id]

    samples = []
    sources = collections.Counter()
    positive_sources = collections.Counter()
    for book_id, title, source in conn.execute(
        "SELECT b.id, b.title, s.name FROM book b LEFT JOIN source s ON s.id=b.sourceId",
    ):
        sources[source] += 1
        if book_id in book_hits:
            positive_sources[source] += 1
            samples.append({"book_id": book_id, "title": title, "source": source,
                            "positive_rows": book_hits[book_id]})
    samples.sort(key=lambda book: (-book["positive_rows"], book["book_id"]))
    return {
        "books_in_db": sum(sources.values()),
        "books_with_any_isolate_in_text": len(book_hits),
        "books_by_source": dict(sources),
        "positive_books_by_source": dict(positive_sources),
        "matching_book_samples": samples[:20],
    }


def scan_db(path: str, progress: int = 200_000):
    # The release input is a standalone, immutable snapshot without sidecars.
    uri = Path(path).resolve().as_uri() + "?mode=ro&immutable=1"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        conn.execute("PRAGMA query_only = ON")
        conn.execute("PRAGMA mmap_size = 268435456")
        conn.execute("PRAGMA cache_size = -65536")
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        if not {"line", "book", "source"}.issubset(tables):
            raise RuntimeError("Expected Otzaria line, book and source tables")
        dicts = list(conn.execute("SELECT dict FROM zstd_dict")) if "zstd_dict" in tables else []
        if len(dicts) > 1:
            raise RuntimeError(f"Multiple zstd dictionaries: {len(dicts)}; check frame dictionary IDs")
        main_table = "line_content" if "line_content" in tables else "line"
        columns = {r[1] for r in conn.execute(f'PRAGMA table_info("{main_table}")')}
        pk = "id" if "id" in columns else "lineId"
        if pk not in columns or "content" not in columns:
            raise RuntimeError(f"Unrecognized {main_table} columns: {sorted(columns)}")
        text_tables = {
            "main_text": (main_table, pk, "content"),
            "edition_overlays": ("version_line", "lineId", "content"),
            "bundled_notes": ("line_note", "lineId", "body"),
        }
        started = time.monotonic()
        positive_lines = collections.Counter()
        counters = {}
        tables_examined = []
        with closing(Decoder(dicts[0][0] if dicts else None)) as decoder:
            for name, (table, line_id, text) in text_tables.items():
                if table not in tables:
                    counters[name] = None
                    continue
                query = f'SELECT t."{line_id}", t."{text}" FROM "{table}" AS t'
                counters[name] = scan_text(conn, query, name, decoder, positive_lines, progress, started)
                tables_examined.append(table)

            metadata, metadata_tables = scan_metadata(conn, tables, decoder)
            result = {
                "database": Path(path).name,
                "schema_version": dict(conn.execute("SELECT key,value FROM schema_meta")).get("db_schema_version")
                    if "schema_meta" in tables else None,
                "tables_examined": [*tables_examined, *metadata_tables],
                **summarize_books(conn, positive_lines),
                "zstd_dictionary_rows": len(dicts),
                "zstd_frames_decoded": decoder.frames,
                "duration_seconds": round(time.monotonic() - started, 2),
                **counters,
                "metadata": metadata,
                "metadata_tables": metadata_tables,
                "interpretation": (
                    "Counts stored codepoints in main lines, edition overrides, bundled notes, "
                    "selected bibliographic metadata and TOC labels. Row counts include SQL NULL "
                    "edition records, which inherit main text. Text value counts exclude NULL. "
                    "Book samples count positive storage rows in main lines, editions and notes. "
                    "The census covers these stored populations only."
                ),
            }
    return result


def compress_test_frame(raw, dictionary):
    """Produce a frame for the self-test using libzstd's one-shot encoder."""
    lib = load_zstd()
    pointer = ctypes.c_void_p
    size = ctypes.c_size_t
    lib.ZSTD_createCCtx.argtypes = []
    lib.ZSTD_createCCtx.restype = pointer
    lib.ZSTD_freeCCtx.argtypes = [pointer]
    lib.ZSTD_freeCCtx.restype = size
    lib.ZSTD_compressBound.argtypes = [size]
    lib.ZSTD_compressBound.restype = size
    lib.ZSTD_compress_usingDict.argtypes = [pointer, pointer, size, pointer, size, pointer, size, ctypes.c_int]
    lib.ZSTD_compress_usingDict.restype = size
    lib.ZSTD_isError.argtypes = [size]
    lib.ZSTD_isError.restype = ctypes.c_uint
    ctx = lib.ZSTD_createCCtx()
    if not ctx:
        raise RuntimeError("ZSTD_createCCtx failed")
    try:
        dst = ctypes.create_string_buffer(lib.ZSTD_compressBound(len(raw)))
        result = lib.ZSTD_compress_usingDict(ctx, dst, len(dst), raw, len(raw), dictionary, len(dictionary), 3)
        if lib.ZSTD_isError(result):
            raise RuntimeError("Test frame compression failed")
        return dst.raw[:result]
    finally:
        lib.ZSTD_freeCCtx(ctx)


def self_test():
    dictionary = bytes(range(32, 127)) * 8
    plain = bytes(range(40, 120))
    frame = compress_test_frame(plain + "\u2066abc\u2069".encode(), dictionary)
    # This fixture must actually reference the dictionary during decoding.
    with closing(Decoder(None)) as decoder:
        try:
            decoder.decode(frame)
        except RuntimeError:
            pass
        else:
            raise AssertionError("Test frame decoded without the dictionary")
        assert decoder.decode(compress_test_frame(plain, b"")) == plain

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "test#1%.db"
        with closing(sqlite3.connect(path)) as db:
            db.executescript("""
                CREATE TABLE book(id INTEGER PRIMARY KEY, title TEXT, sourceId INTEGER, heRef TEXT);
                CREATE TABLE source(id INTEGER PRIMARY KEY, name TEXT);
                CREATE TABLE line(id INTEGER PRIMARY KEY, bookId INTEGER, heRef TEXT);
                CREATE TABLE line_content(id INTEGER PRIMARY KEY, content BLOB);
                CREATE TABLE version_line(lineId INTEGER, content BLOB);
                CREATE TABLE line_note(lineId INTEGER, body BLOB);
                CREATE TABLE tocText(id INTEGER PRIMARY KEY, text TEXT);
                CREATE TABLE zstd_dict(id INTEGER PRIMARY KEY, dict BLOB);
                INSERT INTO source VALUES (1, 'sample');
                INSERT INTO line VALUES (10,1,'ref'), (11,2,'ref2'), (12,3,'ref3');
                INSERT INTO line_content VALUES (11,x'616263'), (12,'plain text');
                INSERT INTO version_line VALUES (10, NULL);
            """)
            db.executemany("INSERT INTO book VALUES (?, ?, ?, ?)", [
                (1, "שלום\u2067", 1, "ref\u2069"), (2, "Notes only", 1, None),
                (3, "Metadata only\u2066", 1, None),
            ])
            db.execute("INSERT INTO zstd_dict VALUES (1, ?)", (dictionary,))
            db.execute("INSERT INTO line_content VALUES (10, ?)", (frame,))
            db.executemany("INSERT INTO version_line VALUES (10, ?)", [
                (frame,), (compress_test_frame(plain + "\u2068test\u2069".encode(), dictionary),),
            ])
            db.execute("INSERT INTO line_note VALUES (11, ?)",
                       (compress_test_frame(plain + "\u2067note\u2069\u200e".encode(), dictionary),))
            db.execute("INSERT INTO tocText VALUES (1, ?)", ("toc\u2069",))
            db.commit()
        before = path.read_bytes()
        report = scan_db(path, progress=0)
        assert path.read_bytes() == before
        assert list(Path(tmp).iterdir()) == [path]
        assert report["zstd_frames_decoded"] == 4
        assert report["main_text"]["character_occurrences"] == {"LRI": 1, "PDI": 1}
        assert report["main_text"]["rows_examined"] == 3
        assert report["edition_overlays"]["character_occurrences"] == {"LRI": 1, "FSI": 1, "PDI": 2}
        assert report["edition_overlays"]["rows_examined"] == 3
        assert report["edition_overlays"]["text_values_examined"] == 2
        assert report["bundled_notes"]["character_occurrences"] == {"RLI": 1, "PDI": 1}
        assert report["bundled_notes"]["other_bidi_controls"] == {"LRM": 1}
        assert report["metadata_tables"]["tocText"]["character_occurrences"] == {"PDI": 1}
        assert report["metadata_tables"]["book"]["rows_examined"] == 3
        assert report["metadata_tables"]["book"]["text_values_examined"] == 4
        assert report["metadata_tables"]["book"]["rows_with_any_isolate"] == 2
        assert report["books_in_db"] == 3 and report["books_with_any_isolate_in_text"] == 2
        assert report["positive_books_by_source"] == {"sample": 2}
        assert [(b["book_id"], b["positive_rows"]) for b in report["matching_book_samples"]] == [(1, 3), (2, 1)]
    print("Census self-test passed")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("database", nargs="?")
    p.add_argument("--out", default="bidi-isolate-counts.json")
    p.add_argument("--self-test", action="store_true")
    a = p.parse_args()
    if a.self_test:
        self_test()
        return
    if not a.database:
        p.error("database path is required")
    database = Path(a.database).resolve()
    destination = Path(a.out)
    if destination.resolve() == database or (destination.exists() and database.exists()
                                              and destination.samefile(database)):
        p.error("report output must differ from the database")
    output = scan_db(a.database)
    formatted = json.dumps(output, ensure_ascii=False, indent=2)
    destination.write_text(formatted + "\n", encoding="utf-8")
    print(formatted, flush=True)


if __name__ == "__main__":
    main()
