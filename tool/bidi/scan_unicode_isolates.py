#!/usr/bin/env python3
"""Measure UAX #9 isolate controls in a released Otzaria/Seforim SQLite DB.

Only reads the database. Never treats compressed bytes as Unicode text.
Schema 7 stores line_content as independent zstd frames with a dictionary
embedded in the zstd_dict table; Python's standard library uses system libzstd.
Reports main text, edition overlays, and metadata as distinct populations.
"""
from __future__ import annotations

import argparse
import collections
import ctypes
import ctypes.util
import json
import os
import sqlite3
import sys
import tempfile
import time

ISOLATES = {
    "LRI": "\u2066".encode("utf-8"),
    "RLI": "\u2067".encode("utf-8"),
    "FSI": "\u2068".encode("utf-8"),
    "PDI": "\u2069".encode("utf-8"),
}
OTHER = {
    "LRE": "\u202a".encode("utf-8"),
    "RLE": "\u202b".encode("utf-8"),
    "PDF": "\u202c".encode("utf-8"),
    "LRO": "\u202d".encode("utf-8"),
    "RLO": "\u202e".encode("utf-8"),
    "LRM": "\u200e".encode("utf-8"),
    "RLM": "\u200f".encode("utf-8"),
}
PATTERNS = {**ISOLATES, **OTHER}
PREFIX = b"\x28\xb5\x2f\xfd"


class Decoder:
    """Reusable dictionary-aware zstd context without pip dependencies."""

    def __init__(self, dictionary: bytes | None):
        name = ctypes.util.find_library("zstd") or "libzstd.so.1"
        self.lib = ctypes.CDLL(name)
        self.lib.ZSTD_createDCtx.restype = ctypes.c_void_p
        self.lib.ZSTD_freeDCtx.argtypes = [ctypes.c_void_p]
        self.lib.ZSTD_getFrameContentSize.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        self.lib.ZSTD_getFrameContentSize.restype = ctypes.c_ulonglong
        self.lib.ZSTD_decompress_usingDict.argtypes = [
            ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t,
            ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t
        ]
        self.lib.ZSTD_decompress_usingDict.restype = ctypes.c_size_t
        self.lib.ZSTD_isError.argtypes = [ctypes.c_size_t]
        self.lib.ZSTD_isError.restype = ctypes.c_uint
        self.ctx = self.lib.ZSTD_createDCtx()
        if not self.ctx:
            raise RuntimeError("ZSTD_createDCtx failed")
        self.dictionary = dictionary
        self.dictionary_buffer = ctypes.create_string_buffer(dictionary) if dictionary else None
        self.frames = 0

    def close(self):
        if self.ctx:
            self.lib.ZSTD_freeDCtx(self.ctx)
            self.ctx = None

    def decode(self, value) -> bytes:
        # An absent edition override is stored as SQL NULL; it is not text.
        if value is None:
            return b""
        if isinstance(value, str):
            return value.encode("utf-8")
        data = bytes(value)
        if not data.startswith(PREFIX):
            return data
        src = ctypes.create_string_buffer(data)
        uncompressed = self.lib.ZSTD_getFrameContentSize(src, len(data))
        if uncompressed > 16 * 1024 * 1024:
            raise ValueError(f"Invalid/unknown zstd decoded size: {uncompressed}")
        dst = ctypes.create_string_buffer(max(1, uncompressed))
        res = self.lib.ZSTD_decompress_usingDict(
            self.ctx, dst, uncompressed, src, len(data),
            self.dictionary_buffer, len(self.dictionary or b""),
        )
        if self.lib.ZSTD_isError(res) or res != uncompressed:
            raise RuntimeError(f"ZSTD_decompress_usingDict failed for frame ({res=} {uncompressed=})")
        self.frames += 1
        return dst.raw[:res]


def fields(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def make_stats():
    return {
        "rows_examined": 0,
        "rows_with_any_isolate": 0,
        "utf8_bytes_examined": 0,
        "character_occurrences": collections.Counter(),
        "other_bidi_controls": collections.Counter(),
    }


def tally(stats, raw):
    stats["rows_examined"] += 1
    stats["utf8_bytes_examined"] += len(raw)
    found = False
    for name, encoded in ISOLATES.items():
        occurrences = raw.count(encoded)
        if occurrences:
            stats["character_occurrences"][name] += occurrences
            found = True
    if found:
        stats["rows_with_any_isolate"] += 1
    for name, encoded in OTHER.items():
        count = raw.count(encoded)
        if count:
            stats["other_bidi_controls"][name] += count
    return found


def scan_db(path: str, progress: int = 200_000):
    uri = "file:" + os.path.abspath(path) + "?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA mmap_size = 268435456")
    conn.execute("PRAGMA cache_size = -65536")
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    if "line" not in tables or "book" not in tables:
        raise RuntimeError("Expected Otzaria book/line tables not found")
    dicts = []
    if "zstd_dict" in tables:
        dicts = list(conn.execute("SELECT id, dict FROM zstd_dict"))
    if len(dicts) > 1:
        raise RuntimeError(f"Multiple zstd dictionaries: {len(dicts)}; check frame dictionary ids")
    decoder = Decoder(dicts[0][1] if dicts else None)

    counters = {}
    found_rows = {}
    total = time.monotonic()
    try:
        if "line_content" in tables:
            c = fields(conn, "line_content")
            pk = "id" if "id" in c else "lineId"
            if pk not in c or "content" not in c:
                raise RuntimeError(f"Unrecognized line_content columns: {sorted(c)}")
            main_query = f"SELECT {pk}, content FROM line_content"
            main_table = "line_content"
        else:
            c = fields(conn, "line")
            if "content" not in c:
                raise RuntimeError(f"No text content in line: {sorted(c)}")
            main_query = "SELECT id, content FROM line"
            main_table = "line"

        for table, query, id_name in [
            (main_table, main_query, "line"),
            ("version_line", "SELECT lineId, content FROM version_line", "edition"),
        ]:
            if table not in tables:
                continue
            stats = make_stats()
            counters[id_name] = stats
            matches = []
            for key, content in conn.execute(query):
                raw = decoder.decode(content)
                if tally(stats, raw):
                    matches.append(key)
                if progress and stats["rows_examined"] % progress == 0:
                    print(f"{id_name}: {stats['rows_examined']:,} records, "
                          f"{len(matches):,} isolate-positive rows, elapsed={time.monotonic()-total:.1f}s",
                          flush=True)
            found_rows[id_name] = matches
            print(f"COMPLETED {id_name}: {stats['rows_examined']:,} records, "
                  f"{len(matches):,} isolate-positive, "
                  f"isolates={dict(stats['character_occurrences'])}", flush=True)

        metadata = make_stats()
        metadata_counts = {}
        for table in ["book", "tocEntry", "alt_toc_structure", "book_version", "source"]:
            if table not in tables:
                continue
            cs = [(r[1], (r[2] or "").upper()) for r in conn.execute(f"PRAGMA table_info({table})")]
            names = [n for n, dtype in cs if "TEXT" in dtype and n != "content"]
            if not names:
                continue
            part = make_stats()
            projection = ", ".join('"' + name.replace('"', '""') + '"' for name in names)
            for row in conn.execute(f"SELECT {projection} FROM {table}"):
                for v in row:
                    if v is not None:
                        raw = v.encode("utf-8") if isinstance(v, str) else bytes(v)
                        tally(part, raw)
            metadata_counts[table] = part
            for k in ["rows_examined", "rows_with_any_isolate", "utf8_bytes_examined"]:
                metadata[k] += part[k]
            metadata["character_occurrences"].update(part["character_occurrences"])
            metadata["other_bidi_controls"].update(part["other_bidi_controls"])
        counters["metadata"] = metadata

        positive_line_ids = set(found_rows.get("line", []))
        positive_line_ids.update(found_rows.get("edition", []))
        book_id_hits = collections.Counter()
        for id_ in positive_line_ids:
            row = conn.execute("SELECT bookId FROM line WHERE id=?", (id_,)).fetchone()
            if row:
                book_id_hits[row[0]] += 1

        positive_books_by_source = collections.Counter()
        samples = []
        for book_id, n in book_id_hits.most_common():
            row = conn.execute(
                "SELECT b.title, s.name FROM book b JOIN source s ON s.id=b.sourceId WHERE b.id=?",
                (book_id,),
            ).fetchone()
            if row:
                title, source = row
                positive_books_by_source[source] += 1
                if len(samples) < 20:
                    samples.append({"book_id": book_id, "title": title, "source": source,
                                    "positive_rows": n})
        total_books = conn.execute("SELECT COUNT(*) FROM book").fetchone()[0]
        total_sources = dict(conn.execute(
            "SELECT s.name, COUNT(*) FROM book b JOIN source s ON s.id=b.sourceId GROUP BY s.name"
        ))

        result = {
            "database": os.path.basename(path),
            "schema_version": dict(conn.execute("SELECT key,value FROM schema_meta")).get("db_schema_version", "unknown")
                if "schema_meta" in tables and {"key", "value"}.issubset(fields(conn, "schema_meta"))
                else None,
            "tables_examined": list(counters),
            "books_in_db": total_books,
            "books_with_any_isolate_in_text": len(book_id_hits),
            "books_by_source": total_sources,
            "positive_books_by_source": dict(positive_books_by_source),
            "matching_book_samples": samples,
            "zstd_dictionary_rows": len(dicts),
            "zstd_frames_decoded": decoder.frames,
            "duration_seconds": round(time.monotonic() - total, 2),
            "main_text": dict(counters.get("line", {})),
            "edition_overlays": dict(counters.get("edition", {})),
            "metadata": dict(counters["metadata"]),
            "metadata_tables": metadata_counts,
            "interpretation": (
                "Exact UTF-8 Unicode isolate codepoint counts in all main-line text rows, "
                "edition overlay text and selected textual metadata; scope excludes PDFs "
                "stored outside SQLite, user-imported documents and on-device notes."
            )
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2), flush=True)
        return result
    finally:
        decoder.close()
        conn.close()


def self_test():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "test.db")
        db = sqlite3.connect(path)
        db.executescript("""
            CREATE TABLE book(id INTEGER PRIMARY KEY, title TEXT, sourceId INTEGER);
            CREATE TABLE source(id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE line(id INTEGER PRIMARY KEY, bookId INTEGER, heRef TEXT);
            CREATE TABLE line_content(id INTEGER PRIMARY KEY, content BLOB);
            CREATE TABLE version_line(lineId INTEGER, content BLOB);
            INSERT INTO version_line VALUES (10, NULL);
            INSERT INTO source VALUES (1, 'sample');
            INSERT INTO book VALUES (1,'שלום',1);
            INSERT INTO line VALUES (10,1,'ref');
            INSERT INTO line VALUES (11,1,'ref2');
            INSERT INTO line_content VALUES (10,x'616263');
        """)
        db.execute("INSERT INTO line_content(id,content) VALUES (?,?)",
                   (11, "שלום\u2066abc\u2069\u2068test\u2069".encode()))
        db.commit()
        db.close()
        r = scan_db(path, progress=0)
        assert r["main_text"]["character_occurrences"] == {"LRI": 1, "FSI": 1, "PDI": 2}
        assert r["main_text"]["rows_examined"] == 2
        assert r["edition_overlays"]["rows_examined"] == 1
        assert r["edition_overlays"]["character_occurrences"] == {}
        assert r["books_in_db"] == r["books_with_any_isolate_in_text"] == 1


def main():
    p = argparse.ArgumentParser()
    p.add_argument("database", nargs="?")
    p.add_argument("--out", default="bidi-isolate-counts.json")
    p.add_argument("--self-test", action="store_true")
    a = p.parse_args()
    if a.self_test:
        self_test()
        return
    if not a.database:
        p.error("database path is required")
    output = scan_db(a.database)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()
