#!/usr/bin/env python3
"""בדיקה 1 (סעיף 10): ספירת book.dependenceType מול ה-schemas. baseline 4,941."""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (load_schema_books, normalize_title_key, open_db,
                    require_columns, resolve_schemas_dir, sefaria_source_id, die,
                    gate_snapshot_drift)

SNAPSHOT_TOTAL = 4941
SNAPSHOT_BREAKDOWN = {"commentary": 4889, "targum": 45, "midrash": 5, "guides": 2}


def main():
    ap = argparse.ArgumentParser(description="בדיקה 1: dependenceType")
    ap.add_argument("--db", required=True)
    ap.add_argument("--sefaria-dir", required=True,
                    help="export/ (מכיל schemas/) או schemas/ ישירות")
    ap.add_argument("--expect-snapshot", action="store_true",
                    help="לאכוף את ה-baselines הקשיחים (4,941 והפילוח)")
    args = ap.parse_args()

    conn = open_db(args.db)
    require_columns(conn, "book", ["heRef", "dependenceType"])
    # רק ספרי source='Sefaria': ספרי מקורות אחרים ששמם צירוף-מקרים זהה ל-schema לא ידלפו.
    src_id = sefaria_source_id(conn)

    db_rows = conn.execute(
        "SELECT dependenceType AS d, COUNT(*) AS c FROM book "
        "WHERE sourceId = ? AND dependenceType IS NOT NULL GROUP BY dependenceType",
        (src_id,)).fetchall()
    db_breakdown = {r["d"]: r["c"] for r in db_rows}
    db_total = sum(db_breakdown.values())

    # התאמה לפי heRef ולא title: 6 ספרים תלויים משוני-שם בייבוא (title משתנה, heRef נשאר heTitle).
    db_norm_herefs = set()
    for r in conn.execute("SELECT heRef FROM book WHERE sourceId = ? AND heRef IS NOT NULL",
                          (src_id,)):
        n = normalize_title_key(r["heRef"])
        if n is not None:
            db_norm_herefs.add(n)

    schemas_dir = resolve_schemas_dir(args.sefaria_dir)
    books = load_schema_books(schemas_dir)
    exp_breakdown = {}
    for b in books:
        if b.raw_dependence is None:
            continue
        if normalize_title_key(b.he_title) in db_norm_herefs:
            exp_breakdown[b.raw_dependence] = exp_breakdown.get(b.raw_dependence, 0) + 1
    exp_total = sum(exp_breakdown.values())

    print(f"DB dependenceType total = {db_total}; פילוח = {dict(sorted(db_breakdown.items()))}")
    print(f"expected (schemas∩DB)  = {exp_total}; פילוח = {dict(sorted(exp_breakdown.items()))}")
    print(f"reference snapshot      = {SNAPSHOT_TOTAL}; פילוח = {SNAPSHOT_BREAKDOWN}")

    if db_breakdown != exp_breakdown:
        keys = set(db_breakdown) | set(exp_breakdown)
        diff = {k: (db_breakdown.get(k, 0), exp_breakdown.get(k, 0))
                for k in sorted(keys) if db_breakdown.get(k, 0) != exp_breakdown.get(k, 0)}
        die(f"DB לא תואם ל-schemas (סוג: DB,expected): {diff}")

    # שער הסחיפה: הסך וכל סוג בנפרד, כדי שהחלפה מקזזת (סוג יורד, אחר עולה) לא
    # תעבור מתחת לסך יציב.
    gate_snapshot_drift("dependenceType total", db_total, SNAPSHOT_TOTAL)
    for kind, expected in sorted(SNAPSHOT_BREAKDOWN.items()):
        gate_snapshot_drift(f"dependenceType {kind}", db_breakdown.get(kind, 0), expected)

    if args.expect_snapshot:
        if db_total != SNAPSHOT_TOTAL:
            die(f"snapshot: total={db_total} != {SNAPSHOT_TOTAL}")
        if db_breakdown != SNAPSHOT_BREAKDOWN:
            die(f"snapshot: פילוח {dict(sorted(db_breakdown.items()))} != {SNAPSHOT_BREAKDOWN}")

    print(f"PASS: dependenceType תואם schemas (total={db_total})")
    sys.exit(0)


if __name__ == "__main__":
    main()
