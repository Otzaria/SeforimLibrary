#!/usr/bin/env python3
"""בדיקה 2 (סעיף 10): book_base_text מוצהר בלבד, עם רזולוציית alt-titles בסדר-priority."""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (load_schema_books, normalize_title_key, open_db,
                    require_columns, resolve_schemas_dir, sefaria_source_id, die,
                    default_priority_list_path, load_priority_list,
                    order_books_by_priority, build_normalized_title_to_bookid,
                    gate_snapshot_drift)

# ‏5,426 הוא ה-baseline הנכון (אחרי תיקון היבואן ב-8358a16). ‏5,437 שנצפה קודם היה תוצר
# באג ביבואן — alias של ספר מוקדם (מ"מעילה") האפיל על פרימרי של ספר מאוחר, וכפל 11 זוגות.
SNAPSHOT_ROWS = 5426


def main():
    ap = argparse.ArgumentParser(description="בדיקה 2: book_base_text")
    ap.add_argument("--db", required=True)
    ap.add_argument("--sefaria-dir", required=True)
    ap.add_argument("--priority-list", default=default_priority_list_path(),
                    help="priority.txt של היבואן (ברירת מחדל: ה-resource המגורסן ברפו)")
    ap.add_argument("--expect-snapshot", action="store_true")
    args = ap.parse_args()

    conn = open_db(args.db)
    require_columns(conn, "book", ["heRef"])
    require_columns(conn, "book_base_text", ["bookId", "baseBookId"])
    # רק ספרי source='Sefaria': בלי הסינון, ספר ממקור אחר (למשל MoreBooks) ששמו זהה
    # במקרה ל-heTitle של schema היה נתפס כיעד רזולוציה ומעוות את הזוגות הצפויים.
    src_id = sefaria_source_id(conn)

    # מיפוי heRef-DB מנורמל → bookId (heRef == payload.heTitle; title עשוי להשתנות בייבוא).
    db_id_by_he = {}
    for r in conn.execute("SELECT id, heRef FROM book WHERE sourceId = ? AND heRef IS NOT NULL",
                          (src_id,)):
        n = normalize_title_key(r["heRef"])
        if n is not None:
            db_id_by_he.setdefault(n, r["id"])

    schemas_dir = resolve_schemas_dir(args.sefaria_dir)
    # סדר הספרים כמו applyPriorityOrdering של היבואן: priority.txt תחילה, השאר אחריו.
    # הנחת סדר שיורית: סדר "השאר" אצל היבואן הוא סדר סריקת קבצי הטקסט (לא זמין
    # מהארטיפקטים) — כאן סדר שמות קובצי schemas; משפיע רק על התנגשויות בין לא-priority.
    books = order_books_by_priority(load_schema_books(schemas_dir),
                                    load_priority_list(args.priority_list))

    # שכפול buildNormalizedTitleToBookId (SefariaDirectImporter.kt): שני מעברים גלובליים —
    # כל הפרימריז בסדר-priority ואז כל ה-aliases. רק ספרים שקיימים ב-DB (post-blacklist):
    # heTitle מנורמל מול heRef.
    schema_dbid = [(b, db_id_by_he.get(normalize_title_key(b.he_title))) for b in books]
    norm_to_id = build_normalized_title_to_bookid(schema_dbid)

    expected_pairs = set()
    for b, dbid in schema_dbid:
        if dbid is None:
            continue
        for key in b.declared_keys:
            base_id = norm_to_id.get(key)
            if base_id is not None:
                expected_pairs.add((dbid, base_id))

    # book_base_text נקרא במלואו (ללא סינון source): שורה של ספר לא-Sefaria תופיע כ"עודף" ותיכשל בקול.
    db_pairs = {(r["bookId"], r["baseBookId"])
                for r in conn.execute("SELECT bookId, baseBookId FROM book_base_text")}

    print(f"DB book_base_text rows = {len(db_pairs)}")
    print(f"expected (schemas alt-title resolution) = {len(expected_pairs)}")
    print(f"reference snapshot = {SNAPSHOT_ROWS}")

    missing = expected_pairs - db_pairs
    extra = db_pairs - expected_pairs
    if missing or extra:
        print(f"expected∖DB (עד 10): {sorted(missing)[:10]}", file=sys.stderr)
        print(f"DB∖expected (עד 10): {sorted(extra)[:10]}", file=sys.stderr)
        die(f"אי-התאמת זוגות: {len(missing)} חסרים, {len(extra)} עודפים")

    gate_snapshot_drift("book_base_text rows", len(db_pairs), SNAPSHOT_ROWS)

    if args.expect_snapshot and len(db_pairs) != SNAPSHOT_ROWS:
        die(f"snapshot: rows={len(db_pairs)} != {SNAPSHOT_ROWS}")

    print(f"PASS: book_base_text תואם רזולוציית schemas (rows={len(db_pairs)})")
    sys.exit(0)


if __name__ == "__main__":
    main()
