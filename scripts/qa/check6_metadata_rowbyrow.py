#!/usr/bin/env python3
"""בדיקה 6 (סעיף 10): השוואה שורה-שורה לפי heRef (ספרי source='Sefaria' בלבד) —
dependenceType, collectiveTitleHe/En מול schemas."""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (load_schema_books, open_db, require_columns,
                    resolve_schemas_dir, sefaria_source_id, die,
                    gate_snapshot_drift)

# ספר schema שנעדר כולו מה-DB יורד גם מאגף הצפי בבדיקות 1/2; כאן הוא מוריד את
# matched, ולכן גם המדד הזה נושא baseline ועובר בשער הסחיפה. ‏5,821 = ספרי
# source='Sefaria' שהותאמו ל-schema (ר' README, סעיף סינון ה-source).
SNAPSHOT_MATCHED = 5821


def main():
    ap = argparse.ArgumentParser(description="בדיקה 6: מטא-דאטה שורה-שורה")
    ap.add_argument("--db", required=True)
    ap.add_argument("--sefaria-dir", required=True)
    ap.add_argument("--max-report", type=int, default=20)
    ap.add_argument("--expect-snapshot", action="store_true")
    args = ap.parse_args()

    conn = open_db(args.db)
    require_columns(conn, "book",
                    ["heRef", "dependenceType", "collectiveTitleHe", "collectiveTitleEn"])
    # רק ספרי source='Sefaria': ספר ממקור אחר (למשל MoreBooks) ש-heRef שלו זהה במקרה
    # ל-heTitle של schema אינו שייך להשוואה — המטא-דאטה הריק שלו אינו pass/fail.
    src_id = sefaria_source_id(conn)

    schemas_dir = resolve_schemas_dir(args.sefaria_dir)
    books = load_schema_books(schemas_dir)
    if not books:
        die(f"אף schema-book לא נטען מ-{schemas_dir}")
    # heRef ב-DB == payload.heTitle == schema heTitle (השוואה מדויקת, לא מנורמלת).
    by_he = {}
    collisions = 0
    for b in books:
        if b.he_title in by_he:
            collisions += 1
        by_he[b.he_title] = b
    if collisions:
        print(f"אזהרה: {collisions} כותרות heTitle כפולות ב-schemas (נלקח האחרון)")

    mismatches = []
    matched = 0
    unmatched_with_meta = []
    for r in conn.execute(
            "SELECT heRef, dependenceType, collectiveTitleHe, collectiveTitleEn "
            "FROM book WHERE sourceId = ?", (src_id,)):
        b = by_he.get(r["heRef"]) if r["heRef"] is not None else None
        if b is None:
            # ספר עם מטא-דאטת Sefaria (dependenceType/collectiveTitle*) חייב schema תואם.
            if (r["dependenceType"] or r["collectiveTitleHe"] or r["collectiveTitleEn"]):
                unmatched_with_meta.append(r["heRef"])
            continue
        matched += 1
        for field, dbv, schv in (
                ("dependenceType", r["dependenceType"], b.raw_dependence),
                ("collectiveTitleHe", r["collectiveTitleHe"], b.collective_he),
                ("collectiveTitleEn", r["collectiveTitleEn"], b.collective_en)):
            if dbv != schv:
                mismatches.append((r["heRef"], field, dbv, schv))

    print(f"books שהותאמו ל-schema: {matched}")
    print(f"books עם מטא-דאטה ללא schema תואם: {len(unmatched_with_meta)}")
    print(f"אי-התאמות שדה: {len(mismatches)}")
    print(f"reference snapshot = {SNAPSHOT_MATCHED}")

    if matched == 0:
        die("אף ספר ב-DB לא הותאם ל-schema (heRef לא תואם אף heTitle) — בדיקה ריקה, כשל")
    if unmatched_with_meta:
        for heref in unmatched_with_meta[:args.max_report]:
            print(f"  מטא-דאטה ללא schema: {heref!r}", file=sys.stderr)
        die(f"{len(unmatched_with_meta)} ספרים עם מטא-דאטת Sefaria וללא schema תואם")
    if mismatches:
        for heref, field, dbv, schv in mismatches[:args.max_report]:
            print(f"  {heref!r} :: {field}: DB={dbv!r} schema={schv!r}", file=sys.stderr)
        die(f"{len(mismatches)} אי-התאמות מטא-דאטה שורה-שורה")

    gate_snapshot_drift("schema-matched books", matched, SNAPSHOT_MATCHED)

    if args.expect_snapshot and matched != SNAPSHOT_MATCHED:
        die(f"snapshot: matched={matched} != {SNAPSHOT_MATCHED}")

    print(f"PASS: מטא-דאטה תואם schemas ({matched} ספרים)")
    sys.exit(0)


if __name__ == "__main__":
    main()
