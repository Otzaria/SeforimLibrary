#!/usr/bin/env python3
"""בדיקה 7 (סעיף 10): דרגת baseProvenance — inferred(1)/declared(2), הרכב דרגות פר ספר-מקור.

סדר ה-SOURCE עצמו (ORDER BY של ה-mirror query) אינו נבדק כאן — סקריפט SQL אינו יכול
לאמת ORDER BY בלי לשכפל אותו (טאוטולוגיה). הוא מכוסה בבדיקת האינטגרציה של ה-dao:
SeforimRepositoryIntegrationTest.`SOURCE view orders by baseProvenance DESC ahead of
all tie-breakers` (קומיט f92c99c), הרצה מול השאילתה האמיתית selectInverseLinksByTargetLineIds."""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import open_db, require_columns, die, gate_snapshot_drift

SNAPSHOT_INFERRED = 13056          # baseProvenance=1
SNAPSHOT_INFERRED_PAIRS = 20       # זוגות ספרים מוסקים

# סוגי הקישור של ה-mirror query (LinkQueries.sq:selectInverseLinksByTargetLineIds).
_MIRROR_TYPES = ("COMMENTARY", "SUPER_COMMENTARY", "TARGUM", "MIDRASH",
                 "PARSHANUT", "DIBUR_HAMATCHIL", "EIN_MISHPAT", "ELUCIDATION")
_MIRROR_TYPES_SQL = "(" + ",".join("'%s'" % t for t in _MIRROR_TYPES) + ")"
_MIRROR_SAMPLE = 25                # ספרי-מקור לדגימת הרכב דרגות ה-provenance


def main():
    ap = argparse.ArgumentParser(description="בדיקה 7: baseProvenance")
    ap.add_argument("--db", required=True)
    ap.add_argument("--expect-snapshot", action="store_true")
    args = ap.parse_args()

    conn = open_db(args.db)
    require_columns(conn, "link",
                    ["sourceBookId", "targetBookId", "sourceLineId",
                     "connectionTypeId", "baseProvenance"])
    require_columns(conn, "book_base_text", ["bookId", "baseBookId"])

    # (א) baseProvenance=1 (INFERRED_TITLE): ספירה, פילוח לפי סוג, זוגות מוסקים, הזוג הגדול.
    inferred_total = conn.execute(
        "SELECT COUNT(*) AS c FROM link WHERE baseProvenance = 1").fetchone()["c"]
    by_type = conn.execute(
        "SELECT ct.name AS n, COUNT(*) AS c FROM link l "
        "JOIN connection_type ct ON ct.id = l.connectionTypeId "
        "WHERE l.baseProvenance = 1 GROUP BY ct.name ORDER BY c DESC").fetchall()
    inferred_pairs = conn.execute(
        "SELECT sourceBookId AS s, targetBookId AS t, COUNT(*) AS c FROM link "
        "WHERE baseProvenance = 1 GROUP BY sourceBookId, targetBookId "
        "ORDER BY c DESC").fetchall()

    print(f"baseProvenance=1 (מוסק): {inferred_total} קישורים")
    print(f"  פילוח לפי סוג: {[(r['n'], r['c']) for r in by_type]}")
    print(f"  זוגות ספרים מוסקים נבדלים: {len(inferred_pairs)}")
    if inferred_pairs:
        big = inferred_pairs[0]
        print(f"  הזוג הגדול: source={big['s']} target={big['t']} ({big['c']} קישורים)")
    print(f"reference snapshot: {SNAPSHOT_INFERRED} קישורים, {SNAPSHOT_INFERRED_PAIRS} זוגות")
    gate_snapshot_drift("baseProvenance=1 links", inferred_total, SNAPSHOT_INFERRED)
    gate_snapshot_drift("baseProvenance=1 book pairs", len(inferred_pairs),
                        SNAPSHOT_INFERRED_PAIRS)

    # (ב) baseProvenance=2 (SEFARIA_DECLARED): לא-ריק ועקבי עם book_base_text.
    # book_base_text מאוחסן (bookId=תלוי, baseBookId=בסיס); קישור מוצהר מאוחסן base→dependant
    # (source=בסיס, target=תלוי — ראו ה-swap ב-SefariaLinksImporter). לכן זוג-הקישור הצפוי
    # הוא (baseBookId, bookId): השוואה מכוונת (source,target), לא frozenset — קישור בכיוון
    # הפוך (תלוי→בסיס) חייב להיכשל.
    declared_total = conn.execute(
        "SELECT COUNT(*) AS c FROM link WHERE baseProvenance = 2").fetchone()["c"]
    bbt_link_pairs = {(r["baseBookId"], r["bookId"])
                      for r in conn.execute("SELECT bookId, baseBookId FROM book_base_text")}
    declared_link_pairs = {(r["s"], r["t"]) for r in conn.execute(
        "SELECT DISTINCT sourceBookId AS s, targetBookId AS t FROM link "
        "WHERE baseProvenance = 2")}
    print(f"baseProvenance=2 (מוצהר): {declared_total} קישורים, "
          f"{len(declared_link_pairs)} זוגות ספרים נבדלים")

    if bbt_link_pairs and declared_total == 0:
        die("book_base_text לא-ריק אך אין קישורי baseProvenance=2")
    off = declared_link_pairs - bbt_link_pairs
    if off:
        sample = sorted(off)[:10]
        die(f"{len(off)} זוגות מוצהרים (source,target) בקישורים שאינם ב-book_base_text "
            f"בכיוון base→dependant: {sample}")

    # (ג) הרכב דרגות פר ספר-מקור (סעיף 4.2) — אימות נתונים בלבד; סדר ה-SOURCE עצמו
    # מכוסה בבדיקת ה-dao (ראו docstring).
    _verify_tier_composition(conn)

    if args.expect_snapshot:
        if inferred_total != SNAPSHOT_INFERRED:
            die(f"snapshot: baseProvenance=1 total={inferred_total} != {SNAPSHOT_INFERRED}")
        if len(inferred_pairs) != SNAPSHOT_INFERRED_PAIRS:
            die(f"snapshot: זוגות מוסקים={len(inferred_pairs)} != {SNAPSHOT_INFERRED_PAIRS}")

    print("PASS: baseProvenance עקבי; הרכב דרגות תקין (סדר ה-SOURCE — בבדיקת ה-dao)")
    sys.exit(0)


def _verify_tier_composition(conn):
    # אימות ברמת הנתונים, בלי לטעון לבדיקת סדר-שאילתה: דוגם את ספרי-המקור עם מירב דרגות
    # ה-provenance בסוגי ה-mirror, ולכל ספר מוודא שהרכב הדרגות (0/1/2) שנספר שורה-שורה
    # תואם GROUP BY ישיר. ההגנה נגד PASS ריק: לפחות ספר-מקור אחד בדגימה חייב לשאת בפועל
    # את שלוש הדרגות declared/inferred/none בכפיפה אחת.
    sources = conn.execute(
        "SELECT l.sourceBookId AS sb, COUNT(DISTINCT l.baseProvenance) AS d, COUNT(*) AS c "
        "FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId "
        f"WHERE ct.name IN {_MIRROR_TYPES_SQL} AND l.sourceBookId != l.targetBookId "
        "GROUP BY l.sourceBookId HAVING d >= 2 "
        "ORDER BY d DESC, c DESC LIMIT ?", (_MIRROR_SAMPLE,)).fetchall()
    if not sources:
        die("אף ספר-מקור אינו נושא קישורים ב-≥2 דרגות provenance — דגימה לא-מייצגת, כשל")

    base_sql = ("FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId "
                f"WHERE l.sourceBookId = ? AND ct.name IN {_MIRROR_TYPES_SQL} "
                "AND l.sourceBookId != l.targetBookId")
    saw_all_three = False
    for row in sources:
        sb = row["sb"]
        counted = {}
        for r in conn.execute("SELECT l.baseProvenance AS p " + base_sql, (sb,)):
            counted[r["p"]] = counted.get(r["p"], 0) + 1
        grouped = {r["p"]: r["c"] for r in conn.execute(
            "SELECT l.baseProvenance AS p, COUNT(*) AS c " + base_sql +
            " GROUP BY l.baseProvenance", (sb,))}
        if counted != grouped:
            die(f"הרכב דרגות לספר {sb}: ספירה שורה-שורה {counted} != GROUP BY {grouped}")
        if {0, 1, 2} <= set(counted):
            saw_all_three = True
    if not saw_all_three:
        die("אף ספר-מקור בדגימה אינו נושא את שלוש הדרגות (declared/inferred/none) — "
            "דגימה לא-מייצגת, כשל למניעת PASS ריק")
    print(f"  הרכב דרגות תקין על {len(sources)} ספרי-מקור; שלוש הדרגות (2/1/0) נצפו בדגימה")
    print("  סדר ה-SOURCE עצמו (ORDER BY baseProvenance DESC) מכוסה בבדיקת ה-dao: "
          "SeforimRepositoryIntegrationTest.`SOURCE view orders by baseProvenance DESC "
          "ahead of all tie-breakers` (f92c99c)")


if __name__ == "__main__":
    main()
