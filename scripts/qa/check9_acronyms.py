#!/usr/bin/env python3
"""בדיקה 9: כינויי ספרים (book_acronym) — לא ריקים ולא מתכווצים מול ה-snapshot.

הכינויים מיובאים מה-release האחרון של Otzaria/SeforimAcronymizer בכל בנייה. release
שבור (סכמה אחרת, נתונים חסרים) היה מוציא DB שחיפוש הספרים בו לא מכיר שום כינוי."""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import open_db, require_columns, die, gate_snapshot_drift

SNAPSHOT_TERMS = 52544   # db_version 28
SNAPSHOT_BOOKS = 6344    # ספרים עם כינוי אחד לפחות


def main():
    ap = argparse.ArgumentParser(description="בדיקה 9: כינויי ספרים")
    ap.add_argument("--db", required=True)
    args = ap.parse_args()

    conn = open_db(args.db)
    require_columns(conn, "book_acronym", ["bookId", "term"])

    row = conn.execute(
        "SELECT COUNT(*) AS terms, COUNT(DISTINCT bookId) AS books FROM book_acronym").fetchone()
    terms, books = row["terms"], row["books"]
    print(f"book_acronym: {terms} כינויים ב-{books} ספרים")
    print(f"reference snapshot: {SNAPSHOT_TERMS} כינויים, {SNAPSHOT_BOOKS} ספרים")
    if terms == 0:
        die("book_acronym ריקה — ה-Acronymizer לא יובא")
    gate_snapshot_drift("book_acronym terms", terms, SNAPSHOT_TERMS)
    gate_snapshot_drift("book_acronym books", books, SNAPSHOT_BOOKS)

    print("PASS: כינויי ספרים")
    sys.exit(0)


if __name__ == "__main__":
    main()
