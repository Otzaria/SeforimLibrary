#!/usr/bin/env python3
"""בדיקה 9: כינויי ספרים (book_acronym) — תואמים בדיוק לקובץ ה-Acronymizer שממנו נבנו.

הכינויים מיובאים מה-release האחרון של Otzaria/SeforimAcronymizer בכל בנייה, והוא
מתעדכן בכוונה (גם בניקויים גדולים), ולכן אין כאן מספר-בסיס היסטורי. הבדיקה מחשבת
לכל ספר ב-DB את הכינויים שהגנרטור אמור היה לתת לו מאותו קובץ — אותו חיפוש כותרת
ואותו ניקוי של AcronymizerLookup — ודורשת התאמה מדויקת. ספר שאין לו רשומה בקובץ
יכול לקבל כינויים רק מ-carryAcronymsAcrossRenames (שינוי שם); אלה נספרים ומדווחים."""
import argparse
import os
import re
import sqlite3
import string
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import open_db, require_columns, die

# JVM: "\\s" ו-"\\p{Punct}" הם ASCII בלבד (בלי UNICODE_CHARACTER_CLASS)
_JAVA_WS = re.compile(r"[ \t\n\x0b\f\r]+")
_JAVA_PUNCT = re.compile("[" + re.escape(string.punctuation) + "]")
MAX_LISTED = 20


def _is_diacritic(ch):
    # HebrewTextUtils.isNikudChar / isTeamimChar
    c = ord(ch)
    return 0x0591 <= c <= 0x05BD or c in (0x05C1, 0x05C2, 0x05C7)


def sanitize_term(raw):
    """AcronymizerLookup.sanitizeTerm."""
    s = raw.strip()
    if not s:
        return ""
    s = "".join(ch for ch in s if not _is_diacritic(ch))
    s = s.replace("־", " ").replace("״", "").replace("׳", "")
    return _JAVA_WS.sub(" ", s).strip()


def lookup_variants(title):
    """AcronymizerLookup.lookupVariants."""
    out = [title]
    stripped = title.replace("״", "").replace('"', "").replace("׳", "").replace("'", "").strip()
    no_comma = title.replace(",", " ").strip()
    no_punct = _JAVA_WS.sub(" ", _JAVA_PUNCT.sub(" ", title)).strip()
    for v in (stripped, no_comma, no_punct, sanitize_term(title)):
        if v.strip():
            out.append(v)
    return list(dict.fromkeys(out))


def _equals_ignore_case(a, b):
    # java.lang.String.equalsIgnoreCase: תו מול תו
    return len(a) == len(b) and all(
        x == y or x.upper() == y.upper() or x.lower() == y.lower() for x, y in zip(a, b))


def clean(raw, title):
    """AcronymizerLookup.clean."""
    title_norm = sanitize_term(title)
    terms = (sanitize_term(t).strip() for t in raw)
    return list(dict.fromkeys(
        t for t in terms
        if t and not _equals_ignore_case(t, title) and not _equals_ignore_case(t, title_norm)))


def raw_terms(acr, title):
    """AcronymizerLookup.rawTerms: הכתיב הראשון שיש לו רשומות כלשהן."""
    for candidate in lookup_variants(title):
        found = [r[0] for r in acr.execute(
            "SELECT a.acronym FROM Books b "
            "JOIN BookAcronyms ba ON b.id = ba.book_id "
            "JOIN Acronyms a ON ba.acronym_id = a.id "
            "WHERE b.title = ? ORDER BY a.acronym", (candidate,))
            if r[0] is not None and r[0].strip()]
        if found:
            return found
    return []


def main():
    ap = argparse.ArgumentParser(description="בדיקה 9: כינויי ספרים")
    ap.add_argument("--db", required=True)
    ap.add_argument("--acronym-db", required=True,
                    help="ה-acronymizer.db שהבנייה השתמשה בו")
    args = ap.parse_args()

    conn = open_db(args.db)
    require_columns(conn, "book_acronym", ["bookId", "term"])
    if not os.path.isfile(args.acronym_db) or os.path.getsize(args.acronym_db) == 0:
        die(f"קובץ ה-Acronymizer חסר או ריק: {args.acronym_db}")
    acr = sqlite3.connect(f"file:{args.acronym_db}?mode=ro", uri=True)
    try:
        source_pairs = acr.execute("SELECT COUNT(*) FROM BookAcronyms").fetchone()[0]
    except sqlite3.Error as e:
        die(f"קובץ ה-Acronymizer אינו קריא או בסכמה לא צפויה: {e}")
    if source_pairs == 0:
        die("קובץ ה-Acronymizer ריק (BookAcronyms=0)")

    got = {}
    for book_id, term in conn.execute("SELECT bookId, term FROM book_acronym"):
        got.setdefault(book_id, set()).add(term)
    total_terms = sum(len(v) for v in got.values())
    print(f"book_acronym: {total_terms} כינויים ב-{len(got)} ספרים; "
          f"קובץ המקור: {source_pairs} זוגות ספר-כינוי")
    if total_terms == 0:
        die("book_acronym ריקה — ה-Acronymizer לא יובא")

    mismatched, carried_books, carried_terms, matched_books = [], 0, 0, 0
    for book_id, title in conn.execute("SELECT id, title FROM book ORDER BY id"):
        db_terms = got.get(book_id, set())
        raw = raw_terms(acr, title)
        if not raw:
            # רק carryAcronymsAcrossRenames נותן כינויים לספר בלי רשומה תחת שמו
            if db_terms:
                carried_books += 1
                carried_terms += len(db_terms)
            continue
        expected = set(clean(raw, title))
        if db_terms != expected:
            mismatched.append((book_id, title, sorted(expected - db_terms), sorted(db_terms - expected)))
        elif expected:
            matched_books += 1

    print(f"תואמים לקובץ המקור: {matched_books} ספרים; "
          f"כינויים משינויי שם (בלי רשומה תחת השם הנוכחי): {carried_terms} ב-{carried_books} ספרים")
    if mismatched:
        for book_id, title, missing, extra in mismatched[:MAX_LISTED]:
            print(f"  book {book_id} '{title}': חסרים {missing} עודפים {extra}")
        if len(mismatched) > MAX_LISTED:
            print(f"  … ועוד {len(mismatched) - MAX_LISTED}")
        die(f"{len(mismatched)} ספרים שכינוייהם ב-DB שונים ממה שקובץ ה-Acronymizer נותן להם")

    print("PASS: כינויי ספרים תואמים לקובץ המקור")
    sys.exit(0)


if __name__ == "__main__":
    main()
