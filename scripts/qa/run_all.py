#!/usr/bin/env python3
"""מריץ את כל סקריפטי ה-QA הבר-סקריפטיים (סעיף 10) ומסכם pass/fail.

ברירת מחדל: בדיקה שאין לה את הארגומנט הנדרש (למשל --metrics) מדולגת (SKIP) —
נוח להרצות אד-הוק. בהרצת release יש להעביר --require-all: אז כל דילוג הוא כשל
(יציאה שונה מ-0), כדי שבנייה ששכחה --metrics לא "תעבור" בשקט ותפספס בדיקה 10.5.
‏--require-all גם דורש QA_DRIFT_MAX_SHRINK_PCT בסביבה, אחרת שער הסחיפה מול ה-
reference snapshot (common.gate_snapshot_drift) היה כבוי בהרצת release — בשקט.
"""
import argparse
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from common import DRIFT_ENV  # noqa: E402

# (script, needs_sefaria_dir, accepts_expect_snapshot, needs_metrics)
SCRIPTS = [("check3_elucidation.py", False, False, False),
           ("check5_import_metrics.py", False, False, True),
           ("check7_provenance.py", False, True, False),
           ("check8_integrity.py", False, False, False),
           ("check1_dependence_count.py", True, True, False),
           ("check2_book_base_text.py", True, True, False),
           ("check6_metadata_rowbyrow.py", True, False, False)]


def main():
    ap = argparse.ArgumentParser(description="run_all: כל בדיקות ה-QA")
    ap.add_argument("--db", required=True)
    ap.add_argument("--sefaria-dir",
                    help="נדרש לבדיקות 1/2/6; בהיעדרו הן מדולגות")
    ap.add_argument("--metrics",
                    help="נתיב <db>.link-import-metrics.json לבדיקה 5; "
                         "בהיעדרו בדיקה 5 מדולגת (אין ניחוש נתיב ברירת-מחדל)")
    ap.add_argument("--sefaria-stage", action="store_true",
                    help="ה-DB הוא DB בשלב-ספריא (טרם צבירת havrouta/otzaria/linker); "
                         "מחמיר את הצלבת ה-DB בבדיקה 5 מ-≥ ל-==")
    ap.add_argument("--expect-snapshot", action="store_true")
    ap.add_argument("--require-all", action="store_true",
                    help="הרצת release: כל דילוג הוא כשל (יציאה!=0), ו-"
                         "QA_DRIFT_MAX_SHRINK_PCT חייב להיות מוגדר. ברירת המחדל "
                         "מתירה דילוגים להרצות אד-הוק.")
    args = ap.parse_args()

    # ‏--require-all הוא מצב release. שער הסחיפה מול ה-reference snapshot
    # (common.gate_snapshot_drift) מופעל רק כשהסף מוגדר בסביבה, ולכן הרצת release
    # שאיבדה את המשתנה הייתה מריצה את הבדיקות בלי שער — בדיוק המצב שהשער בא לתקן,
    # ובשקט. נכשל מיד, עם השם המדויק של מה שחסר.
    if args.require_all and os.environ.get(DRIFT_ENV, "").strip() == "":
        print(f"::error::--require-all (הרצת release) ללא {DRIFT_ENV} — שער הסחיפה "
              "מול ה-reference snapshot היה כבוי", file=sys.stderr)
        sys.exit(1)

    results = []
    for name, needs_sefaria, accepts_snapshot, needs_metrics in SCRIPTS:
        if needs_sefaria and not args.sefaria_dir:
            results.append((name, "SKIP", "אין --sefaria-dir", "--sefaria-dir"))
            continue
        if needs_metrics and not args.metrics:
            results.append((name, "SKIP", "אין --metrics", "--metrics"))
            continue
        cmd = [sys.executable, os.path.join(HERE, name), "--db", args.db]
        if needs_sefaria:
            cmd += ["--sefaria-dir", args.sefaria_dir]
        if needs_metrics:
            cmd += ["--metrics", args.metrics]
            if args.sefaria_stage:
                cmd.append("--sefaria-stage")
        if args.expect_snapshot and accepts_snapshot:
            cmd.append("--expect-snapshot")
        print(f"\n===== {name} =====")
        rc = subprocess.run(cmd).returncode
        results.append((name, "PASS" if rc == 0 else "FAIL", f"rc={rc}", None))

    print("\n===== סיכום =====")
    passed = failed = skipped = 0
    for name, status, note, _ in results:
        print(f"  {status:4}  {name}  ({note})")
        if status == "PASS":
            passed += 1
        elif status == "FAIL":
            failed += 1
        else:
            skipped += 1

    skips = [(name, arg) for name, status, _, arg in results if status == "SKIP"]

    # --require-all: כל דילוג = כשל, עם פירוט מה דולג ואיזה ארגומנט מפעיל אותו.
    if args.require_all and skips:
        print(f"\n--require-all: {len(skips)} בדיקות דולגו — כשל:")
        for name, arg in skips:
            print(f"  {name} — הפעל עם {arg}")
        # אם גם היו כשלים, נדווח עליהם בשורה נפרדת לצורך בהירות.
        if failed:
            print(f"{failed} בדיקות נכשלו")
        sys.exit(1)

    if failed:
        # מציגים תמונה מלאה: כמה עברו/דולגו/נכשלו.
        print(f"\nעברו {passed}, דולגו {skipped}, נכשלו {failed}")
        sys.exit(1)

    if skipped:
        print(f"\nעברו {passed}, דולגו {skipped}")
        sys.exit(0)

    print("\nכל הבדיקות עברו")
    sys.exit(0)


if __name__ == "__main__":
    main()
