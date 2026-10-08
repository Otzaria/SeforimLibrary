"""Offline tests for books_blacklist_check.py (no network)."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import books_blacklist_check as check  # noqa: E402


def run(text, export, live, lock=None):
    result = check.analyze(check.parse_blacklist(text), export, live, lock or {})
    return result, check.apply_fixes(text, result)


class NormalizeKeyTest(unittest.TestCase):
    def test_matches_the_kotlin_rules(self):
        self.assertEqual(check.normalize_key('  שו"ת  מהר״ם_א '), "שות מהרם א")
        self.assertEqual(check.normalize_key("Ish  Kilvavo"), "ish kilvavo")
        self.assertIsNone(check.normalize_key("   "))

    def test_keeps_punctuation_the_importer_keeps(self):
        self.assertNotEqual(check.normalize_key("תלמוד מהו"), check.normalize_key("תלמוד מהו?"))


class AnalyzeTest(unittest.TestCase):
    EXPORT = {"Learning to Read Midrash": "ללמוד לקרוא מדרש", "What is the Talmud": "תלמוד מהו?"}
    LIVE = {"Learning to Read Midrash": "לפשוטו של מדרש", "What is the Talmud": "תלמוד מהו?"}

    def test_clean_file_is_left_alone(self):
        text = "# c\nללמוד לקרוא מדרש\nלפשוטו של מדרש\nתלמוד מהו?\n"
        result, fixed = run(text, self.EXPORT, self.LIVE)
        self.assertFalse(result.changed)
        self.assertEqual(result.unresolved, [])
        self.assertEqual(fixed, text)

    def test_live_rename_adds_the_new_title_and_keeps_the_old(self):
        result, fixed = run("ללמוד לקרוא מדרש\n", self.EXPORT, self.LIVE)
        self.assertEqual(fixed, "ללמוד לקרוא מדרש\nלפשוטו של מדרש\n")
        self.assertEqual(result.lock, {"ללמוד לקרוא מדרש": "Learning to Read Midrash"})

    def test_title_missing_from_the_export_is_restored(self):
        # The October 2026 case: the line was moved to the live name while the
        # export (= the next build) still carried the old one.
        _, fixed = run("לפשוטו של מדרש\n", self.EXPORT, self.LIVE)
        self.assertEqual(fixed, "לפשוטו של מדרש\nללמוד לקרוא מדרש\n")

    def test_rename_in_both_sources_is_followed_through_the_lock(self):
        moved = {"Learning to Read Midrash": "לפשוטו של מדרש"}
        lock = {"ללמוד לקרוא מדרש": "Learning to Read Midrash"}
        result, fixed = run("ללמוד לקרוא מדרש\n", moved, moved, lock)
        self.assertEqual(fixed, "ללמוד לקרוא מדרש\nלפשוטו של מדרש\n")
        self.assertEqual([e.title for e, _ in result.obsolete], ["ללמוד לקרוא מדרש"])
        self.assertEqual(result.unresolved, [])

    def test_english_line_needs_no_hebrew_alias(self):
        result, _ = run("Learning to Read Midrash\n", self.EXPORT, self.LIVE)
        self.assertFalse(result.changed)

    def test_punctuation_typo_is_replaced(self):
        result, fixed = run("א\nתלמוד מהו\nב\n", self.EXPORT, self.LIVE)
        self.assertEqual(fixed, "א\nתלמוד מהו?\nב\n")
        self.assertEqual([t for _, t in result.replaces], ["תלמוד מהו?"])

    def test_duplicate_key_is_dropped(self):
        result, fixed = run('תלמוד מהו?\nתלמוד  מהו?\n', self.EXPORT, self.LIVE)
        self.assertEqual(fixed, "תלמוד מהו?\n")
        self.assertEqual(len(result.duplicates), 1)

    def test_unknown_line_is_reported_with_suggestions_not_changed(self):
        export = {"Ish Kilvavo; on Samuel": "איש כלבבו; שמואל"}
        text = "הערות על איש כלבבו; שמואל\n"
        result, fixed = run(text, export, export)
        self.assertEqual(fixed, text)
        self.assertEqual(len(result.unresolved), 1)
        self.assertIn("איש כלבבו; שמואל", result.unresolved[0][1])

    def test_lock_pointing_at_a_vanished_book_does_not_resolve(self):
        result, _ = run("ספר שנמחק\n", self.EXPORT, self.LIVE, {"ספר שנמחק": "Gone Book"})
        self.assertEqual(len(result.unresolved), 1)

    def test_path_lines_are_skipped(self):
        result, _ = run("תנך/ספר\n", self.EXPORT, self.LIVE)
        self.assertEqual(len(result.skipped_paths), 1)
        self.assertEqual(result.unresolved, [])

    def test_escaped_quotes_are_read_like_the_importer(self):
        export = {"Ein Ayah": 'עין אי"ה'}
        result, _ = run('עין אי\\"ה\n', export, export)
        self.assertEqual(result.unresolved, [])


class LockTest(unittest.TestCase):
    def test_lock_follows_file_order_and_drops_removed_lines(self):
        text = "ב\nא\n"
        lock = {"א": "A", "ב": "B", "ג": "C"}
        self.assertEqual(check.render_lock(text, lock), check.LOCK_HEADER + "ב\tB\nא\tA\n")


if __name__ == "__main__":
    unittest.main()
