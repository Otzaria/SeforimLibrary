#!/usr/bin/env python3
"""Tests for library_update_announce.py and its workflow."""
import io
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

try:
    import yaml
except ImportError:  # pragma: no cover - CI installs PyYAML explicitly
    yaml = None

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import library_update_announce as lua  # noqa: E402

WORKFLOW = ROOT / ".github" / "workflows" / "library-update-announce.yml"


def make_db(path, books, lines):
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE category (id INTEGER PRIMARY KEY, parentId INTEGER, title TEXT NOT NULL);
        CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE book (id INTEGER PRIMARY KEY, categoryId INTEGER, sourceId INTEGER, title TEXT NOT NULL);
        CREATE TABLE line (id INTEGER PRIMARY KEY, bookId INTEGER, lineIndex INTEGER, content TEXT NOT NULL);
        INSERT INTO category VALUES (1, 1, 'ספרייה'), (2, 1, 'תנך'), (3, 1, 'הלכה');
        INSERT INTO source VALUES (1, 'sefaria'), (2, 'otzaria');
    """)
    conn.executemany("INSERT INTO book VALUES (?, ?, ?, ?)", books)
    conn.executemany("INSERT INTO line (bookId, lineIndex, content) VALUES (?, ?, ?)", lines)
    conn.commit()
    conn.close()


def catalog(books, lines):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "seforim.db"
        make_db(path, books, lines)
        return lua.build_catalog(path)


class PreviousTagTest(unittest.TestCase):
    def test_picks_the_newest_older_tag_and_ignores_others(self):
        tags = ["v27-20260906092829", "v28-20260910220310", "v30-20261001000000",
                "lines-snapshot-sha256-abc", "v9-20260706195857"]
        self.assertEqual("v28-20260910220310", lua.previous_published_tag("v29-20260927072953", tags))

    def test_first_release_has_no_previous(self):
        self.assertIsNone(lua.previous_published_tag("v1-20260625232529", ["v1-20260625232529"]))

    def test_refuses_a_tag_that_is_not_a_db_release(self):
        with self.assertRaises(ValueError):
            lua.previous_published_tag("pipeline-result-run-1-1", [])


class CatalogTest(unittest.TestCase):
    def test_paths_walk_up_to_the_self_parented_root(self):
        value = catalog([(10, 2, 1, "בראשית")], [(10, 0, "א")])
        self.assertEqual("ספרייה/תנך", value["books"][0]["path"])
        self.assertEqual("sefaria", value["books"][0]["source"])
        self.assertEqual(1, value["books"][0]["lines"])

    def test_hash_follows_line_order_not_insertion_order(self):
        a = catalog([(10, 2, 1, "בראשית")], [(10, 0, "א"), (10, 1, "ב")])
        b = catalog([(10, 2, 1, "בראשית")], [(10, 1, "ב"), (10, 0, "א")])
        c = catalog([(10, 2, 1, "בראשית")], [(10, 0, "ב"), (10, 1, "א")])
        self.assertEqual(a["books"][0]["hash"], b["books"][0]["hash"])
        self.assertNotEqual(a["books"][0]["hash"], c["books"][0]["hash"])

    def test_book_without_lines_has_no_hash(self):
        value = catalog([(10, 2, 1, "ריק")], [])
        self.assertIsNone(value["books"][0]["hash"])


class DiffTest(unittest.TestCase):
    def setUp(self):
        self.old = catalog(
            [(1, 2, 1, "בראשית"), (2, 2, 1, "שמות"), (3, 3, 2, "ישן"), (4, 3, 2, "נמחק"), (5, 2, 2, "נודד")],
            [(1, 0, "א"), (2, 0, "ב"), (3, 0, "תוכן ישן"), (4, 0, "ד"), (5, 0, "ה")])
        self.new = catalog(
            [(1, 2, 1, "בראשית"), (2, 2, 1, "שמות"), (6, 3, 2, "חדש-בשם"), (7, 2, 1, "חדש"), (5, 3, 2, "נודד")],
            [(1, 0, "א"), (2, 0, "ב שונה"), (6, 0, "תוכן ישן"), (7, 0, "ז"), (5, 0, "ה")])

    def test_classifies_every_kind_of_change(self):
        changes = lua.diff_catalogs(self.old, self.new)
        self.assertEqual(["ספרייה/תנך/חדש"], changes["added"])
        self.assertEqual(["ספרייה/הלכה/נמחק"], changes["removed"])
        self.assertEqual(["ספרייה/תנך/שמות"], changes["changed"])
        self.assertEqual(sorted([
            "ספרייה/הלכה/חדש-בשם (לשעבר: ספרייה/הלכה/ישן)",
            "ספרייה/הלכה/נודד (לשעבר: ספרייה/תנך/נודד)",
        ]), changes["moved"])

    def test_identical_catalogs_have_no_changes(self):
        changes = lua.diff_catalogs(self.old, self.old)
        self.assertFalse(any(changes.values()))


class PostTest(unittest.TestCase):
    CHANGES = {"added": ["א/ב"], "removed": [], "moved": [], "changed": ["ג/ד", "ג/ה"]}

    def test_post_lists_only_non_empty_sections(self):
        post = lua.build_forum_post(self.CHANGES, 30, "א' תשרי")
        self.assertTrue(post.startswith("# גירסת ספרייה 30\n"))
        self.assertIn("## התווספו הספרים הבאים:\n* א/ב\n", post)
        self.assertIn("## השתנו הספרים הבאים:\n* ג/ד\n* ג/ה\n", post)
        self.assertNotIn("נמחקו", post)

    def test_post_is_cut_to_the_forum_limit(self):
        changes = {"added": [f"ספר {i}" for i in range(1000)], "removed": [], "moved": [], "changed": []}
        post = lua.build_forum_post(changes, 30, "x", limit=500)
        self.assertLess(len(post), 530)
        self.assertRegex(post, r"\* ועוד \d+ ספרים\n$")

    def test_yemot_reads_titles_without_paths(self):
        content = lua.build_yemot_content(self.CHANGES)
        self.assertEqual({"התווספו הספרים הבאים:": "ב", "השתנו הספרים הבאים:": "ד\nה"}, content)


class AnnounceTest(unittest.TestCase):
    def run_announce(self, old, new, env, argv=()):
        with tempfile.TemporaryDirectory() as tmp:
            paths = []
            for name, value in (("old.json", old), ("new.json", new)):
                path = Path(tmp) / name
                path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
                paths.append(str(path))
            out = io.StringIO()
            with mock.patch.dict("os.environ", env, clear=False), redirect_stdout(out):
                code = lua.main(["announce", "--old", paths[0], "--new", paths[1],
                                 "--tag", "v30-20261001000000", "--as-of", "2026-10-01", *argv])
            return code, out.getvalue()

    def setUp(self):
        self.old = catalog([(1, 2, 1, "בראשית")], [(1, 0, "א")])
        self.new = catalog([(1, 2, 1, "בראשית"), (2, 2, 1, "שמות")], [(1, 0, "א"), (2, 0, "ב")])

    def test_nothing_changed_sends_nothing(self):
        with mock.patch.object(lua, "send_forum") as forum:
            code, out = self.run_announce(self.old, self.old, {"ANNOUNCE": "true"})
        self.assertEqual(0, code)
        self.assertIn("nothing to announce", out)
        forum.assert_not_called()

    def test_dry_run_prints_but_does_not_send(self):
        with mock.patch.object(lua, "send_forum") as forum:
            code, out = self.run_announce(self.old, self.new, {"ANNOUNCE": "false"})
        self.assertEqual(0, code)
        self.assertIn("ספרייה/תנך/שמות", out)
        forum.assert_not_called()

    def test_each_channel_is_tried_and_a_failure_fails_the_run(self):
        env = {"ANNOUNCE": "true", "FORUM_TOKEN": "t",
               "GOOGLE_CHAT_URL": "https://chat.invalid", "TOKEN_YEMOT": "t"}
        with mock.patch.object(lua, "send_forum", side_effect=RuntimeError("down")) as forum, \
             mock.patch.object(lua, "send_chat") as chat, \
             mock.patch.object(lua, "send_yemot") as yemot:
            code, out = self.run_announce(self.old, self.new, env)
        self.assertEqual(1, code)
        forum.assert_called_once()
        chat.assert_called_once()
        yemot.assert_called_once()
        self.assertIn("forum announcement failed", out)

    def test_channels_can_be_limited_for_a_resend(self):
        env = {"ANNOUNCE": "true", "GOOGLE_CHAT_URL": "https://chat.invalid"}
        with mock.patch.object(lua, "send_forum") as forum, mock.patch.object(lua, "send_chat") as chat:
            code, _ = self.run_announce(self.old, self.new, env, ["--channels", "chat"])
        self.assertEqual(0, code)
        forum.assert_not_called()
        chat.assert_called_once()


class ForumTest(unittest.TestCase):
    def test_the_rate_limit_refusal_is_retried(self):
        post = mock.Mock(side_effect=[lua.ForumRefused("400: [[error:too-many-posts, 10]]"), None])
        with mock.patch.object(lua.time, "sleep"):
            lua.send_forum("x", "t", post=post)
        self.assertEqual(2, post.call_count)

    def test_any_other_refusal_is_not_retried(self):
        post = mock.Mock(side_effect=lua.ForumRefused("403: [[error:no-privileges]]"))
        with self.assertRaises(lua.ForumRefused):
            lua.send_forum("x", "t", post=post)
        post.assert_called_once()

    def test_the_token_is_sent_as_a_bearer_to_the_topic(self):
        session = mock.Mock()
        session.post.return_value.json.return_value = {"status": {"code": "ok"}}
        lua.post_to_forum("טקסט", "secret", session=session)
        url = session.post.call_args.args[0]
        kwargs = session.post.call_args.kwargs
        self.assertEqual(f"https://otzaria.org/forum/api/v3/topics/{lua.FORUM_TOPIC_ID}", url)
        self.assertEqual("Bearer secret", kwargs["headers"]["Authorization"])
        self.assertEqual({"content": "טקסט"}, kwargs["json"])

    def test_a_refusal_in_the_body_is_not_a_success(self):
        session = mock.Mock()
        session.post.return_value.json.return_value = {"status": {"code": "bad-request", "message": "x"}}
        with self.assertRaises(lua.ForumRefused):
            lua.post_to_forum("x", "t", session=session)


@unittest.skipIf(yaml is None, "PyYAML is required to parse the workflow")
class WorkflowContractTest(unittest.TestCase):
    def setUp(self):
        self.doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
        # PyYAML reads the bare `on:` key as boolean True.
        self.on = self.doc.get("on", self.doc.get(True))

    def test_runs_only_for_published_releases(self):
        self.assertEqual(["published"], self.on["release"]["types"])
        job = self.doc["jobs"]["announce"]
        self.assertIn("github.event.release.prerelease == false", job["if"])

    def test_manual_run_is_a_dry_run_by_default(self):
        inputs = self.on["workflow_dispatch"]["inputs"]
        self.assertTrue(inputs["tag"]["required"])
        self.assertIs(True, inputs["dry_run"]["default"])


if __name__ == "__main__":
    unittest.main()
