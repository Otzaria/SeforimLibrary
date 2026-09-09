import base64
import hashlib
import importlib.util
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("verify_relink_recovery_snapshot.py")


def _verifier():
    spec = importlib.util.spec_from_file_location("verify_relink_recovery_snapshot", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# The accepted set is never restated here: the tests read the one constant the
# verifier and the dispatch-time preflight both use.
PAYLOAD_META_SCHEMAS = _verifier().PAYLOAD_META_SCHEMAS

# What the Linker writes TODAY, observed in the sibling checkout rather than
# assumed: LinkerToOtzaria src/incremental.py, write_meta() at lines 275-288,
# '"schema_version": 3' at line 280 (a9ae2d4, 2026-08-31). CI here cannot see
# that repository, so this pair — the literal below and the key layout in
# linker_meta() — is the recorded observation, and the durable cross-repo gate
# is the "Preflight the recovery relink inputs" step in
# manual-generate-release.yml, which runs the verifier against the real
# published meta.json before the build spends a minute.
LINKER_META_SCHEMA_TODAY = 3


def linker_meta(snapshot_sha256: str, schema: int = LINKER_META_SCHEMA_TODAY) -> dict:
    """A meta.json in the Linker's exact writer layout (incremental.write_meta)."""
    return {
        "schema_version": schema,
        "description": "Lineage of the last linker run (see stage 3). Source-change clock = snapshot.",
        "snapshot": {"sha256": snapshot_sha256, "book_count": 7316},
        "sefaria": {"export_tag": "2026-09-05_22-40-33987734987-1"},
        "engine": {
            "ambiguity_policy": "drop",
            "bavli_convention": False,
            "fingerprint": "dump=44e55916743983b8;policy=drop;bavli=0",
        },
        "generated_at": "2026-09-06T08:23:55Z",
    }


def make_snapshot(path: Path, content: str, context: str = "ספר א") -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE lines_snapshot (
            source_name TEXT NOT NULL,
            canonical_he_title TEXT NOT NULL,
            line_index INTEGER NOT NULL,
            content TEXT NOT NULL,
            context_ref TEXT NOT NULL
        );
        CREATE TABLE lines_snapshot_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE INDEX idx_ls_book ON lines_snapshot(source_name, canonical_he_title, line_index);
        """
    )
    connection.execute(
        "INSERT INTO lines_snapshot VALUES(?,?,?,?,?)",
        ("Sefaria", "ספר", 0, content, context),
    )
    for key, value in (
        ("book_count", "1"),
        ("context_policy", "explicit-relative-v1"),
        ("line_count", "1"),
        ("schema_version", "2"),
        ("source_db", "seforim.db"),
    ):
        connection.execute("INSERT INTO lines_snapshot_meta VALUES(?,?)", (key, value))
    connection.commit()
    connection.close()


class _VerifierCase(unittest.TestCase):
    def temp_root(self) -> Path:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return Path(temporary.name)

    def preflight(self, document):
        """Run the dispatch-time mode: meta.json only, no database."""
        payload_meta = self.temp_root() / "meta.json"
        payload_meta.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--preflight", "--payload-meta", str(payload_meta)],
            text=True,
            capture_output=True,
        )


class RecoverySnapshotVerifierTest(_VerifierCase):
    def run_case(self, original_content, rebuilt_content, artifact_record=None, rebuilt_context="ספר א",
                 meta_schema=LINKER_META_SCHEMA_TODAY, baseline_schema=2):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        original, rebuilt = root / "original.db", root / "rebuilt.db"
        make_snapshot(original, original_content)
        make_snapshot(rebuilt, rebuilt_content, rebuilt_context)
        digest = hashlib.sha256(original.read_bytes()).hexdigest()
        payload_meta = root / "meta.json"
        payload_meta.write_text(
            json.dumps(linker_meta(digest, meta_schema), ensure_ascii=False),
            encoding="utf-8",
        )
        baseline = root / "baseline.json"
        baseline.write_text(
            json.dumps({"schema_version": baseline_schema, "snapshot_sha256": digest}),
            encoding="utf-8",
        )
        artifacts = root / "artifacts" / "Sefaria"
        artifacts.mkdir(parents=True)
        if artifact_record is not None:
            (artifacts / "ספר.jsonl").write_text(
                json.dumps(artifact_record, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        return subprocess.run(
            [
                sys.executable, str(SCRIPT), "--original", str(original),
                "--rebuilt", str(rebuilt), "--artifacts", str(root / "artifacts"),
                "--payload-meta", str(payload_meta),
                "--baseline-manifest", str(baseline),
            ],
            text=True,
            capture_output=True,
        )

    def test_identical_snapshots_pass(self):
        result = self.run_case("טקסט", "טקסט")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("unlinked_image_src_differences=0", result.stdout)

    def test_every_accepted_linker_meta_schema_passes(self):
        # One case per accepted version, taken from the verifier's own constant:
        # the Linker writes schema 3 since a9ae2d4 and the snapshot sha field it
        # binds is unchanged, so a recovery must still verify.
        for schema in sorted(PAYLOAD_META_SCHEMAS):
            with self.subTest(schema=schema):
                result = self.run_case("טקסט", "טקסט", meta_schema=schema)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("RECOVERY_SNAPSHOT_SEMANTIC_OK", result.stdout)

    def test_unknown_linker_meta_schema_fails(self):
        unknown = max(PAYLOAD_META_SCHEMAS) + 1
        result = self.run_case("טקסט", "טקסט", meta_schema=unknown)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Linker meta schema", result.stderr)
        self.assertIn("meta.json", result.stderr)
        self.assertIn(str(unknown), result.stderr)

    def test_phase2_and_the_dispatch_preflight_refuse_a_schema_identically(self):
        # Single-sourced: both entry points call check_payload_meta, so a schema
        # the cheap dispatch-time gate accepts can never be one the expensive
        # Phase-2 gate rejects — the drift that cost run 34021998271 48 minutes.
        unknown = max(PAYLOAD_META_SCHEMAS) + 1
        phase2 = self.run_case("טקסט", "טקסט", meta_schema=unknown)
        preflight = self.preflight(linker_meta("a" * 64, unknown))
        self.assertNotEqual(phase2.returncode, 0)
        self.assertNotEqual(preflight.returncode, 0)
        reason = f"is not an accepted Linker meta schema {sorted(PAYLOAD_META_SCHEMAS)}"
        self.assertIn(reason, phase2.stderr)
        self.assertIn(reason, preflight.stderr)

    def test_unknown_baseline_schema_fails(self):
        result = self.run_case("טקסט", "טקסט", baseline_schema=3)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("line baseline schema 2", result.stderr)

    def test_unlinked_remote_to_inline_image_passes(self):
        inline = base64.b64encode(b"png").decode()
        result = self.run_case(
            '<img src="https://textimages.sefaria.org/book/image.png">',
            f'<img src="data:image/png;base64,{inline}">',
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("unlinked_image_src_differences=1", result.stdout)

    def test_unlinked_image_inside_text_passes(self):
        inline = base64.b64encode(b"png").decode()
        result = self.run_case(
            'לפני <img class="diagram" src="https://textimages.sefaria.org/book/image.png"> אחרי',
            f'לפני <img class="diagram" src="data:image/png;base64,{inline}"> אחרי',
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("unlinked_image_src_differences=1", result.stdout)

    def test_only_changed_remote_sources_may_be_embedded(self):
        inline = base64.b64encode(b"png").decode()
        result = self.run_case(
            '<img src="data:image/png;base64,c2FtZQ=="> '
            '<img src="https://textimages.sefaria.org/book/image.png">',
            '<img src="data:image/png;base64,c2FtZQ=="> '
            f'<img src="data:image/png;base64,{inline}">',
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_other_image_src_change_fails(self):
        result = self.run_case(
            '<img src="https://example.com/old.png">',
            '<img src="https://example.com/new.png">',
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("text differs", result.stderr)

    def test_text_around_image_difference_fails(self):
        inline = base64.b64encode(b"png").decode()
        result = self.run_case(
            'טקסט ישן <img src="https://textimages.sefaria.org/book/image.png">',
            f'טקסט חדש <img src="data:image/png;base64,{inline}">',
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("text differs", result.stderr)

    def test_linked_image_difference_fails(self):
        inline = base64.b64encode(b"png").decode()
        record = {
            "book_key": {"source_name": "Sefaria", "canonical_he_title": "ספר"},
            "line_index": 0,
        }
        result = self.run_case(
            '<img src="https://textimages.sefaria.org/book/image.png">',
            f'<img src="data:image/png;base64,{inline}">',
            artifact_record=record,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("has a Linker record", result.stderr)

    def test_linked_mixed_content_image_difference_fails(self):
        inline = base64.b64encode(b"png").decode()
        record = {
            "book_key": {"source_name": "Sefaria", "canonical_he_title": "ספר"},
            "line_index": 0,
        }
        result = self.run_case(
            'טקסט <img src="https://textimages.sefaria.org/book/image.png">',
            f'טקסט <img src="data:image/png;base64,{inline}">',
            artifact_record=record,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("has a Linker record", result.stderr)

    def test_text_difference_fails(self):
        result = self.run_case("טקסט ישן", "טקסט חדש")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("text differs", result.stderr)

    def test_context_difference_fails(self):
        result = self.run_case("טקסט", "טקסט", rebuilt_context="ספר ב")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("context_ref differs", result.stderr)


class RecoveryMetaPreflightTest(_VerifierCase):
    """The dispatch-time gate: the same checks, on the real file, at second 0.

    Build 34021998271 rebuilt the whole database, dumped the snapshot and
    uploaded 987 MiB before Phase-2 rejected the Linker's meta.json. These cases
    pin the cheap mode that now runs that check while the recovery run id is
    still just a dispatch input.
    """

    def test_the_linker_writer_layout_is_accepted(self):
        # The fixture is the Linker's real key layout, not a two-key stand-in:
        # a fixture that only ever contains what the verifier reads can never
        # observe the drift that killed 34021998271.
        digest = "0" * 63 + "1"
        result = self.preflight(linker_meta(digest))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(),
            f"RECOVERY_META_PREFLIGHT_OK schema={LINKER_META_SCHEMA_TODAY} "
            f"snapshot_sha256={digest}",
        )

    def test_every_accepted_schema_preflights(self):
        for schema in sorted(PAYLOAD_META_SCHEMAS):
            with self.subTest(schema=schema):
                result = self.preflight(linker_meta("a" * 64, schema))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"schema={schema}", result.stdout)

    def test_the_next_linker_schema_is_refused_by_name_and_value(self):
        unknown = max(PAYLOAD_META_SCHEMAS) + 1
        result = self.preflight(linker_meta("a" * 64, unknown))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("meta.json", result.stderr)
        self.assertIn("Linker meta schema", result.stderr)
        self.assertIn(str(unknown), result.stderr)

    def test_meta_without_a_usable_snapshot_digest_is_refused(self):
        document = linker_meta("a" * 64)
        document["snapshot"] = {"book_count": 7316}
        result = self.preflight(document)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("snapshot.sha256", result.stderr)

    def test_preflight_cannot_silently_downgrade_the_full_comparison(self):
        # --preflight beside the comparison flags must refuse, not quietly skip
        # the 5 GiB check and exit 0 — that would be a fail-open Phase-2.
        root = self.temp_root()
        payload_meta = root / "meta.json"
        payload_meta.write_text(json.dumps(linker_meta("a" * 64)), encoding="utf-8")
        result = subprocess.run(
            [
                sys.executable, str(SCRIPT), "--preflight",
                "--payload-meta", str(payload_meta),
                "--original", str(root / "o.db"), "--rebuilt", str(root / "r.db"),
                "--artifacts", str(root), "--baseline-manifest", str(root / "b.json"),
            ],
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cannot be combined with", result.stderr)
        self.assertNotIn("RECOVERY_META_PREFLIGHT_OK", result.stdout)

    def test_accepted_set_records_the_observed_linker_schema(self):
        # A bump must be an observation, not a guess: whoever widens the set has
        # to re-read LinkerToOtzaria src/incremental.py write_meta and update the
        # recorded version and the fixture layout together.
        self.assertIn(LINKER_META_SCHEMA_TODAY, PAYLOAD_META_SCHEMAS)
        self.assertEqual(
            max(PAYLOAD_META_SCHEMAS),
            LINKER_META_SCHEMA_TODAY,
            "PAYLOAD_META_SCHEMAS accepts a schema newer than the Linker writer "
            "recorded in LINKER_META_SCHEMA_TODAY — re-observe write_meta() in "
            "LinkerToOtzaria src/incremental.py and update both.",
        )


if __name__ == "__main__":
    unittest.main()
