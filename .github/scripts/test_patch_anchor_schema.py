"""Contract for the pre-download patch-fan anchor check.

The check is allowed to be wrong in exactly one direction: it may say PROCEED
about an anchor the producer later rejects (the old behaviour, minus nothing),
but an UNPATCHABLE verdict must reproduce the producer's own rule, because it
costs the fan an anchor without ever downloading the DB that would prove it.
"""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("patch_anchor_schema.py")
SPEC = importlib.util.spec_from_file_location("patch_anchor_schema", SCRIPT)
anchor = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(anchor)

VALIDATOR = Path(__file__).with_name("validate_build_provenance.py")
V_SPEC = importlib.util.spec_from_file_location("build_provenance", VALIDATOR)
provenance = importlib.util.module_from_spec(V_SPEC)
assert V_SPEC.loader is not None
V_SPEC.loader.exec_module(provenance)

RESOURCES = (
    Path(__file__).parents[2]
    / "generator" / "common" / "src" / "jvmTest" / "resources"
)

# This build's physical tables. `scratch` is deliberately NOT in the patch
# contract below, and `line_dh` is deliberately promoted into it at schema 4.
THIS_SCHEMA = {
    "db_schema_version": 4,
    "tables": {
        "category": ["heShortDesc", "id"],
        "line_dh": ["id", "lineId"],
        "link": ["baseProvenance", "id", "sourceBookId"],
        "schema_meta": ["key", "value"],
        "scratch": ["id"],
    },
}
CONTRACT_4 = ["category", "line_dh", "link", "schema_meta"]
CONTRACT_1 = ["category", "link", "schema_meta"]


def contract_fixture(tables, schema_version):
    return {
        "schemaVersion": schema_version,
        "fkOrder": [{"table": t, "pk": ["id"], "updatable": True} for t in tables],
        "hashOrder": list(tables),
    }


class PatchAnchorSchemaTest(unittest.TestCase):
    def paths(self, tmp, anchor_tables=None, extra=None, anchor_schema_version=1):
        """(this build's dump, anchor provenance, current contract fixture)."""
        root = Path(tmp)
        this = root / "db_schema.json"
        this.write_text(json.dumps(THIS_SCHEMA), encoding="utf-8")
        contract = root / "patch_tables_contract.json"
        contract.write_text(json.dumps(contract_fixture(CONTRACT_4, 4)), encoding="utf-8")
        (root / "patch_tables_contract_schema_1.json").write_text(
            json.dumps(contract_fixture(CONTRACT_1, 1)), encoding="utf-8"
        )
        prov = root / "build_provenance.json"
        if prov.exists():
            prov.unlink()
        if anchor_tables is not None:
            block = {"db_schema_version": anchor_schema_version, "tables": anchor_tables}
            prov.write_text(json.dumps({"db_schema": block, **(extra or {})}), encoding="utf-8")
        elif extra is not None:
            prov.write_text(json.dumps(extra), encoding="utf-8")
        return this, prov, contract

    def test_dropped_column_is_the_producers_rule(self):
        # PatchDbProducer.planColumnMigrations: a column prev carries and the
        # new schema no longer declares can never be expressed as a patch.
        with tempfile.TemporaryDirectory() as tmp:
            this, prov, contract = self.paths(
                tmp, {"link": ["id", "isDeclaredBase", "sourceBookId"]}
            )
            verdict, reason = anchor.check(this, 20, prov, contract)
        self.assertEqual(anchor.UNPATCHABLE, verdict)
        self.assertIn("isDeclaredBase", reason)
        self.assertIn("link", reason)

    def test_added_column_is_migratable_and_never_skipped(self):
        # 849f754 made an unbumped ADD COLUMN patchable via ALTER TABLE, so the
        # pre-check must not resurrect the old skip.
        with tempfile.TemporaryDirectory() as tmp:
            this, prov, contract = self.paths(tmp, {"category": ["id"]})
            verdict, _ = anchor.check(this, 20, prov, contract)
        self.assertEqual(anchor.PROCEED, verdict)

    def test_table_absent_from_the_anchor_is_not_a_drop(self):
        # A table missing from prev is (re)created from a full snapshot by the
        # producer, never column-diffed.
        with tempfile.TemporaryDirectory() as tmp:
            this, prov, contract = self.paths(
                tmp, {"link": ["baseProvenance", "id", "sourceBookId"]}
            )
            verdict, _ = anchor.check(this, 20, prov, contract)
        self.assertEqual(anchor.PROCEED, verdict)

    def test_table_outside_the_patch_contract_is_never_compared(self):
        # The producer iterates PATCH_TABLES_IN_FK_ORDER only. A physical table
        # outside that list losing a column is none of the patch's business, and
        # skipping the anchor for it would throw away a patch the producer would
        # have produced — the severe direction of being wrong.
        with tempfile.TemporaryDirectory() as tmp:
            this, prov, contract = self.paths(
                tmp,
                {
                    "link": ["baseProvenance", "id", "sourceBookId"],
                    "scratch": ["gone", "id"],
                },
            )
            verdict, reason = anchor.check(this, 20, prov, contract)
        self.assertEqual(anchor.PROCEED, verdict, reason)

    def test_promoted_table_present_in_the_anchor_is_never_compared(self):
        # `promotedTables = target − from` are `continue`d by the producer even
        # when they exist physically in prev, so a dropped column there must not
        # skip the anchor either. line_dh is in the schema-4 contract and not in
        # the schema-1 one, and the anchor claims schema 1.
        with tempfile.TemporaryDirectory() as tmp:
            this, prov, contract = self.paths(
                tmp,
                {
                    "line_dh": ["id", "legacyColumn", "lineId"],
                    "link": ["baseProvenance", "id", "sourceBookId"],
                },
                anchor_schema_version=1,
            )
            verdict, reason = anchor.check(this, 20, prov, contract)
        self.assertEqual(anchor.PROCEED, verdict, reason)

    def test_missing_contract_fixture_defers_instead_of_widening(self):
        # Without the producer's table list the comparison has no defensible
        # scope, so it must not fall back to "every physical table".
        with tempfile.TemporaryDirectory() as tmp:
            this, prov, contract = self.paths(
                tmp, {"link": ["id", "isDeclaredBase", "sourceBookId"]}
            )
            self.assertEqual(anchor.UNPATCHABLE, anchor.check(this, 20, prov, contract)[0])
            for absent in (None, Path(tmp) / "no-contract.json"):
                verdict, reason = anchor.check(this, 20, prov, absent)
                self.assertEqual(anchor.PROCEED, verdict)
                self.assertIn("contract", reason)
            # An unparsable fixture is an error inside check(); the CLI turns
            # every such surprise into PROCEED (see the CLI test below).
            contract.write_text("{not json", encoding="utf-8")
            with self.assertRaises(ValueError):
                anchor.check(this, 20, prov, contract)

    def test_underivable_promotion_scope_defers(self):
        # No frozen contract exists for db schema 2, so the promoted-table set
        # cannot be derived and the comparison must not run at all.
        with tempfile.TemporaryDirectory() as tmp:
            this, prov, contract = self.paths(
                tmp,
                {"link": ["id", "isDeclaredBase", "sourceBookId"]},
                anchor_schema_version=2,
            )
            verdict, reason = anchor.check(this, 20, prov, contract)
        self.assertEqual(anchor.PROCEED, verdict)
        self.assertIn("promoted-table set is not derivable", reason)

    def test_this_builds_schema_must_match_the_contract_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            this, prov, contract = self.paths(
                tmp, {"link": ["id", "isDeclaredBase", "sourceBookId"]}
            )
            contract.write_text(json.dumps(contract_fixture(CONTRACT_4, 5)), encoding="utf-8")
            verdict, reason = anchor.check(this, 20, prov, contract)
        self.assertEqual(anchor.PROCEED, verdict)
        self.assertIn("contract fixture freezes db schema 5", reason)

    def test_repository_contract_fixtures_are_the_ones_this_check_reads(self):
        # The workflow points --contract-tables at this exact file in the
        # payload checkout, and the schema-1 sibling is what makes the
        # promoted-table set derivable for the oldest anchors still in the fan.
        current = RESOURCES / "patch_tables_contract.json"
        tables, version = anchor._contract(current)
        # Not pinned to a number: every db_schema_version bump (4 -> 5 for
        # line_dh.dhDisplay, PR #24) adds a patch_table_columns_schema_<N>.json
        # and freezes the previous contract as a sibling; the check must keep
        # reading whichever pair the repo currently carries.
        self.assertGreaterEqual(version, 4)
        columns = json.loads(
            (RESOURCES / f"patch_table_columns_schema_{version}.json").read_text(encoding="utf-8")
        )
        self.assertEqual(version, int(columns["dbSchemaVersion"]))
        self.assertEqual(set(columns["tables"]), set(tables))
        siblings = sorted(
            int(p.stem.rsplit("_", 1)[1])
            for p in RESOURCES.glob("patch_tables_contract_schema_*.json")
        )
        # The oldest anchors still in the fan need schema 1 and 3; the schema
        # just superseded must be frozen too, or promotion is underivable.
        self.assertTrue({1, 3, version - 1} <= set(siblings), siblings)
        for older in siblings:
            self.assertLess(older, version)
            sibling = current.with_name(f"patch_tables_contract_schema_{older}.json")
            older_tables, older_version = anchor._contract(sibling)
            self.assertEqual(older, older_version)
            # Frozen contracts only ever gain tables, so promotion is a
            # subtraction in one direction.
            self.assertEqual(set(), set(older_tables) - set(tables))

    def test_legacy_versions_are_skipped_without_any_metadata(self):
        # v9–v13 carry link.isDeclaredBase (dropped in 7755902 with no
        # db_schema_version bump) and predate the db_schema block entirely.
        self.assertEqual(frozenset(range(9, 14)), anchor.LEGACY_UNPATCHABLE_DB_VERSIONS)
        with tempfile.TemporaryDirectory() as tmp:
            this, prov, contract = self.paths(tmp)
            for version in sorted(anchor.LEGACY_UNPATCHABLE_DB_VERSIONS):
                verdict, reason = anchor.check(this, version, prov, contract)
                self.assertEqual(anchor.UNPATCHABLE, verdict)
                self.assertIn("isDeclaredBase", reason)
            # v14–v25 were proven patchable by 849f754 and must stay in the fan.
            for version in (8, 14, 25, 26):
                self.assertEqual(
                    anchor.PROCEED, anchor.check(this, version, prov, contract)[0]
                )

    def test_missing_or_legacy_provenance_defers_to_the_producer(self):
        with tempfile.TemporaryDirectory() as tmp:
            this, prov, contract = self.paths(tmp)
            self.assertEqual(anchor.PROCEED, anchor.check(this, 20, prov, contract)[0])
            this, prov, contract = self.paths(tmp, extra={"schema_version": 3})
            self.assertEqual(anchor.PROCEED, anchor.check(this, 20, prov, contract)[0])
            prov.write_text(json.dumps({"db_schema": {"tables": []}}), encoding="utf-8")
            self.assertEqual(anchor.PROCEED, anchor.check(this, 20, prov, contract)[0])

    def test_cli_never_fails_the_release_and_prints_one_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            this, prov, contract = self.paths(
                tmp, {"link": ["id", "isDeclaredBase", "sourceBookId"]}
            )
            base = ["check", "--this-schema", str(this), "--anchor-version", "20",
                    "--anchor-provenance", str(prov)]
            expected = (
                (base + ["--contract-tables", str(contract)], anchor.UNPATCHABLE),
                # No --contract-tables at all: PROCEED, never a wider comparison.
                (base, anchor.PROCEED),
                # A corrupt input degrades to PROCEED rather than exiting non-zero.
                (["check", "--this-schema", str(Path(tmp) / "nope.json"),
                  "--anchor-version", "20", "--anchor-provenance", str(prov),
                  "--contract-tables", str(contract)], anchor.PROCEED),
            )
            for args, verdict in expected:
                captured = io.StringIO()
                with contextlib.redirect_stdout(captured):
                    self.assertEqual(0, anchor.main(args))
                # Exactly one line, so `${PRECHECK%% *}` in the workflow sees a
                # verdict and `${PRECHECK#* }` sees the whole reason.
                self.assertEqual(1, captured.getvalue().count("\n"))
                self.assertEqual(verdict, captured.getvalue().split(" ", 1)[0])

    def test_dump_is_canonical_and_survives_the_provenance_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "seforim.db"
            conn = sqlite3.connect(db)
            conn.executescript(
                "CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT);"
                "INSERT INTO schema_meta VALUES('db_schema_version','4');"
                "CREATE TABLE link(id INTEGER PRIMARY KEY, sourceBookId INTEGER,"
                " baseProvenance INTEGER);"
            )
            conn.commit()
            conn.close()
            dumped = anchor.dump(db)

        self.assertEqual(4, dumped["db_schema_version"])
        # Columns are sorted like PatchTableColumnContractTest sorts them, and
        # the block must satisfy the provenance contract it is embedded in.
        self.assertEqual(
            ["baseProvenance", "id", "sourceBookId"], dumped["tables"]["link"]
        )
        self.assertEqual(["key", "value"], dumped["tables"]["schema_meta"])
        value = self.provenance_value(dumped)
        provenance.validate(value)
        canonical = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        self.assertIn('"db_schema":{"db_schema_version":4', canonical)

    def provenance_value(self, db_schema):
        sha = "a" * 64
        return {
            "schema_version": 4,
            "correlation_id": f"sefaria:1:2:export-v1:{sha}",
            "source_commit": "b" * 40,
            "sefaria_tag": "export-v1",
            "sefaria_release_metadata_sha256": sha,
            "sefaria_archive_sha256": "c" * 64,
            "otzaria_tag": "library-links-1",
            "otzaria_asset_sha256": "d" * 64,
            "fordb_archive_sha256": "e" * 64,
            "fordb_tag": "fordb-sha256-" + "e" * 64,
            "expected_links_commit": "f" * 40,
            "otzaria_target_commit": "f" * 40,
            "linker_payload_sha256": "1" * 64,
            "linker_engine_fingerprint": "engine=test",
            "linker_relink_run_id": 3,
            "linker_commit": "2" * 40,
            "linker_relink_run_attempt": 1,
            "linker_relink_request_id": "3" * 64,
            "phase2_implementation_commit": "4" * 40,
            "lineage_sha256": "4" * 64,
            "config_sha256": "5" * 64,
            "source_links_tree_sha256": "6" * 64,
            "packaged_links_tree_sha256": "7" * 64,
            "db_schema": db_schema,
            "assets": [
                {"name": "lines_snapshot.db.zst", "size": 1, "sha256": "8" * 64},
                {"name": "seforim.db.buildstate", "size": 1, "sha256": "9" * 64},
                {"name": "seforim.db.zst", "size": 1, "sha256": "a" * 64},
            ],
        }


if __name__ == "__main__":
    unittest.main()
