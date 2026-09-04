import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("validate_build_provenance.py")
SPEC = importlib.util.spec_from_file_location("build_provenance", SCRIPT)
contract = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(contract)


class BuildProvenanceContractTest(unittest.TestCase):
    # The asset set every release up to and including v26 (provenance v1..v4)
    # published: an uncompressed 990 MB buildstate plus a second copy of the
    # lines snapshot that the content-addressed pre-release already carried.
    LEGACY_ASSETS = [
        {"name": "lines_snapshot.db.zst", "size": 1, "sha256": "8" * 64},
        {"name": "seforim.db.buildstate", "size": 1, "sha256": "9" * 64},
        {"name": "seforim.db.zst", "size": 1, "sha256": "a" * 64},
    ]

    def downgrade(self, version):
        """A published document of an older schema version, assets and all."""
        value = self.value()
        value["schema_version"] = version
        for key in contract.V5_KEYS - {1: contract.V1_KEYS, 2: contract.V2_KEYS,
                                       3: contract.V3_KEYS, 4: contract.V4_KEYS}[version]:
            del value[key]
        value["assets"] = [dict(asset) for asset in self.LEGACY_ASSETS]
        return value

    def value(self):
        sha = "a" * 64
        return {
            "schema_version": 5,
            "snapshot_zst_sha256": "b" * 64,
            "snapshot_release_tag": "lines-snapshot-sha256-" + "b" * 64,
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
            "db_schema": {
                "db_schema_version": 4,
                "tables": {
                    "link": ["baseProvenance", "id", "sourceBookId"],
                    "schema_meta": ["key", "value"],
                },
            },
            "assets": [
                {"name": "seforim.db.buildstate.zst", "size": 1, "sha256": "9" * 64},
                {"name": "seforim.db.zst", "size": 1, "sha256": "a" * 64},
            ],
        }

    def write(self, root, value=None, raw=None):
        path = Path(root) / "build_provenance.json"
        value = self.value() if value is None else value
        path.write_bytes(raw if raw is not None else (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode())
        return path

    def test_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            value = contract.load(self.write(tmp))
            contract.validate(value)

    def test_published_v1_contract_remains_readable_but_cannot_claim_v2_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            value = self.downgrade(1)
            contract.validate(contract.load(self.write(tmp, value)))

            value["fordb_archive_sha256"] = "e" * 64
            with self.assertRaises(ValueError):
                contract.load(self.write(tmp, value))

            del value["fordb_archive_sha256"]
            value["assets"][0]["size"] = 0
            with self.assertRaises(ValueError):
                contract.validate(contract.load(self.write(tmp, value)))

    def test_duplicate_and_boolean_schema_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = json.dumps(self.value(), sort_keys=True, separators=(",", ":"))[:-1] + ',"schema_version":5}\n'
            with self.assertRaises(ValueError):
                contract.load(self.write(tmp, raw=raw.encode()))
            value = self.value()
            value["schema_version"] = True
            with self.assertRaises(ValueError):
                contract.validate(contract.load(self.write(tmp, value)))

    def test_published_v2_contract_remains_readable(self):
        with tempfile.TemporaryDirectory() as tmp:
            contract.validate(contract.load(self.write(tmp, self.downgrade(2))))

    def test_phase2_commit_is_strict(self):
        with tempfile.TemporaryDirectory() as tmp:
            value = self.value()
            value["phase2_implementation_commit"] = "not-a-commit"
            with self.assertRaises(ValueError):
                contract.validate(contract.load(self.write(tmp, value)))

    def test_published_v3_contract_remains_readable_without_db_schema(self):
        # A prior release published before the patch-fan pre-check existed is
        # still read (and its reuse claim still trusted) by this workflow, so
        # v3 must keep validating — it simply offers no anchor evidence.
        with tempfile.TemporaryDirectory() as tmp:
            value = self.downgrade(3)
            contract.validate(contract.load(self.write(tmp, value)))

            # …and a v3 document may not smuggle the v4 key in.
            value["db_schema"] = self.value()["db_schema"]
            with self.assertRaises(ValueError):
                contract.load(self.write(tmp, value))

    def test_published_v4_contract_remains_readable_with_its_own_asset_set(self):
        # v26 and every earlier release published the uncompressed buildstate and
        # a duplicate lines_snapshot.db.zst. Those documents must keep validating
        # exactly as published — the patch fan still reads them off old releases.
        with tempfile.TemporaryDirectory() as tmp:
            value = self.downgrade(4)
            contract.validate(contract.load(self.write(tmp, value)))

            # …and a v4 document may not smuggle the v5 keys in.
            value["snapshot_zst_sha256"] = "b" * 64
            with self.assertRaises(ValueError):
                contract.load(self.write(tmp, value))

    def test_v5_names_the_snapshot_pre_release_instead_of_shipping_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            # The tag must be the digest's own content-addressed release: a
            # consumer resolves the snapshot from here and verifies the bytes.
            value = self.value()
            value["snapshot_release_tag"] = "lines-snapshot-sha256-" + "c" * 64
            with self.assertRaises(ValueError):
                contract.validate(contract.load(self.write(tmp, value)))

            for broken in ("", "not-a-sha", "B" * 64, "b" * 63):
                value = self.value()
                value["snapshot_zst_sha256"] = broken
                value["snapshot_release_tag"] = "lines-snapshot-sha256-" + broken
                with self.assertRaises(ValueError):
                    contract.validate(contract.load(self.write(tmp, value)))

            # v5 requires the compressed buildstate…
            value = self.value()
            value["assets"] = [
                {"name": "seforim.db.buildstate", "size": 1, "sha256": "9" * 64},
                {"name": "seforim.db.zst", "size": 1, "sha256": "a" * 64},
            ]
            with self.assertRaises(ValueError):
                contract.validate(contract.load(self.write(tmp, value)))

            # …and refuses to re-publish either superseded asset.
            for superseded in ("seforim.db.buildstate", "lines_snapshot.db.zst"):
                value = self.value()
                value["assets"] = sorted(
                    value["assets"] + [{"name": superseded, "size": 1, "sha256": "8" * 64}],
                    key=lambda asset: asset["name"].encode("utf-8"),
                )
                with self.assertRaises(ValueError):
                    contract.validate(contract.load(self.write(tmp, value)))

    def test_db_schema_block_is_strict(self):
        with tempfile.TemporaryDirectory() as tmp:
            for broken in (
                {"db_schema_version": 4},
                {"db_schema_version": 4, "tables": {}, "extra": 1},
                {"db_schema_version": 0, "tables": {"link": ["id"]}},
                {"db_schema_version": True, "tables": {"link": ["id"]}},
                {"db_schema_version": 4, "tables": {}},
                {"db_schema_version": 4, "tables": {"link": []}},
                {"db_schema_version": 4, "tables": {"link": "id"}},
                {"db_schema_version": 4, "tables": {"link": ["id", 1]}},
                {"db_schema_version": 4, "tables": {"link": ["id", "id"]}},
                # Unsorted columns would make the same DB hash two ways.
                {"db_schema_version": 4, "tables": {"link": ["id", "baseProvenance"]}},
                {"db_schema_version": 4, "tables": {"drop table": ["id"]}},
            ):
                value = self.value()
                value["db_schema"] = broken
                with self.assertRaises(ValueError):
                    contract.validate(contract.load(self.write(tmp, value)))

    def test_attempt_and_asset_order_are_strict(self):
        with tempfile.TemporaryDirectory() as tmp:
            value = self.value()
            value["linker_relink_run_attempt"] = True
            with self.assertRaises(ValueError):
                contract.validate(contract.load(self.write(tmp, value)))
            value = self.value()
            value["assets"].reverse()
            with self.assertRaises(ValueError):
                contract.validate(contract.load(self.write(tmp, value)))


if __name__ == "__main__":
    unittest.main()
