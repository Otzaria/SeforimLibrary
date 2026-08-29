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
    def value(self):
        sha = "a" * 64
        return {
            "schema_version": 3,
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
            "assets": [
                {"name": "lines_snapshot.db.zst", "size": 1, "sha256": "8" * 64},
                {"name": "seforim.db.buildstate", "size": 1, "sha256": "9" * 64},
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
            value = self.value()
            value["schema_version"] = 1
            for key in contract.V3_KEYS - contract.V1_KEYS:
                del value[key]
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
            raw = json.dumps(self.value(), sort_keys=True, separators=(",", ":"))[:-1] + ',"schema_version":3}\n'
            with self.assertRaises(ValueError):
                contract.load(self.write(tmp, raw=raw.encode()))
            value = self.value()
            value["schema_version"] = True
            with self.assertRaises(ValueError):
                contract.validate(contract.load(self.write(tmp, value)))

    def test_published_v2_contract_remains_readable(self):
        with tempfile.TemporaryDirectory() as tmp:
            value = self.value()
            value["schema_version"] = 2
            for key in contract.V3_KEYS - contract.V2_KEYS:
                del value[key]
            contract.validate(contract.load(self.write(tmp, value)))

    def test_phase2_commit_is_strict(self):
        with tempfile.TemporaryDirectory() as tmp:
            value = self.value()
            value["phase2_implementation_commit"] = "not-a-commit"
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
