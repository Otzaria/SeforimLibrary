"""Sandbox tests for the split full-DB helpers in db_asset_names.sh.

A DB above GitHub's per-asset limit is staged as <name>.part-NNN plus
<name>.manifest.json (stage_full_db) and every consumer reads it back through
download_full_db_by_name / stream_full_db / split_full_db_meta. A stub `gh`
serves $ASSET_ROOT/<tag>/ as the release; the thresholds are shrunk to bytes.
"""

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).parent
NAMES = SCRIPTS / "db_asset_names.sh"
NAME = "seforim-schema6.db.zst"

STUB_GH = r"""
#!/bin/sh
[ "$1" = release ] && [ "$2" = download ] || { echo "stub gh: $*" >&2; exit 90; }
shift 2
tag=$1; shift
pattern=""; dir="."; out=""
while [ $# -gt 0 ]; do
  case "$1" in
    --pattern|-p) pattern=$2; shift 2 ;;
    --dir|-D) dir=$2; shift 2 ;;
    -O) out=$2; shift 2 ;;
    -R|--repo) shift 2 ;;
    *) shift ;;
  esac
done
src="$ASSET_ROOT/$tag/$pattern"
[ -f "$src" ] || { echo "no assets match the file pattern $pattern" >&2; exit 1; }
[ -z "${DOWNLOAD_LOG:-}" ] || echo "$pattern" >> "$DOWNLOAD_LOG"
if [ "$out" = - ]; then cat "$src"; exit 0; fi
mkdir -p "$dir"
cp "$src" "$dir/$pattern"
"""


def bash():
    candidates = [shutil.which("bash")]
    if os.name == "nt":
        candidates.append(r"C:\Program Files\Git\usr\bin\bash.exe")
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            probe = subprocess.run([candidate, "-c", "command -v jq && command -v sha256sum && command -v split"],
                                   capture_output=True, text=True)
            if probe.returncode == 0:
                return candidate
    return None


BASH = bash()


@unittest.skipIf(BASH is None, "needs bash with jq, sha256sum and split")
class SplitFullDbTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        (self.tmp / "bin").mkdir()
        gh = self.tmp / "bin" / "gh"
        gh.write_text(textwrap.dedent(STUB_GH).lstrip(), encoding="utf-8", newline="\n")
        gh.chmod(0o755)
        self.release = self.tmp / "assets" / "v31"
        self.release.mkdir(parents=True)
        self.archive = self.tmp / NAME
        self.payload = bytes(range(256)) * 40 + b"tail"
        self.archive.write_bytes(self.payload)

    def run_sh(self, script, *args, threshold=4096, part=4000):
        env = dict(
            os.environ,
            PATH=f"{self.tmp / 'bin'}{os.pathsep}{os.environ['PATH']}",
            ASSET_ROOT=str(self.tmp / "assets"),
            DOWNLOAD_LOG=str(self.tmp / "downloads.log"),
            FULL_DB_SPLIT_THRESHOLD=str(threshold),
            FULL_DB_PART_SIZE=str(part),
        )
        return subprocess.run(
            [BASH, "-c", f'set -euo pipefail; . "$0"; {script}', NAMES.as_posix(), *args],
            env=env, capture_output=True, cwd=self.tmp,
        )

    def stage(self, **kw):
        done = self.run_sh('stage_full_db "$1" "$2"', NAME, str(self.release), **kw)
        self.assertEqual(done.returncode, 0, done.stderr.decode())
        return sorted(p.name for p in self.release.iterdir())

    def test_a_db_below_the_threshold_is_staged_as_is(self):
        self.assertEqual(self.stage(threshold=len(self.payload)), [NAME])

    def test_a_db_above_the_threshold_is_staged_as_parts_and_manifest(self):
        staged = self.stage()
        self.assertEqual(staged, [f"{NAME}.manifest.json", f"{NAME}.part-000",
                                  f"{NAME}.part-001", f"{NAME}.part-002"])
        manifest = json.loads((self.release / f"{NAME}.manifest.json").read_text())
        self.assertEqual(manifest["sha256"], hashlib.sha256(self.payload).hexdigest())
        self.assertEqual(sum(p["size"] for p in manifest["parts"]), len(self.payload))

    def test_the_selector_names_the_archive_of_a_split_db(self):
        release = {"assets": [{"name": f"{NAME}.manifest.json", "size": 300, "digest": "sha256:aa"},
                              {"name": f"{NAME}.part-000", "size": 4000},
                              {"name": "seforim.db.zst", "size": 9, "digest": "sha256:bb"},
                              {"name": "patch-v30-v31.db.zst.manifest.json", "size": 1}]}
        (self.tmp / "release.json").write_text(json.dumps(release))
        done = self.run_sh('jq -c "$FULL_DB_ASSET_JQ | {name, manifest, size, digest}" release.json')
        self.assertEqual(json.loads(done.stdout),
                         {"name": NAME, "manifest": f"{NAME}.manifest.json",
                          "size": None, "digest": None})
        # A single file of the same schema wins.
        release["assets"].append({"name": NAME, "size": 5, "digest": "sha256:cc"})
        (self.tmp / "release.json").write_text(json.dumps(release))
        done = self.run_sh('jq -c "$FULL_DB_ASSET_JQ | .name, .manifest" release.json')
        self.assertEqual(done.stdout.decode().split(), [f'"{NAME}"', "null"])

    def test_a_split_db_is_downloaded_assembled_and_verified(self):
        self.stage()
        out = self.tmp / "out"
        out.mkdir()
        done = self.run_sh('download_full_db_by_name v31 "$1" "$2"', NAME, str(out))
        self.assertEqual(done.returncode, 0, done.stderr.decode())
        self.assertEqual((out / NAME).read_bytes(), self.payload)
        self.assertEqual(sorted(p.name for p in out.iterdir()), [NAME])
        meta = self.run_sh('split_full_db_meta v31 "$1"', NAME)
        self.assertEqual(meta.stdout.decode().strip().split("\t"),
                         [str(len(self.payload)), "sha256:" + hashlib.sha256(self.payload).hexdigest()])
        streamed = self.run_sh('stream_full_db v31 "$1"', NAME)
        self.assertEqual(streamed.stdout, self.payload)

    def test_a_single_db_is_downloaded_without_touching_a_manifest(self):
        self.stage(threshold=len(self.payload))
        out = self.tmp / "out"
        out.mkdir()
        done = self.run_sh('download_full_db_by_name v31 "$1" "$2"', NAME, str(out))
        self.assertEqual(done.returncode, 0, done.stderr.decode())
        self.assertEqual((out / NAME).read_bytes(), self.payload)
        self.assertEqual((self.tmp / "downloads.log").read_text().split(), [NAME])
        self.assertEqual(self.run_sh('stream_full_db v31 "$1"', NAME).stdout, self.payload)

    def test_a_corrupt_or_missing_part_fails_and_leaves_nothing(self):
        self.stage()
        out = self.tmp / "out"
        out.mkdir()
        part = self.release / f"{NAME}.part-001"
        part.write_bytes(b"x" * part.stat().st_size)
        done = self.run_sh('download_full_db_by_name v31 "$1" "$2"', NAME, str(out))
        self.assertNotEqual(done.returncode, 0)
        self.assertIn(b"checksum mismatch", done.stderr)
        self.assertEqual(list(out.iterdir()), [])
        part.unlink()
        done = self.run_sh('download_full_db_by_name v31 "$1" "$2"', NAME, str(out))
        self.assertNotEqual(done.returncode, 0)
        self.assertEqual(list(out.iterdir()), [])

    def test_a_release_without_the_db_reports_the_single_asset_error(self):
        out = self.tmp / "out"
        out.mkdir()
        done = self.run_sh('download_full_db_by_name v31 "$1" "$2"', NAME, str(out))
        self.assertNotEqual(done.returncode, 0)
        self.assertIn(f"no assets match the file pattern {NAME}".encode(), done.stderr)
        self.assertEqual(list(out.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
