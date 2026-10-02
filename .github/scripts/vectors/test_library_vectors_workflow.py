"""Contract tests for build-library-vectors.yml and its driver, build_library_vectors.sh.

The workflow is thin on purpose: it decides when (after the release's index is stored), where
(the build machine, which holds the warehouse and the GPU) and in which mode; the driver does
the work, the same way by hand. Load-bearing properties, pinned here:

  * it runs only behind a successful index build of a database release (or a manual run
    naming one), and publishing is opt-in;
  * a gate failure, an existing vectors release or a damaged index stops it before anything
    is published;
  * it publishes data first and the manifest last, and persists the ledger only after a publish.

The YAML assertions read the parsed workflow; the driver's are made by running it against stub
tools (gh, the planner, the sidecar CLI, the worker's Python, zstd) and reading what it called.
"""

import hashlib
import io
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import unittest
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
WORKFLOWS = SCRIPTS.parent / "workflows"
VECTORS = WORKFLOWS / "build-library-vectors.yml"
INDEX = WORKFLOWS / "build-library-index.yml"
DRIVER = HERE / "build_library_vectors.sh"
PINS = HERE / "pins.env"
DATABASE_TAG = re.compile(r"^v[0-9]+-[0-9]{14}$")
TAG = "v30-20260930165019"


def run_text(job):
    return "\n".join(step["run"] for step in job.get("steps") or [] if "run" in step)


def linux_tools():
    if not all(shutil.which(t) for t in ("bash", "jq", "sha256sum", "split", "flock", "tar")):
        return False
    return subprocess.run(["stat", "-c", "%s", "/"], capture_output=True).returncode == 0


@unittest.skipIf(yaml is None, "PyYAML unavailable")
class Workflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.doc = yaml.safe_load(VECTORS.read_text(encoding="utf-8"))
        cls.index = yaml.safe_load(INDEX.read_text(encoding="utf-8"))
        cls.gate = cls.doc["jobs"]["gate"]
        cls.job = cls.doc["jobs"]["vectors"]

    def test_it_runs_after_the_index_workflow_or_by_hand_and_on_nothing_else(self):
        on = self.doc[True]
        self.assertEqual(set(on), {"workflow_run", "workflow_dispatch"})
        self.assertEqual(on["workflow_run"]["workflows"], [self.index["name"]])
        self.assertEqual(on["workflow_run"]["types"], ["completed"])
        inputs = on["workflow_dispatch"]["inputs"]
        self.assertEqual(set(inputs), {"release_tag", "mode"})
        self.assertTrue(inputs["release_tag"]["required"])
        self.assertEqual(inputs["mode"]["options"], ["dry-run", "base"])
        self.assertEqual(inputs["mode"]["default"], "dry-run")

    def test_the_gate_admits_a_successful_index_build_of_a_database_release_only(self):
        gate = run_text(self.gate)
        self.assertIn('[ "$RUN_CONCLUSION" != success ]', gate)
        patterns = re.findall(r"grep -Eq '(\^v[^']*\$)'", gate)
        self.assertEqual(set(patterns), {DATABASE_TAG.pattern})
        # the index workflow names its runs `library-index <tag>`; the gate reads the tag from that
        self.assertTrue(self.index["run-name"].startswith("library-index "))
        self.assertIn("${RUN_TITLE#library-index }", gate)
        self.assertEqual(self.gate["steps"][0]["env"]["RUN_CONCLUSION"], "${{ github.event.workflow_run.conclusion }}")

    def test_a_mistyped_manual_tag_fails_instead_of_skipping(self):
        self.assertIn("::error::release_tag '$TAG' is not a database release tag", run_text(self.gate))

    def test_publishing_is_opt_in(self):
        gate = run_text(self.gate)
        self.assertIn("MODE=dry-run", gate)
        self.assertIn('[ "$PUBLISH" != true ] || MODE=base', gate)
        self.assertEqual(self.gate["steps"][0]["env"]["PUBLISH"], "${{ vars.LIBRARY_VECTORS_PUBLISH }}")

    def test_the_heavy_job_runs_only_behind_the_gate_on_the_build_machine(self):
        self.assertEqual(self.job["needs"], "gate")
        self.assertEqual(self.job["if"], "needs.gate.outputs.proceed == 'true'")
        self.assertIn("otzaria-db", self.job["runs-on"])          # the index workflow's local runner
        self.assertIn('LABELS=\'["otzaria-db"]\'', run_text(self.index["jobs"]["gate"]))
        self.assertIn("amd-gpu", self.job["runs-on"])
        self.assertIn("self-hosted", self.job["runs-on"])

    def test_one_build_at_a_time_and_never_cancelled(self):
        c = self.doc["concurrency"]
        self.assertEqual(c["cancel-in-progress"], False)
        self.assertEqual(c["queue"], "max")
        self.assertNotEqual(c["group"], self.index["concurrency"]["group"])

    def test_every_job_is_bounded(self):
        for name, job in self.doc["jobs"].items():
            self.assertIsInstance(job.get("timeout-minutes"), int, name)

    def test_the_workflow_is_thin_and_the_driver_does_the_work(self):
        runs = [s for s in self.job["steps"] if "run" in s]
        self.assertEqual(len(runs), 1)
        self.assertIn("bash .github/scripts/vectors/build_library_vectors.sh", runs[0]["run"])
        self.assertTrue(DRIVER.exists() and os.access(DRIVER, os.X_OK))

    def test_untrusted_values_never_reach_the_shell_as_expressions(self):
        for name, job in self.doc["jobs"].items():
            for step in job.get("steps") or []:
                self.assertNotIn("${{", step.get("run", ""), f"{name}: an expression inside run")

    def test_the_tokens_come_from_secrets_through_the_environment(self):
        env = self.job["env"]
        self.assertEqual(env["GH_TOKEN"], "${{ secrets.PIPELINE_TOKEN }}")
        self.assertEqual(env["OTZARIA_HF_TOKEN"], "${{ secrets.OTZARIA_HF_TOKEN }}")


class Pins(unittest.TestCase):
    def test_the_sidecar_and_the_planner_are_pinned_to_full_commits(self):
        pins = dict(line.split("=", 1) for line in PINS.read_text().splitlines()
                    if line and not line.startswith("#") and "=" in line)
        self.assertRegex(pins["SIDECAR_REV"], r"^[0-9a-f]{40}$")
        self.assertRegex(pins["PLUGIN_REV"], r"^[0-9a-f]{40}$")
        self.assertRegex(pins["PASSAGE_PACKAGE_CHECKSUM"], r"^[0-9a-f]{64}$")
        self.assertEqual(pins["PASSAGE_QUANTIZATION"], "fp32")


STUB_GH = r"""#!/usr/bin/env bash
echo "gh $*" >> "$CALLS"
cmd="$1 $2"; shift 2
case "$cmd" in
  "auth status") exit 0 ;;
  "release view")
    tag=$1
    case "$tag" in vectors-*) [ -n "${EXISTING:-}" ] && exit 0; exit 1 ;; esac
    case " $* " in *" --json publishedAt "*) echo "2026-09-30T21:38:29Z" ;; esac
    exit 0 ;;
  "release download")
    tag=$1; shift; dir=.; pats=()
    while [ $# -gt 0 ]; do case "$1" in --dir) dir=$2; shift 2;; --pattern) pats+=("$2"); shift 2;; *) shift;; esac; done
    for p in "${pats[@]}"; do cp "$ASSETS/$tag/$p" "$dir/$p" || exit 1; done ;;
  "release create") ;;
  "release upload") ;;
  *) echo "stub gh: $cmd" >&2; exit 90 ;;
esac
"""

STUB_EXPORT = r"""#!/usr/bin/env bash
echo "export $*" >> "$CALLS"
out=""; while [ $# -gt 0 ]; do case "$1" in --out) out=$2; shift 2;; *) shift;; esac; done
mkdir -p "$out"
: > "$out/embed.jsonl"
jq -n --argjson n "${TO_EMBED:-0}" '{format:"otzaria-embed-plan",version:2,records:$n,plan_sha256:("a"*64),passage_package:{checksum:"4a4a2ae88a86f15ffe6069bfcefc3abd13c207cec5d7aaef52c0c59d752ade46",quantization:"fp32"}}' > "$out/embed-manifest.json"
jq -n --arg tag "${PLAN_TAG:-v30-20260930165019}" '{format:"otzaria-vector-plan",version:1,library_release_tag:$tag,parity:{checked:9,mismatches:0},counts:{records:9}}' > "$out/plan-manifest.json"
"""

STUB_CLI = r"""#!/usr/bin/env bash
echo "cli $*" >> "$CALLS"
sub=$1; shift
out=""; files=(); verify=""
while [ $# -gt 0 ]; do case "$1" in --out) out=$2; shift 2;; --files) files+=("$2"); shift 2;; --verify) verify=1; shift;; *) shift;; esac; done
case "$sub" in
  warehouse-add) ;;
  assemble)
    if [ -n "$verify" ]; then echo '{"gates":"stub"}' > "$out/gates.json"; exit "${VERIFY_EXIT:-0}"; fi
    mkdir -p "$out"; head -c 3000 /dev/urandom > "$out/segment.oxv"
    echo '{"identityDigest":"0123456789abcdef","toLibraryVersion":30}' > "$out/release.json"
    for f in ledger-v30.keys pairs-v30.bin ledger-v30.manifest.json; do echo "$f" > "$out/$f"; done ;;
  release-files) echo '{"files":[]}' > "$out"; echo "=== stem ==="; echo "Manifest SHA-256 $(sha256sum "$out" | cut -d' ' -f1)" ;;
  *) echo "stub cli: $sub" >&2; exit 90 ;;
esac
"""

STUB_PY = r"""#!/usr/bin/env bash
echo "py $*" >> "$CALLS"
out=""; while [ $# -gt 0 ]; do case "$1" in --out) out=$2; shift 2;; *) shift;; esac; done
mkdir -p "$out"; for f in vectors.f32 keys.bin shard-manifest.json; do : > "$out/$f"; done
"""

STUB_ZSTD = r"""#!/usr/bin/env bash
echo "zstd $*" >> "$CALLS"
if [ "$1" = "-dc" ]; then cat; exit 0; fi
in=""; out=""; while [ $# -gt 0 ]; do case "$1" in -o) out=$2; shift 2;; -*) shift;; *) in=$1; shift;; esac; done
cp "$in" "$out"
"""


@unittest.skipUnless(linux_tools(), "needs bash, jq, sha256sum, split, flock, tar and GNU stat")
class Driver(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        r = Path(self.tmp.name)
        self.root, self.calls = r, r / "calls.log"
        (r / "bin").mkdir()
        for name, body in (("gh", STUB_GH), ("export", STUB_EXPORT), ("cli", STUB_CLI), ("py", STUB_PY), ("zstd", STUB_ZSTD)):
            p = r / "bin" / name
            p.write_text(body)
            p.chmod(p.stat().st_mode | stat.S_IEXEC)
        # a release whose index archive is two parts of a tar (the zstd stub passes it through)
        assets = r / "assets" / TAG
        assets.mkdir(parents=True)
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            data = b'{"segments": []}'
            info = tarfile.TarInfo("index/meta.json"); info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        archive = buf.getvalue()
        half = len(archive) // 2
        parts = {"otzaria-library-index.tar.zst.part-000": archive[:half], "otzaria-library-index.tar.zst.part-001": archive[half:]}
        for name, b in parts.items():
            (assets / name).write_bytes(b)
        (assets / "otzaria-library-index.tar.zst.manifest.json").write_text(json.dumps({
            "sha256": hashlib.sha256(archive).hexdigest(),
            "parts": [{"name": n, "size": len(b), "sha256": hashlib.sha256(b).hexdigest()} for n, b in parts.items()]}))
        (assets / "otzaria-library-index.provenance.json").write_text(json.dumps({"searchEngineVersion": "0.8.7"}))
        self.state = r / "state"
        (self.state / "bin").mkdir(parents=True)
        for f in ("family-model.json", "chunking.json"):
            (self.state / "bin" / f).write_text("{}")

    def tearDown(self):
        self.tmp.cleanup()

    def run_driver(self, mode="dry-run", tag=TAG, **env):
        e = dict(os.environ, CALLS=str(self.calls), ASSETS=str(self.root / "assets"),
                 OTZARIA_SEMANTIC_CLI=str(self.root / "bin" / "cli"), EXPORT_SEMANTIC_PLAN=str(self.root / "bin" / "export"),
                 VECTOR_PYTHON=str(self.root / "bin" / "py"), GH=str(self.root / "bin" / "gh"), ZSTD=str(self.root / "bin" / "zstd"),
                 VECTORS_MIN_FREE_GB="0", OTZARIA_HF_TOKEN="hf_s3cr3t", GITHUB_ACTIONS="")
        e.update({k: str(v) for k, v in env.items()})
        p = subprocess.run(["bash", str(DRIVER), "--tag", tag, "--mode", mode, "--state", str(self.state), "--repo", "o/r"],
                           capture_output=True, text=True, env=e)
        calls = self.calls.read_text().splitlines() if self.calls.exists() else []
        self.assertNotIn("hf_s3cr3t", p.stdout + p.stderr)
        return p, calls

    def index_of(self, calls, needle):
        return next(i for i, c in enumerate(calls) if needle in c)

    def test_a_dry_run_builds_and_verifies_and_publishes_and_persists_nothing(self):
        p, calls = self.run_driver(TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        order = [self.index_of(calls, n) for n in ("--pattern otzaria-library-index.tar.zst.manifest.json", "export --index",
                                                   "cli assemble --kind base", "cli assemble --verify", "cli release-files")]
        self.assertEqual(order, sorted(order))
        self.assertFalse([c for c in calls if c.startswith("py ") or "warehouse-add" in c])   # nothing to embed
        self.assertFalse([c for c in calls if "release create" in c or "release upload" in c])
        self.assertFalse((self.state / "ledger").exists())

    def test_only_missing_texts_are_embedded_in_windows_and_then_added_to_the_warehouse(self):
        p, calls = self.run_driver(TO_EMBED=3, **{})
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        workers = [c for c in calls if c.startswith("py ")]
        self.assertEqual(len(workers), 1)
        self.assertIn("--skip 0 --take", workers[0])
        add = self.index_of(calls, "cli warehouse-add")
        self.assertGreater(add, self.index_of(calls, "py "))
        self.assertIn("--plan", calls[add])
        self.assertLess(add, self.index_of(calls, "cli assemble --kind base"))

    def test_a_publish_uploads_data_then_the_manifest_and_only_then_persists_the_ledger(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        create = self.index_of(calls, "gh release create vectors-" + TAG)
        self.assertIn("--latest=false", calls[create])
        uploads = [c for c in calls if "gh release upload" in c]
        self.assertTrue(all(self.index_of(calls, u) > create for u in uploads))
        self.assertIn(".oxv.zst", uploads[0])
        self.assertIn(".manifest.json", uploads[-1])
        ledger = self.state / "ledger"
        self.assertEqual(sorted(f.name for f in ledger.iterdir()),
                         ["ledger-v30.keys", "ledger-v30.manifest.json", "pairs-v30.bin", "published.json"])
        self.assertEqual(json.loads((ledger / "published.json").read_text())["published"], "vectors-" + TAG)
        self.assertFalse((self.state / "work" / TAG).exists())

    def test_a_failed_gate_stops_before_anything_is_published(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0, VERIFY_EXIT=2)
        self.assertEqual(p.returncode, 2)
        self.assertIn("a gate failed", p.stdout)
        self.assertFalse([c for c in calls if "release create" in c or "release upload" in c or "release-files" in c])
        self.assertFalse((self.state / "ledger").exists())

    def test_an_existing_vectors_release_is_never_rebuilt_in_place(self):
        p, calls = self.run_driver(mode="base", EXISTING=1)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("exists already", p.stdout)
        self.assertFalse([c for c in calls if "release download" in c])

    def test_a_damaged_index_part_is_refused(self):
        part = self.root / "assets" / TAG / "otzaria-library-index.tar.zst.part-001"
        part.write_bytes(part.read_bytes() + b"x")
        p, calls = self.run_driver()
        self.assertNotEqual(p.returncode, 0)
        self.assertFalse([c for c in calls if c.startswith("export ")])

    def test_a_plan_for_another_release_is_refused(self):
        p, calls = self.run_driver(PLAN_TAG="v29-20260927072953")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("another release", p.stdout)
        self.assertFalse([c for c in calls if "assemble" in c])

    def test_only_database_tags_are_accepted(self):
        for tag in ("vectors-v30-20260930165019", "v30", "lines-snapshot-sha256-" + "a" * 40):
            p, calls = self.run_driver(tag=tag)
            self.assertEqual(p.returncode, 64, tag)
            self.assertEqual(calls, [])

    def test_the_planner_is_given_the_model_the_warehouse_and_the_release(self):
        p, calls = self.run_driver(TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        plan = calls[self.index_of(calls, "export --index")]
        for arg in ("--library-version 30", "--release-tag " + TAG, "--model ", "--passage-quantization fp32",
                    "--warehouse ", "--created-at 2026-09-30T21:38:29Z", "--out "):
            self.assertIn(arg, plan)
        self.assertNotIn("--chunking", plan)   # the recipe comes from the model

if __name__ == "__main__":
    unittest.main(verbosity=2)
