"""Contract tests for build-library-vectors.yml and its driver, build_library_vectors.sh.

The workflow is thin on purpose: it decides when (after the release's index is stored), where
(the build machine, which holds the warehouse and the GPU) and in which mode; the driver does
the work, the same way by hand. Load-bearing properties, pinned here:

  * it runs only behind a successful index build of a database release (or a manual run
    naming one), and publishing is opt-in;
  * a runner with no warehouse yet plans every text and creates the warehouse on its first add;
  * a plan of fewer than CPU_EMBED_MAX texts is embedded on the CPU by the sidecar's embed-shard
    (the reference), so the GPU worker never gets a plan its parity certificate cannot cover;
  * a gate failure, an existing vectors release or a damaged index stops it before anything
    is published;
  * it publishes through a draft at the commit of the library release's tag: data first, the
    manifest last, and the draft is published only once it holds exactly the files built;
  * what it publishes is a prerelease unless the repository says otherwise, which keeps it out
    of update-release-manifest.yml and the database history;
  * it persists the ledger only after a publish.

The YAML assertions read the parsed workflow; the driver's are made by running it against stub
tools (gh, the planner, the sidecar CLI, the worker's Python, zstd) and reading what it called.
The stubs refuse what the real tools refuse (a missing warehouse, an add with no shards), so a
driver that depends on state the runner lacks fails here too.
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
RELEASE_MANIFEST = WORKFLOWS / "update-release-manifest.yml"
DRIVER = HERE / "build_library_vectors.sh"
PINS = HERE / "pins.env"
DATABASE_TAG = re.compile(r"^v[0-9]+-[0-9]{14}$")
BOOTSTRAP = HERE / "bootstrap_runner.sh"
PIN = dict(line.split("=", 1) for line in PINS.read_text().splitlines() if line and not line.startswith("#") and "=" in line)
TAG = "v30-20260930165019"
TAG_COMMIT = "c66256253b25c0aa0b13d04e399647496f3b0bee"   # the commit TAG names (the stub gh's answer)


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

    def test_a_publish_is_a_prerelease_unless_the_repository_says_otherwise(self):
        self.assertEqual(self.job["env"]["LIBRARY_VECTORS_PRERELEASE"], "${{ vars.LIBRARY_VECTORS_PRERELEASE || 'true' }}")

    def test_a_prerelease_stays_out_of_the_release_manifest_and_the_history(self):
        # Why the default is a prerelease: every published release that is not one starts
        # update-release-manifest.yml, which lists it in the database history it commits.
        doc = yaml.safe_load(RELEASE_MANIFEST.read_text(encoding="utf-8"))
        self.assertEqual(doc[True]["release"]["types"], ["published"])
        (job,) = doc["jobs"].values()
        self.assertIn("github.event.release.prerelease == false", job["if"])
        self.assertIn("select((.draft|not) and (.prerelease|not))", run_text(job))


class Pins(unittest.TestCase):
    def test_the_sidecar_and_the_planner_are_pinned_to_full_commits(self):
        pins = dict(line.split("=", 1) for line in PINS.read_text().splitlines()
                    if line and not line.startswith("#") and "=" in line)
        self.assertRegex(pins["SIDECAR_REV"], r"^[0-9a-f]{40}$")
        self.assertRegex(pins["PLUGIN_REV"], r"^[0-9a-f]{40}$")
        self.assertRegex(pins["PASSAGE_PACKAGE_CHECKSUM"], r"^[0-9a-f]{64}$")
        self.assertRegex(pins["MODEL_REVISION"], r"^[0-9a-f]{7,40}$")   # the mirror at a commit, never a branch
        self.assertEqual(pins["PASSAGE_QUANTIZATION"], "fp32")

    def test_a_plan_the_gpu_worker_gets_can_always_be_certified(self):
        # the GPU worker's parity certificate needs 1,000 samples, 41 of them golden: a plan of fewer
        # than 959 texts can never have one, so everything below CPU_EMBED_MAX goes to the CPU
        self.assertGreaterEqual(int(PIN["CPU_EMBED_MAX"]), 1000)
        self.assertGreaterEqual(int(PIN["CPU_PROCESSES"]), 1)
        self.assertGreaterEqual(int(PIN["CPU_THREADS"]), 1)

    def test_the_sidecar_cli_is_built_able_to_embed(self):
        # embed-shard needs a real inference backend; a default build of the sidecar has none
        line = next(l for l in BOOTSTRAP.read_text().splitlines() if l.startswith("build_bin otzaria-semantic-search "))
        self.assertIn("--features onnx-backend", line)


STUB_GH = r"""#!/usr/bin/env bash
echo "gh $*" >> "$CALLS"
if [ "$1" = api ]; then
  case "$2" in */commits/v*) echo "${TAG_COMMIT:-c66256253b25c0aa0b13d04e399647496f3b0bee}"; exit 0 ;; esac
  echo "stub gh: api $2" >&2; exit 90
fi
cmd="$1 $2"; shift 2
case "$cmd" in
  "auth status") [ -z "${GH_UNAUTHENTICATED:-}" ] ;;
  "release view")
    tag=$1
    case "$tag" in vectors-*)
      # a draft's assets: what was uploaded, as name<TAB>size
      case " $* " in *" --json assets "*) cat "$UPLOADED" 2>/dev/null; exit 0 ;; esac
      [ -n "${EXISTING:-}" ] && exit 0; exit 1 ;;
    esac
    case " $* " in *" --json publishedAt "*) echo "2026-09-30T21:38:29Z" ;; esac
    exit 0 ;;
  "release download")
    tag=$1; shift; dir=.; pats=()
    while [ $# -gt 0 ]; do case "$1" in --dir) dir=$2; shift 2;; --pattern) pats+=("$2"); shift 2;; *) shift;; esac; done
    for p in "${pats[@]}"; do cp "$ASSETS/$tag/$p" "$dir/$p" || exit 1; done ;;
  "release create") ;;
  "release upload")
    # SHORT_UPLOAD names an asset that arrives one byte short
    name=$(basename "$2"); size=$(stat -c %s "$2")
    [ "$name" != "${SHORT_UPLOAD:-}" ] || size=$((size - 1))
    printf '%s\t%s\n' "$name" "$size" >> "$UPLOADED" ;;
  "release edit") ;;
  *) echo "stub gh: $cmd" >&2; exit 90 ;;
esac
"""

STUB_EXPORT = r"""#!/usr/bin/env bash
echo "export $*" >> "$CALLS"
out=""; warehouse=""
while [ $# -gt 0 ]; do case "$1" in --out) out=$2; shift 2;; --warehouse) warehouse=$2; shift 2;; *) shift;; esac; done
# as export_semantic_plan does: a --warehouse is opened, and a directory without warehouse.json is none
if [ -n "$warehouse" ] && [ ! -f "$warehouse/warehouse.json" ]; then
  echo "Could not open the warehouse: $warehouse/warehouse.json: No such file or directory" >&2; exit 1
fi
mkdir -p "$out"
: > "$out/embed.jsonl"
jq -n --argjson n "${TO_EMBED:-0}" '{format:"otzaria-embed-plan",version:2,records:$n,plan_sha256:("a"*64),passage_package:{checksum:"4a4a2ae88a86f15ffe6069bfcefc3abd13c207cec5d7aaef52c0c59d752ade46",quantization:"fp32"}}' > "$out/embed-manifest.json"
jq -n --arg tag "${PLAN_TAG:-v30-20260930165019}" '{format:"otzaria-vector-plan",version:1,library_release_tag:$tag,parity:{checked:9,mismatches:0},counts:{records:9}}' > "$out/plan-manifest.json"
"""

STUB_CLI = r"""#!/usr/bin/env bash
echo "cli $*" >> "$CALLS"
sub=$1; shift
out=""; files=(); verify=""; warehouse=""; create=""; model=""; shards=()
while [ $# -gt 0 ]; do case "$1" in
  --out) out=$2; shift 2;; --files) files+=("$2"); shift 2;; --verify) verify=1; shift;;
  --warehouse) warehouse=$2; shift 2;; --create) create=1; shift;; --model) model=$2; shift 2;;
  --shards) shards+=("$2"); shift 2;; --processes) processes=$2; shift 2;; *) shift;; esac; done
# as the sidecar does: --create makes a missing warehouse for --model; everything else needs one
need_warehouse() { [ -f "$warehouse/warehouse.json" ] || { echo "Could not open the warehouse $warehouse" >&2; exit 1; }; }
case "$sub" in
  embed-shard)
    # as the sidecar does: one shard in --out, or with --processes P one per process in shard-NNN
    echo "cli-env OTZARIA_ONNX_RUNTIME=${OTZARIA_ONNX_RUNTIME:-}" >> "$CALLS"
    p=${processes:-1}; [ "$p" -le "${TO_EMBED:-1}" ] || p=${TO_EMBED:-1}
    for i in $(seq 0 $((p - 1))); do
      d=$out; [ "${processes:-1}" -le 1 ] || d=$out/$(printf 'shard-%03d' "$i")
      mkdir -p "$d"; for f in vectors.f32 keys.bin shard-manifest.json; do : > "$d/$f"; done
    done ;;
  warehouse-add)
    if [ -n "$create" ] && [ ! -f "$warehouse/warehouse.json" ]; then
      [ -f "$model" ] || { echo "--create needs --model" >&2; exit 1; }
      mkdir -p "$warehouse"; echo '{"format":"otzaria-vector-warehouse","records":0}' > "$warehouse/warehouse.json"
    fi
    found=$(for s in "${shards[@]}"; do find "$s" -name shard-manifest.json; done | wc -l)
    [ "$found" -gt 0 ] || { echo "Nothing to add: no shard-manifest.json under --shards" >&2; exit 1; }
    need_warehouse ;;
  assemble)
    need_warehouse
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
script=$1; shift
out=""; cache=""; checksum=""; graph=""
while [ $# -gt 0 ]; do case "$1" in --out) out=$2; shift 2;; --cache) cache=$2; shift 2;;
  --checksum) checksum=$2; shift 2;; --graph) graph=$2; shift 2;; *) shift;; esac; done
case "$script" in
  */model_package.py)   # fetch: the verified package's directory on stdout
    mkdir -p "$cache/$checksum"; : > "$cache/$checksum/$graph"; echo "$cache/$checksum" ;;
  *)                    # the GPU worker: a shard in --out
    mkdir -p "$out"; for f in vectors.f32 keys.bin shard-manifest.json; do : > "$out/$f"; done ;;
esac
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
        # a runner that has built before: its warehouse (the driver's path for the fp32 package)
        self.warehouse = self.state / "warehouse" / "fp32-4a4a2ae8"
        self.warehouse.mkdir(parents=True)
        (self.warehouse / "warehouse.json").write_text("{}")
        self.ort = self.state / "venv/lib/python3.12/site-packages/onnxruntime/capi/libonnxruntime.so.1.28.0"
        self.ort.parent.mkdir(parents=True)
        self.ort.write_bytes(b"")

    def tearDown(self):
        self.tmp.cleanup()

    def run_driver(self, mode="dry-run", tag=TAG, **env):
        e = dict(os.environ, CALLS=str(self.calls), ASSETS=str(self.root / "assets"), UPLOADED=str(self.root / "uploaded.tsv"),
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
        self.assertFalse([c for c in calls if "release create" in c or "release upload" in c or "release edit" in c])
        self.assertFalse((self.state / "ledger").exists())

    def test_a_large_update_is_embedded_on_the_gpu_in_windows_and_then_added_to_the_warehouse(self):
        n = int(PIN["CPU_EMBED_MAX"])                    # the smallest plan the GPU worker gets
        p, calls = self.run_driver(TO_EMBED=n)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        workers = [c for c in calls if "embed_worker.py" in c]
        self.assertEqual(len(workers), -(-n // int(PIN["EMBED_WINDOW"])))
        self.assertIn("--skip 0 --take", workers[0])
        self.assertFalse([c for c in calls if "cli embed-shard" in c or "model_package.py" in c])
        self.assertIn('"worker":"seforim-gpu-worker 1.0"', calls[self.index_of(calls, "cli assemble --kind base")])
        add = self.index_of(calls, "cli warehouse-add")
        self.assertGreater(add, self.index_of(calls, "embed_worker.py"))
        self.assertIn("--plan", calls[add])
        self.assertLess(add, self.index_of(calls, "cli assemble --kind base"))

    def test_a_small_update_is_embedded_on_the_cpu_by_the_sidecar_with_no_gpu_worker(self):
        for n in (1, 100, 958, 959, int(PIN["CPU_EMBED_MAX"]) - 1):
            with self.subTest(texts=n):
                self.calls.unlink(missing_ok=True)
                p, calls = self.run_driver(TO_EMBED=n)
                self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
                self.assertFalse([c for c in calls if "embed_worker.py" in c])
                package = self.state / "model-cache" / PIN["PASSAGE_PACKAGE_CHECKSUM"]
                fetch = calls[self.index_of(calls, "model_package.py fetch")]
                for arg in ("--cache " + str(self.state / "model-cache"), "--checksum " + PIN["PASSAGE_PACKAGE_CHECKSUM"],
                            "--graph " + PIN["MODEL_GRAPH"], "--repo " + PIN["MODEL_REPO"], "--revision " + PIN["MODEL_REVISION"]):
                    self.assertIn(arg + " ", fetch + " ")
                embed = self.index_of(calls, "cli embed-shard")
                self.assertGreater(embed, self.index_of(calls, "model_package.py fetch"))
                for arg in ("--plan ", "--model-file " + str(package / PIN["MODEL_GRAPH"]),
                            "--processes " + PIN["CPU_PROCESSES"], "--threads " + PIN["CPU_THREADS"], "--out "):
                    self.assertIn(arg, calls[embed])
                self.assertEqual(calls[embed + 1], "cli-env OTZARIA_ONNX_RUNTIME=" + str(self.ort))
                self.assertGreater(self.index_of(calls, "cli warehouse-add"), embed)
                self.assertIn('"worker":"otzaria-semantic-search embed-shard', calls[self.index_of(calls, "cli assemble --kind base")])

    def test_the_cpu_path_needs_the_venvs_onnx_runtime(self):
        self.ort.unlink()
        p, calls = self.run_driver(TO_EMBED=5)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("ONNX Runtime", p.stdout)
        self.assertFalse([c for c in calls if "cli embed-shard" in c or "warehouse-add" in c])

    def test_the_stubs_refuse_what_the_real_tools_refuse(self):
        missing, env = self.root / "no-warehouse", dict(os.environ, CALLS=str(self.calls))
        plan = subprocess.run([str(self.root / "bin" / "export"), "--warehouse", str(missing), "--out", str(self.root / "p")],
                              env=env, capture_output=True)
        self.assertNotEqual(plan.returncode, 0)
        shard = self.root / "shard"
        shard.mkdir()
        (shard / "shard-manifest.json").write_text("{}")
        add = subprocess.run([str(self.root / "bin" / "cli"), "warehouse-add", "--warehouse", str(missing), "--shards", str(shard)],
                             env=env, capture_output=True)
        self.assertNotEqual(add.returncode, 0)
        self.assertFalse(missing.exists())

    def test_a_runner_with_no_warehouse_yet_plans_every_text_and_creates_it_on_the_first_add(self):
        shutil.rmtree(self.warehouse)
        p, calls = self.run_driver(TO_EMBED=2_000_000)      # the whole library
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertNotIn("--warehouse", calls[self.index_of(calls, "export --index")])   # every text is planned
        add = self.index_of(calls, "cli warehouse-add")
        for arg in ("--create", "--model " + str(self.state / "bin" / "family-model.json"), "--passage-quantization fp32",
                    "--plan "):
            self.assertIn(arg, calls[add])
        self.assertLess(add, self.index_of(calls, "cli assemble --kind base"))
        self.assertTrue((self.warehouse / "warehouse.json").exists())

    def test_the_gpu_worker_fetches_the_model_at_the_pinned_revision(self):
        pins = dict(line.split("=", 1) for line in PINS.read_text().splitlines()
                    if line and not line.startswith("#") and "=" in line)
        p, calls = self.run_driver(TO_EMBED=2_000_000)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        workers = [c for c in calls if c.startswith("py ") and "embed_worker.py" in c]
        self.assertTrue(workers)
        for c in workers:
            self.assertIn(f"--repo {pins['MODEL_REPO']} --revision {pins['MODEL_REVISION']} ", c + " ")

    def test_a_runner_with_a_warehouse_plans_against_it_and_adds_to_it(self):
        p, calls = self.run_driver(TO_EMBED=2_000_000)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("--warehouse " + str(self.warehouse), calls[self.index_of(calls, "export --index")])
        self.assertNotIn("--create", calls[self.index_of(calls, "cli warehouse-add")])

    def test_a_warehouse_directory_without_its_manifest_is_refused_before_the_build(self):
        (self.warehouse / "warehouse.json").unlink()
        (self.warehouse / "vectors.f32").write_bytes(b"\0" * 1024)
        p, calls = self.run_driver(mode="base")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("no warehouse.json", p.stdout)
        self.assertFalse([c for c in calls if "release download" in c or c.startswith(("export ", "cli ", "py "))])
        self.assertEqual((self.warehouse / "vectors.f32").stat().st_size, 1024)   # never recreated over

    def test_an_empty_plan_on_a_runner_with_no_warehouse_stops_before_assembling(self):
        shutil.rmtree(self.warehouse)
        p, calls = self.run_driver(TO_EMBED=0)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("no warehouse", p.stdout)
        self.assertFalse([c for c in calls if "cli assemble" in c])

    def test_a_publish_is_a_draft_at_the_library_commit_with_the_data_first_and_the_manifest_last(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        # the commit is resolved before the build, from the library release's tag
        resolve = self.index_of(calls, "gh api repos/o/r/commits/" + TAG)
        self.assertLess(resolve, self.index_of(calls, "gh release download"))
        create = self.index_of(calls, "gh release create vectors-" + TAG)
        for arg in ("--draft", "--target " + TAG_COMMIT, "--latest=false"):
            self.assertIn(arg, calls[create].split(" --title")[0])
        uploads = [c for c in calls if "gh release upload" in c]
        self.assertTrue(all(self.index_of(calls, u) > create for u in uploads))
        names = [Path(u.split()[4]).name for u in uploads]
        self.assertEqual(len(names), 3)
        self.assertTrue(names[0].endswith(".oxv.zst"))
        self.assertEqual(names[1], "gates.json")
        self.assertTrue(names[2].endswith(".manifest.json"))   # the manifest last

    def test_the_draft_is_published_only_once_its_files_are_checked_and_then_the_ledger_is_persisted(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        last_upload = max(i for i, c in enumerate(calls) if "gh release upload" in c)
        check = self.index_of(calls, "gh release view vectors-" + TAG + " --repo o/r --json assets")
        edits = [i for i, c in enumerate(calls) if "gh release edit" in c]
        self.assertEqual(len(edits), 1)
        self.assertTrue(last_upload < check < edits[0])
        self.assertEqual(calls[edits[0]], "gh release edit vectors-" + TAG + " --repo o/r --draft=false --latest=false")
        self.assertEqual(edits[0], len(calls) - 1)   # nothing reaches the release after it is published
        ledger = self.state / "ledger"
        self.assertEqual(sorted(f.name for f in ledger.iterdir()),
                         ["ledger-v30.keys", "ledger-v30.manifest.json", "pairs-v30.bin", "published.json"])
        self.assertEqual(json.loads((ledger / "published.json").read_text())["published"], "vectors-" + TAG)
        self.assertFalse((self.state / "work" / TAG).exists())

    def test_a_publish_is_a_prerelease_unless_the_variable_says_false(self):
        for value, flag in ((None, "--prerelease=true"), ("", "--prerelease=true"), ("true", "--prerelease=true"),
                            ("false", "--prerelease=false")):
            self.calls.unlink(missing_ok=True)
            (self.root / "uploaded.tsv").unlink(missing_ok=True)
            shutil.rmtree(self.state / "ledger", ignore_errors=True)
            env = {} if value is None else {"LIBRARY_VECTORS_PRERELEASE": value}
            p, calls = self.run_driver(mode="base", TO_EMBED=0, **env)
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
            self.assertIn(" " + flag + " ", calls[self.index_of(calls, "gh release create")], value)

    def test_a_mistyped_prerelease_value_stops_before_anything_runs(self):
        p, calls = self.run_driver(mode="base", LIBRARY_VECTORS_PRERELEASE="yes")
        self.assertEqual(p.returncode, 64)
        self.assertIn("LIBRARY_VECTORS_PRERELEASE is true or false", p.stdout)
        self.assertEqual(calls, [])

    def test_a_draft_that_does_not_hold_the_files_built_is_never_published(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0, SHORT_UPLOAD="gates.json")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("it stays a draft", p.stdout)
        self.assertFalse([c for c in calls if "release edit" in c])
        self.assertFalse((self.state / "ledger").exists())

    def test_a_library_tag_that_names_no_commit_stops_before_the_build(self):
        p, calls = self.run_driver(mode="base", TAG_COMMIT="not-a-commit")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("not a commit", p.stdout)
        self.assertFalse([c for c in calls if "release download" in c or "release create" in c])

    def test_a_publish_needs_an_authenticated_gh(self):
        p, calls = self.run_driver(mode="base", GH_UNAUTHENTICATED=1)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("gh is not authenticated", p.stdout)
        self.assertFalse([c for c in calls if "release download" in c or "release create" in c])

    def test_a_failed_gate_stops_before_anything_is_published(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0, VERIFY_EXIT=2)
        self.assertEqual(p.returncode, 2)
        self.assertIn("a gate failed", p.stdout)
        self.assertFalse([c for c in calls if "release create" in c or "release upload" in c or "release edit" in c
                          or "release-files" in c])
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
