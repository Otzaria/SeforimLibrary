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
  * nothing is published unless the plugin's validate_semantic_vectors passes the release on its
    index (G3 coverage, G4 resolution, G6 retrieval), and its report is published with the gates;
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
        self.assertRegex(pins["QUERY_PACKAGE_CHECKSUM"], r"^[0-9a-f]{64}$")      # G6 measures the int8 query model
        self.assertTrue(pins["QUERY_GRAPH"].endswith("-int8.onnx"))

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

# As validate_semantic_vectors does: arguments that are wrong, or inputs that do not read, exit 2;
# a gate that failed or did not run (its inputs not given, and not skipped) exits 1, with the
# report written; every gate passed or skipped exits 0. VALIDATE_FAILS names gates that fail and
# VALIDATE_UNREADABLE makes the index unreadable, for the driver's tests.
STUB_VALIDATE = r"""#!/usr/bin/env bash
echo "validate $*" >> "$CALLS"
echo "validate-env TMPDIR=${TMPDIR:-}" >> "$CALLS"
wrong() { echo "$1" >&2; exit 2; }
given=" "; releases=0; skips=""
index=""; vectors=""; plan=""; warehouse=""; model=""; identity=""; ort=""; report=""; threads=""
while [ $# -gt 0 ]; do
  flag=$1
  case "$flag" in
    --release) [ $# -ge 2 ] || wrong "--release needs a value"
      { [ -f "$2/segment.oxv" ] && [ -f "$2/release.json" ]; } || wrong "the releases do not install as a device installs them: $2"
      releases=$((releases + 1)); shift 2; continue ;;
    --skip) case "${2:-}" in G3|G4|G6) skips="$skips $2"; shift 2; continue ;; *) wrong "--skip needs a gate" ;; esac ;;
    --index|--vectors|--plan|--max-stale-hints|--warehouse|--model|--model-identity|--onnx-runtime|--queries|\
    --sample-queries|--min-recall-10|--min-recall-50|--threads|--report) ;;
    *) wrong "unknown argument $flag" ;;
  esac
  [ $# -ge 2 ] || wrong "$flag needs a value"
  case "$given" in *" $flag "*) wrong "$flag is given twice" ;; esac
  given="$given$flag "
  case "$flag" in
    --index) index=$2 ;; --vectors) vectors=$2 ;; --plan) plan=$2 ;; --warehouse) warehouse=$2 ;; --model) model=$2 ;;
    --model-identity) identity=$2 ;; --onnx-runtime) ort=$2 ;; --report) report=$2 ;; --threads) threads=$2 ;;
  esac
  shift 2
done
n=0; for g in G3 G4 G6; do case "$skips" in *$g*) n=$((n + 1)) ;; esac; done
[ "$n" -lt 3 ] || wrong "--skip names every gate, which leaves nothing to validate"
[ -n "$index" ] || wrong "--index is required"
if [ -n "$vectors" ] && [ "$releases" -gt 0 ]; then wrong "give --vectors, or one --release or more, and not both"; fi
if [ -z "$vectors" ] && [ "$releases" -eq 0 ]; then wrong "give --vectors, or one --release or more, and not both"; fi
[ "$threads" != 0 ] || wrong "--threads is 0, and a scan needs one"
g6=""; [ -z "$warehouse" ] || g6=${g6}w; [ -z "$model" ] || g6=${g6}m; [ -z "$identity" ] || g6=${g6}i
case "$g6" in ""|wmi) ;; *) wrong "G6 needs --warehouse, --model and --model-identity together" ;; esac
{ [ -d "$index" ] && [ -z "${VALIDATE_UNREADABLE:-}" ]; } || wrong "could not validate against the index $index"
[ -z "$plan" ] || [ -f "$plan/plan-manifest.json" ] || wrong "could not read the plan $plan"
if [ "$g6" = wmi ]; then
  [ -f "$warehouse/warehouse.json" ] || wrong "could not open the warehouse $warehouse"
  [ -f "$model" ] || wrong "loading the query model $model"
  [ -f "$identity" ] || wrong "could not read the model identity $identity"
  [ -z "$ort" ] || [ -f "$ort" ] || wrong "could not load ONNX Runtime from $ort"
fi
verdict() {
  case "$skips" in *$1*) echo skipped; return ;; esac
  case "${VALIDATE_FAILS:-}" in *$1*) echo failed; return ;; esac
  if [ "$1" = G6 ] && [ "$g6" != wmi ]; then echo notRun; return; fi
  echo passed
}
g3=$(verdict G3); g4=$(verdict G4); g6v=$(verdict G6); passed=true
for v in $g3 $g4 $g6v; do case "$v" in passed|skipped) ;; *) passed=false ;; esac; done
[ -z "$report" ] || jq -n --argjson passed "$passed" --arg g3 "$g3" --arg g4 "$g4" --arg g6 "$g6v" \
  '{tool: "validate_semantic_vectors", reportVersion: 1, passed: $passed,
    gates: [{gate: "G3", status: $g3}, {gate: "G4", status: $g4}, {gate: "G6", status: $g6}]}' > "$report"
[ "$passed" = true ]
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
        for name, body in (("gh", STUB_GH), ("export", STUB_EXPORT), ("cli", STUB_CLI), ("py", STUB_PY), ("zstd", STUB_ZSTD),
                           ("validate", STUB_VALIDATE)):
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
                 VALIDATE_SEMANTIC_VECTORS=str(self.root / "bin" / "validate"),
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
                                                   "cli assemble --kind base", "cli assemble --verify", "validate --index",
                                                   "cli release-files")]
        self.assertEqual(order, sorted(order))                 # a dry run validates too
        self.assertFalse([c for c in calls if "embed_worker.py" in c or "embed-shard" in c or "warehouse-add" in c])
        self.assertFalse([c for c in calls if "release create" in c or "release upload" in c or "release edit" in c])
        self.assertFalse((self.state / "ledger").exists())

    def test_a_large_update_is_embedded_on_the_gpu_in_windows_and_then_added_to_the_warehouse(self):
        n = int(PIN["CPU_EMBED_MAX"])                    # the smallest plan the GPU worker gets
        p, calls = self.run_driver(TO_EMBED=n)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        workers = [c for c in calls if "embed_worker.py" in c]
        self.assertEqual(len(workers), -(-n // int(PIN["EMBED_WINDOW"])))
        self.assertIn("--skip 0 --take", workers[0])
        self.assertFalse([c for c in calls if "cli embed-shard" in c
                          or ("model_package.py" in c and PIN["PASSAGE_PACKAGE_CHECKSUM"] in c)])   # not the CPU path
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

    def test_the_release_is_validated_on_its_index_with_every_gate_before_anything_is_published(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        work, cache = self.state / "work" / TAG, self.state / "model-cache"
        fetch = calls[self.index_of(calls, "--checksum " + PIN["QUERY_PACKAGE_CHECKSUM"])]   # the int8 query package
        for arg in ("model_package.py fetch", "--cache " + str(cache), "--graph " + PIN["QUERY_GRAPH"],
                    "--repo " + PIN["MODEL_REPO"], "--revision " + PIN["MODEL_REVISION"]):
            self.assertIn(arg + " ", fetch + " ")
        i = self.index_of(calls, "validate --index")
        args = calls[i].split()[1:]
        pairs = dict(zip(args[::2], args[1::2]))
        self.assertEqual(args[::2], ["--index", "--release", "--plan", "--warehouse", "--model", "--model-identity",
                                     "--onnx-runtime", "--threads", "--report"])
        self.assertEqual(pairs, {
            "--index": str(work / "index" / "index"), "--release": str(work / "release"), "--plan": str(work / "plan"),
            "--warehouse": str(self.warehouse), "--model": str(cache / PIN["QUERY_PACKAGE_CHECKSUM"] / PIN["QUERY_GRAPH"]),
            "--model-identity": str(self.state / "bin" / "family-model.json"), "--onnx-runtime": str(self.ort),
            "--threads": pairs["--threads"], "--report": str(work / "release" / "validation.json")})
        self.assertGreater(int(pairs["--threads"]), 0)
        self.assertEqual(calls[i + 1], "validate-env TMPDIR=" + str(work))   # its installed set stays in the work dir
        self.assertTrue(self.index_of(calls, "cli assemble --verify") < self.index_of(calls, "validate --index")
                        < self.index_of(calls, "cli release-files") < self.index_of(calls, "gh release create"))

    def test_a_release_the_validator_does_not_pass_is_never_published(self):
        for env, code in (({"VALIDATE_FAILS": "G6"}, 1), ({"VALIDATE_FAILS": "G3 G4"}, 1), ({"VALIDATE_UNREADABLE": "1"}, 2)):
            with self.subTest(env=env):
                self.calls.unlink(missing_ok=True)
                p, calls = self.run_driver(mode="base", TO_EMBED=0, **env)
                self.assertNotEqual(p.returncode, 0)
                self.assertIn(f"validate_semantic_vectors exited {code}", p.stdout)
                after = calls[self.index_of(calls, "validate --index"):]
                self.assertFalse([c for c in after if c.startswith("gh ") or "release-files" in c])
                self.assertFalse([c for c in calls if "gh release create" in c or "gh release upload" in c
                                  or "gh release edit" in c])
                self.assertFalse((self.state / "ledger").exists())

    def test_a_dry_run_the_validator_does_not_pass_fails_too(self):
        p, calls = self.run_driver(TO_EMBED=0, VALIDATE_FAILS="G4")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("validate_semantic_vectors exited 1", p.stdout)
        self.assertFalse([c for c in calls if "release-files" in c])

    def test_a_missing_validator_stops_the_build_before_it_starts(self):
        p, calls = self.run_driver(VALIDATE_SEMANTIC_VECTORS=str(self.root / "bin" / "no-validator"))
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("validate_semantic_vectors is not at", p.stdout)
        self.assertFalse([c for c in calls if "release download" in c])

    def test_the_stub_validator_refuses_what_the_real_one_refuses(self):
        release, index, plan = self.root / "rel", self.root / "idx", self.root / "plan"
        release.mkdir(); index.mkdir(); plan.mkdir()
        for f in ("segment.oxv", "release.json"):
            (release / f).write_text("x")
        (plan / "plan-manifest.json").write_text("{}")
        model, ort = self.root / "q.onnx", self.root / "libonnxruntime.so"
        model.write_text("x"); ort.write_text("x")
        g6 = ["--warehouse", str(self.warehouse), "--model", str(model), "--model-identity", str(self.state / "bin" / "family-model.json")]
        base = ["--index", str(index), "--release", str(release)]
        report = self.root / "report.json"
        for args, code in ((base + g6, 0), (base + g6 + ["--onnx-runtime", str(ort), "--plan", str(plan)], 0),
                           (base + ["--report", str(report)], 1),                       # G6 not given: not run
                           (base + g6[:2], 2),                                          # G6 half given
                           (base + g6 + ["--frobnicate", "1"], 2),
                           (base + ["--skip", "G3", "--skip", "G4", "--skip", "G6"], 2),
                           (base + ["--skip", "G6"], 0),
                           (["--release", str(release)] + g6, 2),                      # no --index
                           (["--index", str(index)] + g6, 2),                          # no set
                           (base + g6 + ["--threads", "0"], 2),
                           (["--index", str(index), "--release", str(self.root)] + g6, 2),   # not a release
                           (base + g6 + ["--plan", str(self.root / "nowhere")], 2)):
            with self.subTest(args=args):
                p = subprocess.run([str(self.root / "bin" / "validate")] + args, capture_output=True, text=True,
                                   env=dict(os.environ, CALLS=str(self.calls)))
                self.assertEqual(p.returncode, code, p.stderr)
        self.assertEqual(json.loads(report.read_text())["gates"][2], {"gate": "G6", "status": "notRun"})

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
        self.assertEqual(len(names), 4)
        self.assertTrue(names[0].endswith(".oxv.zst"))
        self.assertEqual(names[1:3], ["gates.json", "validation.json"])
        self.assertTrue(names[3].endswith(".manifest.json"))   # the manifest last

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

STUB_GIT = r"""#!/usr/bin/env bash
echo "git $*" >> "$CALLS"
if [ "$1" = clone ]; then           # git clone -q --filter=blob:none <url> <dest>
  url=${@: -2:1}; dest=${@: -1}
  mkdir -p "$dest"
  case "$url" in *otzaria-semantic-search*)
    mkdir -p "$dest/config/models/meivin-round2-onnx"
    echo '{"family_id":"stub"}' > "$dest/config/models/meivin-round2-onnx/model.json"
    echo '{"chunking_version":1}' > "$dest/config/models/meivin-round2-onnx/chunking.json" ;;
    *otzaria_search_engine*) mkdir -p "$dest/rust" ;;     # the plugin's crate
  esac
fi
"""

STUB_CARGO = r"""#!/usr/bin/env bash
echo "cargo $*" >> "$CALLS"
bins=(); while [ $# -gt 0 ]; do case "$1" in --bin) bins+=("$2"); shift 2;; *) shift;; esac; done
mkdir -p "$CARGO_TARGET_DIR/release"
for b in "${bins[@]}"; do printf '#!/bin/sh\n' > "$CARGO_TARGET_DIR/release/$b"; chmod +x "$CARGO_TARGET_DIR/release/$b"; done
"""

# a venv made before: its python stands in for pip and for the import check
STUB_VENV_PYTHON = r"""#!/usr/bin/env bash
echo "venv-python PIP_CERT=${PIP_CERT:-} $*" >> "$CALLS"
if [ "${1:-}" = - ]; then cat > /dev/null; echo "venv: stub"; fi
"""


@unittest.skipUnless(linux_tools(), "needs bash, flock and install")
class Bootstrap(unittest.TestCase):
    """bootstrap_runner.sh against stub git, cargo and venv: what it builds, records and hands pip."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        r = Path(self.tmp.name)
        self.root, self.calls, self.state = r, r / "calls.log", r / "state"
        (r / "bin").mkdir()
        python = self.state / "venv" / "bin" / "python"
        python.parent.mkdir(parents=True)
        for path, body in ((r / "bin" / "git", STUB_GIT), (r / "bin" / "cargo", STUB_CARGO), (python, STUB_VENV_PYTHON)):
            path.write_text(body)
            path.chmod(path.stat().st_mode | stat.S_IEXEC)
        self.ca = r / "system-ca.crt"
        self.ca.write_text("a stand-in for the system bundle")
        # the stubs, flock, and the system's tools: no other cargo
        self.path = os.pathsep.join([str(r / "bin"), os.path.dirname(shutil.which("flock")), "/usr/bin", "/bin"])

    def tearDown(self):
        self.tmp.cleanup()

    def run_bootstrap(self, **env):
        self.calls.unlink(missing_ok=True)
        e = {"PATH": self.path, "CALLS": str(self.calls), "HOME": str(self.root), "SYSTEM_CA": str(self.ca)}
        e.update({k: str(v) for k, v in env.items()})
        p = subprocess.run(["bash", str(BOOTSTRAP), "--state", str(self.state)], capture_output=True, text=True, env=e)
        return p, (self.calls.read_text().splitlines() if self.calls.exists() else [])

    def test_the_validator_is_built_able_to_run_g6_and_recorded_like_every_binary(self):
        p, calls = self.run_bootstrap()
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        b = self.state / "bin"
        for name, rev, build in (
                ("otzaria-semantic-search", PIN["SIDECAR_REV"], "--features onnx-backend --bin otzaria-semantic-search"),
                ("export_semantic_plan", PIN["PLUGIN_REV"], "--features semantic-integration --bin export_semantic_plan"),
                ("validate_semantic_vectors", PIN["PLUGIN_REV"], "--features semantic --bin validate_semantic_vectors")):
            self.assertTrue(os.access(b / name, os.X_OK), name)
            self.assertEqual((b / f"{name}.rev").read_text().strip(), rev, name)
            self.assertEqual((b / f"{name}.build").read_text().strip(), build, name)
        self.assertTrue((b / "family-model.json").exists() and (b / "chunking.json").exists())
        self.assertFalse((self.state / "build").exists())        # the build directory goes
        p, calls = self.run_bootstrap()                            # and a second run builds nothing
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertFalse([c for c in calls if c.startswith("cargo ")])

    def test_pip_is_handed_the_system_ca_bundle_unless_it_is_told_another(self):
        for env, expected in (({}, str(self.ca)), ({"PIP_CERT": "/elsewhere/bundle.pem"}, "/elsewhere/bundle.pem"),
                              ({"SYSTEM_CA": str(self.root / "no-bundle")}, "")):
            with self.subTest(env=env):
                p, calls = self.run_bootstrap(**env)
                self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
                pips = [c for c in calls if " -m pip install " in c]
                self.assertEqual(len(pips), 2)
                for c in pips:
                    self.assertTrue(c.startswith(f"venv-python PIP_CERT={expected} -m pip install "), c)

    def test_with_no_cargo_it_says_to_install_rustup_first(self):
        (self.root / "bin" / "cargo").unlink()
        p, calls = self.run_bootstrap()
        self.assertEqual(p.returncode, 69)
        self.assertIn("install rustup", p.stderr)
        self.assertFalse((self.state / "bin" / "otzaria-semantic-search").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
