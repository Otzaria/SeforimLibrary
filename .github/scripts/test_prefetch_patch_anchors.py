"""Sandbox tests for the patch-fan anchor prefetch and for the fan's driver loop.

Two things are driven for real here, against stub `gh`/`java`/`sqlite3` binaries:

  * `prefetch_patch_anchors.sh` — cold fetch, warm reuse, a corrupted cache
    entry, the size bound, the pruner's stale lock and every `abort` verdict;
  * the `run:` body of the workflow's "Produce + verify patch fan" step,
    extracted from the workflow text itself — together with the real
    `patch_fan_lib.sh` it sources — so the progress lines this asserts are the
    ones the runner will print, not a copy that can drift.

The anchors are 200 KB of filler instead of 1.3 GB, so the whole file runs in
seconds while exercising the same digest/size/eviction code paths.
"""

import functools
import os
import re
import shutil
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path

try:  # PyYAML ships with the ubuntu-latest image this job runs on.
    import yaml
except ImportError:  # pragma: no cover - only on a runner without PyYAML
    yaml = None

SCRIPTS = Path(__file__).parent
REPO = SCRIPTS.parents[1]
WORKFLOW = REPO / ".github" / "workflows" / "manual-generate-release.yml"
PREFETCH = SCRIPTS / "prefetch_patch_anchors.sh"
ANCHOR_DERIVATION = SCRIPTS / "patch_fan_anchors.sh"
PRECHECK = SCRIPTS / "patch_anchor_schema.py"
CONTRACT_TABLES = (
    REPO / "generator" / "common" / "src" / "jvmTest" / "resources"
    / "patch_tables_contract.json"
)


@functools.lru_cache(maxsize=None)
def sandbox_bash():
    """A bash that actually inherits the environment we hand it.

    On a Windows host `shutil.which("bash")` can resolve to WSL's bash.exe,
    which drops the environment (and the stub PATH) this file depends on, so
    probe the candidates and take the first one that answers. Resolved once:
    the answer is a fact about the host, and every sandbox below asks for it.
    """
    candidates = [shutil.which("bash")]
    if os.name == "nt":
        candidates.append(r"C:\Program Files\Git\usr\bin\bash.exe")
    for candidate in candidates:
        if not candidate or not Path(candidate).exists():
            continue
        probe = subprocess.run(
            [candidate, "-c", 'printf %s "${SANDBOX_PROBE:-}"'],
            env=dict(os.environ, SANDBOX_PROBE="ok"),
            capture_output=True,
            text=True,
        )
        if probe.stdout.strip() == "ok":
            return candidate
    return None


# ── Can this host run the sandboxes at all? ─────────────────────────────────
# The three sandbox classes drive `prefetch_patch_anchors.sh` and the
# workflow's own fan step body through a real bash, so they need exactly what
# those need: `mapfile` (bash >= 4 — the fan step reads the anchor table with
# it), GNU `stat --format=…` (the cache's mtime and size reads, and the stub
# `gh`), `sha256sum` (the cache's digest check) and GNU `sed -i` with no backup
# suffix (the fixtures that strip a field out of a cached `.meta`). Linux
# runners and Windows Git Bash have all four; a stock macOS has none of them —
# bash 3.2, BSD stat, `shasum`, and a `sed -i` that eats the script as its
# backup suffix. `touch -d` is GNU-only too and is deliberately NOT probed: it
# ships in the same coreutils as `stat`, so the `stat --format` line already
# answers for it, while GNU `sed` is a separate package and has to be asked
# separately. So gate on the CAPABILITY and never on the platform name: a mac
# with coreutils, GNU sed and bash 5 first on PATH runs these tests fine and
# must not skip, and a host that is missing a piece should say WHICH piece
# instead of failing halfway through a sandbox.
SANDBOX_PROBE_SCRIPT = r"""
mapfile -t __probe < /dev/null 2>/dev/null || printf 'missing:bash >= 4 (mapfile)\n'
stat --format=%Y / >/dev/null 2>&1 || printf 'missing:GNU stat (--format)\n'
printf '' | sha256sum >/dev/null 2>&1 || printf 'missing:sha256sum\n'
# A host that cannot even make a temp dir is not told it lacks `sed`: the probe
# stays quiet rather than inventing a gap out of an unrelated quirk.
__probe_dir=$(mktemp -d 2>/dev/null) || __probe_dir=
if [ -n "$__probe_dir" ]; then
  printf 'x\n' > "$__probe_dir/f"
  { sed -i '/^x$/d' "$__probe_dir/f" </dev/null >/dev/null 2>&1 \
      && [ ! -s "$__probe_dir/f" ]; } \
    || printf 'missing:GNU sed (-i with no backup suffix)\n'
  rm -rf "$__probe_dir"
fi
printf 'probe-ok\n'
"""
_PROBE_DONE = "probe-ok"
_PROBE_GAP = "missing:"


def _missing_sandbox_capabilities():
    """One bash call: the capabilities the sandboxes need and this host lacks."""
    bash = sandbox_bash()
    if bash is None:  # pragma: no cover - only on a host without bash
        return ("a bash that inherits its environment",)
    probe = subprocess.run(
        [bash, "-c", SANDBOX_PROBE_SCRIPT], capture_output=True, text=True
    )
    # Read only the lines the probe itself wrote: a shell that greets on stdout
    # (a $BASH_ENV, a chatty profile) must not be mistaken for a missing tool,
    # and one that never reached the last line has told us nothing.
    lines = [line.strip() for line in probe.stdout.splitlines()]
    if _PROBE_DONE not in lines:  # pragma: no cover - a bash that cannot run it
        return ("a bash that can run the capability probe",)
    return tuple(
        line[len(_PROBE_GAP):] for line in lines if line.startswith(_PROBE_GAP)
    )


_SANDBOX_GAPS = _missing_sandbox_capabilities()
HAS_GNU_SANDBOX = not _SANDBOX_GAPS
SANDBOX_SKIP_REASON = (
    "the sandbox drives prefetch_patch_anchors.sh and the fan step body for"
    " real; this host is missing " + ", ".join(_SANDBOX_GAPS)
)

# The counting gate that runs this suite accepts skips: `OK (skipped=22)` is a
# green step, so a probe that wrongly answered "this host cannot" would retire
# the whole S8/S12 sandbox coverage in silence. Both jobs that run it — the
# release workflow's `reconcile` and ci.yml's `contracts` — are ubuntu-latest,
# where every capability above is present. A gap reported on Linux is therefore
# a broken probe, not an unsupported host: fail loudly instead of skipping.
# (Not `assert`: `python3 -O` would strip it, and CI is where this must fire.)
if sys.platform.startswith("linux") and not HAS_GNU_SANDBOX:
    raise AssertionError(
        "sandbox capability probe reported '" + ", ".join(_SANDBOX_GAPS)
        + "' on a Linux host, where this suite is contracted to run its"
        " sandboxes for real — that is a broken probe, not an unsupported"
        " host, and it must not be allowed to skip quietly"
    )


def write(path, text, executable=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip(), encoding="utf-8", newline="\n")
    if executable:
        path.chmod(0o755)


# `gh` as the prefetch and the fan use it: one `api` call for the release's
# asset list, and `release download --pattern … --dir …` for the asset itself.
# $ASSET_ROOT/<tag>/ is the release; a tag with no seforim.db.zst is a release
# that does not publish one.
STUB_GH = r"""
    #!/bin/sh
    case "$1" in
      api)
        tag=${2##*/}
        f="$ASSET_ROOT/$tag/seforim.db.zst"
        [ -f "$f" ] || { echo "no seforim.db.zst on $tag" >&2; exit 1; }
        digest=""
        [ -f "$ASSET_ROOT/$tag/digest" ] && digest=$(cat "$ASSET_ROOT/$tag/digest")
        printf '%s\t%s\n' "$(stat --format='%s' "$f")" "$digest"
        exit 0 ;;
      release)
        shift; shift
        tag=$1; shift
        pattern=""; dir="."
        while [ $# -gt 0 ]; do
          case "$1" in
            --pattern) pattern=$2; shift 2 ;;
            --dir)     dir=$2;     shift 2 ;;
            *) shift ;;
          esac
        done
        if [ "${DOWNLOAD_FAIL_PATTERN:-}" = "$pattern" ]; then
          echo "HTTP 503: server error" >&2
          exit 1
        fi
        src="$ASSET_ROOT/$tag/$pattern"
        if [ ! -f "$src" ]; then
          echo "no assets match the file pattern $pattern" >&2
          exit 1
        fi
        mkdir -p "$dir"
        cp "$src" "$dir/$pattern"
        [ -z "${DOWNLOAD_LOG:-}" ] || echo "$tag/$pattern" >> "$DOWNLOAD_LOG"
        exit 0 ;;
    esac
    echo "stub gh: unexpected invocation: $*" >&2
    exit 90
    """

STUB_PYTHON3 = """
    #!/bin/sh
    exec "$REAL_PYTHON" "$@"
    """


class AnchorCacheRankContractTest(unittest.TestCase):
    """The two properties the LRU rank rests on, read off the script itself.

    Neither needs a sandbox — and neither may be skipped on a host that cannot
    run one, because both are what keeps the pruner from evicting the anchor a
    run just spent ~16 minutes downloading.
    """

    @classmethod
    def setUpClass(cls):
        cls.script = PREFETCH.read_text(encoding="utf-8")

    def test_the_rank_key_allocation_is_serialized(self):
        # A lock file of its own, inside the cache dir, taken for APPEND so
        # opening it cannot truncate the stamp another writer holds — and the
        # last key handed out is remembered there, because the `.meta` a caller
        # is about to write is not on disk while the next caller scans.
        self.assertIn('exec 9>> "$CACHE_DIR/.stamp.lock"', self.script)
        # Bounded, because a prefetch must never hang on its own bookkeeping.
        self.assertIn("flock -w 30 9", self.script)
        self.assertIn('cat "$CACHE_DIR/.stamp"', self.script)
        self.assertIn('> "$CACHE_DIR/.stamp"', self.script)
        # It is a different critical section from the pruner's, which keeps its
        # own `mkdir` lock.
        self.assertIn('lock="$CACHE_DIR/.prune.lock"', self.script)
        # …and where flock is unavailable the key cannot tie either: the writer's
        # own pid becomes a minor part, which cache_use_key accepts and the
        # pruner sorts on as a key of its own.
        self.assertIn("""printf '%s.%s' "$(cache_alloc_use_key)" "${BASHPID:-$$}\"""", self.script)

    def test_the_prune_sort_is_stable_and_never_ranks_by_tag(self):
        # `-s` disables sort's last-resort whole-line comparison; without it a
        # full tie was decided by the third field — the tag — which ranked v26-x
        # last and evicted it. Three explicit numeric keys, then stability: a
        # tie can only ever degrade to input order, which claims no recency.
        self.assertIn(
            "sort -t$'\\t' -s -k1,1rn -k2,2rn -k3,3rn", self.script
        )
        self.assertIn(
            """printf '%s\\t%s\\t%s\\t%s\\n' "${key%%.*}" "$minor" "$(mtime_ns "$meta")\"""",
            self.script,
        )
        self.assertIn(
            'while IFS=$\'\\t\' read -r use_key use_minor mtime dir; do', self.script
        )


@unittest.skipIf(yaml is None, "PyYAML unavailable")
@unittest.skipUnless(HAS_GNU_SANDBOX, SANDBOX_SKIP_REASON)
class AnchorPrefetchCacheSandboxTest(unittest.TestCase):
    """One sandbox, driven through the attempts a real cycle would make."""

    phases = {}

    @classmethod
    def setUpClass(cls):
        bash = sandbox_bash()
        if bash is None:  # pragma: no cover - only on a host without bash
            raise unittest.SkipTest("no bash that inherits its environment")
        import tempfile

        cls._tmp = tempfile.TemporaryDirectory()
        root = Path(cls._tmp.name)
        cls.root = root

        shutil.copy(PREFETCH, root / "prefetch_patch_anchors.sh")
        shutil.copy(PRECHECK, root / "patch_anchor_schema.py")
        shutil.copy(CONTRACT_TABLES, root / "contract.json")
        write(root / "bin" / "gh", STUB_GH, executable=True)
        write(root / "bin" / "python3", STUB_PYTHON3, executable=True)
        # Only P8 gets this on its PATH: it makes the recorded pid look like a
        # live prefetch, which is the one abort branch that signals anything.
        write(
            root / "bin-ps" / "ps",
            """
            #!/bin/sh
            echo "bash prefetch_patch_anchors.sh run anchors.tsv dest"
            """,
            executable=True,
        )

        # Anchors: two fetchable, one on the documented unpatchable list, one
        # whose release publishes no seforim.db.zst at all.
        write(
            root / "anchors.tsv",
            "ANCHOR\t1\t26\tv26-x\n"
            "ANCHOR\t2\t25\tv25-x\n"
            "ANCHOR\t3\t24\tv24-noasset\n"
            "ANCHOR\t16\t11\tv11-x\n",
        )
        write(root / "anchors-v23.tsv", "ANCHOR\t4\t23\tv23-x\n")
        write(root / "anchors-v22.tsv", "ANCHOR\t5\t22\tv22-x\n")
        write(root / "anchors-v21.tsv", "ANCHOR\t6\t21\tv21-x\n")
        # The LRU-rank phases: one anchor per run, so the store order is the
        # script's own and not a race between five parallel fetches.
        write(root / "anchors-v26.tsv", "ANCHOR\t1\t26\tv26-x\n")
        write(root / "anchors-v25.tsv", "ANCHOR\t2\t25\tv25-x\n")
        # Unpatchable: nothing is downloaded and nothing is stored, but
        # run_prefetch still prunes at the end — a pruner trigger that cannot
        # itself change the ranking it is being asked about.
        write(root / "anchors-v11.tsv", "ANCHOR\t16\t11\tv11-x\n")
        # A hit (v26-x, already cached) and a store (v22-x) in the same run.
        write(
            root / "anchors-hit.tsv",
            "ANCHOR\t1\t26\tv26-x\n"
            "ANCHOR\t5\t22\tv22-x\n",
        )
        (root / "assets" / "v24-noasset").mkdir(parents=True)
        for tag, filler in (
            ("v26-x", b"a"), ("v25-x", b"b"), ("v23-x", b"c"),
            ("v22-x", b"d"), ("v21-x", b"e"), ("v11-x", b"f"),
        ):
            asset = root / "assets" / tag / "seforim.db.zst"
            asset.parent.mkdir(parents=True, exist_ok=True)
            asset.write_bytes(filler * 204800)
            import hashlib

            write(
                root / "assets" / tag / "digest",
                "sha256:" + hashlib.sha256(asset.read_bytes()).hexdigest() + "\n",
            )

        driver = r"""
            set -uo pipefail
            export PATH="$PWD/bin:$PATH"
            export ASSET_ROOT="$PWD/assets"
            export DOWNLOAD_LOG="$PWD/downloads.log"
            export GITHUB_REPOSITORY=Otzaria/SeforimLibrary
            export PATCH_ANCHOR_CHECK="$PWD/patch_anchor_schema.py"
            export PATCH_ANCHOR_THIS_SCHEMA="$PWD/db_schema.json"
            export PATCH_ANCHOR_CONTRACT="$PWD/contract.json"
            export PATCH_ANCHOR_CACHE_DIR="$PWD/cache"
            export PATCH_ANCHOR_CACHE_KEEP=2
            export PREFETCH_PARALLEL=2
            run() { bash ./prefetch_patch_anchors.sh "$@"; }
            phase() { echo "##### $1"; : > "$DOWNLOAD_LOG"; }
            markers() {  # <dest>
              for m in "$1"/*/.done; do
                [ -f "$m" ] || continue
                echo "-- marker $(basename "$(dirname "$m")") $(head -n1 "$m")"
                sed -n '2,$p' "$m"
              done
            }
            cached_in() { echo "-- cached: $(ls "$1" 2>/dev/null | tr '\n' ' ')"; }
            report() {  # <dest>
              markers "$1"
              echo "-- downloaded: $(tr '\n' ' ' < "$DOWNLOAD_LOG")"
              cached_in cache
            }
            # The LRU-rank phases drive their own cache root and their own
            # bound, with a clean environment: nothing they do can reach the
            # shared `cache` the phases above build, and nothing above can
            # reach them.
            rank_run() {  # <cache-dir> <keep> <anchors-tsv> <dest>
              env -u DOWNLOAD_FAIL_PATTERN \
                PATCH_ANCHOR_CACHE_DIR="$PWD/$1" PATCH_ANCHOR_CACHE_KEEP="$2" \
                bash ./prefetch_patch_anchors.sh run "$3" "$4"
            }
            rank_keys() {  # <cache-dir> <tag>...
              local d="$1"; shift
              local t out=""
              for t in "$@"; do
                out="$out $t=$(sed -n 's/^used_ns=//p' "$d/$t/.meta" 2>/dev/null | head -n1)"
              done
              echo "-- keys:$out"
            }
            distinct_mtimes() {  # <cache-dir>
              echo "-- distinct mtimes: $(stat --format='%.9Y' "$1"/*/.meta \
                | sort -u | wc -l)"
            }

            phase COLD
            run run anchors.tsv dest-cold
            report dest-cold

            phase WARM
            run run anchors.tsv dest-warm
            report dest-warm

            phase CORRUPT
            printf 'X' | dd of=cache/v26-x/seforim.db.zst bs=1 seek=99 \
              conv=notrunc status=none
            run run anchors.tsv dest-corrupt
            report dest-corrupt

            phase BOUND
            # cache_prune used to rank by `.meta` mtime alone, which
            # `stat --format=%Y` reports in whole seconds. On a real runner the
            # anchors are 1.3 GB and minutes apart, but the three phases above
            # finish in well under a second here: the mtimes tied and `sort -rn`
            # then fell back to comparing the whole line, so which entry
            # survived depended on the TAG NAME, not on recency, and this phase
            # passed or failed with the clock (~50% on a Linux box, reliably
            # green on a slower one). The rank is now the key each entry records
            # for itself (see TIE_SAME_SECOND below), so this ages the two
            # entries the bound is supposed to drop only to keep the phase
            # honest about what it asserts — an hour, well inside the 30-day max
            # age this phase is not testing.
            touch -d '-1 hour' cache/v26-x/.meta cache/v25-x/.meta
            run run anchors-v23.tsv dest-bound
            report dest-bound

            phase PRUNE_LOCK_HELD
            mkdir -p cache/.prune.lock
            PATCH_ANCHOR_PRUNE_LOCK_STALE=99999 run run anchors-v22.tsv dest-lock
            report dest-lock

            phase PRUNE_LOCK_STALE
            PATCH_ANCHOR_PRUNE_LOCK_STALE=0 run run anchors-v21.tsv dest-stale
            report dest-stale

            phase ABORT_NEVER_STARTED
            mkdir -p dest-none
            run abort dest-none

            phase ABORT_ALREADY_FINISHED
            run abort dest-cold

            phase ABORT_SIGNALLED
            mkdir -p dest-live
            sleep 30 &
            echo $! > dest-live/.pid
            PATH="$PWD/bin-ps:$PATH" run abort dest-live

            phase CACHE_REPORT
            run cache-report

            phase CACHE_ON_TMPFS_BUILD_DIR
            GITHUB_WORKSPACE="$PWD/ws" PATCH_ANCHOR_CACHE_DIR="$PWD/ws/build/a" \
              run cache-report

            phase CACHE_DISABLED
            PATCH_ANCHOR_CACHE_DIR= run cache-report

            phase DOWNLOAD_FAILS
            DOWNLOAD_FAIL_PATTERN=seforim.db.zst PATCH_ANCHOR_CACHE_DIR="$PWD/cache-dl" \
              run run anchors-v21.tsv dest-dlfail
            report dest-dlfail

            # ── the LRU rank is a total order, not a coin flip ───────────────
            # Both phases below force the worst case any filesystem can hand
            # the pruner: EVERY `.meta` mtime identical to the nanosecond, so
            # nothing observable about the files can order them and only the
            # key each entry recorded for itself can. That is the same
            # situation a same-second store produces on a 1-second-granularity
            # mtime (some tmpfs/overlay configurations), and it is what the old
            # `stat %Y` + `sort -rn` ranking got wrong: the mtimes tied, `sort`
            # fell back to comparing the rest of the line — the TAG — and the
            # entry stored LAST (v23-x) ranked below v25-x and v26-x and was
            # evicted while it was the newest.
            phase TIE_SAME_SECOND
            rank_run cache-tie 9 anchors-v26.tsv dest-tie1 > /dev/null 2>&1
            rank_run cache-tie 9 anchors-v25.tsv dest-tie2 > /dev/null 2>&1
            rank_run cache-tie 9 anchors-v23.tsv dest-tie3 > /dev/null 2>&1
            touch -r cache-tie/v26-x/.meta cache-tie/*/.meta
            distinct_mtimes cache-tie
            rank_keys cache-tie v26-x v25-x v23-x
            rank_run cache-tie 2 anchors-v11.tsv dest-tie4 2>&1
            cached_in cache-tie

            phase HIT_REFRESH_TIE
            rank_run cache-hit 9 anchors-v26.tsv dest-hit1 > /dev/null 2>&1
            rank_run cache-hit 9 anchors-v25.tsv dest-hit2 > /dev/null 2>&1
            rank_run cache-hit 9 anchors-v23.tsv dest-hit3 > /dev/null 2>&1
            touch -r cache-hit/v26-x/.meta cache-hit/*/.meta
            # v26-x is the OLDEST store in this cache; reusing it must make it
            # the newest entry, in the same second as v22-x is stored fresh.
            : > "$DOWNLOAD_LOG"
            rank_run cache-hit 9 anchors-hit.tsv dest-hit4 > /dev/null 2>&1
            markers dest-hit4
            echo "-- downloaded: $(tr '\n' ' ' < "$DOWNLOAD_LOG")"
            touch -r cache-hit/v23-x/.meta cache-hit/*/.meta
            distinct_mtimes cache-hit
            rank_keys cache-hit v26-x v25-x v23-x v22-x
            rank_run cache-hit 2 anchors-v11.tsv dest-hit5 2>&1
            cached_in cache-hit

            # The durable cache on the runner outlives the script: on the first
            # attempt after this change every entry in it was written without a
            # key. They rank by their mtime, as they always did, and must not
            # outrank the anchor this run just paid for.
            phase LEGACY_ENTRIES_WITHOUT_A_KEY
            rank_run cache-legacy 9 anchors-v26.tsv dest-leg1 > /dev/null 2>&1
            rank_run cache-legacy 9 anchors-v25.tsv dest-leg2 > /dev/null 2>&1
            sed -i '/^used_ns=/d' cache-legacy/v26-x/.meta cache-legacy/v25-x/.meta
            rank_run cache-legacy 2 anchors-v23.tsv dest-leg3 2>&1
            # cache_store prunes, so this eviction is reported in the marker.
            markers dest-leg3
            cached_in cache-legacy

            # Recording the key made a cache HIT rewrite `.meta` instead of
            # merely touching it, so two workers marking the SAME entry now
            # write the same file. They are subshells of one script and share
            # $$, so a temp named after $$ would have one truncating what the
            # other had just written and the survivor renaming a `.meta` that
            # had lost sha256= — the field a warm reuse is verified against —
            # or was empty. The anchor list cannot repeat a tag today, so this
            # drives cache_mark_used directly: it is the guard, not the path.
            phase MARK_USED_RACE
            sed -n '/^case "$mode" in/q;p' prefetch_patch_anchors.sh > rank-lib.sh
            rm -rf cache-race && mkdir -p cache-race/v-x
            printf 'tag=v-x\nsize=1\nsha256=deadbeef\ncached_at=1\nused_ns=1700000000000000000\n' \
              > cache-race/v-x/.meta
            (
              export PATCH_ANCHOR_CACHE_DIR="$PWD/cache-race"
              . ./rank-lib.sh
              for w in 1 2 3 4 5; do
                ( i=0; while [ "$i" -lt 40 ]; do
                    cache_mark_used "$PWD/cache-race/v-x"; i=$((i + 1)); done ) &
              done
              wait
            )
            echo "-- race meta: $(tr '\n' ' ' < cache-race/v-x/.meta)"
            echo "-- race used_ns lines: $(grep -c '^used_ns=' cache-race/v-x/.meta)"
            echo "-- race strays: $(ls -a cache-race/v-x | grep -c '^\.meta\.')"

            # ── allocating the rank key is a critical section ────────────────
            # The five fetches allocate `used_ns` AT THE SAME TIME (a hit marks,
            # a store stamps, both from `fetch_one … &` subshells). Read-max →
            # write is a lost update unless it is serialized, and a stamp from
            # the FUTURE puts every caller on the `highest + 1` branch, which is
            # read-max and nothing else: unlocked, all five read the same
            # maximum and write the same key. Then the mtimes decide — and when
            # those tie too (every `.meta` touched to one nanosecond below, the
            # worst case a whole-second filesystem hands the pruner), `sort`
            # fell back to the rest of the line, i.e. the TAG, and evicted the
            # newest anchor. The entries are named so that tag order is the
            # REVERSE of the order they are marked in.
            phase STAMP_ALLOC_RACE
            rm -rf cache-stamp && mkdir -p cache-stamp/v99-future
            printf 'tag=v99-future\nsize=1\nsha256=deadbeef\ncached_at=1\nused_ns=4000000000000000000\n' \
              > cache-stamp/v99-future/.meta
            for t in v26-x v25-x v24-x v23-x v22-x; do
              mkdir -p "cache-stamp/$t"
              printf 'tag=%s\nsize=1\nsha256=deadbeef\ncached_at=1\n' "$t" \
                > "cache-stamp/$t/.meta"
            done
            (
              export PATCH_ANCHOR_CACHE_DIR="$PWD/cache-stamp"
              . ./rank-lib.sh
              for t in v26-x v25-x v24-x v23-x v22-x; do
                ( cache_mark_used "$PWD/cache-stamp/$t" ) &
              done
              wait
            )
            rank_keys cache-stamp v99-future v26-x v25-x v24-x v23-x v22-x
            echo "-- stamp distinct: $(sed -n 's/^used_ns=//p' cache-stamp/*/.meta \
              | sort -u | wc -l)"
            touch -r cache-stamp/v99-future/.meta cache-stamp/*/.meta
            distinct_mtimes cache-stamp
            (
              export PATCH_ANCHOR_CACHE_DIR="$PWD/cache-stamp" PATCH_ANCHOR_CACHE_KEEP=1
              . ./rank-lib.sh
              cache_prune
            )
            cached_in cache-stamp
            """
        result = subprocess.run(
            [bash, "-c", textwrap.dedent(driver)],
            cwd=root,
            env=dict(os.environ, REAL_PYTHON=sys.executable),
            capture_output=True,
            text=True,
        )
        cls.result = result
        blocks = re.split(r"^##### ", result.stdout, flags=re.M)[1:]
        cls.phases = {}
        for block in blocks:
            name, _, rest = block.partition("\n")
            cls.phases[name.strip()] = rest

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def phase(self, name):
        self.assertIn(name, self.phases, self.result.stdout + self.result.stderr)
        return self.phases[name]

    def test_cold_attempt_downloads_verifies_and_caches_every_anchor(self):
        cold = self.phase("COLD")
        # One line per anchor naming the release asset and its size (the five
        # bare `ANCHOR` lines of run 34021998271 said none of this).
        self.assertRegex(
            cold, r"anchor v26-x \(offset 1\): seforim\.db\.zst 204800 bytes sha256:[0-9a-f]{64}"
        )
        self.assertRegex(cold, r"downloaded v26-x in \d+s")
        self.assertRegex(cold, r"downloaded v25-x in \d+s")
        self.assertNotIn("reused v26-x from cache", cold)
        # Verdicts the fan reads off the first line of each marker.
        self.assertIn("-- marker v26-x ok", cold)
        self.assertIn("-- marker v11-x unpatchable", cold)
        # An anchor on the documented unpatchable list is never downloaded.
        downloaded = cold.split("-- downloaded:")[1].splitlines()[0]
        self.assertIn("v26-x/seforim.db.zst", downloaded)
        self.assertIn("v25-x/seforim.db.zst", downloaded)
        self.assertNotIn("v11-x/seforim.db.zst", downloaded)
        # A release that publishes no seforim.db.zst is named, not silent.
        self.assertIn("-- marker v24-noasset failed", cold)
        self.assertIn(
            "release publishes no seforim.db.zst asset", cold
        )
        self.assertIn("source=release", cold)

    def test_second_attempt_reuses_the_cache_and_downloads_nothing(self):
        warm = self.phase("WARM")
        self.assertIn("reused v26-x from cache (sha256 ok)", warm)
        self.assertIn("reused v25-x from cache (sha256 ok)", warm)
        self.assertIn("source=cache", warm)
        self.assertNotIn("downloaded v26-x", warm)
        self.assertEqual(warm.split("-- downloaded:")[1].splitlines()[0].strip(), "")
        # The verdict the fan reads is unchanged by where the bytes came from.
        self.assertIn("-- marker v26-x ok", warm)

    def test_a_corrupted_cache_entry_warns_and_is_downloaded_again(self):
        corrupt = self.phase("CORRUPT")
        self.assertRegex(
            corrupt,
            r"::warning::patch-fan anchor cache: digest mismatch for v26-x "
            r"\(cached sha256:[0-9a-f]{64}, expected sha256:[0-9a-f]{64}\)"
            r" — evicting and downloading it again",
        )
        self.assertRegex(corrupt, r"downloaded v26-x in \d+s")
        downloaded = corrupt.split("-- downloaded:")[1].splitlines()[0]
        self.assertIn("v26-x/seforim.db.zst", downloaded)
        # The untouched sibling is still a hit — one bad entry is not a flush.
        self.assertIn("reused v25-x from cache (sha256 ok)", corrupt)
        self.assertIn("-- marker v26-x ok", corrupt)

    def test_the_cache_is_bounded_and_says_what_it_evicted(self):
        bound = self.phase("BOUND")
        self.assertRegex(
            bound,
            r"patch-fan anchor cache: evicting v\d+-x \([^)]*\) — bound is the 2 "
            r"most recent, max age 30d",
        )
        cached = self.phase("BOUND").split("-- cached:")[1].splitlines()[0].split()
        self.assertEqual(len(cached), 2, cached)
        self.assertIn("v23-x", cached)

    @staticmethod
    def _rank_keys(phase):
        # A key is "<major>" — or "<major>.<minor>" on the lockless fallback, in
        # which case the two parts are exactly what the pruner sorts on, in that
        # order. Compare them as tuples so both shapes order identically here
        # and in `sort`.
        line = phase.split("-- keys:")[1].splitlines()[0]
        return {
            tag: tuple(int(part) for part in key.split("."))
            for tag, _, key in (field.partition("=") for field in line.split())
        }

    def test_entries_stored_in_the_same_second_still_rank_by_recency(self):
        # THE defect: `stat --format=%Y` is whole seconds, so entries stored
        # inside one second tie, and `sort -rn` then breaks the tie on the rest
        # of the line — the tag. v23-x, stored last, ranked below v25-x and
        # v26-x and was evicted while it was the newest: in production that
        # throws away the 1.3 GB anchor the run just spent ~16 minutes
        # downloading. The mtimes here are forced identical to the NANOSECOND,
        # which is strictly worse than a same-second tie and is also what a
        # filesystem with 1-second mtime granularity hands the pruner.
        tie = self.phase("TIE_SAME_SECOND")
        self.assertIn("-- distinct mtimes: 1", tie)
        # The bound drops the oldest store and keeps the two newest. (Against
        # the pre-fix pruner this is `evicting v23-x` and `v25-x v26-x`.)
        self.assertRegex(
            tie,
            r"patch-fan anchor cache: evicting v26-x \([^)]*\) — bound is the 2 "
            r"most recent, max age 30d",
        )
        cached = tie.split("-- cached:")[1].splitlines()[0].split()
        self.assertEqual(sorted(cached), ["v23-x", "v25-x"], tie)
        # …because the recorded key is a total order in store order — never
        # equal, never inverted, whatever the mtimes say.
        keys = self._rank_keys(tie)
        self.assertEqual(sorted(keys), ["v23-x", "v25-x", "v26-x"], keys)
        self.assertLess(keys["v26-x"], keys["v25-x"], keys)
        self.assertLess(keys["v25-x"], keys["v23-x"], keys)

    def test_a_hit_refreshed_in_the_same_second_as_a_store_ranks_newest(self):
        # A hit is a use: cache_lookup must move the entry to the front of the
        # LRU even when the store it races is written in the same second — and
        # even when the filesystem gives both the same mtime, as it does here.
        hit = self.phase("HIT_REFRESH_TIE")
        self.assertIn("reused v26-x from cache (sha256 ok)", hit)
        downloaded = hit.split("-- downloaded:")[1].splitlines()[0]
        self.assertIn("v22-x/seforim.db.zst", downloaded)
        self.assertNotIn("v26-x/seforim.db.zst", downloaded)
        self.assertIn("-- distinct mtimes: 1", hit)
        # The two survivors are the reuse and the store of this run. (The
        # pre-fix pruner evicted v22-x — the anchor it had just downloaded —
        # together with v23-x, and kept the two entries nothing had touched.)
        cached = hit.split("-- cached:")[1].splitlines()[0].split()
        self.assertEqual(sorted(cached), ["v22-x", "v26-x"], hit)
        for evicted in ("v25-x", "v23-x"):
            self.assertIn(
                f"patch-fan anchor cache: evicting {evicted}", hit
            )
        # v26-x was stored FIRST of the four and is nonetheless ranked above
        # the two entries that were only stored, because it was just reused.
        keys = self._rank_keys(hit)
        self.assertEqual(len(set(keys.values())), 4, keys)
        self.assertGreater(keys["v26-x"], keys["v25-x"], keys)
        self.assertGreater(keys["v26-x"], keys["v23-x"], keys)
        self.assertGreater(keys["v22-x"], keys["v23-x"], keys)

    def test_entries_cached_before_the_rank_key_existed_still_rank(self):
        # The cache is durable and per-runner: the first attempt after this
        # change finds 8 entries written by the version before it, with no
        # used_ns line at all. They fall back to the mtime the old pruner used,
        # so they still rank among themselves — and none of them can outrank
        # the anchor this run downloaded.
        legacy = self.phase("LEGACY_ENTRIES_WITHOUT_A_KEY")
        cached = legacy.split("-- cached:")[1].splitlines()[0].split()
        self.assertEqual(len(cached), 2, legacy)
        self.assertIn("v23-x", cached)
        self.assertRegex(legacy, r"patch-fan anchor cache: evicting v2[56]-x")

    def test_two_workers_marking_one_entry_cannot_shred_its_meta(self):
        # A hit rewrites `.meta` now. Five subshells of one script share $$, so
        # the temp file has to be named per subshell or two of them collide:
        # one truncates the temp the other is appending to, and the `.meta`
        # that survives the rename has lost sha256= (a release that publishes
        # no digest then has nothing to verify the entry against and the entry
        # is evicted) or is empty. The rename itself is the only interleaving
        # that may happen, and it is atomic.
        race = self.phase("MARK_USED_RACE")
        self.assertIn("sha256=deadbeef", race)
        self.assertIn("tag=v-x", race)
        self.assertIn("size=1", race)
        self.assertIn("-- race used_ns lines: 1", race)
        self.assertIn("-- race strays: 0", race)

    def test_concurrent_allocations_get_distinct_strictly_higher_keys(self):
        # Five subshells allocate at once, all of them on the `highest + 1`
        # branch (a planted stamp from the future). Unserialized they read the
        # same maximum and are handed the SAME key — five of five, reproduced by
        # the audit. The allocation is a critical section now, so every key is
        # distinct and every one of them is above the future stamp it had to
        # beat.
        race = self.phase("STAMP_ALLOC_RACE")
        keys = self._rank_keys(race)
        self.assertEqual(len(keys), 6, keys)
        self.assertIn("-- stamp distinct: 6", race)
        planted = keys.pop("v99-future")
        self.assertEqual(planted, (4000000000000000000,), keys)
        self.assertEqual(len(set(keys.values())), 5, keys)
        for tag, key in keys.items():
            self.assertGreater(key, planted, (tag, keys))

    def test_a_tie_can_no_longer_evict_the_entry_that_was_marked_last(self):
        # …and the ranking that follows from those keys is the one the pruner
        # uses: with every `.meta` mtime forced identical to the nanosecond, the
        # single survivor of a keep-1 bound is the entry whose key is highest —
        # the one marked last. Against the pre-fix allocation all five keys tied,
        # the mtimes tied, and `sort`'s whole-line fallback kept the highest TAG
        # (v26-x) while evicting the anchor the run had just paid for.
        race = self.phase("STAMP_ALLOC_RACE")
        self.assertIn("-- distinct mtimes: 1", race)
        keys = self._rank_keys(race)
        newest = max(keys, key=keys.get)
        cached = race.split("-- cached:")[1].splitlines()[0].split()
        self.assertEqual(cached, [newest], (cached, keys))
        for evicted in set(keys) - {newest}:
            self.assertIn(f"patch-fan anchor cache: evicting {evicted}", race)

    def test_a_pruner_killed_mid_run_cannot_disable_the_bound_for_ever(self):
        held = self.phase("PRUNE_LOCK_HELD")
        self.assertNotIn("evicting", held)
        self.assertEqual(
            len(held.split("-- cached:")[1].splitlines()[0].split()), 3
        )
        stale = self.phase("PRUNE_LOCK_STALE")
        self.assertRegex(
            stale, r"patch-fan anchor cache: clearing a stale prune lock \(\d+s old, limit 0s\)"
        )
        self.assertIn("evicting", stale)
        self.assertEqual(
            len(stale.split("-- cached:")[1].splitlines()[0].split()), 2
        )

    def test_every_abort_says_what_it_aborted_and_none_of_them_fail(self):
        self.assertIn(
            "patch-fan anchor prefetch: nothing to abort — no background prefetch"
            " is recorded in dest-none",
            self.phase("ABORT_NEVER_STARTED"),
        )
        finished = self.phase("ABORT_ALREADY_FINISHED")
        self.assertRegex(
            finished,
            r"patch-fan anchor prefetch: pid \d+ is no longer this script \(it had"
            r" already finished\) — nothing signalled;",
        )
        # The state of the work is on every one of those lines.
        self.assertIn(
            "4 anchors had finished (2 ok, 1 unpatchable, 1 failed);"
            " verified anchors stay in the durable cache",
            finished,
        )
        self.assertRegex(
            self.phase("ABORT_SIGNALLED"),
            r"patch-fan anchor prefetch aborted: signalled process group \d+;",
        )
        # `abort` is called with the job already failing: it must never add a
        # failure of its own.
        self.assertEqual(self.result.returncode, 0, self.result.stderr[-2000:])

    def test_the_cache_reports_itself_and_refuses_ram_backed_roots(self):
        report = self.phase("CACHE_REPORT")
        self.assertRegex(
            report,
            r"patch-fan anchor cache: \d+ anchors, \S+ at \S+ \(bound: 2 anchors,"
            r" max age 30d\) — kept across runs on purpose, no cleanup deletes it",
        )
        tmpfs = self.phase("CACHE_ON_TMPFS_BUILD_DIR")
        self.assertIn(
            "is inside the tmpfs build dir — caching disabled for this run", tmpfs
        )
        self.assertIn("patch-fan anchor cache: disabled", tmpfs)
        self.assertIn(
            "patch-fan anchor cache: disabled — every anchor is downloaded again"
            " on every attempt",
            self.phase("CACHE_DISABLED"),
        )

    def test_a_failed_download_names_ghs_own_reason(self):
        # The release publishes the asset (asset_meta just read its size and
        # digest), so a failed download is a real failure — but gh only writes
        # a bare line to stderr, and this log is replayed inside the fan's.
        out = self.phase("DOWNLOAD_FAILS")
        self.assertIn("-- marker v21-x failed", out)
        self.assertIn(
            "prefetch anchor v21 (v21-x): download of seforim.db.zst failed "
            "(HTTP 503: server error) — the fan falls back to its serial download",
            out,
        )
        # It degrades instead of failing the fan, and leaves no half file behind.
        self.assertNotIn("dest-dlfail/v21-x/seforim.db.zst", out)


# A `gh release download` that refuses to overwrite, like the real one without
# `--clobber`: it is what turns a partial file left in the run dir into a lost
# anchor, so the cache's failure paths have to leave that dir clean.
STUB_GH_NO_CLOBBER = r"""
    #!/bin/sh
    case "$1" in
      release)
        shift; shift
        tag=$1; shift
        pattern=""; dir="."
        while [ $# -gt 0 ]; do
          case "$1" in
            --pattern) pattern=$2; shift 2 ;;
            --dir)     dir=$2;     shift 2 ;;
            *) shift ;;
          esac
        done
        src="$ASSET_ROOT/$tag/$pattern"
        [ -f "$src" ] || { echo "no assets match the file pattern $pattern" >&2; exit 1; }
        if [ -e "$dir/$pattern" ]; then
          echo "would clobber existing file: $dir/$pattern" >&2
          exit 1
        fi
        mkdir -p "$dir"
        cp "$src" "$dir/$pattern"
        [ -z "${DOWNLOAD_LOG:-}" ] || echo "$tag/$pattern" >> "$DOWNLOAD_LOG"
        exit 0 ;;
    esac
    exec "$STUB_GH_PLAIN" "$@"
    """


@unittest.skipIf(yaml is None, "PyYAML unavailable")
@unittest.skipUnless(HAS_GNU_SANDBOX, SANDBOX_SKIP_REASON)
class AnchorCacheEdgeCaseSandboxTest(unittest.TestCase):
    """The states a durable cache has to survive without losing an anchor.

    The happy path is the sibling class above; this one is the adversarial half
    — a republished tag, a release with no digest at all, a filesystem that
    cannot hardlink, a cache root that cannot be written, no $HOME, and the
    RAM-backed root the anchors must never land on.
    """

    phases = {}

    @classmethod
    def setUpClass(cls):
        bash = sandbox_bash()
        if bash is None:  # pragma: no cover - only on a host without bash
            raise unittest.SkipTest("no bash that inherits its environment")
        import tempfile

        cls._tmp = tempfile.TemporaryDirectory()
        root = Path(cls._tmp.name)
        cls.root = root

        shutil.copy(PREFETCH, root / "prefetch_patch_anchors.sh")
        shutil.copy(PRECHECK, root / "patch_anchor_schema.py")
        shutil.copy(CONTRACT_TABLES, root / "contract.json")
        write(root / "bin" / "gh", STUB_GH, executable=True)
        write(root / "bin" / "python3", STUB_PYTHON3, executable=True)
        write(root / "bin-strict" / "gh", STUB_GH_NO_CLOBBER, executable=True)
        # A filesystem that cannot hardlink, and one whose `cp` dies part-way
        # out of the cache leaving a truncated destination behind.
        write(root / "bin-noln" / "ln", "#!/bin/sh\nexit 1\n", executable=True)
        write(root / "bin-badcp" / "ln", "#!/bin/sh\nexit 1\n", executable=True)
        write(
            root / "bin-badcp" / "cp",
            """
            #!/bin/sh
            case "$1" in
              */cache/*/seforim.db.zst) head -c 10 "$1" > "$2"; exit 1 ;;
            esac
            exec /usr/bin/cp "$@"
            """,
            executable=True,
        )
        # `stat -f -c %T` is how "is this RAM-backed?" is answered; drive both
        # answers and the "this host has no GNU stat" degradation.
        for name, body in (
            ("tmpfs", 'if [ "$1" = "-f" ]; then echo tmpfs; exit 0; fi'),
            ("ramfs", 'if [ "$1" = "-f" ]; then echo ramfs; exit 0; fi'),
            ("unknown", 'if [ "$1" = "-f" ]; then exit 1; fi'),
        ):
            write(
                root / f"bin-fs-{name}" / "stat",
                f'#!/bin/sh\n{body}\nexec /usr/bin/stat "$@"\n',
                executable=True,
            )

        write(
            root / "publish.sh",
            """
            #!/bin/sh
            # <tag> <filler-byte> [nodigest]
            mkdir -p "$ASSET_ROOT/$1"
            head -c 204800 /dev/zero | tr '\\0' "$2" > "$ASSET_ROOT/$1/seforim.db.zst"
            if [ -n "${3:-}" ]; then
              rm -f "$ASSET_ROOT/$1/digest"
            else
              printf 'sha256:%s\\n' \\
                "$(sha256sum "$ASSET_ROOT/$1/seforim.db.zst" | cut -d' ' -f1)" \\
                > "$ASSET_ROOT/$1/digest"
            fi
            """,
            executable=True,
        )

        for tag in ("rp", "nd", "ln", "ro", "keep"):
            write(root / f"a-{tag}.tsv", f"ANCHOR\t1\t26\tv-{tag}\n")
        write(root / "a-dots.tsv", "ANCHOR\t1\t26\t..\nANCHOR\t2\t26\t.\n")

        driver = r"""
            set -uo pipefail
            export PATH="$PWD/bin:$PATH"
            export ASSET_ROOT="$PWD/assets"
            export DOWNLOAD_LOG="$PWD/downloads.log"
            export STUB_GH_PLAIN="$PWD/bin/gh"
            export GITHUB_REPOSITORY=Otzaria/SeforimLibrary
            export PATCH_ANCHOR_CHECK="$PWD/patch_anchor_schema.py"
            export PATCH_ANCHOR_THIS_SCHEMA="$PWD/db_schema.json"
            export PATCH_ANCHOR_CONTRACT="$PWD/contract.json"
            export PATCH_ANCHOR_CACHE_DIR="$PWD/cache"
            run() { bash ./prefetch_patch_anchors.sh "$@"; }
            phase() { echo "##### $1"; : > "$DOWNLOAD_LOG"; }
            marker() { echo "-- verdict $(head -n1 "$1/.done" 2>/dev/null)"; sed -n '2,$p' "$1/.done" 2>/dev/null; }
            fetched() { echo "-- downloaded: $(tr '\n' ' ' < "$DOWNLOAD_LOG")"; }

            # ── a republished tag must never serve the bytes it replaced ──────
            ./publish.sh v-rp A
            phase REPUBLISH_COLD
            run run a-rp.tsv d-rp1 > /dev/null 2>&1
            echo "-- first byte cached: $(head -c1 cache/v-rp/seforim.db.zst)"
            ./publish.sh v-rp B
            phase REPUBLISH_WITH_NEW_DIGEST
            run run a-rp.tsv d-rp2 2>&1
            marker d-rp2/v-rp; fetched
            echo "-- first byte delivered: $(head -c1 d-rp2/v-rp/seforim.db.zst)"
            echo "-- first byte cached: $(head -c1 cache/v-rp/seforim.db.zst)"

            # ── a release that publishes no digest at all ─────────────────────
            ./publish.sh v-nd C nodigest
            phase NO_PUBLISHED_DIGEST_COLD
            run run a-nd.tsv d-nd1 2>&1
            marker d-nd1/v-nd; fetched
            echo "-- meta records: $(sed -n 's/^sha256=//p' cache/v-nd/.meta | cut -c1-16)"
            phase NO_PUBLISHED_DIGEST_WARM
            run run a-nd.tsv d-nd2 2>&1
            marker d-nd2/v-nd; fetched
            phase NOTHING_TO_VERIFY_AGAINST
            sed -i '/^sha256=/d' cache/v-nd/.meta
            run run a-nd.tsv d-nd3 2>&1
            marker d-nd3/v-nd; fetched

            # ── no hardlinks on this filesystem: cp, never a missing file ─────
            ./publish.sh v-ln E
            phase STORE_WITHOUT_HARDLINKS
            PATH="$PWD/bin-noln:$PATH" run run a-ln.tsv d-ln1 2>&1
            marker d-ln1/v-ln; fetched
            phase REUSE_WITHOUT_HARDLINKS
            PATH="$PWD/bin-noln:$PATH" run run a-ln.tsv d-ln2 2>&1
            marker d-ln2/v-ln; fetched
            echo "-- delivered bytes: $(stat --format='%s' d-ln2/v-ln/seforim.db.zst 2>/dev/null || echo MISSING)"

            # ── and a copy that dies part-way must not block the download ─────
            phase PARTIAL_COPY_FALLS_BACK_TO_THE_DOWNLOAD
            PATH="$PWD/bin-badcp:$PWD/bin-strict:$PATH" run run a-ln.tsv d-ln3 2>&1
            marker d-ln3/v-ln; fetched
            echo "-- delivered bytes: $(stat --format='%s' d-ln3/v-ln/seforim.db.zst 2>/dev/null || echo MISSING)"

            # ── a hit is a hardlink, and deleting the run dir only drops it ───
            phase HARDLINK_SURVIVES_THE_RUN_DIR
            run run a-rp.tsv d-hl > /dev/null 2>&1
            echo "-- same inode: $(stat --format='%i' cache/v-rp/seforim.db.zst) $(stat --format='%i' d-hl/v-rp/seforim.db.zst)"
            rm -rf d-hl d-rp1 d-rp2
            echo "-- cache sha after rm -rf: $(sha256sum cache/v-rp/seforim.db.zst | cut -d' ' -f1)"
            echo "-- release sha:            $(sha256sum assets/v-rp/seforim.db.zst | cut -d' ' -f1)"

            # ── a cache root that cannot be written ───────────────────────────
            ./publish.sh v-ro F
            : > afile
            phase CACHE_ROOT_UNWRITABLE
            PATCH_ANCHOR_CACHE_DIR="$PWD/afile/cache" run run a-ro.tsv d-ro 2>&1
            marker d-ro/v-ro; fetched
            echo "-- delivered bytes: $(stat --format='%s' d-ro/v-ro/seforim.db.zst 2>/dev/null || echo MISSING)"

            # ── the age bound on a run that stored nothing at all ─────────────
            phase AGE_BOUND_ON_AN_ALL_HIT_RUN
            ./publish.sh v-keep K
            run run a-keep.tsv d-k1 > /dev/null 2>&1
            touch -d '2020-01-01' cache/v-nd/.meta cache/v-ln/.meta
            echo "-- before: $(ls cache | tr '\n' ' ')"
            : > "$DOWNLOAD_LOG"   # only the second, all-hit run is measured
            PATCH_ANCHOR_CACHE_MAX_AGE_DAYS=1 run run a-keep.tsv d-k2 2>&1
            fetched
            echo "-- after: $(ls cache | tr '\n' ' ')"

            # ── no $HOME, no $XDG_CACHE_HOME, `set -u` ───────────────────────
            phase NO_HOME_NO_XDG
            env -u HOME -u XDG_CACHE_HOME -u PATCH_ANCHOR_CACHE_DIR \
              bash ./prefetch_patch_anchors.sh cache-report
            echo "-- rc=$?"
            phase XDG_WINS_OVER_HOME
            env -u PATCH_ANCHOR_CACHE_DIR HOME="$PWD/home" XDG_CACHE_HOME="$PWD/xdg" \
              bash ./prefetch_patch_anchors.sh cache-report
            phase HOME_ONLY
            env -u XDG_CACHE_HOME -u PATCH_ANCHOR_CACHE_DIR HOME="$PWD/home" \
              bash ./prefetch_patch_anchors.sh cache-report

            # ── never the tmpfs, whatever the path looks like ────────────────
            phase FS_TYPE_TMPFS
            PATH="$PWD/bin-fs-tmpfs:$PATH" run cache-report
            phase FS_TYPE_RAMFS
            PATH="$PWD/bin-fs-ramfs:$PATH" run cache-report
            phase FS_TYPE_UNKNOWN
            PATH="$PWD/bin-fs-unknown:$PATH" run cache-report

            # ── a tag that is a path traversal never reaches the cache ───────
            phase UNSAFE_TAGS
            run run a-dots.tsv d-dots 2>&1
            echo "-- cache root: $(ls cache | tr '\n' ' ')"
            echo "-- siblings of the cache root survive: $(ls | tr '\n' ' ')"
            """
        result = subprocess.run(
            [bash, "-c", textwrap.dedent(driver)],
            cwd=root,
            env=dict(os.environ, REAL_PYTHON=sys.executable),
            capture_output=True,
            text=True,
        )
        cls.result = result
        blocks = re.split(r"^##### ", result.stdout, flags=re.M)[1:]
        cls.phases = {}
        for block in blocks:
            name, _, rest = block.partition("\n")
            cls.phases[name.strip()] = rest

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def phase(self, name):
        self.assertIn(name, self.phases, self.result.stdout + self.result.stderr)
        return self.phases[name]

    def test_a_republished_tag_is_never_served_from_the_old_entry(self):
        # The cache is keyed by tag, so the ONLY thing standing between a
        # re-cut release and 1.3 GB of the bytes it replaced is the digest.
        self.assertIn("-- first byte cached: A", self.phase("REPUBLISH_COLD"))
        again = self.phase("REPUBLISH_WITH_NEW_DIGEST")
        self.assertIn("::warning::patch-fan anchor cache: digest mismatch for v-rp", again)
        self.assertIn("— evicting and downloading it again", again)
        self.assertIn("v-rp/seforim.db.zst", again.split("-- downloaded:")[1])
        self.assertIn("-- first byte delivered: B", again)
        self.assertIn("-- first byte cached: B", again)
        self.assertIn("-- verdict ok", again)

    def test_a_release_without_a_published_digest_still_verifies_before_reuse(self):
        # GitHub computes asset digests asynchronously and old releases carry
        # none. Size is then the only published check, so the entry records the
        # sha it verified and every later reuse is re-hashed against THAT —
        # never handed over unverified.
        cold = self.phase("NO_PUBLISHED_DIGEST_COLD")
        self.assertIn("anchor v-nd (offset 1): seforim.db.zst 204800 bytes\n", cold)
        self.assertIn("-- verdict ok", cold)
        self.assertRegex(cold, r"-- meta records: [0-9a-f]{16}")
        warm = self.phase("NO_PUBLISHED_DIGEST_WARM")
        self.assertIn("reused v-nd from cache (sha256 ok)", warm)
        self.assertEqual(warm.split("-- downloaded:")[1].strip(), "")
        # …and an entry with nothing to check against is evicted, not trusted.
        nothing = self.phase("NOTHING_TO_VERIFY_AGAINST")
        self.assertIn(
            "::warning::patch-fan anchor cache: nothing to verify v-nd against"
            " (the release publishes no digest and the entry records none)"
            " — evicting and downloading it again",
            nothing,
        )
        self.assertIn("v-nd/seforim.db.zst", nothing.split("-- downloaded:")[1])

    def test_the_cache_never_loses_an_anchor_it_cannot_hardlink_or_store(self):
        # Every degraded filesystem must cost at most the download it would
        # have saved — never the anchor itself, and never the job.
        for name in ("STORE_WITHOUT_HARDLINKS", "REUSE_WITHOUT_HARDLINKS"):
            self.assertIn("-- verdict ok", self.phase(name), name)
        self.assertIn("-- delivered bytes: 204800", self.phase("REUSE_WITHOUT_HARDLINKS"))
        # A `cp` that dies part-way used to leave a truncated file exactly where
        # the fallback `gh release download --dir` writes; gh refuses to clobber,
        # so the anchor was lost to the cache's own failure.
        partial = self.phase("PARTIAL_COPY_FALLS_BACK_TO_THE_DOWNLOAD")
        self.assertIn("-- verdict ok", partial)
        self.assertIn("-- delivered bytes: 204800", partial)
        self.assertIn("v-ln/seforim.db.zst", partial.split("-- downloaded:")[1])
        # A cache root that cannot be written warns and downloads anyway.
        unwritable = self.phase("CACHE_ROOT_UNWRITABLE")
        self.assertRegex(
            unwritable,
            r"::warning::patch-fan anchor cache: cannot write \S+ —"
            r" v-ro will be downloaded again next attempt",
        )
        self.assertIn("-- verdict ok", unwritable)
        self.assertIn("-- delivered bytes: 204800", unwritable)
        self.assertEqual(self.result.returncode, 0, self.result.stderr[-2000:])

    def test_deleting_the_run_dir_only_drops_a_link_into_the_cache(self):
        # "Clean run-scoped disk leftovers" runs `rm -rf … prefetch` on every
        # attempt. If that took the cached bytes with it there would be no
        # cache — the whole point is surviving run 34021998271's cleanup.
        hardlink = self.phase("HARDLINK_SURVIVES_THE_RUN_DIR")
        inodes = hardlink.split("-- same inode:")[1].splitlines()[0].split()
        self.assertEqual(inodes[0], inodes[1], hardlink)
        after, release = (
            line.split(":")[1].strip()
            for line in hardlink.splitlines()
            if line.startswith(("-- cache sha after rm -rf:", "-- release sha:"))
        )
        self.assertEqual(after, release)

    def test_the_age_bound_applies_to_a_run_that_downloaded_nothing(self):
        # cache_store prunes after every write, so before the second call at the
        # end of run_prefetch an all-hit run never applied the age limit at all.
        aged = self.phase("AGE_BOUND_ON_AN_ALL_HIT_RUN")
        self.assertEqual(aged.split("-- downloaded:")[1].splitlines()[0].strip(), "")
        self.assertIn("patch-fan anchor cache: evicting v-nd", aged)
        self.assertIn("patch-fan anchor cache: evicting v-ln", aged)
        self.assertEqual(
            aged.split("-- after:")[1].split(), ["v-keep", "v-rp"]
        )

    def test_the_cache_root_degrades_safely_when_the_environment_does_not_have_one(self):
        # `set -u` is on; neither variable existing must disable the cache, not
        # crash the prefetch.
        none = self.phase("NO_HOME_NO_XDG")
        self.assertIn(
            "patch-fan anchor cache: disabled — every anchor is downloaded again"
            " on every attempt",
            none,
        )
        self.assertIn("-- rc=0", none)
        self.assertNotIn("unbound variable", self.result.stderr)
        # The image embedder's root wins the same way: XDG first, then $HOME.
        self.assertIn("/xdg/seforimlibrary/patch-anchors", self.phase("XDG_WINS_OVER_HOME"))
        self.assertIn("/home/.cache/seforimlibrary/patch-anchors", self.phase("HOME_ONLY"))

    def test_a_ram_backed_root_is_refused_by_filesystem_type_not_only_by_name(self):
        # 8 anchors is ~11 GB. On the 16 GiB tmpfs build/ that is the
        # generator's RAM, however the path was spelled.
        for name in ("FS_TYPE_TMPFS", "FS_TYPE_RAMFS"):
            phase = self.phase(name)
            self.assertIn(
                "is on a RAM-backed filesystem — caching disabled for this run"
                " (anchors are never held in RAM)",
                phase,
                name,
            )
            self.assertIn("patch-fan anchor cache: disabled", phase)
        # …and a host without GNU `stat -f` must not lose its cache to the guard.
        self.assertIn(
            "kept across runs on purpose", self.phase("FS_TYPE_UNKNOWN")
        )

    def test_a_tag_that_is_a_path_traversal_never_reaches_the_cache(self):
        # The tag is a path component of the durable cache AND the argument to
        # `rm -rf "$CACHE_DIR/$tag"`; `..` would point that at the shared
        # ~/.cache/seforimlibrary root the image embedder also lives in.
        unsafe = self.phase("UNSAFE_TAGS")
        self.assertIn("refusing to prefetch unsafe tag '..' (offset 1)", unsafe)
        self.assertIn("refusing to prefetch unsafe tag '.' (offset 2)", unsafe)
        self.assertIn("v-keep", unsafe.split("-- cache root:")[1].splitlines()[0])
        self.assertIn("cache", unsafe.split("-- siblings of the cache root survive:")[1])


@unittest.skipIf(yaml is None, "PyYAML unavailable")
@unittest.skipUnless(HAS_GNU_SANDBOX, SANDBOX_SKIP_REASON)
class PatchFanDriverSandboxTest(unittest.TestCase):
    """Drive the real 'Produce + verify patch fan' step body with stubs."""

    @classmethod
    def setUpClass(cls):
        cls.bash = sandbox_bash()
        if cls.bash is None:  # pragma: no cover
            raise unittest.SkipTest("no bash that inherits its environment")
        doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
        body = None
        for step in doc["jobs"]["build-and-release"]["steps"]:
            if step.get("name") == "Produce + verify patch fan":
                body = step["run"]
        assert body, "the patch fan step must exist"
        # The only GitHub expression in the body; everything else it needs is
        # step env, which the sandbox sets.
        cls.body = body.replace(
            "${{ steps.discover.outputs.db_version }}", "27"
        ).replace("\r\n", "\n")

    def run_fan(self, publish_v25_asset=True):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "assets" / "v26-x").mkdir(parents=True)
            (root / "assets" / "v25-x").mkdir(parents=True)
            control = root / ".pipeline-control" / ".github" / "scripts"
            control.mkdir(parents=True)
            # patch_fan_lib.sh carries read_schema, produce_anchor, the
            # heartbeat and drain_batch: the step body sources it, so the
            # sandbox has to stage it exactly like the runner's
            # pipeline-control checkout does.
            for name in ("patch_anchor_schema.py", "patch_fan_anchors.sh",
                         "patch_fan_lib.sh", "prefetch_patch_anchors.sh"):
                shutil.copy(SCRIPTS / name, control / name)
            shutil.copy(
                CONTRACT_TABLES,
                _mkparents(root / "generator" / "common" / "src" / "jvmTest"
                           / "resources" / "patch_tables_contract.json"),
            )
            # The only sibling helper the sandbox fakes: the early buildstate
            # upload is a different step's contract (and needs real release
            # credentials), not part of the fan's own narration.
            write(control / "upload_early_release_assets.sh",
                  "#!/bin/sh\nexit 0\n", executable=True)
            _mkparents(root / "build" / "seforim.db").write_bytes(b"db" * 512)
            (root / "build" / "seforim.db.buildstate.zst").write_bytes(b"bs" * 512)
            write(root / "prior-versions.tsv", "26\tv26-x\n25\tv25-x\n")
            (root / "assets" / "v26-x" / "seforim.db.zst").write_bytes(b"v26" * 4096)
            if publish_v25_asset:
                (root / "assets" / "v25-x" / "seforim.db.zst").write_bytes(b"v25" * 4096)
            write(
                _mkparents(root / "generator" / "common" / "build"
                           / "patch-pipeline-launcher.properties"),
                "mainClass=io.example.PatchPipelineCli\n"
                "jvmArgs=-Xmx1g\nclasspath=/stub/cp\njavaVersion=25\n",
            )
            write(root / "bin" / "gh", STUB_GH, executable=True)
            write(root / "bin" / "python3", STUB_PYTHON3, executable=True)
            write(root / "bin" / "sqlite3", "#!/bin/sh\necho 5\n", executable=True)
            write(
                root / "bin" / "unzstd",
                """
                #!/bin/sh
                [ "$1" = "-c" ] || exit 1
                cat "$2"
                """,
                executable=True,
            )
            # Stands in for PatchPipelineCli: same exit contract, same two log
            # lines the driver folds into its per-anchor end line.
            write(
                root / "bin" / "java",
                r"""
                #!/bin/sh
                case "$1" in
                  -XshowSettings:properties)
                    echo "    java.specification.version = 25" >&2; exit 0 ;;
                esac
                out=""; from=""
                for a in "$@"; do
                  case "$a" in
                    -Dout=*)         out=${a#-Dout=} ;;
                    -DfromVersion=*) from=${a#-DfromVersion=} ;;
                  esac
                done
                echo "Info: (PatchPipelineCli) Producing patch v${from} -> v27"
                secs=${STUB_PRODUCE_SECONDS:-4}
                if [ "$from" = "${STUB_FAST_VERSION:-}" ]; then
                  secs=1
                  echo "Info: (PatchPipelineCli) Table 'line_dh': prev lacks [dhDisplay] - emitting ADD COLUMN migration(s) and shipping a full snapshot (synthesised NOT NULL default)"
                fi
                sleep "$secs"
                : > "$out"
                head -c 200000 /dev/zero | tr '\0' 'z' > "$out.zst"
                echo "Info: (PatchPipelineCli) Patch apply verified: target hash matches new (deadbeef)"
                exit 0
                """,
                executable=True,
            )
            (root / "runner-temp").mkdir()
            write(root / "step.sh", self.body)
            env = dict(
                os.environ,
                PATH=str(root / "bin") + os.pathsep + os.environ["PATH"],
                REAL_PYTHON=sys.executable,
                ASSET_ROOT=str(root / "assets"),
                RUNNER_TEMP=str(root / "runner-temp"),
                GITHUB_RUN_ID="99",
                GITHUB_RUN_ATTEMPT="1",
                GITHUB_REPOSITORY="Otzaria/SeforimLibrary",
                GITHUB_WORKSPACE=str(root),
                PATCH_OFFSETS="1 2",
                ZSTD_LEVEL="19",
                PATCH_FAN_PARALLELISM="2",
                PATCH_FAN_HEARTBEAT_SECONDS="1",
                STUB_PRODUCE_SECONDS="4",
                STUB_FAST_VERSION="26",
                AUTOMATIC_TOKEN="stub",
                CROSS_REPO_TOKEN="stub",
                RELEASE_TAG="v27-sandbox",
                SOURCE_COMMIT="deadbeef",
            )
            return subprocess.run(
                [self.bash, "step.sh"], cwd=root, env=env,
                capture_output=True, text=True,
            )

    def test_the_fan_narrates_every_anchor_from_start_to_end(self):
        result = self.run_fan()
        out = result.stdout
        self.assertEqual(result.returncode, 0, out + result.stderr[-2000:])

        # Numbered start lines: how much of the fan is left, without counting.
        self.assertIn(
            "anchor 1/2 v26-x (offset 1, v26 → v27): starting;", out
        )
        self.assertIn(
            "anchor 2/2 v25-x (offset 2, v25 → v27): starting;", out
        )
        # End line per anchor: elapsed, what it shipped, the verify verdict —
        # and the producer's standing "prev lacks […] full snapshot" condition
        # folded in, because that is what explains a 454 MB offset-1 patch.
        self.assertRegex(
            out,
            r"anchor 1/2 v26-x \(offset 1, v26 → v27\) done in \d+s: "
            r"patch-v26-v27\.db\.zst \S+, verify=ok, full-snapshot columns: "
            r"line_dh\[dhDisplay\]",
        )
        self.assertRegex(
            out,
            re.compile(
                r"anchor 2/2 v25-x \(offset 2, v25 → v27\) done in \d+s: "
                r"patch-v25-v27\.db\.zst \S+, verify=ok$",
                re.M,
            ),
        )
        # …and no anchor without that condition claims it.
        self.assertEqual(out.count("full-snapshot columns:"), 1)

        # The heartbeat: the step is no longer dark while a producer runs.
        beats = re.findall(
            r"^still producing patch for (.*) \(elapsed \d+m\d+s\)$", out, re.M
        )
        self.assertTrue(beats, out)
        # v26 finishes first; from then on the heartbeat must not claim it.
        self.assertTrue(
            any("v25-x" in b and "v26-x" not in b for b in beats),
            f"the heartbeat must drop an anchor once it lands: {beats}",
        )
        # And it stops when the batch does.
        self.assertNotIn(
            "still producing", out.split("=== Final patch artefacts ===")[1]
        )

    def test_a_missing_release_asset_is_named_in_the_failure(self):
        result = self.run_fan(publish_v25_asset=False)
        out = result.stdout
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "::error::anchor v25 (v25-x): could not download seforim.db.zst from"
            " that release (no assets match the file pattern seforim.db.zst)"
            " — no patch can be produced against this anchor",
            out,
        )
        # The driver's own line names the anchor, the elapsed time and the code.
        self.assertRegex(
            out,
            r"::error::anchor 2/2 v25-x \(offset 2, v25 → v27\) failed after \d+s"
            r" with exit code 1",
        )
        # The healthy sibling still ran to completion and said so.
        self.assertRegex(
            out, r"anchor 1/2 v26-x \(offset 1, v26 → v27\) done in \d+s:"
        )


def _mkparents(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
