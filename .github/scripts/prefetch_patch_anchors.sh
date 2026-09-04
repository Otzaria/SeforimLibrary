#!/usr/bin/env bash
# Fetch the patch fan's anchor DBs in parallel while the job is busy elsewhere.
#
# The fan reconstitutes every anchor from that release's 1.3 GB seforim.db.zst.
# Serially, in the fan itself, that cost run 33865604251 110-135 s per anchor
# (~10 min of the 2015 s step) with the CPU idle. The anchor tags are known long
# before: prior-versions.tsv exists before "Generate Seforim Database", which
# then burns ~36 minutes of CPU without touching the network. This script runs
# in the background across that window and leaves each anchor's verified .zst in
# <dest>/<tag>/seforim.db.zst on the workspace disk (never on the 16 GiB tmpfs
# build/, which has no room for 6.5 GB of anchors).
#
#   prefetch_patch_anchors.sh start <anchors-tsv> <dest-dir>
#   prefetch_patch_anchors.sh run   <anchors-tsv> <dest-dir>   (internal)
#   prefetch_patch_anchors.sh abort <dest-dir>
#
# <anchors-tsv> is the output of patch_fan_anchors.sh — the SAME derivation the
# fan walks, so the two can never disagree about which tag an offset means.
#
# Per anchor it writes <dest>/<tag>/.done whose FIRST line is the verdict the fan
# reads and whose remaining lines are the human timing report:
#
#   ok           <dest>/<tag>/seforim.db.zst exists and matches the release
#                asset's published size (and digest, when GitHub has computed it)
#   unpatchable  the shared pre-download patchability check rejected the anchor,
#                so nothing was downloaded
#   failed       something went wrong; the fan falls back to its serial download
#
# Nothing here can fail the job: `start` only forks, and every fan-side use of a
# marker degrades to the unchanged serial `gh release download` path.
set -uo pipefail

self="${BASH_SOURCE[0]}"
mode="${1:-}"

# The pre-download patchability check, run with exactly the arguments the fan
# uses (task C). Note that build/db_schema.json does NOT exist yet while this
# runs — it is dumped after the DB is generated — so patch_anchor_schema.py can
# only reach its first, disk-free source here: the documented list of db_versions
# proven unpatchable (LEGACY_UNPATCHABLE_DB_VERSIONS; that is exactly the v10
# anchor that cost run 33865604251 ~140 s). Everything else degrades to PROCEED,
# and the fan re-runs the very same check later against the real schema dump —
# so an anchor this prefetch fetched can still be skipped there, having cost
# nothing but idle bandwidth.
CHECK_SCRIPT="${PATCH_ANCHOR_CHECK:-.pipeline-control/.github/scripts/patch_anchor_schema.py}"
THIS_SCHEMA="${PATCH_ANCHOR_THIS_SCHEMA:-build/db_schema.json}"
# From the PAYLOAD checkout, like the fan's own invocation: a comparison wider
# than the producer's own table list would skip anchors it would have patched.
CONTRACT_TABLES="${PATCH_ANCHOR_CONTRACT:-generator/common/src/jvmTest/resources/patch_tables_contract.json}"
# 5 parallel `gh release download` is well inside GitHub's rate/concurrency
# budget and saturates the runner's 8-22 MB/s downlink.
PARALLEL="${PREFETCH_PARALLEL:-5}"

pid_file_for() { printf '%s/.pid' "$1"; }

kill_group() {
  local dest="$1" pid
  pid=$(cat "$(pid_file_for "$dest")" 2>/dev/null || true)
  [ -n "${pid:-}" ] || return 0
  # This runner lives for weeks and pids get reused: only ever signal a process
  # that is still this very script.
  ps -o args= -p "$pid" 2>/dev/null | grep -q 'prefetch_patch_anchors\.sh' || return 0
  # It drives `gh` children and runs in its own session (setsid), so the whole
  # group goes at once and nothing keeps competing for the downlink with the
  # serial fallback.
  kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
}

verify_asset() {  # <file> <tag>  — size always, digest when GitHub published one
  local file="$1" tag="$2" expected expected_size expected_digest actual_size actual_digest
  expected=$(gh api "repos/$GITHUB_REPOSITORY/releases/tags/$tag" \
    --jq '.assets[] | select(.name=="seforim.db.zst") | [(.size|tostring), (.digest // "")] | @tsv') || return 1
  [ -n "$expected" ] || { echo "release $tag publishes no seforim.db.zst asset"; return 1; }
  IFS=$'\t' read -r expected_size expected_digest <<<"$expected"
  actual_size=$(stat --format='%s' "$file") || return 1
  [ "$actual_size" = "$expected_size" ] || {
    echo "size mismatch for $tag: got $actual_size, release says $expected_size"
    return 1
  }
  if [ -n "${expected_digest:-}" ]; then
    actual_digest="sha256:$(sha256sum "$file" | cut -d' ' -f1)"
    [ "$actual_digest" = "$expected_digest" ] || {
      echo "digest mismatch for $tag: got $actual_digest, release says $expected_digest"
      return 1
    }
  fi
}

fetch_one() {  # <target_version> <tag> <dest-dir>
  local version="$1" tag="$2" dest="$3" dir="$3/$2" verdict reason
  local t_start t_checked t_downloaded t_verified

  mkdir -p "$dir"
  t_start=$(date +%s)
  rm -rf "$dir/meta"
  mkdir -p "$dir/meta"
  # A few KB, exactly like the fan's own pre-check fetch; absence is not fatal.
  gh release download "$tag" --pattern 'build_provenance.json' --dir "$dir/meta" || true
  verdict=$(python3 "$CHECK_SCRIPT" check \
    --this-schema "$THIS_SCHEMA" \
    --anchor-version "$version" \
    --anchor-provenance "$dir/meta/build_provenance.json" \
    --contract-tables "$CONTRACT_TABLES") \
    || verdict="PROCEED pre-check did not run — deferring to the producer"
  rm -rf "$dir/meta"
  t_checked=$(date +%s)
  if [ "${verdict%% *}" = UNPATCHABLE ]; then
    printf 'unpatchable\nprefetch anchor v%s (%s): %s — not downloaded\n' \
      "$version" "$tag" "$verdict" > "$dir/.done"
    return 0
  fi

  rm -f "$dir/seforim.db.zst"
  if ! gh release download "$tag" --pattern 'seforim.db.zst' --dir "$dir"; then
    printf 'failed\nprefetch anchor v%s (%s): download failed\n' "$version" "$tag" > "$dir/.done"
    rm -f "$dir/seforim.db.zst"
    return 0
  fi
  t_downloaded=$(date +%s)
  if ! reason=$(verify_asset "$dir/seforim.db.zst" "$tag"); then
    printf 'failed\nprefetch anchor v%s (%s): %s\n' "$version" "$tag" "${reason:-verification failed}" \
      > "$dir/.done"
    rm -f "$dir/seforim.db.zst"
    return 0
  fi
  t_verified=$(date +%s)
  printf 'ok\nprefetch anchor v%s (%s) timings: precheck=%ss download=%ss verify=%ss total=%ss\n' \
    "$version" "$tag" \
    "$((t_checked - t_start))" "$((t_downloaded - t_checked))" \
    "$((t_verified - t_downloaded))" "$((t_verified - t_start))" > "$dir/.done"
}

run_prefetch() {  # <anchors-tsv> <dest-dir>
  local anchors="$1" dest="$2" status offset version tag running=0
  echo "$$" > "$(pid_file_for "$dest")"
  while IFS=$'\t' read -r status offset version tag; do
    [ "$status" = ANCHOR ] || continue
    case "$tag" in
      ''|*[!A-Za-z0-9._-]*)
        echo "refusing to prefetch unsafe tag '$tag' (offset $offset)"
        continue
        ;;
    esac
    fetch_one "$version" "$tag" "$dest" &
    running=$((running + 1))
    if [ "$running" -ge "$PARALLEL" ]; then
      wait -n 2>/dev/null || true
      running=$((running - 1))
    fi
  done < "$anchors"
  wait
  echo "prefetch finished"
}

case "$mode" in
  start)
    anchors="${2:-}"; dest="${3:-}"
    [ -n "$anchors" ] && [ -n "$dest" ] || { echo "usage: ${0##*/} start <anchors-tsv> <dest-dir>" >&2; exit 2; }
    mkdir -p "$dest"
    rm -f "$dest/.pid" "$dest/.started"
    # setsid: its own session, so `abort` and the run-scoped cleanup can take
    # down the whole group (script plus every `gh` child) in one signal.
    setsid nohup bash "$self" run "$anchors" "$dest" > "$dest/.log" 2>&1 &
    : > "$dest/.started"
    echo "patch-fan anchor prefetch started in the background (dest=$dest, parallel=$PARALLEL)"
    ;;
  run)
    anchors="${2:-}"; dest="${3:-}"
    [ -n "$anchors" ] && [ -n "$dest" ] || exit 2
    run_prefetch "$anchors" "$dest"
    ;;
  abort)
    dest="${2:-}"
    [ -n "$dest" ] || { echo "usage: ${0##*/} abort <dest-dir>" >&2; exit 2; }
    kill_group "$dest"
    echo "patch-fan anchor prefetch aborted"
    ;;
  *)
    echo "usage: ${0##*/} start|run|abort ..." >&2
    exit 2
    ;;
esac
exit 0
