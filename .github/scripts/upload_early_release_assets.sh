#!/usr/bin/env bash
# Push release assets that are already byte-final up the ~2.1 MB/s uplink while
# the job is busy doing something else.
#
# Run 33865604251 spent 1756 s in "Create draft, verify every uploaded asset,
# then publish", almost all of it serial upload — while "Run LinkerToOtzaria
# relink on this snapshot (and wait)" had spent 1119 s doing nothing but polling
# and the patch fan another 2015 s of pure CPU. Anything final by then can ride
# along in those windows for free.
#
#   upload_early_release_assets.sh start <label> <asset-path>...
#   upload_early_release_assets.sh wait  <label> [timeout-seconds]
#   upload_early_release_assets.sh abort <label>
#   upload_early_release_assets.sh run   <label> <asset-path>...   (internal)
#
# `start` creates/adopts this build's draft release (the shared release_draft.sh
# machinery the publish step itself uses) and returns immediately. `wait` blocks
# until the upload is done, echoes its log and ALWAYS succeeds: a failed early
# upload is a lost optimisation, never a failed build — the publish step uploads
# whatever is missing exactly as it always did and re-verifies every asset by
# name+size+digest either way.
set -uo pipefail

self="${BASH_SOURCE[0]}"
mode="${1:-}"
label="${2:-}"
if [ -z "$mode" ] || [ -z "$label" ]; then
  echo "usage: ${0##*/} start|wait|run <label> ..." >&2
  exit 2
fi
shift 2

state="${RUNNER_TEMP:-/tmp}/early-upload-$label"
pid_file="$state.pid"
done_file="$state.done"
log_file="$state.log"

# True while the recorded pid is still THIS script (pids get reused on a runner
# that lives for weeks, so identity is checked, never just existence).
process_alive() {
  local pid
  pid=$(cat "$pid_file" 2>/dev/null || true)
  [ -n "${pid:-}" ] || return 1
  ps -o args= -p "$pid" 2>/dev/null | grep -q 'upload_early_release_assets\.sh'
}

kill_group() {
  local pid
  pid=$(cat "$pid_file" 2>/dev/null || true)
  [ -n "${pid:-}" ] || return 0
  # This runner lives for weeks and pids get reused: only ever signal a process
  # that is still this very script.
  ps -o args= -p "$pid" 2>/dev/null | grep -q 'upload_early_release_assets\.sh' || return 0
  kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
}

run_uploads() {
  local path started elapsed
  # shellcheck source=release_draft.sh
  source "$(dirname "$self")/release_draft.sh" || return 1
  use_token "$RELEASE_TOKEN_KIND" || {
    echo "release credential preflight result is missing or invalid"
    return 1
  }
  ensure_draft || return 1
  RELEASE_ID="$(resolve_release_id)" || {
    echo "could not resolve the exact draft release id"
    return 1
  }
  for path in "$@"; do
    [ -f "$path" ] || { echo "asset $path does not exist — refusing to guess"; return 1; }
    started=$(date +%s)
    upload_asset "$path" || return 1
    elapsed=$(( $(date +%s) - started ))
    echo "early upload ${path##*/}: $(stat --format='%s' "$path") bytes in ${elapsed}s"
  done
}

case "$mode" in
  start)
    [ "$#" -ge 1 ] || { echo "usage: ${0##*/} start <label> <asset-path>..." >&2; exit 2; }
    rm -f "$pid_file" "$done_file" "$log_file"
    # setsid: its own session, so a cancelled job can take down the whole group
    # (this script plus the `gh` upload it drives) with one signal.
    setsid nohup bash "$self" run "$label" "$@" > "$log_file" 2>&1 &
    echo "early release-asset upload '$label' started in the background for: $*"
    ;;
  run)
    echo "$$" > "$pid_file"
    if run_uploads "$@"; then
      echo ok > "$done_file"
    else
      echo failed > "$done_file"
    fi
    ;;
  wait)
    timeout="${1:-3600}"
    waited=0
    if [ ! -f "$pid_file" ] && [ ! -f "$done_file" ]; then
      echo "no early release-asset upload '$label' was started — nothing to wait for"
      exit 0
    fi
    while [ ! -f "$done_file" ]; do
      if [ "$waited" -ge "$timeout" ]; then
        echo "::warning::early release-asset upload '$label' did not finish within ${timeout}s — abandoning it; the publish step uploads what is missing"
        kill_group
        break
      fi
      sleep 5
      waited=$((waited + 5))
      # The uploader writes its verdict on every exit path it controls, but it
      # can also be killed outright (the OOM killer on this 32 GB VM, an abort,
      # a reboot). Without this the wait would then block for the WHOLE timeout
      # — an hour added to a step that has nothing left to wait for. Re-check
      # the marker once after the pid is gone, so a child that finished between
      # the two probes is still read as finished.
      if ! process_alive; then
        sleep 2
        [ -f "$done_file" ] || {
          echo "::warning::early release-asset upload '$label' died without a verdict after ${waited}s; the publish step will upload these assets as usual"
          break
        }
      fi
    done
    [ -f "$log_file" ] && cat "$log_file"
    status=$(head -n1 "$done_file" 2>/dev/null || true)
    case "${status:-}" in
      ok) echo "early release-asset upload '$label' completed (waited ${waited}s in this step)" ;;
      *) echo "::warning::early release-asset upload '$label' ended '${status:-unfinished}'; the publish step will upload these assets as usual" ;;
    esac
    ;;
  abort)
    kill_group
    echo "early release-asset upload '$label' aborted (if it was still running)"
    ;;
  *)
    echo "usage: ${0##*/} start|wait|run|abort <label> ..." >&2
    exit 2
    ;;
esac
exit 0
