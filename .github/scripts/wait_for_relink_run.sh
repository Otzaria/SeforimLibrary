#!/usr/bin/env bash
# Wait for the one dispatched LinkerToOtzaria relink run and report how it
# ended: a low-frequency poll, a heartbeat line every 10 minutes, and one
# wording per terminal state.
#
# WHY THIS EXISTS
# ---------------
# GitHub Actions refuses to LOAD a workflow whose single `run:` template is
# longer than 21,000 characters; run 34195296928 died on the sibling patch-fan
# step with "Exceeded max expression length 21000" and never started a job.
# "Run LinkerToOtzaria relink on this snapshot (and wait)" stood at 21,230.
# This is the cohesive block that came out of it, unchanged.
#
# SHELL SEMANTICS
# ---------------
# That step carries no `shell:` key — GitHub runs it as `bash -e {0}` — and
# `set -euo pipefail` on its first line. The same three options are set below,
# so every `exit 1` here fails the step exactly as it did inline, and nothing
# after the loop in the step body reads a variable this sets.
#
# Environment: RELINK_RUN_ID, RELINK_RUN_URL and RELINK_REQUEST_ID are shell
# locals in the step and are passed explicitly by its invocation; GH_TOKEN,
# SERIAL_LINKER_TARGET, RELINK_WAIT_CAP_MIN, RUNNER_TEMP, GITHUB_RUN_ID and
# GITHUB_RUN_ATTEMPT are already that step's environment.
set -euo pipefail
RUN="$RELINK_RUN_ID"
RUN_URL="$RELINK_RUN_URL"
# One jobs listing per call, and never fatal: the job/step names are decoration
# for the log, so an API hiccup must not fail an otherwise-healthy wait — nor
# abort the terminal message we are in the middle of printing.
child_jobs() {
  gh api "repos/Otzaria/LinkerToOtzaria/actions/runs/$RUN/jobs?per_page=100" \
    --jq "$1" 2>/dev/null || true
}
child_where() {
  local where
  where="$(child_jobs '.jobs[] | select(.status != "completed") | .name + " · " + ([.steps[]? | select(.status == "in_progress") | .name] | join(" + "))' | paste -sd ';' -)"
  printf '%s' "${where:-no running job reported}"
}
hhmm() { printf '%02d:%02d' $(( $1 / 3600 )) $(( $1 % 3600 / 60 )); }
# ── What the child may legally cost ─────────────────────────────────────────
# The child is a CHAIN of jobs, not one job, and the two questions this script
# asks about it have two different answers:
#
#   child_job_max_min     the longest SINGLE job on this path — the only number
#                         a `cancelled` conclusion can be compared against, to
#                         tell a job that hit its own `timeout-minutes:` from an
#                         operator/API cancel;
#   child_path_budget_min the SUM of the jobs on this path — the wall clock a
#                         healthy run may legally take, and therefore the only
#                         number the wait cap may be derived from. Waiting only
#                         `child_job_max_min` cancelled a healthy kaggle run at
#                         150 minutes, 450 minutes inside its own contract.
#
# One table, mirroring the `timeout-minutes:` of the corresponding jobs in
# LinkerToOtzaria .github/workflows/relink.yml — update together:
#
#   relink  (relink.yml:264)   kaggle 90 · local 1440 · server 480 (serial mode,
#                              i.e. library_run_id != '', which is how this
#                              parent always dispatches it)
#   resolve (relink.yml:1287)  480 — gated `inputs.target == 'kaggle'`, so it
#                              exists on the kaggle path ONLY
#   publish (relink.yml:1607)  30 — every path
#
# path → jobs:  kaggle = relink + resolve + publish (600)
#               server = relink + publish (510)
#               local  = relink + publish (1470)
# Inter-job queue time (publish sits in the `linker-release-publisher`
# concurrency group with `queue: max`, and kaggle's resolve waits on the
# cross-repo host lease) is covered by the hour of grace added below, not here.
child_job_max_min=1440
child_path_budget_min=$((1440 + 30))
case "$SERIAL_LINKER_TARGET" in
  kaggle) child_job_max_min=480; child_path_budget_min=$((90 + 480 + 30)) ;;
  server) child_job_max_min=480; child_path_budget_min=$((480 + 30)) ;;
esac
# Hard cap on the whole wait.  Without one the only thing that ends this loop is
# the PARENT's `timeout-minutes`, which ends it by killing the job in silence,
# hours after the last heartbeat and with no cause anywhere in the log.  Once the
# child is past its OWN ceiling and GitHub still has not reported it terminal,
# nothing good is coming: fail here instead, naming the run and printing the same
# recovery recipe every terminal branch prints.  The job's `always()` cleanup then
# cancels that child exactly as it would on any other failure — the difference is
# that it is now provably out of contract, and the operator gets a message
# instead of a job killed mid-wait.
# The cap arrives as env from the step (RELINK_WAIT_CAP_MIN), so the parent's
# budget stays next to the parent's own `timeout-minutes:`.  Take the TIGHTER of
# it and this path's whole budget plus an hour of grace: the step's value is
# written for the default target, so the kaggle (660) and server (570) fallbacks
# must not be held to it, and a missing or malformed value still leaves a real
# cap rather than an unbounded wait.
wait_cap_min=$((child_path_budget_min + 60))
if [[ "${RELINK_WAIT_CAP_MIN:-}" =~ ^[1-9][0-9]*$ ]] && [ "$RELINK_WAIT_CAP_MIN" -lt "$wait_cap_min" ]; then
  wait_cap_min="$RELINK_WAIT_CAP_MIN"
fi
# One wording for every terminal failure, matching what the reuse path at the
# top of this step accepts: an exactly correlated recovery child, consumed by
# re-running this build with relink_recovery_run_id.
recovery_hint() {
  echo "recover: dispatch relink-recovery request=$RELINK_REQUEST_ID parent=${GITHUB_RUN_ID}:${GITHUB_RUN_ATTEMPT} on Otzaria/LinkerToOtzaria, then re-run this build with relink_recovery_run_id=<that run id>"
}
# Low-frequency resilient wait: `gh run watch` polls every 3s, which can
# exhaust the shared 5k/hr API quota on an hours-long relink; transient
# API errors must not kill an otherwise-healthy build. A run stuck in
# `queued` past 30 min means the Kaggle session never booted (quota,
# dataset, image drift) — fail loudly instead of burning the job timeout.
# The poll stays at 60s but the LOG gets one line per 10 min: run 33991433362
# spent 8h02m here without printing a character, which reads exactly like a
# hung step, while one line per poll would be 480 lines of noise.
echo "waiting for relink $RUN_URL — heartbeat every 10 min"
queued_for=0
fails=0
next_beat=600
wait_started=$SECONDS
while :; do
  waited=$((SECONDS - wait_started))
  state="$(gh api "repos/Otzaria/LinkerToOtzaria/actions/runs/$RUN" --jq '.status + ":" + (.conclusion // "")' 2>"$RUNNER_TEMP/wait-err.txt" || { grep -q "HTTP 401" "$RUNNER_TEMP/wait-err.txt" && echo fatal-auth || echo transient; })"
  case "$state" in
    completed:success)
      echo "relink $RUN_URL succeeded after $(hhmm "$waited")"
      break ;;
    completed:cancelled)
      # GitHub reports `cancelled` both for an operator stop and for the child
      # hitting its own timeout-minutes, with no reason attached anywhere in the
      # API. The longest job's wall clock is what separates the two — so the
      # number it is held against is the longest JOB's ceiling on this path
      # (child_job_max_min), never the whole path's budget: on the split paths a
      # resolve job legitimately running 200 minutes and then cancelled by an
      # operator would otherwise be reported as its own 90-minute timeout.
      ran="$(child_jobs '[.jobs[] | select(.started_at and .completed_at) | (.completed_at|fromdateiso8601) - (.started_at|fromdateiso8601)] | max // 0')"
      [[ "$ran" =~ ^[0-9]+$ ]] || ran=0
      if [ "$ran" -ge $((child_job_max_min * 60)) ]; then
        echo "relink $RUN_URL was cancelled after $(hhmm "$ran") — this matches its ${child_job_max_min}-minute timeout-minutes"
      elif [ "$ran" -gt 0 ]; then
        echo "relink $RUN_URL was cancelled externally after $(hhmm "$ran") — well inside its ${child_job_max_min}-minute timeout-minutes"
      else
        echo "relink $RUN_URL was cancelled; its job timings are unavailable, so an external cancel and its ${child_job_max_min}-minute timeout-minutes cannot be told apart here"
      fi
      recovery_hint
      echo "::error::relink run $RUN concluded cancelled"; exit 1 ;;
    completed:failure)
      echo "relink $RUN_URL failed after $(hhmm "$waited") — failed jobs · steps:"
      child_jobs '.jobs[] | select(.conclusion == "failure") | "  " + .name + " · " + ([.steps[]? | select(.conclusion == "failure") | .name] | join(", "))'
      recovery_hint
      echo "::error::relink run $RUN concluded failure"; exit 1 ;;
    completed:*)
      echo "relink $RUN_URL concluded ${state#completed:} after $(hhmm "$waited")"
      recovery_hint
      echo "::error::relink run $RUN concluded ${state#completed:}"; exit 1 ;;
    fatal-auth) echo "::error::token rejected (HTTP 401) while waiting — dead credentials cannot heal"; exit 1 ;;
    transient)
      # A failed poll is evidence about the API, not about the run: print its
      # first stderr line (the file used to be overwritten unread) and leave
      # `queued_for` ALONE — resetting it here disarmed the pre-start cap, so a
      # sustained outage polled silently to the job ceiling.
      fails=$((fails + 1))
      echo "relink poll failed ($fails consecutive, at $(hhmm "$waited")): $(head -n 1 "$RUNNER_TEMP/wait-err.txt")"
      if [ "$fails" -ge 30 ]; then
        echo "::error::relink run $RUN: 30 consecutive poll failures ($RUN_URL) — last error: $(head -n 1 "$RUNNER_TEMP/wait-err.txt")"; exit 1
      fi ;;
    queued:*|pending:*|requested:*|waiting:*)
      # Every pre-start state counts toward the 30-min cap — a run blocked in
      # `pending` on a busy concurrency group is just as stuck as one `queued`.
      fails=0
      queued_for=$((queued_for + 60))
      if [ "$queued_for" -ge 1800 ]; then
        echo "::error::relink run $RUN still ${state%%:*} after 30 min — never started (Kaggle session or concurrency slot)"; exit 1
      fi ;;
    *) fails=0; queued_for=0 ;;
  esac
  if [ "$waited" -ge "$next_beat" ]; then
    # Re-derive from elapsed rather than accumulating, so poll latency cannot
    # drift the cadence over hundreds of iterations.
    next_beat=$(( (waited / 600 + 1) * 600 ))
    echo "relink $(hhmm "$waited") elapsed · ${state%%:*} · $(child_where) · $RUN_URL"
  fi
  if [ "$waited" -ge $((wait_cap_min * 60)) ]; then
    echo "relink $RUN_URL reached no terminal state in $(hhmm "$waited") (last poll: $state · $(child_where))"
    recovery_hint
    echo "::error::relink run $RUN exceeded the ${wait_cap_min}-minute wait cap (the ${SERIAL_LINKER_TARGET} path's jobs may legally take ${child_path_budget_min}) — out of contract and no longer waited on; this job's cleanup cancels it, see $RUN_URL"; exit 1
  fi
  sleep 60
done
