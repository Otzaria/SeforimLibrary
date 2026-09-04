#!/usr/bin/env bash
# Shared zstd multi-threading policy for this workflow's pack steps.
#
# WHY THIS EXISTS
# ---------------
# `zstd -T0` does NOT mean "use every CPU".  It resolves to the number of
# PHYSICAL cores (zstd's UTIL_countPhysicalCores(), which de-duplicates
# /proc/cpuinfo by "core id"), not the number of logical CPUs the scheduler
# will actually hand us.  On the self-hosted DB runner (Ubuntu in WSL2, 16
# vCPUs exposed as 8 physical + 8 sibling threads) `-T0` therefore starts only
# 8 workers and leaves half the box idle for the whole compression.
#
# DETERMINISM
# -----------
# zstd's frame output depends on the compression LEVEL, the job size and the
# overlap size -- NOT on how many workers chew through those jobs.  Pinning the
# worker count to the logical CPU count is therefore BYTE-NEUTRAL for every
# job-based multithread run (`-T<n>`, n >= 1); only `--single-thread` produces
# different bytes.  Because output is worker-count independent, this is safe to
# vary per machine.
#
# This mirrors LinkerToOtzaria's ci/zstd_mt.sh so both halves of the pipeline
# share one policy.
zstd_workers() {
  local n
  n="$(nproc 2>/dev/null || echo 0)"
  case "$n" in
    ''|*[!0-9]*) n=0 ;;
  esac
  # Bound worst-case resident set: each worker holds roughly one job buffer plus
  # one match-finder context.  32 workers is ~4 GB at level 19 (~124 MB per worker measured), well inside the
  # runner's budget while still saturating any realistic runner.
  if [ "$n" -gt 32 ]; then
    n=32
  fi
  # 0 falls back to zstd's own detection, i.e. exactly the previous behaviour.
  printf '%s\n' "$n"
}
