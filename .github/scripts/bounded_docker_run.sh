#!/usr/bin/env bash
# bounded_run <bound> <tag> <docker run args...> — build-library-index.yml's only way to run a container.
#
# A bound that reaches only the docker CLIENT is half a bound. SIGTERM
# is forwarded to PID 1 of the container — a non-interactive bash,
# which installs no handler and, as PID 1, is not killed by default
# action — and the SIGKILL that --kill-after sends afterwards goes to
# the client and cannot be proxied. The step would fail loudly while
# the container kept running, kept holding -v "$WORK":/work, and kept
# writing into an index the job had already given up on. So every run
# records its id and the bound disposes of it explicitly.
bounded_run() {
  local bound="$1" tag="$2"; shift 2
  local cid="$WORK/cid-$tag"
  local rc=0
  rm -f "$cid"
  timeout --kill-after=60 "$bound" docker run --cidfile "$cid" "$@" || rc=$?
  [ "$rc" -ne 0 ] && [ -s "$cid" ] || return "$rc"
  # timeout exits 124 (TERM) or 137 (KILL) when the bound fired; any other failure is the container's own.
  if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
    echo "::warning::the bound of $bound expired; removing the container it left behind ($(cat "$cid"))"
  elif docker inspect "$(cat "$cid")" > /dev/null 2>&1; then
    echo "::warning::container $(cat "$cid") outlived its failed run; removing it"
  else
    return "$rc"
  fi
  docker rm -f "$(cat "$cid")" > /dev/null 2>&1 \
    || echo "::warning::that container could not be removed — it may still be running"
  return "$rc"
}
