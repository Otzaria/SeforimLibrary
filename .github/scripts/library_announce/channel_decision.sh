#!/usr/bin/env bash
# Prints skip|proceed for "$CHANNEL $TAG" in this run, or fails. A partial send in
# another run must be resumed there (its recorded progress), never repeated here.
set -euo pipefail
: "${TAG:?}" "${CHANNEL:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_REPOSITORY:?}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
state="$(bash "$here/already_sent.sh")"
bad() { echo "::error::already_sent.sh printed '$state' for $CHANNEL $TAG" >&2; exit 1; }
[[ "$state" =~ ^(none|sent\ [0-9]+|partial\ [0-9]+(\ [0-9]+)*)$ ]] || bad
read -r kind runs <<<"$state"
case "$kind" in
  none) echo proceed ;;
  sent)
    echo "::notice::$CHANNEL already announced $TAG in run $runs" >&2
    echo skip ;;
  partial)
    others=()
    for run in $runs; do [ "$run" = "$GITHUB_RUN_ID" ] || others+=("$run"); done
    if [ "${#others[@]}" -gt 0 ]; then
      for run in "${others[@]}"; do
        echo "::error::$CHANNEL for $TAG was started but not completed by run $run (failed, cancelled or still running)." \
          "Open https://github.com/$GITHUB_REPOSITORY/actions/runs/$run and use \"Re-run failed jobs\" there, so it" \
          "resumes from its recorded progress instead of sending a duplicate." >&2
      done
      exit 1
    fi
    echo proceed ;;
  *) bad ;;
esac
