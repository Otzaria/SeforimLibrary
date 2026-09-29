#!/usr/bin/env bash
# shellcheck disable=SC2016  # the $ENV references are jq, not shell
# Prints sent=true|false to $GITHUB_OUTPUT: whether an earlier non-dry run of this
# workflow for $TAG has a successful $CHANNEL job in any attempt.
set -euo pipefail
: "${TAG:?}" "${CHANNEL:?}" "${GITHUB_REPOSITORY:?}" "${GITHUB_OUTPUT:?}"
# A dry run's display title ends in " (dry run)", so the exact title excludes it.
export TITLE="announce $TAG"
runs="$(gh api --paginate "repos/$GITHUB_REPOSITORY/actions/workflows/library-update-announce.yml/runs?per_page=100" \
  --jq '.workflow_runs[] | select(.display_title == $ENV.TITLE) | .id')"
for run in $runs; do
  done_by="$(gh api --paginate "repos/$GITHUB_REPOSITORY/actions/runs/$run/jobs?filter=all&per_page=100" \
    --jq '.jobs[] | select(.name == $ENV.CHANNEL and .conclusion == "success") | .html_url')"
  if [ -n "$done_by" ]; then
    echo "::notice::$CHANNEL already announced $TAG in $(head -n1 <<<"$done_by") — skipping"
    echo "sent=true" >> "$GITHUB_OUTPUT"
    exit 0
  fi
done
echo "$CHANNEL: no earlier successful announcement of $TAG among $(wc -w <<<"$runs") matching run(s)"
echo "sent=false" >> "$GITHUB_OUTPUT"
