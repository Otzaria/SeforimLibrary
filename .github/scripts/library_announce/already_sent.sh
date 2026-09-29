#!/usr/bin/env bash
# shellcheck disable=SC2016  # the $ENV references are jq, not shell
# Prints true|false: whether any run of this workflow has a successful "$CHANNEL $TAG"
# job in any attempt. Dry runs never run channel jobs, so they never count.
set -euo pipefail
: "${TAG:?}" "${CHANNEL:?}" "${GITHUB_REPOSITORY:?}"
export JOB_NAME="$CHANNEL $TAG"
# An announcing run cannot predate the release itself.
since="$(gh api "repos/$GITHUB_REPOSITORY/releases/tags/$TAG" --jq .created_at)"
[ -n "$since" ] || { echo "::error::release $TAG has no created_at" >&2; exit 1; }
runs="$(gh api -X GET --paginate "repos/$GITHUB_REPOSITORY/actions/workflows/library-update-announce.yml/runs" \
  -f per_page=100 -f created=">=$since" --jq '.workflow_runs[].id')"
for run in $runs; do
  done_by="$(gh api --paginate "repos/$GITHUB_REPOSITORY/actions/runs/$run/jobs?filter=all&per_page=100" \
    --jq '.jobs[] | select(.name == $ENV.JOB_NAME and .conclusion == "success") | .html_url')"
  if [ -n "$done_by" ]; then
    echo "::notice::$CHANNEL already announced $TAG in $(head -n1 <<<"$done_by")" >&2
    echo true
    exit 0
  fi
done
echo "$CHANNEL: no successful '$JOB_NAME' job among $(wc -w <<<"$runs") run(s) since $since" >&2
echo false
