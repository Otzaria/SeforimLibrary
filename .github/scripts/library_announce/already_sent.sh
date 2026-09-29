#!/usr/bin/env bash
# shellcheck disable=SC2016  # the $ENV references are jq, not shell
# Prints the state of "$CHANNEL $TAG" across every run of this workflow, all attempts:
#   sent <run>          a job with that name succeeded;
#   partial <run>...    none succeeded, but one failed, was cancelled or has not finished;
#   none                no such job ever started (dry runs skip channel jobs).
set -euo pipefail
: "${TAG:?}" "${CHANNEL:?}" "${GITHUB_REPOSITORY:?}"
export JOB_NAME="$CHANNEL $TAG"
# An announcing run cannot predate the release itself.
since="$(gh api "repos/$GITHUB_REPOSITORY/releases/tags/$TAG" --jq .created_at)"
[ -n "$since" ] || { echo "::error::release $TAG has no created_at" >&2; exit 1; }
runs="$(gh api -X GET --paginate "repos/$GITHUB_REPOSITORY/actions/workflows/library-update-announce.yml/runs" \
  -f per_page=100 -f created=">=$since" --jq '.workflow_runs[].id')"
seen=""
for run in $runs; do
  jobs="$(gh api --paginate "repos/$GITHUB_REPOSITORY/actions/runs/$run/jobs?filter=all&per_page=100" \
    --jq '.jobs[] | select(.name == $ENV.JOB_NAME) | "\(.status) \(.conclusion // "none")"')"
  while read -r status conclusion; do
    [ -n "${status:-}" ] || continue
    seen+="$run $status $conclusion"$'\n'
  done <<<"$jobs"
done
echo "$CHANNEL: $(grep -c . <<<"$seen" || true) '$JOB_NAME' job(s) among $(wc -w <<<"$runs") run(s) since $since" >&2
if sent_by="$(awk '$3 == "success" {print $1; exit}' <<<"$seen")" && [ -n "$sent_by" ]; then
  echo "sent $sent_by"
  exit 0
fi
partial="$(awk '$1 != "" && $3 != "skipped" {print $1}' <<<"$seen" | sort -un | tr '\n' ' ' | sed 's/ $//')"
if [ -n "$partial" ]; then
  echo "partial $partial"
else
  echo none
fi
