#!/usr/bin/env bash
# The single derivation of "which prior releases this build's patch fan considers".
#
# Two places need the identical answer and must never drift:
#
#   * "Prefetch patch-fan anchor DBs" starts the 1.3 GB downloads in parallel
#     while "Generate Seforim Database" runs, and
#   * "Produce + verify patch fan" walks the same anchors ~70 minutes later.
#
# A prefetch that resolved even one tag differently would download the wrong DB
# and silently degrade back to the serial path, so the loop lives here instead of
# being written twice.
#
# Usage: patch_fan_anchors.sh <this_db_version> <prior-versions.tsv> <offset>...
#
# Prints one TSV row per offset — <status>\t<offset>\t<target_version>\t<tag>:
#
#   ANCHOR      the tag exists; the fan will try to produce this patch
#   BELOW-ONE   the offset reaches below db_version 1 — nothing to anchor on
#   NO-RELEASE  no prior release carries that db_version
#
# prior-versions.tsv is written by "Auto-discover prior releases"
# (cols: db_version<TAB>release_tag, newest db_version first).
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "usage: ${0##*/} <this_db_version> <prior-versions.tsv> <offset>..." >&2
  exit 2
fi

this_version="$1"
prior_versions="$2"
shift 2

case "$this_version" in
  ''|*[!0-9]*) echo "::error::patch_fan_anchors: db version '$this_version' is not a number" >&2; exit 2 ;;
esac
[ -f "$prior_versions" ] || { echo "::error::patch_fan_anchors: $prior_versions does not exist" >&2; exit 2; }

for offset in "$@"; do
  case "$offset" in
    ''|*[!0-9]*) echo "::error::patch_fan_anchors: offset '$offset' is not a number" >&2; exit 2 ;;
  esac
  target=$((this_version - offset))
  if [ "$target" -lt 1 ]; then
    printf 'BELOW-ONE\t%s\t%s\t\n' "$offset" "$target"
    continue
  fi
  tag=$(awk -F'\t' -v v="$target" '$1==v{print $2; exit}' "$prior_versions" || true)
  if [ -z "$tag" ]; then
    printf 'NO-RELEASE\t%s\t%s\t\n' "$offset" "$target"
    continue
  fi
  printf 'ANCHOR\t%s\t%s\t%s\n' "$offset" "$target" "$tag"
done
