#!/usr/bin/env bash
# Shared draft-release machinery for the weekly DB release. Sourced, not run.
#
# Two steps drive the SAME draft of $RELEASE_TAG and must therefore share one
# create/reconcile/upload contract:
#
#   * "Run LinkerToOtzaria relink on this snapshot (and wait)" creates the draft
#     and pushes the assets that are already byte-final up the ~2.1 MB/s uplink
#     while the job sits idle waiting for the linker, and
#   * "Create draft, verify every uploaded asset, then publish" adopts that same
#     draft, uploads whatever is still missing, re-verifies EVERY asset by
#     name+size+digest against the staged bytes, and flips draft -> published.
#
# Nothing here ever uses --clobber: an asset that already exists is verified by
# name+size+digest and skipped, or the build fails. A create/upload whose
# response was lost is reconciled, never blindly repeated.
#
# Required in the environment: GITHUB_REPOSITORY, RUNNER_TEMP, RELEASE_TAG,
# SOURCE_COMMIT, RELEASE_TOKEN_KIND, RELEASE_AUTOMATIC_WRITABLE,
# RELEASE_CROSS_REPO_WRITABLE, AUTOMATIC_TOKEN, CROSS_REPO_TOKEN.
# Call use_token first, then ensure_draft, then RELEASE_ID="$(resolve_release_id)"
# before any asset_state/upload_asset call.

# The only assets a build may upload before "Stage release assets" has run, i.e.
# the ones already byte-final earlier in the job. A draft carrying nothing but
# these is still unambiguously this build's own draft and is adopted; anything
# else is a conflict and fails closed.
#
# seforim.db.buildstate is on the list but is NOT final before the relink wait:
# Phase-2 allocates fresh stable link ids straight into it
# (DiskBackedLinkIdAllocator.open/commit on build/seforim.db.buildstate), so it
# may only be uploaded after "Apply LINKER links (Phase-2)" has run.
EARLY_RELEASE_ASSETS="lines_snapshot.db.zst seforim.db.buildstate"

use_token() {
  case "$1" in
    automatic)
      [ "${RELEASE_AUTOMATIC_WRITABLE:-}" = true ] || return 1
      export GH_TOKEN="${AUTOMATIC_TOKEN:-}"
      ;;
    cross-repo)
      [ "${RELEASE_CROSS_REPO_WRITABLE:-}" = true ] || return 1
      export GH_TOKEN="${CROSS_REPO_TOKEN:-}"
      ;;
    *) return 1 ;;
  esac
  CURRENT_TOKEN_KIND="$1"
}

switch_token() {
  case "${CURRENT_TOKEN_KIND:-}" in
    automatic) use_token cross-repo ;;
    cross-repo) use_token automatic ;;
    *) return 1 ;;
  esac
}

list_matching_releases() {
  gh api --paginate "repos/$GITHUB_REPOSITORY/releases?per_page=100" | \
    jq -s --arg tag "$RELEASE_TAG" '[add[] | select(.tag_name == $tag)]'
}

# Print absent, exact-draft, or conflict. Draft releases are not returned by
# GitHub's /releases/tags/{tag} REST route, so reconciliation must use the fully
# paginated release collection. A response-lost create is adopted only when every
# immutable field matches this invocation and the draft carries nothing but the
# early assets this build itself uploads; an unrelated release is never
# overwritten, and a wrong-sized early asset still fails in upload_asset.
release_state() {
  local output="$RUNNER_TEMP/release-state-$$.json"
  list_matching_releases > "$output" || return 1
  python3 - "$output" "$RELEASE_TAG" "$SOURCE_COMMIT" "$EARLY_RELEASE_ASSETS" <<'PY'
import json,sys
matches=json.load(open(sys.argv[1],encoding="utf-8"))
if not matches:
    print("absent")
    raise SystemExit(0)
if len(matches) != 1:
    print("conflict")
    raise SystemExit(0)
value=matches[0]
allowed=set(sys.argv[4].split())
names=[asset.get("name") for asset in value.get("assets",[])]
exact=(
    value.get("tag_name")==sys.argv[2]
    and value.get("name")==sys.argv[2]
    and value.get("target_commitish")==sys.argv[3]
    and value.get("draft") is True
    and len(names)==len(set(names))
    and set(names) <= allowed
)
print("exact-draft" if exact else "conflict")
PY
}

ensure_draft() {
  local state
  state=$(release_state) || return 1
  case "$state" in
    exact-draft)
      echo "Adopting this build's existing draft release."
      return 0
      ;;
    conflict)
      echo "::error::Release tag already exists with conflicting identity or assets."
      return 1
      ;;
    absent) ;;
    *) echo "::error::Unknown release state: $state"; return 1 ;;
  esac
  if gh release create "$RELEASE_TAG" --target "$SOURCE_COMMIT" --title "$RELEASE_TAG" --draft; then
    return 0
  fi
  echo "::warning::Release create failed with ${CURRENT_TOKEN_KIND:-}; reconciling before retry."
  state=$(release_state) || state=query-error
  [ "$state" != conflict ] || return 1
  if [ "$state" = exact-draft ]; then return 0; fi
  switch_token || {
    echo "::error::No independently preflighted release credential remains."
    return 1
  }
  state=$(release_state) || return 1
  case "$state" in
    exact-draft) return 0 ;;
    absent)
      if gh release create "$RELEASE_TAG" --target "$SOURCE_COMMIT" --title "$RELEASE_TAG" --draft; then
        return 0
      fi
      state=$(release_state) || return 1
      [ "$state" = exact-draft ]
      ;;
    *) echo "::error::Release tag is conflicting after credential fallback."; return 1 ;;
  esac
}

resolve_release_id() {
  list_matching_releases | jq -er \
    --arg tag "$RELEASE_TAG" --arg target "$SOURCE_COMMIT" --arg allowed "$EARLY_RELEASE_ASSETS" '
      select(length == 1) | .[0] |
      select(.tag_name == $tag and .name == $tag and .target_commitish == $target and
             .draft == true and
             (([.assets[].name] - ($allowed | split(" "))) | length) == 0) | .id |
      select(type == "number" and . > 0)
    '
}

release_assets_json() {
  gh api "repos/$GITHUB_REPOSITORY/releases/$RELEASE_ID" --jq \
    '{assets:[.assets[]|{name,size,digest}]}'
}

asset_state() {
  local asset_path="$1" expected_size="$2" expected_digest="$3"
  local output="$RUNNER_TEMP/release-assets-state-$$.json"
  release_assets_json > "$output" || return 1
  python3 - "$output" "$asset_path" "$expected_size" "$expected_digest" <<'PY'
import json,sys
from pathlib import Path
assets=json.load(open(sys.argv[1],encoding="utf-8"))["assets"]
path=Path(sys.argv[2])
matches=[item for item in assets if item.get("name")==path.name]
if not matches:
    print("absent")
elif len(matches)!=1:
    print("conflict")
else:
    item=matches[0]
    if item.get("size")!=int(sys.argv[3]):
        print("conflict")
    elif item.get("digest") in (None, ""):
        print("pending")
    else:
        print("exact" if item.get("digest")=="sha256:"+sys.argv[4] else "conflict")
PY
}

# Upload one asset. If an upload response is lost, reconcile by exact
# name+size+digest and continue. Never use --clobber.
upload_asset() {
  local asset_path="$1" expected_size expected_digest state attempt
  expected_size=$(stat --format='%s' "$asset_path")
  expected_digest=$(sha256sum "$asset_path" | cut -d ' ' -f1)
  state=$(asset_state "$asset_path" "$expected_size" "$expected_digest") || return 1
  case "$state" in
    exact) return 0 ;;
    conflict) echo "::error::Conflicting remote asset: ${asset_path##*/}"; return 1 ;;
    absent) ;;
    pending)
      for attempt in $(seq 1 12); do
        sleep 5
        state=$(asset_state "$asset_path" "$expected_size" "$expected_digest") || state=query-error
        case "$state" in
          exact) return 0 ;;
          conflict|absent) return 1 ;;
        esac
      done
      echo "::error::Digest metadata never settled for existing asset: ${asset_path##*/}"
      return 1
      ;;
    *) echo "::error::Unknown asset state: $state"; return 1 ;;
  esac
  if gh release upload "$RELEASE_TAG" "$asset_path"; then return 0; fi
  echo "::warning::Upload failed for ${asset_path##*/}; reconciling before retry."
  for attempt in $(seq 1 12); do
    state=$(asset_state "$asset_path" "$expected_size" "$expected_digest") || state=query-error
    case "$state" in
      exact) return 0 ;;
      conflict) return 1 ;;
    esac
    sleep 5
  done
  if [ "$state" = pending ]; then
    echo "::error::Refusing to duplicate an upload whose digest metadata is still pending: ${asset_path##*/}"
    return 1
  fi
  switch_token || return 1
  if gh release upload "$RELEASE_TAG" "$asset_path"; then return 0; fi
  for attempt in $(seq 1 6); do
    state=$(asset_state "$asset_path" "$expected_size" "$expected_digest") || state=query-error
    [ "$state" != conflict ] || return 1
    if [ "$state" = exact ]; then return 0; fi
    sleep 2
  done
  return 1
}
