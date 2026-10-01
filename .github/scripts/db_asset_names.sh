#!/usr/bin/env bash
# The one definition of the full-DB release asset name. Sourced, not run.
#
# `seforim.db.zst` is reserved for DB schema <= 5 forever: released updaters, the
# first-install path and old app builds match that exact name, so a schema they
# cannot open must never appear under it. From schema 6 on the full DB ships
# page-compressed as `seforim-schema<N>.zdb` (with `<name>.manifest.json`), which
# those clients never see.
# validate_build_provenance.py:full_db_asset_name mirrors this (a test pins both).

LEGACY_FULL_DB_ASSET=seforim.db.zst

full_db_asset_name() {  # <db_schema_version> -> the release asset name of that full DB
  local schema="$1"
  case "$schema" in
    ''|*[!0-9]*|0*)
      echo "::error::full_db_asset_name: db schema '$schema' is not a positive integer" >&2
      return 1
      ;;
  esac
  if [ "$schema" -ge 6 ]; then
    printf 'seforim-schema%s.zdb' "$schema"
  else
    printf '%s' "$LEGACY_FULL_DB_ASSET"
  fi
}

# The full-DB assets an anchor release may carry for <schema>, preferred first. A
# seforim-schema<N>.db.zst is a schema-6 test build published before the zdb.
anchor_db_asset_candidates() {  # <db_schema_version>
  local name
  name=$(full_db_asset_name "$1") || return 1
  if [ "$name" = "$LEGACY_FULL_DB_ASSET" ]; then
    printf '%s' "$name"
  else
    printf '%s seforim-schema%s.db.zst %s' "$name" "$1" "$LEGACY_FULL_DB_ASSET"
  fi
}

# jq over a release's JSON (`.assets[]`): its full-DB asset, preferring a zdb, then a
# seforim-schema<N>.db.zst, then seforim.db.zst (highest schema within a kind); null when none.
FULL_DB_ASSET_JQ='[.assets[] | select(.name == "seforim.db.zst" or (.name | test("^seforim-schema[1-9][0-9]*\\.(zdb|db\\.zst)$")))]
  | sort_by(if .name == "seforim.db.zst" then [0, 0]
      elif (.name | endswith(".zdb")) then [2, (.name | ltrimstr("seforim-schema") | rtrimstr(".zdb") | tonumber)]
      else [1, (.name | ltrimstr("seforim-schema") | rtrimstr(".db.zst") | tonumber)] end)
  | last'
