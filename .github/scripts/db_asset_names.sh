#!/usr/bin/env bash
# The one definition of the full-DB release asset name. Sourced, not run.
#
# `seforim.db.zst` is reserved for DB schema <= 5 forever: released updaters, the
# first-install path and old app builds match that exact name, so a schema they
# cannot open must never appear under it. From schema 6 on the full DB ships as
# `seforim-schema<N>.db.zst`, which those clients never see.
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
    printf 'seforim-schema%s.db.zst' "$schema"
  else
    printf '%s' "$LEGACY_FULL_DB_ASSET"
  fi
}

# jq over a release's JSON (`.assets[]`): the full-DB asset with the highest
# schema, the legacy name counting as schema 0; null when the release has none.
# A split DB (<name>.part-NNN + <name>.manifest.json) yields the archive name, .manifest
# and no .size/.digest; a single file wins over a split one of the same schema.
FULL_DB_ASSET_JQ='[.assets[]
    | if (.name | test("^(seforim|seforim-schema[1-9][0-9]*)\\.db\\.zst\\.manifest\\.json$"))
      then . + {name: (.name | rtrimstr(".manifest.json")), manifest: .name, size: null, digest: null}
      else . end
    | select(.name == "seforim.db.zst" or (.name | test("^seforim-schema[1-9][0-9]*\\.db\\.zst$")))]
  | sort_by([(if .name == "seforim.db.zst" then 0 else (.name | ltrimstr("seforim-schema") | rtrimstr(".db.zst") | tonumber) end),
             (if .manifest then 0 else 1 end)])
  | last'

# Split only above 1.9 GiB, into 1900 MiB parts: GitHub refuses an asset of 2 GiB.
FULL_DB_SPLIT_THRESHOLD="${FULL_DB_SPLIT_THRESHOLD:-2040109465}"
FULL_DB_PART_SIZE="${FULL_DB_PART_SIZE:-1992294400}"
DB_ASSET_SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

stage_full_db() {  # <archive> <stage-dir>: the archive itself, or its parts + manifest when too big
  local archive="$1" stage="$2" size work
  size=$(stat -c '%s' "$archive" 2>/dev/null || stat -f '%z' "$archive") || return 1
  if [ "$size" -le "$FULL_DB_SPLIT_THRESHOLD" ]; then
    cp "$archive" "$stage/"
    return
  fi
  echo "$(basename "$archive") is $size bytes, above $FULL_DB_SPLIT_THRESHOLD - publishing it in parts"
  work=$(mktemp -d "$stage/.split.XXXXXX") || return 1
  bash "$DB_ASSET_SCRIPTS_DIR/split_release_asset.sh" "$archive" "$work" "$FULL_DB_PART_SIZE" \
    && mv "$work"/* "$stage/" && rmdir "$work"
}

download_full_db_by_name() {  # <tag> <asset-name> <dest-dir> [repo] -> <dest-dir>/<asset-name>
  # A split asset is assembled; every part and the whole are sha256-verified.
  local tag="$1" name="$2" dest="$3" work part rc=0
  local -a repo=()
  [ -z "${4:-}" ] || repo=(-R "$4")
  work=$(mktemp -d "$dest/.fulldb.XXXXXX") || return 1
  if gh release download "$tag" "${repo[@]}" --pattern "$name" --dir "$dest" 2>"$work/single.err"; then
    rm -rf "$work"
    return 0
  fi
  if ! gh release download "$tag" "${repo[@]}" --pattern "$name.manifest.json" --dir "$work" 2>/dev/null; then
    cat "$work/single.err" >&2
    rm -rf "$work"
    return 1
  fi
  while IFS= read -r part; do
    case "$part" in ''|*/*|.|..) echo "unsafe part name in $name.manifest.json: $part" >&2; rc=1; break ;; esac
    gh release download "$tag" "${repo[@]}" --pattern "$part" --dir "$work" || { rc=1; break; }
  done < <(jq -r '.parts[].name' "$work/$name.manifest.json")
  if [ "$rc" = 0 ] && ! bash "$DB_ASSET_SCRIPTS_DIR/assemble_split_asset.sh" \
       "$work/$name.manifest.json" "$dest/$name" >/dev/null; then
    rc=1
    rm -f "$dest/$name"
  fi
  rm -rf "$work"
  return "$rc"
}

split_full_db_meta() {  # <tag> <asset-name> [repo] -> "<size>\t<sha256:digest>" from its split manifest
  local -a repo=()
  [ -z "${3:-}" ] || repo=(-R "$3")
  gh release download "$1" "${repo[@]}" --pattern "$2.manifest.json" -O - \
    | jq -er '[(.size | tostring), ("sha256:" + .sha256)] | @tsv'
}

stream_full_db() {  # <tag> <asset-name> [repo] -> the archive's bytes on stdout
  # Parts are streamed unchecked; the consumer's zstd verifies the frame checksum.
  local tag="$1" name="$2" manifest part
  local -a repo=()
  [ -z "${3:-}" ] || repo=(-R "$3")
  if ! manifest=$(gh release download "$tag" "${repo[@]}" --pattern "$name.manifest.json" -O - 2>/dev/null); then
    gh release download "$tag" "${repo[@]}" --pattern "$name" -O -
    return
  fi
  for part in $(jq -r '.parts[].name' <<<"$manifest"); do
    gh release download "$tag" "${repo[@]}" --pattern "$part" -O - || return 1
  done
}
