#!/usr/bin/env bash
# Builds the zvfs_cli pinned by .github/contracts/zvfs.json, cached per commit+arch.
# Usage: build_zvfs_cli.sh <zvfs.json> [cache-dir]; prints the CLI's path last.
#
# The converter lives in the app repository (packages/otzaria_zvfs). Only that
# package is fetched, at exactly the pinned commit, and built with its own
# tool/build_cli.sh (plain cc, no cmake). Logs go to stderr.
set -euo pipefail

contract="$1"
cache="${2:-${XDG_CACHE_HOME:-$HOME/.cache}/seforimlibrary/zvfs_cli}"
package=packages/otzaria_zvfs

repository=$(jq -er '.repository' "$contract")
commit=$(jq -er '.commit' "$contract")
[[ "$repository" =~ ^[A-Za-z0-9-]+/[A-Za-z0-9._-]+$ ]] || {
  echo "::error::$contract: repository '$repository' is not owner/name" >&2; exit 1; }
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || {
  echo "::error::$contract: commit '$commit' is not a full lowercase SHA" >&2; exit 1; }
[ "$(jq -c 'keys' "$contract")" = '["commit","repository"]' ] || {
  echo "::error::$contract must carry exactly repository and commit" >&2; exit 1; }

arch=$(uname -m)
slot="$cache/$commit-$arch"
cli="$slot/zvfs_cli"

# A cached binary is used only when it still hashes to what was stored with it
# and still knows the dictionary the release converts with.
if [ -x "$cli" ] && [ -s "$slot/zvfs_cli.sha256" ] \
   && (cd "$slot" && sha256sum -c --status zvfs_cli.sha256) \
   && "$cli" dicts | grep -q '^seforim-v1 '; then
  echo "zvfs_cli $repository@${commit:0:12} ($arch): cached $cli" >&2
  printf '%s\n' "$cli"
  exit 0
fi

mkdir -p "$cache"
work=$(mktemp -d "$cache/.build.XXXXXX")
trap 'rm -rf "$work"' EXIT
src="$work/src"
git init -q "$src"
git -C "$src" remote add origin "https://github.com/$repository.git"
git -C "$src" config core.sparseCheckout true
git -C "$src" config core.sparseCheckoutCone false
printf '/%s/\n' "$package" > "$src/.git/info/sparse-checkout"
git -C "$src" fetch -q --depth 1 --filter=blob:none origin "$commit"
git -C "$src" -c advice.detachedHead=false checkout -q FETCH_HEAD
[ "$(git -C "$src" rev-parse HEAD)" = "$commit" ] || {
  echo "::error::fetched $(git -C "$src" rev-parse HEAD), not the pinned $commit" >&2; exit 1; }

mkdir -p "$work/slot"
sh "$src/$package/tool/build_cli.sh" "$work/slot/zvfs_cli" >&2
# The package's own roundtrip: convert, verify, info, export byte-equal, reproducible.
sh "$src/$package/tool/cli_roundtrip.sh" "$work/slot/zvfs_cli" >&2
(cd "$work/slot" && sha256sum zvfs_cli > zvfs_cli.sha256)

rm -rf "$slot"
mv "$work/slot" "$slot"
echo "zvfs_cli $repository@${commit:0:12} ($arch): built $cli" >&2
printf '%s\n' "$cli"
