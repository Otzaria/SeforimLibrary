#!/usr/bin/env bash
# Build one library release's semantic vectors on the build machine, and publish them.
#
#   build_library_vectors.sh --tag v<N>-<YYYYMMDDHHMMSS> [--mode base|dry-run]
#                            [--state DIR] [--work DIR] [--repo OWNER/NAME]
#
#   1 index     download the release's search index (split parts, sha256-checked) and expand it,
#               once its provenance names this release, this archive and this release's DB
#   2 plan      export_semantic_plan v2 over it, split against the warehouse: embed.jsonl holds
#               only the texts the warehouse has no vector for (every text, on a runner that
#               has no warehouse yet)
#   3 embed     those texts: fewer than CPU_EMBED_MAX on the CPU, with the sidecar's embed-shard
#               (ONNX Runtime and the fp32 package — the reference, so no parity certificate); more
#               on the GPU, with embed_worker.py — windows of EMBED_WINDOW records, each a v2 shard
#               with its parity certificate
#   4 warehouse warehouse-add --plan: the shards held to the plan, their vectors appended (the
#               first add creates the warehouse)
#   5 assemble  a base (POC: every release is a base), then assemble --verify (the gates), then
#               the plugin's validate_semantic_vectors on the release index: G3 coverage, G4
#               resolution, G6 retrieval with the int8 query model; any exit but 0 stops the build
#   6 files     the segment compressed (and split below GitHub's asset limit), release-files
#   7 publish   (mode base) a sibling release vectors-<tag> at the commit of <tag>, made a
#               draft first: data, then gates.json and validation.json, the manifest last;
#               once the draft holds exactly those files at their sizes it is published,
#               --latest=false; then the ledger is persisted — only after a publish
#
# LIBRARY_VECTORS_PRERELEASE (true unless "false") publishes vectors-<tag> as a prerelease:
# like the lines-snapshot-sha256-* and pipeline-result-* releases it then stays out of
# update-release-manifest.yml and the database history; a download by tag works the same.
#
# --mode dry-run stops after step 6: it publishes nothing and persists no ledger (the warehouse
# keeps what it embedded, as in a base). The state directory (default
# /home/runner/otzaria-vectors; see bootstrap_runner.sh) holds the binaries, the venv, the
# model cache, the warehouse and the published ledger, and is locked for the whole build.
# Runs by hand on the build machine as it runs in the workflow. Tests replace the tools with
# OTZARIA_SEMANTIC_CLI, EXPORT_SEMANTIC_PLAN, VALIDATE_SEMANTIC_VECTORS, VECTOR_PYTHON, GH and ZSTD.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=pins.env
. "$HERE/pins.env"
# shellcheck source=../db_asset_names.sh
. "$HERE/../db_asset_names.sh"   # FULL_DB_ASSET_JQ: the release's full DB, which the index is built from

TAG=""; MODE=dry-run; STATE=/home/runner/otzaria-vectors; WORK=""; REPO=${GITHUB_REPOSITORY:-Otzaria/SeforimLibrary}
while [ $# -gt 0 ]; do
  case "$1" in
    --tag) TAG=$2; shift 2 ;;
    --mode) MODE=$2; shift 2 ;;
    --state) STATE=$2; shift 2 ;;
    --work) WORK=$2; shift 2 ;;
    --repo) REPO=$2; shift 2 ;;
    *) echo "usage: build_library_vectors.sh --tag vN-YYYYMMDDHHMMSS [--mode base|dry-run] [--state DIR] [--work DIR] [--repo R]" >&2; exit 64 ;;
  esac
done
echo "$TAG" | grep -Eq '^v[0-9]+-[0-9]{14}$' || { echo "::error::'$TAG' is not a database release tag (v<dbVersion>-<utcTimestamp>)"; exit 64; }
case "$MODE" in base|dry-run) ;; *) echo "::error::--mode is base or dry-run, not '$MODE'"; exit 64 ;; esac
PRERELEASE=${LIBRARY_VECTORS_PRERELEASE:-true}
case "$PRERELEASE" in true|false) ;; *) echo "::error::LIBRARY_VECTORS_PRERELEASE is true or false, not '$PRERELEASE'"; exit 64 ;; esac
VERSION=${TAG#v}; VERSION=${VERSION%%-*}
WORK=${WORK:-$STATE/work/$TAG}
CLI=${OTZARIA_SEMANTIC_CLI:-$STATE/bin/otzaria-semantic-search}
EXPORT=${EXPORT_SEMANTIC_PLAN:-$STATE/bin/export_semantic_plan}
VALIDATE=${VALIDATE_SEMANTIC_VECTORS:-$STATE/bin/validate_semantic_vectors}
PY=${VECTOR_PYTHON:-$STATE/venv/bin/python}
GH=${GH:-gh}
ZSTD=${ZSTD:-zstd}
FAMILY=$STATE/bin/family-model.json
WAREHOUSE=$STATE/warehouse/$PASSAGE_QUANTIZATION-${PASSAGE_PACKAGE_CHECKSUM:0:8}
LEDGER=$STATE/ledger
RELEASE_TAG="vectors-$TAG"
PART_SIZE=1992294400
export HSA_ENABLE_DXG_DETECTION=1

T0=$(date +%s)
group() { local t="[+$(( $(date +%s) - T0 ))s]"; if [ "${GITHUB_ACTIONS:-}" = true ]; then echo "::group::$* $t"; else echo "== $* $t"; fi; }
endgroup() { if [ "${GITHUB_ACTIONS:-}" = true ]; then echo "::endgroup::"; fi; }
die() { echo "::error::$*"; exit 1; }
onnx_runtime() {  # the venv's ONNX Runtime library, which embed-shard and the validator load
  local lib
  for lib in "$STATE"/venv/lib/python3*/site-packages/onnxruntime/capi/libonnxruntime.so.*; do
    [ -f "$lib" ] && { echo "$lib"; return 0; }
  done
  return 1
}

# ─── preflight ─────────────────────────────────────────────────────────────
group "preflight ($TAG, $MODE)"
mkdir -p "$STATE"
exec 9>"$STATE/.lock"; flock -n 9 || die "another vector build or bootstrap holds $STATE/.lock"
[ -x "$CLI" ] || die "the sidecar CLI is not at $CLI — run bootstrap_runner.sh"
if [ -z "${OTZARIA_SEMANTIC_CLI:-}" ]; then
  [ "$(cat "$CLI.rev" 2>/dev/null)" = "$SIDECAR_REV" ] || die "$CLI is not the pinned sidecar $SIDECAR_REV — run bootstrap_runner.sh"
  grep -q -- '--features onnx-backend' "$CLI.build" 2>/dev/null \
    || die "$CLI was built with no inference backend, so its embed-shard cannot embed — run bootstrap_runner.sh"
fi
[ -n "$PLUGIN_REV" ] || [ -n "${EXPORT_SEMANTIC_PLAN:-}" ] || die "export_semantic_plan v2 (plugin P5) is not pinned yet in pins.env: the release cannot be planned"
[ -x "$EXPORT" ] || die "export_semantic_plan is not at $EXPORT — run bootstrap_runner.sh"
[ -x "$VALIDATE" ] || die "validate_semantic_vectors is not at $VALIDATE — run bootstrap_runner.sh"
if [ -z "${EXPORT_SEMANTIC_PLAN:-}${VALIDATE_SEMANTIC_VECTORS:-}" ]; then
  for bin in "$EXPORT" "$VALIDATE"; do
    [ "$(cat "$bin.rev" 2>/dev/null)" = "$PLUGIN_REV" ] || die "$bin is not the pinned plugin $PLUGIN_REV — run bootstrap_runner.sh"
  done
  grep -qE -- '--features semantic( |$)' "$VALIDATE.build" 2>/dev/null \
    || die "$VALIDATE was built without --features semantic, so it cannot run G6 — run bootstrap_runner.sh"
fi
[ -f "$FAMILY" ] || die "$FAMILY is missing — run bootstrap_runner.sh"
[ -n "${MODEL_REVISION:-}" ] || die "pins.env pins no MODEL_REVISION: the model is fetched at a commit, never a branch"
for tool in jq sha256sum split tar; do command -v "$tool" >/dev/null || die "$tool is required"; done
MIN_FREE_GB=${VECTORS_MIN_FREE_GB:-$MIN_FREE_GB}
free_kb=$(df -Pk "$STATE" | awk 'NR==2 {print $4}')
[ "$free_kb" -ge $((MIN_FREE_GB * 1024 * 1024)) ] || die "only $((free_kb / 1024 / 1024)) GiB free on $STATE, fewer than $MIN_FREE_GB"
# The warehouse, or none yet: then every text is planned and the first add creates it. A
# directory holding files but no warehouse.json is neither, and --create would empty it.
if [ -f "$WAREHOUSE/warehouse.json" ]; then
  FRESH=""
elif [ -n "$(ls -A "$WAREHOUSE" 2>/dev/null)" ]; then
  die "$WAREHOUSE holds files but no warehouse.json: it is not a warehouse (move it aside to start a new one)"
else
  FRESH=1
fi
if "$GH" auth status >/dev/null 2>&1; then GH_OK=1; else GH_OK=""; fi
if [ "$MODE" = base ]; then
  [ -n "$GH_OK" ] || die "mode base publishes through gh, and gh is not authenticated"
  if "$GH" release view "$RELEASE_TAG" --repo "$REPO" >/dev/null 2>&1; then
    die "$RELEASE_TAG exists already (a draft a failed publish left counts too: delete it to retry); a published vector release is never rebuilt in place"
  fi
  # vectors-<tag> is tagged at the commit <tag> names (an annotated tag resolves to its commit)
  TARGET=$("$GH" api "repos/$REPO/commits/$TAG" --jq .sha) || die "could not resolve the commit of $TAG"
  echo "$TARGET" | grep -Eq '^[0-9a-f]{40}$' || die "$TAG resolves to '$TARGET', not a commit"
  echo "publish: $RELEASE_TAG at $TARGET, prerelease $PRERELEASE"
fi
rm -rf "$WORK"; mkdir -p "$WORK/dl" "$WORK/index" "$WORK/shards" "$WORK/files"
fetch() {  # <asset name>: into $WORK/dl, through gh, or anonymously from the public release URL
  if [ -n "$GH_OK" ]; then
    "$GH" release download "$TAG" --repo "$REPO" --dir "$WORK/dl" --pattern "$1"
  else
    curl -fsSL --retry 5 --retry-delay 10 -o "$WORK/dl/$1" "https://github.com/$REPO/releases/download/$TAG/$1"
  fi
}
RELEASE_JSON=$WORK/library-release.json   # when <tag> was published, and its assets' digests
if [ -n "$GH_OK" ]; then
  "$GH" api "repos/$REPO/releases/tags/$TAG" > "$RELEASE_JSON" || die "could not read the release $TAG"
else
  curl -fsSL "https://api.github.com/repos/$REPO/releases/tags/$TAG" -o "$RELEASE_JSON" || die "could not read the release $TAG"
fi
CREATED=$(jq -r .published_at "$RELEASE_JSON")
echo "$CREATED" | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' || die "could not read when $TAG was published"
endgroup

# ─── 1 index ───────────────────────────────────────────────────────────────
group "1 index: the search index of $TAG"
ARCHIVE=otzaria-library-index.tar.zst
fetch "$ARCHIVE.manifest.json"
fetch otzaria-library-index.provenance.json
manifest="$WORK/dl/$ARCHIVE.manifest.json"
provenance="$WORK/dl/otzaria-library-index.provenance.json"
# Before the parts download: the provenance names this release, the manifest's archive and
# this release's full DB (its digest, or its split manifest's sha256). A missing field fails.
archive_sha=$(jq -r '.sha256 // empty' "$manifest")
echo "$archive_sha" | grep -Eqx '[0-9a-f]{64}' || die "the index manifest states no sha256 of its archive"
[ "$(jq -r '.archive // empty' "$manifest")" = "$ARCHIVE" ] || die "the index manifest does not describe $ARCHIVE"
prov() { jq -r "$1 // empty" "$provenance"; }
[ "$(prov .schemaVersion)" = 1 ] || die "the index provenance is not a schema 1 provenance"
[ "$(prov .libraryReleaseTag)" = "$TAG" ] \
  || die "the index provenance names release '$(prov .libraryReleaseTag)': this index is another release's, not $TAG's"
[ "$(prov .indexArchive)" = "$ARCHIVE" ] && [ "$(prov .indexArchiveSha256)" = "$archive_sha" ] \
  || die "the index provenance names archive '$(prov .indexArchive)' sha256 '$(prov .indexArchiveSha256)', not the manifest's $ARCHIVE $archive_sha"
db=$(jq -c "$FULL_DB_ASSET_JQ" "$RELEASE_JSON")
db_name=$(printf '%s' "$db" | jq -r '.name // empty')
[ -n "$db_name" ] || die "$TAG carries no full DB, so the index provenance's database cannot be checked"
if [ -n "$(printf '%s' "$db" | jq -r '.manifest // empty')" ]; then
  fetch "$db_name.manifest.json"
  db_sha=$(jq -r '.sha256 // empty' "$WORK/dl/$db_name.manifest.json")
else
  db_sha=$(printf '%s' "$db" | jq -r '.digest // empty'); db_sha=${db_sha#sha256:}
fi
echo "$db_sha" | grep -Eqx '[0-9a-f]{64}' || die "$TAG publishes no sha256 of $db_name, so the index provenance's database cannot be checked"
[ "$(prov .seforimDbZstSha256)" = "$db_sha" ] \
  || die "the index provenance names database sha256 '$(prov .seforimDbZstSha256)', not $db_name's $db_sha: this index was built from another database"
# The parts' paths in an array, never split on whitespace: --state and --work may hold any character.
parts=()
while IFS= read -r part; do
  echo "$part" | grep -Eqx 'otzaria-library-index\.tar\.zst\.part-[0-9]+' || die "the index manifest names an unexpected part '$part'"
  parts+=("$WORK/dl/$part")
done < <(jq -r '.parts[].name' "$manifest")
[ "${#parts[@]}" -gt 0 ] || die "the index manifest lists no parts"
for part in "${parts[@]}"; do fetch "${part##*/}"; done
jq -r '.parts[] | "\(.sha256)  \(.name)"' "$manifest" | (cd "$WORK/dl" && sha256sum -c --quiet -) || die "an index part does not hash to its manifest"
whole=$(cat "${parts[@]}" | sha256sum | cut -d' ' -f1)
[ "$whole" = "$archive_sha" ] || die "the index archive hashes to $whole, not the $archive_sha its manifest and provenance state"
cat "${parts[@]}" | "$ZSTD" -dc --long=31 | tar -x -C "$WORK/index"
rm -f "${parts[@]}"
INDEX=$WORK/index/index
[ -f "$INDEX/meta.json" ] || die "the archive holds no index/meta.json"
echo "index: $(du -sh "$INDEX" | cut -f1), $(jq -r .searchEngineVersion "$WORK/dl/otzaria-library-index.provenance.json") engine"
endgroup

# ─── 2 plan ────────────────────────────────────────────────────────────────
group "2 plan"
# The plugin's export_semantic_plan (PLUGIN_REV): the recipe comes from the model's
# chunking_identity, PDF lines are left out, and keys are recomputed from the text of a
# schema 4 index (held to its chunkKey column on schema 5). POC: every release is a base, so
# the plan is split against the warehouse only, never against the previous ledger.
SPLIT=(--warehouse "$WAREHOUSE")
if [ -n "$FRESH" ]; then SPLIT=(); echo "no warehouse yet at $WAREHOUSE: every text is planned"; fi
"$EXPORT" --index "$INDEX" --library-version "$VERSION" --release-tag "$TAG" \
  --model "$FAMILY" --passage-quantization "$PASSAGE_QUANTIZATION" \
  ${SPLIT[@]+"${SPLIT[@]}"} --created-at "$CREATED" --out "$WORK/plan" | tee "$WORK/plan.log"
PLAN=$WORK/plan
jq -e '.format == "otzaria-vector-plan" and .version == 1' "$PLAN/plan-manifest.json" >/dev/null || die "the plan is not an otzaria-vector-plan version 1"
[ "$(jq -r .library_release_tag "$PLAN/plan-manifest.json")" = "$TAG" ] || die "the plan was made for another release"
[ "$(jq -r .parity.mismatches "$PLAN/plan-manifest.json")" = 0 ] || die "the planner's key parity (G2) failed"
[ "$(jq -r .passage_package.checksum "$PLAN/embed-manifest.json")" = "$PASSAGE_PACKAGE_CHECKSUM" ] || die "the plan names another passage package"
TO_EMBED=$(jq -r .records "$PLAN/embed-manifest.json")
jq -c .counts "$PLAN/plan-manifest.json"
echo "to embed: $TO_EMBED"
endgroup

# ─── 3 embed + 4 warehouse ─────────────────────────────────────────────────
group "3 embed: $TO_EMBED text(s) the warehouse lacks"
EMBEDDED_BY="none: every vector came from the warehouse"
if [ "$TO_EMBED" -eq 0 ]; then
  [ -z "$FRESH" ] || die "the plan has no text to embed and there is no warehouse: nothing to assemble a release from"
  echo "every text of the plan has its vector in the warehouse already"
  endgroup
else
  if [ "$TO_EMBED" -lt "$CPU_EMBED_MAX" ]; then
    # Few texts: ONNX Runtime with the fp32 package on this CPU, the reference itself, which needs
    # no parity certificate (the GPU worker's could not sample 1,000 texts from fewer than 959).
    PKG=$("$PY" "$HERE/model_package.py" fetch --cache "$STATE/model-cache" --checksum "$PASSAGE_PACKAGE_CHECKSUM" \
      --graph "$MODEL_GRAPH" --repo "$MODEL_REPO" --revision "$MODEL_REVISION" --token-env OTZARIA_HF_TOKEN) \
      || die "could not fetch the passage package $PASSAGE_PACKAGE_CHECKSUM"
    ORT=$(onnx_runtime) || die "the venv holds no ONNX Runtime library for embed-shard — run bootstrap_runner.sh"
    echo "on the CPU: embed-shard in $CPU_PROCESSES process(es) of $CPU_THREADS thread(s), ONNX Runtime $ORT"
    EMBEDDED_BY="otzaria-semantic-search embed-shard (onnxruntime, cpu)"
    OTZARIA_ONNX_RUNTIME=$ORT "$CLI" embed-shard --plan "$PLAN" --model-file "$PKG/$MODEL_GRAPH" \
      --processes "$CPU_PROCESSES" --threads "$CPU_THREADS" --out "$WORK/shards/cpu"
  else
    EMBEDDED_BY="seforim-gpu-worker 1.0"
    skip=0; n=0
    while [ "$skip" -lt "$TO_EMBED" ]; do
      "$PY" "$HERE/embed_worker.py" --plan "$PLAN" --out "$WORK/shards/$(printf 's%03d' "$n")" \
        --skip "$skip" --take "$EMBED_WINDOW" --cache "$STATE/model-cache" \
        --repo "$MODEL_REPO" --revision "$MODEL_REVISION" --graph "$MODEL_GRAPH" --token-env OTZARIA_HF_TOKEN
      skip=$((skip + EMBED_WINDOW)); n=$((n + 1))
    done
  fi
  endgroup
  group "4 warehouse: add the shards, held to the plan"
  CREATE=()
  [ -z "$FRESH" ] || CREATE=(--create --model "$FAMILY" --passage-quantization "$PASSAGE_QUANTIZATION")
  "$CLI" warehouse-add --warehouse "$WAREHOUSE" ${CREATE[@]+"${CREATE[@]}"} --plan "$PLAN" --shards "$WORK/shards"
  endgroup
fi

# ─── 5 assemble + gates ────────────────────────────────────────────────────
group "5 assemble a base, then the gates"
BUILT_BY=$(jq -cn --arg repo "$REPO" --arg run "${GITHUB_RUN_ID:-manual}" --arg sha "${GITHUB_SHA:-$(git -C "$HERE" rev-parse HEAD 2>/dev/null || echo unknown)}" \
  --arg sidecar "$SIDECAR_REV" --arg plugin "$PLUGIN_REV" --arg worker "$EMBEDDED_BY" \
  '{repository: $repo, workflow: "build-library-vectors", runId: $run, commit: $sha, sidecar: $sidecar, plugin: $plugin, worker: $worker}')
"$CLI" assemble --kind base --plan "$PLAN" --warehouse "$WAREHOUSE" --out "$WORK/release" \
  --created-at "$CREATED" --built-by "$BUILT_BY"
set +e
"$CLI" assemble --verify --plan "$PLAN" --warehouse "$WAREHOUSE" --out "$WORK/release"
verified=$?
set -e
jq -c . "$WORK/release/gates.json" 2>/dev/null || true
[ "$verified" -eq 0 ] || { echo "::error::a gate failed (assemble --verify exited $verified); nothing is published"; exit 2; }
endgroup

group "5 validate the release on its index: G3 coverage, G4 resolution, G6 retrieval"
# What assemble --verify cannot check, since it needs the index: the plugin's validator installs
# the release as a device installs it (into a set under the work directory), and holds it to the
# index and the plan (G3, G4) and the recall of its scan with the int8 query model the
# application runs against the exact f32 scan of the warehouse (G6). Its report is published.
QPKG=$("$PY" "$HERE/model_package.py" fetch --cache "$STATE/model-cache" --checksum "$QUERY_PACKAGE_CHECKSUM" \
  --graph "$QUERY_GRAPH" --repo "$MODEL_REPO" --revision "$MODEL_REVISION" --token-env OTZARIA_HF_TOKEN) \
  || die "could not fetch the query package $QUERY_PACKAGE_CHECKSUM"
ORT=$(onnx_runtime) || die "the venv holds no ONNX Runtime library for the validator — run bootstrap_runner.sh"
set +e
TMPDIR="$WORK" "$VALIDATE" --index "$INDEX" --release "$WORK/release" --plan "$PLAN" \
  --warehouse "$WAREHOUSE" --model "$QPKG/$QUERY_GRAPH" --model-identity "$FAMILY" --onnx-runtime "$ORT" \
  --threads "$(getconf _NPROCESSORS_ONLN)" --report "$WORK/release/validation.json"
validated=$?
set -e
jq -c '{passed, gates: [.gates[] | {gate, status}]}' "$WORK/release/validation.json" 2>/dev/null || true
[ "$validated" -eq 0 ] || { echo "::error::validate_semantic_vectors exited $validated: the release did not pass G3, G4 and G6; nothing is published"; exit 2; }
endgroup

# ─── 6 files ───────────────────────────────────────────────────────────────
group "6 the published files"
REL=$WORK/release/release.json
STEM="otzaria-vectors-$(jq -r '.identityDigest[0:8]' "$REL")-v$(jq -r .toLibraryVersion "$REL")-base"
"$ZSTD" -q -T0 -10 "$WORK/release/segment.oxv" -o "$WORK/files/$STEM.oxv.zst"
size=$(stat -c %s "$WORK/files/$STEM.oxv.zst")
if [ "$size" -ge "$PART_SIZE" ]; then
  split -b "$PART_SIZE" -d -a 3 "$WORK/files/$STEM.oxv.zst" "$WORK/files/$STEM.oxv.zst.part-"
  rm "$WORK/files/$STEM.oxv.zst"
fi
DATA=()
while IFS= read -r f; do DATA+=("$f"); done < <(find "$WORK/files" -maxdepth 1 -name "$STEM.oxv.zst*" | sort)
FILE_ARGS=()
for f in "${DATA[@]}"; do FILE_ARGS+=(--files "$f"); done
"$CLI" release-files --release "$REL" --compression zstd "${FILE_ARGS[@]}" --out "$WORK/files/$STEM.manifest.json" | tee "$WORK/release-files.log"
MANIFEST_SHA=$(sed -n 's/^Manifest SHA-256 \([0-9a-f]\{64\}\)$/\1/p' "$WORK/release-files.log" | tail -n1)
[ -n "$MANIFEST_SHA" ] || die "release-files printed no manifest SHA-256"
[ "$(sha256sum "$WORK/files/$STEM.manifest.json" | cut -d' ' -f1)" = "$MANIFEST_SHA" ] || die "the manifest written is not the one release-files names"
echo "manifest $STEM.manifest.json sha256 $MANIFEST_SHA; data: ${#DATA[@]} file(s)"
endgroup

if [ "$MODE" = dry-run ]; then
  echo "dry run: built and verified $STEM; nothing published, the ledger is not persisted"
  echo "work directory kept: $WORK"
  exit 0
fi

# ─── 7 publish, then persist the ledger ────────────────────────────────────
group "7 publish $RELEASE_TAG"
notes="$WORK/notes.md"
{
  echo "Semantic vectors for library release $TAG (base)."
  echo
  echo "Manifest: \`$STEM.manifest.json\`, SHA-256 \`$MANIFEST_SHA\` — check it before trusting the files it lists."
  echo
  echo "Gates: \`gates.json\` (assemble --verify) and \`validation.json\` (validate_semantic_vectors on the release index: G3, G4, G6)."
} > "$notes"
# A draft fires no `release` event and creates no tag: nothing is published until every
# file is in place and checked.
"$GH" release create "$RELEASE_TAG" --repo "$REPO" --draft --target "$TARGET" --prerelease="$PRERELEASE" \
  --latest=false --title "Library vectors $TAG" --notes-file "$notes"
UPLOADS=("${DATA[@]}" "$WORK/release/gates.json" "$WORK/release/validation.json" \
  "$WORK/files/$STEM.manifest.json")   # data first, the manifest last
for f in "${UPLOADS[@]}"; do
  "$GH" release upload "$RELEASE_TAG" "$f" --repo "$REPO"
done
expected=$(for f in "${UPLOADS[@]}"; do printf '%s\t%s\n' "$(basename "$f")" "$(stat -c %s "$f")"; done | sort)
actual=$("$GH" release view "$RELEASE_TAG" --repo "$REPO" --json assets --jq '.assets[] | "\(.name)\t\(.size)"' | sort) \
  || die "could not list the assets of the draft $RELEASE_TAG; it stays a draft"
[ "$actual" = "$expected" ] || die "the draft $RELEASE_TAG does not hold the files built (expected: $(printf '%s' "$expected" | tr '\t\n' '  '); found: $(printf '%s' "$actual" | tr '\t\n' '  ')); it stays a draft"
"$GH" release edit "$RELEASE_TAG" --repo "$REPO" --draft=false --latest=false
endgroup

group "persist the ledger of $TAG"
next="$LEDGER.next"
rm -rf "$next"; mkdir -p "$next"
cp "$WORK"/release/ledger-v"$VERSION".* "$WORK"/release/pairs-v"$VERSION".bin "$next/"   # the ledger: keys, pairs, manifest
jq -n --arg tag "$TAG" --arg release "$RELEASE_TAG" --arg manifest "$STEM.manifest.json" --arg sha "$MANIFEST_SHA" \
  '{published: $release, library_release_tag: $tag, manifest: $manifest, manifest_sha256: $sha}' > "$next/published.json"
rm -rf "$LEDGER.prev"; [ ! -d "$LEDGER" ] || mv "$LEDGER" "$LEDGER.prev"
mv "$next" "$LEDGER"; rm -rf "$LEDGER.prev"
rm -rf "$WORK"
echo "published $RELEASE_TAG; ledger v$VERSION persisted in $LEDGER"
endgroup
