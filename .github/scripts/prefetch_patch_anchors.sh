#!/usr/bin/env bash
# Fetch the patch fan's anchor DBs in parallel while the job is busy elsewhere,
# through a cache that OUTLIVES a failed attempt.
#
# The fan reconstitutes every anchor from that release's 1.3 GB seforim.db.zst.
# Serially, in the fan itself, that cost run 33865604251 110-135 s per anchor
# (~10 min of the 2015 s step) with the CPU idle. The anchor tags are known long
# before: prior-versions.tsv exists before "Generate Seforim Database", which
# then burns ~36 minutes of CPU without touching the network. This script runs
# in the background across that window and leaves each anchor's verified .zst in
# <dest>/<tag>/seforim.db.zst on the workspace disk (never on the 16 GiB tmpfs
# build/, which has no room for 6.5 GB of anchors).
#
#   prefetch_patch_anchors.sh start <anchors-tsv> <dest-dir>
#   prefetch_patch_anchors.sh run   <anchors-tsv> <dest-dir>   (internal)
#   prefetch_patch_anchors.sh abort <dest-dir>
#   prefetch_patch_anchors.sh cache-report
#
# <anchors-tsv> is the output of patch_fan_anchors.sh — the SAME derivation the
# fan walks, so the two can never disagree about which tag an offset means.
#
# ── The durable anchor cache ────────────────────────────────────────────────
# <dest-dir> is run-scoped: "Clean run-scoped disk leftovers" deletes it, and
# must, because a half-finished download must never be inherited. But the thing
# it holds is IMMUTABLE — a published release asset, content-addressed by the
# release's own tag, byte-identical on every attempt. Run 34021998271 downloaded
# 4 × 1.3 GB (237-249 s each), failed 45 min later on an unrelated schema
# assert, deleted the lot, and its successful retry re-downloaded exactly the
# same bytes: ~16 min of pure loss for inputs that had not changed.
#
# So every verified anchor is ALSO stored under $PATCH_ANCHOR_CACHE_DIR
# (default: ${XDG_CACHE_HOME:-$HOME/.cache}/seforimlibrary/patch-anchors, the
# same durable per-runner cache root the image embedder uses) — real disk, never
# the tmpfs, never the workspace, and untouched by any cleanup. A later run
# reuses an entry only after re-verifying it against the release's own published
# size and digest; anything else is evicted with a ::warning:: and downloaded
# again. Bound: the $PATCH_ANCHOR_CACHE_KEEP (8) most recently used anchors,
# nothing older than $PATCH_ANCHOR_CACHE_MAX_AGE_DAYS (30) days — at 1.3 GB per
# anchor that is ~11 GB, against ~860 GB free on the runner's disk. Entries are
# hardlinked into place where the filesystem allows it, so the cache usually
# costs no extra bytes at all.
#
# Per anchor it writes <dest>/<tag>/.done whose FIRST line is the verdict the fan
# reads and whose remaining lines are the human timing report:
#
#   ok           <dest>/<tag>/seforim.db.zst exists and matches the release
#                asset's published size (and digest, when GitHub has computed it)
#   unpatchable  the shared pre-download patchability check rejected the anchor,
#                so nothing was downloaded
#   failed       something went wrong; the fan falls back to its serial download
#
# Nothing here can fail the job: `start` only forks, and every fan-side use of a
# marker degrades to the unchanged serial `gh release download` path.
set -uo pipefail

self="${BASH_SOURCE[0]}"
mode="${1:-}"

# The pre-download patchability check, run with exactly the arguments the fan
# uses (task C). Note that build/db_schema.json does NOT exist yet while this
# runs — it is dumped after the DB is generated — so patch_anchor_schema.py can
# only reach its first, disk-free source here: the documented list of db_versions
# proven unpatchable (LEGACY_UNPATCHABLE_DB_VERSIONS; that is exactly the v10
# anchor that cost run 33865604251 ~140 s). Everything else degrades to PROCEED,
# and the fan re-runs the very same check later against the real schema dump —
# so an anchor this prefetch fetched can still be skipped there, having cost
# nothing but idle bandwidth.
CHECK_SCRIPT="${PATCH_ANCHOR_CHECK:-.pipeline-control/.github/scripts/patch_anchor_schema.py}"
THIS_SCHEMA="${PATCH_ANCHOR_THIS_SCHEMA:-build/db_schema.json}"
# From the PAYLOAD checkout, like the fan's own invocation: a comparison wider
# than the producer's own table list would skip anchors it would have patched.
CONTRACT_TABLES="${PATCH_ANCHOR_CONTRACT:-generator/common/src/jvmTest/resources/patch_tables_contract.json}"
# 5 parallel `gh release download` is well inside GitHub's rate/concurrency
# budget and saturates the runner's 8-22 MB/s downlink.
PARALLEL="${PREFETCH_PARALLEL:-5}"

# Durable cache root. `${VAR-default}` (not `:-`) on purpose: an explicitly
# empty PATCH_ANCHOR_CACHE_DIR is the off switch, and leaves this script doing
# exactly what it did before the cache existed.
default_cache_dir() {
  if [ -n "${XDG_CACHE_HOME:-}" ]; then
    printf '%s/seforimlibrary/patch-anchors' "$XDG_CACHE_HOME"
  elif [ -n "${HOME:-}" ]; then
    printf '%s/.cache/seforimlibrary/patch-anchors' "$HOME"
  fi
}
CACHE_DIR="${PATCH_ANCHOR_CACHE_DIR-$(default_cache_dir)}"
CACHE_KEEP="${PATCH_ANCHOR_CACHE_KEEP:-8}"
CACHE_MAX_AGE_DAYS="${PATCH_ANCHOR_CACHE_MAX_AGE_DAYS:-30}"
# A pruner killed mid-run (abort signals the whole group) would otherwise leave
# its lock behind and disable the bound for good.
PRUNE_LOCK_STALE_SECONDS="${PATCH_ANCHOR_PRUNE_LOCK_STALE:-600}"
case "$CACHE_KEEP" in ''|*[!0-9]*|0) CACHE_KEEP=8 ;; esac
case "$CACHE_MAX_AGE_DAYS" in ''|*[!0-9]*|0) CACHE_MAX_AGE_DAYS=30 ;; esac
case "$PRUNE_LOCK_STALE_SECONDS" in ''|*[!0-9]*) PRUNE_LOCK_STALE_SECONDS=600 ;; esac
# build/ is a 16 GiB tmpfs on this runner: 8 anchors there would be ~11 GB of
# RAM and would OOM the generator. The cache lives on disk or not at all.
case "${CACHE_DIR:-}" in
  "${GITHUB_WORKSPACE:-/nonexistent-workspace}"/build \
  |"${GITHUB_WORKSPACE:-/nonexistent-workspace}"/build/*)
    echo "::warning::patch-fan anchor cache: $CACHE_DIR is inside the tmpfs build dir — caching disabled for this run"
    CACHE_DIR=""
    ;;
esac

fs_type_of() {  # <path> -> filesystem type of its nearest existing ancestor
  local dir="$1"
  while [ -n "$dir" ] && [ ! -d "$dir" ]; do
    case "$dir" in
      */*) dir="${dir%/*}" ;;
      *)   dir="." ;;
    esac
  done
  [ -n "$dir" ] || dir=/
  stat -f -c '%T' "$dir" 2>/dev/null || echo unknown
}

# …and "not the tmpfs" as a fact about the filesystem, not only about the path:
# a cache root that is RAM-backed however it was configured would trade 11 GB of
# the generator's RAM for the download it saves. Disk or nothing.
if [ -n "$CACHE_DIR" ]; then
  case "$(fs_type_of "$CACHE_DIR")" in
    tmpfs|ramfs)
      echo "::warning::patch-fan anchor cache: $CACHE_DIR is on a RAM-backed filesystem — caching disabled for this run (anchors are never held in RAM)"
      CACHE_DIR=""
      ;;
  esac
fi

cache_enabled() { [ -n "$CACHE_DIR" ]; }

pid_file_for() { printf '%s/.pid' "$1"; }

# ── The LRU rank ────────────────────────────────────────────────────────────
# The bound is "the CACHE_KEEP most recently used entries", so the pruner needs
# a TOTAL order over them. The `.meta` mtime read with `stat --format='%Y'` is
# not one: it is whole seconds, every entry stored or refreshed inside the same
# second ties, and `sort` then falls back to comparing the rest of the line —
# the tag — so v23-x ranked below v25-x and v26-x and the pruner evicted the
# anchor it had just spent ~16 minutes downloading. Sub-second mtimes alone are
# not a fix either: some tmpfs/overlay configurations still stamp whole seconds,
# and the tie comes straight back.
#
# So every entry RECORDS its own rank key in `.meta` (`used_ns=`), on the store
# and on every hit, and the key is
#
#     used_ns = max(the nanosecond clock, the highest used_ns in the cache + 1)
#
# — a Lamport stamp. It is strictly greater than the key of every entry that
# already existed, whatever the clock's resolution and whatever the filesystem
# rounds mtimes to, so "stored or refreshed later" always means "ranks higher"
# and no two entries this version writes can tie — but only because the
# allocation is SERIALIZED (see cache_next_use_key). Unlocked, the read-max →
# write is a lost update: the five fetches allocate concurrently, and on the
# `highest + 1` branch (a stamp from the future, or a `date` without `%N`) a
# five-way tie is routine, not exotic. The secondary key is the
# `.meta` mtime to the nanosecond (`%.9Y`) — deliberately another recency
# signal, never the tag, which is what made the old tie systematically evict the
# NEWEST anchor. It can only decide between entries written by a version older
# than this one, which record no key at all and are therefore ranked by their
# mtime. The age bound still reads that same mtime, exactly as it always did.
mtime_ns() {  # <path> -> mtime in nanoseconds, "0" when stat cannot say
  local raw sec frac
  raw=$(stat --format='%.9Y' "$1" 2>/dev/null) || raw=""
  sec="${raw%%.*}"
  case "$raw" in *.*) frac="${raw#*.}" ;; *) frac="" ;; esac
  # A stat too old for sub-second precision hands the format back unexpanded.
  case "$sec" in
    ''|*[!0-9]*)
      sec=$(stat --format='%Y' "$1" 2>/dev/null) || sec=""
      frac=""
      ;;
  esac
  case "$sec" in ''|*[!0-9]*) printf '0'; return 0 ;; esac
  case "$frac" in *[!0-9]*) frac="" ;; esac
  frac="${frac}000000000"
  printf '%s%s' "$sec" "${frac:0:9}"
}

# A key is "<major>" or, only on the lockless fallback below, "<major>.<minor>".
# Anything else is a key from before this scheme and ranks by its mtime.
cache_use_key() {  # <meta-file> -> that entry's rank key
  local recorded
  recorded=$(sed -n 's/^used_ns=//p' "$1" 2>/dev/null | head -n1)
  case "$recorded" in
    ''|*[!0-9.]*|*.*.*|.*|*.) mtime_ns "$1" ;;  # stored before the key existed
    *) printf '%s' "$recorded" ;;
  esac
}

cache_alloc_use_key() {  # -> the next key; the CALLER holds the allocation lock
  local now meta highest key
  now=$(date +%s%N 2>/dev/null) || now=""
  # `%N` is GNU; a date without it prints it back, so fall to whole seconds —
  # the Lamport floor below then keeps the order strict on its own.
  case "$now" in ''|*[!0-9]*) now="$(date +%s 2>/dev/null)000000000" ;; esac
  case "$now" in ''|*[!0-9]*) now=0 ;; esac
  # The floor is every key on disk AND the last key this cache handed out: the
  # `.meta` a caller is about to write is not on disk while the next caller
  # scans, so the scan alone cannot see it and would hand out the same key.
  highest=$(cat "$CACHE_DIR/.stamp" 2>/dev/null) || highest=""
  case "$highest" in ''|*[!0-9]*) highest=0 ;; esac
  for meta in "$CACHE_DIR"/*/.meta; do
    [ -f "$meta" ] || continue
    key=$(cache_use_key "$meta")
    key="${key%%.*}"
    case "$key" in ''|*[!0-9]*) continue ;; esac
    if [ "$((10#$key))" -gt "$((10#$highest))" ]; then highest="$key"; fi
  done
  # 10# because a key that fell back to an mtime of 0 is a run of zeros, which
  # arithmetic would otherwise read as octal.
  if [ "$((10#$now))" -gt "$((10#$highest))" ]; then key="$now"; else key="$((10#$highest + 1))"; fi
  printf '%s\n' "$key" > "$CACHE_DIR/.stamp" 2>/dev/null || true
  printf '%s' "$key"
}

cache_next_use_key() {  # -> a rank key strictly above every key in the cache
  # Allocation is a read-modify-write over shared state and the five fetches run
  # it CONCURRENTLY — `cache_mark_used` on every hit, `cache_store` on every
  # store, both from `fetch_one … &` subshells. Serialize it with flock(1)
  # (util-linux; present on the WSL/ext4 runner), on a lock file inside the
  # cache dir, opened for APPEND so taking the lock cannot truncate the stamp
  # any writer is holding. `.prune.lock` is a different lock for a different
  # critical section (pruner vs pruner) and stays as it is.
  local key
  if [ -n "$CACHE_DIR" ] && command -v flock > /dev/null 2>&1; then
    key=$(
      exec 9>> "$CACHE_DIR/.stamp.lock" 2>/dev/null || exit 1
      # Bounded: the critical section is a directory scan, so 30s is already
      # absurd, and a prefetch must never hang on its own bookkeeping — a wait
      # that times out drops to the unique-by-construction key below.
      flock -w 30 9 2>/dev/null || exit 1
      cache_alloc_use_key
    )
    case "$key" in ''|*[!0-9]*) ;; *) printf '%s' "$key"; return 0 ;; esac
  fi
  # No flock, or no lock file to take: the allocation cannot be serialized, so
  # make the key unique by CONSTRUCTION instead — `<key>.<BASHPID>`, one minor
  # part per writer. cache_use_key accepts that shape and cache_prune ranks the
  # two parts as separate numeric keys, so two racers handed the same major part
  # still get a total order instead of falling through to the tag.
  printf '%s.%s' "$(cache_alloc_use_key)" "${BASHPID:-$$}"
}

cache_mark_used() {  # <entry-dir> — this entry is in use, so it ranks newest
  # $BASHPID, not $$: the five fetches are SUBSHELLS of one script and share
  # $$, so two of them marking the same entry would write the same temp file —
  # one truncating what the other had just written, and the survivor renaming a
  # .meta that has lost tag=/size=/sha256= (or is empty). A per-subshell name
  # makes the two writers independent and the rename the only interleaving,
  # which is the atomic one. (Today's anchor list cannot repeat a tag, so this
  # is a guard rather than a live bug — but it is what the comment below claims.)
  local meta="$1/.meta" tmp="$1/.meta.${BASHPID:-$$}"
  # Written to a temp and renamed: a concurrent reader never sees a half-written
  # .meta, and nothing here can cost the sha256 the entry is verified against.
  # `grep` failing (an empty or unreadable .meta) leaves the file untouched.
  if grep -v '^used_ns=' "$meta" > "$tmp" 2>/dev/null \
     && printf 'used_ns=%s\n' "$(cache_next_use_key)" >> "$tmp" 2>/dev/null \
     && mv -f "$tmp" "$meta" 2>/dev/null; then
    return 0
  fi
  rm -f "$tmp" 2>/dev/null
  # A hit must never fail over its own bookkeeping. The mtime is still worth
  # refreshing — it is the age bound's input, and the pruner's rank for an entry
  # that has no key of its own. An entry that already HAS one keeps it: a cache
  # whose entry dirs cannot be written ranks that hit by its previous key and
  # may evict it. That is a degradation of an unwritable cache, not of a hit.
  touch "$meta" 2>/dev/null || true
}

asset_meta() {  # <tag> -> "<size>\t<digest>" of that release's seforim.db.zst
  local tag="$1" row
  row=$(gh api "repos/$GITHUB_REPOSITORY/releases/tags/$tag" \
    --jq '.assets[] | select(.name=="seforim.db.zst") | [(.size|tostring), (.digest // "")] | @tsv') || return 1
  [ -n "$row" ] || return 1
  printf '%s' "$row"
}

verify_asset() {  # <file> <tag> <expected-size> <expected-digest> <actual-sha256>
  local file="$1" tag="$2" expected_size="$3" expected_digest="$4" actual_sha="$5" actual_size
  actual_size=$(stat --format='%s' "$file") || return 1
  [ "$actual_size" = "$expected_size" ] || {
    echo "size mismatch for $tag: got $actual_size, release says $expected_size"
    return 1
  }
  # GitHub computes the digest asynchronously and old releases carry none, so
  # size is the always-available check and the digest is the strong one.
  if [ -n "$expected_digest" ] && [ "sha256:$actual_sha" != "$expected_digest" ]; then
    echo "digest mismatch for $tag: got sha256:$actual_sha, release says $expected_digest"
    return 1
  fi
}

cache_lookup() {  # <tag> <expected-size> <expected-digest> <dest-file>
  # 0 only when <dest-file> now holds the anchor and its sha256 was re-verified.
  local tag="$1" size="$2" digest="$3" out="$4"
  local dir file recorded expected actual actual_size
  cache_enabled || return 1
  dir="$CACHE_DIR/$tag"
  file="$dir/seforim.db.zst"
  [ -s "$file" ] || return 1
  actual_size=$(stat --format='%s' "$file" 2>/dev/null) || actual_size=0
  if [ "$actual_size" != "$size" ]; then
    echo "::warning::patch-fan anchor cache: $tag is cached at $actual_size bytes but the release says $size — evicting and downloading it again"
    rm -rf "$dir"
    return 1
  fi
  # The release's published digest is the authority. A release that publishes
  # none falls back to the digest recorded when THIS cache verified the
  # download against the published size — the same guarantee a fresh download
  # gets today, plus proof the entry has not rotted on disk since.
  recorded=$(sed -n 's/^sha256=//p' "$dir/.meta" 2>/dev/null | head -n1)
  expected="$digest"
  [ -n "$expected" ] || expected="${recorded:+sha256:$recorded}"
  if [ -z "$expected" ]; then
    echo "::warning::patch-fan anchor cache: nothing to verify $tag against (the release publishes no digest and the entry records none) — evicting and downloading it again"
    rm -rf "$dir"
    return 1
  fi
  actual="sha256:$(sha256sum "$file" | cut -d' ' -f1)"
  if [ "$actual" != "$expected" ]; then
    echo "::warning::patch-fan anchor cache: digest mismatch for $tag (cached $actual, expected $expected) — evicting and downloading it again"
    rm -rf "$dir"
    return 1
  fi
  # Hardlink when the cache and the workspace share a filesystem (the runner's
  # normal case: no copy, no extra bytes). The fan only ever reads and then
  # unlinks its copy, so the shared inode is never mutated.
  # A `cp` that dies part-way (ENOSPC) would otherwise leave a partial file
  # where the fallback `gh release download --dir` wants to write, and gh
  # refuses to clobber: the anchor would be lost to a failure the cache caused.
  rm -f "$out"
  ln "$file" "$out" 2>/dev/null || cp "$file" "$out" || { rm -f "$out"; return 1; }
  # LRU stamp: this entry is in use, so it is not the one to evict.
  cache_mark_used "$dir"
}

cache_store() {  # <file> <tag> <sha256> <size> <published-digest>
  local file="$1" tag="$2" sha="$3" size="$4" digest="$5" dir tmp
  cache_enabled || return 1
  dir="$CACHE_DIR/$tag"
  mkdir -p "$dir" 2>/dev/null || {
    echo "::warning::patch-fan anchor cache: cannot write $dir — $tag will be downloaded again next attempt"
    return 1
  }
  tmp="$dir/.incoming.$$"
  rm -f "$tmp"
  if ! ln "$file" "$tmp" 2>/dev/null && ! cp "$file" "$tmp"; then
    rm -f "$tmp"
    echo "::warning::patch-fan anchor cache: could not store $tag — it will be downloaded again next attempt"
    return 1
  fi
  # Publish the entry atomically: a half-written file must never look cached.
  mv -f "$tmp" "$dir/seforim.db.zst" || { rm -f "$tmp"; return 1; }
  # used_ns is the pruner's rank key and is computed while this tag's previous
  # .meta (if it had one) is still on disk, so a re-store also ranks newest.
  printf 'tag=%s\nsize=%s\nsha256=%s\npublished_digest=%s\ncached_at=%s\nused_ns=%s\n' \
    "$tag" "$size" "$sha" "$digest" "$(date +%s)" "$(cache_next_use_key)" > "$dir/.meta"
  cache_prune
}

cache_prune() {  # keep the CACHE_KEEP most recently used entries, none older than CACHE_MAX_AGE_DAYS
  local lock lock_mtime lock_age now cutoff cutoff_ns meta key minor
  cache_enabled || return 0
  [ -d "$CACHE_DIR" ] || return 0
  lock="$CACHE_DIR/.prune.lock"
  now=$(date +%s)
  # A lock whose owner was killed (the abort takes down the whole process group)
  # must not disable the bound for ever: past PRUNE_LOCK_STALE_SECONDS it is a
  # corpse, not a race, and the cache would grow without limit behind it.
  if [ -d "$lock" ]; then
    lock_mtime=$(stat --format='%Y' "$lock" 2>/dev/null) || lock_mtime="$now"
    lock_age=$(( now - lock_mtime ))
    if [ "$lock_age" -ge "$PRUNE_LOCK_STALE_SECONDS" ]; then
      echo "patch-fan anchor cache: clearing a stale prune lock (${lock_age}s old, limit ${PRUNE_LOCK_STALE_SECONDS}s)"
      rmdir "$lock" 2>/dev/null || true
    fi
  fi
  # Five fetches run at once and `mkdir` is the atomic primitive every
  # filesystem here agrees on. A pruner that loses the race just skips — the
  # next store prunes again.
  mkdir "$lock" 2>/dev/null || return 0
  cutoff=$(( now - CACHE_MAX_AGE_DAYS * 86400 ))
  # The same cutoff against the same mtime, in the nanoseconds the rank already
  # reads: appending the nine zeros scales a negative cutoff correctly too.
  cutoff_ns="${cutoff}000000000"
  # Recorded rank key first — its major part, then the minor part the lockless
  # fallback appends — and the `.meta` mtime as the documented tie-break; -t so a
  # cache root with a space in its path cannot shift the numeric keys. `-s` plus
  # THREE explicit keys is the point: without it `sort`'s last-resort comparison
  # of the whole line decides a full tie by the third field — the TAG — which
  # ranked the newest anchor last and evicted it. Stable means a full tie keeps
  # input order instead, and input order is never a recency claim.
  for meta in "$CACHE_DIR"/*/.meta; do
    [ -f "$meta" ] || continue
    key=$(cache_use_key "$meta")
    case "$key" in *.*) minor="${key#*.}" ;; *) minor=0 ;; esac
    printf '%s\t%s\t%s\t%s\n' "${key%%.*}" "$minor" "$(mtime_ns "$meta")" "${meta%/.meta}"
  done | sort -t$'\t' -s -k1,1rn -k2,2rn -k3,3rn | {
    rank=0
    while IFS=$'\t' read -r use_key use_minor mtime dir; do
      rank=$((rank + 1))
      if [ "$rank" -le "$CACHE_KEEP" ] && [ "$mtime" -ge "$cutoff_ns" ]; then
        continue
      fi
      echo "patch-fan anchor cache: evicting $(basename "$dir") ($(du -sh "$dir" 2>/dev/null | cut -f1)) — bound is the $CACHE_KEEP most recent, max age ${CACHE_MAX_AGE_DAYS}d"
      rm -rf "$dir"
    done
  }
  rmdir "$lock" 2>/dev/null || true
}

cache_report() {
  local count size
  if ! cache_enabled; then
    echo "patch-fan anchor cache: disabled — every anchor is downloaded again on every attempt"
    return 0
  fi
  if [ ! -d "$CACHE_DIR" ]; then
    echo "patch-fan anchor cache: empty at $CACHE_DIR (bound: $CACHE_KEEP anchors, max age ${CACHE_MAX_AGE_DAYS}d) — kept across runs on purpose"
    return 0
  fi
  count=$(find "$CACHE_DIR" -mindepth 2 -maxdepth 2 -name .meta 2>/dev/null | wc -l | tr -d ' ')
  size=$(du -sh "$CACHE_DIR" 2>/dev/null | cut -f1)
  echo "patch-fan anchor cache: ${count:-0} anchors, ${size:-unknown} at $CACHE_DIR (bound: $CACHE_KEEP anchors, max age ${CACHE_MAX_AGE_DAYS}d) — kept across runs on purpose, no cleanup deletes it"
}

fetch_one() {  # <target_version> <tag> <offset> <dest-dir>
  local version="$1" tag="$2" offset="$3" dest="$4" dir="$4/$2"
  local verdict meta expected_size expected_digest reason sha msg source report=""
  local t_start t_checked t_fetched t_verified

  mkdir -p "$dir"
  t_start=$(date +%s)
  rm -rf "$dir/meta"
  mkdir -p "$dir/meta"
  # A few KB, exactly like the fan's own pre-check fetch; absence is not fatal —
  # but it is NAMED here, instead of leaving gh's bare "no assets match the file
  # pattern" on stderr as the only trace of it (run 34024655297 printed that
  # line twice with nothing saying which asset was missing).
  if ! gh release download "$tag" --pattern 'build_provenance.json' \
       --dir "$dir/meta" 2> "$dir/provenance.err"; then
    reason=$(tr -d '\r' < "$dir/provenance.err" 2>/dev/null | head -n1)
    report+="anchor $tag (offset $offset): release carries no build_provenance.json (${reason:-gh matched no asset}) — the pre-check falls back to the documented unpatchable list"$'\n'
  fi
  rm -f "$dir/provenance.err"
  verdict=$(python3 "$CHECK_SCRIPT" check \
    --this-schema "$THIS_SCHEMA" \
    --anchor-version "$version" \
    --anchor-provenance "$dir/meta/build_provenance.json" \
    --contract-tables "$CONTRACT_TABLES") \
    || verdict="PROCEED pre-check did not run — deferring to the producer"
  rm -rf "$dir/meta"
  t_checked=$(date +%s)
  if [ "${verdict%% *}" = UNPATCHABLE ]; then
    report+="prefetch anchor v$version ($tag): $verdict — not downloaded"$'\n'
    printf 'unpatchable\n%s' "$report" > "$dir/.done"
    return 0
  fi

  # The release asset's own size and digest, fetched ONCE: they name the asset
  # in the log below, key the cache and verify both a cache hit and a download.
  if ! meta=$(asset_meta "$tag"); then
    report+="prefetch anchor v$version ($tag): release publishes no seforim.db.zst asset (or the release API call failed) — the fan falls back to its serial download"$'\n'
    printf 'failed\n%s' "$report" > "$dir/.done"
    return 0
  fi
  IFS=$'\t' read -r expected_size expected_digest <<<"$meta"
  report+="anchor $tag (offset $offset): seforim.db.zst $expected_size bytes${expected_digest:+ $expected_digest}"$'\n'

  rm -f "$dir/seforim.db.zst"
  source=release
  msg=$(cache_lookup "$tag" "$expected_size" "$expected_digest" "$dir/seforim.db.zst" 2>&1) && source=cache
  [ -z "$msg" ] || report+="$msg"$'\n'
  if [ "$source" = cache ]; then
    t_fetched=$(date +%s)
    report+="reused $tag from cache (sha256 ok) in $((t_fetched - t_checked))s — nothing downloaded"$'\n'
  else
    # Absence is NOT expected here — asset_meta saw the asset on the release a
    # moment ago — so this is a real failure. gh's own stderr is the only thing
    # that says why, and it would otherwise land bare in the prefetch log the
    # fan replays (run 34024655297 printed such a line with nothing naming the
    # asset). Capture it and fold it into the line that does.
    if ! gh release download "$tag" --pattern 'seforim.db.zst' --dir "$dir" \
         2> "$dir/download.err"; then
      reason=$(tr -d '\r' < "$dir/download.err" 2>/dev/null | head -n1)
      report+="prefetch anchor v$version ($tag): download of seforim.db.zst failed (${reason:-gh gave no reason}) — the fan falls back to its serial download"$'\n'
      printf 'failed\n%s' "$report" > "$dir/.done"
      rm -f "$dir/seforim.db.zst" "$dir/download.err"
      return 0
    fi
    rm -f "$dir/download.err"
    t_fetched=$(date +%s)
    report+="downloaded $tag in $((t_fetched - t_checked))s"$'\n'
  fi

  # A cache hit was already re-verified byte for byte in cache_lookup, so it is
  # never hashed twice; a fresh download is verified here and then cached.
  if [ "$source" = release ]; then
    sha=$(sha256sum "$dir/seforim.db.zst" | cut -d' ' -f1)
    if ! reason=$(verify_asset "$dir/seforim.db.zst" "$tag" "$expected_size" "$expected_digest" "$sha"); then
      report+="prefetch anchor v$version ($tag): ${reason:-verification failed}"$'\n'
      printf 'failed\n%s' "$report" > "$dir/.done"
      rm -f "$dir/seforim.db.zst"
      return 0
    fi
    msg=$(cache_store "$dir/seforim.db.zst" "$tag" "$sha" "$expected_size" "$expected_digest" 2>&1)
    [ -z "$msg" ] || report+="$msg"$'\n'
  fi
  t_verified=$(date +%s)
  report+="prefetch anchor v$version ($tag) timings: precheck=$((t_checked - t_start))s download=$((t_fetched - t_checked))s verify=$((t_verified - t_fetched))s total=$((t_verified - t_start))s source=$source"$'\n'
  printf 'ok\n%s' "$report" > "$dir/.done"
}

run_prefetch() {  # <anchors-tsv> <dest-dir>
  local anchors="$1" dest="$2" status offset version tag running=0
  mkdir -p "$dest"
  echo "$$" > "$(pid_file_for "$dest")"
  while IFS=$'\t' read -r status offset version tag; do
    [ "$status" = ANCHOR ] || continue
    # The tag is a path component of BOTH the run dir and the durable cache,
    # and cache_lookup evicts with `rm -rf "$CACHE_DIR/$tag"`. `.` and `..` pass
    # the character class, and `..` would resolve that eviction to the shared
    # ~/.cache/seforimlibrary root the image embedder lives in. Git refs cannot
    # be named `.` or `..`, so this only ever refuses a tag that cannot exist.
    case "$tag" in
      ''|.|..|*[!A-Za-z0-9._-]*)
        echo "refusing to prefetch unsafe tag '$tag' (offset $offset)"
        continue
        ;;
    esac
    fetch_one "$version" "$tag" "$offset" "$dest" &
    running=$((running + 1))
    if [ "$running" -ge "$PARALLEL" ]; then
      wait -n 2>/dev/null || true
      running=$((running - 1))
    fi
  done < "$anchors"
  wait
  # cache_store prunes after every write; this second call is what enforces the
  # bound on a run that stored nothing at all (every anchor a cache hit) — the
  # age limit would otherwise only ever be applied by a run that downloaded.
  cache_prune
  cache_report
  echo "prefetch finished"
}

abort_prefetch() {  # <dest-dir> — says what it aborted and why, and never fails
  local dest="$1" pid marker state ok=0 unpatchable=0 failed=0 done_count=0
  for marker in "$dest"/*/.done; do
    [ -f "$marker" ] || continue
    done_count=$((done_count + 1))
    case "$(head -n1 "$marker" 2>/dev/null)" in
      ok) ok=$((ok + 1)) ;;
      unpatchable) unpatchable=$((unpatchable + 1)) ;;
      *) failed=$((failed + 1)) ;;
    esac
  done
  state="$done_count anchors had finished ($ok ok, $unpatchable unpatchable, $failed failed); verified anchors stay in the durable cache"
  pid=$(cat "$(pid_file_for "$dest")" 2>/dev/null || true)
  if [ -z "${pid:-}" ]; then
    echo "patch-fan anchor prefetch: nothing to abort — no background prefetch is recorded in $dest; $state"
    return 0
  fi
  # This runner lives for weeks and pids get reused: only ever signal a process
  # that is still this very script.
  if ! ps -o args= -p "$pid" 2>/dev/null | grep -q 'prefetch_patch_anchors\.sh'; then
    echo "patch-fan anchor prefetch: pid $pid is no longer this script (it had already finished) — nothing signalled; $state"
    return 0
  fi
  # It drives `gh` children and runs in its own session (setsid), so the whole
  # group goes at once and nothing keeps competing for the downlink with the
  # serial fallback.
  if kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null; then
    echo "patch-fan anchor prefetch aborted: signalled process group $pid; $state"
    return 0
  fi
  echo "::warning::patch-fan anchor prefetch: could not signal pid $pid — it may still be downloading in the background; $state"
  return 0
}

case "$mode" in
  start)
    anchors="${2:-}"; dest="${3:-}"
    [ -n "$anchors" ] && [ -n "$dest" ] || { echo "usage: ${0##*/} start <anchors-tsv> <dest-dir>" >&2; exit 2; }
    mkdir -p "$dest"
    rm -f "$dest/.pid" "$dest/.started"
    # setsid: its own session, so `abort` and the run-scoped cleanup can take
    # down the whole group (script plus every `gh` child) in one signal.
    setsid nohup bash "$self" run "$anchors" "$dest" > "$dest/.log" 2>&1 &
    : > "$dest/.started"
    echo "patch-fan anchor prefetch started in the background (dest=$dest, parallel=$PARALLEL, cache=${CACHE_DIR:-disabled})"
    ;;
  run)
    anchors="${2:-}"; dest="${3:-}"
    [ -n "$anchors" ] && [ -n "$dest" ] || exit 2
    run_prefetch "$anchors" "$dest"
    ;;
  abort)
    dest="${2:-}"
    [ -n "$dest" ] || { echo "usage: ${0##*/} abort <dest-dir>" >&2; exit 2; }
    abort_prefetch "$dest"
    ;;
  cache-report)
    cache_report
    ;;
  *)
    echo "usage: ${0##*/} start|run|abort|cache-report ..." >&2
    exit 2
    ;;
esac
exit 0
