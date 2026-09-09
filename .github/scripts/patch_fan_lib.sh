#!/usr/bin/env bash
# Shared machinery for the weekly DB release's "Produce + verify patch fan"
# step: one anchor start to finish, the heartbeat that keeps the step from
# going dark, and the batch driver's drain. Sourced, not run.
#
# WHY THIS EXISTS
# ---------------
# GitHub Actions refuses to LOAD a workflow whose single `run:` template is
# longer than 21,000 characters. Run 34195296928 never started a job:
# "Invalid workflow file … (Line: 1974, Col: 14): Exceeded max expression
# length 21000" — the fan's body had grown to 22,592 characters. Taking the
# function definitions out leaves the step body as the driver alone.
#
# SHELL SEMANTICS
# ---------------
# Sourced into the step's own shell, so nothing about how any of this runs
# changes: the caller's `set -euo pipefail` is inherited verbatim (that is why
# this is sourced and not executed), the variables below are the step's own
# globals, and `produce_anchor` still forks from the step's shell when the
# driver backgrounds it. Deliberately NO `set` line here — re-issuing the
# options would be a no-op for the one caller and would silently impose them
# on anyone else who sources this.
#
# Read at SOURCE time: RUNNER_TEMP, GITHUB_RUN_ID, GITHUB_RUN_ATTEMPT and the
# optional PATCH_FAN_HEARTBEAT_SECONDS (the heartbeat's cadence and paths).
# Read at CALL time, from the driver's globals: THIS_VER, THIS_SCHEMA,
# PREFETCH_DIR, PREFETCH_WAIT_SECONDS, PATCH_MAIN_CLASS, PATCH_JVM_ARGS,
# PATCH_CLASSPATH and the BATCH_* arrays.

# קורא db_schema_version מ-DB; טבלה/שורה חסרה → ERROR (release DB חייב מוטבע).
# sqlite3 זמין ברנרים ה-self-hosted (בשימוש גם בוורקפלואי delta-real-diff-*).
read_schema() {
  local db="$1" v
  if ! v=$(sqlite3 "$db" "SELECT value FROM schema_meta WHERE key='db_schema_version'" 2>/dev/null); then
    echo "::error::schema_meta missing/unreadable in $db (release DB must be stamped)" >&2
    return 1
  fi
  [ -n "$v" ] || { echo "::error::db_schema_version row missing in $db (release DB must be stamped)" >&2; return 1; }
  printf '%s' "$v"
}

# One anchor, start to finish: pre-check, reconstitute, produce,
# verify, clean up. Every path it touches is scoped to its own offset
# so two of these can run side by side, it writes to its own log, and
# its exit status is the only channel back to the driver: 0 for a
# produced patch AND for every legitimate skip, non-zero only for a
# genuine failure — which fails the step exactly as `set -e` inside
# the serial loop always did.
produce_anchor() {  # <offset> <target-version> <tag>
  local OFFSET="$1" TARGET_VER="$2" TAG="$3"
  local ANCHOR_DIR="prev-dbs/anchor-$OFFSET"
  local META_DIR="prev-meta/anchor-$OFFSET"
  local PREV_DB="$ANCHOR_DIR/seforim.db"
  local PATCH_OUT="$PWD/patches/patch-v${TARGET_VER}-v${THIS_VER}.db"
  local PRECHECK WAIT_BUDGET PREFETCH_STATE PREFETCH_WAITED
  local PREV_SCHEMA PRODUCE_RC REASON
  local T_START T_DOWNLOADED T_EXTRACTED T_DONE
  echo "=== Producing patch v${TARGET_VER} → v${THIS_VER} (offset $OFFSET, tag=$TAG) ==="

  # Reconstituting an anchor costs 110–135 s of download plus a
  # decompress; run 33865604251 paid ~140 s for the v10 anchor only
  # to be told it was unpatchable. patch_anchor_schema.py answers the
  # same question first, from the anchor's own build_provenance.json
  # `db_schema` block (a few KB, exact from db_version 27 on — the
  # published v26 provenance is schema_version 3 and carries none) or —
  # for releases that predate that block — from a documented list of
  # db_versions proven unpatchable by this repository's history.
  # It is advisory in one direction only: PROCEED is NOT a
  # patchability claim, the producer's exit-3 + "<out>.unpatchable"
  # marker below stays the sole authority, and the pre-check itself
  # can never fail the release (any surprise degrades to PROCEED).
  # --contract-tables scopes the column comparison to the producer's
  # own table list. It comes from the PAYLOAD checkout — the same
  # commit whose PatchDbProducer runs below, not the control-plane
  # checkout — because a comparison wider than that list would skip
  # anchors the producer would still have patched.
  rm -rf "$META_DIR"
  mkdir -p "$META_DIR"
  # An anchor without provenance is normal (every release before
  # v27 carries none) but gh's bare "no assets match the file
  # pattern" never says which asset was missing — this does.
  if ! gh release download "$TAG" \
       --pattern 'build_provenance.json' \
       --dir "$META_DIR" 2>"$META_DIR/gh.err"; then
    echo "anchor v${TARGET_VER} ($TAG): release carries no build_provenance.json ($(tr -d '\r' < "$META_DIR/gh.err" | head -n1)) — the pre-check falls back to the documented unpatchable list"
  fi
  rm -f "$META_DIR/gh.err"
  PRECHECK=$(python3 .pipeline-control/.github/scripts/patch_anchor_schema.py check \
    --this-schema build/db_schema.json \
    --anchor-version "$TARGET_VER" \
    --anchor-provenance "$META_DIR/build_provenance.json" \
    --contract-tables generator/common/src/jvmTest/resources/patch_tables_contract.json) \
    || PRECHECK="PROCEED pre-check did not run — deferring to the producer"
  rm -rf "$META_DIR"
  echo "pre-check: $PRECHECK"
  if [ "${PRECHECK%% *}" = UNPATCHABLE ]; then
    echo "::warning::anchor v${TARGET_VER} ($TAG): ${PRECHECK#* } — pre-download schema check declared the anchor unpatchable; skip anchor"
    return 0
  fi

  rm -rf "$ANCHOR_DIR"
  mkdir -p "$ANCHOR_DIR"
  # A1: this anchor was very probably downloaded and verified (size,
  # and digest when GitHub published one) while the DB was being
  # generated. Wait for THIS tag's marker; anything short of a
  # verified hit — no marker inside the budget, a failed or
  # unpatchable verdict, a vanished file — falls straight back to the
  # unchanged serial download below. A missing marker also aborts the
  # prefetch, so it cannot keep competing for the downlink.
  # The "stop waiting" verdict has to outlive this anchor: the ones
  # still to come run in their own subshells, where a shell variable
  # could not reach them, so it is recorded as a file next to the
  # prefetch state.
  WAIT_BUDGET="$PREFETCH_WAIT_SECONDS"
  if [ -f "$PREFETCH_DIR/.abandoned" ]; then
    WAIT_BUDGET=0
  fi
  PREFETCH_STATE=absent
  if [ -f "$PREFETCH_DIR/.started" ]; then
    PREFETCH_WAITED=0
    while [ ! -f "$PREFETCH_DIR/$TAG/.done" ] && [ ! -f "$PREFETCH_DIR/.abandoned" ] && [ "$PREFETCH_WAITED" -lt "$WAIT_BUDGET" ]; do
      sleep 5
      PREFETCH_WAITED=$((PREFETCH_WAITED + 5))
    done
    if [ -f "$PREFETCH_DIR/$TAG/.done" ]; then
      PREFETCH_STATE=$(head -n1 "$PREFETCH_DIR/$TAG/.done")
      sed -n '2,$p' "$PREFETCH_DIR/$TAG/.done"
    else
      echo "::warning::anchor v${TARGET_VER} ($TAG): no prefetch marker after ${PREFETCH_WAITED}s — falling back to the serial download"
      bash .pipeline-control/.github/scripts/prefetch_patch_anchors.sh abort "$PREFETCH_DIR" \
        || echo "::warning::anchor v${TARGET_VER} ($TAG): the prefetch abort helper itself failed — continuing with the serial download"
      : > "$PREFETCH_DIR/.abandoned"
    fi
  fi
  T_START=$(date +%s)
  if [ "$PREFETCH_STATE" = ok ] && [ -s "$PREFETCH_DIR/$TAG/seforim.db.zst" ]; then
    mv "$PREFETCH_DIR/$TAG/seforim.db.zst" "$ANCHOR_DIR/seforim.db.zst"
    echo "anchor v${TARGET_VER} ($TAG): reused the prefetched DB"
  else
    # Under `set -e` a failed download used to end this subshell with
    # nothing but gh's own bare stderr, and the driver could only say
    # "exit code 1". Name the release, the asset and gh's reason.
    if ! gh release download "$TAG" \
         --pattern 'seforim.db.zst' \
         --dir "$ANCHOR_DIR" 2>"$ANCHOR_DIR/gh.err"; then
      echo "::error::anchor v${TARGET_VER} ($TAG): could not download seforim.db.zst from that release ($(tr -d '\r' < "$ANCHOR_DIR/gh.err" | head -n1)) — no patch can be produced against this anchor"
      rm -rf "$ANCHOR_DIR"
      return 1
    fi
    rm -f "$ANCHOR_DIR/gh.err"
  fi
  T_DOWNLOADED=$(date +%s)
  if ! unzstd -c "$ANCHOR_DIR/seforim.db.zst" > "$PREV_DB"; then
    echo "::error::anchor v${TARGET_VER} ($TAG): seforim.db.zst from that release could not be decompressed — no patch can be produced against this anchor"
    rm -rf "$ANCHOR_DIR"
    return 1
  fi
  T_EXTRACTED=$(date +%s)
  test -s "$PREV_DB" || { echo "::error::seforim.db.zst not found in release $TAG"; return 1; }

  # Explicitly supported schema transitions. The producer derives
  # contract promotions from both signed schema versions, rebuilds
  # newly tracked tables from a full snapshot, and verifies a real
  # apply. Unknown transitions remain fail-closed per anchor.
  PREV_SCHEMA=$(read_schema "$PREV_DB") || return 1
  if [ "$PREV_SCHEMA" != "$THIS_SCHEMA" ]; then
    if { [ "$PREV_SCHEMA" = 2 ] && [ "$THIS_SCHEMA" = 3 ]; } || \
       { [ "$PREV_SCHEMA" = 1 ] && [ "$THIS_SCHEMA" = 4 ]; } || \
       { [ "$PREV_SCHEMA" = 2 ] && [ "$THIS_SCHEMA" = 4 ]; } || \
       { [ "$PREV_SCHEMA" = 3 ] && [ "$THIS_SCHEMA" = 4 ]; } || \
       { [ "$PREV_SCHEMA" = 1 ] && [ "$THIS_SCHEMA" = 5 ]; } || \
       { [ "$PREV_SCHEMA" = 2 ] && [ "$THIS_SCHEMA" = 5 ]; } || \
       { [ "$PREV_SCHEMA" = 3 ] && [ "$THIS_SCHEMA" = 5 ]; } || \
       { [ "$PREV_SCHEMA" = 4 ] && [ "$THIS_SCHEMA" = 5 ]; }; then
      echo "schema $PREV_SCHEMA → $THIS_SCHEMA — producing the supported cross-schema delta"
    else
      echo "schema $PREV_SCHEMA → $THIS_SCHEMA is unsupported — skip anchor"
      rm -rf "$ANCHOR_DIR"
      return 0
    fi
  fi

  # A column added without a db_schema_version bump (e.g.
  # category.heShortDesc, 2026-07-16, still schema 1) no longer costs
  # us the anchor: PatchDbProducer emits an
  # `ALTER TABLE … ADD COLUMN` migration — plain SQL that every
  # released PatchApplier already runs before the upserts — and ships
  # every row whose new value is non-NULL. The shell pre-check that
  # compared PRAGMA table_info is therefore gone; the producer itself
  # is now the single authority on what is patchable.
  #
  # It declares an anchor unpatchable only when no patch could ever
  # express the change (a PRIMARY KEY column missing from prev, or a
  # column dropped without a bump): PatchPipelineCli then exits 3
  # (UnpatchableAnchor) and drops "<out>.unpatchable" carrying the
  # reason, and this anchor is skipped for that one code.
  # Every other non-zero exit still fails the release.
  rm -f "$PATCH_OUT.unpatchable"
  PRODUCE_RC=0
  if [ -n "$PATCH_MAIN_CLASS" ]; then
    # Exactly what `gradle :generator-common:producePatchAndVerify`
    # forks: main class, -Xmx/GC flags and runtime classpath all come
    # from that task's own definition (see patchPipelineLauncher in
    # generator/common/build.gradle.kts), and its -P properties are
    # the -D system properties the task sets on the fork. ZSTD_LEVEL
    # reaches the CLI through the step env either way. Keeping Gradle
    # out of the loop is what lets two of these run at once.
    java $PATCH_JVM_ARGS -cp "$PATCH_CLASSPATH" \
      -DprevDb=$PWD/$PREV_DB \
      -DnewDb=$PWD/build/seforim.db \
      -Dout=$PATCH_OUT \
      -DfromVersion=$TARGET_VER -DtoVersion=$THIS_VER \
      "$PATCH_MAIN_CLASS" || PRODUCE_RC=$?
  else
    gradle :generator-common:producePatchAndVerify \
      -PprevDb=$PWD/$PREV_DB \
      -PnewDb=$PWD/build/seforim.db \
      -Pout=$PATCH_OUT \
      -PfromVersion=$TARGET_VER -PtoVersion=$THIS_VER \
      --no-daemon --stacktrace || PRODUCE_RC=$?
  fi
  # The Gradle task maps the CLI's exit 3 onto a warning and a
  # successful build; run directly, that 3 arrives here. Both funnel
  # into the one marker path below, and every OTHER non-zero exit
  # fails this anchor — which fails the whole step, as before.
  if [ "$PRODUCE_RC" -ne 0 ] && [ "$PRODUCE_RC" -ne 3 ]; then
    echo "::error::anchor v${TARGET_VER} ($TAG): producePatchAndVerify failed with exit code $PRODUCE_RC"
    rm -rf "$ANCHOR_DIR"
    return "$PRODUCE_RC"
  fi
  if [ "$PRODUCE_RC" -eq 3 ] || [ -f "$PATCH_OUT.unpatchable" ]; then
    REASON=$(cat "$PATCH_OUT.unpatchable" 2>/dev/null || true)
    echo "::warning::anchor v${TARGET_VER} ($TAG): ${REASON:-see PatchPipelineCli output} — producer declared the anchor unpatchable; skip anchor"
    # Leave nothing behind: the marker plus the producer's half-built
    # .tmp (and any stale .db) must not clutter patches/, which the
    # release staging step globs.
    rm -f "$PATCH_OUT.unpatchable" "$PATCH_OUT" "$PATCH_OUT.tmp"
    rm -rf "$ANCHOR_DIR"
    return 0
  fi
  T_DONE=$(date +%s)
  echo "anchor v${TARGET_VER} timings: download=$((T_DOWNLOADED - T_START))s extract=$((T_EXTRACTED - T_DOWNLOADED))s produce+verify+compress=$((T_DONE - T_EXTRACTED))s total=$((T_DONE - T_START))s"

  # Free disk for the anchors still to come — only the .zst + manifest
  # need to ship; the .db was the verify ground-truth.
  rm -rf "$ANCHOR_DIR"
  rm -f "patches/patch-v${TARGET_VER}-v${THIS_VER}.db" \
        "patches/verify-patch-v${TARGET_VER}-v${THIS_VER}.db"
  df -h /
}

# Batch-buffered logging is why the two longest gaps in run
# 34024655297 (1318 s and 1076 s) were both inside this step with
# nothing on stdout: a hung producer looked exactly like a working
# one for 22 minutes. PatchPipelineCli emits nothing between
# "Producing patch" and its result — no progress to forward — so the
# heartbeat is the shell's: one line every HEARTBEAT_SECONDS naming
# the anchors still in flight, and one end line per anchor below.
# PATCH_FAN_HEARTBEAT_SECONDS exists so a test can drive this loop in
# seconds instead of in five-minute steps; the default is the one the
# runner uses.
HEARTBEAT_SECONDS="${PATCH_FAN_HEARTBEAT_SECONDS:-300}"
case "$HEARTBEAT_SECONDS" in ''|*[!0-9]*|0) HEARTBEAT_SECONDS=300 ;; esac
HEARTBEAT_POLL=5
[ "$HEARTBEAT_POLL" -le "$HEARTBEAT_SECONDS" ] || HEARTBEAT_POLL="$HEARTBEAT_SECONDS"
HEARTBEAT_PID=""
HEARTBEAT_FLAG="$RUNNER_TEMP/patch-fan-heartbeat-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
# Which anchors the heartbeat is allowed to claim are still running.
# A file, because the heartbeat is a subshell: drain_batch rewrites it
# as each anchor lands, so the line never names an anchor that has
# already finished.
HEARTBEAT_INFLIGHT="$HEARTBEAT_FLAG.inflight"
heartbeat_inflight() {  # <label>... — the anchors still being waited on
  local label
  : > "$HEARTBEAT_INFLIGHT"
  for label in "$@"; do
    printf '%s\n' "$label" >> "$HEARTBEAT_INFLIGHT"
  done
}
heartbeat_start() {  # <in-flight anchor label>...
  heartbeat_inflight "$@"
  : > "$HEARTBEAT_FLAG"
  (
    waited=0
    since=0
    # The flag file is the stop signal, polled often; the line itself
    # is printed only every HEARTBEAT_SECONDS, and only while some
    # anchor is still in flight.
    while [ -f "$HEARTBEAT_FLAG" ]; do
      sleep "$HEARTBEAT_POLL"
      waited=$((waited + HEARTBEAT_POLL))
      since=$((since + HEARTBEAT_POLL))
      [ "$since" -ge "$HEARTBEAT_SECONDS" ] || continue
      since=0
      names=$(tr '\n' ';' < "$HEARTBEAT_INFLIGHT" 2>/dev/null | sed 's/;$//; s/;/; /g')
      [ -n "$names" ] || continue
      echo "still producing patch for $names (elapsed $((waited / 60))m$((waited % 60))s)"
    done
  ) &
  HEARTBEAT_PID=$!
}
heartbeat_stop() {
  rm -f "$HEARTBEAT_FLAG" "$HEARTBEAT_INFLIGHT"
  [ -n "$HEARTBEAT_PID" ] || return 0
  kill "$HEARTBEAT_PID" 2>/dev/null || true
  wait "$HEARTBEAT_PID" 2>/dev/null || true
  HEARTBEAT_PID=""
}
drain_batch() {
  local i j status rc=0 elapsed shipped verify columns
  [ "${#BATCH_PIDS[@]}" -gt 0 ] || return 0
  heartbeat_start "${BATCH_NAMES[@]}"
  for i in "${!BATCH_PIDS[@]}"; do
    status=0
    wait "${BATCH_PIDS[$i]}" || status=$?
    # This one has landed; the heartbeat must stop claiming it.
    : > "$HEARTBEAT_INFLIGHT"
    for ((j = i + 1; j < ${#BATCH_NAMES[@]}; j++)); do
      printf '%s\n' "${BATCH_NAMES[$j]}" >> "$HEARTBEAT_INFLIGHT"
    done
    cat "${BATCH_LOGS[$i]}" || true
    elapsed=$(( $(date +%s) - BATCH_STARTS[$i] ))
    # What this anchor actually shipped, said by the driver: the
    # producer's own lines are buffered in the log above, so without
    # this the batch ends with no summary of its own.
    shipped="patches/patch-v${BATCH_TARGETS[$i]}-v${THIS_VER}.db.zst"
    if [ -s "$shipped" ]; then
      shipped="$(basename "$shipped") $(du -h "$shipped" 2>/dev/null | cut -f1)"
    else
      shipped="no patch (anchor skipped)"
    fi
    verify=not-reported
    grep -q 'Patch apply verified' "${BATCH_LOGS[$i]}" && verify=ok
    # The producer repeats one Info line per anchor, every cycle, for
    # the same standing condition (a column added since that anchor,
    # e.g. line_dh.dhDisplay in v27): it is what explains a 454 MB
    # offset-1 patch, so it belongs on the size line, not on its own.
    columns=$(sed -n "s/.*Table '\([^']*\)': prev lacks \[\([^]]*\)\].*full snapshot.*/\1[\2]/p" \
      "${BATCH_LOGS[$i]}" | tr '\n' ' ')
    rm -f "${BATCH_LOGS[$i]}"
    if [ "$status" -ne 0 ]; then
      echo "::error::${BATCH_NAMES[$i]} failed after ${elapsed}s with exit code $status"
      rc="$status"
    else
      echo "${BATCH_NAMES[$i]} done in ${elapsed}s: $shipped, verify=$verify${columns:+, full-snapshot columns: ${columns% }}"
    fi
  done
  heartbeat_stop
  BATCH_PIDS=()
  BATCH_LOGS=()
  BATCH_NAMES=()
  BATCH_TARGETS=()
  BATCH_STARTS=()
  return "$rc"
}
