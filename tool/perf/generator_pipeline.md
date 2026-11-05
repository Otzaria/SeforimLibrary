# Otzaria generator input pipeline

## Scope and baseline

Baseline: `344232b6c3e889be2bbf8a8f24f904d16dbef642` on the `otzaria` branch.
The dependency pins are unchanged: Kotlin 2.3.20, JVM 25, SQLDelight 2.3.2,
kotlinx.serialization 1.10.0, Jackson 2.19.2 and zstd-jni 1.5.7-7.

The original generator already wrote lines in batches of 1,000. Its retained
input, however, was unbounded: it created one deferred per book, awaited every
result and copied them into `bookContentCache`. The link phase likewise kept
every completed deferred and parsed link list until the whole phase ended.
The line-index lookup path also loaded entire books, including their text, to
construct long-lived arrays of IDs.

This change keeps the existing parser, serial database writer, stable-ID
allocator and database schema. It changes input scheduling and ownership:

* A metadata-only pass records the same depth-first directory traversal order.
  Priority books still precede the ordinary traversal, and category creation and
  ID allocation stay in the existing serial consumer.
* One producer prepares at most one lookahead input beside the consumer's input.
  There is no deferred per input and no whole-corpus content cache.
* Each prepared input uses a conservative retained-string budget. On overflow it
  spills to a temporary record file and is read sequentially. Completed,
  skipped, failed and cancelled inputs release their memory and temporary files.
* Book lines preserve `String.lines()` semantics, including empty input, CR,
  LF, CRLF and a trailing empty record. BOM stripping and companion-note merging
  happen per line, without copying the book's entire line list.
* Line writes retain the 1,000-row maximum and also flush by estimated bytes.
  Their line-to-TOC mappings flush only after the corresponding lines exist.
* Link files are completely validated before any of their rows allocate IDs or
  write to SQLite. Records then flow through bounded auxiliary-row batches.
  A malformed tail discards the whole file, as the former list decoder did.
  The existing unsupported Dicta object format still produces no imported rows.
* Duplicate title/file resolution and link-file discovery order are preserved.
  Repeated ranged links keep the widest range, even across flush boundaries;
  authoritative reruns can still shorten or remove a previously stored range.
* ID lookups use 4,096-entry LRU pages with a separate maximum of 4 MiB of array
  payload. The SQL projection returns only `lineIndex` and `id`, never text.
* Completed books release their occurrence counters and TOC ancestry after the
  last planned file with that source/title. Companion planning scans base text
  and retains only referenced note bodies; raw hash inputs are streamed.

## Budget and spill configuration

The default retained-input budget is **48 MiB**. A prepared input can retain at
most one third of it; the active input, its lookahead and the SQL line batch
therefore have independent byte limits. The estimate counts two bytes per
UTF-16 code unit plus 64 bytes per retained string/list entry, and additional
overhead for batched line objects.

For a forked Gradle generator, set the inherited environment variable:

```bash
SEFORIM_GENERATOR_INPUT_BUFFER_MIB=24 ./gradlew :otzariasqlite:generateLines \
  -PsourceDir=/path/to/source -PseforimDb=/path/to/output.db
```

Direct JVM launches can use `-Dseforim.generator.inputBufferMiB=24`. Values must
be integers from 1 to 16384. The constructor also accepts `inputBufferBytes`
and `inputSpillDirectory` for embedded use and deterministic small-budget tests.
Spill uses the normal JVM temporary directory unless an explicit directory is
supplied; `-Djava.io.tmpdir=...` can select a suitable local scratch volume.

This is **not a maximum-heap or maximum-RSS guarantee**. Stable-ID allocator
state, book/order metadata, required companion-note plans, per-book duplicate
hashes, per-file range-sync IDs, the existing legacy-link lookup, SQLite
caches/native allocations and the JVM remain outside the retained
input budget. A single line or JSON record must still be materialized by each
active stage. Exceptionally large records can therefore exceed the budget by
their own size; spill prevents whole books or the whole corpus accumulating in
RAM, without truncating records or changing their content. Spill disk usage is
bounded by the two active inputs, rather than the corpus, but requires enough
free temporary disk space for those inputs. Spilling trades extra sequential
disk I/O for lower retained heap, so throughput must be measured on the intended
storage device.

The producer and consumer are structured children of the phase scope. Cleanup
cancels and joins the producer before returning. File streams close before
deletion, so cleanup does not rely on Unix's ability to unlink open files.
I/O errors and batched SQL write errors propagate; coroutine cancellation is
not classified as malformed data.

Input validation is not a phase-wide SQL transaction. The repository's existing
`runInTransaction` wrapper does not create a database transaction. A malformed
JSON file is fully rejected before its own writes, but an I/O or SQL failure
during an import into a directly opened file database can leave earlier writes
in that database. The normal release CLI's existing `DbPublish` staging and
publication flow owns atomic output publication; this change preserves that
boundary and only owns the lifetime of prepared inputs and temporary resources.

## Reproducible verification

Run the focused regression suite with the pinned toolchain:

```bash
./gradlew :otzariasqlite:jvmTest
```

`BoundedGeneratorInputTest` covers the byte limit, spill equivalence, newline/BOM
semantics, replacement of malformed UTF-8, early consumer exit, one-lookahead
scheduling and cleanup on cancellation/preparation failure.
`GeneratorBoundedPipelineTest` compares every application schema object/table
between resident and forced-spill builds, including IDs, contents, links,
ranges, anchors and foreign keys. It also checks malformed JSON tails, repeated
ranges across tiny batches/reruns, sparse line-ID page boundaries/eviction and
an injected SQL coverage-write failure.

The opt-in `generatorPipelineProbe` uses the same public API as production and
produces a real SQLite database plus `metrics.json` with separate line/link
phase times, sampled peak JVM heap and RSS, process peak RSS where available,
JVM arguments and input size. Timing is evidence, not a CI pass/fail threshold.
It is deliberately synthetic and does not replace a full release-corpus run.

Example candidate run:

```bash
./gradlew :otzariasqlite:generatorPipelineProbe \
  -PprobeFixture=/tmp/seforim-fixture-medium \
  -PprobeOutput=/tmp/seforim-candidate-medium \
  -PprobeBooks=24 -PprobeLines=2000 -PprobeHeap=1g \
  -PgeneratorInputBufferMiB=8
```

For baseline/candidate comparisons, the reusable runner accepts two worktrees:

```bash
python3 tool/perf/generator_pipeline_ci.py \
  --baseline /path/to/baseline-worktree \
  --candidate /path/to/candidate-worktree \
  --output /tmp/seforim-generator-comparison
```

It runs the candidate's full generator JVM test suite, copies only
`GeneratorPipelineProbe.kt` to the baseline and attaches the same opt-in task to
both worktrees with a temporary Gradle init script. Baseline production code,
build scripts and dependency pins remain unchanged. It uses separate 1 GiB
probe JVMs, an 8 MiB retained-input budget and 6 × 100, 24 × 500 and 48 × 1,500
line fixtures. The output records exact commit IDs, separate line/link times,
sampled heap/RSS and strict database comparisons. The output directory must not
already exist. `--input-buffer-mib=1` can exercise spill on smaller fixtures.

The `Generator pipeline parity and measurements` workflow runs this script on
`perf/generator-*` pull requests and supports manual runs with an explicit
baseline ref. Automatic comparison is scoped to those performance branches
because other changes can intentionally alter content or schema. It publishes
measurement, comparison and JVM test reports as artifacts; elapsed time and
memory measurements do not decide pass/fail.

For larger or repeated measurements, run the same fixture sizes and JVM flags
in separate JVM processes with the probe directly. Reuse
the **same fixture directory**, because the original link importer used native
filesystem discovery order; regenerating it in a different directory can change
baseline link IDs. Inputs must remain unchanged during both runs. Suggested
fixtures are 4 × 100, 24 × 2,000 and 48 × 10,000 lines, adjusted for available
memory and runtime. Repeat measurements on an otherwise idle machine and
report every run rather than selecting the fastest one.

Compare the resulting databases:

```bash
python tool/perf/generator_compare.py \
  /tmp/seforim-baseline-medium/seforim.db \
  /tmp/seforim-candidate-medium/seforim.db
```

The comparator checks integrity, foreign keys, `user_version`, `application_id`,
every non-internal schema object and ordered values of every application table
(including exact BLOB bytes).
It exits nonzero for differences or integrity failures. Database page layout and
SQLite's internal objects are intentionally excluded. Keep generated databases,
large fixtures and raw benchmark outputs outside version control.

The streaming decoder's ownership/format contracts are documented by
[kotlinx.serialization](https://kotlinlang.org/api/kotlinx.serialization/kotlinx-serialization-json/kotlinx.serialization.json/decode-to-sequence.html).
The pinned
[1.10.0 array iterator](https://github.com/Kotlin/kotlinx.serialization/blob/v1.10.0/formats/json/commonMain/src/kotlinx/serialization/json/internal/JsonIterator.kt)
checks both the closing bracket and EOF; full iteration is required before
declaring an input valid.
