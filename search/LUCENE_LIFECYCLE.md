# Lucene searcher lifetime and measurements

## Contract

`LuceneSearchEngine` and `VectorSearcher` lazily own one `SearcherManager` and its
directory per instance. Creating or closing an unused engine performs no index
I/O. Repeated searches reuse the current searcher and the unchanged segment
readers, rather than opening and closing an entire `DirectoryReader` on every
title, facet, text, or vector request. There is no process-wide cache or timer.

Each **new request or session** calls `maybeRefreshBlocking()` before acquiring a
searcher. This intentionally preserves the previous visibility of already
committed changes. An in-progress writer commit may become visible to the next
request; uncommitted writer state is never exposed. Existing cursor sessions keep
their original snapshot across all `searchAfter` pages, even if a subsequent
request refreshes. Query construction, filters, ranking, snippets, index schema,
analyzers and vector codecs are unchanged.

Lucene recommends moving refresh work away from queries when possible. Switching
to an asynchronous timer would change freshness, so this change keeps the
synchronous check and adds `refresh()` to both concrete search classes. An owner
can call it on an I/O thread just after committing an update, to do the reopen
work ahead of the next query. No-op refresh still reads commit metadata; the
benchmark includes that cost. Only refreshes serialize in Lucene, while searches
on acquired snapshots execute concurrently. See [SearcherManager 10.4.0][manager]
and [ReferenceManager 10.4.0][references].

All stored-field access and snippet fetching stays inside the request lease.
Normal completion, exceptions and coroutine cancellation release that lease.
Failed initialization closes the directory and permits a later retry; failed
refresh disposes the attempted reader without invalidating an acquired snapshot.

`MagicDictionaryIndex` remains lazy and reuses one prepared statement and JDBC
connection per lookup thread. It is now `Closeable`: the owner tracks every
thread's context, scopes result sets, and attempts statement closure, transaction
rollback and connection closure even when an earlier cleanup step fails. Failed
context initialization releases its connection; a failed query discards its
context so a later uncached lookup can retry. Token/base caches clear on close.
A read/write lock protects entire synchronous dictionary calls, including cache
hits and surface loading, so disposal waits for current lookups and rejects new
ones. Query expansion and ranking rules are unchanged.

## Closing and replacing an index

`close()` rejects new index operations and is idempotent. It does not interrupt a
request or a session that already holds a lease. When the final lease is returned,
the owner closes the manager, then the directory. This prevents a directory from
closing during a refresh or active search. A session is still a single-consumer
cursor, as documented by `SearchSession`; its `close()` is idempotent.

The engine also closes its initialized dictionary. Lazy publication and the
engine's closed state share a monitor, while JDBC disposal runs outside that
monitor. Closing an unused engine does not load the dictionary, and a subsequent
call cannot publish or reopen it. Cursor queries and highlight terms are already
computed before acquiring their reader lease, so those cursors remain usable
after the engine has released the dictionary.

For **in-place Lucene updates**, commit the `IndexWriter`; new requests observe the
commit automatically. `IndexWriter` with `OpenMode.CREATE` is also supported.

For a **downloaded directory replacement**, stop new searches, finish/cancel active
requests, close every session and close the engine before moving or deleting the
old index directory. Construct a new engine after installing the replacement. Do
not overwrite segment files belonging to a live reader, or try to refresh an old
reader against an unrelated copied index.

This ordering matters on Windows, where open or mapped files can prevent deletion
and renaming. The choice of `FSDirectory.open()` on normal JVMs and
`NIOFSDirectory` in GraalVM native images is unchanged. Modern Lucene unmaps index
inputs when the final reader reference closes; closing a directory alone is not
a substitute for closing readers. See [FSDirectory 10.4.0][directory] and
[MMapDirectory 10.4.0][mmap].

This closes the engine's **Lucene index and lexical dictionary resources**. It
does not close an external snippet provider, its database connections, or a
caller-supplied analyzer; those remain their owners' responsibility.

## Verification

```bash
./gradlew :search:jvmTest
```

The added fixtures require no external DB, model or corpus. They cover repeated
reader reuse, exact score/order parity with fresh readers, lexical and vector
filters, committed refreshes, cursor stability, concurrent searching during
refresh, close with in-flight work, initial and refresh failures, retry,
coroutine cancellation and file replacement after cleanup. The index replacement
test is portable. The `Search lifecycle and benchmarks` workflow runs the full
`:search:jvmTest` suite on Windows to exercise OS-level file locks. Dictionary
tests also cover multiple live worker threads, blocked reads versus close,
initialization/query/cleanup failures, late lazy initialization, and dictionary
file replacement while an existing Lucene cursor retains its snapshot. The
regular Linux CI continues to run the full repository test suite.

No index regeneration, dependency change or database migration is needed.

## Focused opt-in benchmark

```bash
./gradlew :search:benchmarkLuceneSearchers --args="legacy 10000 1000 1"
./gradlew :search:benchmarkLuceneSearchers --args="managed 10000 1000 1"
./gradlew :search:benchmarkLuceneSearchers --args="legacy 10000 1000 4"
./gradlew :search:benchmarkLuceneSearchers --args="managed 10000 1000 4"
```

Arguments are mode, line count, measured requests **per worker**, and worker
count. Run modes in separate JVMs and alternate their order across repeated runs.
The benchmark sets the same 128 MiB initial / 512 MiB maximum heap for each mode.
It uses the pinned Lucene version and a deterministic multi-segment fixture with
eight book titles, text fields and 16-dimensional unit vectors. The request mix
cycles between a term search, a title prefix and KNN, all retrieving stored fields.
`legacy` reproduces the former per-request `DirectoryReader.open` pattern over a
reused directory. `managed` uses the production owner, including synchronous
freshness checks. Both run the same queries and return a checksum of ids/scores.

The single JSON output reports throughput, p50/p95/p99 request latency, allocations
on request worker threads, sampled heap/RSS/mapped memory, GC time and actual reader
opens. RSS uses `/proc/self/status` on Linux and reports zero when unavailable;
memory peaks are sampled every 10 ms, not exact native high-water marks. Fixture
construction and 100 warmup requests per worker are outside the timed section.
The idle current reader deliberately stays resident between requests; this is a
tradeoff for avoiding repeated opens and mappings. All snapshots are released on
the documented close path.

The focused workflow also runs the benchmark on Linux for pull requests whose
head branch starts with `perf/lucene-`, or through manual dispatch. It measures
both modes with one and four workers, repeats each pair twice with the order
reversed, and checks result checksums. Its `linux-lucene-searcher-benchmark`
artifact contains individual and combined JSON, raw logs, the checked-out commit,
Java version and dependency catalog. The job summary reports throughput and p95
latency without enforcing a performance threshold.

These are synthetic lifecycle measurements, not end-to-end application or
production-corpus search timings. Other pull requests do not run the benchmark.

[manager]: https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/search/SearcherManager.html
[references]: https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/search/ReferenceManager.html
[directory]: https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/store/FSDirectory.html
[mmap]: https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/store/MMapDirectory.html
