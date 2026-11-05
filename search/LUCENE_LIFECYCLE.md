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

## Closing and replacing an index

`close()` rejects new index operations and is idempotent. It does not interrupt a
request or a session that already holds a lease. When the final lease is returned,
the owner closes the manager, then the directory. This prevents a directory from
closing during a refresh or active search. A session is still a single-consumer
cursor, as documented by `SearchSession`; its `close()` is idempotent.

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

The replacement contract here is for the **Lucene index directory**. It does not
claim to close an external snippet provider, a caller-supplied analyzer or the
existing SQLite-backed `MagicDictionaryIndex` connections.

## Verification

```bash
./gradlew :search:jvmTest
```

The added fixtures require no external DB, model or corpus. They cover repeated
reader reuse, exact score/order parity with fresh readers, lexical and vector
filters, committed refreshes, cursor stability, concurrent searching during
refresh, close with in-flight work, initial and refresh failures, retry,
coroutine cancellation and file replacement after cleanup. The index replacement
test is portable and should also run on Windows to exercise OS-level file locks.

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

These are synthetic lifecycle measurements, not end-to-end application or
production-corpus search timings. The task is opt-in and has no CI performance
threshold.

[manager]: https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/search/SearcherManager.html
[references]: https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/search/ReferenceManager.html
[directory]: https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/store/FSDirectory.html
[mmap]: https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/store/MMapDirectory.html
