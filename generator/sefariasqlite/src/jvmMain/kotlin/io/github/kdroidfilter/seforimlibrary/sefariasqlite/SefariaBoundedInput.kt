package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import java.nio.file.Files
import java.nio.file.Path

/**
 * A book's stable discovery identity, without its (potentially enormous) text,
 * line hashes, TOC or edition overlays. Paths always refer to merged.json.
 */
internal data class SefariaBookDescriptor(
    val path: Path,
    val heTitle: String,
    val enTitle: String,
    val categoriesHe: List<String>,
) {
    val normalizedPath: String get() = normalizedBookPath(categoriesHe, heTitle)

    fun verify(payload: BookPayload) {
        check(payload.heTitle == heTitle && payload.enTitle == enTitle &&
            payload.categoriesHe == categoriesHe) {
            "Sefaria input changed between discovery and import: $path"
        }
    }
}

internal data class SefariaBookScan(
    val books: List<SefariaBookDescriptor>,
    val blacklistResult: BlacklistFilterResult,
)

/**
 * Discovery deliberately holds just metadata. Reuse the exact legacy parser
 * and blacklist predicates; don't invent a second interpretation of category
 * paths, author aliases or preferred-version selection.
 *
 * Parsing each book twice costs I/O/CPU, but prevents the discovery pass from
 * retaining its text. A separate, faster metadata parser would need a strict
 * parity test before replacing this correctness-first implementation.
 */
internal fun scanSefariaBooks(
    reader: SefariaBookPayloadReader,
    mergedFiles: List<Path>,
    schemaDir: Path,
    schemaLookup: Map<String, Path>,
    blacklists: SefariaBlacklists,
): SefariaBookScan {
    val books = ArrayList<SefariaBookDescriptor>()
    var skippedTotal = 0
    var skippedByBook = 0
    var skippedByAuthor = 0
    val skippedBookExamples = ArrayList<String>(5)
    val skippedAuthorExamples = ArrayList<String>(5)
    val skippedPaths = LinkedHashSet<String>()
    val skippedTitles = LinkedHashSet<String>()

    for (path in mergedFiles) {
        val payload = reader.parseBookFile(path, schemaDir, schemaLookup) ?: continue
        val filtered = filterBlacklistedPayloads(listOf(payload), blacklists)
        if (filtered.skippedTotal != 0) {
            skippedTotal += filtered.skippedTotal
            skippedByBook += filtered.skippedByBook
            skippedByAuthor += filtered.skippedByAuthor
            skippedPaths += filtered.skippedNormalizedPaths
            skippedTitles += filtered.skippedHeTitles
            if (skippedBookExamples.size < 5) {
                skippedBookExamples += filtered.skippedBookExamples.take(5 - skippedBookExamples.size)
            }
            if (skippedAuthorExamples.size < 5) {
                skippedAuthorExamples += filtered.skippedAuthorExamples.take(5 - skippedAuthorExamples.size)
            }
        } else {
            books += SefariaBookDescriptor(path, payload.heTitle, payload.enTitle, payload.categoriesHe)
        }
    }
    return SefariaBookScan(
        books,
        BlacklistFilterResult(
            payloads = emptyList(),
            skippedTotal = skippedTotal,
            skippedByBook = skippedByBook,
            skippedByAuthor = skippedByAuthor,
            skippedBookExamples = skippedBookExamples,
            skippedAuthorExamples = skippedAuthorExamples,
            skippedNormalizedPaths = skippedPaths,
            skippedHeTitles = skippedTitles,
        ),
    )
}

/**
 * Exactly one I/O producer, one sequential SQLite consumer, and one queued
 * work item. A large on-disk JSON is not read ahead while the previous book
 * is being inserted. This bounds retained BookPayload instances, NOT total
 * heap/RSS: reference indexes, source hashes, allocator state, images and a
 * single exceptionally large book still depend on corpus size.
 */
internal object SefariaBoundedInput {
    private const val DEFAULT_INPUT_MIB = 16L
    internal fun inputBudgetBytes(): Long {
        val raw = System.getProperty("seforim.sefaria.inputBufferMiB")
            ?: System.getenv("SEFORIM_SEFARIA_INPUT_BUFFER_MIB")
        val mib = raw?.toLongOrNull() ?: DEFAULT_INPUT_MIB
        require(mib in 1L..1024L && (raw == null || raw.toLongOrNull() != null)) {
            "Invalid Sefaria input budget '$raw' (expected 1..1024 MiB)"
        }
        return mib * 1024 * 1024
    }

    private sealed interface Work {
        val descriptor: SefariaBookDescriptor

        data class Ready(
            override val descriptor: SefariaBookDescriptor,
            val payload: BookPayload,
        ) : Work

        data class ReadOnConsumer(override val descriptor: SefariaBookDescriptor) : Work
    }

    internal data class Stats(
        val booksWritten: Int,
        val readOnConsumer: Int,
        val inputBudgetMiB: Long,
    )

    suspend fun consumeInOrder(
        descriptors: List<SefariaBookDescriptor>,
        load: (Path) -> BookPayload?,
        logger: Logger,
        budgetBytes: Long = inputBudgetBytes(),
        consume: suspend (SefariaBookDescriptor, BookPayload) -> Unit,
    ): Stats = coroutineScope {
        require(budgetBytes > 0) { "Input budget must be positive" }
        val work = Channel<Work>(capacity = 1)
        var deferred = 0
        val producer = launch(Dispatchers.IO) {
            try {
                for (descriptor in descriptors) {
                    val item = if (Files.size(descriptor.path) > budgetBytes) {
                        deferred++
                        Work.ReadOnConsumer(descriptor)
                    } else {
                        val payload = load(descriptor.path)
                            ?: error("Previously discovered Sefaria book became unreadable: ${descriptor.path}")
                        descriptor.verify(payload)
                        // UTF-16 content and reference records can be far larger
                        // than the source JSON. Do not queue such a book.
                        val estimatedBytes = payload.lines.sumOf { it.length.toLong() * 2L } +
                            payload.refEntries.size.toLong() * 160L
                        if (estimatedBytes > budgetBytes) {
                            deferred++
                            Work.ReadOnConsumer(descriptor)
                        } else {
                            Work.Ready(descriptor, payload.precomputeLineData())
                        }
                    }
                    work.send(item)
                }
                work.close()
            } catch (failure: Throwable) {
                work.close(failure)
                throw failure
            }
        }
        var inserted = 0
        try {
            for (item in work) {
                val payload = when (item) {
                    is Work.Ready -> item.payload
                    is Work.ReadOnConsumer -> {
                        val parsed = load(item.descriptor.path)
                            ?: error("Previously discovered Sefaria book became unreadable: ${item.descriptor.path}")
                        item.descriptor.verify(parsed)
                        parsed.precomputeLineData()
                    }
                }
                consume(item.descriptor, payload)
                inserted++
            }
            producer.join()
        } finally {
            producer.cancelAndJoin()
            work.cancel()
        }
        logger.i {
            "Sefaria bounded input: $inserted books, $deferred consumer-side reads, " +
                "one producer + one sequential writer + one waiting item; " +
                "source-file admission budget=${budgetBytes / (1024 * 1024)} MiB"
        }
        Stats(inserted, deferred, budgetBytes / (1024 * 1024))
    }
}
