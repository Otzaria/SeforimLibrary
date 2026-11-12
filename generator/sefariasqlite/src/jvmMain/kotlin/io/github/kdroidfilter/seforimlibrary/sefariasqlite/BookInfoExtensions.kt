package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository

/** The per-book fields of [BOOK_INFO_EXTENSION_COLUMNS]; empty means "no value / keep". */
internal data class BookInfoExtension(
    val topics: List<String> = emptyList(),
    val pubDate: String? = null,
    val pubPlace: String? = null,
    val description: String? = null,
    val proofread: Boolean = false,
) {
    val isEmpty: Boolean
        get() = topics.isEmpty() && pubDate == null && pubPlace == null && description == null && !proofread
}

internal data class BookInfoExtensionResult(val updated: Int, val unmatched: Int)

private const val MAX_TOPICS = 10
private const val MAX_TOPIC_LENGTH = 60
private const val MAX_PUB_FIELD_LENGTH = 100
private const val MAX_DESCRIPTION_LENGTH = 4000

/** Parses and validates the extension fields of one [BOOK_INFO_FILE] record (1-based [recordNumber]). */
internal fun parseBookInfoExtension(
    topics: String,
    pubDate: String,
    pubPlace: String,
    description: String,
    proofread: String,
    recordNumber: Int,
): BookInfoExtension {
    fun fail(message: String): Nothing = throw IllegalArgumentException("$BOOK_INFO_FILE record $recordNumber: $message")

    val topicList = topics.split(';').map { it.trim() }.filter { it.isNotEmpty() }.distinct()
    if (topicList.size > MAX_TOPICS) fail("more than $MAX_TOPICS topics")
    topicList.firstOrNull { it.length > MAX_TOPIC_LENGTH || '\n' in it }
        ?.let { fail("topic longer than $MAX_TOPIC_LENGTH characters or multi-line: '$it'") }

    fun singleLine(name: String, raw: String): String? {
        val value = raw.trim().ifEmpty { return null }
        if (value.length > MAX_PUB_FIELD_LENGTH || '\n' in value || '\r' in value) {
            fail("$name must be one line of at most $MAX_PUB_FIELD_LENGTH characters")
        }
        return value
    }

    val text = description.replace("\r\n", "\n").trim().ifEmpty { null }
    if (text != null && ('\r' in text || text.length > MAX_DESCRIPTION_LENGTH)) {
        fail("description must have LF line breaks and at most $MAX_DESCRIPTION_LENGTH characters")
    }
    if (proofread.trim() !in setOf("", "1")) fail("proofread must be empty or 1, not '$proofread'")

    return BookInfoExtension(
        topics = topicList,
        pubDate = singleLine("pubDate", pubDate),
        pubPlace = singleLine("pubPlace", pubPlace),
        description = text,
        proofread = proofread.trim() == "1",
    )
}

/**
 * One [BookInfoExtension] per title. The fields belong to the book, so every row of a
 * title should carry the same values; on a conflict the first non-empty value wins, with a warning.
 */
internal fun bookInfoExtensions(records: List<BookInfoRecord>, logger: Logger): Map<String, BookInfoExtension> {
    val merged = LinkedHashMap<String, BookInfoExtension>()
    val conflicts = LinkedHashSet<String>()
    for (record in records) {
        val next = record.extension
        val current = merged[record.bookName]
        if (current == null) {
            merged[record.bookName] = next
            continue
        }
        fun <T> pick(field: String, a: T, b: T, empty: (T) -> Boolean): T = when {
            empty(a) -> b
            empty(b) || a == b -> a
            else -> a.also { conflicts += "'${record.bookName}' ($field)" }
        }
        merged[record.bookName] = BookInfoExtension(
            topics = pick("topics", current.topics, next.topics) { it.isEmpty() },
            pubDate = pick("pubDate", current.pubDate, next.pubDate) { it == null },
            pubPlace = pick("pubPlace", current.pubPlace, next.pubPlace) { it == null },
            description = pick("description", current.description, next.description) { it == null },
            proofread = current.proofread || next.proofread,
        )
    }
    if (conflicts.isNotEmpty()) {
        logger.w {
            "$BOOK_INFO_FILE: ${conflicts.size} book field(s) differ between rows of the same book; " +
                "using the first non-empty value: ${conflicts.take(20).joinToString()}"
        }
    }
    return merged.filterValues { !it.isEmpty }
}

/**
 * Applies [extensions] by exact title. Topics join the book's existing topics; a pub date or
 * place replaces the book's links; a description replaces heDesc. Proofread is not written.
 * Runs after [applyMetadata] so the description wins over sefaria_metadata_changes.csv.
 */
internal suspend fun applyBookInfoExtensions(
    repository: SeforimRepository,
    bindings: IdAllocatorBindings,
    extensions: Map<String, BookInfoExtension>,
    retitledBooks: List<RetitledBook>,
    logger: Logger,
): BookInfoExtensionResult {
    val writable = extensions.filterValues {
        it.topics.isNotEmpty() || it.pubDate != null || it.pubPlace != null || it.description != null
    }
    if (writable.isEmpty()) return BookInfoExtensionResult(0, 0)
    val bookIdsByTitle = repository.getAllBookTitleIds().groupBy({ it.second.trim() }, { it.first })
    // Before any write: this stage has no transaction to roll back.
    requireForDbUsesDisplayTitles(BOOK_INFO_FILE, writable.keys, bookIdsByTitle.keys, retitledBooks)

    var updated = 0
    val unmatched = ArrayList<String>()
    for ((title, ext) in writable) {
        val ids = bookIdsByTitle[title]
        if (ids == null) {
            unmatched += title
            continue
        }
        if (ids.size > 1) {
            logger.w { "$BOOK_INFO_FILE: '$title' has ${ids.size} matches; skipping" }
            continue
        }
        val bookId = ids.single()
        for (topic in ext.topics) {
            val topicId = bindings.upsertTopic(topic)
            // upsertTopic is INSERT OR IGNORE: a name stored under another id would leave a dangling link.
            check(repository.getTopicByName(topic)?.id == topicId) {
                "topic '$topic' was allocated id $topicId but the DB holds another row for it"
            }
            repository.linkTopicToBook(topicId, bookId)
        }
        ext.pubDate?.let { date ->
            repository.deleteBookPubDates(bookId)
            repository.linkPubDateToBook(bindings.upsertPubDate(date), bookId)
        }
        ext.pubPlace?.let { place ->
            repository.deleteBookPubPlaces(bookId)
            repository.linkPubPlaceToBook(bindings.upsertPubPlace(place), bookId)
        }
        ext.description?.let { repository.updateBookMetadata(bookId, heShortDesc = null, heDesc = it) }
        updated++
    }
    if (unmatched.isNotEmpty()) {
        logger.w {
            "$BOOK_INFO_FILE: ${unmatched.size} book(s) with extension fields matched no book (skipped): " +
                unmatched.take(20).joinToString()
        }
    }
    return BookInfoExtensionResult(updated, unmatched.size)
}
