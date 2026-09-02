package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger

internal data class SefariaBlacklists(
    val authorKeys: Set<String>,
    val bookTitleKeys: Set<String>,
    val bookPathKeys: Set<String>
) {
    fun isEmpty(): Boolean = authorKeys.isEmpty() && bookTitleKeys.isEmpty() && bookPathKeys.isEmpty()

    companion object {
        val Empty = SefariaBlacklists(
            authorKeys = emptySet(),
            bookTitleKeys = emptySet(),
            bookPathKeys = emptySet()
        )
    }
}

internal data class BlacklistFilterResult(
    val payloads: List<BookPayload>,
    val skippedTotal: Int,
    val skippedByBook: Int,
    val skippedByAuthor: Int,
    val skippedBookExamples: List<String>,
    val skippedAuthorExamples: List<String>,
    val skippedNormalizedPaths: Set<String>
)

/** Blacklist of book editions (book_version). Format rules: see black_versions.txt. */
internal data class VersionsBlacklist(
    val globalKeys: Set<String>,
    val perBookKeys: Map<String, Set<String>>
) {
    fun isEmpty(): Boolean = globalKeys.isEmpty() && perBookKeys.isEmpty()

    fun isBlocked(bookKeys: Set<String>, versionKeys: Set<String>): Boolean {
        if (versionKeys.any { it in globalKeys }) return true
        return bookKeys.any { book ->
            perBookKeys[book]?.let { blocked -> versionKeys.any { it in blocked } } == true
        }
    }

    companion object {
        val Empty = VersionsBlacklist(globalKeys = emptySet(), perBookKeys = emptyMap())
    }
}

internal fun loadVersionsBlacklist(classLoader: ClassLoader?, logger: Logger): VersionsBlacklist =
    parseVersionsBlacklist(loadBlacklistEntries(classLoader, "black_versions.txt", logger), logger)

internal fun parseVersionsBlacklist(entries: List<String>, logger: Logger): VersionsBlacklist {
    val globalKeys = LinkedHashSet<String>()
    val perBookKeys = LinkedHashMap<String, MutableSet<String>>()
    entries.forEach { entry ->
        val separator = entry.indexOf('|')
        if (separator < 0) {
            val key = normalizeTitleKey(entry)
            if (key != null) globalKeys += key
            return@forEach
        }
        val bookKey = normalizeTitleKey(entry.substring(0, separator))
        val versionKey = normalizeTitleKey(entry.substring(separator + 1))
        if (bookKey == null || versionKey == null) {
            logger.w { "black_versions.txt: malformed entry skipped: $entry" }
            return@forEach
        }
        perBookKeys.getOrPut(bookKey) { LinkedHashSet() } += versionKey
    }
    return VersionsBlacklist(globalKeys = globalKeys, perBookKeys = perBookKeys)
}

internal fun loadSefariaBlacklists(classLoader: ClassLoader?, logger: Logger): SefariaBlacklists {
    val authorEntries = loadBlacklistEntries(classLoader, "authors_blacklist.txt", logger)
    val bookEntries = loadBlacklistEntries(classLoader, "books_blacklist.txt", logger)

    val authorKeys = authorEntries.mapNotNull { normalizeTitleKey(it) }.toSet()
    val bookTitleKeys = bookEntries.mapNotNull { normalizeTitleKey(it) }.toSet()
    val bookPathKeys = bookEntries
        .asSequence()
        .filter { it.contains('/') || it.contains('\\') }
        .map { normalizePriorityEntry(it) }
        .filter { it.isNotBlank() }
        .toSet()

    return SefariaBlacklists(
        authorKeys = authorKeys,
        bookTitleKeys = bookTitleKeys,
        bookPathKeys = bookPathKeys
    )
}

internal fun filterBlacklistedPayloads(
    payloads: List<BookPayload>,
    blacklists: SefariaBlacklists
): BlacklistFilterResult {
    if (payloads.isEmpty() || blacklists.isEmpty()) {
        return BlacklistFilterResult(
            payloads = payloads,
            skippedTotal = 0,
            skippedByBook = 0,
            skippedByAuthor = 0,
            skippedBookExamples = emptyList(),
            skippedAuthorExamples = emptyList(),
            skippedNormalizedPaths = emptySet()
        )
    }

    var skippedTotal = 0
    var skippedByBook = 0
    var skippedByAuthor = 0
    val skippedBookExamples = ArrayList<String>(5)
    val skippedAuthorExamples = ArrayList<String>(5)
    val skippedNormalizedPaths = LinkedHashSet<String>()

    val filtered = payloads.filter { payload ->
        val bookBlacklisted = isBookBlacklisted(payload, blacklists)
        val authorBlacklisted = isAuthorBlacklisted(payload, blacklists)

        if (bookBlacklisted || authorBlacklisted) {
            skippedTotal++
            skippedNormalizedPaths += normalizedBookPath(payload.categoriesHe, payload.heTitle)

            if (bookBlacklisted) {
                skippedByBook++
                if (skippedBookExamples.size < 5) skippedBookExamples += payload.heTitle
            }
            if (authorBlacklisted) {
                skippedByAuthor++
                if (skippedAuthorExamples.size < 5) {
                    val author = payload.authors.firstOrNull().orEmpty()
                    skippedAuthorExamples += if (author.isBlank()) payload.heTitle else "${payload.heTitle} ($author)"
                }
            }

            false
        } else {
            true
        }
    }

    return BlacklistFilterResult(
        payloads = filtered,
        skippedTotal = skippedTotal,
        skippedByBook = skippedByBook,
        skippedByAuthor = skippedByAuthor,
        skippedBookExamples = skippedBookExamples,
        skippedAuthorExamples = skippedAuthorExamples,
        skippedNormalizedPaths = skippedNormalizedPaths
    )
}

private fun isBookBlacklisted(payload: BookPayload, blacklists: SefariaBlacklists): Boolean {
    if (blacklists.bookTitleKeys.isNotEmpty()) {
        normalizeTitleKey(payload.heTitle)?.let { if (it in blacklists.bookTitleKeys) return true }
        normalizeTitleKey(payload.enTitle)?.let { if (it in blacklists.bookTitleKeys) return true }
    }
    if (blacklists.bookPathKeys.isNotEmpty()) {
        val path = normalizedBookPath(payload.categoriesHe, payload.heTitle)
        if (path in blacklists.bookPathKeys) return true
    }
    return false
}

private fun isAuthorBlacklisted(payload: BookPayload, blacklists: SefariaBlacklists): Boolean {
    if (blacklists.authorKeys.isEmpty()) return false
    // Match on every name form the author is known by, not just the one chosen
    // for display. authors.json can turn a bare schema name into an honorific
    // one, and an entry listed bare would then no longer match. Checking all
    // forms can only ever block more, never less — the right direction for a
    // content filter.
    val candidates = payload.authorMatchKeys.ifEmpty { payload.authors }
    if (candidates.isEmpty()) return false
    return candidates.any { author ->
        normalizeTitleKey(author)?.let { it in blacklists.authorKeys } == true
    }
}

private fun loadBlacklistEntries(
    classLoader: ClassLoader?,
    resourceName: String,
    logger: Logger
): List<String> = try {
    val stream = sequenceOf(resourceName, "/$resourceName")
        .mapNotNull { name -> classLoader?.getResourceAsStream(name) }
        .firstOrNull()
        ?: return emptyList()

    stream.bufferedReader(Charsets.UTF_8).useLines { lines ->
        lines
            .map { raw -> raw.removePrefix("\uFEFF") }
            .map { raw -> raw.trim() }
            .filter { it.isNotEmpty() && !it.startsWith("#") }
            .map { unescapeBlacklistLine(it) }
            .toList()
    }
} catch (e: Exception) {
    logger.w(e) { "Unable to read $resourceName, continuing without it" }
    emptyList()
}

private fun unescapeBlacklistLine(value: String): String {
    return value
        .replace("\\\"", "\"")
        .replace("\\'", "'")
}

