package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger

internal const val VERSION_TITLES_HE_RESOURCE = "version_titles_he.txt"

/** שורה פגומה או מפתח כפול ב-version_titles_he.txt — נפילה בזמן הפענוח. */
internal class MalformedVersionTitlesException(message: String) : IllegalArgumentException(message)

/** One row of version_titles_he.txt; [book] is null for a global row. */
internal data class VersionTitleRow(
    val lineNumber: Int,
    val text: String,
    val book: String?,
    val versionTitle: String,
    val heTitle: String,
)

/** Hebrew display names for book_version. Format rules: see version_titles_he.txt. */
internal class VersionHebrewTitles(val rows: List<VersionTitleRow>) {
    private val globalByVersion = rows.filter { it.book == null }.associateBy { it.versionTitle }
    private val bookByKey = rows.filter { it.book != null }.associateBy { it.book!! to it.versionTitle }

    /** [matched]: rows whose key hit this version; [applied]: the row that set the title, if any. */
    data class Resolution(
        val heVersionTitle: String?,
        val matched: List<VersionTitleRow>,
        val applied: VersionTitleRow?,
        val shadowedBookRows: List<VersionTitleRow> = emptyList(),
    )

    fun resolve(bookHeTitle: String, bookEnTitle: String, versionTitle: String, sefariaHeTitle: String?): Resolution {
        val version = versionTitle.trim()
        // Hebrew- and English-title rows for one version: the lower line number wins.
        val bookRows = listOf(bookHeTitle.trim(), bookEnTitle.trim()).distinct()
            .mapNotNull { bookByKey[it to version] }
            .sortedBy { it.lineNumber }
        val globalRow = globalByVersion[version]
        // Book row always wins; a global row only fills a missing Sefaria title.
        val applied = bookRows.firstOrNull() ?: globalRow?.takeIf { sefariaHeTitle.isNullOrBlank() }
        return Resolution(
            heVersionTitle = applied?.heTitle ?: sefariaHeTitle,
            matched = bookRows + listOfNotNull(globalRow),
            applied = applied,
            shadowedBookRows = bookRows.drop(1),
        )
    }

    companion object {
        val Empty = VersionHebrewTitles(emptyList())
    }
}

/** Per-import usage: resolves titles and reports rows that matched no version. */
internal class VersionTitlesUsage(private val titles: VersionHebrewTitles, private val logger: Logger) {
    private val matchedLines = HashSet<Int>()
    private val appliedLines = HashSet<Int>()
    private var versionsRetitled = 0

    fun resolve(bookHeTitle: String, bookEnTitle: String, versionTitle: String, sefariaHeTitle: String?): String? {
        val resolution = titles.resolve(bookHeTitle, bookEnTitle, versionTitle, sefariaHeTitle)
        val winner = resolution.applied?.lineNumber
        resolution.shadowedBookRows.forEach { shadowed ->
            logger.w {
                "$VERSION_TITLES_HE_RESOURCE: lines $winner and ${shadowed.lineNumber} both apply to " +
                    "$bookHeTitle / ${versionTitle.trim()}; line $winner wins"
            }
        }
        resolution.matched.forEach { matchedLines += it.lineNumber }
        resolution.applied?.let {
            appliedLines += it.lineNumber
            versionsRetitled++
        }
        return resolution.heVersionTitle
    }

    fun unmatchedRows(): List<VersionTitleRow> = titles.rows.filter { it.lineNumber !in matchedLines }

    // Sefaria may rename a version at any time: warn, never fail the build.
    fun logSummary() {
        val unmatched = unmatchedRows()
        unmatched.forEach { row ->
            logger.w { "$VERSION_TITLES_HE_RESOURCE:${row.lineNumber}: row matched no imported version: ${row.text}" }
        }
        logger.i {
            "Version Hebrew titles: rows=${titles.rows.size}, applied=${appliedLines.size}, " +
                "unmatched=${unmatched.size}, versionsRetitled=$versionsRetitled"
        }
    }
}

internal fun loadVersionHebrewTitles(classLoader: ClassLoader?): VersionHebrewTitles {
    val stream = classLoader?.getResourceAsStream(VERSION_TITLES_HE_RESOURCE)
        ?: error("$VERSION_TITLES_HE_RESOURCE missing from the classpath")
    return parseVersionHebrewTitles(stream.bufferedReader(Charsets.UTF_8).use { it.readLines() })
}

/** Parses the raw file lines; line numbers are 1-based. */
internal fun parseVersionHebrewTitles(lines: List<String>): VersionHebrewTitles {
    val rows = ArrayList<VersionTitleRow>()
    val lineByKey = HashMap<Pair<String?, String>, Int>()
    lines.forEachIndexed { index, raw ->
        val lineNumber = index + 1
        val text = (if (index == 0) raw.removePrefix("\uFEFF") else raw).trim()
        if (text.isEmpty() || text.startsWith("#")) return@forEachIndexed
        fun fail(reason: String): Nothing =
            throw MalformedVersionTitlesException("$VERSION_TITLES_HE_RESOURCE:$lineNumber: $reason: $text")

        val sides = text.split(" => ")
        if (sides.size != 2) fail("expected exactly one ' => '")
        val heTitle = sides[1].trim()
        if (heTitle.isEmpty()) fail("empty Hebrew title")
        val keyParts = sides[0].split('|')
        if (keyParts.size > 2) fail("more than one '|'")
        val book = if (keyParts.size == 2) keyParts[0].trim().ifEmpty { fail("empty book name") } else null
        val versionTitle = keyParts.last().trim().ifEmpty { fail("empty versionTitle") }
        lineByKey.put(book to versionTitle, lineNumber)?.let { fail("duplicate key, already on line $it") }
        rows += VersionTitleRow(lineNumber, text, book, versionTitle, heTitle)
    }
    return VersionHebrewTitles(rows)
}
