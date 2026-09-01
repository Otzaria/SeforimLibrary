package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.sefariasqlite.BookPayload
import io.github.kdroidfilter.seforimlibrary.sefariasqlite.RefEntry
import io.github.kdroidfilter.seforimlibrary.sefariasqlite.SefariaBookPayloadReader
import io.github.kdroidfilter.seforimlibrary.sefariasqlite.filterBlacklistedPayloads
import io.github.kdroidfilter.seforimlibrary.sefariasqlite.findDatabaseExportRoot
import io.github.kdroidfilter.seforimlibrary.sefariasqlite.loadSefariaBlacklists
import kotlinx.serialization.json.Json
import java.nio.file.Path

internal data class ManualBookIndex(
    val enTitle: String,
    val heTitle: String,
    val lineCount: Int,
    private val retainedLines: Map<Int, String>,
    private var proofLines: List<String>?,
    val refsByRef: Map<String, List<RefEntry>>,
    val refsByHeRef: Map<String, List<RefEntry>>,
    val refsByLineIndex: Map<Int, List<RefEntry>>,
) {
    fun retainedContent(lineIndex: Int): String? = retainedLines[lineIndex] ?: proofLines?.getOrNull(lineIndex - 1)

    fun fullProofLines(): List<String> = requireNotNull(proofLines) { "Full proof lines were not retained for $heTitle" }

    fun releaseFullProofLines() {
        proofLines = null
    }

    internal val retainedAnchorLineCount: Int get() = retainedLines.size
    internal val retainedFullLineCount: Int get() = proofLines?.size ?: 0
}

internal data class RetainedLineRequirements(
    val refs: Set<String> = emptySet(),
    val lineIndexes: Set<Int> = emptySet(),
)

internal data class SefariaCorpusStats(
    val mergedFilesScanned: Int,
    val payloadsLoaded: Int,
    val payloadsAccepted: Int,
    val payloadsBlacklisted: Int,
    val retainedAnchorLines: Int,
    val fullLineBooksLoaded: Int,
    val fullLinesLoaded: Int,
)

internal class SefariaCorpusIndex private constructor(
    private val byPrimaryHeTitle: Map<String, List<ManualBookIndex>>,
    val stats: SefariaCorpusStats,
) {
    fun primaryHeTitleCount(title: String): Int = byPrimaryHeTitle[title]?.size ?: 0

    fun hasPrimaryHeTitle(title: String): Boolean = primaryHeTitleCount(title) == 1

    fun bookByHeTitle(title: String): ManualBookIndex = exactlyOne(byPrimaryHeTitle[title], "Sefaria book heTitle '$title'")

    fun resolveRef(book: ManualBookIndex, ref: String): RefEntry = exactlyOne(book.refsByRef[ref], "ref '$ref' in ${book.enTitle}")

    fun resolveHeRef(book: ManualBookIndex, heRef: String): RefEntry = exactlyOne(book.refsByHeRef[heRef], "heRef '$heRef' in ${book.enTitle}")

    fun resolveHeRefOrNullIfMissing(book: ManualBookIndex, heRef: String): RefEntry? {
        val values = book.refsByHeRef[heRef].orEmpty()
        require(values.size <= 1) { "heRef '$heRef' in ${book.enTitle} is ambiguous; found ${values.size}" }
        return values.singleOrNull()
    }

    fun resolveLine(book: ManualBookIndex, lineIndex: Int): RefEntry =
        exactlyOne(book.refsByLineIndex[lineIndex], "lineIndex $lineIndex in ${book.enTitle}")

    private fun <T> exactlyOne(values: List<T>?, description: String): T {
        require(values?.size == 1) { "$description must resolve exactly once; found ${values?.size ?: 0}" }
        return values.single()
    }

    companion object {
        fun load(
            exportRoot: Path,
            candidates: Set<String>,
            retainedLines: Map<String, RetainedLineRequirements>,
            fullLineTitles: Set<String>,
            logger: Logger,
        ): SefariaCorpusIndex {
            val dbRoot = findDatabaseExportRoot(exportRoot)
            val jsonDir = dbRoot.resolve("json")
            val schemaDir = dbRoot.resolve("schemas")
            val reader = SefariaBookPayloadReader(Json { ignoreUnknownKeys = true; coerceInputValues = true }, logger)
            val schemaLookup = reader.buildSchemaLookup(schemaDir)
            val blacklists = loadSefariaBlacklists(SefariaCorpusIndex::class.java.classLoader, logger)
            require(!blacklists.isEmpty()) { "Sefaria book/author blacklist resources are missing" }
            val accepted = ArrayList<ManualBookIndex>()
            var rejected = 0
            val readerStats = reader.readSelectedBooks(jsonDir, schemaDir, schemaLookup, candidates) { payload ->
                val filtered = filterBlacklistedPayloads(listOf(payload), blacklists).payloads.singleOrNull()
                if (filtered == null) {
                    rejected++
                } else {
                    accepted += filtered.toManualIndex(
                        requirements = retainedLines[filtered.heTitle] ?: RetainedLineRequirements(),
                        retainFullLines = filtered.heTitle in fullLineTitles,
                    )
                }
            }
            return SefariaCorpusIndex(
                byPrimaryHeTitle = accepted.groupBy { it.heTitle },
                stats = SefariaCorpusStats(
                    mergedFilesScanned = readerStats.mergedFilesScanned,
                    payloadsLoaded = readerStats.payloadsLoaded,
                    payloadsAccepted = accepted.size,
                    payloadsBlacklisted = rejected,
                    retainedAnchorLines = accepted.sumOf { it.retainedAnchorLineCount },
                    fullLineBooksLoaded = accepted.count { it.retainedFullLineCount > 0 },
                    fullLinesLoaded = accepted.sumOf { it.retainedFullLineCount },
                ),
            )
        }

        internal fun fromBooks(books: List<ManualBookIndex>): SefariaCorpusIndex = SefariaCorpusIndex(
            byPrimaryHeTitle = books.groupBy { it.heTitle },
            stats = SefariaCorpusStats(
                mergedFilesScanned = books.size,
                payloadsLoaded = books.size,
                payloadsAccepted = books.size,
                payloadsBlacklisted = 0,
                retainedAnchorLines = books.sumOf { it.retainedAnchorLineCount },
                fullLineBooksLoaded = books.count { it.retainedFullLineCount > 0 },
                fullLinesLoaded = books.sumOf { it.retainedFullLineCount },
            ),
        )
    }
}

internal fun BookPayload.toManualIndex(
    requirements: RetainedLineRequirements,
    retainFullLines: Boolean,
): ManualBookIndex {
    val requiredIndexes = buildSet {
        addAll(requirements.lineIndexes)
        refEntries.filter { it.ref in requirements.refs }.forEach { add(it.lineIndex) }
    }
    val retained = requiredIndexes.associateWith { lineIndex ->
        lines.getOrNull(lineIndex - 1) ?: error("Required retained line $lineIndex is outside $heTitle")
    }
    return ManualBookIndex(
        enTitle = enTitle,
        heTitle = heTitle,
        lineCount = lines.size,
        retainedLines = retained,
        proofLines = lines.takeIf { retainFullLines },
        refsByRef = refEntries.groupBy { it.ref },
        refsByHeRef = refEntries.groupBy { it.heRef },
        refsByLineIndex = refEntries.groupBy { it.lineIndex },
    )
}

/** Explicit per-root Otzaria-title to Sefaria-heTitle bridge; no normalization and no fuzzy fallback. */
internal class SefariaTitleAliases(private val aliasesByRoot: Map<String, Map<String, String>>) {
    private val used = linkedSetOf<Pair<String, String>>()

    /** Applies to both link sides, so a source-side alias needs no further plumbing. */
    fun sefariaHeTitle(repositoryPath: String, title: String): String {
        val root = rootFor(repositoryPath) ?: return title
        val alias = root.value[title] ?: return title
        used += root.key to title
        return alias
    }

    /** Reverse lookup used only to explain a Sefaria rename that invalidates a declared alias. */
    fun otzariaTitleFor(repositoryPath: String, sefariaHeTitle: String): String? =
        rootFor(repositoryPath)?.value?.entries?.singleOrNull { it.value == sefariaHeTitle }?.key

    private fun rootFor(repositoryPath: String): Map.Entry<String, Map<String, String>>? {
        val matches = aliasesByRoot.entries.filter { repositoryPath.startsWith(it.key + "/") }
        require(matches.size <= 1) { "Multiple he_title_aliases roots match $repositoryPath" }
        return matches.singleOrNull()
    }

    fun requireEachAliasResolvesToOneBook(index: SefariaCorpusIndex) {
        aliasesByRoot.forEach { (root, aliases) ->
            aliases.forEach { (title, heTitle) ->
                require(index.primaryHeTitleCount(heTitle) == 1) {
                    "he_title_aliases[$root][$title] must resolve to exactly one Sefaria book: $heTitle"
                }
            }
        }
    }

    fun requireEveryAliasIsUsed() {
        val declared = aliasesByRoot.flatMap { (root, aliases) -> aliases.keys.map { root to it } }.toSet()
        require(used == declared) { "Unused he_title_aliases: ${declared - used}" }
    }
}

internal fun sourceTitle(fileName: String): String {
    require(fileName.endsWith("_links.json")) { "Not a links file: $fileName" }
    return fileName.removeSuffix("_links.json").takeIf { it.isNotBlank() } ?: error("Empty source title")
}

/** Non-Sefaria records outside this tool's scope may use extensionless paths. */
internal fun targetTitleOrNull(path2: String): String? {
    val normalized = path2.replace('\\', '/')
    val component = normalized.substringAfterLast('/', missingDelimiterValue = normalized)
    if (!component.endsWith(".txt") || component.length <= 4) return null
    return component.removeSuffix(".txt")
}
