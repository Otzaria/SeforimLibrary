package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.DefaultCommentatorPosition
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Generates links between Havrouta commentaries and their corresponding Talmud tractates.
 *
 * Havrouta books contain the original Talmud text in bold (<b>...</b> tags).
 * This script extracts the bold text and matches it to the corresponding Talmud lines.
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateHavroutaLinks -PseforimDb=/path/to.db
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("GenerateHavroutaLinks")

    val dbPath = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()

    val jdbcUrl = "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    val repository = SeforimRepository(dbPath, driver)
    // The repository init downgrades the GLOBAL kermit severity to Assert;
    // restore Info so this CLI's logs stay visible.
    Logger.setMinSeverity(Severity.Info)

    val sourceDir = System.getProperty("sourceDir")
        ?: System.getenv("OTZARIA_SOURCE_DIR")
        ?: OtzariaFetcher.ensureLocalSource(logger).toString()

    // ─── IdAllocator (delta-update support) ────────────────────────────────────
    val buildStatePath: Path = run {
        val explicit = System.getProperty("buildStatePath") ?: System.getenv("BUILD_STATE_PATH")
        if (explicit != null) Paths.get(explicit) else Paths.get("$dbPath.buildstate")
    }
    val prev = buildStatePath.takeIf { java.nio.file.Files.exists(it) }
    val allocator = InMemoryIdAllocator.load(prev, Logger.withTag("IdAllocator"))
    val bindings = IdAllocatorBindings(allocator, repository)
    // Pre-register every connection type so link ids resolve deterministically.
    ConnectionType.values().forEach { bindings.upsertConnectionType(it.name) }

    try {
        // Performance optimizations
        logger.i { "Setting PRAGMA for bulk operations..." }
        repository.executeRawQuery("PRAGMA foreign_keys = OFF")
        repository.executeRawQuery("PRAGMA synchronous = OFF")
        repository.executeRawQuery("PRAGMA journal_mode = OFF")

        logger.i { "Starting Havrouta-Talmud link generation..." }
        val talmudLinksCreated = generateHavroutaLinks(repository, bindings, logger)
        logger.i { "Havrouta-Talmud link generation completed. Created $talmudLinksCreated links." }

        logger.i { "Starting Havrouta-Hearot link generation from Otzaria files..." }
        val hearotLinksCreated = generateHavroutaHearotLinks(repository, bindings, logger, sourceDir)
        logger.i { "Havrouta-Hearot link generation completed. Created $hearotLinksCreated links." }

        logger.i { "Setting Hearot as default commentators for their base books..." }
        setHearotAsDefaultCommentators(repository, driver, logger)

        // No transitive Talmud→Hearot links: notes on Havrouta are a commentary on a
        // commentary, not on the Talmud (Otzaria/otzaria#1001).
        logger.i { "Total links created: ${talmudLinksCreated + hearotLinksCreated}" }

        // Restore PRAGMAs
        logger.i { "Restoring PRAGMA settings..." }
        repository.executeRawQuery("PRAGMA foreign_keys = ON")
        repository.executeRawQuery("PRAGMA synchronous = NORMAL")
        repository.executeRawQuery("PRAGMA journal_mode = WAL")

        // Persist build_state so subsequent runs preserve link ids. This stage
        // writes straight to the on-disk DB (no VACUUM INTO), so there is no
        // persist step for the snapshot to run ahead of.
        val buildStateMeta = mapOf(
            "generator" to "havroutalinks",
            "generated_at" to java.time.Instant.now().toString(),
        )
        runCatching {
            allocator.snapshotTo(target = buildStatePath, extraMeta = buildStateMeta)
        }.onFailure { e ->
            // Fail closed: a build that cannot write its allocator state would
            // publish last week's — and the build after it would re-issue ids
            // this one already handed out.
            logger.e(e) { "Failed to write build_state to $buildStatePath" }
            throw e
        }
        BuildStateVerifier.verifyFreshSnapshot(
            buildStatePath = buildStatePath,
            dbPath = Paths.get(dbPath),
            expectedMeta = buildStateMeta,
            logger = logger,
        )
    } catch (e: Exception) {
        logger.e(e) { "Error generating Havrouta links" }
        throw e
    } finally {
        repository.close()
    }
}

/**
 * Data class representing a daf section with its line range.
 */
private data class DafSection(
    val dafRef: String,
    val startLineIndex: Int,
    val endLineIndex: Int  // exclusive
)

/**
 * Mapping of Havrouta tractate names to Talmud tractate names (for special cases).
 */
private val tractateNameMapping = mapOf(
    "נידה" to "נדה"
)

/**
 * Regex to extract bold text from Havrouta lines.
 */
private val boldPattern = Regex("""<b>([^<]+)</b>""")

/**
 * Regex to extract daf headers.
 */
private val havroutaDafPattern = Regex("""<h3>\s*דף\s+([^<]+)</h3>""")
private val talmudDafPattern = Regex("""<h2>דף\s*([^<]+)</h2>""")

/**
 * Normalizes text for comparison by removing nikud, punctuation, and extra spaces.
 */
private fun normalizeText(text: String): String {
    return text
        // Remove Hebrew nikud (vowel marks)
        .replace(Regex("[\u0591-\u05C7]"), "")
        // Remove punctuation and special characters
        .replace(Regex("[()\\[\\]{}\"',.;:!?׳״]"), "")
        // Normalize spaces
        .replace(Regex("\\s+"), " ")
        .trim()
        .lowercase()
}

/**
 * Extracts bold text from a Havrouta line.
 */
private fun extractBoldText(content: String): String {
    val matches = boldPattern.findAll(content)
    return matches.map { it.groupValues[1] }.joinToString(" ")
}

/**
 * Checks if a line is a section header (מתניתין, גמרא, etc.)
 */
private fun isSectionHeader(content: String): Boolean {
    val headerPatterns = listOf(
        "<big><b>מתניתין",
        "<big><b>גמרא",
        "<big><b>הדרן",
        "<h2>", "<h3>", "<h4>"
    )
    return headerPatterns.any { content.contains(it) }
}

/**
 * Generates links between Havrouta books and their corresponding Talmud tractates.
 *
 * `internal` rather than private so the found-vs-processed accounting can be
 * tested: the audited build found 38 Havrouta books and processed 37 without
 * saying which one it dropped or why.
 */
internal suspend fun generateHavroutaLinks(
    repository: SeforimRepository,
    bindings: IdAllocatorBindings,
    logger: Logger
): Int {
    val ctCommentary = bindings.upsertConnectionType(ConnectionType.COMMENTARY.name)
    // Find all Havrouta books
    val havroutaBooks = repository.getAllBooks().filter { it.title.startsWith("חברותא על ") }
    logger.i { "Found ${havroutaBooks.size} Havrouta books" }

    // Find all Talmud Bavli tractates
    val talmudBooks = repository.getAllBooks().filter { book ->
        book.sourceId == 1L &&
            !book.title.startsWith("משנה") &&
            !book.title.startsWith("תלמוד ירושלמי") &&
            !book.title.startsWith("תוספתא")
    }.associateBy { it.title }

    var totalLinksCreated = 0

    // Delete any existing Havrouta-Talmud and Havrouta-Hearot links before creating
    // new ones. Scoped to the two types this task owns — COMMENTARY (Talmud→Havrouta)
    // and FOOTNOTES (Havrouta→Hearot); a blanket delete would also kill the book's
    // LINKER citations when re-run on an already-built DB.
    for (havroutaBook in havroutaBooks) {
        repository.executeRawQuery(
            "DELETE FROM link WHERE (sourceBookId = ${havroutaBook.id} OR targetBookId = ${havroutaBook.id}) " +
                "AND connectionTypeId IN (SELECT id FROM connection_type WHERE name IN ('COMMENTARY', 'FOOTNOTES'))"
        )
    }
    logger.i { "Deleted existing Havrouta links" }

    // "Found N Havrouta books" followed by fewer "Processing:" lines used to be
    // the only trace that a book was dropped; the reason is named per book below
    // and the gap is closed by an explicit N/M line after the loop.
    var processedBooks = 0
    val unmatched = mutableListOf<String>()

    for (havroutaBook in havroutaBooks) {
        val tractateName = havroutaBook.title.removePrefix("חברותא על ")
        val talmudTractateName = tractateNameMapping[tractateName] ?: tractateName

        val talmudBook = talmudBooks[talmudTractateName]
        if (talmudBook == null) {
            unmatched += havroutaBook.title
            logger.w {
                "No Talmud match for: ${havroutaBook.title} — no book titled '$talmudTractateName' " +
                    "among the Bavli tractates (sourceId=1, excluding משנה / תלמוד ירושלמי / תוספתא); " +
                    "no links created for it"
            }
            continue
        }

        processedBooks++
        logger.i { "Processing: ${havroutaBook.title} -> ${talmudBook.title}" }

        val linksForBook = processBookPair(
            repository = repository,
            bindings = bindings,
            ctCommentary = ctCommentary,
            havroutaBookId = havroutaBook.id,
            talmudBookId = talmudBook.id,
            havroutaTotalLines = havroutaBook.totalLines,
            talmudTotalLines = talmudBook.totalLines,
            logger = logger
        )

        logger.i { "  Created $linksForBook links" }
        totalLinksCreated += linksForBook
    }

    if (unmatched.isEmpty()) {
        logger.i { "Havrouta-Talmud: $processedBooks/${havroutaBooks.size} books processed" }
    } else {
        logger.w {
            "Havrouta-Talmud: $processedBooks/${havroutaBooks.size} books processed, " +
                "${unmatched.size} skipped with no matching Talmud tractate: ${unmatched.joinToString()}"
        }
    }

    // Update book_has_links table
    updateBookHasLinks(repository, logger)

    return totalLinksCreated
}

/**
 * Processes a single Havrouta-Talmud book pair and creates links.
 */
private suspend fun processBookPair(
    repository: SeforimRepository,
    bindings: IdAllocatorBindings,
    ctCommentary: Long,
    havroutaBookId: Long,
    talmudBookId: Long,
    havroutaTotalLines: Int,
    talmudTotalLines: Int,
    logger: Logger
): Int {
    // Load all lines for both books
    val havroutaLines = repository.getLines(havroutaBookId, 0, havroutaTotalLines - 1)
    val talmudLines = repository.getLines(talmudBookId, 0, talmudTotalLines - 1)

    // Extract daf sections from both books
    val havroutaDafs = extractDafSections(havroutaLines.map { it.content }, havroutaDafPattern)
    val talmudDafs = extractDafSections(talmudLines.map { it.content }, talmudDafPattern)

    // Create map of dafRef -> Talmud lines in that daf
    val talmudLinesByDaf = mutableMapOf<String, List<IndexedValue<String>>>()
    for (daf in talmudDafs) {
        val linesInDaf = talmudLines
            .filter { it.lineIndex >= daf.startLineIndex && it.lineIndex < daf.endLineIndex }
            .map { IndexedValue(it.lineIndex, normalizeText(it.content)) }
        talmudLinesByDaf[daf.dafRef] = linesInDaf
    }

    // Create map of lineIndex -> lineId for Talmud
    val talmudLineIdByIndex = talmudLines.associate { it.lineIndex to it.id }

    var linksCreated = 0
    val linkBatch = mutableListOf<Link>()

    // Process each Havrouta line
    var currentDafRef: String? = null
    var lastMatchedIndex = 0  // Track last matched line for sequential reading
    for (havroutaLine in havroutaLines) {
        // Check if this is a daf header
        val dafMatch = havroutaDafPattern.find(havroutaLine.content)
        if (dafMatch != null) {
            currentDafRef = dafMatch.groupValues[1].trim()
            lastMatchedIndex = 0  // Reset for new daf
            continue
        }

        // Skip if we haven't encountered a daf yet
        if (currentDafRef == null) continue

        // Skip section headers
        if (isSectionHeader(havroutaLine.content)) continue

        // Extract bold text
        val boldText = extractBoldText(havroutaLine.content)
        if (boldText.isBlank()) continue

        val normalizedBold = normalizeText(boldText)
        if (normalizedBold.length < 5) continue  // Skip very short matches

        // Find matching Talmud line in the same daf
        val talmudLinesInDaf = talmudLinesByDaf[currentDafRef] ?: continue

        // Find the best matching line
        val matchingLineIndex = findBestMatch(normalizedBold, talmudLinesInDaf, lastMatchedIndex)
        if (matchingLineIndex == null) continue

        lastMatchedIndex = matchingLineIndex  // Update for sequential reading

        val talmudLineId = talmudLineIdByIndex[matchingLineIndex] ?: continue

        // Single canonical direction Talmud → Havrouta (base → commentary).
        // The reverse SOURCE view is synthesized at read time by the repository.
        linkBatch.add(Link(
            id = bindings.allocator.linkId(talmudLineId, havroutaLine.id, ctCommentary),
            sourceBookId = talmudBookId,
            targetBookId = havroutaBookId,
            sourceLineId = talmudLineId,
            targetLineId = havroutaLine.id,
            targetLineIndex = havroutaLine.lineIndex,
            connectionType = ConnectionType.COMMENTARY
        ))

        linksCreated += 1

        // Batch insert
        if (linkBatch.size >= 1000) {
            repository.insertLinksBatch(linkBatch)
            linkBatch.clear()
        }
    }

    // Insert remaining links
    if (linkBatch.isNotEmpty()) {
        repository.insertLinksBatch(linkBatch)
    }

    return linksCreated
}

/**
 * Extracts daf sections from a list of line contents.
 */
private fun extractDafSections(contents: List<String>, pattern: Regex): List<DafSection> {
    val sections = mutableListOf<DafSection>()
    var lastDafRef: String? = null
    var lastStartIndex = 0

    for ((index, content) in contents.withIndex()) {
        val match = pattern.find(content)
        if (match != null) {
            // Close previous section
            if (lastDafRef != null) {
                sections.add(DafSection(lastDafRef, lastStartIndex, index))
            }
            lastDafRef = match.groupValues[1].trim()
            lastStartIndex = index
        }
    }

    // Close last section
    if (lastDafRef != null) {
        sections.add(DafSection(lastDafRef, lastStartIndex, contents.size))
    }

    return sections
}

/**
 * Finds the best matching Talmud line for a given bold text.
 * Uses a sequential approach where we start searching from lastMatchedIndex.
 * Returns the lineIndex of the best match, or null if no good match found.
 */
private fun findBestMatch(
    normalizedBoldText: String,
    talmudLines: List<IndexedValue<String>>,
    lastMatchedIndex: Int
): Int? {
    if (normalizedBoldText.length < 3) return null

    // Search all lines but prefer lines >= lastMatchedIndex
    val linesAfter = talmudLines.filter { it.index >= lastMatchedIndex }
    val linesBefore = talmudLines.filter { it.index < lastMatchedIndex }

    // First try exact substring match on lines after lastMatchedIndex
    for (line in linesAfter) {
        if (line.value.contains(normalizedBoldText)) {
            return line.index
        }
    }

    // Try matching first significant words on lines after
    val words = normalizedBoldText.split(" ").filter { it.length > 1 }
    if (words.size >= 2) {
        val firstWords = words.take(3).joinToString(" ")
        for (line in linesAfter) {
            if (line.value.contains(firstWords)) {
                return line.index
            }
        }
    }

    // Try significant overlap (but only forward) - this catches minor text variations
    if (normalizedBoldText.length >= 8) {
        for (line in linesAfter) {
            if (hasSignificantOverlap(normalizedBoldText, line.value, 8)) {
                return line.index
            }
        }
    }

    // If nothing found forward, search backwards (but be stricter)
    // Only use exact substring match when going backwards
    for (line in linesBefore) {
        if (line.value.contains(normalizedBoldText)) {
            return line.index
        }
    }

    // Last resort: try first 2-3 words on all lines
    if (words.size >= 2) {
        val twoWords = words.take(2).joinToString(" ")
        for (line in linesAfter + linesBefore) {
            if (line.value.contains(twoWords)) {
                return line.index
            }
        }
    }

    return null
}

/**
 * Checks if two strings have significant overlap (shared substring of minLen+ chars).
 */
private fun hasSignificantOverlap(text1: String, text2: String, minLen: Int = 6): Boolean {
    if (text1.length < minLen || text2.length < minLen) return false

    // Check overlapping substrings from text1
    for (i in 0..text1.length - minLen) {
        val sub = text1.substring(i, minOf(i + minLen + 4, text1.length))
        if (text2.contains(sub)) return true
    }
    return false
}

/**
 * Updates the book_has_links table after generating links.
 */
private suspend fun updateBookHasLinks(repository: SeforimRepository, logger: Logger) {
    logger.i { "Updating book_has_links table..." }

    repository.executeRawQuery(
        "INSERT OR IGNORE INTO book_has_links(bookId, hasSourceLinks, hasTargetLinks) " +
            "SELECT id, 0, 0 FROM book"
    )

    repository.executeRawQuery(
        "UPDATE book_has_links SET hasSourceLinks=1 " +
            "WHERE bookId IN (SELECT DISTINCT sourceBookId FROM link)"
    )

    repository.executeRawQuery(
        "UPDATE book_has_links SET hasTargetLinks=1 " +
            "WHERE bookId IN (SELECT DISTINCT targetBookId FROM link)"
    )

    repository.executeRawQuery(
        "UPDATE book SET hasCommentaryConnection=1 WHERE id IN (" +
            "SELECT DISTINCT sourceBookId FROM link l " +
            "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
            "WHERE ct.name='COMMENTARY'" +
            ")"
    )

    repository.executeRawQuery(
        "UPDATE book SET hasSourceConnection=1 WHERE id IN (" +
            "SELECT DISTINCT sourceBookId FROM link l " +
            "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
            "WHERE ct.name='SOURCE'" +
            ")"
    )

    logger.i { "book_has_links table updated" }
}

/**
 * Data class for parsing Otzaria link JSON files.
 */
@Serializable
private data class OtzariaLinkData(
    val line_index_1: Long,
    val line_index_2: Long,
    val heRef_2: String,
    val path_2: String,
    @SerialName("Conection Type")
    val connectionType: String
)

private val json = Json {
    ignoreUnknownKeys = true
    coerceInputValues = true
}

/**
 * Generates links between Havrouta and Hearot al Havrouta using Otzaria link files.
 * Optimized to load all data into RAM for fast processing.
 */
private suspend fun generateHavroutaHearotLinks(
    repository: SeforimRepository,
    bindings: IdAllocatorBindings,
    logger: Logger,
    sourceDir: String
): Int {
    val ctFootnotes = bindings.upsertConnectionType(ConnectionType.FOOTNOTES.name)
    val linksDir = File(sourceDir, "links")
    if (!linksDir.exists()) {
        logger.w { "Links directory not found: ${linksDir.absolutePath}" }
        return 0
    }

    // Get all books and build lookup maps
    logger.i { "Loading all books into RAM..." }
    val allBooks = repository.getAllBooks()
    val havroutaBooks = allBooks.filter { it.title.startsWith("חברותא על ") }
    val hearotBooks = allBooks.filter { it.title.startsWith("הערות על חברותא") }
    val booksByTitle = allBooks.associateBy { it.title }

    logger.i { "Found ${havroutaBooks.size} Havrouta books and ${hearotBooks.size} Hearot books" }

    // Preload all line IDs for Havrouta and Hearot books into RAM
    // Map: bookId -> (lineIndex -> lineId)
    logger.i { "Loading all line IDs into RAM..." }
    val lineIdCache = mutableMapOf<Long, Map<Int, Long>>()

    for (book in havroutaBooks + hearotBooks) {
        val lines = repository.getLines(book.id, 0, book.totalLines - 1)
        lineIdCache[book.id] = lines.associate { it.lineIndex to it.id }
    }
    logger.i { "Loaded line IDs for ${lineIdCache.size} books" }

    // Preload all link files into RAM
    logger.i { "Loading all Havrouta link files into RAM..." }
    val allLinkData = mutableMapOf<String, List<OtzariaLinkData>>()
    for (havroutaBook in havroutaBooks) {
        val linkFileName = "${havroutaBook.title}_links.json"
        val linkFile = File(linksDir, linkFileName)
        if (linkFile.exists()) {
            try {
                val content = linkFile.readText()
                val links: List<OtzariaLinkData> = json.decodeFromString(content)
                val hearotLinks = links.filter { it.path_2.contains("הערות על חברותא") }
                if (hearotLinks.isNotEmpty()) {
                    allLinkData[havroutaBook.title] = hearotLinks
                }
            } catch (e: Exception) {
                logger.w { "Failed to parse ${linkFileName}: ${e.message}" }
            }
        }
    }
    logger.i { "Loaded ${allLinkData.size} link files with Hearot links" }

    // Process all links in RAM and batch insert
    logger.i { "Processing links..." }
    val allLinks = mutableListOf<Link>()
    var skippedSource = 0
    var skippedTarget = 0
    var skippedBook = 0

    for ((bookTitle, links) in allLinkData) {
        val havroutaBook = booksByTitle[bookTitle] ?: continue
        val sourceLineIds = lineIdCache[havroutaBook.id] ?: continue

        for (linkData in links) {
            // Extract target book title from path
            val targetTitle = linkData.path_2.split('\\').last().substringBeforeLast('.')
            val targetBook = booksByTitle[targetTitle]

            if (targetBook == null) {
                skippedBook++
                continue
            }

            val targetLineIds = lineIdCache[targetBook.id]
            if (targetLineIds == null) {
                skippedBook++
                continue
            }

            // Get line indices (Otzaria links use 1-based, our DB uses 0-based)
            val sourceLineIndex = linkData.line_index_1.toInt() - 1
            val targetLineIndex = linkData.line_index_2.toInt() - 1

            val sourceLineId = sourceLineIds[sourceLineIndex]
            if (sourceLineId == null) {
                skippedSource++
                continue
            }

            val targetLineId = targetLineIds[targetLineIndex]
            if (targetLineId == null) {
                skippedTarget++
                continue
            }

            // Single canonical direction Havrouta → Hearot (base → notes).
            // SOURCE is synthesized at read time.
            allLinks.add(Link(
                id = bindings.allocator.linkId(sourceLineId, targetLineId, ctFootnotes),
                sourceBookId = havroutaBook.id,
                targetBookId = targetBook.id,
                sourceLineId = sourceLineId,
                targetLineId = targetLineId,
                targetLineIndex = targetLineIndex,
                connectionType = ConnectionType.FOOTNOTES
            ))
        }
    }

    logger.i { "Prepared ${allLinks.size} links (skipped: $skippedBook book not found, $skippedSource source line, $skippedTarget target line)" }

    // Batch insert all links using optimized method
    logger.i { "Inserting ${allLinks.size} links into database..." }
    repository.insertLinksBatch(allLinks)

    logger.i { "Inserted ${allLinks.size} Havrouta-Hearot links" }
    return allLinks.size
}

/**
 * Sets each notes companion as a default commentator of the book it annotates, so
 * its notes are visible on open instead of waiting for the reader to know that a
 * "הערות על X" book exists and to tick it by hand. Without this the notes reach no
 * display path at all: a dependent-text link only enters `linksByLine` once its
 * target book is an active commentator, which is what the printed-marker popup
 * reads.
 *
 * A pair qualifies on either of two grounds:
 *  - the link is typed [ConnectionType.FOOTNOTES] — the authoritative signal, and
 *    the only one for a companion whose title does not follow the "הערות על" shape;
 *  - the target is titled exactly "הערות על <source title>" — legacy links still
 *    stored as COMMENTARY, kept so a library that has not been retyped yet does not
 *    regress. The equality (rather than a LIKE prefix) is what keeps the legacy transitive
 *    Talmud→Hearot layer (older DBs) out: those are COMMENTARY and their source is the tractate,
 *    not the annotated Havrouta volume, so on a re-run over an already-built DB they
 *    can no longer turn a tractate into a notes reader.
 *
 * Appends to existing defaults so seeded ones survive.
 */
private suspend fun setHearotAsDefaultCommentators(
    repository: SeforimRepository,
    driver: JdbcSqliteDriver,
    logger: Logger
) {
    val pairs = driver.executeQuery(
        null,
        "SELECT DISTINCT l.sourceBookId, l.targetBookId FROM link l " +
            "JOIN book tb ON tb.id = l.targetBookId " +
            "JOIN book sb ON sb.id = l.sourceBookId " +
            "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
            "WHERE ct.name = 'FOOTNOTES' OR tb.title = 'הערות על ' || sb.title " +
            "ORDER BY l.sourceBookId, l.targetBookId",
        { cursor ->
            val list = mutableListOf<Pair<Long, Long>>()
            while (cursor.next().value) {
                list.add(cursor.getLong(0)!! to cursor.getLong(1)!!)
            }
            QueryResult.Value(list)
        },
        0
    ).value

    var count = 0
    var alreadySet = 0
    for ((baseBookId, hearotBookId) in pairs) {
        val existing = repository.getDefaultCommentatorsForBook(baseBookId)
        if (existing.any { it.commentatorBookId == hearotBookId }) { alreadySet++; continue }
        val nextPosition = (existing.maxOfOrNull { it.position } ?: -1) + 1
        repository.setDefaultCommentatorsForBook(
            baseBookId,
            existing + DefaultCommentatorPosition(hearotBookId, nextPosition)
        )
        count++
    }

    // Fail closed. Silence here is indistinguishable from success in the log, and the
    // only symptom downstream is "the notes do not open" — a DB shipped that way is
    // not reportable as a generator failure by anyone who sees it.
    if (pairs.isEmpty()) {
        val companions = driver.executeQuery(
            null,
            "SELECT COUNT(*) FROM book WHERE title LIKE 'הערות על %'",
            { cursor -> cursor.next(); QueryResult.Value(cursor.getLong(0) ?: 0L) },
            0
        ).value
        check(companions == 0L) {
            "Found $companions 'הערות על' companion books but not one base→notes pair: " +
                "no book will open its notes. Expect FOOTNOTES-typed links from the base " +
                "book, or a companion titled exactly 'הערות על <base title>'."
        }
    }

    logger.i { "Set default commentators for $count base→hearot pairs ($alreadySet already set)" }
}
