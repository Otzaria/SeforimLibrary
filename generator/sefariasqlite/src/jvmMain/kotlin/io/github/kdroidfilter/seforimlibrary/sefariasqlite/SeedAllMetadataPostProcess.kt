package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import java.sql.Connection
import java.sql.Types
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Enriches book metadata in an already-built seforim.db from two assets in the
 * `fordb-latest` release archive (same source as renameCategories/seedGenerations):
 *
 *  - ForDB/all_metadata.json — publication dates/places and source folder per book.
 *  - ForDB/sefaria_metadata_changes.csv — per-title description overrides
 *    (columns: categoryPath,title,author,heShortDesc,heDesc,heDescNew).
 *
 * Runs after appendOtzaria and renameCategories so every book exists under its
 * final title. Matching is exact by book title. The metadata covers the whole
 * library (a superset of any single generated DB), so unmatched titles are
 * skipped and counted — not fatal. Download/parse failures ARE fatal.
 *
 * Per matched book:
 *  - heShortDesc and heDesc (the latter from the CSV's heDescNew column) replace
 *    the existing value only when present (COALESCE keep-existing).
 *  - sourceId is set from Sourcefolder (existing source rows only).
 *  - pub dates and pub place are added (INSERT OR IGNORE links).
 *
 * Usage:
 *   ./gradlew :sefariasqlite:seedAllMetadata -PseforimDb=/path/to/seforim.db
 * Env: SEFORIM_DB
 */
private const val ALL_METADATA_FILE = "all_metadata.json"
private const val METADATA_CHANGES_FILE = "sefaria_metadata_changes.csv"

private val json = Json { ignoreUnknownKeys = true }

fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SeedAllMetadata")

    val dbPath = resolveSeforimDbPath(args)
    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }
    logger.i { "Seeding all-metadata in $dbPath" }

    val bulk = parseBulkMetadata(downloadRequiredForDbFile(ALL_METADATA_FILE, logger))
    val descriptions = parseDescriptionOverrides(downloadRequiredForDbFile(METADATA_CHANGES_FILE, logger))

    val result = withSeforimDbTransaction(dbPath, logger, "Failed to seed all-metadata; aborting") { conn ->
        applyMetadata(conn, bulk, descriptions, logger)
    }
    logger.i { "All-metadata done: updated=${result.updated} unmatched=${result.unmatched}" }
}

private data class BulkMetadata(
    val pubDates: List<Int>,
    val pubPlaceHe: String?,
    val sourcefolder: String?,
)

private data class Description(
    val heShortDesc: String?,
    val heDesc: String?,
)

private data class MetadataResult(val updated: Int, val unmatched: Int)

private fun parseBulkMetadata(lines: List<String>): Map<String, BulkMetadata> =
    json.parseToJsonElement(lines.joinToString("\n")).jsonArray.mapNotNull { element ->
        val obj = element as? JsonObject ?: return@mapNotNull null
        val title = obj.string("title") ?: return@mapNotNull null
        title to BulkMetadata(
            pubDates = (obj["pubDate"] as? JsonArray)?.mapNotNull { it.jsonPrimitive.intOrNull } ?: emptyList(),
            pubPlaceHe = obj.string("pubPlaceStringHe"),
            sourcefolder = obj.string("Sourcefolder"),
        )
    }.toMap()

/**
 * Parses per-title overrides from the changes CSV. Columns:
 * categoryPath,title,author,heShortDesc,heDesc,heDescNew. Only heShortDesc and
 * heDescNew are used (heDescNew is the corrected text that becomes the book's
 * heDesc); the original heDesc column and the rest are ignored.
 */
private fun parseDescriptionOverrides(lines: List<String>): Map<String, Description> {
    if (lines.isEmpty()) return emptyMap()
    require("title" in lines.first().lowercase()) {
        "$METADATA_CHANGES_FILE must start with a header row"
    }
    return lines.drop(1).mapNotNull { line ->
        if (line.isBlank()) return@mapNotNull null
        val cols = parseForDbCsvLine(line).map { it.trim() }
        val title = cols.getOrNull(1)?.takeIf { it.isNotBlank() } ?: return@mapNotNull null
        title to Description(
            heShortDesc = cols.getOrNull(3)?.takeIf { it.isNotBlank() },
            heDesc = cols.getOrNull(5)?.takeIf { it.isNotBlank() },
        )
    }.toMap()
}

private fun applyMetadata(
    conn: Connection,
    bulk: Map<String, BulkMetadata>,
    descriptions: Map<String, Description>,
    logger: Logger,
): MetadataResult {
    val bookIdsByTitle = loadBookIdsByTitle(conn)
    val sourceIdsByName = loadSourceIdsByName(conn)

    var updated = 0
    var unmatched = 0

    for (title in bulk.keys + descriptions.keys) {
        val ids = bookIdsByTitle[title]
        if (ids == null) {
            unmatched++
            continue
        }
        if (ids.size > 1) {
            logger.w { "all-metadata: '$title' has ${ids.size} matches; skipping" }
            continue
        }
        val bookId = ids.single()
        val meta = bulk[title]
        val description = descriptions[title]
        val sourceId = meta?.sourcefolder?.let { sourceIdsByName[it.lowercase()] }

        conn.prepareStatement(
            "UPDATE book SET heShortDesc = COALESCE(?, heShortDesc), heDesc = COALESCE(?, heDesc), " +
                "sourceId = COALESCE(?, sourceId) WHERE id = ?",
        ).use { st ->
            st.setString(1, description?.heShortDesc)
            st.setString(2, description?.heDesc)
            if (sourceId != null) st.setLong(3, sourceId) else st.setNull(3, Types.INTEGER)
            st.setLong(4, bookId)
            st.executeUpdate()
        }

        meta?.pubDates?.forEach { year ->
            linkLookup(
                conn, bookId, year.toString(),
                "INSERT OR IGNORE INTO pub_date (date) VALUES (?)",
                "SELECT id FROM pub_date WHERE date = ?",
                "INSERT OR IGNORE INTO book_pub_date (bookId, pubDateId) VALUES (?, ?)",
            )
        }
        meta?.pubPlaceHe?.let { place ->
            linkLookup(
                conn, bookId, place,
                "INSERT OR IGNORE INTO pub_place (name) VALUES (?)",
                "SELECT id FROM pub_place WHERE name = ?",
                "INSERT OR IGNORE INTO book_pub_place (bookId, pubPlaceId) VALUES (?, ?)",
            )
        }
        updated++
    }
    return MetadataResult(updated, unmatched)
}

private fun loadBookIdsByTitle(conn: Connection): Map<String, List<Long>> {
    val rows = ArrayList<Pair<String, Long>>()
    conn.createStatement().use { st ->
        st.executeQuery("SELECT id, title FROM book").use { rs ->
            while (rs.next()) rows += rs.getString(2) to rs.getLong(1)
        }
    }
    return rows.groupBy({ it.first }, { it.second })
}

private fun loadSourceIdsByName(conn: Connection): Map<String, Long> {
    val map = HashMap<String, Long>()
    conn.createStatement().use { st ->
        st.executeQuery("SELECT id, name FROM source").use { rs ->
            while (rs.next()) map[rs.getString(2).lowercase()] = rs.getLong(1)
        }
    }
    return map
}

/** Inserts [value] into a lookup table (if new) and links it to [bookId]. */
private fun linkLookup(
    conn: Connection,
    bookId: Long,
    value: String,
    insertSql: String,
    selectIdSql: String,
    linkSql: String,
) {
    conn.prepareStatement(insertSql).use { it.setString(1, value); it.executeUpdate() }
    val refId = conn.prepareStatement(selectIdSql).use { st ->
        st.setString(1, value)
        st.executeQuery().use { rs -> if (rs.next()) rs.getLong(1) else return }
    }
    conn.prepareStatement(linkSql).use { st ->
        st.setLong(1, bookId)
        st.setLong(2, refId)
        st.executeUpdate()
    }
}

private fun JsonObject.string(key: String): String? =
    this[key].stringOrNull()?.trim()?.takeIf { it.isNotBlank() }
