package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
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
 *  - pub dates and pub place are created through the IdAllocator and linked.
 *
 * pub_date and pub_place are IdAllocator-managed tables: creating rows via the
 * allocator (not raw INSERT) keeps their ids stable across builds, which the
 * delta-update pipeline relies on. build_state is snapshotted at the end.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:seedAllMetadata -PseforimDb=/path/to/seforim.db
 * Env: SEFORIM_DB
 */
private const val ALL_METADATA_FILE = "all_metadata.json"
private const val METADATA_CHANGES_FILE = "sefaria_metadata_changes.csv"

private val json = Json { ignoreUnknownKeys = true }

fun main(args: Array<String>) = runBlocking {
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

    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")
    val repository = SeforimRepository(dbPath.toString(), driver)

    val buildStatePath = resolveBuildStatePath(dbPath)
    val allocator = InMemoryIdAllocator.load(
        buildStatePath.takeIf { Files.exists(it) },
        Logger.withTag("IdAllocator"),
    )
    val bindings = IdAllocatorBindings(allocator, repository)

    try {
        val result = applyMetadata(repository, bindings, bulk, descriptions, logger)
        runCatching {
            allocator.snapshotTo(
                target = buildStatePath,
                extraMeta = mapOf(
                    "generator" to "seedallmetadata",
                    "generated_at" to Instant.now().toString(),
                ),
            )
        }.onFailure { logger.w(it) { "Failed to write build_state to $buildStatePath" } }
        logger.i { "All-metadata done: updated=${result.updated} unmatched=${result.unmatched}" }
    } catch (e: Exception) {
        logger.e(e) { "Failed to seed all-metadata; aborting" }
        exitProcess(1)
    } finally {
        repository.close()
    }
}

/** Resolves the build_state path: -DbuildStatePath / BUILD_STATE_PATH, else `<db>.buildstate`. */
private fun resolveBuildStatePath(dbPath: Path): Path {
    val explicit = System.getProperty("buildStatePath") ?: System.getenv("BUILD_STATE_PATH")
    return if (explicit != null) Paths.get(explicit) else Paths.get("$dbPath.buildstate")
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

private suspend fun applyMetadata(
    repository: SeforimRepository,
    bindings: IdAllocatorBindings,
    bulk: Map<String, BulkMetadata>,
    descriptions: Map<String, Description>,
    logger: Logger,
): MetadataResult {
    val bookIdsByTitle = repository.getAllBookTitleIds().groupBy({ it.second }, { it.first })
    val sourceIdsByName = repository.getAllSources().associate { it.name.lowercase() to it.id }

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

        repository.updateBookMetadata(
            bookId = bookId,
            heShortDesc = description?.heShortDesc,
            heDesc = description?.heDesc,
            sourceId = sourceId,
        )

        // pub_date / pub_place go through the allocator for stable ids (delta-safety).
        meta?.pubDates?.forEach { year ->
            repository.linkPubDateToBook(bindings.upsertPubDate(year.toString()), bookId)
        }
        meta?.pubPlaceHe?.let { place ->
            repository.linkPubPlaceToBook(bindings.upsertPubPlace(place), bookId)
        }
        updated++
    }
    return MetadataResult(updated, unmatched)
}

private fun JsonObject.string(key: String): String? =
    this[key].stringOrNull()?.trim()?.takeIf { it.isNotBlank() }
