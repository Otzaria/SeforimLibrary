package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.common.reports.GeneratorReport
import io.github.kdroidfilter.seforimlibrary.core.text.normalizeCategoryPath
import io.github.kdroidfilter.seforimlibrary.dao.repository.CategoryDescriptionUpdate
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
 * the immutable release selected by `fordb_latest_pointer.json` (same source as
 * renameCategories/seedGenerations):
 *
 *  - ForDB/all_metadata.json — publication dates/places per book.
 *  - ForDB/sefaria_metadata_changes.csv — per-title description overrides
 *    (columns: categoryPath,title,author,heShortDesc,heDesc,heDescNew).
 *
 * Runs after appendOtzaria and renameCategories so every book exists under its
 * final title. Matching is exact by book title. The metadata covers the whole
 * library (a superset of any single generated DB), so unmatched titles are
 * skipped and counted — not fatal. Download/parse failures ARE fatal.
 *
 * Per matched book: heShortDesc/heDesc replaced only when present; pub dates/place
 * linked via the IdAllocator (stable ids for delta). The book's source is NOT set
 * here — it is authoritative from the manifest at import; all_metadata's Sourcefolder
 * is stale and ignored. build_state is snapshotted at the end.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:seedAllMetadata -PseforimDb=/path/to/seforim.db
 * Env: SEFORIM_DB
 */
private const val ALL_METADATA_FILE = "all_metadata.json"
private const val METADATA_CHANGES_FILE = "sefaria_metadata_changes.csv"
private val CATEGORY_DESCRIPTIONS_FILE = FOR_DB_CSV_FILES.getValue("categoryDescriptions")

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
    val categoryOverrides = parseCategoryDescriptionOverrides(
        downloadRequiredForDbFile(CATEGORY_DESCRIPTIONS_FILE, logger),
    )

    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")
    val repository = SeforimRepository(dbPath.toString(), driver)
    // The repository init downgrades the GLOBAL kermit severity to Assert;
    // restore Info so this CLI's logs stay visible.
    Logger.setMinSeverity(Severity.Info)

    try {
        val buildStatePath = resolveBuildStatePath(dbPath)
        val allocator = InMemoryIdAllocator.load(
            buildStatePath.takeIf { Files.exists(it) },
            Logger.withTag("IdAllocator"),
        )
        val bindings = IdAllocatorBindings(allocator, repository)

        // Resolve every fatal category-path assertion before the first metadata
        // write.  Otherwise a missing late category could leave earlier book/pub
        // metadata committed even though the task reports failure.
        val categoryPlan = planCategoryDescriptionOverrides(repository, categoryOverrides)
        val result = applyMetadata(repository, bindings, bulk, descriptions, logger)
        val categoryResult = applyCategoryDescriptionPlan(repository, categoryPlan, logger)
        // This stage writes straight to the on-disk DB (no VACUUM INTO), so there
        // is no persist step for the snapshot to run ahead of.
        val buildStateMeta = mapOf(
            "generator" to "seedallmetadata",
            "generated_at" to Instant.now().toString(),
        )
        runCatching {
            allocator.snapshotTo(target = buildStatePath, extraMeta = buildStateMeta)
        }.onFailure { e ->
            // Fail closed: a build that cannot write its allocator state would
            // publish last week's — and the build after it would re-issue ids
            // this one already handed out. The catch below turns this into
            // exitProcess(1).
            logger.e(e) { "Failed to write build_state to $buildStatePath" }
            throw e
        }
        BuildStateVerifier.verifyFreshSnapshot(
            buildStatePath = buildStatePath,
            dbPath = dbPath,
            expectedMeta = buildStateMeta,
            logger = logger,
        )
        logger.i {
            "All-metadata done: updated=${result.updated} unmatched=${result.unmatched}; " +
                "category records=${categoryResult.records} updates=${categoryResult.updated} " +
                "unchanged=${categoryResult.unchanged}"
        }
        reportUnmatchedMetadataTitles(result.unmatchedTitles, logger)
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

internal data class BulkMetadata(
    val pubDates: List<Int>,
    val pubPlaceHe: String?,
)

internal data class Description(
    val heShortDesc: String?,
    val heDesc: String?,
)

internal data class MetadataResult(
    val updated: Int,
    val unmatched: Int,
    /** The titles behind [unmatched], in iteration order. See [reportUnmatchedMetadataTitles]. */
    val unmatchedTitles: List<String> = emptyList(),
)

/** How many unmatched titles the summary names before deferring to the report file. */
private const val MAX_REPORTED_UNMATCHED_TITLES = 20

/**
 * One bounded WARN naming the ForDB metadata records that matched no book, plus
 * the complete list in a report file. The records are skipped exactly as before
 * — the metadata corpus legitimately covers the whole library, which is a
 * superset of any single generated DB — but "unmatched=1116" alone gave nobody
 * a way to tell a benign superset from a title-normalisation regression.
 */
internal fun reportUnmatchedMetadataTitles(titles: List<String>, logger: Logger) {
    if (titles.isEmpty()) return
    logger.w {
        "all-metadata: ${titles.size} ForDB metadata record(s) matched no book " +
            "(skipped): ${titles.take(MAX_REPORTED_UNMATCHED_TITLES).joinToString()}" +
            if (titles.size > MAX_REPORTED_UNMATCHED_TITLES) {
                ", … and ${titles.size - MAX_REPORTED_UNMATCHED_TITLES} more"
            } else {
                ""
            }
    }
    GeneratorReport.write("sefaria-all-metadata-unmatched", logger) {
        put("unmatched", titles.size.toLong())
        putStrings("titles", titles)
    }
}

internal sealed interface DescriptionEdit {
    data object Keep : DescriptionEdit
    data object Clear : DescriptionEdit
    data class Replace(val value: String) : DescriptionEdit
}

internal data class CategoryDescriptionOverride(
    val csvRecordNumber: Int,
    val canonicalPath: String,
    val shortEdit: DescriptionEdit,
    val longEdit: DescriptionEdit,
)

internal data class CategoryDescriptionApplyResult(
    val records: Int,
    val updated: Int,
    val unchanged: Int,
)

internal fun parseDescriptionEdit(raw: String): DescriptionEdit {
    val value = raw.trim()
    return when {
        value.isEmpty() -> DescriptionEdit.Keep
        value == "[מחק]" -> DescriptionEdit.Clear
        else -> DescriptionEdit.Replace(value)
    }
}

internal fun parseCategoryDescriptionOverrides(lines: List<String>): List<CategoryDescriptionOverride> {
    val records = parseForDbCsvRecords(lines.joinToString("\n"))
    val expectedHeader = listOf(
        "categoryPath",
        "heShortDesc",
        "heDesc",
        "heShortDescNew",
        "heDescNew",
    )
    require(records.firstOrNull() == expectedHeader) {
        buildString {
            append("$CATEGORY_DESCRIPTIONS_FILE must start with exactly ")
            append(expectedHeader.joinToString(","))
        }
    }

    val firstRecordByPath = mutableMapOf<String, Int>()
    return records.drop(1).mapIndexed { index, record ->
        val recordNumber = index + 2
        require(record.size == expectedHeader.size) {
            "$CATEGORY_DESCRIPTIONS_FILE record $recordNumber must contain exactly five fields"
        }
        val path = record[0]
        require(path.isNotBlank()) {
            "$CATEGORY_DESCRIPTIONS_FILE record $recordNumber has an empty categoryPath"
        }
        require(!path.startsWith('/') && !path.endsWith('/') && "//" !in path) {
            "$CATEGORY_DESCRIPTIONS_FILE record $recordNumber has empty category-path segment: '$path'"
        }
        val normalized = normalizeCategoryPath(path.split('/'))
        require(normalized == path) {
            "$CATEGORY_DESCRIPTIONS_FILE record $recordNumber has non-normalized categoryPath '$path'; " +
                "expected '$normalized'"
        }
        val previousRecord = firstRecordByPath.putIfAbsent(path, recordNumber)
        require(previousRecord == null) {
            "$CATEGORY_DESCRIPTIONS_FILE records $previousRecord and $recordNumber duplicate categoryPath '$path'"
        }
        CategoryDescriptionOverride(
            csvRecordNumber = recordNumber,
            canonicalPath = path,
            shortEdit = parseDescriptionEdit(record[3]),
            longEdit = parseDescriptionEdit(record[4]),
        )
    }
}

internal fun applyDescriptionEdit(current: String?, edit: DescriptionEdit): String? = when (edit) {
    DescriptionEdit.Keep -> current
    DescriptionEdit.Clear -> null
    is DescriptionEdit.Replace -> edit.value
}

internal suspend fun applyCategoryDescriptionOverrides(
    repository: SeforimRepository,
    overrides: List<CategoryDescriptionOverride>,
    logger: Logger,
): CategoryDescriptionApplyResult =
    applyCategoryDescriptionPlan(repository, planCategoryDescriptionOverrides(repository, overrides), logger)

internal data class CategoryDescriptionPlan(
    val records: Int,
    val updates: List<CategoryDescriptionUpdate>,
)

internal suspend fun planCategoryDescriptionOverrides(
    repository: SeforimRepository,
    overrides: List<CategoryDescriptionOverride>,
): CategoryDescriptionPlan {
    val rowsByPath = repository.getAllCategoryDescriptionRows().associateBy { it.canonicalPath }
    val updates = overrides.mapNotNull { override ->
        val current = requireNotNull(rowsByPath[override.canonicalPath]) {
            "$CATEGORY_DESCRIPTIONS_FILE record ${override.csvRecordNumber} references missing " +
                "category '${override.canonicalPath}'"
        }
        val shortValue = applyDescriptionEdit(current.heShortDesc, override.shortEdit)
        val longValue = applyDescriptionEdit(current.heDesc, override.longEdit)
        if (shortValue == current.heShortDesc && longValue == current.heDesc) {
            null
        } else {
            CategoryDescriptionUpdate(current.categoryId, shortValue, longValue)
        }
    }
    return CategoryDescriptionPlan(overrides.size, updates)
}

internal suspend fun applyCategoryDescriptionPlan(
    repository: SeforimRepository,
    plan: CategoryDescriptionPlan,
    logger: Logger,
): CategoryDescriptionApplyResult {
    val updates = plan.updates
    repository.setCategoryDescriptionsBatch(updates)
    val result = CategoryDescriptionApplyResult(
        records = plan.records,
        updated = updates.size,
        unchanged = plan.records - updates.size,
    )
    logger.i {
        "Category description overrides: records=${result.records}, " +
            "updates=${result.updated}, unchanged=${result.unchanged}"
    }
    return result
}

internal fun parseBulkMetadata(lines: List<String>): Map<String, BulkMetadata> =
    json.parseToJsonElement(lines.joinToString("\n")).jsonArray.mapNotNull { element ->
        val obj = element as? JsonObject ?: return@mapNotNull null
        val title = obj.string("title") ?: return@mapNotNull null
        title to BulkMetadata(
            pubDates = (obj["pubDate"] as? JsonArray)?.mapNotNull { it.jsonPrimitive.intOrNull } ?: emptyList(),
            pubPlaceHe = obj.string("pubPlaceStringHe"),
        )
    }.toMap()

/**
 * Parses per-title overrides from the changes CSV. Columns:
 * categoryPath,title,author,heShortDesc,heDesc,heDescNew. Only heShortDesc and
 * heDescNew are used (heDescNew is the corrected text that becomes the book's
 * heDesc); the original heDesc column and the rest are ignored.
 */
internal fun parseDescriptionOverrides(lines: List<String>): Map<String, Description> {
    if (lines.isEmpty()) return emptyMap()
    require("title" in lines.first().lowercase()) {
        "$METADATA_CHANGES_FILE must start with a header row"
    }
    // Rejoin the readLines() split so the record parser can stitch quoted fields
    // that span multiple physical lines (heShortDesc/heDesc carry embedded newlines).
    return parseForDbCsvRecords(lines.joinToString("\n")).drop(1).mapNotNull { record ->
        val cols = record.map { it.trim() }
        val title = cols.getOrNull(1)?.takeIf { it.isNotBlank() } ?: return@mapNotNull null
        title to Description(
            heShortDesc = cols.getOrNull(3)?.takeIf { it.isNotBlank() },
            heDesc = cols.getOrNull(5)?.takeIf { it.isNotBlank() },
        )
    }.toMap()
}

internal suspend fun applyMetadata(
    repository: SeforimRepository,
    bindings: IdAllocatorBindings,
    bulk: Map<String, BulkMetadata>,
    descriptions: Map<String, Description>,
    logger: Logger,
): MetadataResult {
    val bookIdsByTitle = repository.getAllBookTitleIds().groupBy({ it.second }, { it.first })

    var updated = 0
    // Names, not just a count: `unmatched=1116` (24.5% of the ForDB metadata
    // corpus) said nothing about WHICH books were missing their publication
    // data, so nobody downstream — otzaria-library's own metadata pass in
    // particular — could act on it. Iteration order is the deterministic
    // `bulk.keys + descriptions.keys` set order, so the list is stable
    // build-to-build.
    val unmatchedTitles = ArrayList<String>()

    for (title in bulk.keys + descriptions.keys) {
        val ids = bookIdsByTitle[title]
        if (ids == null) {
            unmatchedTitles += title
            continue
        }
        if (ids.size > 1) {
            logger.w { "all-metadata: '$title' has ${ids.size} matches; skipping" }
            continue
        }
        val bookId = ids.single()
        val meta = bulk[title]
        val description = descriptions[title]

        repository.updateBookMetadata(
            bookId = bookId,
            heShortDesc = description?.heShortDesc,
            heDesc = description?.heDesc,
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
    return MetadataResult(updated, unmatchedTitles.size, unmatchedTitles)
}

private fun JsonObject.string(key: String): String? =
    this[key].stringOrNull()?.trim()?.takeIf { it.isNotBlank() }
