package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.serialization.json.Json

// ערך ב-default_commentators.json שמסמן "slot ריק" — משאיר פער (gap) ברצף
// ה-position בלי מפרש. הפרשנות של הפער נתונה לתוכנה הצורכת. ראה applyDefaultCommentators.
private const val EMPTY_SLOT_SENTINEL = "-"

/**
 * Loads default commentators configuration from bundled JSON.
 *
 * @return a map keyed by normalized base-book title → ordered list of slots. Each slot is a
 *   normalized commentator title, or `null` for an intentional gap in the position sequence ("-").
 */
internal fun loadDefaultCommentatorsConfig(
    classLoader: ClassLoader?,
    json: Json,
    logger: Logger
): Map<String, List<String?>> = try {
    val stream = classLoader?.getResourceAsStream("default_commentators.json") ?: return emptyMap()
    val jsonText = stream.bufferedReader(Charsets.UTF_8).use { it.readText() }
    val entries = json.decodeFromString<List<DefaultCommentatorsEntry>>(jsonText)
    entries.mapNotNull { entry ->
        val bookKey = normalizeTitleKey(entry.book)
        if (bookKey.isNullOrBlank()) return@mapNotNull null
        val slots: List<String?> = buildList {
            entry.commentators.forEach { raw ->
                if (raw.trim() == EMPTY_SLOT_SENTINEL) {
                    add(null) // slot ריק מכוון
                } else {
                    val key = normalizeTitleKey(raw)
                    if (!key.isNullOrBlank()) add(key)
                    // מחרוזת ריקה/רווחים: לא slot — מושמטת
                }
            }
        }
        if (slots.none { it != null }) return@mapNotNull null
        bookKey to slots
    }.toMap()
} catch (e: Exception) {
    logger.w(e) { "Unable to read default_commentators.json, continuing without default commentators" }
    emptyMap()
}

/**
 * Loads default targum configuration from bundled JSON.
 *
 * @return a map keyed by normalized base-book title → ordered list of normalized targum titles.
 */
internal fun loadDefaultTargumConfig(
    classLoader: ClassLoader?,
    json: Json,
    logger: Logger
): Map<String, List<String>> = try {
    val stream = classLoader?.getResourceAsStream("default_targumim.json") ?: return emptyMap()
    val jsonText = stream.bufferedReader(Charsets.UTF_8).use { it.readText() }
    val entries = json.decodeFromString<List<DefaultTargumEntry>>(jsonText)
    entries.mapNotNull { entry ->
        val bookKey = normalizeTitleKey(entry.book)
        if (bookKey.isNullOrBlank()) return@mapNotNull null
        val targumKeys = entry.targumim
            .mapNotNull { normalizeTitleKey(it) }
            .filter { it.isNotBlank() }
        if (targumKeys.isEmpty()) return@mapNotNull null
        bookKey to targumKeys
    }.toMap()
} catch (e: Exception) {
    logger.w(e) { "Unable to read default_targumim.json, continuing without default targumim" }
    emptyMap()
}

internal suspend fun applyDefaultCommentators(
    repository: SeforimRepository,
    logger: Logger,
    defaultsByBookKey: Map<String, List<String?>>,
    normalizedTitleToBookId: Map<String, Long>
) {
    if (defaultsByBookKey.isEmpty()) return

    logger.i { "Applying default commentators for ${defaultsByBookKey.size} base books" }

    // Clear previous mappings for a clean regeneration
    repository.clearAllDefaultCommentators()

    var totalRows = 0

    defaultsByBookKey.forEach { (bookKey, slots) ->
        val baseBookId = normalizedTitleToBookId[bookKey] ?: return@forEach
        val positioned =
            resolvePositionedCommentators(slots, baseBookId, normalizedTitleToBookId)
        if (positioned.isNotEmpty()) {
            repository.setDefaultCommentatorsForBook(baseBookId, positioned)
            totalRows += positioned.size
        }
    }

    logger.i { "Inserted $totalRows default commentator rows" }
}

/**
 * Resolves slots to (commentatorBookId, position) pairs.
 *
 * position = slot index. A null slot ("-") advances the position without emitting a row,
 * leaving a gap in the position sequence. A missing or duplicate commentator is packed
 * (position not advanced) — backward-compatible with contiguous defaults.
 */
internal fun resolvePositionedCommentators(
    slots: List<String?>,
    baseBookId: Long,
    normalizedTitleToBookId: Map<String, Long>
): List<Pair<Long, Int>> {
    val positioned = mutableListOf<Pair<Long, Int>>()
    val seen = mutableSetOf<Long>()
    var position = 0
    slots.forEach { key ->
        if (key == null) {
            position++
            return@forEach
        }
        val commentatorBookId = normalizedTitleToBookId[key]
        if (commentatorBookId != null && commentatorBookId != baseBookId &&
            seen.add(commentatorBookId)
        ) {
            positioned += commentatorBookId to position
            position++
        }
    }
    return positioned
}

internal suspend fun applyDefaultTargumim(
    repository: SeforimRepository,
    logger: Logger,
    defaultsByBookKey: Map<String, List<String>>,
    normalizedTitleToBookId: Map<String, Long>
) {
    if (defaultsByBookKey.isEmpty()) return

    logger.i { "Applying default targumim for ${defaultsByBookKey.size} base books" }

    repository.clearAllDefaultTargum()

    var totalRows = 0

    defaultsByBookKey.forEach { (bookKey, targumKeys) ->
        val baseBookId = normalizedTitleToBookId[bookKey] ?: return@forEach

        val uniqueTargumIds = LinkedHashSet<Long>()
        targumKeys.forEach { targumKey ->
            val targumBookId = normalizedTitleToBookId[targumKey]
            if (targumBookId != null && targumBookId != baseBookId) {
                uniqueTargumIds += targumBookId
            }
        }

        if (uniqueTargumIds.isNotEmpty()) {
            repository.setDefaultTargumForBook(baseBookId, uniqueTargumIds.toList())
            totalRows += uniqueTargumIds.size
        }
    }

    logger.i { "Inserted $totalRows default targum rows" }
}

