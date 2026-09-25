package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import java.nio.file.Path
import kotlin.io.path.exists

/** קובץ הגרסה המוצהר חסר בייצוא — נפילה רועשת, בלי נסיגה שקטה ל-merged.json. */
internal class MissingPreferredVersionException(message: String) : IllegalStateException(message)

/** כתובת של מהדורה אחרת שאין לה שורה בטקסט המוצהר — נפילה רועשת, בלי נשירה שקטה. */
internal class PreferredVersionRefGapException(message: String) : IllegalStateException(message)

/**
 * מיפוי מוצהר: כותרת ספר → קובץ הגרסה שממנו הוא מיובא במקום merged.json.
 * Format rules: see preferred_versions.txt.
 */
internal data class SefariaPreferredVersions(
    val fileNameByTitleKey: Map<String, String>
) {
    fun isEmpty(): Boolean = fileNameByTitleKey.isEmpty()

    /** מחזיר את קובץ הגרסה המוצהר לצד merged.json, או null כשלספר אין מיפוי. */
    fun resolve(mergedPath: Path, enTitle: String?, heTitle: String?): Path? {
        if (fileNameByTitleKey.isEmpty()) return null
        val fileName = listOfNotNull(heTitle, enTitle)
            .mapNotNull { normalizeTitleKey(it) }
            .firstNotNullOfOrNull { fileNameByTitleKey[it] }
            ?: return null
        val path = mergedPath.resolveSibling(fileName)
        if (!path.exists()) {
            throw MissingPreferredVersionException(
                "preferred_versions.txt: declared version file not found: $path"
            )
        }
        return path
    }

    companion object {
        val Empty = SefariaPreferredVersions(fileNameByTitleKey = emptyMap())
    }
}

internal fun loadPreferredVersions(classLoader: ClassLoader?, logger: Logger): SefariaPreferredVersions =
    parsePreferredVersions(loadBlacklistEntries(classLoader, "preferred_versions.txt", logger), logger)

internal fun parsePreferredVersions(entries: List<String>, logger: Logger): SefariaPreferredVersions {
    val fileNameByTitleKey = LinkedHashMap<String, String>()
    entries.forEach { entry ->
        val separator = entry.indexOf('|')
        if (separator < 0) {
            logger.w { "preferred_versions.txt: malformed entry skipped: $entry" }
            return@forEach
        }
        val titleKey = normalizeTitleKey(entry.substring(0, separator))
        val fileName = entry.substring(separator + 1).trim()
        if (titleKey == null || fileName.isEmpty()) {
            logger.w { "preferred_versions.txt: malformed entry skipped: $entry" }
            return@forEach
        }
        fileNameByTitleKey[titleKey] = fileName
    }
    return SefariaPreferredVersions(fileNameByTitleKey = fileNameByTitleKey)
}
