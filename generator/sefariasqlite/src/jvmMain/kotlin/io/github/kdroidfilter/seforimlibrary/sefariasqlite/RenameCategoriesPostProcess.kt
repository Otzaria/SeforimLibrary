package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.net.DownloadUrls
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URI
import java.nio.charset.Charset
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Post-processing step to rename and merge categories (and book titles) in the database.
 * This runs after Sefaria import but before Otzaria, so names are unified
 * before additional books are added.
 *
 * Rename mappings are downloaded at runtime from the Otzaria GitHub repository:
 * - Book renames:     [DownloadUrls.OTZARIA_BOOK_RENAMES_CSV]     (Windows-1255)
 * - Category renames: [DownloadUrls.OTZARIA_CATEGORY_RENAMES_CSV] (ISO-8859-8)
 *
 * Category renames support **prefix matching**: a source value like "ראשונים על"
 * will match any category whose title starts with that prefix (e.g. "ראשונים על התלמוד").
 *
 * Handles two category cases:
 * 1. Simple rename: When no target category exists under the same parent.
 * 2. Merge: When a target category already exists, books are moved and the source is deleted.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:renameCategories [-PseforimDb=/path/to/seforim.db]
 *
 * Env alternatives:
 *   SEFORIM_DB
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("RenameCategories")

    val dbPathStr = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    val dbPath = Paths.get(dbPathStr)

    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }

    // ── Download rename CSVs ────────────────────────────────────────────
    val categoryRenames: List<RenameRule> = try {
        downloadCsv(DownloadUrls.OTZARIA_CATEGORY_RENAMES_CSV, Charset.forName("ISO-8859-8"))
            .also { logger.i { "Downloaded ${it.size} category rename rules" } }
    } catch (e: Exception) {
        logger.e(e) { "Failed to download category renames CSV; aborting." }
        exitProcess(1)
    }

    val bookRenames: Map<String, String> = try {
        downloadCsv(DownloadUrls.OTZARIA_BOOK_RENAMES_CSV, Charset.forName("Windows-1255"))
            .associate { it.source to it.target }
            .also { logger.i { "Downloaded ${it.size} book rename rules" } }
    } catch (e: Exception) {
        logger.e(e) { "Failed to download book renames CSV; aborting." }
        exitProcess(1)
    }

    logger.i { "Renaming/merging categories and books in $dbPath" }

    try {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.autoCommit = false

            // ── 1. Category renames ─────────────────────────────────────
            var totalCatRenamed = 0
            var totalCatMerged = 0

            for (rule in categoryRenames) {
                // Collect matching categories: exact or prefix
                val matchingCategories = findMatchingCategories(conn, rule.source)
                for ((catId, catTitle, parentId) in matchingCategories) {
                    val result = renameOrMergeCategory(conn, catId, catTitle, rule.target, parentId, logger)
                    when (result) {
                        is RenameResult.Renamed -> totalCatRenamed += result.count
                        is RenameResult.Merged -> totalCatMerged += result.booksMoved
                        is RenameResult.NotFound -> { /* skip */ }
                    }
                }
            }
            logger.i { "Category processing complete. Renamed: $totalCatRenamed, Merged: $totalCatMerged books" }

            // ── 2. Book renames ─────────────────────────────────────────
            var totalBookRenamed = 0
            for ((oldTitle, newTitle) in bookRenames) {
                val updated = renameBookTitle(conn, oldTitle, newTitle)
                if (updated > 0) {
                    logger.i { "Renamed book '$oldTitle' -> '$newTitle' ($updated rows)" }
                    totalBookRenamed += updated
                }
            }
            logger.i { "Book rename complete. Renamed: $totalBookRenamed books" }

            conn.commit()
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to process renames" }
        exitProcess(1)
    }
}

// ── Data classes ─────────────────────────────────────────────────────────

/** A single rename rule parsed from a CSV line. */
private data class RenameRule(val source: String, val target: String)

private sealed class RenameResult {
    data class Renamed(val count: Int) : RenameResult()
    data class Merged(val booksMoved: Int) : RenameResult()
    data object NotFound : RenameResult()
}

/** Category row returned by [findMatchingCategories]. */
private data class CategoryRow(val id: Long, val title: String, val parentId: Long?)

// ── CSV download & parsing ───────────────────────────────────────────────

/**
 * Downloads a CSV from [url], decodes it with [charset], and returns a list
 * of [RenameRule]s. Each line is expected to have exactly two comma-separated
 * fields: `source,target`. Blank lines and lines without a comma are skipped.
 */
private fun downloadCsv(url: String, charset: Charset): List<RenameRule> {
    val rules = mutableListOf<RenameRule>()
    val seen = mutableSetOf<String>()

    URI(url).toURL().openStream().use { raw ->
        BufferedReader(InputStreamReader(raw, charset)).use { reader ->
            reader.forEachLine { line ->
                val trimmed = line.trim()
                if (trimmed.isBlank()) return@forEachLine
                val commaIdx = trimmed.indexOf(',')
                if (commaIdx < 0) return@forEachLine

                val source = trimmed.substring(0, commaIdx).trim()
                val target = trimmed.substring(commaIdx + 1).trim()
                if (source.isNotEmpty() && target.isNotEmpty() && source !in seen) {
                    seen.add(source)
                    rules.add(RenameRule(source, target))
                }
            }
        }
    }
    return rules
}

// ── Category logic ───────────────────────────────────────────────────────

/**
 * Finds categories matching [pattern] — either an exact match on `title`,
 * or a prefix match if no exact match exists (e.g. "ראשונים על" matches
 * "ראשונים על התלמוד").
 */
private fun findMatchingCategories(conn: Connection, pattern: String): List<CategoryRow> {
    // Try exact matches first
    val exact = queryCategoriesByTitle(conn, pattern)
    if (exact.isNotEmpty()) return exact

    // Fall back to prefix matching
    return queryCategoriesByPrefix(conn, pattern)
}

private fun queryCategoriesByTitle(conn: Connection, title: String): List<CategoryRow> {
    val rows = mutableListOf<CategoryRow>()
    conn.prepareStatement("SELECT id, title, parentId FROM category WHERE title = ?").use { stmt ->
        stmt.setString(1, title)
        stmt.executeQuery().use { rs ->
            while (rs.next()) {
                rows.add(CategoryRow(rs.getLong(1), rs.getString(2), rs.getObject(3) as? Long))
            }
        }
    }
    return rows
}

private fun queryCategoriesByPrefix(conn: Connection, prefix: String): List<CategoryRow> {
    val rows = mutableListOf<CategoryRow>()
    val likePattern = "${prefix.replace("%", "\\%").replace("_", "\\_")}%"
    conn.prepareStatement("SELECT id, title, parentId FROM category WHERE title LIKE ? ESCAPE '\\'").use { stmt ->
        stmt.setString(1, likePattern)
        stmt.executeQuery().use { rs ->
            while (rs.next()) {
                rows.add(CategoryRow(rs.getLong(1), rs.getString(2), rs.getObject(3) as? Long))
            }
        }
    }
    return rows
}

/**
 * Renames a single category (by [sourceId]) to [newName], or merges it into
 * an existing category with the same name under the same parent.
 */
private fun renameOrMergeCategory(
    conn: Connection,
    sourceId: Long,
    oldName: String,
    newName: String,
    parentId: Long?,
    logger: Logger
): RenameResult {
    // Check if a target category with newName exists under the same parent
    val targetId = findCategoryByNameAndParent(conn, newName, parentId)

    return if (targetId != null && targetId != sourceId) {
        // Merge: move books from source to target, then delete source
        val booksMoved = moveBooksToCategory(conn, sourceId, targetId)
        val subCatsMoved = moveSubcategoriesToParent(conn, sourceId, targetId)
        deleteCategory(conn, sourceId)
        logger.i { "Merged '$oldName' (id=$sourceId) into '$newName' (id=$targetId): $booksMoved books, $subCatsMoved subcategories" }
        RenameResult.Merged(booksMoved)
    } else {
        // Simple rename
        conn.prepareStatement("UPDATE category SET title = ? WHERE id = ?").use { stmt ->
            stmt.setString(1, newName)
            stmt.setLong(2, sourceId)
            stmt.executeUpdate()
        }
        logger.i { "Renamed '$oldName' (id=$sourceId) -> '$newName'" }
        RenameResult.Renamed(1)
    }
}

private fun findCategoryByNameAndParent(conn: Connection, name: String, parentId: Long?): Long? {
    val sql = if (parentId != null) {
        "SELECT id FROM category WHERE title = ? AND parentId = ?"
    } else {
        "SELECT id FROM category WHERE title = ? AND parentId IS NULL"
    }
    conn.prepareStatement(sql).use { stmt ->
        stmt.setString(1, name)
        if (parentId != null) {
            stmt.setLong(2, parentId)
        }
        stmt.executeQuery().use { rs ->
            return if (rs.next()) rs.getLong(1) else null
        }
    }
}

private fun moveBooksToCategory(conn: Connection, fromCategoryId: Long, toCategoryId: Long): Int {
    conn.prepareStatement("UPDATE book SET categoryId = ? WHERE categoryId = ?").use { stmt ->
        stmt.setLong(1, toCategoryId)
        stmt.setLong(2, fromCategoryId)
        return stmt.executeUpdate()
    }
}

private fun moveSubcategoriesToParent(conn: Connection, fromCategoryId: Long, toParentId: Long): Int {
    conn.prepareStatement("UPDATE category SET parentId = ? WHERE parentId = ?").use { stmt ->
        stmt.setLong(1, toParentId)
        stmt.setLong(2, fromCategoryId)
        return stmt.executeUpdate()
    }
}

private fun deleteCategory(conn: Connection, categoryId: Long) {
    conn.prepareStatement("DELETE FROM category WHERE id = ?").use { stmt ->
        stmt.setLong(1, categoryId)
        stmt.executeUpdate()
    }
}

// ── Book logic ───────────────────────────────────────────────────────────

/**
 * Renames books whose `heTitle` matches [oldTitle] exactly.
 * Returns the number of rows updated.
 */
private fun renameBookTitle(conn: Connection, oldTitle: String, newTitle: String): Int {
    conn.prepareStatement("UPDATE book SET title = ? WHERE title = ?").use { stmt ->
        stmt.setString(1, newTitle)
        stmt.setString(2, oldTitle)
        return stmt.executeUpdate()
    }
}
