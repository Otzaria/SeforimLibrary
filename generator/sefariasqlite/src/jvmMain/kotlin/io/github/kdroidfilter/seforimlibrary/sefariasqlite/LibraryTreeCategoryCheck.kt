package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.ids.CategoryLabels
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection

// The gate replays ForDB on the last released DB, which still holds categories of library
// folders removed since; this finds those the candidate tree will not create.
internal const val SEFARIA_SOURCE_NAME = "Sefaria"

/** Reads a NUL-separated list of file paths relative to the packaged `אוצריא` root. */
internal fun readLibraryTree(path: Path): Set<String> {
    val entries = String(Files.readAllBytes(path), Charsets.UTF_8).split('\u0000').filter { it.isNotEmpty() }
    require(entries.isNotEmpty()) { "Library tree '$path' lists no files" }
    for (entry in entries) {
        require(!entry.startsWith("/") && entry.split('/').none { it.isEmpty() || it == "." || it == ".." }) {
            "Library tree '$path' has a non-relative entry '$entry'"
        }
    }
    return entries.toSet()
}

private fun folderTitles(folder: List<String>): List<String> = folder.flatMap { CategoryLabels.folderSegments(it) }

private fun folderKey(folder: List<String>): String =
    folderTitles(folder).joinToString("/") { CategoryLabels.comparable(it) }

private fun folderKeys(files: Set<String>): Set<String> {
    val keys = HashSet<String>()
    for (file in files) {
        val folder = file.split('/').dropLast(1)
        for (depth in 1..folder.size) keys += folderKey(folder.take(depth))
    }
    return keys
}

private fun applyCategoryRenames(title: String, renames: List<CategoryRename>): String =
    renames.fold(title) { current, rule ->
        val matches = when (rule.matchMode) {
            CategoryMatchMode.Exact -> current == rule.oldName
            CategoryMatchMode.Prefix -> current.startsWith(rule.oldName)
        }
        if (matches) rule.newName else current
    }

private class LibraryBook(val folderKey: String, val folder: String, val title: String)

/** The `.txt` files Generator turns into books, titled as the build titles them. */
private fun libraryBooks(files: Set<String>, bookRenames: List<Pair<String, String>>): List<LibraryBook> =
    files.mapNotNull { file ->
        val parts = file.split('/')
        val name = parts.last()
        if (parts.size < 2 || name.substringAfterLast('.', "") != "txt") return@mapNotNull null
        val folder = parts.dropLast(1)
        val title = bookRenames.fold(CategoryLabels.bookTitle(name.substringBeforeLast('.'))) { current, (old, new) ->
            if (current == old) new else current
        }
        LibraryBook(folderKey(folder), folder.joinToString("/"), title)
    }

/**
 * Released category id -> its base folder, for categories built only from a removed folder
 * that no surviving folder, Sefaria book or move can recreate. Doubt keeps a category alive.
 */
internal fun findCategoriesLostWithLibraryFolders(
    conn: Connection,
    baseFiles: Set<String>,
    candidateFiles: Set<String>,
    categoryRenames: List<CategoryRename>,
    bookRenames: List<Pair<String, String>>,
    categoryMoves: List<CategoryMove>,
    bookMoves: List<BookMove>,
): Map<Long, String> {
    val candidateKeys = folderKeys(candidateFiles)
    val goneKeys = folderKeys(baseFiles) - candidateKeys
    if (goneKeys.isEmpty()) return emptyMap()

    val movedTitles = bookMoves.map { it.name }.toSet()
    val baseBooks = libraryBooks(baseFiles, bookRenames)

    fun otzariaCategoriesOf(title: String): Set<Long> =
        conn.prepareStatement(
            "SELECT b.categoryId FROM book b JOIN source s ON s.id = b.sourceId WHERE b.title = ? AND s.name <> ?",
        ).use { stmt ->
            stmt.setString(1, title)
            stmt.setString(2, SEFARIA_SOURCE_NAME)
            stmt.executeQuery().use { rs -> buildSet { while (rs.next()) add(rs.getLong(1)) } }
        }

    val categoriesByFolder = HashMap<String, MutableSet<Long>>()
    for (book in baseBooks) {
        if (book.title in movedTitles) continue
        categoriesByFolder.getOrPut(book.folderKey) { HashSet() } += otzariaCategoriesOf(book.title)
    }
    val liveCategories = categoriesByFolder.filterKeys { it !in goneKeys }.values.flatten().toSet()

    // Full paths the next build can still produce: candidate folders after the category
    // renames and moves, plus every book-move destination.
    fun comparableSegments(path: String) = path.split('/').filter { it.isNotBlank() }.map { CategoryLabels.comparable(it) }
    var candidatePaths = candidateFiles.flatMap { file ->
        val folder = file.split('/').dropLast(1)
        (1..folder.size).map { depth ->
            folderTitles(folder.take(depth)).map { CategoryLabels.comparable(applyCategoryRenames(it, categoryRenames)) }
        }
    }.toSet()
    for (move in categoryMoves) {
        val source = comparableSegments(move.sourcePath)
        val dest = comparableSegments(move.destParentPath) + source.last()
        candidatePaths = candidatePaths.mapTo(HashSet()) { path ->
            if (path.size >= source.size && path.subList(0, source.size) == source) dest + path.drop(source.size) else path
        }
    }
    val survivingPaths = candidatePaths.mapTo(HashSet()) { it.joinToString("/") }
    bookMoves.mapTo(survivingPaths) { comparableSegments(it.destPath).joinToString("/") }

    fun comparableCategoryPath(categoryId: Long): String? {
        val segments = ArrayDeque<String>()
        var current: Long? = categoryId
        conn.prepareStatement("SELECT title, parentId FROM category WHERE id = ?").use { stmt ->
            while (current != null) {
                stmt.setLong(1, current)
                current = stmt.executeQuery().use { rs ->
                    if (!rs.next()) return null
                    segments.addFirst(CategoryLabels.comparable(rs.getString(1)))
                    rs.getLong(2).takeUnless { rs.wasNull() }
                }
            }
        }
        return segments.joinToString("/")
    }

    val lost = HashMap<Long, String>()
    for (gone in goneKeys) {
        val categoryId = categoriesByFolder[gone]?.singleOrNull() ?: continue
        if (categoryId in liveCategories) continue
        val path = comparableCategoryPath(categoryId) ?: continue
        if (path in survivingPaths) continue

        val folderTitlesUnder = baseBooks.filter { it.folderKey == gone || it.folderKey.startsWith("$gone/") }
            .map { it.title }.toSet()
        val subtreeBooks = conn.prepareStatement(
            """
            WITH RECURSIVE sub(id) AS (SELECT ? UNION ALL SELECT c.id FROM category c JOIN sub ON c.parentId = sub.id)
            SELECT b.title, s.name FROM book b JOIN source s ON s.id = b.sourceId WHERE b.categoryId IN (SELECT id FROM sub)
            """.trimIndent(),
        ).use { stmt ->
            stmt.setLong(1, categoryId)
            stmt.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getString(1) to rs.getString(2)) } }
        }
        if (subtreeBooks.isEmpty()) continue
        if (subtreeBooks.any { (bookTitle, source) -> source == SEFARIA_SOURCE_NAME || bookTitle !in folderTitlesUnder }) continue

        lost[categoryId] = baseBooks.first { it.folderKey == gone }.folder
    }
    return lost
}

/** Fails on every category-description row whose category the candidate tree will not create. */
internal suspend fun failOnLostCategoryDescriptions(
    repository: SeforimRepository,
    overrides: List<CategoryDescriptionOverride>,
    lostCategories: Map<Long, String>,
) {
    if (lostCategories.isEmpty()) return
    val idByPath = repository.getAllCategoryDescriptionRows().associate { it.canonicalPath to it.categoryId }
    val failures = overrides.mapNotNull { override ->
        val folder = idByPath[override.canonicalPath]?.let { lostCategories[it] } ?: return@mapNotNull null
        "$CATEGORY_DESCRIPTIONS_FILE record ${override.csvRecordNumber} describes '${override.canonicalPath}', " +
            "built only from library folder 'אוצריא/$folder', which the candidate tree no longer has"
    }
    if (failures.isEmpty()) return
    // SeforimRepository silences the logger, so the details travel in the exception.
    error(
        "ForDB validation FAILED: ${failures.size} category description(s) name categories " +
            "the next build will not create:\n" + failures.joinToString("\n"),
    )
}
