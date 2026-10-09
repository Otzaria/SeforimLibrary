package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class LibraryTreeCategoryCheckTest {
    // קבלה(1) > מחברי זמננו(2), אחרונים(3); the last book of 2 moves to 3 (the 2026-10-07 incident).
    private val base = setOf("קבלה/מחברי זמננו/משכיל.txt", "קבלה/אחרונים/אחר.txt")
    private val moved = setOf("קבלה/אחרונים/משכיל.txt", "קבלה/אחרונים/אחר.txt")

    private fun lost(
        conn: Connection,
        candidate: Set<String>,
        baseFiles: Set<String> = base,
        categoryRenames: List<CategoryRename> = emptyList(),
        bookRenames: List<Pair<String, String>> = emptyList(),
        categoryMoves: List<CategoryMove> = emptyList(),
        bookMoves: List<BookMove> = emptyList(),
    ) = findCategoriesLostWithLibraryFolders(
        conn, baseFiles, candidate, categoryRenames, bookRenames, categoryMoves, bookMoves,
    )

    @Test
    fun `category whose only folder emptied is lost`() = withReleasedDb { conn ->
        assertEquals(mapOf(2L to "קבלה/מחברי זמננו"), lost(conn, moved))
    }

    @Test
    fun `unchanged tree loses nothing`() = withReleasedDb { conn ->
        assertEquals(emptyMap(), lost(conn, base))
    }

    @Test
    fun `quote variant of the same folder is not a removal`() = withReleasedDb { conn ->
        insertCategory(conn, 4, "תנ״ך", null)
        insertBook(conn, 12, "פירוש", 4, OTZARIA)
        val withTanakh = base + "תנך/פירוש.txt"
        assertEquals(emptyMap(), lost(conn, withTanakh - "תנך/פירוש.txt" + "תנ\"ך/פירוש.txt", withTanakh))
    }

    @Test
    fun `a Sefaria book keeps the category`() = withReleasedDb { conn ->
        insertBook(conn, 20, "ספר מספריא", 2, SEFARIA)
        assertEquals(emptyMap(), lost(conn, moved))
    }

    @Test
    fun `a new folder renamed into the same title keeps the category`() = withReleasedDb { conn ->
        val renames = listOf(CategoryRename("ספרות מודרנית", "מחברי זמננו", CategoryMatchMode.Exact))
        val candidate = setOf("קבלה/ספרות מודרנית/משכיל.txt", "קבלה/אחרונים/אחר.txt")
        assertEquals(emptyMap(), lost(conn, candidate, categoryRenames = renames))
    }

    @Test
    fun `the same leaf title under another parent does not keep the category`() = withReleasedDb { conn ->
        assertEquals(mapOf(2L to "קבלה/מחברי זמננו"), lost(conn, moved + "משנה/מחברי זמננו/שיעורים.txt"))
    }

    @Test
    fun `a category move onto the same path keeps the category`() = withReleasedDb { conn ->
        val candidate = moved + "חדש/מחברי זמננו/משכיל ב.txt"
        val categoryMoves = listOf(CategoryMove("חדש/מחברי זמננו", "קבלה"))
        assertEquals(emptyMap(), lost(conn, candidate, categoryMoves = categoryMoves))
    }

    @Test
    fun `a book move into the same leaf keeps the category`() = withReleasedDb { conn ->
        val bookMoves = listOf(BookMove("אחר", "קבלה/אחרונים", "קבלה/מחברי זמננו"))
        assertEquals(emptyMap(), lost(conn, moved, bookMoves = bookMoves))
    }

    @Test
    fun `a book renamed since the release is still matched`() = withReleasedDb { conn ->
        conn.createStatement().use { it.execute("UPDATE book SET title = 'משכיל לדוד' WHERE id = 10") }
        assertEquals(
            mapOf(2L to "קבלה/מחברי זמננו"),
            lost(conn, moved, bookRenames = listOf("משכיל" to "משכיל לדוד")),
        )
    }

    @Test
    fun `tree file must list relative paths`() {
        val path = Files.createTempFile("tree-", ".bin")
        try {
            Files.write(path, "/abs/x.txt\u0000".toByteArray())
            assertFailsWith<IllegalArgumentException> { readLibraryTree(path) }
            Files.write(path, "א/ב.txt\u0000ג.txt\u0000".toByteArray())
            assertEquals(setOf("א/ב.txt", "ג.txt"), readLibraryTree(path))
        } finally {
            Files.deleteIfExists(path)
        }
    }

    private fun withReleasedDb(block: (Connection) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)")
                st.execute("CREATE TABLE category (id INTEGER PRIMARY KEY, parentId INTEGER, title TEXT NOT NULL)")
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY, title TEXT NOT NULL, categoryId INTEGER NOT NULL, sourceId INTEGER NOT NULL)")
                st.execute("INSERT INTO source VALUES ($SEFARIA, 'Sefaria'), ($OTZARIA, 'Otzaria')")
            }
            insertCategory(conn, 1, "קבלה", null)
            insertCategory(conn, 2, "מחברי זמננו", 1)
            insertCategory(conn, 3, "אחרונים", 1)
            insertBook(conn, 10, "משכיל", 2, OTZARIA)
            insertBook(conn, 11, "אחר", 3, OTZARIA)
            block(conn)
        }
    }

    private fun insertCategory(conn: Connection, id: Long, title: String, parentId: Long?) {
        conn.prepareStatement("INSERT INTO category (id, parentId, title) VALUES (?, ?, ?)").use { stmt ->
            stmt.setLong(1, id)
            if (parentId == null) stmt.setNull(2, java.sql.Types.INTEGER) else stmt.setLong(2, parentId)
            stmt.setString(3, title)
            stmt.executeUpdate()
        }
    }

    private fun insertBook(conn: Connection, id: Long, title: String, categoryId: Long, sourceId: Long) {
        conn.prepareStatement("INSERT INTO book (id, title, categoryId, sourceId) VALUES (?, ?, ?, ?)").use { stmt ->
            stmt.setLong(1, id)
            stmt.setString(2, title)
            stmt.setLong(3, categoryId)
            stmt.setLong(4, sourceId)
            stmt.executeUpdate()
        }
    }

    private companion object {
        const val SEFARIA = 1L
        const val OTZARIA = 2L
    }
}
