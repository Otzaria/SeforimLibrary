package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CategoryMergeSafetyTest {
    private val logger = Logger.withTag("test")

    @Test
    fun `reusing the old path cannot settle a conflict in the renamed survivor`() = withConnection { conn ->
        insert(conn, 1, "A", short = "source A")
        insert(conn, 2, "B", short = "target B")
        insert(conn, 3, "D", short = "source D")
        book(conn, 100, 1)
        conn.autoCommit = false

        val failures = collect(conn, listOf(rename("A", "B"), rename("B", "C"), rename("D", "B")), listOf(edit("B")))

        assertEquals(1, failures.size)
        assertTrue(failures.single().message.contains("final path 'C'"))
        assertEquals("A", text(conn, "SELECT title FROM category WHERE id=1"))
        assertEquals("source A", text(conn, "SELECT heShortDesc FROM category WHERE id=1"))
        assertEquals(1, number(conn, "SELECT categoryId FROM book WHERE id=100"))
        assertEquals(3, number(conn, "SELECT count(*) FROM category"))
    }

    @Test
    fun `override at the final path is accepted and the actual metadata applier settles it`() = runBlocking {
        val db = Files.createTempFile("category-merge-final-", ".db")
        try {
            SeforimRepository(db.toString(), JdbcSqliteDriver("jdbc:sqlite:$db")).close()
            val overrides = listOf(edit("C"), edit("B", DescriptionEdit.Keep))
            DriverManager.getConnection("jdbc:sqlite:$db").use { conn ->
                insert(conn, 1, "A", short = "source A")
                insert(conn, 2, "B", short = "target B")
                insert(conn, 3, "D", short = "source D")
                conn.autoCommit = false
                assertEquals(emptyList(), collect(conn, listOf(rename("A", "B"), rename("B", "C"), rename("D", "B")), overrides))
                conn.commit()
            }
            val repository = SeforimRepository(db.toString(), JdbcSqliteDriver("jdbc:sqlite:$db"))
            try {
                applyCategoryDescriptionOverrides(repository, overrides, logger)
                val byPath = repository.getAllCategoryDescriptionRows().associateBy { it.canonicalPath }
                assertEquals("final", byPath.getValue("C").heShortDesc)
                assertEquals("source D", byPath.getValue("B").heShortDesc)
            } finally {
                repository.close()
            }
        } finally {
            Files.deleteIfExists(db)
            Files.deleteIfExists(db.resolveSibling("${db.fileName}-wal"))
            Files.deleteIfExists(db.resolveSibling("${db.fileName}-shm"))
        }
    }

    @Test
    fun `conflicts follow an ancestor move and accept clear at the final descendant path`() = withConnection { conn ->
        insert(conn, 1, "old")
        insert(conn, 2, "shared", 1, short = "source")
        insert(conn, 11, "new")
        insert(conn, 12, "shared", 11, short = "target")
        insert(conn, 20, "P")
        conn.autoCommit = false
        val failures = collect(
            conn, listOf(rename("old", "new")), listOf(edit("P/new/shared", DescriptionEdit.Clear)),
            moves = listOf(CategoryMove("new", "P")),
        )
        assertEquals(emptyList(), failures)
        assertEquals(20, number(conn, "SELECT parentId FROM category WHERE id=11"))
    }

    @Test
    fun `a book move recreating the old path cannot settle the moved category conflict`() = withConnection { conn ->
        insert(conn, 1, "A", short = "source")
        insert(conn, 2, "B", short = "target")
        insert(conn, 3, "P")
        insert(conn, 4, "D")
        book(conn, 100, 4, "book")
        conn.autoCommit = false
        val failures = collect(
            conn, listOf(rename("A", "B")), listOf(edit("B")),
            moves = listOf(CategoryMove("B", "P")), bookMoves = listOf(BookMove("book", "D", "B")),
        )
        assertEquals(1, failures.size)
        assertTrue(failures.single().message.contains("final path 'P/B'"))
        assertEquals(4, number(conn, "SELECT count(*) FROM category"))
        assertEquals(4, number(conn, "SELECT categoryId FROM book WHERE id=100"))
    }

    @Test
    fun `a subsequent equal-valued merge still carries the earlier conflict`() = withConnection { conn ->
        insert(conn, 1, "A", short = "source")
        insert(conn, 2, "B", short = "same")
        insert(conn, 3, "C", short = "same")
        conn.autoCommit = false
        val failures = collect(conn, listOf(rename("A", "B"), rename("B", "C")), listOf(edit("B")))
        assertEquals(1, failures.size)
        assertTrue(failures.single().message.contains("final path 'C'"))
        assertEquals(3, number(conn, "SELECT count(*) FROM category"))
        assertEquals(emptyList(), collect(conn, listOf(rename("A", "B"), rename("B", "C")), listOf(edit("C"))))
        assertEquals(1, number(conn, "SELECT count(*) FROM category"))
    }

    @Test
    fun `successive merges retain the union of required description edits`() = withConnection { conn ->
        insert(conn, 1, "A", short = "source", long = "same long")
        insert(conn, 2, "B", short = "same short", long = "same long")
        insert(conn, 3, "C", short = "same short", long = "target long")
        conn.autoCommit = false
        val renames = listOf(rename("A", "B"), rename("B", "C"))
        val failures = collect(conn, renames, listOf(edit("C")))
        assertEquals(1, failures.size)
        assertTrue(failures.single().message.contains("conflicting heDesc"))
        assertEquals(emptyList(), collect(conn, renames, listOf(edit("C", long = DescriptionEdit.Clear))))
    }

    @Test
    fun `final overrides must settle each conflicting field independently`() = withConnection { conn ->
        insert(conn, 1, "A", short = "source", long = "source long")
        insert(conn, 2, "B", short = "target", long = "target long")
        conn.autoCommit = false
        val failures = collect(
            conn, listOf(rename("A", "B"), rename("B", "C")),
            listOf(edit("C", short = DescriptionEdit.Keep, long = DescriptionEdit.Clear)),
        )
        assertEquals(1, failures.size)
        assertTrue(failures.single().message.contains("conflicting heShortDesc"))
        assertFalse(failures.single().message.contains("heDesc"))
    }

    @Test
    fun `a failed rule rolls back its conflict tracking as well as its writes`() = withConnection { conn ->
        insert(conn, 10, "P")
        insert(conn, 1, "A", short = "source")
        insert(conn, 2, "B", short = "target")
        insert(conn, 3, "A", 10)
        insert(conn, 4, "B", 10)
        conn.createStatement().use {
            it.execute("CREATE TRIGGER prevent_delete BEFORE DELETE ON category WHEN OLD.id=3 BEGIN SELECT RAISE(ABORT, 'blocked'); END")
        }
        conn.autoCommit = false
        val failures = collect(conn, listOf(rename("A", "B")), emptyList())
        assertEquals(1, failures.size)
        assertTrue(failures.single().message.contains("blocked"))
        assertEquals(5, number(conn, "SELECT count(*) FROM category"))
        assertEquals("source", text(conn, "SELECT heShortDesc FROM category WHERE id=1"))
    }

    @Test
    fun `recursive matching shares Otzaria quote whitespace and corpus suffix rules`() {
        for ((source, target) in listOf("רש״י" to "רשי", "בית  יוסף" to "בית יוסף", "מפרשים על התורה" to "מפרשים")) {
            withConnection { conn ->
                insert(conn, 1, "old")
                insert(conn, 2, source, 1)
                insert(conn, 11, "new")
                insert(conn, 12, target, 11)
                book(conn, 100, 2)
                renameOrMergeCategory(conn, rename("old", "new"), logger)
                assertEquals(2, number(conn, "SELECT count(*) FROM category"), "$source and $target must merge")
                assertEquals(12, number(conn, "SELECT categoryId FROM book WHERE id=100"))
            }
        }
    }

    @Test
    fun `many aliases into one target accumulate descriptions books and nested children`() = withConnection { conn ->
        insert(conn, 1, "old")
        insert(conn, 2, "רש״י", 1, short = "short")
        insert(conn, 3, "רשי", 1, long = "long")
        insert(conn, 4, "deep", 2, short = "deep short")
        insert(conn, 5, "deep", 3, long = "deep long")
        insert(conn, 11, "new")
        insert(conn, 12, "רש\"י", 11)
        book(conn, 101, 2)
        book(conn, 102, 3)
        book(conn, 103, 4)
        book(conn, 104, 5)
        conn.createStatement().use { it.execute("INSERT INTO line VALUES(201,101),(202,102),(203,103),(204,104)") }

        renameOrMergeCategory(conn, rename("old", "new"), logger)

        assertEquals(3, number(conn, "SELECT count(*) FROM category"))
        assertEquals(4, number(conn, "SELECT count(*) FROM book"))
        assertEquals(4, number(conn, "SELECT count(*) FROM line"))
        assertEquals("short", text(conn, "SELECT heShortDesc FROM category WHERE id=12"))
        assertEquals("long", text(conn, "SELECT heDesc FROM category WHERE id=12"))
        assertEquals("deep short", text(conn, "SELECT heShortDesc FROM category WHERE title='deep'"))
        assertEquals("deep long", text(conn, "SELECT heDesc FROM category WHERE title='deep'"))
        assertEquals(1, number(conn, "SELECT count(DISTINCT categoryId) FROM book WHERE id IN (103,104)"))
        conn.createStatement().use { st -> st.executeQuery("PRAGMA foreign_key_check").use { assertFalse(it.next()) } }
    }

    @Test
    fun `aliases without a preexisting child also merge safely`() = withConnection { conn ->
        insert(conn, 1, "old")
        insert(conn, 2, "רש״י", 1, short = "short")
        insert(conn, 3, "רשי", 1, long = "long")
        insert(conn, 11, "new")
        renameOrMergeCategory(conn, rename("old", "new"), logger)
        assertEquals(2, number(conn, "SELECT count(*) FROM category"))
        assertEquals("short", text(conn, "SELECT heShortDesc FROM category WHERE parentId=11"))
        assertEquals("long", text(conn, "SELECT heDesc FROM category WHERE parentId=11"))
    }

    @Test
    fun `conflicts between aliases fail before any standalone merge writes`() = withConnection { conn ->
        insert(conn, 1, "old")
        insert(conn, 2, "רש״י", 1, short = "first")
        insert(conn, 3, "רשי", 1, short = "second")
        insert(conn, 11, "new")
        insert(conn, 12, "רש\"י", 11)
        book(conn, 100, 1)
        assertFailsWith<IllegalStateException> { renameOrMergeCategory(conn, rename("old", "new"), logger) }
        assertEquals(5, number(conn, "SELECT count(*) FROM category"))
        assertEquals(1, number(conn, "SELECT categoryId FROM book WHERE id=100"))
        assertEquals(null, text(conn, "SELECT heShortDesc FROM category WHERE id=12"))
    }

    @Test
    fun `target alias selection follows Otzaria display order`() = withConnection { conn ->
        insert(conn, 1, "old")
        insert(conn, 2, "רשי", 1)
        insert(conn, 11, "new")
        insert(conn, 12, "רש״י", 11)
        insert(conn, 13, "רש\"י", 11)
        conn.createStatement().use { it.execute("UPDATE category SET orderIndex=1 WHERE id=13") }
        book(conn, 100, 2)
        renameOrMergeCategory(conn, rename("old", "new"), logger)
        assertEquals(13, number(conn, "SELECT categoryId FROM book WHERE id=100"))
        assertEquals(3, number(conn, "SELECT count(*) FROM category"))
    }

    @Test
    fun `global renames refresh parents and skip sources deleted by recursive merges`() = withConnection { conn ->
        insert(conn, 1, "old")
        insert(conn, 2, "old", 1)
        insert(conn, 11, "new")
        insert(conn, 12, "new", 11)
        book(conn, 100, 2)
        renameOrMergeCategory(conn, rename("old", "new"), logger)
        assertEquals(2, number(conn, "SELECT count(*) FROM category"))
        assertEquals(12, number(conn, "SELECT categoryId FROM book WHERE id=100"))
        assertEquals(RenameResult.NotFound, renameOrMergeCategory(conn, rename("old", "new"), logger))
    }

    @Test
    fun `merging categories without books still reports a merge`() = withConnection { conn ->
        insert(conn, 1, "old")
        insert(conn, 11, "new")
        assertEquals(RenameResult.Merged(0), renameOrMergeCategory(conn, rename("old", "new"), logger))
    }

    @Test
    fun `a recursively removed matching source is skipped by the remaining global rename`() = withConnection { conn ->
        insert(conn, 1, "old")
        insert(conn, 2, "old", 1)
        insert(conn, 11, "new")
        insert(conn, 12, "old", 11)
        book(conn, 100, 2)
        renameOrMergeCategory(conn, rename("old", "new"), logger)
        assertEquals(2, number(conn, "SELECT count(*) FROM category"))
        assertEquals(12, number(conn, "SELECT categoryId FROM book WHERE id=100"))
        assertEquals("new", text(conn, "SELECT title FROM category WHERE id=12"))
    }

    private fun collect(
        conn: Connection,
        renames: List<CategoryRename>,
        overrides: List<CategoryDescriptionOverride>,
        moves: List<CategoryMove> = emptyList(),
        bookMoves: List<BookMove> = emptyList(),
    ) = collectForDbRuleFailures(conn, renames, moves, emptyList(), bookMoves, logger, overrides)

    private fun rename(old: String, new: String) = CategoryRename(old, new, CategoryMatchMode.Exact)

    private fun edit(
        path: String,
        short: DescriptionEdit = DescriptionEdit.Replace("final"),
        long: DescriptionEdit = DescriptionEdit.Keep,
    ) = CategoryDescriptionOverride(2, path, short, long)

    private fun withConnection(block: (Connection) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("PRAGMA foreign_keys=ON")
                st.execute("CREATE TABLE category(id INTEGER PRIMARY KEY, parentId INTEGER REFERENCES category(id) ON DELETE CASCADE, title TEXT NOT NULL, level INTEGER NOT NULL DEFAULT 0, orderIndex INTEGER NOT NULL DEFAULT 999, heShortDesc TEXT, heDesc TEXT)")
                st.execute("CREATE INDEX idx_category_parent ON category(parentId)")
                st.execute("CREATE TABLE book(id INTEGER PRIMARY KEY, categoryId INTEGER NOT NULL REFERENCES category(id) ON DELETE CASCADE, title TEXT NOT NULL)")
                st.execute("CREATE INDEX idx_book_category ON book(categoryId)")
                st.execute("CREATE TABLE line(id INTEGER PRIMARY KEY, bookId INTEGER NOT NULL REFERENCES book(id) ON DELETE CASCADE)")
            }
            block(conn)
        }
    }

    private fun insert(conn: Connection, id: Long, title: String, parent: Long? = null, short: String? = null, long: String? = null) {
        val level = if (parent == null) 0 else number(conn, "SELECT level FROM category WHERE id=$parent") + 1
        conn.prepareStatement("INSERT INTO category(id,parentId,title,heShortDesc,heDesc,level) VALUES(?,?,?,?,?,?)").use { st ->
            st.setLong(1, id)
            if (parent == null) st.setNull(2, java.sql.Types.INTEGER) else st.setLong(2, parent)
            st.setString(3, title)
            st.setString(4, short)
            st.setString(5, long)
            st.setInt(6, level)
            st.executeUpdate()
        }
    }

    private fun book(conn: Connection, id: Long, categoryId: Long, title: String = "book $id") {
        conn.prepareStatement("INSERT INTO book(id,categoryId,title) VALUES(?,?,?)").use { st ->
            st.setLong(1, id)
            st.setLong(2, categoryId)
            st.setString(3, title)
            st.executeUpdate()
        }
    }

    private fun number(conn: Connection, sql: String): Int = conn.createStatement().use { st ->
        st.executeQuery(sql).use { rs -> check(rs.next()); rs.getInt(1) }
    }

    private fun text(conn: Connection, sql: String): String? = conn.createStatement().use { st ->
        st.executeQuery(sql).use { rs -> check(rs.next()); rs.getString(1) }
    }
}
