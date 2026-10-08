package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** A ForDB row keyed by a renamed book's former title must fail the build, not vanish. */
class ForDbTitleGateTest {
    private val former = "חידושי אגדות על ברכות"
    private val display = "מהרש\"א - חידושי אגדות על ברכות"
    private val logger = Logger.withTag("test")

    private fun generationsDb(title: String, heRef: String): Connection =
        DriverManager.getConnection("jdbc:sqlite::memory:").apply {
            createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY, title TEXT NOT NULL, heRef TEXT)")
                st.execute("CREATE TABLE generation (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)")
                st.execute(
                    "CREATE TABLE book_generation (bookId INTEGER NOT NULL, generationId INTEGER NOT NULL, " +
                        "PRIMARY KEY (bookId, generationId))",
                )
            }
            prepareStatement("INSERT INTO book (id, title, heRef) VALUES (1, ?, ?)").use {
                it.setString(1, title)
                it.setString(2, heRef)
                it.executeUpdate()
            }
        }

    private fun count(conn: Connection, table: String): Int =
        conn.createStatement().use { st -> st.executeQuery("SELECT COUNT(*) FROM $table").use { it.next(); it.getInt(1) } }

    @Test
    fun `a row naming a renamed book by its former title fails and names both titles`() {
        val e = assertFailsWith<IllegalStateException> {
            requireForDbUsesDisplayTitles(
                "book_info.csv", listOf(former, "ברכות"), setOf(display, "ברכות"),
                listOf(RetitledBook(1, display, former)),
            )
        }
        assertContains(e.message!!, "'$former' → '$display'")
        assertContains(e.message!!, "book_info.csv names 1 book(s)")
    }

    @Test
    fun `a stale former-title row beside a current-title row passes`() {
        requireForDbUsesDisplayTitles(
            "book_info.csv", listOf(former, display), setOf(display),
            listOf(RetitledBook(1, display, former)),
        )
    }

    @Test
    fun `an unmatched title that is no book's former title passes`() {
        // The released DB still has the old titles: ForDB rows with new titles only warn.
        requireForDbUsesDisplayTitles("book_info.csv", listOf(display), setOf(former), emptyList())
    }

    @Test
    fun `retitled books are read exactly and trimmed`() {
        generationsDb("$display ", former).use { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title, heRef) VALUES (2, 'ברכות', 'ברכות')") }
            conn.createStatement().use { it.execute("INSERT INTO book (id, title, heRef) VALUES (3, 'ספר', NULL)") }
            assertEquals(listOf(RetitledBook(1, display, former)), loadRetitledBooks(conn))
        }
    }

    @Test
    fun `applyGenerations fails before writing when a row uses the former title`() {
        generationsDb(display, former).use { conn ->
            conn.autoCommit = false
            val e = assertFailsWith<IllegalStateException> {
                applyGenerations(conn, listOf(former to "אחרונים"), logger)
            }
            assertContains(e.message!!, display)
            assertEquals(0, count(conn, "generation"))
            assertEquals(0, count(conn, "book_generation"))
        }
    }

    @Test
    fun `applyGenerations links a row that uses the display title`() {
        generationsDb(display, former).use { conn ->
            assertEquals(1, applyGenerations(conn, listOf(display to "אחרונים"), logger).linksCreated)
        }
    }

    @Test
    fun `applyGenerations only warns for new titles against a DB that predates the rename`() {
        generationsDb(former, former).use { conn ->
            val result = applyGenerations(conn, listOf(display to "אחרונים"), logger)
            assertEquals(1, result.unmatched)
            assertEquals(0, result.linksCreated)
        }
    }

    @Test
    fun `validateForDbInputs consumers only warn for a new-title ForDB against the released old-title DB`() =
        runBlocking {
            // otzaria-library validates a new ForDB against the last released DB (heRef == title there).
            val path = Files.createTempFile("fordb-title-gate-released-", ".db")
            try {
                val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$path")
                SeforimDb.Schema.create(driver)
                val repo = SeforimRepository(path.toString(), driver)
                val sourceId = repo.insertSource("Sefaria")
                val catId = repo.insertCategory(Category(0, null, "תלמוד", level = 0, order = 1))
                repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = former, heRef = former))
                val retitled = DriverManager.getConnection("jdbc:sqlite:$path").use(::loadRetitledBooks)
                assertEquals(emptyList(), retitled)
                val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)

                val result = applyMetadata(
                    repo, bindings, mapOf(display to BulkMetadata(listOf(1600), null)),
                    mapOf(display to Description(heShortDesc = "קצר", heDesc = null)), retitled, logger,
                )
                assertEquals(0, result.updated)
                assertEquals(listOf(display), result.unmatchedTitles)
                repo.close()
            } finally {
                Files.deleteIfExists(path)
            }
        }

    @Test
    fun `applyMetadata fails before writing when a description uses the former title`() = runBlocking {
        val path = Files.createTempFile("fordb-title-gate-", ".db")
        try {
            val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$path")
            SeforimDb.Schema.create(driver)
            val repo = SeforimRepository(path.toString(), driver)
            val sourceId = repo.insertSource("Sefaria")
            val catId = repo.insertCategory(Category(0, null, "תלמוד", level = 0, order = 1))
            val bookId = repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = display, heRef = former))
            val retitled = DriverManager.getConnection("jdbc:sqlite:$path").use(::loadRetitledBooks)
            val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)

            val e = assertFailsWith<IllegalStateException> {
                applyMetadata(
                    repo, bindings, emptyMap(),
                    mapOf(former to Description(heShortDesc = "קצר", heDesc = null)), retitled, logger,
                )
            }
            assertContains(e.message!!, "sefaria_metadata_changes.csv")
            assertEquals(null, repo.getBook(bookId)?.heShortDesc)

            val ok = applyMetadata(
                repo, bindings, emptyMap(),
                mapOf(display to Description(heShortDesc = "קצר", heDesc = null)), retitled, logger,
            )
            assertEquals(1, ok.updated)
            repo.close()
        } finally {
            Files.deleteIfExists(path)
        }
    }
}
