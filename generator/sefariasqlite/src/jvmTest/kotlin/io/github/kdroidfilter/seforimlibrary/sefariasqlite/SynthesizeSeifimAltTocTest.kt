package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ComputeSeifMarkersTest {

    private fun row(lineIndex: Long, heRef: String, lineId: Long = lineIndex * 10) =
        SeifLinkRow(lineId = lineId, lineIndex = lineIndex, baseHeRef = heRef)

    @Test
    fun `marker opens each seif group, like Mishnah Berurah siman 1`() {
        val markers = computeSeifMarkers(
            listOf(
                row(29, "שולחן ערוך, אורח חיים א, א"),
                row(30, "שולחן ערוך, אורח חיים א, א"),
                row(31, "שולחן ערוך, אורח חיים א, א"),
                row(37, "שולחן ערוך, אורח חיים א, ג"),
                row(38, "שולחן ערוך, אורח חיים א, ג"),
                row(40, "שולחן ערוך, אורח חיים א, ד"),
            ),
        )
        assertEquals(
            listOf(29L to "סעיף א", 37L to "סעיף ג", 40L to "סעיף ד"),
            markers.map { it.lineIndex to it.label },
        )
    }

    @Test
    fun `first link decides a multi-linked line`() {
        val markers = computeSeifMarkers(
            listOf(
                row(10, "שולחן ערוך, אורח חיים ב, א"),
                row(10, "שולחן ערוך, אורח חיים ב, ג"),
                row(11, "שולחן ערוך, אורח חיים ב, ג"),
            ),
        )
        assertEquals(
            listOf(10L to "סעיף א", 11L to "סעיף ג"),
            markers.map { it.lineIndex to it.label },
        )
    }

    @Test
    fun `new siman opening on the same seif letter still gets a marker`() {
        val markers = computeSeifMarkers(
            listOf(
                row(47, "שולחן ערוך, אורח חיים א, א"),
                row(50, "שולחן ערוך, אורח חיים ב, א"),
            ),
        )
        assertEquals(
            listOf(47L to "סעיף א", 50L to "סעיף א"),
            markers.map { it.lineIndex to it.label },
        )
    }

    @Test
    fun `malformed heRefs are skipped`() {
        val markers = computeSeifMarkers(
            listOf(
                row(5, "ללא פסיק"),
                row(6, "שולחן ערוך, אורח חיים א,   "),
                row(7, "שולחן ערוך, אורח חיים א, ב"),
            ),
        )
        assertEquals(listOf(7L to "סעיף ב"), markers.map { it.lineIndex to it.label })
    }

    @Test
    fun `empty input yields no markers`() {
        assertTrue(computeSeifMarkers(emptyList()).isEmpty())
    }
}

/** End-to-end over a real (temp-file) DB: snapshot read + alt-TOC write. */
class SynthesizeSeifimAltTocIntegrationTest {

    private val dbFile = Files.createTempFile("seifim-test", ".db")
    private val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbFile")
    private lateinit var repo: SeforimRepository

    @AfterTest
    fun tearDown() {
        if (::repo.isInitialized) repo.close()
        Files.deleteIfExists(dbFile)
    }

    /**
     * Base book "שולחן ערוך, אורח חיים": seif lines with heRefs.
     * Commentary "משנה ברורה": intro line (unlinked), <h2>סימן א</h2> heading
     * with a toc entry, then ס"ק lines linked to seifim א,א,ג.
     */
    private fun seedMiniDb(): Pair<Long, Long> = runBlocking {
        SeforimDb.Schema.create(driver)
        repo = SeforimRepository(dbFile.toString(), driver)

        val sourceId = repo.insertSource("Sefaria")
        val catId = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 1))

        val saId = repo.insertBook(
            Book(categoryId = catId, sourceId = sourceId, title = "שולחן ערוך, אורח חיים", heRef = "שולחן ערוך, אורח חיים"),
        )
        val saSeifA = repo.insertLine(Line(bookId = saId, lineIndex = 0, content = "סעיף א", heRef = "שולחן ערוך, אורח חיים א, א"))
        val saSeifC = repo.insertLine(Line(bookId = saId, lineIndex = 1, content = "סעיף ג", heRef = "שולחן ערוך, אורח חיים א, ג"))

        val mbId = repo.insertBook(
            Book(categoryId = catId, sourceId = sourceId, title = "משנה ברורה", heRef = "משנה ברורה"),
        )
        repo.insertBookBaseText(mbId, saId)
        // בסיס שני (כמו קול יעקב שמוצהר גם על שו"ע וגם על שו"ע הרב) — הספר
        // חייב להישאר מועמד יחיד עם מבנה אחד.
        val saRavId = repo.insertBook(
            Book(categoryId = catId, sourceId = sourceId, title = "שולחן ערוך הרב", heRef = "שולחן ערוך הרב"),
        )
        repo.insertBookBaseText(mbId, saRavId)

        val intro = repo.insertLine(Line(bookId = mbId, lineIndex = 0, content = "הקדמה"))
        val heading = repo.insertLine(Line(bookId = mbId, lineIndex = 1, content = "<h2>סימן א</h2>"))
        val sk1 = repo.insertLine(Line(bookId = mbId, lineIndex = 2, content = "(א) ס\"ק ראשון"))
        val sk2 = repo.insertLine(Line(bookId = mbId, lineIndex = 3, content = "(ב) ס\"ק שני"))
        val sk3 = repo.insertLine(Line(bookId = mbId, lineIndex = 4, content = "(ג) ס\"ק שלישי"))
        check(intro > 0)

        repo.insertTocEntry(TocEntry(bookId = mbId, parentId = null, text = "סימן א", level = 1, lineId = heading))

        for ((skLine, saLine) in listOf(sk1 to saSeifA, sk2 to saSeifA, sk3 to saSeifC)) {
            repo.insertLink(
                Link(
                    sourceBookId = saId,
                    targetBookId = mbId,
                    sourceLineId = saLine,
                    targetLineId = skLine,
                    targetLineIndex = 0,
                    connectionType = ConnectionType.COMMENTARY,
                ),
            )
        }
        saId to mbId
    }

    @Test
    fun `synthesizes a Seifim structure with heading mirror and seif leaves`() = runBlocking {
        val (_, mbId) = seedMiniDb()

        val leaves = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val snapshots = readSeifimCandidateSnapshots(conn)
            assertEquals(1, snapshots.size)
            val snapshot = snapshots.single()
            assertEquals(mbId, snapshot.bookId)
            assertEquals(listOf("סעיף א", "סעיף ג"), snapshot.markers.map { it.label })
            writeSeifimAltToc(conn, snapshot)
        }
        assertEquals(2, leaves)

        // Structure row exists with the Seifim key.
        val structures = repo.getAltTocStructuresForBook(mbId)
        assertEquals(listOf(SEIFIM_STRUCTURE_KEY), structures.map { it.key })
        val structureId = structures.single().id

        // One container (the mirrored heading, now a parent) + two leaves.
        val entries = repo.getAltTocEntriesForStructure(structureId)
        val container = entries.single { it.parentId == null }
        assertEquals("סימן א", container.text)
        assertTrue(container.hasChildren)
        val leafEntries = entries.filter { it.parentId == container.id }.sortedBy { it.id }
        assertEquals(listOf("סעיף א", "סעיף ג"), leafEntries.map { it.text })
        assertEquals(listOf(false, true), leafEntries.map { it.isLastChild })
    }

    @Test
    fun `second run is a no-op — existing Seifim structure excludes the book`() = runBlocking {
        seedMiniDb()

        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            writeSeifimAltToc(conn, readSeifimCandidateSnapshots(conn).single())
        }

        val second = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            readSeifimCandidateSnapshots(conn)
        }
        assertTrue(second.isEmpty(), "a book with a Seifim structure must not be re-synthesized")
    }
}
