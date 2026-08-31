package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.buildstate.AltTocStructureKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateReader
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSnapshot
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateWriter
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
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
import java.sql.SQLException
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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
     * Commentary "משנה ברורה": intro line (unlinked), a section + siman +
     * se'if-katan main-TOC hierarchy, then ס"ק lines linked to seifim א,א,ג.
     * The synthesized tree must retain section/siman but drop se'if-katan.
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
        val section = repo.insertLine(Line(bookId = mbId, lineIndex = 1, content = "<h2>אורח חיים</h2>"))
        val siman = repo.insertLine(Line(bookId = mbId, lineIndex = 2, content = "<h3>סימן א</h3>"))
        val seifKatan = repo.insertLine(Line(bookId = mbId, lineIndex = 3, content = "<h4>סעיף קטן א</h4>"))
        val sk1 = repo.insertLine(Line(bookId = mbId, lineIndex = 4, content = "(א) ס\"ק ראשון"))
        val sk2 = repo.insertLine(Line(bookId = mbId, lineIndex = 5, content = "(ב) ס\"ק שני"))
        val sk3 = repo.insertLine(Line(bookId = mbId, lineIndex = 6, content = "(ג) ס\"ק שלישי"))
        check(intro > 0)

        val sectionEntry = repo.insertTocEntry(
            TocEntry(bookId = mbId, parentId = null, text = "אורח חיים", level = 1, lineId = section),
        )
        val simanEntry = repo.insertTocEntry(
            TocEntry(bookId = mbId, parentId = sectionEntry, text = "סימן א", level = 2, lineId = siman),
        )
        repo.insertTocEntry(
            TocEntry(bookId = mbId, parentId = simanEntry, text = "סעיף קטן א", level = 3, lineId = seifKatan),
        )

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

    private fun attachEmptyBuildState(conn: java.sql.Connection): java.nio.file.Path {
        val state = Files.createTempFile("seifim-buildstate", ".db")
        Files.delete(state)
        BuildStateWriter().write(BuildStateSnapshot.empty(), state)
        conn.prepareStatement("ATTACH DATABASE ? AS seifim_state").use { st ->
            st.setString(1, state.toString())
            st.execute()
        }
        return state
    }

    @Test
    fun `synthesizes a Seifim structure with heading mirror and seif leaves`() = runBlocking {
        val (_, mbId) = seedMiniDb()

        val result = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val snapshots = readSeifimCandidateSnapshots(conn)
            assertEquals(1, snapshots.size)
            val snapshot = snapshots.single()
            assertEquals(mbId, snapshot.bookId)
            assertEquals(listOf("סעיף א", "סעיף ג"), snapshot.markers.map { it.label })
            assertEquals(listOf("אורח חיים", "סימן א"), snapshot.headings.map { it.text })
            synthesizeSeifimAltTocs(conn, snapshots)
        }
        assertEquals(SeifimSynthesisResult(structures = 1, leaves = 2), result)
        assertTrue(repo.getBook(mbId)!!.hasAltStructures)

        // Structure row exists with the Seifim key.
        val structures = repo.getAltTocStructuresForBook(mbId)
        assertEquals(listOf(SEIFIM_STRUCTURE_KEY), structures.map { it.key })
        val structureId = structures.single().id

        // The section + siman hierarchy is mirrored, but the main TOC's
        // se'if-katan heading is deliberately absent. Both leaves hang from
        // the siman, not from the se'if-katan.
        val entries = repo.getAltTocEntriesForStructure(structureId)
        assertTrue(entries.none { it.text == "סעיף קטן א" })
        val sectionEntry = entries.single { it.parentId == null }
        assertEquals("אורח חיים", sectionEntry.text)
        val simanEntry = entries.single { it.parentId == sectionEntry.id }
        assertEquals("סימן א", simanEntry.text)
        assertTrue(sectionEntry.hasChildren)
        assertTrue(simanEntry.hasChildren)
        val leafEntries = entries.filter { it.parentId == simanEntry.id }.sortedBy { it.id }
        assertEquals(listOf("סעיף א", "סעיף ג"), leafEntries.map { it.text })
        assertEquals(listOf(false, true), leafEntries.map { it.isLastChild })
    }

    @Test
    fun `second run is a no-op — existing Seifim structure excludes the book`() = runBlocking {
        seedMiniDb()

        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            synthesizeSeifimAltTocs(conn, readSeifimCandidateSnapshots(conn))
        }
        repo.updateHasAltStructures(repo.getBookByTitle("משנה ברורה")!!.id, false)

        val second = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val snapshots = readSeifimCandidateSnapshots(conn)
            synthesizeSeifimAltTocs(conn, snapshots)
            snapshots
        }
        assertTrue(second.isEmpty(), "a book with a Seifim structure must not be re-synthesized")
        assertTrue(repo.getBookByTitle("משנה ברורה")!!.hasAltStructures, "a rerun must repair a stale flag")
    }

    @Test
    fun `incidental SA links do not qualify a commentary on another declared base`() = runBlocking {
        val (_, mbId) = seedMiniDb()
        val sourceId = repo.insertSource("Other")
        val categoryId = repo.getBook(mbId)!!.categoryId
        val otherBaseId = repo.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "מגן אברהם", heRef = "מגן אברהם"),
        )
        repo.insertBookBaseText(mbId, otherBaseId)
        val targets = (4..6).map { repo.getLineByIndex(mbId, it)!!.id }
        repeat(4) { index ->
            val sourceLine = repo.insertLine(
                Line(bookId = otherBaseId, lineIndex = index, content = "בסיס $index", heRef = "מגן אברהם $index"),
            )
            repo.insertLink(
                Link(
                    sourceBookId = otherBaseId,
                    targetBookId = mbId,
                    sourceLineId = sourceLine,
                    targetLineId = targets[index % targets.size],
                    targetLineIndex = 0,
                    connectionType = ConnectionType.COMMENTARY,
                ),
            )
        }

        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            assertTrue(readSeifimCandidateSnapshots(conn).isEmpty())
        }
    }

    @Test
    fun `stable ids are persisted in attached buildstate`() = runBlocking {
        val (_, mbId) = seedMiniDb()
        val state = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val state = attachEmptyBuildState(conn)
            synthesizeSeifimAltTocs(conn, readSeifimCandidateSnapshots(conn), AttachedBuildStateIds(conn))
            state
        }
        try {
            val structureId = repo.getAltTocStructuresForBook(mbId).single().id
            val snapshot = BuildStateReader().read(state)
            assertEquals(structureId, snapshot.altTocStructures[AltTocStructureKey(mbId, SEIFIM_STRUCTURE_KEY)])
            val tocTextIds = snapshot.lookups[IdTable.TOC_TEXT].orEmpty()
            assertTrue(tocTextIds.keys.containsAll(listOf("סעיף א", "סעיף ג")))
        } finally {
            Files.deleteIfExists(state)
        }
    }

    @Test
    fun `failed synthesis rolls back every table and restores auto-commit`() = runBlocking {
        seedMiniDb()

        var state: java.nio.file.Path? = null
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val snapshots = readSeifimCandidateSnapshots(conn)
            state = attachEmptyBuildState(conn)
            conn.createStatement().use {
                it.execute(
                    "CREATE TRIGGER reject_seifim_entry BEFORE INSERT ON alt_toc_entry " +
                        "BEGIN SELECT RAISE(ABORT, 'forced failure'); END",
                )
            }

            assertFailsWith<SQLException> {
                synthesizeSeifimAltTocs(conn, snapshots, AttachedBuildStateIds(conn))
            }
            assertTrue(conn.autoCommit)
            assertTrue(!repo.getBookByTitle("משנה ברורה")!!.hasAltStructures)
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM alt_toc_structure WHERE key = 'Seifim'").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(0, rs.getInt(1))
                }
            }
        }
        val rolledBackState = BuildStateReader().read(state!!)
        assertTrue(rolledBackState.altTocStructures.isEmpty())
        assertTrue(IdTable.ALT_TOC_STRUCTURE !in rolledBackState.counters)
        Files.deleteIfExists(state)
        Unit
    }
}
