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
            val snapshots = readLinkedSeifimCandidateSnapshots(conn)
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
            synthesizeSeifimAltTocs(conn, readLinkedSeifimCandidateSnapshots(conn))
        }
        repo.updateHasAltStructures(repo.getBookByTitle("משנה ברורה")!!.id, false)

        val second = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val snapshots = readLinkedSeifimCandidateSnapshots(conn)
            synthesizeSeifimAltTocs(conn, snapshots)
            snapshots
        }
        assertTrue(second.isEmpty(), "a book with a Seifim structure must not be re-synthesized")
        assertTrue(repo.getBookByTitle("משנה ברורה")!!.hasAltStructures, "a rerun must repair a stale flag")
    }

    @Test
    fun `a main TOC that already has seif headings under the siman is not re-synthesized`() = runBlocking {
        // ש"ך על יו"ד: סימן ← סעיף ← תוכן. עלה "סעיף" מסונתז היה מכפיל את
        // הכותרת הקיימת (otzaria#1249).
        val (_, mbId) = seedMiniDb()
        val simanEntryId = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            conn.prepareStatement(
                "SELECT e.id FROM tocEntry e JOIN tocText t ON t.id = e.textId WHERE e.bookId = ? AND t.text = 'סימן א'",
            ).use { st ->
                st.setLong(1, mbId)
                st.executeQuery().use { rs -> assertTrue(rs.next()); rs.getLong(1) }
            }
        }
        repo.insertTocEntry(
            TocEntry(bookId = mbId, parentId = simanEntryId, text = "סעיף א", level = 3, lineId = repo.getLineByIndex(mbId, 4)!!.id),
        )

        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            assertTrue(readLinkedSeifimCandidateSnapshots(conn).isEmpty(), "existing se'if headings must exclude the book")
        }
    }

    @Test
    fun `se'if-katan headings under the siman do not exclude the book`() = runBlocking {
        // seedMiniDb כבר מכיל "סעיף קטן א" מתחת לסימן — הספר חייב להישאר מועמד.
        seedMiniDb()
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            assertEquals(1, readLinkedSeifimCandidateSnapshots(conn).size)
        }
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
            assertTrue(readLinkedSeifimCandidateSnapshots(conn).isEmpty())
        }
    }

    @Test
    fun `stable ids are persisted in attached buildstate`() = runBlocking {
        val (_, mbId) = seedMiniDb()
        val state = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val state = attachEmptyBuildState(conn)
            synthesizeSeifimAltTocs(conn, readLinkedSeifimCandidateSnapshots(conn), AttachedBuildStateIds(conn))
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
            val snapshots = readLinkedSeifimCandidateSnapshots(conn)
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

/** The heRef-derived group: se'if read from the line's own heRef. */
class ComputeHeRefSeifMarkersTest {

    private fun line(lineIndex: Long, heRef: String?) =
        SeifRefLine(lineId = lineIndex * 10, lineIndex = lineIndex, heRef = heRef)

    private val headings = mapOf(
        0L to "ערוך השולחן",
        2L to "הקדמה",
        5L to "אורח חיים",
        6L to "סימן א",
        9L to "סימן ב",
        11L to "סדר הגט",
    )

    private val lines = listOf(
        line(0, null),
        line(1, null), // שורת המחבר
        line(2, null),
        line(3, "ערוך השולחן, הקדמה,  א"),
        line(4, "ערוך השולחן, הקדמה,  ב"),
        line(5, null),
        line(6, null),
        line(7, "ערוך השולחן, אורח חיים,  א, א"),
        line(8, "ערוך השולחן, אורח חיים,  א, ב"),
        line(9, null),
        line(10, "ערוך השולחן, אורח חיים,  ב, א"),
        line(11, null),
        line(12, "ערוך השולחן, אורח חיים, סדר הגט,  א"),
    )

    @Test
    fun `one leaf per seif, the double-space separator trimmed away`() {
        val markers = computeHeRefSeifMarkers("ערוך השולחן", headings, lines)
        assertEquals(
            listOf(7L to "סעיף א", 8L to "סעיף ב", 10L to "סעיף א"),
            markers.map { it.lineIndex to it.label },
        )
        assertEquals(listOf(70L, 80L, 100L), markers.map { it.lineId })
    }

    @Test
    fun `depth-2 intro nodes and named non-siman sections carry no seif`() {
        val markers = computeHeRefSeifMarkers("ערוך השולחן", headings, lines)
        assertTrue(markers.none { it.lineIndex in listOf(3L, 4L, 12L) })
    }

    @Test
    fun `a deeper tree keeps the last heRef component as the seif`() {
        // שו"ע הרב: חלק ← מהדורא ← סימן ← סעיף (97 שורות בעומק 4).
        val markers = computeHeRefSeifMarkers(
            "שולחן ערוך הרב",
            mapOf(0L to "אורח חיים", 1L to "מהדורא תניינא", 2L to "סימן א"),
            listOf(
                line(0, null),
                line(1, null),
                line(2, null),
                line(3, "שולחן ערוך הרב, אורח חיים, מהדורא תניינא,  א, א"),
                line(4, "שולחן ערוך הרב, אורח חיים, מהדורא תניינא,  א, ב"),
            ),
        )
        assertEquals(listOf("סעיף א", "סעיף ב"), markers.map { it.label })
    }

    @Test
    fun `repeated lines of one seif open a single leaf`() {
        val markers = computeHeRefSeifMarkers(
            "ערוך השולחן",
            mapOf(0L to "סימן א"),
            listOf(
                line(0, null),
                line(1, "ערוך השולחן, אורח חיים,  א, א"),
                line(2, "ערוך השולחן, אורח חיים,  א, א"),
                line(3, "ערוך השולחן, אורח חיים,  א, ב"),
            ),
        )
        assertEquals(listOf(1L to "סעיף א", 3L to "סעיף ב"), markers.map { it.lineIndex to it.label })
    }

    @Test
    fun `a heRef that reopens a closed seif aborts`() {
        // א,ב,א בתוך אותו סימן — שחיתות דאטה שהייתה יוצרת שני עלים "סעיף א".
        val failure = assertFailsWith<IllegalArgumentException> {
            computeHeRefSeifMarkers(
                "ערוך השולחן",
                mapOf(0L to "סימן א"),
                listOf(
                    line(0, null),
                    line(1, "ערוך השולחן, אורח חיים,  א, א"),
                    line(2, "ערוך השולחן, אורח חיים,  א, ב"),
                    line(3, "ערוך השולחן, אורח חיים,  א, א"),
                ),
            )
        }
        assertTrue(failure.message!!.contains("reopens a closed se'if"))
    }

    @Test
    fun `the same seif letter reopens freely in the next siman`() {
        val markers = computeHeRefSeifMarkers(
            "ערוך השולחן",
            mapOf(0L to "סימן א", 2L to "סימן ב"),
            listOf(
                line(0, null),
                line(1, "ערוך השולחן, אורח חיים,  א, א"),
                line(2, null),
                line(3, "ערוך השולחן, אורח חיים,  ב, א"),
            ),
        )
        assertEquals(listOf(1L to "סעיף א", 3L to "סעיף א"), markers.map { it.lineIndex to it.label })
    }

    @Test
    fun `a BOM in front of the heading still reads as a siman`() {
        val markers = computeHeRefSeifMarkers(
            "ערוך השולחן",
            mapOf(0L to "\uFEFFסימן א"),
            listOf(line(0, null), line(1, "ערוך השולחן, אורח חיים,  א, א")),
        )
        assertEquals(listOf("סעיף א"), markers.map { it.label })
    }

    @Test
    fun `a missing heRef inside a siman aborts`() {
        val failure = assertFailsWith<IllegalStateException> {
            computeHeRefSeifMarkers("ערוך השולחן", mapOf(0L to "סימן א"), listOf(line(0, null), line(1, null)))
        }
        assertTrue(failure.message!!.contains("no heRef"))
    }

    @Test
    fun `a too-shallow heRef inside a siman aborts`() {
        assertFailsWith<IllegalArgumentException> {
            computeHeRefSeifMarkers(
                "ערוך השולחן",
                mapOf(0L to "סימן א"),
                listOf(line(0, null), line(1, "ערוך השולחן, אורח חיים,  א")),
            )
        }
    }

    @Test
    fun `a heRef naming another siman aborts`() {
        val failure = assertFailsWith<IllegalArgumentException> {
            computeHeRefSeifMarkers(
                "ערוך השולחן",
                mapOf(0L to "סימן א"),
                listOf(line(0, null), line(1, "ערוך השולחן, אורח חיים,  ב, א")),
            )
        }
        assertTrue(failure.message!!.contains("does not sit in its siman"))
    }

    @Test
    fun `no siman container yields no markers`() {
        assertTrue(
            computeHeRefSeifMarkers(
                "ערוך השולחן",
                mapOf(0L to "הקדמה"),
                listOf(line(0, null), line(1, "ערוך השולחן, הקדמה,  א")),
            ).isEmpty(),
        )
    }
}

/**
 * End-to-end for the heRef group: a שולחן ערוך הרב-shaped book with a depth-3
 * and a depth-4 branch, seeded *before* a link-derived candidate so the
 * candidate order proves the group is appended and not sorted by bookId.
 */
class HeRefSeifimAltTocIntegrationTest {

    private val dbFile = Files.createTempFile("heref-seifim-test", ".db")
    private val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbFile")
    private lateinit var repo: SeforimRepository

    @AfterTest
    fun tearDown() {
        if (::repo.isInitialized) repo.close()
        Files.deleteIfExists(dbFile)
    }

    /** Returns (שולחן ערוך הרב id, משנה ברורה id); the Rav book gets the lower id. */
    private fun seedMiniDb(): Pair<Long, Long> = runBlocking {
        SeforimDb.Schema.create(driver)
        repo = SeforimRepository(dbFile.toString(), driver)
        val sourceId = repo.insertSource("Sefaria")
        val catId = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 1))

        val ravId = repo.insertBook(
            Book(categoryId = catId, sourceId = sourceId, title = "שולחן ערוך הרב", heRef = "שולחן ערוך הרב"),
        )
        fun ravLine(index: Int, content: String, heRef: String? = null) = runBlocking {
            repo.insertLine(Line(bookId = ravId, lineIndex = index, content = content, heRef = heRef))
        }
        val title = ravLine(0, "<h1>שולחן ערוך הרב</h1>")
        ravLine(1, "שניאור זלמן מליאדי")
        val intro = ravLine(2, "<h2>הקדמת בני המחבר</h2>")
        ravLine(3, "פסקה", "שולחן ערוך הרב, הקדמת בני המחבר,  א")
        ravLine(4, "פסקה", "שולחן ערוך הרב, הקדמת בני המחבר,  ב")
        val oc = ravLine(5, "<h2>אורח חיים</h2>")
        val simanHe = ravLine(6, "<h3>סימן ה</h3>")
        ravLine(7, "סעיף", "שולחן ערוך הרב, אורח חיים,  ה, א")
        ravLine(8, "סעיף", "שולחן ערוך הרב, אורח חיים,  ה, ב")
        // 9 ו-14 נשארים פנויים לטסטים שמזריקים שורה פגומה בתוך סימן.
        val mahadura = ravLine(10, "<h3>מהדורא תניינא</h3>")
        val simanAlef = ravLine(11, "<h4>סימן א</h4>")
        ravLine(12, "סעיף", "שולחן ערוך הרב, אורח חיים, מהדורא תניינא,  א, א")
        ravLine(13, "סעיף", "שולחן ערוך הרב, אורח חיים, מהדורא תניינא,  א, ב")
        val yd = ravLine(15, "<h2>יורה דעה</h2>")
        val hilchot = ravLine(16, "<h3>הלכות רבית ועיסקא</h3>")
        ravLine(17, "פסקה", "שולחן ערוך הרב, יורה דעה, הלכות רבית ועיסקא,  א")

        repo.insertTocEntry(TocEntry(bookId = ravId, parentId = null, text = "שולחן ערוך הרב", level = 0, lineId = title))
        repo.insertTocEntry(TocEntry(bookId = ravId, parentId = null, text = "הקדמת בני המחבר", level = 1, lineId = intro))
        val ocEntry = repo.insertTocEntry(TocEntry(bookId = ravId, parentId = null, text = "אורח חיים", level = 1, lineId = oc))
        repo.insertTocEntry(TocEntry(bookId = ravId, parentId = ocEntry, text = "סימן ה", level = 2, lineId = simanHe))
        val mahaduraEntry =
            repo.insertTocEntry(TocEntry(bookId = ravId, parentId = ocEntry, text = "מהדורא תניינא", level = 2, lineId = mahadura))
        repo.insertTocEntry(TocEntry(bookId = ravId, parentId = mahaduraEntry, text = "סימן א", level = 3, lineId = simanAlef))
        val ydEntry = repo.insertTocEntry(TocEntry(bookId = ravId, parentId = null, text = "יורה דעה", level = 1, lineId = yd))
        repo.insertTocEntry(TocEntry(bookId = ravId, parentId = ydEntry, text = "הלכות רבית ועיסקא", level = 2, lineId = hilchot))

        // מועמד מבוסס-קישורים, שנוצר אחרי — ומקבל bookId גבוה יותר.
        val saId = repo.insertBook(
            Book(categoryId = catId, sourceId = sourceId, title = "שולחן ערוך, אורח חיים", heRef = "שולחן ערוך, אורח חיים"),
        )
        val saSeif = repo.insertLine(Line(bookId = saId, lineIndex = 0, content = "סעיף א", heRef = "שולחן ערוך, אורח חיים א, א"))
        val mbId = repo.insertBook(
            Book(categoryId = catId, sourceId = sourceId, title = "משנה ברורה", heRef = "משנה ברורה"),
        )
        repo.insertBookBaseText(mbId, saId)
        val mbSiman = repo.insertLine(Line(bookId = mbId, lineIndex = 0, content = "<h3>סימן א</h3>"))
        val mbSk = repo.insertLine(Line(bookId = mbId, lineIndex = 1, content = "(א) ס\"ק ראשון"))
        repo.insertTocEntry(TocEntry(bookId = mbId, parentId = null, text = "סימן א", level = 1, lineId = mbSiman))
        repo.insertLink(
            Link(
                sourceBookId = saId,
                targetBookId = mbId,
                sourceLineId = saSeif,
                targetLineId = mbSk,
                targetLineIndex = 0,
                connectionType = ConnectionType.COMMENTARY,
            ),
        )
        // שני הכותרים הנעולים הנוספים — הקורא דורש שכולם יהיו בקורפוס.
        for (stub in listOf("ערוך השולחן", "ערוך השולחן העתיד")) {
            seedStubBook(catId, sourceId, stub)
        }
        ravId to mbId
    }

    /** A minimal pinned book: חלק ← סימן ← two se'if lines. */
    private suspend fun seedStubBook(catId: Long, sourceId: Long, title: String): Long {
        val id = repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = title, heRef = title))
        val part = repo.insertLine(Line(bookId = id, lineIndex = 0, content = "<h2>אורח חיים</h2>"))
        val siman = repo.insertLine(Line(bookId = id, lineIndex = 1, content = "<h3>סימן א</h3>"))
        repo.insertLine(Line(bookId = id, lineIndex = 2, content = "סעיף", heRef = "$title, אורח חיים,  א, א"))
        repo.insertLine(Line(bookId = id, lineIndex = 3, content = "סעיף", heRef = "$title, אורח חיים,  א, ב"))
        val partEntry = repo.insertTocEntry(TocEntry(bookId = id, parentId = null, text = "אורח חיים", level = 1, lineId = part))
        repo.insertTocEntry(TocEntry(bookId = id, parentId = partEntry, text = "סימן א", level = 2, lineId = siman))
        return id
    }

    @Test
    fun `the heRef group is appended after the link-derived one and keeps its entry ids`() = runBlocking {
        val (ravId, mbId) = seedMiniDb()
        assertTrue(ravId < mbId, "the Rav book must hold the lower bookId for this order proof")

        val result = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val snapshots = readSeifimCandidateSnapshots(conn)
            val stubIds = listOf("ערוך השולחן", "ערוך השולחן העתיד").map { repo.getBookByTitle(it)!!.id }
            assertEquals(listOf(mbId, ravId) + stubIds, snapshots.map { it.bookId })
            synthesizeSeifimAltTocs(conn, snapshots)
        }
        assertEquals(SeifimSynthesisResult(structures = 4, leaves = 9), result)

        // המועמד הקיים ממשיך לקבל את אותם ids כאילו רץ לבדו (1..n).
        val mbEntries = repo.getAltTocEntriesForStructure(repo.getAltTocStructuresForBook(mbId).single().id)
        val ravEntries = repo.getAltTocEntriesForStructure(repo.getAltTocStructuresForBook(ravId).single().id)
        assertEquals((1L..mbEntries.size.toLong()).toList(), mbEntries.map { it.id }.sorted())
        assertTrue(mbEntries.maxOf { it.id } < ravEntries.minOf { it.id })
    }

    @Test
    fun `containers mirror the main TOC and every siman gets its seif leaves`() = runBlocking {
        val (ravId, _) = seedMiniDb()
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            synthesizeSeifimAltTocs(conn, readSeifimCandidateSnapshots(conn))
        }
        assertTrue(repo.getBook(ravId)!!.hasAltStructures)

        val structure = repo.getAltTocStructuresForBook(ravId).single()
        assertEquals(SEIFIM_STRUCTURE_KEY, structure.key)
        val entries = repo.getAltTocEntriesForStructure(structure.id)
        // הקדמה, יורה דעה והלכות רבית אינם סימנים ואינם אבות של סימן.
        assertTrue(entries.none { it.text in listOf("הקדמת בני המחבר", "יורה דעה", "הלכות רבית ועיסקא") })

        val oc = entries.single { it.parentId == null }
        assertEquals("אורח חיים", oc.text)
        val ocChildren = entries.filter { it.parentId == oc.id }
        assertEquals(listOf("סימן ה", "מהדורא תניינא"), ocChildren.map { it.text })
        assertTrue(ocChildren.all { it.hasChildren })

        val simanHe = ocChildren.first()
        val simanHeLeaves = entries.filter { it.parentId == simanHe.id }.sortedBy { it.id }
        assertEquals(listOf("סעיף א", "סעיף ב"), simanHeLeaves.map { it.text })
        assertEquals(listOf(false, true), simanHeLeaves.map { it.isLastChild })
        assertTrue(simanHeLeaves.none { it.hasChildren })
        assertEquals(listOf(7, 8), simanHeLeaves.map { leaf -> repo.getLine(leaf.lineId!!)!!.lineIndex })

        val mahadura = ocChildren.last()
        val simanAlef = entries.single { it.parentId == mahadura.id }
        assertEquals("סימן א", simanAlef.text)
        val deepLeaves = entries.filter { it.parentId == simanAlef.id }.sortedBy { it.id }
        assertEquals(listOf("סעיף א", "סעיף ב"), deepLeaves.map { it.text })
        assertEquals(listOf(12, 13), deepLeaves.map { leaf -> repo.getLine(leaf.lineId!!)!!.lineIndex })
    }

    @Test
    fun `a second run is a no-op — the existing Seifim structure excludes the book`() = runBlocking {
        val (ravId, _) = seedMiniDb()
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            synthesizeSeifimAltTocs(conn, readSeifimCandidateSnapshots(conn))
        }
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            assertTrue(readHeRefSeifimCandidateSnapshots(conn).none { it.bookId == ravId })
        }
    }

    @Test
    fun `content outside a siman is not mapped to the previous section's seif`() = runBlocking {
        val (ravId, _) = seedMiniDb()
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            synthesizeSeifimAltTocs(conn, readSeifimCandidateSnapshots(conn))
        }
        val structureId = repo.getAltTocStructuresForBook(ravId).single().id
        val mapped = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            conn.prepareStatement(
                "SELECT l.lineIndex FROM line_alt_toc m JOIN line l ON l.id = m.lineId WHERE m.structureId = ? ORDER BY l.lineIndex",
            ).use { st ->
                st.setLong(1, structureId)
                st.executeQuery().use { rs ->
                    buildList { while (rs.next()) add(rs.getInt(1)) }
                }
            }
        }
        // ההקדמה (3,4), יורה דעה והלכות רבית (15-17) אינם תחת סימן — לא ממופים.
        assertEquals(listOf(5, 6, 7, 8, 10, 11, 12, 13), mapped)
    }

    @Test
    fun `a heading line with no tocEntry row aborts`() = runBlocking {
        val (ravId, _) = seedMiniDb()
        repo.insertLine(Line(bookId = ravId, lineIndex = 9, content = "<h4>כותרת ללא רשומת TOC</h4>"))
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val failure = assertFailsWith<IllegalStateException> { readSeifimCandidateSnapshots(conn) }
            assertTrue(failure.message!!.contains("no heRef"))
        }
        Unit
    }

    @Test
    fun `two tocEntry rows on one line abort`() = runBlocking {
        val (ravId, _) = seedMiniDb()
        repo.insertTocEntry(
            TocEntry(bookId = ravId, parentId = null, text = "כפילות", level = 1, lineId = repo.getLineByIndex(ravId, 6)!!.id),
        )
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val failure = assertFailsWith<IllegalArgumentException> { readSeifimCandidateSnapshots(conn) }
            assertTrue(failure.message!!.contains("Two tocEntry rows"))
        }
        Unit
    }

    @Test
    fun `a pinned title missing from the corpus aborts`() = runBlocking {
        // בדיוק מוטציית ה-QA: rename ב-book_renames.csv שמעלים את הספר בשקט.
        val (ravId, _) = seedMiniDb()
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            conn.prepareStatement("UPDATE book SET title = 'שולחן ערוך הרב ' WHERE id = ?").use { st ->
                st.setLong(1, ravId)
                st.executeUpdate()
            }
        }
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val failure = assertFailsWith<IllegalArgumentException> { readSeifimCandidateSnapshots(conn) }
            assertTrue(failure.message!!.contains("matched 0 books"))
        }
        Unit
    }

    @Test
    fun `a pinned book that stops yielding markers aborts`() = runBlocking {
        val (ravId, _) = seedMiniDb()
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            conn.prepareStatement(
                "DELETE FROM tocEntry WHERE bookId = ? AND textId IN (SELECT id FROM tocText WHERE text LIKE 'סימן %')",
            ).use { st ->
                st.setLong(1, ravId)
                st.executeUpdate()
            }
        }
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val failure = assertFailsWith<IllegalArgumentException> { readSeifimCandidateSnapshots(conn) }
            assertTrue(failure.message!!.contains("No se'if markers derived"))
        }
        Unit
    }

    @Test
    fun `a main TOC that already breaks simanim into seifim is left alone`() = runBlocking {
        val (ravId, _) = seedMiniDb()
        val simanHeEntryId = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            conn.prepareStatement(
                "SELECT e.id FROM tocEntry e JOIN tocText t ON t.id = e.textId WHERE e.bookId = ? AND t.text = 'סימן ה'",
            ).use { st ->
                st.setLong(1, ravId)
                st.executeQuery().use { rs -> assertTrue(rs.next()); rs.getLong(1) }
            }
        }
        repo.insertTocEntry(
            TocEntry(bookId = ravId, parentId = simanHeEntryId, text = "סעיף א", level = 3, lineId = repo.getLineByIndex(ravId, 7)!!.id),
        )
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            assertTrue(readHeRefSeifimCandidateSnapshots(conn).none { it.bookId == ravId })
        }
    }
}
