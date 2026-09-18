package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// Malbim on Leviticus declares only Leviticus, yet is densely a commentary on Sifra.
class PrimaryBaseDensityChainingTest {
    private val leviticus = 1L
    private val sifra = 2L
    private val malbim = 3L

    private fun Pair<Long, Long>.norm(): Pair<Long, Long> = if (first < second) this else second to first

    private fun meta() = hashMapOf(
        leviticus to BookMeta(false, 0, null),
        sifra to BookMeta(false, 0, null),
        malbim to BookMeta(
            false, 1, null,
            dependence = Dependence.COMMENTARY,
            baseTextBookIds = setOf(leviticus),
            sefariaDeclaredBaseTextBookIds = setOf(leviticus),
        ),
    )

    private fun counts(nDP: Int, nDS: Int) = mapOf(
        (leviticus to malbim).norm() to nDP,
        (sifra to malbim).norm() to nDS,
    )

    private fun demotedType(keptBaseEdges: Set<Pair<Long, Long>>): String = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)
        ConnectionType.values().forEach { bindings.upsertConnectionType(it.name) }
        val sourceId = repo.insertSource("Sefaria")
        val tanach = repo.insertCategory(Category(0, null, "תנ״ך", level = 0, order = 1))
        val midrash = repo.insertCategory(Category(0, null, "מדרש", level = 0, order = 2))
        repo.rebuildCategoryClosure()
        fun book(id: Long, catId: Long, title: String) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = false, totalLines = 1, hasAltStructures = false, hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(sifra, midrash, "ספרא"))
        repo.insertBook(book(malbim, tanach, "מלבי\"ם על ויקרא"))
        listOf(sifra, malbim).forEach { bid ->
            repo.insertLinesBatch(listOf(Line(id = bid * 10, bookId = bid, lineIndex = 0, content = "c$bid", heRef = "r$bid")))
        }
        repo.insertLink(
            Link(
                sourceBookId = sifra, targetBookId = malbim, sourceLineId = sifra * 10, targetLineId = malbim * 10,
                targetLineIndex = 0, connectionType = ConnectionType.COMMENTARY, baseProvenance = 0,
            )
        )
        SefariaLinksImporter(repo, bindings, Logger.withTag("PrimaryBaseDensityChainingTest"))
            .demoteCrossCorpusDependantLinks(keptBaseEdges)
        driver.getConnection().createStatement().use { st ->
            st.executeQuery(
                "SELECT ct.name FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId " +
                    "WHERE l.sourceBookId = $sifra AND l.targetBookId = $malbim",
            ).use { rs -> if (rs.next()) rs.getString(1) else "MISSING" }
        }
    }

    @Test
    fun densePrimaryEdgeIsFoundAndItsLinkSurvivesDemotion() {
        val bookMeta = meta()
        // Real commentary-typed counts: Malbim↔Sifra 1907, Malbim↔Leviticus 1910.
        val edges = findPrimaryBaseDensityEdges(bookMeta, counts(nDP = 1910, nDS = 1907), Logger.withTag("test"))
        assertEquals(setOf(sifra to malbim), edges)
        assertEquals(meta(), bookMeta, "exemption only — book metadata is never mutated")
        assertEquals("COMMENTARY", demotedType(edges))
    }

    @Test
    fun sparsePrimaryIsNotFoundAndItsLinkIsDemoted() {
        val bookMeta = meta()
        // Mekhilta ↔ Malbim on Exodus shape: ratio 0.25.
        val edges = findPrimaryBaseDensityEdges(bookMeta, counts(nDP = 1200, nDS = 300), Logger.withTag("test"))
        assertTrue(edges.isEmpty())
        assertEquals("RELATED", demotedType(edges))
    }

    @Test
    fun linkFloorIsEnforced() {
        val bookMeta = meta()
        // Ratio 1.2 but below LINK_DENSITY_BASE_FLOOR.
        val edges = findPrimaryBaseDensityEdges(bookMeta, counts(nDP = 40, nDS = LINK_DENSITY_BASE_FLOOR - 1), Logger.withTag("test"))
        assertTrue(edges.isEmpty())
        val atFloor = findPrimaryBaseDensityEdges(meta(), counts(nDP = 40, nDS = LINK_DENSITY_BASE_FLOOR), Logger.withTag("test"))
        assertEquals(setOf(sifra to malbim), atFloor)
    }

    @Test
    fun dependantPrimaryCandidateIsIgnored() {
        val bookMeta = meta()
        bookMeta[sifra] = BookMeta(false, 1, null, dependence = Dependence.COMMENTARY)
        val edges = findPrimaryBaseDensityEdges(bookMeta, counts(nDP = 1910, nDS = 1907), Logger.withTag("test"))
        assertTrue(edges.isEmpty())
    }

    @Test
    fun dependantTypedCountsSubtractAndFailLoudly() {
        val all = mapOf((1L to 3L) to 2577, (2L to 3L) to 1937)
        val withoutCommentary = mapOf((1L to 3L) to 667, (2L to 3L) to 30)
        assertEquals(mapOf((1L to 3L) to 1910, (2L to 3L) to 1907), dependantTypedLinkCounts(all, withoutCommentary))
        assertFailsWith<IllegalArgumentException> { dependantTypedLinkCounts(all, mapOf((1L to 3L) to 3000)) }
        assertFailsWith<IllegalStateException> { dependantTypedLinkCounts(all, mapOf((4L to 5L) to 1)) }
    }
}
