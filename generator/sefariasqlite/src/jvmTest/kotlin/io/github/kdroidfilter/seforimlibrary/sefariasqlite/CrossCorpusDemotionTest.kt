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
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Regression: flattenTalmudCategories renamed the corpus root "תלמוד" to
 * "תלמוד בבלי"/"תלמוד ירושלמי", but the demotion whitelist still matched the
 * old "תלמוד" — so every Shas book got a NULL corpus and cross-corpus
 * COMMENTARY links (e.g. חתם סופר → ברכות) were no longer demoted to RELATED.
 */
class CrossCorpusDemotionTest {
    @Test
    fun crossCorpusCommentaryOnFlattenedTalmudIsDemoted_sameCorpusKept() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)
        ConnectionType.values().forEach { bindings.upsertConnectionType(it.name) }

        val sourceId = repo.insertSource("Sefaria")

        val talmudRoot = repo.insertCategory(Category(0, null, "תלמוד בבלי", level = 0, order = 1))
        val sederZeraim = repo.insertCategory(Category(0, talmudRoot, "סדר זרעים", level = 1, order = 1))
        val tanachRoot = repo.insertCategory(Category(0, null, "תנ״ך", level = 0, order = 2))
        repo.rebuildCategoryClosure()

        fun book(id: Long, catId: Long, title: String) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = false, totalLines = 1, hasAltStructures = false, hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, sederZeraim, "ברכות"))
        repo.insertBook(book(2, tanachRoot, "חתם סופר על התורה"))
        repo.insertBook(book(3, sederZeraim, "רש\"י על ברכות"))
        listOf(1L, 2L, 3L).forEach { bid ->
            repo.insertLinesBatch(listOf(Line(id = bid * 10, bookId = bid, lineIndex = 0, content = "c$bid", heRef = "r$bid")))
        }

        fun link(src: Long, tgt: Long) = Link(
            sourceBookId = src, targetBookId = tgt, sourceLineId = src * 10, targetLineId = tgt * 10,
            targetLineIndex = 0, connectionType = ConnectionType.COMMENTARY, baseProvenance = 0,
        )
        repo.insertLink(link(1, 2)) // ברכות → חתם סופר (cross-corpus) — must demote
        repo.insertLink(link(1, 3)) // ברכות → רש"י (same corpus) — must stay

        SefariaLinksImporter(repo, bindings, Logger.withTag("CrossCorpusDemotionTest"))
            .demoteCrossCorpusDependantLinks()

        fun typeOf(src: Long, tgt: Long): String {
            val conn: Connection = driver.getConnection()
            conn.createStatement().use { st ->
                st.executeQuery(
                    "SELECT ct.name FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId " +
                        "WHERE l.sourceBookId = $src AND l.targetBookId = $tgt",
                ).use { rs -> return if (rs.next()) rs.getString(1) else "MISSING" }
            }
        }

        assertEquals("RELATED", typeOf(1, 2), "cross-corpus COMMENTARY on a flattened Talmud book must be demoted")
        assertEquals("COMMENTARY", typeOf(1, 3), "genuine same-corpus Talmud commentary must be kept")
    }

    // Regression: MIN(title) tagged מדרש>הלכה books as הלכה, demoting ויקרא→ספרא.
    @Test
    fun midrashHalakhaSubcategoryIsTaggedByRoot_notByTitle() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)
        ConnectionType.values().forEach { bindings.upsertConnectionType(it.name) }

        val sourceId = repo.insertSource("Sefaria")

        val tanachRoot = repo.insertCategory(Category(0, null, "תנ״ך", level = 0, order = 1))
        val midrashRoot = repo.insertCategory(Category(0, null, "מדרש", level = 0, order = 2))
        val midrashHalakha = repo.insertCategory(Category(0, midrashRoot, "הלכה", level = 1, order = 1))
        val halakhaRoot = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 3))
        repo.rebuildCategoryClosure()

        fun book(id: Long, catId: Long, title: String) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = false, totalLines = 1, hasAltStructures = false, hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, tanachRoot, "ויקרא"))
        repo.insertBook(book(2, midrashHalakha, "ספרא"))
        repo.insertBook(book(3, halakhaRoot, "שולחן ערוך"))
        listOf(1L, 2L, 3L).forEach { bid ->
            repo.insertLinesBatch(listOf(Line(id = bid * 10, bookId = bid, lineIndex = 0, content = "c$bid", heRef = "r$bid")))
        }

        fun link(src: Long, tgt: Long) = Link(
            sourceBookId = src, targetBookId = tgt, sourceLineId = src * 10, targetLineId = tgt * 10,
            targetLineIndex = 0, connectionType = ConnectionType.MIDRASH, baseProvenance = 0,
        )
        repo.insertLink(link(1, 2)) // ויקרא → ספרא (תנ״ך→מדרש) — must stay
        repo.insertLink(link(1, 3)) // ויקרא → שולחן ערוך (תנ״ך→הלכה) — must demote

        SefariaLinksImporter(repo, bindings, Logger.withTag("CrossCorpusDemotionTest"))
            .demoteCrossCorpusDependantLinks()

        fun typeOf(src: Long, tgt: Long): String {
            val conn: Connection = driver.getConnection()
            conn.createStatement().use { st ->
                st.executeQuery(
                    "SELECT ct.name FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId " +
                        "WHERE l.sourceBookId = $src AND l.targetBookId = $tgt",
                ).use { rs -> return if (rs.next()) rs.getString(1) else "MISSING" }
            }
        }

        assertEquals("MIDRASH", typeOf(1, 2), "מדרש>הלכה book must be tagged מדרש and keep its Tanakh MIDRASH link")
        assertEquals("RELATED", typeOf(1, 3), "a real הלכה-root target must still be demoted")
    }
}
