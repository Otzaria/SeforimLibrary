package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/** Torat HaBayit HaKatzar ↔ HaAroch, gate by gate (Otzaria/otzaria#4890). */
class SefariaParallelSectionLinksTest {
    private val aroch = "Torat HaBayit HaAroch"
    private val katzar = "Torat HaBayit HaKatzar"

    // Addresses as Sefaria spells them; the women's house is named differently in each book.
    private val arochAddresses = listOf(
        "Introduction,  1",
        "Introduction,  2",
        "The First House, The First Gate,  1",
        "The First House, The First Gate,  2",
        "The First House, The First Gate,  3",
        "The First House, The Second Gate,  1",
        "The Seventh House, The Women's House, Introduction,  1",
        "The Seventh House, The Women's House, The First Gate,  1",
        "The Seventh House, The Women's House, The First Gate,  2",
    )
    private val katzarAddresses = listOf(
        "The First House, The First Gate,  1",
        "The First House, The Second Gate,  1",
        "The First House, The Second Gate,  2",
        "The First House, The Third Gate,  1",
        "The Seventh House, The First Gate,  1",
    )

    private fun refs(title: String, addresses: List<String>) =
        addresses.mapIndexed { i, address -> RefEntry("$title, $address", "", title, i + 1) }

    @Test
    fun pairsTheGatesBothBooksHave() {
        val links = parallelSectionLinks(
            PARALLEL_SECTION_BOOKS.single(),
            listOf(refs(aroch, arochAddresses), refs(katzar, katzarAddresses)).associateBy { it.first().path },
        )
        assertEquals(
            listOf(
                listOf(3, 5, 1, 1),
                listOf(6, 6, 2, 3),
                listOf(8, 9, 5, 5),
            ),
            links.map { link ->
                listOf(link.source, link.target).flatMap { listOf(it.first.lineIndex, it.last.lineIndex) }
            },
        )
    }

    @Test
    fun storesOneRangedRelatedLinkPerGate() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(Category(0, null, "ספרים", level = 0, order = 1))
        val lineKeyToId = HashMap<Pair<String, Int>, Long>()
        val lineIdToBookId = HashMap<Long, Long>()
        val refsByPath = HashMap<String, List<RefEntry>>()
        listOf(1L to (aroch to arochAddresses), 2L to (katzar to katzarAddresses)).forEach { (bookId, book) ->
            val (title, addresses) = book
            repo.insertBook(
                Book(
                    id = bookId, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
                    authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(), heShortDesc = null,
                    notesContent = null, order = bookId.toFloat(), topics = emptyList(), isBaseBook = false,
                    totalLines = addresses.size, hasAltStructures = false, hasTeamim = false, hasNekudot = false,
                )
            )
            repo.insertLinesBatch(
                addresses.indices.map { i ->
                    val lineId = bookId * 1000 + i
                    lineKeyToId[title to i] = lineId
                    lineIdToBookId[lineId] = bookId
                    Line(id = lineId, bookId = bookId, lineIndex = i, content = "$i", heRef = "$i")
                }
            )
            refsByPath[title] = refs(title, addresses)
        }
        val importer = SefariaLinksImporter(
            repo, IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo), Logger.withTag("parallel"),
        )
        val sections = parallelSectionLinks(PARALLEL_SECTION_BOOKS.single(), refsByPath)
        // The Aroch's third segment is a heading: it keeps no coverage row.
        val inserted = importer.insertParallelSectionLinks(sections, lineKeyToId, lineIdToBookId, setOf(1004L), emptyMap())
        assertEquals(3, inserted)

        fun query(sql: String): List<List<Long>> = driver.getConnection().createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                buildList { while (rs.next()) add((1..rs.metaData.columnCount).map { rs.getLong(it) }) }
            }
        }
        assertEquals(
            listOf(listOf(1002L, 2000L), listOf(1005L, 2001L), listOf(1007L, 2004L)),
            query(
                """
                SELECT l.sourceLineId, l.targetLineId FROM link l JOIN connection_type t ON t.id = l.connectionTypeId
                WHERE t.name = 'RELATED' ORDER BY l.sourceLineId
                """.trimIndent(),
            ),
        )
        assertEquals(
            listOf(listOf(1002L, 0L, 1004L), listOf(1005L, 1L, 2002L), listOf(1007L, 0L, 1008L)),
            query(
                "SELECT l.sourceLineId, r.side, r.endLineId FROM link_range r JOIN link l ON l.id = r.linkId ORDER BY l.sourceLineId",
            ),
        )
        assertEquals(
            listOf(listOf(1003L, 0L), listOf(1008L, 0L), listOf(2002L, 1L)),
            query("SELECT lineId, side FROM link_coverage ORDER BY lineId"),
        )
        assertEquals(3L, importer.metricsSnapshot().insertedByType.getValue("RELATED").written)
        // A second run finds every link already stored.
        assertEquals(0, importer.insertParallelSectionLinks(sections, lineKeyToId, lineIdToBookId, emptySet(), emptyMap()))
        repo.close()
    }
}
