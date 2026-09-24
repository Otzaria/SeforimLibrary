package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateReader
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocEntry
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `alt_toc_entry` ids of the Sefaria builder must survive a rebuild, and within a
 * sibling group they must stay ascending in content order — the shipped reader
 * renders alt-TOC children with `ORDER BY e.id`
 * (otzaria `lib/data/data_providers/database_library_provider.dart`).
 *
 * These drive the real [SefariaAltTocBuilder.buildAltTocStructuresForBook], so
 * reverting any of its six insert sites to `repository.insertAltTocEntry` fails
 * them: a rowid-issued id never reaches `id_alt_toc_entry`.
 */
class SefariaAltTocEntryIdStabilityTest {

    @JvmField
    @Rule
    val tmp = TemporaryFolder()

    private data class Built(
        val entries: List<AltTocEntry>,
        val structureId: Long,
    )

    /** One book, one alt structure: a titled container over [chapters] ref-bearing nodes. */
    private fun payloadWith(chapters: List<Int>, dafRefs: List<Int> = emptyList()): BookPayload {
        val dafRefEntries = dafRefs.map { daf ->
            RefEntry(ref = "Alpha daf $daf", heRef = "Alpha daf $daf", path = BOOK_PATH, lineIndex = 20 + daf)
        }
        val refEntries = dafRefEntries + chapters.flatMap { chapter ->
            val head = chapter * 3
            listOf(
                RefEntry(ref = "Alpha $chapter", heRef = "Alpha $chapter", path = BOOK_PATH, lineIndex = head + 1),
                RefEntry(ref = "Alpha $chapter:1", heRef = "Alpha $chapter:1", path = BOOK_PATH, lineIndex = head + 2),
                RefEntry(ref = "Alpha $chapter:2", heRef = "Alpha $chapter:2", path = BOOK_PATH, lineIndex = head + 3),
            )
        }
        val chapterNodes = chapters.map { chapter ->
            AltNodePayload(
                title = "Chapter $chapter",
                heTitle = "Chapter $chapter",
                wholeRef = "Alpha $chapter",
                refs = listOf("Alpha $chapter:1", "Alpha $chapter:2"),
                addressTypes = listOf("Siman"),
                childLabel = null,
                addresses = emptyList(),
                skippedAddresses = emptyList(),
                startingAddress = null,
                offset = null,
                children = emptyList(),
            )
        }
        // Titleless Talmud node: its refs become entries at the root's own parent,
        // which is the only writer reaching the inline-children insert site.
        val dafNode = AltNodePayload(
            title = null,
            heTitle = null,
            wholeRef = null,
            refs = dafRefs.map { "Alpha daf $it" },
            addressTypes = listOf("Talmud"),
            childLabel = null,
            addresses = emptyList(),
            skippedAddresses = emptyList(),
            startingAddress = null,
            offset = null,
            children = emptyList(),
        )
        val root = AltNodePayload(
            title = "Part One",
            heTitle = "Part One",
            wholeRef = null,
            refs = emptyList(),
            addressTypes = listOf("Siman"),
            childLabel = null,
            addresses = emptyList(),
            skippedAddresses = emptyList(),
            startingAddress = null,
            offset = null,
            children = chapterNodes,
        )
        return BookPayload(
            heTitle = "Alpha",
            enTitle = "Alpha",
            categoriesHe = listOf("Test"),
            lines = (0 until TOTAL_LINES).map { "line $it" },
            refEntries = refEntries,
            headings = emptyList(),
            authors = emptyList(),
            description = null,
            heShortDesc = null,
            pubDates = emptyList(),
            altStructures = listOf(
                AltStructurePayload(
                    key = "Topic", title = "Topic", heTitle = "Topic",
                    nodes = if (dafRefs.isEmpty()) listOf(root) else listOf(root, dafNode),
                ),
            ),
        )
    }

    /** Runs the production builder over a fresh DB with the build state at [statePath]. */
    private fun build(statePath: Path, chapters: List<Int>, dafRefs: List<Int> = emptyList()): Built = runBlocking {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SeforimDb.Schema.create(driver)
        val repository = SeforimRepository(":memory:", driver)
        try {
            val categoryId = repository.insertCategory(Category(0, null, "Test", level = 0, order = 0))
            val sourceId = repository.insertSource("Sefaria")
            val bookId = repository.insertBook(
                Book(
                    id = 0, categoryId = categoryId, sourceId = sourceId, title = "Alpha",
                    heShortDesc = null, notesContent = null, order = 0f,
                    totalLines = TOTAL_LINES, isBaseBook = true, hasAltStructures = true,
                ),
            )
            val lineKeyToId = (0 until TOTAL_LINES).associate { idx ->
                (BOOK_PATH to idx) to repository.insertLine(
                    Line(id = 0, bookId = bookId, lineIndex = idx, content = "line $idx", heRef = null),
                )
            }

            val allocator = InMemoryIdAllocator.load(statePath.takeIf { java.nio.file.Files.exists(it) })
            // The Sefaria importer registers every book through the allocator; without
            // it the snapshot GC drops this structure's keys as orphans.
            allocator.bookId("Sefaria", "Alpha")
            val bindings = IdAllocatorBindings(allocator, repository)
            val generated = SefariaAltTocBuilder(repository, bindings).buildAltTocStructuresForBook(
                payload = payloadWith(chapters, dafRefs),
                bookId = bookId,
                bookPath = BOOK_PATH,
                lineKeyToId = lineKeyToId,
                totalLines = TOTAL_LINES,
            )
            assertTrue(generated, "the fixture must produce alt structures")
            allocator.snapshotTo(statePath)

            val structureId = repository.getAltTocStructuresForBook(bookId).single().id
            Built(repository.getAltTocEntriesForStructure(structureId).sortedBy { it.id }, structureId)
        } finally {
            repository.close()
        }
    }

    @Test
    fun `an unchanged payload reproduces every entry id from the build state`() {
        val state = tmp.newFolder().toPath().resolve("build_state.db")

        val first = build(state, chapters = listOf(1, 2, 3))
        // container + 3 chapters + 2 ref children each.
        assertEquals(10, first.entries.size)
        val firstVector = first.entries.map { listOf(it.id, it.parentId, it.level.toLong(), it.lineId, it.textId) }

        val second = build(state, chapters = listOf(1, 2, 3))
        val secondVector = second.entries.map { listOf(it.id, it.parentId, it.level.toLong(), it.lineId, it.textId) }

        assertEquals(firstVector, secondVector, "alt_toc_entry ids must be reproduced from the build state")
        assertEquals(first.structureId, second.structureId)

        // Every live entry is backed by a natural key; a rowid-issued id would not be.
        val snapshot = BuildStateReader().read(state)
        val keysOfStructure = snapshot.altTocEntries.filterKeys { it.structureId == second.structureId }
        assertEquals(second.entries.size, keysOfStructure.size)
        assertEquals(second.entries.map { it.id }.toSet(), keysOfStructure.values.toSet())
    }

    @Test
    fun `a node inserted mid-list keeps sibling ids ascending in content order`() {
        val state = tmp.newFolder().toPath().resolve("build_state.db")

        val first = build(state, chapters = listOf(1, 3))
        assertEquals(7, first.entries.size)

        val second = build(state, chapters = listOf(1, 2, 3))
        assertEquals(10, second.entries.size)

        // lineId ascends with content here, so "ids ascend with content order" is
        // exactly "within each sibling group, sorting by id sorts by lineId".
        for ((parentId, siblings) in second.entries.groupBy { it.parentId }) {
            val byId = siblings.sortedBy { it.id }.mapNotNull { it.lineId }
            assertEquals(byId.sorted(), byId, "sibling group $parentId is out of content order")
        }

        // The inserted chapter takes the id its successor used to hold; the successor
        // moves on. A content-identity key would instead give chapter 2 a fresh high id.
        val chapterTwo = second.entries.single { it.text == "Chapter 2" }
        val chapterThree = second.entries.single { it.text == "Chapter 3" }
        assertTrue(chapterTwo.id < chapterThree.id)
        assertEquals(first.entries.single { it.text == "Chapter 3" }.id, chapterTwo.id)
    }

    /**
     * The inline-children branch (a titleless Talmud node whose refs become
     * entries directly) is the one insert site no other test reaches. Reverting
     * it to `repository.insertAltTocEntry` fails this: its rows carry rowids and
     * never reach `id_alt_toc_entry`.
     */
    @Test
    fun `inline daf children are allocator-issued and reproduced from the build state`() {
        val state = tmp.newFolder().toPath().resolve("build_state.db")

        val first = build(state, chapters = listOf(1), dafRefs = listOf(1, 2))
        val firstDaf = first.entries.filter { it.lineId != null && it.parentId == null }
        assertEquals(2, firstDaf.size, "the titleless Talmud node must emit its refs as root entries")

        val second = build(state, chapters = listOf(1), dafRefs = listOf(1, 2))
        assertEquals(first.entries.map { it.id }, second.entries.map { it.id })

        // Every entry, inline daf children included, is backed by a natural key.
        val snapshot = BuildStateReader().read(state)
        val keys = snapshot.altTocEntries.filterKeys { it.structureId == second.structureId }
        assertEquals(second.entries.size, keys.size)
        assertEquals(second.entries.map { it.id }.toSet(), keys.values.toSet())
    }

    private companion object {
        const val BOOK_PATH = "Alpha"
        const val TOTAL_LINES = 30
    }
}
