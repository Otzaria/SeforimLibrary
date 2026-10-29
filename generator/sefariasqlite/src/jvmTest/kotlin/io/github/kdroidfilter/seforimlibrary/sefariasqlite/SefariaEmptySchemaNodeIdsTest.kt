package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.LegacyLineKey
import io.github.kdroidfilter.seforimlibrary.common.ids.LineOccurrenceCounter
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Rebuilds seeded from the pre-pruning layout must retain every surviving line and TOC id. */
class SefariaEmptySchemaNodeIdsTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()
    private val reader = SefariaBookPayloadReader(Json, Logger.withTag("EmptySchemaNodeIds"))

    private fun leaf(title: String, he: String) = """
        {"key":"$title","title":"$title","heTitle":"$he","depth":2,
         "sectionNames":["Chapter","Paragraph"],"heSectionNames":["פרק","פסקה"],
         "addressTypes":["Integer","Integer"]}
    """

    private fun parent(title: String, he: String, vararg children: String) = """
        {"key":"$title","title":"$title","heTitle":"$he","nodes":[${children.joinToString(",")}]}
    """

    private val schema = parent(
        "Book", "ספר",
        parent("A", "A", leaf("Introduction", "הקדמה"), leaf("Body", "גוף")),
        parent("B", "B", leaf("Introduction", "הקדמה")),
    )
    private val text = """{"A":{"Introduction":[],"Body":[["a"]]},"B":{"Introduction":[["b"]]}}"""
    private val oldLines = listOf(
        "<h1>ספר</h1>", "<h2>A</h2>", "<h3>הקדמה</h3>", "<h3>גוף</h3>", "<h4>פרק א</h4>", "a",
        "<h2>B</h2>", "<h3>הקדמה</h3>", "<h4>פרק א</h4>", "b",
    )

    // Exercise parseBookFile's metadata wiring, not only the standalone walker.
    private fun read(schema: String = this.schema, text: String = this.text): BookPayload {
        val root = tmp.newFolder().toPath()
        val schemas = Files.createDirectory(root.resolve("schemas"))
        Files.writeString(
            root.resolve("table_of_contents.json"),
            """[{"category":"Test","heCategory":"Test","contents":[{"title":"Book","heTitle":"ספר"}]}]""",
        )
        Files.writeString(schemas.resolve("Book.json"), """{"schema":$schema,"heCategories":["Test"],"categories":["Test"]}""")
        val merged = Files.createDirectories(root.resolve("json/Book")).resolve("merged.json")
        Files.writeString(merged, """{"title":"Book","heTitle":"ספר","text":$text}""")
        return requireNotNull(reader.parseBookFile(merged, schemas, reader.buildSchemaLookup(schemas)))
    }

    private fun allocate(allocator: InMemoryIdAllocator, payload: BookPayload, legacyOnly: Boolean = false): List<Long> {
        payload.precomputeLineData()
        val pre = requireNotNull(payload.precomputed)
        val bookId = allocator.bookId("Sefaria", "ספר")
        val occurrences = LineOccurrenceCounter()
        val legacyOccurrences = LineOccurrenceCounter()
        return payload.lines.indices.map { index ->
            pre.reserveOmittedLineOccurrences(bookId, index, occurrences, legacyOccurrences)
            val hash = pre.lineKeyHashes!![index]
            val legacyHash = pre.legacyLineKeyHashes!![index]
            val occurrence = occurrences.next(bookId, hash)
            val legacyOccurrence = legacyOccurrences.next(bookId, legacyHash)
            if (legacyOnly) allocator.lineId(bookId, legacyHash, legacyOccurrence)
            else allocator.lineId(bookId, hash, occurrence, LegacyLineKey(legacyHash, legacyOccurrence))
        }
    }

    private fun beforePruning(after: BookPayload, lines: List<String>, removed: Set<Int>): BookPayload {
        val positions = lines.indices.filterNot { it in removed }
        assertEquals(positions.map { lines[it] }, after.lines)
        val heading = Regex("<h([1-5])>(.*?)</h[1-5]>")
        return after.copy(
            lines = lines,
            refEntries = after.refEntries.map { it.copy(lineIndex = positions[it.lineIndex - 1] + 1) },
            headings = lines.mapIndexedNotNull { index, line ->
                heading.matchEntire(line)?.let { Heading(it.groupValues[2], it.groupValues[1].toInt() - 1, index) }
            },
            cleanShiftByLineIndex = after.cleanShiftByLineIndex.mapKeys { positions[it.key] },
            lineKeyHashOverrides = after.lineKeyHashOverrides.mapKeys { positions[it.key] },
            omittedLinesBefore = emptyMap(),
        )
    }

    private fun assertStable(after: BookPayload, lines: List<String>, removed: Set<Int>, legacyOnly: Boolean = false) {
        val state = tmp.newFolder().toPath().resolve("build_state.db")
        val old = beforePruning(after, lines, removed)
        val first = InMemoryIdAllocator.load(null)
        val oldIds = allocate(first, old, legacyOnly)
        val oldToc = buildToc(old, first, oldIds)
        first.snapshotTo(state)

        val second = InMemoryIdAllocator.load(state)
        val newIds = allocate(second, after)
        assertEquals(oldIds.filterIndexed { index, _ -> index !in removed }, newIds)
        for (index in removed) assertFalse(oldIds[index] in newIds, "a removed heading's id must not move to a live line")
        val newToc = buildToc(after, second, newIds)
        assertEquals(oldToc.filterKeys { it in newIds }, newToc, "surviving TOC ids and parents must be stable")
        second.snapshotTo(state)
        assertEquals(newIds, allocate(InMemoryIdAllocator.load(state), after), "the next rebuild must remain stable")
    }

    private fun buildToc(payload: BookPayload, allocator: InMemoryIdAllocator, ids: List<Long>): Map<Long?, Pair<Long, Long?>> = runBlocking {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SeforimDb.Schema.create(driver)
        val repository = SeforimRepository(":memory:", driver)
        try {
            val categoryId = repository.insertCategory(Category(0, null, "Test", level = 0, order = 0))
            val sourceId = repository.insertSource("Sefaria")
            val bookId = allocator.bookId("Sefaria", "ספר")
            repository.insertBook(Book(bookId, categoryId, sourceId, "ספר", totalLines = payload.lines.size))
            repository.insertLinesBatch(payload.lines.mapIndexed { index, content -> Line(ids[index], bookId, index, content) })
            SefariaTocInserter(repository, IdAllocatorBindings(allocator, repository)).insertTocEntriesOptimized(
                payload, bookId, "book", categoryId, "ספר",
                ids.mapIndexed { index, id -> ("book" to index) to id }.toMap(), mutableListOf(),
            )
            repository.getTocEntriesForBook(bookId).associate { it.lineId to (it.id to it.parentId) }
        } finally {
            repository.close()
        }
    }

    @Test fun repeatedHeadingKeepsItsIdAcrossSeededRebuilds() {
        assertStable(read(), oldLines, setOf(2))
    }

    @Test fun legacySeedAlsoRetainsTheSecondHeadingId() {
        assertStable(read(), oldLines, setOf(2), legacyOnly = true)
    }

    @Test fun productionImporterPreservesSeededIdsAndStoresOnlyCompactedLines() = runBlocking {
        val after = read()
        val first = InMemoryIdAllocator.load(null)
        val old = beforePruning(after, oldLines, setOf(2))
        val oldIds = allocate(first, old)
        val oldToc = buildToc(old, first, oldIds)
        val state = tmp.newFolder().toPath().resolve("build_state.db")
        first.snapshotTo(state)
        val second = InMemoryIdAllocator.load(state)
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SeforimDb.Schema.create(driver)
        val repository = SeforimRepository(":memory:", driver)
        try {
            val root = Path.of(requireNotNull(after.sourceDirPath)).parent.parent
            SefariaDirectImporter(root, repository, second).import()
            val bookId = requireNotNull(second.peekBookId("Sefaria", "ספר"))
            val lines = repository.getLines(bookId, 0, after.lines.size)
            assertEquals(after.lines, lines.map { it.content })
            assertEquals(oldIds.filterIndexed { index, _ -> index != 2 }, lines.map { it.id })
            assertEquals(
                oldToc.filterKeys { it != oldIds[2] },
                repository.getTocEntriesForBook(bookId).associate { it.lineId to (it.id to it.parentId) },
            )
        } finally {
            repository.close()
        }
    }

    @Test fun anOmissionBetweenIdenticalLiveHeadingsOnlyReservesItsOwnPosition() {
        val schema = parent("Book", "ספר", leaf("A", "הקדמה"), leaf("B", "הקדמה"), leaf("C", "הקדמה"))
        val after = read(schema, """{"A":[["a"]],"B":[],"C":[["c"]]}""")
        val old = listOf(
            "<h1>ספר</h1>", "<h2>הקדמה</h2>", "<h3>פרק א</h3>", "a", "<h2>הקדמה</h2>",
            "<h2>הקדמה</h2>", "<h3>פרק א</h3>", "c",
        )
        assertStable(after, old, setOf(4))
    }

    @Test fun emptyParentsReserveTheDescendantsTheOldWalkerActuallyVisited() {
        val intro = leaf("Introduction", "הקדמה")
        val inner = parent("Inner", "פנימי", intro)
        val schema = parent("Book", "ספר", parent("A", "חלק", intro, inner), parent("B", "חלק", intro, inner))
        val live = """"B":{"Introduction":[["a"]],"Inner":{"Introduction":[["b"]]}}"""
        val cases = listOf(
            "" to listOf("<h2>חלק</h2>"),
            "\"A\":null," to listOf("<h2>חלק</h2>", "<h3>הקדמה</h3>", "<h3>פנימי</h3>"),
            "\"A\":{}," to listOf("<h2>חלק</h2>", "<h3>הקדמה</h3>", "<h3>פנימי</h3>"),
            "\"A\":{\"Introduction\":[],\"Inner\":{\"Introduction\":[]}}," to
                listOf("<h2>חלק</h2>", "<h3>הקדמה</h3>", "<h3>פנימי</h3>", "<h4>הקדמה</h4>"),
        )
        for ((empty, removed) in cases) {
            val after = read(schema, "{$empty$live}")
            val old = after.lines.take(1) + removed + after.lines.drop(1)
            assertStable(after, old, (1..removed.size).toSet())
        }
    }

    @Test fun compactionRemapsRefsCleaningOverridesAndAlternativeVersions() {
        val text = """{"A":{"Introduction":[],"Body":[[],["@04טקסט} שנוקה","a<br>b"]]},"B":{"Introduction":[["b"]]}}"""
        val after = read(text = text)
        val version = reader.walkTextWithSchema(
            Json.parseToJsonElement(schema).jsonObject, Json.parseToJsonElement(text), "ספר", "Book",
            collectLineKeyOverrides = false,
        )
        assertEquals(after.lines, version.lines)
        assertEquals(after.refEntries, version.refs)
        assertEquals(after.headings, version.headings)
        assertTrue(version.omittedLinesBefore.isEmpty())
        assertEquals(listOf("טקסט שנוקה", "a<br>b", "b"), after.refEntries.map { after.lines[it.lineIndex - 1] })
        assertEquals(setOf(after.lines.indexOf("טקסט שנוקה")), after.cleanShiftByLineIndex.keys)
        assertEquals(setOf(after.lines.indexOf("a<br>b")), after.lineKeyHashOverrides.keys)
        val old = after.lines.toMutableList().apply { add(2, "<h3>הקדמה</h3>") }
        assertStable(after, old, setOf(2))
    }

    @Test fun unchangedBooksKeepTheFastPathAndOriginalPositions() {
        val after = read(text = """{"A":{"Introduction":[["intro"]],"Body":[["a"]]},"B":{"Introduction":[["b"]]}}""")
        assertTrue(after.omittedLinesBefore.isEmpty())
        assertTrue(after.headings.all { it.keyLineIndex == it.lineIndex })
    }
}
