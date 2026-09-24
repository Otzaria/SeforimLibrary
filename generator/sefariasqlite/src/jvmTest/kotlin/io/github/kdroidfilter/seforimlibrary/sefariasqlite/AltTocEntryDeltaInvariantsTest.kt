package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.common.patch.PatchDbProducer
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Delta-mechanism invariants for allocator-issued `alt_toc_entry` ids: the
 * natural keys must reach the build_state file, a rebuild must reuse them
 * without allocating anything fresh, adding a book must not renumber the books
 * already there, and the resulting DB must stay referentially intact.
 */
class AltTocEntryDeltaInvariantsTest {

    @JvmField
    @Rule
    val tmp = TemporaryFolder()

    private data class Built(
        val entriesByBook: Map<String, List<List<Any?>>>,
        val reused: Long,
        val fresh: Long,
        val stateKeyCount: Int,
        val entryCount: Int,
    )

    /** One alt structure per book: a titled container over [chapters] ref-bearing nodes. */
    private fun payloadOf(title: String, chapters: List<Int>, ghostFirst: Boolean = false): BookPayload {
        val refEntries = chapters.flatMap { chapter ->
            val head = chapter * 3
            listOf(
                RefEntry(ref = "$title $chapter", heRef = "$title $chapter", path = title, lineIndex = head + 1),
                RefEntry(ref = "$title $chapter:1", heRef = "$title $chapter:1", path = title, lineIndex = head + 2),
                RefEntry(ref = "$title $chapter:2", heRef = "$title $chapter:2", path = title, lineIndex = head + 3),
            )
        }
        val chapterNodes = chapters.map { chapter ->
            AltNodePayload(
                title = "Chapter $chapter", heTitle = "Chapter $chapter",
                wholeRef = "$title $chapter",
                refs = listOf("$title $chapter:1", "$title $chapter:2"),
                addressTypes = listOf("Siman"), childLabel = null, addresses = emptyList(),
                skippedAddresses = emptyList(), startingAddress = null, offset = null,
                children = emptyList(),
            )
        }
        val root = AltNodePayload(
            title = "Part One", heTitle = "Part One", wholeRef = null, refs = emptyList(),
            addressTypes = listOf("Siman"), childLabel = null, addresses = emptyList(),
            skippedAddresses = emptyList(), startingAddress = null, offset = null,
            children = chapterNodes,
        )
        return BookPayload(
            heTitle = title, enTitle = title, categoriesHe = listOf("Test"),
            lines = (0 until TOTAL_LINES).map { "line $it" },
            refEntries = refEntries, headings = emptyList(), authors = emptyList(),
            description = null, heShortDesc = null, pubDates = emptyList(),
            altStructures = listOf(
                AltStructurePayload(
                    key = "Topic", title = "Topic", heTitle = "Topic",
                    nodes = if (ghostFirst) listOf(ghostContainer(), root) else listOf(root),
                ),
            ),
        )
    }

    /** A titled container whose only child resolves to nothing — inserted, then deleted. */
    private fun ghostContainer() = AltNodePayload(
        title = "Ghost", heTitle = "Ghost", wholeRef = null, refs = emptyList(),
        addressTypes = listOf("Siman"), childLabel = null, addresses = emptyList(),
        skippedAddresses = emptyList(), startingAddress = null, offset = null,
        children = listOf(
            AltNodePayload(
                title = "Missing", heTitle = "Missing", wholeRef = "Nowhere 99", refs = emptyList(),
                addressTypes = listOf("Siman"), childLabel = null, addresses = emptyList(),
                skippedAddresses = emptyList(), startingAddress = null, offset = null,
                children = emptyList(),
            ),
        ),
    )

    /** Builds a file-backed DB holding [books], snapshotting the allocator to [statePath]. */
    private fun build(
        statePath: Path,
        dbPath: Path,
        books: List<Pair<String, List<Int>>>,
        ghostFirst: Boolean = false,
    ): Built = runBlocking {
        if (Files.exists(dbPath)) Files.delete(dbPath)
        val driver = JdbcSqliteDriver("jdbc:sqlite:${dbPath.toAbsolutePath()}")
        SeforimDb.Schema.create(driver)
        val repository = SeforimRepository(dbPath.toString(), driver)
        val allocator = InMemoryIdAllocator.load(statePath.takeIf { Files.exists(it) })
        val bindings = IdAllocatorBindings(allocator, repository)
        val perBook = LinkedHashMap<String, List<List<Any?>>>()
        try {
            val categoryId = bindings.upsertCategory("Test", null, "Test", level = 0, orderIndex = 0)
            val sourceId = bindings.upsertSource("Sefaria")
            for ((title, chapters) in books) {
                val bookId = bindings.insertBookStable(
                    sourceName = "Sefaria", canonicalHeTitle = title,
                    book = Book(
                        id = 0, categoryId = categoryId, sourceId = sourceId, title = title,
                        heShortDesc = null, notesContent = null, order = 0f,
                        totalLines = TOTAL_LINES, isBaseBook = true, hasAltStructures = true,
                    ),
                )
                val lineKeyToId = (0 until TOTAL_LINES).associate { idx ->
                    (title to idx) to bindings.insertLineStable(
                        Line(id = 0, bookId = bookId, lineIndex = idx, content = "line $idx", heRef = null),
                    )
                }
                val generated = SefariaAltTocBuilder(repository, bindings).buildAltTocStructuresForBook(
                    payload = payloadOf(title, chapters, ghostFirst), bookId = bookId, bookPath = title,
                    lineKeyToId = lineKeyToId, totalLines = TOTAL_LINES,
                )
                assertTrue(generated, "fixture book $title must produce alt structures")
                val structureId = repository.getAltTocStructuresForBook(bookId).single().id
                perBook[title] = repository.getAltTocEntriesForStructure(structureId)
                    .sortedBy { it.id }
                    .map { listOf(it.id, it.parentId, it.level, it.lineId, it.text) }
            }
            val stats = allocator.stats().perTable.getValue(IdTable.ALT_TOC_ENTRY)
            allocator.snapshotTo(statePath, extraMeta = mapOf("generator" to "test"))
            Built(
                entriesByBook = perBook,
                reused = stats.reused,
                fresh = stats.freshlyAllocated,
                stateKeyCount = countRows(statePath, "id_alt_toc_entry"),
                entryCount = countRows(dbPath, "alt_toc_entry"),
            )
        } finally {
            repository.close()
            driver.close()
        }
    }

    private fun countRows(db: Path, table: String): Int {
        Class.forName("org.sqlite.JDBC")
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM $table").use { rs -> rs.next(); return rs.getInt(1) }
            }
        }
    }

    /** `PRAGMA foreign_key_check` rows, as "table:rowid:parent" strings. */
    private fun foreignKeyViolations(db: Path): List<String> {
        Class.forName("org.sqlite.JDBC")
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("PRAGMA foreign_key_check").use { rs ->
                    val out = ArrayList<String>()
                    while (rs.next()) out += "${rs.getString(1)}:${rs.getString(2)}:${rs.getString(3)}"
                    return out
                }
            }
        }
    }

    @Test
    fun `entry keys reach the build state and a rebuild reuses every one of them`() {
        val dir = tmp.newFolder().toPath()
        val state = dir.resolve("build_state.db")
        val dbA = dir.resolve("a.db")
        val dbB = dir.resolve("b.db")

        val first = build(state, dbA, listOf("Alpha" to listOf(1, 2, 3), "Beta" to listOf(1, 2)))
        assertEquals(0L, first.reused, "a first build has nothing to reuse")
        assertTrue(first.fresh > 0, "the first build must allocate entry ids")
        assertEquals(first.entryCount, first.stateKeyCount, "every live entry needs a persisted natural key")
        assertEquals(first.fresh, first.entryCount.toLong())

        val second = build(state, dbB, listOf("Alpha" to listOf(1, 2, 3), "Beta" to listOf(1, 2)))
        assertEquals(0L, second.fresh, "an unchanged rebuild must allocate no fresh entry id")
        assertEquals(first.entryCount.toLong(), second.reused, "every entry id must come from the build state")
        assertEquals(first.entriesByBook, second.entriesByBook)
        assertEquals(first.stateKeyCount, second.stateKeyCount, "a stable rebuild must not grow the key table")
    }

    @Test
    fun `adding a book leaves the existing books' entry ids and patch rows untouched`() {
        val dir = tmp.newFolder().toPath()
        val state = dir.resolve("build_state.db")
        val dbA = dir.resolve("a.db")
        val dbB = dir.resolve("b.db")

        val first = build(state, dbA, listOf("Alpha" to listOf(1, 2, 3), "Beta" to listOf(1, 2)))
        val second = build(state, dbB, listOf("Alpha" to listOf(1, 2, 3), "Beta" to listOf(1, 2), "Gamma" to listOf(1)))

        assertEquals(first.entriesByBook["Alpha"], second.entriesByBook["Alpha"])
        assertEquals(first.entriesByBook["Beta"], second.entriesByBook["Beta"])

        val patch = PatchDbProducer().produce(
            prevDb = dbA, newDb = dbB, outputPath = dir.resolve("patch.db"),
            fromVersion = 1, toVersion = 2,
        )
        val addedEntries = second.entryCount - first.entryCount
        assertEquals(addedEntries, patch.upsertCounts["alt_toc_entry"], "only the new book's entries may be patched")
        assertEquals(0, patch.deleteCounts["alt_toc_entry"])
        assertEquals(0, patch.deleteCounts["line_alt_toc"])
        // line_alt_toc is keyed on (lineId, structureId): only the new book's rows move.
        val addedMappings = countRows(dbB, "line_alt_toc") - countRows(dbA, "line_alt_toc")
        assertEquals(addedMappings, patch.upsertCounts["line_alt_toc"])
    }

    @Test
    fun `the built db is referentially intact and the check would notice if it were not`() {
        val dir = tmp.newFolder().toPath()
        val state = dir.resolve("build_state.db")
        val db = dir.resolve("a.db")
        build(state, db, listOf("Alpha" to listOf(1, 2, 3), "Beta" to listOf(1, 2)))

        assertEquals(emptyList(), foreignKeyViolations(db))
        BuildStateVerifier.verifyFreshSnapshot(state, db, mapOf("generator" to "test"))

        // Positive control: the check is only meaningful if it reports a planted orphan.
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate(
                    "UPDATE line_alt_toc SET altTocEntryId = 999999 " +
                        "WHERE rowid = (SELECT MIN(rowid) FROM line_alt_toc)",
                )
            }
        }
        assertTrue(foreignKeyViolations(db).any { it.startsWith("line_alt_toc:") })
    }

    /**
     * A container whose children all fail to resolve is inserted and then deleted.
     * Its ordinal stays consumed, so the surviving sibling keeps the same id on
     * every build. Deriving the ordinal from `entriesByParent` instead of the
     * dedicated counter breaks this: the delete removes the container from that
     * list, so the sibling would claim the container's natural key.
     */
    @Test
    fun `a container dropped for having no children still consumes its ordinal`() {
        val dir = tmp.newFolder().toPath()
        val state = dir.resolve("build_state.db")
        val plainState = dir.resolve("plain_state.db")

        val first = build(state, dir.resolve("a.db"), listOf("Alpha" to listOf(1, 2, 3)), ghostFirst = true)
        val second = build(state, dir.resolve("b.db"), listOf("Alpha" to listOf(1, 2, 3)), ghostFirst = true)
        assertEquals(first.entriesByBook, second.entriesByBook, "the dropped container must not destabilise its siblings")
        assertEquals(first.entryCount + 1, first.stateKeyCount, "the dropped container keeps its retired key")

        // Without the ghost the same rows exist, one id lower — proof the ordinal was consumed.
        val plain = build(plainState, dir.resolve("c.db"), listOf("Alpha" to listOf(1, 2, 3)))
        assertEquals(plain.entryCount, first.entryCount)
        assertEquals(plain.stateKeyCount + 1, first.stateKeyCount)
    }

    private companion object {
        const val TOTAL_LINES = 30
    }
}
