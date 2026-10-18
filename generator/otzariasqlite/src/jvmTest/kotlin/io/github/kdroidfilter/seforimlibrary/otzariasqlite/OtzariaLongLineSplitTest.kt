package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Long Otzaria lines are split in phase 1 (issue #1941); phase 2 runs in a fresh
 * instance, as in production, and must still land every txt-numbered link,
 * anchor, range and alt-toc node on the right stored line.
 */
class OtzariaLongLineSplitTest {

    private val split = LongLineSplit.Config(minLineChars = 50, targetChars = 20, maxChars = 30, minTailChars = 10)
    private val noSplit = LongLineSplit.Config(minLineChars = Int.MAX_VALUE)

    private fun words(from: Int, to: Int) = (from..to).joinToString(" ") { "w%02da".format(it) }

    // Stored as three pieces of 20 / 20 / 29 visible chars.
    private val long = words(1, 14)

    private val dirs = mutableListOf<Path>()

    @AfterTest
    fun cleanup() = dirs.forEach { it.toFile().deleteRecursively() }

    private class Db(val repo: SeforimRepository, val driver: JdbcSqliteDriver) {
        fun query(sql: String): List<List<Long>> = driver.getConnection().createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                val out = mutableListOf<List<Long>>()
                while (rs.next()) out += (1..rs.metaData.columnCount).map { rs.getLong(it) }
                out
            }
        }
    }

    private fun newDb(): Db {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        return Db(SeforimRepository(":memory:", driver), driver)
    }

    private fun library(
        books: Map<String, List<String>>,
        links: Map<String, String> = emptyMap(),
        altToc: Map<String, String> = emptyMap(),
    ): Path {
        val sourceDir = Files.createTempDirectory("otzaria-long-lines").also { dirs.add(it) }
        val bookDir = Files.createDirectories(sourceDir.resolve("אוצריא").resolve("Shelf"))
        books.forEach { (title, lines) -> Files.writeString(bookDir.resolve("$title.txt"), lines.joinToString("\n")) }
        val linksDir = Files.createDirectories(sourceDir.resolve("links"))
        links.forEach { (title, json) -> Files.writeString(linksDir.resolve("${title}_links.json"), json) }
        val altTocDir = Files.createDirectories(sourceDir.resolve("alt_toc"))
        altToc.forEach { (title, json) -> Files.writeString(altTocDir.resolve("${title}_alt_toc.json"), json) }
        writeFilesManifest(sourceDir)
        return sourceDir
    }

    private fun generator(
        sourceDir: Path,
        db: Db,
        config: LongLineSplit.Config = split,
        allocator: InMemoryIdAllocator = InMemoryIdAllocator.load(null),
        buildVersion: Int = 1,
    ) = DatabaseGenerator(sourceDirectory = sourceDir, repository = db.repo, allocator = allocator, buildVersion = buildVersion)
        .also { it.longLineSplitConfig = config }

    private fun link(i1: Int, target: String, i2: Int, type: String = "commentary", extra: String = "") =
        """{"line_index_1": $i1, "heRef_2": "x", "path_2": "$target.txt", "line_index_2": $i2, "Conection Type": "$type"$extra}"""

    private suspend fun lineIds(db: Db, title: String): List<Long> {
        val book = db.repo.getAllBooks().single { it.title == title }
        return db.repo.getLines(book.id, 0, Int.MAX_VALUE).map { it.id }
    }

    @Test
    fun linksAnchorsRangesAndAltTocFollowTheSplit() = runBlocking {
        val sourceDir = library(
            books = mapOf(
                "Alpha" to listOf("<h1>Alpha</h1>", "intro", long, "after", "<h2>Second</h2>", "tail"),
                "Beta" to listOf("<h1>Beta</h1>", "beta one", "beta two", "beta three"),
            ),
            links = mapOf(
                "Alpha" to listOf(
                    link(3, "Beta", 2),
                    link(3, "Beta", 3, extra = """, "start": 26, "end": 30"""),
                    link(4, "Beta", 2),
                    link(2, "Beta", 3, extra = """, "line_index_1_end": 3"""),
                    link(3, "Beta", 4, extra = """, "line_index_1_end": 3"""),
                ).joinToString(",", "[", "]"),
                "Beta" to listOf(
                    link(2, "Alpha", 3),
                    link(3, "Alpha", 3, type = "source"),
                    link(3, "Alpha", 6, type = "source"),
                ).joinToString(",", "[", "]"),
            ),
            altToc = mapOf(
                "Alpha" to """[{"key": "K", "heTitle": "T", "nodes": [{"heTitle": "n1", "line": 3}, {"heTitle": "n2", "line": 6}]}]""",
            ),
        )
        val db = newDb()
        generator(sourceDir, db).generateLinesOnly()
        generator(sourceDir, db).generateLinksOnly()

        val a = lineIds(db, "Alpha")
        val b = lineIds(db, "Beta")
        assertEquals(8, a.size, "the long line is stored as three pieces")
        suspend fun linkId(source: Long, target: Long) = db.repo.getLinkIdsBetweenLines(source, target).single()

        val wholeSource = linkId(a[2], b[1])
        val anchored = linkId(a[3], b[2])
        linkId(a[5], b[1])
        val ranged = linkId(a[1], b[2])
        val wholeTarget = linkId(b[1], a[2])
        val flipped = linkId(a[2], b[2])
        linkId(a[7], b[2])
        val degenerate = linkId(a[2], b[3])
        assertEquals(8, db.query("SELECT COUNT(*) FROM link").single().single().toInt())

        val anchor = db.repo.getLinkAnchors(anchored).single()
        assertEquals(6 to 10, anchor.charStart to anchor.charEnd)

        assertEquals(
            setOf(
                listOf(wholeSource, 0L, a[4]),
                listOf(ranged, 0L, a[4]),
                listOf(wholeTarget, 1L, a[4]),
                listOf(flipped, 0L, a[4]),
                listOf(degenerate, 0L, a[4]),
            ),
            db.query("SELECT linkId, side, endLineId FROM link_range").toSet(),
        )
        assertEquals(
            listOf(a[2], a[3], a[4]),
            db.query("SELECT lineId FROM link_coverage WHERE linkId = $ranged ORDER BY lineId").map { it.single() },
        )
        assertEquals(
            listOf(a[3], a[4]),
            db.query("SELECT lineId FROM link_coverage WHERE linkId = $wholeTarget ORDER BY lineId").map { it.single() },
        )

        val alpha = db.repo.getAllBooks().single { it.title == "Alpha" }
        val structure = db.repo.getAltTocStructuresForBook(alpha.id).single()
        assertEquals(listOf(a[2], a[7]), db.repo.getAltTocEntriesForStructure(structure.id).sortedBy { it.id }.map { it.lineId })
    }

    @Test
    fun mergedFootnoteStaysWholeAndFreshPhaseTwoReproducesTheSplit() = runBlocking {
        val base = words(1, 3) + "<sup>1</sup> " + words(4, 13)
        val sourceDir = library(
            books = mapOf(
                "Gamma" to listOf("<h1>Gamma</h1>", base, "after"),
                "הערות על Gamma" to listOf("1 " + words(30, 37)),
            ),
            links = mapOf("Gamma" to "[${link(2, "הערות על Gamma", 1)}]"),
            altToc = mapOf("Gamma" to """[{"key": "K", "heTitle": "T", "nodes": [{"heTitle": "n", "line": 3}]}]"""),
        )
        val db = newDb()
        val config = split.copy(maxChars = 60)
        generator(sourceDir, db, config).generateLinesOnly()
        generator(sourceDir, db, config).generateLinksOnly()

        val gamma = db.repo.getAllBooks().single { it.title == "Gamma" }
        val lines = db.repo.getLines(gamma.id, 0, Int.MAX_VALUE)
        assertEquals(4, lines.size)
        val note = "<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">" + words(30, 37) + "</i>"
        assertEquals(words(1, 3) + note + " ", lines[1].content)
        assertEquals(words(4, 13), lines[2].content)
        val structure = db.repo.getAltTocStructuresForBook(gamma.id).single()
        assertEquals(listOf(lines[3].id), db.repo.getAltTocEntriesForStructure(structure.id).map { it.lineId })
    }

    @Test
    fun idsSurviveTheFirstSplitBuild() = runBlocking {
        val sourceDir = library(
            books = mapOf("Delta" to listOf("<h1>Delta</h1>", long, "mid", long, "<h2>Sec</h2>", "tail")),
        )
        val allocator = InMemoryIdAllocator.load(null)
        val before = newDb()
        generator(sourceDir, before, noSplit, allocator, buildVersion = 1).generateLinesOnly()
        val state = sourceDir.resolve("allocator.db")
        allocator.snapshotTo(state, mapOf("run" to "long-line-split-test"))

        val after = newDb()
        generator(sourceDir, after, split, InMemoryIdAllocator.load(state), buildVersion = 2).generateLinesOnly()

        val old = lineIds(before, "Delta")
        val new = lineIds(after, "Delta")
        assertEquals(6, old.size)
        assertEquals(10, new.size)
        // First pieces keep their whole line's id (the duplicate too); every other line keeps its own.
        assertEquals(old, listOf(new[0], new[1], new[4], new[5], new[8], new[9]))
        assertTrue((new - old.toSet()).size == 4, "the other pieces get fresh ids")

        fun toc(db: Db) = runBlocking {
            val book = db.repo.getAllBooks().single { it.title == "Delta" }
            db.repo.getBookToc(book.id).map { it.id to it.text }
        }
        assertEquals(toc(before), toc(after))
    }

    @Test
    fun unimportedSameTitleFileIsNotTheOneReproduced() = runBlocking {
        // The imported book has a line of targetChars but nothing to split.
        val sourceDir = library(books = mapOf("Eps" to listOf("<h1>Eps</h1>", words(1, 5), "x")))
        // A root-level file of the same title is never imported, though its long line would split.
        Files.writeString(sourceDir.resolve("אוצריא").resolve("Eps.txt"), listOf("<h1>Eps</h1>", long).joinToString("\n"))
        writeFilesManifest(sourceDir)
        val db = newDb()
        generator(sourceDir, db).generateLinesOnly()
        assertEquals(3, db.repo.getAllBooks().single { it.title == "Eps" }.totalLines)
        generator(sourceDir, db).generateLinksOnly()
    }

    @Test
    fun splitBookWithoutItsFileInPhaseTwoFailsTheBuild() {
        val sourceDir = library(books = mapOf("Zeta" to listOf("<h1>Zeta</h1>", long, "after")))
        val db = newDb()
        runBlocking { generator(sourceDir, db).generateLinesOnly() }
        Files.delete(sourceDir.resolve("אוצריא").resolve("Shelf").resolve("Zeta.txt"))
        val failure = assertFailsWith<IllegalStateException> {
            runBlocking { generator(sourceDir, db).generateLinksOnly() }
        }
        assertTrue(failure.message.orEmpty().contains("No imported txt file"), failure.message)
    }
}
