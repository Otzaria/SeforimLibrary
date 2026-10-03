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
import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Sefaria's Rif Megillah "Chapters" names chapters 3 and 4 in the Bavli order
 * (הקורא עומד, בני העיר), but the Rif follows the Mishnah: 7a:4 opens "בני העיר
 * שמכרו" and 11b:5 ends "סליקו להו בני העיר". The fix renames by range.
 */
class SefariaAltNodeNameFixesTest {

    private val logger = Logger.withTag("SefariaAltNodeNameFixesTest")

    // The four nodes exactly as the Sefaria export ships them (titles/refs abridged to what the importer reads).
    private val chapters = listOf(
        Triple("Rif Megillah 1a:1-4b:2", "מגילה נקראת", listOf("1a:1") + amudim("1b", "4a") + "4b:1-2"),
        Triple("Rif Megillah 4b:3-7a:3", "הקורא למפרע", listOf("4b:3-9") + amudim("5a", "6b") + "7a:1-3"),
        Triple("Rif Megillah 7a:4-11b:5", "הקורא עומד", listOf("7a:4") + amudim("7b", "11a") + "11b:1-5"),
        Triple("Rif Megillah 11b:6-16b:9", "בני העיר", listOf("11b:6-7") + amudim("12a", "16a") + "16b:1-9"),
    )

    private fun amudim(from: String, to: String): List<String> {
        fun idx(a: String) = (a.dropLast(1).toInt() - 1) * 2 + if (a.last() == 'b') 1 else 0
        return (idx(from)..idx(to)).map { i -> "${i / 2 + 1}${if (i % 2 == 0) 'a' else 'b'}" }
    }

    private fun schemaJson(): String {
        val nodes = chapters.withIndex().joinToString(",\n") { (i, c) ->
            val refs = c.third.joinToString(", ") { "\"Rif Megillah $it\"" }
            """{"nodeType": "ArrayMapNode", "depth": 1, "wholeRef": "${c.first}", "includeSections": true,
               |"title": "Chapter ${i + 1}", "heTitle": "${c.second}", "refs": [$refs],
               |"addressTypes": ["Talmud"], "sectionNames": ["Daf"]}""".trimMargin()
        }
        return """
            {"title": "Rif Megillah", "heTitle": "רי\"ף מגילה",
             "categories": ["Talmud", "Bavli", "Rishonim on Talmud", "Rif", "Seder Moed"],
             "heCategories": ["תלמוד", "בבלי", "ראשונים על התלמוד", "רי\"ף", "סדר מועד"],
             "schema": {"nodeType": "JaggedArrayNode", "depth": 2, "addressTypes": ["Talmud", "Integer"],
                        "sectionNames": ["Daf", "Line"], "heSectionNames": ["דף", "שורה"],
                        "title": "Rif Megillah", "heTitle": "רי\"ף מגילה", "key": "Rif Megillah"},
             "alts": {"Chapters": {"title": "Rif Megillah", "heTitle": "רי\"ף מגילה", "nodes": [$nodes]}}}
        """.trimIndent()
    }

    /** 1a … 16b, ten segments each, every segment naming its own address. */
    private fun mergedJson(): String {
        val amudim = (0 until 32).joinToString(", ") { i ->
            val amud = "${i / 2 + 1}${if (i % 2 == 0) 'a' else 'b'}"
            (1..10).joinToString(", ", "[", "]") { "\"seg $amud:$it\"" }
        }
        return """{"title": "Rif Megillah", "heTitle": "רי\"ף מגילה", "text": [$amudim]}"""
    }

    @Test
    fun rifMegillahChaptersFollowTheRifsOwnOrder() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-rif-megillah")
        try {
            val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
            val jsonDir = Files.createDirectories(tempDir.resolve("json"))
            val bookDir = Files.createDirectories(jsonDir.resolve("Rif Megillah"))
            Files.writeString(schemaDir.resolve("Rif_Megillah.json"), schemaJson())
            Files.writeString(bookDir.resolve("merged.json"), mergedJson())

            val reader = SefariaBookPayloadReader(Json { ignoreUnknownKeys = true; coerceInputValues = true }, logger)
            val payload = reader.readBooksInParallel(jsonDir, schemaDir, reader.buildSchemaLookup(schemaDir)).single()
            assertEquals(
                listOf("מגילה נקראת", "הקורא למפרע", "בני העיר", "הקורא עומד"),
                payload.altStructures.single().nodes.map { it.heTitle },
            )

            // Through the real builder: each name lands on the line its range starts at.
            val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
            SeforimDb.Schema.create(driver)
            val repo = SeforimRepository(":memory:", driver)
            try {
                val sourceId = repo.insertSource("Sefaria")
                val catId = repo.insertCategory(Category(0, null, "רי\"ף", level = 0, order = 1))
                repo.insertBook(
                    Book(
                        id = 1, categoryId = catId, sourceId = sourceId, title = payload.heTitle, heRef = payload.heTitle,
                        authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(), heShortDesc = null,
                        notesContent = null, order = 1f, topics = emptyList(), isBaseBook = false,
                        totalLines = payload.lines.size, hasAltStructures = false, hasTeamim = false, hasNekudot = false,
                    )
                )
                val lineKeyToId = ConcurrentHashMap<Pair<String, Int>, Long>()
                repo.insertLinesBatch(
                    payload.lines.mapIndexed { idx, content ->
                        lineKeyToId["Rif Megillah" to idx] = idx + 1L
                        Line(id = idx + 1L, bookId = 1, lineIndex = idx, content = content,
                            heRef = payload.refEntries.getOrNull(idx)?.heRef)
                    }
                )
                val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)
                assertTrue(
                    SefariaAltTocBuilder(repo, bindings).buildAltTocStructuresForBook(
                        payload = payload, bookId = 1, bookPath = "Rif Megillah",
                        lineKeyToId = lineKeyToId, totalLines = payload.lines.size,
                    )
                )
                val structure = repo.getAltTocStructuresForBook(1).single()
                val chapterStarts = repo.getAltTocEntriesForStructure(structure.id)
                    .filter { it.level == 0 }
                    .map { it.text to repo.getLine(it.lineId!!)!!.content }
                assertEquals(
                    listOf(
                        "מגילה נקראת" to "seg 1a:1",
                        "הקורא למפרע" to "seg 4b:3",
                        "בני העיר" to "seg 7a:4",
                        "הקורא עומד" to "seg 11b:6",
                    ),
                    chapterStarts,
                )
            } finally {
                repo.close()
            }
        } finally {
            tempDir.toFile().deleteRecursively()
        }
    }

    private fun node(wholeRef: String?, heTitle: String, children: List<AltNodePayload> = emptyList()) = AltNodePayload(
        title = null, heTitle = heTitle, wholeRef = wholeRef, refs = emptyList(), addressTypes = emptyList(),
        childLabel = null, addresses = emptyList(), skippedAddresses = emptyList(), startingAddress = null,
        offset = null, children = children,
    )

    private fun structure(key: String, vararg names: Pair<String, String>) =
        AltStructurePayload(key = key, title = null, heTitle = null, nodes = names.map { node(it.first, it.second) })

    @Test
    fun alreadyCorrectUpstreamIsANoOp() {
        val upstreamFixed = structure(
            "Chapters",
            "Rif Megillah 7a:4-11b:5" to "בני העיר",
            "Rif Megillah 11b:6-16b:9" to "הקורא עומד",
        )
        assertEquals(listOf(upstreamFixed), SefariaAltNodeNameFixes.apply("Rif Megillah", listOf(upstreamFixed), logger))
    }

    @Test
    fun aChangedRangeIsLeftAsSefariaGaveIt() {
        val restructured = structure(
            "Chapters",
            "Rif Megillah 7a:5-11b:5" to "הקורא עומד",
            "Rif Megillah 11b:6-16b:9" to "בני העיר",
        )
        val fixed = SefariaAltNodeNameFixes.apply("Rif Megillah", listOf(restructured), logger).single()
        assertEquals(listOf("הקורא עומד", "הקורא עומד"), fixed.nodes.map { it.heTitle })
    }

    @Test
    fun otherBooksAndStructuresAreUntouched() {
        val bavli = listOf(structure("Chapters", "Megillah 21a:1-25b:12" to "הקורא עומד"))
        assertSame(bavli, SefariaAltNodeNameFixes.apply("Megillah", bavli, logger))
        val otherKey = listOf(structure("Daf", "Rif Megillah 7a:4-11b:5" to "הקורא עומד"))
        assertEquals(otherKey, SefariaAltNodeNameFixes.apply("Rif Megillah", otherKey, logger))
    }

    @Test
    fun nestedNodesAreFixedToo() {
        val nested = AltStructurePayload(
            key = "Chapters", title = null, heTitle = null,
            nodes = listOf(node(null, "חלק", children = listOf(node("Rif Megillah 7a:4-11b:5", "הקורא עומד")))),
        )
        val fixed = SefariaAltNodeNameFixes.apply("Rif Megillah", listOf(nested), logger).single()
        assertEquals("בני העיר", fixed.nodes.single().children.single().heTitle)
    }
}
