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
import kotlinx.serialization.json.jsonObject
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * A section's first segment that only repeats its generated heading ("דרוש ד':" under
 * "דרוש ד", שער הכוונות) folds into that heading, which takes over the segment's ref.
 */
class SefariaSectionHeadingEchoTest {
    private val reader = SefariaBookPayloadReader(Json, Logger.withTag("SefariaSectionHeadingEchoTest"))

    @Test
    fun aSegmentRepeatingItsSectionHeadingFoldsIntoIt() {
        val walk = reader.walkTextWithSchema(Json.parseToJsonElement(schema).jsonObject, Json.parseToJsonElement(text), "ספר", "Book")

        assertEquals(
            listOf(
                "<h1>ספר</h1>",
                "<h2>דרושי ציצית</h2>",
                "<h3>דרוש א</h3>",
                "טקסט א",
                "<h3>דרוש ב</h3>",
                "טקסט ב",
                "<h3>דרוש ג</h3>",
                "x",
                // More than the heading: the segment stays.
                "<h3>דרוש ד</h3>",
                "דרוש ד' ובו יבואר",
                "y",
                // Vowels are never ignored.
                "<h3>דרוש ה</h3>",
                "דְּרוּשׁ ה",
                "z",
            ),
            walk.lines,
        )
        assertEquals(
            listOf(
                "Book, Derushim,  1:1" to "<h3>דרוש א</h3>",
                "Book, Derushim,  1:2" to "טקסט א",
                "Book, Derushim,  2:1" to "<h3>דרוש ב</h3>",
                "Book, Derushim,  2:2" to "טקסט ב",
                "Book, Derushim,  3:1" to "<h3>דרוש ג</h3>",
                "Book, Derushim,  3:2" to "x",
                "Book, Derushim,  4:1" to "דרוש ד' ובו יבואר",
                "Book, Derushim,  4:2" to "y",
                "Book, Derushim,  5:1" to "דְּרוּשׁ ה",
                "Book, Derushim,  5:2" to "z",
            ),
            walk.refs.map { it.ref to walk.lines[it.lineIndex - 1] },
        )
        assertEquals(setOf(2, 4, 6), walk.titleSegmentHeadingLines)
        assertEquals(List(3) { CLEAN_MODIFIED }, listOf(2, 4, 6).map { walk.cleanShifts[it] })
        // Heading lines keep their content and TOC key; the removed segments keep their slots.
        assertEquals(
            listOf(0 to 0, 1 to 1, 2 to 2, 4 to 5, 6 to 8, 8 to 11, 11 to 14),
            walk.headings.map { it.lineIndex to it.keyLineIndex },
        )
        assertEquals(
            mapOf(3 to listOf("דרוש א':"), 5 to listOf("<b>דרוש ב</b>"), 7 to listOf("דרוש ג.")),
            walk.omittedLinesBefore.mapValues { (_, lines) -> lines.map { it.content } },
        )
    }

    @Test
    fun linksToAFoldedSegmentLandOnItsSectionHeading() = runBlocking {
        val root = Files.createTempDirectory("section-heading-echo")
        val schemas = Files.createDirectory(root.resolve("schemas"))
        Files.writeString(
            root.resolve("table_of_contents.json"),
            """[{"category":"Test","heCategory":"Test","contents":[{"title":"Book","heTitle":"ספר"}]}]""",
        )
        Files.writeString(schemas.resolve("Book.json"), """{"schema":$schema,"heCategories":["Test"],"categories":["Test"]}""")
        val merged = Files.createDirectories(root.resolve("json/Book")).resolve("merged.json")
        Files.writeString(merged, """{"title":"Book","heTitle":"ספר","text":$text}""")
        val payload = requireNotNull(reader.parseBookFile(merged, schemas, reader.buildSchemaLookup(schemas)))
            .precomputeLineData()

        val linksDir = Files.createDirectories(root.resolve("links"))
        Files.writeString(
            linksDir.resolve("links0.csv"),
            """
            |Citation 1,Citation 2,Conection Type
            |"Book, Derushim 1:1","Other 1","Commentary"
            |"Book, Derushim 2:1-2","Other 2","Commentary"
            |"Book, Derushim 5:1","Other 2","Commentary"
            """.trimMargin()
        )

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        try {
            val sourceId = repo.insertSource("Sefaria-Test")
            val catId = repo.insertCategory(Category(0, null, "Test", level = 0, order = 1))
            repo.insertBook(Book(1, catId, sourceId, "אחר", totalLines = 2, isBaseBook = true))
            repo.insertBook(Book(2, catId, sourceId, "ספר", totalLines = payload.lines.size))
            repo.insertLinesBatch(listOf(Line(1, 1, 0, "other 1"), Line(2, 1, 1, "other 2")))
            fun bookLineId(index: Int) = 100L + index
            repo.insertLinesBatch(payload.lines.mapIndexed { index, content -> Line(bookLineId(index), 2, index, content) })

            val allRefs = listOf(
                RefEntry("Other 1", "אחר א", "Other", 1),
                RefEntry("Other 2", "אחר ב", "Other", 2),
            ) + payload.refEntries.map { it.copy(path = "Book") }
            val refsByBase = mutableMapOf<String, RefEntry>()
            allRefs.forEach { e ->
                val base = canonicalBase(e.ref)
                val existing = refsByBase[base]
                if (existing == null || e.lineIndex < existing.lineIndex) refsByBase[base] = e
            }
            val lineIsHeading = requireNotNull(payload.precomputed?.lineIsHeading)
            SefariaLinksImporter(
                repo,
                IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo),
                Logger.withTag("SefariaSectionHeadingEchoTest"),
            ).processLinksInParallel(
                linksDir = linksDir,
                refsByCanonical = allRefs.groupBy { canonicalCitation(it.ref) },
                refsByBase = refsByBase,
                lineKeyToId = mapOf(("Other" to 0) to 1L, ("Other" to 1) to 2L) +
                    payload.lines.indices.associate { ("Book" to it) to bookLineId(it) },
                lineIdToBookId = mapOf(1L to 1L, 2L to 1L) + payload.lines.indices.associate { bookLineId(it) to 2L },
                bookMetaById = mapOf(
                    1L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0),
                    2L to BookMeta(
                        isBaseBook = false, categoryLevel = 1, priorityRank = null,
                        dependence = Dependence.COMMENTARY, baseTextBookIds = setOf(1L),
                    ),
                ),
                headingLineIds = payload.lines.indices.filter { lineIsHeading[it] }.mapTo(HashSet()) { bookLineId(it) },
                refsByPath = allRefs.groupBy { it.path },
            )

            fun query(sql: String): List<List<Long>> = driver.getConnection().createStatement().use { st ->
                st.executeQuery(sql).use { rs ->
                    buildList { while (rs.next()) add((1..rs.metaData.columnCount).map { rs.getLong(it) }) }
                }
            }
            fun content(lineId: Long) = payload.lines[(lineId - 100).toInt()]

            assertEquals(
                listOf(1L to "<h3>דרוש א</h3>", 2L to "<h3>דרוש ב</h3>", 2L to "דְּרוּשׁ ה"),
                query("SELECT sourceLineId, targetLineId FROM link ORDER BY targetLineId")
                    .map { (source, target) -> source to content(target) },
            )
            assertEquals(listOf("טקסט ב"), query("SELECT endLineId FROM link_range WHERE side = 1").map { content(it.single()) })
            assertEquals(listOf("טקסט ב"), query("SELECT lineId FROM link_coverage WHERE side = 1").map { content(it.single()) })
        } finally {
            repo.close()
            Files.walk(root).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }

    private val schema = """
        {
          "key": "Book", "title": "Book", "heTitle": "ספר",
          "nodes": [{
            "nodeType": "JaggedArrayNode", "depth": 2, "key": "Derushim",
            "title": "Derushim", "heTitle": "דרושי ציצית",
            "sectionNames": ["Derush", "Paragraph"], "heSectionNames": ["דרוש", "פסקה"],
            "addressTypes": ["Integer", "Integer"]
          }]
        }
    """

    private val text = """
        {"Derushim": [
          ["דרוש א':", "טקסט א"],
          ["<b>דרוש ב</b>", "טקסט ב"],
          ["דרוש ג.", "x"],
          ["דרוש ד' ובו יבואר", "y"],
          ["דְּרוּשׁ ה", "z"]
        ]}
    """
}
