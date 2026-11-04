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
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Sefaria often opens a node with its own title as its first segment. The heading shows once
 * and takes over that segment's ref, so links to the segment land on the heading.
 */
class SefariaTitleSegmentHeadingTest {
    private val reader = SefariaBookPayloadReader(Json, Logger.withTag("SefariaTitleSegmentHeadingTest"))

    @Test
    fun aSegmentRepeatingItsNodeTitleFoldsIntoTheHeading() {
        val walk = reader.walkTextWithSchema(schema, Json.parseToJsonElement(text), "ספר", "Book")

        assertEquals(
            listOf(
                "<h1>ספר</h1>",
                "<h2>מעשה אברהם</h2>",
                "גוף המעשה",
                "<h2>סדר הקריאה</h2>",
                "כהן",
                "<h2>חלק</h2>",
                "<h3>הקדמה</h3>",
                // A note marker is more than the title: the segment stays.
                NOTED_INTRO,
                "a",
                "<h3>גוף</h3>",
                "b",
                // Vowelled prayer text is not a title, even when it spells one.
                "<h2>אבינו מלכנו</h2>",
                "אָבִֽינוּ מַלְכֵּֽנוּ",
                "חָטָֽאנוּ לְפָנֶֽיךָ",
            ),
            walk.lines,
        )
        assertEquals(
            listOf(
                "Book, Story,  1" to "<h2>מעשה אברהם</h2>",
                "Book, Story,  2" to "גוף המעשה",
                "Book, Reading,  1" to "<h2>סדר הקריאה</h2>",
                "Book, Reading,  2" to "כהן",
                "Book, Part, Intro,  1" to NOTED_INTRO,
                "Book, Part, Intro,  2" to "a",
                "Book, Part, Body,  1" to "<h3>גוף</h3>",
                "Book, Part, Body,  2" to "b",
                "Book, Prayer,  1" to "אָבִֽינוּ מַלְכֵּֽנוּ",
                "Book, Prayer,  2" to "חָטָֽאנוּ לְפָנֶֽיךָ",
            ),
            walk.refs.map { it.ref to walk.lines[it.lineIndex - 1] },
        )
        assertEquals(setOf(1, 3, 9), walk.titleSegmentHeadingLines)
        // Char offsets of the folded segment must not be applied to the heading's text.
        assertEquals(List(3) { CLEAN_MODIFIED }, listOf(1, 3, 9).map { walk.cleanShifts[it] })

        // Heading lines and their TOC keys are untouched; only later positions shift.
        assertEquals(
            listOf(
                Heading("ספר", 0, lineIndex = 0, keyLineIndex = 0),
                Heading("מעשה אברהם", 1, lineIndex = 1, keyLineIndex = 1),
                Heading("סדר הקריאה", 1, lineIndex = 3, keyLineIndex = 4),
                Heading("חלק", 1, lineIndex = 5, keyLineIndex = 7),
                Heading("הקדמה", 2, lineIndex = 6, keyLineIndex = 8),
                Heading("גוף", 2, lineIndex = 9, keyLineIndex = 11),
                Heading("אבינו מלכנו", 1, lineIndex = 11, keyLineIndex = 14),
            ),
            walk.headings,
        )
        // The removed segments still reserve their occurrence slots under their own heRef.
        val folded = walk.refs.filter { it.lineIndex - 1 in walk.titleSegmentHeadingLines }
        assertEquals(
            mapOf(
                2 to listOf("<b>מעשה אברהם</b>" to folded[0].heRef),
                4 to listOf("<b><big><b>סדר הקריאה:</b></big></b>" to folded[1].heRef),
                10 to listOf("<b>גוף </b>" to folded[2].heRef),
            ),
            walk.omittedLinesBefore.mapValues { (_, lines) -> lines.map { it.content to it.heRef } },
        )
    }

    @Test
    fun linksToAFoldedSegmentLandOnItsHeading() = runBlocking {
        val root = Files.createTempDirectory("title-segment-links")
        val schemas = Files.createDirectory(root.resolve("schemas"))
        Files.writeString(
            root.resolve("table_of_contents.json"),
            """[{"category":"Test","heCategory":"Test","contents":[{"title":"Book","heTitle":"ספר"}]}]""",
        )
        Files.writeString(schemas.resolve("Book.json"), """{"schema":$schemaJson,"heCategories":["Test"],"categories":["Test"]}""")
        val merged = Files.createDirectories(root.resolve("json/Book")).resolve("merged.json")
        Files.writeString(merged, """{"title":"Book","heTitle":"ספר","text":$text}""")
        val payload = requireNotNull(reader.parseBookFile(merged, schemas, reader.buildSchemaLookup(schemas)))
            .precomputeLineData()

        val linksDir = Files.createDirectories(root.resolve("links"))
        Files.writeString(
            linksDir.resolve("links0.csv"),
            """
            |Citation 1,Citation 2,Conection Type
            |"Book, Story 1","Other 1","Commentary"
            |"Book, Story 2","Other 2","Commentary"
            |"Book, Part, Body 1-2","Other 2","Commentary"
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
                Logger.withTag("SefariaTitleSegmentHeadingTest"),
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
                listOf(
                    1L to "<h2>מעשה אברהם</h2>",
                    2L to "גוף המעשה",
                    2L to "<h3>גוף</h3>",
                ),
                query("SELECT sourceLineId, targetLineId FROM link ORDER BY targetLineId")
                    .map { (source, target) -> source to content(target) },
            )
            // A range that starts at a folded segment keeps its end and its coverage.
            assertEquals(listOf("b"), query("SELECT endLineId FROM link_range WHERE side = 1").map { content(it.single()) })
            assertEquals(listOf("b"), query("SELECT lineId FROM link_coverage WHERE side = 1").map { content(it.single()) })
        } finally {
            repo.close()
            Files.walk(root).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }

    private fun leaf(title: String, heTitle: String) = """
        {
          "nodeType": "JaggedArrayNode", "depth": 1, "key": "$title",
          "title": "$title", "heTitle": "$heTitle",
          "sectionNames": ["Paragraph"], "heSectionNames": ["פסקה"],
          "addressTypes": ["Integer"]
        }
    """

    private val schemaJson = """
        {
          "key": "Book", "title": "Book", "heTitle": "ספר",
          "nodes": [
            ${leaf("Story", "מעשה אברהם")},
            ${leaf("Reading", "סדר הקריאה")},
            {
              "key": "Part", "title": "Part", "heTitle": "חלק",
              "nodes": [${leaf("Intro", "הקדמה")}, ${leaf("Body", "גוף")}]
            },
            ${leaf("Prayer", "אבינו מלכנו")}
          ]
        }
    """

    private val schema = Json.parseToJsonElement(schemaJson).jsonObject

    private val text = """
        {
          "Story": ["<b>מעשה אברהם</b>", "גוף המעשה"],
          "Reading": ["<b><big><b>סדר הקריאה:</b></big></b>", "כהן"],
          "Part": {"Intro": [${JsonPrimitive(NOTED_INTRO)}, "a"], "Body": ["<b>גוף </b>", "b"]},
          "Prayer": ["אָבִֽינוּ מַלְכֵּֽנוּ", "חָטָֽאנוּ לְפָנֶֽיךָ"]
        }
    """

    private companion object {
        const val NOTED_INTRO = """<big><strong>הקדמה<i data-commentator="Endnotes" data-label="1" data-order="1"></i></strong></big>"""
    }
}
