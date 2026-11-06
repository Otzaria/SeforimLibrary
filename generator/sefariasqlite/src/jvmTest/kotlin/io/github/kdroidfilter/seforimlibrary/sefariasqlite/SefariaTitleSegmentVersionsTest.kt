package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A title may fold only when every Hebrew edition can do without a separate text overlay. */
class SefariaTitleSegmentVersionsTest {
    @Test
    fun substantiveVariantAtMainTitleAddressIsRetained() = runBlocking {
        val variant = "הקדמה: כאן יש תוכן ייחודי למהדורה האחרת"
        val result = importVersions(intro("<b>הקדמה</b>"), listOf(Edition(intro(variant))))
        assertEquals(listOf(variant, "גוף אחר"), result.contents())
        assertTrue(result.payload.titleSegmentHeadingLines.isEmpty())
        assertTrue("<b>הקדמה</b>" in result.payload.lines)
    }

    @Test
    fun versionTitleAtNonFoldedMainAddressIsRetained() = runBlocking {
        val variant = "<b>הקדמה</b>"
        val result = importVersions(intro("פתיחה עם תוכן בטקסט הראשי"), listOf(Edition(intro(variant))))
        assertEquals(listOf(variant, "גוף אחר"), result.contents())
    }

    @Test
    fun notesAndVowelsInAnEditionPreventFolding() = runBlocking {
        for (variant in listOf(
            """<b>הקדמה<i data-commentator="Endnotes" data-label="1" data-order="1"></i></b>""",
            "הַקְדָּמָה",
        )) {
            val result = importVersions(intro("<b>הקדמה</b>"), listOf(Edition(intro(variant))))
            assertEquals(listOf(variant, "גוף אחר"), result.contents())
            assertTrue(result.payload.titleSegmentHeadingLines.isEmpty())
        }
    }

    @Test
    fun matchingTitlesAcrossEditionsStillFold() = runBlocking {
        val result = importVersions(
            intro("<b>הקדמה</b>"),
            listOf(Edition(intro("<strong>הקדמה:</strong>"), "A"), Edition(intro("הקדמה."), "B")),
        )
        assertEquals(setOf(1), result.payload.titleSegmentHeadingLines)
        assertEquals(listOf("A" to "גוף אחר", "B" to "גוף אחר"), result.rows)
        val first = result.payload.refEntries.first()
        assertEquals("<h2>הקדמה</h2>", result.payload.lines[first.lineIndex - 1])
        assertEquals(CLEAN_MODIFIED, result.payload.cleanShiftByLineIndex[first.lineIndex - 1])
    }

    @Test
    fun missingTitleAndForeignEditionDoNotPreventFolding() = runBlocking {
        val result = importVersions(
            intro("<b>הקדמה</b>"),
            listOf(Edition(intro("")), Edition(intro("different text"), "Foreign", language = "fa")),
        )
        assertEquals(setOf(1), result.payload.titleSegmentHeadingLines)
        assertEquals(listOf("גוף אחר"), result.contents())
    }

    @Test
    fun onlyTheNodeWithVariantTextKeepsItsSegment() = runBlocking {
        val nestedSchema = """
            {"title":"Book","heTitle":"ספר","nodes":[
              {"key":"Part","title":"Part","heTitle":"חלק","nodes":[
                ${leaf("Intro", "הקדמה")}, ${leaf("Body", "גוף")}
              ]}
            ]}
        """.trimIndent()
        val main = """{"Part":{"Intro":["<b>הקדמה</b>","a"],"Body":["<b>גוף</b>","b"]}}"""
        val edition = """{"Part":{"Intro":["הקדמה שונה","c"],"Body":["<b>גוף</b>","d"]}}"""
        val result = importVersions(main, listOf(Edition(edition)), nestedSchema)
        assertEquals(1, result.payload.titleSegmentHeadingLines.size)
        val titleIndex = result.payload.titleSegmentHeadingLines.single()
        assertEquals("<h3>גוף</h3>", result.payload.lines[titleIndex])
        assertTrue("<b>הקדמה</b>" in result.payload.lines)
        assertEquals(listOf("הקדמה שונה", "c", "d"), result.contents())
    }

    @Test
    fun preferredVersionAlsoRetainsOtherEditionsText() = runBlocking {
        val result = importVersions(
            intro("פתיחה ממוזגת"),
            listOf(Edition(intro("<b>הקדמה</b>"), "A"), Edition(intro("נוסח אחר"), "B")),
            preferred = "A.json",
        )
        assertEquals("A.json", result.payload.preferredVersionFileName)
        assertTrue(result.payload.titleSegmentHeadingLines.isEmpty())
        assertEquals(
            listOf("A" to "<b>הקדמה</b>", "A" to "גוף אחר", "B" to "נוסח אחר", "B" to "גוף אחר"),
            result.rows,
        )
    }

    @Test
    fun generatedEditionPrefixesAreNotMistakenForVariantText() = runBlocking {
        // Main has one nonempty segment (no generated label); the edition has
        // two, so its identical title acquires an (א) prefix while walking.
        val schemaWithLabels = """
            {"title":"Book","heTitle":"ספר","nodes":[
              {"key":"Intro","title":"Intro","heTitle":"הקדמה","depth":1,
               "sectionNames":["Paragraph"],"heSectionNames":["פסקה"],"addressTypes":["Paragraph"]}
            ]}
        """.trimIndent()
        val result = importVersions(
            """{"Intro":["<b>הקדמה</b>",""]}""",
            listOf(Edition(intro("<b>הקדמה</b>"))), schemaWithLabels,
        )
        assertEquals(setOf(1), result.payload.titleSegmentHeadingLines)
    }

    private data class Edition(val text: String, val title: String = "Other", val language: String = "he")

    private data class Result(val payload: BookPayload, val rows: List<Pair<String, String>>) {
        fun contents(): List<String> = rows.map { it.second }
    }

    private suspend fun importVersions(
        mainText: String,
        editions: List<Edition>,
        bookSchema: String = schema,
        preferred: String? = null,
    ): Result {
        val root = Files.createTempDirectory("title-segment-versions")
        var repo: SeforimRepository? = null
        try {
            val schemas = Files.createDirectory(root.resolve("schemas"))
            val bookDir = Files.createDirectories(root.resolve("json/Book"))
            Files.writeString(schemas.resolve("Book.json"), """{"schema":$bookSchema,"heCategories":["Test"]}""")
            val merged = bookDir.resolve("merged.json")
            Files.writeString(merged, """{"title":"Book","heTitle":"ספר","language":"he","text":$mainText}""")
            for (edition in editions) {
                Files.writeString(bookDir.resolve("${edition.title}.json"), """
                    {"title":"Book","heTitle":"ספר","language":"he","actualLanguage":"${edition.language}",
                     "versionTitle":"${edition.title}","text":${edition.text}}
                """.trimIndent())
            }
            val logger = Logger.withTag("SefariaTitleSegmentVersionsTest")
            val preferredVersions = preferred?.let { parsePreferredVersions(listOf("ספר|$it"), logger) }
                ?: SefariaPreferredVersions.Empty
            val reader = SefariaBookPayloadReader(Json, logger, preferredVersions = preferredVersions)
            val payload = requireNotNull(reader.parseBookFile(merged, schemas, reader.buildSchemaLookup(schemas)))
            val driver = JdbcSqliteDriver("jdbc:sqlite::memory:")
            SeforimDb.Schema.create(driver)
            val repository = SeforimRepository(":memory:", driver).also { repo = it }
            val source = repository.insertSource("Test")
            val cat = repository.insertCategory(Category(0, null, "Test", level = 0, order = 1))
            repository.insertBook(Book(1, cat, source, "ספר", totalLines = payload.lines.size))
            repository.insertLinesBatch(payload.lines.mapIndexed { idx, content -> Line(100L + idx, 1, idx, content) })
            SefariaVersionsImporter(repository, InMemoryIdAllocator.load(null), Json, reader)
                .import(
                    listOf(SefariaVersionsImporter.BookInput(payload, 1, "Book")),
                    payload.lines.indices.associate { ("Book" to it) to (100L + it) },
                )
            val rows = driver.getConnection().createStatement().use { st ->
                st.executeQuery("""
                    SELECT bv.versionTitle, vl.content FROM version_line vl
                    JOIN book_version bv ON bv.id = vl.versionId ORDER BY bv.versionTitle, vl.lineId
                """.trimIndent()).use { rs ->
                    buildList { while (rs.next()) add(rs.getString(1) to rs.getString(2)) }
                }
            }
            return Result(payload, rows)
        } finally {
            repo?.close()
            Files.walk(root).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists) }
        }
    }

    private companion object {
        fun leaf(title: String, heTitle: String) = """
            {"key":"$title","title":"$title","heTitle":"$heTitle","depth":1,
             "sectionNames":["Paragraph"],"heSectionNames":["פסקה"],"addressTypes":["Integer"]}
        """.trimIndent()

        val schema = """{"title":"Book","heTitle":"ספר","nodes":[${leaf("Intro", "הקדמה")}]}"""

        fun intro(first: String) = """{"Intro":[${JsonPrimitive(first)},"גוף אחר"]}"""
    }
}
