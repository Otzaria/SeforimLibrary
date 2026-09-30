package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.LogWriter
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.StaticConfig
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** version_titles_he.txt: parsing, precedence, unmatched-row warnings and importer wiring. */
class SefariaVersionTitlesHeTest {

    private class Capture : LogWriter() {
        val lines = mutableListOf<Pair<Severity, String>>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += severity to message
        }

        fun warnings(): List<String> = lines.filter { it.first == Severity.Warn }.map { it.second }
        fun infos(): List<String> = lines.filter { it.first == Severity.Info }.map { it.second }
    }

    private fun capturingLogger(capture: Capture) =
        Logger(StaticConfig(Severity.Verbose, listOf(capture)), "test")

    @Test
    fun parsesBothFormsSkipsCommentsAndKeepsLineNumbers() {
        val titles = parseVersionHebrewTitles(
            listOf(
                "﻿# header",
                "",
                "  Torat-Emet  =>  תורת אמת  ",
                "   # indented comment",
                "מגילת אנטיוכוס | the Open Siddur Project => תרגום עברי",
                "Megillat Antiochus|the Open Siddur Project - Aramaic => מקור ארמי",
            )
        )
        assertEquals(
            listOf(
                VersionTitleRow(3, "Torat-Emet  =>  תורת אמת", null, "Torat-Emet", "תורת אמת"),
                VersionTitleRow(
                    5, "מגילת אנטיוכוס | the Open Siddur Project => תרגום עברי",
                    "מגילת אנטיוכוס", "the Open Siddur Project", "תרגום עברי",
                ),
                VersionTitleRow(
                    6, "Megillat Antiochus|the Open Siddur Project - Aramaic => מקור ארמי",
                    "Megillat Antiochus", "the Open Siddur Project - Aramaic", "מקור ארמי",
                ),
            ),
            titles.rows,
        )
    }

    @Test
    fun malformedLinesFailAtParseTimeWithLineNumber() {
        val malformed = listOf(
            "Torat-Emet תורת אמת",                 // no separator
            "Torat-Emet=>תורת אמת",                // separator without spaces
            "Torat-Emet => ",                      // empty Hebrew side
            " => תורת אמת",                        // empty versionTitle side
            "ספר | => תורת אמת",                   // empty versionTitle in a book row
            " | Torat-Emet => תורת אמת",           // empty book name
            "a | b | Torat-Emet => תורת אמת",      // two pipes
            "Torat-Emet => תורת => אמת",           // two separators
        )
        malformed.forEach { line ->
            val error = assertFailsWith<MalformedVersionTitlesException>(line) {
                parseVersionHebrewTitles(listOf("# header", line))
            }
            assertTrue(error.message!!.startsWith("version_titles_he.txt:2: "), error.message)
        }
    }

    @Test
    fun duplicateKeysFailButSameVersionInDifferentScopesIsAllowed() {
        val global = assertFailsWith<MalformedVersionTitlesException> {
            parseVersionHebrewTitles(listOf("Torat-Emet => תורת אמת", "Torat-Emet => אחר"))
        }
        assertTrue(global.message!!.contains("already on line 1"), global.message)
        assertFailsWith<MalformedVersionTitlesException> {
            parseVersionHebrewTitles(listOf("ספר | Torat-Emet => א", " ספר |  Torat-Emet => א"))
        }
        // Global + book row, or two different books, are distinct keys.
        val ok = parseVersionHebrewTitles(
            listOf("Torat-Emet => א", "ספר | Torat-Emet => ב", "ספר אחר | Torat-Emet => ג")
        )
        assertEquals(3, ok.rows.size)
    }

    @Test
    fun bookRowOverridesSefariaHebrewAndGlobalOnlyFillsEmpty() {
        val titles = parseVersionHebrewTitles(
            listOf(
                "Torat-Emet => תורת אמת",
                "ספר בדיקה | Vilna => וילנא מתוקן",
            )
        )
        // Book row wins even over Sefaria's own Hebrew title.
        assertEquals("וילנא מתוקן", titles.resolve("ספר בדיקה", "Test Book", "Vilna", "וילנא").heVersionTitle)
        // Global row fills an empty title (null or blank) only.
        assertEquals("תורת אמת", titles.resolve("ספר", "Book", "Torat-Emet", null).heVersionTitle)
        assertEquals("תורת אמת", titles.resolve("ספר", "Book", "Torat-Emet", " ").heVersionTitle)
        val kept = titles.resolve("ספר", "Book", "Torat-Emet", "תורת אמת של ספריא")
        assertEquals("תורת אמת של ספריא", kept.heVersionTitle)
        assertNull(kept.applied)
        assertEquals(listOf(1), kept.matched.map { it.lineNumber })
        // Book row scoped to its book only.
        assertEquals("וילנא", titles.resolve("ספר אחר", "Other", "Vilna", "וילנא").heVersionTitle)
        assertNull(titles.resolve("ספר אחר", "Other", "Vilna", null).heVersionTitle)
    }

    @Test
    fun bookRowBeatsGlobalRow() {
        val titles = parseVersionHebrewTitles(
            listOf(
                "ספר בדיקה | Torat-Emet => תורת אמת בספר",
                "Torat-Emet => תורת אמת",
            )
        )
        val resolution = titles.resolve("ספר בדיקה", "Test Book", "Torat-Emet", null)
        assertEquals("תורת אמת בספר", resolution.heVersionTitle)
        assertEquals(1, resolution.applied?.lineNumber)
        assertEquals(listOf(1, 2), resolution.matched.map { it.lineNumber })
    }

    @Test
    fun matchesByHebrewOrEnglishBookTitleExactly() {
        val titles = parseVersionHebrewTitles(
            listOf(
                "מגילת אנטיוכוס | the Open Siddur Project => תרגום עברי",
                "Megillat Antiochus | the Open Siddur Project - Aramaic => מקור ארמי",
            )
        )
        val he = "מגילת אנטיוכוס"
        val en = "Megillat Antiochus"
        assertEquals("תרגום עברי", titles.resolve(he, en, "the Open Siddur Project", null).heVersionTitle)
        assertEquals("מקור ארמי", titles.resolve(he, en, "the Open Siddur Project - Aramaic", null).heVersionTitle)
        // Exact: no case, quote or inner-space normalization; surrounding spaces trimmed.
        assertEquals("מקור ארמי", titles.resolve(he, en, " the Open Siddur Project - Aramaic ", null).heVersionTitle)
        assertNull(titles.resolve(he, "megillat antiochus", "the Open Siddur Project - Aramaic", null).heVersionTitle)
        assertNull(titles.resolve(he, en, "The Open Siddur Project", null).heVersionTitle)
        assertNull(titles.resolve(he, en, "the Open Siddur  Project", null).heVersionTitle)
    }

    @Test
    fun hebrewAndEnglishRowsForTheSameVersionWarnAndLowerLineWins() {
        val he = "מגילת אנטיוכוס"
        val en = "Megillat Antiochus"
        val version = "the Open Siddur Project"
        // Either order in the file: the lower line number wins, never throws.
        for ((lines, expected) in listOf(
            listOf("$he | $version => א", "$en | $version => ב") to "א",
            listOf("$en | $version => ב", "$he | $version => א") to "ב",
        )) {
            val capture = Capture()
            val usage = VersionTitlesUsage(parseVersionHebrewTitles(lines), capturingLogger(capture))
            assertEquals(expected, usage.resolve(he, en, version, "שם של ספריא"))
            assertEquals(
                listOf("version_titles_he.txt: lines 1 and 2 both apply to $he / $version; line 1 wins"),
                capture.warnings(),
            )
            // Both rows matched a version, so neither is reported as unmatched.
            assertEquals(emptyList(), usage.unmatchedRows())
        }
    }

    @Test
    fun unmatchedRowsWarnWithLineNumberAndDoNotThrow() {
        val titles = parseVersionHebrewTitles(
            listOf(
                "# header",
                "Torat-Emet => תורת אמת",
                "Renamed By Sefaria => שם ישן",
                "ספר בדיקה | Gone => נעלם",
                "ספר בדיקה | Vilna => וילנא",
            )
        )
        val capture = Capture()
        val usage = VersionTitlesUsage(titles, capturingLogger(capture))
        assertEquals("תורת אמת", usage.resolve("ספר", "Book", "Torat-Emet", null))
        assertEquals("וילנא", usage.resolve("ספר בדיקה", "Test Book", "Vilna", "וילנא ישן"))
        // Matched but not applied (Sefaria already has Hebrew): not reported as unmatched.
        assertEquals("קיים", usage.resolve("ספר", "Book", "Torat-Emet", "קיים"))

        usage.logSummary()
        assertEquals(
            listOf(
                "version_titles_he.txt:3: row matched no imported version: Renamed By Sefaria => שם ישן",
                "version_titles_he.txt:4: row matched no imported version: ספר בדיקה | Gone => נעלם",
            ),
            capture.warnings(),
        )
        assertEquals(
            listOf("Version Hebrew titles: rows=4, applied=2, unmatched=2, versionsRetitled=2"),
            capture.infos(),
        )
    }

    @Test
    fun shippedResourceParsesAndMatchesTheExportTitles() {
        val rows = loadVersionHebrewTitles(javaClass.classLoader).rows.map { Triple(it.book, it.versionTitle, it.heTitle) }
        for (row in listOf(
            Triple("Megillat Antiochus", "the Open Siddur Project - Aramaic", "מקור ארמי (פרויקט הסידור הפתוח)"),
            Triple("Megillat Antiochus", "the Open Siddur Project", "תרגום עברי (פרויקט הסידור הפתוח)"),
            Triple(null, "Vilna Edition", "דפוס וילנא"),
        )) assertContains(rows, row)
    }

    @Test
    fun shippedResourceHasNoNiqqud() {
        // Display names are plain letters: no points or cantillation.
        val niqqud = Regex("[֑-ׇ]")
        val offending = loadVersionHebrewTitles(javaClass.classLoader).rows.filter { niqqud.containsMatchIn(it.heTitle) }
        assertEquals(emptyList(), offending.map { "${it.lineNumber}: ${it.text}" })
    }

    /** Importer: mapping applies to file and metadata-only rows, after the blacklist, without id changes. */
    @Test
    fun importerAppliesTitlesAfterBlacklistAndKeepsIds() = runBlocking {
        val fixture = writeFixture()
        val titles = parseVersionHebrewTitles(
            listOf(
                "Test Book | Vilna 1880 => וילנא (מתוקן)",   // book row over Sefaria Hebrew, English key
                "Warsaw 1900 => ורשא",                       // global fills an empty title
                "ספר יחיד | Solo Edition => מהדורה יחידה",   // metadata-only row, Hebrew key
                "Blocked 1800 => חסום",                      // blacklisted version: no row, so unmatched
                "Missing 2000 => חסר",                       // no such version
            )
        )
        // Blacklist still keys on Sefaria's original Hebrew name.
        val logger = Logger.withTag("SefariaVersionTitlesHeTest")
        val blacklist = parseVersionsBlacklist(listOf("ספר בדיקה | בלוק תק\"ס"), logger)

        val capture = Capture()
        val mapped = importFixture(fixture, blacklist, titles, capturingLogger(capture))
        val baseline = importFixture(fixture, blacklist, VersionHebrewTitles.Empty, logger)

        assertEquals(
            listOf(
                listOf("Solo Edition", "מהדורה יחידה"),
                listOf("Vilna 1880", "וילנא (מתוקן)"),
                listOf("Warsaw 1900", "ורשא"),
            ),
            mapped.map { listOf(it[1], it[2]) },
        )
        assertEquals(
            listOf(listOf("Solo Edition", null), listOf("Vilna 1880", "וילנא תר\"ם"), listOf("Warsaw 1900", null)),
            baseline.map { listOf(it[1], it[2]) },
        )
        // Delta safety: same ids either way.
        assertEquals(baseline.map { it[0] to it[1] }, mapped.map { it[0] to it[1] })
        assertEquals(
            listOf(
                "version_titles_he.txt:4: row matched no imported version: Blocked 1800 => חסום",
                "version_titles_he.txt:5: row matched no imported version: Missing 2000 => חסר",
            ),
            capture.warnings().filter { it.startsWith("version_titles_he.txt") },
        )
        assertTrue(
            "Version Hebrew titles: rows=5, applied=3, unmatched=2, versionsRetitled=3" in capture.infos(),
            capture.infos().toString(),
        )
    }

    private fun writeFixture(): Pair<java.nio.file.Path, java.nio.file.Path> {
        val tempDir = Files.createTempDirectory("seforim-version-titles-he")
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        fun schema(en: String, he: String) = """
            |{
            |  "schema": {
            |    "title": "$en",
            |    "heTitle": "$he",
            |    "sectionNames": ["Paragraph"],
            |    "heSectionNames": ["פסקה"],
            |    "addressTypes": ["Integer"],
            |    "depth": 1
            |  },
            |  "heCategories": ["תנך"]
            |}
            """.trimMargin()

        val bookDir = Files.createDirectories(jsonDir.resolve("Test Book"))
        Files.writeString(schemaDir.resolve("Test_Book.json"), schema("Test Book", "ספר בדיקה"))
        Files.writeString(
            bookDir.resolve("merged.json"),
            """{"title": "Test Book", "heTitle": "ספר בדיקה", "language": "he", "text": ["פסקה"],
              |"versions": [["Vilna 1880", null], ["Warsaw 1900", null], ["Blocked 1800", null]]}""".trimMargin()
        )
        listOf(
            Triple("Vilna 1880", "וילנא תר\\\"ם", "פסקה וילנא"),
            Triple("Warsaw 1900", null, "פסקה ורשא"),
            Triple("Blocked 1800", "בלוק תק\\\"ס", "פסקה חסומה"),
        ).forEach { (title, heTitle, text) ->
            val heField = heTitle?.let { ", \"versionTitleInHebrew\": \"$it\"" } ?: ""
            Files.writeString(
                bookDir.resolve("$title.json"),
                """{"title": "Test Book", "language": "he", "actualLanguage": "he",
                  |"versionTitle": "$title"$heField, "text": ["$text"]}""".trimMargin()
            )
        }

        val soloDir = Files.createDirectories(jsonDir.resolve("Solo Book"))
        Files.writeString(schemaDir.resolve("Solo_Book.json"), schema("Solo Book", "ספר יחיד"))
        Files.writeString(
            soloDir.resolve("merged.json"),
            """{"title": "Solo Book", "heTitle": "ספר יחיד", "language": "he", "text": ["פסקה"],
              |"versions": [["Solo Edition", null]]}""".trimMargin()
        )
        return jsonDir to schemaDir
    }

    /** Returns book_version rows as [id, versionTitle, heVersionTitle], ordered by versionTitle. */
    private suspend fun importFixture(
        fixture: Pair<java.nio.file.Path, java.nio.file.Path>,
        blacklist: VersionsBlacklist,
        titles: VersionHebrewTitles,
        logger: Logger,
    ): List<List<String?>> {
        val (jsonDir, schemaDir) = fixture
        val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
        val reader = SefariaBookPayloadReader(json, logger)
        val payloads = reader.readBooksInParallel(jsonDir, schemaDir, reader.buildSchemaLookup(schemaDir))
            .sortedBy { it.enTitle }

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        try {
            val sourceId = repo.insertSource("Sefaria-Test")
            val catId = repo.insertCategory(Category(0, null, "תנך", level = 0, order = 1))
            val lineKeyToId = mutableMapOf<Pair<String, Int>, Long>()
            val inputs = payloads.mapIndexed { bookIdx, payload ->
                val bookId = (bookIdx + 1).toLong()
                val bookPath = buildBookPath(payload.categoriesHe, payload.heTitle)
                repo.insertBook(
                    Book(
                        id = bookId, categoryId = catId, sourceId = sourceId,
                        title = payload.heTitle, heRef = payload.heTitle,
                        authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
                        heShortDesc = null, notesContent = null, order = bookId.toFloat(),
                        topics = emptyList(), isBaseBook = false, totalLines = payload.lines.size,
                        hasAltStructures = false, hasTeamim = false, hasNekudot = false,
                    )
                )
                repo.insertLinesBatch(
                    payload.lines.mapIndexed { idx, content ->
                        val lineId = bookId * 100 + idx
                        lineKeyToId[bookPath to idx] = lineId
                        Line(id = lineId, bookId = bookId, lineIndex = idx, content = content, heRef = null)
                    }
                )
                SefariaVersionsImporter.BookInput(payload = payload, bookId = bookId, bookPath = bookPath)
            }
            SefariaVersionsImporter(
                repo, InMemoryIdAllocator.load(path = null), json, reader, logger, blacklist, titles,
            ).import(inputs, lineKeyToId)

            val out = mutableListOf<List<String?>>()
            driver.getConnection().createStatement().use { st ->
                st.executeQuery("SELECT id, versionTitle, heVersionTitle FROM book_version ORDER BY versionTitle")
                    .use { rs -> while (rs.next()) out += listOf(rs.getString(1), rs.getString(2), rs.getString(3)) }
            }
            return out
        } finally {
            repo.close()
        }
    }
}
