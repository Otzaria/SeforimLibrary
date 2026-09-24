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
import java.nio.file.Files
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * End-to-end versions import: per-version sibling files are walked with the
 * book's schema and joined to the book's lines by ref (hasContent=1); books
 * without sibling files get metadata-only rows from merged.json's `versions`
 * array (hasContent=0).
 */
class SefariaVersionsImportTest {
    @Test
    fun versionFilesAndMetadataOnlyRowsAreImported() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-versions")
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))

        val bookDir = Files.createDirectories(jsonDir.resolve("Test Book"))
        Files.writeString(
            schemaDir.resolve("Test_Book.json"),
            """
            |{
            |  "schema": {
            |    "title": "Test Book",
            |    "heTitle": "ספר בדיקה",
            |    "sectionNames": ["Chapter", "Verse"],
            |    "heSectionNames": ["פרק", "פסוק"],
            |    "addressTypes": ["Perek", "Pasuk"],
            |    "depth": 2
            |  },
            |  "heCategories": ["תנך"]
            |}
            """.trimMargin()
        )
        Files.writeString(
            bookDir.resolve("merged.json"),
            """
            |{
            |  "title": "Test Book",
            |  "heTitle": "ספר בדיקה",
            |  "language": "he",
            |  "versionTitle": "merged",
            |  "text": [["בראשית נוסח ממוזג", "והארץ נוסח ממוזג"], ["ויאמר נוסח ממוזג"]],
            |  "versions": [["Vilna 1880", "http://vilna"], ["Warsaw 1900", null]]
            |}
            """.trimMargin()
        )
        // Complete edition, full metadata (numeric priority).
        Files.writeString(
            bookDir.resolve("Vilna 1880.json"),
            """
            |{
            |  "title": "Test Book",
            |  "language": "he",
            |  "actualLanguage": "he",
            |  "versionTitle": "Vilna 1880",
            |  "versionSource": "http://vilna",
            |  "versionTitleInHebrew": "וילנא תר\"ם",
            |  "priority": 2,
            |  "license": "Public Domain",
            |  "text": [["בראשית נוסח וילנא", "והארץ נוסח וילנא"]]
            |}
            """.trimMargin()
        )
        // Partial edition: 1:1 empty (skipped), string priority.
        Files.writeString(
            bookDir.resolve("Warsaw 1900.json"),
            """
            |{
            |  "title": "Test Book",
            |  "language": "he",
            |  "actualLanguage": "he",
            |  "versionTitle": "Warsaw 1900",
            |  "priority": "1.5",
            |  "text": [["", "והארץ נוסח ורשא"], ["ויאמר נוסח ורשא"]]
            |}
            """.trimMargin()
        )

        // Single-version book: merged.json only → metadata-only row.
        val soloDir = Files.createDirectories(jsonDir.resolve("Solo Book"))
        Files.writeString(
            schemaDir.resolve("Solo_Book.json"),
            """
            |{
            |  "schema": {
            |    "title": "Solo Book",
            |    "heTitle": "ספר יחיד",
            |    "sectionNames": ["Paragraph"],
            |    "heSectionNames": ["פסקה"],
            |    "addressTypes": ["Integer"],
            |    "depth": 1
            |  },
            |  "heCategories": ["תנך"]
            |}
            """.trimMargin()
        )
        Files.writeString(
            soloDir.resolve("merged.json"),
            """
            |{
            |  "title": "Solo Book",
            |  "heTitle": "ספר יחיד",
            |  "language": "he",
            |  "versionTitle": "merged",
            |  "text": ["פסקה ראשונה", "פסקה שניה"],
            |  "versions": [["Solo Edition", "http://solo"]]
            |}
            """.trimMargin()
        )

        val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
        val logger = Logger.withTag("SefariaVersionsImportTest")
        val reader = SefariaBookPayloadReader(json, logger)
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payloads = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup)
            .sortedBy { it.enTitle }
        assertEquals(listOf("Solo Book", "Test Book"), payloads.map { it.enTitle })
        payloads.forEach { assertNotNull(it.sourceDirPath); assertNotNull(it.schemaFilePath) }
        assertEquals(
            listOf("Solo Edition" to "http://solo"),
            payloads[0].versionsMeta.map { it.title to it.source },
        )
        assertEquals(
            listOf("Vilna 1880" to "http://vilna", "Warsaw 1900" to null),
            payloads[1].versionsMeta.map { it.title to it.source },
        )

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
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
            val refsByLineIndex = payload.refEntries.associateBy { it.lineIndex - 1 }
            repo.insertLinesBatch(
                payload.lines.mapIndexed { idx, content ->
                    val lineId = bookId * 100 + idx
                    lineKeyToId[bookPath to idx] = lineId
                    Line(
                        id = lineId, bookId = bookId, lineIndex = idx,
                        content = content, heRef = refsByLineIndex[idx]?.heRef,
                    )
                }
            )
            SefariaVersionsImporter.BookInput(payload = payload, bookId = bookId, bookPath = bookPath)
        }

        SefariaVersionsImporter(repo, InMemoryIdAllocator.load(path = null), json, reader, logger)
            .import(inputs, lineKeyToId)

        fun query(sql: String): List<List<String?>> {
            val conn: Connection = driver.getConnection()
            conn.createStatement().use { st ->
                st.executeQuery(sql).use { rs ->
                    val out = mutableListOf<List<String?>>()
                    val cols = rs.metaData.columnCount
                    while (rs.next()) out += (1..cols).map { rs.getString(it) }
                    return out
                }
            }
        }

        // Test Book (bookId 2): two full editions. Solo Book (bookId 1): metadata-only.
        assertEquals(
            listOf(
                listOf("1", "Solo Edition", null, "http://solo", null, null, "0"),
                listOf("2", "Vilna 1880", "וילנא תר\"ם", "http://vilna", "2.0", "Public Domain", "1"),
                listOf("2", "Warsaw 1900", null, null, "1.5", null, "1"),
            ),
            query(
                "SELECT bookId, versionTitle, heVersionTitle, versionSource, priority, license, hasContent " +
                    "FROM book_version ORDER BY bookId, versionTitle"
            ),
        )

        // Merged walk of Test Book: [h1, h2 פרק א, (א) 1:1, (ב) 1:2, h2 פרק ב, 2:1]
        // → line ids 200..205. Vilna joins 1:1+1:2; Warsaw joins 1:2+2:1 (1:1 empty).
        // Warsaw's chapter 1 has a single non-empty verse → no "(letter)" prefix.
        assertEquals(
            listOf(
                listOf("Vilna 1880", "202", "(א) בראשית נוסח וילנא"),
                listOf("Vilna 1880", "203", "(ב) והארץ נוסח וילנא"),
                listOf("Warsaw 1900", "203", "והארץ נוסח ורשא"),
                listOf("Warsaw 1900", "205", "ויאמר נוסח ורשא"),
            ),
            query(
                "SELECT bv.versionTitle, vl.lineId, vl.content FROM version_line vl " +
                    "JOIN book_version bv ON bv.id = vl.versionId ORDER BY bv.versionTitle, vl.lineId"
            ),
        )

        // charCount counts visible chars of the stored (formatted) content.
        assertEquals(
            listOf(listOf("202", "(א) בראשית נוסח וילנא".length.toString())),
            query("SELECT lineId, charCount FROM version_line WHERE lineId = 202"),
        )

        repo.close()
    }

    @Test
    fun versionsBlacklistParsingAndMatching() {
        val logger = Logger.withTag("SefariaVersionsImportTest")
        val blacklist = parseVersionsBlacklist(
            listOf(
                "Version of the Rashash",
                "נוסחת הרש\"ש",
                "משנה ביכורים | נוסחת הגמרא -- מנוקד",
                "Mishnah Bikkurim | Version of the Gemara - vocalized",
                " | broken entry",
            ),
            logger,
        )
        assertEquals(2, blacklist.globalKeys.size)
        assertEquals(2, blacklist.perBookKeys.values.sumOf { it.size })

        val bookKeys = setOf("משנה ביכורים", "mishnah bikkurim")
        // Global: any book; quotes/case-insensitive via normalizeTitleKey.
        assertEquals(
            true,
            blacklist.isBlocked(setOf("ספר אחר"), setOfNotNull(normalizeTitleKey("נוסחת הרש''ש"))),
        )
        // Scoped: blocked only for the named book.
        val scopedVersion = setOfNotNull(normalizeTitleKey("Version of the Gemara - Vocalized"))
        assertEquals(true, blacklist.isBlocked(bookKeys, scopedVersion))
        assertEquals(false, blacklist.isBlocked(setOf("ספר אחר"), scopedVersion))
        // Unlisted version passes.
        assertEquals(
            false,
            blacklist.isBlocked(bookKeys, setOfNotNull(normalizeTitleKey("Torat Emet 357"))),
        )
    }

    @Test
    fun blacklistedVersionsAreNotImported() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-versions-blacklist")
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))

        val bookDir = Files.createDirectories(jsonDir.resolve("Test Book"))
        Files.writeString(
            schemaDir.resolve("Test_Book.json"),
            """
            |{
            |  "schema": {
            |    "title": "Test Book",
            |    "heTitle": "ספר בדיקה",
            |    "sectionNames": ["Paragraph"],
            |    "heSectionNames": ["פסקה"],
            |    "addressTypes": ["Integer"],
            |    "depth": 1
            |  },
            |  "heCategories": ["תנך"]
            |}
            """.trimMargin()
        )
        Files.writeString(
            bookDir.resolve("merged.json"),
            """
            |{
            |  "title": "Test Book",
            |  "heTitle": "ספר בדיקה",
            |  "language": "he",
            |  "versionTitle": "merged",
            |  "text": ["פסקה ראשונה"],
            |  "versions": [["Vilna 1880", null], ["Warsaw 1900", null]]
            |}
            """.trimMargin()
        )
        Files.writeString(
            bookDir.resolve("Vilna 1880.json"),
            """
            |{
            |  "title": "Test Book",
            |  "language": "he",
            |  "actualLanguage": "he",
            |  "versionTitle": "Vilna 1880",
            |  "versionTitleInHebrew": "וילנא תר\"ם",
            |  "text": ["פסקה נוסח וילנא"]
            |}
            """.trimMargin()
        )
        Files.writeString(
            bookDir.resolve("Warsaw 1900.json"),
            """
            |{
            |  "title": "Test Book",
            |  "language": "he",
            |  "actualLanguage": "he",
            |  "versionTitle": "Warsaw 1900",
            |  "text": ["פסקה נוסח ורשא"]
            |}
            """.trimMargin()
        )

        // Metadata-only book: one of its two meta rows is blocked globally.
        val soloDir = Files.createDirectories(jsonDir.resolve("Solo Book"))
        Files.writeString(
            schemaDir.resolve("Solo_Book.json"),
            """
            |{
            |  "schema": {
            |    "title": "Solo Book",
            |    "heTitle": "ספר יחיד",
            |    "sectionNames": ["Paragraph"],
            |    "heSectionNames": ["פסקה"],
            |    "addressTypes": ["Integer"],
            |    "depth": 1
            |  },
            |  "heCategories": ["תנך"]
            |}
            """.trimMargin()
        )
        Files.writeString(
            soloDir.resolve("merged.json"),
            """
            |{
            |  "title": "Solo Book",
            |  "heTitle": "ספר יחיד",
            |  "language": "he",
            |  "versionTitle": "merged",
            |  "text": ["פסקה ראשונה"],
            |  "versions": [["Solo Edition", null], ["Warsaw 1900", null]]
            |}
            """.trimMargin()
        )

        val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
        val logger = Logger.withTag("SefariaVersionsImportTest")
        val reader = SefariaBookPayloadReader(json, logger)
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payloads = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup)
            .sortedBy { it.enTitle }

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
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

        // Warsaw 1900 blocked everywhere; Vilna blocked only in ספר בדיקה, by its Hebrew title.
        val blacklist = parseVersionsBlacklist(
            listOf("Warsaw 1900", "ספר בדיקה | וילנא תר\"ם"),
            logger,
        )
        SefariaVersionsImporter(repo, InMemoryIdAllocator.load(path = null), json, reader, logger, blacklist)
            .import(inputs, lineKeyToId)

        val conn: Connection = driver.getConnection()
        val versions = mutableListOf<Pair<String, String>>()
        conn.createStatement().use { st ->
            st.executeQuery("SELECT bookId, versionTitle FROM book_version ORDER BY bookId, versionTitle").use { rs ->
                while (rs.next()) versions += rs.getString(1) to rs.getString(2)
            }
        }
        // Solo Book keeps only Solo Edition; Test Book loses both editions.
        assertEquals(listOf("1" to "Solo Edition"), versions)
        conn.createStatement().use { st ->
            st.executeQuery("SELECT COUNT(*) FROM version_line").use { rs ->
                rs.next()
                assertEquals(0, rs.getInt(1))
            }
        }

        repo.close()
    }

    /** Declared-version text is not the union of addresses — an unmatched segment fails loudly. */
    @Test
    fun declaredVersionMakesUnmatchedSegmentsFailLoudly() = runBlocking {
        // Control: no mapping → merged.json is the book text and both editions join.
        assertEquals(
            listOf(
                "Other 1900" to "נוסח אחר א",
                "Other 1900" to "נוסח אחר ב",
                "Preferred 1923" to "נוסח מוצהר א",
            ),
            importRefGapFixture(SefariaPreferredVersions.Empty),
        )

        val logger = Logger.withTag("SefariaVersionsImportTest")
        val error = assertFailsWith<PreferredVersionRefGapException> {
            importRefGapFixture(parsePreferredVersions(listOf("ספר בדיקה|$PREFERRED_FILE"), logger))
        }
        assertTrue(error.message!!.contains(PREFERRED_FILE), "error should name the declared file")
        assertTrue(error.message!!.contains("Other 1900"), "error should name the dropped edition")
    }

    /** Same fixture both ways: merged=2 segments, declared version=1, other edition=2. */
    private suspend fun importRefGapFixture(
        preferredVersions: SefariaPreferredVersions,
    ): List<Pair<String, String>> {
        val tempDir = Files.createTempDirectory("seforim-versions-refgap")
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Test Book"))

        Files.writeString(
            schemaDir.resolve("Test_Book.json"),
            """
            |{
            |  "schema": {
            |    "title": "Test Book",
            |    "heTitle": "ספר בדיקה",
            |    "sectionNames": ["Paragraph"],
            |    "heSectionNames": ["פסקה"],
            |    "addressTypes": ["Integer"],
            |    "depth": 1
            |  },
            |  "heCategories": ["תנך"]
            |}
            """.trimMargin()
        )
        Files.writeString(
            bookDir.resolve("merged.json"),
            """
            |{
            |  "title": "Test Book",
            |  "heTitle": "ספר בדיקה",
            |  "language": "he",
            |  "text": ["נוסח ממוזג א", "נוסח ממוזג ב"],
            |  "versions": [["Preferred 1923", null], ["Other 1900", null]]
            |}
            """.trimMargin()
        )
        Files.writeString(
            bookDir.resolve(PREFERRED_FILE),
            """
            |{
            |  "title": "Test Book",
            |  "heTitle": "ספר בדיקה",
            |  "language": "he",
            |  "actualLanguage": "he",
            |  "versionTitle": "Preferred 1923",
            |  "text": ["נוסח מוצהר א"]
            |}
            """.trimMargin()
        )
        Files.writeString(
            bookDir.resolve("Other 1900.json"),
            """
            |{
            |  "title": "Test Book",
            |  "heTitle": "ספר בדיקה",
            |  "language": "he",
            |  "actualLanguage": "he",
            |  "versionTitle": "Other 1900",
            |  "text": ["נוסח אחר א", "נוסח אחר ב"]
            |}
            """.trimMargin()
        )

        val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
        val logger = Logger.withTag("SefariaVersionsImportTest")
        val reader = SefariaBookPayloadReader(json, logger, preferredVersions = preferredVersions)
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(Category(0, null, "תנך", level = 0, order = 1))
        val bookPath = buildBookPath(payload.categoriesHe, payload.heTitle)
        repo.insertBook(
            Book(
                id = 1L, categoryId = catId, sourceId = sourceId,
                title = payload.heTitle, heRef = payload.heTitle,
                authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
                heShortDesc = null, notesContent = null, order = 1f,
                topics = emptyList(), isBaseBook = false, totalLines = payload.lines.size,
                hasAltStructures = false, hasTeamim = false, hasNekudot = false,
            )
        )
        val lineKeyToId = mutableMapOf<Pair<String, Int>, Long>()
        repo.insertLinesBatch(
            payload.lines.mapIndexed { idx, content ->
                val lineId = 100L + idx
                lineKeyToId[bookPath to idx] = lineId
                Line(id = lineId, bookId = 1L, lineIndex = idx, content = content, heRef = null)
            }
        )

        try {
            SefariaVersionsImporter(repo, InMemoryIdAllocator.load(path = null), json, reader, logger)
                .import(
                    listOf(SefariaVersionsImporter.BookInput(payload, bookId = 1L, bookPath = bookPath)),
                    lineKeyToId,
                )
            val stored = mutableListOf<Pair<String, String>>()
            val conn: Connection = driver.getConnection()
            conn.createStatement().use { st ->
                st.executeQuery(
                    "SELECT bv.versionTitle, vl.content FROM version_line vl " +
                        "JOIN book_version bv ON bv.id = vl.versionId ORDER BY bv.versionTitle, vl.lineId"
                ).use { rs ->
                    while (rs.next()) stored += rs.getString(1) to rs.getString(2)
                }
            }
            return stored
        } finally {
            repo.close()
        }
    }

    /**
     * שער השפה: `language` הוא "he" בכל קובץ מהדורה בייצוא, ולכן ההכרעה היא לפי
     * actualLanguage בלבד — עברית נכנסת, כל שפה אחרת נחסמת (יידיש בכלל זה).
     */
    @Test
    fun foreignLanguageVersionsAreSkippedByActualLanguage() = runBlocking {
        val (versionTitles, lines) = importLanguageFixture(
            listOf(
                Triple("Hebrew Edition", "he", "נוסח עברי"),
                Triple("Persian Edition", "fa", "נוסח פרסי"),
                Triple("Yiddish Edition", "yi", "נוסח יידיש"),
            )
        )
        // הפרסית והיידיש לא מקבלות אפילו שורת book_version.
        assertEquals(listOf("Hebrew Edition"), versionTitles)
        assertEquals(listOf("Hebrew Edition" to "נוסח עברי"), lines)
    }

    /** actualLanguage חסר — נפילה רועשת, בלי ניחוש שפה ובלי דילוג שקט. */
    @Test
    fun versionFileWithoutActualLanguageFailsLoudly() = runBlocking {
        val error = assertFailsWith<MissingVersionLanguageException> {
            importLanguageFixture(listOf(Triple("Unmarked Edition", null, "נוסח בלי שפה")))
        }
        assertTrue(error.message!!.contains("actualLanguage"), "error should name the missing field")
        assertTrue(error.message!!.contains("Unmarked Edition"), "error should name the version file")
    }

    /** מהדורה אחת לשורה במרכז הספר; `actualLanguage=null` פירושו שדה חסר בקובץ. */
    private suspend fun importLanguageFixture(
        versions: List<Triple<String, String?, String>>,
    ): Pair<List<String>, List<Pair<String, String>>> {
        val tempDir = Files.createTempDirectory("seforim-versions-language")
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Test Book"))

        Files.writeString(
            schemaDir.resolve("Test_Book.json"),
            """
            |{
            |  "schema": {
            |    "title": "Test Book",
            |    "heTitle": "ספר בדיקה",
            |    "sectionNames": ["Paragraph"],
            |    "heSectionNames": ["פסקה"],
            |    "addressTypes": ["Integer"],
            |    "depth": 1
            |  },
            |  "heCategories": ["תנך"]
            |}
            """.trimMargin()
        )
        Files.writeString(
            bookDir.resolve("merged.json"),
            """
            |{
            |  "title": "Test Book",
            |  "heTitle": "ספר בדיקה",
            |  "language": "he",
            |  "text": ["נוסח ממוזג"],
            |  "versions": [${versions.joinToString(", ") { "[\"${it.first}\", null]" }}]
            |}
            """.trimMargin()
        )
        versions.forEach { (title, actualLanguage, content) ->
            val actualLanguageLine = actualLanguage?.let { "\n|  \"actualLanguage\": \"$it\"," } ?: ""
            Files.writeString(
                bookDir.resolve("$title.json"),
                """
                |{
                |  "title": "Test Book",
                |  "heTitle": "ספר בדיקה",
                |  "language": "he",$actualLanguageLine
                |  "versionTitle": "$title",
                |  "text": ["$content"]
                |}
                """.trimMargin()
            )
        }

        val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
        val logger = Logger.withTag("SefariaVersionsImportTest")
        val reader = SefariaBookPayloadReader(json, logger)
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(Category(0, null, "תנך", level = 0, order = 1))
        val bookPath = buildBookPath(payload.categoriesHe, payload.heTitle)
        repo.insertBook(
            Book(
                id = 1L, categoryId = catId, sourceId = sourceId,
                title = payload.heTitle, heRef = payload.heTitle,
                authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
                heShortDesc = null, notesContent = null, order = 1f,
                topics = emptyList(), isBaseBook = false, totalLines = payload.lines.size,
                hasAltStructures = false, hasTeamim = false, hasNekudot = false,
            )
        )
        val lineKeyToId = mutableMapOf<Pair<String, Int>, Long>()
        repo.insertLinesBatch(
            payload.lines.mapIndexed { idx, content ->
                val lineId = 100L + idx
                lineKeyToId[bookPath to idx] = lineId
                Line(id = lineId, bookId = 1L, lineIndex = idx, content = content, heRef = null)
            }
        )

        try {
            SefariaVersionsImporter(repo, InMemoryIdAllocator.load(path = null), json, reader, logger)
                .import(
                    listOf(SefariaVersionsImporter.BookInput(payload, bookId = 1L, bookPath = bookPath)),
                    lineKeyToId,
                )
            val conn: Connection = driver.getConnection()
            val versionTitles = mutableListOf<String>()
            conn.createStatement().use { st ->
                st.executeQuery("SELECT versionTitle FROM book_version ORDER BY versionTitle").use { rs ->
                    while (rs.next()) versionTitles += rs.getString(1)
                }
            }
            val stored = mutableListOf<Pair<String, String>>()
            conn.createStatement().use { st ->
                st.executeQuery(
                    "SELECT bv.versionTitle, vl.content FROM version_line vl " +
                        "JOIN book_version bv ON bv.id = vl.versionId ORDER BY bv.versionTitle, vl.lineId"
                ).use { rs ->
                    while (rs.next()) stored += rs.getString(1) to rs.getString(2)
                }
            }
            return versionTitles to stored
        } finally {
            repo.close()
        }
    }

    private companion object {
        const val PREFERRED_FILE = "Preferred 1923.json"
    }
}
