package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.LogWriter
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.StaticConfig
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** The extension columns of ForDB/book_info.csv (topics … proofread) and how seedAllMetadata applies them. */
class BookInfoExtensionsTest {

    private class Capture : LogWriter() {
        val lines = mutableListOf<Pair<Severity, String>>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += severity to message
        }

        fun warnings(): List<String> = lines.filter { it.first == Severity.Warn }.map { it.second }
    }

    private fun capturingLogger(capture: Capture) =
        Logger(StaticConfig(Severity.Verbose, listOf(capture)), "test")

    private val header =
        "bookName,authorName,generationName,subGenerationName,startYear,endYear," +
            "topics,pubDate,pubPlace,description,proofread"

    @Test
    fun `an 11-column file parses every field, including a multi-line description with quotes and commas`() {
        val lines = (
            "$header\n" +
                "\"אור החיים\",\"חיים בן עטר\",\"אחרונים\",\"\",\"1696\",\"1743\"," +
                "\"מוסר ; חסידות;;מוסר\",\"תרפ\"\"ה\",\"ונציה\"," +
                "\"שורה ראשונה, עם פסיק\nושורה \"\"שנייה\"\"\n\",\"1\"\n"
            ).lines()
        val records = parseBookInfo(lines)

        val record = records.single()
        assertEquals("אור החיים", record.bookName)
        assertEquals("אחרונים", record.generationName)
        assertEquals(
            BookInfoExtension(
                topics = listOf("מוסר", "חסידות"),
                pubDate = "תרפ\"ה",
                pubPlace = "ונציה",
                description = "שורה ראשונה, עם פסיק\nושורה \"שנייה\"",
                proofread = true,
            ),
            record.extension,
        )
        assertEquals(listOf("אור החיים" to "אחרונים"), parseBookInfoGenerations(lines, Logger.withTag("test")))
    }

    @Test
    fun `a 6-column file has empty extension fields`() {
        val records = parseBookInfo(
            listOf(
                "bookName,authorName,generationName,subGenerationName,startYear,endYear",
                "\"בראשית רבה\",\"\",\"חז\"\"ל\",\"אמוראים\",\"300\",\"500\"",
            ),
        )
        assertEquals(BookInfoExtension(), records.single().extension)
        assertTrue(bookInfoExtensions(records, Logger.withTag("test")).isEmpty())
    }

    @Test
    fun `any other header fails`() {
        val base = "bookName,authorName,generationName,subGenerationName,startYear,endYear"
        for (wrong in listOf(
            "$base,topics",
            "$base,topic,pubDate,pubPlace,description,proofread",
            "$base,pubDate,topics,pubPlace,description,proofread",
            "$base,topics,pubDate,pubPlace,description,proofread,extra",
        )) {
            val e = assertFailsWith<IllegalArgumentException>(wrong) { parseBookInfo(listOf(wrong)) }
            assertContains(e.message!!, "must start with the header")
        }
        val e = assertFailsWith<IllegalArgumentException> {
            parseBookInfo(listOf(header, "\"בראשית\",\"\",\"\",\"\",\"\",\"\""))
        }
        assertContains(e.message!!, "record 2 is malformed")
    }

    @Test
    fun `invalid extension values fail`() {
        fun row(topics: String = "", date: String = "", place: String = "", desc: String = "", proof: String = "") =
            listOf(header, "\"ס\",\"\",\"\",\"\",\"\",\"\",\"$topics\",\"$date\",\"$place\",\"$desc\",\"$proof\"")
        val bad = listOf(
            row(proof = "2"),
            row(topics = (1..11).joinToString(";") { "t$it" }),
            row(topics = "א".repeat(61)),
            row(date = "1".repeat(101)),
            row(place = "ירושלים\nתל אביב"),
            row(desc = "ד".repeat(4001)),
        )
        for (lines in bad) assertFailsWith<IllegalArgumentException>(lines[1]) { parseBookInfo(lines) }
    }

    @Test
    fun `lengths count code points like the library CI and the site`() {
        val astral = "📖"
        val line = "\"ס\",\"\",\"\",\"\",\"\",\"\",\"${astral.repeat(60)}\",\"\",\"\",\"${astral.repeat(4000)}\",\"\""
        val record = parseBookInfo(listOf(header, line)).single()
        assertEquals(listOf(astral.repeat(60)), record.extension.topics)
    }

    @Test
    fun `rows of one book that disagree keep the first non-empty value, with a warning`() {
        val capture = Capture()
        val records = parseBookInfo(
            listOf(
                header,
                "\"ספר\",\"א\",\"ראשונים\",\"\",\"\",\"\",\"\",\"1925\",\"וילנא\",\"\",\"\"",
                "\"ספר\",\"ב\",\"ראשונים\",\"\",\"\",\"\",\"הלכה\",\"1930\",\"וילנא\",\"תיאור\",\"\"",
            ),
        )
        val ext = bookInfoExtensions(records, capturingLogger(capture)).getValue("ספר")

        assertEquals(BookInfoExtension(listOf("הלכה"), "1925", "וילנא", "תיאור", false), ext)
        val warning = capture.warnings().single()
        assertContains(warning, "1 book field(s) differ")
        assertContains(warning, "'ספר' (pubDate)")
    }

    @Test
    fun `apply joins topics without duplicates, replaces pub links and overrides heDesc`() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val sourceId = repo.insertSource("Otzaria")
        val catId = repo.insertCategory(Category(0, null, "מוסר", level = 0, order = 1))
        val bookId = repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "מסילת ישרים"))
        val other = repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "אחר"))
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)
        val logger = Logger.withTag("test")
        repo.linkTopicToBook(bindings.upsertTopic("מוסר"), bookId)

        // seedAllMetadata's own pass first: all_metadata and sefaria_metadata_changes.
        applyMetadata(
            repo, bindings,
            mapOf(
                "מסילת ישרים" to BulkMetadata(listOf(1740, 1800), "אמשטרדם"),
                "אחר" to BulkMetadata(listOf(1740), "אמשטרדם"),
            ),
            mapOf("מסילת ישרים" to Description("קצר", "ישן")),
            emptyList(), logger,
        )
        val extensions = mapOf(
            "מסילת ישרים" to BookInfoExtension(
                topics = listOf("מוסר", "חסידות"),
                pubDate = "תק\"ס",
                pubPlace = "וילנא",
                description = "חדש\nשורה שנייה",
            ),
            "לא קיים" to BookInfoExtension(pubDate = "1900"),
        )
        val result = applyBookInfoExtensions(repo, bindings, extensions, emptyList(), logger)
        // Re-running must not duplicate anything.
        applyBookInfoExtensions(repo, bindings, extensions, emptyList(), logger)

        assertEquals(BookInfoExtensionResult(updated = 1, unmatched = 1), result)
        assertEquals(listOf("חסידות", "מוסר"), repo.getBook(bookId)!!.topics.map { it.name })
        assertEquals(listOf("תק\"ס"), repo.getBook(bookId)!!.pubDates.map { it.date })
        assertEquals(listOf("וילנא"), repo.getBook(bookId)!!.pubPlaces.map { it.name })
        assertEquals("חדש\nשורה שנייה", repo.getBook(bookId)!!.heDesc)
        assertEquals("קצר", repo.getBook(bookId)!!.heShortDesc)
        // Another book's links to the replaced rows stay.
        assertEquals(listOf("1740"), repo.getBook(other)!!.pubDates.map { it.date })
        assertEquals(listOf("אמשטרדם"), repo.getBook(other)!!.pubPlaces.map { it.name })
        repo.close()
    }

    @Test
    fun `empty extension fields leave the book untouched`() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val sourceId = repo.insertSource("Otzaria")
        val catId = repo.insertCategory(Category(0, null, "מוסר", level = 0, order = 1))
        val bookId = repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "ספר"))
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)
        val logger = Logger.withTag("test")
        applyMetadata(
            repo, bindings, mapOf("ספר" to BulkMetadata(listOf(1740), "אמשטרדם")),
            mapOf("ספר" to Description(null, "תיאור ספריא")), emptyList(), logger,
        )

        val result = applyBookInfoExtensions(
            repo, bindings, mapOf("ספר" to BookInfoExtension(proofread = true)), emptyList(), logger,
        )

        assertEquals(BookInfoExtensionResult(0, 0), result)
        val book = repo.getBook(bookId)!!
        assertEquals(listOf("1740"), book.pubDates.map { it.date })
        assertEquals(listOf("אמשטרדם"), book.pubPlaces.map { it.name })
        assertEquals("תיאור ספריא", book.heDesc)
        repo.close()
    }

    @Test
    fun `two builds of the same inputs write identical book-info rows and ids`() = runBlocking {
        val dir = Files.createTempDirectory("book-info-stable")
        val state = dir.resolve("build_state.json")
        suspend fun build(name: String, statePath: Path?): List<String> {
            val dbPath = dir.resolve("$name.db")
            val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")
            SeforimDb.Schema.create(driver)
            val repo = SeforimRepository(dbPath.toString(), driver)
            val allocator = InMemoryIdAllocator.load(statePath)
            val bindings = IdAllocatorBindings(allocator, repo)
            val sourceId = repo.insertSource("Otzaria")
            val catId = repo.insertCategory(Category(0, null, "מוסר", level = 0, order = 1))
            val bookId = repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "ספר"))
            repo.linkTopicToBook(bindings.upsertTopic("מוסר"), bookId)
            val logger = Logger.withTag("test")
            applyMetadata(repo, bindings, mapOf("ספר" to BulkMetadata(listOf(1740), "אמשטרדם")), emptyMap(), emptyList(), logger)
            val ext = mapOf("ספר" to BookInfoExtension(listOf("מוסר", "חסידות"), "תק\"ס", "וילנא", "תיאור"))
            applyBookInfoExtensions(repo, bindings, ext, emptyList(), logger)
            allocator.snapshotTo(state, emptyMap())
            repo.close()
            return DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
                listOf(
                    "SELECT id, date FROM pub_date", "SELECT bookId, pubDateId FROM book_pub_date",
                    "SELECT id, name FROM pub_place", "SELECT bookId, pubPlaceId FROM book_pub_place",
                    "SELECT id, name FROM topic", "SELECT bookId, topicId FROM book_topic",
                    "SELECT id, heDesc FROM book",
                ).flatMap { sql ->
                    conn.createStatement().use { st ->
                        st.executeQuery("$sql ORDER BY 1, 2").use { rs ->
                            buildList { while (rs.next()) add("$sql: ${rs.getString(1)}|${rs.getString(2)}") }
                        }
                    }
                }
            }
        }

        val first = build("v1", null)
        assertEquals(first, build("v2", state))
    }
}
