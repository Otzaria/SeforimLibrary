package io.github.kdroidfilter.seforimlibrary.dao.repository

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Integration tests for [SeforimRepository].
 * Uses an in-memory SQLite database for isolation and speed.
 */
class SeforimRepositoryIntegrationTest {
    private lateinit var driver: JdbcSqliteDriver
    private lateinit var repository: SeforimRepository

    @BeforeTest
    fun setup() {
        driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        repository = SeforimRepository(":memory:", driver)
    }

    @AfterTest
    fun tearDown() {
        driver.close()
    }

    // ==================== Source Tests ====================

    @Test
    fun `insertSource creates new source and returns id`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        assertTrue(sourceId > 0)
    }

    @Test
    fun `insertSource returns existing id for duplicate source`() = runBlocking {
        val firstId = repository.insertSource("Sefaria")
        val secondId = repository.insertSource("Sefaria")
        assertEquals(firstId, secondId)
    }

    @Test
    fun `getSourceByName returns source when exists`() = runBlocking {
        val sourceName = "Otzaria"
        repository.insertSource(sourceName)

        val source = repository.getSourceByName(sourceName)

        assertNotNull(source)
        assertEquals(sourceName, source.name)
    }

    @Test
    fun `getSourceByName returns null when not exists`() = runBlocking {
        val source = repository.getSourceByName("NonExistent")
        assertNull(source)
    }

    // ==================== Category Tests ====================

    @Test
    fun `category inserts round-trip descriptions`() = runBlocking {
        val generatedId = repository.insertCategory(
            Category(title = "תנ״ך", heShortDesc = "קצר", heDesc = "ארוך"),
        )
        repository.insertCategoryWithId(
            id = 4_242,
            parentId = generatedId,
            title = "תרגומים",
            level = 1,
            orderIndex = 1,
            heShortDesc = "קצר בן",
            heDesc = "ארוך בן",
        )

        assertEquals("קצר", repository.getCategory(generatedId)?.heShortDesc)
        assertEquals("ארוך", repository.getCategory(generatedId)?.heDesc)
        assertEquals("קצר בן", repository.getCategory(4_242)?.heShortDesc)
        assertEquals("ארוך בן", repository.getCategory(4_242)?.heDesc)
    }

    @Test
    fun `category description rows build normalized full paths in one tree read`() = runBlocking {
        val root = repository.insertCategory(Category(title = " תנ\"ך "))
        val child = repository.insertCategory(Category(parentId = root, title = "תרגומים", level = 1))
        repository.insertCategory(Category(parentId = child, title = "אונקלוס", level = 2))

        assertEquals(
            setOf("תנ״ך", "תנ״ך/תרגומים", "תנ״ך/תרגומים/אונקלוס"),
            repository.getAllCategoryDescriptionRows().map { it.canonicalPath }.toSet(),
        )
    }

    @Test
    fun `category description rows reject broken trees`() = runBlocking {
        driver.execute(null, "INSERT INTO category (id,parentId,title) VALUES (900,901,'orphan')", 0)
        val missing = assertFailsWith<IllegalStateException> {
            repository.getAllCategoryDescriptionRows()
        }
        assertTrue(missing.message.orEmpty().contains("Missing parent"))

        driver.execute(null, "DELETE FROM category", 0)
        driver.execute(null, "INSERT INTO category (id,parentId,title) VALUES (900,NULL,'a')", 0)
        driver.execute(null, "INSERT INTO category (id,parentId,title) VALUES (901,900,'b')", 0)
        driver.execute(null, "UPDATE category SET parentId=901 WHERE id=900", 0)
        val cycle = assertFailsWith<IllegalStateException> {
            repository.getAllCategoryDescriptionRows()
        }
        assertTrue(cycle.message.orEmpty().contains("Cycle"))

        driver.execute(null, "DELETE FROM category", 0)
        driver.execute(null, "INSERT INTO category (id,parentId,title) VALUES (900,NULL,'same')", 0)
        driver.execute(null, "INSERT INTO category (id,parentId,title) VALUES (901,NULL,'same')", 0)
        val duplicate = assertFailsWith<IllegalStateException> {
            repository.getAllCategoryDescriptionRows()
        }
        assertTrue(duplicate.message.orEmpty().contains("Duplicate category path"))
    }

    @Test
    fun `category description batch is atomic and invalidates cache`() = runBlocking {
        val first = repository.insertCategory(Category(title = "first"))
        val second = repository.insertCategory(Category(title = "second"))
        assertNull(repository.getCategory(first)?.heShortDesc)

        repository.setCategoryDescriptionsBatch(
            listOf(
                CategoryDescriptionUpdate(first, "short", "long"),
                CategoryDescriptionUpdate(second, "short 2", null),
            ),
        )
        assertEquals("short", repository.getCategory(first)?.heShortDesc)

        driver.execute(
            null,
            "CREATE TRIGGER fail_second BEFORE UPDATE ON category " +
                "WHEN NEW.id=$second BEGIN SELECT RAISE(ABORT, 'test'); END",
            0,
        )
        assertFailsWith<Exception> {
            repository.setCategoryDescriptionsBatch(
                listOf(
                    CategoryDescriptionUpdate(first, "changed", null),
                    CategoryDescriptionUpdate(second, "changed", null),
                ),
            )
        }
        fun readDirect(categoryId: Long): Pair<String?, String?> = driver.executeQuery(
            identifier = null,
            sql = "SELECT heShortDesc, heDesc FROM category WHERE id = ?",
            mapper = { cursor: SqlCursor ->
                check(cursor.next().value)
                QueryResult.Value(cursor.getString(0) to cursor.getString(1))
            },
            parameters = 1,
        ) {
            bindLong(0, categoryId)
        }.value

        // Read through the driver, bypassing categoryCache, to prove SQLite
        // rolled back the first update when the trigger aborted the second.
        assertEquals("short" to "long", readDirect(first))
        assertEquals("short 2" to null, readDirect(second))
    }

    @Test
    fun `insertCategory creates root category`() = runBlocking {
        val category = Category(
            parentId = null,
            title = "Torah",
            level = 0,
            order = 1
        )

        val categoryId = repository.insertCategory(category)

        assertTrue(categoryId > 0)
    }

    @Test
    fun `insertCategory creates child category with parent`() = runBlocking {
        val parentCategory = Category(parentId = null, title = "Torah", level = 0, order = 1)
        val parentId = repository.insertCategory(parentCategory)

        val childCategory = Category(parentId = parentId, title = "Bereshit", level = 1, order = 1)
        val childId = repository.insertCategory(childCategory)

        assertTrue(childId > 0)
        assertNotEquals(parentId, childId)
    }

    @Test
    fun `insertCategory returns existing id for duplicate in same parent`() = runBlocking {
        val category = Category(parentId = null, title = "Torah", level = 0, order = 1)
        val firstId = repository.insertCategory(category)
        val secondId = repository.insertCategory(category)

        assertEquals(firstId, secondId)
    }

    @Test
    fun `getRootCategories returns only root categories`() = runBlocking {
        // Insert root categories
        repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        repository.insertCategory(Category(parentId = null, title = "Neviim", level = 0, order = 2))

        // Insert a child category
        val parentId = repository.insertCategory(Category(parentId = null, title = "Ketuvim", level = 0, order = 3))
        repository.insertCategory(Category(parentId = parentId, title = "Tehillim", level = 1, order = 1))

        val rootCategories = repository.getRootCategories()

        assertEquals(3, rootCategories.size)
        assertTrue(rootCategories.all { it.parentId == null })
    }

    @Test
    fun `getSubcategories returns children of parent`() = runBlocking {
        val parentId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        repository.insertCategory(Category(parentId = parentId, title = "Bereshit", level = 1, order = 1))
        repository.insertCategory(Category(parentId = parentId, title = "Shemot", level = 1, order = 2))
        repository.insertCategory(Category(parentId = parentId, title = "Vayikra", level = 1, order = 3))

        val subcategories = repository.getCategoryChildren(parentId)

        assertEquals(3, subcategories.size)
        assertTrue(subcategories.all { cat -> cat.parentId == parentId })
    }

    @Test
    fun `getCategory returns category by id`() = runBlocking {
        val title = "Torah"
        val categoryId = repository.insertCategory(Category(parentId = null, title = title, level = 0, order = 1))

        val category = repository.getCategory(categoryId)

        assertNotNull(category)
        assertEquals(title, category.title)
        assertEquals(categoryId, category.id)
    }

    @Test
    fun `getCategory returns null for non-existent id`() = runBlocking {
        val category = repository.getCategory(99999L)
        assertNull(category)
    }

    // ==================== Book Tests ====================

    @Test
    fun `insertBook creates new book`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))

        val book = Book(
            categoryId = categoryId,
            sourceId = sourceId,
            title = "Bereshit",
            order = 1f
        )
        val bookId = repository.insertBook(book)

        assertTrue(bookId > 0)
    }

    @Test
    fun `getBook returns book with all metadata`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val book = Book(
            categoryId = categoryId,
            sourceId = sourceId,
            title = "Bereshit",
            order = 1f,
            heShortDesc = "בראשית"
        )
        val bookId = repository.insertBook(book)

        val retrievedBook = repository.getBook(bookId)

        assertNotNull(retrievedBook)
        assertEquals("Bereshit", retrievedBook.title)
        assertEquals("בראשית", retrievedBook.heShortDesc)
        assertEquals(categoryId, retrievedBook.categoryId)
    }

    @Test
    fun `getBook returns null for non-existent id`() = runBlocking {
        val book = repository.getBook(99999L)
        assertNull(book)
    }

    @Test
    fun `getBookCore returns lightweight book data`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Shemot", order = 2f)
        )

        val book = repository.getBookCore(bookId)

        assertNotNull(book)
        assertEquals("Shemot", book.title)
    }

    @Test
    fun `getBooksByCategory returns books in category`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))

        repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f))
        repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Shemot", order = 2f))
        repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Vayikra", order = 3f))

        val books = repository.getBooksByCategory(categoryId)

        assertEquals(3, books.size)
        assertTrue(books.all { it.categoryId == categoryId })
    }

    @Test
    fun `updateBookTotalLines updates line count`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        repository.updateBookTotalLines(bookId, 1533)

        val book = repository.getBook(bookId)
        assertNotNull(book)
        assertEquals(1533, book.totalLines)
    }

    // ==================== Line Tests ====================

    @Test
    fun `insertLine creates new line`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        val line = Line(
            bookId = bookId,
            lineIndex = 0,
            content = "<p>בראשית ברא אלהים את השמים ואת הארץ</p>"
        )
        val lineId = repository.insertLine(line)

        assertTrue(lineId > 0)
    }

    @Test
    fun `getLine returns line by id`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )
        val content = "<p>בראשית ברא אלהים</p>"
        val lineId = repository.insertLine(Line(bookId = bookId, lineIndex = 0, content = content))

        val line = repository.getLine(lineId)

        assertNotNull(line)
        assertEquals(content, line.content)
        assertEquals(bookId, line.bookId)
    }

    @Test
    fun `getLine returns null for non-existent id`() = runBlocking {
        val line = repository.getLine(99999L)
        assertNull(line)
    }

    @Test
    fun `getLines returns lines for book within range`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        repository.insertLine(Line(bookId = bookId, lineIndex = 0, content = "Line 0"))
        repository.insertLine(Line(bookId = bookId, lineIndex = 1, content = "Line 1"))
        repository.insertLine(Line(bookId = bookId, lineIndex = 2, content = "Line 2"))

        val lines = repository.getLines(bookId, startIndex = 0, endIndex = 3)

        assertEquals(3, lines.size)
        assertTrue(lines.all { it.bookId == bookId })
    }

    @Test
    fun `getLines returns paginated lines`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        // Insert 10 lines
        repeat(10) { i ->
            repository.insertLine(Line(bookId = bookId, lineIndex = i, content = "Line $i"))
        }

        // Get first range (lineIndex 0-4)
        val firstRange = repository.getLines(bookId, startIndex = 0, endIndex = 4)
        assertTrue(firstRange.isNotEmpty())
        assertTrue(firstRange.all { it.lineIndex < 5 })

        // Get second range (lineIndex 5-9)
        val secondRange = repository.getLines(bookId, startIndex = 5, endIndex = 9)
        assertTrue(secondRange.isNotEmpty())
        assertTrue(secondRange.all { it.lineIndex >= 5 })

        // Verify no overlap by line index
        val firstRangeIndices = firstRange.map { it.lineIndex }.toSet()
        val secondRangeIndices = secondRange.map { it.lineIndex }.toSet()
        assertTrue(firstRangeIndices.intersect(secondRangeIndices).isEmpty())
    }

    @Test
    fun `line char count roundtrip via insertLine`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )
        repository.insertLine(Line(bookId = bookId, lineIndex = 0, content = "a", charCount = 1))
        repository.insertLine(Line(bookId = bookId, lineIndex = 1, content = "bcd", charCount = 3))
        repository.insertLine(Line(bookId = bookId, lineIndex = 2, content = "ef", charCount = 2))

        val lines = repository.getLines(bookId, startIndex = 0, endIndex = 2)
        assertEquals(listOf(1, 3, 2), lines.map { it.charCount })
    }

    // ==================== TOC Tests ====================

    @Test
    fun `insertTocEntry creates new toc entry`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        val tocEntry = TocEntry(
            bookId = bookId,
            parentId = null,
            text = "Chapter 1",
            level = 0,
            hasChildren = false
        )
        val tocId = repository.insertTocEntry(tocEntry)

        assertTrue(tocId > 0)
    }

    @Test
    fun `getTocEntriesForBook returns entries for book`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        // Insert entries
        repository.insertTocEntry(TocEntry(bookId = bookId, parentId = null, text = "Chapter 1", level = 0, hasChildren = true))
        repository.insertTocEntry(TocEntry(bookId = bookId, parentId = null, text = "Chapter 2", level = 0, hasChildren = true))

        val entries = repository.getTocEntriesForBook(bookId)

        assertEquals(2, entries.size)
        assertTrue(entries.all { it.bookId == bookId })
    }

    @Test
    fun `getTocChildren returns children of parent entry`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        val parentId = repository.insertTocEntry(
            TocEntry(bookId = bookId, parentId = null, text = "Chapter 1", level = 0, hasChildren = true)
        )
        repository.insertTocEntry(TocEntry(bookId = bookId, parentId = parentId, text = "Verse 1", level = 1, hasChildren = false))
        repository.insertTocEntry(TocEntry(bookId = bookId, parentId = parentId, text = "Verse 2", level = 1, hasChildren = false))
        repository.insertTocEntry(TocEntry(bookId = bookId, parentId = parentId, text = "Verse 3", level = 1, hasChildren = false))

        val children = repository.getTocChildren(parentId)

        assertEquals(3, children.size)
        assertTrue(children.all { it.parentId == parentId })
    }

    // ==================== Category Closure Tests ====================

    @Test
    fun `rebuildCategoryClosure creates self and ancestor pairs`() = runBlocking {
        // Create a hierarchy: Torah -> Bereshit -> Chapters
        val torahId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bereshitId = repository.insertCategory(Category(parentId = torahId, title = "Bereshit", level = 1, order = 1))
        val chaptersId = repository.insertCategory(Category(parentId = bereshitId, title = "Chapters", level = 2, order = 1))

        repository.rebuildCategoryClosure()

        // Verify hierarchy works by getting books under ancestor category
        val sourceId = repository.insertSource("Test")
        repository.insertBook(Book(categoryId = chaptersId, sourceId = sourceId, title = "Test Book", order = 1f))

        val booksUnderTorah = repository.getBooksUnderCategoryTree(torahId)
        assertEquals(1, booksUnderTorah.size)
    }

    // ==================== Transaction Tests ====================

    @Test
    fun `runInTransaction executes block and returns result`() = runBlocking {
        val result = repository.runInTransaction {
            val sourceId = repository.insertSource("Test")
            val categoryId = repository.insertCategory(Category(parentId = null, title = "Test", level = 0, order = 1))
            repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Test Book", order = 1f))
        }

        assertTrue(result > 0)
    }

    // ==================== Max ID Tests ====================

    @Test
    fun `getMaxBookId returns 0 when no books exist`() = runBlocking {
        val maxId = repository.getMaxBookId()
        assertEquals(0L, maxId)
    }

    @Test
    fun `getMaxBookId returns highest book id`() = runBlocking {
        val sourceId = repository.insertSource("Test")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Test", level = 0, order = 1))

        repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Book 1", order = 1f))
        repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Book 2", order = 2f))
        val lastBookId = repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Book 3", order = 3f))

        val maxId = repository.getMaxBookId()

        assertEquals(lastBookId, maxId)
    }

    @Test
    fun `getMaxLineId returns 0 when no lines exist`() = runBlocking {
        val maxId = repository.getMaxLineId()
        assertEquals(0L, maxId)
    }

    // ==================== Link Ordering Tests ====================

    // Regression guard for Zayit issue #415: commentary links must be returned in the
    // natural order of their target line (e.g. Bartenura segments 1→N on a single Mishna),
    // not in link insertion order.
    @Test
    fun `getCommentariesForLineRange returns links sorted by targetLineIndex`() = runBlocking {
        val sourceId = repository.insertSource("Test")
        val mishnahCategoryId = repository.insertCategory(
            Category(parentId = null, title = "Mishnah", level = 0, order = 1)
        )
        val mishnahBookId = repository.insertBook(
            Book(categoryId = mishnahCategoryId, sourceId = sourceId, title = "Avot", order = 1f)
        )
        val bartenuraBookId = repository.insertBook(
            Book(categoryId = mishnahCategoryId, sourceId = sourceId, title = "Bartenura on Avot", order = 2f)
        )

        val mishnaLineId = repository.insertLine(
            Line(bookId = mishnahBookId, lineIndex = 0, content = "Mishna Avot 1:1")
        )

        val bartenuraSegmentIds = (0 until 5).map { idx ->
            repository.insertLine(
                Line(bookId = bartenuraBookId, lineIndex = idx, content = "Segment $idx")
            )
        }

        // Insert links in a deliberately shuffled order (4, 2, 0, 3, 1) to prove the
        // ordering comes from targetLineIndex, not insertion order.
        val shuffledOrder = listOf(4, 2, 0, 3, 1)
        for (idx in shuffledOrder) {
            repository.insertLink(
                Link(
                    sourceBookId = mishnahBookId,
                    targetBookId = bartenuraBookId,
                    sourceLineId = mishnaLineId,
                    targetLineId = bartenuraSegmentIds[idx],
                    targetLineIndex = idx,
                    connectionType = ConnectionType.COMMENTARY
                )
            )
        }

        val commentaries = repository.getCommentariesForLineRange(
            lineIds = listOf(mishnaLineId),
            connectionTypes = setOf(ConnectionType.COMMENTARY),
            offset = 0,
            limit = 100
        )

        assertEquals(5, commentaries.size)
        assertEquals(listOf(0, 1, 2, 3, 4), commentaries.map { it.link.targetLineIndex })
        assertEquals(
            listOf("Segment 0", "Segment 1", "Segment 2", "Segment 3", "Segment 4"),
            commentaries.map { it.targetText }
        )
    }

    @Test
    fun `link baseProvenance round-trips through insert and read`() = runBlocking {
        val sourceId = repository.insertSource("Test")
        val categoryId = repository.insertCategory(
            Category(parentId = null, title = "Cat", level = 0, order = 1)
        )
        val baseBookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Base", order = 1f)
        )
        val depBookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Dep", order = 2f)
        )
        val baseLineId = repository.insertLine(Line(bookId = baseBookId, lineIndex = 0, content = "base"))
        val depLineId = repository.insertLine(Line(bookId = depBookId, lineIndex = 0, content = "dep"))

        val linkId = repository.insertLink(
            Link(
                sourceBookId = baseBookId,
                targetBookId = depBookId,
                sourceLineId = baseLineId,
                targetLineId = depLineId,
                targetLineIndex = 0,
                connectionType = ConnectionType.COMMENTARY,
                baseProvenance = 2,
            )
        )

        assertEquals(2, repository.getLink(linkId)?.baseProvenance)
    }

    // Exercises the REAL selectInverseLinksByTargetLineIds mirror query: links are
    // inserted in provenance order 0,1,2 with every tie-breaker (isBaseBook,
    // orderIndex) stacked toward that insertion order, so the [2,1,0] output can
    // only come from `ORDER BY l.baseProvenance DESC` in LinkQueries.sq.
    @Test
    fun `SOURCE view orders by baseProvenance DESC ahead of all tie-breakers`() = runBlocking {
        val sourceId = repository.insertSource("Test")
        val categoryId = repository.insertCategory(
            Category(parentId = null, title = "Cat", level = 0, order = 1)
        )
        // Base books: provenance-0 book gets the STRONGEST tie-breakers
        // (isBaseBook=true, lowest orderIndex); provenance-2 the weakest.
        val noneBookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "None", order = 1f, isBaseBook = true)
        )
        val inferredBookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Inferred", order = 2f, isBaseBook = false)
        )
        val declaredBookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Declared", order = 3f, isBaseBook = false)
        )
        val depBookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Dep", order = 4f, isBaseBook = false)
        )
        val noneLineId = repository.insertLine(Line(bookId = noneBookId, lineIndex = 0, content = "none"))
        val inferredLineId = repository.insertLine(Line(bookId = inferredBookId, lineIndex = 0, content = "inferred"))
        val declaredLineId = repository.insertLine(Line(bookId = declaredBookId, lineIndex = 0, content = "declared"))
        val depLineId = repository.insertLine(Line(bookId = depBookId, lineIndex = 0, content = "dep"))

        // Deliberately REVERSED provenance insertion order: none(0), inferred(1), declared(2).
        for ((bookId, lineId, provenance) in listOf(
            Triple(noneBookId, noneLineId, 0),
            Triple(inferredBookId, inferredLineId, 1),
            Triple(declaredBookId, declaredLineId, 2),
        )) {
            repository.insertLink(
                Link(
                    sourceBookId = bookId,
                    targetBookId = depBookId,
                    sourceLineId = lineId,
                    targetLineId = depLineId,
                    targetLineIndex = 0,
                    connectionType = ConnectionType.COMMENTARY,
                    baseProvenance = provenance,
                )
            )
        }

        // includeSources=true routes through selectInverseLinksByTargetLineIds.
        val sources = repository.getCommentariesForLines(
            lineIds = listOf(depLineId),
            includeSources = true,
        )

        assertEquals(3, sources.size)
        assertEquals(
            listOf(declaredBookId, inferredBookId, noneBookId),
            sources.map { it.link.targetBookId },
            "SOURCE view must return provenance sequence [2,1,0] (declared, inferred, none)"
        )
    }

    // Plan commit 9: selectCommentatorsByBook now spans the full dependent group,
    // so a MIDRASH link (not just COMMENTARY/TARGUM) must surface as a commentator.
    @Test
    fun `getAvailableCommentators includes MIDRASH-typed links`() = runBlocking {
        val sourceId = repository.insertSource("Test")
        val catId = repository.insertCategory(Category(parentId = null, title = "C", level = 0, order = 1))
        val baseBookId = repository.insertBook(
            Book(categoryId = catId, sourceId = sourceId, title = "Genesis", order = 1f)
        )
        val midrashBookId = repository.insertBook(
            Book(categoryId = catId, sourceId = sourceId, title = "Bereshit Rabbah", order = 2f)
        )
        val baseLineId = repository.insertLine(Line(bookId = baseBookId, lineIndex = 0, content = "Genesis 1:1"))
        val midrashLineId = repository.insertLine(Line(bookId = midrashBookId, lineIndex = 0, content = "Midrash on 1:1"))

        repository.insertLink(
            Link(
                sourceBookId = baseBookId,
                targetBookId = midrashBookId,
                sourceLineId = baseLineId,
                targetLineId = midrashLineId,
                targetLineIndex = 0,
                connectionType = ConnectionType.MIDRASH,
            )
        )

        val commentators = repository.getAvailableCommentators(baseBookId)
        assertEquals(1, commentators.size)
        assertEquals(midrashBookId, commentators.first().bookId)
        assertEquals("Bereshit Rabbah", commentators.first().title)
    }

    // ==================== Virtual SOURCE view tests ====================

    // Validates the single-direction storage + virtual SOURCE view contract:
    // a stored COMMENTARY link base→dep MUST be visible both as a COMMENTARY
    // outgoing from the base line and as a SOURCE incoming to the dep line.
    @Test
    fun `SOURCE view returns swapped commentary link from the dependant side`() = runBlocking {
        val sourceId = repository.insertSource("Test")
        val catId = repository.insertCategory(Category(parentId = null, title = "C", level = 0, order = 1))
        val baseBookId = repository.insertBook(
            Book(categoryId = catId, sourceId = sourceId, title = "Genesis", order = 1f, isBaseBook = true)
        )
        val depBookId = repository.insertBook(
            Book(categoryId = catId, sourceId = sourceId, title = "Rashi", order = 2f, isBaseBook = false)
        )
        val baseLineId = repository.insertLine(Line(bookId = baseBookId, lineIndex = 0, content = "Genesis 1:1"))
        val depLineId = repository.insertLine(Line(bookId = depBookId, lineIndex = 0, content = "Rashi on 1:1"))

        repository.insertLink(
            Link(
                sourceBookId = baseBookId,
                targetBookId = depBookId,
                sourceLineId = baseLineId,
                targetLineId = depLineId,
                targetLineIndex = 0,
                connectionType = ConnectionType.COMMENTARY,
            )
        )

        // Forward: from base line, the dep is a COMMENTARY.
        val commentaries = repository.getCommentariesForLineRange(
            lineIds = listOf(baseLineId),
            connectionTypes = setOf(ConnectionType.COMMENTARY),
            offset = 0,
            limit = 10,
        )
        assertEquals(1, commentaries.size)
        assertEquals(depBookId, commentaries.first().link.targetBookId)
        assertEquals(ConnectionType.COMMENTARY, commentaries.first().link.connectionType)

        // Reverse virtual: from dep line, the base appears as a SOURCE with
        // src/tgt swapped so the consumer sees the base book in `targetBookId`.
        val sources = repository.getCommentariesForLineRange(
            lineIds = listOf(depLineId),
            connectionTypes = setOf(ConnectionType.SOURCE),
            offset = 0,
            limit = 10,
        )
        assertEquals(1, sources.size)
        assertEquals(ConnectionType.SOURCE, sources.first().link.connectionType)
        assertEquals(depBookId, sources.first().link.sourceBookId)
        assertEquals(baseBookId, sources.first().link.targetBookId)
        assertEquals("Genesis 1:1", sources.first().targetText)
    }

    @Test
    fun `recomputeHasSourceConnection flags FOOTNOTES targets and clears stale flags`() = runBlocking {
        val sourceId = repository.insertSource("Test")
        val catId = repository.insertCategory(Category(parentId = null, title = "C", level = 0, order = 1))
        fun book(title: String, order: Float) =
            Book(categoryId = catId, sourceId = sourceId, title = title, order = order)
        val baseId = repository.insertBook(book("Base", 1f))
        val notesId = repository.insertBook(book("Notes on Base", 2f))
        val quotingId = repository.insertBook(book("Quoting", 3f))
        val staleId = repository.insertBook(book("Stale", 4f))
        val baseLine = repository.insertLine(Line(bookId = baseId, lineIndex = 0, content = "b"))
        val notesLine = repository.insertLine(Line(bookId = notesId, lineIndex = 0, content = "n"))
        val quotingLine = repository.insertLine(Line(bookId = quotingId, lineIndex = 0, content = "q"))
        fun link(target: Long, targetLine: Long, type: ConnectionType) = Link(
            sourceBookId = baseId, targetBookId = target, sourceLineId = baseLine,
            targetLineId = targetLine, targetLineIndex = 0, connectionType = type,
        )
        repository.insertLink(link(notesId, notesLine, ConnectionType.FOOTNOTES))
        repository.insertLink(link(quotingId, quotingLine, ConnectionType.QUOTATION))
        repository.executeRawQuery("UPDATE book SET hasSourceConnection=1 WHERE id=$staleId")

        repository.recomputeHasSourceConnection()

        assertTrue(repository.getBookCore(notesId)!!.hasSourceConnection)
        assertFalse(repository.getBookCore(baseId)!!.hasSourceConnection)
        assertFalse(repository.getBookCore(quotingId)!!.hasSourceConnection)
        assertFalse(repository.getBookCore(staleId)!!.hasSourceConnection)
        // The flag must agree with the SOURCE view it advertises.
        val sources = repository.getCommentariesForLineRange(
            lineIds = listOf(notesLine),
            connectionTypes = setOf(ConnectionType.SOURCE),
            offset = 0,
            limit = 10,
        )
        assertEquals(listOf(baseId), sources.map { it.link.targetBookId })
    }

    @Test
    fun `mixing SOURCE with other types in a single query is rejected`() = runBlocking {
        try {
            repository.getCommentariesForLineRange(
                lineIds = listOf(1L),
                connectionTypes = setOf(ConnectionType.SOURCE, ConnectionType.COMMENTARY),
                offset = 0,
                limit = 10,
            )
            error("Expected IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message!!.contains("SOURCE"))
        }
    }

    // ==================== Max ID Tests (continued) ====================

    @Test
    fun `getMaxLineId returns highest line id`() = runBlocking {
        val sourceId = repository.insertSource("Test")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Test", level = 0, order = 1))
        val bookId = repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Book", order = 1f))

        repository.insertLine(Line(bookId = bookId, lineIndex = 0, content = "Line 1"))
        repository.insertLine(Line(bookId = bookId, lineIndex = 1, content = "Line 2"))
        val lastLineId = repository.insertLine(Line(bookId = bookId, lineIndex = 2, content = "Line 3"))

        val maxId = repository.getMaxLineId()

        assertEquals(lastLineId, maxId)
    }

    // ==================== Book dependence-metadata Tests ====================

    @Test
    fun `insertBook round-trips dependence metadata`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(
                categoryId = categoryId,
                sourceId = sourceId,
                title = "Rashi on Genesis",
                order = 1f,
                dependenceType = "Commentary",
                collectiveTitleHe = "רש\"י",
                collectiveTitleEn = "Rashi"
            )
        )

        val book = repository.getBook(bookId)
        assertNotNull(book)
        assertEquals("Commentary", book.dependenceType)
        assertEquals("רש\"י", book.collectiveTitleHe)
        assertEquals("Rashi", book.collectiveTitleEn)
    }

    @Test
    fun `insertBook leaves dependence metadata null by default`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Genesis", order = 1f)
        )

        val book = repository.getBook(bookId)
        assertNotNull(book)
        assertNull(book.dependenceType)
        assertNull(book.collectiveTitleHe)
        assertNull(book.collectiveTitleEn)
    }

    // ==================== book_base_text junction Tests ====================

    @Test
    fun `book_base_text select returns bases and dependents`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val catId = repository.insertCategory(Category(parentId = null, title = "C", level = 0, order = 1))
        val baseId = repository.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "Genesis", order = 1f))
        val depId = repository.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "Rashi", order = 2f))

        repository.insertBookBaseText(depId, baseId)

        val bases = repository.getBaseBooks(depId)
        assertEquals(1, bases.size)
        assertEquals(baseId, bases.first().id)

        val dependents = repository.getDependentBooks(baseId)
        assertEquals(1, dependents.size)
        assertEquals(depId, dependents.first().id)
    }

    @Test
    fun `insertBookBaseText is idempotent on duplicate`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val catId = repository.insertCategory(Category(parentId = null, title = "C", level = 0, order = 1))
        val baseId = repository.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "Genesis", order = 1f))
        val depId = repository.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "Rashi", order = 2f))

        repository.insertBookBaseText(depId, baseId)
        repository.insertBookBaseText(depId, baseId)

        assertEquals(1, repository.getBaseBooks(depId).size)
    }

    @Test
    fun `deleteBookBaseTexts removes relations of a book`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val catId = repository.insertCategory(Category(parentId = null, title = "C", level = 0, order = 1))
        val baseId = repository.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "Genesis", order = 1f))
        val depId = repository.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "Rashi", order = 2f))
        repository.insertBookBaseText(depId, baseId)

        repository.deleteBookBaseTexts(depId)

        assertTrue(repository.getBaseBooks(depId).isEmpty())
    }

    @Test
    fun `deleting a book cascades book_base_text on both sides`() = runBlocking {
        // Dedicated FK-enforcing driver: the shared repository driver leaves FKs off.
        val fkDriver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        fkDriver.execute(null, "PRAGMA foreign_keys=ON", 0)
        val repo = SeforimRepository(":memory:", fkDriver)
        try {
            val sourceId = repo.insertSource("Sefaria")
            val catId = repo.insertCategory(Category(parentId = null, title = "C", level = 0, order = 1))
            val baseId = repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "Genesis", order = 1f))
            val depId = repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "Rashi", order = 2f))
            repo.insertBookBaseText(depId, baseId)

            // Deleting the base book removes the row via baseBookId cascade.
            fkDriver.execute(null, "DELETE FROM book WHERE id = $baseId", 0)
            assertTrue(repo.getBaseBooks(depId).isEmpty())

            // Re-create and delete from the dependent side.
            val baseId2 = repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "Exodus", order = 3f))
            repo.insertBookBaseText(depId, baseId2)
            fkDriver.execute(null, "DELETE FROM book WHERE id = $depId", 0)
            assertTrue(repo.getDependentBooks(baseId2).isEmpty())
        } finally {
            fkDriver.close()
        }
    }
}
