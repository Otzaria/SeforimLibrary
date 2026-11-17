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
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

// Otzaria/otzaria#3264: reviewed links for commentary segments that Sefaria leaves unlinked.
class SefariaCuratedCommentaryLinksTest {
    private val logger = Logger.withTag("curated")

    @Test
    fun `resource parses and every segment is listed once`() {
        val entries = SefariaCuratedCommentaryLinks.load(SefariaCuratedCommentaryLinks::class.java.classLoader)
        assertTrue(entries.isNotEmpty())
        assertTrue(
            entries.any { it.commentaryHeRef == "מאירי על חולין,  צו., ג" && it.baseHeRef == "חולין, צו., ו" },
            "the reported Meiri segment",
        )
    }

    @Test
    fun `rejects malformed and duplicate rows`() {
        assertFailsWith<IllegalArgumentException> { SefariaCuratedCommentaryLinks.parse(sequenceOf("a\tb")) }
        assertFailsWith<IllegalArgumentException> {
            SefariaCuratedCommentaryLinks.parse(sequenceOf("a\tb\tc", "a\td\te"))
        }
        assertEquals(1, SefariaCuratedCommentaryLinks.parse(sequenceOf("# note", "", "a\tb\tc")).size)
    }

    @Test
    fun `opening words are compared on visible text`() {
        val content = "<b>המשנה החמישית</b>  והיא גם כן"
        assertTrue(SefariaCuratedCommentaryLinks.opensWith(content, "המשנה החמישית והיא"))
        assertFalse(SefariaCuratedCommentaryLinks.opensWith(content, "המשנה הששית והיא"))
        assertFalse(SefariaCuratedCommentaryLinks.opensWith("המשנה", "המשנה החמישית"))
    }

    @Test
    fun `writes reviewed links only where Sefaria still gives none and the text is unchanged`() = runBlocking {
        val base = "Chullin"
        val meiri = "Meiri on Chullin"
        val baseLines = listOf("96a:5" to "בסיס ה", "96a:6" to "בסיס ו")
        val meiriLines = listOf(
            "96a:1" to "פתיחה א",
            "96a:2" to "פתיחה ב",
            "96a:3" to "המשנה החמישית והיא גם",
            "96a:4" to "פתיחה ד",
            "96a:5" to "פתיחה ה",
            "96a:6" to "פתיחה ו",
            "96a:7" to "טקסט שהשתנה",
            "96a:8" to "פתיחה ח",
        )
        val heRef = { title: String, address: String -> "$title, $address" }
        val books = listOf(1L to (base to baseLines), 2L to (meiri to meiriLines))

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(Category(0, null, "ספרים", level = 0, order = 1))
        val refs = mutableListOf<RefEntry>()
        val lineKeyToId = HashMap<Pair<String, Int>, Long>()
        val lineIdToBookId = HashMap<Long, Long>()
        books.forEach { (id, book) ->
            val (title, lines) = book
            repo.insertBook(
                Book(
                    id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
                    authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(), heShortDesc = null,
                    notesContent = null, order = id.toFloat(), topics = emptyList(), isBaseBook = id == 1L,
                    totalLines = lines.size, hasAltStructures = false, hasTeamim = false, hasNekudot = false,
                )
            )
            repo.insertLinesBatch(
                lines.mapIndexed { i, (address, text) ->
                    val lineId = id * 1000 + i
                    lineKeyToId[title to i] = lineId
                    lineIdToBookId[lineId] = id
                    refs += RefEntry("$title $address", heRef(title, address), title, i + 1)
                    Line(id = lineId, bookId = id, lineIndex = i, content = text, heRef = heRef(title, address))
                }
            )
        }
        val bookMeta = mapOf(
            1L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = null, enTitle = base),
            2L to BookMeta(
                isBaseBook = false, categoryLevel = 0, priorityRank = null, dependence = Dependence.COMMENTARY,
                baseTextBookIds = setOf(1L), sefariaDeclaredBaseTextBookIds = setOf(1L), enTitle = meiri,
            ),
        )
        val entries = SefariaCuratedCommentaryLinks.parse(
            sequenceOf(
                // The reported case: no link from Sefaria.
                "${heRef(meiri, "96a:3")}\t${heRef(base, "96a:6")}\tהמשנה החמישית והיא",
                // Sefaria links it now.
                "${heRef(meiri, "96a:1")}\t${heRef(base, "96a:6")}\tפתיחה א",
                // Inside a commentary range, after its first line.
                "${heRef(meiri, "96a:5")}\t${heRef(base, "96a:6")}\tפתיחה ה",
                // The segment's text changed.
                "${heRef(meiri, "96a:7")}\t${heRef(base, "96a:6")}\tהטקסט הישן",
                // Linked by an earlier derived stage.
                "${heRef(meiri, "96a:8")}\t${heRef(base, "96a:6")}\tפתיחה ח",
                // A ref missing from the export.
                "${heRef(meiri, "97a:1")}\t${heRef(base, "96a:6")}\tפתיחה",
            )
        )
        val resolved = SefariaCuratedCommentaryLinks.resolve(entries, refs, lineKeyToId, lineIdToBookId, logger)
        assertEquals(5, resolved.size)
        val guard = SefariaCuratedCommentaryLinks.Guard(resolved)
        guard.recordDerived(
            listOf(DerivedCommentaryLink(DerivedLinkStage.SEGMENT_ADDRESS, 1L, 2L, 1000L, 2007L, 7)),
        )

        val linksDir = Files.createDirectories(Files.createTempDirectory("seforim-curated-links").resolve("links"))
        Files.writeString(
            linksDir.resolve("links0.csv"),
            listOf(
                "Citation 1,Citation 2,Conection Type,Suppression Mask 1,Suppression Mask 2",
                "\"$base 96a:5\",\"$meiri 96a:1\",commentary,0,0",
                "\"$base 96a:6\",\"$meiri 96a:2\",commentary,0,0",
                "\"$base 96a:6\",\"$meiri 96a:4-6\",commentary,0,0",
            ).joinToString("\n"),
        )
        val importer = SefariaLinksImporter(repo, IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo), logger)
        importer.processLinksInParallel(
            linksDir = linksDir,
            refsByCanonical = refs.groupBy { canonicalCitation(it.ref) },
            refsByBase = emptyMap(),
            lineKeyToId = lineKeyToId,
            lineIdToBookId = lineIdToBookId,
            bookMetaById = bookMeta,
            refsByPath = refs.groupBy { it.path },
            onLinkStored = guard::record,
            onRangeResolved = guard::recordRange,
        )
        val written = importer.insertDerivedCommentaryLinks(
            SefariaCuratedCommentaryLinks.linksToWrite(resolved, guard, { repo.getLine(it)?.content }, logger),
            bookMeta,
        )
        assertEquals(1, written)
        assertEquals(1L, importer.metricsSnapshot().derivedCommentaryByStage["curated"])

        fun linksTo(lineId: Long): List<Long> = driver.getConnection().createStatement().use { st ->
            st.executeQuery("SELECT sourceLineId FROM link WHERE targetLineId = $lineId").use { rs ->
                buildList { while (rs.next()) add(rs.getLong(1)) }
            }
        }
        assertEquals(listOf(1001L), linksTo(2002L), "the reported segment gets the reviewed base line")
        assertEquals(listOf(1000L), linksTo(2000L), "Sefaria's own link is kept alone")
        assertEquals(emptyList(), linksTo(2004L), "a segment inside a range keeps the range alone")
        assertEquals(emptyList(), linksTo(2006L), "changed text is not linked")
        assertEquals(emptyList(), linksTo(2007L), "an earlier derived link is not doubled")
    }
}
