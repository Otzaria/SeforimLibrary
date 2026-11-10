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

/**
 * The three reported segments, imported from Sefaria-shaped links: Maggid Mishneh on Shabbat
 * 17:16:2 (only a self-link, Otzaria/otzaria#3854), Meiri on Chullin 96a:3 (#3264) and
 * Chidushei Halachot on Eruvin 31b:2 (#3806), neither linked at all.
 */
class SefariaDerivedCommentaryLinksImportTest {
    private class BookSpec(val id: Long, val enTitle: String, val addresses: List<String>, val base: BookSpec? = null, val mapping: String? = null)

    @Test
    fun reportedSegmentsGetTheirBaseLines() = runBlocking {
        val shabbat = BookSpec(1, "Mishneh Torah, Sabbath", (12..20).map { "17:$it" })
        val maggid = BookSpec(2, "Maggid Mishneh on Mishneh Torah, Sabbath", (12..20).map { "17:$it:1" } + "17:16:2", shabbat, "many_to_one")
        val chullin = BookSpec(3, "Chullin", (1..10).map { "96a:$it" } + (1..10).map { "96b:$it" })
        val meiri = BookSpec(4, "Meiri on Chullin", (1..5).map { "96a:$it" } + (1..4).map { "96b:$it" }, chullin)
        val eruvin = BookSpec(5, "Eruvin", (1..20).map { "31b:$it" } + (1..20).map { "32a:$it" })
        val chidushei = BookSpec(6, "Chidushei Halachot on Eruvin", listOf("31b:1", "31b:2") + (1..6).map { "32a:$it" }, eruvin)
        val books = listOf(shabbat, maggid, chullin, meiri, eruvin, chidushei)

        val rows = buildList {
            (12..20).forEach { add("Mishneh Torah, Sabbath 17:$it" to "Maggid Mishneh on Mishneh Torah, Sabbath 17:$it:1") }
            add("Maggid Mishneh on Mishneh Torah, Sabbath 17:16:2" to "Maggid Mishneh on Mishneh Torah, Sabbath 17:16:2")
            listOf("96a:5" to "96a:1", "96a:6" to "96a:2", "96a:6" to "96a:4", "96a:9" to "96a:5").forEach { (b, s) ->
                add("Chullin $b" to "Meiri on Chullin $s")
            }
            listOf(2 to 1, 4 to 2, 6 to 3, 9 to 4).forEach { (b, s) -> add("Chullin 96b:$b" to "Meiri on Chullin 96b:$s") }
            add("Chidushei Halachot on Eruvin 31b:1" to "Eruvin 31b:15")
            listOf(1 to 1, 3 to 2, 5 to 3, 8 to 4, 10 to 5, 15 to 6).forEach { (b, s) ->
                add("Chidushei Halachot on Eruvin 32a:$s" to "Eruvin 32a:$b")
            }
        }
        val linksDir = Files.createDirectories(Files.createTempDirectory("seforim-derived-links").resolve("links"))
        Files.writeString(
            linksDir.resolve("links0.csv"),
            (listOf("Citation 1,Citation 2,Conection Type") + rows.map { (a, b) -> "\"$a\",\"$b\",commentary" })
                .joinToString("\n"),
        )

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(Category(0, null, "ספרים", level = 0, order = 1))
        val refs = mutableListOf<RefEntry>()
        val lineKeyToId = HashMap<Pair<String, Int>, Long>()
        val lineIdToBookId = HashMap<Long, Long>()
        books.forEach { spec ->
            repo.insertBook(
                Book(
                    id = spec.id, categoryId = catId, sourceId = sourceId, title = spec.enTitle, heRef = spec.enTitle,
                    authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(), heShortDesc = null,
                    notesContent = null, order = spec.id.toFloat(), topics = emptyList(), isBaseBook = spec.base == null,
                    totalLines = spec.addresses.size, hasAltStructures = false, hasTeamim = false, hasNekudot = false,
                )
            )
            repo.insertLinesBatch(
                spec.addresses.mapIndexed { i, address ->
                    val lineId = spec.id * 1000 + i
                    lineKeyToId[spec.enTitle to i] = lineId
                    lineIdToBookId[lineId] = spec.id
                    refs += RefEntry("${spec.enTitle} $address", "", spec.enTitle, i + 1)
                    Line(id = lineId, bookId = spec.id, lineIndex = i, content = address, heRef = address)
                }
            )
        }
        val bookMeta = books.associate { spec ->
            spec.id to BookMeta(
                isBaseBook = spec.base == null,
                categoryLevel = 0,
                priorityRank = null,
                dependence = spec.base?.let { Dependence.COMMENTARY },
                baseTextBookIds = setOfNotNull(spec.base?.id),
                sefariaDeclaredBaseTextBookIds = setOfNotNull(spec.base?.id),
                enTitle = spec.enTitle,
                baseTextMapping = spec.mapping,
            )
        }

        val refsByPath = refs.groupBy { it.path }
        val commentaries = collectAddressedCommentaries(refsByPath, lineKeyToId, lineIdToBookId, emptySet(), bookMeta)
        val baseLinks = CommentaryBaseLinks(commentaries, lineIdToBookId)
        val importer = SefariaLinksImporter(repo, IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo), Logger.withTag("derived"))
        importer.processLinksInParallel(
            linksDir = linksDir,
            refsByCanonical = refs.groupBy { canonicalCitation(it.ref) },
            refsByBase = emptyMap(),
            lineKeyToId = lineKeyToId,
            lineIdToBookId = lineIdToBookId,
            bookMetaById = bookMeta,
            refsByPath = refsByPath,
            onLinkStored = baseLinks::record,
            onLineCovered = baseLinks::recordCoveredLine,
        )
        val storedBySefaria = importer.metricsSnapshot().insertedByType.getValue("COMMENTARY").written
        importer.insertDerivedCommentaryLinks(commentaries.flatMap { deriveSegmentAddressLinks(it, baseLinks) }, bookMeta)
            .forEach(baseLinks::record)
        importer.insertDerivedCommentaryLinks(commentaries.flatMap { deriveTalmudNeighbourLinks(it, baseLinks) }, bookMeta)

        fun line(spec: BookSpec, address: String) = spec.id * 1000 + spec.addresses.indexOf(address)
        fun query(sql: String): List<List<Long>> = driver.getConnection().createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                buildList { while (rs.next()) add((1..rs.metaData.columnCount).map { rs.getLong(it) }) }
            }
        }
        fun derivedFor(spec: BookSpec, address: String) = query(
            "SELECT sourceLineId, baseProvenance FROM link WHERE targetLineId = ${line(spec, address)}"
        )

        assertEquals(listOf(listOf(line(shabbat, "17:16"), 2L)), derivedFor(maggid, "17:16:2"))
        assertEquals(listOf(listOf(line(chullin, "96a:6"), 2L)), derivedFor(meiri, "96a:3"))
        assertEquals(listOf(listOf(line(eruvin, "31b:15"), 2L)), derivedFor(chidushei, "31b:2"))
        val rangeLink = query("SELECT id FROM link WHERE targetLineId = ${line(chidushei, "31b:2")}").single().single()
        assertEquals(
            listOf(listOf(0L, line(eruvin, "31b:20"), 19L)),
            query("SELECT side, endLineId, endLineIndex FROM link_range WHERE linkId = $rangeLink"),
        )
        assertEquals(
            (16..20).map { listOf(line(eruvin, "31b:$it"), 0L) },
            query("SELECT lineId, side FROM link_coverage WHERE linkId = $rangeLink ORDER BY lineId"),
        )
        val metrics = importer.metricsSnapshot()
        assertEquals(
            mapOf("segment_address" to 1L, "talmud_neighbours" to 1L, "talmud_neighbours_range" to 1L),
            metrics.derivedCommentaryByStage,
        )
        assertEquals(storedBySefaria + 3, metrics.insertedByType.getValue("COMMENTARY").written)
        assertEquals(storedBySefaria + 3, query("SELECT COUNT(*) FROM link").single().single())
        repo.close()
    }
}
