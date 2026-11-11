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

/** Maggid Mishneh's missing link (Otzaria/otzaria#3854) through CSV import and SQLite. */
class SefariaDerivedCommentaryLinksImportTest {
    private class BookSpec(
        val id: Long, val enTitle: String, val addresses: List<String>, val base: BookSpec? = null, val mapping: String? = null,
    )

    @Test
    fun reportedSegmentGetsItsBaseLineWithoutUsingNonExactLinks() = runBlocking {
        val shabbat = BookSpec(1, "Mishneh Torah, Sabbath", (12..23).map { "17:$it" })
        val maggid = BookSpec(
            2, "Maggid Mishneh on Mishneh Torah, Sabbath",
            (12..23).flatMap { if (it >= 16) listOf("17:$it:1", "17:$it:2") else listOf("17:$it:1") },
            shabbat, "many_to_one",
        )
        val books = listOf(shabbat, maggid)
        fun row(citation1: String, citation2: String, mask1: Int = 0) =
            "\"$citation1\",\"$citation2\",commentary,$mask1,0"

        val rows = buildList {
            (12..17).forEach {
                add(row("${shabbat.enTitle} 17:$it", "${maggid.enTitle} 17:$it:1"))
            }
            val self = "${maggid.enTitle} 17:16:2"
            add(row(self, self))
            // Base range in CSV Citation 2, mapped back to stored side 0.
            add(row("${maggid.enTitle} 17:18:1", "${shabbat.enTitle} 17:18-19"))
            // Both a point and a coverage-free range resolve to the same stored link ID.
            add(row("${shabbat.enTitle} 17:19", "${maggid.enTitle} 17:19:1"))
            add(row("${shabbat.enTitle} 17:19-20", "${maggid.enTitle} 17:19:1", mask1 = 4))
            // Commentary range: its covered segment already has a link, even without a link row of its own.
            add(row("${shabbat.enTitle} 17:20", "${maggid.enTitle} 17:20:1-2"))
            add(row("${shabbat.enTitle} 17", "${maggid.enTitle} 17:21:1"))
            add(row("${shabbat.enTitle} 17:22", "${maggid.enTitle} 17:22:1", mask1 = 1))
            // Coverage-free commentary range, with the commentary in CSV Citation 1.
            add(row("${maggid.enTitle} 17:23:1-2", "${shabbat.enTitle} 17:23", mask1 = 4))
        }
        val linksDir = Files.createDirectories(Files.createTempDirectory("seforim-derived-links").resolve("links"))
        Files.writeString(
            linksDir.resolve("links0.csv"),
            (listOf("Citation 1,Citation 2,Conection Type,Suppression Mask 1,Suppression Mask 2") + rows).joinToString("\n"),
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
        val baseLinks = CommentaryBaseLinks(commentaries)
        val importer = SefariaLinksImporter(repo, IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo), Logger.withTag("derived"))
        importer.processLinksInParallel(
            linksDir = linksDir,
            refsByCanonical = refs.groupBy { canonicalCitation(it.ref) },
            refsByBase = mapOf(
                canonicalCitation("${shabbat.enTitle} 17") to refs.single { it.ref == "${shabbat.enTitle} 17:21" },
            ),
            lineKeyToId = lineKeyToId,
            lineIdToBookId = lineIdToBookId,
            bookMetaById = bookMeta,
            refsByPath = refsByPath,
            onLinkStored = baseLinks::record,
            onNonExactCommentary = baseLinks::recordNonExactCommentary,
            onRangeResolved = baseLinks::recordRange,
        )
        val storedBySefaria = importer.metricsSnapshot().insertedByType.getValue("COMMENTARY").written
        val inserted = importer.insertDerivedCommentaryLinks(
            commentaries.flatMap { deriveSegmentAddressLinks(it, baseLinks) }, bookMeta,
        )
        // 17:17:2 has the same supporting point evidence as the original report.
        assertEquals(2, inserted)

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
        assertEquals(listOf(listOf(line(shabbat, "17:17"), 2L)), derivedFor(maggid, "17:17:2"))
        (18..23).forEach { assertEquals(emptyList(), derivedFor(maggid, "17:$it:2"), "17:$it:2") }
        assertEquals(
            listOf(listOf(0L, line(shabbat, "17:20"), 0L), listOf(1L, line(maggid, "17:23:2"), 0L)),
            query(
                """
                SELECT r.side, r.endLineId, (SELECT COUNT(*) FROM link_coverage c WHERE c.linkId = l.id)
                FROM link l JOIN link_range r ON r.linkId = l.id
                WHERE l.targetLineId IN (${line(maggid, "17:19:1")}, ${line(maggid, "17:23:1")})
                ORDER BY l.targetLineId
                """.trimIndent(),
            ),
        )
        assertEquals(
            listOf(listOf(line(maggid, "17:20:2"), 1L)),
            query("SELECT lineId, side FROM link_coverage WHERE lineId = ${line(maggid, "17:20:2")}"),
        )
        val metrics = importer.metricsSnapshot()
        assertEquals(mapOf("segment_address" to 2L), metrics.derivedCommentaryByStage)
        assertEquals(storedBySefaria + 2, metrics.insertedByType.getValue("COMMENTARY").written)
        assertEquals(storedBySefaria + 2, query("SELECT COUNT(*) FROM link").single().single())
        repo.close()
    }
}
