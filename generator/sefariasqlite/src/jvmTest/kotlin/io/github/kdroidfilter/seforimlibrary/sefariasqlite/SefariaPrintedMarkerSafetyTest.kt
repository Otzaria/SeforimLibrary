package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.visibleUnits
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.core.models.LinkAnchor
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

class SefariaPrintedMarkerSafetyTest {
    @Test
    fun missingFirstLinkDoesNotPromoteTheSecondComment() = runBlocking {
        assertEquals(emptyList(), anchors(listOf(Comment("ראשון", linked = false), Comment("שני", number = 2))))
    }

    @Test
    fun unlinkedPrintedLabelStillDisablesFallback() = runBlocking {
        assertEquals(emptyList(), anchors(listOf(Comment("(ב) ראשון", linked = false), Comment("שני", number = 2))))
    }

    @Test
    fun missingFirstTextDoesNotPromoteTheSecondComment() = runBlocking {
        assertEquals(emptyList(), anchors(listOf(Comment("שני", number = 2))))
    }

    @Test
    fun missingFirstLineIdDoesNotPromoteTheSecondComment() = runBlocking {
        assertEquals(emptyList(), anchors(listOf(Comment("ראשון", hasLineId = false), Comment("שני", number = 2))))
    }

    @Test
    fun duplicatePrintedLabelsAreAmbiguousEvenWhenOnlyOneHasALink() = runBlocking {
        for (secondLinked in listOf(true, false)) {
            assertEquals(emptyList(), anchors(listOf(Comment("(ב) ראשון"), Comment("(ב) שני", number = 2, linked = secondLinked))))
        }
    }

    @Test
    fun twoCommentariesClaimingTheMarkerAreAmbiguous() = runBlocking {
        assertEquals(emptyList(), anchors(listOf(Comment("(ב) ראשון"), Comment("(ב) אחר", group = 1))))
    }

    @Test
    fun explicitLabelDoesNotRequireALinkToAnEarlierComment() = runBlocking {
        assertEquals(
            listOf(LinkAnchor(2, charStart = 3, charEnd = 6, label = "ב")),
            anchors(listOf(Comment("(א) ראשון", linked = false), Comment("(ב) שני", number = 2))),
        )
    }

    @Test
    fun fallbackStillUsesCommentOneWhenRefsAreOutOfOrder() = runBlocking {
        assertEquals(
            listOf(LinkAnchor(2, charStart = 3, charEnd = 6, label = "ב")),
            anchors(listOf(Comment("שני", number = 2), Comment("ראשון"))),
        )
    }

    @Test
    fun fallbackRequiresOneInteriorMarkerEqualToTheSegmentNumber() = runBlocking {
        for (text in listOf("(ב) אב", "אב (א) גד", "אב (ב) גד (ג)")) {
            assertEquals(emptyList(), anchors(listOf(Comment("בלי תווית")), baseText = text))
        }
    }

    @Test
    fun otherBookPairsAreNotGuessedFromNumberedLists() = runBlocking {
        assertEquals(emptyList(), anchors(listOf(Comment("(ב) פירוש")), baseTitle = "Sifra", commentaryTitle = "Chafetz Chaim on Sifra"))
        assertEquals(emptyList(), anchors(listOf(Comment("(ב) פירוש")), baseTitle = "Other Book"))
        assertEquals(emptyList(), anchors(listOf(Comment("(ב) פירוש")), commentaryTitle = "Other Commentary"))
    }

    @Test
    fun declaredBaseRelationshipIsRequired() = runBlocking {
        assertEquals(emptyList(), anchors(listOf(Comment("(ב) פירוש")), declaredBase = false))
    }

    @Test
    fun referenceLinksNeverBecomeCommentaryMarkers() = runBlocking {
        assertEquals(emptyList(), anchors(listOf(Comment("(ב) פירוש")), connectionType = ConnectionType.REFERENCE))
    }

    @Test
    fun britMosheRequiresAnExplicitPrintedLabel() = runBlocking {
        val labelled = anchors(listOf(Comment("<b>(ב)</b> פירוש")), baseTitle = "Sefer Mitzvot Gadol", commentaryTitle = "Brit Moshe")
        assertEquals(listOf(LinkAnchor(1, charStart = 3, charEnd = 6, label = "ב")), labelled)
        assertEquals(emptyList(), anchors(listOf(Comment("בלי תווית")), baseTitle = "Sefer Mitzvot Gadol", commentaryTitle = "Brit Moshe"))
    }

    @Test
    fun britMosheOpeningQuoteMustRepeatTheExactMarker() = runBlocking {
        for (text in listOf("<b>שלא (ב) לעשות כו׳.</b> פירוש", "בפ׳ (ב) קדושים כתוב כו׳")) {
            assertEquals(
                listOf(LinkAnchor(1, charStart = 3, charEnd = 6, label = "ב")),
                anchors(listOf(Comment(text)), baseTitle = "Sefer Mitzvot Gadol", commentaryTitle = "Brit Moshe"),
            )
        }
        for (text in listOf("שלא (א) לעשות כו׳", "פירוש ארוך ואחריו (ב)", "בלי אות מפורשת")) {
            assertEquals(emptyList(), anchors(listOf(Comment(text)), baseTitle = "Sefer Mitzvot Gadol", commentaryTitle = "Brit Moshe"))
        }
    }

    @Test
    fun generatedPrefixesAreExcludedEvenAfterCleaning() = runBlocking {
        for (shift in listOf(4, cleanedLineShift(4))) {
            assertEquals(emptyList(), anchors(listOf(Comment("(ב) פירוש")), baseText = "(ב) טקסט", baseShift = shift))
            assertEquals(emptyList(), anchors(listOf(Comment("(ב) פירוש", shift = shift)), segment = 3))
        }
    }

    @Test
    fun repeatedExplicitMarkersKeepTheirOwnVisibleRanges() = runBlocking {
        assertEquals(
            listOf(LinkAnchor(1, charStart = 3, charEnd = 6, label = "ב"), LinkAnchor(1, charStart = 10, charEnd = 13, label = "ב")),
            anchors(listOf(Comment("(ב) פירוש")), baseText = "אב (ב) גד (ב)"),
        )
    }

    @Test
    fun itagLinesAreLeftToTheExistingImporter() = runBlocking {
        assertEquals(emptyList(), anchors(listOf(Comment("(ב) פירוש")), baseText = "אב <i data-commentator=\"X\"></i> (ב)"))
    }

    @Test
    fun scannerPreservesHtmlVisibleCoordinates() {
        // Independent reference representation from the shared charCount contract.
        val random = Random(117)
        val chunks = listOf("אב ", "(א)", "(יט)", "(ת\"ה)", "(שם)", "<b>", "</b>", "<i x='(ב)'>", "&quot;", "&#40;", "&bad", "😀", " ", "<unfinished")
        val pattern = Regex("""\(([א-ת"'׳״]{1,4})\)""")
        repeat(500) {
            val text = (1..random.nextInt(1, 20)).joinToString("") { chunks.random(random) }
            val visible = visibleUnits(text).joinToString("") { if (it.length == 1) it else "\uFFFC" }
            val skip = random.nextInt(visible.length + 2)
            val first = (skip until visible.length).firstOrNull { !visible[it].isWhitespace() }
            val expected = pattern.findAll(visible).mapNotNull {
                val label = it.groupValues[1]
                val value = gematriaValue(label) ?: return@mapNotNull null
                if (it.range.first < skip || toGematria(value) != label.filter { ch -> ch in 'א'..'ת' }) null
                else PrintedMarker(label, value, it.range.first, it.range.last + 1, it.range.first == first)
            }.toList()
            assertEquals(expected, parsePrintedMarkers(text, skip), "text=$text skip=$skip")
        }
    }

    private data class Comment(
        val text: String,
        val number: Int = 1,
        val linked: Boolean = true,
        val hasLineId: Boolean = true,
        val group: Int = 0,
        val shift: Int = 0,
    )

    private suspend fun anchors(
        comments: List<Comment>,
        baseText: String = "אב (ב) גד",
        baseTitle: String = "Chafetz Chaim",
        commentaryTitle: String = "Be'er Mayim Chaim",
        segment: Int = 2,
        baseShift: Int = 0,
        declaredBase: Boolean = true,
        connectionType: ConnectionType = ConnectionType.COMMENTARY,
    ): List<LinkAnchor> {
        val driver = JdbcSqliteDriver("jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        try {
            val source = repo.insertSource("Sefaria-Test")
            val category = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 1))
            fun book(id: Long, title: String) = Book(
                id, category, source, title, heRef = title, authors = emptyList(), pubPlaces = emptyList(),
                pubDates = emptyList(), heShortDesc = null, notesContent = null, order = id.toFloat(),
                topics = emptyList(), isBaseBook = id == 1L, totalLines = 1, hasAltStructures = false,
                hasTeamim = false, hasNekudot = false,
            )
            repo.insertBook(book(1, baseTitle))
            val baseRef = "$baseTitle, Principle 1 $segment"
            val books = mutableListOf(SefariaInlineAnchors.BookInput(
                1, baseTitle, "base", listOf(baseText), mapOf(0 to RefEntry(baseRef, "", "base", 1)),
                cleanShiftByLineIndex = mapOf(0 to baseShift),
            ))
            val meta = mutableMapOf(1L to BookMeta(true, 0, 0))
            val ids = mutableMapOf(("base" to 0) to 100L)
            val lines = mutableListOf(Line(100, 1, 0, content = baseText, heRef = ""))
            val links = mutableListOf<Link>()
            for ((group, entries) in comments.withIndex().groupBy { it.value.group }) {
                val bookId = 2L + group
                val path = "commentary$group"
                repo.insertBook(book(bookId, commentaryTitle))
                books += SefariaInlineAnchors.BookInput(
                    bookId, commentaryTitle, path, entries.map { it.value.text },
                    entries.mapIndexed { i, entry ->
                        i to RefEntry("$commentaryTitle, Principle 1 $segment:${entry.value.number}", "", path, i + 1)
                    }.toMap(),
                    cleanShiftByLineIndex = entries.mapIndexed { i, entry -> i to entry.value.shift }.toMap(),
                )
                meta[bookId] = BookMeta(false, 0, null, dependence = Dependence.COMMENTARY,
                    sefariaDeclaredBaseTextBookIds = if (declaredBase) setOf(1L) else emptySet())
                entries.forEachIndexed { i, (ordinal, comment) ->
                    val lineId = 200L + ordinal
                    lines += Line(lineId, bookId, i, content = comment.text, heRef = "")
                    if (comment.hasLineId) ids[path to i] = lineId
                    if (comment.linked) links += Link(ordinal + 1L, 1, bookId, 100, lineId, i, connectionType)
                }
            }
            repo.insertLinesBatch(lines)
            repo.insertLinksBatch(links)
            SefariaPrintedMarkerAnchors(repo, Logger.withTag("PrintedMarkerSafetyTest")).generate(books, meta, ids)
            return links.flatMap { repo.getLinkAnchors(it.id) }
        } finally {
            repo.close()
        }
    }
}
