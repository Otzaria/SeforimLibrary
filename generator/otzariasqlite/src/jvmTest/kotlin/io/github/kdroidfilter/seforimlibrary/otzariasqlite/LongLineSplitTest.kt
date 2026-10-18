package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class LongLineSplitTest {

    private val config = LongLineSplit.Config(minLineChars = 50, targetChars = 20, maxChars = 30, minTailChars = 10)

    /** `w01a w02a …`: 5 visible chars per word, 5n-1 in all. */
    private fun words(from: Int, to: Int) = (from..to).joinToString(" ") { "w%02da".format(it) }

    private fun pieces(line: String, cfg: LongLineSplit.Config = config) = LongLineSplit.split(listOf(line), cfg).lines

    @Test
    fun shortLinesAndHeadingsStayWhole() {
        val lines = listOf("<h1>t</h1>", words(1, 10), "<h2>" + words(1, 14) + "</h2>")
        val result = LongLineSplit.split(lines, config)
        assertFalse(result.isSplit)
        assertEquals(lines, result.lines)
        assertEquals(2, result.firstIndex(2))
        assertEquals(0, result.sourceIndex(0))
    }

    @Test
    fun cutsAtTheFirstSpaceFromTheTargetAndMapsEveryLine() {
        val lines = listOf("a", words(1, 14), "b")
        val result = LongLineSplit.split(lines, config)
        assertEquals(
            listOf("a", words(1, 4) + " ", words(5, 8) + " ", words(9, 14), "b"),
            result.lines,
        )
        assertEquals(listOf(0, 1, 4), (0..2).map(result::firstIndex))
        assertEquals(listOf(0, 3, 4), (0..2).map(result::lastIndex))
        assertEquals(listOf(1, 3, 1), (0..2).map(result::pieceCount))
        assertEquals(listOf(0, 1, 1, 1, 2), (0..4).map(result::sourceIndex))
        assertEquals(-1, result.firstIndex(3), "past the txt end stays out of range")
        assertEquals(words(1, 14), result.splitSourceOfFirstPiece(1))
        assertNull(result.splitSourceOfFirstPiece(2))
        assertNull(result.splitSourceOfFirstPiece(0))
    }

    @Test
    fun brWinsTheWindowAndIsDropped() {
        val line = words(1, 5) + "<br>" + words(6, 14)
        val out = pieces(line)
        assertEquals(words(1, 5), out[0])
        assertTrue(out[1].startsWith("w06a"))
        assertTrue(out.none { it.contains("<br>") })
    }

    @Test
    fun colonBeatsPeriodBeatsSpace() {
        val tail = " " + words(10, 20)
        assertTrue(pieces(words(1, 4) + " w5. w6:" + tail)[0].endsWith("w6: "))
        assertTrue(pieces(words(1, 4) + " w5. w6," + tail)[0].endsWith("w5. "))
    }

    @Test
    fun neverCutsInsideAnOpenTag() {
        val line = words(1, 3) + " <b>" + words(4, 7) + "</b> " + words(8, 16)
        // The only spaces in the window are inside <b>: no cut, the line stays whole.
        assertEquals(listOf(line), pieces(line))
        val wider = pieces(line, config.copy(maxChars = 40))
        assertTrue(wider[0].endsWith("</b> "), wider[0])
    }

    @Test
    fun wrappedLineReopensItsWrapperInEveryPiece() {
        val open = "<small><span class=\"c\">"
        val line = open + words(1, 14) + "</span></small>"
        assertEquals(
            listOf(
                open + words(1, 4) + " </span></small>",
                open + words(5, 8) + " </span></small>",
                open + words(9, 14) + "</span></small>",
            ),
            pieces(line),
        )
    }

    @Test
    fun mergedFootnoteIsNeverCut() {
        val note = "<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">" + words(30, 37) + "</i>"
        val line = words(1, 3) + note + " " + words(4, 13)
        assertEquals(listOf(line), pieces(line), "the window falls inside the footnote")
        val out = pieces(line, config.copy(maxChars = 60))
        assertEquals(2, out.size)
        assertTrue(out[0].endsWith(note + " "))
        val wholeNote = "<i class=\"footnote\">" + words(1, 14) + "</i>"
        assertEquals(listOf(wholeNote), pieces(wholeNote), "a footnote is never a wrapper to reopen")
    }

    @Test
    fun anchorOffsetsLandInTheirPiece() {
        val line = "<b>w01a</b> " + words(2, 14)
        val result = LongLineSplit.split(listOf(line), config)
        assertEquals(3, result.lines.size)
        val w06 = line.indexOf("w06a")
        assertEquals(LongLineSplit.PiecePosition(1, 5, 9), result.locate(0, w06, w06 + 4))
        // An end past the piece is clamped to the piece's end.
        val w04 = line.indexOf("w04a")
        assertEquals(LongLineSplit.PiecePosition(0, 15, 20), result.locate(0, w04, w06))
        assertNull(result.locate(0, line.length + 1, null))
    }

    @Test
    fun identicalLinesSplitAlike() {
        val result = LongLineSplit.split(listOf(words(1, 14), "x", words(1, 14)), config)
        assertEquals(result.lines.subList(0, 3), result.lines.subList(4, 7))
        assertEquals(words(1, 14), result.splitSourceOfFirstPiece(4))
    }

    @Test
    fun piecesRebuildTheLineWithinBoundsAndNoTinyTail() {
        val random = Random(1941)
        repeat(300) {
            val line = buildString {
                val n = random.nextInt(10, 40)
                for (i in 0 until n) {
                    append("x".repeat(random.nextInt(1, 7)))
                    when (random.nextInt(12)) {
                        0 -> append("<br>")
                        1 -> append(": ")
                        2 -> append(". ")
                        3 -> append(" <b>bb cc</b> ")
                        4 -> append(" &amp; ")
                        else -> append(' ')
                    }
                }
            }
            val out = pieces(line)
            assertEquals(line.replace("<br>", ""), out.joinToString("").replace("<br>", ""))
            assertEquals(countVisibleChars(line), out.sumOf { countVisibleChars(it) })
            if (out.size > 1) {
                out.dropLast(1).forEach { assertTrue(countVisibleChars(it) in config.targetChars..config.maxChars, it) }
                assertTrue(countVisibleChars(out.last()) in config.minTailChars..config.maxChars, out.last())
            }
            assertEquals(out, pieces(line), "deterministic")
        }
    }

    @Test
    fun noPieceOpensWithAHeadingTag() {
        // Not a heading line (the importer tests the first chars), but <h2> wraps all its text.
        val wrapped = "<b></b><h2>" + words(1, 14) + "</h2>"
        assertEquals(listOf(wrapped), pieces(wrapped))
        val midLine = words(1, 4) + " <h2>w5</h2> " + words(6, 16)
        assertTrue(pieces(midLine).drop(1).none { it.startsWith("<h2") }, pieces(midLine).toString())
    }

    @Test
    fun fuzzNestedWrappersUnbalancedTagsAndLocate() {
        val random = Random(7)
        var splitCount = 0
        repeat(2000) {
            val wrapOpen = listOf("", "<span class=\"a\">", "<small><b>", "<p>")[random.nextInt(4)]
            val wrapClose = when (wrapOpen) {
                "" -> ""
                "<span class=\"a\">" -> "</span>"
                "<small><b>" -> "</b></small>"
                else -> "</p>"
            }
            val body = buildString {
                repeat(random.nextInt(10, 40)) {
                    append("y".repeat(random.nextInt(1, 6)))
                    when (random.nextInt(16)) {
                        0 -> append("<br/>")
                        1 -> append(": ")
                        2 -> append(". ")
                        3 -> append(" <i>a <u>b</u> c</i> ")
                        4 -> append(" &nbsp; ")
                        5 -> append(" <img src=\"q\"> ")
                        6 -> append(" & ")
                        7 -> append("</q> ")
                        8 -> append("  ")
                        else -> append(' ')
                    }
                }
            }
            val line = wrapOpen + body + wrapClose
            val result = LongLineSplit.split(listOf(line), config)
            val out = result.lines
            if (out.size > 1) splitCount++
            assertEquals(countVisibleChars(line), out.sumOf { countVisibleChars(it) }, line)
            fun vis(s: String) = s.replace(Regex("<[^>]*>"), "")
            assertEquals(vis(line), out.joinToString("") { vis(it) }, line)
            // Every visible char maps to the same visible char of its piece.
            val visLine = vis(line)
            if (!line.contains('&')) {
                for (raw in line.indices) {
                    val before = line.substring(0, raw)
                    if (line[raw] == '<' || before.count { it == '<' } != before.count { it == '>' }) continue
                    val p = result.locate(0, raw, null) ?: error("null locate at $raw")
                    val v = countVisibleChars(line, raw)
                    if (v >= visLine.length) continue
                    assertEquals(visLine[v], vis(out[p.lineIndex])[p.charStart], "raw=$raw line=$line")
                }
            }
        }
        assertTrue(splitCount > 100, "split $splitCount")
    }
}
