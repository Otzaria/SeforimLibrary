package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import co.touchlab.kermit.Logger
import org.jsoup.Jsoup
import java.sql.Connection

private const val BEKHOROT_CONTINUATION_BASE_REF = "בכורות, נב:, ה"
private const val BEKHOROT_BASE_WORDS =
    "מאי טעמא דרבי מאיר מכר הוא דאמר רחמנא ליהדר ביובל מתנה וירושה לא והני מירושה ירושה דמתנה מתנה"
private val BEKHOROT_CONTINUATION_TEXT = listOf(
    "ולפיכך הני ממשנתנו דירושה הם כדמפרש ואזיל הרי ירושה הם שאינה חוזרת",
    "והני ממשנתנו דמתנה הם כדמפרש ואזיל הרי מתנה הם שאינה חוזרת ביובל",
    "ומפרשת הגמרא אלו הם ירושה ואלו הם מתנה",
)

private data class ContinuationLine(val id: Long, val index: Int, val text: String, val isHeading: Boolean)

/**
 * Extends the reviewed Havrouta explanation of Bekhorot 52b:5 over its three continuation
 * paragraphs (otzaria#5230). No continuation is inferred for any other text.
 *
 * Runs after Havrouta's links are created. Both neighbouring paragraphs must already link to
 * the verified base line; the complete continuation text must match the Otzaria source.
 * Authored ranges and other existing base links take precedence. LINKER citations may coexist
 * with this reviewed commentary mapping, including citations added by release Phase 2 later.
 * Requires the working database shape, before splitLineContent.
 */
internal fun extendCuratedHavroutaContinuations(conn: Connection, logger: Logger): Int {
    fun bookId(title: String, sefaria: Boolean): Long? = conn.prepareStatement(
        """
        SELECT b.id FROM book b JOIN source s ON s.id = b.sourceId
        WHERE b.title = ? AND (LOWER(s.name) LIKE '%sefaria%') = ?
        """.trimIndent(),
    ).use { st ->
        st.setString(1, title)
        st.setBoolean(2, sefaria)
        st.executeQuery().use { rs ->
            if (!rs.next()) null else rs.getLong(1).takeUnless { rs.next() }
        }
    }

    val commentaryBookId = bookId("חברותא על בכורות", sefaria = false) ?: return 0
    val baseBookId = bookId("בכורות", sefaria = true) ?: return 0
    val baseLineId = conn.prepareStatement("SELECT id, content FROM line WHERE bookId = ? AND heRef = ?").use { st ->
        st.setLong(1, baseBookId)
        st.setString(2, BEKHOROT_CONTINUATION_BASE_REF)
        st.executeQuery().use { rs ->
            if (!rs.next()) return 0
            val id = rs.getLong(1)
            if (!reviewedWords(rs.getString(2)).contains(BEKHOROT_BASE_WORDS) || rs.next()) return 0
            id
        }
    }
    val lines = conn.prepareStatement("SELECT id, lineIndex, content FROM line WHERE bookId = ? ORDER BY lineIndex").use { st ->
        st.setLong(1, commentaryBookId)
        st.executeQuery().use { rs ->
            buildList {
                while (rs.next()) {
                    val content = rs.getString(3)
                    add(ContinuationLine(
                        rs.getLong(1), rs.getInt(2), reviewedWords(content), HEADING_TAG.containsMatchIn(content),
                    ))
                }
            }
        }
    }
    val starts = (1 until lines.size - BEKHOROT_CONTINUATION_TEXT.size).filter { first ->
        BEKHOROT_CONTINUATION_TEXT.indices.all { offset ->
            !lines[first + offset].isHeading && lines[first + offset].text == BEKHOROT_CONTINUATION_TEXT[offset] &&
                lines[first + offset].index == lines[first - 1].index + offset + 1
        } && lines[first + BEKHOROT_CONTINUATION_TEXT.size].index ==
            lines[first - 1].index + BEKHOROT_CONTINUATION_TEXT.size + 1
    }
    if (starts.size != 1) return 0
    val first = starts.single()
    val gap = lines.subList(first, first + BEKHOROT_CONTINUATION_TEXT.size)

    fun neighbourLinks(lineId: Long): List<Long> = conn.prepareStatement(
        """
        SELECT l.id FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId
        WHERE l.sourceBookId = ? AND l.targetBookId = ?
          AND l.sourceLineId = ? AND l.targetLineId = ? AND ct.name = 'COMMENTARY'
        """.trimIndent(),
    ).use { st ->
        st.setLong(1, baseBookId)
        st.setLong(2, commentaryBookId)
        st.setLong(3, baseLineId)
        st.setLong(4, lineId)
        st.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getLong(1)) } }
    }
    val linkId = neighbourLinks(lines[first - 1].id).singleOrNull() ?: return 0
    if (neighbourLinks(lines[first + gap.size].id).size != 1) return 0

    check(conn.autoCommit) { "Curated Havrouta continuations require an unowned JDBC connection" }
    conn.autoCommit = false
    try {
        val hasAuthoredRange = conn.prepareStatement("SELECT 1 FROM link_range WHERE linkId = ? AND side = 1").use { st ->
            st.setLong(1, linkId)
            st.executeQuery().use { it.next() }
        }
        val gapIds = gap.joinToString(",") { it.id.toString() }
        val alreadyLinked = conn.prepareStatement(
            """
            SELECT 1 FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId
            WHERE ct.name != 'LINKER' AND (
                (l.sourceBookId = ? AND l.targetLineId IN ($gapIds)) OR
                (l.targetBookId = ? AND l.sourceLineId IN ($gapIds))
            )
            UNION ALL
            SELECT 1 FROM link_coverage c JOIN link l ON l.id = c.linkId
            JOIN connection_type ct ON ct.id = l.connectionTypeId
            WHERE c.lineId IN ($gapIds) AND ? IN (l.sourceBookId, l.targetBookId)
              AND ct.name != 'LINKER'
            LIMIT 1
            """.trimIndent(),
        ).use { st ->
            for (i in 1..3) st.setLong(i, baseBookId)
            st.executeQuery().use { it.next() }
        }
        if (hasAuthoredRange || alreadyLinked) {
            conn.rollback()
            return 0
        }
        conn.prepareStatement("INSERT INTO link_range(linkId, side, endLineId, endLineIndex) VALUES (?, 1, ?, ?)").use { st ->
            st.setLong(1, linkId)
            st.setLong(2, gap.last().id)
            st.setInt(3, gap.last().index)
            st.executeUpdate()
        }
        conn.prepareStatement("INSERT INTO link_coverage(lineId, linkId, side) VALUES (?, ?, 1)").use { st ->
            for (line in gap) {
                st.setLong(1, line.id)
                st.setLong(2, linkId)
                st.addBatch()
            }
            st.executeBatch()
        }
        conn.commit()
        logger.i { "Extended the reviewed Bekhorot 52b:5 Havrouta explanation over ${gap.size} paragraphs" }
        return gap.size
    } catch (failure: Throwable) {
        runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        conn.autoCommit = true
    }
}

private fun reviewedWords(content: String): String = Jsoup.parseBodyFragment(content).text()
    .filter { it.isLetterOrDigit() || it.isWhitespace() }
    .trim().replace(WHITESPACE, " ")

private val HEADING_TAG = Regex("<h[1-6](?:\\s|>)", RegexOption.IGNORE_CASE)
private val WHITESPACE = Regex("\\s+")
