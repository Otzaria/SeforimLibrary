package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.StaticConfig
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** ForDB commentator_collective_titles.csv → NULL book.collectiveTitleHe/En. */
class SeedCollectiveTitlesTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private val logger = Logger(StaticConfig(Severity.Error, emptyList()), "test")

    private val header = "bookName,collectiveTitleHe,collectiveTitleEn"

    @Test
    fun `quoted rows parse like book_info, with an optional English title`() {
        val rows = parseCommentatorCollectiveTitles(
            "$header\n" +
                "\"ריטבא על גיטין\",\"ריטב\"\"א\",\"Ritva\"\n" +
                "\"חברותא על ברכות\",\"חברותא\",\"\"\n",
        )
        assertEquals(
            listOf(
                CollectiveTitleRow("ריטבא על גיטין", "ריטב\"א", "Ritva"),
                CollectiveTitleRow("חברותא על ברכות", "חברותא", null),
            ),
            rows,
        )
        assertEquals(
            listOf(CollectiveTitleRow("חברותא על ברכות", "חברותא", null)),
            parseCommentatorCollectiveTitles("bookName,collectiveTitleHe\n\"חברותא על ברכות\",\"חברותא\"\n"),
        )
        assertEquals(emptyList(), parseCommentatorCollectiveTitles(""))
        assertEquals(emptyList(), parseCommentatorCollectiveTitles("$header\n"))
    }

    @Test
    fun `malformed files fail instead of guessing`() {
        val bad = listOf(
            "שם ספר,מפרש\nא על ב,א\n",
            "$header\n\"א על ב\",\"א\"\n",
            "$header\n\"א על ב\",\"\",\"\"\n",
            "$header\n\"\",\"א\",\"\"\n",
            "$header\n\"א על ב \",\"א\",\"\"\n",
            "$header\n\"רשי על ב\",\"רש״י\",\"\"\n",
            "$header\n\"א על ב\",\"א\",\"\"\n\"א על ב\",\"ג\",\"\"\n",
        )
        for (text in bad) {
            assertFailsWith<IllegalArgumentException>(text) { parseCommentatorCollectiveTitles(text) }
        }
    }

    @Test
    fun `an archive without the file fills nothing, and a BOM with CRLF parses the same`() {
        assertEquals(emptyList(), loadCollectiveTitleRows(logger) { null })
        val plain = "$header\n\"חברותא על ברכות\",\"חברותא\",\"\"\n"
        val bomCrlf = "﻿" + plain.replace("\n", "\r\n")
        val lines = bomCrlf.byteInputStream().bufferedReader().readLines()
        val read = { name: String -> lines.takeIf { name == COMMENTATOR_COLLECTIVE_TITLES_FILE } }
        assertEquals(parseCommentatorCollectiveTitles(plain), loadCollectiveTitleRows(logger, read))
    }

    @Test
    fun `only NULL values are filled, and an existing value is kept and reported`() {
        db().use {
            val result = applyCollectiveTitles(
                it,
                listOf(
                    CollectiveTitleRow("ריטבא על גיטין", "ריטב\"א", "Ritva"),
                    CollectiveTitleRow("חברותא על ברכות", "חברותא", null),
                    CollectiveTitleRow("תוספות על מכות", "תוספות רי\"ד", null),
                    CollectiveTitleRow("אין כזה", "אין", null),
                ),
                logger,
            )
            assertEquals(3, result.filled)
            assertEquals(listOf("'תוספות על מכות': תוספות ≠ תוספות רי\"ד"), result.conflicts)
            assertEquals(listOf("אין כזה"), result.unmatched)
            assertEquals(
                listOf(
                    listOf<Any?>(1, "תוספות", "Tosafot"),
                    listOf<Any?>(2, "ריטב\"א", "Ritva"),
                    // Both books with this title are filled; the English title is left alone.
                    listOf<Any?>(3, "חברותא", null),
                    listOf<Any?>(4, "חברותא", "Kept"),
                    listOf<Any?>(5, null, null),
                ),
                rows(it, "SELECT id, collectiveTitleHe, collectiveTitleEn FROM book ORDER BY id"),
            )
        }
    }

    @Test
    fun `a rerun changes nothing`() {
        db().use {
            val input = listOf(CollectiveTitleRow("ריטבא על גיטין", "ריטב\"א", "Ritva"))
            assertEquals(1, applyCollectiveTitles(it, input, logger).filled)
            val again = applyCollectiveTitles(it, input, logger)
            assertEquals(CollectiveTitlesResult(0, 1, emptyList(), emptyList()), again)
            assertEquals(CollectiveTitlesResult(0, 0, emptyList(), emptyList()), applyCollectiveTitles(it, emptyList(), logger))
        }
    }

    @Test
    fun `a row keyed by a renamed book's former title fails before any write`() {
        db().use {
            val error = assertFailsWith<IllegalStateException> {
                applyCollectiveTitles(
                    it,
                    listOf(
                        CollectiveTitleRow("ריטבא על גיטין", "ריטב\"א", null),
                        CollectiveTitleRow("שם ישן", "חדש", null),
                    ),
                    logger,
                )
            }
            assertContains(error.message.orEmpty(), "'שם ישן' → 'שם חדש על ב'")
            assertEquals(listOf(listOf<Any?>(null)), rows(it, "SELECT collectiveTitleHe FROM book WHERE id = 2"))
        }
    }

    private fun db(): Connection {
        val conn = DriverManager.getConnection("jdbc:sqlite:${tmp.newFolder().toPath().resolve("s.db")}")
        conn.createStatement().use { st ->
            st.execute(
                "CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL, heRef TEXT, " +
                    "collectiveTitleHe TEXT DEFAULT NULL, collectiveTitleEn TEXT DEFAULT NULL)",
            )
            st.execute(
                "INSERT INTO book VALUES " +
                    "(1, 'תוספות על מכות', 'תוספות על מכות', 'תוספות', 'Tosafot'), " +
                    "(2, 'ריטבא על גיטין', NULL, NULL, NULL), " +
                    "(3, 'חברותא על ברכות', NULL, NULL, NULL), " +
                    "(4, 'חברותא על ברכות', NULL, NULL, 'Kept'), " +
                    "(5, 'שם חדש על ב', 'שם ישן', NULL, NULL)",
            )
        }
        return conn
    }

    private fun rows(conn: Connection, sql: String): List<List<Any?>> = conn.createStatement().use { st ->
        st.executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            buildList { while (rs.next()) add((1..n).map { rs.getObject(it) }) }
        }
    }
}
