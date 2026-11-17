package io.github.kdroidfilter.seforimlibrary.common.anchors

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CommentarySideMarkerAnchorsTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private val logger = Logger.withTag("CommentarySideMarkerAnchorsTest")

    @Test
    fun `opening marker range counts visible chars and skips tags and leading blanks`() {
        assertEquals(0 until 3, openingMarkerRange("(א) לעבודת בוראו", "א"))
        assertEquals(1 until 5, openingMarkerRange(" <b>(יא)</b> השחר", "יא"))
        assertEquals(0 until 4, openingMarkerRange("(יב) טקסט", "(יב)"))
        assertNull(openingMarkerRange("טקסט (א) באמצע", "א"))
        assertNull(openingMarkerRange("(ב) טקסט", "א"))
        assertNull(openingMarkerRange("(א", "א"))
    }

    @Test
    fun `anchors the opening marker of each labelled commentary line`() {
        val db = db(
            commentaryLines = listOf("(א) לעבודת בוראו", "<b>(ב)</b> השחר", "בלי סימן", "(ד) טקסט"),
            links = listOf(
                // linkId, connection type, base line, commentary line index, base-side labels
                LinkRow(1, "COMMENTARY", 10, 0, listOf("א")),
                LinkRow(2, "SUPER_COMMENTARY", 10, 1, listOf("ב")),
                LinkRow(3, "COMMENTARY", 11, 2, listOf("ג")),
                LinkRow(4, "COMMENTARY", 11, 3, listOf("ג")),
                LinkRow(5, "REFERENCE", 11, 0, listOf("א")),
            ),
        )

        val report = connect(db).use { buildCommentarySideMarkerAnchors(it, logger) }

        assertEquals(CommentarySideAnchorReport(4, 2, 2, 0, 0), report)
        assertEquals(
            listOf(listOf<Any?>(1, 0, 3, "א"), listOf<Any?>(2, 0, 3, "ב")),
            query(db, "SELECT linkId, charStart, charEnd, label FROM link_anchor WHERE side = 1 ORDER BY linkId"),
        )
    }

    @Test
    fun `skips conflicting labels, shared lines and links already anchored, and reruns as a no-op`() {
        val db = db(
            commentaryLines = listOf("(א) ראשון", "(ב) שני", "(ג) שלישי"),
            links = listOf(
                LinkRow(1, "COMMENTARY", 10, 0, listOf("א", "ב")),
                LinkRow(2, "COMMENTARY", 10, 1, listOf("ב")),
                LinkRow(3, "COMMENTARY", 11, 1, listOf("ב")),
                LinkRow(4, "COMMENTARY", 11, 2, listOf("ג")),
            ),
        )
        connect(db).use {
            it.createStatement().use { st ->
                st.execute("INSERT INTO link_anchor(linkId, side, charStart, charEnd) VALUES (4, 1, 0, 3)")
            }
        }

        val report = connect(db).use { buildCommentarySideMarkerAnchors(it, logger) }
        assertEquals(CommentarySideAnchorReport(3, 0, 0, 1, 1), report)

        val rerun = connect(db).use { buildCommentarySideMarkerAnchors(it, logger) }
        assertEquals(0, rerun.written)
        assertEquals(
            listOf(listOf<Any?>(4, 0, 3)),
            query(db, "SELECT linkId, charStart, charEnd FROM link_anchor WHERE side = 1"),
        )
    }

    private data class LinkRow(
        val id: Long,
        val type: String,
        val baseLineId: Long,
        val commentaryLineIndex: Int,
        val baseLabels: List<String>,
    )

    private fun db(commentaryLines: List<String>, links: List<LinkRow>): Path {
        val path = tmp.newFolder().toPath().resolve("seforim.db")
        JdbcSqliteDriver("jdbc:sqlite:${path.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        connect(path).use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO source(id, name) VALUES (1, 'Sefaria')")
                st.execute("INSERT INTO category(id, title) VALUES (1, 'cat')")
                st.execute("INSERT INTO book(id, categoryId, sourceId, title) VALUES (1, 1, 1, 'base')")
                st.execute("INSERT INTO book(id, categoryId, sourceId, title) VALUES (2, 1, 1, 'commentary')")
                st.execute("INSERT INTO line(id, bookId, lineIndex, content) VALUES (10, 1, 0, 'a'), (11, 1, 1, 'b')")
                for ((i, type) in links.map { it.type }.distinct().withIndex()) {
                    st.execute("INSERT INTO connection_type(id, name) VALUES (${i + 1}, '$type')")
                }
            }
            conn.prepareStatement("INSERT INTO line(id, bookId, lineIndex, content) VALUES (?, 2, ?, ?)").use { ps ->
                commentaryLines.forEachIndexed { i, content ->
                    ps.setLong(1, 100L + i); ps.setInt(2, i); ps.setString(3, content); ps.executeUpdate()
                }
            }
            for (link in links) {
                conn.prepareStatement(
                    "INSERT INTO link(id, sourceBookId, targetBookId, sourceLineId, targetLineId, targetLineIndex, " +
                        "targetBookOrderIndex, connectionTypeId) " +
                        "VALUES (?, 1, 2, ?, ?, ?, 0, (SELECT id FROM connection_type WHERE name = ?))",
                ).use { ps ->
                    ps.setLong(1, link.id); ps.setLong(2, link.baseLineId)
                    ps.setLong(3, 100L + link.commentaryLineIndex); ps.setInt(4, link.commentaryLineIndex)
                    ps.setString(5, link.type); ps.executeUpdate()
                }
                link.baseLabels.forEachIndexed { i, label ->
                    conn.prepareStatement(
                        "INSERT INTO link_anchor(linkId, side, charStart, label) VALUES (?, 0, ?, ?)",
                    ).use { ps ->
                        ps.setLong(1, link.id); ps.setInt(2, i); ps.setString(3, label); ps.executeUpdate()
                    }
                }
            }
        }
        return path
    }

    private fun connect(p: Path): Connection = DriverManager.getConnection("jdbc:sqlite:${p.toAbsolutePath()}")

    private fun query(p: Path, sql: String): List<List<Any?>> = connect(p).use { conn ->
        conn.createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                val n = rs.metaData.columnCount
                buildList { while (rs.next()) add((1..n).map { rs.getObject(it) }) }
            }
        }
    }
}
