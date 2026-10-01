package io.github.kdroidfilter.seforimlibrary.common.patch

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentCompression
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CompressLineContentCliTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private val lines = mapOf(
        1L to "בראשית ברא אלהים את השמים ואת הארץ",
        2L to "<b>אמר רבי יוחנן</b> משום רבי שמעון בן יוחי",
        3L to "",
        4L to "plain ascii",
        5L to "ד\"ה ".repeat(500),
    )
    private val versions = listOf(
        Triple(1L, 1L, "בראשית ברא אלהים את השמים ואת הארץ"),
        Triple(1L, 2L, "גרסה אחרת"),
        Triple(1L, 3L, "שורה ריקה בבסיס"),
    )

    @Test
    fun `every text becomes a frame that decodes to the same bytes and NULL stays NULL`() {
        val db = schemaSixDb("c.db")

        val report = connect(db).use { compressLineContent(it, chunkRows = 2, threads = 3) }

        assertEquals(lines.size.toLong(), report.lineRowsCompressed)
        assertEquals(2L, report.versionRowsCompressed, "one inherited (NULL) row is left alone")
        connect(db).use { conn ->
            assertTrue(LineContentCompression.isCompressed(conn))
            assertEquals(
                setOf(LineContentCompression.bundledDictionaryId),
                LineContentCompression.storedDictionaries(conn).keys,
            )
            LineContentCompression.Decompressor().use { d ->
                assertEquals(lines, blobs(conn, "SELECT id, content FROM line_content").mapValues { d.text(it.value) })
                val v = rows(conn, "SELECT lineId, content FROM version_line ORDER BY lineId")
                assertNull(v.getValue(1L), "identical edition text is inherited")
                assertEquals("גרסה אחרת", d.text(v.getValue(2L)!!))
                assertEquals("שורה ריקה בבסיס", d.text(v.getValue(3L)!!))
            }
            stampSchemaVersion(conn, dbVersion = 31, dbSchemaVersion = 6)
            conn.createStatement().use { it.execute("UPDATE line_content SET content = 'plain' WHERE id = 1") }
            assertFailsWith<IllegalStateException> { stampSchemaVersion(conn, dbVersion = 31, dbSchemaVersion = 6) }
        }
    }

    @Test
    fun `a second run only validates and leaves the hash unchanged`() {
        val db = schemaSixDb("again.db")
        connect(db).use { compressLineContent(it, threads = 2) }
        val first = connect(db).use { LogicalContentHasher.forSchemaVersion(6).computeReport(it) }

        val again = connect(db).use { compressLineContent(it, threads = 2) }

        assertTrue(again.alreadyCompressed)
        assertEquals(first, connect(db).use { LogicalContentHasher.forSchemaVersion(6).computeReport(it) })
    }

    @Test
    fun `an interrupted run resumes and leaves already compressed rows untouched`() {
        val db = schemaSixDb("resume.db")
        val oneRow = connect(db).use { conn ->
            compressLineContent(conn, threads = 1)
            blobs(conn, "SELECT id, content FROM line_content WHERE id = 2").getValue(2L)
        }
        connect(db).use { conn ->
            conn.prepareStatement("UPDATE line_content SET content = ? WHERE id <> 2").use { ps ->
                ps.setString(1, "placeholder"); ps.executeUpdate()
            }
        }

        val report = connect(db).use { compressLineContent(it, threads = 2) }

        assertEquals(lines.size - 1L, report.lineRowsCompressed)
        connect(db).use { conn ->
            assertContentEquals(oneRow, blobs(conn, "SELECT id, content FROM line_content WHERE id = 2").getValue(2L))
            validateCompressed(conn)
        }
    }

    @Test
    fun `a BLOB that is not a zstd frame fails validation`() {
        val db = schemaSixDb("notframe.db")
        connect(db).use { conn ->
            compressLineContent(conn, threads = 1)
            conn.createStatement().use { it.execute("UPDATE line_content SET content = x'00010203' WHERE id = 1") }
            val error = assertFailsWith<IllegalStateException> { validateCompressed(conn) }
            assertTrue("1 line_content rows" in error.message.orEmpty(), error.message)
        }
    }

    @Test
    fun `a frame that is only the zstd magic fails validation with its row`() {
        val error = corruptAfterCompress(line = 3, frame = byteArrayOf(0x28, 0xB5.toByte(), 0x2F, 0xFD.toByte()))
        assertTrue("line_content row 3" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `a version frame made with another dictionary id fails validation`() {
        val other = LineContentCompression.bundledDictionary.copyOf().also { it[4] = (it[4] + 1).toByte() }
        val frame = LineContentCompression.Compressor(other).use { it.compress("גרסה אחרת".toByteArray()) }
        val db = schemaSixDb("dictid.db")
        val error = connect(db).use { conn ->
            compressLineContent(conn, threads = 1)
            conn.prepareStatement("UPDATE version_line SET content = ? WHERE lineId = 2").use { ps ->
                ps.setBytes(1, frame); ps.executeUpdate()
            }
            assertFailsWith<IllegalStateException> { validateCompressed(conn) }
        }
        assertTrue("version_line row" in error.message.orEmpty() && "dictionary" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `a frame that decodes past the reader cap fails validation`() {
        val huge = LineContentCompression.Compressor().use {
            it.compress(ByteArray(LineContentCompression.MAX_LINE_BYTES + 1) { 'a'.code.toByte() })
        }
        val error = corruptAfterCompress(line = 4, frame = huge)
        assertTrue("line_content row 4" in error.message.orEmpty() && "cap" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `a frame that decodes to invalid UTF-8 fails validation`() {
        val frame = LineContentCompression.Compressor().use { it.compress(byteArrayOf(0x61, 0xFF.toByte(), 0x62)) }
        val error = corruptAfterCompress(line = 2, frame = frame)
        assertTrue("line_content row 2" in error.message.orEmpty() && "UTF-8" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `a resumed run decodes the BLOBs it skips`() {
        val db = schemaSixDb("resume-corrupt.db")
        connect(db).use { conn ->
            conn.createStatement().use { it.execute("UPDATE line_content SET content = x'28B52FFD' WHERE id = 5") }
        }
        val error = assertFailsWith<IllegalStateException> { connect(db).use { compressLineContent(it, threads = 2) } }
        assertTrue("line_content row 5" in error.message.orEmpty(), error.message)
    }

    /** Shared dictionaries must not change a byte: the stored frames match the frozen contract. */
    @Test
    fun `frames written by the parallel run are byte-identical to the frozen contract`() {
        val db = schemaSixDb("frozen.db")
        connect(db).use { compressLineContent(it, chunkRows = 2, threads = 3) }
        val stored = connect(db).use { blobs(it, "SELECT id, content FROM line_content ORDER BY id") }
        val digest = LineContentCompression.sha256(stored.toSortedMap().values.fold(ByteArray(0)) { acc, f -> acc + f })
        assertEquals(FROZEN_FRAMES_SHA256, digest)
    }

    @Test
    fun `a DB that already holds another dictionary is refused`() {
        val db = schemaSixDb("other.db")
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE zstd_dict (id INTEGER PRIMARY KEY NOT NULL, dict BLOB NOT NULL)")
                st.execute("INSERT INTO zstd_dict VALUES (7, x'00')")
            }
        }
        val error = assertFailsWith<IllegalStateException> { connect(db).use { compressLineContent(it) } }
        assertTrue("already holds dictionaries" in error.message.orEmpty(), error.message)
    }

    /**
     * Frozen bytes: a different zstd-jni, level or dictionary changes every row and
     * turns each patch into the whole library. Update only with a full-rebase bump.
     */
    @Test
    fun `frames are byte-identical to the frozen contract`() {
        val frames = LineContentCompression.Compressor().use { c ->
            lines.toSortedMap().values.map { c.compress(it.toByteArray()) }
        }
        val digest = LineContentCompression.sha256(frames.fold(ByteArray(0)) { acc, f -> acc + f })
        assertEquals(FROZEN_FRAMES_SHA256, digest)
        assertEquals(LineContentCompression.DICT_SHA256, LineContentCompression.sha256(LineContentCompression.bundledDictionary))
    }

    @Test
    fun `a delta between DBs with the same dictionary carries frames and reproduces the hash`() {
        val prev = schemaSixDb("prev.db").also { db -> connect(db).use { compressLineContent(it) } }
        val next = schemaSixDb("next.db", changeLine2 = true).also { db -> connect(db).use { compressLineContent(it) } }
        val patch = tmp.root.toPath().resolve("p.db")

        val produced = PatchDbProducer().produce(
            prev, next, patch, fromVersion = 30, toVersion = 31, fromSchemaVersion = 6, toSchemaVersion = 6,
        )

        assertEquals(1, produced.upsertCounts.getValue("line_content"), "only the changed row")
        val target = tmp.root.toPath().resolve("t.db").also { Files.copy(prev, it) }
        val expected = connect(next).use { LogicalContentHasher.forSchemaVersion(6).compute(it) }
        connect(target).use { PatchApplier().apply(it, patch, expectedToContentHash = expected, expectedToSchemaVersion = 6) }
        connect(target).use { conn ->
            LineContentCompression.Decompressor().use { d ->
                assertEquals("שונה", d.text(blobs(conn, "SELECT id, content FROM line_content WHERE id = 2").getValue(2L)))
            }
        }
    }

    @Test
    fun `a delta across a dictionary change is unpatchable`() {
        val plain = schemaSixDb("plain.db")
        val compressed = schemaSixDb("compressed.db").also { db -> connect(db).use { compressLineContent(it) } }

        val error = assertFailsWith<UnpatchableAnchorException> {
            PatchDbProducer().produce(
                plain, compressed, tmp.root.toPath().resolve("x.db"),
                fromVersion = 30, toVersion = 31, fromSchemaVersion = 6, toSchemaVersion = 6,
            )
        }
        assertEquals("zstd_dict", error.table)
    }

    // ── fixtures ────────────────────────────────────────────────────────────

    private fun LineContentCompression.Decompressor.text(frame: ByteArray): String =
        String(decompress(frame), Charsets.UTF_8)

    private fun schemaSixDb(name: String, changeLine2: Boolean = false): Path {
        val db = tmp.root.toPath().resolve(name)
        JdbcSqliteDriver("jdbc:sqlite:${db.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO source(id, name) VALUES (1, 'src')")
                st.execute("INSERT INTO category(id, title) VALUES (1, 'cat')")
                st.execute("INSERT INTO book(id, categoryId, sourceId, title) VALUES (1, 1, 1, 'book')")
                st.execute("INSERT INTO book_version(id, bookId, versionTitle, hasContent) VALUES (1, 1, 'A', 1)")
            }
            conn.prepareStatement("INSERT INTO line(id, bookId, lineIndex, content) VALUES (?, 1, ?, ?)").use { ps ->
                for ((id, text) in lines) {
                    ps.setLong(1, id); ps.setLong(2, id)
                    ps.setString(3, if (changeLine2 && id == 2L) "שונה" else text)
                    ps.executeUpdate()
                }
            }
            conn.prepareStatement("INSERT INTO version_line(versionId, lineId, content) VALUES (?, ?, ?)").use { ps ->
                for ((versionId, lineId, text) in versions) {
                    ps.setLong(1, versionId); ps.setLong(2, lineId); ps.setString(3, text); ps.executeUpdate()
                }
            }
            splitLineContent(conn)
        }
        return db
    }

    private fun corruptAfterCompress(line: Long, frame: ByteArray): IllegalStateException {
        val db = schemaSixDb("corrupt-$line.db")
        return connect(db).use { conn ->
            compressLineContent(conn, threads = 1)
            conn.prepareStatement("UPDATE line_content SET content = ? WHERE id = ?").use { ps ->
                ps.setBytes(1, frame); ps.setLong(2, line); ps.executeUpdate()
            }
            assertFailsWith<IllegalStateException> { validateCompressed(conn) }
        }
    }

    private fun connect(db: Path): Connection = DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}")

    private fun blobs(conn: Connection, sql: String): Map<Long, ByteArray> =
        rows(conn, sql).mapValues { checkNotNull(it.value) }

    private fun rows(conn: Connection, sql: String): Map<Long, ByteArray?> =
        conn.createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                buildMap {
                    while (rs.next()) {
                        assertFalse(rs.getObject(2) is String, "row ${rs.getLong(1)} is still TEXT")
                        put(rs.getLong(1), rs.getBytes(2))
                    }
                }
            }
        }

    private companion object {
        const val FROZEN_FRAMES_SHA256 = "1663035dda0397b1664dae380546d3661644696346f55a818acb86f5f2118360"
    }
}
