package io.github.kdroidfilter.seforimlibrary.common.patch

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import com.github.luben.zstd.ZstdInputStream
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Patch contract across the line_content split: 6 -> 6 deltas, the 5 -> 6 refusal, the barrier. */
class SchemaSixPatchTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `schema 6 delta carries line_content and version NULL changes and reproduces the hash`() {
        val prev = schemaSixDb(
            "prev.db",
            lines = mapOf(1L to "one", 2L to "two", 3L to "three"),
            versions = listOf(Triple(1L, 1L, "one"), Triple(1L, 2L, "TWO"), Triple(1L, 3L, "three")),
        )
        val next = schemaSixDb(
            "next.db",
            lines = mapOf(1L to "one", 2L to "two!", 4L to "four"),
            versions = listOf(Triple(1L, 1L, "uno"), Triple(1L, 2L, "two!"), Triple(1L, 4L, "four")),
        )
        val patch = path("patch-v6.db")

        val produced = PatchDbProducer().produce(
            prev, next, patch,
            fromVersion = 29, toVersion = 30,
            fromSchemaVersion = 6, toSchemaVersion = 6,
        )

        assertEquals(2, produced.upsertCounts.getValue("line_content"), "changed + added text")
        assertEquals(1, produced.deleteCounts.getValue("line_content"), "removed line")
        val target = path("target.db")
        Files.copy(prev, target)
        val expected = hash(next)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            PatchApplier().apply(conn, patch, expectedToContentHash = expected, expectedToSchemaVersion = 6)
            val inherited = conn.createStatement().use { st ->
                st.executeQuery("SELECT lineId FROM version_line WHERE content IS NULL ORDER BY lineId").use { rs ->
                    buildList { while (rs.next()) add(rs.getLong(1)) }
                }
            }
            assertEquals(listOf(2L, 4L), inherited)
        }
        assertEquals(expected, hash(target))
    }

    @Test
    fun `a schema 5 anchor cannot be patched to schema 6`() {
        val prev = path("prev5.db")
        JdbcSqliteDriver("jdbc:sqlite:${prev.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        val next = schemaSixDb("next6.db", lines = mapOf(1L to "one"), versions = emptyList())

        val refusal = assertFailsWith<UnpatchableAnchorException> {
            PatchDbProducer().produce(
                prev, next, path("patch-5-6.db"),
                fromVersion = 28, toVersion = 29,
                fromSchemaVersion = 5, toSchemaVersion = 6,
            )
        }
        assertEquals("line", refusal.table)
        assertTrue(requiresFullRebase(1, 6) && requiresFullRebase(5, 7) && requiresFullRebase(6, 7))
        assertFalse(requiresFullRebase(4, 5) || requiresFullRebase(6, 6) || requiresFullRebase(7, 7))
    }

    @Test
    fun `barrier manifest describes a placeholder every applier refuses`() {
        val out = path("fan/patch-v28-v29.db")

        val manifestPath = writeSchemaBarrier(out, fromVersion = 28, toVersion = 29, fromSchemaVersion = 5, toSchemaVersion = 6)

        val zst = path("fan/patch-v28-v29.db.zst")
        assertEquals(path("fan/patch-v28-v29.db.zst.manifest.json"), manifestPath)
        assertFalse(Files.exists(out), "the raw placeholder is not left for staging")
        val manifest = Json.parseToJsonElement(Files.readString(manifestPath)).jsonObject
        assertEquals(28, manifest.getValue("fromVersion").jsonPrimitive.int)
        assertEquals(29, manifest.getValue("toVersion").jsonPrimitive.int)
        assertEquals(5, manifest.getValue("fromSchemaVersion").jsonPrimitive.int)
        assertEquals(6, manifest.getValue("toSchemaVersion").jsonPrimitive.int)
        assertEquals(999, manifest.getValue("patchFormatVersion").jsonPrimitive.int)
        assertTrue(manifest.getValue("fullRebase").jsonPrimitive.boolean)
        assertEquals("full-rebase", manifest.getValue("fromContentHash").jsonPrimitive.content)
        assertEquals("full-rebase", manifest.getValue("toContentHash").jsonPrimitive.content)
        val file = manifest.getValue("patchFiles").jsonArray.single().jsonObject
        assertEquals("patch-v28-v29.db.zst", file.getValue("file").jsonPrimitive.content)
        assertEquals("zstd", file.getValue("compression").jsonPrimitive.content)
        assertEquals(sha256(Files.readAllBytes(zst)), file.getValue("sha256").jsonPrimitive.content)
        assertEquals(Files.size(zst), file.getValue("size").jsonPrimitive.long)

        val raw = ZstdInputStream(Files.newInputStream(zst)).use { it.readBytes() }
        assertEquals(sha256(raw), file.getValue("uncompressedSha256").jsonPrimitive.content)
        assertEquals(raw.size.toLong(), file.getValue("uncompressedSize").jsonPrimitive.long)
        val placeholder = path("placeholder.db").also { Files.write(it, raw) }
        DriverManager.getConnection("jdbc:sqlite:${placeholder.toAbsolutePath()}").use { conn ->
            val format = conn.createStatement().use { st ->
                st.executeQuery("SELECT value FROM patch_meta WHERE key = 'schema_version'").use { rs ->
                    rs.next(); rs.getString(1)
                }
            }
            assertEquals("999", format)
        }
        val client = schemaSixDb("client.db", lines = mapOf(1L to "one"), versions = emptyList())
        DriverManager.getConnection("jdbc:sqlite:${client.toAbsolutePath()}").use { conn ->
            val error = assertFailsWith<IllegalStateException> { PatchApplier().apply(conn, placeholder) }
            assertTrue("schema_version=999" in error.message.orEmpty())
        }
    }

    @Test
    fun `a barrier is refused where a real delta is possible`() {
        assertFailsWith<IllegalArgumentException> {
            writeSchemaBarrier(path("x/patch-v29-v30.db"), 29, 30, fromSchemaVersion = 6, toSchemaVersion = 6)
        }
    }

    private fun path(name: String): Path = tmp.root.toPath().resolve(name).also {
        Files.createDirectories(it.parent)
    }

    /** A fresh working-shape DB converted by the real split stage. */
    private fun schemaSixDb(name: String, lines: Map<Long, String>, versions: List<Triple<Long, Long, String>>): Path {
        val db = path(name)
        JdbcSqliteDriver("jdbc:sqlite:${db.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO source(id, name) VALUES (1, 'src')")
                st.execute("INSERT INTO category(id, title) VALUES (1, 'cat')")
                st.execute("INSERT INTO book(id, categoryId, sourceId, title) VALUES (1, 1, 1, 'book')")
                st.execute("INSERT INTO book_version(id, bookId, versionTitle, hasContent) VALUES (1, 1, 'A', 1)")
            }
            conn.prepareStatement("INSERT INTO line(id, bookId, lineIndex, content) VALUES (?, 1, ?, ?)").use { ps ->
                for ((id, text) in lines) {
                    ps.setLong(1, id); ps.setLong(2, id); ps.setString(3, text); ps.executeUpdate()
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

    private fun hash(db: Path): String = DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use {
        LogicalContentHasher.forSchemaVersion(6).compute(it)
    }

    private fun sha256(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
}
