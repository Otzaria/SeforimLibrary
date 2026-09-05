package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Round-trips every cross-schema edge admitted by the release workflow. */
class PatchMultiSchemaPromotionTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `schema 1 and 2 clients can jump directly to schema 4`() {
        for (fromSchema in listOf(1, 2)) {
            val prev = path("prev-v$fromSchema.db")
            val next = path("next-v$fromSchema-to-v4.db")
            val patch = path("v$fromSchema-v4-patch.db")
            val target = path("target-v$fromSchema.db")

            buildDb(prev, schemaVersion = fromSchema, stalePromotedTables = true)
            buildDb(next, schemaVersion = 4, stalePromotedTables = false)

            val produced = PatchDbProducer().produce(
                prev,
                next,
                patch,
                fromVersion = 10 + fromSchema,
                toVersion = 14,
                fromSchemaVersion = fromSchema,
                toSchemaVersion = 4,
            )

            val promoted = if (fromSchema == 1) {
                listOf("book_base_text", "line_ref", "line_dh", "link_suppressed_side")
            } else {
                listOf("line_ref", "line_dh", "link_suppressed_side")
            }
            for (table in promoted) {
                assertEquals(1, produced.upsertCounts.getValue(table), "full snapshot for $table")
                assertEquals(0, produced.deleteCounts.getValue(table), "no stale deletes for $table")
            }
            if (fromSchema == 2) {
                assertEquals(0, produced.upsertCounts.getValue("book_base_text"))
                assertEquals(0, produced.deleteCounts.getValue("book_base_text"))
            }

            DriverManager.getConnection("jdbc:sqlite:${patch.toAbsolutePath()}").use { conn ->
                val migrations = conn.createStatement().use { st ->
                    st.executeQuery("SELECT sql FROM migrations ORDER BY version").use { rs ->
                        buildList { while (rs.next()) add(rs.getString(1)) }
                    }
                }
                for (table in promoted) {
                    assertTrue("DROP TABLE IF EXISTS \"$table\"" in migrations)
                    assertTrue(migrations.any { it.startsWith("CREATE TABLE") && it.contains(table) })
                }
            }

            Files.copy(prev, target)
            val expectedHash = hash(next, 4)
            DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
                PatchApplier().apply(
                    conn,
                    patch,
                    expectedToContentHash = expectedHash,
                    expectedToSchemaVersion = 4,
                )
            }
            assertEquals(expectedHash, hash(target, 4))
        }
    }

    @Test
    fun `schema 1 through 3 clients can jump directly to schema 5`() {
        for (fromSchema in 1..3) {
            val prev = path("prev-v$fromSchema-to-v5.db")
            val next = path("next-v$fromSchema-to-v5.db")
            val patch = path("v$fromSchema-v5-patch.db")
            val target = path("target-v$fromSchema-to-v5.db")

            buildDb(prev, schemaVersion = fromSchema, stalePromotedTables = true)
            buildDb(next, schemaVersion = 5, stalePromotedTables = false)

            val produced = PatchDbProducer().produce(
                prev,
                next,
                patch,
                fromVersion = 20 + fromSchema,
                toVersion = 27,
                fromSchemaVersion = fromSchema,
                toSchemaVersion = 5,
            )

            val promoted = when (fromSchema) {
                1 -> listOf("book_base_text", "line_ref", "line_dh", "link_suppressed_side")
                2 -> listOf("line_ref", "line_dh", "link_suppressed_side")
                else -> listOf("line_ref", "line_dh")
            }
            for (table in promoted) {
                assertEquals(1, produced.upsertCounts.getValue(table), "full snapshot for $table")
                assertEquals(0, produced.deleteCounts.getValue(table), "no stale deletes for $table")
            }

            Files.copy(prev, target)
            val expectedHash = hash(next, 5)
            DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
                PatchApplier().apply(
                    conn,
                    patch,
                    expectedToContentHash = expectedHash,
                    expectedToSchemaVersion = 5,
                )
            }
            assertEquals(expectedHash, hash(target, 5))
        }
    }

    private fun path(name: String): Path = tmp.root.toPath().resolve(name)

    private fun buildDb(path: Path, schemaVersion: Int, stalePromotedTables: Boolean) {
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)")
                st.execute("INSERT INTO schema_meta VALUES ('db_version', '1'), ('db_schema_version', '$schemaVersion')")
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                st.execute("INSERT INTO book VALUES (1, 'Base'), (2, 'Commentary')")
                st.execute(
                    "CREATE TABLE book_base_text (" +
                        "bookId INTEGER NOT NULL, baseBookId INTEGER NOT NULL, PRIMARY KEY(bookId, baseBookId))",
                )
                val baseText = if (stalePromotedTables && schemaVersion == 1) "2, 1" else "1, 2"
                st.execute("INSERT INTO book_base_text VALUES ($baseText)")

                st.execute("CREATE TABLE line (id INTEGER PRIMARY KEY NOT NULL, bookId INTEGER NOT NULL)")
                st.execute("INSERT INTO line VALUES (10, 1), (11, 1)")
                st.execute(
                    "CREATE TABLE line_ref (" +
                        "bookId INTEGER NOT NULL, refKeyHash INTEGER NOT NULL, lineIndex INTEGER NOT NULL, " +
                        "PRIMARY KEY(bookId, refKeyHash, lineIndex)) WITHOUT ROWID",
                )
                val refKey = if (stalePromotedTables) 700 else 800
                st.execute("INSERT INTO line_ref VALUES (1, $refKey, 0)")
                val dhDisplayColumn = if (schemaVersion >= 5) ", dhDisplay TEXT NOT NULL" else ""
                st.execute(
                    "CREATE TABLE line_dh (" +
                        "bookId INTEGER NOT NULL, dhText TEXT NOT NULL, lineIndex INTEGER NOT NULL" +
                        "$dhDisplayColumn, PRIMARY KEY(bookId, dhText, lineIndex)) WITHOUT ROWID",
                )
                val dh = if (stalePromotedTables) "ישן" else "חדש"
                val dhValues = if (schemaVersion >= 5) "1, '$dh', 0, '$dh מודפס'" else "1, '$dh', 0"
                st.execute("INSERT INTO line_dh VALUES ($dhValues)")

                st.execute("CREATE TABLE link (id INTEGER PRIMARY KEY NOT NULL, label TEXT NOT NULL)")
                st.execute("INSERT INTO link VALUES (100, 'link')")
                st.execute(
                    "CREATE TABLE link_suppressed_side (" +
                        "linkId INTEGER NOT NULL, side INTEGER NOT NULL, reasonMask INTEGER NOT NULL, " +
                        "PRIMARY KEY(linkId, side)) WITHOUT ROWID",
                )
                val reasonMask = if (stalePromotedTables) 4 else 8
                st.execute("INSERT INTO link_suppressed_side VALUES (100, 0, $reasonMask)")
            }
        }
    }

    private fun hash(path: Path, schemaVersion: Int): String =
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use {
            LogicalContentHasher.forSchemaVersion(schemaVersion).compute(it)
        }
}
