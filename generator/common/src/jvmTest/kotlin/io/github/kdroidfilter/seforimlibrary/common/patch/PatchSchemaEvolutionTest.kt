package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PatchSchemaEvolutionTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `producer emits migrations and upserts when target adds a tracked table`() {
        val prev = tmp.newFolder().toPath().resolve("prev.db")
        val next = tmp.newFolder().toPath().resolve("next.db")
        val patch = tmp.newFolder().toPath().resolve("patch.db")
        val target = tmp.newFolder().toPath().resolve("target.db")

        buildPreviousDb(prev)
        buildNextDb(next)

        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        assertEquals(2, produced.upsertCounts.getValue("generation"))
        assertEquals(2, produced.upsertCounts.getValue("book_generation"))

        Files.copy(prev, target)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            val applied = PatchApplier().apply(conn, patch)

            assertTrue(applied.migrationsApplied >= 2, "expected table-create migrations to be applied")
            assertEquals(2, countRows(conn, "generation"))
            assertEquals(2, countRows(conn, "book_generation"))
            assertEquals(logicalHash(next), LogicalContentHasher().compute(conn))
        }
    }

    @Test
    fun `producer emits ADD COLUMN migration when target adds a column to a tracked table`() {
        val prev = tmp.newFolder().toPath().resolve("prev.db")
        val next = tmp.newFolder().toPath().resolve("next.db")
        val patch = tmp.newFolder().toPath().resolve("patch.db")
        val target = tmp.newFolder().toPath().resolve("target.db")

        buildBookDb(prev, withHeDesc = false)
        buildBookDb(next, withHeDesc = true)

        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        assertEquals(2, produced.upsertCounts.getValue("book"))

        Files.copy(prev, target)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            val applied = PatchApplier().apply(conn, patch)

            assertTrue(applied.migrationsApplied >= 1, "expected an ADD COLUMN migration")
            assertTrue(hasColumn(conn, "book", "heDesc"), "heDesc column should exist after apply")
            assertEquals(logicalHash(next), LogicalContentHasher().compute(conn))
        }
    }

    private fun buildBookDb(path: Path, withHeDesc: Boolean) {
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate("CREATE TABLE schema_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)")
                st.executeUpdate("INSERT INTO schema_meta(key, value) VALUES ('db_version', '1')")
                if (withHeDesc) {
                    st.executeUpdate("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL, heDesc TEXT)")
                    st.executeUpdate("INSERT INTO book(id, title, heDesc) VALUES (10, 'Genesis', 'long desc A'), (11, 'Exodus', 'long desc B')")
                } else {
                    st.executeUpdate("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                    st.executeUpdate("INSERT INTO book(id, title) VALUES (10, 'Genesis'), (11, 'Exodus')")
                }
            }
        }
    }

    private fun hasColumn(conn: Connection, table: String, column: String): Boolean =
        conn.createStatement().use { st ->
            st.executeQuery("PRAGMA table_info(\"$table\")").use { rs ->
                generateSequence { if (rs.next()) rs.getString("name") else null }.any { it == column }
            }
        }

    private fun buildPreviousDb(path: Path) {
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate("CREATE TABLE schema_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)")
                st.executeUpdate("INSERT INTO schema_meta(key, value) VALUES ('db_version', '1')")
                st.executeUpdate("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                st.executeUpdate("INSERT INTO book(id, title) VALUES (10, 'Genesis'), (11, 'Exodus')")
            }
        }
    }

    private fun buildNextDb(path: Path) {
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate("CREATE TABLE schema_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)")
                st.executeUpdate("INSERT INTO schema_meta(key, value) VALUES ('db_version', '2')")
                st.executeUpdate("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                st.executeUpdate("INSERT INTO book(id, title) VALUES (10, 'Genesis'), (11, 'Exodus')")
                st.executeUpdate("CREATE TABLE generation (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE)")
                st.executeUpdate("""
                    CREATE TABLE book_generation (
                        bookId INTEGER NOT NULL,
                        generationId INTEGER NOT NULL,
                        PRIMARY KEY (bookId, generationId),
                        FOREIGN KEY (bookId) REFERENCES book(id) ON DELETE CASCADE,
                        FOREIGN KEY (generationId) REFERENCES generation(id) ON DELETE CASCADE
                    )
                """.trimIndent())
                st.executeUpdate("CREATE INDEX idx_book_generation_generation ON book_generation(generationId)")
                st.executeUpdate("INSERT INTO generation(id, name) VALUES (1, 'Rishonim'), (2, 'Acharonim')")
                st.executeUpdate("INSERT INTO book_generation(bookId, generationId) VALUES (10, 1), (11, 2)")
            }
        }
    }

    private fun countRows(conn: Connection, table: String): Long =
        conn.createStatement().use { st ->
            st.executeQuery("SELECT COUNT(*) FROM \"$table\"").use { rs ->
                rs.next()
                rs.getLong(1)
            }
        }

    private fun logicalHash(path: Path): String =
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use {
            LogicalContentHasher().compute(it)
        }
}
