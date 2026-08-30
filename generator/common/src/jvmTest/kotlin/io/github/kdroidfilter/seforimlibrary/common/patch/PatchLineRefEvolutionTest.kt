package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * Schema 3 → 4 evolution: the `line_ref` reference index appears as a brand
 * new table. The producer must auto-infer its CREATE TABLE migration (it
 * exists in `new` but not in `prev`), ship its full contents as upserts, and
 * the applier must materialise it while both hash contracts stay intact —
 * schema-3 DBs keep their released hash, the post-apply DB matches the
 * schema-4 hash of the directly-built target.
 */
class PatchLineRefEvolutionTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `schema 3 to 4 preserves the released from-hash contract`() {
        val prev = tmp.newFile("prev-v3.db").toPath()
        val next = tmp.newFile("next-v4.db").toPath()
        val patch = tmp.newFile("v3-v4-patch.db").toPath()
        val target = tmp.newFile("target.db").toPath()
        Files.delete(prev); Files.delete(next); Files.delete(patch); Files.delete(target)

        buildDb(prev, schemaVersion = 3, withLineRef = false)
        buildDb(next, schemaVersion = 4, withLineRef = true)

        val releasedV3Hash = hash(prev, 3)
        assertNotEquals(
            releasedV3Hash,
            hash(prev, 4),
            "the regression is only caught if an absent schema-4 table changes the current-order hash",
        )

        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 30, toVersion = 31)
        assertEquals(2, produced.upsertCounts.getValue("line_ref"))

        // The CREATE TABLE migration for line_ref must have been auto-inferred.
        DriverManager.getConnection("jdbc:sqlite:${patch.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT sql FROM migrations ORDER BY version").use { rs ->
                    rs.next()
                    assertTrue(
                        rs.getString(1).contains("line_ref"),
                        "expected an inferred CREATE TABLE migration for line_ref",
                    )
                }
            }
        }

        Files.copy(prev, target)
        val expectedV4Hash = hash(next, 4)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            PatchApplier().apply(
                conn,
                patch,
                expectedToContentHash = expectedV4Hash,
                expectedToSchemaVersion = 4,
            )
        }
        assertEquals(expectedV4Hash, hash(target, 4))
        assertEquals(releasedV3Hash, hash(prev, 3), "producing the patch must not rewrite the v3 contract")
    }

    @Test
    fun `schema 4 patch adds and removes reference keys`() {
        val prev = tmp.newFile("prev-v4.db").toPath()
        val next = tmp.newFile("next-v4.db").toPath()
        val patch = tmp.newFile("v4-v4-patch.db").toPath()
        val target = tmp.newFile("target-v4.db").toPath()
        Files.delete(prev); Files.delete(next); Files.delete(patch); Files.delete(target)

        buildDb(prev, schemaVersion = 4, withLineRef = true)
        buildDb(next, schemaVersion = 4, withLineRef = true, extraRefKey = 999L, dropRefKey = 777L)

        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 31, toVersion = 32)
        assertEquals(1, produced.upsertCounts.getValue("line_ref"))
        assertEquals(1, produced.deleteCounts.getValue("line_ref"))

        Files.copy(prev, target)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            PatchApplier().apply(conn, patch)
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM line_ref WHERE refKeyHash = 999").use { rs ->
                    rs.next(); assertEquals(1, rs.getInt(1))
                }
                st.executeQuery("SELECT COUNT(*) FROM line_ref WHERE refKeyHash = 777").use { rs ->
                    rs.next(); assertEquals(0, rs.getInt(1))
                }
            }
        }
        assertEquals(hash(next, 4), hash(target, 4))
    }

    private fun buildDb(
        path: Path,
        schemaVersion: Int,
        withLineRef: Boolean,
        extraRefKey: Long? = null,
        dropRefKey: Long? = null,
    ) {
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)")
                st.execute("INSERT INTO schema_meta VALUES ('db_version', '1'), ('db_schema_version', '$schemaVersion')")
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                st.execute("INSERT INTO book VALUES (1, 'Book')")
                st.execute("CREATE TABLE line (id INTEGER PRIMARY KEY NOT NULL, bookId INTEGER NOT NULL)")
                st.execute("INSERT INTO line VALUES (10, 1), (11, 1)")
                if (withLineRef) {
                    st.execute(
                        """
                        CREATE TABLE line_ref (
                            bookId INTEGER NOT NULL,
                            refKeyHash INTEGER NOT NULL,
                            lineIndex INTEGER NOT NULL,
                            PRIMARY KEY(bookId, refKeyHash, lineIndex)
                        ) WITHOUT ROWID
                        """.trimIndent(),
                    )
                    val keys = mutableListOf(777L to 0L, 778L to 1L)
                    if (dropRefKey != null) keys.removeAll { it.first == dropRefKey }
                    if (extraRefKey != null) keys.add(extraRefKey to 1L)
                    for ((key, lineIndex) in keys) {
                        st.execute("INSERT INTO line_ref VALUES (1, $key, $lineIndex)")
                    }
                }
            }
        }
    }

    private fun hash(path: Path, schemaVersion: Int): String =
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use {
            LogicalContentHasher.forSchemaVersion(schemaVersion).compute(it)
        }
}
