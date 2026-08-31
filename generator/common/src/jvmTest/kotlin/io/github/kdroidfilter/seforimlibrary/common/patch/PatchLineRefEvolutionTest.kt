package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Schema 3 → 4 evolution: the `line_ref` reference index and the `line_dh`
 * dibbur-hamatchil index appear as brand new tables. The producer must
 * reset them through CREATE TABLE migrations, ship their full contents as
 * upserts, and
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
        val deltaClientTarget = tmp.newFile("target-without-uncontracted-tables.db").toPath()
        Files.delete(prev); Files.delete(next); Files.delete(patch); Files.delete(target)
        Files.delete(deltaClientTarget)

        // PR18 can put these tables in a full schema-3 DB before schema 4
        // signs them into the contract. A client that reached the same
        // release through a schema-3 patch can legitimately lack them.
        buildDb(prev, schemaVersion = 3, withIndexes = true, staleIndexes = true)
        buildDb(next, schemaVersion = 4, withIndexes = true)

        val releasedV3Hash = hash(prev, 3)
        assertNotEquals(
            releasedV3Hash,
            hash(prev, 4),
            "the regression is only caught if an absent schema-4 table changes the current-order hash",
        )

        val produced = PatchDbProducer().produce(
            prev,
            next,
            patch,
            fromVersion = 30,
            toVersion = 31,
            fromSchemaVersion = 3,
            toSchemaVersion = 4,
        )
        assertEquals(2, produced.upsertCounts.getValue("line_ref"))
        assertEquals(2, produced.upsertCounts.getValue("line_dh"))
        assertEquals(0, produced.deleteCounts.getValue("line_ref"))
        assertEquals(0, produced.deleteCounts.getValue("line_dh"))

        // Contract promotion resets any unsigned physical copy before loading
        // the full signed snapshot.
        DriverManager.getConnection("jdbc:sqlite:${patch.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                val migrations = buildList {
                    st.executeQuery("SELECT sql FROM migrations ORDER BY version").use { rs ->
                        while (rs.next()) add(rs.getString(1))
                    }
                }
                for (table in listOf("line_ref", "line_dh")) {
                    assertTrue(
                        migrations.any { it == "DROP TABLE IF EXISTS \"$table\"" },
                        "expected an inferred reset migration for $table",
                    )
                    assertTrue(
                        migrations.any { it.startsWith("CREATE TABLE") && it.contains(table) },
                        "expected an inferred create migration for $table",
                    )
                }
            }
        }

        val expectedV4Hash = hash(next, 4)
        Files.copy(prev, target)
        applyAndAssert(target, patch, expectedV4Hash)

        Files.copy(prev, deltaClientTarget)
        DriverManager.getConnection("jdbc:sqlite:${deltaClientTarget.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("DROP TABLE line_ref")
                st.execute("DROP TABLE line_dh")
            }
        }
        applyAndAssert(deltaClientTarget, patch, expectedV4Hash)
        assertEquals(releasedV3Hash, hash(prev, 3), "producing the patch must not rewrite the v3 contract")
    }

    @Test
    fun `hash failure rolls back promoted table DDL and data`() {
        val prev = tmp.newFile("rollback-prev-v3.db").toPath()
        val next = tmp.newFile("rollback-next-v4.db").toPath()
        val patch = tmp.newFile("rollback-v3-v4-patch.db").toPath()
        val target = tmp.newFile("rollback-target.db").toPath()
        Files.delete(prev); Files.delete(next); Files.delete(patch); Files.delete(target)

        buildDb(prev, schemaVersion = 3, withIndexes = true, staleIndexes = true)
        buildDb(next, schemaVersion = 4, withIndexes = true)
        PatchDbProducer().produce(
            prev,
            next,
            patch,
            fromVersion = 30,
            toVersion = 31,
            fromSchemaVersion = 3,
            toSchemaVersion = 4,
        )
        Files.copy(prev, target)
        val beforeHash = hash(target, 3)

        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            assertFailsWith<IllegalStateException> {
                PatchApplier().apply(
                    conn,
                    patch,
                    expectedToContentHash = "0".repeat(64),
                    expectedToSchemaVersion = 4,
                )
            }
        }

        assertEquals(beforeHash, hash(target, 3))
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT refKeyHash FROM line_ref ORDER BY lineIndex").use { rs ->
                    assertTrue(rs.next()); assertEquals(777L, rs.getLong(1))
                    assertTrue(rs.next()); assertEquals(999L, rs.getLong(1))
                }
                st.executeQuery("SELECT dhText FROM line_dh ORDER BY lineIndex").use { rs ->
                    assertTrue(rs.next()); assertEquals("מאימתי קורין", rs.getString(1))
                    assertTrue(rs.next()); assertEquals("רשומה ישנה", rs.getString(1))
                }
                st.executeQuery("SELECT value FROM schema_meta WHERE key='db_schema_version'").use { rs ->
                    assertTrue(rs.next()); assertEquals("3", rs.getString(1))
                }
            }
        }
    }

    @Test
    fun `schema 4 patch adds and removes reference keys`() {
        val prev = tmp.newFile("prev-v4.db").toPath()
        val next = tmp.newFile("next-v4.db").toPath()
        val patch = tmp.newFile("v4-v4-patch.db").toPath()
        val target = tmp.newFile("target-v4.db").toPath()
        Files.delete(prev); Files.delete(next); Files.delete(patch); Files.delete(target)

        buildDb(prev, schemaVersion = 4, withIndexes = true)
        buildDb(next, schemaVersion = 4, withIndexes = true, extraRefKey = 999L, dropRefKey = 777L)

        val produced = PatchDbProducer().produce(
            prev,
            next,
            patch,
            fromVersion = 31,
            toVersion = 32,
            fromSchemaVersion = 4,
            toSchemaVersion = 4,
        )
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

    @Test
    fun `schema 3 patch ignores physically present unsigned indexes`() {
        val prev = tmp.newFile("prev-v3-unsigned.db").toPath()
        val next = tmp.newFile("next-v3-unsigned.db").toPath()
        val patch = tmp.newFile("v3-v3-unsigned-patch.db").toPath()
        Files.delete(prev); Files.delete(next); Files.delete(patch)

        buildDb(prev, schemaVersion = 3, withIndexes = true, staleIndexes = true)
        buildDb(next, schemaVersion = 3, withIndexes = true)

        val produced = PatchDbProducer().produce(
            prev,
            next,
            patch,
            fromVersion = 29,
            toVersion = 30,
            fromSchemaVersion = 3,
            toSchemaVersion = 3,
        )
        assertFalse("line_ref" in produced.upsertCounts)
        assertFalse("line_dh" in produced.upsertCounts)
        DriverManager.getConnection("jdbc:sqlite:${patch.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' " +
                        "AND name IN ('upsert_line_ref', 'upsert_line_dh')",
                ).use { rs -> rs.next(); assertEquals(0, rs.getInt(1)) }
            }
        }
    }

    @Test
    fun `producer rejects schema downgrades`() {
        val prev = tmp.newFile("prev-v4-downgrade.db").toPath()
        val next = tmp.newFile("next-v3-downgrade.db").toPath()
        val patch = tmp.newFile("downgrade-patch.db").toPath()
        Files.delete(prev); Files.delete(next); Files.delete(patch)
        buildDb(prev, schemaVersion = 4, withIndexes = true)
        buildDb(next, schemaVersion = 3, withIndexes = true)

        assertFailsWith<IllegalArgumentException> {
            PatchDbProducer().produce(
                prev,
                next,
                patch,
                fromVersion = 31,
                toVersion = 32,
                fromSchemaVersion = 4,
                toSchemaVersion = 3,
            )
        }
    }

    private fun buildDb(
        path: Path,
        schemaVersion: Int,
        withIndexes: Boolean,
        staleIndexes: Boolean = false,
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
                if (withIndexes) {
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
                    val keys = if (staleIndexes) {
                        mutableListOf(777L to 0L, 999L to 1L)
                    } else {
                        mutableListOf(777L to 0L, 778L to 1L)
                    }
                    if (dropRefKey != null) keys.removeAll { it.first == dropRefKey }
                    if (extraRefKey != null) keys.add(extraRefKey to 1L)
                    for ((key, lineIndex) in keys) {
                        st.execute("INSERT INTO line_ref VALUES (1, $key, $lineIndex)")
                    }
                    st.execute(
                        """
                        CREATE TABLE line_dh (
                            bookId INTEGER NOT NULL,
                            dhText TEXT NOT NULL,
                            lineIndex INTEGER NOT NULL,
                            PRIMARY KEY(bookId, dhText, lineIndex)
                        ) WITHOUT ROWID
                        """.trimIndent(),
                    )
                    if (staleIndexes) {
                        st.execute("INSERT INTO line_dh VALUES (1, 'מאימתי קורין', 0), (1, 'רשומה ישנה', 1)")
                    } else {
                        st.execute("INSERT INTO line_dh VALUES (1, 'מאימתי קורין', 0), (1, 'עד סוף האשמורה', 1)")
                    }
                }
            }
        }
    }

    private fun applyAndAssert(target: Path, patch: Path, expectedV4Hash: String) {
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
    }

    private fun hash(path: Path, schemaVersion: Int): String =
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use {
            LogicalContentHasher.forSchemaVersion(schemaVersion).compute(it)
        }
}
