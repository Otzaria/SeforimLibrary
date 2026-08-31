package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class PatchLinkSuppressedSideEvolutionTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `schema 2 to 3 preserves the released from-hash contract`() {
        val prev = tmp.newFile("prev-v2.db").toPath()
        val next = tmp.newFile("next-v3.db").toPath()
        val patch = tmp.newFile("v2-v3-patch.db").toPath()
        val target = tmp.newFile("target.db").toPath()
        Files.delete(prev); Files.delete(next); Files.delete(patch); Files.delete(target)

        buildDb(prev, schemaVersion = 2, reasonMask = null)
        buildDb(next, schemaVersion = 3, reasonMask = 4)

        val releasedV2Hash = hash(prev, 2)
        assertNotEquals(
            releasedV2Hash,
            hash(prev, 3),
            "the regression is only caught if an absent schema-3 table changes the current-order hash",
        )

        val produced = PatchDbProducer().produce(
            prev,
            next,
            patch,
            fromVersion = 22,
            toVersion = 23,
            fromSchemaVersion = 2,
            toSchemaVersion = 3,
        )
        assertEquals(1, produced.upsertCounts.getValue("link_suppressed_side"))
        DriverManager.getConnection("jdbc:sqlite:${patch.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT value FROM patch_meta WHERE key='schema_version'").use { rs ->
                    rs.next()
                    assertEquals(
                        PatchDbSchema.CURRENT_VERSION.toString(),
                        rs.getString(1),
                        "patch_meta carries the artifact format, not the target DB schema",
                    )
                }
            }
        }

        Files.copy(prev, target)
        val expectedV3Hash = hash(next, 3)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            PatchApplier().apply(
                conn,
                patch,
                expectedToContentHash = expectedV3Hash,
                expectedToSchemaVersion = 3,
            )
        }
        assertEquals(expectedV3Hash, hash(target, 3))
        assertEquals(releasedV2Hash, hash(prev, 2), "producing the patch must not rewrite the v2 contract")
    }

    @Test
    fun `schema 3 patch updates reason mask on a stable key`() {
        val prev = tmp.newFile("prev-v3.db").toPath()
        val next = tmp.newFile("next-v3.db").toPath()
        val patch = tmp.newFile("v3-v3-patch.db").toPath()
        val target = tmp.newFile("target-v3.db").toPath()
        Files.delete(prev); Files.delete(next); Files.delete(patch); Files.delete(target)

        buildDb(prev, schemaVersion = 3, reasonMask = 4)
        buildDb(next, schemaVersion = 3, reasonMask = 5)
        val produced = PatchDbProducer().produce(
            prev,
            next,
            patch,
            fromVersion = 23,
            toVersion = 24,
            fromSchemaVersion = 3,
            toSchemaVersion = 3,
        )
        assertEquals(1, produced.upsertCounts.getValue("link_suppressed_side"))

        Files.copy(prev, target)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            PatchApplier().apply(conn, patch)
            conn.createStatement().use { st ->
                st.executeQuery("SELECT reasonMask FROM link_suppressed_side WHERE linkId=100 AND side=0").use { rs ->
                    rs.next()
                    assertEquals(5, rs.getInt(1))
                }
            }
        }
        assertEquals(hash(next, 3), hash(target, 3))
    }

    private fun buildDb(path: Path, schemaVersion: Int, reasonMask: Int?) {
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)")
                st.execute("INSERT INTO schema_meta VALUES ('db_version', '1'), ('db_schema_version', '$schemaVersion')")
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                st.execute("INSERT INTO book VALUES (1, 'Book')")
                st.execute("CREATE TABLE line (id INTEGER PRIMARY KEY NOT NULL, bookId INTEGER NOT NULL)")
                st.execute("INSERT INTO line VALUES (10, 1), (11, 1)")
                st.execute("CREATE TABLE connection_type (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
                st.execute("INSERT INTO connection_type VALUES (1, 'OTHER')")
                st.execute(
                    """
                    CREATE TABLE link (
                        id INTEGER PRIMARY KEY NOT NULL,
                        sourceBookId INTEGER NOT NULL,
                        targetBookId INTEGER NOT NULL,
                        sourceLineId INTEGER NOT NULL,
                        targetLineId INTEGER NOT NULL,
                        connectionTypeId INTEGER NOT NULL
                    )
                    """.trimIndent(),
                )
                st.execute("INSERT INTO link VALUES (100, 1, 1, 10, 11, 1)")
                if (schemaVersion >= 3) {
                    st.execute(
                        """
                        CREATE TABLE link_suppressed_side (
                            linkId INTEGER NOT NULL,
                            side INTEGER NOT NULL CHECK(side IN (0, 1)),
                            reasonMask INTEGER NOT NULL CHECK(reasonMask > 0 AND (reasonMask & -16) = 0),
                            PRIMARY KEY(linkId, side),
                            FOREIGN KEY(linkId) REFERENCES link(id) ON DELETE CASCADE
                        )
                        """.trimIndent(),
                    )
                    if (reasonMask != null) {
                        st.execute("INSERT INTO link_suppressed_side VALUES (100, 0, $reasonMask)")
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
