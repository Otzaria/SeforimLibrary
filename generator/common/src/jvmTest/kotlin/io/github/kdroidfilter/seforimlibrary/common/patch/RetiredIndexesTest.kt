package io.github.kdroidfilter.seforimlibrary.common.patch

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Indexes removed from seforim.db: gone from the schema, and dropped by patches. */
class RetiredIndexesTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `the schema no longer creates the retired indexes`() {
        val created = schemaDdl().filter { it.startsWith("CREATE INDEX", ignoreCase = true) }
        for (name in RETIRED.keys) {
            assertTrue(created.none { Regex("\\b$name\\b").containsMatchIn(it) }, name)
        }
    }

    @Test
    fun `a patch from a DB that still has them drops exactly the retired indexes`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")
        val ddl = schemaDdl()
        buildDb(prev, ddl + RETIRED.values)
        buildDb(next, ddl)

        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        assertEquals(
            RETIRED.keys.map { """DROP INDEX IF EXISTS main."$it"""" }.sorted(),
            migrationsOf(patch).sorted(),
        )
    }

    /** The live schema's DDL, in creation order. */
    private fun schemaDdl(): List<String> {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        try {
            SeforimDb.Schema.create(driver)
            return driver.executeQuery(
                identifier = null,
                sql = "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid",
                mapper = { cursor ->
                    app.cash.sqldelight.db.QueryResult.Value(
                        buildList { while (cursor.next().value) add(cursor.getString(0)!!) },
                    )
                },
                parameters = 0,
            ).value
        } finally {
            driver.close()
        }
    }

    private fun buildDb(path: Path, ddl: List<String>) {
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st -> ddl.forEach { st.executeUpdate(it) } }
        }
    }

    private fun newDbPath(name: String): Path = tmp.newFolder().toPath().resolve(name)

    private fun migrationsOf(patch: Path): List<String> =
        DriverManager.getConnection("jdbc:sqlite:${patch.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT sql FROM migrations ORDER BY version ASC").use { rs ->
                    buildList { while (rs.next()) add(rs.getString(1)) }
                }
            }
        }

    private companion object {
        val RETIRED = linkedMapOf(
            "idx_link_type" to "CREATE INDEX idx_link_type ON link(connectionTypeId)",
            "idx_line_heref" to "CREATE INDEX idx_line_heref ON line(heRef)",
        )
    }
}
