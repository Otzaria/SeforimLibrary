package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.LinkSuppressedSide
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/** One raw CSV row's visibility verdict after mapping it to the stored link direction. */
internal data class SuppressionContribution(
    val linkId: Long,
    val side: Int,
    val mask: Int,
)

/**
 * Bounded-memory aggregation for per-row visibility verdicts.
 *
 * A normal export resolves millions of link sides. Keeping every `(linkId, side)`
 * in a boxed ConcurrentHashMap adds more than a gigabyte of live heap, so the
 * cross-file state lives in a disposable SQLite database. The importer feeds it
 * bounded batches from one coroutine while link files are resolved in parallel.
 */
internal class LinkSuppressionAccumulator private constructor(
    private val path: Path,
    private val conn: Connection,
) : AutoCloseable {

    data class Result(
        val contributedSides: Long,
        val suppressedSides: Long,
    )

    fun addBatch(rows: List<SuppressionContribution>) {
        if (rows.isEmpty()) return
        conn.prepareStatement(
            """
            INSERT INTO verdict(linkId, side, hasVisible, reasonMask)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(linkId, side) DO UPDATE SET
                hasVisible = verdict.hasVisible | excluded.hasVisible,
                reasonMask = verdict.reasonMask | excluded.reasonMask
            """.trimIndent(),
        ).use { ps ->
            for (row in rows) {
                ps.setLong(1, row.linkId)
                ps.setInt(2, row.side)
                ps.setInt(3, if (row.mask == 0) 1 else 0)
                ps.setInt(4, row.mask)
                ps.addBatch()
            }
            ps.executeBatch()
        }
        conn.commit()
    }

    suspend fun drainSuppressed(
        batchSize: Int,
        consume: suspend (List<LinkSuppressedSide>) -> Unit,
    ): Result {
        val contributed = conn.createStatement().use { st ->
            st.executeQuery("SELECT COUNT(*) FROM verdict").use { rs ->
                rs.next()
                rs.getLong(1)
            }
        }
        var suppressedCount = 0L
        val batch = ArrayList<LinkSuppressedSide>(batchSize)
        conn.createStatement().use { st ->
            st.executeQuery(
                "SELECT linkId, side, reasonMask FROM verdict WHERE hasVisible = 0 ORDER BY linkId, side",
            ).use { rs ->
                while (rs.next()) {
                    batch += LinkSuppressedSide(
                        linkId = rs.getLong(1),
                        side = rs.getInt(2),
                        reasonMask = rs.getInt(3),
                    )
                    suppressedCount++
                    if (batch.size >= batchSize) {
                        consume(batch.toList())
                        batch.clear()
                    }
                }
            }
        }
        if (batch.isNotEmpty()) consume(batch.toList())
        return Result(contributedSides = contributed, suppressedSides = suppressedCount)
    }

    override fun close() {
        runCatching { conn.close() }
        runCatching { Files.deleteIfExists(path) }
        runCatching { Files.deleteIfExists(path.resolveSibling("${path.fileName}-journal")) }
        runCatching { Files.deleteIfExists(path.resolveSibling("${path.fileName}-wal")) }
        runCatching { Files.deleteIfExists(path.resolveSibling("${path.fileName}-shm")) }
    }

    companion object {
        fun create(): LinkSuppressionAccumulator {
            Class.forName("org.sqlite.JDBC")
            val path = Files.createTempFile("seforim-link-visibility-", ".db")
            val conn = DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}")
            try {
                conn.createStatement().use { st ->
                    st.execute("PRAGMA journal_mode=OFF")
                    st.execute("PRAGMA synchronous=OFF")
                    st.execute("PRAGMA temp_store=FILE")
                    st.execute(
                        """
                        CREATE TABLE verdict (
                            linkId INTEGER NOT NULL,
                            side INTEGER NOT NULL CHECK(side IN (0, 1)),
                            hasVisible INTEGER NOT NULL CHECK(hasVisible IN (0, 1)),
                            reasonMask INTEGER NOT NULL,
                            PRIMARY KEY(linkId, side)
                        ) WITHOUT ROWID
                        """.trimIndent(),
                    )
                }
                conn.autoCommit = false
                return LinkSuppressionAccumulator(path, conn)
            } catch (t: Throwable) {
                runCatching { conn.close() }
                runCatching { Files.deleteIfExists(path) }
                throw t
            }
        }
    }
}
