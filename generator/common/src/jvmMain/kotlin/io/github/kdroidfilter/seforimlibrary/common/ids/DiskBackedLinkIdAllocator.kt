package io.github.kdroidfilter.seforimlibrary.common.ids

import io.github.kdroidfilter.seforimlibrary.common.buildstate.LinkKey
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

/**
 * Stable link-id allocator backed directly by `build_state.db`.
 *
 * Phase-2 only allocates links, so loading the complete allocator lineage into
 * object maps (millions of lines and links) is unnecessary. This allocator
 * keeps the authoritative `id_link` index on disk and only retains a bounded
 * hot-key cache. All mutations are committed together after Phase-2 succeeds.
 */
class DiskBackedLinkIdAllocator private constructor(
    private val connection: Connection,
    private val selectId: PreparedStatement,
    private val insertId: PreparedStatement,
    private var nextId: Long,
) : AutoCloseable {

    private val cache = object : LinkedHashMap<LinkKey, Long>(CACHE_SIZE, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<LinkKey, Long>?): Boolean =
            size > CACHE_SIZE
    }
    private var committed = false
    var reusedCount: Long = 0
        private set
    var freshCount: Long = 0
        private set

    fun linkId(srcLineId: Long, tgtLineId: Long, connectionTypeId: Long): Long {
        val key = LinkKey(srcLineId, tgtLineId, connectionTypeId)
        cache[key]?.let {
            reusedCount++
            return it
        }

        selectId.setLong(1, srcLineId)
        selectId.setLong(2, tgtLineId)
        selectId.setLong(3, connectionTypeId)
        selectId.executeQuery().use { rs ->
            if (rs.next()) {
                val id = rs.getLong(1)
                cache[key] = id
                reusedCount++
                return id
            }
        }

        val id = nextId++
        insertId.setLong(1, srcLineId)
        insertId.setLong(2, tgtLineId)
        insertId.setLong(3, connectionTypeId)
        insertId.setLong(4, id)
        check(insertId.executeUpdate() == 1) { "Failed to persist fresh stable link id for $key" }
        cache[key] = id
        freshCount++
        return id
    }

    fun commit(extraMeta: Map<String, String>) {
        connection.prepareStatement(
            """
            INSERT INTO id_counters(table_name, next_id) VALUES ('link', ?)
            ON CONFLICT(table_name) DO UPDATE SET next_id=excluded.next_id
            """.trimIndent(),
        ).use { ps ->
            ps.setLong(1, nextId)
            ps.executeUpdate()
        }
        connection.prepareStatement(
            """
            INSERT INTO meta(key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """.trimIndent(),
        ).use { ps ->
            for ((key, value) in extraMeta) {
                ps.setString(1, key)
                ps.setString(2, value)
                ps.addBatch()
            }
            ps.executeBatch()
        }
        connection.commit()
        connection.autoCommit = true
        connection.createStatement().use { statement ->
            statement.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            statement.execute("PRAGMA journal_mode=DELETE")
        }
        committed = true
    }

    override fun close() {
        runCatching { if (!committed) connection.rollback() }
        runCatching { selectId.close() }
        runCatching { insertId.close() }
        connection.close()
    }

    companion object {
        private const val CACHE_SIZE = 100_000

        fun open(
            path: Path,
            expectedConnectionTypeName: String,
            expectedConnectionTypeId: Long,
            minimumNextId: Long,
        ): DiskBackedLinkIdAllocator {
            check(Files.isRegularFile(path)) { "build_state.db is required for Phase-2: $path" }
            Class.forName("org.sqlite.JDBC")
            val connection = DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}")
            try {
                connection.createStatement().use { st ->
                    st.execute("PRAGMA synchronous=OFF")
                    st.execute("PRAGMA journal_mode=WAL")
                    st.execute("PRAGMA cache_size=-65536")
                }
                connection.autoCommit = false

                val stateTypeId = connection.prepareStatement(
                    "SELECT id FROM id_lookup WHERE kind='connection_type' AND natural_key=?",
                ).use { ps ->
                    ps.setString(1, expectedConnectionTypeName)
                    ps.executeQuery().use { rs ->
                        check(rs.next()) {
                            "build_state.db has no stable connection-type id for $expectedConnectionTypeName"
                        }
                        rs.getLong(1)
                    }
                }
                check(stateTypeId == expectedConnectionTypeId) {
                    "LINKER connection-type lineage mismatch: db=$expectedConnectionTypeId build_state=$stateTypeId"
                }

                val persistedNext = connection.prepareStatement(
                    "SELECT next_id FROM id_counters WHERE table_name='link'",
                ).use { ps -> ps.executeQuery().use { rs -> if (rs.next()) rs.getLong(1) else 1L } }
                val maxKnown = connection.createStatement().use { st ->
                    st.executeQuery("SELECT COALESCE(MAX(id), 0) FROM id_link").use { rs ->
                        check(rs.next())
                        rs.getLong(1)
                    }
                }
                val nextId = maxOf(1L, persistedNext, maxKnown + 1L, minimumNextId)
                val select = connection.prepareStatement(
                    """
                    SELECT id FROM id_link
                    WHERE src_line_id=? AND tgt_line_id=? AND connection_type_id=?
                    """.trimIndent(),
                )
                val insert = connection.prepareStatement(
                    """
                    INSERT INTO id_link(src_line_id, tgt_line_id, connection_type_id, id)
                    VALUES (?, ?, ?, ?)
                    """.trimIndent(),
                )
                return DiskBackedLinkIdAllocator(connection, select, insert, nextId)
            } catch (t: Throwable) {
                connection.close()
                throw t
            }
        }
    }
}
