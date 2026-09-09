package io.github.kdroidfilter.seforimlibrary.common.buildstate

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/**
 * Post-write self-check of `build_state.db`, run by every generator stage right
 * after its `snapshotTo`.
 *
 * It answers two questions the stages could not answer before:
 *
 *  1. **Is the file on disk the one this stage just wrote?** A failed snapshot
 *     used to leave the PREVIOUS release's state in place, and nothing
 *     distinguishes it from a fresh one — same name, plausible size, valid
 *     SQLite. [verifyFreshSnapshot] matches the `meta` rows this stage passed as
 *     `extraMeta` (`generator` + `generated_at`, unique per run) against what the
 *     file actually holds.
 *  2. **Are its counters ahead of the DB it describes?** `id_counters.next_id` is
 *     the id the NEXT build hands to a freshly-discovered natural key. If the DB
 *     already holds that id, the next build re-issues a published id to a
 *     different row — the delta/patch fan then changes the meaning of rows rather
 *     than their content.
 *
 * Cost: deliberately NOT [BuildStateReader], which materialises the full
 * `id_line` / `id_link` maps (millions of rows, ~990 MB file). This reads only
 * `meta` and `id_counters` from the snapshot, plus one `MAX(id)` per allocator
 * table from the DB — every one of those `id` columns is an INTEGER PRIMARY KEY,
 * i.e. the rowid, so SQLite answers from the b-tree edge without scanning.
 * Milliseconds even on the 7 GiB DB.
 */
object BuildStateVerifier {

    /**
     * Allocator tables whose row ids the generator deliberately does NOT take
     * from the allocator, so `next_id > MAX(id)` does not hold for them and must
     * not be asserted.
     *
     * `alt_toc_entry` is the only one: both builders insert its rows with an
     * implicit rowid — the Sefaria one via `repository.insertAltTocEntry(...)`
     * (`SefariaAltTocBuilder`, six call sites, all with `id = 0`) and the Otzaria
     * one at `Generator.kt` ("Entry ids stay auto-allocated, matching the Sefaria
     * builder's Phase 1.5 deferral"). `IdAllocatorBindings.insertAltTocEntryStable`
     * exists but has no call site, so `IdTable.ALT_TOC_ENTRY`'s counter never
     * leaves 1 while the shipped DB holds tens of thousands of rows: measured on
     * a real `generateSefariaSqlite` output, `next_id=1` against `MAX(id)=71602`.
     * Asserting here would fail every stage of every release.
     *
     * Delete the entry the moment those inserts start going through the allocator.
     */
    private val NOT_ALLOCATOR_ISSUED: Set<IdTable> = setOf(IdTable.ALT_TOC_ENTRY)

    /** The cheap half of [BuildStateSnapshot]: `meta` + `id_counters`, nothing else. */
    data class Header(val meta: Map<String, String>, val counters: Map<IdTable, Long>)

    /** Reads [Header] from a build_state.db without touching the id maps. */
    fun readHeader(path: Path): Header {
        Class.forName("org.sqlite.JDBC")
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.autoCommit = true
            val meta = HashMap<String, String>()
            if (tableExists(conn, "meta")) {
                conn.createStatement().use { st ->
                    st.executeQuery("SELECT key, value FROM meta").use { rs ->
                        while (rs.next()) meta[rs.getString(1)] = rs.getString(2)
                    }
                }
            }
            val counters = HashMap<IdTable, Long>()
            if (tableExists(conn, "id_counters")) {
                conn.createStatement().use { st ->
                    st.executeQuery("SELECT table_name, next_id FROM id_counters").use { rs ->
                        while (rs.next()) {
                            val table = IdTable.fromTableName(rs.getString(1)) ?: continue
                            counters[table] = rs.getLong(2)
                        }
                    }
                }
            }
            return Header(meta, counters)
        }
    }

    /**
     * Throws [IllegalStateException] unless the build_state at [buildStatePath] is
     * the one this stage just wrote ([expectedMeta], typically the very map passed
     * to `snapshotTo`) and its counters are ahead of every id in [dbPath].
     *
     * Tables absent from [dbPath] or without an `id` column are skipped and named
     * in the log; in practice `SeforimDb.Schema.create` gives every stage all 15,
     * so this only fires for hand-built fixtures. Tables in [NOT_ALLOCATOR_ISSUED]
     * are skipped for a different reason and reported separately.
     */
    fun verifyFreshSnapshot(
        buildStatePath: Path,
        dbPath: Path,
        expectedMeta: Map<String, String>,
        logger: Logger = Logger.withTag("BuildStateVerifier"),
    ) {
        check(Files.exists(buildStatePath)) {
            "build_state was reported written but $buildStatePath does not exist"
        }
        val header = readHeader(buildStatePath)

        val schemaVersion = header.meta["schema_version"]?.toIntOrNull()
        checkNotNull(schemaVersion) {
            "build_state at $buildStatePath has no usable meta.schema_version " +
                "(got '${header.meta["schema_version"]}') — it is not a snapshot this build wrote"
        }
        check(schemaVersion <= BuildStateSchema.CURRENT_VERSION) {
            "build_state at $buildStatePath has schema_version=$schemaVersion, " +
                "newer than supported ${BuildStateSchema.CURRENT_VERSION}"
        }
        for ((key, expected) in expectedMeta) {
            val actual = header.meta[key]
            check(actual == expected) {
                "build_state at $buildStatePath is not the snapshot this stage wrote: " +
                    "meta.$key='$actual', expected '$expected'. A previous build's state was " +
                    "left in place — publishing it would re-issue ids already handed out."
            }
        }
        check(header.counters.isNotEmpty()) {
            "build_state at $buildStatePath carries no id_counters rows"
        }

        check(Files.exists(dbPath)) {
            "build_state at $buildStatePath cannot be verified: the DB it describes, $dbPath, does not exist"
        }

        val violations = ArrayList<String>()
        val skipped = ArrayList<String>()
        val notIssued = ArrayList<String>()
        var checked = 0
        Class.forName("org.sqlite.JDBC")
        DriverManager.getConnection("jdbc:sqlite:${dbPath.toAbsolutePath()}").use { conn ->
            conn.autoCommit = true
            for (table in IdTable.values()) {
                if (table in NOT_ALLOCATOR_ISSUED) {
                    notIssued += table.tableName
                    continue
                }
                if (!tableExists(conn, table.tableName) || !hasIdColumn(conn, table.tableName)) {
                    skipped += table.tableName
                    continue
                }
                val maxId = maxId(conn, table.tableName)
                val nextId = header.counters[table]
                if (nextId == null) {
                    violations += "${table.tableName}: no id_counters row, but the DB holds MAX(id)=$maxId"
                    continue
                }
                // next_id is handed to the next fresh key, so it must be strictly
                // above everything the DB already holds.
                if (nextId <= maxId) {
                    violations += "${table.tableName}: next_id=$nextId <= MAX(id)=$maxId"
                }
                checked++
            }
        }
        check(violations.isEmpty()) {
            "build_state at $buildStatePath is behind the DB at $dbPath — the next build would " +
                "re-issue ids this build already published: ${violations.joinToString("; ")}"
        }
        logger.i {
            "build_state verified against $dbPath: $checked counters ahead of the DB" +
                (if (skipped.isEmpty()) "" else " (absent here: ${skipped.joinToString()})") +
                (if (notIssued.isEmpty()) "" else " (ids not allocator-issued: ${notIssued.joinToString()})")
        }
    }

    private fun maxId(conn: Connection, table: String): Long =
        conn.createStatement().use { st ->
            st.executeQuery("SELECT COALESCE(MAX(id), 0) FROM \"$table\"").use { rs ->
                if (rs.next()) rs.getLong(1) else 0L
            }
        }

    private fun hasIdColumn(conn: Connection, table: String): Boolean =
        conn.createStatement().use { st ->
            st.executeQuery("PRAGMA table_info(\"$table\")").use { rs ->
                while (rs.next()) if (rs.getString("name") == "id") return true
            }
            false
        }

    private fun tableExists(conn: Connection, name: String): Boolean {
        conn.prepareStatement(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        ).use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }
}
