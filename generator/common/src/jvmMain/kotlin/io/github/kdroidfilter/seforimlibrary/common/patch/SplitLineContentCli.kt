package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentShape
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager

/**
 * Converts a finished schema-5-shaped `seforim.db` to the schema-6 shape.
 *
 *  - `line.content` moves to `line_content(id, content)`, one row per line, same id.
 *  - `version_line.content` becomes nullable and is NULL exactly when it is
 *    byte-identical to the base line's text.
 *
 * Every generator stage keeps writing the schema-5 working shape; this is the last
 * rewriting stage, before `stampSchemaVersion` and `analyzeSeforimDb`.
 *
 * Disk: the text is moved in id-ordered chunks, each its own transaction that
 * re-inserts the chunk's `line` rows without text, so later chunks reuse the freed
 * pages and the file never holds two copies. One transaction would need a rollback
 * journal as large as the whole `line` table. An interrupted run resumes from
 * `MAX(line_content.id)`.
 *
 * Idempotent: on an already split DB it only re-validates.
 *
 * System properties: `dbPath` (required), `chunkRows` (default [DEFAULT_CHUNK_ROWS]),
 * `vacuumInto` (optional: also write a compacted copy there; the target must not exist).
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SplitLineContentCli")

    val dbPath = System.getProperty("dbPath") ?: error("-PdbPath= missing")
    val chunkRows = System.getProperty("chunkRows")?.toIntOrNull() ?: DEFAULT_CHUNK_ROWS
    val vacuumInto = System.getProperty("vacuumInto")?.takeIf { it.isNotBlank() }
    val path = Paths.get(dbPath)
    require(Files.isRegularFile(path)) { "Database file not found: $dbPath" }

    val started = System.nanoTime()
    DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
        val report = splitLineContent(conn, chunkRows = chunkRows, dbFile = path, logger = logger)
        logger.i { report.describe() }
        if (vacuumInto != null) {
            val target = Paths.get(vacuumInto).toAbsolutePath()
            require(!Files.exists(target)) { "vacuumInto target already exists: $target" }
            conn.prepareStatement("VACUUM INTO ?").use { ps ->
                ps.setString(1, target.toString())
                ps.execute()
            }
            logger.i { "VACUUM INTO $target: ${Files.size(target)} bytes (source ${Files.size(path)} bytes)" }
        }
    }
    logger.i { "Split $dbPath in ${(System.nanoTime() - started) / 1_000_000} ms" }
}

internal const val DEFAULT_CHUNK_ROWS: Int = 50_000

/** What [splitLineContent] did, with the page accounting the release disk budget needs. */
internal data class SplitLineContentReport(
    val alreadySplit: Boolean,
    val lines: Long,
    val versionLines: Long,
    val versionLinesInherited: Long,
    val pageSize: Long,
    val pageCountBefore: Long,
    val freelistBefore: Long,
    val pageCountAfter: Long,
    val freelistAfter: Long,
    val peakPageCount: Long,
    val peakJournalBytes: Long,
) {
    fun describe(): String =
        "line_content split ${if (alreadySplit) "already in place (validated)" else "done"}: " +
            "lines=$lines version_line=$versionLines (NULL=inherit: $versionLinesInherited); " +
            "pages ${pageCountBefore}→$pageCountAfter (peak $peakPageCount, page_size=$pageSize), " +
            "freelist ${freelistBefore}→$freelistAfter, peak journal ${peakJournalBytes} bytes"
}

/**
 * Brings [conn]'s DB to the schema-6 line/version_line shape; see [main].
 * [dbFile] is only used to measure the rollback journal.
 */
internal fun splitLineContent(
    conn: Connection,
    chunkRows: Int = DEFAULT_CHUNK_ROWS,
    dbFile: Path? = null,
    logger: Logger = Logger.withTag("SplitLineContent"),
): SplitLineContentReport {
    require(chunkRows > 0) { "chunkRows=$chunkRows must be positive" }
    check(conn.autoCommit) { "splitLineContent requires an unowned JDBC connection" }

    val pageSize = pragmaLong(conn, "page_size")
    val pageCountBefore = pragmaLong(conn, "page_count")
    val freelistBefore = pragmaLong(conn, "freelist_count")
    var peakPages = pageCountBefore
    var peakJournal = 0L
    fun sample() {
        peakPages = maxOf(peakPages, pragmaLong(conn, "page_count"))
        val journal = dbFile?.resolveSibling("${dbFile.fileName}-journal")
        if (journal != null && Files.exists(journal)) peakJournal = maxOf(peakJournal, Files.size(journal))
    }

    val lineHasContent = columns(conn, "line").contains("content")
    val hasLineContent = tableExists(conn, "line_content")
    check(lineHasContent || hasLineContent) {
        "line has no content column and there is no line_content table — not a seforim.db"
    }
    val versionLineNeedsRebuild = tableExists(conn, VERSION_LINE_OLD) || (
        tableExists(conn, "version_line") &&
            PatchDbSchema.readTableInfo(conn, "main", "version_line").single { it.name == "content" }.notNull
        )
    val alreadySplit = !lineHasContent && !versionLineNeedsRebuild

    // WAL would hold every page a transaction writes; DELETE journals only the
    // overwritten ones, and reused freelist pages not at all.
    val journalMode = pragmaString(conn, "journal_mode")
    val switchJournal = !alreadySplit && journalMode.equals("wal", ignoreCase = true)
    if (switchJournal) pragmaString(conn, "journal_mode=DELETE")
    // The chunk move deletes line rows; version_line and tocEntry must not cascade.
    val foreignKeys = pragmaLong(conn, "foreign_keys")
    // Freed pages are zeroed: whatever stays on the freelist compresses to nothing
    // in the published seforim.db.zst instead of shipping stale text.
    val secureDelete = pragmaLong(conn, "secure_delete")
    conn.createStatement().use { st ->
        st.execute("PRAGMA foreign_keys = OFF")
        st.execute("PRAGMA temp_store = MEMORY")
        st.execute("PRAGMA secure_delete = ON")
    }
    try {
        if (lineHasContent) {
            moveLineContent(conn, chunkRows, logger, ::sample)
            requireLineContentComplete(conn)
            conn.createStatement().use { it.execute("ALTER TABLE line DROP COLUMN content") }
            sample()
            logger.i { "Dropped line.content" }
        }
        if (versionLineNeedsRebuild) {
            rebuildVersionLine(conn, chunkRows, logger, ::sample)
            sample()
        }
    } finally {
        conn.createStatement().use { st ->
            st.execute("PRAGMA foreign_keys = ${if (foreignKeys == 1L) "ON" else "OFF"}")
            st.execute("PRAGMA secure_delete = ${when (secureDelete) { 1L -> "ON"; 2L -> "FAST"; else -> "OFF" }}")
        }
        if (switchJournal) pragmaString(conn, "journal_mode=$journalMode")
    }

    validateSplit(conn)
    val versionLines = if (tableExists(conn, "version_line")) count(conn, "SELECT COUNT(*) FROM version_line") else 0
    val inherited = if (tableExists(conn, "version_line")) {
        count(conn, "SELECT COUNT(*) FROM version_line WHERE content IS NULL")
    } else 0
    return SplitLineContentReport(
        alreadySplit = alreadySplit,
        lines = count(conn, "SELECT COUNT(*) FROM line"),
        versionLines = versionLines,
        versionLinesInherited = inherited,
        pageSize = pageSize,
        pageCountBefore = pageCountBefore,
        freelistBefore = freelistBefore,
        pageCountAfter = pragmaLong(conn, "page_count"),
        freelistAfter = pragmaLong(conn, "freelist_count"),
        peakPageCount = maxOf(peakPages, pragmaLong(conn, "page_count")),
        peakJournalBytes = peakJournal,
    )
}

private fun moveLineContent(conn: Connection, chunkRows: Int, logger: Logger, sample: () -> Unit) {
    conn.createStatement().use {
        it.execute(
            """
            CREATE TABLE IF NOT EXISTS line_content (
                id INTEGER PRIMARY KEY NOT NULL,
                content TEXT NOT NULL,
                FOREIGN KEY (id) REFERENCES line(id) ON DELETE CASCADE
            )
            """.trimIndent(),
        )
    }
    // Each committed chunk is moved whole, so MAX(id) is an exact resume point.
    var lastId = conn.createStatement().use { st ->
        st.executeQuery("SELECT MAX(id) FROM line_content").use { rs ->
            rs.next()
            rs.getLong(1).takeUnless { rs.wasNull() } ?: Long.MIN_VALUE
        }
    }
    if (lastId != Long.MIN_VALUE) logger.i { "Resuming line_content copy after id $lastId" }
    val keep = PatchDbSchema.readTableInfo(conn, "main", "line").map { it.name }.filter { it != "content" }
    val keepCsv = keep.joinToString(",") { "\"$it\"" }
    conn.createStatement().use { st ->
        st.execute("DROP TABLE IF EXISTS temp.line_chunk")
        st.execute("CREATE TEMP TABLE line_chunk AS SELECT $keepCsv, content FROM main.line WHERE 0")
    }
    // Shrinking a row in place never frees its page, so each chunk is deleted and
    // re-inserted slim; its pages go back to the freelist for the next chunk.
    val chunkSteps = listOf(
        "INSERT INTO temp.line_chunk SELECT $keepCsv, content FROM main.line WHERE id > ? AND id <= ? ORDER BY id",
        "DELETE FROM main.line WHERE id > ? AND id <= ?",
    )
    val upperSql = "SELECT MAX(id) FROM (SELECT id FROM main.line WHERE id > ? ORDER BY id LIMIT $chunkRows)"
    var moved = 0L
    while (true) {
        val upper = conn.prepareStatement(upperSql).use { ps ->
            ps.setLong(1, lastId)
            ps.executeQuery().use { rs -> rs.next(); rs.getLong(1).takeUnless { rs.wasNull() } }
        } ?: break
        inTransaction(conn) {
            val counts = chunkSteps.map { sql ->
                conn.prepareStatement(sql).use { ps -> ps.setLong(1, lastId); ps.setLong(2, upper); ps.executeUpdate() }
            }
            conn.createStatement().use { st ->
                val contents = st.executeUpdate(
                    "INSERT INTO main.line_content(id, content) SELECT id, content FROM temp.line_chunk ORDER BY id",
                )
                val slim = st.executeUpdate(
                    "INSERT INTO main.line($keepCsv, content) SELECT $keepCsv, '' FROM temp.line_chunk ORDER BY id",
                )
                st.execute("DELETE FROM temp.line_chunk")
                check(counts.all { it == contents } && slim == contents) {
                    "chunk ($lastId, $upper]: row counts diverged ${counts + contents + slim}"
                }
                moved += contents
            }
            sample()
        }
        lastId = upper
    }
    conn.createStatement().use { it.execute("DROP TABLE temp.line_chunk") }
    logger.i { "Moved $moved line texts into line_content" }
}

private fun requireLineContentComplete(conn: Connection) {
    val lines = count(conn, "SELECT COUNT(*) FROM line")
    val contents = count(conn, "SELECT COUNT(*) FROM line_content")
    check(lines == contents) { "line has $lines rows but line_content $contents — refusing to drop line.content" }
}

private const val VERSION_LINE_OLD = "version_line_schema5"

/**
 * SQLite's table rebuild, the only way to relax NOT NULL on an existing column, done
 * in committed chunks like the line move; a leftover [VERSION_LINE_OLD] means resume.
 */
private fun rebuildVersionLine(conn: Connection, chunkRows: Int, logger: Logger, sample: () -> Unit) {
    if (!tableExists(conn, VERSION_LINE_OLD)) {
        val cols = PatchDbSchema.readTableInfo(conn, "main", "version_line").map { it.name }
        check(cols == listOf("versionId", "lineId", "content", "charCount")) {
            "version_line has columns $cols — this rebuild only knows the schema-5 shape"
        }
        inTransaction(conn) {
            conn.createStatement().use { st ->
                st.execute("DROP INDEX IF EXISTS idx_version_line_line")
                st.execute("ALTER TABLE version_line RENAME TO $VERSION_LINE_OLD")
                st.execute(
                    """
                    CREATE TABLE version_line (
                        versionId INTEGER NOT NULL,
                        lineId INTEGER NOT NULL,
                        content TEXT,
                        charCount INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (versionId, lineId),
                        FOREIGN KEY (versionId) REFERENCES book_version(id) ON DELETE CASCADE,
                        FOREIGN KEY (lineId) REFERENCES line(id) ON DELETE CASCADE
                    )
                    """.trimIndent(),
                )
            }
        }
    }
    val fkViolationsBefore = foreignKeyViolations(conn, VERSION_LINE_OLD) + foreignKeyViolations(conn, "version_line")
    // Rowids are carried over; the BLOB comparison is byte equality, immune to collation.
    val upperSql = "SELECT MAX(rowid) FROM (SELECT rowid FROM $VERSION_LINE_OLD ORDER BY rowid LIMIT $chunkRows)"
    val copySql = """
        INSERT INTO version_line(rowid, versionId, lineId, content, charCount)
        SELECT o.rowid, o.versionId, o.lineId,
               CASE WHEN lc.id IS NOT NULL AND CAST(o.content AS BLOB) = CAST(lc.content AS BLOB)
                    THEN NULL ELSE o.content END,
               o.charCount
        FROM $VERSION_LINE_OLD o
        LEFT JOIN line_content lc ON lc.id = o.lineId
        WHERE o.rowid <= ?
        ORDER BY o.rowid
    """.trimIndent()
    val deleteSql = "DELETE FROM $VERSION_LINE_OLD WHERE rowid <= ?"
    var moved = 0L
    while (true) {
        val upper = conn.createStatement().use { st ->
            st.executeQuery(upperSql).use { rs -> rs.next(); rs.getLong(1).takeUnless { rs.wasNull() } }
        } ?: break
        inTransaction(conn) {
            val copied = conn.prepareStatement(copySql).use { ps -> ps.setLong(1, upper); ps.executeUpdate() }
            val deleted = conn.prepareStatement(deleteSql).use { ps -> ps.setLong(1, upper); ps.executeUpdate() }
            check(copied == deleted) { "version_line chunk (.., $upper]: copied $copied rows but removed $deleted" }
            sample()
            moved += copied
        }
    }
    inTransaction(conn) {
        conn.createStatement().use { st ->
            st.execute("DROP TABLE $VERSION_LINE_OLD")
            st.execute("CREATE INDEX idx_version_line_line ON version_line(lineId)")
        }
        val after = foreignKeyViolations(conn, "version_line")
        check(after <= fkViolationsBefore) {
            "version_line rebuild introduced FK violations ($fkViolationsBefore → $after)"
        }
    }
    logger.i { "Rebuilt version_line with nullable content ($moved rows moved)" }
}

private inline fun <T> inTransaction(conn: Connection, block: () -> T): T {
    conn.autoCommit = false
    try {
        val result = block()
        conn.commit()
        return result
    } catch (failure: Throwable) {
        runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        conn.autoCommit = true
    }
}

/** The schema-6 invariants; throws on the first one that does not hold. */
internal fun validateSplit(conn: Connection) {
    check(LineContentShape.isSplit(conn)) { "DB is not in the line_content shape" }
    val lines = count(conn, "SELECT COUNT(*) FROM line")
    val contents = count(conn, "SELECT COUNT(*) FROM line_content")
    check(lines == contents) { "line has $lines rows but line_content $contents" }
    val orphans = count(
        conn,
        "SELECT COUNT(*) FROM line_content lc LEFT JOIN line l ON l.id = lc.id WHERE l.id IS NULL",
    )
    check(orphans == 0L) { "$orphans line_content rows have no line" }
    check(foreignKeyViolations(conn, "line_content") == 0L) { "line_content has FK violations" }
    check(!tableExists(conn, VERSION_LINE_OLD)) { "$VERSION_LINE_OLD is left over from an interrupted rebuild" }
    if (tableExists(conn, "version_line")) {
        val content = PatchDbSchema.readTableInfo(conn, "main", "version_line").single { it.name == "content" }
        check(!content.notNull) { "version_line.content is still NOT NULL" }
        val indexes = conn.createStatement().use { st ->
            st.executeQuery("SELECT name FROM pragma_index_list('version_line')").use { rs ->
                buildSet { while (rs.next()) add(rs.getString(1)) }
            }
        }
        check("idx_version_line_line" in indexes) { "idx_version_line_line is missing" }
    }
}

private fun foreignKeyViolations(conn: Connection, table: String): Long =
    conn.createStatement().use { st ->
        st.executeQuery("PRAGMA foreign_key_check(\"$table\")").use { rs ->
            var n = 0L
            while (rs.next()) n++
            n
        }
    }

private fun columns(conn: Connection, table: String): Set<String> =
    PatchDbSchema.readTableInfo(conn, "main", table).mapTo(HashSet()) { it.name }

private fun tableExists(conn: Connection, name: String): Boolean =
    conn.prepareStatement("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").use { ps ->
        ps.setString(1, name)
        ps.executeQuery().use { it.next() }
    }

private fun count(conn: Connection, sql: String): Long =
    conn.createStatement().use { st -> st.executeQuery(sql).use { rs -> rs.next(); rs.getLong(1) } }

private fun pragmaLong(conn: Connection, pragma: String): Long =
    conn.createStatement().use { st -> st.executeQuery("PRAGMA $pragma").use { rs -> rs.next(); rs.getLong(1) } }

private fun pragmaString(conn: Connection, pragma: String): String =
    conn.createStatement().use { st -> st.executeQuery("PRAGMA $pragma").use { rs -> rs.next(); rs.getString(1) } }
