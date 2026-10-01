package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentCompression
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentShape
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Compresses the line text of a finished schema-6 `seforim.db`: every
 * `line_content.content` and every non-NULL `version_line.content` becomes one zstd
 * frame (BLOB) made with [LineContentCompression.bundledDictionary], which is
 * stored in `zstd_dict`.
 *
 * Runs after the last reader of plain text (Phase-2 `generateLinkerLinks`) and
 * before `compactSeforimDb`, which reclaims the space the shorter rows free.
 *
 * Every frame is decompressed and compared before it is written. Rows already
 * stored as BLOB are skipped, so an interrupted run resumes and a second run only
 * checks that no TEXT row is left. NULL in `version_line` (inherit the base text)
 * stays NULL. Compression keeps the schema version: readers decode when `zstd_dict` exists.
 *
 * System properties: `dbPath` (required), `chunkRows` (default [COMPRESS_CHUNK_ROWS]),
 * `threads` (default: available processors).
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("CompressLineContentCli")

    val dbPath = System.getProperty("dbPath") ?: error("-PdbPath= missing")
    val chunkRows = System.getProperty("chunkRows")?.toIntOrNull() ?: COMPRESS_CHUNK_ROWS
    val threads = System.getProperty("threads")?.toIntOrNull() ?: Runtime.getRuntime().availableProcessors()
    val path = Paths.get(dbPath)
    require(Files.isRegularFile(path)) { "Database file not found: $dbPath" }

    val started = System.nanoTime()
    DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
        val report = compressLineContent(conn, chunkRows = chunkRows, threads = threads, logger = logger)
        logger.i { report.describe() }
    }
    logger.i { "Compressed $dbPath in ${(System.nanoTime() - started) / 1_000_000} ms" }
}

internal const val COMPRESS_CHUNK_ROWS: Int = 20_000

internal data class CompressLineContentReport(
    val lineRowsCompressed: Long,
    val versionRowsCompressed: Long,
    val textBytes: Long,
    val frameBytes: Long,
) {
    val alreadyCompressed: Boolean get() = lineRowsCompressed == 0L && versionRowsCompressed == 0L

    fun describe(): String =
        "line text compression ${if (alreadyCompressed) "already in place (validated)" else "done"}: " +
            "line_content=$lineRowsCompressed version_line=$versionRowsCompressed rows, " +
            "$textBytes → $frameBytes bytes"
}

/** See [main]. [dictionary] is the bundled one in production; tests pass their own. */
internal fun compressLineContent(
    conn: Connection,
    dictionary: ByteArray = LineContentCompression.bundledDictionary,
    chunkRows: Int = COMPRESS_CHUNK_ROWS,
    threads: Int = Runtime.getRuntime().availableProcessors(),
    logger: Logger = Logger.withTag("CompressLineContent"),
): CompressLineContentReport {
    require(chunkRows > 0) { "chunkRows=$chunkRows must be positive" }
    require(threads > 0) { "threads=$threads must be positive" }
    check(conn.autoCommit) { "compressLineContent requires an unowned JDBC connection" }
    check(LineContentShape.isSplit(conn)) { "compressLineContent needs the schema-6 line_content shape" }

    storeDictionary(conn, dictionary)

    val journalMode = conn.createStatement().use { st ->
        st.executeQuery("PRAGMA journal_mode").use { rs -> rs.next(); rs.getString(1) }
    }
    val switchJournal = journalMode.equals("wal", ignoreCase = true)
    if (switchJournal) conn.createStatement().use { it.execute("PRAGMA journal_mode=DELETE") }

    val workers = ConcurrentLinkedQueue<Pair<LineContentCompression.Compressor, LineContentCompression.Decompressor>>()
    val local = ThreadLocal.withInitial {
        (LineContentCompression.Compressor(dictionary) to LineContentCompression.Decompressor(dictionary))
            .also(workers::add)
    }
    val pool = Executors.newFixedThreadPool(threads)
    try {
        val encode = { rows: List<Pair<Long, ByteArray>> ->
            val slice = (rows.size + threads - 1) / threads
            rows.chunked(maxOf(slice, 1)).map { part ->
                pool.submit<List<ByteArray>> {
                    val (compressor, decompressor) = local.get()
                    part.map { (key, text) ->
                        check(text.size <= LineContentCompression.MAX_LINE_BYTES) {
                            "row $key is ${text.size} bytes, over the reader cap ${LineContentCompression.MAX_LINE_BYTES}"
                        }
                        val frame = compressor.compress(text)
                        check(decompressor.decompress(frame).contentEquals(text)) {
                            "zstd round trip failed for row $key"
                        }
                        frame
                    }
                }
            }.flatMap { it.get() }
        }
        val line = compressColumn(
            conn,
            selectSql = "SELECT id, CAST(content AS BLOB) FROM line_content " +
                "WHERE id > ? AND typeof(content) = 'text' ORDER BY id LIMIT $chunkRows",
            updateSql = "UPDATE line_content SET content = ? WHERE id = ?",
            encode = encode,
        )
        logger.i { "line_content: ${line.rows} rows compressed" }
        val version = compressColumn(
            conn,
            selectSql = "SELECT rowid, CAST(content AS BLOB) FROM version_line " +
                "WHERE rowid > ? AND typeof(content) = 'text' ORDER BY rowid LIMIT $chunkRows",
            updateSql = "UPDATE version_line SET content = ? WHERE rowid = ?",
            encode = encode,
        )
        logger.i { "version_line: ${version.rows} rows compressed" }
        validateCompressed(conn)
        return CompressLineContentReport(
            lineRowsCompressed = line.rows,
            versionRowsCompressed = version.rows,
            textBytes = line.textBytes + version.textBytes,
            frameBytes = line.frameBytes + version.frameBytes,
        )
    } finally {
        // A failed chunk leaves other tasks running; their contexts close only after they stop.
        pool.shutdownNow()
        pool.awaitTermination(1, TimeUnit.MINUTES)
        workers.forEach { (c, d) -> c.close(); d.close() }
        if (switchJournal) conn.createStatement().use { it.execute("PRAGMA journal_mode=$journalMode") }
    }
}

/** A DB holds exactly one dictionary; a different one already stored is a hard error. */
private fun storeDictionary(conn: Connection, dictionary: ByteArray) {
    val id = com.github.luben.zstd.Zstd.getDictIdFromDict(dictionary)
    check(id != 0L) { "the dictionary has no zstd dictionary id" }
    conn.createStatement().use {
        it.execute(
            "CREATE TABLE IF NOT EXISTS ${LineContentCompression.DICT_TABLE} " +
                "(id INTEGER PRIMARY KEY NOT NULL, dict BLOB NOT NULL)",
        )
    }
    val stored = LineContentCompression.storedDictionaries(conn)
    if (stored.isEmpty()) {
        conn.prepareStatement("INSERT INTO ${LineContentCompression.DICT_TABLE}(id, dict) VALUES (?, ?)").use { ps ->
            ps.setLong(1, id)
            ps.setBytes(2, dictionary)
            ps.executeUpdate()
        }
        return
    }
    check(stored.keys == setOf(id) && stored.getValue(id).contentEquals(dictionary)) {
        "${LineContentCompression.DICT_TABLE} already holds dictionaries ${stored.keys}, not $id"
    }
}

private class ColumnResult(val rows: Long, val textBytes: Long, val frameBytes: Long)

private fun compressColumn(
    conn: Connection,
    selectSql: String,
    updateSql: String,
    encode: (List<Pair<Long, ByteArray>>) -> List<ByteArray>,
): ColumnResult {
    var lastKey = Long.MIN_VALUE
    var rows = 0L
    var textBytes = 0L
    var frameBytes = 0L
    while (true) {
        val chunk = conn.prepareStatement(selectSql).use { ps ->
            ps.setLong(1, lastKey)
            ps.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getLong(1) to rs.getBytes(2)) } }
        }
        if (chunk.isEmpty()) break
        val frames = encode(chunk)
        conn.autoCommit = false
        try {
            conn.prepareStatement(updateSql).use { ps ->
                chunk.forEachIndexed { i, (key, _) ->
                    ps.setBytes(1, frames[i])
                    ps.setLong(2, key)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
            conn.commit()
        } catch (failure: Throwable) {
            runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
            throw failure
        } finally {
            conn.autoCommit = true
        }
        rows += chunk.size
        textBytes += chunk.sumOf { it.second.size.toLong() }
        frameBytes += frames.sumOf { it.size.toLong() }
        lastKey = chunk.last().first
    }
    return ColumnResult(rows, textBytes, frameBytes)
}

/** No line text is left as TEXT and there is one dictionary; throws on the first violation. */
internal fun validateCompressed(conn: Connection) {
    val dictionaries = LineContentCompression.storedDictionaries(conn)
    check(dictionaries.size == 1) { "expected one dictionary in ${LineContentCompression.DICT_TABLE}, found ${dictionaries.keys}" }
    fun count(sql: String): Long =
        conn.createStatement().use { st -> st.executeQuery(sql).use { rs -> rs.next(); rs.getLong(1) } }
    // A BLOB that is not a zstd frame (wrong magic) would pass a typeof check alone.
    val notFrame = "(typeof(content) <> 'blob' OR substr(content, 1, 4) <> X'28B52FFD')"
    val plainLines = count("SELECT COUNT(*) FROM line_content WHERE $notFrame")
    check(plainLines == 0L) { "$plainLines line_content rows are not zstd frames" }
    val plainVersions = count("SELECT COUNT(*) FROM version_line WHERE content IS NOT NULL AND $notFrame")
    check(plainVersions == 0L) { "$plainVersions version_line rows are not zstd frames" }
}
