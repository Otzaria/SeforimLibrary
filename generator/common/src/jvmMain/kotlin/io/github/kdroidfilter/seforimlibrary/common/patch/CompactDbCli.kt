package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.sql.Connection
import java.sql.DriverManager

/**
 * Rewrites a finished `seforim.db` without its freelist, so the file users install
 * is as small as its content. `splitLineContent` leaves the pages it freed on the
 * freelist (zeroed): the `.zst` shrinks, the installed file does not.
 *
 * `VACUUM INTO` writes the compact copy on [scratchDir], a volume other than the
 * DB's; the original is then deleted and the copy moved into its place. The DB's
 * own volume therefore never holds more than one copy — the release build keeps
 * `build/` on a 20 GiB tmpfs, while `scratchDir` needs about one compacted DB of
 * free space (checked up front). A plain `VACUUM` would need the DB plus a full
 * rollback journal on the tmpfs.
 *
 * Must run after the last stage that writes the DB (Phase-2 and its ANALYZE);
 * `sqlite_stat1` is a table and is carried over. No-op when the freelist is empty.
 *
 * System properties: `dbPath` (required), `scratchDir` (default: the DB's directory).
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("CompactDbCli")

    val dbPath = System.getProperty("dbPath") ?: error("-PdbPath= missing")
    val path = Paths.get(dbPath).toAbsolutePath()
    require(Files.isRegularFile(path)) { "Database file not found: $dbPath" }
    val scratchDir = System.getProperty("scratchDir")?.takeIf { it.isNotBlank() }
        ?.let { Paths.get(it).toAbsolutePath() }
        ?: path.parent

    val started = System.nanoTime()
    val report = compactDatabase(path, scratchDir, logger)
    logger.i { "${report.describe()} in ${(System.nanoTime() - started) / 1_000_000} ms" }
}

internal data class CompactReport(
    val compacted: Boolean,
    val bytesBefore: Long,
    val bytesAfter: Long,
    val freelistPagesBefore: Long,
) {
    fun describe(): String =
        if (!compacted) "freelist empty, $bytesBefore bytes left as is"
        else "compacted $bytesBefore → $bytesAfter bytes ($freelistPagesBefore freelist pages dropped)"
}

/** See [main]. Throws, leaving the original DB untouched, if the compact copy fails its checks. */
internal fun compactDatabase(
    db: Path,
    scratchDir: Path,
    logger: Logger = Logger.withTag("CompactDb"),
): CompactReport {
    val target = db.toAbsolutePath()
    val bytesBefore = Files.size(target)
    val (pageSize, pageCount, freelist) = connect(target).use { conn ->
        Triple(pragmaLong(conn, "page_size"), pragmaLong(conn, "page_count"), pragmaLong(conn, "freelist_count"))
    }
    if (freelist == 0L) return CompactReport(false, bytesBefore, bytesBefore, 0)

    Files.createDirectories(scratchDir)
    val scratch = scratchDir.toAbsolutePath().resolve("${target.fileName}.compact")
    Files.deleteIfExists(scratch)
    // VACUUM INTO keeps the page size, so the copy is the used pages plus a small margin.
    val needed = (pageCount - freelist) * pageSize + pageSize * 64
    val usable = Files.getFileStore(scratchDir).usableSpace
    check(usable >= needed) {
        "compactDatabase: $scratchDir has $usable bytes free, the compact copy of $target needs ~$needed"
    }

    try {
        val tableRows = connect(target).use { conn ->
            val counts = rowCounts(conn)
            conn.prepareStatement("VACUUM INTO ?").use { ps ->
                ps.setString(1, scratch.toString())
                ps.execute()
            }
            counts
        }
        connect(scratch).use { conn ->
            val check = conn.createStatement().use { st ->
                st.executeQuery("PRAGMA quick_check").use { rs -> rs.next(); rs.getString(1) }
            }
            check(check == "ok") { "compact copy $scratch failed quick_check: $check" }
            check(pragmaLong(conn, "freelist_count") == 0L) { "compact copy $scratch still has a freelist" }
            val copied = rowCounts(conn)
            check(copied == tableRows) { "compact copy $scratch lost rows: $tableRows → $copied" }
        }
        logger.i { "VACUUM INTO $scratch: ${Files.size(scratch)} bytes (source $bytesBefore bytes)" }

        // Delete first: a move across volumes is a copy, and the DB's volume must
        // never hold both files. The checked copy stays on scratchDir until moved.
        for (suffix in listOf("", "-journal", "-wal", "-shm")) {
            Files.deleteIfExists(target.resolveSibling("${target.fileName}$suffix"))
        }
        Files.move(scratch, target, StandardCopyOption.REPLACE_EXISTING)
    } catch (failure: Throwable) {
        if (Files.exists(target)) Files.deleteIfExists(scratch)
        else logger.e { "compactDatabase: $target was removed; its checked compact copy is kept at $scratch" }
        throw failure
    }
    return CompactReport(true, bytesBefore, Files.size(target), freelist)
}

private fun connect(db: Path): Connection = DriverManager.getConnection("jdbc:sqlite:$db")

private fun rowCounts(conn: Connection): Map<String, Long> {
    val tables = conn.createStatement().use { st ->
        st.executeQuery("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").use { rs ->
            buildList { while (rs.next()) add(rs.getString(1)) }
        }
    }
    return tables.associateWith { table ->
        conn.createStatement().use { st ->
            st.executeQuery("SELECT COUNT(*) FROM \"$table\"").use { rs -> rs.next(); rs.getLong(1) }
        }
    }
}

private fun pragmaLong(conn: Connection, pragma: String): Long =
    conn.createStatement().use { st -> st.executeQuery("PRAGMA $pragma").use { rs -> rs.next(); rs.getLong(1) } }
