package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager

/**
 * Writes `sqlite_stat1` into a finished `seforim.db` so the app's query planner
 * stops preferring low-selectivity indexes such as `idx_link_target_book`.
 *
 * Must run after the last stage that writes the DB. `sqlite_stat1` is outside
 * every hash and patch table list, so it never reaches a logical hash or a patch.
 *
 * Required system property: `dbPath`.
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("AnalyzeDbCli")

    val dbPath = System.getProperty("dbPath") ?: error("-PdbPath= missing")
    val path = Paths.get(dbPath)
    require(Files.isRegularFile(path)) { "Database file not found: $dbPath" }

    val started = System.nanoTime()
    DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
        analyzeForQueryPlanner(conn)
    }
    logger.i { "Analyzed $dbPath in ${(System.nanoTime() - started) / 1_000_000} ms" }
}

/** Rebuilds the planner statistics of every table and index in [conn]. */
internal fun analyzeForQueryPlanner(conn: Connection) {
    check(conn.autoCommit) { "analyzeForQueryPlanner requires an unowned JDBC connection" }
    conn.createStatement().use { it.execute("ANALYZE") }
}
