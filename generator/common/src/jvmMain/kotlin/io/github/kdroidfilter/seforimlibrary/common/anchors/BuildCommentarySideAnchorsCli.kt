package io.github.kdroidfilter.seforimlibrary.common.anchors

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
 * Fills the commentary side of `link_anchor` (see [buildCommentarySideMarkerAnchors]) —
 * `:generator-common:buildCommentarySideAnchors`.
 *
 * Runs after every stage that writes lines, links or their base-side anchors, and before
 * `buildLineNoteIndex` moves footnote bodies out of the text the offsets count.
 *
 * Required system property: `dbPath`.
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("BuildCommentarySideAnchorsCli")

    val dbPath = System.getProperty("dbPath") ?: error("-PdbPath= missing")
    val path = Paths.get(dbPath)
    require(Files.isRegularFile(path)) { "Database file not found: $dbPath" }

    DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
        buildCommentarySideMarkerAnchors(conn, logger)
    }
}
