package io.github.kdroidfilter.seforimlibrary.packaging

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentShape
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.DriverManager
import kotlin.system.exitProcess

/**
 * Dumps a built `seforim.db` into a compact "lines snapshot" that the external
 * linker consumes (see `LINKER_IMPLEMENTATION_STAGES.md` stage 0 / stage 2).
 *
 * Why a snapshot instead of feeding the linker raw `merged.json`/`.txt`:
 * citation offsets are only valid against the *exact* cleaned line text the
 * build stored — the same bytes anchors later index into. The line text in a
 * built DB (`line.content`, or `line_content` from schema 6) is precisely that
 * post-cleaning text, so copying it verbatim removes
 * the silent-offset-skew failure mode entirely.
 *
 * The snapshot's `(source_name, canonical_he_title)` is the exact allocator
 * [io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey], derived
 * *identically* to the Phase-2 importer's source-side identity mapping:
 * `source.name` (via `book.sourceId`) + `COALESCE(book.heRef, book.title)`
 * (both importers store `heRef == canonicalHeTitle`). `line_index` is carried
 * verbatim so it maps 1:1 back to `line.lineIndex` at build time.
 * `context_ref` is the exact Sefaria location of the source line (falling back
 * to the canonical book title only for structural/non-Sefaria lines). The
 * linker may use it only for explicit relative citations such as לעיל/לקמן.
 *
 * Usage:
 *   ./gradlew :packaging:dumpLines -PseforimDb=/path/to/seforim.db
 *   ./gradlew :packaging:dumpLines -PseforimDb=/path/to/seforim.db -PlinesSnapshot=/out/lines_snapshot.db
 *   (optional) -PlinesSnapshotBookLimit=50   # smoke-test: dump only the first N books
 *
 * Output (default): `lines_snapshot.db` next to the source DB.
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("DumpLines")

    val srcDb = resolveSeforimDbPath(args)
    if (!Files.isRegularFile(srcDb)) {
        logger.e { "Source DB not found: $srcDb" }
        exitProcess(1)
    }
    val outPath = resolveSnapshotOutPath(srcDb)
    val bookLimit = System.getProperty("linesSnapshotBookLimit")?.toIntOrNull()

    logger.i { "Dumping lines: $srcDb -> $outPath" + (bookLimit?.let { " (first $it books)" } ?: "") }
    Files.deleteIfExists(outPath)
    Files.createDirectories(outPath.parent)

    Class.forName("org.sqlite.JDBC")
    DriverManager.getConnection("jdbc:sqlite:$srcDb").use { src ->
        // Invariant: (source_name, canonical_he_title) must uniquely identify a book. The
        // snapshot is keyed on it and the linker slices books by it; if two distinct books
        // shared a key their lines would merge into one phantom book. Fail loudly, never
        // silently corrupt (see the no-heuristics rule).
        src.createStatement().use { s ->
            s.executeQuery(
                """
                SELECT s.name, COALESCE(b.heRef, b.title) AS ct, COUNT(*) AS c
                FROM book b JOIN source s ON b.sourceId = s.id
                GROUP BY s.name, ct HAVING c > 1
                """.trimIndent(),
            ).use { rs ->
                val dups = ArrayList<String>()
                while (rs.next()) dups.add("(${rs.getString(1)}, ${rs.getString(2)}) x${rs.getInt(3)}")
                if (dups.isNotEmpty()) {
                    logger.e { "Duplicate book_key(s) in DB — snapshot would be corrupt: ${dups.take(20)}" }
                    exitProcess(2)
                }
            }
        }
        DriverManager.getConnection("jdbc:sqlite:$outPath").use { out ->
            out.createStatement().use { st ->
                st.execute("PRAGMA journal_mode=OFF")
                st.execute("PRAGMA synchronous=OFF")
                st.execute(
                    """
                    CREATE TABLE lines_snapshot (
                        source_name        TEXT    NOT NULL,
                        canonical_he_title TEXT    NOT NULL,
                        line_index         INTEGER NOT NULL,
                        content            TEXT    NOT NULL,
                        context_ref        TEXT    NOT NULL
                    )
                    """.trimIndent(),
                )
                st.execute("CREATE TABLE lines_snapshot_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            }

            // Optional book allow-list for smoke tests (deterministic: lowest ids first).
            val bookFilter = if (bookLimit != null) {
                val ids = ArrayList<Long>(bookLimit)
                src.createStatement().use { s ->
                    s.executeQuery("SELECT id FROM book ORDER BY id LIMIT $bookLimit").use { rs ->
                        while (rs.next()) ids.add(rs.getLong(1))
                    }
                }
                " WHERE l.bookId IN (${ids.joinToString(",")}) "
            } else {
                ""
            }

            val insert = out.prepareStatement(
                """
                INSERT INTO lines_snapshot(
                    source_name, canonical_he_title, line_index, content, context_ref
                ) VALUES(?,?,?,?,?)
                """.trimIndent(),
            )
            out.autoCommit = false

            // Stream forward-only; ordering by (book, lineIndex) keeps per-book lines contiguous
            // and in index order — the linker relies on this to slice books cheaply.
            val split = LineContentShape.isSplit(src)
            var books = 0L
            var lines = 0L
            var lastKey: Pair<String, String>? = null
            src.createStatement().use { s ->
                s.fetchSize = 10_000
                s.executeQuery(
                    """
                    SELECT s.name AS source_name,
                           COALESCE(b.heRef, b.title) AS canonical_he_title,
                           l.lineIndex AS line_index,
                           ${LineContentShape.contentExpr(split)} AS content,
                           COALESCE(NULLIF(TRIM(l.heRef), ''), COALESCE(b.heRef, b.title)) AS context_ref
                    FROM line l
                    JOIN book b   ON l.bookId = b.id
                    JOIN source s ON b.sourceId = s.id
                    ${LineContentShape.contentJoin(split)}
                    $bookFilter
                    ORDER BY b.id, l.lineIndex
                    """.trimIndent(),
                ).use { rs ->
                    while (rs.next()) {
                        val sourceName = rs.getString(1)
                        val title = rs.getString(2)
                        val lineIndex = rs.getLong(3)
                        val content = rs.getString(4) ?: ""
                        val contextRef = rs.getString(5) ?: title
                        insert.setString(1, sourceName)
                        insert.setString(2, title)
                        insert.setLong(3, lineIndex)
                        insert.setString(4, content)
                        insert.setString(5, contextRef)
                        insert.addBatch()
                        lines++
                        val key = sourceName to title
                        if (key != lastKey) {
                            books++
                            lastKey = key
                        }
                        if (lines % 100_000L == 0L) {
                            insert.executeBatch()
                            out.commit()
                            logger.i { "  …$lines lines / $books books" }
                        }
                    }
                }
            }
            insert.executeBatch()
            out.commit()
            insert.close()

            out.createStatement().use { st ->
                // Include line_index so the linker's `... WHERE source_name=? AND canonical_he_title=?
                // ORDER BY line_index` is served straight from the index — WITHOUT it SQLite adds a
                // "USE TEMP B-TREE FOR ORDER BY" that spills big books' content to a temp file on
                // disk, and N parallel workers turn that into brutal disk thrashing (learned running
                // the stage-6 bootstrap on a 16GB machine).
                st.execute("CREATE INDEX idx_ls_book ON lines_snapshot(source_name, canonical_he_title, line_index)")
            }
            out.prepareStatement("INSERT INTO lines_snapshot_meta(key, value) VALUES(?,?)").use { m ->
                fun put(k: String, v: String) { m.setString(1, k); m.setString(2, v); m.executeUpdate() }
                put("schema_version", "2")
                put("context_policy", "explicit-relative-v1")
                put("source_db", srcDb.fileName.toString())
                put("book_count", books.toString())
                put("line_count", lines.toString())
            }
            out.commit()
            logger.i { "Done: $books books, $lines lines -> $outPath (${Files.size(outPath) / 1_000_000}MB)" }
            println("Snapshot: $outPath ($books books, $lines lines)")
        }
    }
}

private fun resolveSeforimDbPath(args: Array<String>): Path {
    val dbPathStr = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    return Paths.get(dbPathStr).toAbsolutePath()
}

private fun resolveSnapshotOutPath(srcDb: Path): Path {
    val out = System.getProperty("linesSnapshot")
        ?: System.getenv("LINES_SNAPSHOT")
        ?: srcDb.resolveSibling("lines_snapshot.db").toString()
    return Paths.get(out).toAbsolutePath()
}
