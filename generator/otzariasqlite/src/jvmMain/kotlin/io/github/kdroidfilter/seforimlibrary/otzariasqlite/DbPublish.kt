package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import co.touchlab.kermit.Logger
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.sql.DriverManager

/**
 * How the Otzaria phases put a freshly built database in place of the previous
 * one.
 *
 * Both phases run against `:memory:` and finish with `VACUUM INTO`, whose target
 * must not exist — so the code used to **delete the target first** and vacuum
 * into the hole. In the release path `baseDb == persistDb`
 * (build.gradle.kts:appendOtzariaLines / appendOtzariaLinks), i.e. the file it
 * deletes is the 7 GiB DB the run was seeded from, and anything that stops the
 * process in between (OOM, a full disk, a cancelled job) leaves **no database at
 * all** and nothing to retry from.
 *
 * [publishAtomically] closes that window: vacuum into `<target>.candidate` in the
 * same directory, check it, then rename it over the target in one step. Either
 * the previous file or the new one is there at every instant.
 */
internal object DbPublish {

    /** Suffix of the file a publish writes before it renames it over the target. */
    const val CANDIDATE_SUFFIX = ".candidate"

    /**
     * Runs [writeInto] against `<target>$CANDIDATE_SUFFIX`, verifies what it
     * produced, and ATOMIC_MOVEs it over [target]. On any failure the candidate
     * is removed and the previous [target] is left exactly as it was; the
     * throwable propagates so the caller fails the build.
     *
     * The candidate deliberately lives in the target's own directory: an
     * ATOMIC_MOVE across filesystems is refused, and the copy it would fall back
     * to is the very window this removes.
     *
     * **Space.** Holding the old file while the new one is written costs a
     * second full copy, and the release runner builds on a 16 GiB tmpfs that
     * already carries the ~7 GiB DB (manual-generate-release.yml, "RAM-backed
     * build dir"). Turning a working build into an ENOSPC would be a worse bug
     * than the one this fixes, so when there is not room for a second copy the
     * publish falls back to the previous in-place behaviour and says so in the
     * log. On that runner the fallback costs nothing real: build/ is a tmpfs
     * that is unmounted whatever the job's outcome, so a DB lost mid-write is a
     * DB that would have been discarded anyway. On a developer's disk — where
     * the file IS the only copy — the atomic path is the one that runs.
     */
    suspend fun publishAtomically(target: Path, logger: Logger, writeInto: suspend (Path) -> Unit): Path {
        val absolute = target.toAbsolutePath()
        absolute.parent?.let { Files.createDirectories(it) }
        val candidate = absolute.resolveSibling(absolute.fileName.toString() + CANDIDATE_SUFFIX)
        // A candidate left by a killed previous run is stale by definition.
        Files.deleteIfExists(candidate)
        deleteSqliteSidecars(candidate, logger)

        tooTightForACandidate(absolute)?.let { reason ->
            logger.w {
                "Publishing ${absolute.fileName} in place ($reason). " +
                    "A crash during the write therefore leaves no database at $absolute."
            }
            deleteSqliteSidecars(absolute, logger)
            Files.deleteIfExists(absolute)
            writeInto(absolute)
            verifyPublishable(absolute)
            logger.i { "Published ${absolute.fileName} in place (${Files.size(absolute)} bytes)" }
            return absolute
        }

        try {
            writeInto(candidate)
            verifyPublishable(candidate)
            // The previous file's journal/WAL belong to the previous file. Removed
            // BEFORE the rename: left beside the new DB, SQLite would replay them
            // into it on the next open (it trusts a `-wal` it finds next to a DB,
            // whatever the DB header says), and the phase after this one opens the
            // published file straight away. Removed after the rename, a crash in
            // between leaves exactly that pair on disk.
            deleteSqliteSidecars(absolute, logger)
            moveOver(candidate, absolute)
        } catch (error: Throwable) {
            runCatching { Files.deleteIfExists(candidate) }
            throw error
        }
        logger.i { "Published ${absolute.fileName} atomically (${Files.size(absolute)} bytes)" }
        return absolute
    }

    private const val MIB = 1024L * 1024L

    /**
     * Why a second copy of [target] does not fit beside it, or null when it does.
     *
     * Sized off the file being replaced (the new DB is within a few percent of
     * it), plus 12.5% and 64 MiB of slack — 875 MiB of headroom at the 7 GiB
     * this actually guards, and a negligible floor for a small DB. With no previous file there is
     * nothing to keep alive, so the candidate costs nothing extra and always
     * runs — which is also what makes the first build of a fresh checkout
     * atomic. An unreadable filestore is treated as "room": a candidate that
     * runs out of space fails the build with the previous DB intact, which is
     * the safer way to be wrong.
     */
    private fun tooTightForACandidate(target: Path): String? {
        val existing = if (Files.isRegularFile(target)) Files.size(target) else 0L
        if (existing == 0L) return null
        val needed = existing + existing / 8 + 64 * MIB
        val usable = runCatching { Files.getFileStore(target.parent).usableSpace }.getOrNull() ?: return null
        if (usable >= needed) return null
        return "${usable / MIB} MiB free on its filesystem, ~${needed / MIB} MiB needed to keep the current " +
            "${existing / MIB} MiB file while the new one is written"
    }

    /**
     * The files SQLite keeps beside a database: rollback journal, WAL and its
     * shared-memory index. Every Otzaria stage that writes the on-disk file
     * directly opens it in WAL mode (SeforimRepository.init, GenerateHavroutaLinks),
     * so a stage killed mid-run leaves `<db>-wal`/`-shm` behind; a fresh DB
     * renamed under them inherits somebody else's frames.
     */
    internal val SQLITE_SIDECAR_SUFFIXES = listOf("-journal", "-wal", "-shm")

    private fun deleteSqliteSidecars(db: Path, logger: Logger) {
        for (suffix in SQLITE_SIDECAR_SUFFIXES) {
            val sidecar = db.resolveSibling(db.fileName.toString() + suffix)
            if (Files.deleteIfExists(sidecar)) {
                logger.w { "Removed stale ${sidecar.fileName} left beside ${db.fileName} by an earlier run" }
            }
        }
    }

    private fun moveOver(candidate: Path, target: Path) {
        try {
            Files.move(candidate, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        } catch (_: AtomicMoveNotSupportedException) {
            // Same directory, so this should not happen; if the filesystem
            // refuses it anyway, a plain replace still beats delete-then-write.
            Files.move(candidate, target, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    /**
     * The candidate exists, is not empty, and opens as a SQLite database that
     * carries a schema.
     *
     * Deliberately NOT `PRAGMA integrity_check`: that is a full read of a ~7 GiB
     * file on the release runner, and it is not what makes this safe — the
     * atomic rename is. A torn candidate can only come from a process that died
     * mid-vacuum, and such a process never reaches the rename.
     */
    private fun verifyPublishable(candidate: Path) {
        check(Files.isRegularFile(candidate)) { "VACUUM INTO left no file at $candidate" }
        val size = Files.size(candidate)
        check(size > 0) { "VACUUM INTO produced an empty file at $candidate" }
        runCatching { Class.forName("org.sqlite.JDBC") }
        val objects = DriverManager.getConnection("jdbc:sqlite:$candidate").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM sqlite_master").use { rs ->
                    rs.next()
                    rs.getLong(1)
                }
            }
        }
        check(objects > 0) { "VACUUM INTO produced a database with no schema at $candidate ($size bytes)" }
    }

    /**
     * `-PallowEmptyBase=true` / `ALLOW_EMPTY_BASE=true`: the ONLY way to run an
     * append phase without the DB it appends to. Absence of the file is not a
     * mode — a phase that appends to a base and cannot find one has lost its
     * input, and continuing publishes an empty DB as a success.
     */
    fun allowEmptyBase(): Boolean = listOf(
        System.getProperty("allowEmptyBase"),
        System.getenv("ALLOW_EMPTY_BASE"),
    ).firstOrNull { !it.isNullOrBlank() }
        ?.let { it.equals("true", ignoreCase = true) || it == "1" }
        ?: false

    /** The message every "the base is missing" abort shares. */
    fun missingBaseMessage(phase: String, baseDb: String, selector: String): String =
        "$phase appends to an existing DB and refuses to create one, but no base DB exists at $baseDb. " +
            "Point $selector at the database the previous phase produced, " +
            "or pass -PallowEmptyBase=true (ALLOW_EMPTY_BASE=true) to accept an empty one."
}
