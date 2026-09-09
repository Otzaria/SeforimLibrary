package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import co.touchlab.kermit.Logger
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.sql.DriverManager
import java.util.Properties
import java.util.UUID

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
     * is removed and the previous [target]'s committed logical contents are
     * retained; sealing may checkpoint a WAL and switch its journal mode before
     * the eventual move. The throwable propagates so the caller fails the
     * build.
     *
     * The candidate deliberately lives in the target's own directory: an
     * ATOMIC_MOVE across filesystems is refused, and the copy it would fall back
     * to is the very window this removes.
     *
     * **Space.** Holding the old file while the new one is written costs a
     * second full copy. That is a capacity requirement, not permission to
     * delete a published database: when the preflight says there is not enough
     * room, this operation fails *before* it touches the target. A caller can
     * free space and retry from the intact previous DB; it never gets an
     * in-place write that can turn ENOSPC or a cancellation into data loss.
     */
    suspend fun publishAtomically(
        target: Path,
        logger: Logger,
        availableSpace: (Path) -> Long? = ::usableSpace,
        writeInto: suspend (Path) -> Unit,
    ): Path {
        val absolute = target.toAbsolutePath()
        val candidate = prepareDatabaseCandidate(absolute, logger, availableSpace)

        try {
            writeInto(candidate)
            sealSqliteForPublish(candidate, logger)
            verifyPublishable(candidate)
            if (Files.exists(absolute)) sealSqliteForPublish(absolute, logger)
            moveOver(candidate, absolute)
        } catch (error: Throwable) {
            runCatching { discardCandidate(absolute, logger) }
            throw error
        }
        logger.i { "Published ${absolute.fileName} atomically (${Files.size(absolute)} bytes)" }
        return absolute
    }

    /** Returns the sibling candidate path used for a future publish. */
    fun candidatePath(target: Path): Path {
        val absolute = target.toAbsolutePath()
        return absolute.resolveSibling(absolute.fileName.toString() + CANDIDATE_SUFFIX)
    }

    /**
     * Makes an empty database candidate path available without touching
     * [target]. This is used by the non-memory generators, which must build in
     * a disk candidate before their DB/build-state pair can be committed.
     *
     * [availableSpace] is injectable so the fail-closed low-space path can be
     * tested without filling a filesystem.
     */
    fun prepareDatabaseCandidate(
        target: Path,
        logger: Logger,
        availableSpace: (Path) -> Long? = ::usableSpace,
    ): Path {
        val absolute = target.toAbsolutePath()
        absolute.parent?.let { Files.createDirectories(it) }
        val candidate = candidatePath(absolute)
        discardCandidate(absolute, logger)
        requireCandidateSpace(absolute, availableSpace)
        return candidate
    }

    /** Prepares a non-DB file candidate (the allocator snapshot). */
    fun prepareFileCandidate(target: Path, logger: Logger): Path {
        val absolute = target.toAbsolutePath()
        absolute.parent?.let { Files.createDirectories(it) }
        val candidate = candidatePath(absolute)
        discardCandidate(absolute, logger)
        return candidate
    }

    /** Copies an existing SQLite DB into an already-prepared candidate. */
    fun copyDatabaseIntoCandidate(source: Path, candidate: Path) {
        val base = source.toAbsolutePath()
        check(Files.isRegularFile(base)) { "Cannot copy missing SQLite base DB at $base" }
        check(Files.notExists(candidate)) { "SQLite candidate already exists at $candidate" }
        runCatching { Class.forName("org.sqlite.JDBC") }
        val escaped = candidate.toString().replace("'", "''")
        DriverManager.getConnection("jdbc:sqlite:$base").use { connection ->
            connection.createStatement().use { it.execute("VACUUM INTO '$escaped'") }
        }
        verifyPublishable(candidate)
    }

    /** Deletes an uncommitted candidate and the SQLite files it may have left. */
    fun discardCandidate(target: Path, logger: Logger) {
        val candidate = candidatePath(target)
        Files.deleteIfExists(candidate)
        Files.deleteIfExists(candidate.resolveSibling(candidate.fileName.toString() + ".tmp"))
        deleteCandidateSidecars(candidate, logger)
    }

    /**
     * Builds and publishes a DB plus its allocator snapshot as one recoverable
     * unit. Filesystems cannot atomically rename two independent paths, so the
     * commit has a small durable journal. Before that journal is written, every
     * SQLite member is sealed: committed WAL frames are checkpointed into its
     * main file and its journal mode becomes DELETE. This may normalize bytes,
     * but preserves the same committed logical contents and allocator identity.
     * Once the journal exists, an interrupted commit is rolled back to the
     * previous pair before either generator loads its allocator on the next
     * invocation.
     *
     * A crash after both replacement moves but before the journal is marked
     * committed is conservatively rolled back. That may repeat work, but it
     * never makes an advanced DB visible beside an old allocator state, which
     * is the condition that can re-issue IDs.
     */
    suspend fun publishPairAtomically(
        databaseTarget: Path,
        buildStateTarget: Path,
        logger: Logger,
        availableSpace: (Path) -> Long? = ::usableSpace,
        writeDatabaseCandidate: suspend (Path) -> Unit,
        writeBuildStateCandidate: suspend (Path) -> Unit,
        verifyCandidates: (Path, Path) -> Unit,
        beforePublish: () -> Unit = {},
        afterTransition: (PairPublishTransition) -> Unit = {},
    ) {
        val database = databaseTarget.toAbsolutePath()
        val state = buildStateTarget.toAbsolutePath()
        recoverInterruptedPair(database, state, logger)
        val databaseCandidate = prepareDatabaseCandidate(database, logger, availableSpace)
        val stateCandidate = prepareFileCandidate(state, logger)
        try {
            writeDatabaseCandidate(databaseCandidate)
            verifyPublishable(databaseCandidate)
            writeBuildStateCandidate(stateCandidate)
            verifyStateCandidate(stateCandidate)
            verifyCandidates(databaseCandidate, stateCandidate)
            beforePublish()
            publishPreparedPair(database, state, logger, verifyCandidates, afterTransition)
        } catch (error: Throwable) {
            // A surviving journal means commitPair could not prove rollback.
            // Its candidates/backups are recovery evidence; do not erase them.
            if (!hasPairJournal(database)) {
                runCatching { discardCandidate(database, logger) }
                runCatching { discardCandidate(state, logger) }
            }
            throw error
        }
    }

    /**
     * Commits candidates prepared by a disk-backed generator. Callers must
     * first use [prepareDatabaseCandidate] and [prepareFileCandidate], then
     * write and validate both candidates before calling this method.
     */
    fun publishPreparedPair(
        databaseTarget: Path,
        buildStateTarget: Path,
        logger: Logger,
        verifyCandidates: (Path, Path) -> Unit,
        afterTransition: (PairPublishTransition) -> Unit = {},
    ) {
        val database = databaseTarget.toAbsolutePath()
        val state = buildStateTarget.toAbsolutePath()
        require(database != state) { "Database and build_state targets must be different files: $database" }
        recoverInterruptedPair(database, state, logger)
        val databaseCandidate = candidatePath(database)
        val stateCandidate = candidatePath(state)
        try {
            // Never move a main file away from an uncheckpointed WAL. Sealing
            // checkpoints it, switches back to DELETE mode, closes that
            // connection, and proves no SQLite sidecar remains; it never
            // deletes a WAL as a substitute for checkpointing it.
            sealSqliteForPublish(databaseCandidate, logger)
            sealSqliteForPublish(stateCandidate, logger)
            if (Files.exists(database)) sealSqliteForPublish(database, logger)
            if (Files.exists(state)) sealSqliteForPublish(state, logger)
            verifyPublishable(databaseCandidate)
            verifyStateCandidate(stateCandidate)
            verifyCandidates(databaseCandidate, stateCandidate)
            commitPair(database, state, logger, afterTransition)
        } catch (error: Throwable) {
            // Before the journal exists this only clears uncommitted candidates;
            // after it exists commitPair has already rolled the targets back (or
            // left the journal for recovery if rollback itself failed).
            if (!hasPairJournal(database)) {
                runCatching { discardCandidate(database, logger) }
                runCatching { discardCandidate(state, logger) }
            }
            throw error
        }
    }

    /**
     * Restores a pair interrupted after its journal was made durable. This is
     * deliberately public within the module so the entry points can run it
     * *before* loading build_state and handing out any IDs.
     */
    fun recoverInterruptedPair(databaseTarget: Path, buildStateTarget: Path, logger: Logger) {
        val database = databaseTarget.toAbsolutePath()
        val state = buildStateTarget.toAbsolutePath()
        val journalPath = pairJournalPath(database)
        if (Files.notExists(journalPath)) return
        val journal = readPairJournal(journalPath)
        check(journal.database == database && journal.state == state) {
            "Refusing to recover pair journal $journalPath for ${journal.database} / ${journal.state} " +
                "while this run requested $database / $state"
        }
        if (journal.committed) {
            cleanupPairArtifacts(journal, logger)
            Files.deleteIfExists(journalPath)
            logger.w { "Completed cleanup of an already-committed DB/build_state pair from $journalPath" }
            return
        }

        rollbackPair(journal, logger)
        Files.deleteIfExists(journalPath)
        logger.w { "Recovered an interrupted DB/build_state publish; restored the previous pair" }
    }

    /** Whether a failed caller must leave pair artifacts for recovery intact. */
    fun hasPendingPairRecovery(databaseTarget: Path): Boolean =
        hasPairJournal(databaseTarget.toAbsolutePath())

    private fun commitPair(
        database: Path,
        state: Path,
        logger: Logger,
        afterTransition: (PairPublishTransition) -> Unit,
    ) {
        val id = UUID.randomUUID().toString()
        val journal = PairJournal(
            database = database,
            state = state,
            databaseBackup = backupPath(database, id),
            stateBackup = backupPath(state, id),
            databaseExisted = Files.exists(database),
            stateExisted = Files.exists(state),
            committed = false,
        )
        val journalPath = pairJournalPath(database)
        writePairJournal(journalPath, journal)
        try {
            moveExistingToBackup(database, journal.databaseBackup, journal.databaseExisted)
            afterTransition(PairPublishTransition.DATABASE_BACKED_UP)
            moveExistingToBackup(state, journal.stateBackup, journal.stateExisted)
            afterTransition(PairPublishTransition.STATE_BACKED_UP)
            moveOver(candidatePath(database), database)
            afterTransition(PairPublishTransition.DATABASE_INSTALLED)
            moveOver(candidatePath(state), state)
            afterTransition(PairPublishTransition.STATE_INSTALLED)
            writePairJournal(journalPath, journal.copy(committed = true))
            afterTransition(PairPublishTransition.COMMITTED_MARKED)
        } catch (error: Throwable) {
            // Tests model process death at a precise boundary. A real process
            // death cannot run this catch either; leave the durable journal and
            // let the next invocation prove recovery from the same on-disk
            // state.
            if (error is SimulatedProcessDeathForTest) throw error
            runCatching { rollbackPair(journal, logger) }
                .onFailure { error.addSuppressed(it) }
            throw error
        }

        try {
            cleanupPairArtifacts(journal, logger)
            Files.deleteIfExists(journalPath)
        } catch (error: Throwable) {
            // The committed journal is intentionally retained. Recovery keeps
            // the new pair and retries only cleanup; it never rolls it back.
            throw error
        }
        logger.i { "Published ${database.fileName} and ${state.fileName} as one recoverable pair" }
    }

    private fun rollbackPair(journal: PairJournal, logger: Logger) {
        restorePrevious(journal.database, journal.databaseBackup, journal.databaseExisted, logger)
        restorePrevious(journal.state, journal.stateBackup, journal.stateExisted, logger)
        discardCandidate(journal.database, logger)
        discardCandidate(journal.state, logger)
    }

    private fun restorePrevious(target: Path, backup: Path, existed: Boolean, logger: Logger) {
        if (Files.exists(backup)) {
            deleteSealedMember(target)
            moveOver(backup, target)
            return
        }
        if (existed) {
            check(Files.exists(target)) {
                "Interrupted publish lost both $target and its backup $backup; refusing to continue"
            }
            // The journal was written but this member was not moved yet, so
            // target is still the old member of the pair.
            return
        }
        // The old pair had no member here. If the candidate was installed
        // before a crash, remove it so retry starts from the same empty slot.
        deleteSealedMember(target)
    }

    private fun moveExistingToBackup(target: Path, backup: Path, existed: Boolean) {
        if (!existed) return
        check(Files.exists(target)) { "Expected published pair member $target before replacement" }
        deleteSealedMember(backup)
        requireSealedSqlite(target)
        moveOver(target, backup)
    }

    private fun cleanupPairArtifacts(journal: PairJournal, logger: Logger) {
        deleteSealedMember(journal.databaseBackup)
        deleteSealedMember(journal.stateBackup)
        discardCandidate(journal.database, logger)
        discardCandidate(journal.state, logger)
    }

    private fun verifyStateCandidate(candidate: Path) {
        check(Files.isRegularFile(candidate)) { "build_state snapshot left no file at $candidate" }
        check(Files.size(candidate) > 0) { "build_state snapshot produced an empty file at $candidate" }
    }

    private fun deleteSealedMember(target: Path) {
        requireSealedSqlite(target)
        Files.deleteIfExists(target)
    }

    private data class PairJournal(
        val database: Path,
        val state: Path,
        val databaseBackup: Path,
        val stateBackup: Path,
        val databaseExisted: Boolean,
        val stateExisted: Boolean,
        val committed: Boolean,
    )

    /** Every durable boundary of a two-file commit, exposed for crash tests. */
    internal enum class PairPublishTransition {
        DATABASE_BACKED_UP,
        STATE_BACKED_UP,
        DATABASE_INSTALLED,
        STATE_INSTALLED,
        COMMITTED_MARKED,
    }

    /** Test-only signal: model a process that dies before the catch can run. */
    internal class SimulatedProcessDeathForTest : RuntimeException()

    private fun pairJournalPath(database: Path): Path =
        database.resolveSibling(database.fileName.toString() + ".pair-publish")

    private fun hasPairJournal(database: Path): Boolean = Files.exists(pairJournalPath(database))

    private fun backupPath(target: Path, id: String): Path =
        target.resolveSibling(target.fileName.toString() + ".pair-backup-$id")

    private fun writePairJournal(path: Path, journal: PairJournal) {
        path.parent?.let { Files.createDirectories(it) }
        val properties = Properties().apply {
            setProperty("database", journal.database.toString())
            setProperty("state", journal.state.toString())
            setProperty("databaseBackup", journal.databaseBackup.toString())
            setProperty("stateBackup", journal.stateBackup.toString())
            setProperty("databaseExisted", journal.databaseExisted.toString())
            setProperty("stateExisted", journal.stateExisted.toString())
            setProperty("committed", journal.committed.toString())
        }
        val temporary = path.resolveSibling(path.fileName.toString() + ".tmp")
        Files.newOutputStream(temporary).use { properties.store(it, "DB/build_state pair publish") }
        FileChannel.open(temporary, StandardOpenOption.WRITE).use { it.force(true) }
        moveOver(temporary, path)
        forceDirectory(path.parent)
    }

    private fun readPairJournal(path: Path): PairJournal {
        val properties = Properties()
        Files.newInputStream(path).use { properties.load(it) }
        fun required(name: String): String = checkNotNull(properties.getProperty(name)) {
            "Malformed DB/build_state pair journal $path: missing $name"
        }
        fun boolean(name: String): Boolean = required(name).toBooleanStrictOrNull()
            ?: error("Malformed DB/build_state pair journal $path: $name is not boolean")
        return PairJournal(
            database = Path.of(required("database")).toAbsolutePath(),
            state = Path.of(required("state")).toAbsolutePath(),
            databaseBackup = Path.of(required("databaseBackup")).toAbsolutePath(),
            stateBackup = Path.of(required("stateBackup")).toAbsolutePath(),
            databaseExisted = boolean("databaseExisted"),
            stateExisted = boolean("stateExisted"),
            committed = boolean("committed"),
        )
    }

    private fun forceDirectory(directory: Path?) {
        if (directory == null) return
        FileChannel.open(directory, StandardOpenOption.READ).use { it.force(true) }
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
    private fun requireCandidateSpace(target: Path, availableSpace: (Path) -> Long?) {
        tooTightForACandidate(target, availableSpace)?.let { reason ->
            throw InsufficientCandidateSpaceException(
                "Refusing to replace $target: $reason. Free space and retry; the existing DB was left untouched.",
            )
        }
    }

    private fun tooTightForACandidate(target: Path, availableSpace: (Path) -> Long?): String? {
        val existing = if (Files.isRegularFile(target)) Files.size(target) else 0L
        if (existing == 0L) return null
        val needed = existing + existing / 8 + 64 * MIB
        val usable = availableSpace(target.parent) ?: return null
        if (usable >= needed) return null
        return "${usable / MIB} MiB free on its filesystem, ~${needed / MIB} MiB needed to keep the current " +
            "${existing / MIB} MiB file while the new one is written"
    }

    private fun usableSpace(directory: Path): Long? =
        runCatching { Files.getFileStore(directory).usableSpace }.getOrNull()

    internal class InsufficientCandidateSpaceException(message: String) : IllegalStateException(message)

    /** The files that must not accompany a sealed SQLite member. */
    internal val SQLITE_SIDECAR_SUFFIXES = listOf("-journal", "-wal", "-shm")

    /**
     * Seals one SQLite file before it can enter the pair journal protocol.
     *
     * `-wal` is data, not litter. We first checkpoint it and require the
     * checkpoint to report no busy or remaining frames, then switch the DB to
     * rollback-journal DELETE mode. Only SQLite itself is allowed to remove the
     * WAL/SHM files. A sidecar that remains afterwards is an abort, never a
     * deletion: moving only the main file would make a crash lose or replay
     * frames under the wrong database name.
     */
    fun sealSqliteForPublish(database: Path, logger: Logger = Logger.withTag("DbPublish")) {
        val absolute = database.toAbsolutePath()
        check(Files.isRegularFile(absolute)) { "Cannot seal missing SQLite file $absolute" }
        runCatching { Class.forName("org.sqlite.JDBC") }
        try {
            DriverManager.getConnection("jdbc:sqlite:$absolute").use { connection ->
                connection.autoCommit = true
                connection.createStatement().use { statement ->
                    statement.executeQuery("PRAGMA wal_checkpoint(TRUNCATE)").use { result ->
                        check(result.next()) { "SQLite returned no wal_checkpoint result for $absolute" }
                        val busy = result.getLong(1)
                        val frames = result.getLong(2)
                        val checkpointed = result.getLong(3)
                        check(busy == 0L && (frames < 0L || frames == checkpointed)) {
                            "Cannot seal $absolute: WAL checkpoint is busy=$busy, frames=$frames, " +
                                "checkpointed=$checkpointed. Close every SQLite connection and retry."
                        }
                    }
                    statement.executeQuery("PRAGMA journal_mode=DELETE").use { result ->
                        check(result.next() && result.getString(1).equals("delete", ignoreCase = true)) {
                            "Cannot seal $absolute: SQLite refused PRAGMA journal_mode=DELETE"
                        }
                    }
                }
            }
        } catch (error: Throwable) {
            throw IllegalStateException("Cannot seal SQLite file $absolute for pair publication", error)
        }
        requireSealedSqlite(absolute)
        FileChannel.open(absolute, StandardOpenOption.WRITE).use { it.force(true) }
        forceDirectory(absolute.parent)
        logger.i { "Sealed ${absolute.fileName} as a single SQLite file for pair publication" }
    }

    private fun requireSealedSqlite(database: Path) {
        val sidecars = SQLITE_SIDECAR_SUFFIXES.map { suffix ->
            database.resolveSibling(database.fileName.toString() + suffix)
        }.filter(Files::exists)
        check(sidecars.isEmpty()) {
            "Refusing to move or delete $database while SQLite sidecars remain: " +
                "${sidecars.joinToString()}; checkpoint and close SQLite first"
        }
    }

    /** Candidate files are disposable, so their sidecars may be removed with them. */
    private fun deleteCandidateSidecars(db: Path, logger: Logger) {
        for (suffix in SQLITE_SIDECAR_SUFFIXES) {
            val sidecar = db.resolveSibling(db.fileName.toString() + suffix)
            if (Files.deleteIfExists(sidecar)) {
                logger.w { "Discarded ${sidecar.fileName} with uncommitted candidate ${db.fileName}" }
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
        // The pair journal is the recovery authority, but every member rename
        // must also be durable before the next injected/process-crash boundary.
        forceDirectory(target.parent)
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
