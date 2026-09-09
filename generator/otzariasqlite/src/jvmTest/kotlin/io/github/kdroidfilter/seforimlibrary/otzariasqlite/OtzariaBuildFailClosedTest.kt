package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import kotlinx.coroutines.runBlocking
import java.lang.reflect.InvocationTargetException
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The Otzaria phases used to keep going after a failed seed and after a failed
 * build_state write, and to report success either way.
 *
 * That matters because `appendOtzariaLines` / `appendOtzariaLinks` pass the SAME
 * path as `baseDb` and `persistDb` (build.gradle.kts): the seed reads
 * build/seforim.db and the VACUUM INTO at the end of the phase deletes and
 * rewrites it. A swallowed seed failure therefore replaced the DB the run had
 * just produced with an empty one — and a swallowed snapshot failure published
 * the previous release's id allocator state beside it.
 *
 * These drive the real entry points (through their file facades, since all three
 * declare `main` in this package) and assert the phase now stops.
 */
class OtzariaBuildFailClosedTest {

    private val savedProperties = mutableMapOf<String, String?>()
    private val previousSeverity = Logger.config.minSeverity

    @AfterTest
    fun restoreGlobals() {
        savedProperties.forEach { (key, value) ->
            if (value == null) System.clearProperty(key) else System.setProperty(key, value)
        }
        savedProperties.clear()
        Logger.setMinSeverity(previousSeverity)
    }

    private fun setProperty(key: String, value: String) {
        if (key !in savedProperties) savedProperties[key] = System.getProperty(key)
        System.setProperty(key, value)
    }

    /** Runs a generator entry point; unwraps the reflective invocation. */
    private fun runGenerator(facade: String) {
        val main = Class.forName("io.github.kdroidfilter.seforimlibrary.otzariasqlite.$facade")
            .getMethod("main", Array<String>::class.java)
        try {
            main.invoke(null, arrayOf<String>() as Any)
        } catch (e: InvocationTargetException) {
            throw e.targetException
        }
    }

    /**
     * Pins the abort to the ATTACH of the corrupt base — not to some later step
     * that happens to fail too, which would make these tests pass on the code
     * that swallowed the seed failure.
     */
    private fun assertSeedFailure(failure: Throwable) {
        val text = generateSequence<Throwable>(failure) { it.cause }.mapNotNull { it.message }.joinToString(" | ")
        assertTrue(
            text.contains("not a database", ignoreCase = true),
            "expected the corrupt base DB to be the cause, got: $text",
        )
    }

    private fun corruptDb(dir: Path): Path {
        val file = dir.resolve("corrupt-base.db")
        Files.write(file, "this is emphatically not a SQLite database".repeat(8).toByteArray())
        return file
    }

    /** A stand-in for the DB a previous phase produced: three books, on disk. */
    private fun populatedTargetDb(dir: Path): Path {
        val file = dir.resolve("seforim.db")
        Class.forName("org.sqlite.JDBC")
        DriverManager.getConnection("jdbc:sqlite:${file.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT)")
                st.executeUpdate("INSERT INTO book(id, title) VALUES (1, 'א'), (2, 'ב'), (3, 'ג')")
            }
        }
        return file
    }

    private fun countBooks(db: Path): Long {
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM book").use { rs ->
                    rs.next()
                    return rs.getLong(1)
                }
            }
        }
    }

    private fun writeBooks(db: Path, ids: Iterable<Long>) {
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT)")
                ids.forEach { id -> st.executeUpdate("INSERT INTO book(id, title) VALUES ($id, 'book-$id')") }
            }
        }
    }

    private fun writeStateMarker(db: Path, marker: String) {
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate("CREATE TABLE state_marker (value TEXT NOT NULL)")
                st.executeUpdate("INSERT INTO state_marker(value) VALUES ('$marker')")
            }
        }
    }

    private fun stateMarker(db: Path): String =
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT value FROM state_marker").use { rs ->
                    check(rs.next())
                    rs.getString(1)
                }
            }
        }

    private fun walPath(db: Path): Path =
        db.resolveSibling(db.fileName.toString() + "-wal")

    private fun openWalWriter(db: Path, id: Long): Connection {
        val conn = DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}")
        conn.createStatement().use { st ->
            st.executeQuery("PRAGMA journal_mode=WAL").use { rs ->
                check(rs.next())
                check(rs.getString(1).equals("wal", ignoreCase = true))
            }
            st.execute("PRAGMA wal_autocheckpoint=0")
            st.executeUpdate("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT)")
            st.executeUpdate("INSERT INTO book(id, title) VALUES ($id, 'wal-book-$id')")
        }
        assertTrue(Files.isRegularFile(walPath(db)), "premise: the open writer has committed frames in WAL")
        assertTrue(Files.size(walPath(db)) > 0, "premise: WAL contains frames")
        return conn
    }

    /** Makes the exact main+WAL image a killed writer would leave at [target]. */
    private fun copyUncheckpointedWalImage(target: Path, id: Long) {
        val source = target.resolveSibling("source-${target.fileName}")
        val writer = openWalWriter(source, id)
        try {
            Files.copy(source, target)
            Files.copy(walPath(source), walPath(target))
            assertTrue(Files.size(walPath(target)) > 0, "target fixture has copied uncheckpointed WAL frames")
        } finally {
            writer.close()
        }
    }

    private fun maxId(db: Path, table: String): Long {
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COALESCE(MAX(id), 0) FROM \"$table\"").use { rs ->
                    rs.next()
                    return rs.getLong(1)
                }
            }
        }
    }

    // ─── (1) seed fail-closed ──────────────────────────────────────────────

    @Test
    fun `phase 2 aborts on an unreadable base DB instead of overwriting the target`() {
        val dir = Files.createTempDirectory("s13-links-seed")
        val target = populatedTargetDb(dir)
        val before = Files.readAllBytes(target)

        setProperty("seforimDb", ":memory:")
        setProperty("persistDb", target.toAbsolutePath().toString())
        setProperty("baseDb", corruptDb(dir).toAbsolutePath().toString())
        setProperty("sourceDir", Files.createDirectory(dir.resolve("source")).toAbsolutePath().toString())
        setProperty("buildStatePath", dir.resolve("seforim.db.buildstate").toAbsolutePath().toString())

        val failure = assertFailsWith<Exception> { runGenerator("GenerateLinksKt") }
        assertSeedFailure(failure)

        assertContentEquals(before, Files.readAllBytes(target), "the target DB must survive a failed seed")
        assertEquals(3, countBooks(target))
        assertTrue(Files.notExists(dir.resolve("seforim.db.buildstate")), "no buildstate for a phase that failed")
    }

    @Test
    fun `phase 1 aborts on an unreadable base DB instead of overwriting the target`() {
        val dir = Files.createTempDirectory("s13-lines-seed")
        val target = populatedTargetDb(dir)
        val before = Files.readAllBytes(target)

        setProperty("seforimDb", ":memory:")
        setProperty("appendExistingDb", "true")
        setProperty("persistDb", target.toAbsolutePath().toString())
        setProperty("baseDb", corruptDb(dir).toAbsolutePath().toString())
        setProperty("sourceDir", Files.createDirectory(dir.resolve("source")).toAbsolutePath().toString())
        setProperty("acronymDb", dir.resolve("acronymizer.db").toAbsolutePath().toString())
        setProperty("buildStatePath", dir.resolve("seforim.db.buildstate").toAbsolutePath().toString())

        val failure = assertFailsWith<Exception> { runGenerator("GenerateLinesKt") }
        assertSeedFailure(failure)

        assertContentEquals(before, Files.readAllBytes(target), "the target DB must survive a failed seed")
        assertEquals(3, countBooks(target))
    }

    // ─── (1b) a MISSING base is fail-closed too ────────────────────────────
    //
    // The absence of the base used to be a WARN and the phase continued: with
    // baseDb == persistDb (the release path) that vacuumed an empty in-memory DB
    // over the target and exited 0 — an empty database published as a success.

    private fun assertNamesTheMissingBase(failure: Throwable, missing: Path) {
        val text = generateSequence<Throwable>(failure) { it.cause }.mapNotNull { it.message }.joinToString(" | ")
        assertTrue(text.contains(missing.toAbsolutePath().toString()), "the abort must name the base path, got: $text")
        assertTrue(text.contains("allowEmptyBase"), "the abort must name the opt-in that overrides it, got: $text")
    }

    @Test
    fun `phase 2 refuses a missing base DB before it touches the target`() {
        val dir = Files.createTempDirectory("s16-links-missing-base")
        val target = populatedTargetDb(dir)
        val before = Files.readAllBytes(target)
        val missing = dir.resolve("nowhere").resolve("base.db")

        setProperty("seforimDb", ":memory:")
        setProperty("persistDb", target.toAbsolutePath().toString())
        setProperty("baseDb", missing.toAbsolutePath().toString())
        setProperty("sourceDir", Files.createDirectory(dir.resolve("source")).toAbsolutePath().toString())
        setProperty("buildStatePath", dir.resolve("seforim.db.buildstate").toAbsolutePath().toString())

        val failure = assertFailsWith<IllegalStateException> { runGenerator("GenerateLinksKt") }
        assertNamesTheMissingBase(failure, missing)

        assertContentEquals(before, Files.readAllBytes(target), "the target must be byte-identical")
        assertEquals(3, countBooks(target))
        assertTrue(Files.notExists(dir.resolve("seforim.db.buildstate")), "no buildstate for a phase that failed")
        assertNoCandidateBeside(target)
    }

    @Test
    fun `phase 1 with appendExistingDb refuses a missing base DB before it touches the target`() {
        val dir = Files.createTempDirectory("s16-lines-missing-base")
        val target = populatedTargetDb(dir)
        val before = Files.readAllBytes(target)
        val missing = dir.resolve("nowhere").resolve("base.db")

        setProperty("seforimDb", ":memory:")
        setProperty("appendExistingDb", "true")
        setProperty("persistDb", target.toAbsolutePath().toString())
        setProperty("baseDb", missing.toAbsolutePath().toString())
        setProperty("sourceDir", Files.createDirectory(dir.resolve("source")).toAbsolutePath().toString())
        setProperty("acronymDb", dir.resolve("acronymizer.db").toAbsolutePath().toString())
        setProperty("buildStatePath", dir.resolve("seforim.db.buildstate").toAbsolutePath().toString())

        val failure = assertFailsWith<IllegalStateException> { runGenerator("GenerateLinesKt") }
        assertNamesTheMissingBase(failure, missing)

        assertContentEquals(before, Files.readAllBytes(target), "the target must be byte-identical")
        assertEquals(3, countBooks(target))
        assertTrue(Files.notExists(dir.resolve("seforim.db.buildstate")), "no buildstate for a phase that failed")
        assertNoCandidateBeside(target)
    }

    @Test
    fun `phase 1 on disk with appendExistingDb refuses a missing DB before the driver creates it`() {
        // The on-disk append path has no seed and no VACUUM INTO: the JDBC driver
        // simply creates an empty file at dbPath and the phase runs on it. Same
        // explicit flag, same rule — and the check has to come before the driver
        // opens the URL, or the "missing" DB exists by the time it is reported.
        val dir = Files.createTempDirectory("s16-lines-disk-missing")
        val missing = dir.resolve("nowhere").resolve("seforim.db")

        setProperty("seforimDb", missing.toAbsolutePath().toString())
        setProperty("appendExistingDb", "true")
        setProperty("sourceDir", Files.createDirectory(dir.resolve("source")).toAbsolutePath().toString())
        setProperty("acronymDb", dir.resolve("acronymizer.db").toAbsolutePath().toString())
        setProperty("buildStatePath", dir.resolve("seforim.db.buildstate").toAbsolutePath().toString())

        val failure = assertFailsWith<IllegalStateException> { runGenerator("GenerateLinesKt") }
        assertNamesTheMissingBase(failure, missing)

        assertTrue(Files.notExists(missing), "no DB may be created at the path that was reported missing")
        assertTrue(Files.notExists(missing.parent), "nor its directory")
        assertTrue(Files.notExists(dir.resolve("seforim.db.buildstate")), "no buildstate for a phase that failed")
    }

    // ─── (1c) the publish is atomic ────────────────────────────────────────

    private fun assertNoCandidateBeside(target: Path) {
        assertTrue(
            Files.notExists(target.resolveSibling(target.fileName.toString() + DbPublish.CANDIDATE_SUFFIX)),
            "no .candidate may survive",
        )
    }

    @Test
    fun `a successful publish renames the candidate and leaves none behind`() {
        val dir = Files.createTempDirectory("s16-publish-ok")
        val target = populatedTargetDb(dir)

        runBlocking {
            DbPublish.publishAtomically(target, Logger.withTag("test")) { candidate ->
                DriverManager.getConnection("jdbc:sqlite:${candidate.toAbsolutePath()}").use { conn ->
                    conn.createStatement().use { st ->
                        st.executeUpdate("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT)")
                        st.executeUpdate("INSERT INTO book(id, title) VALUES (1, 'א'), (2, 'ב')")
                    }
                }
            }
        }

        assertEquals(2, countBooks(target), "the target now holds what the candidate held")
        assertNoCandidateBeside(target)
    }

    @Test
    fun `a publish that fails mid-write keeps the previous DB and discards the candidate`() {
        val dir = Files.createTempDirectory("s16-publish-torn")
        val target = populatedTargetDb(dir)
        val before = Files.readAllBytes(target)

        assertFailsWith<IllegalStateException> {
            runBlocking {
                DbPublish.publishAtomically(target, Logger.withTag("test")) { candidate ->
                    // A half-written file, then death — exactly what an OOM or a
                    // cancelled job leaves behind mid-VACUUM.
                    Files.write(candidate, byteArrayOf(1, 2, 3))
                    error("vacuum died")
                }
            }
        }

        assertContentEquals(before, Files.readAllBytes(target), "the previous DB is still there, untouched")
        assertEquals(3, countBooks(target))
        assertNoCandidateBeside(target)
    }

    @Test
    fun `a candidate that is not a database is never published`() {
        val dir = Files.createTempDirectory("s16-publish-garbage")
        val target = populatedTargetDb(dir)
        val before = Files.readAllBytes(target)

        assertFailsWith<Exception> {
            runBlocking {
                DbPublish.publishAtomically(target, Logger.withTag("test")) { candidate ->
                    Files.write(candidate, ByteArray(0))
                }
            }
        }

        assertContentEquals(before, Files.readAllBytes(target))
        assertNoCandidateBeside(target)
    }

    @Test
    fun `low candidate space fails closed before the existing DB is touched`() {
        val dir = Files.createTempDirectory("s29-publish-low-space")
        val target = populatedTargetDb(dir)
        val before = Files.readAllBytes(target)

        val failure = assertFailsWith<DbPublish.InsufficientCandidateSpaceException> {
            runBlocking {
                DbPublish.publishAtomically(
                    target = target,
                    logger = Logger.withTag("test"),
                    // Injection avoids filling a real filesystem merely to
                    // exercise the fail-closed branch.
                    availableSpace = { 0L },
                ) { error("writer must not run after the space preflight") }
            }
        }

        assertTrue(failure.message.orEmpty().contains("existing DB was left untouched"))
        assertContentEquals(before, Files.readAllBytes(target))
        assertEquals(3, countBooks(target))
        assertNoCandidateBeside(target)
    }

    @Test
    fun `a stale candidate from a killed run is replaced, not appended to`() {
        val dir = Files.createTempDirectory("s16-publish-stale")
        val target = populatedTargetDb(dir)
        val stale = target.resolveSibling(target.fileName.toString() + DbPublish.CANDIDATE_SUFFIX)
        Files.write(stale, "left over by a killed run".toByteArray())

        runBlocking {
            DbPublish.publishAtomically(target, Logger.withTag("test")) { candidate ->
                assertTrue(Files.notExists(candidate), "the stale candidate is removed before the write")
                DriverManager.getConnection("jdbc:sqlite:${candidate.toAbsolutePath()}").use { conn ->
                    conn.createStatement().use { st ->
                        st.executeUpdate("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT)")
                        st.executeUpdate("INSERT INTO book(id, title) VALUES (1, 'א')")
                    }
                }
            }
        }

        assertEquals(1, countBooks(target))
        assertNoCandidateBeside(target)
    }

    @Test
    fun `a stale WAL or journal beside the target does not survive the publish`() {
        // Every on-disk Otzaria stage opens seforim.db in WAL mode; one killed
        // mid-run leaves seforim.db-wal/-shm behind. SQLite trusts a -wal it finds
        // next to a database whatever that database's header says, so a fresh
        // file renamed under a stale one has the old frames replayed into it on
        // the next open. The publisher must ask SQLite to normalize them before
        // moving either main file; it must not delete a target WAL itself.
        val dir = Files.createTempDirectory("s16-publish-stale-wal")
        val target = populatedTargetDb(dir)
        val stale = DbPublish.SQLITE_SIDECAR_SUFFIXES.map { suffix ->
            target.resolveSibling(target.fileName.toString() + suffix).also {
                Files.write(it, "frames of the previous database".toByteArray())
            }
        }
        assertEquals(3, stale.size, "premise: -journal, -wal and -shm are all covered")

        runBlocking {
            DbPublish.publishAtomically(target, Logger.withTag("test")) { candidate ->
                DriverManager.getConnection("jdbc:sqlite:${candidate.toAbsolutePath()}").use { conn ->
                    conn.createStatement().use { st ->
                        st.executeUpdate("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT)")
                        st.executeUpdate("INSERT INTO book(id, title) VALUES (1, 'א'), (2, 'ב')")
                    }
                }
            }
        }

        stale.forEach { assertTrue(Files.notExists(it), "${it.fileName} must not outlive the DB it belonged to") }
        assertEquals(2, countBooks(target), "the published DB reads back as the candidate, not as a WAL replay")
        assertNoCandidateBeside(target)
    }

    @Test
    fun `a WAL candidate is closed sealed and published as one main file`() {
        val dir = Files.createTempDirectory("s29-seal-candidate-wal")
        val database = populatedTargetDb(dir)
        val state = dir.resolve("seforim.db.buildstate")
        writeStateMarker(state, "old")

        var candidateWriter: Connection? = null
        try {
            runBlocking {
                DbPublish.publishPairAtomically(
                    databaseTarget = database,
                    buildStateTarget = state,
                    logger = Logger.withTag("test"),
                    writeDatabaseCandidate = { candidate ->
                        // Deliberately leave the writer open while the
                        // candidate callback returns. The pair code must not
                        // seal/move it until beforePublish closes this owner.
                        candidateWriter = openWalWriter(candidate, 91)
                    },
                    writeBuildStateCandidate = { candidate -> writeStateMarker(candidate, "new") },
                    verifyCandidates = { _, _ -> },
                    beforePublish = {
                        candidateWriter?.close()
                        candidateWriter = null
                    },
                )
            }
        } finally {
            candidateWriter?.close()
        }

        assertEquals(91L, maxId(database, "book"))
        assertEquals("new", stateMarker(state))
        DbPublish.SQLITE_SIDECAR_SUFFIXES.forEach { suffix ->
            assertTrue(
                Files.notExists(database.resolveSibling(database.fileName.toString() + suffix)),
                "published main file must have no $suffix sidecar",
            )
        }
    }

    @Test
    fun `old WAL frames survive backup crash recovery after sealing`() {
        val dir = Files.createTempDirectory("s29-seal-old-wal")
        val database = dir.resolve("seforim.db")
        // Copy the main+WAL image while the source writer is still open, then
        // close only the source. Do not open target before DbPublish seals it.
        copyUncheckpointedWalImage(database, 41)
        assertTrue(Files.isRegularFile(walPath(database)), "premise: old target still has committed WAL frames")
        val state = dir.resolve("seforim.db.buildstate")
        writeStateMarker(state, "old")

        assertFailsWith<DbPublish.SimulatedProcessDeathForTest> {
            runBlocking {
                DbPublish.publishPairAtomically(
                    databaseTarget = database,
                    buildStateTarget = state,
                    logger = Logger.withTag("test"),
                    writeDatabaseCandidate = { candidate -> writeBooks(candidate, listOf(99)) },
                    writeBuildStateCandidate = { candidate -> writeStateMarker(candidate, "new") },
                    verifyCandidates = { _, _ -> },
                    afterTransition = { transition ->
                        if (transition == DbPublish.PairPublishTransition.DATABASE_BACKED_UP) {
                            throw DbPublish.SimulatedProcessDeathForTest()
                        }
                    },
                )
            }
        }

        DbPublish.recoverInterruptedPair(database, state, Logger.withTag("test"))
        assertEquals(41L, maxId(database, "book"), "checkpointed old WAL row survives backup/recovery")
        assertEquals("old", stateMarker(state))
        assertFalse(DbPublish.hasPendingPairRecovery(database))
    }

    // ─── (1e) DB + build_state are a recoverable pair ─────────────────────

    @Test
    fun `snapshot failure preserves the old pair and retry allocates no reused ID`() {
        val dir = Files.createTempDirectory("s29-pair-snapshot")
        val database = dir.resolve("seforim.db")
        writeBooks(database, listOf(1))
        val state = dir.resolve("seforim.db.buildstate")
        val oldAllocator = InMemoryIdAllocator.load(null)
        assertEquals(1L, oldAllocator.bookId("test", "old"))
        oldAllocator.snapshotTo(state, mapOf("run" to "old"))
        val beforeDatabase = Files.readAllBytes(database)
        val beforeState = Files.readAllBytes(state)

        assertFailsWith<IllegalStateException> {
            runBlocking {
                DbPublish.publishPairAtomically(
                    databaseTarget = database,
                    buildStateTarget = state,
                    logger = Logger.withTag("test"),
                    writeDatabaseCandidate = { candidate -> writeBooks(candidate, listOf(1, 2)) },
                    writeBuildStateCandidate = { error("simulated snapshot failure") },
                    verifyCandidates = { _, _ -> error("verification must not run without a state candidate") },
                )
            }
        }

        assertContentEquals(beforeDatabase, Files.readAllBytes(database), "new DB must not precede its state")
        assertContentEquals(beforeState, Files.readAllBytes(state), "old state remains paired with old DB")
        assertNoCandidateBeside(database)
        assertNoCandidateBeside(state)

        // A retry loads the old pair, so the id that the failed candidate had
        // planned to use is still safe: it is not present in the published DB.
        val retry = InMemoryIdAllocator.load(state)
        assertEquals(2L, retry.bookId("test", "new"))
        val retryMeta = mapOf("run" to "retry")
        runBlocking {
            DbPublish.publishPairAtomically(
                databaseTarget = database,
                buildStateTarget = state,
                logger = Logger.withTag("test"),
                writeDatabaseCandidate = { candidate -> writeBooks(candidate, listOf(1, 2)) },
                writeBuildStateCandidate = { candidate -> retry.snapshotTo(candidate, retryMeta) },
                verifyCandidates = { candidateDatabase, candidateState ->
                    BuildStateVerifier.verifyFreshSnapshot(
                        buildStatePath = candidateState,
                        dbPath = candidateDatabase,
                        expectedMeta = retryMeta,
                    )
                },
            )
        }
        assertEquals(2, countBooks(database))
        assertEquals(3L, InMemoryIdAllocator.load(state).bookId("test", "after-retry"))
    }

    @Test
    fun `recovery restores a complete old pair at every uncommitted move boundary`() {
        val boundaries = DbPublish.PairPublishTransition.entries
        for (boundary in boundaries) {
            val dir = Files.createTempDirectory("s29-pair-crash-${boundary.name.lowercase()}")
            val database = dir.resolve("seforim.db")
            writeBooks(database, listOf(1))
            val state = dir.resolve("seforim.db.buildstate")
            writeStateMarker(state, "old-state")

            assertFailsWith<DbPublish.SimulatedProcessDeathForTest> {
                runBlocking {
                    DbPublish.publishPairAtomically(
                        databaseTarget = database,
                        buildStateTarget = state,
                        logger = Logger.withTag("test"),
                        writeDatabaseCandidate = { candidate -> writeBooks(candidate, listOf(2)) },
                        writeBuildStateCandidate = { candidate -> writeStateMarker(candidate, "new-state") },
                        verifyCandidates = { _, _ -> },
                        afterTransition = { reached ->
                            if (reached == boundary) throw DbPublish.SimulatedProcessDeathForTest()
                        },
                    )
                }
            }

            assertTrue(DbPublish.hasPendingPairRecovery(database), "the crash must leave a recovery journal")
            DbPublish.recoverInterruptedPair(database, state, Logger.withTag("test"))
            assertFalse(DbPublish.hasPendingPairRecovery(database))
            if (boundary == DbPublish.PairPublishTransition.COMMITTED_MARKED) {
                assertEquals(2L, maxId(database, "book"), "committed recovery keeps the new DB")
                assertEquals("new-state", stateMarker(state))
            } else {
                // Sealing/checkpointing the old members happens before the
                // journal is durable, so recovery promises the same committed
                // logical pair (and allocator identity), not byte-for-byte
                // SQLite layout. The dedicated low-space tests retain the
                // stricter untouched-byte guarantee because their preflight
                // exits before sealing either target.
                assertEquals(1L, maxId(database, "book"), "$boundary restores the old DB contents")
                assertEquals("old-state", stateMarker(state), "$boundary restores the old allocator state")
            }
        }
    }

    // ─── (1d) the build script keeps the contract these rules rest on ──────

    private fun otzariaBuildScript(): String = generateSequence(Path.of("").toAbsolutePath()) { it.parent }
        .map { it.resolve("generator/otzariasqlite/build.gradle.kts") }
        .firstOrNull { Files.isRegularFile(it) }
        ?.let { Files.readString(it) }
        ?: error("could not locate generator/otzariasqlite/build.gradle.kts")

    /** The body of one `tasks.register<JavaExec>("name")` block. */
    private fun javaExecTask(script: String, name: String): String {
        val header = "tasks.register<JavaExec>(\"$name\")"
        val start = script.indexOf(header)
        assertTrue(start >= 0, "the build script must still register $name")
        val next = script.indexOf("tasks.register<", start + header.length)
        return if (next < 0) script.substring(start) else script.substring(start, next)
    }

    @Test
    fun `the release tasks pass one path as both base and target and forward the opt-in`() {
        val script = otzariaBuildScript()
        // Every rule above rests on this: the append phases are seeded from the
        // very file they publish back, so a missing base is a lost input and
        // continuing would vacuum an empty DB over the release DB. If this ever
        // stops being true the fail-closed rule needs rethinking, not silence.
        val links = javaExecTask(script, "appendOtzariaLinks")
        assertTrue(
            links.contains("""systemProperty("baseDb", persistDb)"""),
            "appendOtzariaLinks must seed from the file it publishes",
        )
        assertTrue(
            links.contains("""systemProperty("persistDb", persistDb)"""),
            "appendOtzariaLinks must publish to the file it was seeded from",
        )
        // -PallowEmptyBase is what every abort message advertises as the way
        // past a missing base. A Gradle project property is NOT a system
        // property of the forked JVM, so without this forwarding the flag would
        // silently do nothing and the message would be a lie.
        listOf("generateLines", "generateLinks", "appendOtzariaLines", "appendOtzariaLinks").forEach { task ->
            assertTrue(
                javaExecTask(script, task)
                    .contains("""systemProperty("allowEmptyBase", project.property("allowEmptyBase")"""),
                "$task must forward -PallowEmptyBase to the JVM it starts",
            )
        }
    }

    // ─── (2) build_state fail-closed ───────────────────────────────────────

    @Test
    fun `a build_state that cannot be written stops the stage`() {
        val dir = Files.createTempDirectory("s13-havrouta-unwritable")
        val db = dir.resolve("seforim.db")
        // The snapshot's parent is a regular file, so BuildStateWriter's
        // createDirectories fails — the cheapest portable unwritable target.
        val blocker = Files.write(dir.resolve("blocker"), byteArrayOf(0))
        val statePath = blocker.resolve("seforim.db.buildstate")

        setProperty("seforimDb", db.toAbsolutePath().toString())
        setProperty("sourceDir", Files.createDirectory(dir.resolve("source")).toAbsolutePath().toString())
        setProperty("buildStatePath", statePath.toAbsolutePath().toString())

        val failure = assertFailsWith<Exception> { runGenerator("GenerateHavroutaLinksKt") }
        val text = generateSequence<Throwable>(failure) { it.cause }.mapNotNull { it.message }.joinToString(" | ")
        assertTrue(text.contains("blocker"), "expected the unwritable build_state path in the cause, got: $text")
        // It must be the snapshot's own rethrow that stops the stage, not the
        // verifier noticing the file afterwards — otherwise dropping the rethrow
        // would leave this test green.
        assertFalse(
            text.contains("was reported written but"),
            "the snapshot failure must propagate itself, not be caught downstream by the verifier: $text",
        )

        assertTrue(Files.notExists(statePath))
    }

    // ─── (3) happy path + the written-state self-check ─────────────────────

    @Test
    fun `a completed stage leaves a build_state whose counters lead the DB`() {
        val dir = Files.createTempDirectory("s13-havrouta-ok")
        val db = dir.resolve("seforim.db")
        val statePath = dir.resolve("seforim.db.buildstate")

        setProperty("seforimDb", db.toAbsolutePath().toString())
        setProperty("sourceDir", Files.createDirectory(dir.resolve("source")).toAbsolutePath().toString())
        setProperty("buildStatePath", statePath.toAbsolutePath().toString())

        runGenerator("GenerateHavroutaLinksKt")

        assertTrue(Files.exists(statePath), "the stage must leave its build_state behind")
        val header = BuildStateVerifier.readHeader(statePath)
        assertEquals("havroutalinks", header.meta["generator"])
        // The stage pre-registers every ConnectionType through the allocator, so
        // its counter has to sit above what the DB now holds.
        val connectionTypes = maxId(db, "connection_type")
        assertTrue(connectionTypes > 0, "the stage inserted connection types")
        assertTrue(
            header.counters.getValue(IdTable.CONNECTION_TYPE) > connectionTypes,
            "next_id must lead MAX(id)",
        )
    }
}
