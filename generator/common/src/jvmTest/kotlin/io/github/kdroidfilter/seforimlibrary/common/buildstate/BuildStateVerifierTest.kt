package io.github.kdroidfilter.seforimlibrary.common.buildstate

import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.sql.DriverManager
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The five generator stages used to publish whatever build_state.db happened to
 * be on disk. These cover the post-write self-check that now stands between a
 * snapshot and "stage completed": the file must be the one this stage wrote, and
 * its counters must be ahead of every id the produced DB holds.
 */
class BuildStateVerifierTest {

    @JvmField
    @Rule
    val tmp = TemporaryFolder()

    private fun lineHash(seed: Int): ByteArray =
        MessageDigest.getInstance("SHA-1").digest(byteArrayOf(seed.toByte()))

    /** A DB holding only the `id` columns the verifier reads. */
    private fun miniDb(name: String, tables: Map<String, List<Long>>): Path {
        val db = tmp.newFolder().toPath().resolve(name)
        Class.forName("org.sqlite.JDBC")
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                for ((table, ids) in tables) {
                    st.executeUpdate("CREATE TABLE \"$table\" (id INTEGER PRIMARY KEY NOT NULL, v TEXT)")
                    for (id in ids) st.executeUpdate("INSERT INTO \"$table\"(id, v) VALUES ($id, 'x')")
                }
            }
        }
        return db
    }

    /** Two books, three lines in the first — counters land at book=3, line=4. */
    private fun snapshot(target: Path, meta: Map<String, String>) {
        val allocator = InMemoryIdAllocator.load(null)
        val bookA = allocator.bookId("Otzaria", "A")
        allocator.bookId("Otzaria", "B")
        repeat(3) { i -> allocator.lineId(bookA, lineHash(i), 0) }
        allocator.snapshotTo(target, meta)
    }

    private fun meta(generatedAt: String) = mapOf(
        "generator" to "test",
        "generated_at" to generatedAt,
    )

    @Test
    fun `a fresh snapshot whose counters lead the db passes`() {
        val state = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        val expected = meta("2026-09-08T10:00:00Z")
        snapshot(state, expected)
        val db = miniDb("seforim.db", mapOf("book" to listOf(1L, 2L), "line" to listOf(1L, 2L, 3L)))

        BuildStateVerifier.verifyFreshSnapshot(state, db, expected)
    }

    @Test
    fun `counters behind the db are rejected per table`() {
        val state = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        val expected = meta("2026-09-08T10:00:00Z")
        snapshot(state, expected)
        // A stale/foreign buildstate: the DB holds ids this allocator never issued,
        // so the next build would hand id 9 to some other book.
        val db = miniDb("seforim.db", mapOf("book" to listOf(1L, 2L, 9L), "line" to listOf(1L, 2L, 3L)))

        val failure = assertFailsWith<IllegalStateException> {
            BuildStateVerifier.verifyFreshSnapshot(state, db, expected)
        }
        assertContains(failure.message!!, "book: next_id=3 <= MAX(id)=9")
        assertContains(failure.message!!, "re-issue ids this build already published")
    }

    /**
     * The boundary the gate exists for: `next_id` is handed to the NEXT fresh key,
     * so `next_id == MAX(id)` is already a collision, not a near miss. Without this
     * case `<=` could be weakened to `<` and the whole suite would still pass.
     */
    @Test
    fun `a counter equal to the db maximum is already a collision`() {
        val state = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        val expected = meta("2026-09-08T10:00:00Z")
        snapshot(state, expected)
        // Counters are book=3, line=4; a book already sitting on id 3 means the next
        // build would hand 3 to a different book.
        val db = miniDb("seforim.db", mapOf("book" to listOf(1L, 2L, 3L), "line" to listOf(1L)))

        val failure = assertFailsWith<IllegalStateException> {
            BuildStateVerifier.verifyFreshSnapshot(state, db, expected)
        }
        assertContains(failure.message!!, "book: next_id=3 <= MAX(id)=3")
    }

    @Test
    fun `a snapshot left over from another run is rejected`() {
        val state = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        // Written by the PREVIOUS build; this build's snapshotTo failed, so the
        // file simply stayed in place — same name, plausible size, valid SQLite.
        snapshot(state, meta("2026-09-01T10:00:00Z"))
        val db = miniDb("seforim.db", mapOf("book" to listOf(1L, 2L), "line" to listOf(1L, 2L, 3L)))

        val failure = assertFailsWith<IllegalStateException> {
            BuildStateVerifier.verifyFreshSnapshot(state, db, meta("2026-09-08T10:00:00Z"))
        }
        assertContains(failure.message!!, "not the snapshot this stage wrote")
        assertContains(failure.message!!, "meta.generated_at='2026-09-01T10:00:00Z'")
    }

    @Test
    fun `a missing snapshot is rejected`() {
        val state = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        val db = miniDb("seforim.db", mapOf("book" to listOf(1L)))

        val failure = assertFailsWith<IllegalStateException> {
            BuildStateVerifier.verifyFreshSnapshot(state, db, meta("now"))
        }
        assertContains(failure.message!!, "does not exist")
    }

    @Test
    fun `a missing db is rejected`() {
        val state = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        val expected = meta("2026-09-08T10:00:00Z")
        snapshot(state, expected)
        val missing = tmp.newFolder().toPath().resolve("seforim.db")

        val failure = assertFailsWith<IllegalStateException> {
            BuildStateVerifier.verifyFreshSnapshot(state, missing, expected)
        }
        assertContains(failure.message!!, "does not exist")
    }

    @Test
    fun `a snapshot that is not a build_state is rejected`() {
        val state = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        Files.write(state, "certainly not a database".repeat(20).toByteArray())
        val db = miniDb("seforim.db", mapOf("book" to listOf(1L)))

        // Either the driver refuses the file or the meta/counters are absent —
        // both must abort the stage rather than let it publish this.
        assertFailsWith<Exception> {
            BuildStateVerifier.verifyFreshSnapshot(state, db, meta("now"))
        }
    }

    @Test
    fun `readHeader returns counters and meta without the id maps`() {
        val state = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        val expected = meta("2026-09-08T10:00:00Z")
        snapshot(state, expected)

        val header = BuildStateVerifier.readHeader(state)
        assertEquals(3L, header.counters[IdTable.BOOK])
        assertEquals(4L, header.counters[IdTable.LINE])
        assertEquals("2026-09-08T10:00:00Z", header.meta["generated_at"])
        assertTrue(header.meta.containsKey("schema_version"))
    }

    /**
     * All three `alt_toc_entry` writers now allocate their ids, so the table has
     * no exemption left: a counter behind the DB is a real "the next build
     * re-issues a published id" defect and must fail here. Re-adding
     * `IdTable.ALT_TOC_ENTRY` to `NOT_ALLOCATOR_ISSUED` makes this test fail.
     */
    @Test
    fun `alt_toc_entry counters behind the db are rejected like every other table`() {
        val state = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        val expected = meta("2026-09-08T10:00:00Z")
        snapshot(state, expected)
        // The allocator never issued an alt_toc_entry id, so its counter is absent
        // while the DB carries the ids the old implicit-rowid path produced.
        val db = miniDb(
            "seforim.db",
            mapOf(
                "book" to listOf(1L, 2L),
                "line" to listOf(1L, 2L, 3L),
                "alt_toc_entry" to listOf(1L, 2L, 71602L),
            ),
        )

        val failure = assertFailsWith<IllegalStateException> {
            BuildStateVerifier.verifyFreshSnapshot(state, db, expected)
        }
        assertContains(failure.message!!, "alt_toc_entry")
        assertContains(failure.message!!, "MAX(id)=71602")
    }

    @Test
    fun `an allocator-issued alt_toc_entry counter ahead of the db passes`() {
        val state = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        val expected = meta("2026-09-08T10:00:00Z")
        val allocator = InMemoryIdAllocator.load(null)
        val bookA = allocator.bookId("Otzaria", "A")
        allocator.bookId("Otzaria", "B")
        repeat(3) { i -> allocator.lineId(bookA, lineHash(i), 0) }
        val structureId = allocator.altTocStructureId(bookA, "Parasha")
        val entryIds = (1..3).map { allocator.altTocEntryId(structureId, "$it") }
        assertEquals(listOf(1L, 2L, 3L), entryIds)
        allocator.snapshotTo(state, expected)

        val db = miniDb(
            "seforim.db",
            mapOf(
                "book" to listOf(1L, 2L),
                "line" to listOf(1L, 2L, 3L),
                "alt_toc_entry" to entryIds,
            ),
        )

        BuildStateVerifier.verifyFreshSnapshot(state, db, expected)
    }

    @Test
    fun `tables the db does not carry are skipped, not failed`() {
        val state = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        val expected = meta("2026-09-08T10:00:00Z")
        snapshot(state, expected)
        // Only `book` exists here. Real stage DBs carry all 15 (the schema is
        // created wholesale at repository init); this pins the fixture path.
        val db = miniDb("seforim.db", mapOf("book" to listOf(1L, 2L)))

        BuildStateVerifier.verifyFreshSnapshot(state, db, expected)
    }
}
