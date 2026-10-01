package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateReader
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSnapshot
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateWriter
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

/**
 * book_moves.csv destination leaves created by renameCategories take stable ids from
 * the build state instead of SQLite's implicit rowid.
 *
 * v30 regression: the new leaf `תלמוד בבלי/כללי הש״ס` took MAX(category.id) + 1 = 1478,
 * the id the build state reserved for the Otzaria folder `אור הישר/סדר נשים`. The
 * Otzaria stage's verify-after-insert caught it and renumbered that folder to 1526.
 *
 * Each test runs the three stages a release build runs, over real files: the Sefaria
 * stage (fresh DB, ids from the seed build state), renameCategories (build state
 * ATTACHed), and the Otzaria stage (InMemoryIdAllocator + verify-after-insert).
 */
class BookMoveLeafIdsTest {
    private val logger = Logger.withTag("test")
    private val dir: Path = Files.createTempDirectory("book-move-leaf-ids")

    @AfterTest
    fun cleanup() {
        dir.toFile().deleteRecursively()
    }

    private val sefaria = listOf("תלמוד בבלי", "תלמוד בבלי/אחרונים", "תלמוד בבלי/ראשונים")
    private val ohrHayashar = "תלמוד בבלי/אחרונים/אור הישר"
    private val sederNashim = "תלמוד בבלי/אחרונים/אור הישר/סדר נשים"
    private val klaleiHashas = "תלמוד בבלי/כללי הש״ס"
    private val moveToKlalei = BookMove("דרכי התלמוד", "תלמוד בבלי/ראשונים", klaleiHashas)

    /** The state the v30 build was seeded with, in miniature: the next id after Sefaria is reserved. */
    private fun v30LikeSeed(): Path = seed(
        categories = sefaria.withIndex().associate { (i, path) -> path to i + 1L } +
            mapOf(ohrHayashar to 4L, sederNashim to 5L),
        nextId = 6,
    )

    @Test
    fun `a new leaf does not take the id the build state reserves for an Otzaria folder`() {
        val build = runBuild("v31", v30LikeSeed(), listOf(moveToKlalei))

        // The old implicit rowid would have been 4: MAX(id) after the Sefaria stage + 1.
        assertEquals(4L, build.implicitLeafId, "the fixture must reproduce the v30 collision")
        assertEquals(6L, build.paths.getValue(klaleiHashas), "a new leaf gets a fresh id")
        assertEquals(4L, build.paths.getValue(ohrHayashar))
        assertEquals(5L, build.paths.getValue(sederNashim), "the Otzaria folder keeps its reserved id")
        assertEquals(0L, build.otzariaFreshCategories, "nothing was reallocated")
        assertEquals(6L, build.bookCategory)
    }

    @Test
    fun `a leaf keeps its id in the next build even when the Sefaria tree grows`() {
        val first = runBuild("v31", v30LikeSeed(), listOf(moveToKlalei))

        // Next build: Sefaria gains a category (MAX(id) moves) and book_moves.csv gains a
        // row ahead of the old one, so the old code would have renumbered the leaf.
        val newLeaf = "תלמוד בבלי/אחרונים/מפרשי הש״ס"
        val moves = listOf(BookMove("מבוא התלמוד", "תלמוד בבלי/ראשונים", newLeaf), moveToKlalei)
        val second = runBuild("v32", first.state, moves, extraSefaria = listOf("תלמוד בבלי/מפרשים"))
        assertEquals(first.paths.getValue(klaleiHashas), second.paths.getValue(klaleiHashas))
        assertEquals(5L, second.paths.getValue(sederNashim))
        assertEquals(7L, second.paths.getValue("תלמוד בבלי/מפרשים"), "Sefaria's new category is fresh")
        assertEquals(8L, second.paths.getValue(newLeaf), "the second new leaf is fresh too")
        assertEquals(0L, second.otzariaFreshCategories)

        // Same inputs again: nothing moves.
        val third = runBuild("v33", second.state, moves, extraSefaria = listOf("תלמוד בבלי/מפרשים"))
        assertEquals(second.paths, third.paths)
    }

    /** The real v30 build state around the hijack, in the ids that matter. */
    private val v30SefariaIds = linkedMapOf(
        "שו״ת" to 1L, "שו״ת/אחרונים" to 2L, "קבלה" to 3L, "מדרש" to 4L, "מדרש/אגדה" to 5L,
        "מחשבת ישראל" to 6L, "מחשבת ישראל/אחרונים" to 7L, "מחשבת ישראל/ראשונים" to 8L,
        "תלמוד בבלי" to 12L, "תלמוד בבלי/אחרונים" to 420L, "תלמוד בבלי/ראשונים" to 1471L,
    )
    private val piskeiRabbeinuMendel = "תלמוד בבלי/ראשונים/פסקי רבינו מענדל קלויזנער"
    private val v30Otzaria = listOf(sederNashim, piskeiRabbeinuMendel)

    /** Sefaria ends at 1471, the shipped leaves sit on 1472-1478 with no key, סדר נשים on 1526. */
    private fun realV30Seed(): Path = seed(
        categories = v30SefariaIds + mapOf(ohrHayashar to 1385L, sederNashim to 1526L, piskeiRabbeinuMendel to 1479L),
        nextId = 1536,
    )

    private val v30Moves = (PUBLISHED_BOOK_MOVE_LEAF_IDS.keys + klaleiHashas).mapIndexed { i, dest ->
        BookMove("ספר $i", "תלמוד בבלי/ראשונים", dest)
    }

    private fun realV30Build(name: String, seed: Path, moves: List<BookMove>) = runBuild(
        name, seed, moves,
        sefaria = v30SefariaIds.keys.toList(),
        books = v30Moves.map { it.name } + moveToKlalei.name,
        publishedIds = PUBLISHED_BOOK_MOVE_LEAF_IDS,
        otzariaFolders = v30Otzaria,
        restores = CATEGORY_ID_RESTORES,
    )

    @Test
    fun `the first keyed build keeps the shipped leaf ids and gives סדר נשים back 1478`() {
        val first = realV30Build("v31", realV30Seed(), v30Moves)
        for ((dest, id) in PUBLISHED_BOOK_MOVE_LEAF_IDS) {
            assertEquals(id, first.paths.getValue(dest), dest)
        }
        assertEquals(1478L, first.paths.getValue(sederNashim), "back on its v28/v29 id")
        assertEquals(1536L, first.paths.getValue(klaleiHashas), "the v30-only leaf moves to a fresh id")
        assertEquals(1479L, first.paths.getValue(piskeiRabbeinuMendel))
        assertEquals(1385L, first.paths.getValue(ohrHayashar))
        assertEquals(0L, first.otzariaFreshCategories, "nothing was reallocated")

        // Next build, rows reordered and a new leaf added: every id holds, the new leaf
        // lands above the counter (not on 1479), and the restore does not run again.
        val newLeaf = "תלמוד בבלי/ראשונים/חדש"
        val second = realV30Build(
            "v32", first.state,
            v30Moves.reversed() + BookMove(moveToKlalei.name, "תלמוד בבלי/ראשונים", newLeaf),
        )
        assertEquals(first.paths - newLeaf, second.paths - newLeaf)
        assertEquals(1537L, second.paths.getValue(newLeaf))
        assertEquals(0L, second.otzariaFreshCategories)
    }

    @Test
    fun `the restore keeps the reallocated id when the published one is taken`() {
        // A key already holds 1478: the restore must not take it.
        val state = seed(
            categories = v30SefariaIds + mapOf(
                ohrHayashar to 1385L, sederNashim to 1526L, piskeiRabbeinuMendel to 1479L, "אחר" to 1478L,
            ),
            nextId = 1536,
        )
        val db = dir.resolve("taken.db")
        sefariaStage(db, state, v30SefariaIds.keys.toList(), books)
        withAttached(db, state) { conn ->
            conn.autoCommit = false
            assertEquals(0, BookMoveLeafIds(conn).restore(CATEGORY_ID_RESTORES, logger))
            conn.commit()
        }
        assertEquals(1526L, BuildStateReader().read(state).lookups.getValue(IdTable.CATEGORY).getValue(sederNashim))
    }

    @Test
    fun `a published id is used only while nothing else holds it`() {
        val seed = seed(
            categories = sefaria.withIndex().associate { (i, path) -> path to i + 1L } +
                mapOf(sederNashim to 6L, ohrHayashar to 7L),
            nextId = 8,
        )
        val held = "תלמוד בבלי/אחרונים/חדש"
        val inDb = "תלמוד בבלי/אחרונים/אחר"
        val build = runBuild(
            "v31",
            seed,
            listOf(
                moveToKlalei,
                BookMove("מבוא התלמוד", "תלמוד בבלי/ראשונים", held),
                BookMove("כללי הגמרא", "תלמוד בבלי/ראשונים", inDb),
            ),
            // 4 is free; 6 is held by סדר נשים's key; 5 is a row written outside the allocator.
            publishedIds = mapOf(klaleiHashas to 4L, held to 6L, inDb to 5L),
            foreignRows = mapOf(5L to "זר"),
        )
        assertEquals(4L, build.paths.getValue(klaleiHashas), "the free published id is kept")
        assertEquals(8L, build.paths.getValue(held), "a held id is never taken")
        assertEquals(9L, build.paths.getValue(inDb), "an id the DB uses is never taken")
        assertEquals(6L, build.paths.getValue(sederNashim))
        assertEquals(0L, build.otzariaFreshCategories)
    }

    @Test
    fun `leaf ids roll back with the DB transaction`() {
        val state = v30LikeSeed()
        val db = dir.resolve("rollback.db")
        sefariaStage(db, state, sefaria, books)
        val before = BuildStateReader().read(state)
        withAttached(db, state) { conn ->
            conn.autoCommit = false
            applyBookMove(conn, moveToKlalei, logger, BookMoveLeafIds(conn, publishedIds = emptyMap()))
            conn.rollback()
        }
        val after = BuildStateReader().read(state)
        assertEquals(before.lookups, after.lookups)
        assertEquals(before.counters, after.counters)
        withAttached(db, state) { conn -> assertNull(categoryIdByTitle(conn, "כללי הש״ס")) }
    }

    @Test
    fun `a keyed leaf id that the DB already holds fails instead of merging folders`() {
        val state = seed(
            categories = sefaria.withIndex().associate { (i, path) -> path to i + 1L } +
                mapOf(BOOK_MOVE_LEAF_KEY_PREFIX + klaleiHashas to 2L),
            nextId = 6,
        )
        val db = dir.resolve("occupied.db")
        sefariaStage(db, state, sefaria, books)
        withAttached(db, state) { conn ->
            conn.autoCommit = false
            assertFailsWith<SQLException> { applyBookMove(conn, moveToKlalei, logger, BookMoveLeafIds(conn)) }
            conn.rollback()
        }
    }

    private val books = listOf("דרכי התלמוד", "מבוא התלמוד", "כללי הגמרא")

    // ─── Simulated build ──────────────────────────────────────────────────────

    private class Build(
        val state: Path,
        val paths: Map<String, Long>,
        val implicitLeafId: Long,
        val otzariaFreshCategories: Long,
        val bookCategory: Long,
    )

    private fun runBuild(
        name: String,
        seed: Path,
        moves: List<BookMove>,
        extraSefaria: List<String> = emptyList(),
        sefaria: List<String> = this.sefaria,
        books: List<String> = this.books,
        publishedIds: Map<String, Long> = emptyMap(),
        foreignRows: Map<Long, String> = emptyMap(),
        otzariaFolders: List<String> = listOf(sederNashim),
        restores: List<CategoryIdRestore> = emptyList(),
    ): Build {
        val db = dir.resolve("$name.db")
        val state = dir.resolve("$name.db.buildstate")
        Files.copy(seed, state)

        sefariaStage(db, state, sefaria + extraSefaria, books)
        DriverManager.getConnection("jdbc:sqlite:$db").use { conn ->
            for ((id, title) in foreignRows) {
                conn.prepareStatement("INSERT INTO category (id, parentId, title, level) VALUES (?, NULL, ?, 0)").use { st ->
                    st.setLong(1, id)
                    st.setString(2, title)
                    st.executeUpdate()
                }
            }
        }

        var implicitLeafId = -1L
        withAttached(db, state) { conn ->
            conn.autoCommit = false
            // What the old code did: the dry-run path still uses the implicit rowid.
            applyBookMove(conn, moves.last(), logger)
            implicitLeafId = categoryIdByTitle(conn, moves.last().destPath.substringAfterLast('/'))!!
            conn.rollback()

            val leafIds = BookMoveLeafIds(conn, publishedIds = publishedIds)
            leafIds.restore(restores, logger)
            for (move in moves) applyBookMove(conn, move, logger, leafIds)
            conn.commit()
        }
        BuildStateVerifier.verifyFreshSnapshot(state, db, emptyMap())

        val otzariaFresh = otzariaStage(db, state, otzariaFolders)
        val paths = categoryPaths(db)
        val bookCategory = DriverManager.getConnection("jdbc:sqlite:$db").use { conn ->
            conn.prepareStatement("SELECT categoryId FROM book WHERE title = ?").use { st ->
                st.setString(1, moveToKlalei.name)
                st.executeQuery().use { rs -> rs.next(); rs.getLong(1) }
            }
        }
        return Build(state, paths, implicitLeafId, otzariaFresh, bookCategory)
    }

    /** Fresh DB; Sefaria categories (keyed by path) and books get their ids from [state]. */
    private fun sefariaStage(db: Path, state: Path, categories: List<String>, books: List<String>) = runBlocking {
        val driver = JdbcSqliteDriver("jdbc:sqlite:$db")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(db.toString(), driver)
        try {
            val allocator = InMemoryIdAllocator.load(state)
            val bindings = IdAllocatorBindings(allocator, repo)
            val ids = HashMap<String, Long>()
            for (path in categories) {
                val parent = path.substringBeforeLast('/', "").ifEmpty { null }
                ids[path] = bindings.upsertCategory(
                    canonicalPath = path,
                    parentId = parent?.let { ids.getValue(it) },
                    title = path.substringAfterLast('/'),
                    level = path.count { it == '/' },
                    orderIndex = 1,
                )
            }
            val rishonim = ids.getValue("תלמוד בבלי/ראשונים")
            for (title in books) {
                val bookId = allocator.bookId("Sefaria", title)
                repo.executeRawQuery(
                    "INSERT INTO book (id, categoryId, sourceId, title) VALUES ($bookId, $rishonim, 1, '$title')",
                )
            }
            allocator.snapshotTo(state, mapOf("generator" to "test-sefaria"))
        } finally {
            repo.close()
        }
    }

    /**
     * The Otzaria stage as GenerateLines + Generator run it: reuse an existing folder
     * by (parent, title), otherwise upsertCategory. Returns the fresh category count,
     * i.e. how many folders were new or reallocated.
     */
    private fun otzariaStage(db: Path, state: Path, folders: List<String>): Long = runBlocking {
        val driver = JdbcSqliteDriver("jdbc:sqlite:$db")
        val repo = SeforimRepository(db.toString(), driver)
        try {
            val allocator = InMemoryIdAllocator.load(state)
            val maxId = DriverManager.getConnection("jdbc:sqlite:$db").use { queryMaxId(it, "category") }
            allocator.ensureCounterAtLeast(IdTable.CATEGORY, maxId + 1)
            val bindings = IdAllocatorBindings(allocator, repo)
            for (folder in folders) {
                var parentId: Long? = null
                var path = ""
                for ((level, title) in folder.split('/').withIndex()) {
                    path = if (path.isEmpty()) title else "$path/$title"
                    val children = if (parentId == null) repo.getRootCategories() else repo.getCategoryChildren(parentId)
                    parentId = children.firstOrNull { it.title == title }?.id
                        ?: bindings.upsertCategory(path, parentId, title, level, 999)
                }
            }
            allocator.snapshotTo(state, mapOf("generator" to "test-otzaria"))
            allocator.stats().perTable.getValue(IdTable.CATEGORY).freshlyAllocated
        } finally {
            repo.close()
        }
    }

    private fun seed(categories: Map<String, Long>, nextId: Long): Path {
        val path = Files.createTempFile(dir, "seed", ".buildstate")
        Files.delete(path)
        BuildStateWriter().write(
            BuildStateSnapshot.empty().copy(
                counters = mapOf(IdTable.CATEGORY to nextId),
                lookups = mapOf(IdTable.CATEGORY to categories),
            ),
            path,
        )
        return path
    }

    private fun withAttached(db: Path, state: Path, block: (Connection) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite:$db").use { conn ->
            conn.prepareStatement("ATTACH DATABASE ? AS $BOOK_MOVE_STATE_SCHEMA").use { st ->
                st.setString(1, state.toAbsolutePath().toString())
                st.execute()
            }
            block(conn)
        }
    }

    private fun categoryIdByTitle(conn: Connection, title: String): Long? =
        conn.prepareStatement("SELECT id FROM category WHERE title = ?").use { st ->
            st.setString(1, title)
            st.executeQuery().use { rs -> if (rs.next()) rs.getLong(1) else null }
        }

    /** path -> id for every category, from the DB itself. */
    private fun categoryPaths(db: Path): Map<String, Long> =
        DriverManager.getConnection("jdbc:sqlite:$db").use { conn ->
            val rows = HashMap<Long, Pair<Long?, String>>()
            conn.createStatement().use { st ->
                st.executeQuery("SELECT id, parentId, title FROM category").use { rs ->
                    while (rs.next()) {
                        val parent = rs.getLong(2).let { if (rs.wasNull()) null else it }
                        rows[rs.getLong(1)] = parent to rs.getString(3)
                    }
                }
            }
            fun pathOf(id: Long): String {
                val (parent, title) = rows.getValue(id)
                return if (parent == null) title else pathOf(parent) + "/" + title
            }
            val result = rows.keys.associateBy(::pathOf)
            assertEquals(rows.size, result.size, "two categories share a path")
            result.values.forEach { id -> assertNotEquals(0L, id) }
            result
        }
}
