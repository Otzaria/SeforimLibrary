package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateReader
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSnapshot
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateWriter
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import io.github.kdroidfilter.seforimlibrary.common.ids.BOOK_MOVE_LEAF_KEY_PREFIX
import io.github.kdroidfilter.seforimlibrary.common.ids.CategoryLabels
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
import kotlin.test.assertTrue

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

    // ─── Folders that are both a book_moves leaf and an Otzaria folder ──────────

    private val yadDavid = "תלמוד בבלי/אחרונים/יד דוד"
    private val shared = "תלמוד בבלי/אחרונים/משותף"

    @Test
    fun `adding a book move into an existing Otzaria folder keeps that folder's id`() {
        val seed = seed(
            categories = sefaria.withIndex().associate { (i, path) -> path to i + 1L } +
                mapOf(ohrHayashar to 4L, sederNashim to 5L, yadDavid to 6L),
            nextId = 7,
        )
        val folders = listOf(sederNashim, yadDavid)
        val first = runBuild("q1", seed, listOf(moveToKlalei), otzariaFolders = folders)
        assertEquals(6L, first.paths.getValue(yadDavid))

        val second = runBuild(
            "q2", first.state,
            listOf(BookMove("מבוא התלמוד", "תלמוד בבלי/ראשונים", yadDavid), moveToKlalei),
            otzariaFolders = folders,
        )
        assertEquals(6L, second.paths.getValue(yadDavid), "the leaf takes the Otzaria folder's id")
        assertEquals(first.paths - yadDavid, second.paths - yadDavid)
        assertEquals(6L, leafKeys(second.state).getValue(yadDavid))
        assertEquals(0L, second.otzariaFreshCategories)
    }

    @Test
    fun `removing the last book move into a shared folder keeps that folder's id`() {
        val folders = listOf(sederNashim, shared)
        val first = runBuild(
            "r1", v30LikeSeed(),
            listOf(BookMove("מבוא התלמוד", "תלמוד בבלי/ראשונים", shared), moveToKlalei),
            otzariaFolders = folders,
        )
        val sharedId = first.paths.getValue(shared)

        val second = runBuild("r2", first.state, listOf(moveToKlalei), otzariaFolders = folders)
        assertEquals(sharedId, second.paths.getValue(shared))
        assertEquals(first.paths, second.paths)
        assertEquals(0L, second.otzariaFreshCategories)
        assertEquals(sharedId, plainKeys(first.state).getValue(shared), "the Otzaria key follows the leaf")
    }

    @Test
    fun `a shared leaf whose Otzaria key holds an older id keeps the leaf id when its rows go`() {
        // The real v30 state of מחשבת ישראל/אחרונים/רמחל: shipped on 1476 as a book_moves
        // leaf, while the build state still holds its Otzaria key on 1322.
        val ramchal = "מחשבת ישראל/אחרונים/רמחל"
        val seed = seed(
            categories = v30SefariaIds + mapOf(
                ohrHayashar to 1385L, sederNashim to 1526L, piskeiRabbeinuMendel to 1479L, ramchal to 1322L,
            ),
            nextId = 1536,
        )
        val folders = v30Otzaria + ramchal
        fun build(name: String, state: Path, moves: List<BookMove>) = runBuild(
            name, state, moves,
            sefaria = v30SefariaIds.keys.toList(),
            books = v30Moves.map { it.name } + moveToKlalei.name,
            publishedIds = PUBLISHED_BOOK_MOVE_LEAF_IDS,
            otzariaFolders = folders,
            restores = CATEGORY_ID_RESTORES,
        )
        val first = build("m1", seed, v30Moves)
        assertEquals(1476L, first.paths.getValue(ramchal))

        val withoutRamchal = v30Moves.filter { it.destPath != ramchal }
        val second = build("m2", first.state, withoutRamchal)
        assertEquals(1476L, second.paths.getValue(ramchal), "not back to the stale 1322")
        assertEquals(first.paths, second.paths)
        assertEquals(0L, second.otzariaFreshCategories)
        // The first build moved the Otzaria key onto 1476 ...
        assertEquals(1476L, plainKeys(first.state).getValue(ramchal))

        // ... and even a key still on 1322 (no build aligned it) does not win over the leaf id.
        val unaligned = seed(
            categories = v30SefariaIds + mapOf(
                ohrHayashar to 1385L, sederNashim to 1478L, piskeiRabbeinuMendel to 1479L, ramchal to 1322L,
                BOOK_MOVE_LEAF_KEY_PREFIX + ramchal to 1476L,
            ),
            nextId = 1537,
        )
        val third = build("m3", unaligned, withoutRamchal)
        assertEquals(1476L, third.paths.getValue(ramchal), "the leaf id wins over the stale Otzaria key")
        assertEquals(1476L, plainKeys(third.state).getValue(ramchal))
    }

    @Test
    fun `a folder id survives book_moves rows added, removed and reordered over many builds`() {
        val reserved = "תלמוד בבלי/אחרונים/שמור"
        val leafOnly = "תלמוד בבלי/ראשונים/ליקוטים"
        val seed = seed(
            categories = sefaria.withIndex().associate { (i, path) -> path to i + 1L } +
                mapOf(ohrHayashar to 4L, sederNashim to 5L, yadDavid to 6L, reserved to 7L),
            nextId = 8,
        )
        val toYadDavid = BookMove("מבוא התלמוד", "תלמוד בבלי/ראשונים", yadDavid)
        val toShared = BookMove("כללי הגמרא", "תלמוד בבלי/ראשונים", shared)
        val toLeafOnly = BookMove("סדר הדורות", "תלמוד בבלי/ראשונים", leafOnly)
        val baseFolders = listOf(sederNashim, yadDavid, shared)
        // (moves, Otzaria folders, extra Sefaria categories) per build.
        val plan = listOf(
            Triple(listOf(moveToKlalei, toShared, toLeafOnly), baseFolders, emptyList()),
            Triple(listOf(toYadDavid, toLeafOnly, moveToKlalei, toShared), baseFolders, emptyList()),
            Triple(listOf(moveToKlalei), baseFolders, listOf("תלמוד בבלי/מפרשים")),
            // The leaf-only folder is gone from book_moves.csv, and Otzaria now has a folder there.
            Triple(listOf(toShared, toYadDavid), baseFolders + leafOnly, listOf("תלמוד בבלי/מפרשים")),
            Triple(emptyList(), baseFolders + leafOnly, listOf("תלמוד בבלי/מפרשים", "תלמוד בבלי/עזר")),
            Triple(listOf(toLeafOnly, toShared, moveToKlalei, toYadDavid), baseFolders, emptyList()),
            Triple(listOf(toYadDavid, toShared, moveToKlalei, toLeafOnly), baseFolders, emptyList()),
        )
        val seen = HashMap<String, Long>()
        var state = seed
        for ((i, step) in plan.withIndex()) {
            val (moves, folders, extra) = step
            val build = runBuild(
                "s$i", state, moves,
                extraSefaria = extra,
                books = books + "סדר הדורות",
                otzariaFolders = folders,
            )
            for ((path, id) in build.paths) {
                assertEquals(seen.getOrPut(path) { id }, id, "build $i moved '$path'")
            }
            for (move in moves) assertEquals(build.paths.getValue(move.destPath), bookCategoryOf(build, move.name))
            assertEquals(5L, build.paths.getValue(sederNashim))
            assertEquals(6L, build.paths.getValue(yadDavid))
            assertNull(build.paths.entries.firstOrNull { it.value == 7L }, "the reserved id is never handed out")
            state = build.state
        }
        // Every folder the plan touches was seen, and each kept one id throughout.
        for (path in listOf(klaleiHashas, shared, leafOnly, yadDavid, sederNashim)) assertTrue(path in seen, path)
        assertEquals(seen.size, seen.values.toSet().size, "two folders shared an id over the builds")
    }

    @Test
    fun `the leaf finds the Otzaria folder whatever quote mark either one spells`() {
        val otzariaSpelling = "תלמוד בבלי/אחרונים/חידושי הרי״ם"
        val csvSpelling = "תלמוד בבלי/אחרונים/חידושי הרי\"ם"
        val seed = seed(
            categories = sefaria.withIndex().associate { (i, path) -> path to i + 1L } +
                mapOf(ohrHayashar to 4L, sederNashim to 5L, otzariaSpelling to 6L),
            nextId = 7,
        )
        val folders = listOf(sederNashim, otzariaSpelling)
        val first = runBuild("g1", seed, listOf(moveToKlalei), otzariaFolders = folders)
        assertEquals(6L, first.paths.getValue(otzariaSpelling))

        val moves = listOf(BookMove("מבוא התלמוד", "תלמוד בבלי/ראשונים", csvSpelling), moveToKlalei)
        val second = runBuild("g2", first.state, moves, otzariaFolders = folders)
        assertEquals(6L, second.paths.getValue(csvSpelling), "the leaf (CSV spelling) takes the folder's id")
        assertNull(second.paths[otzariaSpelling], "one folder, not two")
        assertEquals(0L, second.otzariaFreshCategories)

        val third = runBuild("g3", second.state, listOf(moveToKlalei), otzariaFolders = folders)
        assertEquals(6L, third.paths.getValue(otzariaSpelling))
    }

    @Test
    fun `an Otzaria key whose id a DB row holds is not given to the leaf`() {
        // The Otzaria key for the destination holds 6, but a row written outside the
        // allocator sits on 6: the leaf gets a fresh id, and the key is not moved onto it.
        val seed = seed(
            categories = sefaria.withIndex().associate { (i, path) -> path to i + 1L } +
                mapOf(ohrHayashar to 4L, sederNashim to 5L, yadDavid to 6L),
            nextId = 7,
        )
        val build = runBuild(
            "f1", seed,
            listOf(BookMove("מבוא התלמוד", "תלמוד בבלי/ראשונים", yadDavid)),
            foreignRows = mapOf(6L to "זר"),
            otzariaFolders = listOf(sederNashim, yadDavid),
        )
        assertEquals(7L, build.paths.getValue(yadDavid))
        assertEquals(6L, build.paths.getValue("זר"))
        assertEquals(6L, plainKeys(build.state).getValue(yadDavid), "a key on a live row is left alone")
    }

    @Test
    fun `an Otzaria key that another folder's key also holds is not given to the leaf`() {
        val other = "תלמוד בבלי/אחרונים/אחר"
        val seed = seed(
            categories = sefaria.withIndex().associate { (i, path) -> path to i + 1L } +
                mapOf(ohrHayashar to 4L, sederNashim to 5L, yadDavid to 6L, other to 6L),
            nextId = 7,
        )
        val build = runBuild(
            "h1", seed,
            listOf(BookMove("מבוא התלמוד", "תלמוד בבלי/ראשונים", yadDavid)),
            otzariaFolders = listOf(sederNashim),
        )
        assertEquals(7L, build.paths.getValue(yadDavid))
    }

    private fun bookCategoryOf(build: Build, title: String): Long =
        DriverManager.getConnection("jdbc:sqlite:${build.state.toString().removeSuffix(".buildstate")}").use { conn ->
            conn.prepareStatement("SELECT categoryId FROM book WHERE title = ?").use { st ->
                st.setString(1, title)
                st.executeQuery().use { rs -> rs.next(); rs.getLong(1) }
            }
        }

    private fun categoryKeys(state: Path): Map<String, Long> =
        BuildStateReader().read(state).lookups[IdTable.CATEGORY].orEmpty()

    private fun plainKeys(state: Path): Map<String, Long> =
        categoryKeys(state).filterKeys { !it.startsWith(BOOK_MOVE_LEAF_KEY_PREFIX) }

    private fun leafKeys(state: Path): Map<String, Long> =
        categoryKeys(state).filterKeys { it.startsWith(BOOK_MOVE_LEAF_KEY_PREFIX) }
            .mapKeys { it.key.removePrefix(BOOK_MOVE_LEAF_KEY_PREFIX) }

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
            moves.lastOrNull()?.let { last ->
                applyBookMove(conn, last, logger)
                implicitLeafId = categoryIdByTitle(conn, last.destPath.substringAfterLast('/'))!!
                conn.rollback()
            }

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
     * The Otzaria stage as GenerateLines + Generator.ensureCategoryHierarchy run it:
     * reuse an existing folder by (parent, comparable title) and align its key with a
     * book_moves leaf, otherwise upsertOtzariaCategory. Returns the fresh category
     * count, i.e. how many folders were new or reallocated.
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
                for ((level, rawTitle) in folder.split('/').withIndex()) {
                    val title = CategoryLabels.normalize(rawTitle)
                    path = if (path.isEmpty()) title else "$path/$title"
                    val children = if (parentId == null) repo.getRootCategories() else repo.getCategoryChildren(parentId)
                    val existing = children.firstOrNull {
                        CategoryLabels.comparable(it.title) == CategoryLabels.comparable(title)
                    }
                    parentId = if (existing != null) {
                        bindings.alignWithBookMoveLeaf(path, existing.id)
                        existing.id
                    } else {
                        bindings.upsertOtzariaCategory(path, parentId, title, level, 999)
                    }
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
