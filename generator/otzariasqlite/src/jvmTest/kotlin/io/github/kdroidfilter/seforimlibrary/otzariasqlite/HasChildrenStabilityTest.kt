package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.idresolver.IdResolverAdapter
import io.github.kdroidfilter.seforimlibrary.idresolver.IdResolverLoader
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertEquals

/**
 * Verifies that hasChildren is set correctly on TOC entries both with and without
 * an IdResolver (i.e. on first generation and on regeneration from a backup DB).
 *
 * The bug: TocEntryData was populated with a local counter ID before insertTocEntry()
 * was called. When the IdResolver assigned stable IDs from a previous DB, the counter
 * ID and the real DB ID diverged, so the second-pass hasChildren update always missed.
 */
class HasChildrenStabilityTest {

    // A minimal book with a parent chapter and two sub-chapters.
    //   Chapter A (h1) → should have hasChildren = true
    //   Chapter B (h2) → child of A, no children → hasChildren = false
    //   Chapter C (h2) → child of A, no children → hasChildren = false
    private val bookContent = """
        <h1>Chapter A</h1>
        Some text under chapter A.
        <h2>Chapter B</h2>
        Text under chapter B.
        <h2>Chapter C</h2>
        Text under chapter C.
    """.trimIndent()

    private fun setupSourceDir(root: java.io.File) {
        val bookDir = root.resolve("אוצריא/TestCategory")
        bookDir.mkdirs()
        bookDir.resolve("TestBook.txt").writeText(bookContent)
    }

    private fun openRepo(dbPath: String): Pair<SeforimRepository, JdbcSqliteDriver> {
        val driver = JdbcSqliteDriver("jdbc:sqlite:$dbPath")
        return SeforimRepository(dbPath, driver) to driver
    }

    private suspend fun runGeneration(sourceDir: java.io.File, dbPath: String, idProvider: IdResolverAdapter? = null) {
        val (repo, driver) = openRepo(dbPath)
        try {
            DatabaseGenerator(
                sourceDirectory = sourceDir.toPath(),
                repository = repo,
                idResolverProvider = idProvider
            ).generateLinesOnly()
        } finally {
            driver.close()
        }
    }

    private suspend fun getTocForFirstBook(dbPath: String): List<io.github.kdroidfilter.seforimlibrary.core.models.TocEntry> {
        val (repo, driver) = openRepo(dbPath)
        return try {
            val bookId = repo.getAllBooks().first().id
            repo.getTocEntriesForBook(bookId)
        } finally {
            driver.close()
        }
    }

    @Test
    fun `hasChildren is correct on first generation`() = runBlocking {
        val tmp = Files.createTempDirectory("otzaria-test-run1").toFile()
        val db = tmp.resolve("run1.db").absolutePath
        try {
            setupSourceDir(tmp)
            runGeneration(tmp, db)

            val toc = getTocForFirstBook(db)
            val chapterA = toc.first { it.level == 1 }
            assertTrue(chapterA.hasChildren, "Chapter A should have hasChildren=true (it has B and C as children)")
            val chapterB = toc.first { it.level == 2 && it.text.contains("B") }
            val chapterC = toc.first { it.level == 2 && it.text.contains("C") }
            assertFalse(chapterB.hasChildren, "Chapter B should have hasChildren=false (leaf)")
            assertFalse(chapterC.hasChildren, "Chapter C should have hasChildren=false (leaf)")
        } finally {
            tmp.deleteRecursively()
        }
    }

    /**
     * Simulates the production pipeline: Sefaria generates the base DB, then Otzaria
     * content is appended using an IdResolver seeded from the *previous combined run*.
     *
     * Why prevDb has more entries than sefariaDb:
     *   prevDb    = the previous combined build (Sefaria + Otzaria together), so its
     *               max TOC ID reflects both corpora from the last run.
     *   sefariaDb = a fresh Sefaria-only generation, so its max TOC ID is lower
     *               (no Otzaria entries yet).
     * This is the normal production state: prevDb.maxId > sefariaDb.maxId.
     *
     * Setup:
     *   prevDb    – 2 books × (parent + child) = 4 toc entries, max=4.
     *               IdResolver loaded from here: nextTocEntryId starts at 5.
     *   sefariaDb – fresh Sefaria-only generation, 1 book → toc IDs 1,2, max=2.
     *               Generator counter for the append step would start at 3.
     *   Otzaria append – new book (not in prevDb map) appended to sefariaDb with the
     *               resolver.  Resolver allocates IDs 5 and 6.  Old buggy counter gave
     *               3 and 4, so allTocEntries had wrong IDs and the second-pass
     *               updateTocEntryHasChildren(3, true) was a no-op → Otzaria parent
     *               stayed hasChildren=false.
     *
     * The fix stores the real inserted ID in allTocEntries, so the second pass
     * correctly calls updateTocEntryHasChildren(5, true).
     */
    @Test
    fun `hasChildren is correct when otzaria appended to sefaria with diverged id counter`() = runBlocking {
        val tmp = Files.createTempDirectory("otzaria-test-sefaria-otzaria").toFile()
        try {
            // Build prevDb from a source that is larger than the later Sefaria DB so that
            // the resolver's nextTocEntryId (prevMax+1=5) diverges from the Generator counter
            // (sefariaMax+1=3) at append time.
            val prevDir = tmp.resolve("prev_source").also { it.mkdirs() }
            val prevBookDir = prevDir.resolve("אוצריא/PrevCat").also { it.mkdirs() }
            prevBookDir.resolve("PrevBook1.txt").writeText(
                "<h1>Prev1Parent</h1>\ntext\n<h2>Prev1Child</h2>\ntext"
            )
            prevBookDir.resolve("PrevBook2.txt").writeText(
                "<h1>Prev2Parent</h1>\ntext\n<h2>Prev2Child</h2>\ntext"
            )
            val prevDb = tmp.resolve("prev.db").absolutePath
            runGeneration(prevDir, prevDb)
            // prevDb: 4 toc entries (IDs 1–4), resolver's nextTocEntryId = 5

            // Build a fresh Sefaria DB with 1 book → 2 toc entries (IDs 1, 2), max = 2.
            val sefariaDir = tmp.resolve("sefaria_source").also { it.mkdirs() }
            sefariaDir.resolve("אוצריא/SefCat").also { it.mkdirs() }
                .resolve("SefariaBook.txt")
                .writeText("<h1>SefParent</h1>\ntext\n<h2>SefChild</h2>\ntext")
            val sefariaDb = tmp.resolve("combined.db").absolutePath
            runGeneration(sefariaDir, sefariaDb)
            // sefariaDb: SefParent=ID1 (hasChildren=true), SefChild=ID2 (hasChildren=false)

            val sefariaBookIds = run {
                val (repo, driver) = openRepo(sefariaDb)
                try { repo.getAllBooks().map { it.id }.toSet() } finally { driver.close() }
            }

            // Append a NEW Otzaria book (path not in prevDb map) to the Sefaria DB.
            // Resolver allocates toc IDs 5 and 6 (prevMax+1).
            // Old Generator counter would have started at 3 (sefariaMax+1) → divergence.
            val otzariaDir = tmp.resolve("otzaria_source").also { it.mkdirs() }
            otzariaDir.resolve("אוצריא/OtzCat").also { it.mkdirs() }
                .resolve("NewOtzariaBook.txt")
                .writeText("<h1>OtzParent</h1>\ntext\n<h2>OtzChild</h2>\ntext")
            val resolver = IdResolverLoader.load(prevDb)
            runGeneration(otzariaDir, sefariaDb, IdResolverAdapter(resolver))

            val (repo, driver) = openRepo(sefariaDb)
            try {
                val allBooks = repo.getAllBooks()
                assertEquals(2, allBooks.size, "Expected exactly one Sefaria book and one Otzaria book")

                val sefariaBook = allBooks.first { it.id in sefariaBookIds }
                val otzariaBook  = allBooks.first { it.id !in sefariaBookIds }

                val sefToc = repo.getTocEntriesForBook(sefariaBook.id)
                val sefParent = sefToc.first { it.level == 1 }
                val sefChild  = sefToc.first { it.level == 2 }
                assertTrue(sefParent.hasChildren,
                    "Sefaria parent (ID ${sefParent.id}) must keep hasChildren=true after Otzaria append")
                assertFalse(sefChild.hasChildren,
                    "Sefaria child (ID ${sefChild.id}) must keep hasChildren=false")

                val otzToc = repo.getTocEntriesForBook(otzariaBook.id)
                val otzParent = otzToc.first { it.level == 1 }
                val otzChild  = otzToc.first { it.level == 2 }
                assertTrue(otzParent.hasChildren,
                    "Otzaria parent (ID ${otzParent.id}) must have hasChildren=true — this was false before the fix")
                assertFalse(otzChild.hasChildren,
                    "Otzaria child (ID ${otzChild.id}) must have hasChildren=false")
            } finally {
                driver.close()
            }
        } finally {
            tmp.deleteRecursively()
        }
    }

    @Test
    fun `hasChildren is correct on regeneration with IdResolver`() = runBlocking {
        val tmp = Files.createTempDirectory("otzaria-test-run2").toFile()
        val db1 = tmp.resolve("run1.db").absolutePath
        val db2 = tmp.resolve("run2.db").absolutePath
        try {
            setupSourceDir(tmp)

            // Run 1: establish a baseline DB (no resolver)
            runGeneration(tmp, db1)

            // Run 2: regenerate using the baseline as the ID resolver source
            val resolver = IdResolverLoader.load(db1)
            runGeneration(tmp, db2, IdResolverAdapter(resolver))

            val toc = getTocForFirstBook(db2)
            val chapterA = toc.first { it.level == 1 }
            assertTrue(
                chapterA.hasChildren,
                "Run 2 with IdResolver: Chapter A should still have hasChildren=true (was always false before the fix)"
            )
            val chapterB = toc.first { it.level == 2 && it.text.contains("B") }
            val chapterC = toc.first { it.level == 2 && it.text.contains("C") }
            assertFalse(chapterB.hasChildren, "Chapter B should have hasChildren=false (leaf)")
            assertFalse(chapterC.hasChildren, "Chapter C should have hasChildren=false (leaf)")
        } finally {
            tmp.deleteRecursively()
        }
    }
}
