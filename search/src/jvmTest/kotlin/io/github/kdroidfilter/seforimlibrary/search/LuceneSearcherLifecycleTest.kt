package io.github.kdroidfilter.seforimlibrary.search

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import org.apache.lucene.document.IntPoint
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.KnnFloatVectorQuery
import org.apache.lucene.store.AlreadyClosedException
import org.apache.lucene.store.FSDirectory
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class LuceneSearcherLifecycleTest {
    @Test
    fun `cursor remains on its snapshot while new searches and facets see a commit`() = withIndex { path ->
        LuceneSearchEngine(path).use { engine ->
            runBlocking {
                assertNotNull(engine.openSession("alpha")).use { old ->
                    val ids = assertNotNull(old.nextPage(5)).hits.map { it.lineId }.toMutableList()
                    LuceneLifecycleFixture.write(path, 25..32)
                    engine.refresh()
                    assertEquals(32L, assertNotNull(engine.computeFacets("alpha")).totalHits)
                    assertEquals((1L..32L).toSet(), engine.searchAllText("alpha", limit = 100).map { it.lineId }.toSet())
                    while (true) {
                        val page = old.nextPage(5) ?: break
                        ids += page.hits.map { it.lineId }
                    }
                    assertEquals(24, ids.size)
                    assertEquals((1L..24L).toSet(), ids.toSet())
                }
            }
        }
    }

    @Test
    fun `engine close preserves existing session and rejects new index requests`() = withIndex { path ->
        val engine = LuceneSearchEngine(path)
        assertNotNull(engine.openSession("alpha")).use { session ->
            engine.close()
            engine.close()
            assertEquals(24, runBlocking { assertNotNull(session.nextPage(100)).hits.size })
            assertFailsWith<AlreadyClosedException> { engine.openSession("alpha") }
            assertFailsWith<AlreadyClosedException> { engine.searchBooksByTitlePrefix("book") }
            session.close()
            session.close()
            assertFailsWith<AlreadyClosedException> { runBlocking { session.nextPage(10) } }
        }
    }

    @Test
    fun `stateless search survives engine close while snippet I O is suspended`() = withIndex { path ->
        runBlocking {
            val entered = CompletableDeferred<Unit>()
            val finish = CompletableDeferred<Unit>()
            val engine = LuceneSearchEngine(path, SnippetProvider {
                entered.complete(Unit)
                finish.await()
                emptyMap()
            })
            try {
                val result = async { engine.searchAllText("alpha", limit = 10) }
                entered.await()
                engine.close()
                finish.complete(Unit)
                assertEquals(10, result.await().size)
            } finally {
                finish.complete(Unit)
                engine.close()
            }
        }
    }

    @Test
    fun `cancelled stateless search releases its lease before index removal`() = withIndex { path ->
        runBlocking {
            val entered = CompletableDeferred<Unit>()
            val never = CompletableDeferred<Unit>()
            val engine = LuceneSearchEngine(path, SnippetProvider {
                entered.complete(Unit)
                never.await()
                emptyMap()
            })
            try {
                val result = async { engine.searchAllText("alpha", limit = 10) }
                entered.await()
                result.cancelAndJoin()
            } finally {
                engine.close()
            }
            // withIndex deletes every file immediately; this must also pass on Windows.
        }
    }

    @Test
    fun `closing a cursor defers release until suspended page retrieval completes`() = withIndex { path ->
        runBlocking {
            val entered = CompletableDeferred<Unit>()
            val finish = CompletableDeferred<Unit>()
            val engine = LuceneSearchEngine(path, SnippetProvider {
                entered.complete(Unit)
                finish.await()
                emptyMap()
            })
            val session = assertNotNull(engine.openSession("alpha"))
            val heldReader = (session.javaClass.getDeclaredField("lease").apply { isAccessible = true }
                .get(session) as ManagedIndexSearchers.Lease).searcher.indexReader
            try {
                val page = async { session.nextPage(10) }
                entered.await()
                engine.close()
                session.close()
                session.close()
                assertTrue(heldReader.refCount > 0, "an active page must keep its snapshot alive")
                finish.complete(Unit)
                assertEquals(10, assertNotNull(page.await()).hits.size)
                assertEquals(0, heldReader.refCount, "the final page releases the closed engine's reader")
                assertFailsWith<AlreadyClosedException> { session.nextPage(10) }
            } finally {
                finish.complete(Unit)
                session.close()
                engine.close()
            }
        }
    }

    @Test
    fun `concurrent cursor pages advance the cursor once per completed page`() = withIndex { path ->
        runBlocking {
            val entered = CompletableDeferred<Unit>()
            val finish = CompletableDeferred<Unit>()
            val engine = LuceneSearchEngine(path, SnippetProvider {
                entered.complete(Unit)
                finish.await()
                emptyMap()
            })
            engine.use {
                assertNotNull(engine.openSession("alpha")).use { session ->
                    val first = async { assertNotNull(session.nextPage(10)).hits.map { it.lineId } }
                    entered.await()
                    val second = async { assertNotNull(session.nextPage(10)).hits.map { it.lineId } }
                    finish.complete(Unit)
                    val ids = first.await() + second.await()
                    assertEquals(20, ids.size)
                    assertEquals(20, ids.toSet().size)
                }
            }
        }
    }

    @Test
    fun `vector results and filters match fresh readers before and after a commit`() = withIndex { path ->
        VectorSearcher(path).use { vector ->
            repeat(2) { iteration ->
                if (iteration > 0) LuceneLifecycleFixture.write(path, 25..48)
                for (baseOnly in listOf(false, true)) {
                    for (books in listOf(null, listOf(1L, 3L))) {
                        val query = LuceneLifecycleFixture.vector(9)
                        val actual = vector.search(query, 7, baseOnly, books)
                        val expected = FSDirectory.open(path).use { directory ->
                            DirectoryReader.open(directory).use { reader ->
                                val searcher = IndexSearcher(reader)
                                val filter = BooleanQuery.Builder()
                                if (baseOnly) filter.add(IntPoint.newExactQuery("is_base_book", 1), BooleanClause.Occur.FILTER)
                                if (books != null) filter.add(IntPoint.newSetQuery("book_id", 1, 3), BooleanClause.Occur.FILTER)
                                val knn = KnnFloatVectorQuery("vec", query, 7, if (baseOnly || books != null) filter.build() else null)
                                val stored = searcher.storedFields()
                                searcher.search(knn, 7).scoreDocs.map { hit ->
                                    val doc = stored.document(hit.doc)
                                    DenseHit(doc.getField("line_id").numericValue().toLong(),
                                        doc.getField("book_id").numericValue().toLong(), hit.score)
                                }
                            }
                        }
                        assertTrue(actual.isNotEmpty())
                        assertEquals(expected, actual)
                    }
                }
            }
        }
    }

    @Test
    fun `vector owner closes before first search without creating an index`() {
        val parent = Files.createTempDirectory("lazy-vector-index")
        val path = parent.resolve("absent")
        try {
            val vector = VectorSearcher(path)
            vector.close()
            vector.close()
            assertFailsWith<AlreadyClosedException> { vector.search(LuceneLifecycleFixture.vector(1), 1) }
            assertTrue(Files.notExists(path))
        } finally {
            LuceneLifecycleFixture.delete(parent)
        }
    }

    private inline fun withIndex(block: (Path) -> Unit) {
        val path = Files.createTempDirectory("lucene-lifecycle")
        try {
            LuceneLifecycleFixture.create(path)
            block(path)
        } finally {
            LuceneLifecycleFixture.delete(path)
        }
    }
}
