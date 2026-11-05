package io.github.kdroidfilter.seforimlibrary.search

import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.KnnFloatVectorQuery
import org.apache.lucene.search.PhraseQuery
import org.apache.lucene.search.PrefixQuery
import org.apache.lucene.search.Query
import org.apache.lucene.search.SearcherFactory
import org.apache.lucene.search.TermQuery
import org.apache.lucene.store.AlreadyClosedException
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.FilterDirectory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue

class ManagedIndexSearchersTest {
    @Test
    fun `closing unused owner is lazy and final`() {
        val parent = Files.createTempDirectory("lazy-searchers")
        val path = parent.resolve("missing-index")
        try {
            val owner = ManagedIndexSearchers(path)
            owner.close()
            owner.close()
            assertFalse(Files.exists(path))
            assertFailsWith<AlreadyClosedException> { owner.acquire() }
            assertFailsWith<AlreadyClosedException> { owner.refresh() }
            assertFalse(Files.exists(path))
        } finally {
            LuceneLifecycleFixture.delete(parent)
        }
    }

    @Test
    fun `unchanged requests share searcher and match fresh-reader results`() = withIndex { path ->
        ManagedIndexSearchers(path).use { owner ->
            val original = owner.acquire().use { it.searcher }
            FSDirectory.open(path).use { directory ->
                repeat(3) {
                    owner.acquire().use { lease ->
                        assertSame(original, lease.searcher)
                        DirectoryReader.open(directory).use { reader ->
                            val fresh = IndexSearcher(reader)
                            for (query in queries()) {
                                assertEquals(results(fresh, query), results(lease.searcher, query))
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    fun `refresh keeps acquired snapshot alive and sees committed changes`() = withIndex { path ->
        ManagedIndexSearchers(path).use { owner ->
            owner.acquire().use { old ->
                assertEquals(24, old.searcher.count(TermQuery(Term("type", "line"))))
                LuceneLifecycleFixture.write(path, 25..29)
                owner.refresh()
                owner.acquire().use { current ->
                    assertNotSame(old.searcher, current.searcher)
                    assertEquals(29, current.searcher.count(TermQuery(Term("type", "line"))))
                    assertEquals(24, old.searcher.count(TermQuery(Term("type", "line"))))
                }
            }
        }
    }

    @Test
    fun `in-place writer CREATE is observed without changing outstanding snapshot`() = withIndex { path ->
        ManagedIndexSearchers(path).use { owner ->
            owner.acquire().use { old ->
                LuceneLifecycleFixture.write(path, 51..57, recreate = true)
                owner.acquire().use { current ->
                    assertEquals(7, current.searcher.count(TermQuery(Term("type", "line"))))
                    assertEquals(24, old.searcher.count(TermQuery(Term("type", "line"))))
                }
            }
        }
    }

    @Test
    fun `missing index closes directory and can be retried after creation`() {
        val path = Files.createTempDirectory("missing-search-index")
        val closed = AtomicInteger()
        try {
            ManagedIndexSearchers(path, trackedDirectories(closed)).use { owner ->
                assertFailsWith<IOException> { owner.acquire() }
                assertEquals(1, closed.get())
                LuceneLifecycleFixture.create(path)
                owner.acquire().use { assertEquals(24, it.searcher.count(TermQuery(Term("type", "line")))) }
            }
            assertEquals(2, closed.get())
        } finally {
            LuceneLifecycleFixture.delete(path)
        }
    }

    @Test
    fun `failed initial searcher construction releases reader and directory`() = withIndex { path ->
        val closed = AtomicInteger()
        var failedReader: IndexReader? = null
        var shouldFail = true
        val factory = object : SearcherFactory() {
            override fun newSearcher(reader: IndexReader, previousReader: IndexReader?): IndexSearcher {
                if (shouldFail) {
                    shouldFail = false
                    failedReader = reader
                    throw IOException("injected initial failure")
                }
                return super.newSearcher(reader, previousReader)
            }
        }
        ManagedIndexSearchers(path, trackedDirectories(closed), factory).use { owner ->
            assertFailsWith<IOException> { owner.acquire() }
            assertEquals(0, failedReader!!.refCount)
            assertEquals(1, closed.get())
            owner.acquire().close()
        }
        assertEquals(2, closed.get())
    }

    @Test
    fun `failed refresh releases new reader and allows retry without corrupting snapshot`() = withIndex { path ->
        var shouldFail = true
        var failedReader: IndexReader? = null
        val factory = object : SearcherFactory() {
            override fun newSearcher(reader: IndexReader, previousReader: IndexReader?): IndexSearcher {
                if (previousReader != null && shouldFail) {
                    shouldFail = false
                    failedReader = reader
                    throw IOException("injected refresh failure")
                }
                return super.newSearcher(reader, previousReader)
            }
        }
        ManagedIndexSearchers(path, searcherFactory = factory).use { owner ->
            owner.acquire().use { old ->
                LuceneLifecycleFixture.write(path, 25..27)
                assertFailsWith<IOException> { owner.refresh() }
                assertEquals(0, failedReader!!.refCount)
                assertEquals(24, old.searcher.count(TermQuery(Term("type", "line"))))
                owner.acquire().use { current ->
                    assertEquals(27, current.searcher.count(TermQuery(Term("type", "line"))))
                }
            }
        }
    }

    @Test
    fun `close waits for leases without blocking callers or leaking directory`() = withIndex { path ->
        val directoryClosed = AtomicInteger()
        val owner = ManagedIndexSearchers(path, trackedDirectories(directoryClosed))
        val lease = owner.acquire()
        val reader = lease.searcher.indexReader
        owner.close()
        assertEquals(0, directoryClosed.get())
        assertTrue(reader.refCount > 0)
        assertEquals(24, lease.searcher.count(TermQuery(Term("type", "line"))))
        assertFailsWith<AlreadyClosedException> { owner.acquire() }
        lease.close()
        lease.close()
        owner.close()
        assertEquals(0, reader.refCount)
        assertEquals(1, directoryClosed.get())
    }

    @Test
    fun `query failure returns lease and does not prevent deterministic cleanup`() = withIndex { path ->
        val directoryClosed = AtomicInteger()
        val owner = ManagedIndexSearchers(path, trackedDirectories(directoryClosed))
        lateinit var reader: IndexReader
        assertFailsWith<IllegalStateException> {
            owner.acquire().use { lease ->
                reader = lease.searcher.indexReader
                error("injected query failure")
            }
        }
        owner.close()
        assertEquals(0, reader.refCount)
        assertEquals(1, directoryClosed.get())
    }

    @Test
    fun `parallel queries survive refresh and release before directory replacement`() = withIndex { path ->
        val owner = ManagedIndexSearchers(path)
        val pool = Executors.newFixedThreadPool(4)
        val started = CountDownLatch(4)
        val proceed = CountDownLatch(1)
        try {
            val futures = (1..4).map {
                pool.submit<Int> {
                    owner.acquire().use { lease ->
                        started.countDown()
                        check(proceed.await(20, TimeUnit.SECONDS))
                        repeat(50) {
                            assertEquals(24, lease.searcher.count(TermQuery(Term("type", "line"))))
                        }
                        24
                    }
                }
            }
            assertTrue(started.await(20, TimeUnit.SECONDS))
            LuceneLifecycleFixture.write(path, 25..30)
            owner.acquire().use { assertEquals(30, it.searcher.count(TermQuery(Term("type", "line")))) }
            owner.close()
            proceed.countDown()
            futures.forEach { assertEquals(24, it.get(20, TimeUnit.SECONDS)) }
            // Exercise the required close -> move/delete -> new owner protocol.
            // On Windows any remaining mapped reader would make this fail.
            val moved = path.resolveSibling(path.fileName.toString() + "-old")
            Files.move(path, moved)
            try {
                LuceneLifecycleFixture.create(path, lines = 3)
                ManagedIndexSearchers(path).use { reopened ->
                    reopened.acquire().use { assertEquals(3, it.searcher.count(TermQuery(Term("type", "line")))) }
                }
            } finally {
                LuceneLifecycleFixture.delete(moved)
            }
        } finally {
            proceed.countDown()
            pool.shutdown()
            assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS))
            owner.close()
        }
    }

    private fun trackedDirectories(closed: AtomicInteger): (Path) -> Directory = { path ->
        object : FilterDirectory(FSDirectory.open(path)) {
            override fun close() {
                closed.incrementAndGet()
                super.close()
            }
        }
    }

    private fun queries(): List<Query> = listOf(
        TermQuery(Term("text", "alpha")),
        PhraseQuery("text", "alpha", "beta"),
        PrefixQuery(Term("title", "book")),
        BooleanQuery.Builder()
            .add(TermQuery(Term("text", "alpha")), BooleanClause.Occur.MUST)
            .add(org.apache.lucene.document.IntPoint.newSetQuery("book_id", 1, 3), BooleanClause.Occur.FILTER)
            .build(),
        KnnFloatVectorQuery("vec", LuceneLifecycleFixture.vector(9), 10),
        KnnFloatVectorQuery("vec", LuceneLifecycleFixture.vector(9), 10,
            org.apache.lucene.document.IntPoint.newExactQuery("is_base_book", 1)),
    )

    private fun results(searcher: IndexSearcher, query: Query): List<Pair<Int, Float>> =
        searcher.search(query, 20).scoreDocs.map { it.doc to it.score }

    private inline fun withIndex(block: (Path) -> Unit) {
        val path = Files.createTempDirectory("managed-searchers")
        try {
            LuceneLifecycleFixture.create(path)
            block(path)
        } finally {
            LuceneLifecycleFixture.delete(path)
        }
    }
}
