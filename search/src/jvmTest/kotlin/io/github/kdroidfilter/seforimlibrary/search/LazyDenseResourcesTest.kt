package io.github.kdroidfilter.seforimlibrary.search

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class LazyDenseResourcesTest {
    private class Resource(private val closeAction: () -> Unit = {}) : AutoCloseable {
        val closes = AtomicInteger()
        override fun close() {
            closes.incrementAndGet()
            closeAction()
        }
    }

    @Test
    fun initializationPublishesOnePairAndShutdownClosesItOnce() = runBlocking {
        val embedder = Resource()
        val searcher = Resource()
        val loads = AtomicInteger()
        val owner = LazyDenseResources({ loads.incrementAndGet(); embedder }, { searcher })
        try {
            val attempts = List(8) { async(Dispatchers.Default) { owner.ensureLoaded() } }
            assertEquals(1, attempts.count { it.await() })
            assertEquals(1, loads.get())
            assertSame(embedder, owner.current?.embedder)
            assertSame(searcher, owner.current?.searcher)
        } finally {
            owner.close()
        }
        owner.close()
        assertTrue(owner.isClosed)
        assertNull(owner.current)
        assertFalse(owner.ensureLoaded())
        assertEquals(1, embedder.closes.get())
        assertEquals(1, searcher.closes.get())
    }

    @Test
    fun closeBeforeLoadingDoesNotCallEitherFactory() = runBlocking {
        val owner = LazyDenseResources<Resource, Resource>(
            { error("embedder factory must not run after close") },
            { error("searcher factory must not run after close") },
        )
        owner.close()
        assertFalse(owner.ensureLoaded())
        assertNull(owner.current)
    }

    @Test
    fun closeDuringLoadingRejectsAndClosesBothLateResources() = runBlocking {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val embedder = Resource()
        val searcher = Resource()
        val owner = LazyDenseResources({ embedder }, {
            entered.countDown()
            assertTrue(release.await(10, TimeUnit.SECONDS))
            searcher
        })
        val closer = Executors.newSingleThreadExecutor()
        val loading = async(Dispatchers.Default) { owner.ensureLoaded() }
        try {
            assertTrue(entered.await(10, TimeUnit.SECONDS))
            // Shutdown must finish while the synchronous/native constructor is blocked.
            closer.submit { owner.close() }.get(10, TimeUnit.SECONDS)
            assertTrue(owner.isClosed)
            assertNull(owner.current)
            release.countDown()
            assertFalse(withTimeout(10_000) { loading.await() })
            assertEquals(1, embedder.closes.get())
            assertEquals(1, searcher.closes.get())
            assertFalse(owner.ensureLoaded())
        } finally {
            release.countDown()
            loading.cancelAndJoin()
            owner.close()
            closer.shutdownNow()
        }
    }

    @Test
    fun cancellationAfterEmbedderConstructionClosesItBeforeSearcherLoads() = runBlocking {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val embedder = Resource()
        val owner = LazyDenseResources<Resource, Resource>({
            entered.countDown()
            assertTrue(release.await(10, TimeUnit.SECONDS))
            embedder
        }, { error("cancelled initialization must not start a second native load") })
        val loading = launch(Dispatchers.Default) { owner.ensureLoaded() }
        try {
            assertTrue(entered.await(10, TimeUnit.SECONDS))
            loading.cancel()
            release.countDown()
            withTimeout(10_000) { loading.join() }
            assertNull(owner.current)
            assertEquals(1, embedder.closes.get())
        } finally {
            release.countDown()
            loading.cancelAndJoin()
            owner.close()
        }
    }

    @Test
    fun cancellationAfterSearcherConstructionClosesBothWithoutPublishing() = runBlocking {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val embedder = Resource()
        val searcher = Resource()
        val owner = LazyDenseResources({ embedder }, {
            entered.countDown()
            assertTrue(release.await(10, TimeUnit.SECONDS))
            searcher
        })
        val loading = launch(Dispatchers.Default) { owner.ensureLoaded() }
        try {
            assertTrue(entered.await(10, TimeUnit.SECONDS))
            loading.cancel()
            release.countDown()
            withTimeout(10_000) { loading.join() }
            assertNull(owner.current)
            assertEquals(1, embedder.closes.get())
            assertEquals(1, searcher.closes.get())
        } finally {
            release.countDown()
            loading.cancelAndJoin()
            owner.close()
        }
    }

    @Test
    fun failedSearcherConstructionCleansEmbedderAndPreservesFailure() = runBlocking {
        val cleanupFailure = IllegalArgumentException("embedder close failed")
        var failClose = true
        val embedder = Resource { if (failClose) throw cleanupFailure }
        val loadFailure = IllegalStateException("vector index unavailable")
        val owner = LazyDenseResources<Resource, Resource>({ embedder }, { throw loadFailure })
        try {
            val thrown = assertFailsWith<IllegalStateException> { owner.ensureLoaded() }
            // Coroutine stacktrace recovery can copy the exception at withContext;
            // the original failure and its suppressed cleanup must remain reachable.
            assertTrue(generateSequence<Throwable>(thrown) { it.cause }.any { it === loadFailure })
            assertEquals(listOf(cleanupFailure), loadFailure.suppressed.toList())
            assertEquals(1, embedder.closes.get())
            assertNull(owner.current)
        } finally {
            failClose = false
            owner.close()
        }
    }

    @Test
    fun missingSearcherClosesEmbedderAndCompletesTheAttempt() = runBlocking {
        val embedder = Resource()
        val loads = AtomicInteger()
        val owner = LazyDenseResources<Resource, Resource>({ loads.incrementAndGet(); embedder }, { null })
        try {
            assertTrue(owner.ensureLoaded())
            assertFalse(owner.ensureLoaded())
            assertEquals(1, loads.get())
            assertEquals(1, embedder.closes.get())
            assertNull(owner.current)
        } finally {
            owner.close()
        }
    }

    @Test
    fun nativeCloseRunsOutsidePublicationMonitorAndStillAttemptsBothResources() = runBlocking {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val embedder = Resource()
        val closeFailure = IllegalStateException("searcher close failed")
        val attempts = AtomicInteger()
        val searcher = Resource {
            if (attempts.incrementAndGet() == 1) {
                entered.countDown()
                assertTrue(release.await(10, TimeUnit.SECONDS))
                throw closeFailure
            }
        }
        val owner = LazyDenseResources({ embedder }, { searcher })
        assertTrue(owner.ensureLoaded())
        val pool = Executors.newFixedThreadPool(2)
        try {
            val closing = pool.submit<Throwable?> {
                try { owner.close(); null } catch (failure: Throwable) { failure }
            }
            assertTrue(entered.await(10, TimeUnit.SECONDS))
            assertTrue(owner.isClosed)
            assertNull(owner.current)
            // A second close needs the publication monitor. It must not wait for native close.
            pool.submit { owner.close() }.get(10, TimeUnit.SECONDS)
            release.countDown()
            assertSame(closeFailure, closing.get(10, TimeUnit.SECONDS))
            assertEquals(1, embedder.closes.get())
            assertEquals(1, searcher.closes.get())
            owner.close()
            owner.close()
            assertEquals(1, embedder.closes.get(), "successful cleanup must not be repeated")
            assertEquals(2, searcher.closes.get(), "only failed cleanup must be retried")
        } finally {
            release.countDown()
            pool.shutdownNow()
            owner.close()
        }
    }

    @Test
    fun retryAfterSessionCloseFailureReleasesOptionsWithoutRepublishing() = runBlocking {
        var fail = true
        val session = Resource { if (fail) error("session close failed") }
        val options = Resource()
        val searcher = Resource()
        val owned = OwnedOnnxSession.open({ options }, {}, { session })
        val owner = LazyDenseResources({ owned }, { searcher })
        owner.ensureLoaded()
        assertFailsWith<IllegalStateException> { owner.close() }
        assertTrue(owner.isClosed)
        assertNull(owner.current)
        assertFalse(owner.ensureLoaded())
        assertEquals(0, options.closes.get())
        fail = false
        owner.close()
        owner.close()
        assertEquals(2, session.closes.get())
        assertEquals(1, options.closes.get())
        assertEquals(1, searcher.closes.get())
    }

    @Test
    fun retryAfterOptionsCloseFailureDoesNotCloseSessionOrSearcherAgain() = runBlocking {
        var fail = true
        val session = Resource()
        val options = Resource { if (fail) error("options close failed") }
        val searcher = Resource()
        val owner = LazyDenseResources(
            { OwnedOnnxSession.open({ options }, {}, { session }) }, { searcher },
        )
        owner.ensureLoaded()
        assertFailsWith<IllegalStateException> { owner.close() }
        fail = false
        owner.close()
        owner.close()
        assertEquals(1, session.closes.get())
        assertEquals(2, options.closes.get())
        assertEquals(1, searcher.closes.get())
    }

    @Test
    fun repeatedCleanupFailuresRetainBothResourcesAndPreserveSuppressedFailure() = runBlocking {
        var fail = true
        val embedderFailure = IllegalStateException("embedder close failed")
        val searcherFailure = IllegalArgumentException("searcher close failed")
        val embedder = Resource { if (fail) throw embedderFailure }
        val searcher = Resource { if (fail) throw searcherFailure }
        val owner = LazyDenseResources({ embedder }, { searcher })
        owner.ensureLoaded()
        repeat(2) {
            assertSame(searcherFailure, assertFailsWith<IllegalArgumentException> { owner.close() })
        }
        assertTrue(searcherFailure.suppressed.all { it === embedderFailure })
        assertEquals(2, searcherFailure.suppressed.size)
        fail = false
        owner.close()
        owner.close()
        assertEquals(3, embedder.closes.get())
        assertEquals(3, searcher.closes.get())
        assertNull(owner.current)
    }

    @Test
    fun failedInitializationRollbackRemainsOwnedForShutdownRetry() = runBlocking {
        var failClose = true
        val cleanupFailure = IllegalArgumentException("embedder close failed")
        val embedder = Resource { if (failClose) throw cleanupFailure }
        val loadFailure = IllegalStateException("searcher load failed")
        val owner = LazyDenseResources<Resource, Resource>({ embedder }, { throw loadFailure })
        val thrown = assertFailsWith<IllegalStateException> { owner.ensureLoaded() }
        assertTrue(generateSequence<Throwable>(thrown) { it.cause }.any { it === loadFailure })
        assertEquals(listOf(cleanupFailure), loadFailure.suppressed.toList())
        failClose = false
        owner.close()
        owner.close()
        assertEquals(2, embedder.closes.get())
        assertNull(owner.current)
    }

    @Test
    fun cancelledInitializationCleanupFailureCanBeRetriedAfterShutdown() = runBlocking {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        var failClose = true
        val embedder = Resource { if (failClose) error("embedder close failed") }
        val owner = LazyDenseResources<Resource, Resource>({
            entered.countDown()
            assertTrue(release.await(10, TimeUnit.SECONDS))
            embedder
        }, { error("cancelled initialization must not load the searcher") })
        val loading = launch(Dispatchers.Default) { owner.ensureLoaded() }
        try {
            assertTrue(entered.await(10, TimeUnit.SECONDS))
            loading.cancel()
            owner.close()
            release.countDown()
            withTimeout(10_000) { loading.join() }
            assertEquals(1, embedder.closes.get())
            failClose = false
            owner.close()
            owner.close()
            assertEquals(2, embedder.closes.get())
            assertNull(owner.current)
        } finally {
            release.countDown()
            loading.cancelAndJoin()
            failClose = false
            owner.close()
        }
    }

    @Test
    fun lateInitializationCleanupFailureCanBeRetriedAfterShutdown() = runBlocking {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        var failClose = true
        val embedder = Resource { if (failClose) error("embedder close failed") }
        val searcher = Resource()
        val owner = LazyDenseResources({ embedder }, {
            entered.countDown()
            assertTrue(release.await(10, TimeUnit.SECONDS))
            searcher
        })
        // Catch locally so the expected failure does not cancel the test's parent.
        val loading = async(Dispatchers.Default) { runCatching { owner.ensureLoaded() } }
        try {
            assertTrue(entered.await(10, TimeUnit.SECONDS))
            owner.close()
            release.countDown()
            assertTrue(withTimeout(10_000) { loading.await() }.isFailure)
            failClose = false
            owner.close()
            owner.close()
            assertEquals(2, embedder.closes.get())
            assertEquals(1, searcher.closes.get())
            assertNull(owner.current)
            assertFalse(owner.ensureLoaded())
        } finally {
            release.countDown()
            loading.cancelAndJoin()
            failClose = false
            owner.close()
        }
    }
}
