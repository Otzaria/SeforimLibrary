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
        val embedder = Resource { throw cleanupFailure }
        val loadFailure = IllegalStateException("vector index unavailable")
        val owner = LazyDenseResources<Resource, Resource>({ embedder }, { throw loadFailure })
        try {
            val thrown = assertFailsWith<IllegalStateException> { owner.ensureLoaded() }
            assertSame(loadFailure, thrown)
            assertEquals(listOf(cleanupFailure), thrown.suppressed.toList())
            assertEquals(1, embedder.closes.get())
            assertNull(owner.current)
        } finally {
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
        val searcher = Resource {
            entered.countDown()
            assertTrue(release.await(10, TimeUnit.SECONDS))
            throw closeFailure
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
        } finally {
            release.countDown()
            pool.shutdownNow()
            owner.close()
        }
    }
}
