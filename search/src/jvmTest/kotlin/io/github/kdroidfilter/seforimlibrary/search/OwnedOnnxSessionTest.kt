package io.github.kdroidfilter.seforimlibrary.search

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

class OwnedOnnxSessionTest {
    private class Resource(val closeAction: () -> Unit = {}) : AutoCloseable {
        var closes = 0
        override fun close() {
            closes++
            closeAction()
        }
    }

    private fun owner(
        session: Resource,
        options: Resource,
        serial: Boolean = false,
    ) = OwnedOnnxSession.open({ options }, {}, { session }, serial)

    @Test
    fun retainsOptionsUntilSessionClosesAndCloseIsIdempotent() {
        val events = mutableListOf<String>()
        val options = Resource { events += "options" }
        val session = Resource { events += "session" }
        val owned = owner(session, options)
        owned.useSession { assertSame(session, it) }
        assertTrue(events.isEmpty())
        owned.close()
        owned.close()
        assertEquals(listOf("session", "options"), events)
        assertFailsWith<IllegalStateException> { owned.useSession { } }
    }

    @Test
    fun optionsAreReleasedWhenProviderConfigurationFails() {
        val options = Resource()
        val failure = IllegalStateException("provider unavailable")
        val thrown = assertFailsWith<IllegalStateException> {
            OwnedOnnxSession.open(
                createOptions = { options },
                configure = { throw failure },
                createSession = { error("configuration failed; must not create a session") },
            )
        }
        assertSame(failure, thrown)
        assertEquals(1, options.closes)
    }

    @Test
    fun optionsAreReleasedWhenModelLoadingFails() {
        val options = Resource()
        val failure = IllegalArgumentException("invalid model")
        val thrown = assertFailsWith<IllegalArgumentException> {
            OwnedOnnxSession.open({ options }, {}, { throw failure })
        }
        assertSame(failure, thrown)
        assertEquals(1, options.closes)
    }

    @Test
    fun failedRollbackDoesNotHideTheLoadFailure() {
        val cleanupFailure = IllegalStateException("options close failed")
        val options = Resource { throw cleanupFailure }
        val loadFailure = IllegalArgumentException("invalid model")
        val thrown = assertFailsWith<IllegalArgumentException> {
            OwnedOnnxSession.open({ options }, {}, { throw loadFailure })
        }
        assertSame(loadFailure, thrown)
        assertEquals(listOf(cleanupFailure), thrown.suppressed.toList())
    }

    @Test
    fun failedSessionCloseRetainsOptionsAndCanBeRetried() {
        var fail = true
        val options = Resource()
        val session = Resource { if (fail) error("session close failed") }
        val owned = owner(session, options)
        assertFailsWith<IllegalStateException> { owned.close() }
        assertEquals(0, options.closes)
        assertFailsWith<IllegalStateException> { owned.useSession { } }
        fail = false
        owned.close()
        owned.close()
        assertEquals(2, session.closes)
        assertEquals(1, options.closes)
    }

    @Test
    fun failedOptionsCloseDoesNotCloseSessionTwiceOnRetry() {
        var fail = true
        val options = Resource { if (fail) error("options close failed") }
        val session = Resource()
        val owned = owner(session, options)
        assertFailsWith<IllegalStateException> { owned.close() }
        fail = false
        owned.close()
        assertEquals(1, session.closes)
        assertEquals(2, options.closes)
    }

    @Test
    fun closeWaitsForInferenceAndReleasesInOrder() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val closeStarted = CountDownLatch(1)
        val active = AtomicInteger()
        val events = mutableListOf<String>()
        val session = Resource {
            assertEquals(0, active.get(), "native session must not close during inference")
            events += "session"
        }
        val options = Resource { events += "options" }
        val owned = owner(session, options)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val inference = pool.submit {
                owned.useSession {
                    active.incrementAndGet()
                    entered.countDown()
                    try {
                        assertTrue(release.await(10, TimeUnit.SECONDS))
                    } finally {
                        active.decrementAndGet()
                    }
                }
            }
            assertTrue(entered.await(10, TimeUnit.SECONDS))
            val closing = pool.submit {
                closeStarted.countDown()
                owned.close()
            }
            assertTrue(closeStarted.await(10, TimeUnit.SECONDS))
            assertFalse(closing.isDone)
            release.countDown()
            inference.get(10, TimeUnit.SECONDS)
            closing.get(10, TimeUnit.SECONDS)
            assertEquals(listOf("session", "options"), events)
            assertFailsWith<IllegalStateException> { owned.useSession { } }
        } finally {
            release.countDown()
            pool.shutdownNow()
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))
            owned.close()
        }
    }

    @Test
    fun cpuSessionsStillAllowConcurrentInference() {
        val entered = CountDownLatch(2)
        val release = CountDownLatch(1)
        val owned = owner(Resource(), Resource())
        val pool = Executors.newFixedThreadPool(2)
        try {
            val runs = List(2) {
                pool.submit {
                    owned.useSession {
                        entered.countDown()
                        assertTrue(release.await(10, TimeUnit.SECONDS))
                    }
                }
            }
            assertTrue(entered.await(10, TimeUnit.SECONDS), "CPU inference must not be serialized")
            release.countDown()
            runs.forEach { it.get(10, TimeUnit.SECONDS) }
        } finally {
            release.countDown()
            pool.shutdownNow()
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))
            owned.close()
        }
    }

    @Test
    fun directMlSessionsSerializeInference() {
        val active = AtomicInteger()
        val maximum = AtomicInteger()
        val owned = owner(Resource(), Resource(), serial = true)
        val pool = Executors.newFixedThreadPool(4)
        try {
            val runs = List(4) {
                pool.submit {
                    repeat(100) {
                        owned.useSession {
                            val current = active.incrementAndGet()
                            maximum.accumulateAndGet(current, ::maxOf)
                            Thread.yield()
                            active.decrementAndGet()
                        }
                    }
                }
            }
            runs.forEach { it.get(10, TimeUnit.SECONDS) }
            assertEquals(1, maximum.get())
        } finally {
            pool.shutdownNow()
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))
            owned.close()
        }
    }

    @Test
    fun failedInferenceStillReleasesItsLifetimeLease() {
        val session = Resource()
        val options = Resource()
        val owned = owner(session, options)
        assertFailsWith<IllegalArgumentException> {
            owned.useSession { throw IllegalArgumentException("bad tensor") }
        }
        owned.close()
        assertEquals(1, session.closes)
        assertEquals(1, options.closes)
    }
}
