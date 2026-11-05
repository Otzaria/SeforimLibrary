package io.github.kdroidfilter.seforimlibrary.search

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.io.Closeable

/**
 * Owns the lazily created embedder/searcher pair. Construction and disposal stay outside
 * the publication monitor so shutdown can reject an initializer without waiting for it.
 */
internal class LazyDenseResources<E : AutoCloseable, V : AutoCloseable>(
    private val loadEmbedder: () -> E?,
    private val loadSearcher: () -> V?,
) : Closeable {
    internal data class Resources<E, V>(val embedder: E, val searcher: V)

    private val initialization = Mutex()
    private val publication = Any()
    @Volatile private var tried = false
    @Volatile private var closed = false
    @Volatile private var resources: Resources<E, V>? = null

    val current: Resources<E, V>? get() = resources
    val isClosed: Boolean get() = closed

    /** Returns true only when this call completes the first initialization attempt. */
    suspend fun ensureLoaded(): Boolean {
        if (tried || closed) return false
        return initialization.withLock {
            if (tried || closed) return@withLock false
            withContext(Dispatchers.IO) {
                var embedder: E? = null
                var searcher: V? = null
                var transferred = false
                var failure: Throwable? = null
                try {
                    currentCoroutineContext().ensureActive()
                    if (closed) return@withContext false
                    embedder = loadEmbedder()
                    currentCoroutineContext().ensureActive()
                    if (embedder != null && !closed) searcher = loadSearcher()
                    currentCoroutineContext().ensureActive()
                    val candidate = if (embedder != null && searcher != null) Resources(embedder, searcher) else null
                    synchronized(publication) {
                        if (closed) return@synchronized false
                        if (candidate != null) {
                            resources = candidate
                            transferred = true
                        }
                        tried = true
                        true
                    }
                } catch (cause: Throwable) {
                    failure = cause
                    throw cause
                } finally {
                    // Publish inside the IO block: prompt cancellation when withContext
                    // returns must never strand a pair with neither an owner nor cleanup.
                    if (!transferred) dispose(embedder, searcher, failure)
                }
            }
        }
    }

    override fun close() {
        val detached = synchronized(publication) {
            if (closed) return
            closed = true
            tried = true
            resources.also { resources = null }
        }
        dispose(detached?.embedder, detached?.searcher)
    }

    private fun dispose(embedder: E?, searcher: V?, originalFailure: Throwable? = null) {
        var failure = originalFailure
        try {
            searcher?.close()
        } catch (cause: Throwable) {
            if (failure == null) failure = cause
            else if (cause !== failure) failure.addSuppressed(cause)
        }
        try {
            embedder?.close()
        } catch (cause: Throwable) {
            if (failure == null) failure = cause
            else if (cause !== failure) failure.addSuppressed(cause)
        }
        if (originalFailure == null && failure != null) throw failure
    }
}
