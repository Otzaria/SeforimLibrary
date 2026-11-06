package io.github.kdroidfilter.seforimlibrary.search

import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.SearcherFactory
import org.apache.lucene.search.SearcherManager
import org.apache.lucene.store.AlreadyClosedException
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.NIOFSDirectory
import java.io.Closeable
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Lazily owns the directory and a manager of committed-index snapshots.
 *
 * New requests refresh before acquiring, preserving the freshness of the former
 * per-request DirectoryReader.open. A cursor keeps its lease across pages, so a
 * refresh cannot change document ids underneath searchAfter. Query execution is
 * never under [lifecycleLock]; the Lucene manager coordinates concurrent refreshes.
 */
internal class ManagedIndexSearchers(
    private val indexDir: Path,
    private val directoryFactory: (Path) -> Directory = ::openSearchDirectory,
    private val searcherFactory: SearcherFactory? = null,
) : Closeable {
    private val lifecycleLock = Any()
    private var resources: Resources? = null
    private var closed = false

    /** A lease must outlive both searching and retrieving stored fields. */
    fun acquire(): Lease {
        val current = synchronized(lifecycleLock) {
            if (closed) throw AlreadyClosedException("Search engine is closed")
            (resources ?: openResources().also { resources = it }).also { it.users++ }
        }
        var acquired: IndexSearcher? = null
        try {
            // maybeRefresh (non-blocking) could return an old snapshot when a
            // concurrent refresh is in progress, unlike DirectoryReader.open.
            current.manager.maybeRefreshBlocking()
            val searcher = current.manager.acquire().also { acquired = it }
            return Lease(searcher) {
                // Drop the reader reference before possibly closing its directory.
                try {
                    current.manager.release(searcher)
                } catch (failure: Throwable) {
                    try {
                        releaseResources(current)
                    } catch (cleanupFailure: Throwable) {
                        failure.addSuppressed(cleanupFailure)
                    }
                    throw failure
                }
                releaseResources(current)
            }
        } catch (failure: Throwable) {
            if (acquired != null) {
                try {
                    current.manager.release(acquired)
                } catch (cleanupFailure: Throwable) {
                    failure.addSuppressed(cleanupFailure)
                }
            }
            try {
                releaseResources(current)
            } catch (cleanupFailure: Throwable) {
                failure.addSuppressed(cleanupFailure)
            }
            throw failure
        }
    }

    /** Proactively refresh after a commit, before the next user query. */
    fun refresh() {
        acquire().close()
    }

    private fun openResources(): Resources {
        val directory = directoryFactory(indexDir)
        try {
            return Resources(directory, SearcherManager(directory, searcherFactory))
        } catch (failure: Throwable) {
            try {
                directory.close()
            } catch (cleanupFailure: Throwable) {
                failure.addSuppressed(cleanupFailure)
            }
            throw failure
        }
    }

    private fun releaseResources(current: Resources) {
        val toClose = synchronized(lifecycleLock) {
            current.users--
            if (closed && current.users == 0) {
                resources = null
                current
            } else {
                null
            }
        }
        toClose?.close()
    }

    /**
     * Reject new requests without interrupting an acquired searcher. Actual
     * resources close after the last request/session releases its lease. Closing
     * an unused owner does not open a directory or create an index folder.
     */
    override fun close() {
        val toClose = synchronized(lifecycleLock) {
            if (closed) return
            closed = true
            resources?.takeIf { it.users == 0 }?.also { resources = null }
        }
        toClose?.close()
    }

    internal class Lease(
        val searcher: IndexSearcher,
        private val release: () -> Unit,
    ) : Closeable {
        private val closed = AtomicBoolean()

        fun ensureOpen() {
            if (closed.get()) throw AlreadyClosedException("Search session is closed")
        }

        override fun close() {
            if (closed.compareAndSet(false, true)) release()
        }
    }

    private class Resources(
        val directory: Directory,
        val manager: SearcherManager,
        var users: Int = 0,
    ) : Closeable {
        override fun close() {
            // use preserves a manager failure and suppresses any directory failure.
            directory.use { manager.close() }
        }
    }
}

private fun openSearchDirectory(path: Path): Directory =
    // GraalVM native images cannot use MMapDirectory's Panama downcalls.
    if (System.getProperty("org.graalvm.nativeimage.imagecode") != null) {
        NIOFSDirectory(path)
    } else {
        FSDirectory.open(path)
    }
