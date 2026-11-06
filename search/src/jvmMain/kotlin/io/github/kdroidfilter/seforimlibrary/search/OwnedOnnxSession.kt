package io.github.kdroidfilter.seforimlibrary.search

import java.io.Closeable
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write

/**
 * Owns an ONNX session together with the options that must outlive it. The type parameters
 * allow failure/ordering tests without a model or a particular native execution provider.
 *
 * [useSession] holds the lifetime of every per-call resource, including the tokenizer and
 * input/output tensors. Closing rejects new calls and waits for calls already in progress.
 */
internal class OwnedOnnxSession<S : AutoCloseable, O : AutoCloseable> private constructor(
    private val session: S,
    private val options: O,
    private val serializeUse: Boolean,
) : Closeable {
    private val lifetime = ReentrantReadWriteLock()
    private val execution = ReentrantLock()
    private var closing = false
    private var sessionClosed = false
    private var optionsClosed = false

    fun <T> useSession(block: (S) -> T): T {
        // DirectML does not allow concurrent Run calls on the same session. Take this
        // lock before the lifetime lease so queued callers cannot delay close().
        return if (serializeUse) execution.withLock { withLease(block) } else withLease(block)
    }

    private fun <T> withLease(block: (S) -> T): T = lifetime.read {
        check(!closing) { "Embedding session is closed" }
        block(session)
    }

    override fun close() = lifetime.write {
        closing = true
        if (!sessionClosed) {
            session.close()
            sessionClosed = true
        }
        // If session.close() failed, retain its options: releasing them while a native
        // session may still be alive violates ORT's contract. A later close can retry.
        if (!optionsClosed) {
            options.close()
            optionsClosed = true
        }
    }

    companion object {
        fun <S : AutoCloseable, O : AutoCloseable> open(
            createOptions: () -> O,
            configure: (O) -> Unit,
            createSession: (O) -> S,
            serializeUse: Boolean = false,
        ): OwnedOnnxSession<S, O> {
            val options = createOptions()
            var session: S? = null
            try {
                configure(options)
                session = createSession(options)
                return OwnedOnnxSession(session, options, serializeUse)
            } catch (failure: Throwable) {
                if (session == null) {
                    closeAfterFailure(options, failure)
                } else {
                    // Also cover a failure while constructing the owner after native
                    // session creation. Do not release options if session cleanup fails.
                    try {
                        session.close()
                        closeAfterFailure(options, failure)
                    } catch (cleanupFailure: Throwable) {
                        if (cleanupFailure !== failure) failure.addSuppressed(cleanupFailure)
                    }
                }
                throw failure
            }
        }
    }
}

/** Roll back an owned resource without hiding the failure that caused the rollback. */
internal fun closeAfterFailure(resource: AutoCloseable, failure: Throwable) {
    try {
        resource.close()
    } catch (cleanupFailure: Throwable) {
        if (cleanupFailure !== failure) failure.addSuppressed(cleanupFailure)
    }
}
