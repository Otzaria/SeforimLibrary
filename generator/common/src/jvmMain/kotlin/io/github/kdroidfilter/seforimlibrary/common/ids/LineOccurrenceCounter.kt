package io.github.kdroidfilter.seforimlibrary.common.ids

import java.util.concurrent.ConcurrentHashMap

/**
 * Per-(bookId, contentHash) counter that gives duplicate lines inside one book
 * distinct occurrence indices, so their natural keys — and thus ids — differ.
 *
 * One instance per key scheme per build: the index only means anything relative
 * to the hash it counts.
 */
class LineOccurrenceCounter {

    private val byBook = ConcurrentHashMap<Long, ConcurrentHashMap<Long, Int>>()

    fun next(bookId: Long, contentHash: ByteArray): Int {
        // Lossy 64-bit fold: collisions inside a single book are vanishingly
        // rare and only ever cost an occurrence index, never correctness.
        val hashKey = contentHash.fold(0L) { acc, b -> (acc shl 5) - acc + b.toLong() }
        val map = byBook.computeIfAbsent(bookId) { ConcurrentHashMap() }
        return map.compute(hashKey) { _, v -> (v ?: -1) + 1 }!!
    }
}
