package io.github.kdroidfilter.seforimlibrary.common.ids

import java.security.MessageDigest

/**
 * Transition shim. Builds up to and including db v27 keyed a Sefaria line on
 * its heRef when it had one and on its rendered content otherwise, so both an
 * heRef edit and a generated-prefix shift renumbered the book (issue #1211).
 *
 * The key is the raw segment now, so the fallback is tried for EVERY line, not
 * only ref-bearing ones. A seed build_state written by an older build
 * still holds the heRef-based keys, so [InMemoryIdAllocator.lineId] falls back
 * to this key once and re-registers the id under the new one — the snapshot it
 * writes is fully migrated. Delete this file (and the `legacy` parameter on
 * [IdAllocator.lineId]) once no build_state in circulation predates the change.
 */
class LegacyLineKey(val contentHash: ByteArray, val occurrenceIdx: Int) {

    companion object {
        /** The pre-#1211 natural-key hash: heRef when present, content otherwise. */
        fun hash(content: String, heRef: String?): ByteArray {
            val prefixed = if (heRef != null) "REF:$heRef" else "CT:$content"
            return MessageDigest.getInstance("SHA-1").digest(prefixed.toByteArray(Charsets.UTF_8))
        }
    }
}
