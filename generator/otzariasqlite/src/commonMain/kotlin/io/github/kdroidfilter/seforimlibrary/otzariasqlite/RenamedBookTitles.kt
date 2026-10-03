package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.changes.BookRenameDetector
import io.github.kdroidfilter.seforimlibrary.core.text.HebrewTextUtils

/** A book as the finished DB holds it. */
internal data class LibraryBook(
    val id: Long,
    val title: String,
    val sourceName: String,
)

/** An earlier title of a book, and what showed it is the same book. */
internal data class FormerTitle(val title: String, val evidence: String)

/** A rename that was found but not followed, and why. */
internal data class RefusedRename(val oldTitle: String, val newTitle: String, val reason: String)

internal data class FormerTitles(
    val byBookId: Map<Long, List<FormerTitle>>,
    val refused: List<RefusedRename>,
)

/**
 * Finds the earlier titles of books, so that data keyed by title (the Acronymizer's
 * acronyms) can follow a rename instead of being lost with the old name.
 *
 * The build state never forgets a book key, so a renamed book leaves its old
 * title behind in one of two shapes:
 *
 *  - **same id**: the rename happened after the id was allocated
 *    (ForDB/book_renames.csv renames Sefaria books in place), so the old key and
 *    the book share one id.
 *  - **same content**: the file was renamed (the id follows the title), so the old
 *    key holds a dead id. It is matched to a live book of the same source whose
 *    line hashes overlap it by at least [threshold] (Jaccard, as in
 *    [BookRenameDetector]), and only when that live book is its single match.
 *
 * Either way the rename is followed only when it kept the name and just added or
 * dropped words, or respelled it ([isSameWorkTitle]): 'אמת ואמונה - מנחם מנדל מקוצק'
 * → 'אמת ואמונה' is followed; 'פאר הדור תשובות הרמב"ם' → 'תשובות הרמב"ם - מהדורת
 * בלאו' replaces words, which is how a corrected identification looks, and is not.
 * An old title that is now another book's title, or that two books would inherit,
 * is not followed either: it would hand one book's names to another.
 */
internal class RenamedBookTitles(private val threshold: Double = BookRenameDetector.DEFAULT_THRESHOLD) {

    /**
     * @param books every book in the DB.
     * @param targets ids of the books that need an earlier title (the only ones looked up).
     * @param knownKeys every book key the build state knows, with its id.
     * @param lineHashes the line content hashes of the given book ids.
     */
    fun resolve(
        books: List<LibraryBook>,
        targets: Set<Long>,
        knownKeys: Map<BookKey, Long>,
        lineHashes: (Set<Long>) -> Map<Long, List<ByteArray>>,
    ): FormerTitles {
        if (targets.isEmpty() || knownKeys.isEmpty()) return FormerTitles(emptyMap(), emptyList())
        val byId = books.associateBy { it.id }
        val found = ArrayList<Pair<Long, FormerTitle>>()
        val refused = ArrayList<RefusedRename>()

        // Same id: the key still names the book under its old title.
        for ((key, id) in knownKeys) {
            val book = byId[id] ?: continue
            if (id !in targets || key.canonicalHeTitle == book.title) continue
            found += id to FormerTitle(key.canonicalHeTitle, "same id")
        }

        // Same content: a dead key whose last lines one live book of its source now holds.
        // Every live book of the source competes, not only the targets: a dead key that
        // also matches a book with acronyms of its own has no single heir.
        val deadBySource = knownKeys.entries
            .filter { it.value !in byId }
            .groupBy({ it.key.sourceName }, { it.key to it.value })
        val targetSources = targets.mapNotNullTo(HashSet()) { byId[it]?.sourceName }
        val sources = deadBySource.keys.intersect(targetSources)
        if (sources.isNotEmpty()) {
            val deadIds = sources.flatMap { s -> deadBySource.getValue(s).map { it.second } }.toSet()
            val deadHashes = hashSets(lineHashes(deadIds))
            // Every book of these sources is compared by its distinct lines, as the dead key
            // is. Its line count is no stand-in: repeated lines count there too, so 20 lines
            // written twice are 40 lines but the same 20 distinct ones. A book left out here
            // is also a rival left out, and a dead key with two heirs would seem to have one.
            val candidates = books.filter { it.sourceName in sources }
            val liveRaw = lineHashes(candidates.mapTo(HashSet()) { it.id })
            val matches = HashMap<Long, MutableList<LibraryBook>>()
            for (book in candidates) {
                // One live set at a time: the candidates can hold millions of lines.
                val live = liveRaw[book.id]?.takeIf { it.isNotEmpty() }?.let { raw ->
                    BookRenameDetector.LineHashSet(raw.mapTo(HashSet()) { BookRenameDetector.ContentHash(it) })
                } ?: continue
                for ((_, deadId) in deadBySource.getValue(book.sourceName)) {
                    val old = deadHashes[deadId] ?: continue
                    if (sizeCompatible(old.size, live.size) && old.jaccard(live) >= threshold) {
                        matches.getOrPut(deadId) { ArrayList() } += book
                    }
                }
            }
            for (source in sources) {
                for ((key, deadId) in deadBySource.getValue(source)) {
                    val heirs = matches[deadId] ?: continue
                    when {
                        heirs.size > 1 -> refused += RefusedRename(
                            key.canonicalHeTitle,
                            heirs.joinToString(" | ") { it.title },
                            "its content matches ${heirs.size} books",
                        )
                        heirs[0].id in targets ->
                            found += heirs[0].id to FormerTitle(key.canonicalHeTitle, "same content")
                    }
                }
            }
        }

        // Guards, over the pooled candidates.
        val liveTitles = books.groupBy({ normalizedTitle(it.title) }, { it.id })
        val claims = found.groupBy({ normalizedTitle(it.second.title) }, { it.first })
        val accepted = LinkedHashMap<Long, MutableList<FormerTitle>>()
        for ((bookId, former) in found) {
            val book = byId.getValue(bookId)
            val oldKey = normalizedTitle(former.title)
            val reason = when {
                oldKey == normalizedTitle(book.title) -> null
                !isSameWorkTitle(former.title, book.title) -> "the title changed words, not only added or dropped them"
                liveTitles[oldKey].orEmpty().any { it != bookId } -> "the old title is another book's title now"
                claims.getValue(oldKey).toSet().size > 1 -> "two books have this old title"
                else -> null
            }
            if (reason != null) {
                refused += RefusedRename(former.title, book.title, reason)
                continue
            }
            val list = accepted.getOrPut(bookId) { ArrayList() }
            if (list.none { it.title == former.title }) list += former
        }
        return FormerTitles(accepted, refused)
    }

    private fun hashSets(raw: Map<Long, List<ByteArray>>): Map<Long, BookRenameDetector.LineHashSet> =
        raw.filterValues { it.isNotEmpty() }.mapValues { (_, hashes) ->
            BookRenameDetector.LineHashSet(hashes.mapTo(HashSet()) { BookRenameDetector.ContentHash(it) })
        }

    /** A Jaccard of t needs the smaller set to be at least t of the larger one. */
    private fun sizeCompatible(a: Int, b: Int): Boolean {
        if (a <= 0 || b <= 0) return false
        return minOf(a, b).toDouble() / maxOf(a, b) >= threshold
    }

    companion object {
        // Honorifics come and go in renames ("ר' סעדיה גאון" → "רבינו סעדיה גאון").
        private val HONORIFICS = setOf("ר", "רב", "רבי", "רבינו", "רבנו", "הרב", "מרן")
        private val QUOTES = Regex("[\"'`״׳“”‘’]")
        private val SEPARATORS = Regex("[\\p{Punct}\\p{Pd}\\p{Ps}\\p{Pe}]")
        private val WHITESPACE = Regex("\\s+")

        /** The words of [title], spelled so that respellings compare equal. */
        fun titleWords(title: String): Set<String> {
            var s = HebrewTextUtils.removeAllDiacritics(title)
            s = HebrewTextUtils.replaceMaqaf(s, " ")
            s = s.replace(QUOTES, "").replace(SEPARATORS, " ")
            return s.split(WHITESPACE)
                .filter { it.isNotBlank() && it !in HONORIFICS }
                // Full and defective spelling (הזוהר / הזהר, תיקוני / תקוני).
                .map { it.first() + it.drop(1).replace("ו", "").replace("י", "") }
                .toSet()
        }

        /**
         * True when one title's words contain the other's: the rename added or dropped
         * words (an author, a qualifier) or respelled them, but replaced none.
         */
        fun isSameWorkTitle(oldTitle: String, newTitle: String): Boolean {
            val old = titleWords(oldTitle)
            val new = titleWords(newTitle)
            if (old.isEmpty() || new.isEmpty()) return false
            return old.containsAll(new) || new.containsAll(old)
        }

        /** The comparison key for "is this the same title": the Acronymizer's sanitizing, without quotes. */
        fun normalizedTitle(title: String): String =
            AcronymizerLookup.sanitizeTerm(title).replace(QUOTES, "").replace(WHITESPACE, " ").trim()
    }
}
