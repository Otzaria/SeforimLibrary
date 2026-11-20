package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import io.github.kdroidfilter.seforimlibrary.common.ids.CategoryLabels

/**
 * The book.title an Otzaria book is shown under: its file stem with the gershayim
 * a Windows file name cannot hold put back ('שות באר מרדכי' → 'שו״ת באר מרדכי').
 *
 * The stem stays the book's key (ids, links, alt-toc, blacklists); only the title
 * and heRef change, and only by quote marks ([CategoryLabels.onlyAddsQuoteMarks]).
 *
 *  1. The whole first heading, when it is the stem plus gershayim.
 *  2. Then per word still without them: an acronym word of the heading, or one of [acronyms].
 */
internal class BookDisplayTitles(acronyms: Collection<String>) {

    private val acronymsByBareWord: Map<String, String> = acronyms
        .map(CategoryLabels::normalize)
        .onEach { require(isAcronymWord(it)) { "Not an acronym with gershayim: '$it'" } }
        .associateBy(CategoryLabels::stripQuoteMarks)

    /**
     * @param stemTitle the book's key title, `CategoryLabels.bookTitle(stem)`.
     * @param firstHeading the plain text of the book's first `<h1>`, if any.
     */
    fun displayTitle(stemTitle: String, firstHeading: String?): String {
        val heading = firstHeading?.let(CategoryLabels::normalize).orEmpty()
        val candidate = CategoryLabels.bookTitle(byWords(fromWholeHeading(stemTitle, heading) ?: stemTitle, heading))
        return if (CategoryLabels.onlyAddsQuoteMarks(candidate, stemTitle)) candidate else stemTitle
    }

    private fun fromWholeHeading(stemTitle: String, heading: String): String? =
        heading.takeIf {
            it.isNotEmpty() && CategoryLabels.stripQuoteMarks(it) == CategoryLabels.stripQuoteMarks(stemTitle) &&
                it.count { c -> c == GERSHAYIM } > stemTitle.count { c -> c == GERSHAYIM }
        }

    private fun byWords(stemTitle: String, heading: String): String {
        val headingAcronyms = HashMap<String, String>()
        for (word in heading.split(' ')) {
            val core = word.trim(*WORD_PUNCTUATION)
            if (isAcronymWord(core)) headingAcronyms.putIfAbsent(CategoryLabels.stripQuoteMarks(core), core)
        }
        return stemTitle.split(' ').joinToString(" ") { word ->
            val core = word.trim(*WORD_PUNCTUATION)
            if (core.isEmpty() || CategoryLabels.stripQuoteMarks(core) != core) return@joinToString word
            val replacement = headingAcronyms[core] ?: acronymsByBareWord[core] ?: return@joinToString word
            val start = word.indexOf(core)
            word.substring(0, start) + replacement + word.substring(start + core.length)
        }
    }

    companion object {
        private const val GERSHAYIM = '״'
        private val WORD_PUNCTUATION = "()[]{},.:;!?-–—".toCharArray()
        private val ACRONYM_WORD = Regex("^[\\p{L}׳]*\\p{L}״\\p{L}[\\p{L}׳]*$")

        private fun isAcronymWord(word: String): Boolean = ACRONYM_WORD.matches(word)

        /** Loads the curated acronyms from the `title-acronyms.txt` resource. */
        fun fromResources(): BookDisplayTitles {
            val stream = BookDisplayTitles::class.java.getResourceAsStream("/title-acronyms.txt")
                ?: error("title-acronyms.txt resource is missing")
            val acronyms = stream.bufferedReader(Charsets.UTF_8).use { reader ->
                reader.lineSequence().map { it.trim() }.filter { it.isNotEmpty() && !it.startsWith("#") }.toList()
            }
            return BookDisplayTitles(acronyms)
        }
    }
}
