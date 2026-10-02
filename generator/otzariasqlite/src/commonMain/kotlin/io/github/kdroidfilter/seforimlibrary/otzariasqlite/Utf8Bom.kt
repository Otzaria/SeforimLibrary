package io.github.kdroidfilter.seforimlibrary.otzariasqlite

/**
 * A UTF-8 byte-order mark at the head of a library file.
 *
 * `readText(Charsets.UTF_8)` keeps it as U+FEFF, so without stripping it the
 * first line of a book is `"﻿<h1>…"`: `detectHeaderLevel` misses the
 * heading (the book title never reaches the TOC), the mark is stored in the
 * line's content, and a JSON file that starts with one fails to parse.
 * Only a leading mark is a BOM; a U+FEFF inside the text is left alone.
 */
internal object Utf8Bom {
    const val CHAR: Char = '﻿'

    fun strip(text: String): String =
        if (text.isNotEmpty() && text[0] == CHAR) text.substring(1) else text

    /**
     * The lines of a book file without its BOM. Line count and every line but
     * the first are unchanged, so 1-based line references stay valid.
     */
    fun stripFirstLine(lines: List<String>): List<String> {
        val first = lines.firstOrNull() ?: return lines
        if (first.isEmpty() || first[0] != CHAR) return lines
        return ArrayList<String>(lines.size).apply {
            add(first.substring(1))
            addAll(lines.subList(1, lines.size))
        }
    }

    /**
     * Content of line 0 as builds before the stripping keyed it (BOM included),
     * so its stable id can be carried over instead of renumbered.
     */
    fun legacyFirstLineContent(line: String): String = CHAR + line
}
