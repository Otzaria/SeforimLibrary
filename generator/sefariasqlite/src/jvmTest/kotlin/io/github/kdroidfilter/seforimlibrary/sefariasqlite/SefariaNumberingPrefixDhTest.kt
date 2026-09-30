package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.dh.DhExtractor
import kotlin.test.Test
import kotlin.test.assertEquals

class SefariaNumberingPrefixDhTest {

    // Pins DhExtractor's marker set to every "(x) " prefix the reader can generate.
    @Test
    fun `every generated line-number prefix is ignored by line_dh`() {
        for (n in 1..999) {
            val prefix = "(${toGematria(n)}) "
            assertEquals("דיבור", DhExtractor.extract("$prefix<b>דיבור</b> פירוש", DhExtractor.Format.BOLD)?.key, prefix)
            assertEquals("דיבור", DhExtractor.extract("${prefix}דיבור – פירוש", DhExtractor.Format.DASH)?.key, prefix)
        }
    }
}
