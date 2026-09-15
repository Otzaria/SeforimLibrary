package io.github.kdroidfilter.seforimlibrary.core.models

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ConnectionTypeTest {

    @Test
    fun declarationOrderIsFrozen() {
        // Ids are seeded from declaration order (id = ordinal + 1): any reorder,
        // insertion, or removal below silently remaps existing DB rows.
        val expected = listOf(
            "COMMENTARY", "SUPER_COMMENTARY", "TARGUM", "REFERENCE", "SOURCE",
            "MIDRASH", "QUOTATION", "MESORAT_HASHAS", "EIN_MISHPAT", "DIBUR_HAMATCHIL",
            "PARSHANUT", "MISHNAH_IN_TALMUD", "RELATED", "OTHER", "LINKER",
            "SIFREI_MITZVOT", "ESSAY", "ALLUSION", "LITURGY", "ELUCIDATION",
            "EXPLICATION", "LAW", "SUMMARY", "FOOTNOTES",
        )
        assertEquals(expected, ConnectionType.values().map { it.name })
    }

    @Test
    fun fromStringRoundTripsEveryEnumName() {
        for (v in ConnectionType.values()) {
            assertEquals(v, ConnectionType.fromString(v.name))
        }
    }

    @Test
    fun fromKnownStringOrNullMapsRawSefariaValues() {
        assertEquals(ConnectionType.SIFREI_MITZVOT, ConnectionType.fromKnownStringOrNull("sifrei mitzvot"))
        assertEquals(ConnectionType.ESSAY, ConnectionType.fromKnownStringOrNull("essay"))
        assertEquals(ConnectionType.ALLUSION, ConnectionType.fromKnownStringOrNull("allusion"))
        assertEquals(ConnectionType.LITURGY, ConnectionType.fromKnownStringOrNull("liturgy"))
        assertEquals(ConnectionType.EXPLICATION, ConnectionType.fromKnownStringOrNull("explication"))
        assertEquals(ConnectionType.LAW, ConnectionType.fromKnownStringOrNull("law"))
        assertEquals(ConnectionType.SUMMARY, ConnectionType.fromKnownStringOrNull("summary"))
        assertEquals(ConnectionType.FOOTNOTES, ConnectionType.fromKnownStringOrNull("footnotes"))
        assertEquals(ConnectionType.FOOTNOTES, ConnectionType.fromKnownStringOrNull("footnote"))
    }

    @Test
    fun fromKnownStringOrNullAcceptsBothElucidationSpellings() {
        assertEquals(ConnectionType.ELUCIDATION, ConnectionType.fromKnownStringOrNull("ellucidation"))
        assertEquals(ConnectionType.ELUCIDATION, ConnectionType.fromKnownStringOrNull("elucidation"))
    }

    @Test
    fun fromKnownStringOrNullMapsEmptyNoneOtherToOther() {
        assertEquals(ConnectionType.OTHER, ConnectionType.fromKnownStringOrNull(""))
        assertEquals(ConnectionType.OTHER, ConnectionType.fromKnownStringOrNull("none"))
        assertEquals(ConnectionType.OTHER, ConnectionType.fromKnownStringOrNull("other"))
    }

    @Test
    fun fromKnownStringOrNullReturnsNullForUnknown() {
        assertNull(ConnectionType.fromKnownStringOrNull("no_such_type"))
    }
}
