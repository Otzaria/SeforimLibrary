package io.github.kdroidfilter.seforimlibrary.dao.repository

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SourceViewTypesContractTest {

    @Test
    fun `SOURCE_VIEW_TYPES equals the type list of every mirror SOURCE query`() {
        val sq = File("src/commonMain/sqldelight/io/github/kdroidfilter/seforimlibrary/db/LinkQueries.sq").readText()
        val blocks = Regex("""(?m)^(\w+):[ \t]*$""").findAll(sq).toList()
        val mirrorQueries = blocks.mapIndexed { i, m ->
            m.groupValues[1] to sq.substring(m.range.last + 1, blocks.getOrNull(i + 1)?.range?.first ?: sq.length)
        }.filter { (name, _) -> name.startsWith("selectInverse") || name == "selectSourcesByBook" }
        assertEquals(9, mirrorQueries.size, "mirror queries found: ${mirrorQueries.map { it.first }}")

        val expected = ConnectionType.SOURCE_VIEW_TYPES.map { it.name }.toSet()
        for ((name, body) in mirrorQueries) {
            val lists = Regex("""ct\.name IN \(([^)]*)\)""").findAll(body).toList()
            assertEquals(1, lists.size, "$name must have exactly one ct.name IN (...) list")
            val types = lists.single().groupValues[1].split(',').map { it.trim().trim('\'') }.toSet()
            assertEquals(expected, types, "$name drifted from ConnectionType.SOURCE_VIEW_TYPES")
        }
    }

    @Test
    fun `SOURCE_VIEW_TYPES never contains the virtual SOURCE type`() {
        assertTrue(ConnectionType.SOURCE !in ConnectionType.SOURCE_VIEW_TYPES)
    }
}
