package io.github.kdroidfilter.seforimlibrary.common.ids

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSnapshot
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateWriter
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import io.github.kdroidfilter.seforimlibrary.common.buildstate.LinkKey
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals

class DiskBackedLinkIdAllocatorTest {

    @Test
    fun reusesAndPersistsStableIdsWithoutLoadingTheSnapshot() {
        val directory = Files.createTempDirectory("disk-link-allocator")
        val path = directory.resolve("buildstate.db")
        val linkerTypeId = 15L
        try {
            BuildStateWriter().write(
                BuildStateSnapshot.empty().copy(
                    counters = mapOf(IdTable.LINK to 100L),
                    lookups = mapOf(
                        IdTable.CONNECTION_TYPE to mapOf("LINKER" to linkerTypeId),
                    ),
                    links = mapOf(LinkKey(1L, 2L, linkerTypeId) to 41L),
                ),
                path,
            )

            DiskBackedLinkIdAllocator.open(path, "LINKER", linkerTypeId, 150L).use { allocator ->
                assertEquals(41L, allocator.linkId(1L, 2L, linkerTypeId))
                assertEquals(150L, allocator.linkId(3L, 4L, linkerTypeId))
                assertEquals(150L, allocator.linkId(3L, 4L, linkerTypeId))
                allocator.commit(mapOf("generator" to "linkerlinks"))
                assertEquals(2L, allocator.reusedCount)
                assertEquals(1L, allocator.freshCount)
            }

            DriverManager.getConnection("jdbc:sqlite:$path").use { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery(
                        "SELECT id FROM id_link WHERE src_line_id=3 AND tgt_line_id=4 AND connection_type_id=15",
                    ).use { result ->
                        assertEquals(true, result.next())
                        assertEquals(150L, result.getLong(1))
                    }
                    statement.executeQuery("SELECT next_id FROM id_counters WHERE table_name='link'").use { result ->
                        assertEquals(true, result.next())
                        assertEquals(151L, result.getLong(1))
                    }
                    statement.executeQuery("SELECT value FROM meta WHERE key='generator'").use { result ->
                        assertEquals(true, result.next())
                        assertEquals("linkerlinks", result.getString(1))
                    }
                }
            }
        } finally {
            Files.deleteIfExists(path.resolveSibling("${path.fileName}-shm"))
            Files.deleteIfExists(path.resolveSibling("${path.fileName}-wal"))
            Files.deleteIfExists(path)
            Files.deleteIfExists(directory)
        }
    }
}
