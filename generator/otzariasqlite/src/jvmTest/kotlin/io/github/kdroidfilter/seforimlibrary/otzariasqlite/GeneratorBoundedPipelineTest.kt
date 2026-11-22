package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.SQLException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class GeneratorBoundedPipelineTest {
    @Test
    fun sparseIdPagesResolveBoundariesAndMissingRowsAfterEviction() = runBlocking {
        withFixture { root ->
            val links = Files.createDirectories(root.resolve("links"))
            Files.createDirectories(root.resolve("spill"))
            withRepo { driver, repo ->
                val source = repo.insertSource("Tashma")
                val category = repo.insertCategory(Category(0, null, "בדיקה", level = 0, order = 1))
                repo.insertBook(Book(1, category, source, "מקור", heRef = "מקור", totalLines = 8194))
                repo.insertBook(Book(2, category, source, "יעד", heRef = "יעד", totalLines = 8194))
                val indices = listOf(0, 4095, 4096, 8193)
                repo.insertLinesBatch(indices.flatMap { index ->
                    listOf(
                        Line(id = 10L + index, bookId = 1, lineIndex = index, content = "מקור $index"),
                        Line(id = 20_000L + index, bookId = 2, lineIndex = index, content = "יעד $index"),
                    )
                })
                assertEquals(listOf(4095 to 4105L, 4096 to 4106L), repo.getLineIdIndexPairs(1, 4090, 4100))
                val requests = listOf(4095 to 4096, 0 to 8193, 8193 to 0, 4096 to 4095, 7 to 0)
                Files.writeString(links.resolve("מקור_links.json"), requests.joinToString(",", "[", "]") { (from, to) ->
                    """{"heRef_2":"יעד","line_index_1":${from + 1},"line_index_2":${to + 1},"path_2":"יעד.txt","Conection Type":"commentary"}"""
                })
                DatabaseGenerator(root, repo, inputBufferBytes = 1024, inputSpillDirectory = root.resolve("spill"))
                    .generateLinksOnly()
                assertEquals(
                    requests.filter { it.first in indices }.map { (from, to) ->
                        listOf((10L + from).toString(), (20_000L + to).toString())
                    }.sortedBy { it[0].toLong() },
                    query(driver.getConnection(), "SELECT sourceLineId, targetLineId FROM link ORDER BY sourceLineId"),
                )
                assertSpillsRemoved(root)
            }
        }
    }

    @Test
    fun spilledAndResidentInputsProduceIdenticalSchemaIdsLinksAndContent() = runBlocking {
        withFixture { root ->
            writeFixture(root, withNotes = true)
            // Tiny budget forces both files to spill and puts duplicate ranged
            // links in different batches. A large budget keeps the same inputs resident.
            val spilled = buildSnapshot(root, 1024)
            val resident = buildSnapshot(root, 8L * 1024 * 1024)
            assertEquals(resident, spilled)
            assertTrue(spilled.getValue("line").any { row -> row.any { it?.contains("footnote-marker") == true } })
            assertTrue(spilled.getValue("link_range").isNotEmpty())
            assertTrue(spilled.getValue("link_anchor").isNotEmpty())
            assertSpillsRemoved(root)
        }
    }

    @Test
    fun malformedSuffixDiscardsEveryValidPrefixRecordBeforeSqlWrites() = runBlocking {
        withFixture { root ->
            writeFixture(root, withNotes = false)
            val valid = linkRecord(end = 100)
            val invalidFiles = listOf(
                "[${List(30) { valid }.joinToString()}, {\"line_index_1\": 2}",
                "[$valid] {\"trailing\":true}",
                "[$valid,]",
                "{\"unsupportedDictaObject\":[]}",
            )
            for (invalid in invalidFiles) {
                Files.writeString(root.resolve("links/מקור_links.json"), invalid)
                withRepo { driver, repo ->
                    val allocator = InMemoryIdAllocator.load(null)
                    val generator = DatabaseGenerator(
                        root, repo, allocator = allocator,
                        inputBufferBytes = 1024, inputSpillDirectory = root.resolve("spill"),
                    )
                    generator.generateLinesOnly()
                    generator.generateLinksOnly()
                    assertEquals(listOf(listOf("0")), query(driver.getConnection(), "SELECT count(*) FROM link"))
                }
                assertSpillsRemoved(root)
            }
        }
    }

    @Test
    fun databaseFailureInPrimaryLinkWritePropagatesAndAllowsRetryAfterCleanup() = runBlocking {
        withFixture { root ->
            writeFixture(root, withNotes = false)
            withRepo { driver, repo ->
                val generator = DatabaseGenerator(
                    root, repo, inputBufferBytes = 1024, inputSpillDirectory = root.resolve("spill"),
                )
                generator.generateLinesOnly()
                driver.getConnection().createStatement().use { statement ->
                    statement.execute(
                        "CREATE TRIGGER reject_probe_link BEFORE INSERT ON link " +
                            "BEGIN SELECT RAISE(ABORT, 'injected primary link failure'); END",
                    )
                }
                val failure = assertFails { generator.generateLinksOnly() }
                assertTrue(
                    generateSequence(failure) { it.cause }.any {
                        it is SQLException && it.message?.contains("injected primary link failure") == true
                    },
                    "Primary SQL errors must propagate, not become skipped links: $failure",
                )
                assertEquals(listOf(listOf("0")), query(driver.getConnection(), "SELECT count(*) FROM link"))
                assertSpillsRemoved(root)
                driver.getConnection().createStatement().use { it.execute("DROP TRIGGER reject_probe_link") }
                generator.generateLinksOnly()
                assertEquals(listOf(listOf("1")), query(driver.getConnection(), "SELECT count(*) FROM link"))
                assertSpillsRemoved(root)
            }
        }
    }

    @Test
    fun stableIdCollisionWhileReplacingLegacyLinkPropagatesAndRemovesSpills() = runBlocking {
        withFixture { root ->
            val links = Files.createDirectories(root.resolve("links"))
            Files.createDirectories(root.resolve("spill"))
            withRepo { _, repo ->
                val source = repo.insertSource("Tashma")
                val category = repo.insertCategory(Category(0, null, "בדיקה", level = 0, order = 1))
                repo.insertBook(Book(1, category, source, "מקור", totalLines = 1))
                repo.insertBook(Book(2, category, source, "יעד", totalLines = 1, isBaseBook = true))
                repo.insertLinesBatch(listOf(Line(10, 1, 0, "מקור"), Line(20, 2, 0, "יעד")))
                val delegate = InMemoryIdAllocator.load(null)
                val allocator = object : IdAllocator by delegate {
                    override fun linkId(srcLineId: Long, tgtLineId: Long, connectionTypeId: Long): Long = 900
                }
                val type = allocator.connectionTypeId(ConnectionType.COMMENTARY.name)
                repo.insertConnectionTypeWithId(type, ConnectionType.COMMENTARY.name)
                repo.insertLinkWithId(900, 1, 2, 10, 20, 0, type)
                val row = """{"heRef_2":"יעד","line_index_1":1,"line_index_2":1,"path_2":"יעד.txt","Conection Type":"commentary"}"""
                Files.writeString(
                    links.resolve("מקור_links.json"),
                    "[${List(4) { row }.joinToString()}]",
                )
                val generator = DatabaseGenerator(
                    root, repo, allocator = allocator,
                    inputBufferBytes = 1024, inputSpillDirectory = root.resolve("spill"),
                )
                val failure = assertFailsWith<IllegalStateException> { generator.generateLinksOnly() }
                assertTrue(failure.message?.contains("Stable link id collision") == true)
                assertSpillsRemoved(root)
            }
        }
    }

    @Test
    fun malformedTargetPathsStillAllowLaterValidLinks() = runBlocking {
        withFixture { root ->
            writeFixture(root, withNotes = false)
            withRepo { driver, repo ->
                val generator = DatabaseGenerator(
                    root, repo, inputBufferBytes = 1024, inputSpillDirectory = root.resolve("spill"),
                )
                generator.generateLinesOnly()
                val valid = linkRecord(end = null)
                val invalidPath = valid.replace("יעד.txt", "\\u0000")
                val pathWithoutFilename = valid.replace("יעד.txt", "/")
                Files.writeString(root.resolve("links/מקור_links.json"), "[$invalidPath,$pathWithoutFilename,$valid]")
                generator.generateLinksOnly()
                assertEquals(listOf(listOf("1")), query(driver.getConnection(), "SELECT count(*) FROM link"))
                assertSpillsRemoved(root)
            }
        }
    }

    @Test
    fun databaseFailureWhileFlushingLargeRangePropagatesAndRemovesSpills() = runBlocking {
        withFixture { root ->
            writeFixture(root, withNotes = false)
            withRepo { driver, repo ->
                val generator = DatabaseGenerator(
                    root, repo, inputBufferBytes = 1024, inputSpillDirectory = root.resolve("spill"),
                )
                generator.generateLinesOnly()
                driver.getConnection().createStatement().use { statement ->
                    statement.execute(
                        "CREATE TRIGGER reject_probe_coverage BEFORE INSERT ON link_coverage " +
                            "BEGIN SELECT RAISE(ABORT, 'injected coverage failure'); END",
                    )
                }
                val failure = assertFails { generator.generateLinksOnly() }
                assertTrue(
                    generateSequence(failure) { it.cause }.any { it.message?.contains("injected coverage failure") == true },
                    "SQL flush errors must propagate, not be treated as malformed link rows: $failure",
                )
                assertSpillsRemoved(root)
                assertEquals(listOf(listOf("1")), query(driver.getConnection(), "SELECT 1"))
            }
        }
    }

    @Test
    fun duplicateRangedRowsKeepTheirWidestRangeAcrossTinyBatchesAndReruns() = runBlocking {
        withFixture { root ->
            writeFixture(root, withNotes = false)
            withRepo { driver, repo ->
                val allocator = InMemoryIdAllocator.load(null)
                val generator = DatabaseGenerator(
                    root, repo, allocator = allocator,
                    inputBufferBytes = 1024, inputSpillDirectory = root.resolve("spill"),
                )
                generator.generateLinesOnly()
                generator.generateLinksOnly()
                assertEquals(listOf(listOf("99")), query(driver.getConnection(), "SELECT max(endLineIndex) FROM link_range"))
                Files.writeString(root.resolve("links/מקור_links.json"), "[${linkRecord(end = 50)},${linkRecord(end = null)}]")
                generator.generateLinksOnly()
                assertEquals(listOf(listOf("49")), query(driver.getConnection(), "SELECT max(endLineIndex) FROM link_range"))
                assertEquals(listOf(listOf("1")), query(driver.getConnection(), "SELECT count(*) FROM link"))
                assertSpillsRemoved(root)
            }
        }
    }

    private suspend fun buildSnapshot(root: Path, budget: Long): Map<String, List<List<String?>>> =
        withRepo { driver, repo ->
            val allocator = InMemoryIdAllocator.load(null)
            val generator = DatabaseGenerator(
                root, repo, allocator = allocator,
                inputBufferBytes = budget, inputSpillDirectory = root.resolve("spill"),
            )
            generator.generateLinesOnly()
            generator.generateLinksOnly()
            val connection = driver.getConnection()
            assertEquals(emptyList(), query(connection, "PRAGMA foreign_key_check"))
            val tables = query(connection, "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
                .map { checkNotNull(it.single()) }
            buildMap {
                put("schema", query(connection, "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"))
                for (table in tables) {
                    val quoted = "\"${table.replace("\"", "\"\"")}\""
                    val columns = query(connection, "PRAGMA table_info($quoted)").map { row ->
                        "\"${checkNotNull(row[1]).replace("\"", "\"\"")}\""
                    }
                    put(table, query(connection, "SELECT * FROM $quoted ORDER BY ${columns.joinToString() }"))
                }
            }
        }

    private fun writeFixture(root: Path, withNotes: Boolean) {
        val books = Files.createDirectories(root.resolve("אוצריא/בדיקה"))
        for (title in listOf("מקור", "יעד")) {
            val text = buildList {
                add("\uFEFF<h1>$title</h1>")
                for (line in 1 until 120) add(when {
                    line == 30 -> "<h3>כותרת משנה</h3>"
                    title == "מקור" && withNotes && line == 4 -> "תוכן<sup>1</sup> המשך"
                    line % 7 == 0 -> "תוכן שחוזר בדיוק"
                    else -> "תוכן $line — " + "טקסט ".repeat(10)
                })
            }.mapIndexed { index, line -> line + when (index % 3) { 0 -> "\r\n"; 1 -> "\n"; else -> "\r" } }
                .joinToString("")
            Files.writeString(books.resolve("$title.txt"), text)
        }
        val links = Files.createDirectories(root.resolve("links"))
        val records = buildList {
            add(linkRecord(end = 100))
            repeat(50) { add(linkRecord(end = if (it % 2 == 0) 50 else null)) }
            if (withNotes) add(
                """{"heRef_2":"הערה","line_index_1":5,"line_index_2":2,"path_2":"הערות על מקור.txt","Conection Type":"commentary"}""",
            )
        }
        Files.writeString(links.resolve("מקור_links.json"), "\uFEFF[${records.joinToString()}]")
        if (withNotes) Files.writeString(books.resolve("הערות על מקור.txt"), "<h1>הערות</h1>\n<sup>1</sup> תוכן הערה\n")
        Files.createDirectories(root.resolve("spill"))
        writeFilesManifest(root)
    }

    private fun linkRecord(end: Int?): String = buildString {
        append("""{"heRef_2":"יעד אות א","line_index_1":2,"line_index_2":2,"path_2":"יעד.txt","Conection Type":"commentary","start":0,"end":2""")
        if (end != null) append(",\"line_index_2_end\":$end")
        append('}')
    }

    private suspend fun <T> withRepo(block: suspend (JdbcSqliteDriver, SeforimRepository) -> T): T {
        val driver = JdbcSqliteDriver("jdbc:sqlite::memory:")
        val repo = try { SeforimRepository(":memory:", driver) } catch (error: Throwable) { driver.close(); throw error }
        return try { block(driver, repo) } finally { repo.close() }
    }

    private inline fun <T> withFixture(block: (Path) -> T): T {
        val root = Files.createTempDirectory("generator-pipeline-parity-")
        return try { block(root) } finally { root.toFile().deleteRecursively() }
    }

    private fun assertSpillsRemoved(root: Path) {
        assertEquals(0L, Files.list(root.resolve("spill")).use { it.count() })
    }

    private fun query(connection: Connection, sql: String): List<List<String?>> =
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { result ->
                buildList {
                    while (result.next()) add((1..result.metaData.columnCount).map { index ->
                        when (val value = result.getObject(index)) {
                            null -> null
                            is ByteArray -> "blob:" + value.joinToString("") { "%02x".format(it) }
                            else -> value.toString()
                        }
                    })
                }
            }
        }
}
