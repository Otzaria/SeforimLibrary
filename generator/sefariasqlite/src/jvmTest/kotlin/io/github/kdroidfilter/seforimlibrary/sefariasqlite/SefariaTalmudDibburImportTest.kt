package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateReader
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.dh.DhExtractor
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.sql.DriverManager
import java.util.concurrent.TimeUnit
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Actual primary/version importer regression: formatting must never enter stable keys or raw anchor coordinates. */
class SefariaTalmudDibburImportTest {
    private val baseTitle = "ברכות"
    private val commentaryTitle = "בדיקת דיבור על ברכות"
    private val first = "עד סוף האשמורה הראשונה – שליש הלילה כדמפרש בגמרא"
    private val comments = listOf(
        first,
        first, // Duplicate raw content must retain its occurrence-specific ID.
        "מאימתי קורין את שמע בערבין. משעה שהכהנים נכנסים לאכול בתרומתן – כהנים שטבלו",
        "<b>דיבור קודם</b> – פירוש שכבר מודגש",
        "תודה 😊 – פירוש &amp; נוסף",
    )

    private fun strings(values: List<String>) = JsonArray(values.map(::JsonPrimitive))

    private fun fixture(root: Path) {
        Files.createDirectories(root.resolve("schemas"))
        Files.createDirectories(root.resolve("json"))
        Files.writeString(root.resolve("table_of_contents.json"), "[]")
        for ((en, he, rows, dependent) in listOf(
            listOf("Berakhot", baseTitle, "base", "false"),
            listOf("Bold Fixture on Berakhot", commentaryTitle, "commentary", "true"),
        )) {
            val schema = buildJsonObject {
                put("schema", buildJsonObject {
                    put("title", en); put("heTitle", he); put("nodeType", "JaggedArrayNode"); put("depth", 2)
                    put("sectionNames", strings(listOf("Daf", "Comment")))
                    put("heSectionNames", strings(listOf("דף", "פירוש")))
                    put("addressTypes", strings(listOf("Talmud", "Integer")))
                })
                put("categories", strings(listOf("Talmud", "Bavli", "Commentary")))
                put("heCategories", strings(listOf("תלמוד", "בבלי", "מפרשים")))
                if (dependent == "true") { put("dependence", "Commentary"); put("base_text_titles", strings(listOf("Berakhot"))) }
            }
            Files.writeString(root.resolve("schemas/${en.replace(' ', '_')}.json"), schema.toString())
            val dir = Files.createDirectories(root.resolve("json/$en"))
            fun edition(alternative: Boolean) = buildJsonObject {
                put("title", en); put("heTitle", he); put("language", "he"); put("actualLanguage", "he")
                put("versionTitle", if (alternative) "Alternative Edition" else "Fixture Edition")
                put("versions", JsonArray(listOf(strings(listOf("Fixture Edition", "")))))
                val text = if (rows == "base") listOf("מקור הציטוט") else comments.map {
                    if (alternative) it.replace("שליש הלילה", "תחילת הלילה") else it
                }
                put("text", JsonArray(listOf(JsonArray(emptyList()), JsonArray(emptyList()), strings(text))))
            }
            Files.writeString(dir.resolve("merged.json"), edition(false).toString())
            if (dependent == "true") Files.writeString(dir.resolve("Alternative Edition.json"), edition(true).toString())
        }
    }

    private fun query(db: Path, sql: String): List<List<String?>> = DriverManager.getConnection("jdbc:sqlite:$db").use { conn ->
        conn.createStatement().use { st -> st.executeQuery(sql).use { rs ->
            buildList { while (rs.next()) add((1..rs.metaData.columnCount).map { rs.getString(it) }) }
        } }
    }

    @Test
    fun primaryAndAlternativeFormattingKeepsRawIdsCountsAndCharAnchors() = runBlocking {
        val root = Files.createTempDirectory("sefaria-bold-dibbur-import")
        try {
            fixture(root)
            val reader = SefariaBookPayloadReader(Json { ignoreUnknownKeys = true }, Logger.withTag("DibburImportExpected"))
            val payloads = reader.readBooksInParallel(root.resolve("json"), root.resolve("schemas"), reader.buildSchemaLookup(root.resolve("schemas")))
            val commentary = payloads.single { it.heTitle == commentaryTitle }
            assertTrue(commentary.boldDashDibburim)
            val base = payloads.single { it.heTitle == baseTitle }
            assertTrue(!base.boldDashDibburim)

            // Seed the state exactly as the old unformatted importer did. The
            // actual importer must reuse every ID despite storing new HTML.
            val allocator = InMemoryIdAllocator.load(path = null)
            val expected = mutableListOf<List<String?>>()
            for (payload in payloads.sortedBy { it.heTitle }) {
                val bookId = allocator.bookId("Sefaria", payload.heTitle)
                val counts = mutableMapOf<String, Int>()
                val pre = payload.precomputed!!
                payload.lines.forEachIndexed { i, raw ->
                    val hash = pre.lineKeyHashes!![i]
                    val key = hash.joinToString("") { "%02x".format(it) }
                    val occurrence = counts[key] ?: 0
                    counts[key] = occurrence + 1
                    val id = allocator.lineId(bookId, hash, occurrence)
                    expected += listOf(id.toString(), payload.heTitle, i.toString(),
                        if (payload.boldDashDibburim) SefariaTalmudDibburBold.bold(raw) else raw,
                        countVisibleChars(raw).toString(), pre.refsByLineIndex[i]?.heRef)
                }
            }
            val state = root.resolve("state.db")
            allocator.snapshotTo(state)
            val beforeKeys = BuildStateReader().read(state).lines

            // Exercise char-anchor conversion with raw offsets in a line whose
            // stored representation gains a bold prefix before the quotation.
            val targetRef = commentary.refEntries.first { commentary.lines[it.lineIndex - 1].contains(first) }
            val sourceRef = base.refEntries.first()
            val start = first.indexOf("שליש הלילה")
            val end = start + "שליש הלילה".length
            val db = root.resolve("import.db")
            importInIsolatedJvm(root, db, state)
            val actual = query(db, "SELECT l.id,b.title,l.lineIndex,l.content,l.charCount,l.heRef FROM line l JOIN book b ON b.id=l.bookId ORDER BY b.title,l.lineIndex")
            assertEquals(expected.sortedWith(compareBy({ it[1] }, { it[2]!!.toInt() })), actual)
            assertEquals(beforeKeys, BuildStateReader().read(state).lines, "formatting cannot allocate a second set of line keys")

            val target = query(db, "SELECT l.id FROM line l JOIN book b ON b.id=l.bookId WHERE b.title='$commentaryTitle' AND l.lineIndex=${targetRef.lineIndex - 1}").single().single()!!.toLong()
            val source = query(db, "SELECT l.id FROM line l JOIN book b ON b.id=l.bookId WHERE b.title='$baseTitle' AND l.lineIndex=${sourceRef.lineIndex - 1}").single().single()!!.toLong()
            // The real anchoring stage consumes the original payload, exactly
            // as SefariaDirectImporter does, against the newly formatted DB.
            val driver = JdbcSqliteDriver("jdbc:sqlite:$db")
            val repo = SeforimRepository(db.toString(), driver)
            try {
                val baseId = allocator.peekBookId("Sefaria", baseTitle)!!
                val targetId = allocator.peekBookId("Sefaria", commentaryTitle)!!
                repo.insertLinksBatch(listOf(io.github.kdroidfilter.seforimlibrary.core.models.Link(
                    id = 999, sourceBookId = baseId, targetBookId = targetId, sourceLineId = source,
                    targetLineId = target, targetLineIndex = targetRef.lineIndex - 1,
                    connectionType = io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType.QUOTATION,
                )))
                val bookPath = buildBookPath(commentary.categoriesHe, commentary.heTitle)
                SefariaCharLevelAnchors(repo, Logger.withTag("DibburImportAnchor")).generate(
                    listOf(PendingCharLevelAnchor(bookPath, targetRef.lineIndex - 1, source, target, 1, start, end, "Fixture Edition", "he", false)),
                    listOf(SefariaInlineAnchors.BookInput(targetId, commentary.enTitle, bookPath, commentary.lines,
                        commentary.refEntries.associateBy { it.lineIndex - 1 }, commentary.singleVersionTitle, commentary.cleanShiftByLineIndex)),
                )
                val anchor = repo.getLinkAnchors(999).single()
                val shift = generatedPrefixLength(commentary.cleanShiftByLineIndex[targetRef.lineIndex - 1] ?: 0)
                assertEquals(countVisibleChars(commentary.lines[targetRef.lineIndex - 1], shift + start), anchor.charStart)
                assertEquals(countVisibleChars(commentary.lines[targetRef.lineIndex - 1], shift + end), anchor.charEnd)
            } finally { repo.close() }

            val versions = query(db, "SELECT vl.lineId,vl.content,vl.charCount FROM version_line vl ORDER BY vl.lineId")
            assertEquals(comments.size, versions.size)
            for (row in versions) {
                val primary = actual.single { it[0] == row[0] }
                val rawPrimary = commentary.lines[primary[2]!!.toInt()]
                val expectedVersion = SefariaTalmudDibburBold.bold(rawPrimary.replace("שליש הלילה", "תחילת הלילה"))
                assertEquals(expectedVersion, row[1])
                assertEquals(countVisibleChars(expectedVersion).toString(), row[2])
                assertEquals(DhExtractor.extract(rawPrimary, DhExtractor.Format.DASH),
                    if (DhExtractor.dashDibburEnd(rawPrimary) == null) null else DhExtractor.extract(row[1]!!, DhExtractor.Format.BOLD))
            }
            val replay = root.resolve("replay.db")
            importInIsolatedJvm(root, replay, state)
            assertEquals(actual, query(replay, "SELECT l.id,b.title,l.lineIndex,l.content,l.charCount,l.heRef FROM line l JOIN book b ON b.id=l.bookId ORDER BY b.title,l.lineIndex"))
            assertEquals(versions, query(replay, "SELECT vl.lineId,vl.content,vl.charCount FROM version_line vl ORDER BY vl.lineId"))
        } finally {
            Files.walk(root).use { paths -> paths.sorted(java.util.Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }

    companion object {
        /** A fork isolates the importer's process-global ForDB cache, keeping the fixture entirely local. */
        private fun importInIsolatedJvm(export: Path, db: Path, state: Path) {
            val archive = export.resolve("fixture-fordb.zip")
            ZipOutputStream(Files.newOutputStream(archive)).use { zip ->
                for (name in FOR_DB_CSV_FILES.values) {
                    zip.putNextEntry(ZipEntry("ForDB/$name").apply { time = 0L })
                    zip.closeEntry()
                }
            }
            val sha = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(archive)).joinToString("") { "%02x".format(it) }
            val urls = generateSequence(SefariaTalmudDibburImportTest::class.java.classLoader) { it.parent }
                .filterIsInstance<URLClassLoader>().flatMap { it.urLs.asSequence() }
                .filter { it.protocol == "file" }.map { Path.of(it.toURI()).toString() }
            val classpath = (urls + System.getProperty("java.class.path").split(java.io.File.pathSeparator).asSequence())
                .distinct().joinToString(java.io.File.pathSeparator)
            val log = export.resolve("import.log")
            val process = ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx512m",
                "--enable-native-access=ALL-UNNAMED", "-DforDbArchive=$archive", "-DforDbSha256=$sha", "-cp", classpath,
                SefariaTalmudDibburImportTest::class.java.name, export.toString(), db.toString(), state.toString(),
            ).redirectErrorStream(true).redirectOutput(log.toFile()).start()
            if (!process.waitFor(60, TimeUnit.SECONDS)) {
                process.destroyForcibly().waitFor()
                error("Direct import fixture timed out: ${Files.readString(log)}")
            }
            assertEquals(0, process.exitValue(), Files.readString(log))
        }

        @JvmStatic
        fun main(args: Array<String>) = runBlocking {
            val driver = JdbcSqliteDriver("jdbc:sqlite:${args[1]}")
            val repo = SeforimRepository(args[1], driver)
            val allocator = InMemoryIdAllocator.load(Path.of(args[2]))
            try {
                SefariaDirectImporter(Path.of(args[0]), repo, allocator).import()
                allocator.snapshotTo(Path.of(args[2]))
            } finally { repo.close() }
        }
    }
}
