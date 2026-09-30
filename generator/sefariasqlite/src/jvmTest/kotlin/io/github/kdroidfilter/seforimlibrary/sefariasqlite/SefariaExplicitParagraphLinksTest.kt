package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.nio.file.Files
import java.nio.file.Path
import java.net.URLClassLoader
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Runs the complete export → direct importer → DB pipeline. Real Sefaria
 * coordinate shapes group independent comments and even a new parasha's
 * introduction under one parent. Only explicit links/ranges establish coverage;
 * absent markers or a shared Chapter/Verse/Paragraph parent cannot establish it.
 */
class SefariaExplicitParagraphLinksTest {
    private val women = "נשים פטורות וכי הבת פטורה מכבוד אב ואם. שנים שהזהיר הבן והבת. תנינא תנן במתני' מה ששנו בברייתא:"
    private val shoftimEnd = "ובזה יעשו הישר בעיני ה': נשלם סדר שופטים"
    private val kiTetzeiEnd = "והותר בזה הספק הכ\"ו: נשלם סדר כי תצא:"
    private val nextIntroduction = "ואמנם הספקות אשר יתחייבו בדברי הסדר הזה כפי פשטי הכתובים הם כ\"ח:"
    private val bikkurim = "הספק הראשון בטעם הבאת הבכורים והוא למה הטריח הקדוש ברוך הוא לבעלי הנחלות"

    private fun strings(vararg values: String): JsonArray = JsonArray(values.map(::JsonPrimitive))

    private fun sections(size: Int, values: Map<Int, JsonElement>): JsonArray =
        JsonArray((1..size).map { values[it] ?: JsonArray(emptyList()) })

    private fun segments(size: Int, values: Map<Int, String>): JsonArray =
        JsonArray((1..size).map { JsonPrimitive(values[it].orEmpty()) })

    private fun writeBook(
        root: Path,
        title: String,
        heTitle: String,
        schema: JsonElement,
        text: JsonElement,
        heCategories: List<String>,
        enCategories: List<String>,
        base: String? = null,
    ) {
        val schemaRoot = buildJsonObject {
            put("schema", schema)
            put("heCategories", JsonArray(heCategories.map(::JsonPrimitive)))
            put("categories", JsonArray(enCategories.map(::JsonPrimitive)))
            if (base != null) {
                put("dependence", "Commentary")
                put("base_text_titles", strings(base))
            }
        }
        val textRoot = buildJsonObject {
            put("title", title)
            put("heTitle", heTitle)
            put("language", "he")
            put("text", text)
        }
        Files.writeString(root.resolve("schemas").resolve(title.replace(' ', '_') + ".json"), schemaRoot.toString())
        val directory = Files.createDirectories(root.resolve("json").resolve(title))
        Files.writeString(directory.resolve("merged.json"), textRoot.toString())
    }

    private fun fixture(root: Path) {
        Files.createDirectories(root.resolve("schemas"))
        Files.createDirectories(root.resolve("json"))
        Files.createDirectories(root.resolve("links"))
        Files.writeString(root.resolve("table_of_contents.json"), "[]")
        val visibility = buildJsonObject {
            put("schema_version", 1)
            put("sefaria_project_sha", "a".repeat(40))
            put("mask_bits", buildJsonObject { LINK_VISIBILITY_MASK_NAMES.forEach { (bit, name) -> put(bit, name) } })
            for ((unit, ref) in mapOf("perek" to "Kiddushin 29a:5-8", "parasha" to "Deuteronomy 21:1-26:1")) {
                put("${unit}_refs", strings(ref))
                put("${unit}_refs_sha256", MessageDigest.getInstance("SHA-256").digest(ref.toByteArray()).joinToString("") { "%02x".format(it) })
            }
            put("counts", buildJsonObject {
                put("perek_refs", 1)
                put("parasha_refs", 1)
                put("suppressed_side_1", 0)
                put("suppressed_side_2", 0)
                put("suppressed_by_side_and_bit", buildJsonObject {
                    for (side in 1..2) put(side.toString(), buildJsonObject {
                        LINK_VISIBILITY_MASK_NAMES.values.forEach { put(it, 0) }
                    })
                })
            })
        }
        Files.writeString(Files.createDirectories(root.resolve("metadata")).resolve("link-visibility-v1.json"), visibility.toString())
        writeBook(root, "Kiddushin", "קידושין", Json.parseToJsonElement("""
            {"title":"Kiddushin","heTitle":"קידושין","nodeType":"JaggedArrayNode","depth":2,
             "sectionNames":["Daf","Line"],"heSectionNames":["דף","שורה"],"addressTypes":["Talmud","Integer"]}
        """), sections(57, mapOf(57 to segments(9, mapOf(
            5 to "אטו הדיוט לאו במי שפרע קאי?", 8 to "נשים פטורות ממצות הבן על האב",
        )))), listOf("תלמוד", "בבלי", "סדר נשים"), listOf("Talmud", "Bavli", "Seder Nashim"))
        writeBook(root, "Tosafot Ri HaZaken on Kiddushin", "תוספות ר\"י הזקן על קידושין", Json.parseToJsonElement("""
            {"title":"Tosafot Ri HaZaken on Kiddushin","heTitle":"תוספות ר\"י הזקן על קידושין",
             "nodeType":"JaggedArrayNode","depth":2,"sectionNames":["Daf","Comment"],
             "heSectionNames":["דף","פירוש"],"addressTypes":["Talmud","Integer"],
             "isSegmentLevelDiburHamatchil":true,"diburHamatchilRegexes":["^(.*?)\\."]}
        """), sections(57, mapOf(57 to segments(4, mapOf(
            3 to "אטו. כלומר וכי הדיוט רשאי לחזור בו והא אם יחזור בו יקבל עליו מי שפרע", 4 to women,
        )))), listOf("תלמוד", "בבלי", "מפרשי התלמוד"), listOf("Talmud", "Bavli", "Commentary"), "Kiddushin")
        writeBook(root, "Deuteronomy", "דברים", Json.parseToJsonElement("""
            {"title":"Deuteronomy","heTitle":"דברים","nodeType":"JaggedArrayNode","depth":2,
             "sectionNames":["Chapter","Verse"],"heSectionNames":["פרק","פסוק"],"addressTypes":["Perek","Pasuk"]}
        """), sections(27, mapOf(
            21 to segments(1, mapOf(1 to "כי ימצא חלל באדמה")),
            25 to segments(19, mapOf(17 to "זכור את אשר עשה לך עמלק", 18 to "אשר קרך בדרך", 19 to "תמחה את זכר עמלק")),
            26 to segments(1, mapOf(1 to "והיה כי תבוא אל הארץ")),
            27 to segments(1, mapOf(1 to "ויצו משה וזקני ישראל את העם")),
        )), listOf("תנ״ך", "תורה"), listOf("Tanakh", "Torah"))
        val commentarySchema = Json.parseToJsonElement("""
            {"title":"Abarbanel on Torah","heTitle":"אברבנאל על תורה","nodeType":"SchemaNode",
             "nodes":[{"title":"Deuteronomy","heTitle":"דברים","key":"Deuteronomy","nodes":[
               {"title":"","heTitle":"","key":"default","default":true,"nodeType":"JaggedArrayNode",
                "depth":3,"sectionNames":["Chapter","Verse","Paragraph"],"heSectionNames":["פרק","פסוק","פסקה"],
                "addressTypes":["Perek","Integer","Integer"]}]}]}
        """)
        val commentaryText = buildJsonObject { put("Deuteronomy", buildJsonObject { put("", sections(25, mapOf(
            21 to sections(1, mapOf(1 to segments(6, mapOf(
                1 to "כי ימצא חלל באדמה וגו' עד סוף הסדר", 2 to "פירוש מצות עגלה ערופה",
                3 to shoftimEnd, 4 to "ואמנם הספקות הנופלות בכתובים מהסדר הזה",
                5 to "הספק הי\"א באמרו לנכרי תשיך ולאחיך לא תשיך", 6 to "הספק הכ\"ו במצות עמלק",
            )))),
            25 to sections(17, mapOf(17 to segments(9, mapOf(
                1 to "פירוש מפורש ראשון", 2 to "פירוש מפורש שני", 3 to "פירוש מפורש שלישי", 4 to "פירוש מפורש רביעי",
                5 to kiTetzeiEnd, 6 to nextIntroduction, 7 to bikkurim,
                8 to "הספק הג' בוידוי מעשר", 9 to "הספק הי\"א בברכות ובקללות",
            )))),
        ))) }) }
        writeBook(root, "Abarbanel on Torah", "אברבנאל על תורה", commentarySchema, commentaryText,
            listOf("תנ״ך", "מפרשי תנ״ך", "אברבנאל"), listOf("Tanakh", "Rishonim on Tanakh", "Abarbanel"), "Deuteronomy")
        Files.writeString(root.resolve("links/links0.csv"), """
            |Citation 1,Citation 2,Conection Type,Text 1,Text 2,Category 1,Category 2,Char Level Data 1,Char Level Data 2,Suppression Mask 1,Suppression Mask 2
            |"Kiddushin 29a:5","Tosafot Ri HaZaken on Kiddushin 29a:3","Commentary","","","","","","",0,0
            |"Deuteronomy 21:1","Abarbanel on Torah, Deuteronomy 21:1:1","Commentary","","","","","","",0,0
            |"Deuteronomy 25:17","Abarbanel on Torah, Deuteronomy 25:17:5","Commentary","","","","","","",0,0
            |"Deuteronomy 25:17-19","Abarbanel on Torah, Deuteronomy 25:17:1-4","Commentary","","","","","","",0,0
        """.trimMargin())
    }

    @Test
    fun fullImportPreservesExplicitCoverageWithoutGuessingSiblingContinuity() = runBlocking {
        val root = Files.createTempDirectory("sefaria-explicit-paragraphs")
        var repositoryToClose: SeforimRepository? = null
        try {
            fixture(root)
            val reader = SefariaBookPayloadReader(Json { ignoreUnknownKeys = true }, co.touchlab.kermit.Logger.withTag("expected"))
            val expected = reader.readBooksInParallel(root.resolve("json"), root.resolve("schemas"), reader.buildSchemaLookup(root.resolve("schemas")))
                .associate { it.heTitle to it.lines }
            val db = root.resolve("import.db")
            importInIsolatedJvm(root, db)
            val driver = JdbcSqliteDriver("jdbc:sqlite:$db")
            val repo = SeforimRepository(db.toString(), driver)
            repositoryToClose = repo
            fun query(sql: String, selectedDriver: JdbcSqliteDriver = driver): List<List<String?>> = selectedDriver.getConnection().createStatement().use { st ->
                st.executeQuery(sql).use { rs -> buildList {
                    while (rs.next()) add((1..rs.metaData.columnCount).map { rs.getString(it) })
                } }
            }
            fun lineId(content: String): Long = query("SELECT id FROM line WHERE content='${content.replace("'", "''")}'")
                .single().single()!!.toLong()
            val immutableBefore = query("SELECT id,bookId,lineIndex,content,heRef FROM line ORDER BY id")
            assertEquals(expected.keys, query("SELECT title FROM book").map { it.single()!! }.toSet())
            for ((title, lines) in expected) {
                assertEquals(lines, query("SELECT content FROM line WHERE bookId=(SELECT id FROM book WHERE title='${title.replace("'", "''")}') ORDER BY lineIndex").map { it.single() }, title)
            }
            assertEquals(4L, query("SELECT COUNT(*) FROM link").single().single()!!.toLong())
            assertEquals(listOf(listOf("COMMENTARY", "4")), query("SELECT ct.name,COUNT(*) FROM link l JOIN connection_type ct ON ct.id=l.connectionTypeId GROUP BY ct.name"))
            // Independent Ri29a:4 and both following introductions have no
            // direct target link and no target-side coverage, despite sharing
            // the preceding anchor's numeric parent with no intervening heading.
            for (content in listOf(women, shoftimEnd, nextIntroduction, bikkurim,
                "ואמנם הספקות הנופלות בכתובים מהסדר הזה", "הספק הג' בוידוי מעשר", "הספק הי\"א בברכות ובקללות")) {
                val id = lineId(content)
                assertTrue(query("SELECT id FROM link WHERE targetLineId=$id").isEmpty(), content)
                assertTrue(query("SELECT linkId FROM link_coverage WHERE lineId=$id AND side=1").isEmpty(), content)
            }
            val closedAnchor = lineId(kiTetzeiEnd)
            assertEquals(1, query("SELECT id FROM link WHERE targetLineId=$closedAnchor").size)
            assertTrue(query("SELECT lr.linkId FROM link_range lr JOIN link l ON l.id=lr.linkId WHERE l.targetLineId=$closedAnchor AND lr.side=1").isEmpty())
            // Legitimate explicit source25:17-19 / target25:17:1-4 ranges stay
            // intact, with exactly two source and three target coverage rows.
            assertEquals(listOf(listOf("0", "2"), listOf("1", "3")), query("SELECT side,COUNT(*) FROM link_coverage GROUP BY side ORDER BY side"))
            assertEquals(2, query("SELECT linkId,side FROM link_range").size)
            assertEquals(listOf(listOf("0", expected.getValue("דברים").single { it.endsWith("תמחה את זכר עמלק") }), listOf("1", "פירוש מפורש רביעי")),
                query("SELECT lr.side,e.content FROM link_range lr JOIN line e ON e.id=lr.endLineId ORDER BY lr.side"))
            val replayDb = root.resolve("replay.db")
            importInIsolatedJvm(root, replayDb)
            val replay = JdbcSqliteDriver("jdbc:sqlite:$replayDb")
            try {
                assertEquals(immutableBefore, query("SELECT id,bookId,lineIndex,content,heRef FROM line ORDER BY id", replay))
                for (tableAndOrder in listOf("link ORDER BY id", "link_range ORDER BY linkId,side", "link_coverage ORDER BY lineId,linkId,side")) {
                    assertEquals(query("SELECT * FROM $tableAndOrder"), query("SELECT * FROM $tableAndOrder", replay))
                }
            } finally { replay.close() }
        } finally {
            repositoryToClose?.close()
            Files.walk(root).use { paths -> paths.sorted(java.util.Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }
    companion object {
        /** Isolates the importer's process-global ForDB cache with a local, pinned fixture archive. */
        internal fun importInIsolatedJvm(export: Path, db: Path) {
            val archive = export.resolve("fixture-fordb.zip")
            ZipOutputStream(Files.newOutputStream(archive)).use { zip ->
                for (name in FOR_DB_CSV_FILES.values) {
                    zip.putNextEntry(ZipEntry("ForDB/$name").apply { time = 0L })
                    zip.closeEntry()
                }
            }
            val sha = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(archive))
                .joinToString("") { "%02x".format(it) }
            val urls = generateSequence(SefariaExplicitParagraphLinksTest::class.java.classLoader) { it.parent }
                .filterIsInstance<URLClassLoader>().flatMap { it.urLs.asSequence() }
                .filter { it.protocol == "file" }.map { Path.of(it.toURI()).toString() }
            val classpath = (urls + System.getProperty("java.class.path").split(java.io.File.pathSeparator).asSequence())
                .distinct().joinToString(java.io.File.pathSeparator)
            val log = export.resolve("import.log")
            val process = ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx512m",
                "--enable-native-access=ALL-UNNAMED", "-DforDbArchive=$archive", "-DforDbSha256=$sha", "-cp", classpath,
                SefariaExplicitParagraphLinksTest::class.java.name, export.toString(), db.toString(),
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
            try {
                SefariaDirectImporter(Path.of(args[0]), repo, InMemoryIdAllocator.load(path = null)).import()
            } finally { repo.close() }
        }
    }

}
