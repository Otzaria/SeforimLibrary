package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.LogWriter
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.StaticConfig
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * חברותא על שקלים never had a Gemara link: there is no Bavli שקלים, and the Vilna Shas
 * prints the Yerushalmi in its place. The book follows the Yerushalmi's perakim and
 * halachot, which Sefaria's "תלמוד ירושלמי שקלים" has too, so it is linked by those.
 */
class HavroutaYerushalmiTest {

    @Test
    fun `Hebrew numerals and ordinal words are read`() {
        assertEquals(1, hebrewNumber("א"))
        assertEquals(15, hebrewNumber("טו"))
        assertEquals(23, hebrewNumber("כ\"ג"))
        assertEquals(1, hebrewNumber("ראשון"))
        assertEquals(8, hebrewNumber("שמיני"))
        assertNull(hebrewNumber("x"))
        assertNull(hebrewNumber(""))
        assertNull(hebrewNumber("פרק")) // letters, but no numeral: they rise
        assertEquals(16, hebrewNumber("טז"))
    }

    @Test
    fun `the two spellings of one text give the same words`() {
        // Shas (Chavruta) against Sefaria's Yerushalmi, 1:1
        assertEquals(yerushalmiWords("באחד באדר משמיעין על השקלים"), yerushalmiWords("באחד באדר משמעין על השקלים"))
        assertEquals(yerushalmiWords("ויהי בחדש הראשון"), yerushalmiWords("ויהי בחודש הראשון"))
        assertEquals(yerushalmiWords("גף"), yerushalmiWords("גפ"))
        assertEquals(yerushalmiWords("נמכר"), yerushalmiWords("נמ‍כר"))
        // a stray one-letter word is dropped
        assertEquals(listOf("על", "הכלאמ"), yerushalmiWords("ו על הכלאים"))
    }

    private fun w(text: String) = text.split(' ')

    @Test
    fun `quotes are aligned in reading order, on the earlier line when it is a tie`() {
        val lines = listOf(w("אמר רב חננ טעמא דהדא"), w("תננ התמ כל המקדש"), w("אמר רב חננ טעמא דהדא שנאמר"))
        // The second quote holds on lines 0 and 2; after a quote on line 1 only line 2 is in order.
        assertEquals(listOf(1, 2), alignYerushalmiQuotes(listOf(w("כל המקדש"), w("רב חננ טעמא")), lines))
        // Alone, it takes the first.
        assertEquals(listOf(0), alignYerushalmiQuotes(listOf(w("רב חננ טעמא")), lines))
    }

    @Test
    fun `a quote matches only on half of its word pairs`() {
        val lines = listOf(w("א1 ב1 ג1 ד1 ה1"))
        assertEquals(listOf(0), alignYerushalmiQuotes(listOf(w("א1 ב1 ג1 זז")), lines)) // 2 of 3
        assertEquals(listOf<Int?>(null), alignYerushalmiQuotes(listOf(w("א1 ב1 זז חח טט")), lines)) // 1 of 4
        assertEquals(listOf<Int?>(null), alignYerushalmiQuotes(listOf(w("א1")), lines)) // no pair at all
    }

    @Test
    fun `a variant reading between two quotes of one line joins them, on half its words`() {
        val lines = listOf(w("א1 ב1 ג1 ד1 ה1 ו1 ז1"), w("ח1 ט1 י1"))
        val quotes = listOf(w("א1 ב1"), w("ג1 זז ה1"), w("ו1 ז1"), w("זז חח טט"), w("ח1 ט1"))
        // "ג1 זז ה1" has no pair of line 0 but two of its three words; "זז חח טט" none.
        assertEquals(listOf(0, 0, 0, null, 1), alignYerushalmiQuotes(quotes, lines))
        // with its neighbours on two lines, the variant stays unlinked
        assertEquals(listOf(0, null, 1), alignYerushalmiQuotes(listOf(w("א1 ב1"), w("ג1 זז ה1"), w("ח1 ט1")), lines))
    }

    private class Capture : LogWriter() {
        val lines = mutableListOf<Pair<Severity, String>>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += severity to message
        }
    }

    private val yerushalmi = listOf(
        "<h1>תלמוד ירושלמי שקלים</h1>",
        "<h2>פרק א</h2>",
        "<h3>הלכה א</h3>",
        "משנה באחד באדר משמעין על השקלים ועל הכלאים",                // 3
        "הלכה ולמה באחד באדר כדי שיביאו ישראל את שקליהן בעונתן",     // 4
        "<h3>הלכה ב</h3>",
        "משנה בחמשה עשר בו היו שולחנות יושבין במדינה",               // 6
        "<h2>פרק ב</h2>",
        "<h3>הלכה א</h3>",
        "משנה מצרפין שקלים לדרכונות מפני משוי הדרך",                // 9
    )

    private val chavruta = listOf(
        "<h1>חברותא על שקלים</h1>",
        "<h2>פרק ראשון - באחד באדר</h2>",
        "<b>באחד באדר</b> מבוא שאינו מקושר",                           // before the first halacha
        "<h3>דף ב.</h3>",
        "<big><b>הלכה א - מתניתין:</b></big>",
        "<b>באחד באדר משמיעין על השקלים</b>, מכריזים",                // 5 -> 3
        "<big><b>גמרא:</b></big>",
        "<b>ולמה באחד באדר</b>, ומבארת",                               // 7 -> 4
        "<b>כדי שיביאו ישראל את שקליהן בעונתן</b>",                    // 8 -> 4
        "<big><b>הלכה ב - מתניתין:</b></big>",
        "<h3>דף ב:</h3>",
        "<b>בחמשה עשר בו</b> היו יושבים",                              // 11 -> 6
        "<h2>פרק שני - מצרפין שקלים</h2>",
        "<big><b>הלכה א - מתניתין:</b></big>",
        "<b>מצרפין שקלים לדרכונות</b>",                               // 14 -> 9
        "<b>באחד באדר משמיעין</b> חוזר ומזכיר",                       // 15: perek א's text, not linked here
    )

    private fun run(yerushalmiTitle: String = "תלמוד ירושלמי שקלים"): Triple<Capture, List<String>, List<Pair<Int, Int>>> = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val sefaria = repo.insertSource("Sefaria")
        val otzaria = repo.insertSource("Otzaria")
        val catId = repo.insertCategory(Category(0, null, "תלמוד", level = 0, order = 1))
        val yId = repo.insertBook(Book(id = 1, categoryId = catId, sourceId = sefaria, title = yerushalmiTitle, heRef = yerushalmiTitle, totalLines = yerushalmi.size))
        val hId = repo.insertBook(Book(id = 2, categoryId = catId, sourceId = otzaria, title = "חברותא על שקלים", heRef = "חברותא על שקלים", totalLines = chavruta.size))
        repo.insertLinesBatch(yerushalmi.mapIndexed { i, c -> Line(id = 1L + i, bookId = yId, lineIndex = i, content = c) })
        repo.insertLinesBatch(chavruta.mapIndexed { i, c -> Line(id = 1000L + i, bookId = hId, lineIndex = i, content = c) })
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)
        ConnectionType.entries.forEach { bindings.upsertConnectionType(it.name) }
        val capture = Capture()
        val annotations = mutableListOf<String>()
        generateHavroutaLinks(repo, bindings, Logger(StaticConfig(Severity.Verbose, listOf(capture)), "t")) { annotations += it }
        val pairs = driver.executeQuery(
            null,
            "SELECT t.lineIndex, s.lineIndex FROM link k JOIN line s ON s.id = k.sourceLineId " +
                "JOIN line t ON t.id = k.targetLineId JOIN connection_type c ON c.id = k.connectionTypeId " +
                "WHERE k.sourceBookId = $yId AND k.targetBookId = $hId AND c.name = 'COMMENTARY' ORDER BY 1",
            { cur ->
                val out = mutableListOf<Pair<Int, Int>>()
                while (cur.next().value) out += cur.getLong(0)!!.toInt() to cur.getLong(1)!!.toInt()
                QueryResult.Value(out)
            },
            0,
        ).value
        driver.close()
        Triple(capture, annotations, pairs)
    }

    @Test
    fun `חברותא על שקלים is linked to the Yerushalmi by perek and halacha`() {
        val (capture, annotations, pairs) = run()
        assertEquals(listOf(5 to 3, 7 to 4, 8 to 4, 11 to 6, 14 to 9), pairs)
        assertTrue(
            capture.lines.any {
                it.second == "Processing: חברותא על שקלים -> תלמוד ירושלמי שקלים (by perek and halacha: 'שקלים' has no Bavli Gemara)"
            },
            capture.lines.toString(),
        )
        assertTrue(capture.lines.any { it.second == "Havrouta-Talmud: 1/1 books processed" }, capture.lines.toString())
        assertTrue(capture.lines.none { it.first == Severity.Warn }, capture.lines.toString())
        assertEquals(emptyList(), annotations)
    }

    @Test
    fun `without the Yerushalmi book the skip says why, once`() {
        val (capture, annotations, pairs) = run(yerushalmiTitle = "ירושלמי אחר")
        assertEquals(emptyList(), pairs)
        val warnings = capture.lines.filter { it.first == Severity.Warn }.map { it.second }
        assertContains(
            warnings.single { it.startsWith("No Talmud match for:") },
            "'שקלים' has no Bavli Gemara, and no Sefaria book is titled 'תלמוד ירושלמי שקלים'",
        )
        assertEquals(warnings, annotations)
    }
}
