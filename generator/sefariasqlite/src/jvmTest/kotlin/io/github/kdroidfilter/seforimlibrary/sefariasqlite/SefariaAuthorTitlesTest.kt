package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import kotlin.io.path.writeText
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * A book schema gives one bare Hebrew author name; authors.json holds every
 * other form Sefaria knows. Only an honorific *added in front of that same
 * name* may replace it — an acronym is an alias, not a title.
 */
class SefariaAuthorTitlesTest {
    @get:Rule
    val tmp = TemporaryFolder()

    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
    private val logger = Logger.withTag("test")

    /** Present in every fixture: if this one stops being upgraded, the file
     *  did not load, and the negative assertions around it mean nothing. */
    private val control = record("control-slug", "אברהם יצחק הכהן קוק", "הרב אברהם יצחק הכהן קוק")

    private fun assertLoaded(t: SefariaAuthorTitles) {
        assertEquals(
            "הרב אברהם יצחק הכהן קוק",
            t.displayName("control-slug", "אברהם יצחק הכהן קוק"),
            "control: authors.json did not load, so this test proves nothing",
        )
    }

    private fun titles(vararg records: String): SefariaAuthorTitles {
        val root = tmp.newFolder().toPath()
        root.resolve(SefariaAuthorTitles.FILE_NAME).writeText("[${records.joinToString(",")}]")
        return SefariaAuthorTitles.load(root, json, logger)
    }

    /** Names contain gershayim, so every value has to be JSON-escaped. */
    private fun esc(value: String) = value.replace("\\", "\\\\").replace("\"", "\\\"")

    private fun record(slug: String, vararg hebrew: String): String {
        val forms = hebrew.joinToString(",") { """{"text":"${esc(it)}","lang":"he"}""" }
        val primary = esc(hebrew.firstOrNull().orEmpty())
        return """{"slug":"${esc(slug)}","primaryHe":"$primary","primaryEn":"","titles":[$forms]}"""
    }

    // --- the honorifics we are here to add ---

    @Test
    fun `honorific prefix replaces the bare name`() {
        val t = titles(record("kook", "אברהם יצחק הכהן קוק", "הרב אברהם יצחק הכהן קוק"))
        assertEquals("הרב אברהם יצחק הכהן קוק", t.displayName("kook", "אברהם יצחק הכהן קוק"))
    }

    @Test
    fun `definite article on an acronym counts as an honorific`() {
        val t = titles(record("malbim", "מלבי\"ם", "המלבי\"ם"))
        assertEquals("המלבי\"ם", t.displayName("malbim", "מלבי\"ם"))
    }

    @Test
    fun `several honorific words in front are accepted`() {
        val t = titles(record("sacks", "יונתן זקס", "הרב לורד יונתן זקס"))
        assertEquals("הרב לורד יונתן זקס", t.displayName("sacks", "יונתן זקס"))
    }

    @Test
    fun `longest honorific form wins and the choice is order independent`() {
        val forward = titles(record("x", "יונתן זקס", "הרב יונתן זקס", "הרב לורד יונתן זקס"))
        val reversed = titles(record("x", "יונתן זקס", "הרב לורד יונתן זקס", "הרב יונתן זקס"))
        assertEquals("הרב לורד יונתן זקס", forward.displayName("x", "יונתן זקס"))
        assertEquals("הרב לורד יונתן זקס", reversed.displayName("x", "יונתן זקס"))
    }

    // --- what must NOT happen ---

    @Test
    fun `an acronym never replaces the name`() {
        val t = titles(control, record("segal", "דוד הלוי סגל", "ט\"ז"))
        assertLoaded(t)
        assertEquals("דוד הלוי סגל", t.displayName("segal", "דוד הלוי סגל"))
    }

    @Test
    fun `a fuller name is not an honorific`() {
        // "אברהם אבן עזרא" completes the name; it does not title it.
        val t = titles(control, record("ibn-ezra", "אבן עזרא", "אברהם אבן עזרא"))
        assertLoaded(t)
        assertEquals("אבן עזרא", t.displayName("ibn-ezra", "אבן עזרא"))
    }

    @Test
    fun `a suffix is not an honorific`() {
        val t = titles(control, record("h", "נפתלי צבי הורוביץ", "נפתלי צבי הורוביץ מרופשיץ"))
        assertLoaded(t)
        assertEquals("נפתלי צבי הורוביץ", t.displayName("h", "נפתלי צבי הורוביץ"))
    }

    @Test
    fun `trailing punctuation is not an honorific`() {
        val t = titles(control, record("a", "אהרן הלוי", "אהרן הלוי,"))
        assertLoaded(t)
        assertEquals("אהרן הלוי", t.displayName("a", "אהרן הלוי"))
    }

    @Test
    fun `a partial word match is rejected`() {
        // "יעקב לנדא" is a substring of "יעקב לנדאו" but not a prefix-honorific of it.
        val t = titles(control, record("landa", "יעקב לנדא", "יעקב לנדאו"))
        assertLoaded(t)
        assertEquals("יעקב לנדא", t.displayName("landa", "יעקב לנדא"))
    }

    // --- the file is optional and the data is messy ---

    @Test
    fun `unknown slug keeps the schema name`() {
        val t = titles(control, record("known", "א", "רבי א"))
        assertLoaded(t)
        assertEquals("ב", t.displayName("missing", "ב"))
    }

    @Test
    fun `null slug keeps the schema name`() {
        val t = titles(control, record("known", "א", "רבי א"))
        assertLoaded(t)
        assertEquals("ב", t.displayName(null, "ב"))
    }

    @Test
    fun `a missing authors file leaves every name untouched`() {
        val empty = SefariaAuthorTitles.load(tmp.newFolder().toPath(), json, logger)
        assertEquals("אברהם יצחק הכהן קוק", empty.displayName("kook", "אברהם יצחק הכהן קוק"))
    }

    @Test
    fun `an unreadable authors file throws instead of degrading every name`() {
        val root = tmp.newFolder().toPath()
        root.resolve(SefariaAuthorTitles.FILE_NAME).writeText("{ not json")
        assertFailsWith<IllegalStateException> { SefariaAuthorTitles.load(root, json, logger) }
    }

    @Test
    fun `a non-object entry throws`() {
        val root = tmp.newFolder().toPath()
        root.resolve(SefariaAuthorTitles.FILE_NAME).writeText("""["just a string"]""")
        assertFailsWith<IllegalStateException> { SefariaAuthorTitles.load(root, json, logger) }
    }

    @Test
    fun `a duplicate slug throws rather than dropping one set of forms`() {
        val root = tmp.newFolder().toPath()
        root.resolve(SefariaAuthorTitles.FILE_NAME).writeText(
            "[${record("dup", "א", "רבי א")},${record("dup", "ב", "רבי ב")}]"
        )
        assertFailsWith<IllegalArgumentException> { SefariaAuthorTitles.load(root, json, logger) }
    }

    @Test
    fun `an honorific glued on without a space is rejected`() {
        val t = titles(control, record("g", "אברהם יצחק הכהן קוק", "הרבאברהם יצחק הכהן קוק"))
        assertLoaded(t)
        assertEquals("אברהם יצחק הכהן קוק", t.displayName("g", "אברהם יצחק הכהן קוק"))
    }

    @Test
    fun `the definite article is only accepted on an acronym`() {
        val t = titles(control, record("levi", "לוי", "הלוי"), record("m", "מלבי\"ם", "המלבי\"ם"))
        assertLoaded(t)
        // "הלוי" is a different name, not a titled "לוי".
        assertEquals("לוי", t.displayName("levi", "לוי"))
        assertEquals("המלבי\"ם", t.displayName("m", "מלבי\"ם"))
    }

    @Test
    fun `a non-breaking space does not hide the honorific`() {
        val t = titles(control, record("nb", "יונתן זקס", "הרב\u00A0יונתן זקס"))
        assertLoaded(t)
        assertEquals("הרב\u00A0יונתן זקס", t.displayName("nb", "יונתן זקס"))
    }

    @Test
    fun `all name forms are exposed for blacklist matching`() {
        val t = titles(control, record("sacks", "יונתן זקס", "הרב לורד יונתן זקס", "זקס"))
        assertEquals(
            listOf("יונתן זקס", "הרב לורד יונתן זקס", "זקס"),
            t.allNameForms("sacks"),
        )
        assertEquals(emptyList(), t.allNameForms("nobody"))
        assertEquals(emptyList(), t.allNameForms(null))
    }

    @Test
    fun `a slug with only english forms is not indexed`() {
        val root = tmp.newFolder().toPath()
        root.resolve(SefariaAuthorTitles.FILE_NAME).writeText(
            """[{"slug":"en-only","primaryHe":"","primaryEn":"A","titles":[{"text":"A","lang":"en"}]}]"""
        )
        val t = SefariaAuthorTitles.load(root, json, logger)
        assertEquals(emptyList(), t.allNameForms("en-only"))
        assertEquals("א", t.displayName("en-only", "א"))
    }

    @Test
    fun `nikud and bidi marks do not block the match`() {
        // Sefaria's primary form for Rabbi Akiva Eiger carries nikud.
        val t = titles(record("eiger", "עֲקִיבָא אֵיגֶר", "רבי עקיבא איגר"))
        assertEquals("רבי עקיבא איגר", t.displayName("eiger", "עקיבא איגר"))
    }

    @Test
    fun `gershayim variants do not block the match`() {
        val t = titles(record("m", "מלבי״ם", "המלבי\"ם"))
        assertEquals("המלבי\"ם", t.displayName("m", "מלבי\"ם"))
    }

    @Test
    fun `english titles are ignored`() {
        val root = tmp.newFolder().toPath()
        root.resolve(SefariaAuthorTitles.FILE_NAME).writeText(
            """[{"slug":"k","primaryHe":"א","primaryEn":"A",
                 "titles":[{"text":"Rabbi A","lang":"en"},{"text":"רבי א","lang":"he"}]}]"""
        )
        val t = SefariaAuthorTitles.load(root, json, logger)
        assertEquals("רבי א", t.displayName("k", "א"))
    }
}
