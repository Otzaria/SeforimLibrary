package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import kotlin.io.path.writeText
import kotlin.test.assertEquals

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
        val t = titles(record("segal", "דוד הלוי סגל", "ט\"ז"))
        assertEquals("דוד הלוי סגל", t.displayName("segal", "דוד הלוי סגל"))
    }

    @Test
    fun `a fuller name is not an honorific`() {
        // "אברהם אבן עזרא" completes the name; it does not title it.
        val t = titles(record("ibn-ezra", "אבן עזרא", "אברהם אבן עזרא"))
        assertEquals("אבן עזרא", t.displayName("ibn-ezra", "אבן עזרא"))
    }

    @Test
    fun `a suffix is not an honorific`() {
        val t = titles(record("h", "נפתלי צבי הורוביץ", "נפתלי צבי הורוביץ מרופשיץ"))
        assertEquals("נפתלי צבי הורוביץ", t.displayName("h", "נפתלי צבי הורוביץ"))
    }

    @Test
    fun `trailing punctuation is not an honorific`() {
        val t = titles(record("a", "אהרן הלוי", "אהרן הלוי,"))
        assertEquals("אהרן הלוי", t.displayName("a", "אהרן הלוי"))
    }

    @Test
    fun `a partial word match is rejected`() {
        // "יעקב לנדא" is a substring of "יעקב לנדאו" but not a prefix-honorific of it.
        val t = titles(record("landa", "יעקב לנדא", "יעקב לנדאו"))
        assertEquals("יעקב לנדא", t.displayName("landa", "יעקב לנדא"))
    }

    // --- the file is optional and the data is messy ---

    @Test
    fun `unknown slug keeps the schema name`() {
        val t = titles(record("known", "א", "רבי א"))
        assertEquals("ב", t.displayName("missing", "ב"))
    }

    @Test
    fun `null slug keeps the schema name`() {
        val t = titles(record("known", "א", "רבי א"))
        assertEquals("ב", t.displayName(null, "ב"))
    }

    @Test
    fun `a missing authors file leaves every name untouched`() {
        val empty = SefariaAuthorTitles.load(tmp.newFolder().toPath(), json, logger)
        assertEquals("אברהם יצחק הכהן קוק", empty.displayName("kook", "אברהם יצחק הכהן קוק"))
    }

    @Test
    fun `an unreadable authors file leaves every name untouched`() {
        val root = tmp.newFolder().toPath()
        root.resolve(SefariaAuthorTitles.FILE_NAME).writeText("{ not json")
        val t = SefariaAuthorTitles.load(root, json, logger)
        assertEquals("א", t.displayName("kook", "א"))
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
