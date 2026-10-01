package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.Line
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The shapes below are real lines of the v30 candidate: Chavruta (H) from otzaria-library,
 * Talmud (G) from Sefaria, cut to the lines each case needs.
 */
class HavroutaRealignTest {

    private fun talmud(vararg contents: String) =
        contents.mapIndexed { i, c -> Line(id = 100L + i, bookId = 1, lineIndex = i, content = c) }

    private fun row(havroutaIndex: Int, content: String, daf: String, target: Int?) =
        QuoteRow(Line(id = 1000L + havroutaIndex, bookId = 2, lineIndex = havroutaIndex, content = content),
            daf, extractBoldText(content), target)

    private fun oneDaf(lines: List<Line>, ref: String = "x") = listOf(DafSection(ref, 0, lines.size))

    // ---- bold extraction

    @Test
    fun `bold wrapped around small is read (ערכין 1057)`() {
        assertEquals("חדא אמצורע עני ומדירו עני וחדא למעוטי", extractBoldText("ומשנינן: מה שאמר התנא: אבל בקרבנות \"אינו כן\", לא הולך על שתי הדוגמאות ששנה התנא במצורע: א. \"היה מצורע עני, מביא קרבן עני\". ב. \"היה מצורע עשיר מביא קרבן עשיר\", אלא <small>(</small><b><small>חדא אמצורע עני ומדירו עני וחדא למעוטי</small></b> <small>- רש\"י לא גרס כן)</small>"))
    }

    @Test
    fun `a small marker inside the bold is dropped, the quote around it is kept (פסחים 1837)`() {
        val text = extractBoldText("<b>אחד איסור אכילה ואחד איסור הנאה <small>(משמע)</small> עד שיפרט לך הכתוב</b>")
        assertEquals("אחד איסור אכילה ואחד איסור הנאה עד שיפרט לך הכתוב", text.replace(Regex("\\s+"), " "))
    }

    @Test
    fun `a bold page marker between two quote spans is dropped (כתובות 4217)`() {
        assertEquals("עבד עושה מעשה מחט שלשים", extractBoldText("ואם הרג השור <b>עבד עושה</b> <span style=\"color:Gray;\"><b><small><small>(תחילת העמוד)</small></small></b></span> <b>מעשה מחט</b> - שאינו שוה אלא מעט - אף הוא משלם <b>שלשים</b> סלעים, והיכן הוא החילוק שביניהם!?").replace(Regex("\\s+"), " "))
    }

    @Test
    fun `bold inside bold keeps reading the inner spans only (מועד קטן 38)`() {
        // the outer <b> wraps the explanation "ומנין", "צמאון" too; the inner spans are the Gemara
        assertEquals("ומאי משמע דהאי בית השלחין לישנא דצחותא היא", extractBoldText("<b><b>ומאי</b> ומנין <b>משמע דהאי</b> \"<b>בית השלחין</b>\" - <b>לישנא דצחותא</b> צמאון <b>היא</b>?</b>"))
    }

    @Test
    fun `the enhanced normalization drops Sefaria's opening tags and dashes`() {
        assertEquals("מעשה מחט שלשים", enhancedNormalize("מַעֲשֵׂה מַחַט — שְׁלֹשִׁים?!"))
        assertEquals("האיש מקדש בו ובשלוחו", enhancedNormalize("<big><strong>הָאִישׁ מְקַדֵּשׁ</strong></big> בּוֹ וּבִשְׁלוּחוֹ."))
    }

    // ---- realignment

    @Test
    fun `a link that missed its quote moves to the line that holds it (כתובות 9379)`() {
        val g = talmud("<h2>דף פה.</h2>", "אֶלָּא לַישְׁמְעִינַן כְּתוּבָּה נַעֲשֵׂית מוֹתָר לַחֲבֶרְתָּהּ, וַאֲנָא יָדַעְנָא מִשּׁוּם דְּאַחַת בְּחַיָּיו וְאַחַת בְּמוֹתוֹ יֵשׁ לָהֶן כְּתוּבַּת בְּנִין דִּכְרִין!", "אִי אַשְׁמְעִינַן הָכִי, הֲוָה אָמֵינָא: כְּגוֹן שֶׁנָּשָׂא שָׁלֹשׁ נָשִׁים, וָמֵתוּ שְׁתַּיִם בְּחַיָּיו וְאַחַת בְּמוֹתוֹ. וְהָךְ דְּמָיֵית לְאַחַר מִיתָה — יוֹלֶדֶת נְקֵבָה הִיא, וְלָאו בַּת יְרוּשָּׁה הִיא.")
        val rows = listOf(
            row(9377, "<b>בשלמא אי אשמעינן</b> [הניחא אם היה משמיענו מר זוטרא רק] ש<b>\"אחת בחייו ואחת במותו יש להן כתובת בנין דיכרין</b>\", <b>ולא אשמעינן</b> [ולא היה משמיענו מר זוטרא] שה<b>כתובה</b> של זו שבמותו <b>נעשית מותר לחברתה</b>.", "x", 1),
            row(9379, "<b>אלא</b> תיקשי: <b>לישמעינן</b> [ישמיענו מר זוטרא רק] ש<b>\"כתובה נעשית מותר לחברתה</b>\", <b>ואנא ידענא</b> [ומעצמי אני יודע] <b>משום דאחת בחייו ואחת במותו יש להן \"כתובת בנין דכרין\"!?</b>", "x", 2),
            row(9381, "ומשנינן: <b>אי אשמעינן הכי</b> ש\"כתובה נעשית מותר לחברתה\" לא היינו יודעים ש\"אחת בחייו ואחת במותו\" יש להן \"כתובת בנין דכרין\", כי <b>הוה אמינא</b> שבאופן זה הוא שנעשית כתובה מותר לחברתה: <b>כגון שנשא שלש נשים, ומתו שתים בחייו</b> ובאין בני זו וזו ליטול כתובת אמן מדין \"כתובת בנין דכרין\", <b>ו</b>ה<b>אחת</b> מתה <b>במותו</b> ויורשיה נוטלין כתובת אמן כשאר יורשי בעל חוב, <b>והך דמיית לאחר מיתה יולדת נקבה היא ולאו בת ירושה היא</b> [זו שמתה במותו לא היו לה בנים זכרים אלא נקבה ילדה, והיא זו שבאה לרשת את כתובת אמה].", "x", 2),
        )
        assertEquals(1, realignQuotes(rows, g, oneDaf(g)))
        assertEquals(listOf(1, 1, 2), rows.map { it.talmudLineIndex })
    }

    @Test
    fun `a quote said twice in the daf stays with the reading order (קידושין 6675)`() {
        // 1279 (the Mishnah) holds the whole quote, but the Gemara 1284 that cites it is
        // where the neighbours are; the window between the anchors excludes 1279
        val g = talmud("<h2>דף מד:</h2>", "<big><strong>הָאִישׁ מְקַדֵּשׁ</strong></big> בּוֹ וּבִשְׁלוּחוֹ. הָאִשָּׁה מִתְקַדֶּשֶׁת בָּהּ וּבִשְׁלוּחָהּ. הָאִישׁ מְקַדֵּשׁ אֶת בִּתּוֹ כְּשֶׁהִיא נַעֲרָה, בּוֹ וּבִשְׁלוּחוֹ.", "אֲבָל בְּהָא אִיסּוּרָא לֵית בַּהּ, כִּדְרֵישׁ לָקִישׁ, דְּאָמַר רֵישׁ לָקִישׁ: טָב לְמֵיתַב טַן דּוּ מִלְּמֵיתַב אַרְמְלוּ.", "הָאִישׁ מְקַדֵּשׁ אֶת בִּתּוֹ כְּשֶׁהִיא נַעֲרָה. כְּשֶׁהִיא נַעֲרָה – אִין, כְּשֶׁהִיא קְטַנָּה – לָא. מְסַיַּיע לֵיהּ לְרַב, דְּאָמַר רַב יְהוּדָה אָמַר רַב וְאִיתֵּימָא רַבִּי אֶלְעָזָר: אָסוּר לְאָדָם שֶׁיְּקַדֵּשׁ אֶת בִּתּוֹ כְּשֶׁהִיא קְטַנָּה, עַד שֶׁתִּגְדַּל וְתֹאמַר: ״בִּפְלוֹנִי אֲנִי רוֹצָה״.")
        val rows = listOf(
            row(6674, "משל הוא שהנשים אומרות על בעל שאינו מוצלח: \"בכל זאת <b>טב למיתב טן דו</b> [טוב לשבת שני גופים יחדיו] <b>מלמיתב ארמלו</b> [מאשר לשבת אלמנה ללא בעל] \", ואם כן, אף אם תראה בו דבר מגונה, לא תסור אהבתה ממנו.", "x", 2),
            row(6675, "שנינו במשנה: <b>האיש מקדש את בתו כשהיא נערה, בו ובשלוחו</b>:", "x", 3),
            row(6676, "ומפרשינן: <b>כשהיא נערה, אין</b>, אכן הוא מקדשה, ואילו <b>כשהיא קטנה לא</b> מקדש אותה האב; ולא שאין הוא יכול לקדשה, שהרי ודאי זכאי האב בקדושי בתו הקטנה, וגם יכול לקדשה על ידי שליח מכל שכן דנערה, שהרי היא יותר ברשותו מאשר נערה, אלא לומר שלא יעשה כן לכתחילה -", "x", 3),
        )
        assertEquals(0, realignQuotes(rows, g, oneDaf(g)))
        assertEquals(listOf(2, 3, 3), rows.map { it.talmudLineIndex })
    }

    @Test
    fun `a quote that runs over the daf heading links to the line after it (כתובות 4217)`() {
        val g = talmud("<h2>דף מ.</h2>", "<big><strong>גְּמָ׳</strong></big> וְאֵימָא: חֲמִשִּׁים סְלָעִים אָמַר רַחֲמָנָא, מִכֹּל מִילֵּי! אָמַר רַבִּי זֵירָא יֹאמְרוּ: בָּעַל בַּת מְלָכִים חֲמִשִּׁים, בָּעַל בַּת הֶדְיוֹטוֹת חֲמִשִּׁים?! אֲמַר לֵיהּ אַבָּיֵי: אִי הָכִי, גַּבֵּי עֶבֶד נָמֵי, יֹאמְרוּ: עֶבֶד נוֹקֵב מַרְגָּלִיּוֹת — שְׁלֹשִׁים, עֶבֶד עוֹשֶׂה", "<h2>דף מ:</h2>", "מַעֲשֵׂה מַחַט — שְׁלֹשִׁים?!")
        val dafs = listOf(DafSection("מ.", 0, 2), DafSection("מ:", 2, 4))
        val rows = listOf(row(4217, "ואם הרג השור <b>עבד עושה</b> <span style=\"color:Gray;\"><b><small><small>(תחילת העמוד)</small></small></b></span> <b>מעשה מחט</b> - שאינו שוה אלא מעט - אף הוא משלם <b>שלשים</b> סלעים, והיכן הוא החילוק שביניהם!?", "מ:", null))
        assertEquals(1, realignQuotes(rows, g, dafs))
        assertEquals(3, rows.single().talmudLineIndex)
    }

    @Test
    fun `out-of-order anchors widen the window to the anchors around them (נדרים 340)`() {
        val g = talmud("<h2>דף ו:</h2>", "וְרָבָא אָמַר: אֲנָא דַּאֲמַרִי אֲפִילּוּ לְרַבָּנַן. עַד כָּאן לָא קָאָמְרִי רַבָּנַן דְּלָא בָּעִינַן יָדַיִם מוֹכִיחוֹת אֶלָּא גַּבֵּי גֵּט,", "דְּאֵין אָדָם מְגָרֵשׁ אֶת אֵשֶׁת חֲבֵירוֹ, אֲבָל בְּעָלְמָא — מִי שָׁמְעַתְּ לְהוּ?", "מֵיתִיבִי: ״הֲרֵי הוּא עָלַי״, ״הֲרֵי זֶה [עָלַי]״ — אָסוּר, מִפְּנֵי שֶׁהוּא יָד לְקׇרְבָּן. טַעְמָא דְּאָמַר ״עָלַי״ — הוּא דְּאָסוּר, אֲבָל לָא אָמַר ״עָלַי״ — לָא. תְּיוּבְתָּא דְאַבָּיֵי!", "אָמַר לָךְ רָבָא: אֲנָא דַּאֲמַרִי — אֲפִילּוּ לְרַבָּנַן. עַד כָּאן לָא קָאָמְרִי רַבָּנַן דְּלָא בָּעִינַן יָדַיִם מוֹכִיחוֹת אֶלָּא גַּבֵּי גֵּט, דְּאֵין אָדָם מְגָרֵשׁ אֶת אֵשֶׁת חֲבֵירוֹ. אֲבָל בְּעָלְמָא בָּעִינַן יָדַיִם מוֹכִיחוֹת.")
        val rows = listOf(
            row(337, "<b>ורבא אמר: אנא דאמרי</b> - <b>אפילו לרבנן</b>.", "x", 1),
            row(339, "<b>עד כאן לא קאמרי רבנן דלא בעינן ידים מוכיחות</b> - <b>אלא גבי גט</b> היות <span style=\"color:Gray;\"><small><small>(תחילת העמוד)</small></small></span> <b>דאין אדם מגרש את אשת חבירו.</b> ולכן הלשון של הגט היא לשון כריתות מוחלטת גם אם לא יהיה כתוב \"מינאי\".", "x", 4),
            row(340, "<b>אבל בעלמא</b>, שאין את ההוכחה המוחלטת הזאת - <b>מי שמעת להו</b> שידים שאינן מוכיחות הויין ידים!? <small>(1)</small>", "x", 4),
            row(341, "<b>מיתיבי</b> לאביי הסובר שידים שאין מוכיחות הויין ידים, מברייתא:", "x", 3),
        )
        assertEquals(1, realignQuotes(rows, g, oneDaf(g)))
        assertEquals(listOf(1, 4, 2, 3), rows.map { it.talmudLineIndex })
    }

    @Test
    fun `a link that holds its quote never moves, even to a nearer copy`() {
        val g = talmud("<h2>דף ב.</h2>", "אמר רב יהודה אמר שמואל הלכה כרבי מאיר", "תניא נמי הכי אמר רב יהודה אמר שמואל הלכה כרבי מאיר")
        val rows = listOf(row(1, "<b>אמר רב יהודה אמר שמואל הלכה כרבי מאיר</b>", "x", 2))
        assertEquals(0, realignQuotes(rows, g, oneDaf(g)))
        assertEquals(2, rows.single().talmudLineIndex)
    }

    @Test
    fun `a short quote is not moved on the strength of a containing line`() {
        val g = talmud("<h2>דף ב.</h2>", "איתיביה רב פפא לאביי", "אמר ליה אביי לרב פפא")
        val rows = listOf(row(1, "<b>איתיביה רב פפא</b>", "x", 2))
        assertEquals(0, realignQuotes(rows, g, oneDaf(g)))
        assertEquals(2, rows.single().talmudLineIndex)
    }

    @Test
    fun `two lines that hold the quote in the window - the nearer to the first pass wins, then the lower`() {
        val q = "שמע מינה תלת שמע מינה"
        val g = talmud("<h2>דף ב.</h2>", q, "לא כלום", q, "לא כלום", q)
        val rows = listOf(row(1, "<b>$q</b>", "x", 2))
        assertEquals(1, realignQuotes(rows, g, oneDaf(g)))
        assertEquals(1, rows.single().talmudLineIndex)   // 1 and 3 are both one away from 2
    }
}
