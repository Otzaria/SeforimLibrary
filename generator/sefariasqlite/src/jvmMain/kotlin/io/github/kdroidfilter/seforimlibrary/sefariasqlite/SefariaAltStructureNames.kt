package io.github.kdroidfilter.seforimlibrary.sefariasqlite

/**
 * Hebrew display name of a Sefaria alt structure.
 *
 * Sefaria gives every alt structure the book's own heTitle, so a book with two
 * structures (Zohar: Daf + Essay, Yerushalmi: Venice + Vilna) showed two
 * identical "ספר הזהר" nodes. The structure key is the only thing that tells
 * them apart, so it becomes the name — like the generator's own "סעיפים".
 */
internal object SefariaAltStructureNames {
    private val HEBREW_BY_KEY = mapOf(
        "30 Day Cycle" to "לפי ימי החודש",
        "Authors" to "מחברים",
        "Book" to "ספרים",
        "Chamber" to "בתים",
        "Chapter" to "פרקים",
        "Chapters" to "פרקים",
        "Compositions" to "חיבורים",
        "Contents" to "תוכן",
        "Daf" to "דפים",
        "Essay" to "מאמרים",
        "Gate" to "שערים",
        "Hilchot" to "הלכות",
        "Letter" to "אותיות",
        "Parasha" to "פרשות",
        "Pillars" to "עמודים",
        "Remez" to "רמזים",
        "Section" to "חלקים",
        "Siman" to "סימנים",
        "Tanaim and Amoraim" to "תנאים ואמוראים",
        "Tikkunim" to "תקונים",
        "Topic" to "נושאים",
        "Vavim and Amudim" to "ווים ועמודים",
        "Venice" to "דפוס ונציה",
        "Vilna" to "דפוס וילנא",
    )

    fun forKey(key: String): String? = HEBREW_BY_KEY[key]

    /**
     * A structure heTitle that Sefaria actually wrote for the structure (it
     * differs from the book's) is kept; an unknown key keeps what Sefaria gave.
     */
    fun heTitle(key: String, structureHeTitle: String?, bookHeTitle: String?): String? {
        val isBookTitle = structureHeTitle.isNullOrBlank() || structureHeTitle == bookHeTitle
        if (!isBookTitle) return structureHeTitle
        return forKey(key) ?: structureHeTitle
    }
}
