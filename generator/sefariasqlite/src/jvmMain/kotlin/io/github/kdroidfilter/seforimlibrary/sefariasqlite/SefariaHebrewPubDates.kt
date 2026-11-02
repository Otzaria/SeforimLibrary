package io.github.kdroidfilter.seforimlibrary.sefariasqlite

// Sefaria's pubDate is Gregorian, which spans two Hebrew years; pub_dates_he.tsv
// holds the exact Hebrew year where a catalog confirms it.
private val hebrewPubDates: Map<Pair<String, String>, String> by lazy {
    val stream = object {}.javaClass.classLoader
        ?.getResourceAsStream("pub_dates_he.tsv") ?: return@lazy emptyMap()
    stream.bufferedReader(Charsets.UTF_8).useLines { lines ->
        lines.filter { it.isNotBlank() && !it.startsWith("#") }
            .map { it.split('	') }
            .filter { it.size >= 3 }
            .associate { (title, gregorian, hebrew) -> (title to gregorian) to hebrew }
    }
}

// Keyed by Sefaria's own heTitle (book.heRef): display titles and later renames
// change book.title only.
internal fun hebrewPubDate(heTitle: String, date: String): String =
    hebrewPubDates[heTitle to date] ?: date
