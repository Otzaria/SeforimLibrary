package io.github.kdroidfilter.seforimlibrary.core.text

/**
 * Nikud and te'amim. U+05BE (maqaf) is deliberately outside both ranges: it is
 * a joining character in names like `ברוך תאומים-פרנקל`, not a vowel.
 */
private val AUTHOR_NAME_NIKUD = Regex("[\\u0591-\\u05BD\\u05BF-\\u05C7]")

/** Bidi controls. They survive copy-paste from the source catalogues and render as nothing. */
private val AUTHOR_NAME_BIDI = Regex("[\\u200E\\u200F\\u202A-\\u202E\\u2066-\\u2069\\uFEFF]")

/** `\s` misses NBSP and the typographic spaces, which do occur in the data. */
private val AUTHOR_NAME_WHITESPACE = Regex("[\\s\\u00A0\\u2000-\\u200B\\u202F\\u205F\\u3000]+")

/**
 * [raw] with nikud, bidi marks and redundant whitespace removed.
 *
 * The `author` table keys on `name`, so two spellings of one person are two
 * authors — and a search by author then finds only half his books. Every corpus
 * spells the same man differently: Sefaria writes `חיים בן עטר` where Dicta
 * writes `חיים בן עֲטַר`, and `מנחם מנדל שניאורסון` differs from
 * `מנחם מנדל שניאורסון‏` only by a mark nothing renders. Those splits are
 * pure noise, and this removes them before the name reaches the id allocator.
 *
 * It does NOT decide that two different spellings are one person:
 * `יעקב בן יעקב משה מליסא` and `רבי יעקב בן יעקב משה לורברבוים מליסא` need a
 * human to say so, and that judgement lives in `ForDB/sefaria_author_changes.csv`.
 *
 * Gershayim and geresh are left alone — unlike a vowel point they are part of
 * how the name is written, and folding `״` into `"` would rewrite names that
 * are already consistent within their corpus.
 */
fun normalizeAuthorName(raw: String): String =
    AUTHOR_NAME_WHITESPACE
        .replace(AUTHOR_NAME_BIDI.replace(AUTHOR_NAME_NIKUD.replace(raw, ""), ""), " ")
        .trim()
