package io.github.kdroidfilter.seforimlibrary.common.ids

/**
 * Prefix of the build-state keys (`id_lookup`, kind `category`) that renameCategories
 * gives the leaves it creates for book_moves.csv destinations. It keeps them apart from
 * the plain-path keys of the Sefaria and Otzaria stages: a Sefaria key is its
 * pre-rename path, so sharing that namespace could renumber a live category.
 *
 * A folder can be both: a book_moves leaf that the Otzaria stage also puts books in.
 * The two keys of such a folder are kept on one id (see
 * [IdAllocatorBindings.alignWithBookMoveLeaf] and [IdAllocatorBindings.upsertOtzariaCategory],
 * and `BookMoveLeafIds.leafId` in renameCategories), so the folder keeps its id when
 * book_moves.csv rows into it are added or removed.
 */
const val BOOK_MOVE_LEAF_KEY_PREFIX = "book_moves:"

/**
 * The category-title comparison the Otzaria stage uses to put a folder into an
 * existing category (`Generator.findExistingCategory`). Two paths that compare equal
 * under [comparablePath] are the same folder in the built DB.
 */
object CategoryLabels {
    /** Normalizes quote variants to Hebrew gershayim/geresh and collapses whitespace. */
    fun normalize(raw: String): String {
        var s = raw.trim()
        // Normalize common quote variants to Hebrew gershayim/geresh
        s = s.replace('\u201C', '"').replace('\u201D', '"')
        s = s.replace('\u2018', '\'').replace('\u2019', '\'')
        s = s.replace("\"", "״")
        s = s.replace("''", "״")
        s = s.replace("׳׳", "״")
        s = s.replace("`", "׳")
        s = s.replace("\u05f3", "׳")
        s = s.replace("\\s+".toRegex(), " ").trim()
        return s
    }

    /** [normalize], without any quote marks or a trailing corpus suffix ("על התורה" etc.). */
    fun comparable(raw: String): String {
        fun stripCorpusSuffix(s: String): String {
            val pattern = "(?i)\\s+על\\s+(התנ\"ך|התורה|התלמוד|המשנה|תנך|תורה|תלמוד|משנה)$".toRegex()
            return s.replace(pattern, "").trim()
        }

        val base = normalize(raw)
            .replace("״", "")
            .replace("\"", "")
            .replace("׳", "")
            .replace("'", "")
            .replace("\\s+".toRegex(), " ")
            .trim()
        return stripCorpusSuffix(base)
    }

    /** [comparable] applied to every segment of a `/`-separated category path. */
    fun comparablePath(path: String): String =
        path.split('/').map { comparable(it) }.filter { it.isNotEmpty() }.joinToString("/")
}
