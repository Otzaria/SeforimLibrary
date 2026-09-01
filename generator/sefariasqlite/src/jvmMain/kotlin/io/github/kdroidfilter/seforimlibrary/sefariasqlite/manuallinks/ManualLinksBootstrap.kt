package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

internal object ManualLinksBootstrap {
    fun nationalLibraryHeRef(heRef2: String, expectedTargetTitle: String): String {
        val parts = heRef2.split(", ")
        require(parts.size == 4 && parts[2].startsWith("פרק ") && parts[2].length > 4 && parts[3].isNotEmpty()) {
            "Malformed National Library heRef_2"
        }
        return "$expectedTargetTitle ${parts[2].removePrefix("פרק ")}, ${parts[3]}"
    }

    fun moreBooksHeRef(heRef2: String): String = heRef2.trimEnd { it == ',' || it == ' ' }

    /** Dicta stores the verbatim Sefaria heRef; only its exact title boundary is re-proved. */
    fun dictaHeRef(heRef2: String, expectedTargetTitle: String): String {
        require(heRef2 == heRef2.trim() && !heRef2.endsWith(',')) { "Malformed Dicta heRef_2" }
        require(heRef2.startsWith("$expectedTargetTitle ") && heRef2.length > expectedTargetTitle.length + 1) {
            "Dicta heRef_2 does not open with the Sefaria heTitle '$expectedTargetTitle'"
        }
        return heRef2
    }
}
