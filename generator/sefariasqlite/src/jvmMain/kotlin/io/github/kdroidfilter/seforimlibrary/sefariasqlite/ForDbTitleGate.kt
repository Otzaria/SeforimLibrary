package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import java.sql.Connection

/** A book whose title is no longer its heRef: a prefixed display title or a book_renames.csv rename. */
internal data class RetitledBook(val id: Long, val title: String, val heRef: String)

/** Every [RetitledBook] in the DB, compared trimmed like the ForDB matchers. */
internal fun loadRetitledBooks(conn: Connection): List<RetitledBook> =
    conn.createStatement().use { st ->
        st.executeQuery(
            "SELECT id, title, heRef FROM book WHERE heRef IS NOT NULL AND trim(title) <> trim(heRef) ORDER BY id",
        ).use { rs ->
            buildList {
                while (rs.next()) add(RetitledBook(rs.getLong(1), rs.getString(2).trim(), rs.getString(3).trim()))
            }
        }
    }

/**
 * Fails when a ForDB [file] names a book only by its former title (its heRef): that row
 * matches no book, so its data would be silently dropped. Exact matching only; a row
 * whose book also has a row under its current title is a stale duplicate and passes.
 */
internal fun requireForDbUsesDisplayTitles(
    file: String,
    rowTitles: Collection<String>,
    bookTitles: Set<String>,
    retitledBooks: List<RetitledBook>,
) {
    val titles = rowTitles.toSet()
    val byHeRef = retitledBooks.groupBy { it.heRef }
    val stale = titles.filter { it !in bookTitles }.sorted().flatMap { former ->
        byHeRef[former].orEmpty().filter { it.title !in titles }.map { "'$former' → '${it.title}'" }
    }
    check(stale.isEmpty()) {
        "$file names ${stale.size} book(s) by their former title, so their rows would match nothing: " +
            "${stale.joinToString()}. Update ForDB to the display titles the generator now writes."
    }
}
