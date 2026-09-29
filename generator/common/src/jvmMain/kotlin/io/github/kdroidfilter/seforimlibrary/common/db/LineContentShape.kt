package io.github.kdroidfilter.seforimlibrary.common.db

import java.sql.Connection

/**
 * Where a finished `seforim.db` keeps line text: `line.content` up to schema 5,
 * the one-row-per-line `line_content` table from schema 6 on.
 *
 * Readers that run on the finished DB (after `splitLineContent`) must accept both,
 * because the same code also reads older release DBs.
 */
object LineContentShape {

    /** One row, one column: 1 when the DB keeps line text in `line_content`, else 0. */
    const val DETECT_SPLIT_SQL: String =
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='line_content') " +
            "AND NOT EXISTS(SELECT 1 FROM pragma_table_info('line') WHERE name='content')"

    fun isSplit(conn: Connection): Boolean =
        conn.createStatement().use { st ->
            st.executeQuery(DETECT_SPLIT_SQL).use { rs -> rs.next() && rs.getInt(1) == 1 }
        }

    /** SQL expression for the text of the `line` row aliased [lineAlias]. */
    fun contentExpr(split: Boolean, lineAlias: String = "l"): String =
        if (split) "lc.content" else "$lineAlias.content"

    /** JOIN clause [contentExpr] needs (empty for the schema-5 shape). */
    fun contentJoin(split: Boolean, lineAlias: String = "l"): String =
        if (split) "JOIN line_content lc ON lc.id = $lineAlias.id" else ""
}
