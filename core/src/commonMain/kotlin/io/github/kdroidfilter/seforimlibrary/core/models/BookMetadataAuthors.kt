package io.github.kdroidfilter.seforimlibrary.core.models

/**
 * The author names this record declares, in reading order.
 *
 * [BookMetadata.authors] is used when it holds at least one non-blank name;
 * otherwise [BookMetadata.author] is. An empty or all-blank `authors` must not
 * erase a perfectly good `author` — hence the emptiness check rather than a
 * plain null check.
 *
 * Names are never split on commas: 95 existing records are in catalogue order
 * ("אלגאזי, ישראל יעקב בן יום טוב"), where the comma is part of one name.
 */
fun BookMetadata.resolveAuthorNames(): List<String> {
    val fromList = authors.orEmpty().map { it.trim() }.filter { it.isNotEmpty() }
    if (fromList.isNotEmpty()) return fromList.distinct()
    return listOfNotNull(author?.trim()).filter { it.isNotEmpty() }
}
