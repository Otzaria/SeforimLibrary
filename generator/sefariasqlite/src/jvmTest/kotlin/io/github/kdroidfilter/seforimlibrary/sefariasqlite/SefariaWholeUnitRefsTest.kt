package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * [SefariaWholeUnitRefs] reproduces Sefaria's `get_talmud_perek_ref_set()` and
 * stands in for `get_parasha_ref_set()` with a structure-key proxy (the Parasha
 * TermSet is not exported). Covered here: the JSON-level predicate
 * (`match_templates` scope, English categories), the set assembly, and the
 * invariants that keep the proxy honest — an empty set would silently disable
 * the whole-unit coverage rule.
 */
class SefariaWholeUnitRefsTest {

    private fun node(
        wholeRef: String?,
        alone: Boolean = false,
        children: List<AltNodePayload> = emptyList(),
    ) = AltNodePayload(
        title = null, heTitle = null, wholeRef = wholeRef, refs = emptyList(),
        addressTypes = emptyList(), childLabel = null, addresses = emptyList(),
        skippedAddresses = emptyList(), startingAddress = null, offset = null,
        children = children, referenceableAlone = alone,
    )

    private fun payload(
        enTitle: String,
        categoriesEn: List<String>,
        structures: List<AltStructurePayload>,
        rawDependence: String? = null,
    ) = BookPayload(
        heTitle = enTitle, enTitle = enTitle, categoriesHe = emptyList(), lines = emptyList(),
        refEntries = emptyList(), headings = emptyList(), authors = emptyList(),
        description = null, heShortDesc = null, pubDates = emptyList(),
        altStructures = structures, categoriesEn = categoriesEn, rawDependence = rawDependence,
    )

    @Test
    fun collectsPerakimOnlyForAloneNodesInTalmudicCategories() {
        val refs = SefariaWholeUnitRefs.build(
            listOf(
                // Bavli: one citable-alone perek, one not.
                payload(
                    "Bava Batra", listOf("Talmud", "Bavli", "Seder Nezikin"),
                    listOf(
                        AltStructurePayload(
                            key = "Chapters", title = null, heTitle = null,
                            nodes = listOf(
                                node("Bava Batra 28a:1-60b:22", alone = true),
                                node("Bava Batra 61a:1-72b:5", alone = false),
                            )
                        )
                    )
                ),
                // Same shape outside the four categories → never a perek.
                payload(
                    "Shulchan Arukh, Choshen Mishpat", listOf("Halakhah", "Shulchan Arukh"),
                    listOf(
                        AltStructurePayload(
                            key = "Chapters", title = null, heTitle = null,
                            nodes = listOf(node("Shulchan Arukh, Choshen Mishpat 1:1-15", alone = true))
                        )
                    )
                ),
            )
        )

        assertEquals(setOf(canonicalCitation("Bava Batra 28a:1-60b:22")), refs.all)
        assertEquals(setOf("Talmud/Bavli"), refs.perekByFamily.keys)
    }

    @Test
    fun excludesDependantBooksNestedUnderPrimaryCategoryPaths() {
        val chapters = listOf(
            AltStructurePayload(
                key = "Chapters", title = null, heTitle = null,
                nodes = listOf(node("Rashi on Bava Batra 2a:1-3b:4", alone = true)),
            ),
        )
        val parasha = listOf(
            AltStructurePayload(
                key = "Parasha", title = null, heTitle = null,
                nodes = listOf(node("Rashi on Genesis 1:1-6:8")),
            ),
        )
        val refs = SefariaWholeUnitRefs.build(
            listOf(
                payload(
                    "Rashi on Bava Batra",
                    listOf("Talmud", "Bavli", "Rishonim on Talmud"),
                    chapters,
                    rawDependence = "commentary",
                ),
                payload(
                    "Rashi on Genesis",
                    listOf("Tanakh", "Torah", "Rishonim on Tanakh"),
                    parasha,
                    rawDependence = "commentary",
                ),
            ),
        )

        assertTrue(refs.all.isEmpty())
    }

    @Test
    fun collectsParashiyotWithoutRequiringAloneScope() {
        val refs = SefariaWholeUnitRefs.build(
            listOf(
                payload(
                    "Exodus", listOf("Tanakh", "Torah"),
                    listOf(
                        AltStructurePayload(
                            key = "Parasha", title = null, heTitle = null,
                            nodes = listOf(node("Exodus 25:1-27:19", alone = false))
                        ),
                        // A non-Parasha structure in the same book is not a parasha.
                        AltStructurePayload(
                            key = "Topic", title = null, heTitle = null,
                            nodes = listOf(node("Exodus 1:1-6:1", alone = true))
                        ),
                    )
                )
            )
        )

        assertEquals(setOf(canonicalCitation("Exodus 25:1-27:19")), refs.parasha)
        assertEquals(emptyMap(), refs.perekByFamily)
    }

    /**
     * A "Parasha"-keyed structure only bypasses the alone-scope check inside the
     * Torah. In a perek category the node must still be citable alone, so a
     * `Parasha` key there is not a free pass.
     */
    @Test
    fun parashaKeyOutsideTheTorahDoesNotBypassTheAloneCheck() {
        val refs = SefariaWholeUnitRefs.build(
            listOf(
                payload(
                    "Mishnah Sotah", listOf("Mishnah", "Seder Nashim"),
                    listOf(
                        AltStructurePayload(
                            key = "Parasha", title = null, heTitle = null,
                            nodes = listOf(node("Mishnah Sotah 7:1-8:7", alone = false))
                        )
                    )
                ),
                // Neither a perek category nor the Torah → the book is skipped whole.
                payload(
                    "Yalkut Shimoni on Torah", listOf("Midrash", "Aggadic Midrash"),
                    listOf(
                        AltStructurePayload(
                            key = "Parasha", title = null, heTitle = null,
                            nodes = listOf(node("Yalkut Shimoni on Torah 1:1-15:3", alone = true))
                        )
                    )
                ),
            )
        )

        assertEquals(emptySet(), refs.all)
    }

    /**
     * If English `categories` ever moves in the export, EVERY source goes empty
     * at once — which is why the importer asserts each family separately rather
     * than checking one flat set.
     */
    @Test
    fun losingEnglishCategoriesEmptiesEverySourceAtOnce() {
        val bavliStructure = AltStructurePayload(
            key = "Chapters", title = null, heTitle = null,
            nodes = listOf(node("Bava Batra 28a:1-60b:22", alone = true))
        )
        val parashaStructure = AltStructurePayload(
            key = "Parasha", title = null, heTitle = null,
            nodes = listOf(node("Exodus 25:1-27:19"))
        )

        val intact = SefariaWholeUnitRefs.build(
            listOf(
                payload("Bava Batra", listOf("Talmud", "Bavli"), listOf(bavliStructure)),
                payload("Exodus", listOf("Tanakh", "Torah"), listOf(parashaStructure)),
            )
        )
        assertEquals(1, intact.perekByFamily.getValue("Talmud/Bavli").size)
        assertEquals(1, intact.parasha.size)

        val broken = SefariaWholeUnitRefs.build(
            listOf(
                payload("Bava Batra", emptyList(), listOf(bavliStructure)),
                payload("Exodus", emptyList(), listOf(parashaStructure)),
            )
        )
        assertTrue(broken.perekByFamily.isEmpty())
        assertTrue(broken.parasha.isEmpty())
    }

    /** A parasha-only break leaves the perek sets intact, so it needs its own check. */
    @Test
    fun renamingTheParashaStructureBreaksOnlyTheParashaSet() {
        val refs = SefariaWholeUnitRefs.build(
            listOf(
                payload("Bava Batra", listOf("Talmud", "Bavli"), listOf(
                    AltStructurePayload(
                        key = "Chapters", title = null, heTitle = null,
                        nodes = listOf(node("Bava Batra 28a:1-60b:22", alone = true))
                    )
                )),
                payload("Exodus", listOf("Tanakh", "Torah"), listOf(
                    AltStructurePayload(
                        key = "Parashot", title = null, heTitle = null,
                        nodes = listOf(node("Exodus 25:1-27:19"))
                    )
                )),
            )
        )

        assertEquals(1, refs.perekByFamily.getValue("Talmud/Bavli").size)
        assertTrue(refs.parasha.isEmpty(), "a renamed key must not silently pass")
    }

    /**
     * The two shapes an early `wholeRef == null` return used to let through: a
     * ref-less parent whose child carries the ref, and a ref-carrying parent
     * that is no longer a leaf. Sefaria would pick neither.
     */
    @Test
    fun aliyahLevelWholeRefUnderARefLessParentFailsLoudly() {
        val error = assertFailsWith<IllegalStateException> {
            SefariaWholeUnitRefs.build(
                listOf(
                    payload("Exodus", listOf("Tanakh", "Torah"), listOf(
                        AltStructurePayload(
                            key = "Parasha", title = null, heTitle = null,
                            nodes = listOf(
                                node(null, children = listOf(node("Exodus 25:1-25:16")))
                            )
                        )
                    ))
                )
            )
        }
        assertTrue(error.message!!.contains("nested wholeRef"), error.message!!)
    }

    @Test
    fun parashaNodeWithRefLessChildrenFailsLoudly() {
        val error = assertFailsWith<IllegalStateException> {
            SefariaWholeUnitRefs.build(
                listOf(
                    payload("Exodus", listOf("Tanakh", "Torah"), listOf(
                        AltStructurePayload(
                            key = "Parasha", title = null, heTitle = null,
                            nodes = listOf(
                                node("Exodus 25:1-27:19", children = listOf(node(null)))
                            )
                        )
                    ))
                )
            )
        }
        assertTrue(error.message!!.contains("is not a leaf"), error.message!!)
    }

    /**
     * The Parasha-key proxy stands in for Sefaria's term matching only while
     * parasha nodes are leaves. A nested `wholeRef` must fail the build, not
     * silently suppress an aliyah citation.
     */
    @Test
    fun nestedWholeRefUnderAParashaNodeFailsLoudly() {
        val error = assertFailsWith<IllegalStateException> {
            SefariaWholeUnitRefs.build(
                listOf(
                    payload("Exodus", listOf("Tanakh", "Torah"), listOf(
                        AltStructurePayload(
                            key = "Parasha", title = null, heTitle = null,
                            nodes = listOf(
                                node("Exodus 25:1-27:19", children = listOf(node("Exodus 25:1-25:16")))
                            )
                        )
                    ))
                )
            )
        }
        assertTrue(error.message!!.contains("nested wholeRef"), error.message!!)
    }

    @Test
    fun traversesNestedNodes() {
        val refs = SefariaWholeUnitRefs.build(
            listOf(
                payload(
                    "Mishnah Berakhot", listOf("Mishnah", "Seder Zeraim"),
                    listOf(
                        AltStructurePayload(
                            key = "Chapters", title = null, heTitle = null,
                            nodes = listOf(
                                node(null, children = listOf(node("Mishnah Berakhot 1:1-1:5", alone = true)))
                            )
                        )
                    )
                )
            )
        )

        assertEquals(setOf(canonicalCitation("Mishnah Berakhot 1:1-1:5")), refs.all)
    }

    /**
     * The predicate that decides membership lives in the JSON parser: a
     * `match_templates` entry with scope "any"/"alone" makes the node citable
     * on its own; a scope-less entry (Sefaria's "combined" default) does not.
     */
    @Test
    fun readerDerivesReferenceableAloneFromMatchTemplateScope() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-whole-unit-refs")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Bava Batra"))

        Files.writeString(schemaDir.resolve("Bava_Batra.json"), SCHEMA_JSON)
        Files.writeString(bookDir.resolve("merged.json"), MERGED_JSON)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaWholeUnitRefsTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        assertEquals(listOf("Talmud", "Bavli", "Seder Nezikin"), payload.categoriesEn)
        val nodes = payload.altStructures.single { it.key == "Chapters" }.nodes
        assertTrue(nodes[0].referenceableAlone, "scope \"any\" ⇒ citable alone")
        assertTrue(nodes[1].referenceableAlone, "scope \"alone\" ⇒ citable alone")
        assertFalse(nodes[2].referenceableAlone, "no scope ⇒ Sefaria's \"combined\" default")
        assertFalse(nodes[3].referenceableAlone, "no match_templates at all")

        assertEquals(
            setOf(
                canonicalCitation("Bava Batra 2a:1-16b:9"),
                canonicalCitation("Bava Batra 17a:1-27b:12"),
            ),
            SefariaWholeUnitRefs.build(listOf(payload)).all,
        )
    }

    private companion object {
        private val SCHEMA_JSON = """
            {
              "title": "Bava Batra",
              "heTitle": "בבא בתרא",
              "heCategories": ["תלמוד", "בבלי", "סדר נזיקין"],
              "categories": ["Talmud", "Bavli", "Seder Nezikin"],
              "schema": {
                "nodeType": "JaggedArrayNode",
                "depth": 2,
                "addressTypes": ["Talmud", "Integer"],
                "sectionNames": ["Daf", "Line"],
                "heSectionNames": ["דף", "שורה"],
                "title": "Bava Batra",
                "heTitle": "בבא בתרא"
              },
              "alts": {
                "Chapters": {
                  "nodes": [
                    {
                      "nodeType": "ArrayMapNode",
                      "wholeRef": "Bava Batra 2a:1-16b:9",
                      "title": "Chapter 1",
                      "heTitle": "השותפין",
                      "match_templates": [{"term_slugs": ["perek", "first"], "scope": "any"}]
                    },
                    {
                      "nodeType": "ArrayMapNode",
                      "wholeRef": "Bava Batra 17a:1-27b:12",
                      "title": "Chapter 2",
                      "heTitle": "לא יחפור",
                      "match_templates": [{"term_slugs": ["perek", "second"], "scope": "alone"}]
                    },
                    {
                      "nodeType": "ArrayMapNode",
                      "wholeRef": "Bava Batra 28a:1-60b:22",
                      "title": "Chapter 3",
                      "heTitle": "חזקת הבתים",
                      "match_templates": [{"term_slugs": ["perek", "third"]}]
                    },
                    {
                      "nodeType": "ArrayMapNode",
                      "wholeRef": "Bava Batra 61a:1-72b:5",
                      "title": "Chapter 4",
                      "heTitle": "המוכר את הבית"
                    }
                  ]
                }
              }
            }
        """.trimIndent()

        private val MERGED_JSON = """
            {
              "title": "Bava Batra",
              "language": "he",
              "versionTitle": "Wikisource Talmud Bavli",
              "categories": ["תלמוד", "בבלי", "סדר נזיקין"],
              "text": [["שורה א", "שורה ב"], ["שורה ג"]]
            }
        """.trimIndent()
    }
}
