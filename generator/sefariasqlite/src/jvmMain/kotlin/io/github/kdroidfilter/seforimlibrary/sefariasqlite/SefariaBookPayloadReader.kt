package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import io.github.kdroidfilter.seforimlibrary.core.models.PubDate
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.exists
import kotlin.io.path.name
import kotlin.io.path.readText

internal class SefariaBookPayloadReader(
    private val json: Json,
    private val logger: Logger,
    // Sefaria's wider author-name vocabulary. Defaults to empty so an export
    // without authors.json — and every existing caller — behaves as before.
    private val authorTitles: SefariaAuthorTitles = SefariaAuthorTitles.EMPTY,
) {
    internal data class SelectedBookReadStats(
        val mergedFilesScanned: Int,
        val payloadsLoaded: Int,
    )

    fun buildSchemaLookup(schemaDir: Path): Map<String, Path> {
        val lookup = ConcurrentHashMap<String, Path>()
        Files.newDirectoryStream(schemaDir) { it.fileName.toString().endsWith(".json") }.use { ds ->
            for (schemaPath in ds) {
                runCatching {
                    val obj = json.parseToJsonElement(schemaPath.readText()).jsonObject
                    val schemaObj = obj["schema"]?.jsonObject ?: return@runCatching
                    listOf(schemaObj["title"]?.stringOrNull(), schemaObj["heTitle"]?.stringOrNull()).forEach { key ->
                        normalizeTitleKey(key)?.let { normalized ->
                            lookup.putIfAbsent(normalized, schemaPath)
                        }
                    }
                }
            }
        }
        return lookup
    }

    /**
     * Read and parse book files in parallel using coroutines.
     */
    suspend fun readBooksInParallel(
        jsonDir: Path,
        schemaDir: Path,
        schemaLookup: Map<String, Path>
    ): List<BookPayload> = coroutineScope {
        val mergedFiles = Files.walk(jsonDir).use { stream ->
            stream.filter { Files.isRegularFile(it) && it.fileName.name.equals("merged.json", ignoreCase = true) }
                .toList()
        }

        logger.i { "Found ${mergedFiles.size} merged.json files to process" }

        // Process files in parallel with limited concurrency
        val semaphore = Semaphore(SefariaImportTuning.FILE_PARALLELISM)

        mergedFiles.map { textPath ->
            async(Dispatchers.IO) {
                semaphore.withPermit {
                    // Derive the text-only per-line values here, on the worker that
                    // already holds the book's text, so the single-threaded insert
                    // loop is left with nothing but id allocation and inserts.
                    parseBookFile(textPath, schemaDir, schemaLookup)?.precomputeLineData()
                }
            }
        }.awaitAll().filterNotNull()
    }

    /**
     * Streams only top-level titles for all merged files and materializes
     * candidate payloads one at a time. Payload construction remains identical
     * to the regular importer, while unrelated book text never enters memory.
     */
    internal fun readSelectedBooks(
        jsonDir: Path,
        schemaDir: Path,
        schemaLookup: Map<String, Path>,
        candidatePrimaryHeTitles: Set<String>,
        consume: (BookPayload) -> Unit,
    ): SelectedBookReadStats {
        var scanned = 0
        var loaded = 0
        val mergedFiles = Files.walk(jsonDir).use { stream ->
            stream.filter { Files.isRegularFile(it) && it.fileName.name.equals("merged.json", ignoreCase = true) }
                .sorted()
                .toList()
        }
        for (textPath in mergedFiles) {
            scanned++
            val header = readTopLevelTitles(textPath)
            val schemaPath = resolveSchemaPath(
                title = header.first,
                heTitle = header.second,
                folderName = textPath.parent?.fileName?.name,
                schemaDir = schemaDir,
                lookup = schemaLookup,
            ) ?: continue
            val schemaHeader = readSchemaTitles(schemaPath)
            if (sequenceOf(header.second, schemaHeader.second).filterNotNull()
                    .none { it in candidatePrimaryHeTitles }
            ) continue
            parseBookFile(textPath, schemaDir, schemaLookup)?.let {
                loaded++
                consume(it)
            }
        }
        return SelectedBookReadStats(scanned, loaded)
    }

    private fun readTopLevelTitles(path: Path): Pair<String?, String?> {
        var title: String? = null
        var heTitle: String? = null
        JsonFactory().createParser(path.toFile()).use { parser ->
            require(parser.nextToken() == JsonToken.START_OBJECT) { "Expected object in $path" }
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                require(parser.currentToken == JsonToken.FIELD_NAME) { "Expected field in $path" }
                val name = parser.currentName
                val token = parser.nextToken()
                if (token == JsonToken.VALUE_STRING && name == "title") title = parser.text
                if (token == JsonToken.VALUE_STRING && name == "heTitle") heTitle = parser.text
                parser.skipChildren()
            }
        }
        return title to heTitle
    }

    private fun readSchemaTitles(path: Path): Pair<String?, String?> {
        val root = json.parseToJsonElement(path.readText()).jsonObject
        val schema = root["schema"]?.jsonObject ?: return null to null
        return schema["title"]?.stringOrNull() to schema["heTitle"]?.stringOrNull()
    }

    internal fun parseBookFile(
        textPath: Path,
        schemaDir: Path,
        schemaLookup: Map<String, Path>
    ): BookPayload? {
        return runCatching {
            val textJson = json.parseToJsonElement(textPath.readText()).jsonObject
            val fileTitle = textJson["title"]?.stringOrNull()
            val fileHeTitle = textJson["heTitle"]?.stringOrNull()
            val folderName = textPath.parent?.fileName?.name

            val schemaPath = resolveSchemaPath(
                title = fileTitle,
                heTitle = fileHeTitle,
                folderName = folderName,
                schemaDir = schemaDir,
                lookup = schemaLookup
            ) ?: return@runCatching null

            val schemaJson = json.parseToJsonElement(schemaPath.readText()).jsonObject
            val schemaObj = schemaJson["schema"]?.jsonObject ?: return@runCatching null
            val englishTitle = schemaObj["title"]?.stringOrNull() ?: fileTitle ?: folderName ?: return@runCatching null
            val hebrewTitle = schemaObj["heTitle"]?.stringOrNull() ?: fileHeTitle ?: englishTitle

            val textElement = textJson["text"] ?: return@runCatching null
            val categories = schemaJson["heCategories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: schemaObj["heCategories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: textJson["categories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: emptyList()

            // Unsanitized: the whole-unit ref sets gate on Sefaria's own
            // English category paths ("Talmud/Bavli", "Mishnah", …).
            val categoriesEn = schemaJson["categories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: schemaObj["categories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: emptyList()

            // `slug` is the topic id authors.json is keyed by; it is what lets a
            // bare "אברהם יצחק הכהן קוק" become "הרב אברהם יצחק הכהן קוק".
            // Without authors.json this resolves to the schema name unchanged.
            val authors = schemaJson["authors"]?.jsonArray?.mapNotNull { author ->
                val entry = author.jsonObject
                entry["he"]?.stringOrNull()?.let { he ->
                    authorTitles.displayName(entry["slug"]?.stringOrNull(), he)
                }
            } ?: emptyList()

            val (lines, refs, headings, cleanShifts) = buildBookContent(
                schemaObj = schemaObj,
                textElement = textElement,
                bookHeTitle = hebrewTitle,
                bookEnTitle = englishTitle,
                authors = authors
            )
            // merged.json "versions" is a list of [title, url] pairs. A single
            // entry means the merged text IS that version verbatim — the
            // precondition for exact charLevelData offsets.
            val singleVersionTitle = (textJson["versions"] as? JsonArray)
                ?.takeIf { it.size == 1 }
                ?.let { (it.first() as? JsonArray)?.firstOrNull()?.stringOrNull() }
                ?.trim()?.takeIf { it.isNotEmpty() }
            val versionsMeta = (textJson["versions"] as? JsonArray)?.mapNotNull { el ->
                val pair = el as? JsonArray ?: return@mapNotNull null
                val title = pair.getOrNull(0)?.stringOrNull()?.trim()?.takeIf { it.isNotEmpty() }
                    ?: return@mapNotNull null
                VersionMeta(title = title, source = pair.getOrNull(1)?.stringOrNull())
            } ?: emptyList()
            val description = extractDescription(schemaJson, schemaObj)
            val heShortDesc = extractShortDescription(schemaJson, schemaObj)
            val pubDates = extractPubDates(schemaJson, schemaObj)
            val altStructures = parseAltStructures(schemaJson)
            val rawDependence = extractRawDependence(schemaJson, schemaObj)
            val dependence = rawDependence?.let(::mapDependence)
            val declaredBaseTextTitleKeys = extractBaseTextTitleKeys(schemaJson, schemaObj)
            // When Sefaria didn't ship `base_text_titles` (≈ 74 books, e.g.
            // Nachalat Avot on Avot, Bartenura on Torah, Ralbag on Torah),
            // try to recover the base from the title's "X on Y" / "X על Y"
            // pattern. Kept separate from declared keys — provenance INFERRED.
            val inferredBaseTextTitleKeys = if (declaredBaseTextTitleKeys.isEmpty() && dependence != null) {
                extractBaseKeysFromTitle(englishTitle, hebrewTitle)
            } else {
                emptyList()
            }
            val collectiveTitleObj = schemaJson["collective_title"] as? JsonObject
            val collectiveTitleEn = collectiveTitleObj?.get("en")?.stringOrNull()?.trim()?.takeIf { it.isNotEmpty() }
            val collectiveTitleHe = collectiveTitleObj?.get("he")?.stringOrNull()?.trim()?.takeIf { it.isNotEmpty() }
            val titleAliasKeys = extractTitleAliasKeys(schemaJson, schemaObj)

            BookPayload(
                heTitle = hebrewTitle,
                enTitle = englishTitle,
                categoriesHe = flattenTalmudCategories(categories.map { sanitizeFolder(it) }),
                lines = lines,
                refEntries = refs,
                headings = headings,
                authors = authors,
                description = description,
                heShortDesc = heShortDesc,
                pubDates = pubDates,
                altStructures = altStructures,
                dependence = dependence,
                rawDependence = rawDependence,
                declaredBaseTextTitleKeys = declaredBaseTextTitleKeys,
                inferredBaseTextTitleKeys = inferredBaseTextTitleKeys,
                collectiveTitleHe = collectiveTitleHe,
                collectiveTitleEn = collectiveTitleEn,
                titleAliasKeys = titleAliasKeys,
                singleVersionTitle = singleVersionTitle,
                cleanShiftByLineIndex = cleanShifts,
                versionsMeta = versionsMeta,
                sourceDirPath = textPath.parent?.toString(),
                schemaFilePath = schemaPath.toString(),
                categoriesEn = categoriesEn,
            )
        }.onFailure { e ->
            logger.w(e) { "Failed to prepare book from $textPath" }
        }.getOrNull()
    }

    private fun resolveSchemaPath(
        title: String?,
        heTitle: String?,
        folderName: String?,
        schemaDir: Path,
        lookup: Map<String, Path>
    ): Path? {
        val candidates = listOfNotNull(title, heTitle, folderName?.replace('_', ' '), folderName)
        for (candidate in candidates) {
            val normalized = normalizeTitleKey(candidate)
            if (normalized != null) {
                val fromLookup = lookup[normalized]
                if (fromLookup != null) return fromLookup
            }
            val path = schemaDir.resolve("${candidate.replace(" ", "_")}.json")
            if (path.exists()) return path
        }
        return null
    }

    /**
     * When a book has `dependence != null` but no `base_text_titles`, try to
     * recover the declared base from the title's "X on Y" or "X על Y" pattern.
     * Returns the normalized title key(s) for "Y", which the importer's first
     * pass will resolve against `normalizedTitleToBookId` (including aliases).
     *
     * Drops empty or whitespace-only results — unresolvable keys are silently
     * skipped at resolution time, so we don't filter further here.
     */
    private fun extractBaseKeysFromTitle(enTitle: String?, heTitle: String?): List<String> {
        val out = mutableListOf<String>()
        enTitle?.let {
            val idx = it.lastIndexOf(" on ")
            if (idx > 0) {
                normalizeTitleKey(it.substring(idx + 4).trim())?.let(out::add)
            }
        }
        heTitle?.let {
            val idx = it.lastIndexOf(" על ")
            if (idx > 0) {
                normalizeTitleKey(it.substring(idx + 4).trim())?.let(out::add)
                // Also try without the leading definite article "ה" (e.g. "התורה" → "תורה").
                val raw = it.substring(idx + 4).trim()
                if (raw.startsWith("ה") && raw.length > 1) {
                    normalizeTitleKey(raw.substring(1))?.let(out::add)
                }
            }
        }
        return out.distinct()
    }

    /**
     * Extracts all Sefaria-recognised title aliases for the book. Used by the
     * importer to populate `normalizedTitleToBookId` so that title-pattern
     * base parsing (e.g. "X on Avot" → Pirkei Avot) can resolve abbreviated
     * names that aren't the primary `title`/`heTitle`.
     *
     * Filters out HTML-marked variants (`<i>Pirkei Avot</i>`) to avoid noise.
     */
    private fun extractTitleAliasKeys(schemaJson: JsonObject, schemaObj: JsonObject): List<String> {
        val out = mutableListOf<String>()
        fun ingest(key: String) {
            (schemaJson[key]?.jsonArray ?: schemaObj[key]?.jsonArray)?.forEach { el ->
                val s = el.jsonPrimitive.contentOrNull?.takeIf { it.isNotBlank() } ?: return@forEach
                if (s.contains('<') || s.contains('>')) return@forEach
                normalizeTitleKey(s)?.let(out::add)
            }
        }
        ingest("titleVariants")
        ingest("heTitleVariants")
        return out.distinct()
    }

    // Raw `dependence` after trim + canonical lowercasing; null when absent.
    private fun extractRawDependence(schemaJson: JsonObject, schemaObj: JsonObject): String? =
        (schemaJson["dependence"]?.stringOrNull() ?: schemaObj["dependence"]?.stringOrNull())
            ?.trim()?.lowercase()?.takeIf { it.isNotEmpty() }

    private fun mapDependence(raw: String): Dependence = when (raw) {
        "commentary", "sub-commentary", "supercommentary", "super-commentary",
        "super_commentary" -> Dependence.COMMENTARY
        "targum" -> Dependence.TARGUM
        "midrash" -> Dependence.MIDRASH
        else -> Dependence.OTHER_DEPENDANT
    }

    private fun extractBaseTextTitleKeys(schemaJson: JsonObject, schemaObj: JsonObject): List<String> {
        val arr = schemaJson["base_text_titles"]?.jsonArray
            ?: schemaObj["base_text_titles"]?.jsonArray
            ?: return emptyList()
        val keys = mutableListOf<String>()
        arr.forEach { el ->
            when (el) {
                is JsonObject -> {
                    el["en"]?.stringOrNull()?.let { normalizeTitleKey(it)?.let(keys::add) }
                    el["he"]?.stringOrNull()?.let { normalizeTitleKey(it)?.let(keys::add) }
                }
                is JsonPrimitive -> el.contentOrNull?.let { normalizeTitleKey(it)?.let(keys::add) }
                else -> {}
            }
        }
        return keys.distinct()
    }

    private fun extractDescription(schemaJson: JsonObject, schemaObj: JsonObject): String? {
        return schemaJson["heDesc"]?.stringOrNull()
            ?: schemaObj["heDesc"]?.stringOrNull()
            ?: schemaJson["description"]?.stringOrNull()
            ?: schemaObj["description"]?.stringOrNull()
            ?: schemaJson["heDescription"]?.stringOrNull()
            ?: schemaObj["heDescription"]?.stringOrNull()
    }

    // Real one-line summary → book.heShortDesc (long text goes to heDesc via extractDescription).
    private fun extractShortDescription(schemaJson: JsonObject, schemaObj: JsonObject): String? {
        return schemaJson["heShortDesc"]?.stringOrNull()
            ?: schemaObj["heShortDesc"]?.stringOrNull()
    }

    private fun extractPubDates(schemaJson: JsonObject, schemaObj: JsonObject): List<PubDate> {
        val dates = mutableListOf<String>()
        fun collect(key: String, obj: JsonObject) {
            obj[key]?.let { el ->
                when (el) {
                    is JsonArray -> dates += el.mapNotNull { it.jsonPrimitive.contentOrNull }
                    is JsonPrimitive -> el.contentOrNull?.let { dates += it }
                    else -> {}
                }
            }
        }
        collect("pubDate", schemaJson)
        collect("pubDate", schemaObj)
        return dates.distinct().map { PubDate(date = it) }
    }

    private fun parseAltStructures(schemaJson: JsonObject): List<AltStructurePayload> {
        val altsObj = schemaJson["alts"]?.jsonObject
            ?: schemaJson["alt_structs"]?.jsonObject
            ?: return emptyList()
        return altsObj.mapNotNull { (key, value) ->
            val altObj = value.jsonObject
            val nodesArray = altObj["nodes"]?.jsonArray ?: return@mapNotNull null
            val nodes = nodesArray.mapNotNull { parseAltNode(it.jsonObject) }
            AltStructurePayload(
                key = key,
                title = altObj["title"]?.stringOrNull(),
                heTitle = altObj["heTitle"]?.stringOrNull(),
                nodes = nodes
            )
        }
    }

    private fun parseAltNode(obj: JsonObject): AltNodePayload {
        val title = obj["title"]?.stringOrNull()
        val heTitle = obj["heTitle"]?.stringOrNull()
        val wholeRef = obj["wholeRef"]?.stringOrNull()
        val refs = obj["refs"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
        val addressTypes = obj["addressTypes"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
        val addresses = obj["addresses"]?.jsonArray?.mapNotNull { it.jsonPrimitive.intOrNull } ?: emptyList()
        val skippedAddresses = obj["skipped_addresses"]?.jsonArray?.mapNotNull { it.jsonPrimitive.intOrNull } ?: emptyList()
        val startingAddress = obj["startingAddress"]?.stringOrNull()
        val offset = obj["offset"]?.jsonPrimitive?.intOrNullSafe()
        val childLabel = obj["heSectionNames"]?.jsonArray?.firstOrNull()?.jsonPrimitive?.contentOrNull
            ?: obj["sectionNames"]?.jsonArray?.firstOrNull()?.jsonPrimitive?.contentOrNull
        val children = obj["nodes"]?.jsonArray?.mapNotNull { parseAltNode(it.jsonObject) } ?: emptyList()
        // Sefaria's `has_scope_alone_match_template()`; a scope-less template
        // defaults to "combined" and does not make the node citable alone.
        val referenceableAlone = obj["match_templates"]?.jsonArray?.any { template ->
            template.jsonObject["scope"]?.stringOrNull() in ALONE_MATCH_TEMPLATE_SCOPES
        } ?: false
        return AltNodePayload(
            title = title,
            heTitle = heTitle,
            wholeRef = wholeRef,
            refs = refs,
            addressTypes = addressTypes,
            childLabel = childLabel,
            addresses = addresses,
            skippedAddresses = skippedAddresses,
            startingAddress = startingAddress,
            offset = offset,
            children = children,
            referenceableAlone = referenceableAlone
        )
    }

    internal data class BuiltBookContent(
        val lines: List<String>,
        val refs: List<RefEntry>,
        val headings: List<Heading>,
        val cleanShifts: Map<Int, Int>,
    )

    /**
     * Walks an alternative version's text with the BOOK's schema — the exact
     * walk merged.json went through, so identical addresses yield identical
     * refs and identical line formatting. Join version→merged lines by ref.
     */
    internal fun walkTextWithSchema(
        schemaObj: JsonObject,
        textElement: JsonElement,
        bookHeTitle: String,
        bookEnTitle: String,
    ): BuiltBookContent = buildBookContent(
        schemaObj = schemaObj,
        textElement = textElement,
        bookHeTitle = bookHeTitle,
        bookEnTitle = bookEnTitle,
        authors = emptyList(),
    )

    private fun buildBookContent(
        schemaObj: JsonObject,
        textElement: JsonElement,
        bookHeTitle: String,
        bookEnTitle: String,
        authors: List<String>
    ): BuiltBookContent {
        // Pre-allocate with estimated capacity
        val output = ArrayList<String>(1000)
        val refs = ArrayList<RefEntry>(1000)
        val headings = ArrayList<Heading>(100)
        // See BookPayload.cleanShiftByLineIndex — sparse raw-offset bookkeeping.
        val cleanShifts = HashMap<Int, Int>()

        fun headingTagForLevel(level: Int): Pair<String, String> = when (level) {
            0 -> "<h1>" to "</h1>"
            1 -> "<h2>" to "</h2>"
            2 -> "<h3>" to "</h3>"
            3 -> "<h4>" to "</h4>"
            else -> "<h5>" to "</h5>"
        }

        val tag = headingTagForLevel(0)
        output += "${tag.first}$bookHeTitle${tag.second}"
        authors.forEach { output += it }
        headings += Heading(title = bookHeTitle, level = 0, lineIndex = 0)

        fun processNode(
            node: JsonObject,
            text: JsonElement?,
            level: Int,
            refPrefix: String,
            heRefPrefix: String
        ) {
            val nodeTitle = node["heTitle"]?.stringOrNull()?.takeIf { it.isNotBlank() }
                ?: node["title"]?.stringOrNull()?.takeIf { it.isNotBlank() }
            val hasTitle = nodeTitle != null
            if (hasTitle && level > 0) {
                val tag = headingTagForLevel(level)
                output += "${tag.first}$nodeTitle${tag.second}"
                headings += Heading(
                    title = nodeTitle,
                    level = level,
                    lineIndex = output.size - 1
                )
            }

            if (text !is JsonArray && text !is JsonPrimitive && text !is JsonObject) return

            if (node.containsKey("nodes")) {
                val children = node["nodes"]?.jsonArray ?: JsonArray(emptyList())
                for (child in children) {
                    val childNode = child.jsonObject
                    val childText = selectNodeText(childNode, text)
                    val childTitle = childNode["title"]?.stringOrNull().orEmpty()
                    val childHeTitle = childNode["heTitle"]?.stringOrNull().orEmpty()
                    val key = childNode["key"]?.stringOrNull()
                    val nextRefPrefix = buildString {
                        append(refPrefix)
                        if (!key.equals("default", ignoreCase = true) && childTitle.isNotBlank()) append(childTitle).append(", ")
                    }
                    val nextHeRefPrefix = buildString {
                        append(heRefPrefix)
                        if (!key.equals("default", ignoreCase = true) && childHeTitle.isNotBlank()) append(childHeTitle).append(", ")
                    }
                    processNode(childNode, childText, level + 1, nextRefPrefix, nextHeRefPrefix)
                }
            } else {
                val sectionNames = readHeSectionNames(node)
                val depth = node["depth"]?.jsonPrimitive?.intOrNullSafe() ?: sectionNames.size
                val addressTypes = node["addressTypes"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
                val referenceableSections = node["referenceableSections"]?.jsonArray?.mapNotNull { it.jsonPrimitive.booleanOrNull } ?: emptyList()
                val childRefOffsets = readIndexOffsets(node, depth)
                val nextLevel = if (hasTitle) level + 1 else level
                recursiveSections(
                    sectionNames = sectionNames,
                    text = text,
                    depth = depth,
                    level = nextLevel,
                    output = output,
                    refEntries = refs,
                    refPrefix = "$refPrefix ",
                    heRefPrefix = "$heRefPrefix ",
                    bookEnTitle = bookEnTitle,
                    bookHeTitle = bookHeTitle,
                    headings = headings,
                    addressTypes = addressTypes,
                    referenceableSections = referenceableSections,
                    childRefOffsets = childRefOffsets,
                    cleanShifts = cleanShifts
                )
            }
        }

        if (schemaObj.containsKey("nodes")) {
            val nodes = schemaObj["nodes"]?.jsonArray ?: JsonArray(emptyList())
            for (nodeElement in nodes) {
                val node = nodeElement.jsonObject
                val nodeText = selectNodeText(node, textElement)
                val nodeTitle = node["title"]?.stringOrNull().orEmpty()
                val nodeHeTitle = node["heTitle"]?.stringOrNull().orEmpty()
                val key = node["key"]?.stringOrNull()
                val refPrefix = buildString {
                    append(bookEnTitle)
                    append(", ")
                    if (!key.equals("default", ignoreCase = true) && nodeTitle.isNotBlank()) append(nodeTitle).append(", ")
                }
                val heRefPrefix = buildString {
                    append(bookHeTitle)
                    append(", ")
                    if (!key.equals("default", ignoreCase = true) && nodeHeTitle.isNotBlank()) append(nodeHeTitle).append(", ")
                }
                processNode(node, nodeText, 1, refPrefix, heRefPrefix)
            }
        } else {
            val sectionNames = readHeSectionNames(schemaObj)
            val depth = schemaObj["depth"]?.jsonPrimitive?.intOrNullSafe() ?: sectionNames.size
            val addressTypes = schemaObj["addressTypes"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
            val referenceableSections = schemaObj["referenceableSections"]?.jsonArray?.mapNotNull { it.jsonPrimitive.booleanOrNull } ?: emptyList()
            val childRefOffsets = readIndexOffsets(schemaObj, depth)
            recursiveSections(
                sectionNames = sectionNames,
                text = textElement,
                depth = depth,
                level = 1,
                output = output,
                refEntries = refs,
                refPrefix = "$bookEnTitle ",
                heRefPrefix = "$bookHeTitle ",
                bookEnTitle = bookEnTitle,
                bookHeTitle = bookHeTitle,
                headings = headings,
                addressTypes = addressTypes,
                referenceableSections = referenceableSections,
                childRefOffsets = childRefOffsets,
                cleanShifts = cleanShifts
            )
        }

        return BuiltBookContent(output, refs, headings, cleanShifts)
    }

    private fun recursiveSections(
        sectionNames: List<String>,
        text: JsonElement?,
        depth: Int,
        level: Int,
        output: MutableList<String>,
        refEntries: MutableList<RefEntry>,
        refPrefix: String,
        heRefPrefix: String,
        bookEnTitle: String,
        bookHeTitle: String,
        headings: MutableList<Heading>,
        linePrefix: String = "",
        addressTypes: List<String> = emptyList(),
        referenceableSections: List<Boolean> = emptyList(),
        // Offsets applied to the *English* ref number built at this iteration.
        // Propagated from `index_offsets_by_depth` so Zohar-style books emit
        // the same global paragraph indices that Sefaria links use (e.g.
        // "Zohar, Tetzaveh 14:127" instead of "14:13"). See readIndexOffsets.
        refIndexOffset: Int = 0,
        // Offsets to hand down to the *children* of the iteration at this
        // level (one entry per outer-dim index).
        childRefOffsets: List<Int>? = null,
        // Sparse raw-offset bookkeeping (see BookPayload.cleanShiftByLineIndex).
        cleanShifts: MutableMap<Int, Int>? = null
    ) {
        // Leaf when depth reached zero, OR when the data is shallower than the
        // schema declares (e.g. Keter Malkhut: schema says depth=2 but most
        // chapters are stored as a single string instead of a paragraph array).
        // Falling through to `text !is JsonArray return` would silently drop
        // the chapter's content.
        val leafPrimitive = text as? JsonPrimitive
        if (depth == 0 || (leafPrimitive != null && leafPrimitive.isString)) {
            val content = leafPrimitive?.takeIf { it.isString }?.content
            if (!content.isNullOrEmpty()) {
                val cleaned = cleanSefariaLine(content)
                if (cleaned.isNotEmpty()) {
                    output += linePrefix + cleaned
                    if (cleanShifts != null) {
                        if (cleaned != content) {
                            cleanShifts[output.size - 1] = CLEAN_MODIFIED
                        } else if (linePrefix.isNotEmpty()) {
                            cleanShifts[output.size - 1] = linePrefix.length
                        }
                    }
                    val cleanRef = trimTrailingSeparators(refPrefix)
                    val cleanHeRef = trimTrailingSeparators(heRefPrefix)
                    refEntries.add(
                        RefEntry(
                            ref = cleanRef,
                            heRef = cleanHeRef,
                            path = "",
                            lineIndex = output.size
                        )
                    )
                }
            }
            return
        }
        if (text !is JsonArray) return
        val index = (sectionNames.size - depth).coerceAtLeast(0)
        val sectionName = sectionNames.getOrNull(index) ?: ""

        val nonEmptyCount = if (depth == 1) {
            text.count { !it.isTriviallyEmpty() }
        } else {
            0
        }

        // Prefixing a self-numbered array would double its printed markers.
        // Guarded by the same conditions as nextLinePrefix so no wasted scan.
        val sourceNumbered = depth == 1 && nonEmptyCount > 1 &&
            (referenceableSections.getOrNull(sectionNames.size - depth) ?: true) &&
            addressTypes.getOrNull(addressTypes.size - depth) != "Integer" &&
            when (sourceMarkerRun(text)) {
                MarkerRun.CONSECUTIVE -> true
                MarkerRun.BROKEN -> {
                    logger.d { "Partially numbered leaf array in '$bookHeTitle' ($refPrefix); keeping generated prefixes" }
                    false
                }
                MarkerRun.NONE -> false
            }

        text.forEachIndexed { idx, item ->
            if (item.isTriviallyEmpty()) return@forEachIndexed

            val currentAddressType = addressTypes.getOrNull(addressTypes.size - depth)
            val letter = when (currentAddressType) {
                "Talmud" -> toDaf(idx + 1)
                "Integer" -> toGematria(idx + 1)
                else -> toGematria(idx + 1)
            }

            val sectionIndex = sectionNames.size - depth
            val isReferenceable = referenceableSections.getOrNull(sectionIndex) ?: true
            val nextLinePrefix = if (depth == 1 && isReferenceable && currentAddressType != "Integer" && nonEmptyCount > 1 && !sourceNumbered) {
                "($letter) "
            } else {
                ""
            }

            if (depth > 1 && sectionName.isNotBlank() && isReferenceable) {
                val tag = when (level) {
                    1 -> "<h2>" to "</h2>"
                    2 -> "<h3>" to "</h3>"
                    3 -> "<h4>" to "</h4>"
                    else -> "<h5>" to "</h5>"
                }
                output += "${tag.first}$sectionName $letter${tag.second}"
                headings += Heading(
                    title = "$sectionName $letter",
                    level = level,
                    lineIndex = output.size - 1
                )
            }

            // Paragraph/ref offset from index_offsets_by_depth (Zohar-style):
            // for the current iteration's element i, children should build refs
            // as "(i+1):(offset[i]+childIdx+1)".
            val shiftedIdx = idx + 1 + refIndexOffset
            val newRefPrefix = buildString {
                append(refPrefix)
                val refNumber = when (currentAddressType) {
                    "Talmud" -> toEnglishDaf(shiftedIdx)
                    else -> shiftedIdx.toString()
                }
                append(refNumber)
                append(":")
            }
            val newHeRefPrefix = buildString {
                append(heRefPrefix)
                append(letter)
                append(", ")
            }
            val nextRefIndexOffset = childRefOffsets?.getOrNull(idx) ?: 0

            recursiveSections(
                sectionNames = sectionNames,
                text = item,
                depth = depth - 1,
                level = level + 1,
                output = output,
                refEntries = refEntries,
                refPrefix = newRefPrefix,
                heRefPrefix = newHeRefPrefix,
                bookEnTitle = bookEnTitle,
                bookHeTitle = bookHeTitle,
                headings = headings,
                linePrefix = nextLinePrefix,
                addressTypes = addressTypes,
                referenceableSections = referenceableSections,
                refIndexOffset = nextRefIndexOffset,
                cleanShifts = cleanShifts
            )
        }
    }

    /**
     * Reads `index_offsets_by_depth` from a schema node and returns the list
     * of cumulative offsets that should be applied to the child iteration
     * when building English refs.
     *
     * Sefaria uses this mechanism for books like Zohar where chapters are
     * stored as arrays but references use a single global paragraph index
     * per parasha (e.g. "Zohar, Tetzaveh 14:127" means the 127th paragraph of
     * Tetzaveh, which happens to live inside chapter 14). Without these
     * offsets the alt-struct daf refs (and all external Zohar links) fail to
     * resolve and pages like קפו: end up missing from the daf navigation.
     *
     * The map is keyed by the schema `depth` it applies at; the only
     * practical use so far is `{"2": [...]}` on depth-2 Zohar-style nodes.
     */
    private fun readIndexOffsets(node: JsonObject, schemaDepth: Int): List<Int>? {
        val map = node["index_offsets_by_depth"]?.jsonObject ?: return null
        val key = schemaDepth.toString()
        val arr = map[key]?.jsonArray ?: return null
        val offsets = arr.mapNotNull { it.jsonPrimitive.intOrNullSafe() }
        return if (offsets.isNotEmpty()) offsets else null
    }

    /**
     * Returns the Hebrew section names for a schema node, filling in blanks
     * with a best-effort mapping from the English `sectionNames` counterpart.
     *
     * Sefaria schemas frequently ship `heSectionNames=["", "פסקה"]` where the
     * top-level Hebrew label (e.g. "תשובה", "סימן") was never filled in; the
     * English `sectionNames` still has it. Without this fallback the importer
     * skips the matching heading and responsa-style books end up with no TOC.
     */
    private fun readHeSectionNames(obj: JsonObject): List<String> {
        val he = obj["heSectionNames"]?.jsonArray?.map { it.jsonPrimitive.contentOrNull.orEmpty() } ?: emptyList()
        val en = obj["sectionNames"]?.jsonArray?.map { it.jsonPrimitive.contentOrNull.orEmpty() } ?: emptyList()
        val size = maxOf(he.size, en.size)
        if (size == 0) return emptyList()
        return List(size) { idx ->
            val heVal = he.getOrNull(idx)?.trim().orEmpty()
            if (heVal.isNotEmpty()) {
                heVal
            } else {
                val enVal = en.getOrNull(idx)?.trim().orEmpty()
                mapSectionNameToHebrew(enVal.ifBlank { null }).orEmpty()
            }
        }
    }

    private fun selectNodeText(node: JsonObject, text: JsonElement?): JsonElement? {
        val key = node["key"]?.stringOrNull()
        val title = node["title"]?.stringOrNull().orEmpty()
        val obj = text as? JsonObject ?: return null
        return if (!key.equals("default", ignoreCase = true) && title.isNotBlank()) {
            obj[title]
        } else {
            obj[""] ?: obj[title]
        }
    }
}

/** `match_templates[].scope` values that make an alt-struct node citable alone. */
private val ALONE_MATCH_TEMPLATE_SCOPES = setOf("any", "alone")

// Leading printed marker of a line: "(אות) " or "{אות} ".
private val SOURCE_MARKER_REGEX = Regex("""^\s*[({]([א-ת"׳״]{1,5})[)}]\s""")

// Inverse of toGematria, for validating printed markers.
private val gematriaToInt: Map<String, Int> = (1..999).associateBy { toGematria(it) }

private enum class MarkerRun { NONE, CONSECUTIVE, BROKEN }

// CONSECUTIVE = contiguous block of >=2 markers rising by 1, first value <= its
// 1-based position, unmarked items only as one leading intro or a trailing tail.
private fun sourceMarkerRun(items: JsonArray): MarkerRun {
    var next = -1
    var pre = 0
    var post = 0
    var block = 0
    items.forEachIndexed { idx, item ->
        val content = (item as? JsonPrimitive)?.contentOrNull ?: return@forEachIndexed
        if (content.isBlank()) return@forEachIndexed
        val match = SOURCE_MARKER_REGEX.find(content)
        if (match == null) {
            if (block == 0) pre++ else post++
            return@forEachIndexed
        }
        if (post > 0) return MarkerRun.BROKEN
        val value = gematriaToInt[match.groupValues[1].filter { it !in "\"׳״" }]
            ?: return MarkerRun.BROKEN
        when {
            next < 0 -> if (value > idx + 1) return MarkerRun.BROKEN else next = value + 1
            value != next -> return MarkerRun.BROKEN
            else -> next++
        }
        block++
    }
    return when {
        block == 0 -> MarkerRun.NONE
        block >= 2 && pre <= 1 && (pre == 0 || post == 0) -> MarkerRun.CONSECUTIVE
        else -> MarkerRun.BROKEN
    }
}
