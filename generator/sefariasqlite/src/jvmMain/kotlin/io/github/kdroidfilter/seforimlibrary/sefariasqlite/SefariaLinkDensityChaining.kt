package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists

/**
 * Density-based sibling-chaining for super-commentaries that Sefaria didn't
 * encode in [BookMeta.baseTextBookIds].
 *
 * Sefaria's individual schemas only chain a fraction of super-commentaries
 * (≈ 292 / 5505 dependant books, mostly Talmud-side Rif/Ran/HaMaor families).
 * Tanakh super-commentaries like Mizrachi, Gur Aryeh and Levush HaOrah are
 * declared as direct commentaries on the Torah — even though they are in
 * fact super-commentaries on Rashi.
 *
 * Sefaria itself exports an aggregated link-density file at
 * `links/links_by_book.csv` (`Text 1, Text 2, Link Count`). The ratio
 *
 *     r(D, S) = linkCount(D, S) / linkCount(D, P)
 *
 * where `D` is a dependant book, `P` is a base it declares, and `S` is
 * another dependant sharing `P` as base — separates two qualitatively
 * different populations:
 *
 *   - `r ≈ 1.0`: D *pivots through* S (Mizrachi 0.97, Gur Aryeh 0.94,
 *     Levush HaOrah 0.85). These are real super-commentaries.
 *   - `r ≈ 0.5–0.7`: D treats S as a *secondary citation* (Bartenura on
 *     Torah cites Rashi at 0.65 but its primary text is Genesis directly).
 *     These are NOT super-commentaries.
 *
 * The original Phase-3 histogram showed a clean valley below 0.5 separating
 * noise from signal, but within the signal band the citation-vs-pivot
 * distinction sits around 0.7–0.8. Using `0.8` retains the four canonical
 * Tanakh super-commentaries (Mizrachi, Gur Aryeh, Levush, Siftei Chakhamim)
 * while excluding Bartenura-style "directly-on-Torah, cites Rashi" books.
 */
internal const val LINK_DENSITY_CHAIN_THRESHOLD: Double = 0.8

/**
 * Parses `links_by_book.csv` and indexes link counts by (bookId, bookId).
 * The CSV stores English titles which we resolve to bookIds via the same
 * normalized title map that the rest of the importer uses. Pairs that
 * don't fully resolve are dropped — they would never be queried anyway.
 *
 * Returned map is keyed by `(min, max)` so callers don't need to know
 * the original direction.
 */
internal fun parseLinksByBookCsv(
    file: Path,
    normalizedTitleToBookId: Map<String, Long>,
): Map<Pair<Long, Long>, Int> {
    if (!file.exists()) return emptyMap()
    val out = HashMap<Pair<Long, Long>, Int>(256_000)
    Files.newBufferedReader(file).use { reader ->
        val iter = reader.lineSequence().iterator()
        if (!iter.hasNext()) return emptyMap()
        // Header: Text 1, Text 2, Link Count
        iter.next()
        while (iter.hasNext()) {
            val row = parseCsvLine(iter.next())
            if (row.size < 3) continue
            val a = normalizedTitleToBookId[normalizeTitleKey(row[0]) ?: continue] ?: continue
            val b = normalizedTitleToBookId[normalizeTitleKey(row[1]) ?: continue] ?: continue
            if (a == b) continue
            val count = row[2].trim().toIntOrNull() ?: continue
            val key = if (a < b) a to b else b to a
            // CSV has no duplicates per the data audit; keep max defensively
            // in case Sefaria changes that later.
            val existing = out[key]
            if (existing == null || count > existing) out[key] = count
        }
    }
    return out
}

/**
 * Minimum absolute link count to a candidate base before considering the edge.
 * Sefaria's `links_by_book.csv` is noisy below this threshold (sporadic
 * cross-references) and the asymmetry ratio loses meaning.
 */
internal const val LINK_DENSITY_BASE_FLOOR: Int = 50

/**
 * For dependants whose schema declares `dependence` but ships an empty
 * `base_text_titles` list (74 books in the current Sefaria export — e.g.
 * Bartenura on Torah, Tzafnat Pa'neach on Torah, Ralbag on Torah, Ri Migash
 * on Bava Batra, …), pick the **primary** books they are most densely
 * linked to and assign them as their bases.
 *
 * Strictly data-driven: a candidate base is kept iff
 *   1. `linkCount(D, B) >= LINK_DENSITY_BASE_FLOOR`, **and**
 *   2. `bookMetaById[B].dependence == null` (B is itself a primary text in
 *      Sefaria's model — Tanakh, Talmud tractate, Mishnah, etc.).
 *
 * The downstream [applyLinkDensitySiblingChaining] then walks the asymmetric
 * density rule from these primary bases and adds the natural intermediate
 * commentators (e.g. Rashi-on-Torah for Bartenura) on its own.
 */
internal fun inferPrimaryBasesForEmptyDeclaredBookmeta(
    bookMetaById: MutableMap<Long, BookMeta>,
    linkCountByBookPair: Map<Pair<Long, Long>, Int>,
    logger: Logger,
): Pair<Int, Int> {
    if (linkCountByBookPair.isEmpty()) return 0 to 0
    // Pre-compute per-book incident link counts so we can scan candidates per book
    // without iterating the full pair map each time.
    val incidentByBook = HashMap<Long, MutableList<Pair<Long, Int>>>()
    for ((pair, count) in linkCountByBookPair) {
        if (count < LINK_DENSITY_BASE_FLOOR) continue
        val (a, b) = pair
        incidentByBook.getOrPut(a) { ArrayList() }.add(b to count)
        incidentByBook.getOrPut(b) { ArrayList() }.add(a to count)
    }

    var booksTouched = 0
    var basesAssigned = 0
    for ((d, meta) in bookMetaById.toMap()) {
        if (meta.dependence == null) continue
        if (meta.baseTextBookIds.isNotEmpty()) continue
        val incident = incidentByBook[d] ?: continue
        val primaryCandidates = incident
            .asSequence()
            .filter { (b, _) -> bookMetaById[b]?.dependence == null }
            .sortedByDescending { it.second }
            .map { it.first }
            .toHashSet()
        if (primaryCandidates.isEmpty()) continue
        bookMetaById[d] = meta.copy(baseTextBookIds = primaryCandidates)
        booksTouched++
        basesAssigned += primaryCandidates.size
    }
    logger.i {
        "Primary-base inference for empty-base dependants: " +
            "assigned $basesAssigned base edges across $booksTouched books"
    }
    return booksTouched to basesAssigned
}

/**
 * Fills the gaps Sefaria leaves in a super-commentary's declared base list.
 *
 * A dependant `D` that declares another *dependant* `B` as its base is a
 * declared super-commentary on `B`'s family. Sefaria lists those bases per
 * volume and sometimes skips one: both Mechokekei Yehudah books declare Ibn
 * Ezra on Genesis, Leviticus, Numbers and Deuteronomy, but neither Ibn Ezra
 * on Exodus nor Ibn Ezra HaKatzar on Exodus, although 6,513 + 2,889 of Yahel
 * Ohr's links are typed `commentary` against them.
 *
 * `X` is added to `D.baseTextBookIds` iff
 *   1. `X` is a dependant of the same family as one of `D`'s declared dependant
 *      bases — same `collective_title` (the long Ibn Ezra) or a shared author
 *      (HaKatzar has its own collective title but the same author);
 *   2. `X` is itself based on a book `D` declares (Exodus) — so the family is
 *      completed only over the base texts `D` already says it follows;
 *   3. `linkCount(D, X) >= LINK_DENSITY_BASE_FLOOR`, the same noise floor the
 *      other density rules use; and
 *   4. `X` does not already list `D` as its base.
 *
 * Without this, [applyLinkDensitySiblingChaining] sees Yahel Ohr as the denser
 * sibling on Exodus (6,745 links to the verses against Ibn Ezra's 2,337) and
 * chains it as a *base* of Ibn Ezra — every link of the pair is stored
 * backwards and Yahel Ohr leaves Ibn Ezra's commentator panel.
 *
 * Only `baseTextBookIds` grows: the declared and inferred provenance sets are
 * untouched, so `book_base_text` and `baseProvenance` stay as before.
 */
internal fun completeDeclaredBaseFamilies(
    bookMetaById: MutableMap<Long, BookMeta>,
    linkCountByBookPair: Map<Pair<Long, Long>, Int>,
    logger: Logger,
): Pair<Int, Int> {
    if (linkCountByBookPair.isEmpty()) return 0 to 0
    fun linkCount(a: Long, b: Long): Int =
        if (a == b) 0 else linkCountByBookPair[if (a < b) a to b else b to a] ?: 0

    // Candidates are found through the pairs that clear the floor anyway (rule 3).
    val incidentByBook = HashMap<Long, MutableList<Long>>()
    for ((pair, count) in linkCountByBookPair) {
        if (count < LINK_DENSITY_BASE_FLOOR) continue
        incidentByBook.getOrPut(pair.first) { ArrayList() }.add(pair.second)
        incidentByBook.getOrPut(pair.second) { ArrayList() }.add(pair.first)
    }

    val snapshot = bookMetaById.toMap()
    val additionsByBook = HashMap<Long, Set<Long>>()
    for ((d, meta) in snapshot) {
        if (meta.dependence == null) continue
        val declaredDependantBases = meta.sefariaDeclaredBaseTextBookIds
            .mapNotNull { b -> snapshot[b]?.takeIf { it.dependence != null } }
        if (declaredDependantBases.isEmpty()) continue
        val familyCollectives = declaredDependantBases.mapNotNullTo(HashSet()) { it.collectiveTitleEn }
        val familyAuthors = declaredDependantBases.flatMapTo(HashSet()) { it.authorKeys }

        val additions = incidentByBook[d].orEmpty().filterTo(HashSet()) { x ->
            val xMeta = snapshot[x] ?: return@filterTo false
            x != d &&
                x !in meta.baseTextBookIds &&
                xMeta.dependence != null &&
                d !in xMeta.baseTextBookIds &&
                xMeta.baseTextBookIds.any { it in meta.sefariaDeclaredBaseTextBookIds } &&
                (xMeta.collectiveTitleEn in familyCollectives || xMeta.authorKeys.any { it in familyAuthors }) &&
                linkCount(d, x) >= LINK_DENSITY_BASE_FLOOR
        }
        if (additions.isNotEmpty()) additionsByBook[d] = additions
    }
    for ((d, additions) in additionsByBook) {
        val meta = bookMetaById.getValue(d)
        bookMetaById[d] = meta.copy(baseTextBookIds = meta.baseTextBookIds + additions)
    }
    val edges = additionsByBook.values.sumOf { it.size }
    logger.i {
        "Declared-family base completion: added $edges base edges across ${additionsByBook.size} books"
    }
    return additionsByBook.size to edges
}

/**
 * For every dependant book `D` with declared base `P`, finds siblings `S`
 * (other dependants that also declare `P` as base) whose link-density
 * ratio to `D` clears [LINK_DENSITY_CHAIN_THRESHOLD], and adds `S` to
 * `D.baseTextBookIds`.
 *
 * The heuristic uses these rules as Sefaria's link counts drift:
 *  - **Never against a declaration.** `S` is skipped when it already lists `D`
 *    as its base. Chaining it anyway made the pair mutual, the resolver could no
 *    longer orient it, and every link fell to OTHER or to priorityRank: Magen
 *    Avraham ↔ Machatzit HaShekel (whose schema declares Magen Avraham), Turei
 *    Zahav ↔ Peri Megadim, Ibn Ezra on Genesis ↔ Yahel Ohr.
 *  - **Per work, not only per volume.** The ratio is also aggregated over all the
 *    volumes of `D`'s `collective_title` against the same sibling collective, and
 *    a volume that misses the threshold alone still chains when the work as a
 *    whole clears it. Maharam Schiff → Tosafot is 1.14 over its seven tractates,
 *    yet Chullin fell from 0.86 to 0.79 between two exports and Ketubot sits at
 *    0.79, so their typed commentary links were downgraded to OTHER. The
 *    per-volume result is never withdrawn by the aggregate.
 *  - **Sparse bases can fill gaps.** First score only shared bases with at least
 *    [LINK_DENSITY_BASE_FLOOR] links to `D`. Then also score the sparse shared
 *    bases, applying the floor to the larger side of the ratio. For example,
 *    Pilpula Charifta on Bava Batra has 28 links to the tractate and 590 to the
 *    Rosh. The second score can add edges where the first offered no direction
 *    or conflict for the pair.
 *
 * This *augments* Sefaria's metadata; it never removes a base text that
 * Sefaria already declared. Returns `(booksTouched, edgesAdded)` for logging.
 */
internal fun applyLinkDensitySiblingChaining(
    bookMetaById: MutableMap<Long, BookMeta>,
    linkCountByBookPair: Map<Pair<Long, Long>, Int>,
    logger: Logger,
): Pair<Int, Int> {
    if (linkCountByBookPair.isEmpty()) return 0 to 0

    // Index: for each declared base P, which dependant books declared it?
    // We use baseTextBookIds (post-resolution) — that's the source of truth
    // after schema parsing.
    val basesToDependants = HashMap<Long, MutableSet<Long>>()
    for ((bookId, meta) in bookMetaById) {
        if (meta.dependence == null) continue
        for (baseId in meta.baseTextBookIds) {
            basesToDependants.getOrPut(baseId) { HashSet() }.add(bookId)
        }
    }

    fun linkCount(a: Long, b: Long): Int {
        if (a == b) return 0
        val key = if (a < b) a to b else b to a
        return linkCountByBookPair[key] ?: 0
    }

    // A sibling chained into D's bases must not already have D among its own
    // bases (see the KDoc); every check reads this pre-chaining snapshot.
    val snapshot = bookMetaById.toMap()

    val candidatesByBook = HashMap<Long, Map<String, SiblingCandidates>>()
    val mutualSkipped = HashSet<Pair<Long, Long>>()
    for ((d, meta) in snapshot) {
        if (meta.dependence == null) continue
        val declaredBases = meta.baseTextBookIds
        if (declaredBases.isEmpty()) continue

        // Aggregate candidate siblings by their schema `collective_title.en`
        // (e.g. "Rashi" groups the 5 Rashi-on-Torah volumes). The per-volume
        // ratio is too noisy to distinguish a super-commentary (Mizrachi:
        // all 5 volumes ≥ 0.91) from a citation pattern (Bartenura on Torah:
        // volumes spread 0.57–0.82). The per-collective aggregate
        // `Σ lc(D, S_i) / Σ lc(D, p_i_shared)` collapses that volume noise:
        // Mizrachi→Rashi = 0.94, Levush HaOrah→Rashi = 0.83, Bartenura→Rashi
        // = 0.69. Threshold 0.8 then cleanly separates them.
        val evidenceByCollective = HashMap<String, SiblingEvidence>()
        for (p in declaredBases) {
            val nDP = linkCount(d, p)
            val siblings = basesToDependants[p] ?: continue
            for (s in siblings) {
                if (s == d || s in declaredBases) continue
                val nDS = linkCount(d, s)
                if (nDS == 0) continue
                // Asymmetry guard: only chain D→S when S is *more* directly
                // linked to the shared base P than D is. Picks the canonical
                // commentary (S) as base of the deeper-chained super-
                // commentary (D).
                val nSP = linkCount(s, p)
                if (nSP <= nDP) continue
                // S already depends on D: chaining back would cancel S's own
                // declaration in the resolver.
                if (d in snapshot[s]?.baseTextBookIds.orEmpty()) {
                    mutualSkipped += d to s
                    continue
                }
                // Singletons (no collective_title) get a per-book bucket so
                // they're still considered, just on a per-book ratio.
                val key = snapshot[s]?.collectiveTitleEn ?: "book#$s"
                evidenceByCollective.getOrPut(key) { SiblingEvidence() }.add(s, p, nDS, nDP)
            }
        }

        if (evidenceByCollective.isNotEmpty()) {
            candidatesByBook[d] = evidenceByCollective.mapValues { (_, evidence) -> evidence.toCandidates() }
        }
    }

    val dense = proposeSiblingEdges(candidatesByBook, snapshot, SiblingCandidates::dense)
    val combined = proposeSiblingEdges(candidatesByBook, snapshot, SiblingCandidates::combined)
    val additionsByBook = HashMap<Long, MutableSet<Long>>()
    var workEdges = 0
    var conflicts = 0
    var sparseEdges = 0
    for (edge in dense.edges) {
        val (d, s) = edge
        if (s to d in dense.edges) {
            conflicts++
            continue
        }
        additionsByBook.getOrPut(d) { HashSet() }.add(s)
        if (edge in dense.workEdges) workEdges++
    }
    for (edge in combined.edges) {
        if (edge in dense.edges) continue
        val (d, s) = edge
        val reverse = s to d
        // A sparse proposal cannot reverse a dense direction or resolve a dense tie.
        if (reverse in dense.edges || reverse in combined.edges) {
            conflicts++
            continue
        }
        additionsByBook.getOrPut(d) { HashSet() }.add(s)
        sparseEdges++
        if (edge in combined.workEdges) workEdges++
    }

    for ((d, additions) in additionsByBook) {
        val meta = bookMetaById.getValue(d)
        bookMetaById[d] = meta.copy(baseTextBookIds = meta.baseTextBookIds + additions)
    }
    val booksTouched = additionsByBook.size
    val edgesAdded = additionsByBook.values.sumOf { it.size }
    logger.i {
        "Link-density chaining: added $edgesAdded sibling base edges across $booksTouched books " +
            "(threshold $LINK_DENSITY_CHAIN_THRESHOLD; $workEdges of them from the per-work aggregate; " +
            "$sparseEdges using sparse shared bases; " +
            "${mutualSkipped.size} siblings skipped because they already depend on the book" +
            (if (conflicts > 0) "; $conflicts conflicting proposals dropped" else "") + ")"
    }
    return booksTouched to edgesAdded
}

private class SiblingEvidence {
    private val members = HashSet<Long>()
    private val denseMembers = HashSet<Long>()
    private val sharedBases = HashSet<Long>()
    private var sumDS = 0L
    private var sumDP = 0L
    private var denseSumDS = 0L
    private var denseSumDP = 0L

    fun add(sibling: Long, sharedBase: Long, linksToSibling: Int, linksToSharedBase: Int) {
        // Count each sibling and shared base once, even if several share the same base.
        if (members.add(sibling)) sumDS += linksToSibling
        val newBase = sharedBases.add(sharedBase)
        if (newBase) sumDP += linksToSharedBase
        if (linksToSharedBase >= LINK_DENSITY_BASE_FLOOR) {
            if (denseMembers.add(sibling)) denseSumDS += linksToSibling
            if (newBase) denseSumDP += linksToSharedBase
        }
    }

    fun toCandidates(): SiblingCandidates {
        val dense = if (denseMembers.isEmpty()) null else SiblingCandidate(denseMembers, denseSumDS, denseSumDP)
        return SiblingCandidates(dense, SiblingCandidate(members, sumDS, sumDP))
    }
}

private class SiblingCandidate(val members: Set<Long>, val sumDS: Long, val sumDP: Long)

private class SiblingCandidates(val dense: SiblingCandidate?, val combined: SiblingCandidate)

private class SiblingProposals(val edges: Set<Pair<Long, Long>>, val workEdges: Set<Pair<Long, Long>>)

private fun proposeSiblingEdges(
    candidatesByBook: Map<Long, Map<String, SiblingCandidates>>,
    bookMetaById: Map<Long, BookMeta>,
    select: (SiblingCandidates) -> SiblingCandidate?,
): SiblingProposals {
    val edges = HashSet<Pair<Long, Long>>()
    val volumesByWorkPair = HashMap<Pair<String, String>, MutableList<Pair<Long, SiblingCandidate>>>()
    for ((d, groups) in candidatesByBook) {
        val work = bookMetaById[d]?.collectiveTitleEn
        for ((key, group) in groups) {
            val candidate = select(group) ?: continue
            if (clearsChainThreshold(candidate.sumDS, candidate.sumDP)) {
                for (s in candidate.members) edges.add(d to s)
            }
            if (work != null) volumesByWorkPair.getOrPut(work to key) { ArrayList() }.add(d to candidate)
        }
    }

    // Per-work aggregate over at least two volumes with the same sibling collective.
    val workEdges = HashSet<Pair<Long, Long>>()
    for (volumes in volumesByWorkPair.values) {
        if (volumes.size < 2) continue
        val sumDS = volumes.sumOf { it.second.sumDS }
        val sumDP = volumes.sumOf { it.second.sumDP }
        if (!clearsChainThreshold(sumDS, sumDP)) continue
        for ((d, candidate) in volumes) {
            for (s in candidate.members) {
                val edge = d to s
                if (edges.add(edge)) workEdges.add(edge)
            }
        }
    }
    return SiblingProposals(edges, workEdges)
}

private fun clearsChainThreshold(sumDS: Long, sumDP: Long): Boolean =
    maxOf(sumDS, sumDP) >= LINK_DENSITY_BASE_FLOOR &&
        (sumDP == 0L || sumDS.toDouble() / sumDP >= LINK_DENSITY_CHAIN_THRESHOLD)

/**
 * Per-pair count of links Sefaria typed commentary/targum: `links_by_book.csv`
 * minus `links_by_book_without_commentary.csv`. Fails if the two files disagree.
 */
internal fun dependantTypedLinkCounts(
    allLinks: Map<Pair<Long, Long>, Int>,
    withoutCommentary: Map<Pair<Long, Long>, Int>,
): Map<Pair<Long, Long>, Int> {
    val out = HashMap<Pair<Long, Long>, Int>(allLinks.size)
    for ((key, rest) in withoutCommentary) {
        val total = allLinks[key] ?: error("links_by_book_without_commentary pair $key missing from links_by_book")
        require(rest <= total) { "links_by_book_without_commentary count $rest > links_by_book $total for $key" }
    }
    for ((key, total) in allLinks) {
        val dependantTyped = total - (withoutCommentary[key] ?: 0)
        if (dependantTyped > 0) out[key] = dependantTyped
    }
    return out
}

/** (primary S, dependant D) edges (Sifra → Malbim on Leviticus) where commentary-typed lc(D,S) ≥ floor
 *  and ≥ threshold × lc(D, densest declared base). Used only as a cross-corpus demotion exemption. */
internal fun findPrimaryBaseDensityEdges(
    bookMetaById: Map<Long, BookMeta>,
    dependantLinkCountByBookPair: Map<Pair<Long, Long>, Int>,
    logger: Logger,
): Set<Pair<Long, Long>> {
    val incidentByBook = HashMap<Long, MutableList<Pair<Long, Int>>>()
    for ((pair, count) in dependantLinkCountByBookPair) {
        if (count < LINK_DENSITY_BASE_FLOOR) continue
        val (a, b) = pair
        incidentByBook.getOrPut(a) { ArrayList() }.add(b to count)
        incidentByBook.getOrPut(b) { ArrayList() }.add(a to count)
    }
    fun linkCount(a: Long, b: Long): Int =
        dependantLinkCountByBookPair[if (a < b) a to b else b to a] ?: 0

    val edges = HashSet<Pair<Long, Long>>()
    for ((d, meta) in bookMetaById) {
        if (meta.dependence == null) continue
        val declaredBases = meta.sefariaDeclaredBaseTextBookIds + meta.inferredBaseTextBookIds
        if (declaredBases.isEmpty()) continue
        val nDP = declaredBases.maxOf { linkCount(d, it) }
        if (nDP == 0) continue
        val additions = incidentByBook[d].orEmpty()
            .filter { (s, nDS) ->
                s !in meta.baseTextBookIds &&
                    bookMetaById[s]?.let { it.dependence == null } == true &&
                    nDS.toDouble() / nDP >= LINK_DENSITY_CHAIN_THRESHOLD
            }
            .map { it.first }
        additions.forEach { edges.add(it to d) }
    }
    logger.i {
        "Primary-base density edges: ${edges.size} demotion-exempt edges across " +
            "${edges.map { it.second }.toSet().size} books (threshold $LINK_DENSITY_CHAIN_THRESHOLD)"
    }
    return edges
}
