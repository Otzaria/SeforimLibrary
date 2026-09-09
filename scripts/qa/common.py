"""עזרי-QA משותפים: נירמול כותרות, קריאת schemas, פתיחת DB. stdlib בלבד."""
import json
import os
import re
import sqlite3
import sys

_WS = re.compile(r"\s+")


def normalize_title_key(value):
    # שכפול מדויק של normalizeTitleKey ב-SefariaImportText.kt.
    if value is None or value.strip() == "":
        return None
    s = (value.replace('"', "").replace("'", "")
         .replace("׳", "").replace("״", ""))
    s = s.lower()
    s = _WS.sub(" ", s)
    s = s.replace("_", " ")
    return s.strip()


def _str_or_none(v):
    return v if isinstance(v, str) and v != "" else None


def sanitize_folder(name):
    # שכפול sanitizeFolder (SefariaImportText.kt): '"'→'״' + trim.
    if name is None or name.strip() == "":
        return ""
    return name.replace('"', "״").strip()


def flatten_talmud_categories(parts):
    # שכפול flattenTalmudCategories (SefariaImportOrdering.kt).
    flattened = []
    idx = 0
    while idx < len(parts):
        part = parts[idx]
        if part == "תלמוד" and idx + 1 < len(parts) and parts[idx + 1] in ("בבלי", "ירושלמי"):
            flattened.append("תלמוד " + parts[idx + 1])
            idx += 2
            continue
        flattened.append(part)
        idx += 1
    return flattened


def normalize_priority_entry(raw):
    # שכפול normalizePriorityEntry (SefariaImportOrdering.kt).
    entry = raw.strip().replace("\\", "/").lstrip("/")
    parts = [sanitize_folder(p) for p in entry.split("/") if p.strip() != ""]
    return "/".join(flatten_talmud_categories(parts))


def normalized_book_path(categories_he, he_title):
    return "/".join([sanitize_folder(c) for c in categories_he] + [sanitize_folder(he_title)])


def default_priority_list_path():
    # priority.txt הוא resource מגורסן באותו repo — נתיב יחסי לסקריפט, לא אבסולוטי.
    return os.path.normpath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "..",
        "generator", "sefariasqlite", "src", "jvmMain", "resources", "priority.txt"))


def load_priority_list(path):
    if not os.path.isfile(path):
        die(f"priority.txt לא נמצא: {path}")
    with open(path, encoding="utf-8") as fh:
        entries = []
        for line in fh:
            s = line.strip()
            if s == "" or s.startswith("#"):
                continue
            n = normalize_priority_entry(s)
            if n != "":
                entries.append(n)
    return entries


def order_books_by_priority(books, priority_entries):
    # שכפול applyPriorityOrdering: מתאימי-priority לפי סדר הרשימה, השאר בסדר הקלט.
    if not priority_entries:
        return list(books)
    lookup = {}
    for b in books:
        lookup[normalized_book_path(b.categories_he, b.he_title)] = b  # אחרון מנצח (associateBy)
    used = set()
    ordered = []
    for entry in priority_entries:
        b = lookup.get(entry)
        if b is not None and entry not in used:
            used.add(entry)
            ordered.append(b)
    for b in books:
        if normalized_book_path(b.categories_he, b.he_title) not in used:
            ordered.append(b)
    return ordered


def _dir_has_schema_books(d):
    # מאתר לפחות schema אחד תקין (dict עם "schema" מקונן); עצירה מוקדמת.
    for fn in os.listdir(d):
        if not fn.endswith(".json"):
            continue
        try:
            with open(os.path.join(d, fn), encoding="utf-8") as fh:
                top = json.load(fh)
        except (ValueError, OSError):
            continue
        if isinstance(top, dict) and isinstance(top.get("schema"), dict):
            return True
    return False


def resolve_schemas_dir(sefaria_dir):
    # מקבל schemas/ ישירות, export/ (מכיל schemas/), או sefaria/ (מכיל export/schemas/).
    # סדר קבוע: schemas תחילה, אחר-כך export/schemas, ורק לבסוף התיקייה עצמה — כדי
    # ש-export/ (המכיל table_of_contents.json בלבד) לא ייבחר בטעות במקום export/schemas.
    if not os.path.isdir(sefaria_dir):
        die(f"--sefaria-dir אינו תיקייה: {sefaria_dir}")
    candidates = [os.path.join(sefaria_dir, "schemas"),
                  os.path.join(sefaria_dir, "export", "schemas"),
                  sefaria_dir]
    attempted = []
    for cand in candidates:
        if not (os.path.isdir(cand) and any(f.endswith(".json") for f in os.listdir(cand))):
            continue
        attempted.append(cand)
        # מועמד עם *.json אך ללא ולו schema אחד (למשל export/ עם toc בלבד) — נמשיך הלאה.
        if _dir_has_schema_books(cand):
            return cand
    die("לא נמצאה תיקיית schemas עם קובצי schema תקינים; נוסו: "
        + ", ".join(attempted or candidates))


def build_normalized_title_to_bookid(schema_dbid):
    # שכפול buildNormalizedTitleToBookId (SefariaDirectImporter.kt): שני מעברים גלובליים
    # על הספרים בסדר-priority — כל הפרימריז (heTitle→enTitle, putIfAbsent) ואז כל
    # ה-aliases (putIfAbsent). הפיצול מבטיח שפרימרי מנצח alias, גם alias של ספר מוקדם.
    # קלט: רשימת (SchemaBook, dbid) בסדר-priority; ערך dbid בערך None מדולג.
    norm_to_id = {}
    for b, dbid in schema_dbid:  # מעבר 1: פרימריז של כל הספרים
        if dbid is None:
            continue
        for t in (b.he_title, b.en_title):
            n = normalize_title_key(t)
            if n is not None:
                norm_to_id.setdefault(n, dbid)
    for b, dbid in schema_dbid:  # מעבר 2: aliases של כל הספרים
        if dbid is None:
            continue
        for a in b.alias_keys:
            norm_to_id.setdefault(a, dbid)
    return norm_to_id


class SchemaBook:
    __slots__ = ("en_title", "he_title", "raw_dependence", "declared_keys",
                 "alias_keys", "collective_he", "collective_en", "path",
                 "categories_he")


def _extract_base_text_keys(top, schema):
    arr = top.get("base_text_titles")
    if not isinstance(arr, list):
        arr = schema.get("base_text_titles")
    if not isinstance(arr, list):
        return []
    keys = []
    for el in arr:
        if isinstance(el, dict):
            for k in ("en", "he"):
                n = normalize_title_key(_str_or_none(el.get(k)))
                if n is not None:
                    keys.append(n)
        elif isinstance(el, str):
            n = normalize_title_key(el)
            if n is not None:
                keys.append(n)
    return list(dict.fromkeys(keys))


def _extract_alias_keys(top, schema):
    out = []
    for key in ("titleVariants", "heTitleVariants"):
        arr = top.get(key)
        if not isinstance(arr, list):
            arr = schema.get(key)
        if not isinstance(arr, list):
            continue
        for el in arr:
            if not isinstance(el, str) or el.strip() == "":
                continue
            if "<" in el or ">" in el:
                continue
            n = normalize_title_key(el)
            if n is not None:
                out.append(n)
    return list(dict.fromkeys(out))


def _extract_raw_dependence(top, schema):
    v = top.get("dependence")
    if not isinstance(v, str):
        v = schema.get("dependence")
    if not isinstance(v, str):
        return None
    v = v.strip().lower()
    return v if v != "" else None


def _file_title(path):
    base = os.path.basename(path)[:-5] if path.endswith(".json") else os.path.basename(path)
    return base.replace("_", " ")


# קובצי schema שהייצוא המקובע של ספריא שולח בכוונה כך שאינם נטענים לספר, עם הסיבה.
# Sheet.json הוא ה-pseudo-index של "דפי מקורות" (Sheets) — לא ספר: הייצוא כותב
# אותו כקובץ באורך 0, ולכן אין לו schema, אין לו heTitle ואין לו שורה ב-book.
# היבואן עצמו מפיל אותו באותה הדרך בדיוק (SefariaBookPayloadReader.buildSchemaLookup,
# runCatching פר-קובץ), ולכן דילוג עליו כאן אינו מקטין כיסוי — הוא משחזר את התנהגות
# היבואן. כל קובץ אחר שאינו נטען לספר הוא נזק בארכיון מקובע ומאומת-digest, ונכשל בקול.
KNOWN_UNREADABLE_SCHEMAS = {
    "Sheet.json": "ה-pseudo-index של דפי-מקורות בספריא, נשלח כקובץ באורך 0; "
                  "היבואן מפיל אותו זהה, ואין לו ספר ב-DB",
}


def load_schema_books(schemas_dir):
    # שכפול קריאת SefariaBookPayloadReader.kt: כותרות מ-schema המקונן, dependence/base מ-top עם fallback.
    books = []
    known_skipped = []
    unreadable = []
    for fn in sorted(os.listdir(schemas_dir)):
        if not fn.endswith(".json"):
            continue
        path = os.path.join(schemas_dir, fn)
        try:
            with open(path, encoding="utf-8") as fh:
                top = json.load(fh)
        except (ValueError, OSError) as e:
            # שכפול דטרמיניסטי של runCatching פר-קובץ ב-SefariaBookPayloadReader.kt:33-49
            # (buildSchemaLookup בולע קובץ לא-פריס בשקט).
            if fn in KNOWN_UNREADABLE_SCHEMAS:
                known_skipped.append(fn)
                continue
            unreadable.append((fn, str(e)))
            continue
        # JSON תקין אך לא-שמיש הוא בדיוק אותו נזק כמו JSON לא-פריס: היבואן מפיל גם
        # אותו (בלי אובייקט schema מקונן אין payload), הספר נעדר גם מהצפי וגם מה-DB,
        # וכל בדיקות ההשוואה עוברות על קבוצה שהצטמצמה בשקט מתחת לשער הסחיפה. לכן
        # אותה הנהלת-חשבונות בדיוק: החרגה מנומקת ב-KNOWN_UNREADABLE_SCHEMAS, או כשל.
        if not isinstance(top, dict):
            unusable = f"top-level JSON is {type(top).__name__}, not an object"
        else:
            schema = top.get("schema")
            unusable = None if isinstance(schema, dict) else "no object-valued 'schema' key"
        if unusable is not None:
            if fn in KNOWN_UNREADABLE_SCHEMAS:
                known_skipped.append(fn)
                continue
            unreadable.append((fn, unusable))
            continue
        en = _str_or_none(schema.get("title")) or _file_title(path)
        he = _str_or_none(schema.get("heTitle")) or en
        cats = top.get("heCategories")
        if not isinstance(cats, list):
            cats = schema.get("heCategories")
        cats = [c for c in cats if isinstance(c, str)] if isinstance(cats, list) else []
        ct = top.get("collective_title")
        ct = ct if isinstance(ct, dict) else {}
        b = SchemaBook()
        b.path = path
        b.en_title = en
        b.he_title = he
        # כמו payload.categoriesHe: ‏flattenTalmudCategories(heCategories.map(sanitizeFolder)).
        b.categories_he = flatten_talmud_categories([sanitize_folder(c) for c in cats])
        b.raw_dependence = _extract_raw_dependence(top, schema)
        b.declared_keys = _extract_base_text_keys(top, schema)
        b.alias_keys = _extract_alias_keys(top, schema)
        ch = _str_or_none(ct.get("he"))
        ce = _str_or_none(ct.get("en"))
        b.collective_he = ch.strip() if ch else None
        b.collective_en = ce.strip() if ce else None
        if b.collective_he == "":
            b.collective_he = None
        if b.collective_en == "":
            b.collective_en = None
        books.append(b)
    # דילוג ידוע = שורת INFO אחת שמסבירה למה, במקום אזהרה + שורת-ספירה שאיש אינו פועל לפיהן.
    if known_skipped:
        print("INFO: schemas ידועים שאינם ספרים, מדולגים: " + "; ".join(
            f"{fn} ({KNOWN_UNREADABLE_SCHEMAS[fn]})" for fn in known_skipped))
    # כל schema אחר שאינו נטען לספר בארכיון ספריא המקובע (שה-digest שלו אומת) הוא
    # נזק אמיתי — לא-פריס, או פריס אך בלי אובייקט schema: דילוג שקט עליו היה מצמצם
    # את כיסוי הבדיקה ועדיין מדווח PASS. כשל רועש.
    if unreadable:
        for fn, msg in unreadable[:10]:
            print(f"  schema לא-שמיש: {fn} ({msg})", file=sys.stderr)
        die(f"{len(unreadable)} קובצי schema לא-שמישים בארכיון ספריא המקובע "
            f"(מעבר ל-{sorted(KNOWN_UNREADABLE_SCHEMAS)} הידועים) — "
            "כיסוי הבדיקה היה מצטמצם בשקט")
    return books


def open_db(db_path):
    if not os.path.isfile(db_path):
        die(f"קובץ DB לא קיים: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def sefaria_source_id(conn):
    # id שורת source בשם 'Sefaria' — בדיקות 1/2/6 מסננות אליו את book (book.sourceId),
    # אחרת ספרי מקורות אחרים (למשל MoreBooks) ששמם צירוף-מקרים זהה ל-schema דולפים פנימה.
    require_columns(conn, "book", ["sourceId"])
    require_columns(conn, "source", ["id", "name"])
    row = conn.execute("SELECT id FROM source WHERE name = 'Sefaria'").fetchone()
    if row is None:
        die("אין שורת source בשם 'Sefaria' ב-DB")
    return row["id"]


def require_columns(conn, table, cols):
    have = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
    if not have:
        die(f"טבלה חסרה ב-DB: {table}")
    missing = [c for c in cols if c not in have]
    if missing:
        die(f"עמודות חסרות בטבלה {table}: {', '.join(missing)}")


def die(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def ok(msg):
    print(f"PASS: {msg}")
    sys.exit(0)


# ─── שער סחיפה מול ה-reference snapshot ────────────────────────────────────
# עד כאן כל בדיקה שנושאת baseline קשיח רק הדפיסה אותו לצד הערך הנמדד ועברה בכל
# מקרה. הרצה 34024655297 פרסמה DB עם dependenceType 4940 מול snapshot 4941,
# book_base_text 5425 מול 5426 ו-baseProvenance=1 12970 מול 13056 (‎−0.659%) —
# שלושתן PASS. סחיפה איטית הייתה נראית רק לאדם שקורא את הלוג.
#
# השער חד-צדדי ונדיב בכוונה:
#   * התכווצות גדולה מ-QA_DRIFT_MAX_SHRINK_PCT מה-snapshot → ‎::error:: ויציאה 1
#   * כל הפרש אחר (התכווצות קטנה יותר, או גדילה) → ‎::warning:: עם המספרים,
#     כי הקורפוס אכן זז בין ייצוא לייצוא וזה לא יהפוך לאזעקת-שווא שבועית.
# הסף מגיע מהסביבה: QA_DRIFT_MAX_SHRINK_PCT, וברירת המחדל שלו נקבעת ב-workflow
# השבועי (צעד ה-QA). כשהמשתנה אינו מוגדר השער כבוי — ה-baselines משמעותיים רק מול
# הקורפוס המקובע האמיתי, לא מול פיקסטורה סינתטית או DB חלקי בהרצת אד-הוק. כדי
# שהשער לא ייעלם בשקט מהרצת release, run_all.py ‎--require-all נכשל כשהוא אינו
# מוגדר. אין בכך רענון snapshot: ‎--expect-snapshot ממשיך לאכוף את המספרים המדויקים.
DRIFT_ENV = "QA_DRIFT_MAX_SHRINK_PCT"
# רצפה מוחלטת: התכווצות היא ‎::error:: רק כשהיא חוצה גם את האחוז וגם את המספר
# הזה. בלעדיה, על מדד קטן (guides=2, midrash=5, זוגות מוסקים=20, targum=45)
# ירידה של 1 = 2%..50% והייתה מפילה בנייה שבועית על ספר יחיד שנעלם מהייצוא.
# ברירת המחדל 10 חלה גם כשהמשתנה אינו מוגדר; ה-workflow קובע אותו במפורש.
DRIFT_ABS_ENV = "QA_DRIFT_MIN_SHRINK_ABS"
DRIFT_ABS_DEFAULT = 10


def drift_max_shrink_pct():
    """הסף באחוזים, או None כשהשער כבוי (המשתנה אינו מוגדר/ריק)."""
    raw = os.environ.get(DRIFT_ENV, "").strip()
    if raw == "":
        return None
    try:
        value = float(raw)
    except ValueError:
        die(f"{DRIFT_ENV} אינו מספר: {raw!r}")
    if value < 0:
        die(f"{DRIFT_ENV} חייב להיות ≥ 0: {raw!r}")
    return value


def drift_min_shrink_abs():
    """הרצפה המוחלטת (מספר שלם ≥ 0); ברירת מחדל DRIFT_ABS_DEFAULT כשלא מוגדר."""
    raw = os.environ.get(DRIFT_ABS_ENV, "").strip()
    if raw == "":
        return DRIFT_ABS_DEFAULT
    try:
        value = int(raw)
    except ValueError:
        die(f"{DRIFT_ABS_ENV} אינו מספר שלם: {raw!r}")
    if value < 0:
        die(f"{DRIFT_ABS_ENV} חייב להיות ≥ 0: {raw!r}")
    return value


def gate_snapshot_drift(label, observed, snapshot):
    """משווה מדד סָפוּר אחד ל-reference snapshot שלו ומחיל את השער.

    יוצא 1 (עם ‎::error::) כשההתכווצות עוברת את הסף; אחרת מדפיס שורה אחת בלבד.
    כשהשער כבוי אינו מדפיס דבר — הערך וה-snapshot כבר הודפסו על ידי הבדיקה עצמה.
    """
    limit = drift_max_shrink_pct()
    if limit is None:
        return
    delta = observed - snapshot
    if delta == 0:
        print(f"drift {label}: 0 (snapshot {snapshot})")
        return
    pct = (abs(delta) * 100.0 / snapshot) if snapshot else float("inf")
    detail = f"{label}: DB={observed} snapshot={snapshot} delta={delta:+d} ({pct:.3f}%)"
    if delta < 0 and pct > limit:
        floor = drift_min_shrink_abs()
        if abs(delta) >= floor:
            print(f"::error::drift {detail} — התכווצות מעבר ל-{DRIFT_ENV}={limit}% "
                  f"וגם ל-{DRIFT_ABS_ENV}={floor}", file=sys.stderr)
            sys.exit(1)
        print(f"::warning::drift {detail} (מעבר ל-{DRIFT_ENV}={limit}%, אך מתחת "
              f"לרצפה המוחלטת {DRIFT_ABS_ENV}={floor})")
        return
    print(f"::warning::drift {detail} (בתוך {DRIFT_ENV}={limit}%)")
