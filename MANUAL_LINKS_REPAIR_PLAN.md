# תוכנית עבודה: סנכרון אוטומטי ומדויק של קישורי Otzaria מול Sefaria

> **גרסה 5 — תוכנית ביצוע מחייבת, מצומצמת ומאומתת אדברסרית.**
>
> מטרת הפרויקט היא דבר אחד בלבד: לאחר כל Sefaria release, לעדכן דטרמיניסטית את האינדקסים של הקישורים הידניים התלויים ב־Sefaria, ליצור commit ב־`otzaria-library`, לארוז Otzaria release שמכיל את אותו commit, ורק אחר כך לבנות את מסד Seforim מאותו זוג releases נעול.
>
> אין fuzzy matching, fallback פוזיציונלי, בחירת "תוצאה ראשונה" או פרסום חלקי. אם אין פתרון יחיד ומוכח — לא נכתב קובץ, לא נוצר commit, לא נוצר Otzaria release ולא נבנה DB.

---

## 0. התוצאה הסופית

```text
SefariaExport release immutable
        ↓  tag + asset SHA-256 + changelog
refreshManualLinks (strict, atomic, no DB)
        ↓
commit אוטומטי וממוקד ב-otzaria-library/main
        ↓
update-library release מאותו commit
        ↓  Otzaria tag + asset SHA-256
SeforimLibrary build מאותם inputs נעולים
        ↓
בדיקות DB + delta + release
```

`line_index_1`/`line_index_2` בקבצי `otzaria-library` יהיו תמיד מיושרים ל־Sefaria export שממנו נבנה ה־DB. ה־ref האנגלי הנשמר בקובץ הוא מקור האמת; האינדקס הוא נגזרת המתעדכנת אוטומטית.

### הכרעת הארכיטקטורה

**העדכון מתבצע לפני אריזת Otzaria, לא בזמן ייבוא הקישורים ל־DB.**

לכן:

- לא משנים את resolver הקישורים ב־`Generator.kt`.
- לא מרחיבים את `LinkData` עבור runtime.
- לא מחווטים `linker_sidecar.tsv` ל־`appendOtzariaLinks`.
- לא בונים DB של כ־7GB כדי לעדכן JSON.
- לא משנים allocator, ‏`lineId`, ‏`linkId`, patch producer או patch verifier.
- הגנרטור ממשיך לקרוא אינדקסים פוזיציונליים; הוא כבר מתעלם משדות JSON לא מוכרים (`Generator.kt:53-56`, ‏`ignoreUnknownKeys=true`).

השינוי היחיד המותר במסלול הבנייה הקיים הוא חיווט input שכבר קיים: להעביר את
`-PsourceDir` גם ל־`generateHavroutaLinks`. כיום `generateSefariaSqlite` כבר מכבד
`-PexportDir`, ומשימות append של Otzaria כבר מכבדות `-PsourceDir`, אבל משימת
Havrouta אינה מעבירה אותו ל־JVM ולכן עלולה להוריד `latest` אחר. זהו תיקון wiring
קטן והכרחי ל־pinning; אין בו שינוי לוגיקת קישורים, schema או IDs.

ההגנה מפני pairing שגוי נעשית באמצעות lineage מחייב לפני תחילת
`generateSeforimDb`.

---

## 1. Scope מדויק

### 1.1 בתוך ה־scope

1. קובצי `*_links.json` הנארזים ב־`otzaria_latest.zip`.
2. רשומות שבהן **צד אחד בדיוק** הוא ספר Sefaria והצד השני אינו Sefaria.
3. הזרקת `ref_1` או `ref_2` לצד הספריאתי.
4. עדכון `line_index_1`/`line_index_2` אחרי כל Sefaria release.
5. הגנה על `start` של משנה ברורה באמצעות hash גולמי של שורת המקור.
6. טיפול מדויק ב־`en_renamed` וב־`he_renamed` מתוך שרשרת changelogs רציפה.
7. גילוי collision של packaged path לפני אריזה.
8. commit אוטומטי, Otzaria release, ובניית DB מאותם inputs נעולים.

### 1.2 מחוץ ל־scope

- קישורי Sefaria↔Sefaria — מיובאים על ידי `SefariaLinksImporter` ומדולגים במסלול הידני (`Generator.kt:1354`).
- LINKER, ‏havrouta ו־alt_toc.
- תיקון drift בספרים שאינם Sefaria.
- `line_hash_1`/`line_hash_2` לכל הקורפוס.
- שכתוב parser הקישורים של `Generator.kt`.
- שינוי schema של ה־DB או שינוי מפתוח IDs.
- תיקון אוטומטי של offset לאחר שינוי תוכן; מצב כזה חוסם release.
- טיפול כללי בכל warning היסטורי שאינו משפיע על קישור בעל צד Sefaria.

### 1.3 קורפוס היעד המדוד

כל מספר כאן הוא **baseline מדוד** ולא קבוע שרירותי: הוא נספר ישירות מעץ
הקישורים, ולא נגזר בהוספת דלתא למספר קודם. המדידה היא מול `otzaria-library`
`f3ddc7fb1cafa85629eaa3dea7ffaf8e0dda9c92` בתוספת מחיקת `דרך ה_links.json`
(§1.4) — כלומר מצב ה־PR — ומול `manual_links_sync.json` שבאותו עץ.

- **81,496 רשומות רלוונטיות עם יעד Sefaria** (`records.target_sefaria_relevant`):
  - 63,107 ב־`National-LibraryToOtzaria/links`, כולן כבר נושאות `ref_2`.
  - 3,151 ב־`MoreBooks/links`, כולן כבר נושאות `ref_2`.
  - 15,238 ב־`DictaToOtzaria/ערוך/links`: ‏14,499 נושאות `ref_2` כבר היום
    ועוד 739 שכתיב ה־`path_2` שלהן תוקן לכתיב ספריא (§4.1). ‏2,566 הרשומות הנותרות
    בשורש הזה הן `הערות על שות רבי משולם איגרא` — יעד שאינו Sefaria, ולכן
    irrelevant נכון וקבוע.
- **17,980 רשומות** משנה ברורה→שער הציון עם מקור Sefaria
  (`records.source_sefaria_relevant`). לכולן יש `start`, ולכן
  `anchors.checked` הוא אף הוא 17,980.
- בסך הכול 99,476 רשומות עם צד Sefaria יחיד.

איך נגזר 81,496: רשומה שכבר נושאת `ref_2` נאכפת בכל ריצה כ־target-Sefaria
(`ref_2 side classification changed`), ולכן ספירת ה־`ref_2` בעץ — 80,757 — היא
הרצפה המדויקת. רשומה ללא ref שיעדה כן Sefaria הייתה נכשלת רועש
(`no bootstrap adapter for` ב־bootstrap/migrate, `new_target_ref_required`
ב־refresh), ולכן התוספת האפשרית היחידה היא רשומות שתיקון הנתונים הופך לפתירות:
בדיוק 739, שהן כל הרשומות חסרות ה־ref בשורש דיקטא ששם היעד שלהן הוא כותרת ספריא
עם גרשיים (‏24 כותרות). ‏80,757 + 739 = 81,496.

הקבוע הישן `65,397` = 63,107 + 2,290 היה נכון ל־`ec6971b0c3e9489fac27e94ed09276e542caf5b3`
בלבד. מאז גדלו MoreBooks ושורש דיקטא, ולכן הוא היה מיושן עוד לפני התיקון הזה —
ואסור היה לעדכן אותו בהוספת 739. קבוע כזה נמדד מחדש, לעולם לא נגזר בדלתא.

מאזן הקורפוס המלא:

```text
462 root files = 425 parsed *_links.json + 37 headings
184,925 source records = 99,476 relevant + 85,449 irrelevant + 0 failures
```

המספר 167,793 הוא תוצאת ה־ZIP הישן אחרי last-wins שהעלים 165 רשומות Wiki
`טורי אבן`; הוא אינו baseline חוקי ל־source scan. אחרי rename+collision gate כל
184,925 הרשומות נשמרות באריזה.

מדידות המיגרציה הבאות נותרו מ־`ec6971b0…` ולא נמדדו מחדש כאן: ‏65,372 יעדים
מיושרים, 23 קישורים שגויים חיים, 2 קישורי אבודרהם חסרים, ו־0 רשומות רלוונטיות
בלתי־פתירות לאחר Grammar A וארבעת ה־overrides המפורשים.

המספרים הם baseline למיגרציה, לא constants בקוד. הכלי מחשב אותם ודורש מאזן סגור בכל ריצה.
ה־corpus fixture חייב לנעול גם את Sefaria `release_metadata.json` ואת tool commit
ששימשו בפועל. סטייה דורשת דוח diff ו־review, לא עדכון אוטומטי של expected counts.

### 1.4 מחיקת `דרך ה_links.json`

הקובץ הוסר מ־`otzaria-library` יחד עם מנגנון `excluded_files` כולו. הנימוק
שהוחזק בקונפיג — both-sides-sefaria — היה שגוי: `sourceTitle` מחזיר `דרך ה`
בעוד ש־heTitle בספריא הוא `דרך ה'` עם גרש, ולכן `primaryHeTitleCount("דרך ה")`
הוא 0 וההחרגה גישרה ידנית מעל אי־התאמת כותרת, לא מעל סיווג both-Sefaria.

הסיבה האמיתית היא יתמות: אין ספר בשם `דרך ה` בספרייה הארוזה ולא ב־DB (רק
`דרך ה'`), ולכן `Generator.kt` נופל על `Source book not found for links` וכל
174 הרשומות נזרקות. תרומת הקובץ ל־DB היא אפס.

מדידה מול ה־DB: ‏88 רשומות ה־self מכוסות במלואן ע"י ספריא (44/44 זוגות),
‏69 מתוך 86 החיצוניות קיימות, וספריא מוסיפה 49 שאין בקובץ. ‏17 קישורים ייחודיים
לאוצריא אבודים כבר היום ואינם ניתנים לשחזור דרך המסלול הידני — ההכרעה היא
לאבד אותם ביודעין ולא להנציח גשר ידני שבור.

---

## 2. עובדות וחוקים מחייבים

### 2.1 בסיסי אינדקס

| מערכת | בסיס |
|---|---:|
| `line_index_X` בקובץ JSON | 1-based |
| `RefEntry.lineIndex` | 1-based |
| `line.lineIndex` ב־DB | 0-based |

ה־updater עובד רק עם JSON ו־`RefEntry`, ולכן:

```text
line_index_X = RefEntry.lineIndex
```

### 2.2 זהות ויחידוּת

- העוגן הנשמר הוא `RefEntry.ref` האנגלי.
- `RefEntry.heRef` משמש בגזירה החד־פעמית ובאימות expected book.
- גם הספר וגם השורה חייבים להיות יחידים.
- מבנה המפה הוא `Map<Key, List<Candidate>>`; רק רשימה בגודל 1 נחשבת פתרון.
- `associateBy`, ‏`first`, ‏`firstOrNull` או overwrite שקט אסורים במסלול הפתרון.

### 2.3 גבול האוטומציה

ניתן לתקן אוטומטית:

- הוספה/מחיקה של שורות לפני ref;
- שינוי אחר שמזיז lineIndex אך שומר ref;
- `en_renamed` או `he_renamed` מפורשים;
- שינוי Reader ששומר ref ותוכן עוגן.

לא ניתן לתקן בלי ניחוש:

- ref שנמחק או השתנה ללא rename;
- ref כפול בספר;
- שינוי תוכן בשורה שנושאת `start`;
- schema change ללא מיפוי מפורש.

כל אחד מאלה חוסם את כל השרשרת.

---

## 3. שינויים מינימליים לפי repository

### 3.1 SeforimLibrary

להוסיף כלי JVM אחד בתוך `generator/sefariasqlite`, כדי להשתמש ב־`SefariaBookPayloadReader` וב־`BookPayload` ה־`internal` בלי לשנות visibility או תלות בין modules.

קבצים מוצעים:

```text
generator/sefariasqlite/src/jvmMain/kotlin/.../manuallinks/
  RefreshManualLinksMain.kt
  ManualLinksConfig.kt
  ManualLinksJson.kt
  SefariaCorpusIndex.kt
  ManualLinksResolver.kt
  ManualLinksBootstrap.kt
  ManualLinksRefresh.kt
  ManualLinksReport.kt
  ManualLinksLineage.kt

generator/sefariasqlite/src/jvmTest/kotlin/.../manuallinks/
  NationalLibraryAdapterTest.kt
  MoreBooksAdapterTest.kt
  TashmaBootstrapTest.kt
  ManualLinksRefreshTest.kt
  ManualLinksAtomicityTest.kt
```

Task חדש ב־`generator/sefariasqlite/build.gradle.kts`:

```text
:sefariasqlite:refreshManualLinks
```

להוסיף ל־`gradle/libs.versions.toml` version נעול של Jackson ול־jvmMain של
`sefariasqlite` את `jackson-core` ו־`jackson-databind`; אלה שתי התלויות החדשות
היחידות. הן נדרשות ל־duplicate detection ולשמירת ordered JSON tree.

באותו קובץ Gradle, להוסיף ל־`generateHavroutaLinks` העברה של `sourceDir` כאשר
ה־property סופק, בדיוק כפי שכבר נעשה ב־`appendOtzariaLines` וב־`appendOtzariaLinks`.
בדיקת workflow תחסום כל build שבו אחת משלוש משימות Otzaria לא קיבלה את אותו
נתיב extracted ונעול.

הכלי **אינו כותב ל־checkout הקלט**. הוא מקבל repo לקריאה ו־output directory ריק, מעתיק אליו את `links_roots`, משנה את העותק, וכותב `.manual-links-refresh-complete` רק אחרי שכל הבדיקות עברו.

### 3.2 otzaria-library

להוסיף:

```text
manual_links_sync.json
manual_links_lineage.json
.github/workflows/sync-manual-links.yml
```

לעדכן:

```text
.github/workflows/update-library.yml
.github/workflows/weekly-pipeline.yml
docs/קישורים-וכותרות.md  (פרק 9, רק לאחר שהמימוש עובד)
```

### 3.3 SefariaExport

לעדכן את release workflow כך שיפרסם release שלם ו־immutable ורק אז יפעיל את
`sync-manual-links.yml` ב־fire-and-forget. SefariaExport אינו בוחר Seforim SHA
ואינו ממתין ל־sync או לבניית DB; האחריות הזאת שייכת ל־otzaria-library.

חובה:

- `concurrency: sefaria-export-release`, ‏`cancel-in-progress:false`;
- tag ייחודי הכולל `run_id` וגם `run_attempt`;
- `release_metadata.json` כולל `tag/run_id/run_attempt`, ‏`previous.tag`,
  ‏`previous.metadata_sha256`, archive descriptor, ו־changelog descriptor עם
  `old_tag/new_tag/name/size/sha256`;
- archive יחיד מיוצג כרשימה בגודל 1; archive מפוצל מורכב רק בסדר שמות החלקים ולאחר אימות hash וגודל של כל חלק, ואז מאומת גם `archive_sha256` של הזרם המחובר;
- `old_tag` ו־`new_tag` ב־`changelog_diff.json` — כבר קיימים;
- `changelog_diff.json` המכונתי נגזר מן manifests/titles המלאים ואינו מסונן לפי
  blacklist שמורד מהרשת. סינון תצוגה לפורום מותר בעותק נפרד בלבד; הוא אינו
  רשאי להעלים rename שה־updater עשוי להזדקק לו;
- previous release ללא metadata/changelog חובה הוא כשל, לא “initial”; רק bootstrap
  ראשון מסומן מפורשות `previous:null`;
- לייצר archive, manifest, titles, changelog ו־metadata לפני פרסום; ליצור release
  כ־draft, להעלות ללא `--clobber`, לאמת מחדש את כל assets, ולפרסם אחרון;
- asset קיים תחת tag אינו מוחלף. release ו־metadata/changelog מה־baseline ואילך
  אינם נמחקים;
- dispatch רק אחרי publication, עם tag, metadata SHA-256 ו־correlation מדויק.
- dispatch מקבל retries קצרים; כשל נשאר failure גלוי. recovery מפעיל ידנית את
  `sync-manual-links.yml` עם אותו tag+digest בלי להריץ מחדש את export ובלי
  לשנות את release שכבר פורסם.

---

## 4. חוזי הקבצים

### 4.0 `release_metadata.json` של SefariaExport

```json
{
  "schema_version": 1,
  "tag": "2026-07-16_18-00-123456789-2",
  "run_id": 123456789,
  "run_attempt": 2,
  "source_commit": "40-hex-sha",
  "previous": {
    "tag": "...",
    "metadata_sha256": "..."
  },
  "archive": {
    "sha256": "sha256-of-concatenated-stream",
    "size": 0,
    "parts": [
      { "name": "sefaria-exports-....tar.zst.part-00", "size": 0, "sha256": "..." }
    ]
  },
  "manifest": { "name": "manifest.txt", "size": 0, "sha256": "..." },
  "titles": { "name": "titles.json", "size": 0, "sha256": "..." },
  "changelog": {
    "name": "changelog_diff.json",
    "size": 0,
    "sha256": "...",
    "old_tag": "...",
    "new_tag": "2026-07-16_18-00-123456789-2"
  }
}
```

`previous:null` מותר רק ב־baseline הראשון. `titles.json` נעשה חובה מן ה־baseline
כדי ש־`en_renamed` תמיד יישא `old_he/new_he`. JSON נכתב קנונית, ללא timestamp
נוסף; ה־SHA-256 שלו מחושב על bytes של ה־asset עצמו ומועבר ב־dispatch.
הקנוניזציה היא `manual-links-canonical-v1` שב־§5.3, עם golden test זהה ב־Python
וב־Kotlin.
`archive.parts` חייב להיות non-empty, שמותיו unique וממוינים לקסיקוגרפית; זהו
גם סדר ה־concatenation. סכום הגדלים חייב להיות `archive.size` וה־hash המחובר
חייב להיות `archive.sha256`.

### 4.1 `manual_links_sync.json`

זהו מקור האמת היחיד לרשימת תיקיות הקישורים הנארזות ול־adapters.

```json
{
  "schema_version": 1,
  "seforim_tool_ref": "refs/heads/otzaria",
  "links_roots": [
    { "path": "Ben-YehudaToOtzaria/links", "expected_state": "present" },
    { "path": "DictaToOtzaria/ערוך/links", "expected_state": "present" },
    { "path": "OnYourWayToOtzaria/links", "expected_state": "absent" },
    { "path": "OraytaToOtzaria/links", "expected_state": "absent" },
    { "path": "tashmaToOtzaria/links", "expected_state": "present" },
    { "path": "sefariaToOtzaria/sefaria_export/links", "expected_state": "absent" },
    { "path": "sefariaToOtzaria/sefaria_api/links", "expected_state": "absent" },
    { "path": "MoreBooks/links", "expected_state": "present" },
    { "path": "wikiJewishBooksToOtzaria/links", "expected_state": "present" },
    { "path": "wikisourceToOtzaria/links", "expected_state": "absent" },
    { "path": "ToratEmetToOtzaria/links", "expected_state": "present" },
    { "path": "pninimToOtzaria/links", "expected_state": "present" },
    { "path": "National-LibraryToOtzaria/links", "expected_state": "present" }
  ],
  "bootstrap_adapters": {
    "National-LibraryToOtzaria/links": "national_library_mishneh_torah_v1",
    "MoreBooks/links": "morebooks_heref_v1",
    "DictaToOtzaria/ערוך/links": "dicta_heref_v1"
  },
  "bootstrap_file_renames": [
    {
      "from": "National-LibraryToOtzaria/links/טורי אבן_links.json",
      "to": "National-LibraryToOtzaria/links/טורי אבן רוקח_links.json",
      "local_book_path": "National-LibraryToOtzaria/ספרים/אוצריא/הלכה/משנה תורה/מפרשים/טורי אבן רוקח.txt",
      "expected_db_title": "טורי אבן רוקח"
    },
    {
      "from": "National-LibraryToOtzaria/links/חידושי ופירושי מהרי''ק_links.json",
      "to": "National-LibraryToOtzaria/links/חידושי ופירושי מהרי״ק_links.json",
      "local_book_path": "National-LibraryToOtzaria/ספרים/אוצריא/הלכה/משנה תורה/מפרשים/חידושי ופירושי מהרי''ק.txt",
      "expected_db_title": "חידושי ופירושי מהרי״ק"
    },
    {
      "from": "National-LibraryToOtzaria/links/נר שמואל ח''א_links.json",
      "to": "National-LibraryToOtzaria/links/נר שמואל ח״א_links.json",
      "local_book_path": "National-LibraryToOtzaria/ספרים/אוצריא/הלכה/משנה תורה/מפרשים/נר שמואל ח''א.txt",
      "expected_db_title": "נר שמואל ח״א"
    }
  ],
  "bootstrap_record_overrides": [
    {
      "path": "MoreBooks/links/סידור אשכנז_links.json",
      "record_sha256": "587aac82846558b9567e62b19c116477541490ca8699a585c94a8a6fc62081c2",
      "post_record_sha256": "248eca8b9237e7100fa17035ac97e454db5c1f09783d7de96fbb49da9778fa3b",
      "require_heRef_2": "סידור ספרד, ערבית לימות החול, , ק\"ש וברכותיה כד,",
      "ref_2": "Siddur Sefard, Weekday Maariv, The Shema,  24",
      "line_index_2": 1145
    },
    {
      "path": "MoreBooks/links/סידור אשכנז_links.json",
      "record_sha256": "54c276b77c60aa55c22f948c2e5a043e0484dee1f3a76ef55439c154f020820c",
      "post_record_sha256": "242160ef88b25c3f2e2253f82be10429846e79f9a5a7de0c8eae45b64c26b159",
      "require_heRef_2": "אבודרהם, סדר תפילות החול, , קריאת שמע וברכותיה ג,",
      "ref_2": "Abudarham, Weekday Prayers, Blessings on the Shema,  3",
      "line_index_2": 191
    },
    {
      "path": "MoreBooks/links/סידור אשכנז_links.json",
      "record_sha256": "bdff1908342eab1e5a456a79a6bdcb28715b57bf1918b8844f2ffe3f385931c4",
      "post_record_sha256": "db45f5c035afc2ee62b389b58cc40a22ef49eb8d625cdedc313d2746e2afe01b",
      "require_heRef_2": "אבודרהם, סדר תפילות החול, , קריאת שמע וברכותיה ג,",
      "ref_2": "Abudarham, Weekday Prayers, Blessings on the Shema,  3",
      "line_index_2": 191
    },
    {
      "path": "MoreBooks/links/סידור אשכנז_links.json",
      "record_sha256": "333fe6fe60b5d55c2be40e84037641f6e8b51440fedfc52bc4fcc06b6a76c028",
      "post_record_sha256": "c5de80e414c37be2d33ebee225354d13f518d374466981db240aa4b8faa23b8f",
      "require_heRef_2": "קדושת לוי, ויקרא, , דרוש לפסח ו,",
      "ref_2": "Kedushat Levi, Leviticus, Homily for Pesach,  6",
      "line_index_2": 715
    }
  ]
}
```

כללים:

- `update-library.yml` וה־updater קוראים `links_roots[].path`; אסור להחזיק רשימת roots נוספת ב־YAML.
- exclude דורש path מדויק וסיבה. אין glob ואין exclude שקט.
- שדות ההוכחה ב־exclude אינם aliases גלובליים: הם חלים רק על הקובץ וה־SHA
  המדויקים. source נפתר לפי English primary exact. ‏86 targets חייבים להיפתר
  לפי basename primary exact; ‏88 self-targets חייבים basename legacy, קידומת
  `heRef` מדויקת ואינדקס בתחום אותו payload. כל count/hash/field שלא נוצל בדיוק
  הוא כשל.
- `expected_state` הוא `present` או `absent`, לא optional. כל סטייה היא כשל: כך
  root פעיל שנמחק אינו נעלם בשקט, ויצירת root שהיה חסר מחייבת config migration
  reviewed לפני שייכנס לאריזה. גם מצבי `absent` נכנסים ל־source tree hash.
- `seforim_tool_ref` הוא ref מורשה, לא SHA שמשתנה אוטומטית. בתחילת sync חדש
  פותרים אותו פעם אחת ל־SHA מלא בן 40 תווים, מוודאים שה־commit reachable מן ה־ref,
  ומשתמשים באותו SHA ל־updater ולבניית ה־DB. recovery של אותו Sefaria target
  משתמש ב־SHA שכבר נכתב ב־lineage ולא פותר HEAD חדש.
- `bootstrap_file_renames` מופעל רק בתוך העותק הזמני ולפני collision scan. לכל
  entry דורשים `local_book_path` קיים ויחיד ו־basename של `to` זהה ל־
  `expected_db_title + "_links.json"`. טבלת המצבים: `from` קיים ו־`to` חסר →
  rename; ‏`from` חסר ו־`to` קיים → `already_applied` לאחר validation; שניהם
  קיימים או שניהם חסרים → כשל. כך bootstrap idempotent בלי לבדוק ספר Otzaria
  מקומי מול `BookPayload` של Sefaria. התאמת `expected_db_title` לתוצאת
  `Generator.normalizeBookTitle(local_book_path.stem)` ננעלת ב־E2E fixture.
- `bootstrap_record_overrides` הם ארבעה חריגים סגורים בלבד. כל entry חייב לפגוע
  רק ברשומה **חסרת `ref_2`** ש־Grammar A לא פתרה. ב־bootstrap הראשוני כל entry
  חייב לפגוע פעם אחת לפי `path + require_heRef_2 + record_sha256`; פגיעה כפולה,
  hash אחר או entry שלא נוצל הם כשל. `post_record_sha256` הוא golden לאימות
  output ורerun על baseline זהה. ברשומה שכבר יש בה ref, לא מפעילים override כלל:
  פותרים את ה־ref מול export ומעדכנים אינדקס בדרך הרגילה. לכן אחרי shift/rename
  או ב־migrate אין תלות ב־post hash הקפוא; נדרש רק שלא תישאר אף רשומה חסרת ref
  הזקוקה ל־override. אין lookup לפי ordinal; המספרים 47, 66, 67 ו־1948 הם
  diagnostics בלבד.

#### כתיב הגרשיים ב־`path_2`

שמות קבצי אוצריא מנוקים מגרשיים, ולכן `רשי על שבת.txt` אינו מתאים ל־heTitle
`רש"י על שבת` שבספריא. אין גשר, אין מפה ואין נורמליזציה: ההתאמה היא מחרוזתית
מדויקת מול ה־heTitle הראשי, בשני צדי הקישור.

739 רשומות דיקטא שנשאו את הכתיב המנוקה תוקנו **בנתונים** — `path_2` שלהן שוכתב
לכתיב של ספריא ב־`otzaria-library` — ולא בשכבת תרגום. הסיבה: קובצי דיקטא קפואים
ואין ייבוא חוזר שיחזיר את הכתיב השגוי, ומפת גישור הייתה יוצרת חוב חדש (שינוי שם
עברי בספריא לכותרת ממופה לא היה ניתן לתיקון באף מצב ריצה קיים).

לכן `applyHebrewRename` מטפל בכל הרשומות באותו מסלול: `path_2` ו־`heRef_2` נושאים
שניהם את כותרת ספריא, ושניהם משוכתבים יחד בגבול הכותרת. אי־התאמה ביניהם היא כשל
רועש (`heRef_2 has the old Hebrew title but path_2 does not`).

### 4.2 שדות חדשים ברשומת קישור

| שדה | מתי | משמעות |
|---|---|---|
| `ref_1` | מקור Sefaria | `RefEntry.ref` של המקור |
| `ref_2` | יעד Sefaria | `RefEntry.ref` של היעד |
| `anchor_src_hash` | `start` על מקור Sefaria | hash מדויק של מחרוזת שורת ה־BookPayload |

```json
{
  "line_index_1": 30,
  "ref_1": "Mishnah Berurah 1:1",
  "anchor_src_hash": "sha256:...",
  "heRef_2": "שער הציון, סימן א, סעיף א אות א",
  "path_2": "שער הציון.txt",
  "line_index_2": 2,
  "start": 94,
  "Conection Type": "commentary"
}
```

לא מוסיפים כעת `line_hash_X` או `ref_X_end`. רשומת טווח עתידית עם צד Sefaria תיכשל כ־`unsupported_sefaria_range` עד הרחבה נפרדת.

### 4.3 `manual_links_lineage.json`

הקובץ מחויב ל־git ונארז בשורש `otzaria_latest.zip`.

```json
{
  "schema_version": 1,
  "sefaria": {
    "tag": "...",
    "release_metadata_sha256": "...",
    "run_id": 0,
    "run_attempt": 0,
    "archive": {
      "sha256": "...",
      "size": 0,
      "parts": [
        { "name": "...tar.zst.part-00", "sha256": "...", "size": 0 }
      ]
    },
    "applied_changelog_chain": [
      {
        "tag": "...",
        "metadata_sha256": "...",
        "previous": { "tag": "...", "metadata_sha256": "..." },
        "changelog_name": "changelog_diff.json",
        "changelog_sha256": "..."
      }
    ]
  },
  "seforim_tool_commit": "40-hex-sha",
  "source_links_tree_sha256": "...",
  "packaged_links_tree_sha256": "...",
  "config_sha256": "..."
}
```

כללים:

- אין timestamp; rerun על אותם inputs חייב לייצר bytes זהים.
- ב־bootstrap `applied_changelog_chain` ריק; ב־refresh הוא מכיל בדיוק את הצמתים
  שהוחלו, בסדר מן הישן לחדש. הוא audit trail של הריצה, בעוד target metadata hash
  הוא העוגן להמשך השרשרת.
- שני tree hashes משתמשים ב־binary framing `manual-links-tree-v1`: לכל path
  כותבים length כ־unsigned big-endian, אחריו UTF-8 path bytes, ‏file size כ־u64
  ו־32 bytes של SHA-256 הקובץ; הרשומות ממוינות לפי UTF-8 path bytes. ב־source
  manifest קודמים לכל root path ו־state byte (`present`/`absent`). אין hashing
  של raw file bytes לתוך stream ואין delimiter עמום.
- `packaged_links_tree_sha256` מחושב אחרי מיפוי ל־`links/<filename>` בלבד. שני
  קבצים לאותו packaged path הם collision חוסם.
- שני ה־hashes כוללים גם `_headings.json` וקבצים אחרים שנארזים, אף שאינם מפוענחים כקישורים.
- `config_sha256` הוא hash גולמי של `manual_links_sync.json`.
- שינוי config מול lineage הוא `config_drift` חוסם. הוא מותר רק ב־config migration
  reviewed שמריץ corpus+bootstrap ומייצר lineage חדש במכוון.

### 4.4 דוח הרצה

`manual_links_refresh_report.json` מועלה כ־workflow artifact ואינו מחויב ל־git.

שדות חובה **לכל רשומה שנכנסת למסלול הידני**:

```text
mode; input/output lineage; tool commit
files scanned/changed/renamed
records scanned/relevant/unchanged/shifted/enriched
refs renamed/missing/duplicate
anchors checked/drifted
packaging collisions
כל failure עם file + record index + stable record hash
זמן לכל phase
status
```

ל־output תקף נדרש `status: "ok"` או `status: "no_op"`; כל ערך אחר או failure
מונע marker. בשני מצבי ההצלחה נכתבים report, lineage ו־marker.

---

## 5. Parser קשיח ו־diff נקי

### 5.1 קבצים

- מפענחים רק `*_links.json` מתוך `links_roots`.
- `_headings.json` אינו מפוענח כרשומות.
- כל root נארז כעץ שטוח: קובץ כלשהו בעומק גדול מ־1 מתחת ל־root הוא כשל.
  בפרט, `*_links.json` בתת־תיקייה אסור, משום שה־Generator קורא רק
  `Files.list(links/)` ולא יורד רקורסיבית.
- symlink, device, FIFO או entry שאינו regular file/directory הם כשל; אין follow
  symlink ואין קריאה מחוץ ל־checkout.
- לפני parsing מחשבים לכל קובץ את הנתיב הארוז `links/<filename>`; מאחר שעומק
  נוסף אסור, זהו גם relative path היחיד המותר. שני קבצים לאותו נתיב חוסמים;
  בקורפוס הנוכחי זה מתבטא ב־`טורי אבן_links.json` הכפול.

### 5.2 ולידציה

ה־parser יהיה Jackson `jackson-core` + `jackson-databind` עם
`StreamReadFeature.STRICT_DUPLICATE_DETECTION`; אין להסתמך כאן על parser שאינו
מזהה duplicate keys. שדות חובה לכל רשומה:

- `line_index_1`: מספר JSON שערכו שלם, ‏1..Int.MAX_VALUE.
- `heRef_2`: מחרוזת לא ריקה.
- `path_2`: מחרוזת לא ריקה.
- `line_index_2`: מספר JSON שערכו שלם, ‏1..Int.MAX_VALUE.

כללים:

- `1` וגם `1.0` תקינים; `1.5`, ‏0, שלילי או overflow נכשלים. הבדיקה נעשית
  ב־`BigDecimal` עם `stripTrailingZeros().scale() <= 0` ו־`intValueExact()` —
  לא בהמרת `Double`.
- duplicate JSON key — כשל עוד בשלב ה־streaming parse.
- unknown fields נשמרים ואינם נמחקים.
- `start`, אם קיים: מספר JSON שערכו שלם, ‏0..Int.MAX_VALUE; גם `0.0` תקין.
  ב־refresh ובכל output הוא מותר רק כאשר צד המקור Sefaria וקיים
  `anchor_src_hash`. ב־bootstrap/migrate input בלבד מותר hash חסר כמצב
  `pending_anchor_hash`, ורק אם אותה transaction מסווגת את המקור Sefaria,
  מזריקה ref+hash ומאמתת offset; pending כלשהו ב־output הוא כשל. לאחר פתרון
  דורשים `start <= rawContent.length` ביחידות UTF-16 של Kotlin.
- `end`, אם יתווסף בעתיד לרשומה שב־scope, הוא כשל
  `unsupported_sefaria_range`; אין לפרש אותו או לתקן אותו אוטומטית.
- סדר הרשומות והשדות הקיימים נשמר.
- `ref_X` מוכנס מיד אחרי `line_index_X`; ‏`anchor_src_hash` מיד אחרי `ref_1`.
- קובצי links נערכים ב־lossless span patcher: Jackson streaming מספק token
  locations על `StringReader`; מחליפים רק scalar value spans ומכניסים את שורות
  `ref_X`/`anchor_src_hash` לפי indentation וה־newline של הרשומה. כל byte מחוץ
  ל־edit spans נשאר זהה, כולל whitespace, field order וייצוג `1.0`.
- edits ממוינים מן ה־offset הגבוה לנמוך ונדרשים non-overlapping. strings חדשים
  מקודדים ב־Jackson JSON encoder; אין בניית escaping ידנית. אחרי patch מפענחים
  מחדש strict ומשווים לעץ הסמנטי הצפוי.
- קלט links נדרש UTF-8 ללא BOM ו־LF-only, עם 0 או 1 trailing LF; ה־patcher שומר
  בדיוק אם הסיומת הייתה קיימת. ב־baseline ‏400/401 קובצי JSON הם ללא trailing
  LF ורק `משנה ברורה_links.json` מכיל LF סופי. ‏CRLF או יותר מ־LF סופי אחד
  נכשלים במקום לבצע churn.
  lineage/report חדשים נכתבים canonical עם trailing LF אחד. rerun חייב להיות
  byte-identical.
- קובץ נכתב רק אם ערך סמנטי השתנה.

### 5.3 stable record hash

לדוח ולארבעת bootstrap overrides מחשבים SHA-256 על
`manual-links-canonical-v1` של הרשומה **לפני** שינוי:

1. object keys ממוינים לפי Unicode code point; arrays שומרים סדר;
2. אין whitespace; strings נכתבים ב־JSON escaping מינימלי ו־UTF-8;
3. מספר שערכו שלם נכתב כמספר עשרוני שלם (`961.0` → `961`); מספר אחר נכתב
   כ־`BigDecimal.stripTrailingZeros().toPlainString()`;
4. אין NaN/Infinity, leading plus או negative zero.

יש לממש canonicalizer קטן אחד ולנעול אותו ב־golden tests, כולל שמונת ה־pre/post
hashes שב־config. אין להשתמש ב־`jq -cS` (הוא משמר `.0` ולכן נותן hash אחר). ה־hash
מזהה fixture ומופיע בדוח; הוא לעולם אינו משמש לניחוש ref.

---

## 6. אינדקס Sefaria יעיל

### 6.1 אין DB ואין sidecar

אסור לכלי לקרוא `readBooksInParallel()` ולהחזיק את כל הקורפוס: ה־API הקיים
מחזיר `List<BookPayload>` מלא, כולל כל `lines`, ועלול לבזבז זיכרון. מממשים מסלול
סלקטיבי קטן שאינו משנה את מסלול ה־importer:

1. ב־refresh, לאמת ולפענח תחילה את כל שרשרת changelog ללא Reader. אחר כך לסרוק
   את קובצי הקישורים ולהפיק set כותרות מועמדות משמות המקור, מ־`path_2`, מ־config
   ומכל `old_he/new_he`; להחיל לוגית את שרשרת ה־renames עד הכותרת הסופית ולהכניס
   ל־set גם את הכותרות הסופיות. כך export שכבר מכיל רק `new_he` לא ידולג;
2. לעבור על `merged.json` ב־Jackson streaming header scan, לקרוא רק title/heTitle
   ולדלג על `text`; קובץ שכותרתו אינה מועמדת אינו נטען ל־DOM;
3. רק לספר מועמד לקרוא `BookPayload` מלא, להחיל את סינון ה־importer, להמיר מיד
   ל־`ManualBookIndex`, ולשחרר את ה־payload;
4. ב־index לשמור refs, כותרות, ורק את תוכן השורות שמופיעות ברשומת `start`.
   חריג bootstrap יחיד: עבור משנה ברורה שומרים זמנית את כל 18,119 השורות עד
   לסיום full-vector proof שב־§7.4 ואז משחררים אותן.

לצורך reuse נקי מוסיפים ל־`SefariaBookPayloadReader` entry point internal
`readSelectedBooks(..., candidatePrimaryHeTitles, consume)`; ה־entry point הקיים
וה־importer אינם משתנים. `consume` מקבל payload אחד ולא מוחזרת רשימת payloads.

על כל payload מועמד מחילים **בדיוק** את סינון הספרים של ה־importer:

```kotlin
val blacklists = loadSefariaBlacklists(classLoader, logger)
val accepted = filterBlacklistedPayloads(listOf(payload), blacklists).payloads.singleOrNull()
```

האינדקס נבנה רק מ־payloads שאושרו. אסור לבנותו מן הפלט הגולמי. לפני הסינון
ארבעה BookPayloads — `סידור אשכנז`, `יזכור`, `נוסח הכתובה`, `חכמת שלמה` —
מתנגשים עם source basenames מקומיים; אחרי הסינון אף אחד מהם אינו באינדקס.
`קובץ יסודות וחקירות` קיים כ־schema אך ללא `merged.json`, ולכן גם הוא אינו
BookPayload. בפרט `סידור אשכנז` חייב להישאר non-Sefaria וכל 2,289 יעדיו נספרים.

`black_versions.txt` אינו חלק מהסינון הזה: הוא מסנן מהדורות בלבד ואינו משנה את
נוכחות `BookPayload` או `RefEntry`. גם priority ordering אינו משנה זהות או refs.
קובצי `books_blacklist.txt` ו־`authors_blacklist.txt` מגיעים מ־tool commit הנעול;
לכן שינוי בהם מחייב tool commit חדש וריצת corpus מלאה.

התוצאה הנשמרת בזיכרון היא רק:

```text
bookEnTitle, bookHeTitle
RefEntry(ref, heRef, path, lineIndex)
content רק לשורות anchor נדרשות
```

אין סריקה פר־רשומה. סיבוכיות היעד:

```text
O(bytes של export שנסרקו + שורות הספרים הרלוונטיים + רשומות קישור)
```

workflow preflight: לפחות 12 GiB דיסק פנוי ו־6 GiB `MemAvailable`; הרצת הכלי עם
heap קשיח של 4 GiB ו־parallelism מוגבל. חוסר משאבים נכשל לפני extraction. בדיקת
benchmark מחייבת לרשום בדוח peak RSS, מספר merged files שנסרקו ומספר payloads
שנטענו; אין לקבל regression שטוען שוב את כל הקורפוס.

### 6.2 מפות פתרון

```text
exactPrimaryHeTitle -> List<BookPayload>
(expectedBookEnTitle, ref) -> List<RefCandidate>
(expectedBookEnTitle, heRef) -> List<RefCandidate>
```

0 או יותר מתוצאה אחת הם כשל. אין overwrite שקט.

### 6.3 expected book

- מקור: שם הקובץ בלבד לאחר הסרת suffix מדויק `_links.json`.
- יעד: helper טקסטואלי שמחליף `\\` ב־`/`, לוקח את הרכיב האחרון ומסיר suffix
  מדויק `.txt`; suffix חסר או רכיב ריק הם כשל. אסור להשתמש רק ב־`Path.fileName`,
  כי בקורפוס קיימים גם נתיבי Windows.
- התאמה exact ל־`BookPayload.heTitle` הראשי בלבד, אחרי blacklist filtering.
- לא משתמשים ב־`titleAliasKeys`: הם מנורמלים, וה־baseline כולו פתיר מול הכותרת
  הראשית. alias אוטומטי גם עלול לסווג ספר מקומי בטעות כ־Sefaria.
- אין נרמול גרשיים, punctuation או רווחים, ואין מפת גישור; אי־התאמות היסטוריות
  מתוקנות חד־פעמית בנתונים עצמם (ראו §4.1).
- אחרי `he_renamed`, משנים exact את filename/path ורק אז פותרים.

### 6.4 סיווג הצד הספריאתי

ב־bootstrap בלבד:

```text
sourceIsSefaria = exactPrimaryHeTitle(sourceTitle) has exactly one candidate
targetIsSefaria = exactPrimaryHeTitle(targetTitle) has exactly one candidate
```

- רשומה רגילה חייבת `sourceIsSefaria xor targetIsSefaria`; אחרת היא irrelevant
  למסלול או כשל סיווג, לפי הקובץ.
- אין מנגנון החרגה. רשומה both-Sefaria היא כשל רועש בכל קובץ ידני.

לאחר bootstrap נוכחות `ref_1`/`ref_2` היא מקור האמת לצד המנוהל: בדיוק אחד מהם
חייב להיות קיים בכל רשומה רלוונטית. בכל refresh עדיין מאמתים שה־ref side הוא
ספר Sefaria יחיד ושהצד האחר אינו Sefaria לפי הקורפוס המסונן; אין reclassification
שקט ואין הוספת ref אוטומטית לרשומה חדשה.

---

## 7. Bootstrap חד־פעמי

ה־bootstrap רץ על branch ייעודי, עם Sefaria asset ו־Seforim tool commit נעולים.

### 7.1 תיקוני שמות ו־collision

1. לשנות:

```text
National-LibraryToOtzaria/links/טורי אבן_links.json
→ National-LibraryToOtzaria/links/טורי אבן רוקח_links.json
```

אין לאחד אותו עם קובץ Wiki.

2. לתקן exact את שמות שני קובצי המקור לכותרת ה־DB המקומית:

```text
חידושי ופירושי מהרי''ק → חידושי ופירושי מהרי״ק
נר שמואל ח''א          → נר שמואל ח״א
```

יש ליישם את טבלת המצבים שב־§4.1 ולוודא את `local_book_path` ואת
`expected_db_title`. אין לפתור שלושת ספרי המקור האלה מול Sefaria — הם ספרי
Otzaria מקומיים.

3. collision scan חייב להסתיים ב־0 כפילויות.

### 7.2 National Library adapter

מופעל רק על `National-LibraryToOtzaria/links` ורק ליעד Sefaria.
אם `ref_2` כבר קיים, אין גזירה מחדש או overwrite: פותרים אותו ומאמתים שהוא
זהה לתוצאת ה־adapter ולאינדקס; התאמה היא `already_enriched`, אי־התאמה היא כשל.

```text
heRef_2 = "ספר X, הלכות Y, פרק Z, W"
path_2  = ".../משנה תורה, הלכות Y.txt"
```

```text
parts = splitExact(heRef_2, ", ")
require parts.size == 4
require parts[2] startsWith "פרק " and suffix non-empty
require parts[3] non-empty

targetTitle = targetTitleHelper(path_2) מ־§6.3
perek       = parts[2].removePrefix("פרק ")
ot          = parts[3]
builtHeRef  = targetTitle + " " + perek + ", " + ot
```

expected book ו־`builtHeRef` חייבים להיות יחידים. אז:

```text
ref_2        = RefEntry.ref
line_index_2 = RefEntry.lineIndex
```

Baseline: ‏63,107/63,107.

### 7.3 MoreBooks adapter

מופעל רק על `MoreBooks/links`, כאשר היעד Sefaria והמקור אינו Sefaria.
גם כאן ref קיים רק מאומת ואינו מוחלף.

#### Grammar A — רגיל

```text
builtHeRef = heRef_2 אחרי הסרת פסיק/רווח סופיים בלבד
```

אין שינוי פנימי נוסף. נדרש RefEntry יחיד בספר הצפוי.

Baseline מדויק: Grammar A פותר 2,286/2,290 רשומות MoreBooks הרלוונטיות,
exact ו־unique. ‏174 רשומות `דרך ה` נמחקו (§1.4); ‏86 מהן היו target-primary
והופיעו בספירת ה־2,376 הגולמית. המספר ההיסטורי 2,349 היה שגוי
ואסור להנציחו ב־test.

#### ארבעה חריגים מפורשים

אין Grammar B ואין schema-tree parser. ארבע הרשומות שלא נפתרות ב־Grammar A
מטופלות רק באמצעות `bootstrap_record_overrides` שב־§4.1:

- סידור ספרד: רשומה אחת → `Siddur Sefard, Weekday Maariv, The Shema,  24`,
  ‏`line_index_2=1145`;
- אבודרהם: שתי רשומות →
  `Abudarham, Weekday Prayers, Blessings on the Shema,  3`, ‏`line_index_2=191`;
- קדושת לוי: רשומה אחת →
  `Kedushat Levi, Leviticus, Homily for Pesach,  6`, ‏`line_index_2=715`.

כל ref ואינדקס נבדקים מחדש מול ה־export; ה־override אינו עוקף uniqueness.
לאחר ארבעתם: 2,290/2,290 ו־0 unresolved.

### 7.4 Tashma / משנה ברורה

```text
snapshot: tashmaToOtzaria/סקריפטים/ביאור הלכה/otzaria_mb.txt
SHA-256: 0b67db43e6f2dedc2aa63fd670368b1ab23e9995d4c6458044e87b43d2c772e6
lines: 18,119
encoding/newlines: UTF-8, LF-only, trailing LF אחד
```

הוכחת המיפוי נעשית על **כל וקטור השורות**:

1. לדרוש 18,119 שורות בשני הצדדים.
2. לכל position מותר רק:
   - identity;
   - הסרת marker מוביל אחד לפי כללי commit
     `60dc3056c4d5f95bdf5372ec3cd5e76013eb1e74` ב־SeforimLibrary.
3. בדיוק 732 positions הם identity ו־17,381 הם strip-one-leading-marker.
4. שש החריגות הבאות הן fixtures מחייבים. `position` הוא מספר השורה הפיזית
   1-based בשני הווקטורים; כל SHA הוא על UTF-8 של תוכן השורה בלבד, ללא LF:

| position | heRef | old SHA-256 | new SHA-256 | שינוי נוסף |
|---:|---|---|---|---|
| 4717 | `משנה ברורה,  קפח, ט` | `cb563f858d2275238b7d8da58bfb61ae18a44ca939dee2265e59cb58eb3b3183` | `6f1cc2f903f4cb48c4da408028bba9bf0fff6d7f8654662a854ffc38e3419e86` | `דתופס`→`דטופס`, `מתופס`→`מטופס` |
| 6364 | `משנה ברורה,  רנז, יג` | `bf846cd97d29d82d617da2bce572e57c04ca87660234de6b07955b28298f1c5a` | `49e66d24cffc5346896c3614698e0abfeca689b31b47f958cdf1b89cd38256c2` | הוספת `[סק"ח]` לפני `:` |
| 6400 | `משנה ברורה,  רנז, מט` | `dd3494ce9136748f777fba0006acadfe461eb320ec6bd03b48f2b7b0427dbe41` | `b5aa7bbaa64553260bb5e34e5a407d3564671964ad1069b5395ed977d0420600` | רווח ASCII יחיד בסוף אחרי `:` |
| 11277 | `משנה ברורה,  שצב, ח` | `bfd9aaac809853bd6811c61abcecdb4144a25f00d095681f786db4829c5cee63` | `8f7d9f4b3986fa8e281bff1c0f7c371b53c3aeb4c5a43b1f228f3354fbc58a43` | `ואם היו`→`אם היו` |
| 15130 | `משנה ברורה,  תקמז, ו` | `5c4808b7b5aa061b60dae188874a13d646c3ef40236aaad09c4ea0a12392b75a` | `205f9cb7deff294507d1107cfb5d5ce33278649652e9177d29470b1b76d289b9` | `דעכ"פ דעכ"פ`→`דעכ"פ` |
| 15162 | `משנה ברורה,  תקמח, יא` | `bffce9c82d51455f8f75a64705749f8fd55e814e040e7a3ba2d82cd091160809` | `0ae824929f27fb60bc1054b58fb220a2eef064c71a0737596294a6afacc72f69` | `משא"כ משא"כ`→`משא"כ` |

5. בכל gap בין exact anchors נדרש אותו מספר שורות ואותו סדר; אין SequenceMatcher ואין LCS עמום.
6. שתי חריגות מקושרות נבדקות במיוחד:
   - position ‏4717: replacements באותו אורך לפני `start=62/123`.
   - position ‏6400: תוספת רווח אחרי `start=573`.
7. רק אחרי full-vector proof, `line_index_1` ממופה ל־RefEntry באותו position.

לכל 17,980 הרשומות:

- `ref_1 = RefEntry.ref`.
- `line_index_1 = RefEntry.lineIndex`.
- `anchor_src_hash = "sha256:" + lowercaseHex(SHA-256(UTF-8(BookPayload.lines[RefEntry.lineIndex - 1])))`.
  אין newline, trim, Unicode normalization או HTML normalization נוסף.
- `0 <= start <= BookPayload.lines[RefEntry.lineIndex - 1].length`, כאשר `length`
  הוא מספר יחידות UTF-16 של Kotlin — אותה יחידה שבה משתמש ה־Generator.
- אין surrogate pair לפני `start`; אחרת Python code-point מול Kotlin UTF-16 אינו מוכח.

ברשומה שכבר נושאת `ref_1` ו־`anchor_src_hash`, bootstrap מאמת את שניהם ואת
האינדקס ואינו כותב מחדש; mismatch חוסם. לכן rerun מלא של bootstrap הוא no-op.

אסור להפעיל `build_viewer.py` או `SequenceMatcher` כחלק מה־bootstrap.

### 7.5 מאזן bootstrap

```text
scanned_records
  = relevant_records + irrelevant_records + classification_failures

relevant_records
  = unchanged_and_enriched + shifted_and_corrected + relevant_failures
```

ליצירת commit נדרש שכל קבוצות ה־failure יהיו 0.

### 7.6 delta צפוי במיגרציה

אין לצפות ל־"22 בלבד". יש לתעד לפחות:

- `טורי אבן`: 163 שגויים נמחקים, 175 נכונים מתווספים, 165 Wiki משוחזרים.
- מהרי״ק + נר שמואל: 176 links מתווספים.
- יעדי Sefaria: 23 links שגויים מוחלפים ו־2 אבודרהם מתווספים.
- `דרך ה`: אפס שינוי DB — הקובץ יתום ומעולם לא תרם קישור (§1.4).

את הסט המדויק מחשבים מהקבצים; אין hard-code של link IDs.

---

## 8. Refresh אחרי כל Sefaria release

### 8.1 קלטים

```text
otzaria checkout SHA
config + lineage
target Sefaria tag + asset SHA-256 + export
כל changelog_diff.json מאז lineage.tag
Seforim tool commit
output directory ריק
```

### 8.2 שרשרת releases

אין מיון לפי `createdAt`. מתחילים ב־target metadata SHA שקיבל ה־dispatch והולכים
אחורה אך ורק דרך `previous.tag + previous.metadata_sha256`:

1. לכל צומת מורידים metadata בשם tag מדויק ומאמתים את digest הצפוי.
2. מאמתים שה־changelog שלו אומר
   `old_tag == previous.tag` ו־`new_tag == tag`, ושה־asset hash תואם metadata.
3. ממשיכים עד `lineage.sefaria.tag + release_metadata_sha256`.
4. צומת חסר, duplicate tag, digest שונה, fork, cycle או הגעה ל־null לפני lineage
   הם כשל.
5. אם target אינו descendant, הולכים גם אחורה מן lineage: אם target נמצא שם הוא
   `stale_dispatch`; אחרת זה fork חוסם.

כל metadata/changelog מן bootstrap ואילך נשמר לצמיתות. release עוקב יוצר שרשרת
באורך 1; releases שהוחמצו יוצרים שרשרת ארוכה באותו אלגוריתם.

Reader רץ רק על export הסופי; changelogs ביניים מרכיבים rename chain.

`status=no_op` מותר רק כאשר כל tuple הקלט זהה: target tag+metadata/archive
digests, ‏config SHA, ‏source tree SHA וה־tool commit שכבר נעול ב־lineage. שינוי
config דורש migration. tool commit חדש נבחר רק עבור Sefaria target חדש וממילא
עובר corpus מלא; החלפת tool commit עבור אותו target דורשת migration מפורש ואינה
נבחרת בשקט מ־branch HEAD. גם ב־no-op הכלי כותב report, lineage זהה ו־marker תקף;
ה־workflow מדלג על commit אך ממשיך להשלים release/build חסרים.

### 8.3 Renames

#### `en_renamed`

- ref ניתן לשכתוב רק אם `ref == old_en`, מתחיל ב־`old_en + U+0020` או מתחיל
  ב־`old_en + ", "`; ה־suffix נשמר byte-for-byte. האפשרות האחרונה נדרשת ל־
  complex refs כגון `Abudarham, Weekday ...`. כל boundary אחר נכשל.
- אם האירוע כולל `old_he != new_he`, לבצע באותה transaction גם את כל פעולות
  ה־Hebrew rename המפורטות להלן. `old_he/new_he` חסרים באירוע שנוגע לרשומה הם
  כשל, לא fallback.
- להחיל לפי סדר השרשרת מן הישן לחדש.
- cycle או conflict → כשל.

#### `he_renamed`

- יעד Sefaria: להחליף רק את הרכיב האחרון המדויק של `path_2` מ־`old_he.txt`
  ל־`new_he.txt`, תוך שמירת prefix וסוג separator (`/` או `\\`) byte-for-byte.
- מקור Sefaria: rename של `<old_he>_links.json` רק אם היעד אינו קיים.
- `heRef_2` משתנה רק אם הוא שווה `old_he` או שהתו הבא הוא U+0020 או שה־suffix
  מתחיל `", "`; ה־suffix נשמר בדיוק.
- אחרי השינוי expected book חייב להיות יחיד.
- filename/path/heRef/ref של אירוע משולב נכתבים כולם או אף אחד.

### 8.4 רשומות קיימות

לכל `ref_X`:

1. לזהות expected book.
2. לפתור `(expectedBook, ref_X)` ל־RefEntry יחיד.
3. לעדכן `line_index_X = RefEntry.lineIndex`.
4. אינדקס זהה → אין שינוי קובץ.
5. עם `anchor_src_hash`:
   - hash זהה → `start` נשמר;
   - hash שונה → `anchor_content_drift`, כשל; אין עדכון hash או offset.

### 8.5 רשומות חדשות

- adapters ו־overrides קיימים ב־bootstrap בלבד. יעד Sefaria חדש ללא `ref_2`
  נכשל `new_target_ref_required`; מקור Sefaria חדש ללא `ref_1` נכשל
  `new_source_ref_required`. יוצר הרשומה חייב לספק ref מפורש.
- שני צדדים Sefaria: חייבים להיות מחוץ לקבצי הידניים.

### 8.6 כתיבה אטומית

1. להעתיק את roots במצב `present` ל־output זמני ולתעד roots במצב `absent`.
2. לשנות רק את העותק.
3. parser round-trip לכל `*_links.json`.
4. collision scan.
5. מאזן סגור + lineage חדש שנכתב **על ידי הכלי**.
6. report.
7. רק בסוף `.manual-links-refresh-complete` עם hashes של report ו־lineage.

ה־workflow אינו יוצר או עורך lineage בעצמו ואינו מעתיק output בלי marker תקף.
ב־`status=ok` הוא מחיל כל root באמצעות `rsync --archive --delete`; ב־
`status=no_op` הוא אינו מחיל roots. בשני המצבים הוא ממשיך ל־reconciliation של
שלבי ה־release וה־build, כדי שכשל אחרי commit לא ישאיר saga חלקית.

---

## 9. CLI מחייב

Bootstrap:

```bash
./gradlew :sefariasqlite:refreshManualLinks \
  -PmanualLinksMode=bootstrap \
  -PmanualLinksRepo=/work/otzaria-library \
  -PmanualLinksConfig=/work/otzaria-library/manual_links_sync.json \
  -PsefariaExport=/work/sefaria/export \
  -PsefariaReleaseMetadata=/work/sefaria/release_metadata.json \
  -PsefariaReleaseMetadataSha256=SHA256 \
  -PseforimToolCommit=COMMIT \
  -PmanualLinksOutput=/tmp/manual-links-output
```

Refresh:

```bash
./gradlew :sefariasqlite:refreshManualLinks \
  -PmanualLinksMode=refresh \
  -PmanualLinksRepo=/work/otzaria-library \
  -PmanualLinksConfig=/work/otzaria-library/manual_links_sync.json \
  -PmanualLinksLineage=/work/otzaria-library/manual_links_lineage.json \
  -PsefariaExport=/work/sefaria/export \
  -PsefariaReleaseMetadata=/work/sefaria/release_metadata.json \
  -PsefariaReleaseMetadataSha256=SHA256 \
  -PsefariaChangelogDir=/work/changelogs \
  -PseforimToolCommit=COMMIT \
  -PmanualLinksOutput=/tmp/manual-links-output
```

Config/tool migration ידני:

```bash
./gradlew :sefariasqlite:refreshManualLinks \
  -PmanualLinksMode=migrate \
  -PmanualLinksRepo=/work/otzaria-library \
  -PmanualLinksConfig=/work/otzaria-library/manual_links_sync.json \
  -PmanualLinksLineage=/work/otzaria-library/manual_links_lineage.json \
  -PexpectedOldConfigSha256=OLD_CONFIG_SHA \
  -PexpectedOldToolCommit=OLD_TOOL_COMMIT \
  -PsefariaExport=/work/sefaria/export \
  -PsefariaReleaseMetadata=/work/sefaria/release_metadata.json \
  -PsefariaReleaseMetadataSha256=SHA256 \
  -PseforimToolCommit=NEW_COMMIT \
  -PmanualLinksOutput=/tmp/manual-links-output
```

`migrate` הוא bootstrap idempotent על הקורפוס הקיים: הוא מתיר רק את שינויי
config/tool שה־old hashes נועלים, מריץ corpus מלא ומייצר lineage חדש. הוא זמין
רק ב־workflow ידני עם approval ואינו מופעל על ידי Sefaria dispatch.

כל properties המוצגים חובה למצבם. אין default ל־`latest`.

---

## 10. Workflow אוטומטי

### 10.1 PAT והרשאות

Fine-grained PAT בשם `PIPELINE_TOKEN`:

- `otzaria-library`: Actions read/write; Contents read/write.
- `SefariaExport`: Actions/Contents read.
- `SeforimLibrary`: Actions read/write; Contents read.

PAT משמש ל־cross-repo dispatch/watch ול־push רק אם branch policy מחייבת. אין force push ואין הדפסת token.
בכל checkout לקריאה: `persist-credentials:false`; לכל workflow permissions
מינימליות מפורשות. ה־PAT אינו נשמר ב־git config ומוזן רק לפקודת `gh`/push
הזקוקה לו.
Prerequisite מחייב: branch protection של `otzaria-library/main` מאפשר ל־
`PIPELINE_TOKEN` של הבוט push ישיר של ה־commit המצומצם בלבד. אם policy אינו
מאפשר זאת, ההטמעה חסומה עד אישור ההרשאה; גרסה זו אינה מממשת מסלול PR+merge.
ה־workflow מבצע preflight לפני הורדת export גדול; אין לעקוף policy ואין force.

### 10.2 `sync-manual-links.yml`

Inputs:

```text
sefaria_tag
sefaria_release_metadata_sha256
sefaria_run_id
sefaria_run_attempt
correlation_id
```

`correlation_id` נבנה פעם אחת ב־SefariaExport:
`sefaria:<run_id>:<run_attempt>:<tag>:<release_metadata_sha256>` ונבדק ב־regex
קשיח. כל child משתמש ב־run-name מסוג
`<workflow-name> correlation=<correlation_id>`; ההשוואה היא equality מלאה.

```yaml
run-name: sync-manual-links correlation=${{ inputs.correlation_id }}
concurrency:
  group: manual-links-sync
  cancel-in-progress: false
```

זה מסדר Sefaria targets זה אחר זה. בנוסף, job ההחלה+push ו־job
`prepare_only` חולקים group repository-wide בשם `otzaria-main-writer`; job
האריזה חולק group `otzaria-release-publisher`. הקבוצות הן ברמת job, לא workflow,
כדי שה־sync לא יחזיק lock בזמן שהוא dispatches וממתין ל־publisher child.

שלבים:

1. checkout טרי של `otzaria-library/main`.
2. בריצה חדשה לפתור פעם אחת את `seforim_tool_ref` המורשה ל־SHA מלא; ב־recovery
   להשתמש ב־SHA מן lineage. checkout נוסף של SeforimLibrary באותו SHA, ולדרוש
   `git rev-parse HEAD == seforim_tool_commit` ושה־commit reachable מן ה־ref.
3. הורדת `release_metadata.json` לפי tag ואימות ה־SHA-256 שקיבל ה־dispatch.
4. הורדת archive asset/parts בדיוק לפי metadata; אימות כל part, חיבור אם צריך, אימות `archive_sha256`, ורק אז extraction.
5. הורדת changelogs החסרים ואימות `changelog_sha256` לכל release metadata בשרשרת.
6. הרצת הכלי ל־temp output.
7. אימות marker, report, hashes ומאזן.
8. אם `status=ok`, החלת כל root ב־`rsync --archive --delete` והעתקת lineage
   שהכלי יצר; אם `status=no_op`, לא להעתיק roots או lineage.
9. לחשב מחדש אחרי ההחלה את source tree/config/lineage hashes ולדרוש התאמה.
10. לבדוק `git status --porcelain=v1 -z` מול allowlist קשיח של roots+lineage
    (וב־migration גם config), לבצע `git add -- <allowlist בלבד>`, ואז לבדוק גם
    `git diff --cached --name-only`. בדיקה זו כוללת untracked files.
11. אם יש staged diff, commit:

```text
chore(manual-links): sync Sefaria <tag>
```

12. push רגיל. אם אין diff, `expected_links_commit` הוא SHA ה־checkout המאומת.
13. push conflict: לזרוק output ולהתחיל מחדש מ־origin/main, עד 3 פעמים; אין rebase של output ישן ואין force.
14. no-op של updater מונע empty commit בלבד; הוא **אינו** מסיים את ה־workflow.
15. לשמור `expected_links_commit` ולהמשיך ל־ensure-release ול־ensure-build.

### 10.3 Otzaria release

אחרי push:

1. להפעיל `update-library.yml` ב־`links_sync_mode` עם
   `expected_links_commit` ו־`correlation_id`.
2. ה־workflow חייב:
   - job האריזה משתמש ב־`otzaria-release-publisher`, ‏`cancel-in-progress:false`;
   - checkout detached **בדיוק** ל־`expected_links_commit`, לוודא שהוא reachable
     מ־main, ולדלג במצב זה על forum, ‏sync-and-merge, commit ו־push;
   - לקרוא את כל רשימות קישורי ה־manifest וה־ZIP רק מן ה־config. להסיר את שתי
     הרשימות הקשיחות של **links** ב־`update-library.yml`; לעדכן גם את חלקי
     ה־links ב־`check_duplicates.py` וב־`create_release_archives.sh` לקרוא אותו
     config. רשימות ובדיקות book roots הקיימות נשארות ללא שינוי ואינן בתחום
     `manual_links_sync.json`;
   - להכשיל collision;
   - להוסיף config ו־lineage ל־zip;
   - ליצור staging directory חדש, להעתיק כל קובץ root ל־`links/<filename>` עם
     create-new בלבד, ולעולם לא להוסיף שוב לאותו ZIP בשיטת last-wins;
   - לחשב מחדש source ו־packaged tree hashes ולדרוש התאמה ל־lineage;
   - ליצור ZIP reproducible: paths ממוינים, timestamp/mode קבועים, compression
     level וגרסת runtime נעולים; rerun מאותו commit חייב אותו SHA;
   - להוסיף ל־release `otzaria_release_provenance.json` קנוני ובו correlation,
     target commit, asset digest וכל lineage/config/tree hashes;
   - ליצור release immutable ללא `--clobber`, ללא מחיקת release קודם, ורק עם
     tag שמכוון במפורש (`--target`) ל־`expected_links_commit`. נוסחת tag:
     `library-links-<child_run_id>-<child_run_attempt>-<expected_commit_12>`;
     ב־recovery reuse של release קיים שומרים את ה־tag המקורי.
   אותם config/collision/hash gates חלים על כל packaging path; בפועל נשאר
   publisher אחד בלבד.

ל־`update-library.yml` יהיו שני modes מפורשים:

- `prepare_only`: sync sources + manifests + commit/push, ללא package/release;
- `links_sync_mode`: package detached exact בלבד, ללא mutation.

ה־`regular` הישן (sync+package באותה ריצה) מוסר, משום ששינוי link root לפני
refresh היה הופך את lineage לישן או חוסם אריזה. הפעלה ידנית מלאה עוברת דרך
`weekly-pipeline.yml` ב־`workflow_dispatch`, שמריץ `prepare_only` → SefariaExport
→ sync links → package. כל caller קיים מתועד ומועבר ל־entry point הזה. שני
`prepare_only` משתמש ב־`otzaria-main-writer`; ‏`links_sync_mode` משתמש ב־
`otzaria-release-publisher`. אין מסלול publisher שני.
3. child מעלה `pipeline-result.json` קנוני ו־`pipeline-result.sha256`; ה־JSON כולל correlation, child run ID,
   expected/target commit, tag, asset name/size/SHA, lineage/config/tree hashes.
4. לפני dispatch ההורה רושם UTC dispatch time. הוא מאתר בדיוק ריצה אחת לפי
   workflow, ‏event=`workflow_dispatch`, ‏displayTitle ששווה בדיוק ל־run-name
   הכולל correlation, ‏headSha ו־createdAt שאינו מוקדם מן dispatch. אין `latest`
   ואין `contains`. הוא ממתין, מוריד result מן run ID המדויק ומאמת אותו מול
   release API וה־tag target.
5. recovery מחפש תחילה release provenance שלם עם אותו lineage hash ו־target
   commit; אם נמצא — משתמש בו. draft/asset חלקי הוא כשל מפורש. כך כשל אחרי
   release אינו יוצר release נוסף.

מפתח ה־idempotency הוא tuple ה־input digests וה־target commit, לא correlation;
correlation הוא audit/correlation בלבד ולכן retry חדש רשאי reuse release ישן
שכל bytes/provenance שלו תואמים.
אם נדרש חיסכון באחסון, garbage collection יהיה workflow נפרד ורק אחרי build
מוצלח, עם retention policy ששומר את ה־latest, כל release שמוזכר ב־provenance
וכל release של ריצה פעילה. publisher עצמו לעולם אינו מוחק predecessor.

### 10.4 Seforim build

להפעיל `manual-generate-release.yml` עם:

```text
source_commit
sefaria_tag
sefaria_release_metadata_sha256
sefaria_archive_sha256
otzaria_tag
otzaria_asset_sha256
expected_links_commit
otzaria_target_commit
correlation_id
```

ה־workflow:

1. checkout ל־`source_commit`.
2. מוריד assets לפי tags ומאמת hashes.
3. מחלץ לתיקיות run-scoped נקיות. אין pre-stage ב־cache ואין `/releases/latest`.
4. דורש `source_commit == lineage.seforim_tool_commit`, שה־Otzaria tag נפתר
   ל־`otzaria_target_commit == expected_links_commit`, ושכל metadata/archive
   digest תואם lineage.
5. מחשב מחדש `config_sha256` ו־`packaged_links_tree_sha256` מתוך ה־ZIP. אין
   ניסיון לחשב source hash שאינו קיים בארכיון.
6. כל gates מסתיימים **לפני** `generateSeforimDb`.
7. מפעיל במפורש:

```bash
./gradlew generateSeforimDb \
  -PexportDir="$SEFARIA_EXTRACT_ROOT" \
  -PsourceDir="$OTZARIA_EXTRACT_ROOT" \
  -PinMemoryDb=false ...
```

`SEFARIA_EXTRACT_ROOT` הוא הנתיב שמכיל `database_export`; ‏Otzaria root מכיל
`אוצריא`, ‏`links`, config ו־lineage. חיווט `sourceDir` ל־Havrouta שב־§3.1
מבטיח שכל צרכני Otzaria משתמשים באותו input.

8. release כולל `build_provenance.json` עם כל inputs/hashes/correlation. הוא
   נבנה כ־draft ומתפרסם אחרון. retry מאתר provenance שלם ומחזיר אותו; הוא אינו
   מגדיל `db_version` או יוצר release נוסף לאחר כשל post-publish.
9. גם child זה משתמש באותו חוזה correlation/result artifact בדיוק כמו §10.3;
   sync ממתין לתוצאה כדי שיוכל להשלים recovery מקצה לקצה.

### 10.5 ביטול ה־race

`weekly-pipeline.yml` אינו מפעיל עוד SefariaExport ו־`update-library` במקביל.

המסלול המומלץ:

1. weekly pipeline מפעיל `update-library.yml` ב־`prepare_only` וממתין להצלחה;
   כך כל מקורות Otzaria ממשיכים להתעדכן אך טרם נוצר release.
2. רק אחר כך הוא מפעיל SefariaExport וממתין ל־release/dispatch.
3. כל Sefaria release, גם ידני, dispatches את `sync-manual-links`.
4. ה־sync workflow יוצר commit קישורים, מפעיל `links_sync_mode`, ורק אחרי
   Otzaria release מפעיל Seforim build.
5. weekly orchestrator אינו מפעיל publisher נוסף במקביל ואינו בוחר inputs או
   ממתין לבניית ה־DB הארוכה.

---

## 11. בדיקות

### 11.1 Unit

1. National grammar וכל malformed branch.
2. MoreBooks Grammar A וארבעת override hashes; override חסר/עודף/כפול נכשל.
3. ספר/ref חסר או כפול.
4. duplicate JSON keys.
5. integer validation: ‏1, ‏1.0, ‏1.5, ‏0, שלילי, overflow.
6. exact rename boundaries: end, space ו־comma+space של complex ref; כל boundary
   אחר נכשל.
7. rename chain, cycle ו־conflict.
8. raw content hash, HTML וניקוד.
9. surrogate לפני `start`.
10. deterministic tree hash.
11. unknown-field/order preservation.
12. marker נכתב רק אחרי הצלחה.
13. selective Reader אינו טוען payload שאינו מועמד; books/authors blacklist זהה
    ל־importer וחמש התנגשויות השמות אינן מסווגות Sefaria.
14. Windows/Unix `path_2` basename וקובץ link בתת־תיקייה.
15. אותו golden canonical JSON מפיק אותו SHA ב־Kotlin וב־Python.
16. `pending_anchor_hash` מותר רק ב־bootstrap/migrate input ומועשר; refresh או
    output ללא hash נכשל.
17. lossless patch משנה רק spans צפויים גם בקובצי indentation שונים וב־baseline
    ללא final newline; מצב 0/1 final LF נשמר בדיוק.

### 11.2 Tashma regression

- snapshot SHA ו־18,119 שורות.
- full-vector proof ושש exception hashes.
- 10,528 source positions יחידים.
- 17,980 refs, hashes ו־offsets תקפים.
- אפס SequenceMatcher.

### 11.3 Integration

1. insertion לפני ref → index מתעדכן.
2. ref נעלם/כפול → אין marker ואין שינוי input.
3. anchor content משתנה → כשל, hash אינו מתעדכן.
4. `en_renamed` ו־`he_renamed`, כולל export שמכיל רק `new_he` וה־selective
   candidate scan עדיין טוען אותו.
5. record חדש ללא ref בצד Sefaria נכשל בשני הכיוונים; אין adapter ב־refresh.
6. both-Sefaria נכשל רועש.
6א. יעד שכותרתו כוללת גרשיים נפתר בהתאמה ישירה, וכותרת שנושאים אותה שני ספרי
   ספריא נכשלת רועש; ‏`dicta_heref_v1` נאכף מחדש על רשומה קיימת בעלת `ref_2`
   דרך `processExistingTarget` ב־migrate.
7. packaged-path collision.
8. rerun byte-identical.
9. changelog chain חסרה.
10. `en_renamed` שמשנה גם English וגם Hebrew מתבצע אטומית.
11. status=no_op יוצר marker אך אינו יוצר commit, וה־saga ממשיכה.
12. bootstrap → shift refresh → migrate אינו תלוי ב־post hash היסטורי.

### 11.4 Corpus

```text
81,496 target-Sefaria relevant
17,980 source-Sefaria relevant
17,980 anchors checked
0 unresolved
0 duplicate refs touched
0 anchor drift
0 collisions after rename
```

שינוי baseline דורש review והסבר; אין auto-accept. המספרים למעלה נמדדו מחדש
לפי §1.3 ולא נגזרו בדלתא מהקבועים הקודמים.

Corpus suite רץ ב־workflow ייעודי עם Otzaria commit, Sefaria metadata/export
ו־tool commit נעולים. הוא אינו חלק מ־`:sefariasqlite:jvmTest` הרגיל.
ה־workflow תומך בשלושת המצבים `refresh`, `bootstrap` ו־`migrate`; ‏`migrate`
דורש בנוסף `expected_old_config_sha256` ו־`expected_old_tool_commit` מתוך
ה־lineage הנעוץ, ובשאר המצבים שני הקלטים האלה חייבים להישאר ריקים.

### 11.5 Workflow fault/recovery

1. שני Sefaria releases מקבילים נעמדים בתור ואינם יוצרים fork.
2. split archive, part חסר ו־tag קיים עם bytes שונים.
3. dispatch כפול/לא קשור ו־exactly-one correlation.
4. כשל אחרי commit, אחרי Otzaria publish, אחרי Seforim publish ואחרי post-step.
5. config drift, tool migration, stale target ו־forked changelog chain.
6. Otzaria tag שאינו מצביע ל־expected commit.
7. לחסום network access ל־`/releases/latest`; build תקין עדיין חייב לעבור.
8. בכל fault, rerun מתכנס לאותו provenance בלי release או `db_version` נוסף.
9. `prepare_only` עדיין מעדכן את שאר מקורות Otzaria אך אינו מפרסם; שינוי link
   root בו עובר refresh לפני publisher היחיד. אין עוד מסלול `regular` שעוקף זאת.

### 11.6 E2E DB + delta

1. DB לפני migration מ־inputs ישנים נעולים.
2. DB אחרי migration מ־inputs חדשים נעולים.
3. לחשב מהקבצים את סט `(sourceLineId, targetLineId, connectionTypeId)` הצפוי.
4. להשוות ל־DB כולל anchors.
5. לאמת בנפרד את טורי אבן, שני תיקוני השמות ו־25 תיקוני היעד.
6. להריץ `producePatchAndVerify` ללא שינוי.
7. להחיל patch ולבדוק התאמה ל־DB החדש.

---

## 12. השפעה על delta

לא משתנים:

- line/link ID allocation;
- allocator state;
- DB schema;
- PatchTables, patch fan ו־verifier.

בצד Sefaria המנוהל, שורה בעלת ref/heRef יציב משתמשת ב־heRef במפתח הטבעי ולכן
shift פוזיציונלי לבדו משאיר lineId/linkId יציבים; האינדקס הנגזר בלבד משתנה.
אין כאן טענה על שורות non-Sefaria חסרות heRef, שאינן מטופלות בפרויקט. E2E delta
עדיין מודד את התוצאה בפועל ואינו מסתפק בהנחה.

`he_renamed` עדיין מצ'רן IDs משום שמפתח lineId הנוכחי מבוסס heRef עברי. זהו מצב קיים; הפרויקט אינו מחמיר אותו.

המיגרציה החד־פעמית תיצור delta גדול בגלל שחזור ומחיקת קישורים שגויים. זה צפוי ונבדק; אין להסתירו כ־"22 בלבד".

---

## 13. סדר ביצוע לג'וניור

### A — חוזים ו־fixtures

1. להוסיף unit fixtures קטנים ל־National, MoreBooks, canonical JSON ול־Tashma.
2. להוסיף config עם כל expected states, שלושת renames וארבעת overrides.
3. להקפיא golden hashes וטבלאות החריגים.
4. אין workflow ואין שינוי links.

יציאה: `jvmTest` עובר ללא sibling repo, export גדול או snapshot מלא. בדיקות 99,476
הרשומות שייכות ל־corpus suite נפרד, לא ל־unit suite.

### B — כלי read-only

1. להוסיף version-catalog aliases נעולים ל־Jackson, dependencies ל־jvmMain,
   ו־parser קשיח שה־`JsonFactory` שלו מפעיל בפועל
   `StreamReadFeature.STRICT_DUPLICATE_DETECTION`.
2. selective Reader + blacklist parity + primary-title index.
3. bootstrap adapters וארבעת overrides בלבד.
4. refresh, lineage, report ו־marker.
5. output בלבד, ללא שינוי repo.

יציאה:

```bash
./gradlew :sefariasqlite:jvmTest
./gradlew :sefariasqlite:refreshManualLinks ...
```

עוברים עם 0 failures.

### C — תשתית Sefaria metadata, dispatch כבוי

1. להוסיף concurrency, tag ייחודי, draft→verify→publish ו־metadata contract.
2. לבטל `--clobber`; להפוך manifest/titles/changelog לחובה.
3. לפרסם release ראשון תקף עם `release_metadata.json` ולשמור את כל assets.
4. עדיין לא להפעיל cross-repo dispatch.

יציאה: downloader עצמאי מאמת metadata, כל parts וה־combined hash; rerun עם bytes
שונים נכשל ואינו משנה release.

### D — bootstrap branch + corpus

1. branch ב־`otzaria-library`.
2. bootstrap מול release metadata משלב C ו־tool commit נעול.
3. להחיל output ולבדוק diff לפי source.
4. rerun bootstrap חייב לזהות את שלושת renames כ־already-applied; refresh על
   output חייב להיות no-op byte-identical.
5. corpus workflow מוריד checkout/export נעולים ומריץ את כל 99,476 הרשומות,
   ה־snapshot המלא וה־blacklist regression.

יציאה: 100% כיסוי, 0 unresolved/collision.

### E — Generator compatibility ו־E2E

1. לא לשנות resolver/runtime של Generator; לחווט `sourceDir` ל־Havrouta.
2. לבנות DB עם JSON מועשר.
3. E2E + delta.

יציאה: DB נכון ו־patch verification עובר.

### F — workflow dry-run

1. workflow ידני שמוריד, מאמת, מרענן ומעלה artifact.
2. ללא commit/release.
3. לבדוק rerun, stale, missing changelog ו־concurrency.

יציאה: output זהה להרצה מקומית.

### G — commit + Otzaria release

1. להפעיל commit/push.
2. expected commit ו־lineage checks באריזה.
3. release ניסויי; להוריד ולאמת מחדש.

יציאה: release מכיל בדיוק את commit הקישורים וה־lineage.

### H — Seforim build pinned

1. inputs נעולים.
2. extract לתיקיות נקיות והעברת `-PexportDir/-PsourceDir`.
3. lineage/config/tree gates לפני `generateSeforimDb`.
4. build + delta מלא.

יציאה: זוג נכון עובר; tag/hash שגוי נכשל לפני append.

### I — הפעלה אוטומטית

1. dispatch בסוף SefariaExport.
2. להסיר pipeline מקביל ישן.
3. שתי ריצות מלאות: shift ו־no-op, ועוד fault injection אחרי commit, אחרי
   Otzaria release ואחרי Seforim publish.
4. להוכיח שכל rerun מתכנס לאותו commit/release/provenance בלי `db_version` נוסף.
5. לסנכרן את פרק 9 בתיעוד למימוש בפועל.

---

## 14. Commits מומלצים

### SeforimLibrary

```text
feat(sefariasqlite): add strict manual-links refresh tool
test(sefariasqlite): cover bootstrap and atomic refresh
ci(release): pin Sefaria and Otzaria lineage inputs
```

### otzaria-library

```text
fix(manual-links): bootstrap stable Sefaria refs
fix(packaging): reject duplicate packaged link paths
ci(manual-links): automate Sefaria index refresh
docs(links): document automatic Sefaria synchronization
```

### SefariaExport

```text
ci(release): publish immutable lineage and dispatch link sync
```

אין squash בין data bootstrap לבין workflow commit.

---

## 15. מטריצת כשלים

| מצב | output | commit | Otzaria release | DB build |
|---|---:|---:|---:|---:|
| shift רגיל, ref יחיד, hash זהה | כן | כן | כן | כן |
| release חדש ללא shift | lineage | כן | כן | כן |
| rerun אותו full input tuple | report+marker `no_op` | לא | ensure/reuse | ensure/reuse |
| ref חסר/כפול | לא | לא | לא | לא |
| expected book חסר/עמום | לא | לא | לא | לא |
| anchor hash שונה | לא | לא | לא | לא |
| changelog chain חסרה | לא | לא | לא | לא |
| collision | לא | לא | לא | לא |
| JSON פגום/duplicate key | לא | לא | לא | לא |
| config drift ללא migration | לא | לא | לא | לא |
| push conflict אחרי 3 retries | לא | לא | לא | לא |
| lineage mismatch ב־build | — | — | release אינו נצרך | לא |
| כשל אחרי commit | recovery no-op/ok | reuse commit | ensure | ensure |
| כשל אחרי release שלם | recovery no-op | לא | reuse exact | ensure |

---

## 16. Rollback

1. להשבית dispatch מ־SefariaExport.
2. להשבית `sync-manual-links.yml`.
3. `git revert` ל־commit האוטומטי האחרון; אין reset/force.
4. לפרסם Otzaria release חדש מה־revert.
5. לא לבנות DB עד ש־lineage תואם שוב.

אין rollback של schema או allocator משום שלא שונו.

---

## 17. Definition of Done

- [ ] ‏99,476 הרשומות הרלוונטיות נושאות ref בצד הספריאתי.
- [ ] ‏17,980 רשומות משנה ברורה נושאות SHA-256 תקף של מחרוזת BookPayload המדויקת.
- [ ] ‏81,496/81,496 יעדי Sefaria פתירים exact.
- [ ] ‏0 refs חסרים/כפולים בקורפוס הנצרך.
- [ ] ‏0 packaged-path collisions.
- [ ] `דרך ה_links.json` נמחק ואין יותר מנגנון `excluded_files`.
- [ ] אין DB, sidecar, fuzzy, LCS עמום או positional fallback ב־updater.
- [ ] input checkout אינו משתנה; marker נוצר רק בהצלחה.
- [ ] rerun על אותו full input tuple הוא byte-identical no-op ומשלים downstream חסר.
- [ ] כל Sefaria release מפעיל sync.
- [ ] sync יוצר commit scoped ב־`otzaria-library`.
- [ ] Otzaria release immutable מכוון בדיוק ל־expected commit ומכיל את lineage.
- [ ] Seforim build צורך tags+digests מפורשים, לא `latest`.
- [ ] lineage/config/tree mismatch נכשל לפני `generateSeforimDb`.
- [ ] Generator resolver/runtime לא שונה; רק `sourceDir` מחווט ל־Havrouta.
- [ ] allocator/IDs/patch producer לא שונו.
- [ ] E2E DB וה־delta verifier עוברים.
- [ ] פרק 9 מתאר את ה־updater שבוצע, לא את ארכיטקטורת ref-based שנדחתה.

כאשר כל התיבות מסומנות, shift בעקבות Sefaria מעדכן אוטומטית את הקישורים ומחייב commit+release תואמים; שינוי שאינו ניתן להוכחה עוצר את הפרסום במקום ליצור קישור שגוי.
