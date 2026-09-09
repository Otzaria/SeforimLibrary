# סקריפטי QA — ייבוא v2.2 (סעיף 10 בתוכנית)

כלי ייחוס versioned לבדיקות ה-DB של `SEFARIA_METADATA_IMPORT_PLAN.md` סעיף 10.
עד כה הם התקיימו רק ב-scratchpad של סשן (`claim_a..f.py`) ולא היו ניתנים לשחזור
(סעיף 10א). כאן הם קוד versioned. stdlib בלבד (`sqlite3`, `json`, `argparse`).

## שימוש כללי

כל סקריפט מקבל `--db PATH` (בבדיקה 5 הוא אופציונלי — החובה שם היא `--metrics PATH`).
בדיקות ההשוואה לסכמות מקבלות גם `--sefaria-dir PATH`.

`--sefaria-dir` מצביע על ארטיפקטי ה-Sefaria export של אותה בנייה. הפריסה האמיתית:

```
generator/sefariasqlite/build/sefaria/export/schemas/*.json
```

אפשר להעביר את `.../sefaria`, את `.../export` או את `.../schemas` ישירות — הסקריפט
מנסה את המועמדים בסדר קבוע: `<dir>/schemas`, `<dir>/export/schemas`, ורק לבסוף
`<dir>` עצמו. כל מועמד עם קובצי `*.json` מאומת שהוא **באמת** תיקיית schemas (לפחות
קובץ אחד נטען ל-dict עם אובייקט `schema` מקונן); אם למועמד יש `*.json` אך אף לא
schema אחד (למשל `export/` המכיל רק `table_of_contents.json`) — ממשיכים למועמד הבא;
אם אף מועמד לא מספק schema — כשל בקול עם רשימת הנתיבים שנוסו. קובץ schema שאינו
נטען לספר — בין שאינו נפרס כלל, בין שהוא JSON תקין שאינו אובייקט בשורש, ובין שאין
בו אובייקט `schema` מקונן — מדולג רק אם הוא ברשימה המפורשת
`KNOWN_UNREADABLE_SCHEMAS` ב-`common.py`, ואז נרשמת
שורת `INFO:` אחת עם הסיבה. כרגע ברשימה `Sheet.json` בלבד: ה-pseudo-index של
דפי-המקורות בספריא, שהייצוא שולח כקובץ באורך 0 — לא ספר, ואין לו שורה ב-`book`.
זה שכפול דטרמיניסטי של `runCatching` פר-קובץ ב-`SefariaBookPayloadReader.kt:33-49`,
לא היוריסטיקה. כל קובץ אחר שאינו נטען לספר הוא נזק בארכיון מקובע ומאומת-digest:
הוא נספר, נוקב בשם ובסיבה, ומפיל את הבדיקה — במקום לצמצם את כיסויה בשקט ולדווח
`PASS`.

### שער סחיפה מול ה-reference snapshot

בדיקות 1/2/7 נושאות baselines קשיחים. בנוסף להשוואה הפנימית (DB מול schemas) הן
מעבירות כל מדד סָפוּר דרך `gate_snapshot_drift`: התכווצות מעבר ל-
`QA_DRIFT_MAX_SHRINK_PCT` אחוזים מה-baseline היא `::error::` ויציאה 1; כל הפרש אחר
(התכווצות קטנה יותר, או גדילה) הוא `::warning::` עם המספרים.

כשהמשתנה אינו מוגדר (או ריק) **השער כבוי לגמרי ואינו מדפיס דבר** — ה-baselines
משמעותיים רק מול הקורפוס המקובע, לא מול פיקסטורה סינתטית או DB חלקי בהרצת אד-הוק.
אין לכך ברירת-מחדל מובלעת: ה-workflow השבועי קובע `QA_DRIFT_MAX_SHRINK_PCT: "2"`
בצעד ה-QA, ו-`run_all.py --require-all` (מצב release) נכשל מיד אם המשתנה חסר, כדי
שהרצת release לא תוכל לרוץ בלי שער. ערך לא-מספרי או שלילי הוא כשל, לא שער כבוי.
אין בכך רענון של ה-baseline — `--expect-snapshot` ממשיך לאכוף את המספרים המדויקים.

השער אינו אחוזי בלבד: התכווצות היא `::error::` רק כשהיא חוצה **גם** את האחוז
**וגם** את הרצפה המוחלטת `QA_DRIFT_MIN_SHRINK_ABS` (ברירת מחדל 10, ה-workflow
קובע אותה במפורש). על מדד קטן (`guides`=2, `midrash`=5, זוגות מוסקים=20,
`targum`=45) ירידה של 1 היא 2%..50% — בלי הרצפה ספר יחיד שנעלם מהייצוא היה מפיל
בנייה שבועית; עם הרצפה הוא `::warning::` עם המספרים, וקריסה אמיתית עדיין נכשלת.

הרצת הכול:

```
python3 scripts/qa/run_all.py --db seforim.db \
  --sefaria-dir generator/sefariasqlite/build/sefaria/export \
  --metrics seforim.db.link-import-metrics.json
```

בלי `--sefaria-dir` — בדיקות 1/2/6 מדולגות (רק 3/5/7/8 רצות). בלי `--metrics` —
בדיקה 5 מדולגת עם שורת SKIP מפורשת; ‏run_all **אינו** מנחש את נתיב ברירת-המחדל
`<db>.link-import-metrics.json` (ללא fallbacks — הדו"ח נבדק רק כשמורים עליו במפורש).

### `--require-all`: דילוג = כשל (הרצת release)

בברירת המחדל דילוג מותר (נוח להרצות אד-הוק), ולכן run_all **לעולם אינו** מדפיס
"כל הבדיקות עברו" כשמשהו דולג — הסיכום מנסח במפורש `עברו X, דולגו Y`. בהרצת
release יש להעביר `--require-all`: אז **כל** דילוג הופך את הריצה לכשל (יציאה!=0)
עם רשימת הבדיקות שדולגו והארגומנט המפעיל כל אחת. כך בנייה ששכחה `--metrics` לא
"עוברת" בשקט ותפספס את בדיקה 10.5. הדגל `--sefaria-stage` מוזרם לבדיקה 5 (ר' להלן).

קוד יציאה: `0` = עבר, שונה מ-`0` = כשל, עם הודעה ברורה ל-stderr (fail loudly, ללא fallbacks).

## ברירת המחדל: expected מחושב מהבנייה, לא מ-baseline קשיח (סעיף 10א)

ה-exporter משכפל את Sefaria-Project HEAD ללא הצמדה
(`05_clone_sefaria_project.sh` — `git clone --depth 1`), ולכן המספרים
`4,941 / 5,426 / 13,056 / 20` נכונים ל-snapshot הנוכחי בלבד. לכן:

- **ברירת מחדל:** כל בדיקה מחשבת את ה-`expected` מארטיפקטי אותה בנייה (schemas)
  ומשווה שורה-שורה מול ה-DB. זו ההשוואה המחייבת.
- ה-baselines הקשיחים מודפסים כערכי ייחוס בלבד, ונאכפים **רק** עם הדגל
  האופציונלי `--expect-snapshot` (רלוונטי כשבונים מה-snapshot הנוכחי).

## מיפוי סקריפט ↔ בדיקת סעיף 10

| סקריפט | בדיקה | מה נבדק | ארגומנטים |
|---|---|---|---|
| `check1_dependence_count.py` | 10.1 | `book.dependenceType` מול schemas; ‏baseline 4,941 ופילוח | `--db --sefaria-dir [--expect-snapshot]` |
| `check2_book_base_text.py` | 10.2 | `book_base_text` (מוצהר) עם רזולוציית alt-titles בסדר-priority; ‏5,426 | `--db --sefaria-dir [--priority-list] [--expect-snapshot]` |
| `check3_elucidation.py` | 10.3 | 0 קישורי ELUCIDATION ב-DB | `--db` |
| `check5_import_metrics.py` | 10.5 | דו"ח מדדי ייבוא הקישורים (`<db>.link-import-metrics.json`, צורת `insertedByType`/`persistedByType`): אי-שליליות, ‏dropped ≤ rowsRead פר-סוג, ‏Σwritten ≤ ΣresolvedPairs, ‏Σwritten==Σpersisted, ואופציונלית DB ≥ persisted פר-סוג (או == עם `--sefaria-stage`) + הצלבת schema_meta | `--metrics [--db] [--sefaria-stage]` |
| `check6_metadata_rowbyrow.py` | 10.6 | שורה-שורה לפי heRef: dependenceType, collectiveTitleHe/En | `--db --sefaria-dir` |
| `check7_provenance.py` | 10.7 | baseProvenance 1/2, זוגות מוסקים, עקביות מכוונת (source,target) מול book_base_text, הרכב דרגות פר ספר-מקור (סדר ה-SOURCE עצמו — בבדיקת ה-dao, ר' להלן) | `--db [--expect-snapshot]` |
| `check8_integrity.py` | 10.8 | `PRAGMA quick_check` + `PRAGMA foreign_key_check` | `--db` |

## סינון source='Sefaria' (בדיקות 1/2/6)

בדיקות 1/2/6 מצליבות schemas עם `book` **רק** לספרים שמקורם ספריא
(`book.sourceId → source.id`, ‏`source.name = 'Sefaria'`; אם השורה חסרה — כשל בקול).
בלי הסינון, 5 ספרי MoreBooks ששמם (heRef) זהה במקרה ל-heTitle של schema דלפו להשוואה
(check6 החזיר matched=5,826 מול 5,821 ספרי Sefaria בפועל) — ספר כזה גם היה עלול להיתפס
כיעד רזולוציה ב-check2 ולנפח את expected ב-check1. ‏`book_base_text` (בדיקה 2) נקרא
במלואו בכוונה: שורה של ספר לא-Sefaria שם היא באג ותופיע כ"עודף".

## בדיקה 5: דו"ח מדדי ייבוא הקישורים

‏`SefariaLinksImporter` כותב בסוף ייבוא הקישורים דו"ח JSON דטרמיניסטי לצד ה-DB
הנשמר — `<db>.link-import-metrics.json`. מקומיט **ddb1f24** ואילך הצורה היא:

```json
{
  "db_schema_version": "2",
  "db_version": "15",
  "db_size_bytes": 7906639872,
  "insertedByType": {"COMMENTARY": {"rowsRead": 0, "dropped": 0, "resolvedPairs": 0, "written": 0}},
  "persistedByType": {"RELATED": 0}
}
```

הצורה הישנה (`perType` בלבד) נדחית בקול עם הודעה שהדו"ח קדם ל-ddb1f24.

**`insertedByType`** — מוני שלב-ההכנסה, **טרם** ה-demotion. ‏`rowsRead` = כל שורת CSV
שנפרסה; ‏`dropped` = שורות שלא הניבו קישור; ‏`resolvedPairs` = זוגות ששני צידיהם נפתרו
ל-line ids (לפני סינוני כותרת/קישור-עצמי); ‏`written` = הכנסות **בפועל** (INSERT OR
IGNORE). שלושת הראשונים ממופתחים לפי הטיפוס הגולמי מה-CSV; ‏`written` לפי הטיפוס שנשמר
בהכנסה — ולכן `Σwritten ≤ ΣresolvedPairs` נאכף גלובלית, ו-`dropped ≤ rowsRead` פר-סוג.

**`persistedByType`** — ‏`COUNT(*)` סמכותי **אחרי** `demoteCrossCorpusDependantLinks`
(בבנייה אמיתית ה-demotion מעביר ~56K קישורי COMMENTARY/MIDRASH חוצי-קורפוס ל-RELATED).
ה-demotion רק ממיין-מחדש, ולכן `Σwritten(insertedByType) == ΣpersistedByType` (מדויק,
נאכף גם בצד Kotlin). **לכן כל השוואה פר-סוג מול ה-DB משתמשת ב-`persistedByType`,
לא ב-`written`.**

הצלבת ה-DB (עם `--db`) היא `≥` ולא `==`: ה-DB המלא צובר קישורים גם ממקורות אחרים
(havrouta/otzaria/linker). כשה-DB הוא **DB בשלב-ספריא בלבד** (טרם הצבירה) יש להעביר
`--sefaria-stage` וההשוואה הופכת מדויקת (`==`). בנוסף, כש-`db_schema_version`/
`db_version` בדו"ח אינם `null` ו-`--db` הועבר — הם מוצלבים מול `schema_meta` ב-DB
(אי-התאמה = כשל); ‏`null` (טרם שלב ה-stamp) מתקבל.

## סדר ה-SOURCE (בדיקה 7) — מכוסה בבדיקת ה-dao, לא כאן

סקריפט SQL אינו יכול לאמת את ה-ORDER BY של ה-mirror query בלי לשכפל אותו (טאוטולוגיה).
לכן check7 בודק ברמת הנתונים בלבד — הרכב דרגות provenance פר ספר-מקור נדגם (ספירה
שורה-שורה מול GROUP BY, ודרישה ששלוש הדרגות 2/1/0 נצפות בדגימה) — והסדר עצמו מכוסה
בבדיקת האינטגרציה `SeforimRepositoryIntegrationTest.`‏
`` `SOURCE view orders by baseProvenance DESC ahead of all tie-breakers` `` (קומיט
f92c99c), שרצה מול השאילתה האמיתית `selectInverseLinksByTargetLineIds` ונכשלת אם
`l.baseProvenance DESC` יוסר מ-`LinkQueries.sq`.

## התאמת schemas ↔ DB לפי heRef (לא title)

שישה ספרים תלויים משוני-שם בייבוא (ר' סעדיה גאון על בראשית/עזרא/נחמיה, אדרת
אליהו, חומת אנך על דה"י א/ב) — ה-`title` ב-DB שונה מ-heTitle של הסכמה, אבל
`book.heRef` נשאר `payload.heTitle`. לכן כל הצלבת schemas↔DB בסקריפטים
(בדיקות 1/2/6) נעשית לפי **heRef** — כפי שה-baseline ‏4,941 נקבע בתוכנית
(עובדה 5: התאמת title נותנת 4,935 שגוי).

## רזולוציית alt-titles (בדיקה 2 — קריטי)

התאמה נאיבית נותנת 5,420 במקום 5,426 — 6 בסיסים נפתרים רק דרך וריאנט שם.
הסקריפט משכפל את `normalizedTitleToBookId` של `SefariaDirectImporter.kt` (המעבר הדחוי):

1. `normalize_title_key` זהה ל-`SefariaImportText.kt` (הסרת גרש/גרשיים/מרכאות,
   lowercase, כיווץ רווחים, `_`→רווח, trim).
2. מהסכמות מחלצים לכל ספר: `base_text_titles` (מוצהר, en+he מנורמלים) ו-aliases
   (`titleVariants`+`heTitleVariants`, פרט ל-HTML), בדיוק כמו
   `SefariaBookPayloadReader.kt`.
3. הספרים מצומצמים לאלה שקיימים ב-DB (התאמת heTitle מנורמל ל-`book.heRef` —
   מיישם דה-פקטו את ה-blacklist ועמיד לשינויי-שם בייבוא). **סדר הספרים משוכפל
   מ-`applyPriorityOrdering` של היבואן**: תחילה לפי `priority.txt` (ה-resource
   המגורסן ברפו, `generator/sefariasqlite/src/jvmMain/resources/priority.txt`,
   ברירת המחדל של `--priority-list`), אחריו השאר. הנחת סדר שיורית: סדר "השאר"
   אצל היבואן הוא סדר סריקת קבצי הטקסט — כאן סדר שמות קובצי schemas; משפיע רק
   על התנגשויות מפתח בין ספרים לא-priority. בונים מפה `normalized→bookId`
   בשני **מעברים גלובליים**, כמו `buildNormalizedTitleToBookId` המתוקן
   (`SefariaDirectImporter.kt`): מעבר 1 — הפרימריז (heTitle→enTitle) של **כל**
   הספרים בסדר-priority; מעבר 2 — ה-aliases של **כל** הספרים; הכול `putIfAbsent`.
   פיצול המעברים מבטיח שפרימרי מנצח alias — גם alias של ספר מוקדם יותר.
4. כל מפתח בסיס מוצהר נפתר דרך המפה (כולל aliases) → זוג `(bookId, baseBookId)`.
   קבוצת הזוגות מושווית שורה-שורה מול `book_base_text`.

### פוסטמורטם: ‏5,437 היה באג יבואן; ‏5,426 הוא ה-baseline הנכון

הרצה מוקדמת נתנה ‏DB=5,437: ‏11 זוגות "עודפים", כולם מפרשי מעילה עם
`base_text_titles={"en":"Meilah","he":"מעילה"}`. מפתח `"meilah"` נפתר בטעות ל**משנה
מעילה** (id 87) — כי ה-alias `"meilah"` של משנה מעילה, שנטענה מוקדם, האפיל על
ה**פרימרי** `"Meilah"` של תלמוד מעילה שנטען מאוחר. זהו **באג ביבואן**: המפה נבנתה
פר-ספר משולב (פרימריז+aliases של כל ספר יחד), כך ש-alias של ספר מוקדם ניצח פרימרי
של ספר מאוחר, וכפל 11 זוגות. תוקן ב-8358a16 (בניית המפה בשני מעברים גלובליים —
כל הפרימריז ואז כל ה-aliases; ראו למעלה). ה-baseline הנכון הוא **5,426**, וזהו הערך
שהסקריפט מצפה לו. הרצת הסקריפט המתוקן מול DB **שקדם** לתיקון תיכשל בקול עם בדיוק
11 זוגות `(X, 87)` כ"עודפים ב-DB" — כלומר הסקריפט מזהה כעת את הבאג.

## בדיקות שאינן סקריפטים כאן (בכוונה)

- **4 (enum) ו-9 (build נכשל על סוג לא-ממופה):** מכוסות ב-unit tests של המאגר
  (round-trip `fromString`, יציבות ordinal, `fromKnownStringOrNull`, guard סינתטי).
- **סדר ה-SOURCE (חלק מ-10.7):** בבדיקת האינטגרציה של ה-dao (קומיט f92c99c, ר' למעלה).
- **10 (E2E: full download + patch 2→2 דרך ה-updater):** קומיט 13, לא כאן.

(בדיקה 5 הפכה לבת-סקריפט: היבואן פולט כעת מונים per-connection-type ודו"ח JSON —
ר' `check5_import_metrics.py` למעלה.)

## אימות

הסקריפטים אומתו מול DB סינתטי (טבלאות/schemas מינימליים) בתרחיש עובר ובתרחיש
נכשל לכל בדיקה — כולם מדווחים כשל בקול ויוצאים בקוד שונה מ-0 (ראו
`tests/test_qa_synthetic.py`, כולל רגרסיות: פרימרי של ספר מאוחר מנצח alias של ספר
מוקדם; ספר לא-Sefaria החולק heRef עם schema מוחרג מבדיקות 1/2/6 בלי pass/fail כוזב;
תרחישי pass/fail ל-check5). ‏baseline סכמה 2 נכון: ‏4,941 / **5,426** / 13,056+20 /
‏ELUCIDATION=0. ה-`build/seforim.db` שעל הדיסק נבנה **אחרי** תיקון היבואן (8358a16)
ולכן check2 עובר עליו עם 5,426 בדיוק. הדו"ח `build/seforim.db.link-import-metrics.json`
עוד לא קיים בבנייה הנוכחית (קוד המדדים, 6917895, מאוחר לבנייה האחרונה) — ולכן check5
אומת בפיקסטורות סינתטיות בלבד, ו-run_all מדלג עליו עד שתועבר `--metrics` מבנייה חדשה.
ה-E2E המלא (patch 2→2 דרך ה-updater) — קומיט 13.
