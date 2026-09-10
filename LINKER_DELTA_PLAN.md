<div dir="rtl">

# תוכנית: הפיכת הלינקר המקומי ליישום — אחסון, שילוב בגנרטור, ו-delta אינקרמנטלי

תוכנית זו ממשיכה את `LINKER_INTEGRATION_PLAN.md` (שלב 1 — הרצה מקומית — הושלם: 2.16M
קישורים). היא פותרת את שני החלקים שהופכים POC ליישום מתמשך:

1. **אחסון + שילוב** — היכן הקישורים חיים ואיך נכנסים לספרים בגנרטור, **בלי `<a href>`**.
2. **‏delta אינקרמנטלי** — מעקב שינויים, הרצה רק על ספרים שהשתנו, והתאמת קישורים
   קיימים לספרים שהשתנו — כך שעדכון של ספריא לא ישבור קישורים.

> **הבשורה:** רוב התשתית כבר קיימת. ‏SefariaExport כבר מפרסם מעקב-שינויים;
> ‏otzaria-library כבר מכיל שלד delta ללינקר (מושבת); והגנרטור כבר יודע לפתור
> ‏ref→שורה ולבנות patch יציב. התוכנית בעיקר **מחברת ומחיה** רכיבים קיימים.

---

## 0. התובנה המרכזית — למה קישורים לא נשברים

קישור-לינקר הוא שני קצוות:
- **צד-יעד** = **ref של ספריא** ("Yoma 55a:5") — מזהה לוגי יציב.
- **צד-מקור** = מיקום בספר (שורה + טווח-תווים של הציטוט).

**העיקרון:** לא לאחסן `targetLineId` פתור, אלא לאחסן את **ה-ref**, ולפתור אותו
ל-שורה **בזמן בניית ה-DB** דרך ה-resolver הקיים של הגנרטור. כך:

- כשספריא משנה ספר-יעד, ה-ref פשוט נפתר לשורה החדשה — **בלי לגעת בקישור**.
- הגנרטור ממפתח `lineId` לפי **תוכן השורה הגולמי** בלבד
  (`IdAllocatorBindings.lineNaturalKeyHash` = `sha1("CT:"+rawSegment)`, ללא
  ה-heRef וללא הקידומת המיוצרת), כך ש-**כל עוד טקסט הפסוק לא השתנה, ה-`lineId`
  יציב בין builds → ה-`linkId` יציב → הקישור שורד עדכון-מבנה או שינוי-ref של
  ספריא ללא שום churn ב-delta.**
  > עד db v27 המפתח היה `REF:$heRef`, ומעבר-עיצוב שכתב מחדש כל heRef מספרר
  > מחדש את כל הקורפוס (issue #1211). ה-ref הוא עמודה מתעדכנת, לא מפתח.

זו התשובה ל"התאמת קישורים לספרים שהשתנו": **ההתאמה אוטומטית דרך פתרון-מחדש**,
לא הגירה. הצעד היקר (NER) מתבצע רק על ספרי-**מקור** שהשתנו.

---

## חלק 1 — אחסון ופורמט השילוב

### 1א. פורמט הקישור באפליקציה: `link_anchor`, לא `<a href>` ולא תג בטקסט

**המלצה חד-משמעית: להשתמש במנגנון `link_anchor` הקיים — לא להזריק שום דבר לטקסט.**

הבקשה שלך ("שהאפליקציה לא תחשוב שזה קישור רגיל, אולי תג מיוחד") **כבר פתורה
בארכיטקטורה הקיימת**: טבלת `link_anchor` מסמנת טווח-תווים לחיץ על שורה
(`charStart`/`charEnd`, `side=0`) **בלי לגעת בטקסט הספר כלל**. האפליקציה כבר
מרנדרת עוגנים כקישורים פנימיים (סמני "שער הציון"). כלומר:

- אין `<a href>` בטקסט → האפליקציה לא מתבלבלת.
- הטקסט נשאר נקי → חיפוש/העתקה/delta לא נפגעים (תג בטקסט היה מזהם את הכול).
- להבחנה + הפעלה/כיבוי: **`connectionType` ייעודי `LINKER`** (ראו 1ג) — האפליקציה
  יכולה לצבוע/לסנן קישורי-לינקר בנפרד מקישורי ספריא.

**חלופה (רק אם ממש נדרש סימון בתוך הטקסט):** תג ממורחב כמו
`<iref t="Yoma 55a:5">…</iref>` שהאפליקציה מזהה כקישור פנימי. **לא מומלץ** — מזהם
את התוכן, מצמיד offsets לטקסט, ומייצר delta-churn בכל עריכה. ה-`link_anchor`
עדיף בכל פרמטר.

### 1ב. היכן חיים הקישורים (הארטיפקט)

הארטיפקט הוא **ref-based**, קובץ לכל ספר-מקור. **הסכמה הקנונית מוגדרת ב-`LINKER_IMPLEMENTATION_STAGES.md`
(שלב 1)** — כאן רק תמצית. שדה יעד = **ref** ולא line-index:

```json
{ "book_key": {"source_name":"MoreBooks", "canonical_he_title":"חזון איש"},
  "line_index": 28, "line_index_base": 0, "start": 37, "end": 48,
  "target_ref": "Psalms 16:8" }
```

- `book_key` = **אובייקט מובנה** התואם 1:1 ל-`BookKey(sourceName, canonicalHeTitle)` של
  ה-allocator (ראו שלב 5 — identity mapping); מזהה-מקור collision-safe. הסכמה הקנונית
  המלאה ב-`LINKER_IMPLEMENTATION_STAGES.md` שלב 1.
- `start`/`end` = offsets גולמיים (כולל HTML) לתוך שורת-המקור **המנוקה** (כפי שהלינקר מפיק מ-`dumpLines`).
- `target_ref` = ה-ref האנגלי הקנוני (המפתח היציב; מה שהגנרטור יפתור).
- ‏line-index של היעד **מושמט בכוונה** — הוא מקור השבירות; היעד נפתר מ-`target_ref`.
- דו-משמעי מסונן החוצה כבר בהפקה — הארטיפקט מכיל רק חד-משמעיים.

**מיקום: רפו ייעודי (`otzaria-linker-links`)** — הוכרע. מנתק את ~300MB הארטיפקטים
מ-otzaria-library, ומאפשר CI ומחזור-חיים עצמאיים. ‏git-diff אינקרמנטלי מצמצם את
ה-commit לכל עדכון (רק ספרים שהשתנו). השלד המושבת ב-`otzaria-library/linker/`
(`linker_on_commit.py`, `to_otzaria_links.py`) משמש כ**מקור-לוגיקה** להעתקה/החייאה
ברפו החדש — לא כמיקום-אחסון.

### 1ג. הוספת `connectionType` = `LINKER` + wiring מלא (מתוקן)

- להוסיף קבוע `LINKER` ל-`enum ConnectionType` ב-`core/.../models/Link.kt` +
  מקרה ב-`fromString`. ה-id מוקצה יציב אוטומטית (רישום-מראש של `values()`).
- **אוריינטציה (תיקון — היה לא-עקבי):** ל-LINKER **לא** להחיל בסיס→תלוי. לאחסן
  **שורת-הציטוט (מקור) → שורת-היעד (ref ספריא)**, עם `link_anchor side=0` על
  שורת-הציטוט (שם הטווח הלחיץ). ‏LINKER הוא **שכבת-ציטוט חד-כיוונית קדימה**, לא
  יחס בסיס/מפרש. הניסוח הקודם "בסיס→תלוי" היה שגוי כאן — הוא היה ממקם את העוגן
  בצד הלא-נכון.
- **‏wiring מעבר ל-enum (הרחבה נדרשת):** הוספת ה-enum מטפלת רק בכתיבה/id. צד
  הקריאה מכיל רשימות-טיפוסים קשיחות — במיוחד מנגנון ה-SOURCE הווירטואלי
  (`dao/.../LinkQueries.sq`, שאילתות המראה). יש **להחריג את LINKER מהמראה של
  SOURCE** (הוא לא מקבל תצוגת-מקורות הפוכה כברירת-מחדל) ולעדכן את ה-DAO/אפליקציה
  שיזהו LINKER כשכבה נפרדת. (תצוגת "מי מצטט אותי" הפוכה = פיצ'ר עתידי, לא ברירת-מחדל.)

---

## חלק 2 — הפייפליין האינקרמנטלי + delta

### 2א. מה כבר קיים (מפת שימוש-חוזר)

| רכיב | היכן | סטטוס |
|---|---|---|
| זיהוי שינוי בספרי **ספריא** | `SefariaExport/manifest.txt` (sha256 לכל `merged.json`) | ✅ מתפרסם ב-release |
| דיף מובנה (added/removed/renamed/moved/content_changed/versions) | `SefariaExport/changelog_diff.json` | ⚠️ מיוצר, אך נשלח רק לפורום — **לא** מתפרסם |
| זיהוי שינוי בספרי **otzaria** | `otzaria-library/files_manifest.json` (מקונן `{path:{hash}}`, ברפו + ב-`otzaria_latest.zip`) | ✅ קיים (לא `hash_all_files.json` — זה cache פרטי מיושן) |
| שלד delta ללינקר (A/M/D + rename/move) | `otzaria-library/linker/linker_on_commit.py` | ⚠️ קיים, **מושבת**, ו-API-based |
| המרה + מיזוג (מחליף רק ערכי "linker") | `otzaria-library/linker/to_otzaria_links.py` | ⚠️ קיים, **מושבת** |
| פתרון ref→שורה | `SefariaImportRefs.resolveRefs` (+ `refsByCanonical/refsByBase`) | ✅ קיים, בדוק |
| ‏id יציב + patch DB-ל-DB | `InMemoryIdAllocator.linkId`, `PatchDbProducer`, `PatchTables` | ✅ קיים |
| ‏line-id יציב לפי תוכן | `lineNaturalKeyHash` = `sha1("CT:"+rawSegment)` | ✅ קיים (הלב של היציבות) |
| מעקב ספרי-מקור שהשתנו בבנייה | `SourceHashComputer` + `TouchedBookDetector` + `BookRenameDetector` | ✅ קיים |

**המסקנה:** אין לבנות תשתית delta מאפס — יש **להחיות ולחבר**.

### 2ב. הסקריפט האינקרמנטלי (שלב הלינקר, לפני הבנייה)

מחליף את `link_all.py`/`linker_on_commit.py` ה-API-based בלינקר המקומי:

> **עדכון-ארכיטקטורה (2026-07-09):** שעון-שינוי-המקור היחיד הוא ה-`lines_snapshot.db`, **לא**
> ה-manifests. הלינקר יכול לקשר רק תוכן-snapshot וחותם ממנו `source_hash`; אילו ה-baseline היה
> מתקדם לפי שעון manifests נפרד, ספר שנקשר מול snapshot ישן היה נזרק כ-stale ב-Phase-2 ולא חוזר
> ל-re-link. ראו את הניסוח המעשי המחייב ב-`LINKER_IMPLEMENTATION_STAGES.md` שלב 3 ו-`incremental.py`.

1. **זיהוי ספרי-מקור שהשתנו — מ-`lines_snapshot.db` בלבד:** hash-תוכן-פר-ספר
   (`(source_name, canonical_he_title)`) מול `baseline/snapshot_hashes.json`. `changed`=שונה/נוסף;
   `removed`=נעלם מה-snapshot. הזהות נקראת ישירות מה-snapshot — **לא** מ-manifests/titles.
2. **הרצת הלינקר המקומי רק על `changed`** (Mongo+gpu-server+Django), מול **אותו** snapshot
   שממנו חושבו ה-hashים → עדכון הארטיפקט הref-based. **דקות, לא שעות.**
3. **delete של ספרי-מקור:** ספר שעזב את ה-snapshot (`removed`) → מחיקת קובץ-הארטיפקט שלו.
4. **כתיבת עדכוני-target refs (טיפול ב-rename של יעד) — השימוש היחיד ב-`changelog_diff.json`:**
   מעבר-מחרוזות זול על כל הארטיפקטים לפי `en_renamed` — **בלי הרצת לינקר, ולא לזיהוי-מקור.**
5. **קידום baseline רק אחרי engine מוצלח**, תוך החזקת ספרים שנכשלו פר-ספר מחוץ ל-baseline
   (retry במחזור הבא). ואז **commit** של הארטיפקטים המעודכנים.

> **תיקון תשתית קטן נדרש ב-SefariaExport:** לפרסם את `changelog_diff.json`
> כ-release-asset (הוספה לרשימת `UPLOADS` ב-`22_generate_changelog.sh`). כרגע
> הוא נשלח רק לפורום; הסקריפט צריך אותו מכונתית לצעד 4.

### 2ג. שילוב בגנרטור (זמן-בנייה, הלב של ה-delta-robustness)

‏importer חדש, בסגנון `generateHavroutaLinks` (תקדים למקור-קישורים נפרד וקיורטד):

1. קורא את ארטיפקטי הלינקר.
2. **יעד:** `resolveRefs(target_ref)` → `RefEntry` שנושא `lineId` (ה-sidecar מצרף
   ‏lineId לכל RefEntry — ראו identity mapping, שלב 5). מתקן גם את פער ה-4% של
   ה-resolver ה-Python (הגנרטור מטפל בטווחים/ירושלמי/עומק).
3. **מקור:** `book_key` → `bookId` (מפה שנבנית מה-DB לפי `BookKey(sourceName, canonicalHeTitle)`),
   ואז `(bookId, line_index)` → `sourceLineId`.
4. כותב `link` דרך `insertLinkStable(srcLineId, tgtLineId, LINKER)` + `link_anchor side=0`
   (המרת offset גולמי→נראה דרך `countVisibleChars` הקיים).
   > ⚠️ **‏identity mapping (הפרטים המחייבים בשלב 5):** ל-DB אין עמודת `path`/`book_key`
   > (‏`book`=sourceId/title/heRef, `line`=bookId/lineIndex). לכן היעד נפתר דרך lineId
   > מוטבע ב-sidecar, והמקור דרך `BookKey`→bookId שנבנה מה-DB.

**החלטת מיקום: (ב) Phase-2 + sidecar** (מוכרע). זה היחיד שרואה את *כל* הספרים
(מקור ספריא **וגם** otzaria) ומשתמש ב-`resolveRefs`.

**‏sidecar (תיקון — היה מעורפל מדי):** `resolveRefs` דורש **גם** `refsByCanonical`
**וגם** `refsByBase` + נפילות-החזרה (`:1`, base+" 1") — לא רק `canonical→lineIndex`.
לכן ה-sidecar חייב לשאת את כל ה-`RefEntry`-ים הגולמיים (`ref, heRef, path,
lineIndex`) **וגם את `lineId` המוטבע לכל רשומה** (זהות-היעד — ראו שלב 5), וה-Phase-2
**יבנה מחדש** את שתי המפות עם **אותה** פונקציית
`canonicalCitation` (שימוש-חוזר בקוד ה-resolver עצמו — בלי מימוש-כפול, בלי סטייה).
פורמט: **SQLite/Protobuf קומפקטי, לא JSON ענק בזיכרון** (מיליוני refs). זה גם
מייתר את אחסון ה-ref האנגלי ב-DB (שאינו קיים ממילא).

### 2ד. מטריצת השינויים — מה קורה לכל תרחיש, ובכמה

| שינוי | השפעה | טיפול | עלות |
|---|---|---|---|
| **ספר-מקור (otzaria) שינה תוכן** | מיקומי ציטוט זזים | הרצת לינקר מחדש על הספר בלבד | דקות (NER על ספר) |
| **ספר-יעד (ספריא) שינה מבנה/heRef, טקסט הפסוק נשמר** | השורה זזה | פתרון-מחדש בבנייה; `lineId` יציב (content-keyed) | **אפס** — קישור שורד, בלי churn |
| **ספר-יעד שינה את טקסט הפסוק עצמו** | `lineId` מתחלף | הקישור נמחק+נכתב מחדש ב-patch | churn מקומי (הפסוק אכן השתנה) |
| **ספר-יעד שונה-שם/הועבר** | ref-מחרוזת מיושן | rewrite לפי `changelog_diff.json` | זול (מחרוזות) |
| **ספר-מקור שונה-שם/נמחק** | קובץ-ארטיפקט מיושן | move/delete (לוגיקת `linker_on_commit.py`) | זול |

> **הערך:** התרחיש התכוף והמסוכן ביותר — עדכון-מבנה של ספר-יעד ספריא —
> עולה **אפס** בלינקר ואינו שובר קישורים, בזכות שילוב ref-artifact + line-id
> ממופתח-תוכן. זה מה שהופך את זה מ"שעות ריצה שמתפוצצות" ל"בנייה אינקרמנטלית".

### 2ה. מגבלה כנה — עריכת טקסט מחליפה `lineId`

כל השורות, בשני הצדדים, ממופתחות ל-`lineId` לפי **hash-תוכן**. לכן עריכה של
שורת-מקור מחליפה את `srcLineId` → `linkId` משתנה → הקישור נמחק+נכתב מחדש
ב-patch (churn) גם אם הציטוט טקסטואלית זהה. זה **מקובל** (השורה אכן השתנתה)
אבל מייצר delta גדול מהמינימלי. שיפור עתידי אפשרי: התאמת-שורות מנורמלת
(`LineMatcher`) שתשמר id על עריכה קלה. **לא חוסם** — היציבות החשובה (שינויי
מבנה ו-ref בספריא, המקרה התכוף) פתורה במלואה.

---

## חלק 3 — מפת דרכים מומלצת (צעדים קטנים והפיכים)

1. **‏SefariaExport:** לפרסם `changelog_diff.json` כ-release-asset (שינוי חד-שורתי).
2. **‏otzaria-library:** להחליף את גוף `call_sefaria_linker` בלינקר המקומי; לתקן
   באג dir; לוודא ש-`linker_on_commit.py` ו-`to_otzaria_links.py` כותבים target
   כ-**ref** (לא line-index). להחיות את ה-CI (הצעדים כבר קיימים, מוערים).
3. **‏SeforimLibrary:** להוסיף `ConnectionType.LINKER` **+ wiring קריאה**: להחריג
   את LINKER ממנגנון-המראה של SOURCE ב-`dao/.../LinkQueries.sq` ולעדכן את
   ה-DAO/אפליקציה שיטפלו בו כשכבה חד-כיוונית; להוסיף importer Phase-2
   (`generateLinkerLinks`) שמשתמש ב-`resolveRefs` + sidecar; לכתוב מקור→יעד דרך
   `insertLinkStable` + `link_anchor side=0`, עם dedupe מול קישור קיים.
4. **‏bootstrap:** הרצה מלאה חד-פעמית (בוצעה — 2.16M קישורים) מזינה את הארטיפקטים.
5. **אימות delta:** שתי בניות עוקבות עם עדכון-ספריא מדומה → לוודא ש-patch
   הקישורים קטן וש-`verifyApplyChain` עובר.

## חלק 4 — החלטות שהוכרעו

1. **מקשרים גם מספרי ספריא** (כרגע כן; ניתן לצמצם בהמשך). מקור = ספר ספריא **או**
   otzaria; יעד = ref של ספריא תמיד.
2. **אחסון: רפו ייעודי** (`otzaria-linker-links` או דומה). מנתק את ~300MB
   הארטיפקטים מ-otzaria-library, ומאפשר CI ומחזור-חיים עצמאיים.
3. **מיקום ה-importer: Phase-2 + sidecar** — היחיד שרואה את *כל* הספרים (מקור
   ספריא **וגם** otzaria) ומשתמש ב-`resolveRefs`. מינימום שינויים: phase אחד חדש
   בסגנון `generateHavroutaLinks` + dump-עזר קטן של אינדקס-refs משלב-ספריא.
4. **צד-מקור otzaria: להשאיר** content-hash keying (churn מקובל).

## חלק 4.5 — טופולוגיה מקצה-לקצה (מי מפעיל את מי)

**הרפו-הייעודי (`otzaria-linker-links`) הוא המרכז, ויש לו שני מזיני-קלט:**

```
┌─ sefaria-export (release חדש) ──trigger(repository_dispatch+PAT)──┐
│                                                                    ▼
├─ otzaria-library (push/שינוי ספרים) ──trigger──────────►  linker-repo CI
│                                                                    │
│   שעון-שינוי-מקור יחיד = ה-lines_snapshot.db (לא manifests):       │
│   • דיף hash-תוכן-פר-ספר מול baseline/snapshot_hashes.json         │
│   • changelog_diff.json ← רק ל-rewrite של target_ref (לא לזיהוי)   │
│                                                                    ▼
│   מריץ את הלינקר המקומי (Mongo+gpu-server+מודלים+Django) רק עליהם  │
│   → מעדכן ארטיפקטים ref-based → rewrite של target-refs ל-renames   │
│   → אורז zst (עם תג-ה-export שעובד) → מפרסם כ-release              │
└────────────────────────────────────────────────────────────────►  │
                                                                     ▼
          SeforimLibrary (בניית DB) ── מוריד את ה-zst ──► importer Phase-2
          פותר כל target_ref→שורה (resolveRefs) + כותב link+link_anchor
```

**נקודות טופולוגיה קריטיות:**

1. **שני מזינים, לא אחד.** הרפו מקשר ממקורות ספריא **וגם** אוצריא — לכן הוא מגיב
   גם ל-release של sefaria-export וגם לשינוי ב-otzaria-library. שניהם מזוהים
   ע"י דיף-מול-בסיס-עצמי (חלק 5), לא ע"י ה-changelog היחסי.
2. **‏trigger:** ה-workflow של sefaria-export (כרגע `workflow_dispatch` בלבד) יוסיף
   צעד סופי שיורה `repository_dispatch` לרפו-הלינקר (דורש PAT). כנ"ל otzaria-library.
3. **המחיר האמיתי — סטאק כבד ב-CI.** הרצת הלינקר דורשת MongoDB + dump של ספריא
   (~2GB) + gpu-server + מודלים + Django. זה **לא** runner סטנדרטי קליל: צריך
   self-hosted runner (כמו שכבר יש לבניית ה-DB) או job עם caching אגרסיבי
   ל-dump/מודלים. זה הרכיב היקר ביותר בתשתית.
4. **‏עקביות-מקור — לא ע"י pinning של תגים אלא ע"י `source_hash` (תיקון 2026-07-09).** ה-offsets
   רגישים לתוכן-המקור שממנו חושבו. במקום להסתמך על התאמת תגי-export/otzaria (שבירותי — otzaria
   "latest" נמחק בכל ריצה), **כל רשומה נושאת `source_hash` של תוכן שורת-המקור**, ו-Phase-2 מאמת
   אותו מול תוכן ה-DB הנוכחי ו-**safe-drop** באי-התאמה. `meta.json` נושא lineage אינפורמטיבי
   בלבד (sha256 של ה-snapshot + תג-export של ספריא). הכלל: **מקור ששינה תוכן ← re-link (זיהוי
   מ-snapshot); אי-התאמת-תוכן בבנייה ← drop בטוח, לא עוגן שגוי.**
5. **אין שלב "המרה ל-line-index".** בניגוד ל-`to_otzaria_links.py` הישן (שהמיר
   ל-`line_index_2`), כאן שומרים **רק את השלב הגולמי (ref-based)**; הפתרון ל-שורה
   קורה בבנייה. זה מה שמבטל את השבירות ומייתר שלב שלם.
6. **"החלה" בגנרטור = פתרון, לא העתקה.** ה-importer לא מעתיק שורות מוכנות; הוא
   **פותר מחדש** כל `target_ref` דרך `resolveRefs` בכל בנייה, וכותב `link` +
   `link_anchor` דרך ה-allocator היציב. כאן חיה כל ה-delta-robustness.

## חלק 5 — עקביות-בסיס: הרצות export כפולות ללא בנייה בנתיים

**התרחיש (שאלת המשתמש):** ‏sefaria-export רץ ב-R1 ואז ב-R2 בלי שהלינקר/הבנייה
צרכו את R1 בנתיים. מה קורה לספר שהשתנה ב-R1 אך לא ב-R2?

**הסכנה — "פער-delta":** ה-`changelog_diff.json` של כל release הוא **דיף יחסי**
מול ה-release הקודם בלבד (`diff(R1,R2)`). ספר שהשתנה ב-R1 ולא ב-R2 מופיע ב-דיף של
R1 אך **לא** בזה של R2. פייפליין שמסתמך על ה-changelog של ה-release האחרון בלבד
**יפספס** אותו → קישורים מיושנים/שבורים. זו מלכודת קלאסית: הדיף יחסי ל**הרצה
הקודמת של היצרן**, לא ל**הרצה הקודמת של הצרכן**.

**הפתרון (מומש 2026-07-09) — הצרכן מחזיק בסיס-עצמי מוחלט, אך הבסיס הוא ה-`lines_snapshot.db`, לא ה-manifests:**

- העיקרון "בסיס-עצמי מוחלט, לא changelog יחסי" נכון ונשאר — אבל ה-snapshot המוחלט הנכון
  הוא **תוכן ה-`lines_snapshot.db`** עצמו, כי זה הדבר היחיד שה-offsets תלויים בו. הלינקר
  מחשב hash-תוכן-פר-ספר על ה-snapshot ומשווה מול `baseline/snapshot_hashes.json` שלו →
  **קבוצת-השינוי המצטברת האמיתית**, ללא תלות בכמה מחזורים דולגו.
- **לא לצרוך את `changelog_diff.json` כמקור-אמת לזיהוי-מקור.** הוא משמש **רק** ל-rewrite של
  `target_ref` (rename של יעד באנגלית) — best-effort, ולא לזיהוי ספרי-מקור שהשתנו.
- זהות נקראת ישירות מה-snapshot (`(source_name, canonical_he_title)`) → אין צורך במיפוי
  manifest→book_key, ב-titles.json, או בנירמול-כותרת; rename של מקור נראה כ-remove+add
  (מצב-סוף נכון) דרך דיף ה-snapshot.

> **עדכון (2026-07-11) — המחזור הפך סריאלי:** הבנייה עצמה מריצה את הלינקר באמצעה
> (dump snapshot → relink דלתא → הזרקת קישורים לאותו DB), כך שכל release יוצא עם
> קישורים מלאים ואין עוד פיגור-מחזור. שעון-ה-snapshot, ה-baseline ו-guard ה-`source_hash`
> נשארים כלשונם — הם ההגנה בריצת standalone/ידנית, וב-סריאלי `-PlinkerStrict` דורש
> ‏0 stale/unmapped (כשל = הבנייה נכשלת בקול). הפסקה הבאה מתארת את מצב ה-standalone.

**עקביות lineage (הלב של התיקון):** ה-offsets (`start`/`end`, `line_index`) יחסיים
לתוכן-מקור ספציפי, והלינקר חותם `source_hash` על אותו תוכן. מכיוון שהלינקר צורך snapshot
של בנייה **קודמת** (חוצה-מחזור), אי-אפשר להבטיח שהוא רץ על "אותו export" כמו הבנייה שתחיל
את הקישורים. במקום זאת: **שעון-שינוי-המקור וה-baseline עוקבים אך-ורק אחרי ה-snapshot**, וב-build
‏Phase-2 מאמת `source_hash` מול תוכן ה-DB ו-**safe-drop** באי-התאמה. כך ספר-מקור שהשתנה
נקשר-מחדש בדיוק כשהשינוי נכנס ל-snapshot מפורסם (מחזור אחד אח"כ), ולעולם לא נוצר עוגן שגוי.
צד-היעד חסין ממילא (refs נפתרים מחדש בבנייה).

**הגנרטור עושה זאת בעיקרון דומה** (`SourceHashComputer`+`previousSourceHash` מ-`.buildstate`,
בסיס-עצמי מוחלט חסין-דילוגים) — אבל זה השעון של **הגנרטור** לזיהוי-delta שלו-עצמו; שעון-הלינקר
נפרד ומבוסס-snapshot כאמור.

**מסקנה:** הרצת export/מחזור כפולה **בטוחה לגמרי**: זיהוי-המקור מבוסס אך-ורק על ה-snapshot,
ה-baseline מתקדם רק לפי snapshot שנקשר בהצלחה, וה-`source_hash` guard סוגר את פער הצד-מקור.

</div>
