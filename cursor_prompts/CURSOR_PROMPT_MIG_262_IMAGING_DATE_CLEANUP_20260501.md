# Cursor Composer Dispatch — mig_262: Imaging date cleanup (raw_imaging_12_slots_v1 NULL + YY-typos + suspicious-LN-flag rebuild)

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex round-6).
**Lane:** mig_262 — `raw_imaging_12_slots_v1` accounts for **2,050/4,816 NULL exam_dates** (43%) and **2 extreme-outlier dates** (year 0202 and year 3022) that propagate into imaging_exam_master_v1. Apply 2-digit-year convention to recover the 2 outliers; investigate the 2,050 NULLs (likely upstream extraction missing). Plus rebuild `any_suspicious_us_ln_ever` flag — currently fires for 8/4,077 patients (effectively dead).
**Recommended agent:** **Cursor Composer** for the date cleanup (mechanical); **Cursor Chat** if rebuilding suspicious-LN-flag from scratch (definitional).
**Estimated runtime:** 75–90 min
**Triggered by:** Round 6 CF-mig260e-IMAGING-12SLOTS-DATE-QUALITY + CF-mig260g-US-LN-SUSPICIOUS-FLAG-UNDERFIRE.
**Severity:** MED. M032 era buckets, M025 imaging cohort, M076 LN surveillance — all use exam_date or LN-suspicious flag.
**Closes carry-forwards:** CF-mig260e, CF-mig260g.

---

## §0 — First message to paste into Cursor Composer

> mig_262 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_262_IMAGING_DATE_CLEANUP_20260501.md` end-to-end. Two parallel cleanups: (1) `raw_imaging_12_slots_v1.exam_date` — 2,050 NULL + 2 YY-typos to fix per the 2-digit-year convention; (2) `canonical_us_patient_master_VIEW_v2.any_suspicious_us_ln_ever` rebuild — currently 8/4,077 fires; either threshold too tight or never backfilled. Probe upstream first.

---

## §1 — Why this lane exists

### 1a. Date-quality bug
Round-6 Prompt 10 surfaced `raw_imaging_12_slots_v1` as the imaging-side data-quality outlier:
- 2,050 NULL exam_dates out of 4,816 rows (43%)
- 2 extreme-outlier dates: `0202-08-29` (rid 12048) and `3022-03-03` (rid 10511)

Per Logan-ratified `2-digit year convention` (memory: `reference_2digit_year_convention.md`, all YY → 20YY):
- `0202-08-29` likely → `2002-08-29`
- `3022-03-03` likely → `2022-03-03`

The 2,050 NULLs are a separate question — either upstream extraction never populated them or they're legitimately undated.

### 1b. Suspicious-LN flag effectively dead
`canonical_us_patient_master_VIEW_v2.any_suspicious_us_ln_ever` fires for **8 patients**, but:
- `has_us_ln_findings_ever=TRUE` for **4,077 patients**
- NLP cervical-LN rollup `has_positive_ln_level=TRUE` for **974 patients**

Either the threshold is too tight or the column never got backfilled when `canonical_us_thyroid_gland_v2`/per-nodule level data became available.

## §2 — Pre-task probes

### 2a. Date cleanup probes
```sql
-- A. Confirm 2 extreme-outlier rows and their full row state
SELECT * FROM main.raw_imaging_12_slots_v1
WHERE EXTRACT(YEAR FROM exam_date) < 1990 OR EXTRACT(YEAR FROM exam_date) > 2030;
-- Expect: 2 rows (rid 12048 with year 0202, rid 10511 with year 3022)

-- B. Are the NULLs concentrated in any rid range / source date / extraction batch?
SELECT
  CASE WHEN exam_date IS NULL THEN 'NULL' ELSE 'OK' END AS bucket,
  COUNT(*), COUNT(DISTINCT research_id),
  MIN(research_id), MAX(research_id)
FROM main.raw_imaging_12_slots_v1
GROUP BY 1;

-- C. Sample 30 NULL-date rows to surface upstream-source columns that might recover the date
SELECT research_id, slot_index, raw_text_excerpt, * EXCLUDE (exam_date)
FROM main.raw_imaging_12_slots_v1
WHERE exam_date IS NULL ORDER BY HASH(research_id) LIMIT 30;
-- Logan to eyeball whether raw_text_excerpt contains a parseable date the
-- extractor missed.
```

### 2b. Suspicious-LN flag probe
```sql
-- D. What's the current threshold definition?
SELECT view_definition FROM information_schema.views
WHERE view_name = 'canonical_us_patient_master_VIEW_v2';

-- E. Per-source LN-finding signal availability
SELECT
  COUNT_IF(has_us_ln_findings_ever) AS n_any_ln_findings,
  COUNT_IF(any_suspicious_us_ln_ever) AS n_suspicious,
  COUNT(*) AS n_with_us
FROM main.canonical_us_patient_master_VIEW_v2;

-- F. What's in canonical_us_thyroid_gland_v2 that could feed a better suspicious-LN flag?
SELECT column_name FROM information_schema.columns
WHERE table_name = 'canonical_us_thyroid_gland_v2' AND LOWER(column_name) LIKE '%ln%';
```

## §3 — Apply

### 3a. Pre-snapshot
```sql
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.raw_imaging_12_slots_v1_pre_mig262_20260501 AS
SELECT * FROM main.raw_imaging_12_slots_v1
WHERE exam_date IS NULL OR EXTRACT(YEAR FROM exam_date) < 1990 OR EXTRACT(YEAR FROM exam_date) > 2030;
```

### 3b. YY-typo fix (2 rows)
```sql
UPDATE main.raw_imaging_12_slots_v1
SET exam_date = DATE '2002-08-29'
WHERE research_id = '12048' AND exam_date = DATE '0202-08-29';

UPDATE main.raw_imaging_12_slots_v1
SET exam_date = DATE '2022-03-03'
WHERE research_id = '10511' AND exam_date = DATE '3022-03-03';
```

### 3c. NULL-date investigation (CHAT pass — no UPDATE yet)
For the 2,050 NULL-date rows, surface to Logan:
- Are they recoverable from an upstream column (e.g. `raw_text_excerpt`)?
- Or genuinely undated (Logan accepts NULL as the truth)?

If recoverable, apply a regex-extraction UPDATE in §3d:
```sql
-- Example pattern — Logan to ratify regex
UPDATE main.raw_imaging_12_slots_v1
SET exam_date = TRY_TO_DATE(REGEXP_SUBSTR(raw_text_excerpt, '\\d{1,2}/\\d{1,2}/\\d{2,4}'), 'MM/DD/YYYY')
WHERE exam_date IS NULL
  AND raw_text_excerpt RLIKE '\\d{1,2}/\\d{1,2}/\\d{2,4}';
-- Apply 2-digit year convention: TRY_TO_DATE handles YYYY; for YY format, augment.
```

### 3d. Suspicious-LN flag rebuild
After Logan ratifies the threshold definition (probably from `canonical_us_thyroid_gland_v2` per-nodule level), rewrite the flag in `canonical_us_patient_master_VIEW_v2`. Likely a CASE expression:
```sql
-- Pseudo: any_suspicious_us_ln_ever = TRUE if any per-nodule LN finding has
-- (size >= threshold, or category = "suspicious", or NLP-flagged cluster)
CREATE OR REPLACE VIEW main.canonical_us_patient_master_VIEW_v2 AS
SELECT ...,
  EXISTS (
    SELECT 1 FROM main.canonical_us_thyroid_gland_v2 g
    WHERE g.research_id = p.research_id
      AND (g.ln_short_axis_cm >= 1.0 OR g.ln_acr_category = 'suspicious'
           OR g.ln_loss_of_hilar_architecture = TRUE)
  ) AS any_suspicious_us_ln_ever
FROM ... p;
```

### 3e. Registry signoff
```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_262', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
  'Imaging date cleanup: 2 YY-typos fixed (rids 12048, 10511); 2,050 NULL exam_dates dispositioned per Logan ratification. Rebuilt any_suspicious_us_ln_ever flag from canonical_us_thyroid_gland_v2 (was 8 fires, now N).');
```

## §4 — Verify

```sql
-- Post-fix date sanity
SELECT
  COUNT(*) AS n_total,
  COUNT_IF(exam_date IS NULL) AS n_null,
  COUNT_IF(EXTRACT(YEAR FROM exam_date) < 1990) AS n_pre1990,
  COUNT_IF(EXTRACT(YEAR FROM exam_date) > 2030) AS n_post2030
FROM main.raw_imaging_12_slots_v1;
-- Expect: n_pre1990 = 0, n_post2030 = 0; n_null = 2,050 minus whatever §3d recovered

-- Post-fix suspicious-LN
SELECT
  COUNT_IF(any_suspicious_us_ln_ever) AS n_suspicious,
  COUNT_IF(has_us_ln_findings_ever) AS n_any_ln,
  COUNT(*) AS n_with_us
FROM main.canonical_us_patient_master_VIEW_v2;
-- Expect: n_suspicious now in the hundreds, not 8
```

## §5 — Snowflake-side re-verify
```bash
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/01_export_md_to_parquet.py
# Add canonical_us_patient_master_VIEW_v2 + raw_imaging_12_slots_v1 to TABLES if not already
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/02_load_to_snowflake.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/04_build_flat_views.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/16_prompt10_imaging.py  # if exists
```

## §6 — Carry-forwards
- CF-mig260e-IMAGING-12SLOTS-DATE-QUALITY → CLOSED on §3b apply
- CF-mig260g-US-LN-SUSPICIOUS-FLAG-UNDERFIRE → CLOSED on §3d apply
- CF-mig262-NULL-DATE-RECOVERY (open if §3c chooses NULL-stays-NULL)

## §7 — Surgical git add
```
qc_framework_v1/migrations/262_imaging_date_cleanup_20260501.sql
scripts/output/mig_262_*.md
scripts/output/mig_262_pre_snapshot_log.txt
```
