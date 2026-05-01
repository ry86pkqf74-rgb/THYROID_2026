# Cursor Composer Dispatch — mig_261: Path-synoptics CAP-template label normalization (focality + LVI + ETE + surg_date type)

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex round-6).
**Lane:** mig_261 — `path_synoptics` per-tumor CAP-template categorical fields have case/whitespace/typo drift across ≥6 fields. The drift compounds when filters use exact-match against the most-common spelling. Plus `surg_date` is TIMESTAMP with always-zero time component — violates Logan-ratified clinical-dates-calendar-only rule.
**Recommended agent:** **Cursor Composer** — mechanical normalization. CTC-equivalence verification pattern applies.
**Estimated runtime:** 60–90 min
**Triggered by:** Round 6 CF-mig262b/c/d/e (Prompt 12 synoptic).
**Severity:** MED. Per-tumor analyses going through `path_synoptics.tumor_*_*` directly (not the canonical) see split categories. Manuscripts already using canonical_path_malignant_events_v1 are unaffected.
**Closes carry-forwards:** CF-mig262b-CAP-LABEL-DRIFT-FOCALITY, CF-mig262c-CAP-LABEL-DRIFT-LVI, CF-mig262d-CAP-LABEL-DRIFT-ETE, CF-mig262e-PATH-SYNOPTICS-SURG_DATE-TIMESTAMP.

---

## §0 — First message to paste into Cursor Composer

> mig_261 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_261_PATH_SYNOPTICS_LABEL_NORM_20260501.md` end-to-end. Two parallel fixes on `main.path_synoptics`: (1) normalize ≥6 categorical CAP-template fields by `LOWER(TRIM())` + typo-map; (2) retype `surg_date` from TIMESTAMP to DATE. Pre-snapshot to `"Thyroid 2026 UPdated".archive_pub_v1_0.path_synoptics_pre_mig261_20260501`. Verify by GROUP BY on each normalized field — should collapse drift cohorts (e.g. unifocal+Unifocal+unifocal* → 2,583 rows).

---

## §1 — Why this lane exists

`path_synoptics` is the structured CAP-template substrate behind canonical_path_malignant + canonical_invasion. 11,688 rows × 582 cols, 1.08 rows/patient avg. The structured per-field columns are populated for CAP-conforming reports.

Per-field drift documented in round-6 Prompt 12:

### tumor_focality (≥7 variants)
- unifocal: 2,534
- multifocal: 1,315
- **Multifocal: 89** (case)
- **Unifocal: 46** (case)
- **multifocal\n: 3** (newline)
- **unifocal*: 2** (asterisk)
- **multifocal␣: 1** (trailing whitespace)
- **unifocal␣: 1** (trailing whitespace)
- + free-text fallbacks (1 each)

### tumor_1_lymphatic_invasion (≥17 variants, 6 typos)
- x: 2,678 / present: 702 / extensive: 54 / indeterminate: 50 / c/a: 9 / focal: 7
- **preesent: 3 / indeeterminate: 2 / extensivre: 2 / extensiver: 1 / indeterminent: 1 / indetermiante: 1**

### tumor_1_extrathyroidal_extension (≥20 variants)
- x: 3,382 / present: 252 / minimal: 174 / microscopic: 65 / c/a: 29 / extensive: 24 / yes: 19
- **Yes: 1 / Yes;: 7 / Extensive: 1 / extesive: 2** (case + typo)

### surg_date (type-flip)
- TIMESTAMP with always-zero time component. Per `feedback_clinical_dates_calendar_only.md`: clinical event dates MUST be DATE, never TIMESTAMP.

## §2 — Pre-task probes

```sql
-- 1. Confirm field-by-field drift (current state)
SELECT 'focality' AS field, tumor_focality AS value, COUNT(*) AS n
FROM main.path_synoptics WHERE tumor_focality IS NOT NULL
GROUP BY 2 ORDER BY n DESC LIMIT 25;

SELECT 'tumor_1_lvi' AS field, tumor_1_lymphatic_invasion AS value, COUNT(*) AS n
FROM main.path_synoptics WHERE tumor_1_lymphatic_invasion IS NOT NULL
GROUP BY 2 ORDER BY n DESC LIMIT 25;

SELECT 'tumor_1_ete' AS field, tumor_1_extrathyroidal_extension AS value, COUNT(*) AS n
FROM main.path_synoptics WHERE tumor_1_extrathyroidal_extension IS NOT NULL
GROUP BY 2 ORDER BY n DESC LIMIT 30;

-- 2. surg_date sanity (every value should be at-midnight already)
SELECT COUNT(*) AS n_total,
       COUNT_IF(EXTRACT(HOUR FROM surg_date) = 0 AND EXTRACT(MINUTE FROM surg_date) = 0
                AND EXTRACT(SECOND FROM surg_date) = 0) AS n_at_midnight,
       COUNT_IF(EXTRACT(HOUR FROM surg_date) <> 0 OR EXTRACT(MINUTE FROM surg_date) <> 0
                OR EXTRACT(SECOND FROM surg_date) <> 0) AS n_with_time
FROM main.path_synoptics WHERE surg_date IS NOT NULL;
-- Expected: n_with_time = 0 → safe to retype DATE

-- 3. Dependent VIEWs on path_synoptics
SELECT view_name FROM information_schema.views
WHERE view_definition ILIKE '%path_synoptics%';
```

## §3 — Apply

### 3a. Pre-snapshot
```sql
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.path_synoptics_pre_mig261_20260501 AS
SELECT research_id, surg_date, tumor_focality,
       tumor_1_lymphatic_invasion, tumor_1_extrathyroidal_extension,
       tumor_1_histologic_type
FROM main.path_synoptics;
```

### 3b. Categorical normalization (in-place UPDATEs)

```sql
-- Focality
UPDATE main.path_synoptics
SET tumor_focality = LOWER(TRIM(REPLACE(tumor_focality, CHR(10), '')))
WHERE tumor_focality IS NOT NULL;

-- LVI typo-map
UPDATE main.path_synoptics
SET tumor_1_lymphatic_invasion = CASE LOWER(TRIM(tumor_1_lymphatic_invasion))
  WHEN 'preesent' THEN 'present'
  WHEN 'indeeterminate' THEN 'indeterminate'
  WHEN 'indeterminent' THEN 'indeterminate'
  WHEN 'indetermiante' THEN 'indeterminate'
  WHEN 'extensivre' THEN 'extensive'
  WHEN 'extensiver' THEN 'extensive'
  ELSE LOWER(TRIM(tumor_1_lymphatic_invasion))
END
WHERE tumor_1_lymphatic_invasion IS NOT NULL;

-- ETE typo + case
UPDATE main.path_synoptics
SET tumor_1_extrathyroidal_extension = CASE LOWER(TRIM(REPLACE(tumor_1_extrathyroidal_extension, ';', '')))
  WHEN 'extesive' THEN 'extensive'
  ELSE LOWER(TRIM(REPLACE(tumor_1_extrathyroidal_extension, ';', '')))
END
WHERE tumor_1_extrathyroidal_extension IS NOT NULL;

-- Histologic type case+whitespace
UPDATE main.path_synoptics
SET tumor_1_histologic_type = LOWER(TRIM(tumor_1_histologic_type))
WHERE tumor_1_histologic_type IS NOT NULL;
```

### 3c. surg_date type retype
```sql
ALTER TABLE main.path_synoptics ALTER COLUMN surg_date SET DATA TYPE DATE
USING CAST(surg_date AS DATE);

-- Refresh dependent VIEWs that need recompile after type-change
-- (per feedback_alter_view_dependents.md, ALTER VIEW RENAME doesn't propagate types;
-- but ALTER COLUMN type-change generally does. Verify with SELECT * FROM <view> LIMIT 1.)
```

### 3d. Repeat for tumor_2 through tumor_5 fields (multi-tumor cases)
Apply the same normalization to `tumor_{2,3,4,5}_lymphatic_invasion`, `tumor_{2..5}_extrathyroidal_extension`, `tumor_{2..5}_histologic_type`.

### 3e. Registry signoff
```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_261', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
  'Normalized path_synoptics CAP-template labels: focality, tumor_1-5 LVI, ETE, histology types via LOWER+TRIM+typo-map. Retyped surg_date TIMESTAMP→DATE per clinical-dates-calendar-only rule.');
```

## §4 — Verify

```sql
-- A. focality should now have 2 main values + tiny tail
SELECT tumor_focality, COUNT(*) FROM main.path_synoptics
WHERE tumor_focality IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
-- Expect: unifocal ≈ 2,583 (was 2,534 + 46 + 2 + 1), multifocal ≈ 1,408 (was 1,315 + 89 + 3 + 1)

-- B. LVI typos cleared
SELECT COUNT(*) FROM main.path_synoptics
WHERE tumor_1_lymphatic_invasion IN ('preesent','indeeterminate','indeterminent','indetermiante','extensivre','extensiver');
-- Expect: 0

-- C. ETE
SELECT tumor_1_extrathyroidal_extension, COUNT(*) FROM main.path_synoptics
WHERE tumor_1_extrathyroidal_extension IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
-- Expect: x ≈ 3,382 / present ≈ 252 / yes ≈ 27 (was 19 + 7 Yes; + 1 Yes) / extensive ≈ 27 (was 24 + 1 + 2)

-- D. surg_date type
DESCRIBE main.path_synoptics surg_date;
-- Expect: DATE

-- E. CTC-equivalence vs pre-snapshot
SELECT COUNT(*) AS n_changed
FROM main.path_synoptics ps
JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.path_synoptics_pre_mig261_20260501 pre
  ON ps.research_id = pre.research_id
WHERE LOWER(TRIM(pre.tumor_focality)) IS DISTINCT FROM ps.tumor_focality;
-- Expect: 0 (the LOWER+TRIM of pre should equal post)
```

## §5 — Snowflake side
After mig_261 lands, re-export + Snowflake reload + re-run Prompt 12 — categorical drift counts should drop to clean tail.

## §6 — Carry-forwards
- CF-mig262b/c/d-CAP-LABEL-DRIFT → CLOSED
- CF-mig262e-PATH-SYNOPTICS-SURG_DATE-TIMESTAMP → CLOSED
- CF-mig261-MULTI-TUMOR-NORM (open if §3d skipped) — apply same normalization to tumor_2..tumor_5 cluster

## §7 — Surgical git add
```
qc_framework_v1/migrations/261_path_synoptics_label_norm_20260501.sql
scripts/output/mig_261_pre_snapshot_log.txt
scripts/output/mig_261_apply_log.txt
```
