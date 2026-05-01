# Cursor Composer Dispatch — mig_264b: Apply obvious Bethesda-2 fixes from mig_264 read-only audit

**Generated:** 2026-05-02 by Cowork (post mig_264 read-only audit at 4aa7940).
**Lane:** mig_264b — mig_264 was a decision pass; this is the apply step. Three obvious sub-cohorts from the 385 Bethesda-2 + IS_MALIGNANT patients can be flipped without further adjudication. Other patterns require Logan-by-Logan review.
**Recommended agent:** **Cursor Composer** — mechanical apply on 3 well-defined sub-cohorts; the gray zone (~340 PTC patients with surgical-linkage Bethesda 2) defers to mig_264c.
**Estimated runtime:** 30–45 min
**Triggered by:** mig_264 read-only audit (`scripts/output/mig_264_bethesda2_audit_latest.md`).
**Severity:** MED. Reduces false-negative count from 385 → ~340 (cleaner Bethesda 2 cohort statistic for M025/M027 manuscripts).
**Closes carry-forward:** CF-mig264-OBVIOUS-FIXES.

---

## §0 — First message to paste into Cursor Composer

> mig_264b dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_264B_BETHESDA2_OBVIOUS_FIX_20260502.md` end-to-end. Three sub-cohorts get fixed mechanically; the rest defer to mig_264c. Pre-snapshot to `"Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig264b_20260502`. Verify Bethesda-2 false-negative count drops from 385 to ~340 after apply.

---

## §1 — Sub-cohorts from mig_264 read-only audit

Total Bethesda-2 + IS_MALIGNANT cohort: **385 patients**.

### 1a. NIFTP (n=22) — recategorize as non-malignant
NIFTP (Non-Invasive Follicular Thyroid neoplasm with Papillary-like features) was reclassified in 2017 from a malignancy to a borderline / non-malignant tumor. Patients coded `IS_MALIGNANT=TRUE` with `histology_final='NIFTP'` reflect pre-2017 convention or downstream propagation lag. Per AJCC 8, NIFTP is excluded from staging.

### 1b. Follicular adenoma (n=2) — clear benign reclassification
Follicular adenoma is unambiguously benign. These 2 should never have been `IS_MALIGNANT=TRUE`.

### 1c. Negative FNA-to-surgery days (n=19) — postop FNA mismapping
19 patients have `bethesda_final = 2` sourced from an FNA that occurred AFTER first_surgery_date (negative days_fna_to_surg). Postop surveillance FNA is a different clinical context than diagnostic FNA — `bethesda_final` should reference the preop diagnostic FNA, not surveillance.

### 1d. Defer to mig_264c (~342 patients)
- 286 PTC (true malignancy; Bethesda 2 was likely a false-negative cytology call OR sampled wrong nodule)
- 53 follicular carcinoma (FN cytology cannot reliably distinguish FA from FC)
- 6 MTC (FN: medullary calcitonin staining wasn't done)
- 6 FTUMP, 4 metastatic PTC, 2 PDTC, 1 anaplastic, 1 metastatic FTC, 1 high-grade DTC
- Cohort enrichment + true cytology limitation — manuscript footnote rather than DML

---

## §2 — Pre-task probes

```sql
-- 2a. NIFTP candidates
SELECT research_id, histology_final, is_malignant, bethesda_final
FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND is_malignant = TRUE AND histology_final = 'NIFTP';
-- Expected: 22 rows

-- 2b. Follicular adenoma candidates
SELECT research_id, histology_final, is_malignant, bethesda_final
FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND is_malignant = TRUE AND histology_final ILIKE '%follicular adenoma%';
-- Expected: 2 rows

-- 2c. Negative-FNA-day candidates
WITH bethesda2_malig AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE bethesda_final = 2 AND is_malignant = TRUE
),
fna_to_surg AS (
  SELECT b.research_id,
    MIN(DATEDIFF('day', f.fna_date, cpm.first_surgery_date)) AS days_fna_to_surg
  FROM bethesda2_malig b
  JOIN main.canonical_patient_master cpm USING (research_id)
  LEFT JOIN main.canonical_fna_events_v1 f USING (research_id)
  GROUP BY b.research_id
)
SELECT research_id, days_fna_to_surg
FROM fna_to_surg
WHERE days_fna_to_surg < 0;
-- Expected: 19 rows
```

## §3 — Apply

### 3a. Pre-snapshot
```sql
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig264b_20260502 AS
SELECT research_id, bethesda_final, bethesda_final_name, is_malignant, histology_final
FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND is_malignant = TRUE
  AND (histology_final IN ('NIFTP', 'follicular adenoma')
       OR research_id IN (<19 RIDs from probe 2c>));
```

### 3b. NIFTP fix — set is_malignant = FALSE
```sql
UPDATE main.canonical_patient_master
SET is_malignant = FALSE
WHERE bethesda_final = 2 AND is_malignant = TRUE AND histology_final = 'NIFTP';
-- Affects: 22 rows
```

### 3c. Follicular adenoma fix — set is_malignant = FALSE
```sql
UPDATE main.canonical_patient_master
SET is_malignant = FALSE
WHERE bethesda_final = 2 AND is_malignant = TRUE AND histology_final ILIKE '%follicular adenoma%';
-- Affects: 2 rows
```

### 3d. Negative-FNA-day fix — switch bethesda_final to preop FNA value
```sql
-- For each of the 19 RIDs, find the most recent preop FNA's bethesda value
-- and use that instead. If no preop FNA exists, set to NULL.
UPDATE main.canonical_patient_master cpm
SET bethesda_final = (
  SELECT MAX(f.bethesda_value)
  FROM main.canonical_fna_events_v1 f
  WHERE f.research_id = cpm.research_id
    AND f.fna_date < cpm.first_surgery_date
)
WHERE cpm.research_id IN (<19 RIDs from probe 2c>)
  AND cpm.bethesda_final = 2;
```

### 3e. Registry signoff
```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_264b', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
  'Bethesda-2 obvious-fix subset: 22 NIFTP + 2 follicular adenoma → is_malignant=FALSE; 19 negative-FNA-day patients re-pointed to preop FNA bethesda value. Reduces 385 false-negative count to ~342 (manuscript-side caveat for the residual).');
```

## §4 — Verify

```sql
-- A. NIFTP cleared
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND is_malignant = TRUE AND histology_final = 'NIFTP';
-- Expect: 0

-- B. Follicular adenoma cleared
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND is_malignant = TRUE AND histology_final ILIKE '%follicular adenoma%';
-- Expect: 0

-- C. Total Bethesda-2 + malig
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND is_malignant = TRUE;
-- Expect: ~342 (= 385 - 22 - 2 - 19)

-- D. ROM recompute
SELECT bethesda_final, COUNT(*) AS n,
       COUNT_IF(is_malignant) AS malig,
       ROUND(100.0 * COUNT_IF(is_malignant) / COUNT(*), 1) AS rom_pct
FROM main.canonical_patient_master
WHERE bethesda_final = 2 GROUP BY 1;
-- Expect: rom_pct drops from 18.9% to ~16.8%
```

## §5 — Snowflake re-verify

```bash
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/01_export_md_to_parquet.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/02_load_to_snowflake.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/04_build_flat_views.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/13_prompt7_tirads_bethesda.py
# Bethesda 2 ROM should drop from 18.9% to ~16.8%; n_malig from 385 to ~342
```

## §6 — Carry-forwards
- CF-mig264-OBVIOUS-FIXES → CLOSED on apply
- CF-mig264c-PTC-CYTOLOGY-FN-RESIDUAL → OPEN — 286 PTC + 53 FTC residual; manuscript footnote not DML
- CF-mig264-MANUSCRIPT-FOOTNOTE → OPEN until M025/M027 add the methods caveat

## §7 — Surgical git add
```
qc_framework_v1/migrations/264b_bethesda2_obvious_fix_20260502.sql
scripts/output/mig_264b_apply_log.txt
scripts/output/mig_264b_pre_snapshot_log.txt
```
