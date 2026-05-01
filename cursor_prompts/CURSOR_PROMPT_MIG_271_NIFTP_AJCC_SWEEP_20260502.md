# Cursor Composer Dispatch — mig_271: NIFTP + AJCC stage sweep (post mig_264b)

**Generated:** 2026-05-02 by Cowork.
**Lane:** mig_271 — After mig_264b reclassifies 22 NIFTP + 2 follicular adenoma as `IS_MALIGNANT=FALSE`, audit downstream consequences:
1. Are NIFTP/FA patients with `ajcc8_stage_group` populated? (Should be NULL per AJCC 8 — NIFTP excluded from staging.)
2. Are they in M037/M044/M025 cohort views? (Should be excluded per `IS_MALIGNANT=TRUE` filter.)
3. Manuscript text references "10,871 patient cohort, 38.1% malignancy rate" → update to ~37.9% in M032/M044 revisions.
**Recommended agent:** **Cursor Composer** — mechanical sweep.
**Estimated runtime:** 30 min
**Triggered by:** mig_264b landing.
**Severity:** LOW.
**Closes:** CF-mig264b-DOWNSTREAM-CASCADE.

---

## §0 — First message to paste into Cursor Composer

> mig_271 dispatch. After mig_264b, sweep:
> 1. NULL out ajcc8_stage_group for the 24 patients now `IS_MALIGNANT=FALSE`
> 2. Confirm cohort views auto-filter them out (no manual fix needed if `WHERE is_malignant=TRUE`)
> 3. Find/replace "38.1%" → "37.9%" in manuscript drafts that cite cohort malignancy rate

---

## §1 — Probes

```sql
-- 1a. NIFTP/FA patients with stage_group populated (should be 0 post-fix)
SELECT research_id, histology_final, ajcc8_stage_group
FROM main.canonical_patient_master
WHERE is_malignant = FALSE
  AND histology_final IN ('NIFTP', 'follicular adenoma', 'atypical follicular adenoma')
  AND ajcc8_stage_group IS NOT NULL;

-- 1b. Confirm cohort views still exclude them
SELECT COUNT(*) FROM main.cohort_m037_ln_predictors
WHERE histology_final IN ('NIFTP', 'follicular adenoma');
-- Expected: 0

-- 1c. Cohort malignancy rate post-mig_264b
SELECT
  COUNT(*) AS n_total,
  COUNT_IF(is_malignant) AS n_malig,
  ROUND(100.0 * COUNT_IF(is_malignant) / COUNT(*), 1) AS malig_pct
FROM main.canonical_patient_master;
-- Expected: 10,871 total; ~4,113 malignant; 37.8%
```

## §2 — Apply

```sql
-- 2a. Pre-snapshot
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig271_20260502 AS
SELECT research_id, ajcc8_stage_group, histology_final
FROM main.canonical_patient_master
WHERE is_malignant = FALSE AND ajcc8_stage_group IS NOT NULL
  AND histology_final IN ('NIFTP', 'follicular adenoma', 'atypical follicular adenoma');

-- 2b. NULL stage_group for non-malignant
UPDATE main.canonical_patient_master
SET ajcc8_stage_group = NULL,
    ajcc8_t_stage = NULL,
    ajcc8_n_stage = NULL,
    ajcc8_m_stage = NULL
WHERE is_malignant = FALSE AND ajcc8_stage_group IS NOT NULL
  AND histology_final IN ('NIFTP', 'follicular adenoma', 'atypical follicular adenoma');

-- 2c. Signoff
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_271', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
  'Post-mig_264b cascade: NULLed stage_group/T/N/M for NIFTP/FA patients now IS_MALIGNANT=FALSE.');
```

## §3 — Manuscript text sweep

```bash
# Find references to old cohort malignancy rate
grep -rE "38\\.1%|38\\.[0-2]%" M032_*.md M037_*.md M044_*.md M025_*.md 2>/dev/null

# Replace with new rate (verify exact value first via Probe 1c)
# E.g.: sed -i 's/38\.1%/37\.8%/g' M032_*.md (or use Cursor's find/replace UI)
```

## §4 — Snowflake re-verify

```bash
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/01_export_md_to_parquet.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/02_load_to_snowflake.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/04_build_flat_views.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/13_prompt7_tirads_bethesda.py
# Bethesda 2 ROM = ~16.8% (post-mig_264b); n_malig in cohort = ~4,113
```

## §5 — Surgical git add
```
qc_framework_v1/migrations/271_niftp_ajcc_sweep_20260502.sql
scripts/output/mig_271_apply_log.txt
M032_*.md / M037_*.md / M044_*.md / M025_*.md  (text edits)
```
