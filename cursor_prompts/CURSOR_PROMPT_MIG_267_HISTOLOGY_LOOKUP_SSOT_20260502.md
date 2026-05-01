# Cursor Composer Dispatch — mig_267: `canonical_histology_lookup_v1` SSOT (replace ad-hoc CASE WHEN ILIKE)

**Generated:** 2026-05-02 by Cowork.
**Lane:** mig_267 — Build a single-source-of-truth histology mapping table that maps every distinct `histology_final` string to a normalized category {DTC, FTC, MTC, ATC, PDTC, Hurthle, NIFTP, Other-malignant, Benign}. Source = AI_CLASSIFY (Snowflake Cortex) with Logan ratification on edge cases. Replaces the dozens of `CASE WHEN histology_final ILIKE 'PTC%'` patterns scattered across cohort views, M037 Table 1, M037 Table 2, M044 Table 1, snowflake_trial scripts, and downstream manuscripts.
**Recommended agent:** **Cursor Chat (Claude Sonnet 4 / GPT-5)** for AI_CLASSIFY review + Logan ratification → **Cursor Composer** for the lookup table creation + downstream re-point.
**Estimated runtime:** 2–3 hrs (decision pass on edge cases + apply + cascade)
**Triggered by:** Round 1 Prompt 1 finding (4 "metastatic PTC*" variants AI_CLASSIFY couldn't disambiguate); ad-hoc CASE patterns in 8+ files repeating the same logic with subtle drift.
**Severity:** MED. Reduces drift risk; SSOT for any future histology-stratified analysis.
**Closes carry-forwards:** CF-mig260-OTHER-HISTOLOGY-TRIAGE (residual gray cases), CF-mig263-HIST-MAPPING-DRIFT.

---

## §0 — First message to paste into Cursor Chat (decision pass)

> mig_267 decision pass. Read this prompt end-to-end. Build a `canonical_histology_lookup_v1` table mapping every distinct `histology_final` string in `canonical_patient_master` to a normalized category. Source the initial mapping from Snowflake AI_CLASSIFY; surface edge cases (e.g., "metastatic PTC", "differentiated high grade thyroid carcinoma", "FTUMP", "NUT carcinoma", anaplastic with neuroendocrine differentiation) for Logan to ratify before the SSOT table is built. Then re-point downstream code (cohort views, Snowflake scripts, manuscript Tables) to JOIN the lookup instead of inline CASE.

---

## §1 — Why this lane exists

Across the codebase, the same histology mapping logic is repeated with subtle variations:

```
# In snowflake_trial/scripts/09_m037_table1.py:
CASE WHEN HISTOLOGY_FINAL ILIKE 'PTC%' THEN 'PTC'
     WHEN HISTOLOGY_FINAL ILIKE '%follicular%' THEN 'FTC'
     WHEN HISTOLOGY_FINAL ILIKE 'MTC%' THEN 'MTC'
     ...

# In snowflake_trial/scripts/19_m044_table1.py:
CASE WHEN HISTOLOGY_FINAL ILIKE 'PTC%' THEN 'PTC'
     WHEN HISTOLOGY_FINAL ILIKE '%follicular%' THEN 'FTC'
     WHEN HISTOLOGY_FINAL ILIKE 'MTC%' OR HISTOLOGY_FINAL ILIKE '%medullary%' THEN 'MTC'
     ...
```

The scripts have already drifted on the MTC mapping ("MTC%" alone vs "MTC% OR %medullary%"). M037 Table 1 doesn't catch "metastatic medullary thyroid carcinoma" while M044 Table 1 does — same patient gets different histology_group depending on which manuscript queries them.

Round 1 Prompt 1 found 4 unique "metastatic PTC" variants AI_CLASSIFY returned `?` for. Round 6 Prompt 12 found 8 case/whitespace duplicates among top-20 histology values.

Solution: a SSOT lookup table where every distinct string is mapped exactly once.

## §2 — Pre-task probes (Snowflake)

```sql
-- 2a. All distinct histology strings in malignant cohort
SELECT histology_final, COUNT(*) AS n_patients
FROM main.canonical_patient_master
WHERE histology_final IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;
-- Expect: 50-100 distinct strings, with long tail

-- 2b. AI_CLASSIFY against the canonical category set
WITH distinct_hist AS (
  SELECT DISTINCT histology_final FROM main.canonical_patient_master
  WHERE histology_final IS NOT NULL
)
SELECT histology_final,
  AI_CLASSIFY(
    histology_final,
    ARRAY_CONSTRUCT(
      'PTC',                                      -- Papillary classic
      'PTC_follicular_variant',                   -- Follicular variant PTC
      'PTC_tall_cell',                            -- Tall cell PTC
      'PTC_other_variant',                        -- Diffuse sclerosing, Hobnail, etc
      'FTC',                                      -- Follicular thyroid carcinoma
      'MTC',                                      -- Medullary thyroid carcinoma
      'ATC',                                      -- Anaplastic
      'PDTC',                                     -- Poorly differentiated
      'Hurthle',                                  -- Oncocytic / Hurthle cell carcinoma
      'NIFTP',                                    -- Non-invasive follicular tumor
      'FTUMP',                                    -- Follicular tumor of uncertain malignant potential
      'Benign',                                   -- Adenoma, hyperplasia, MNG
      'Other_malignant'                           -- NUT carcinoma, sarcoma, lymphoma, mixed
    )
  ) AS ai_classification
FROM distinct_hist;
```

## §3 — Decision pass output (surface to Logan)

Surface a 3-column table:

| histology_final string | AI suggestion | Logan ratification |
|---|---|---|
| PTC | PTC | ✓ |
| PTC follicular variant | PTC_follicular_variant | ✓ |
| metastatic PTC | PTC (with metastatic flag separately) | ratify |
| metastatic PTC classical | PTC (with metastatic flag) | ratify |
| metastatic PTC tall cell variant | PTC_tall_cell (with metastatic flag) | ratify |
| recurrent/metastatic PTC | PTC (with recurrent + metastatic flag) | ratify |
| follicular carcinoma | FTC | ✓ |
| metastatic follicular carcinoma | FTC (with metastatic flag) | ratify |
| poorly differentiated thyroid carcinoma | PDTC | ✓ |
| differentiated high grade thyroid carcinoma | PDTC (consensus 2022 reclassified) | ratify |
| anaplastic carcinoma | ATC | ✓ |
| NUT carcinoma | Other_malignant | ratify |
| poorly differentiated carcinoma with neuroendocrine differentiation | PDTC or Other? | ratify |
| MTC | MTC | ✓ |
| MTC/PTC mixed composite | Other_malignant or MTC? | ratify |
| Hurthle cell carcinoma | Hurthle | ✓ |
| NIFTP | NIFTP | ✓ |
| FTUMP | FTUMP | ✓ |
| follicular adenoma | Benign | ✓ |
| atypical follicular adenoma | Benign | ✓ |
| ... | ... | ratify |

Plus orthogonal flags Logan may want as separate columns:
- `is_metastatic` (TRUE if string contains "metastatic")
- `is_recurrent` (TRUE if string contains "recurrent")
- `has_specific_variant` (TRUE if a variant qualifier present)

## §4 — Apply

After Logan ratifies the mapping table:

```sql
-- 4a. Pre-snapshot (no CPM data change; just for reproducibility)
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.histology_distinct_pre_mig267_20260502 AS
SELECT DISTINCT histology_final, COUNT(*) AS n_patients
FROM main.canonical_patient_master
WHERE histology_final IS NOT NULL
GROUP BY 1;

-- 4b. Build the SSOT lookup table
CREATE OR REPLACE TABLE main.canonical_histology_lookup_v1 (
  histology_final_raw VARCHAR PRIMARY KEY,
  histology_normalized VARCHAR,
  histology_group VARCHAR,         -- DTC / FTC / MTC / ATC / PDTC / Hurthle / NIFTP / FTUMP / Benign / Other_malignant
  is_metastatic BOOLEAN,
  is_recurrent BOOLEAN,
  ratified_by VARCHAR,
  ratified_at TIMESTAMP,
  notes VARCHAR
);

-- Populate from ratified §3 table (concrete INSERTs after Logan signs off)
INSERT INTO main.canonical_histology_lookup_v1 VALUES
  ('PTC', 'Papillary classic', 'PTC', FALSE, FALSE, 'logan', CURRENT_TIMESTAMP, ''),
  ('metastatic PTC', 'Papillary classic', 'PTC', TRUE, FALSE, 'logan', CURRENT_TIMESTAMP, 'metastatic at presentation OR referral'),
  -- ... ~80 rows total ...
;

-- 4c. Registry signoff
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_267', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
  'Built canonical_histology_lookup_v1 SSOT mapping ~80 distinct histology strings to {DTC/FTC/MTC/ATC/PDTC/Hurthle/NIFTP/FTUMP/Benign/Other} categories with is_metastatic + is_recurrent orthogonal flags. Replaces ad-hoc CASE WHEN ILIKE patterns scattered across cohort views and manuscript scripts.');
```

## §5 — Downstream re-point (separate commit)

Update these scripts to `LEFT JOIN canonical_histology_lookup_v1 USING (histology_final)` instead of inline CASE:
- `snowflake_trial/scripts/08_cohort_views.py` — COHORT_M037_LN_PREDICTORS already uses inline CASE
- `snowflake_trial/scripts/09_m037_table1.py`
- `snowflake_trial/scripts/19_m044_table1.py`
- `snowflake_trial/scripts/21_m004_table1.py`
- `snowflake_trial/scripts/22_m037_table2_logreg.py`

Re-export to Snowflake to pull the new lookup table.

## §6 — Verify

```sql
-- A. Coverage
SELECT
  COUNT(DISTINCT cpm.histology_final) AS n_distinct_in_cpm,
  COUNT(DISTINCT lookup.histology_final_raw) AS n_in_lookup,
  COUNT(DISTINCT cpm.histology_final) - COUNT(DISTINCT lookup.histology_final_raw) AS n_uncovered
FROM main.canonical_patient_master cpm
LEFT JOIN main.canonical_histology_lookup_v1 lookup
  ON cpm.histology_final = lookup.histology_final_raw
WHERE cpm.histology_final IS NOT NULL;
-- Expect: n_uncovered = 0

-- B. Per-group counts using the SSOT
SELECT lookup.histology_group, COUNT(*) AS n_pts
FROM main.canonical_patient_master cpm
LEFT JOIN main.canonical_histology_lookup_v1 lookup
  ON cpm.histology_final = lookup.histology_final_raw
WHERE cpm.is_malignant = TRUE
GROUP BY 1 ORDER BY 2 DESC;
```

## §7 — Carry-forwards
- CF-mig260-OTHER-HISTOLOGY-TRIAGE → CLOSED
- CF-mig263-HIST-MAPPING-DRIFT → CLOSED
- CF-mig267-AI-CLASSIFY-COST → tracked separately (~$0.10 of trial credits for the one-shot classification)

## §8 — Surgical git add
```
qc_framework_v1/migrations/267_canonical_histology_lookup_v1_20260502.sql
scripts/output/mig_267_*.csv     (the AI_CLASSIFY raw output for archive)
scripts/output/mig_267_apply_log.txt
snowflake_trial/scripts/{08,09,19,21,22}_*.py    (re-point edits)
```
