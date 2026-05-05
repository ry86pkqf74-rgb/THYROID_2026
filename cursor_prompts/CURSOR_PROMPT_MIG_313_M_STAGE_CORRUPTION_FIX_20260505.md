# Cursor Prompt — mig_313: Fix `m_stage_ajcc8_resolved` corruption (CRITICAL)

**Agent:** Cursor Composer
**Estimated time:** 4–6 hours (probe + fix + audit + signoff)
**Date:** 2026-05-05
**Priority:** **P0 — blocks every AJCC-staging-dependent manuscript**

## The bug (Cowork-discovered 2026-05-05 during mig_309 v3 deploy)

`canonical_path_malignant_events_v1.m_stage_ajcc8_resolved` and its CPM rollup `canonical_patient_master.ajcc8_m_stage` show **45.19% M1 rate among malignant patients (1,816/4,019)**. Real-world DTC M1 prevalence is 1–3%.

By histology:

| Histology | N | N M1 | % M1 | Plausible? |
|---|---|---|---|---|
| PTC | 3,075 | 1,360 | **44.23%** | NO (expect 1–3%) |
| Follicular carcinoma | 486 | 281 | **57.82%** | NO (expect 5–10%) |
| Follicular adenoma | 3 | 3 | **100%** | IMPOSSIBLE (benign by definition) |
| MTC | 149 | 60 | 40.27% | borderline (MTC mets common) |
| Poorly differentiated | 37 | 26 | 70.27% | borderline plausible |
| Anaplastic | 22 | 9 | 40.91% | plausible |
| FTUMP | 34 | 0 | 0% | correct |

The PTC 44% M1 rate is the smoking gun. Likely root cause: AJCC resolver in mig_25X-26X era (pre-pub_v1_0) flipped the M-stage default — anyone with regional disease (N1) probably got assigned M1 instead of M0, OR the resolver mis-derived M from a different column.

## Cascade impact (must audit each)

| Manuscript | Impact | Audit needed |
|---|---|---|
| **M036 ATA RSS** (active) | 1,642 patients flagged `high:distant_metastasis` (40.9% of cohort) | Re-run `scripts/m036_ata_2025_rss.py` after fix; expected high drops 2,353 → ~600–900, low rises 23 → ~200–500 |
| **M025 v2** (active in another chat) | Stage-IV cohort claims overstated | Audit any Stage IV percentage in M025_v2_manuscript_DRAFT_v1_0.md |
| **M032 25-yr descriptive** | Stage migration trends inflated | Re-check Stage IV-by-era counts |
| **M044 ETE FINAL** (just shipped v5) | ETE × stage interactions distorted | Re-check Table 1 Stage IV row, any aOR involving ajcc8_stage_group |
| **M037 LN predictors** | LN-stage interaction muddled | Lower priority |
| **M044 published numbers** | All ajcc8_stage_group-derivative claims | High priority — the manuscript was just packaged |

## Fix strategy

### Step 1 — Probe ground truth in source data

The path notes corpus (mostly in `clinical_notes_long` note_type='OPNOTE' or path reports) should contain explicit M-stage statements. Run:

```sql
-- MotherDuck side: count explicit M-stage assertions in path notes
WITH path_notes AS (
  SELECT research_id, note_text
  FROM main.clinical_notes_long
  WHERE LOWER(note_type) IN ('opnote','hp','other_history','endocrine_fm')
)
SELECT
  COUNT(*) FILTER (WHERE LOWER(note_text) LIKE '%distant metastas%' OR LOWER(note_text) ILIKE '%pm1%' OR LOWER(note_text) ILIKE '% m1 %') AS n_with_explicit_m1,
  COUNT(*) FILTER (WHERE LOWER(note_text) ILIKE '% m0 %' OR LOWER(note_text) ILIKE 'pm0' OR LOWER(note_text) ILIKE 'cm0') AS n_with_explicit_m0,
  COUNT(*) FILTER (WHERE LOWER(note_text) LIKE '%no distant metastas%' OR LOWER(note_text) LIKE '%absence of distant%') AS n_with_no_distant
FROM path_notes;
```

If `n_with_explicit_m1 << 1816` (likely 50–200 by literature priors), the corruption is confirmed and we rebuild from scratch.

### Step 2 — Trace the resolver

Find where `m_stage_ajcc8_resolved` was first assigned. Check:

```bash
grep -rn "m_stage_ajcc8_resolved\|ajcc8_m_stage" qc_framework_v1/migrations/ scripts/ snowflake_trial/scripts/ 2>/dev/null | head -30
```

Look for the migration that originally wrote this column. Inspect its logic for default-flip bugs. Common patterns to look for:
- `CASE WHEN n_stage IN ('N1a','N1b') THEN 'M1' ELSE 'M0' END` (wrong — N≠M)
- `COALESCE(some_default, 'M1')` (wrong default)
- `path_t_stage LIKE 'T4%' THEN 'M1'` (wrong — T4 isn't M1)

### Step 3 — Rebuild via Cortex AI_EXTRACT (recommended)

Mirror the mig_298 NLP-augmented Option 2 pattern. Build `NLP_M_STAGE_RESOLVED_v1` in SF:

```sql
CREATE OR REPLACE TABLE THYROID_VALIDATION.PUBLIC.NLP_M_STAGE_RESOLVED_v1 AS
WITH path_notes AS (
  SELECT research_id, note_date, note_text
  FROM CLINICAL_NOTES_SEARCH_V1
  WHERE LOWER(note_type) IN ('opnote','hp','other_history','endocrine_fm')
    AND (LOWER(note_text) LIKE '%distant%' OR LOWER(note_text) LIKE '%metast%'
         OR LOWER(note_text) LIKE '%m0%' OR LOWER(note_text) LIKE '%m1%')
)
SELECT
  research_id,
  note_date,
  result:m_stage::VARCHAR AS extracted_m_stage,
  result:evidence::VARCHAR AS evidence_quote,
  result:confidence::VARCHAR AS confidence
FROM (
  SELECT research_id, note_date,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
      note_text,
      'Extract AJCC 8 M-stage for thyroid cancer staging:
       - m_stage: "M0" if no distant metastases at presentation/diagnosis,
                  "M1" only if explicit distant metastasis (lung/bone/brain) is documented at staging,
                  null if not addressed.
       - evidence: verbatim quote supporting the assignment.
       - confidence: high|medium|low.
       Return as JSON. Note: regional lymph node disease (N1) is NOT M1.
       Adenopathy/lymph nodes are N-staging, not M-staging.'
    ) AS result
  FROM path_notes
);
```

Then aggregate to one row per patient (any high-confidence M1 wins; default M0 if no high-conf M1).

### Step 4 — Rebuild canonical + CPM

```sql
-- MotherDuck side: replace the corrupted column
CREATE OR REPLACE VIEW main.canonical_path_malignant_events_v1 AS
SELECT cpe.* REPLACE (
  CASE
    WHEN nlp.extracted_m_stage = 'M1' AND nlp.confidence = 'high' THEN 'M1'
    ELSE 'M0'
  END AS m_stage_ajcc8_resolved
)
FROM main.canonical_path_malignant_events_v1_pre_mig313 cpe
LEFT JOIN manuscript_workspace.nlp_m_stage_resolved_v1 nlp USING(research_id);
```

Snapshot the existing column to `archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig313` first.

Then refresh CPM via the existing CPM rebuild SQL (mig_302-style).

### Step 5 — Validation gates

```sql
-- Expected after fix:
SELECT histology_final, COUNT(*) AS n,
  SUM(CASE WHEN ajcc8_m_stage='M1' THEN 1 ELSE 0 END) AS n_m1,
  100.0 * SUM(CASE WHEN ajcc8_m_stage='M1' THEN 1 ELSE 0 END) / COUNT(*) AS pct_m1
FROM main.canonical_patient_master
WHERE is_malignant
GROUP BY 1
ORDER BY 2 DESC;
```

Acceptance criteria:
- PTC: M1 rate 1–3% (currently 44.23%)
- Follicular carcinoma: M1 rate 5–10% (currently 57.82%)
- Follicular adenoma: M1 rate 0% (currently 100%)
- MTC: M1 rate 5–25% (currently 40.27% — may stay borderline)
- Anaplastic / poorly differentiated / high-grade DTC: 30–70% acceptable

### Step 6 — Cascade refresh

After CPM is fixed, refresh all cohort views that surface AJCC stage. Run:

```sql
-- MotherDuck
-- 1. Refresh ajcc8_stage_group on CPM (M1 → Stage IV by AJCC 8 thyroid rules)
-- 2. CREATE OR REPLACE all m025/m032/m037/m044/m036 cohort_* views
-- 3. Re-export the 6 cohort flats to SF (use load_*_to_sf.py pattern from mig_311/mig_312)
-- 4. CALL VALIDATE_ALL_COHORTS_V3() — drift checks should still pass since row counts unchanged
```

Then re-run downstream:
- `scripts/m036_ata_2025_rss.py` (regenerate v3 with corrected M)
- M044 manuscript audit: re-count Stage IV rows in Table 1
- M025 chat hand-off: tell Logan his manuscript may need a Stage IV recount
- M032: re-run era-by-stage tables

### Step 7 — Signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_313', CURRENT_TIMESTAMP, 'cursor_composer_mig313',
  'mig_313: M-stage corruption fix. Pre-fix CPM ajcc8_m_stage M1 rate=45.19% (1,816/4,019 malignant; PTC 44.23%, FC 57.82%, follicular adenoma 100%). Root cause: <ROOT CAUSE FROM STEP 2>. Fix: NLP_M_STAGE_RESOLVED_v1 via Cortex EXTRACT_ANSWER over path notes (n=<X>); patient-rollup applied to canonical_path_malignant_events_v1.m_stage_ajcc8_resolved with M0 default. Pre-snapshot to archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig313. Post-fix CPM M1 rate=<Y>% (PTC <Z>%). Cascade: ajcc8_stage_group refreshed; 6 cohort flats re-exported to SF; m036 ATA RSS regenerated as v3 (high <A> / int <B> / low <C> / unc <D>); M044 Table 1 Stage IV recount delta=<delta>; M032 era-stage table refreshed. Closes CF-MSTAGE-CORRUPTION.');
```

## Carry-forwards

- `CF-MSTAGE-CORRUPTION` — opened by Cowork 2026-05-05; this prompt closes
- Possible new CF if other AJCC columns also have similar corruption (T-stage, N-stage probably OK based on m036 v2 distribution but worth a sanity check)

## Important notes

- Logan's M025 v2 manuscript draft (in another chat) cites Stage IV percentages — flag him after fix lands so he can recount
- M044 v5 just shipped; if Stage IV counts change >5%, the manuscript needs a v6 patch
- Don't auto-push commits without first reviewing Stage IV deltas with Logan
