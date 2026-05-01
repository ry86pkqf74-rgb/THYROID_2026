# Cursor Composer Dispatch — mig_259: Re-derive `ajcc8_stage_group` overlay (mig_266b → CPM) so all M1 cases are consistent

**Generated:** 2026-05-01 by Cowork (Snowflake post-mig_254 strategic review).
**Lane:** mig_259 — investigate whether the `mig_266b` patient-level AJCC overlay should be re-derived from the `mig_184` source so M1 patients get consistent stage_group derivation, or whether a thin coalesce-into-IVB rule should be added at the rollup boundary. Currently the source `mig_184` writes IVA/IVB/IVC for ATC/MTC, but the rollup at CPM only carries {I, II, III, IVB} — anything outside that set silently becomes NULL or II, requiring case-by-case bandaid migs (mig_254, mig_254b are the latest examples).
**Recommended agent:** **Cursor Chat (Claude Sonnet 4 / GPT-5)** for the architectural pass and per-manuscript impact analysis → **Cursor Composer** for the actual mig once a strategy is picked. Do NOT let Composer self-direct this — it touches the AJCC derivation backbone that mig_184/188/266b spent multiple lanes stabilizing.
**Estimated runtime:** 4–6 hrs (decision pass + dry-run + apply + view cascades + manuscript regression check)
**Triggered by:** mig_254 root cause analysis. The 40 II→IVB flips and rid 9600 NULL→IVB cleanup are both symptoms of the same upstream-vs-rollup label-set mismatch.
**Severity:** MEDIUM (no patients miscoded today, but every future M1 patient with non-DTC histology is at risk of repeating mig_254-style cleanup).
**Opens carry-forward:** none new; consolidates CF-mig254-MIG266B-OVERLAY-RE-DERIVE.
**Closes carry-forward:** CF-mig254-MIG266B-OVERLAY-RE-DERIVE on apply.

---

## §0 — First message to paste into Cursor Chat (architectural decision pass)

> mig_259 architectural decision pass. Read this prompt end-to-end. The question is structural, not a single SQL: should `mig_266b`'s patient-level AJCC overlay be re-derived from `mig_184` source labels (which include IVA/IVC), or should we add a thin `IVA→IVB`, `IVC→IVB` coalesce rule at the rollup boundary?
>
> Run §2 probes; surface the per-rule impact analysis to Logan in a single concise message. Specifically: (a) how many patients today have mig_184 source labels outside {I,II,III,IVB}? (b) which manuscripts (M032/M037/M038/M044) filter on stage_group and would change behavior under each option? (c) what's the dependent-VIEW cascade if we add IVA/IVC to the allowed set?
>
> Pick A/B/C with Logan, then move to Composer.

---

## §1 — Why this lane exists

`canonical_patient_master.ajcc8_stage_group` only carries **{I, II, III, IVB}** in the publication DB. The source-of-truth derivation in `qc_framework_v1/migrations/184_v2_r1_ajcc_derivation_ratified_20260430.sql` writes the full AJCC 8 set including:

```sql
WHEN stage_component = 'ATC' AND m8 = 'M1' THEN 'IVC'
WHEN stage_component = 'ATC' AND t8 = 'T4b' THEN 'IVB'
WHEN stage_component = 'ATC' THEN 'IVA'
WHEN stage_component = 'MTC' AND m8 = 'M1' THEN 'IVC'
```

The mig_266b overlay (`patient_level_ajcc_overlay_dominant_tumor_mig266b_family`, per `canonical_column_verification_registry_v1`) collapses to the smaller set when promoting to CPM. The collapse logic is implicit and lossy — anything outside {I, II, III, IVB} becomes NULL or stale II, depending on how it was reached.

**Evidence the collapse is the root cause:**
- mig_254 found 40 patients (29 MTC + 2 ATC + 9 PDTC) with M1 + Stage II, all of which should have been IVC per mig_184 but got II via the collapse.
- mig_254b found rid 9600 with M1 + NULL stage_group — same pattern, NULL instead of II.
- Going forward, every new M1 + (MTC/ATC/PDTC) patient added to the canonical will hit the same trap unless the overlay knows about IVA/IVC.

## §2 — Pre-task probes

```sql
-- Probe 1: enumerate every distinct (mig_184 derived stage, current CPM stage) pair
-- Need to identify what mig_184 writes vs what CPM carries.
-- This requires either rerunning mig_184 logic against current data OR querying its persisted output if it exists.
SELECT DISTINCT
  source_stage_label,    -- from mig_184 derivation
  cpm_stage_group,       -- from canonical_patient_master
  COUNT(*) AS n
FROM (
  -- placeholder: locate the mig_184 output table or rerun the CASE inline
  -- check qc_framework_v1.<table> or main.<derived_table>
  ...
) GROUP BY 1, 2 ORDER BY 1, 2;

-- Probe 2: which manuscripts filter on stage_group?
SELECT DISTINCT cohort_view_name, definition
FROM main.manuscript_dive_map_v1
WHERE LOWER(definition) LIKE '%stage%' OR LOWER(definition) LIKE '%ivb%';

-- Probe 3: dependent VIEWs on canonical_patient_master.ajcc8_stage_group
-- (Snowflake doesn't have an exact analog; in MD use information_schema.view_column_usage)
SELECT view_name, dependent_column
FROM information_schema.view_column_usage
WHERE column_name = 'ajcc8_stage_group';

-- Probe 4: how many M1 patients exist by histology category, and what are their CURRENT vs WOULD-BE stage_groups?
SELECT
  CASE WHEN histology_final ILIKE 'MTC%' OR histology_final ILIKE '%medullary%' THEN 'MTC'
       WHEN histology_final ILIKE '%anaplastic%' THEN 'ATC'
       WHEN histology_final ILIKE '%poorly differentiated%' THEN 'PDTC'
       WHEN histology_final ILIKE 'PTC%' OR histology_final ILIKE '%follicular%' THEN 'DTC'
       ELSE 'OTHER' END AS hist,
  ajcc8_stage_group AS current_label,
  COUNT(*) AS n
FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_m_stage = 'M1'
GROUP BY 1, 2 ORDER BY 1, 2;
```

## §3 — Three candidate strategies

### Option A: Add IVA + IVC to the allowed CPM label set (AJCC-faithful)

- Re-derive `ajcc8_stage_group` from `mig_184` source for all malignant patients
- Update mig_266b overlay logic to preserve IVA/IVC instead of collapsing
- Cascade: every dependent VIEW filtering on stage_group needs to handle the extended set
- Manuscript impact: M032/M037/M038/M044 cohorts that currently use `WHERE stage_group = 'IVB'` would need to be expanded to `WHERE stage_group IN ('IVA','IVB','IVC')` or split into A/B/C strata

**Pro:** AJCC 8 published convention; reviewer-defensible
**Con:** large blast radius (≥10 dependent views, all manuscripts)

### Option B: Coalesce IVA/IVC → IVB at the rollup boundary (current de-facto)

- Document `mig_266b` formally with the explicit rule: `COALESCE(stage_group, NULL); WHEN IN ('IVA','IVC') THEN 'IVB'`
- Re-run the overlay so existing NULL/II miscodings get IVB
- Manuscript impact: zero — all dependent views and manuscripts continue to work

**Pro:** minimal blast radius; preserves the existing manuscript pipeline
**Con:** IVA vs IVB vs IVC granularity is permanently lost at CPM level (still queryable via the source `mig_184` table if needed for a specific paper)

### Option C: Dual-column — keep CPM `ajcc8_stage_group` collapsed, add `ajcc8_stage_group_full` with IVA/IVC

- Add a new column to CPM that carries the full AJCC 8 label
- Existing column stays at {I,II,III,IVB}; new column carries {I,II,III,IVA,IVB,IVC}
- Manuscripts pick whichever column they want

**Pro:** preserves both views; reviewer-defensible AND existing pipelines unchanged
**Con:** schema bloat; downstream queries must declare which column they trust

## §4 — Apply (template; concrete SQL after Logan picks A/B/C)

### If Option A:
```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig259_20260501 AS
SELECT research_id, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group, histology_final, age_at_surgery
FROM main.canonical_patient_master WHERE is_malignant = TRUE;

-- Re-derive from mig_184 source (or rerun the CASE inline against current canonical events)
UPDATE main.canonical_patient_master cpm
SET ajcc8_stage_group = src.derived_label
FROM mig_184_source_table src
WHERE cpm.research_id = src.research_id
  AND src.derived_label IS NOT NULL;

-- Cascade: regenerate dependent VIEWs that may filter stage_group
-- (each dependent VIEW needs its own ALTER OR REPLACE)
```

### If Option B (recommended for manuscript pipeline stability):
```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig259_20260501 AS
SELECT research_id, ajcc8_stage_group
FROM main.canonical_patient_master WHERE is_malignant = TRUE;

-- Re-derive with explicit coalesce rule
UPDATE main.canonical_patient_master cpm
SET ajcc8_stage_group = CASE
  WHEN src.derived_label IN ('IVA','IVC') THEN 'IVB'
  ELSE src.derived_label
END
FROM mig_184_source_table src
WHERE cpm.research_id = src.research_id;

-- No dependent-VIEW cascade needed; rule is invisible to downstream
```

### If Option C:
```sql
-- Add new column
ALTER TABLE main.canonical_patient_master
  ADD COLUMN IF NOT EXISTS ajcc8_stage_group_full VARCHAR;

-- Populate from source
UPDATE main.canonical_patient_master cpm
SET ajcc8_stage_group_full = src.derived_label
FROM mig_184_source_table src
WHERE cpm.research_id = src.research_id;

-- Update registry to track both columns
INSERT INTO main.canonical_column_verification_registry_v1 (...);
```

## §5 — Verify

```sql
-- A: All M1 + (MTC|ATC|PDTC) should have non-NULL stage_group
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_m_stage = 'M1' AND ajcc8_stage_group IS NULL;
-- Expected: 0 under any option

-- B: Per-histology distribution
-- (same query as mig_254 D3 — should show no II/NULL surprises for non-DTC M1)
```

## §6 — Re-verify on Snowflake

```bash
# Standard reload + Prompt 5 + a Snowflake-side AI_COMPLETE consistency pass
cd /Users/ros/THyroid\ 2026
source .venv/bin/activate
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/01_export_md_to_parquet.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/02_load_to_snowflake.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/04_build_flat_views.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/07_prompt5_staging.py
```
Expect: M1+Stage II count remains 1,018 (Option B) OR shifts (Option A). AI_COMPLETE consistency rate should improve.

## §7 — Manuscript regression check (REQUIRED before signoff under Option A)

For each of M032 / M037 / M038 / M044, regenerate the existing Table 1 / cohort filter results pre- and post-mig and diff them. If any filter behavior changes (e.g. "Stage IV" patient counts shift), surface to Logan for explicit ratification.

```bash
# Pre: mig_259 not yet applied; existing reports
git stash  # if mid-flight changes
# Snapshot: snowflake_trial/reports/m037_table1.md before
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/09_m037_table1.py
mv snowflake_trial/reports/m037_table1.md snowflake_trial/reports/m037_table1_pre_mig259.md

# Apply mig_259 in MD (Cursor)
# Re-export, reload Snowflake, regenerate Table 1
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/01_export_md_to_parquet.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/02_load_to_snowflake.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/04_build_flat_views.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/09_m037_table1.py
mv snowflake_trial/reports/m037_table1.md snowflake_trial/reports/m037_table1_post_mig259.md

diff snowflake_trial/reports/m037_table1_pre_mig259.md snowflake_trial/reports/m037_table1_post_mig259.md
```

## §8 — Carry-forwards
- CF-mig254-MIG266B-OVERLAY-RE-DERIVE → CLOSED on apply
- CF-mig259-MANUSCRIPT-IMPACT (open if Option A picked; closed if B or C)

## §9 — Surgical git add

```
scripts/output/mig_259_*.md
scripts/output/mig_259_pre_snapshot_log.txt
scripts/output/mig_259_dependent_view_diff.csv
qc_framework_v1/migrations/259_*.sql
```
