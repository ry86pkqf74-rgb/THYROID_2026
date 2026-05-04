# mig_279 disposition — registry audit for mig_253 / 256 / 258 / 259

**Date:** 2026-05-03  
**DB:** `thyroid_canonical_publication_v1_0`  
**Runner:** mig_279 probe + retro `signoff_migration` INSERTs via `qc_framework_v1/migrations/279_registry_audit_253_256_258_259_20260503.sql`

## Summary

All four numbered SQL migrations were already applied in MotherDuck; only `main.signoff_migration` rows were missing. No re-apply, no supersede, no archival snapshot required.

## Disposition table (Logan)

| mig   | Apply state | Disposition                         | Action |
|-------|-------------|-------------------------------------|--------|
| mig_253 | APPLIED   | Retro signoff (probe matched dry-run)| §2 INSERT `signoff_migration` |
| mig_256 | APPLIED   | Retro signoff (timing-window cols on cohort_m032)| §2 INSERT |
| mig_258 | APPLIED   | Retro signoff (M044 lineage view live)| §2 INSERT |
| mig_259 | APPLIED   | Retro signoff (ln_status_source bucket counts)| §2 INSERT |

## Probe evidence

### mig_253

- `canonical_patient_master`: `n_total=10871`; rows with all-three NULL surgical fields = **2**; non-null `surg_procedure_type` = 10869; `total_thyroidectomy` count = 5999; `hemithyroidectomy` = 4432.

### mig_256

- `information_schema.columns` on `manuscript_workspace.cohort_m032_descriptive_25yr_v1`:
  - **Present:** `comp_hypocalcemia_timing_window`, `comp_hypoparathyroidism_timing_window`
  - Prompt draft listed `onset_class` / `permanence_class` — those do not appear in mig_256 SQL; adjudication follows actual migration DDL (timing-window passthrough from CPM).

### mig_258

- `cohort_m044_ajcc_ete_v1` columns matched `%lineage%` / `%surg_date%` filter:
  - `surg_first_date_lineage_note`, `surg_date_missing`, `surg_date_pre_1999`, `surg_date_1999_2024`, `surg_date_post_2024`, `surg_date_after_2024_06_04`.

### mig_259

- `SELECT ln_status_source, COUNT(*) ...`: **both=1126**, **staging=1509**, **NULL=8236** (matches mig_259 SQL header expectation).

### signoff_migration (before)

- No rows for `mig_253`, `mig_256`, `mig_258`, or `mig_259` (pattern scan returned empty).

## Closes

- CF-mig253-REGISTRY-GAP  
- CF-mig256-REGISTRY-GAP  
- CF-mig258-REGISTRY-GAP  
- CF-mig259-REGISTRY-GAP  
