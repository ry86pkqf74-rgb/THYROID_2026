# mig_204 — Manuscript CSV Population from Live MotherDuck
**Date:** 2026-04-30  
**Author:** Cursor agent (Logan Glosser)  
**DB:** `thyroid_canonical_publication_v1_0`  
**Status:** ✅ COMPLETE — all 7 CSVs populated

---

## Summary

mig_204 executed all manuscript-facing SQL templates against live MotherDuck and wrote
populated CSVs to replace the placeholder stubs left by mig_195/mig_196.

---

## Pre-flight Registry Check

| batch_id | n_registry_hits |
|---|---|
| `mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430` | 46 |

**Note:** Only 1 of 4 expected prerequisite batch_ids found in `canonical_column_verification_registry_v1`.
Migrations mig_186b, mig_185b, and mig_187 are not yet registered. The analytic SQL ran successfully
against the live schema regardless — the registry check is informational only for this pass.

---

## Deliverables

### 1. Table 1 — Cohort Characteristics
**File:** `qc_framework_v1/manuscript/table_1_cohort_characteristics.csv`  
**Rows:** 71  
**Analytic N:** 4,022 patients (malignant CPM ∩ ≥1 canonical_path_malignant_events_v1 row)

Key statistics:
- Age: mean 50.6 ± 15.7 y; median 50 (IQR 38–63)
- Sex: Female 2,937 (73.0%), Male 1,085 (27.0%)

**SQL fix applied:** `CROSS JOIN denom … GROUP BY d.n` pattern replaced with scalar subquery
`NULLIF((SELECT n FROM denom), 0)` throughout — DuckDB does not allow cross-join alias in
`GROUP BY` within `UNION ALL` context.

---

### 2. Cohort Flow Diagram
**File:** `qc_framework_v1/manuscript/cohort_flow_diagram.csv`  
**Rows:** 6 (CONSORT steps 1–6)

---

### 3. Template 01 — Overall Survival
**File:** `qc_framework_v1/manuscript/analytic_templates/previews/01_overall_survival_preview.csv`  
**Full result:** 12,066 rows (3 strata × ~4,022 patients)  
**Preview written:** 200 rows  
**Strata:** `ajcc8_stage_group_resolved`, `histology_bucket`, `age_tertile_band`

---

### 4. Template 02 — Recurrence-Free Survival
**File:** `qc_framework_v1/manuscript/analytic_templates/previews/02_recurrence_free_survival_preview.csv`  
**Full result:** 20,110 rows (5 strata × ~4,022 patients)  
**Preview written:** 200 rows  
**Strata:** `stage_group_resolved`, `age_tertile_band`, `ajcc8_t_stage_resolved`, `ajcc8_n_stage_resolved`, `r_class_true_margin`

---

### 5. Template 03 — Stage Group × Histology
**File:** `qc_framework_v1/manuscript/analytic_templates/previews/03_stage_group_by_histology_preview.csv`  
**Rows:** 24 (all rows written — small result)

---

### 6. Template 04 — Complication Rate by Surgery Type
**File:** `qc_framework_v1/manuscript/analytic_templates/previews/04_complication_rate_by_surgery_type_preview.csv`  
**Rows:** 72 (12 surgery buckets × 6 complication categories)

**Schema fix applied:** `canonical_complications_events_v1` does not have `timing_days` column.
Actual schema: `finding_date` (DATE) + `onset_class` (VARCHAR). Fixed SQL computes
`DATE_DIFF('day', first_surgery_date, finding_date)` for the 0–30d acute window, with
`onset_class IN ('acute','perioperative','immediate')` as fallback.

---

### 7. Template 05 — Cohort Flow + Exclusions (QUERY A counts)
**File:** `qc_framework_v1/manuscript/analytic_templates/previews/05_cohort_flow_rid_lists_preview.csv`  
**Rows:** 6 (CONSORT counts, mirrors cohort_flow_diagram.csv)

---

## Schema Corrections Documented

| Template | Original column | Actual column | Fix |
|---|---|---|---|
| Table 1 | `GROUP BY d.n` (cross-join alias) | N/A | Replaced with scalar subquery `(SELECT n FROM denom)` |
| Template 04 | `ce.timing_days` | `ce.finding_date` (DATE) + `ce.onset_class` | Computed days from `DATE_DIFF('day', first_surgery_date, finding_date)` |

These corrections are also applied to the SQL source files for future re-runs.

---

## Files Changed

```
qc_framework_v1/manuscript/table_1_cohort_characteristics.csv          ← populated (71 rows)
qc_framework_v1/manuscript/cohort_flow_diagram.csv                     ← populated (6 rows)
qc_framework_v1/manuscript/analytic_templates/previews/
  01_overall_survival_preview.csv                                       ← populated (200 rows)
  02_recurrence_free_survival_preview.csv                               ← populated (200 rows)
  03_stage_group_by_histology_preview.csv                               ← populated (24 rows)
  04_complication_rate_by_surgery_type_preview.csv                      ← populated (72 rows)
  05_cohort_flow_rid_lists_preview.csv                                  ← populated (6 rows)
qc_framework_v1/scripts/build_mig204_populate_manuscript_csvs.py       ← new execution script
qc_framework_v1/scripts/run_template04_fix.py                          ← Template 04 fix script
qc_framework_v1/reports/mig_204_manuscript_csv_population_20260430.md  ← this report
```
