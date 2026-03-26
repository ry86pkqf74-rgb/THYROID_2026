# QA Reconciliation Report

**Generated:** 2026-03-26T05:30:10.586629+00:00

## 1. Cohort flow step counts (`cohort_flow.csv`)

| Step | N |
|------|---|
| all_patients_first_thyroid_procedure_lobe_or_total_or_unknown | 9368 |
| hemithyroidectomy_or_total_only | 8370 |
| pathology_defined_size_2_to_4_cm | 0 |
| preop_imaging_nodule_size_2_to_4_cm | 635 |
| after_strict_preop_ln_exclusion_path_cohort | 0 |
| after_strict_preop_ln_exclusion_preop_cohort | 558 |
| after_broad_suspicious_node_exclusion_preop_cohort | 635 |
| primary_preop_cohort_final_N | 558 |

## 2. Patient-level dataset row counts

- `patient_level_dataset.csv`: **558** rows
- `patient_level_dataset_broad_nodal_exclusion.csv`: **635** rows
- Flow `primary_preop_cohort_final_N`: **558**
- ✅ Primary CSV rows == flow final step: **PASS**
- ✅ Broad CSV rows == flow `after_broad_suspicious_node_exclusion_preop_cohort`: **PASS**

## 3. Outcome event counts

- Initial lobectomy: **238**
- Initial total thyroidectomy: **320**
- Sum: **558** = primary N: **PASS**

## 4. Transition counts cross-check

- {'initial': 'Initial lobectomy', 'Not ultimate total': 238, 'Ultimate total-class': 0}
- {'initial': 'Initial total', 'Not ultimate total': 0, 'Ultimate total-class': 320}
- Sum of all cells: **558** vs primary N **558**: **PASS**

## 5. Model row counts (from table2)

| Model | N in table2 | Expected |
|-------|-------------|----------|
| broad_nodal_parsimonious | 635 | 635 ✅ |
| completion_after_lobe | 238 | 238 ✅ |
| primary_extended | 558 | 558 ✅ |
| primary_parsimonious | 558 | 558 ✅ |

## 6. Figure inventory

- Total figure PNG files: **7**

  - `fig_bethesda_by_extent.png`
  - `fig_cohort_flow.png`
  - `fig_completion_rates.png`
  - `fig_forest_total_vs_lobectomy.png`
  - `fig_initial_to_ultimate_extent.png`
  - `fig_molecular_result_by_extent.png`
  - `fig_platform_specific_extent.png`

## 7. Validation report summary

From `validation_report.md`:

> # Validation report
> - patient_level_dataset.csv rows: 558
> - in-memory patient_df rows: 558
> - distinct research_id (file): 558
> - distinct research_id (memory): 558
> - symmetric ID difference count: 0
> - mismatch pct vs cohort (memory): 0.0000%
> - MotherDuck operative rows matching cohort IDs (any hemi/total row): 559
> - ratio operative_rows_over_cohort_ids: 1.0018

## 8. Manifest cross-check (`analysis_manifest.json`)

- primary_cohort_n: 558 vs CSV: 558 → **PASS**
- broad_nodal_cohort_n: 635 vs CSV: 635 → **PASS**
- path_sensitivity_n: 0 (expected 0; no path cohort)

## 9. Completion thyroidectomy audit

- Lobectomy patients: **238**
- Completion events: **0** (rate 0.000)
- ⚠️ Zero completion events → completion model has complete separation.

## 10. Molecular testing coverage

- Molecular tested: **20** / 558 (3.6%)
- ⚠️ Very low testing rate limits molecular subset model reliability.

## 11. Integrity checksums

- `patient_level_dataset.csv`: SHA-256 prefix `1dbf98a52690e32c`
- `patient_level_dataset_broad_nodal_exclusion.csv`: SHA-256 prefix `8512ed7d35783f60`

## 12. Outcome homogeneity notice

- `final_malignant` = 1.0 for **all 558** primary patients (100% malignant cohort).
- `aggressive_pathology` = True for **all 558** primary patients.
- These columns provide no discrimination between lobectomy and total thyroidectomy groups.
- Baseline table rows for these variables are retained for completeness but are non-informative.
- Verify with PI whether this reflects inclusion criteria (cancer-only) or a data-processing artefact.

## 13. Model calibration note

- Calibration intercept and slope could not be computed for primary models (logit transform of predicted probabilities produced extreme values near probability boundaries).
- AUC, Brier score, and bootstrap optimism-corrected AUC are valid.

---
**QA verdict:** All primary reconciliation checks PASS. Completion model and molecular subgroup analyses flagged as unreliable (separation / small N). 100% malignancy rate noted — confirm with PI.
