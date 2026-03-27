# QA Reconciliation Report

**Generated:** 2026-03-27T02:48:00.089067+00:00

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

- {'initial': 'Initial lobectomy', 'Not ultimate total': 213, 'Ultimate total-class': 25}
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

- **Canonical submission (main text):** `fig1_cohort_flow_publication.png`, `fig1_cohort_flow_publication.pdf`, `fig2_forest_primary_publication.png`, `fig2_forest_primary_publication.pdf` — see **`figure_legends_v2.md`**.
- **Legacy pipeline PNG (internal / replication):** **7** files

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
> - local DuckDB operative rows matching cohort IDs (any hemi/total row): 559
> - ratio operative_rows_over_cohort_ids: 1.0018

## 8. Manifest cross-check (`analysis_manifest.json`)

- primary_cohort_n: 558 vs CSV: 558 → **PASS**
- broad_nodal_cohort_n: 635 vs CSV: 635 → **PASS**
- path_sensitivity_n: 0 (expected 0; no path cohort)

## 9. Completion thyroidectomy audit

- Lobectomy patients: **238**
- OED pipeline (`operative_episode_detail_v2`) completion ever: **0** / 238 (rate 0.000)
- Path-synoptic definite completion ever: **25** / 238 (rate 0.105)
- Any later thyroid surgery (OED or path row after index): **26** patients
- Ambiguous later surgery only (not OED/path definite): **1**
- ⚠️ Completion logistic (OED-flag outcome) still has **zero events** → complete separation.

## 10. Molecular testing coverage

- Molecular tested: **20** / 558 (3.6%)
- ⚠️ Very low testing rate limits molecular subset model reliability.

## 11. Integrity checksums

- `patient_level_dataset.csv`: SHA-256 prefix `d6b1d7ae22fa8d36`
- `patient_level_dataset_broad_nodal_exclusion.csv`: SHA-256 prefix `de014a7f4d15ee9e`

---
**QA verdict:** All primary reconciliation checks PASS. Completion model and molecular subgroup analyses flagged as unreliable (separation / small N).
