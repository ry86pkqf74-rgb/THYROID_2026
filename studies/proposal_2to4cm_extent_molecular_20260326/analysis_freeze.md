# Analysis Freeze Classification

**Date:** 2026-03-26
**Git SHA:** 2e9a787b904cc2b8cab9f94789c07f2e8cf46772

## Primary analyses

| Analysis | Model | N | Outcome | Classification |
|----------|-------|---|---------|----------------|
| Parsimonious logistic regression | primary_parsimonious | 558 | initial_total | **PRIMARY** |
| Extended logistic regression (+ bilateral, TI-RADS) | primary_extended | 558 | initial_total | **PRIMARY** |

## Secondary analyses

| Analysis | Model | N | Outcome | Classification |
|----------|-------|---|---------|----------------|
| Broad nodal exclusion sensitivity | broad_nodal_parsimonious | 635 | initial_total | **SECONDARY** |
| Complete-case bethesda sensitivity | (refit primary on non-missing bethesda) | 409 | initial_total | **SECONDARY** |

## Exploratory analyses

| Analysis | Model/Table | N | Outcome | Classification | Note |
|----------|-------------|---|---------|----------------|------|
| Completion thyroidectomy model | completion_after_lobe | 238 | completion_event | **EXPLORATORY** | Complete separation — 0 completion events; model unreliable |
| Molecular subset model | molecular_subset | 20 | initial_total | **EXPLORATORY** | N=20; extreme coefficients; underpowered |
| ThyroSeq subgroup | thyroseq_only | — | initial_total | **EXPLORATORY** | Platform-specific; tiny N; separation likely |
| Afirma subgroup | afirma_only | — | initial_total | **EXPLORATORY** | Platform-specific; tiny N; separation confirmed |
| Molecular-pathology concordance | table6 | 20 | concordance | **EXPLORATORY** | Descriptive only |
| Univariable screening | univariable_tests.csv | 558 | initial_total | **EXPLORATORY** | Hypothesis-generating |
| Initial → ultimate extent transitions | transition_counts | 558 | — | **EXPLORATORY** | Descriptive |

## Pathology-defined size sensitivity

- **Status:** NOT RUN — pathology-defined cohort N = 0.
- **Reason:** No patients met pathology-defined 2–4 cm inclusion after current data linkage.
- Documented in `cohort_flow.csv` step `pathology_defined_size_2_to_4_cm`.

## Frozen outputs (do not modify without re-running pipeline)

- `patient_level_dataset.csv` (N=558)
- `patient_level_dataset_broad_nodal_exclusion.csv` (N=635)
- All `logistic_*.csv` model files
- All `table*.csv` files
- All `fig_*.png` figures
- `analysis_manifest.json`
