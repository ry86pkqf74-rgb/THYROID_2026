# Claim source ledger

Maps numeric claims in **`manuscript_submission_v1.md`** and **`abstract_structured_v1.md`** to files in this folder. Status: **VERIFIED** (matches artifact), **AMBIGUOUS** (artifact unclear or flow contradiction), **UNSUPPORTED** (no artifact), **STALE** (superseded).

**Convention:** Odds ratios are **adjusted** within the stated logistic model (statsmodels `Logit`); denominators **n/N (%)** where applicable.

---

## Core cohort

| Claim | Value | Source file | Location / rule | Status |
|-------|-------|-------------|-----------------|--------|
| Primary analytic cohort N | 558 | `patient_level_dataset.csv` | `len(df)` | VERIFIED |
| Primary cohort initial lobectomy | 238 | `patient_level_dataset.csv` | `initial_lobectomy.sum()` | VERIFIED |
| Primary cohort initial total | 320 | `patient_level_dataset.csv` | `initial_total.sum()` | VERIFIED |
| Initial total as % of primary N | 320/558 (57.3%) | `patient_level_dataset.csv` | `320/558` | VERIFIED |
| Broad nodal exclusion cohort N | 635 | `patient_level_dataset_broad_nodal_exclusion.csv` | `len(df)`; matches `analysis_manifest.json` | VERIFIED |
| Broad cohort initial total count | 375 | `patient_level_dataset_broad_nodal_exclusion.csv` | `initial_total.sum()` | VERIFIED |
| Broad cohort initial total % | 375/635 (59.1%) | derived | `375/635` | VERIFIED |
| Pathology sensitivity analytic N (strict LN) | 0 | `cohort_build_log.md`; `analysis_manifest.json` `path_sensitivity_n` | single source | VERIFIED |
| Preop molecular tested (primary N) | 20/558 (3.6%) | `patient_level_dataset.csv` | `preop_molecular_tested.sum()` | VERIFIED |
| Distinct patients in CSV vs validation | 558 | `validation_report.md` | symmetric ID diff 0 | VERIFIED |
| Operative rows per cohort ID ratio | 559/558 (1.0018) | `validation_report.md` | line 9–10 | VERIFIED |

---

## Table 1 — `table1_by_initial_extent.csv`

| Claim | Value | Source | Location | Status |
|-------|-------|--------|----------|--------|
| Lobectomy column n | 238 | `table1_by_initial_extent.csv` | row `variable==n`, `lobectomy` | VERIFIED |
| Total column n | 320 | `table1_by_initial_extent.csv` | row `n`, `total` | VERIFIED |
| Mean age lobectomy / total | 56.59 / 52.93 yr | `table1_by_initial_extent.csv` | row `age_mean` | VERIFIED |
| Female % lobectomy | 191/238 (80.3%) | `patient_level_dataset.csv` | cross-check: 191 female when `initial_lobectomy==1` | VERIFIED |
| Female % total | 257/320 (80.3%) | `patient_level_dataset.csv` | 257 female when `initial_total==1` | VERIFIED |
| Table female % ( decimals) | 80.25% / 80.31% | `table1_by_initial_extent.csv` | row `female_pct` | VERIFIED |
| Mean Bethesda category | 2.8 / 4.0 (lob / total) | `table1_by_initial_extent.csv` | row `bethesda_mean` | VERIFIED |
| Molecular tested % by arm | 4.20% / 3.13% | `table1_by_initial_extent.csv` | row `molecular_tested_pct` | VERIFIED |

*Note: Bethesda mean uses available numeric categories; **149/558** rows missing `bethesda_category` in primary cohort per `missingness_summary.csv` — preserve in Methods/Discussion.*

---

## Completion — `table7_completion_thyroidectomy.csv`

| Claim | Value | Source | Location | Status |
|-------|-------|--------|----------|--------|
| Lobectomy subgroup n | 238 | `table7_completion_thyroidectomy.csv` | row `all_lobectomy`, `n` | VERIFIED |
| OED pipeline completion ever / 30 / 90 / 365 d (proportion) | 0 / 0 / 0 / 0 | same | `completion_*_oed_pipeline` | VERIFIED |
| Path-synoptic definite completion ever / 30 / 90 / 365 d | ≈0.105 / 0.0084 / 0.055 / 0.084 | same | `completion_*_path_synoptic_definite` | VERIFIED |
| N any later thyroid surgery (OED or path) | 26 | same | `n_patients_any_later_thyroid_surgery_oed_or_path` | VERIFIED |
| N definite path synoptic completion | 25 | same | `n_patients_definite_path_synoptic_completion` | VERIFIED |
| N ambiguous later only | 1 | same | `n_patients_ambiguous_later_only_not_oed_or_path_definite` | VERIFIED |

---

## Extent transitions — `initial_ultimate_extent_transition_counts.csv`

| Claim | Value | Source | Location | Status |
|-------|-------|--------|----------|--------|
| Initial lobectomy → not ultimate / ultimate total-class | 213 / 25 | `initial_ultimate_extent_transition_counts.csv` | row Initial lobectomy | VERIFIED |
| Initial total row | 0 / 320 | same file | row Initial total | VERIFIED |

---

## Univariable tests — `univariable_tests.csv`

| Variable | Test | p-value | Source row | Status |
|----------|------|---------|------------|--------|
| age_at_surgery | Mann-Whitney U | 0.00715 | `univariable_tests.csv` | VERIFIED |
| sex_f | Chi-square | 1.0 | `univariable_tests.csv` | VERIFIED |
| bethesda_ge4 | Chi-square | 6.02e-07 | `univariable_tests.csv` | VERIFIED |
| has_mol | Chi-square | 0.655 | `univariable_tests.csv` | VERIFIED |
| bilateral_nodule_indicator | Chi-square | 0.0475 | `univariable_tests.csv` | VERIFIED |

---

## Multivariable logistic — primary cohort (N=558)

**Outcome:** `initial_total`. **Parsimonious predictors:** `age_at_surgery`, `sex_f`, `bethesda_ge4`, `has_mol`. **Extended:** adds `bilateral_nodule_indicator`, `tirads_score`.

Source: `logistic_primary_parsimonious.csv`, `logistic_primary_extended.csv`, and `table2_multivariable_total_vs_lobectomy.csv` (duplicate ORs).

| Predictor | OR (95% CI) | p | Status |
|-----------|-------------|---|--------|
| age_at_surgery | 0.986 (0.975–0.998) | 0.0257 | VERIFIED (parsimonious) |
| sex_f | 0.974 (0.627–1.513) | 0.905 | VERIFIED |
| bethesda_ge4 | 2.743 (1.814–4.147) | 1.74e-06 | VERIFIED |
| has_mol | 0.606 (0.237–1.546) | 0.295 | VERIFIED |
| bilateral_nodule_indicator (extended) | 2.005 (1.282–3.134) | 0.00229 | VERIFIED |
| tirads_score (extended) | 0.958 (0.778–1.179) | 0.684 | VERIFIED |

*Intercept (const) ORs are reported in CSV but are **not** emphasized in clinical interpretation.*

---

## Multivariable logistic — broad cohort (N=635)

Source: `logistic_broad_nodal_parsimonious.csv` / `table2_multivariable_total_vs_lobectomy.csv` `model==broad_nodal_parsimonious`.

| Predictor | OR (95% CI) | p | Status |
|-----------|-------------|---|--------|
| age_at_surgery | 0.984 (0.973–0.995) | 0.00525 | VERIFIED |
| sex_f | 0.939 (0.629–1.401) | 0.758 | VERIFIED |
| bethesda_ge4 | 2.765 (1.878–4.071) | 2.56e-07 | VERIFIED |
| has_mol | 0.657 (0.264–1.635) | 0.366 | VERIFIED |

---

## Molecular subset and platform models — **exploratory / not primary**

| Claim | Source | Status |
|-------|--------|--------|
| Molecular-tested n=20 | `patient_level_dataset.csv` `preop_molecular_tested.sum()` | VERIFIED (denominator) |
| ORs in `logistic_molecular_subset.csv` | `table3_molecular_tested_subset.csv` | **AMBIGUOUS** — very small N; some p-values 0.0; const/platform CIs incomplete in file | 
| Concordance 2×2 (tp,fn,fp,tn) 9,11,0,0 n=20 | `table6_molecular_pathology_concordance.csv` | VERIFIED |
| ThyroSeq platform row n=8 | `table6_molecular_pathology_concordance.csv` `platform_ThyroSeq` | VERIFIED |
| Afirma platform row n=12 | `table6_molecular_pathology_concordance.csv` `platform_Afirma` | VERIFIED |

**Do not** report thyroseq_only / afirma_only ORs from `model_summary_final.csv` as stable inference without noting separation / implausible p-values.

---

## Completion logistic (lobectomy only)

| Claim | Source | Status |
|-------|--------|--------|
| Zero **OED-pipeline** completion events (outcome `completion_total_flag`) | `table7` `completion_*_oed_pipeline`; `model_summary_final` events=0 for completion_after_lobe | VERIFIED |
| Path-synoptic definite completions (descriptive) | `table7`, `patient_level_dataset.csv` `completion_path_synoptic_definite_flag` | VERIFIED (not used as logistic outcome in primary bundle) |
| Logistic ORs with infinite/undefined CIs | `logistic_completion_after_lobe.csv` | **STALE / UNINTERPRETABLE** — complete separation on OED outcome; **do not** cite as effect estimates |

---

## Run metadata

| Claim | Source | Status |
|-------|--------|--------|
| analysis run UTC | `analysis_manifest.json` `run_utc` | VERIFIED |
| Frozen git SHA | `analysis_manifest.json` `git_sha` | VERIFIED |
| DuckDB version | `analysis_manifest.json` `duckdb` | VERIFIED |

---

## Items explicitly not claimed (no artifact)

- Institution name, IRB number, calendar date range of surgery — **UNSUPPORTED** in this folder unless added from external approved text.
- Causal effect estimates, policy recommendations — **UNSUPPORTED** as quantitative claims.
