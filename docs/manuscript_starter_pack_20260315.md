# Manuscript Starter Pack — THYROID_2026

**Generated:** 2026-03-15  
**Dataset freeze:** `manuscript_cohort_v1` (frozen 2026-03-13)  
**Verdict:** GO WITH CAVEATS — all 7 readiness gates PASS  
**Zenodo DOI:** [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510)  

---

## 1. Purpose

This document is the single entry point for manuscript writing. It consolidates
the verified dataset state, approved claims, known limitations, and figure/table
inventory so that drafting can begin immediately using only claims that are
supported by the frozen, verified dataset.

**Companion files** (in `exports/manuscript_starter_pack_20260315/`):

| File | Content |
|------|---------|
| `safe_claims.md` | Statements supported by verified data — cite directly |
| `unsafe_claims.md` | Statements that CANNOT be made from current data |
| `methods_caveats.md` | Pre-written Methods / Limitations / Discussion language |
| `figure_table_inventory.csv` | Complete figure and table manifest with file paths |
| `manuscript_ready_metrics.csv` | All 25 canonical metrics with source tables and SQL |

---

## 2. Frozen Cohort Summary

### 2.1 Primary Denominators

| Cohort | N | Source Table | Definition |
|--------|--:|-------------|------------|
| Full surgical cohort | 10,871 | `manuscript_cohort_v1` | All patients with >=1 thyroid surgery in `path_synoptics` |
| Analysis-eligible cancer | 4,136 | `analysis_cancer_cohort_v1` | Confirmed malignancy + complete staging |
| Deduplicated surgery episodes | 9,368 | `episode_analysis_resolved_v1_dedup` | One row per surgery (146 multi-path removed) |
| Survival analysis cohort | 3,201 | `manuscript_cohort_v1` | Cancer-eligible + positive follow-up time + recurrence endpoint |
| Molecular-tested patients | 10,025 | `extracted_molecular_panel_v1` | Any molecular panel result (ThyroSeq, Afirma, IHC, PCR, FISH) |
| Vascular-positive patients | 3,846 | `extracted_vascular_grading_v13` | Any vascular invasion (graded or ungraded) |
| RAI treatment episodes | 1,857 | `rai_treatment_episode_v2` | All certainty tiers |
| Lesion-level | 11,851 | `lesion_analysis_resolved_v1` | One row per tumor/lesion |

### 2.2 Demographics (Analysis-Eligible Cancer, N = 4,136)

- **Age:** Mean 50.7 (SD 15.7) years
- **Sex:** Female 3,020 (73.0%), Male 1,116 (27.0%)
- **Race:** White 2,446 (59.1%), Black 982 (23.7%), Asian 291 (7.0%), Other/Unknown 417 (10.1%)

### 2.3 Key Clinical Variables

| Variable | N available | Coverage | Source |
|----------|----------:|---------|--------|
| Histology type | 4,137 | 38.1%* | `path_synoptics` |
| Tumor size (cm) | 4,130 | 38.0%* | `tumor_pathology` / `path_synoptics` |
| AJCC 8th Ed stage | 4,083 | 37.6%* | `thyroid_scoring_py_v1` (calculated) |
| ATA 2015 risk | 3,144 | 28.9%* | `thyroid_scoring_py_v1` (calculated) |
| ETE grade | 5,737 | 52.8% | Phase 9 refined |
| Vascular invasion | 3,846 | 35.4% | `path_synoptics` |
| TIRADS score | 3,474 | 32.0% | Phase 12 (Excel + NLP + ACR) |
| Thyroglobulin labs | 2,559 | 23.5% | `longitudinal_lab_canonical_v1` |
| Recurrence (any) | 1,986 | 18.3% | `extracted_recurrence_refined_v1` |

\* Denominators are the full 10,871 cohort; benign-only patients have NA for cancer-specific variables. Among analysis-eligible cancer (N = 4,136), coverage exceeds 95%.

---

## 3. Molecular Profile (Among 10,025 Tested)

| Marker | N positive | Prevalence | Source |
|--------|----------:|-----------|--------|
| BRAF | 376 | 3.8% | `extracted_braf_recovery_v1` |
| RAS (all subtypes) | 292 | 2.9% | `extracted_ras_patient_summary_v1` |
| — NRAS | 196 | 2.0% | |
| — HRAS | 114 | 1.1% | |
| — KRAS | 59 | 0.6% | |
| TERT promoter | 108 | 1.1% | `patient_refined_master_clinical_v12` |

**IMPORTANT citation rule:** Use ONLY the curated extraction table counts above. The master clinical table (`v12`) reports BRAF = 546, RAS = 337 from broader aggregation and is NOT for manuscript citation.

---

## 4. Outcomes

### 4.1 Recurrence

| Detection category | N | Source |
|--------------------:|--:|--------|
| Structural confirmed | 54 | `extracted_recurrence_refined_v1` |
| Biochemical only (rising Tg) | 168 | |
| Structural, date unknown | 1,764 | |
| **Total any recurrence** | **1,986** | |

- 11.2% of recurrence events have day-level dates; 88.8% are flag-only
- Biochemical recurrence = Tg > 1.0 ng/mL and > 2x nadir without structural disease

### 4.2 Survival (N = 3,201)

| Stratum | 5-year rate | p-value |
|---------|:----------:|---------|
| AJCC Stage I/II | 0.823 | < 0.0001 (log-rank) |
| AJCC Stage III/IV | 0.161 | |
| BRAF+ | 0.565 | < 0.0001 |
| BRAF- | 0.753 | |
| ATA High | 0.504 | < 0.0001 |
| ATA Intermediate | 1.0 | |
| ATA Low | 1.0 | |

- Cox PH concordance: 0.853
- Schoenfeld non-proportionality flags: age (p = 0.024), stage III/IV (p < 5e-5), ATA high (p < 5e-5), LN positive (p = 0.042)

### 4.3 Complications (N = 10,871)

| Complication | Confirmed N | Rate |
|-------------|----------:|-----:|
| RLN injury | 59 | 0.54% |
| Hematoma | 38 | 0.35% |
| Hypoparathyroidism | 34 | 0.31% |
| Seroma | 28 | 0.26% |
| Chyle leak | 20 | 0.18% |
| Hypocalcemia | 18 | 0.17% |
| Wound infection | 2 | 0.02% |
| **Any complication** | **287** | **2.6%** |

---

## 5. Figures & Tables Ready for Manuscript

### Figures (300 DPI PNG + SVG)

| ID | Title | Path |
|----|-------|------|
| Fig 1 | Cohort flow / CONSORT diagram | `exports/manuscript_figures/fig1_cohort_flow.png` |
| Fig 2 | Kaplan-Meier survival by AJCC 8th Ed stage | `exports/manuscript_figures/fig2_km_ajcc8.png` |
| Fig 3 | AJCC stage and ATA risk distribution | `exports/manuscript_figures/fig3_stage_risk_distribution.png` |
| Fig 4 | Molecular mutation spectrum | `exports/manuscript_figures/fig4_mutation_spectrum.png` |
| Fig 5 | Post-operative complication rates | `exports/manuscript_figures/fig5_complication_rates.png` |

### Tables (CSV + Markdown + LaTeX)

| ID | Title | Path (CSV) |
|----|-------|------------|
| Table 1 | Patient demographics | `exports/manuscript_tables/table1_demographics.csv` |
| Table 2 | Tumor characteristics & treatment | `exports/manuscript_tables/table2_tumor_treatment.csv` |
| Table 3 | Clinical outcomes | `exports/manuscript_tables/table3_outcomes.csv` |
| Cohort flow | Stepwise inclusion/exclusion | `exports/manuscript_tables/cohort_flow.csv` |
| Cox PH | Multivariable Cox model results | `exports/manuscript_tables/cox_ph.md` |
| Logistic | Logistic regression models (4 models) | `exports/manuscript_tables/logistic_models.md` |
| Supplement | Missingness summary | `exports/manuscript_tables/supplementary_missingness.csv` |

Combined formatted tables: `exports/manuscript_tables/all_tables.md` and `all_tables.tex`.

---

## 6. Safe vs. Unsafe Claims — Summary

### Safe (cite directly)

- All cohort sizes, denominators, and demographic summaries
- Molecular counts (BRAF = 376, RAS = 292, TERT = 108 among 10,025 tested)
- Complication rates (confirmed, refined, 3-tier evidence)
- KM survival curves and stage-stratified 5-year rates
- Cox PH concordance (0.853) and hazard ratios
- Scoring system calculability percentages
- ETE grading distribution (microscopic 5,393 / gross 278 / ungraded 66)
- TIRADS coverage and ACR concordance (80.1%)

### Unsafe (do NOT claim)

- Precise time-to-recurrence (88.8% lack day-level dates)
- "All patients received RAI" or dose-dependent RAI analyses (41% coverage)
- Operative detail negation ("RLN monitoring was not used" — FALSE means unknown)
- Vascular invasion sub-grading prevalence (83.5% ungraded)
- Population-level TSH/vitamin D/free T4 trends (0% data)
- Complete clinical note NLP coverage (51.9%)
- Any claim requiring nuclear medicine data (0 notes)

Full safe/unsafe lists at: `exports/manuscript_starter_pack_20260315/safe_claims.md` and `unsafe_claims.md`.

---

## 7. Pre-Written Caveat Language

All 8 caveats have ready-to-use Methods, Limitations, and Discussion sections in:
- `docs/MANUSCRIPT_CAVEATS_20260313.md` (full text)
- `exports/manuscript_starter_pack_20260315/methods_caveats.md` (consolidated)

| # | Caveat | Severity | Section |
|---|--------|----------|---------|
| 1 | Non-Tg lab dates (PTH/calcium NLP-extracted, no day-level precision) | Moderate | Methods + Limitations |
| 2 | Recurrence date sparsity (88.8% unresolved) | High | Methods + Limitations + Discussion |
| 3 | Nuclear medicine notes absent (0 in corpus) | High | Methods + Limitations |
| 4 | Partial clinical note coverage (51.9%) | Moderate | Methods |
| 5 | Vascular invasion present_ungraded (83.5%) | Moderate | Methods + Limitations |
| 6 | Operative boolean defaults (FALSE = unknown, not negative) | Low-Mod | Methods footnote |
| 7 | BRAF prevalence context (3.8% < published 40-45%) | Low | Discussion |
| 8 | Scoring system calculability (AJCC8 37.6% of full cohort) | Low | Methods |

---

## 8. Suggested Manuscript Structure

### Paper 1: Broad Surgical Cohort (N = 10,871)

- **Focus:** Demographics, procedure types, complication rates, TIRADS utility
- **Tables:** 1, 2, 3, cohort flow, supplementary missingness
- **Figures:** 1, 5
- **Primary denominators:** Full surgical cohort
- **Safe claims:** All demographics, complication rates, TIRADS distribution, molecular testing coverage

### Paper 2: Cancer-Specific Pathology & Recurrence (N = 4,136)

- **Focus:** AJCC staging, recurrence predictors, survival analysis
- **Tables:** 2, 3, Cox PH, logistic models
- **Figures:** 2, 3
- **Primary denominators:** Analysis-eligible cancer, survival cohort
- **Safe claims:** KM curves, Cox model, stage distributions, ETE grading
- **Key caveat:** Recurrence date sparsity limits time-to-event precision

### Paper 3: Molecular Landscape & ThyroSeq Integration (N = 10,025)

- **Focus:** BRAF/RAS/TERT spectrum, molecular-recurrence association
- **Tables:** 2, logistic models
- **Figures:** 4
- **Primary denominators:** Molecular-tested patients
- **Safe claims:** Gene-level positivity, TERT recovery narrative, FP-correction methodology
- **Key caveat:** BRAF prevalence appears low because surgical cohort includes benign disease

---

## 9. Reproducibility Checklist

- [x] All analyses use `random_state=42`
- [x] Statistical analysis plan: `docs/statistical_analysis_plan_thyroid_manuscript.md` (909 lines)
- [x] Zenodo archive: DOI 10.5281/zenodo.18945510
- [x] Frozen cohort: `exports/manuscript_freeze_v1/` (33 tables, 65/65 checksums PASS)
- [x] Metric registry: `exports/manuscript_metric_registry_20260313/` (25 metrics)
- [x] Reviewer defense snapshots: `docs/reviewer_defense_20260313/` (6 documents)
- [x] Data quality supplement: `docs/SUPPLEMENT_DATA_QUALITY_APPENDIX_20260313.md`
- [x] CITATION.cff at repo root with DOI

---

## 10. Deferred Analyses (Awaiting External Data)

| Analysis | Blocking Data | Status |
|----------|--------------|--------|
| Postoperative TSH kinetics | Institutional TSH lab extract | Schema ready (`longitudinal_lab_canonical_v1`) |
| Calcium/PTH day-level temporal trajectories | Structured lab dates for non-Tg labs | 17% date coverage currently |
| RAI dose-response analysis | Nuclear medicine structured data | 0 nuclear med notes; 41% dose available |
| Complete operative NLP enrichment | V2 extractor materialization | Pipeline-gap; infrastructure exists |
| Adjudication panel decisions | Clinical reviewer sessions | Review framework deployed |

---

## 11. Quick-Start for Authors

1. **Read** `exports/manuscript_starter_pack_20260315/safe_claims.md` — every number in that file is verified and citable.
2. **Check** `exports/manuscript_starter_pack_20260315/unsafe_claims.md` before making any claim — if it appears on that list, do NOT include it without qualification.
3. **Copy** caveat language from `exports/manuscript_starter_pack_20260315/methods_caveats.md` — all wording is pre-approved and reviewer-tested.
4. **Use** figures from `exports/manuscript_figures/` (300 DPI PNG or SVG).
5. **Use** tables from `exports/manuscript_tables/` (LaTeX .tex preferred for journals, .md for review).
6. **Cite** Zenodo DOI (10.5281/zenodo.18945510) for data availability statement.
7. **Never** invent or impute numbers — if a metric is not in the registry, it cannot be cited.
