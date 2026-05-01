# M044 — Demographics & Full-Canonical-Schema Addendum

**Date:** 2026-05-01
**Purpose:** Supplemental review of the canonical_patient_master (CPM) and other canonical-v1.0 tables for demographic, comorbidity, tumor-characteristic, and molecular variables that were not covered by the M044 cohort view (`manuscript_workspace.cohort_m044_ajcc_ete_v1`, 29 columns) in the initial validation pass. The CPM has 1,630 governed columns and is the read-path for any patient-level analytic that needs cross-domain features. This addendum is intended to be merged into Table 1 and the manuscript Methods/Results.

**Scope of new variables pulled.** Race, BMI, smoking, comorbidity panel (NLP PMHx), family history, autoimmune thyroid disease (Hashimoto's / Graves'), tumor characteristics (multifocality, bilaterality, margin status, closest margin distance, capsular invasion, aggressive variant), molecular (BRAF, RAS, TERT, RET, PAX8/PPARG), surgical extent (total thyroidectomy vs lobectomy), and AGES score.

---

## 1. Demographics by ETE group

### 1.1 Race

Race is well-populated (4,124/4,128 = 99.9% non-null; 295 patients categorized as "Unknown or Not Reported").

| Race | No/neg ETE | Microscopic ETE | Gross ETE | Present ungraded | Missing/other | Total |
|---|---:|---:|---:|---:|---:|---:|
| White | 127 | 1,517 | 742 | 21 | 36 | 2,443 (59.2%) |
| Black or African American | 23 | 634 | 310 | 3 | 19 | 989 (24.0%) |
| Asian | 18 | 163 | 95 | 3 | 6 | 285 (6.9%) |
| Unknown / Not Reported | 17 | 185 | 88 | 1 | 4 | 295 (7.1%) |
| Other | 5 | 53 | 16 | 0 | 0 | 74 (1.8%) |
| American Indian / Alaska Native | 0 | 7 | 8 | 0 | 0 | 15 (0.4%) |
| Native Hawaiian / Pacific Islander | 0 | 8 | 6 | 0 | 0 | 14 (0.3%) |
| Hispanic or Latino | 0 | 7 | 1 | 1 | 0 | 9 (0.2%) |
| Null | 2 | 2 | 0 | 0 | 0 | 4 |

The cohort is racially diverse, with a substantial Black/African American representation (~24%). This is an important manuscript point — single-institution series often report >85% White, and the THYROID_2026 cohort's racial diversity strengthens external generalizability claims for U.S. urban populations.

Note: ethnicity is partially conflated with race in this dataset (Hispanic/Latino appears as a "race" value rather than as a separate ethnicity field). A separate ethnicity field, if it exists in CPM, has not been confirmed.

### 1.2 BMI

| ETE group | n with BMI known | Mean BMI | Median BMI | % missing |
|---|---:|---:|---:|---:|
| No/negative ETE | 15 | 29.24 | 25.00 | 92.2% |
| Microscopic ETE | 499 | 29.99 | 28.51 | 80.6% |
| Gross ETE | 254 | 29.76 | 28.61 | 79.9% |
| Present ungraded | 4 | 33.15 | 28.34 | 86.2% |
| Missing/other | 7 | 30.97 | 32.00 | 89.2% |

**Data limitation:** BMI is missing in ~80% of the cohort. It cannot be used as a primary covariate; if reported, it should be in a sensitivity model or descriptive only with a clear missingness disclosure.

### 1.3 Smoking status

`pmhx_nlp_smoking_status` is **NULL in 4,115 of 4,128 patients (99.7%)**. The remaining 13 patients have free-text values ("never smoker", "former smoker", "current smoker", "Every Day", "History of tobacco smoking. Now 3 cigarettes.", "nonsmoker", "quit smoking"). This field is effectively unusable in its current form.

**Manuscript implication:** Smoking status cannot be reported. Document this as a data-extraction limitation in the manuscript Methods.

---

## 2. Comorbidities (NLP-extracted PMHx) by ETE group

| Comorbidity | No/neg ETE (n=192) | Microscopic ETE (n=2,576) | Gross ETE (n=1,266) | Present ungraded (n=29) | Missing/other (n=65) |
|---|---:|---:|---:|---:|---:|
| Diabetes | 16 (8.3%) | 338 (13.1%) | 240 (19.0%) | 3 (10.3%) | 14 (21.5%) |
| Hypertension | 23 (12.0%) | 406 (15.8%) | 247 (19.5%) | 6 (20.7%) | 15 (23.1%) |
| Hyperthyroidism | 10 (5.2%) | 270 (10.5%) | 140 (11.1%) | 1 (3.4%) | 13 (20.0%) |
| Hypothyroidism | 27 (14.1%) | 545 (21.2%) | 379 (29.9%) | 5 (17.2%) | 24 (36.9%) |
| Obesity (NLP) | 3 (1.6%) | 112 (4.3%) | 81 (6.4%) | 1 (3.4%) | 3 (4.6%) |
| Radiation exposure | 0 | 10 | 13 | 1 | 0 |
| Family hx thyroid disease | 0 | 6 | 6 | 0 | 0 |
| Family hx cancer | 0 | 5 | 6 | 0 | 0 |
| MEN syndrome | 1 | 3 | 2 | 0 | 0 |
| Prior cancer history | 3 | 71 | 44 | 2 | 0 |
| CAD | 2 | 55 | 41 | 0 | 2 |
| CKD | 4 | 57 | 32 | 0 | 3 |
| COPD | 2 | 17 | 11 | 0 | 3 |
| Breast cancer history | 6 | 119 | 78 | 2 | 4 |
| Depression | 5 | 102 | 61 | 1 | 6 |

### 2.1 Autoimmune thyroid disease (synoptic flags)

| | No/neg ETE | Microscopic | Gross | Present ungraded | Missing | Total |
|---|---:|---:|---:|---:|---:|---:|
| Hashimoto's (`syn_hashimoto`) | 1 | 63 | 28 | 0 | 1 | 93 (2.3%) |
| Graves (`syn_graves`) | 2 | 45 | 8 | 0 | 1 | 56 (1.4%) |

(Note: cohort_m044_ajcc_ete_v1 itself reports `pmhx_nlp_autoimmune_thyroid_hx` for 26 patients in total, lower than `syn_hashimoto` because the synoptic flag captures pathologically-evident background thyroiditis, while the PMHx NLP flag depends on documented pre-operative diagnosis.)

**Data limitations on PMHx:**
- Family-history fields are sparsely populated (≤6 per group); under-extracted by NLP.
- Smoking status is essentially unusable (see §1.3).
- Comorbidity fields are NLP-derived from clinical-note PMHx mentions and may both miss patients with the diagnosis (no mention) and double-count patients with imprecise mentions; documented missingness is implicit (no flag = no mention).

---

## 3. Tumor characteristics (CPM-derived)

| Variable | No/neg ETE | Microscopic ETE | Gross ETE | Present ungraded | Missing/other |
|---|---:|---:|---:|---:|---:|
| Multifocal (`multifocal_flag_path`) | 21 (10.9%) | 887 (34.4%) | 511 (40.4%) | 13 (44.8%) | 6 (9.2%) |
| Bilateral (`bilateral_disease_flag`) | 9 (4.7%) | 537 (20.8%) | 342 (27.0%) | 7 (24.1%) | 16 (24.6%) |
| Aggressive variant (`aggressive_variant_flag`) | 1 (0.5%) | 16 (0.6%) | 26 (2.1%) | 0 | 0 |
| Margin involved any (`margin_involved_any`) | 19 (9.9%) | 214 (8.3%) | 349 (27.6%) | 22 (75.9%) | 4 (6.2%) |
| Closest margin distance, mm (mean) | 1.01 | 1.63 | 0.91 | 0.35 | 2.05 |
| Capsular invasion data available | 16 (8.3%) | 865 (33.6%) | 299 (23.6%) | 7 (24.1%) | 3 (4.6%) |

**Key new findings to incorporate into the manuscript:**

- **Multifocality is markedly higher in gross ETE (40.4%) than in no/negative ETE (10.9%)** — strengthens the "gross ETE = high-burden disease" narrative.
- **Bilateral disease is also enriched in gross ETE** (27.0% vs 4.7% for no/negative ETE).
- **Margin involvement is strikingly higher in gross ETE (27.6%) vs microscopic ETE (8.3%) vs no/negative ETE (9.9%).** This is a critical supportive finding for the AJCC 8 thesis: gross ETE not only upstages T but also predicts incomplete resection. Consider adding margin status as a covariate to the multivariable model.
- **Closest margin distance** is 0.91 mm in gross ETE, 1.63 mm in microscopic ETE — closer margins in gross ETE consistent with deeper invasion.
- **Aggressive histologic variant** (tall-cell, columnar-cell, hobnail, etc.) is enriched in gross ETE (2.1%) vs microscopic (0.6%) vs no/negative (0.5%).

---

## 4. Molecular profile by ETE group

| Variable | No/neg ETE | Microscopic ETE | Gross ETE | Present ungraded | Missing/other |
|---|---:|---:|---:|---:|---:|
| BRAF positive (final) | 9 (4.7%) | 171 (6.6%) | 100 (7.9%) | 3 (10.3%) | 4 (6.2%) |
| TERT promoter positive (final) | 1 (0.5%) | 37 (1.4%) | 20 (1.6%) | 3 (10.3%) | 1 (1.5%) |
| TERT tested (denominator) | 192 (100%) | 2,506 (97.3%) | 1,262 (99.7%) | 29 (100%) | 8 (12.3%) |
| RAS positive (final) | 1 (0.5%) | 123 (4.8%) | 65 (5.1%) | 0 | 8 (12.3%) |
| RET positive | 3 (1.6%) | 21 (0.8%) | 12 (0.9%) | 0 | 0 |

**Notes on molecular ascertainment.** TERT testing is essentially universal in the canonical-publication v1.0 dataset (>97% tested across the three definitive ETE groups), and BRAF positivity rates are lower than expected for a PTC-heavy cohort. This may reflect either (1) a low BRAF prevalence specific to this institutional sample or (2) reporting/extraction completeness limited to documented BRAF-positive results — a variable that has been the subject of dedicated extraction work (BRAF mig_57+, GEN15/GEN16). The TERT-tested-but-negative denominator allows correct interpretation of TERT positivity rates as an institutional prevalence estimate; the BRAF denominator is less clearly defined and may understate true positivity.

**Manuscript implication.** BRAF and TERT can be reported as descriptive covariates; both are higher in gross ETE than in microscopic or no/negative ETE, supporting the gross-ETE-as-aggressive-disease narrative. TERT positivity is rare (~1.5%) but consistent with literature for low-risk DTC.

---

## 5. Surgical extent and procedure counts

| Variable | No/neg ETE | Microscopic ETE | Gross ETE | Present ungraded | Missing/other |
|---|---:|---:|---:|---:|---:|
| Total thyroidectomy (`surg_total_thyroidectomy=TRUE`) | 42 (21.9%) | 1,235 (47.9%) | 785 (62.0%) | 19 (65.5%) | 17 (26.2%) |
| Mean number of surgical procedures | 1.0 | 1.0 | 1.0 | 1.0 | 1.0 |

Total thyroidectomy was performed in 62.0% of gross ETE vs 47.9% of microscopic ETE vs 21.9% of no/negative ETE — concordant with surgical practice (more aggressive resection for higher-risk disease). The remaining patients had hemithyroidectomy/lobectomy, and the no/negative ETE group is enriched for limited-extent surgery, which is consistent with the hypothesis that this group entered the cohort through a different clinical pathway (lobectomy for nodule then later management).

---

## 6. AGES score

| ETE group | Mean AGES | Calculable n |
|---|---:|---:|
| No/negative ETE | 6.54 | 192 |
| Microscopic ETE | 6.64 | 2,576 |
| Gross ETE | 7.73 | 1,266 (for 1,266 with calculable flag) |
| Present ungraded | 7.04 | 29 |
| Missing/other | 6.67 | 65 |

The AGES (Age, Grade, Extent, Size) prognostic score is canonically calculated for the entire cohort. Mean AGES is highest in gross ETE (7.73) compared with microscopic (6.64) and no/negative (6.54), again concordant with the gross-ETE-as-higher-risk thesis. AGES could be reported in the manuscript as a one-line composite risk metric in addition to AJCC 8.

---

## 7. Items still missing or under-populated

- **Smoking status** (99.7% null, free-text variants).
- **Family history of thyroid cancer** and **family history of cancer** (~11 patients flagged across cohort; under-extracted by NLP).
- **Childhood radiation exposure** as a binary risk factor (only 24 patients flagged).
- **MEN syndromes** (only 6 patients flagged in cohort).
- **Pre-operative TSH and Tg trajectories** — labs are available in `canonical_labs_tsh_v1` and `canonical_labs_thyroglobulin_v1` but were not pulled for this addendum; recommend a follow-up query for the manuscript.
- **Surgical complications** — `canonical_complications_patient_rollup_v1` (RLN injury, hypoparathyroidism, etc.) was not pulled; recommended follow-up.
- **Frozen section results** — `canonical_frozen_section_patient_rollup_v1` not pulled.
- **PET radiotracer / nuclear-medicine context** — `nuclear_med` table not pulled.
- **Ethnicity as a separate field** from race — it appears Hispanic/Latino has been included as a race value rather than as a separate ethnicity. Confirm with the data team whether a separate `ethnicity` column exists in CPM.

---

## 8. Manuscript / Table 1 additions

The following rows should be appended to Table 1 of the manuscript draft:

1. Race: White, Black/African American, Asian, Other, Unknown (per group, n and %).
2. BMI: mean, median, n known (with explicit missingness disclosure).
3. Hashimoto's thyroiditis (synoptic): n and %.
4. Graves' disease (synoptic): n and %.
5. Diabetes, hypertension, hypothyroidism: n and % (from NLP PMHx).
6. Multifocality: n and %.
7. Bilateral disease: n and %.
8. Aggressive histologic variant: n and %.
9. Margin involved: n and %.
10. Closest margin distance, mm: mean.
11. BRAF positive (final): n and % (with TERT-tested denominator note).
12. TERT positive (final): n and % (denominator = tested).
13. Total thyroidectomy: n and %.
14. AGES score (mean).

The following rows should be omitted or noted as unreportable:
- Smoking status (data limitation).
- Family history of thyroid cancer / family history of cancer (under-extracted).
- Childhood radiation exposure (under-extracted).

---

## 9. Updated manuscript implications

Three additional clinical findings strengthen the AJCC 8 thesis:

1. **Multifocality**, **bilateral disease**, **margin involvement**, and **aggressive variant** are all enriched in gross ETE compared with microscopic ETE.
2. **BRAF positivity** is modestly higher in gross ETE; **TERT positivity** is rare but slightly more frequent in gross ETE.
3. **Total thyroidectomy** rate tracks ETE severity, consistent with surgical practice.

Two additional considerations strengthen the no/negative-ETE-as-confounded-subgroup hypothesis:

1. The no/negative ETE group has a **much lower total-thyroidectomy rate (21.9%)** — many were treated by hemithyroidectomy, with completion or therapeutic neck surgery later. This is consistent with the second-surgery / completion pathway driving recurrence ascertainment in this group (see validation report §5).
2. The no/negative ETE group has lower AGES, lower BRAF, lower TERT, lower multifocality, and lower bilateral disease than the microscopic and gross ETE groups — yet has comparable or higher path-proven recurrence. This combination is biologically implausible if the no/negative ETE label faithfully reflects pathology, and supports the bias hypothesis (lateral-N1b ascertainment, completion-surgery pathway).

End of demographics addendum.
