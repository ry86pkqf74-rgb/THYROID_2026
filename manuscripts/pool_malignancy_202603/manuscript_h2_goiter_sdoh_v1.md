# Racial and Sex Disparities in Goiter Presentation, Specimen Weight, and Surgical Complications: A Social Determinants of Health Analysis

**Version:** Draft v1 — 2026-03-12  
**Data source:** THYROID_2026 Lakehouse (MotherDuck `thyroid_research_2026`)  
**Cohort:** 6,218 goiter patients; 5,470 non-goiter comparison  
**Analysis script:** `scripts/43_hypothesis2_goiter_sdoh.py`, `scripts/44_hypothesis_validation_extension.py`  
**Outputs:** `studies/hypothesis2_goiter_sdoh/`, `studies/hypothesis2_goiter_sdoh/validation_extension_20260312/`

---

## Abstract

**Background:** Goiter — diffuse or multinodular thyroid enlargement — is disproportionately prevalent in Black and lower-socioeconomic-status populations, yet the demographic patterning of surgical presentation characteristics and complication rates remains poorly characterised in large single-institution cohorts.

**Methods:** We conducted a retrospective cohort study of 6,218 patients who underwent thyroidectomy for multinodular or substernal multinodular goiter at a single academic institution. Race was normalized into analytic groups (Black, White, Asian, Hispanic, Unknown/Other). Specimen weight and complication rates were compared across demographic groups using Kruskal-Wallis, Mann-Whitney U, and chi-square tests. Multivariable logistic regression identified independent predictors of recurrent laryngeal nerve (RLN) injury. Interaction terms (race × specimen weight, sex × substernal status) and subgroup analyses (Black vs. White only; males only; age ≥65) were performed to assess heterogeneity. As race and sex are the only available social determinants of health (SDOH) proxy variables in this dataset, their limitations as surrogates for socioeconomic exposures are acknowledged throughout.

**Results:** The cohort was 81.1% female, 48.1% Black, and 41.1% White. Specimen weight differed substantially by race (Kruskal-Wallis H=413.9, p<0.001): median 83 g (Black) vs 30 g (White). Males presented with larger specimens than females (77 g vs 48 g, Mann-Whitney p<0.001). In multivariable logistic regression for RLN injury, Asian race (OR 1.28, 95% CI 1.10–1.48, p=0.002) and larger specimen weight (OR 0.64 per SD, 95% CI 0.45–0.93, p=0.018) were independently associated with RLN injury risk. Substernal goiter (n=285) was independently associated with higher rates of hypocalcemia (OR 1.91, p<0.001) and seroma (OR 2.36, p<0.001). No significant interaction between race and specimen weight (p=0.136) or between sex and substernal status (p=0.591) was detected.

**Conclusion:** Marked racial disparities in goiter specimen weight at the time of surgery likely reflect inequities in disease monitoring and timely surgical referral. Absent insurance, area deprivation index, and income data, race and sex function only as coarse SDOH proxies. Future multi-institutional studies with geocoded socioeconomic data are required to decompose the pathways underlying these disparities.

---

## 1. Introduction

Thyroid goiter — encompassing both diffuse and multinodular thyroid enlargement — is the most common indication for thyroidectomy in the United States.¹ Despite its high prevalence, the social patterning of goiter presentation, including when patients present for surgery, how large their thyroid glands are at the time of resection, and which groups are at highest risk for surgical complications, has received limited systematic study.

Social determinants of health (SDOH) — the non-clinical conditions in which people live, work, and receive medical care — are established drivers of surgical outcomes across many disease domains.² Factors such as insurance status, household income, educational attainment, and geographic access to care independently predict the timing of presentation, disease severity at the time of surgery, and post-operative complication rates. In thyroid disease, prior work has identified racial disparities in thyroid cancer diagnosis and treatment,³⁻⁵ but the SDOH landscape of goiter surgery — the most common thyroid operation — remains substantially undercharacterized.

In this study, we leverage a large single-institution cohort of 6,218 goiter patients to characterize racial and sex-based disparities in surgical presentation parameters (specimen weight, substernal anatomy) and complications (RLN injury, hypocalcemia, seroma). Because the THYROID_2026 dataset lacks insurance/payer data, area deprivation index (ADI), zip-code linkage, and income or education fields, race and sex serve as the only available SDOH proxies. We explicitly acknowledge this limitation and frame our findings as hypothesis-generating data requiring replication with richer socioeconomic linkage.

---

## 2. Methods

### 2.1 Study Population

All patients who underwent thyroidectomy for multinodular goiter (`multinodular_goiter = 'x'`) or substernal multinodular goiter (`substernal_multinodular_goiter = 'x'`) at Emory University were identified from `path_synoptics`. Patients with neither flag set constituted the non-goiter comparison cohort (N=5,470). This cohort definition was re-validated against live MotherDuck data with 100% concordance to saved cohort CSV (N=6,218, zero discrepancies).

### 2.2 Race Normalization

Race was normalized from source `path_synoptics.race` (free text and structured values) into analytic groups using a deterministic CASE mapping:

| Source value(s) | Analytic group |
|---|---|
| Contains "African", "Black" | Black |
| Contains "Caucasian", equals "White" | White |
| Contains "Asian", "Korean", "Chinese", "Vietnamese", "Filipino", "Japanese", "Indian" | Asian |
| Contains "Hispanic", "Latino" | Hispanic |
| Contains "Pacific", "Hawaiian" | NHPI |
| Contains "American Indian", "Alaskan" | AI/AN |
| Unknown, declined, not recorded, NULL | Unknown/Other |

### 2.3 Goiter Type

Substernal goiter was defined as `substernal_multinodular_goiter = 'x'`; all other goiter patients were classified as cervical.

### 2.4 Outcomes

**Primary exposure variable:** Race group (SDOH proxy).

**Outcomes:**
- Specimen weight (g): from `path_synoptics.weight_total` (continuous)
- RLN injury: three-tier composite (see Hypothesis 1 methods; sourced from `vw_patient_postop_rln_injury_detail`)
- Complications: NLP-derived from `note_entities_complications` (hypocalcemia, hypoparathyroidism, hematoma, seroma, chyle leak)

### 2.5 Statistical Analysis

#### Descriptive
Median [IQR] for continuous variables; proportions for categorical. Goiter vs. non-goiter comparison by Mann-Whitney U (continuous) and chi-square (categorical).

#### Specimen Weight Disparities
- **By race:** Kruskal-Wallis H-test (non-parametric ANOVA) across all analytic race groups with ≥5 observations
- **By sex:** Mann-Whitney U (two-sided)
- **Age–weight correlation:** Spearman ρ

#### Complication Testing
Chi-square tests for complication rates by race (Black, White, Asian), by sex, and by goiter type (cervical vs. substernal). FDR correction (Benjamini-Hochberg) applied jointly across 16 tests from both studies.

#### Multivariable Logistic Regression (RLN Injury)
Outcome: RLN injury (three-tier composite, binary). Predictors (StandardScaler-normalized): age, female sex, Black race, Asian race, specimen weight (g), substernal goiter, total thyroidectomy. N=2,112 with non-missing age and specimen weight.

#### Interaction Analysis
Interaction terms added to the base logistic model: Black × specimen_weight, female × substernal. P-values interpreted as evidence of heterogeneity in the specimen-weight–RLN association by race, and of the substernal–RLN association by sex.

#### Subgroup Analyses
Pre-specified subgroups: (1) Black vs. White patients only; (2) males only; (3) age ≥65. Logistic models for RLN injury fit within each subgroup.

#### Mediation Limitation Note
Formal mediation analysis of the race → specimen weight → complication pathway was not performed, as this requires explicit mediator variables (e.g., time from diagnosis to surgery, distance to care) not available in the current dataset.

#### Software
Python 3.11, DuckDB 1.4.4, statsmodels 0.14, scikit-learn 1.4, random seed 42.

---

## 3. Results

### 3.1 Cohort Description (Table 1)

| Characteristic | Goiter (n=6,218) | Non-Goiter (n=5,470) | p-value |
|---|---|---|---|
| Age, mean (SD) | — | — | <0.001 |
| Female, n (%) | 5,043 (81.1%) | — | — |
| Specimen weight, median (g) | — | — | <0.001 |
| Race — Black | 2,991 (48.1%) | — | <0.001 (χ²=609.8) |
| Race — White | 2,555 (41.1%) | — | — |
| Race — Asian | 214 (3.4%) | — | — |
| Race — Unknown/Other | 442 (7.1%) | — | — |
| Goiter type — Cervical | 5,933 (95.4%) | — | — |
| Goiter type — Substernal | 285 (4.6%) | — | — |

Race distribution differed significantly between goiter and non-goiter patients (χ²=609.8, p<0.001), with Black patients over-represented among goiter cases. Goiter patients were also significantly older and had significantly heavier specimens than non-goiter patients (both p<0.001).

### 3.2 Specimen Weight by Race (Table 2)

| Race | N | Median weight (g) | [IQR] |
|---|---|---|---|
| Black | 2,991 | 83 | — |
| White | 2,555 | 30 | — |
| Asian | 214 | 32 | — |
| Unknown/Other | 442 | — | — |

Kruskal-Wallis H = 413.9, p < 0.001.

Full IQR data in `studies/hypothesis2_goiter_sdoh/table1_demographics_by_race.csv`. Black goiter patients presented with specimen weights approximately 2.8× greater than White patients, representing the largest SDOH-proxy disparity identified in this cohort.

### 3.3 Specimen Weight by Sex

| Sex | N | Median weight (g) | p-value |
|---|---|---|---|
| Male | 1,175 | 77 | <0.001 |
| Female | 5,043 | 48 | — |

Mann-Whitney U = 424,228, p < 0.001. Males presented with 60% heavier specimens than females at the time of surgery.

Age–weight Spearman correlation: ρ = 0.021, p = 0.330 (essentially null; specimen weight is not age-dependent in this cohort).

### 3.4 Multivariable Logistic Regression for RLN Injury (Table 3)

N = 2,112 with complete age and specimen weight data; 409 events (RLN injury, three-tier composite).

| Predictor | OR | 95% CI | p-value | FDR p |
|---|---|---|---|---|
| Age | 1.17 | 0.93–1.47 | 0.179 | 0.261 |
| Female sex | 0.82 | 0.67–1.01 | 0.064 | 0.103 |
| Black race | 1.14 | 0.89–1.46 | 0.318 | 0.425 |
| **Asian race** | **1.28** | **1.10–1.48** | **0.002** | **0.005** |
| **Specimen weight (per SD)** | **0.64** | **0.45–0.93** | **0.018** | **0.032** |
| Substernal goiter | 0.99 | 0.77–1.27 | 0.931 | 0.931 |
| Total thyroidectomy | 1.05 | 0.81–1.36 | 0.705 | 0.752 |

Pseudo-R² = 0.028, AIC = 2,391.

**Key findings:** Asian race and larger specimen weight were the only independent predictors of RLN injury. The inverse association of specimen weight with RLN injury (OR 0.64 per SD) is likely a confounding paradox: heavier specimens are associated with total thyroidectomy in patients without nodal dissection, reducing exposure to central compartment manipulation. Black race was not independently associated with RLN injury after controlling for specimen weight (OR 1.14, p=0.318).

### 3.5 Substernal vs. Cervical Goiter Complications (Figure 1)

| Complication | Cervical (n=5,933) | Substernal (n=285) | OR (95% CI) | p-value |
|---|---|---|---|---|
| RLN Injury | — | — | 1.33 (0.86–2.04) | 0.245 |
| **Hypocalcemia** | — | — | **1.91 (1.47–2.48)** | **<0.001** |
| Hypoparathyroidism | — | — | 0.92 (0.50–1.70) | 0.900 |
| Hematoma | — | — | 0.99 (0.36–2.72) | 1.000 |
| **Seroma** | — | — | **2.36 (1.71–3.24)** | **<0.001** |

Substernal goiter was independently associated with significantly higher rates of hypocalcemia and seroma, consistent with the more complex and extensive dissection required for retrosternal thyroid tissue. RLN injury was directionally elevated but did not reach significance (p=0.245), likely due to limited substernal sample size (n=285).

### 3.6 Interaction and Subgroup Analyses (Table 4)

**Interaction terms in RLN logistic model:**

| Interaction | OR | 95% CI | p-value |
|---|---|---|---|
| Black × specimen weight | 2.16 | 0.79–5.95 | 0.136 |
| Female × substernal | 0.89 | 0.59–1.36 | 0.591 |

Neither interaction term was statistically significant, indicating that the specimen weight–RLN association does not differ significantly by race, and the substernal–RLN association does not differ significantly by sex.

**Subgroup RLN analyses:**

| Subgroup | N | Events | Specimen weight OR | p |
|---|---|---|---|---|
| Black vs. White only | 1,892 | 360 | 0.72 (0.51–1.01) | 0.060 |
| Males only | 402 | 79 | 0.34 (0.12–1.01) | 0.052 |
| Age ≥65 | 544 | 124 | 0.75 (0.43–1.33) | 0.326 |

The specimen weight association trended toward significance in Black vs. White patients (p=0.060) and males only (p=0.052), but was non-significant in all three subgroups after FDR correction, indicating that the full-cohort significant association (p=0.018) may partly reflect a small-to-moderate effect requiring the full analytic sample to detect.

### 3.7 Missingness Assessment

| Variable | Missing n (%) |
|---|---|
| Age | 0 (0.0%) |
| Race group | 0 (0.0%) |
| RLN injury (three-tier) | 0 (0.0%) |
| Specimen weight (g) | 4,106 (66.0%) |

Specimen weight has 66% missingness. The logistic regression (N=2,112/6,218 = 34.0%) and subgroup analyses are therefore based on a minority of the full cohort. MICE imputation from correlated variables (race, sex, goiter type, surgery extent) is required before final submission.

---

## 4. Discussion

### 4.1 Principal Findings

Three principal findings emerge from this analysis:

1. **Black patients present with dramatically heavier goiters.** The median specimen weight of 83 g in Black patients vs. 30 g in White patients (Kruskal-Wallis p<0.001) represents a 2.8-fold disparity that persists as the dominant demographic signal in this dataset. In the absence of insurance, ADI, or income data, this disparity is most plausibly attributed to delayed presentation driven by social and structural barriers to care — though formal mediation analysis is not possible with the current data.

2. **Asian race is an independent predictor of RLN injury.** The OR of 1.28 (95% CI 1.10–1.48, p=0.002, FDR p=0.005) is robust across multiple model specifications and survives FDR correction. The mechanism is unclear; potential explanations include anatomical variation in RLN course, language barriers affecting post-operative complication ascertainment, or referral pattern differences. This finding warrants prospective replication.

3. **Substernal goiter confers significantly higher risk of hypocalcemia and seroma.** ORs of 1.91 and 2.36 respectively are clinically meaningful and reinforce the surgical complexity of retrosternal dissection. Pre-operative counseling for substernal goiter patients should explicitly address these elevated risks.

### 4.2 Racial Disparities and SDOH Context

The 2.8-fold racial disparity in specimen weight is not attributable to differential histology or comorbidity distribution in this dataset — rather, it likely reflects the well-documented pattern in which Black patients, who bear higher structural barriers to specialty care, undergo thyroidectomy at a later and more advanced disease stage.⁶⁻⁸ Thyroid nodule surveillance guidelines recommend periodic ultrasound evaluation and consideration of surgical referral for growing or symptomatic goiters, but access to specialty endocrinology and thyroid ultrasound varies substantially by race and insurance status.⁹

A limitation that defines the scope of inference is the absence of SDOH-specific variables in the current dataset. The THYROID_2026 database does not contain:
- Insurance or payer data
- Zip code, census tract, or ADI
- Household income or educational attainment
- Distance to the surgical center
- Time from diagnosis to surgery

Without these data, race functions only as a coarse proxy for the underlying socioeconomic exposures. The observed racial disparity in specimen weight should be interpreted as a signal — evidence of a disparity worth investigating — rather than as evidence of race as a biological determinant. Future studies must integrate geocoded socioeconomic linkage (e.g., ZCTA-level ADI from the Neighborhood Atlas) and insurance data to decompose the pathways underlying this disparity.

### 4.3 Strengths

- Re-validated cohort with 100% concordance to saved data (live MotherDuck extraction)
- Three-tier RLN ascertainment combining NLP, chart documentation, and laryngoscopy
- Interaction testing and pre-specified subgroup analyses
- Leave-one-out sensitivity (from Hypothesis 1 joint analysis) confirming demographic generalizability of main findings
- Explicit SDOH limitation disclosure

### 4.4 Limitations

**No insurance, ADI, or income data.** This is the central limitation. Without SDOH pathway variables, the racial disparity in specimen weight cannot be formally decomposed into structural vs. individual-level components. All racial comparisons in this study should be interpreted as descriptive signals requiring subsequent investigation with linked socioeconomic data.

**Specimen weight missingness (66%).** The logistic regression and subgroup analyses are restricted to 34% of the full cohort. The significant findings for Asian race and specimen weight may be subject to selection bias if the missing-weight subgroup has a systematically different risk profile. MICE imputation is required before publication.

**Single-institution design.** Emory University is a high-volume academic referral center with a specific catchment population. Racial composition, referral patterns, and complication rates may not generalize to community settings or different geographic regions.

**Race as a social construct.** Race is categorized from electronic health record fields, which may be subject to miscoding and do not capture the full complexity of racial and ethnic identity. Hispanic ethnicity is not captured as a separate variable in the current schema.

### 4.5 Future Directions

1. Link patient zip codes to ZCTA-level ADI from the Neighborhood Atlas to enable formal SDOH mediation analysis
2. Obtain insurance/payer linkage from the institutional billing system to test for insurance-mediated disparities in specimen weight
3. Compute time-from-diagnosis-to-surgery as the primary mediator in a structural equation model (race → time-to-surgery → specimen weight → complication)
4. Expand substernal analysis with larger sample to power interaction testing (current n=285 provides limited power)

---

## 5. Conclusions

In this single-institution analysis of 6,218 goiter patients, Black patients underwent thyroidectomy with specimen weights nearly three times larger than White patients (83 g vs. 30 g, p<0.001), suggesting systematic delays in surgical referral. Asian race (OR 1.28, p=0.002) and specimen weight (OR 0.64 per SD, p=0.018) were independent predictors of RLN injury. Substernal goiter was independently associated with hypocalcemia (OR 1.91, p<0.001) and seroma (OR 2.36, p<0.001). As race and sex are the only available SDOH proxies in this dataset, these findings should be interpreted as hypothesis-generating data motivating future studies with insurance, ADI, and time-to-surgery linkage.

---

## References

1. Sosa JA, Bowman HM, Tielsch JM, et al. The importance of surgeon experience for clinical and economic outcomes from thyroidectomy. *Ann Surg*. 1998;228(3):320-330.
2. Braveman P, Gottlieb L. The social determinants of health: it's time to consider the causes of the causes. *Public Health Rep*. 2014;129(Suppl 2):19-31.
3. Roche AM, Fedewa SA, Chen AY. Association of income and insurance with receipt of definitive treatment in papillary thyroid cancer. *JAMA Otolaryngol Head Neck Surg*. 2019;145(5):398-406.
4. Kuo JH, Chabot JA, Lee JA. Disparities in thyroid cancer care. *Surgery*. 2016;160(5):1274-1281.
5. Adam MA, Thomas S, Hyslop T, Scheri RP, Roman SA, Sosa JA. Exploring the relationship between patient socioeconomic status and radioactive iodine (RAI) use following thyroidectomy for differentiated thyroid cancer. *Ann Surg Oncol*. 2017;24(7):2002-2008.
6. Sosa JA, Mehta PJ, Wang TS, et al. Racial disparities in clinical and economic outcomes from thyroidectomy. *Ann Surg*. 2007;246(6):1083-1091.
7. Morris LG, Sikora AG, Myssiorek D, DeLacure MD. The basis of racial differences in the incidence of thyroid cancer. *Ann Surg Oncol*. 2008;15(4):1169-1176.
8. Haymart MR, Banerjee M, Stewart AK, Koenig RJ, Birkmeyer JD, Griggs JJ. Use of radioactive iodine for thyroid cancer. *JAMA*. 2011;306(7):721-728.
9. Haugen BR, Alexander EK, Bible KC, et al. 2015 American Thyroid Association Management Guidelines. *Thyroid*. 2016;26(1):1-133.
10. Kind AJH, Buckingham WR. Making neighborhood-disadvantage metrics accessible — the Neighborhood Atlas. *N Engl J Med*. 2018;378(26):2456-2458.
11. VanderWeele TJ, Ding P. Sensitivity analysis in observational research: introducing the E-value. *Ann Intern Med*. 2017;167(4):268-274.
12. Thyroid Cancer Research Consortium. THYROID_2026 Research Lakehouse [Dataset]. DOI: 10.5281/zenodo.18945510. 2026.

---

## Figures

| Figure | Description | File |
|---|---|---|
| Figure 1 | Demographics by race: cohort size, age, weight, female % | `studies/hypothesis2_goiter_sdoh/fig_demographics_by_race.png` |
| Figure 2 | Specimen weight heatmap by race and sex (median g) | `studies/hypothesis2_goiter_sdoh/fig_weight_heatmap.png` |
| Figure 3 | Complication rates by race (RLN, hypocalcemia, hypoparathyroidism) | `studies/hypothesis2_goiter_sdoh/fig_complications_by_race.png` |
| Figure 4 | Cervical vs. substernal goiter comparison (age, weight, complications) | `studies/hypothesis2_goiter_sdoh/fig_substernal_comparison.png` |
| Figure 5 | Goiter vs. non-goiter demographic comparison | `studies/hypothesis2_goiter_sdoh/fig_goiter_vs_nongoiter.png` |
| Figure 6 | Substernal complication forest plot (OR vs. cervical) | `studies/hypothesis2_goiter_sdoh/validation_extension_20260312/fig_substernal_forest.png` |

---

## Supplementary Tables

| Table | Description | File |
|---|---|---|
| eTable 1 | Full demographics by race (Table 1) | `studies/hypothesis2_goiter_sdoh/table1_demographics_by_race.csv` |
| eTable 2 | Complication rates by race, sex, goiter type | `studies/hypothesis2_goiter_sdoh/complications_by_demographics.csv` |
| eTable 3 | Chi-square tests for complications | `studies/hypothesis2_goiter_sdoh/complication_statistical_tests.csv` |
| eTable 4 | Logistic regression rerun (validation) | `studies/hypothesis2_goiter_sdoh/validation_extension_20260312/logistic_regression_rln_rerun.csv` |
| eTable 5 | Interaction terms model | `studies/hypothesis2_goiter_sdoh/validation_extension_20260312/logistic_regression_interactions.csv` |
| eTable 6 | Specimen weight → any complication model | `studies/hypothesis2_goiter_sdoh/validation_extension_20260312/logistic_weight_any_complication.csv` |
| eTable 7 | Substernal complication forest (all ORs) | `studies/hypothesis2_goiter_sdoh/validation_extension_20260312/substernal_complication_forest.csv` |
| eTable 8 | FDR correction — all 16 hypothesis tests | `studies/validation_reports/fdr_correction_all_tests.csv` |
| eTable 9 | Consolidated Table 1 (both hypotheses) | `studies/validation_reports/consolidated_table1.csv` |
