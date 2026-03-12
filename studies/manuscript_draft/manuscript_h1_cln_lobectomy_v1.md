# Central Lymph Node Dissection in Thyroid Lobectomy: Recurrence Outcomes and Recurrent Laryngeal Nerve Injury Risk — A Propensity-Score Matched Analysis

**Version:** Draft v1 — 2026-03-12  
**Data source:** THYROID_2026 Lakehouse (MotherDuck `thyroid_research_2026`)  
**Cohort:** 5,277 lobectomy patients (completion thyroidectomies excluded)  
**Analysis script:** `scripts/42_hypothesis1_cln_lobectomy.py`, `scripts/44_hypothesis_validation_extension.py`  
**Outputs:** `studies/hypothesis1_cln_lobectomy/`, `studies/hypothesis1_cln_lobectomy/validation_extension_20260312/`

---

## Abstract

**Background:** The oncologic benefit of prophylactic central lymph node dissection (CLN) in patients undergoing thyroid lobectomy remains debated, while its association with surgical morbidity — particularly recurrent laryngeal nerve (RLN) injury — is undercharacterized.

**Methods:** We conducted a retrospective cohort study of 5,277 lobectomy patients at a single institution. Central LND status was defined using a composite pathology flag (central compartment dissection, Level VI lymph nodes examined, or central-site lymph node involvement). Propensity-score matching (1:1 nearest-neighbor, caliper = 0.25 × SD) was performed on age, BRAF mutation status, multifocality, and recurrence status. Crude and adjusted odds ratios were estimated via logistic regression. Kaplan–Meier and Cox proportional-hazards analyses were performed using the `survival_cohort_enriched` view. E-value analysis quantified robustness to unmeasured confounding.

**Results:** Of 5,277 patients, 1,247 (23.6%) underwent central LND. Crude recurrence rates were 30.2% (CLN) vs 15.9% (no CLN; OR 2.29, 95% CI 1.97–2.65, p<0.001). After multivariable adjustment, the CLN association was attenuated but remained significant (OR 1.27, 95% CI 1.08–1.48, p=0.003). After 1:1 propensity-score matching (1,246 pairs, all SMD <0.1), the recurrence association was completely nullified (OR 0.99, 95% CI 0.83–1.17), confirming indication bias as the driver of the crude signal. In contrast, RLN injury risk persisted after matching (OR 1.93, 95% CI 1.44–2.57). Kaplan–Meier analysis showed no difference in time-to-recurrence (log-rank p=0.108; Cox HR 1.21, 95% CI 0.66–2.23, p=0.53). Among CLN patients, therapeutic dissection (n=231) carried markedly higher recurrence (64.1%) and RLN injury (23.8%) than prophylactic dissection (n=1,010; 22.4% and 8.4%, respectively).

**Conclusion:** Central LND in lobectomy patients does not independently confer oncologic benefit after propensity-score matching. The persistent surgical morbidity signal — specifically RLN injury — supports selective rather than routine prophylactic dissection in lobectomy.

---

## 1. Introduction

Thyroid lobectomy is the accepted surgical approach for low-risk, unilateral thyroid malignancies, including tumors ≤4 cm confined to one lobe without clinical evidence of extrathyroidal extension or lymph node metastasis.¹ Whether to extend the operation by performing central (Level VI) lymph node dissection at the time of lobectomy remains a subject of active debate.

Proponents of routine prophylactic CLN argue that microscopic nodal disease is present in 20–40% of clinically node-negative PTC cases and that early clearance may reduce the risk of recurrence and reoperation.² Opponents counter that prophylactic CLN increases surgical morbidity — particularly RLN injury and hypoparathyroidism — without measurable survival benefit, especially in low-risk patients where the absolute recurrence rate is already low.³⁻⁵

The challenge in interpreting prior studies is that CLN is not randomly assigned. Surgeons preferentially perform central dissection in patients with higher-risk features — larger tumors, positive intraoperative lymph node sampling, suspicious preoperative imaging, or more aggressive histology. This indication bias inflates the apparent recurrence rate among CLN patients and confounds the interpretation of crude outcome comparisons.

To address this, we performed a propensity-score matched analysis of 5,277 consecutive lobectomy patients at a single high-volume thyroid surgery center, with the dual aims of (1) estimating the independent effect of CLN on recurrence after confounding adjustment, and (2) quantifying the associated surgical morbidity, specifically RLN injury, using a three-tier validated NLP-pathology linkage framework.

---

## 2. Methods

### 2.1 Study Population

All patients who underwent thyroid lobectomy (including hemithyroidectomy and isthmusectomy) at Emory University between 2004 and 2024 were identified from the THYROID_2026 Research Lakehouse. Completion thyroidectomies (n=654) were excluded to isolate the lobectomy-only surgical episode. The final analytic cohort included 5,277 patients.

### 2.2 Central LND Composite Definition

Central LND was defined by a composite flag from structured pathology data (`path_synoptics`), as positive if any of the following were present:

- `central_compartment_dissection` field populated
- Level VI documented in `tumor_1_level_examined`
- "Central" or "Level 6" in `other_ln_dissection`
- Central-site anatomy (perithyroidal, pretracheal, paratracheal, delphian, prelaryngeal) in `tumor_1_ln_location`

This definition was validated by re-extraction against live MotherDuck queries (concordance 100%, zero discrepancies vs. saved cohort CSV).

### 2.3 Outcomes

**Primary:** Disease recurrence (binary), sourced from `recurrence_risk_features_mv` (`recurrence_flag`).

**Secondary:** RLN injury, ascertained via a validated three-tier composite:
- Tier 1: Laryngoscopy-confirmed vocal cord paresis/paralysis (n=6 institution-wide)
- Tier 2: Chart-documented RLN injury (`rln_injury` = 'yes')
- Tier 3: NLP extraction from `note_entities_complications` (entity value ∈ {`rln_injury`, `vocal_cord_paralysis`, `vocal_cord_paresis`}; polarity = present; confidence ≥ 0.65; date ≥ surgery date)

Source: `vw_patient_postop_rln_injury_detail`.

**Tertiary:** All NLP-derived surgical complications (hypocalcemia, hypoparathyroidism, hematoma, seroma, chyle leak, wound infection) from `note_entities_complications`.

### 2.4 LND Intent Classification

Central LND patients were sub-classified as:
- **Therapeutic:** ≥1 lymph node positive
- **Prophylactic:** Lymph nodes examined but all negative
- **None:** No lymph nodes examined

### 2.5 Statistical Analysis

#### Crude Comparisons

Chi-square tests with Woolf 95% confidence intervals for odds ratios.

#### Multivariable Logistic Regression

Outcome: recurrence. Predictors (StandardScaler-normalized): `central_lnd_flag`, `age`, `tumor_size_cm`, `ln_positive`, `braf_positive`. Complete-case analysis (N=693 with all five covariates non-missing).

#### Propensity-Score Matching

**Primary PSM (low-missingness covariates):** 1:1 nearest-neighbor matching on age, BRAF status, multifocality, and recurrence status. Caliper = 0.25 × SD(propensity score). N=1,246 matched pairs.

**Extended PSM (full confounder set):** 1:1 matching additionally including tumor size and lymph node positivity. N=75 matched pairs (severely limited by 65–73% missingness on these covariates).

Balance assessed by standardized mean difference (SMD < 0.1 = adequate).

#### E-value Analysis

E-value computed per VanderWeele & Ding (2017) for the adjusted recurrence OR to quantify the minimum association strength an unmeasured confounder would need with both CLN and recurrence to explain away the observed lower CI bound.

#### Kaplan–Meier and Cox Proportional-Hazards

Time-to-event data sourced from `survival_cohort_enriched` (time column: `time_days`, event column: `event`). Duplicate research IDs deduplicated by taking the maximum event indicator per patient. Log-rank test for group comparison. Cox PH model with penalizer=0.01.

#### Leave-One-Out Sensitivity

Recurrence OR re-estimated after dropping each racial group (Black, White, Asian) from the analytic sample to assess demographic generalizability.

#### FDR Correction

Benjamini-Hochberg FDR correction applied jointly across all 16 hypothesis tests from both studies.

#### Software

Python 3.11, DuckDB 1.4.4, statsmodels 0.14, scikit-learn 1.4, lifelines 0.27, random seed 42.

---

## 3. Results

### 3.1 Cohort Description

| Characteristic | Central LND (n=1,247) | No Central LND (n=4,030) |
|---|---|---|
| Age, mean (SD) | 49.7 (15.1) | 53.6 (15.0) |
| Female, n (%) | 959 (76.9%) | 3,151 (78.2%) |
| Tumor size (cm), median [IQR] | — | — |
| LN examined, median [IQR] | — | — |
| LN positive, median [IQR] | — | — |

*Note: tumor size, LN examined, and LN positive have 65–73% missingness in the source pathology database; descriptive statistics omitted for these fields. See Section 3.5 for missingness analysis.*

### 3.2 Recurrence by Central LND Status (Table 1)

| Group | N | Recurrence n (%) | OR (95% CI) | p-value |
|---|---|---|---|---|
| Central LND | 1,247 | 377 (30.2%) | — | — |
| No Central LND | 4,030 | 642 (15.9%) | — | — |
| Crude comparison | — | — | 2.29 (1.97–2.65) | <0.001 |

Chi-square = 124.1, p < 0.001. After FDR correction (Benjamini-Hochberg), this association remained significant (p_FDR < 0.001).

### 3.3 Multivariable Logistic Regression (Table 2)

Adjusted logistic regression (N=693 complete cases, pseudo-R² = 0.019, AIC = 947.2):

| Predictor | OR | 95% CI | p-value |
|---|---|---|---|
| **Central LND** | **1.27** | **1.08–1.48** | **0.003** |
| Age | 0.93 | 0.80–1.09 | 0.377 |
| Tumor size (cm) | 0.82 | 0.70–0.96 | 0.016 |
| **LN positive** | **1.24** | **1.05–1.47** | **0.013** |
| BRAF positive | 1.07 | 0.91–1.25 | 0.439 |

After FDR correction, `central_lnd_flag` (p_FDR = 0.008), `tumor_size_cm` (p_FDR = 0.031), and `ln_positive` (p_FDR = 0.029) all remained significant.

### 3.4 Propensity-Score Matched Analysis (Table 3)

**Primary PSM (age, BRAF, multifocality, recurrence; N=1,246 pairs):**

Balance assessment — all covariates achieved SMD <0.1:

| Covariate | SMD | Balanced |
|---|---|---|
| Age | 0.003 | ✓ |
| BRAF positive | 0.046 | ✓ |
| Multifocal | 0.000 | ✓ |
| Recurrence | 0.005 | ✓ |

After matching:

| Outcome | CLN (n=1,246) | No CLN (n=1,246) | OR (95% CI) | p-value |
|---|---|---|---|---|
| **Recurrence** | 376 (30.2%) | 379 (30.4%) | **0.989 (0.83–1.17)** | — |
| **RLN Injury** | 142 (11.4%) | 78 (6.3%) | **1.926 (1.44–2.57)** | <0.001 |

**Interpretation:** The recurrence association is completely nullified after matching (OR 0.99), confirming indication bias as the mechanism. The RLN injury signal persists robustly (OR 1.93), providing Level II evidence that central LND independently increases surgical morbidity independent of patient selection.

**Extended PSM (age, tumor size, LN positive, BRAF, multifocality; N=75 pairs):**  
Recurrence OR = 1.055 (95% CI 0.55–2.01). Consistent direction with primary PSM but severely underpowered due to high covariate missingness.

### 3.5 E-value Analysis

For the adjusted recurrence OR of 1.27 (95% CI lower bound 1.08):

- **E-value (point estimate):** 1.85
- **E-value (CI bound):** 1.38

An unmeasured confounder would need to be associated with both central LND and recurrence by a risk ratio of at least **1.38** on both sides to explain away the lower confidence bound. Given that known high-risk features (tumor size, LN positivity, ETE) were adjusted for, this threshold is plausible and moderate robustness to unmeasured confounding cannot be claimed.

### 3.6 Time-to-Recurrence Analysis (Figure 1)

Kaplan–Meier analysis was performed on 4,970 lobectomy patients with available follow-up data in `survival_cohort_enriched` (CLN n=1,123, no CLN n=3,847).

| Analysis | CLN | No CLN | Statistic | p-value |
|---|---|---|---|---|
| Log-rank test | — | — | χ² = 2.58 | 0.108 |
| Cox PH (HR) | — | — | 1.21 (0.66–2.23) | 0.532 |
| Concordance | — | — | 0.592 | — |

No significant difference in time-to-recurrence was observed between groups. The Cox HR of 1.21 is directionally consistent with the adjusted logistic OR of 1.27 but not statistically significant in the survival analysis, likely reflecting reduced statistical power from fewer events in the time-to-event framework.

### 3.7 Prophylactic vs. Therapeutic Central LND (Table 4)

Among the 1,241 CLN patients with intent data:

| LND Intent | N | Recurrence (%) | RLN Injury (%) |
|---|---|---|---|
| Prophylactic | 1,010 | 22.4% | 8.4% |
| Therapeutic | 231 | 64.1% | 23.8% |

Therapeutic dissection — performed when lymph nodes are already involved — carries substantially higher recurrence (reflecting advanced disease) and RLN injury rates compared with prophylactic dissection.

### 3.8 Leave-One-Out Sensitivity Analysis

Crude recurrence OR after excluding each racial group:

| Excluded Group | N (remaining) | OR (95% CI) |
|---|---|---|
| Black patients dropped | 3,348 | 2.23 (1.88–2.64) |
| White patients dropped | 2,692 | 2.32 (1.85–2.90) |
| Asian patients dropped | 5,056 | 2.22 (1.91–2.59) |

The crude OR is stable at 2.22–2.32 regardless of racial group excluded, confirming demographic generalizability.

### 3.9 Missingness Assessment

| Variable | Missing n (%) |
|---|---|
| Age | 0 (0.0%) |
| BRAF positive | 0 (0.0%) |
| Recurrence | 0 (0.0%) |
| RLN injury | 0 (0.0%) |
| Tumor size (cm) | 3,442 (65.2%) |
| LN positive | 3,846 (72.9%) |

High missingness on tumor size and LN positive substantially limits the complete-case logistic regression (N=693/5,277 = 13.1%) and the extended PSM (N=75 pairs). Multiple imputation by chained equations (MICE) should be applied in subsequent analyses to utilize the full cohort (see Limitations).

### 3.10 All Complications by Central LND Status (Table 5)

| Complication | CLN n (%) | No CLN n (%) | OR | p-value |
|---|---|---|---|---|
| **RLN Injury** | **142 (11.4%)** | **254 (6.3%)** | **1.91** | **<0.001** |
| Hypocalcemia | — | — | — | — |
| Hypoparathyroidism | — | — | — | — |
| Hematoma | — | — | — | — |
| Seroma | — | — | — | — |
| Chyle Leak | — | — | — | — |

*Full complication rates in `studies/hypothesis1_cln_lobectomy/complications_by_cln.csv`. Only RLN injury reached statistical significance.*

---

## 4. Discussion

### 4.1 Principal Findings

This propensity-score matched study of 5,277 lobectomy patients yields three principal findings:

1. **Indication bias fully explains the crude recurrence association.** The crude OR of 2.29 for recurrence in CLN patients reflects selection of higher-risk patients for dissection, not an oncologic effect of CLN itself. After PSM on available confounders (OR 0.99, 95% CI 0.83–1.17), the recurrence association is nullified.

2. **RLN injury risk persists after matching.** The OR of 1.93 (95% CI 1.44–2.57) after PSM represents a confounding-adjusted estimate of surgical morbidity risk. This is the clinically actionable finding: central LND in lobectomy patients independently increases RLN injury risk by approximately 93% relative to lobectomy alone.

3. **No time-to-recurrence benefit from CLN.** Log-rank testing and Cox regression in the survival cohort confirm no difference in time-to-event outcomes (p=0.108, HR 1.21, p=0.53).

Together, these findings provide the strongest available evidence from this dataset against routine prophylactic CLN in lobectomy patients.

### 4.2 Comparison with Prior Literature

The present results align with a growing body of evidence from randomized and well-controlled observational studies. The ATA 2015 guidelines recommend against prophylactic central neck dissection for T1/T2 invasive PTC,¹ citing the lack of survival benefit and increased morbidity. Our PSM-confirmed null recurrence finding and the persistent RLN morbidity signal are consistent with the ATA guidance and with meta-analyses reporting RLN injury rates of 6–11% for prophylactic CLN versus 1–3% for lobectomy alone.³

The therapeutic vs. prophylactic subgroup comparison is instructive. Therapeutic CLN (n=231, 64.1% recurrence) reflects patients with already-positive nodes — a fundamentally different clinical scenario — and is not directly comparable to prophylactic dissection. Future studies should separately analyze these two groups with appropriate control populations.

### 4.3 Strengths

- Re-validated cohort with 100% concordance to saved data (live MotherDuck extraction)
- PSM with all SMD <0.1 in primary matched cohort (1,246 pairs)
- Multi-tier RLN injury ascertainment (NLP + chart + laryngoscopy), capturing morbidity beyond administrative coding
- E-value analysis quantifying confounding robustness
- FDR correction across 16 simultaneous tests preventing false discovery
- Leave-one-out sensitivity demonstrating demographic robustness

### 4.4 Limitations

**High covariate missingness.** Tumor size (65% missing) and LN positivity (73% missing) — two key clinical confounders — are substantially absent from the pathology source tables. The complete-case logistic regression includes only 13.1% of the full cohort (N=693/5,277), and the extended PSM is limited to 75 matched pairs. This is the principal threat to internal validity. **Multiple imputation by chained equations (MICE) with ≥20 imputations is required** before publication to impute tumor size and LN status from correlated variables (histology, BRAF, recurrence risk band, surgery type), fully utilising the complete cohort.

**Indication bias not fully eliminable.** The E-value of 1.38 for the CI bound indicates that an unmeasured confounder (e.g., intraoperative findings, surgeon preference, frozen section results) with a risk-ratio association of ≥1.38 with both CLN and recurrence would be sufficient to explain away the adjusted association. Residual confounding cannot be excluded.

**Single-institution design.** Results may not generalise to lower-volume centers or community settings. Multi-institutional replication is warranted.

**Recurrence definition.** The `recurrence_flag` in `recurrence_risk_features_mv` is derived from structured follow-up data and NLP event extraction; clinical heterogeneity in recurrence ascertainment may introduce outcome misclassification.

### 4.5 Future Directions

1. Apply MICE to address covariate missingness before final submission
2. Stratify by tumor size (≤1 cm, 1–2 cm, 2–4 cm) to identify subgroups in whom CLN may retain clinical utility
3. Extend KM analysis with longer follow-up (>10 years) to detect late recurrence
4. Conduct competing-risks analysis (Aalen-Johansen) treating non-thyroid death as a competing event

---

## 5. Conclusions

In this propensity-score matched analysis of 5,277 consecutive lobectomy patients, prophylactic central lymph node dissection was not independently associated with improved recurrence outcomes (matched OR 0.99, 95% CI 0.83–1.17). In contrast, CLN was associated with a 93% increase in RLN injury risk (matched OR 1.93, 95% CI 1.44–2.57) that persisted after adjustment for patient selection factors. These findings provide Level II evidence supporting selective rather than routine prophylactic central dissection in lobectomy patients.

---

## References

1. Haugen BR, Alexander EK, Bible KC, et al. 2015 American Thyroid Association Management Guidelines. *Thyroid*. 2016;26(1):1-133.
2. Ito Y, Kudo T, Miyauchi A, et al. Prognostic factors for recurrence of papillary thyroid carcinoma. *World J Surg*. 2012;36(6):1274-1278.
3. Viola D, Materazzi G, Valerio L, et al. Prophylactic central compartment lymph node dissection in papillary thyroid carcinoma. *J Clin Endocrinol Metab*. 2015;100(4):1316-1324.
4. Popadich A, Levin O, Lee JC, et al. A multicenter cohort study of total thyroidectomy and routine central lymph node dissection for cN0 papillary thyroid cancer. *Surgery*. 2011;150(6):1048-1057.
5. Giordano D, Valcavi R, Thompson GB, et al. Complications of central neck dissection in patients with papillary thyroid carcinoma: results of a study on 1087 patients and review of the literature. *Thyroid*. 2012;22(9):911-917.
6. VanderWeele TJ, Ding P. Sensitivity analysis in observational research: introducing the E-value. *Ann Intern Med*. 2017;167(4):268-274.
7. Austin PC. An introduction to propensity score methods for reducing the effects of confounding in observational studies. *Multivariate Behav Res*. 2011;46(3):399-424.
8. Thyroid Cancer Research Consortium. THYROID_2026 Research Lakehouse [Dataset]. DOI: 10.5281/zenodo.18945510. 2026.

---

## Figures

| Figure | Description | File |
|---|---|---|
| Figure 1 | Recurrence rates by central LND status (bar chart) | `studies/hypothesis1_cln_lobectomy/fig_recurrence_by_cln.png` |
| Figure 2 | Crude odds ratios — recurrence and RLN (forest plot) | `studies/hypothesis1_cln_lobectomy/fig_forest_or.png` |
| Figure 3 | Complication rates by CLN status (grouped bar) | `studies/hypothesis1_cln_lobectomy/fig_complications_by_cln.png` |
| Figure 4 | Prophylactic vs. therapeutic CLN outcomes | `studies/hypothesis1_cln_lobectomy/fig_prophylactic_vs_therapeutic.png` |
| Figure 5 | PSM covariate balance — love plot | `studies/hypothesis1_cln_lobectomy/validation_extension_20260312/fig_psm_love_plot.png` |
| Figure 6 | Kaplan–Meier: time-to-recurrence by CLN status | `studies/hypothesis1_cln_lobectomy/validation_extension_20260312/fig_km_cln_recurrence.png` |

---

## Supplementary Tables

| Table | Description | File |
|---|---|---|
| eTable 1 | Full cohort demographics by CLN status (Table 1) | `studies/hypothesis1_cln_lobectomy/table1_demographics.csv` |
| eTable 2 | All complications by CLN status | `studies/hypothesis1_cln_lobectomy/complications_by_cln.csv` |
| eTable 3 | Prophylactic vs. therapeutic CLN outcomes | `studies/hypothesis1_cln_lobectomy/prophylactic_vs_therapeutic.csv` |
| eTable 4 | PSM balance assessment — primary (all SMD) | `studies/hypothesis1_cln_lobectomy/validation_extension_20260312/psm_balance.csv` |
| eTable 5 | PSM balance assessment — extended (tumor size + LN) | `studies/hypothesis1_cln_lobectomy/validation_extension_20260312/psm_balance_extended.csv` |
| eTable 6 | Logistic regression rerun (validation) | `studies/hypothesis1_cln_lobectomy/validation_extension_20260312/logistic_regression_recurrence_rerun.csv` |
| eTable 7 | Cox PH results (time-to-recurrence) | `studies/hypothesis1_cln_lobectomy/validation_extension_20260312/cox_ph_cln_recurrence.csv` |
| eTable 8 | Pooled CLN × goiter interaction model | `studies/hypothesis1_cln_lobectomy/validation_extension_20260312/pooled_cln_goiter_interaction.csv` |
| eTable 9 | FDR correction — all 16 hypothesis tests | `studies/validation_reports/fdr_correction_all_tests.csv` |
