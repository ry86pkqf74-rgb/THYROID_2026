# ETE manuscript — revision support packet

**Generated:** 2026-03-26  
**Scope:** Audit-first reconciliation for the Proposal 2 / AJCC 8th ETE staging manuscript.  
**Rule:** Quantitative claims in submission text should trace to frozen artifacts below; post-freeze exports and reruns are labeled explicitly.  
**Distribution:** This file lives on **GitHub `main`**; the Zenodo deposit ([10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510)) reflects an older snapshot until you publish a **new Zenodo version** — see [`docs/ZENODO_GITHUB_SYNC_NOTES_20260326.md`](../../docs/ZENODO_GITHUB_SYNC_NOTES_20260326.md).

---

## 1. Canonical freeze recommendation (source of truth)

**Primary anchor:** [`studies/proposal2_ete_staging/`](../../studies/proposal2_ete_staging/) as of the staged analysis bundle keyed by [`analysis_metadata.yaml`](../../studies/proposal2_ete_staging/analysis_metadata.yaml) (**audit_generated 2026-03-10**; **random seed 42**).

| Role | Path |
|------|------|
| Manifest + AUC + interaction summaries | [`analysis_metadata.yaml`](../../studies/proposal2_ete_staging/analysis_metadata.yaml) |
| Classic PTC narrative (N=596) | [`analysis_report.md`](../../studies/proposal2_ete_staging/analysis_report.md) |
| Expanded PTC audit (N=3,278) | [`audit_report.md`](../../studies/proposal2_ete_staging/audit_report.md) |
| Expanded analytic cohort | [`audit_tables/analytic_cohort_expanded.csv`](../../studies/proposal2_ete_staging/audit_tables/analytic_cohort_expanded.csv) |
| Classic analytic cohort | [`tables/analytic_cohort.csv`](../../studies/proposal2_ete_staging/tables/analytic_cohort.csv) |
| Ordinal regression (expanded CC) | [`audit_tables/table3_ordinal_regression.csv`](../../studies/proposal2_ete_staging/audit_tables/table3_ordinal_regression.csv) |
| PSM effect / balance | [`audit_tables/table6_propensity_matching_effect.csv`](../../studies/proposal2_ete_staging/audit_tables/table6_propensity_matching_effect.csv), [`audit_tables/table6_propensity_matching_balance.csv`](../../studies/proposal2_ete_staging/audit_tables/table6_propensity_matching_balance.csv) |
| Interactions (structural logistic) | [`audit_tables/table8_interaction_tests.csv`](../../studies/proposal2_ete_staging/audit_tables/table8_interaction_tests.csv) |
| PSM / table hashes (sub-freeze note) | [`outputs/manuscript_forensics_20260318/final_manuscript_dataset_provenance.json`](../../outputs/manuscript_forensics_20260318/final_manuscript_dataset_provenance.json) → `psm_freeze` |

**Post-freeze supplementary (CT timing only):** [`outputs/manuscript_forensics_20260318/ptc_ct_imaging_events.csv`](../../outputs/manuscript_forensics_20260318/ptc_ct_imaging_events.csv), [`ct_imaging_surgery_timing.csv`](../../outputs/manuscript_forensics_20260318/ct_imaging_surgery_timing.csv), and SQL logic in [`scripts/106_ct_imaging_date_recovery.py`](../../scripts/106_ct_imaging_date_recovery.py).

**Not the ETE quantitative source:** [`manuscripts/pool_malignancy_202603/manuscript_v1.md`](../pool_malignancy_202603/manuscript_v1.md) — Abstract opens with a **different** design (**6,630** patients, **1,497** matched pairs, Cox HR **~1.84**), inconsistent with this ETE package (**3,278** expanded PTC; **711** mETE vs no-ETE pairs on structural endpoint). **Action:** replace that Abstract/Methods block with ETE-specific text **or** use a separate draft file for the ETE paper.

**Blinded draft:** No separate blinded file was present in-repo beyond `manuscript_v1.md`. Treat **`analysis_report.md` / `audit_report.md` + table CSVs** as the numerical source of truth until a blinded DOCX path is supplied.

---

## 2. Discrepancy log

| Metric | Manuscript / draft text | Frozen audit value | Later forensics / rerun | Source file(s) | Likely reason | Recommended wording |
|--------|-------------------------|--------------------|-------------------------|----------------|---------------|---------------------|
| CT exams “701” vs “7,701” | Typo “701” | **7,701** CT exam rows (timing export, valid dates) | Same | `ct_imaging_surgery_timing.csv` | Transcription / wrong extract | Report **7,701** linked CT examination rows (institutional timing table). Do not use **701**. |
| CT exams (PTC extract) | — | **3,018** event-level rows; **650** patients | Provenance aligns | `ptc_ct_imaging_events.csv` | Second denominator for PTC-only | “In the PTC CT timing extract: **3,018** rows among **650** patients.” |
| “650 patients + pathologic + 1,245 events” | Single composite sentence | **650** = any CT row in PTC extract; **1,245** = rows with `ct_pathologic_ln_flag`; **331** patients ≥1 pathologic row | CSV sums | `ptc_ct_imaging_events.csv` | Conflated denominators | Split: (a) **n=650** with ≥1 CT timing row; (b) **n=331** with ≥1 pathologic-LN–flagged CT; (c) **1,245** pathologic-flagged **exams** (not patients). |
| “581 (46.7%) within 30 days of surgery” | Implies postoperative only | **581** = **508** preoperative + **73** perioperative (0–29 d postop) pathologic rows = **46.7%** of **1,245** | Same | `ptc_ct_imaging_events.csv` + script 106 | “Within 30 days” misread | “Of pathologic-flagged CTs, **581 (46.7%)** were **preoperative** (**n=508**) or **perioperative (0–29 days post-surgery; n=73**); **664** were ≥30 days post-surgery.” |
| Classic PTC N | 596 | **596** rows `tables/analytic_cohort.csv` | MET01: **589** after dedup | Crosswalk | Merge duplicates | Use **596** per frozen export; footnote **7** `research_id` collisions if reporting dedup N **589**. |
| Expanded PTC N | 3,278 | **3,278** | Same | `analysis_metadata.yaml` | — | Keep **3,278**. |
| Complete-case ordinal N | 3,269 | **3,269** (Cohort A CC) per `analysis_report.md` Table 6 / metadata | Forensics master `complete_case_ordinal`: **523** | `audit_tables/table3_ordinal_regression.csv` vs provenance JSON | Different endpoint / dataset build | For **expanded ordinal (Cohort A)**, cite **3,269** complete cases—not forensics **523**. |
| PSM matched pairs | 711 | **711** | Repro **2026-03-26:** **712** pairs (`revision_rerun_20260326/`) | `table6_propensity_matching_effect.csv` | sklearn / row-order noise | **Publish frozen 711**; cite rerun as sensitivity: “Current export replay yielded **712** pairs (±1).” |
| PSM structural OR / p | OR **1.434**, p **0.030** | Frozen CSV | Rerun: OR **1.304**, p **0.132** | effect CSV vs `table6_propensity_matching_effect_rerun.csv` | Software / numerical drift in propensity | **Primary:** frozen **1.434**, **0.030**. **Transparency:** “Blind re-execution on current exports produced OR **1.30** (p=**0.13**); inference is not stable to propensity implementation—interpret as hypothesis-generating.” |
| MET08 “503 pairs” | Crosswalk claim | **711** frozen | Not reproduced | `final_metric_crosswalk.csv` | Stale or erroneous rerun log | Treat **503** as **not verified**; supersede with frozen **711** or current rerun **712**. |
| Nodal covariate | Unclear | Ordinal: **`ln_ratio`**; PSM/structural: **`n_positive_flag`** (N1) | — | `table3_ordinal_regression.csv`; `proposal2_endpoint_psm_strata.py` L95, L154 | Prespecified model choice | Keep both; footnote model-specific definitions (see §5). |
| CT fields on forensics “final dataset” | — | PSM uses `imaging_correlation` flags | Many CT timing columns **null** in forensics extract | `manuscript_forensics_extract.py` | Build mismatch | CT timing prose must cite **script 106 + CSV exports**, not null-filled analytic dataset columns. |

---

## 3. Memo — reviewer science questions (exact numbers)

**Age as effect modifier (structural endpoint):** **No.** `mETE × age_at_surgery` OR **0.99** (95% CI **0.97–1.01**), **p = 0.258** — [`audit_tables/table8_interaction_tests.csv`](../../studies/proposal2_ete_staging/audit_tables/table8_interaction_tests.csv).

**Nodal status as effect modifier (structural endpoint):** **Yes.** `mETE × n_positive_flag` OR **0.36** (95% CI **0.17–0.74**), **p = 0.006** — same file. Interpretation: association of mETE with structural outcome **depends on N1** in this logistic parameterization.

**Baseline nodal burden (mETE vs no ETE):** Expanded cohort — N1 any **56.9%** (no) vs **67.2%** (micro) vs **74.7%** (gross), **p < 0.001** — [`audit_report.md`](../../studies/proposal2_ete_staging/audit_report.md) Table 1. Classic cohort — **44.3%** / **68.3%** / **64.8%**, **p < 0.001** — [`analysis_report.md`](../../studies/proposal2_ete_staging/analysis_report.md) Table 1.

**Age interaction vs subgroup attenuation:** Interaction term **not** significant on structural models (**p = 0.258**). Expanded ordinal sensitivity: age ≥55 **mETE** OR **0.87** (0.64–1.17), **p = 0.352** vs age <55 **0.44** (0.35–0.56), **p < 0.001** — [`audit_report.md`](../../studies/proposal2_ete_staging/audit_report.md) §Sensitivity: describe as **attenuation / heterogeneity**, not formal effect modification.

**Framing recurrence vs structural:** **Ordinal outcome** uses ATA-like **`recurrence_risk_band`** (composite: stage, gross ETE, Tg); **`ete_gross` OR is inflated** (circularity) — emphasize **`ete_micro`** for clinical interpretation ([`analysis_metadata.yaml`](../../studies/proposal2_ete_staging/analysis_metadata.yaml) `OUTCOME_CIRCULARITY`). **Structural endpoint** = **`ct_pathologic_ln_flag` OR `mri_pathologic_ln_flag` OR `reoperation_proxy`** — separate from composite risk band ([`proposal2_endpoint_psm_strata.py`](../../studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py) L97–117).

**PSM balance:** After matching, **`n_positive_flag` SMD = −0.58** (worse than pre-match **0.22**) — [`audit_tables/table6_propensity_matching_balance.csv`](../../studies/proposal2_ete_staging/audit_tables/table6_propensity_matching_balance.csv). State **residual nodal imbalance**; structural comparison is **supportive**, not definitive.

**Micro-ETE nodal burden (manuscript strength):** In classic Table 1, **N1 (any) 68.3%** with mETE vs **44.3%** without — cite with **LN examined/positive** medians as in table. Note **`ln_ratio`** is **quasi-binary** in ~83% of non-missing expanded cohort per [`analysis_metadata.yaml`](../../studies/proposal2_ete_staging/analysis_metadata.yaml) audit `LN_RATIO_QUALITY`; **84%+** missing in forensics patient layer for ratio fields — pair ordinal findings with **N1** description.

---

## 4. Manuscript-ready text blocks

### 4.1 Abstract (replacement sentences — only if current Abstract is generic PSM/cox)

Use after cohort sentence; **do not** use 6,630 / 1,497 / HR 1.84 for this ETE manuscript.

> We studied **3,278** patients with papillary thyroid carcinoma (PTC) from a curated institutional cohort; a classic-variant subset comprised **596** patients. Extrathyroidal extension was classified as absent, microscopic (mETE), or gross. The primary analysis modeled ATA recurrence risk category (ordinal) under AJCC 8th staging. A pre-specified secondary analysis defined a structural burden endpoint combining imaging-based pathologic cervical lymphadenopathy on cross-sectional imaging and a reoperation indicator, and compared mETE with no ETE after **1:1** propensity matching (**711** pairs) on age, sex, tumor size, and N1 status. In matched analysis, structural burden was more frequent with mETE (absolute difference **3.9** percentage points; odds ratio **1.43**, Fisher **p = 0.03**), with **imperfect post-match balance for nodal disease** (standardized mean difference **−0.58** for N1). On formal interaction testing, **mETE×age was not significant** (**p = 0.26**), while **mETE×N1 was significant** (**p = 0.006**). CT timing was evaluated in linked examination-level data (**7,701** examinations across all histologies; **3,018** PTC rows in the timing extract).

*(Shorten for journal word limit; numbers match frozen tables.)*

### 4.2 Variable definitions (Methods fragment)

> **ETE classification.** Gross ETE corresponded to `tumor_1_gross_ete` = 1. Any pathologic extrathyroidal extension without gross criteria was classified as microscopic ETE (`tumor_1_extrathyroidal_ext` true and not gross). Absence of extension was classified as no ETE.

> **Primary ordinal outcome.** Recurrence risk band (`recurrence_risk_band`: low, intermediate, high) was derived from institutional staging, gross ETE, and thyroglobulin trajectory rules consistent with materialized risk features (see Data Source). Because high risk assigns **all** gross ETE patients to the highest category, **gross ETE coefficients in ordinal models are not interpreted independently**; **microscopic ETE** is the primary contrast of interest.

> **Structural endpoint.** For secondary analyses, `structural_recurrence` was 1 if either: (i) the imaging correlation layer indicated **pathologic lymphadenopathy on CT or MRI** (`ct_pathologic_ln_flag` or `mri_pathologic_ln_flag`), or (ii) **more than one distinct surgery date** appeared in the pathology-linked surgery table (`reoperation_proxy`).

> **Nodal variables.** **N1 status** (`n_positive_flag`: `n_stage_ajcc8` beginning with “N1”) was used in propensity matching and structural logistic models. **Lymph node ratio** (`ln_ratio`) was used in **ordinal** models as pre-specified; in our data `ln_examined` was highly discrete (often binary), so **`ln_ratio` behaved as a quasi-binary positivity burden** with substantial missingness—results are presented alongside **N1** (Table 1).

### 4.3 Results — age, mETE, nodes, recurrence / structural (paragraph)

> Baseline **N1** prevalence differed across ETE groups (expanded cohort: **56.9%** no ETE, **67.2%** mETE, **74.7%** gross ETE; *P* < 0.001). After adjustment in the **expanded complete-case ordinal model**, older age predicted higher risk category (OR **1.05** per year, *P* < 0.001) and **`ln_ratio`** retained a positive association (OR **1.31**, *P* = 0.032), while **mETE** remained associated with **lower** odds of higher risk category (OR **0.60**, 95% CI **0.51–0.72**). **Separate from that composite outcome**, propensity-matched comparison on the **structural** endpoint yielded higher structural burden in mETE (**10.6%** vs **14.5%**; OR **1.43**, Fisher *P* = 0.030) but **worsened balance for N1** after matching (standardized mean difference **−0.58**). Formal **interaction** testing on the structural scale showed **no mETE×age** interaction (OR **0.99**, *P* = 0.26) but a **significant mETE×N1** interaction (OR **0.36**, *P* = 0.006). **Sensitivity:** among patients ≥55 years, the expanded ordinal mETE OR attenuated toward null (**0.87**, *P* = 0.35) versus younger patients (**0.44**, *P* < 0.001), consistent with descriptive heterogeneity rather than a significant age-interaction term on the structural endpoint.

### 4.4 Results — CT timing (paragraph)

> Cross-sectional CT examinations with valid dates were linked to index thyroidectomy using surgery dates from the synoptic pathology layer ([`scripts/106_ct_imaging_date_recovery.py`](../../scripts/106_ct_imaging_date_recovery.py)). **7,701** examination rows populated the institutional timing table. In the **PTC timing extract** used for descriptive reporting, **3,018** rows among **650** patients were available. Among **1,245** CT **examinations** flagged for radiographic pathologic lymphadenopathy, **581 (46.7%)** occurred **before surgery (**n = 508**)** or **within the first 29 postoperative days (**n = 73**)**, and **664** occurred **≥30 days** after surgery. **331** patients had at least one pathologic-flagged CT row. **Timing buckets** were defined as: preoperative (exam date **<** surgery); perioperative ( **≥** surgery and **< 30 days** post-surgery); 30–364 days; ≥365 days. The indicator “≥30 days after surgery” (`post_30d_flag`) is **relative to surgery** and is **0 for all preoperative rows**.

### 4.5 Discussion — nodal effect modification vs age attenuation (paragraph)

> We separated **three distinct issues**: (i) **baseline** higher nodal prevalence in mETE relative to no ETE in expanded cohort Tabulations; (ii) **formal interaction** on the structural logistic scale, which was **significant for N1** but **not for age**; and (iii) **descriptive attenuation** of the microscopic ETE association with **composite** risk category among older adults in ordinal sensitivity analyses—this pattern should **not** be labeled as effect modification without a significant product term. Propensity matching did **not** achieve satisfactory **nodal balance** (SMD **−0.58** for N1 despite inclusion in the propensity model), so structural comparisons should be framed as **hypothesis-generating** and potentially confounded by residual lymphatic burden.

### 4.6 Micro-ETE — lymph node burden (added sentence)

> Microscopic ETE mapped to **substantially higher pathologic N1 rates** than no ETE in both classic and all-PTC cohorts (classic: **68.3%** vs **44.3%**; expanded: **67.2%** vs **56.9%**), indicating that **mETE in this database identifies patients with heavier baseline nodal involvement** independent of its association with composite recurrence-risk category on multivariable ordinal analysis.

### 4.7 Table footnotes (suggested)

**Table 2 (staging / migration).** AJCC 7th T-stages were derived for comparison; **T3b mapped to T3** under AJCC 7th (not T4a)—**346** patient-level T-stage revisions on audit ([`analysis_metadata.yaml`](../../studies/proposal2_ete_staging/analysis_metadata.yaml) `AJCC7_T3b_MAP`).

**Table 3 (Cox / survival, if present).** Survival models query `risk_enriched_mv` and may include a broader row count than the **3,278** PTC CSV; denominators must match the reported filter.

**Table 4 (ordinal regression).** Outcome is **ordinal recurrence risk** (`risk_ord`). Covariates include **`ln_ratio`** (see missingness / quasi-binary note). **Gross ETE** coefficients are **not interpreted** owing to inclusion of gross ETE in risk-band derivation.

**Supplement — CT.** Examination counts refer to **row-level** extracts; **650** patients = any row in PTC CT file; **331** = ≥1 **pathologic** row; **1,245** = pathologic **exams**. **581** combines **preoperative** and **0–29 day postoperative** pathologic exams.

---

## 5. Statistical Analysis (journal-style subsection)

**Design.** Retrospective cohort study of patients with PTC in an institutional thyroid database with pathologist-documented ETE and AJCC 8th staging.

**Cohort construction.** The expanded analytic file merged deduplicated PTC rows from `recurrence_full.csv`, `ptc_full.csv`, and patient-level imaging flags from `imaging_correlation.csv` ([`proposal2_endpoint_psm_strata.py`](../../studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py) `load_expanded`). Classic-variant restrictions for the **596-patient** primary descriptive and ordinal sensitivity cohort followed [`proposal2_ete_analysis.py`](../../studies/proposal2_ete_staging/proposal2_ete_analysis.py).

**Primary and secondary endpoints.** The **primary oncology stratification outcome** for ordinal regression was **three-level recurrence risk** (`recurrence_risk_band`, ordinalized as `risk_ord`). The **secondary structural endpoint** (`structural_recurrence`) combined CT/MRI **pathologic lymphadenopathy** flags and a **reoperation** proxy (more than one surgery date per patient in the pathology-linked surgery listing).

**CT timing (descriptive).** CT dates were recovered and aligned to first surgery date; timing categories were computed in SQL as in [`scripts/106_ct_imaging_date_recovery.py`](../../scripts/106_ct_imaging_date_recovery.py) (preoperative; perioperative days 0–29; postoperative days 30–364; ≥365 days).

**Covariates.** Ordinal models adjusted for **age at surgery**, **sex**, **largest tumor diameter**, and **`ln_ratio`**. Structural / matching models used **age**, **sex**, **tumor size**, and **N1 indicator** (`n_positive_flag` from `n_stage_ajcc8`).

**Missing data.** **`ln_ratio`** had extensive missingness in broad extracts; expanded complete-case ordinal analyses used **3,269** patients; **multiple imputation** (**m = 20**, predictive mean matching with jitter, **seed 42**, Rubin pooling) was performed in sensitivity scripts ([`proposal2_recommendations.py`](../../studies/proposal2_ete_staging/proposal2_recommendations.py), [`proposal2_expanded_cohort.py`](../../studies/proposal2_ete_staging/proposal2_expanded_cohort.py)).

**Ordinal logistic regression.** Cumulative logit (proportional odds) models were fit via `statsmodels`; **proportionality** was assessed in audit output — large coefficient differences across cut-points for **gross ETE** were flagged ([`audit_report.md`](../../studies/proposal2_ete_staging/audit_report.md) PROP_ODDS); consider as limitation.

**Discrimination.** High-versus-not-high risk discrimination was summarized with **ROC AUC** and **5-fold cross-validated AUC** (base vs full model with mETE) per [`analysis_metadata.yaml`](../../studies/proposal2_ete_staging/analysis_metadata.yaml).

**Propensity score matching.** Among patients without gross ETE, **microscopic ETE** was compared to **no ETE** using **logistic regression propensity scores** with covariates **age**, **sex**, **tumor size**, and **N1**; **1:1 greedy nearest-neighbor matching** without replacement was applied within **caliper 0.05** on the propensity scale (**seed 42**). Balance was assessed with **standardized mean differences** before and after matching. Effects on binary structural burden used **Fisher’s exact test** and **Haldane–Anscombe–adjusted** odds ratios as implemented.

**Kaplan–Meier and log-rank.** Matched **DFS proxy** curves used time from surgery to last thyroglobulin date with **structural event** indicator ([`proposal2_endpoint_psm_strata.py`](../../studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py) `plot_matched_dfs`).

**Interactions and stratification.** Logistic models tested multiplicative **mETE×tumor size**, **mETE×age**, and **mETE×N1** terms. Pre-specified tumor-size strata used **≤1**, **1–2**, and **2–4 cm** bins.

**Sensitivity.** Aggressive-histology subsets, multiple imputation, age thresholds, and expanded cohorts A–D were run as documented in [`analysis_report.md`](../../studies/proposal2_ete_staging/analysis_report.md) / [`audit_report.md`](../../studies/proposal2_ete_staging/audit_report.md).

**Software.** **Python 3.14.2**; **pandas 2.3.3**, **numpy 2.4.3**, **scipy 1.17.1**, **statsmodels 0.14.6**, **scikit-learn 1.8.0**, **lifelines 0.30.3** per [`analysis_metadata.yaml`](../../studies/proposal2_ete_staging/analysis_metadata.yaml). **Two-sided α = 0.05**. **Random seed 42** for stochastic steps.

---

## 6. Reviewer-facing shorter “framework” paragraph

> We prespecified two complementary estimands. **First**, an **ordinal logistic model** for institutionally derived **recurrence risk category** adjusted for age, sex, size, and **lymph node ratio**, acknowledging that **gross ETE largely determines the highest category** (so gross ETE coefficients are not interpreted causally) and that **node ratio is sparse/quasi-binary**. **Second**, a **binary structural burden endpoint** (imaging pathologic nodes **or** reoperation) with **1:1 propensity matching** of mETE to no ETE on age, sex, size, and **N1**, reporting Fisher and standardized differences; we highlight **worsened N1 balance post-match** and a **significant mETE×N1 interaction** but **no mETE×age interaction**. **CT results** are purely descriptive and use **examination-level** extracts with explicit pre- versus post-surgical timing relative to index surgery.

---

## 7. Exact numeric checklist

| Quantity | Value |
|----------|------:|
| Expanded PTC N | 3278 |
| Classic analytic N | 596 |
| Expanded ordinal mETE OR (CC) | 0.603 |
| Expanded ordinal mETE 95% CI | 0.505–0.720 |
| Expanded ordinal age OR | 1.050 |
| Expanded ordinal ln_ratio OR | 1.309 |
| MI mETE OR (metadata primary) | 0.602 |
| AUC base CV mean / full | 0.851 / 0.876 |
| mETE T-downstage n / % | 1241 / 71.5% |
| PSM pairs (frozen) | 711 |
| No ETE structural % / mETE % | 10.55 / 14.49 |
| Structural OR (frozen) | 1.434 |
| Fisher p (frozen) | 0.030 |
| SMD N1 after match | −0.576 |
| mETE×age p | 0.258 |
| mETE×N1 p | 0.006 |
| CT rows all timing | 7701 |
| PTC CT rows / patients | 3018 / 650 |
| Pathologic CT rows / patients | 1245 / 331 |
| Preop + perioperative pathologic rows | 508 + 73 = 581 (46.7% of 1245) |
| PSM rerun (2026-03-26) pairs | 712 |
| PSM rerun OR / p | 1.304 / 0.132 |

---

## 8. Verification map (major numbers)

| Claim | Evidence |
|------|----------|
| N = 3278, ETE distribution | [`analysis_metadata.yaml`](../../studies/proposal2_ete_staging/analysis_metadata.yaml) `cohort`; [`audit_tables/analytic_cohort_expanded.csv`](../../studies/proposal2_ete_staging/audit_tables/analytic_cohort_expanded.csv) |
| Ordinal ORs | [`audit_tables/table3_ordinal_regression.csv`](../../studies/proposal2_ete_staging/audit_tables/table3_ordinal_regression.csv) |
| AUC | [`analysis_metadata.yaml`](../../studies/proposal2_ete_staging/analysis_metadata.yaml) `auc` |
| PSM effect | [`audit_tables/table6_propensity_matching_effect.csv`](../../studies/proposal2_ete_staging/audit_tables/table6_propensity_matching_effect.csv) |
| PSM balance | [`audit_tables/table6_propensity_matching_balance.csv`](../../studies/proposal2_ete_staging/audit_tables/table6_propensity_matching_balance.csv) |
| Interactions | [`audit_tables/table8_interaction_tests.csv`](../../studies/proposal2_ete_staging/audit_tables/table8_interaction_tests.csv) |
| CT timing SQL | [`scripts/106_ct_imaging_date_recovery.py`](../../scripts/106_ct_imaging_date_recovery.py) L100–133 |
| CT CSV counts | `THYROID_2026/outputs/manuscript_forensics_20260318/ptc_ct_imaging_events.csv`, `ct_imaging_surgery_timing.csv` (summarized 2026-03-26) |
| Structural / PSM code | [`proposal2_endpoint_psm_strata.py`](../../studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py) L83–117, L150–232 |
| PSM rerun | [`revision_rerun_20260326/run_psm_reproduction.py`](revision_rerun_20260326/run_psm_reproduction.py), outputs `*_rerun.csv` |

---

## 9. Items that remain uncertain / need author decision

1. **Exact blinded DOCX** not in repo — reconcile figure/table numbering when file is available.  
2. **PSM numerical instability:** frozen **1.434** vs rerun **1.30** with current sklearn — report both or lock environment to **scikit-learn 1.8.0** from metadata for replication.  
3. **MET08 “503 pairs”** not reproduced — treat as erroneous unless prior log is recovered.  
4. **Forensics `complete_case_ordinal = 523`** vs **3,269** — do not cite **523** for expanded ordinal without reconciling dataset definition.

---

## Appendix A — `manuscript_v1.md` vs ETE freeze (numeric QC)

[manuscript_v1.md](../pool_malignancy_202603/manuscript_v1.md) mixes Introduction text appropriate to AJCC 8 / mETE with **body tables and Abstract** that describe a **different** analysis (full malignant cohort, Cox-primary, **1,497** pairs).

| manuscript_v1 claim | ETE package / repo reality |
|---------------------|----------------------------|
| Analysis cohort **6,630** | Expanded **PTC** ETE file **3,278**; classic **596** |
| Matched pairs **1,497** | mETE vs no ETE structural PSM **711** (frozen) |
| HR **1.84** recurrence | ETE packet: ordinal **mETE OR ~0.60** on risk band; structural OR **1.43** (matched binary) |
| Recurrence **2,965 (44.7%)** in Table 1 | Stale / non–ETE-specific; institutional recurrence metrics per `manuscript_metrics_v2` differ (see repo docs) |
| Python **3.11**, script `31_analytic_models.py` | ETE freeze: **Python 3.14.2**; scripts in `proposal2_ete_staging/` |
| ETE × LN interaction **p=0.073** (Cox) | Structural logistic: **mETE × N1** **p=0.006**; **mETE × age** **p=0.26** |

**Conclusion:** Do not paste numbers from `manuscript_v1.md` Results/Abstract into the ETE AJCC 8 manuscript without replacement—use §4–7 of this packet.

---

*Packet produced under audit-first rules; local DuckDB untouched.*
