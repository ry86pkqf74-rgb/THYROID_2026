# Initial thyroidectomy extent among adults with preoperative 2.0–4.0 cm thyroid nodules: a retrospective cohort study

**Version:** submission v1 (markdown); **figures revision** 2026-03-26  
**Frozen outputs:** See `analysis_manifest.json` (git SHA and run timestamp).  
**Quantitative traceability:** `CLAIM_SOURCE_LEDGER.md`  
**Working bibliography:** `references_working_20260326.md`

---

## Abstract

See `abstract_structured_v1.md` (structured abstract aligned to this manuscript).

---

## Introduction

Contemporary management of differentiated thyroid cancer in the **>2 to ≤4 cm** intrathyroidal setting allows either lobectomy or total thyroidectomy for many patients; current guidelines emphasize tumor size, cytologic risk, multifocality, and patient preference when considering lobectomy versus total thyroidectomy when framing extent options.[1] Prior work documents substantial variation and temporal shifts in surgical extent following modern guideline adoption.[2,3] Within the **1–4 cm to 2–4 cm** decision range, observational studies show that both lobectomy and total thyroidectomy remain in use and that surgical choice remains heterogeneous across cohorts and practice settings.[6]

We examined **cross-sectional associations** between preoperative patient and cytology characteristics (and limited preoperative molecular testing) and **initial total thyroidectomy** versus **initial lobectomy** among adults with an ultrasound-defined **2.0–4.0 cm** index nodule on or before the first qualifying hemithyroidectomy or total thyroidectomy in an integrated thyroid research database. This analysis describes **surgical decision-associated factors** measured preoperatively; it **does not** estimate recurrence, survival, or causal effects of extent on oncology outcomes. Reporting follows STROBE guidance for observational studies.[4]

---

## Methods

### Study design and data source

We performed a **retrospective cohort study** using analytic tables queried in the study pipeline [`study_pipeline.py`](study_pipeline.py). MotherDuck objects were **not** modified during manuscript preparation; all numbers in this version are derived from **frozen** CSV exports in this folder unless otherwise noted (`MANUSCRIPT_STATE_AUDIT.md`).

### Eligibility and cohorts

**Surgical spine.** We identified patients whose **first** thyroid-directed procedure meeting inclusion criteria was either `hemithyroidectomy` or `total_thyroidectomy` (`operative_episode_detail_v2`, procedure normalization as implemented in code). Earlier episodes classified as `unknown` or `other` were allowed upstream in flow logic before narrowing to hemithyroidectomy and total thyroidectomy only (see `cohort_flow.csv` / pipeline `run()`).

**Index nodule size (primary analysis).** The **primary** analysis cohort was restricted to patients whose **preoperative imaging** index nodule had maximum dimension **2.0–4.0 cm** on an exam dated **on or before** the index surgery date, using imaging linkage rules in `cohort_logic.preop_imaging_size_cohort` and `imaging_nodule_long_v2` / `imaging_nodule_master_v1` as wired in `study_pipeline.load_core`.

**Nodal exclusions (strict).** We excluded patients with **definite preoperative lymph-node involvement** by a strict rule: pathologically enlarged lymph nodes on preoperative CT or MRI (`ct_imaging.pathologic_lymph_nodes`), or Bethesda VI on a **node** FNA specimen (`supplement_exclusions_and_definitions.csv`: `strict_ln_exclusion`). This rule is implemented in `cohort_logic.strict_ln_exclusion`.

**Sensitivity cohort (broader imaging suspicion).** We defined an alternative preoperative cohort using the same 2.0–4.0 cm imaging window but applying **broad** suspicious-node exclusion (`broad_sensitivity` in `supplement_exclusions_and_definitions.csv`): any suspicious node at exam level or nodule `suspicious_node_flag`.

**Pathology-defined size sensitivity.** The pipeline also derives a pathology-based size cohort (linked pathology size 2.0–4.0 cm with fallbacks per `cohort_logic.add_pathology_size`). In the **current frozen run**, the **pathology-defined analysis set after strict nodal exclusion has N = 0** (`cohort_build_log.md`, `analysis_manifest.json`). No pathology-size sensitivity results are reported here beyond this fact.

### Outcome and predictors

**Primary outcome for regression.** Binary **`initial_total`**, equal to 1 if the first qualifying procedure was `total_thyroidectomy`, else 0 among the analytic rows (mutually exclusive with `initial_lobectomy` for the main lobectomy-vs-total comparison).

**Preoperative predictors (primary models).** Continuous `age_at_surgery`; indicator female sex (`sex_f`); indicator Bethesda category ≥4 (`bethesda_ge4`, treating missing Bethesda category as not ≥4 after coercion in pipeline); indicator any **preoperative** molecular test (`has_mol` / `preop_molecular_tested`).

**Extended model covariates.** `bilateral_nodule_indicator` and `tirads_score` (numeric), when present after merge, as in `study_pipeline.py` extended predictor list.

**Completion thyroidectomy.** Among patients with initial lobectomy, we summarized completion in **two layers**: (i) **OED pipeline flags**—a later `operative_episode_detail_v2` row with `procedure_normalized` in {`total_thyroidectomy`, `completion_thyroidectomy`} strictly after index surgery (`completion_total_flag`, `completion_within_*`; `cohort_logic.completion_after_lobectomy`); (ii) **path-synoptic definite completion**—a later-dated `path_synoptics` row with synoptic `completion` ∈ {yes, y} and/or completion-thyroidectomy language in `thyroid_procedure` (`cohort_logic.path_synoptic_completion_after_lobectomy`). **Integrated ultimate extent** (`ultimate_total`) treats either layer as evidence of progression to total-class management (`cohort_logic.ultimate_extent_total`). Exports: **`table7_completion_thyroidectomy.csv`**, **`completion_audit_outputs/`** (independent MotherDuck audit bundle).

### Statistical analysis

**Cohort sizes and descriptive statistics** were computed in Python/pandas. **Table 1** contrasts lobectomy vs initial total using means for age and Bethesda category and proportions for female sex and preoperative molecular testing (`table1_by_initial_extent.csv`).

**Univariable comparisons** used **Mann–Whitney U** for age and **chi-square** or **Fisher exact** as implemented for 2×2 screens (`univariable_tests.csv`, `univariable_screen` in `study_pipeline.py`).

**Multivariable logistic regression** used **statsmodels** `Logit` with **complete-case** rows for outcome and included predictors (`fit_lr`). We report **adjusted odds ratios (aOR)** with **95% confidence intervals** and **Wald p-values**. We prespecified a **parsimonious** and **extended** model on the primary strict cohort (N=558) and a **parsimonious** model on the broad-exclusion cohort (N=635). Outputs are in `logistic_primary_parsimonious.csv`, `logistic_primary_extended.csv`, `logistic_broad_nodal_parsimonious.csv`, and combined in `table2_multivariable_total_vs_lobectomy.csv`.

**Molecular subset.** Only **20 / 558** patients had preoperative molecular testing in the primary cohort. We present **descriptive** concordance summaries (`table6_molecular_pathology_concordance.csv`, `molecular_concordance_cases.csv`) **without** claiming validated diagnostic performance. Fitted logistic models on this subset are **exploratory** and are **not** interpreted as stable effects (`logistic_molecular_subset.csv`; small sample and numerical separation issues).

**Software.** Analysis code `study_pipeline.py` documents use of **Python**, **pandas**, **numpy**, **scipy.stats**, and **statsmodels**; see `analysis_manifest.json` for DuckDB version.

### Missing data

Key column missingness is summarized in `missingness_summary.csv` (exported long-form lists for `primary_N558` and `broad_nodal_N635` blocks). In particular, **Bethesda category** was missing for **149 / 558** primary-cohort rows (26.7%); FNA linkage, completion flags, and molecular fields have additional missingness. Multivariable models use **listwise deletion** over the variables included in each fit (primary models retain n=558 per OR tables for the specified covariate set).

---

## Results

### Cohort

The **primary analytic cohort** comprised **558** patients (**238** initial lobectomy, **320** initial total thyroidectomy; **320/558 = 57.3%** initial total). The **broad** suspicious-node exclusion cohort comprised **635** patients (**375** initial total; **375/635 = 59.1%**). The **pathology-defined 2–4 cm** cohort after strict nodal exclusion contained **0** patients in this frozen run; we do **not** report pathology-size parallels beyond that result (`cohort_build_log.md`).

**Figure 1** shows pipeline cohort selection counts (`fig_cohort_flow.png`), including the imaging-defined 2.0–4.0 cm step and final primary **N = 558**; steps with **zero** eligible patients (pathology-defined size under the current implementation; an intermediate strict preoperative LN exclusion row) are visible as empty bars and align with `cohort_build_log.md`. Y-axis labels in this export are **abbreviated**; authors may supply a relabeled flow for final production (`AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`).

### Figure 2 (primary parsimonious model)

**Figure 2** is a forest plot of adjusted odds ratios from the **primary parsimonious** logistic model (`fig_forest_total_vs_lobectomy.png`; `logistic_primary_parsimonious.csv`).

### Practice pattern evidence (Table 1)

Source file: **`table1_by_initial_extent.csv`**.

- **Age (mean):** lobectomy **56.6** years vs initial total **52.9** years.
- **Female sex:** **191/238 (80.3%)** vs **257/320 (80.3%)** (raw counts from `patient_level_dataset.csv`; proportions match Table 1 to listed precision).
- **Mean Bethesda category (non-missing arithmetic mean in pipeline):** **2.8** vs **4.0**.
- **Preoperative molecular testing:** **4.2%** vs **3.1%** of each arm in Table 1; **20/558 (3.6%)** of the overall primary cohort had preoperative molecular testing (`CLAIM_SOURCE_LEDGER.md`).

### Completion after lobectomy

Among **238** initial lobectomy patients (**Table 4** / **`table7_completion_thyroidectomy.csv`**), **zero** had a **later operative episode** meeting OED pipeline criteria for completion (**0 / 238** for ever and within 30, 90, and 365 days). Separately, **25 / 238 (10.5%)** had **path-synoptic definite** completion on a later synoptic row after index surgery (**2**, **13**, and **20** within **30**, **90**, and **365** days, respectively). **26** patients had **any** later thyroid-related operative or synoptic row after index lobectomy; **1** had later surgery without meeting **definite** OED or path-synoptic rules (**ambiguous** bucket). **`fig_completion_rates.png`** contrasts OED-pipeline versus path-synoptic proportions (ever and windowed).

### Univariable associations

Source: **`univariable_tests.csv`**.

- Age differed between groups (**Mann–Whitney p ≈ 0.007**).
- Sex distribution did not differ (**p = 1.0**).
- Bethesda ≥4 was associated with initial total (**p ≈ 6.0 × 10⁻⁷**).
- For preoperative molecular testing, **no statistically significant association was observed** in univariable testing (**p = 0.66**); **however, interpretation is limited by sparse testing (20/558) and potential selection bias.**
- Bilateral nodule indicator differed (**p = 0.048**).

### Multivariable logistic-derived associations (primary cohort, N = 558)

Source: **`logistic_primary_parsimonious.csv`**, **`logistic_primary_extended.csv`**.

**Parsimonious model.** Higher Bethesda category (≥4 vs not) was **associated** with higher odds of initial total thyroidectomy (**aOR 2.743**, 95% **CI 1.814–4.147**, **p ≈ 1.74 × 10⁻⁶**). Older age was **associated** with lower odds (**aOR 0.986** per year, **CI 0.975–0.998**, **p = 0.026**). Female sex (**aOR 0.974**, **CI 0.627–1.513**, **p = 0.91**) was **not** statistically associated with initial total at α = 0.05. For **preoperative molecular testing** (**aOR 0.606**, **CI 0.237–1.546**, **p = 0.29**), **no statistically significant association was observed; however, interpretation is limited by sparse testing (20/558) and potential selection bias.**

**Extended model.** Findings were **directionally similar** for age, sex, Bethesda ≥4, and preoperative molecular testing. **Bilateral nodule indicator** was **associated** with higher odds of initial total (**aOR 2.005**, **CI 1.282–3.134**, **p = 0.0023**). **TIRADS score** was **not** statistically associated (**aOR 0.958** per point, **p = 0.68**).

### Broad nodal exclusion sensitivity (N = 635)

Source: **`logistic_broad_nodal_parsimonious.csv`**.

Patterns were **similar** to the primary parsimonious model: **Bethesda ≥4 aOR 2.765** (**CI 1.878–4.071**, **p ≈ 2.6 × 10⁻⁷**); **age aOR 0.984** per year (**CI 0.973–0.995**, **p = 0.0053**); female sex **not** significant at α = 0.05. For preoperative molecular testing, **no statistically significant association was observed; however, interpretation is limited by sparse testing (20/558) and potential selection bias.**

### Molecular pathology concordance (exploratory, n = 20 tested)

Source: **`table6_molecular_pathology_concordance.csv`**.

Among preoperatively tested patients with binary malignant pathology coding available for the primary concordance frame, the table reports **tp = 9**, **fn = 11**, **fp = 0**, **tn = 0**, **n = 20** for `malignant_concordance_2x2` (descriptive classification counts only; **not** sensitivity, specificity, or predictive values).

Platform-specific strata (ThyroSeq **n = 8**, Afirma **n = 12** in the same file) are **too small** for stable inference or performance claims.

---

## Discussion

### What the data support

In this **single-database** retrospective cohort restricted to **preoperative imaging** nodule size **2.0–4.0 cm** and strict exclusion of definite preoperative nodal involvement, **57.3%** underwent **initial total thyroidectomy** (**320/558**). In adjusted models, **Bethesda category ≥4** and **bilateral nodule indicator** (extended model) **were associated with higher** odds of **initial total** among preoperative variables examined, whereas **older age** was associated with **lower** odds. **Female sex** was **not** significantly associated with initial total in primary adjusted models. For **preoperative molecular testing**, **no statistically significant association was observed; however, interpretation is limited by sparse testing (20/558) and potential selection bias.** A **non-causal** interpretation of testing as measured here remains appropriate.

Prior observational literature supports an association between higher-risk preoperative features, including cytologic risk, and selection of more extensive initial surgery.[5,6] Additional observational series describe variation in surgical strategy among patients treated for low-risk papillary thyroid carcinoma.[7] Population-level surgical series report increasing lobectomy use and other extent shifts after ATA guideline updates.[8] Bethesda-tier context for surgical decision-making is well described even when cytopathology categories differ from our ≥4 indicator.[9] These external papers **do not** replicate our adjusted odds ratios; they provide a **qualitative** backdrop only.

**Completion thyroidectomy** after initial lobectomy showed **no capture** on the **OED-only pipeline** (**0 / 238** later `operative_episode_detail_v2` totals) but **25 / 238** with **path-synoptic definite** second-stage documentation—consistent with **fragmentation** between operative-detail linkage and synoptic pathology rows in this database (**`completion_audit_outputs/final_verdict.md`**). External cohort studies still report non-zero pooled completion rates that are not directly comparable without aligning definitions.[3,5] Authors should distinguish **operative-table ascertainment** from **synoptic-pathology ascertainment** when contrasting with literature.

### Plausible but not proven here

Clinical reasoning often ties extent choice to **risk perception, patient preference, surgeon philosophy, and undocumented indications**; these factors are **largely unmeasured** in the extracted tables and could **confound** or **explain** associations observed here. **Molecular panel results** may co-occur with extent patterns in clinical practice, but testing was **sparse** and **selected** here; we **cannot** infer causal effects of testing from these observational data.

### Limitations of preoperative prediction (context)

Structured guidelines summarize the preoperative factors used to guide extent decisions, but published surgical series show that preoperative assessment often incompletely predicts the final pathologic features that may prompt a more extensive operation or completion thyroidectomy.[3,5] That limitation motivates transparent, **imaging-based** cohort definitions, while underscoring that our models quantify **associations** with **observed** initial surgery only.

### Limitations (this study)

- **Observational design** — associations only; **no causal** interpretation of cytology, molecular testing, or imaging features on surgeon choice.
- **Single integrated database** — generalizability **unknown**.
- **Missing data** on **Bethesda category**, **FNA/imaging linkage**, **completion** fields, and **molecular** variables; listwise deletion may bias estimates if missingness is informative (**149/558** missing Bethesda among primary-cohort rows).
- **Completion thyroidectomy** is summarized using **dual definitions** (OED pipeline vs path-synoptic definite); the **0/238 OED-only** tally reflects **sparse second-row operative linkage**, while **25/238** definite completions were recovered from **later synoptic rows**. Neither definition replaces full **individual chart** review; care outside the linked database may still be missed.
- **Pathology-sized sensitivity** analytic set **empty (N = 0)** — imaging-defined inclusion is the operational preoperative size frame reported here.
- **Molecular** concordance and subset models are **exploratory** only (**n = 20** with preoperative molecular testing in the primary cohort).
- **Temporal** trends (e.g., 2013–2023 surgery years in data) are **not** the focus of frozen tabular outputs.

**Guideline context** is evolving and explicitly allows lobectomy in many low-risk larger intrathyroidal presentations.[1] We do not argue for policy change; we describe **measured preoperative correlates** of **initial** extent in one imaging-defined cohort.[10]

---

## Data availability

Tabular analytic exports referenced in this manuscript are in **`studies/proposal_2to4cm_extent_molecular_20260326/`** (same directory as this file). Canonical cohort row-level file: **`patient_level_dataset.csv`**.

---

## Tables and figures

| Label | Source |
|-------|--------|
| **Figure 1** | `fig_cohort_flow.png` (see `figure_legends_v1.md`) |
| **Figure 2** | `fig_forest_total_vs_lobectomy.png` |
| **Figure 3 (completion)** | `fig_completion_rates.png` — OED pipeline vs path-synoptic definite (`figure_legends_v1.md`) |
| Table 1 | `table1_by_initial_extent.csv` |
| Table 2a–b | `logistic_primary_parsimonious.csv`, `logistic_primary_extended.csv` |
| Table 3 | `logistic_broad_nodal_parsimonious.csv` |
| Table 4 | `table7_completion_thyroidectomy.csv` |
| Table 5 | `univariable_tests.csv` |
| Table 6 | `table6_molecular_pathology_concordance.csv` |

**Completion figure:** `fig_completion_rates.png` is now **non-blank** (OED vs path-synoptic bars); authors may include as **supplemental** or relabel for journal. Exploratory bar charts `fig_molecular_result_by_extent.png`, `fig_platform_specific_extent.png` remain **not** main figures (`AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`; update that note locally).

---

## References

1. Ringel MD, Sosa JA, Baloch ZW, et al. 2025 American Thyroid Association management guidelines for adult patients with differentiated thyroid cancer. *Thyroid*. 2025;35(8):841-985. PMID: 40844370.

2. Montgomery KB, et al. Evolving variation in extent of surgery for low-risk papillary thyroid cancer in the United States. *Surgery*. 2023;174(4):828-835. doi:10.1016/j.surg.2023.07.001. PMID: 37550165.

3. Worrall BJ, Papachristos A, Aniss A, Glover A, Sidhu SB, Clifton-Bligh RJ, Learoyd D. Lobectomy and completion thyroidectomy rates increase after the 2015 American Thyroid Association differentiated thyroid cancer guidelines update. *Endocr Oncol*. 2023;3(1):EO-22-0095. doi:10.1530/EO-22-0095. PMCID: PMC10305631.

4. von Elm E, Altman DG, Egger M, Pocock SJ, Gøtzsche PC, Vandenbroucke JP; STROBE Initiative. The Strengthening the Reporting of Observational Studies in Epidemiology (STROBE) statement: guidelines for reporting observational studies. *Ann Intern Med*. 2007;147(8):573-577. doi:10.7326/0003-4819-147-8-200710160-00010. PMID: 17938396.

5. Dhir M, McCoy KL, Ohori NP, Adkisson CD, LeBeau SO, Carty SE, Yip L. Correct extent of thyroidectomy is poorly predicted preoperatively by the guidelines of the American Thyroid Association for low and intermediate risk thyroid cancers. *Surgery*. 2018;163(1):81-87. doi:10.1016/j.surg.2017.05.042. PMID: 28735877.

6. Wang X, Cheng W, Liu C, Li J, He A, Zeng W. Risk factors that influence surgical decision-making for low-risk differentiated thyroid cancer patients with tumor diameter 1-4 cm: a retrospective study. *World J Surg Oncol*. 2020;18(1):310. doi:10.1186/s12957-020-02064-7. PMCID: PMC7719324.

7. Kiss A, Szili B, Bakos B, et al. Comparison of surgical strategies in the treatment of low-risk differentiated thyroid cancer. *BMC Endocr Disord*. 2023;23:23. doi:10.1186/s12902-023-01276-8.

8. Conroy PC, Wilhelm A, Calthorpe L, et al. Endocrine surgeons are performing more thyroid lobectomies for low-risk differentiated thyroid cancer since the 2015 ATA guidelines. *Surgery*. 2022;172(5):1392-1400. doi:10.1016/j.surg.2022.06.031.

9. Loderer T, Bonati E, Donato V, et al. Malignancy risk in Bethesda class IV thyroid nodules in an iodine deficient region. *Gland Surg*. 2023;12(7):884-893. doi:10.21037/gs-22-491. PMCID: PMC10506119.

10. Hao Q, Segel JE, Vanness DJ, et al. Hemithyroidectomy versus total thyroidectomy for patients with differentiated thyroid cancer: a systematic review and meta-analysis. *Gland Surg*. 2025;14(11):2271-2287. doi:10.21037/gs-2025-364.
