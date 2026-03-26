# Initial thyroidectomy extent among adults with preoperative 2.0–4.0 cm thyroid nodules: a retrospective cohort study

**Version:** submission v1 (markdown)  
**Frozen outputs:** See `analysis_manifest.json` (git SHA and run timestamp).  
**Quantitative traceability:** `CLAIM_SOURCE_LEDGER.md`

---

## Abstract

See `abstract_structured_v1.md` (structured abstract aligned to this manuscript).

---

## Introduction

For cytologically indeterminate or suspicious thyroid nodules in the 2–4 cm size range, both lobectomy and total thyroidectomy remain in contemporary use [REF:NEEDS_GUIDELINE]. Practice varies across centers and over time [REF:NEEDS_EPIDEMIOLOGY]. We examined **cross-sectional associations** between preoperative patient, cytology, and limited molecular testing factors and **initial total thyroidectomy** versus **initial lobectomy** among patients with a **preoperative imaging-derived** index nodule between 2.0 and 4.0 cm who underwent a first qualifying hemithyroidectomy or total thyroidectomy within an integrated thyroid research database. The analysis is **observational** and does **not** support causal interpretation of surgical choice.

---

## Methods

### Study design and data source

We performed a **retrospective cohort study** using MotherDuck-queried analytic tables executed in the study pipeline [`study_pipeline.py`](study_pipeline.py). MotherDuck objects were **not** modified during manuscript preparation; all numbers in this version are derived from **frozen** CSV exports in this folder unless otherwise noted (`MANUSCRIPT_STATE_AUDIT.md`).

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

**Completion thyroidectomy.** Among patients with initial lobectomy, we summarized indicator completion flags (`completion_total_flag`, `completion_within_*`) from assembled patient data (`table7_completion_thyroidectomy.csv`).

### Statistical analysis

**Cohort sizes and descriptive statistics** were computed in Python/pandas. **Table 1** contrasts lobectomy vs initial total using means for age and Bethesda category and proportions for female sex and preoperative molecular testing (`table1_by_initial_extent.csv`).

**Univariable comparisons** used **Mann–Whitney U** for age and **chi-square** or **Fisher exact** (2×2 with expected &lt; 5 in any cell) for binary indicators (`univariable_tests.csv`, `univariable_screen` in `study_pipeline.py`).

**Multivariable logistic regression** used **statsmodels** `Logit` with **complete-case** rows for outcome and included predictors (`fit_lr`). We report **adjusted odds ratios (aOR)** with **95% confidence intervals** and **Wald p-values**. We prespecified a **parsimonious** and **extended** model on the primary strict cohort (N=558) and a **parsimonious** model on the broad-exclusion cohort (N=635). Outputs are in `logistic_primary_parsimonious.csv`, `logistic_primary_extended.csv`, `logistic_broad_nodal_parsimonious.csv`, and combined in `table2_multivariable_total_vs_lobectomy.csv`.

**Molecular subset.** Only **20 / 558** patients had preoperative molecular testing in the primary cohort. We present **descriptive** concordance summaries (`table6_molecular_pathology_concordance.csv`, `molecular_concordance_cases.csv`). Fitted logistic models on this subset are **exploratory** and are **not** interpreted as stable effects (`logistic_molecular_subset.csv`; small sample and numerical separation issues).

**Software.** Analysis code `study_pipeline.py` documents use of **Python**, **pandas**, **numpy**, **scipy.stats**, and **statsmodels**; see `analysis_manifest.json` for DuckDB version.

### Missing data

Key column missingness is summarized in `missingness_summary.csv` (exported long-form lists for `primary_N558` and `broad_nodal_N635` blocks). In particular, **Bethesda category** was missing for **149 / 558** primary-cohort rows (26.7%); FNA linkage and molecular fields have additional missingness. Multivariable models use **listwise deletion** over the variables included in each fit (primary models retain n=558 per OR tables for the specified covariate set).

---

## Results

### Cohort

The **primary analytic cohort** comprised **558** patients (**238** initial lobectomy, **320** initial total thyroidectomy; **320/558 = 57.3%** initial total). The **broad** suspicious-node exclusion cohort comprised **635** patients (**375** initial total; **375/635 = 59.1%**). The **pathology-defined 2–4 cm** cohort after strict nodal exclusion contained **0** patients in this frozen run; we do **not** report pathology-size parallels beyond that result (`cohort_build_log.md`).

### Practice pattern evidence (Table 1)

Source file: **`table1_by_initial_extent.csv`**.

- **Age (mean):** lobectomy **56.6** years vs initial total **52.9** years.
- **Female sex:** **191/238 (80.3%)** vs **257/320 (80.3%)** (raw counts from `patient_level_dataset.csv`; proportions match Table 1 to listed precision).
- **Mean Bethesda category (non-missing arithmetic mean in pipeline):** **2.8** vs **4.0**.
- **Preoperative molecular testing:** **4.2%** vs **3.1%** of each arm in Table 1; **20/558 (3.6%)** of the overall primary cohort had preoperative molecular testing (`CLAIM_SOURCE_LEDGER.md`).

### Completion after lobectomy

Among **238** initial lobectomy patients, **zero** had completion thyroidectomy by the pipeline-defined flags over available follow-up windows summarized in **`table7_completion_thyroidectomy.csv`** (completion ever and within 30, 90, and 365 days all **0 / 238**).

### Univariable associations

Source: **`univariable_tests.csv`**.

- Age differed between groups (**Mann–Whitney p = 0.007**).
- Sex distribution did not differ (**p = 1.0**).
- Bethesda ≥4 was associated with initial total (**p ≈ 6.0 × 10⁻⁷**).
- Preoperative molecular testing indicator was **not** associated with initial total in univariable testing (**p = 0.66**).
- Bilateral nodule indicator differed (**p = 0.048**).

### Multivariable logistic-derived associations (primary cohort, N = 558)

Source: **`logistic_primary_parsimonious.csv`**, **`logistic_primary_extended.csv`**.

**Parsimonious model.** Higher Bethesda category (≥4 vs not) was **associated** with higher odds of initial total thyroidectomy (**aOR 2.74**, 95% **CI 1.81–4.15**, **p ≈ 1.74 × 10⁻⁶**). Older age was **associated** with lower odds (**aOR 0.986** per year, **CI 0.975–0.998**, **p = 0.026**). Female sex and preoperative molecular testing were **not** statistically associated with initial total in this model (**aOR 0.97**, **p = 0.91**; **aOR 0.61**, **p = 0.29**, respectively).

**Extended model.** Findings were **directionally similar** for age, sex, Bethesda ≥4, and preoperative molecular testing. **Bilateral nodule indicator** was **associated** with higher odds of initial total (**aOR 2.01**, **CI 1.28–3.13**, **p = 0.0023**). **TIRADS score** was **not** statistically associated (**aOR 0.958** per point, **p = 0.68**).

### Broad nodal exclusion sensitivity (N = 635)

Source: **`logistic_broad_nodal_parsimonious.csv`**.

Patterns were **similar** to the primary parsimonious model: **Bethesda ≥4 aOR 2.77** (**CI 1.88–4.07**, **p ≈ 2.6 × 10⁻⁷**); **age aOR 0.984** per year (**CI 0.973–0.995**, **p = 0.0053**); sex and preoperative molecular testing **not** significant at α = 0.05.

### Molecular pathology concordance (exploratory, n = 20 tested)

Source: **`table6_molecular_pathology_concordance.csv`**.

Among preoperatively tested patients with binary malignant pathology coding available for the primary concordance frame, the table reports **tp = 9**, **fn = 11**, **fp = 0**, **tn = 0**, **n = 20** for `malignant_concordance_2x2` (descriptive only; **no** claim of diagnostic accuracy without a prespecified rule and CI).

Platform-specific strata (e.g. ThyroSeq **n = 8**, Afirma **n = 12** in the same file) are **too small** for stable inference.

---

## Discussion

### What the data support

In this **single-database retrospective cohort** restricted to **preoperative imaging** nodule size **2.0–4.0 cm** and a **strict** rule excluding definite preoperative nodal involvement, **57.3%** underwent **initial total thyroidectomy** (**320/558**). **Bethesda category ≥4** showed a **strong adjusted association** with initial total thyroidectomy in both parsimonious and extended models. **Older age** was **associated** with **lower** odds of initial total in adjusted models, compatible with within-cohort case-mix differences. **Female sex** was **not** associated with initial extent after adjustment. **Preoperative molecular testing** was **infrequent (20/558)** and was **not** statistically associated with initial extent in primary adjusted models.

**Completion thyroidectomy** after initial lobectomy was **0** in the operationalized flags (**238** lobectomy patients), a finding that should be interpreted in light of **missingness** on completion-related fields for many rows (`missingness_summary.csv`) and the **institutional definition** of completion in the pipeline.

### Plausible but not proven here

Clinical reasoning often ties extent choice to **risk perception, patient preference, surgeon philosophy, and undocumented indications**; these factors are **largely unmeasured** in the extracted tables and could **confound** or **explain** associations observed here. **Molecular panel results** could influence extent, but testing was **sparse** and **selected**; we **cannot** infer the causal impact of testing from these data.

### Limitations

- **Observational design** — associations only; **no causal** claims about lobectomy vs total thyroidectomy.
- **Single integrated database** — generalizability **unknown**.
- **Missingness** on Bethesda, FNA linkage, imaging linkage, and molecular fields (**see `missingness_summary.csv`**); listwise deletion may **bias** estimates if missingness is informative.
- **Pathology-sized sensitivity cohort is empty (N = 0)** in this run — imaging-based selection is the **only** sized cohort reported.
- **Completion and molecular** analyses face **structural zeroes** or **tiny samples**; multivariable models on subsets are **not reliable**.
- **Temporal effects** (policy change, ATA guideline cycles) **not modeled** in primary outputs (surgery year 2013–2023 available in data but not central to frozen tables).

---

## Data availability

Tabular analytic exports referenced in this manuscript are in **`studies/proposal_2to4cm_extent_molecular_20260326/`** (same directory as this file). Canonical cohort row-level file: **`patient_level_dataset.csv`**.

---

## Table mapping (no figure files in folder)

| Label | Source CSV |
|-------|------------|
| Table 1 | `table1_by_initial_extent.csv` |
| Table 2a–b | `logistic_primary_parsimonious.csv`, `logistic_primary_extended.csv` |
| Table 3 | `logistic_broad_nodal_parsimonious.csv` |
| Table 4 | `table7_completion_thyroidectomy.csv` |
| Table 5 | `univariable_tests.csv` |
| Table 6 | `table6_molecular_pathology_concordance.csv` |

---

## References

Placeholders used where a citation is needed but **no** folder bibliography exists. Replace after literature pass:

1. [REF:NEEDS_GUIDELINE] — current ATA / management guidelines for intermediate-sized nodules.
2. [REF:NEEDS_EPIDEMIOLOGY] — epidemiology or patterns of extent in comparable populations.

Full reference list to be finalized in `revision_packet_v1.md` under **NEEDS REFERENCE CHECK**.
