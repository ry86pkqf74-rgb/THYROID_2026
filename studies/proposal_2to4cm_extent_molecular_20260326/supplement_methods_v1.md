# Supplementary methods — submission v1

This supplement aligns narrative **only** with code and exports in `studies/proposal_2to4cm_extent_molecular_20260326/`.

---

## 1. Data extraction

### 1.1 MotherDuck access pattern

`study_pipeline.py` defines `connect()` as `MotherDuckClient(MotherDuckConfig()).connect_rw()` and issues **SELECT** queries for cohort construction. For this manuscript preparation pass, **no** `CREATE`, `ALTER`, `INSERT`, or `DELETE` were required or executed by the authors of these markdown files; numerical results were read from **exported** CSV/JSON in this folder. Optional read-only verification may use `MotherDuckClient.connect_ro_share()` elsewhere in the repo.

### 1.2 Core tables queried

Illustrative list (non-exhaustive) embedded in `load_core()`:

- `surgery_pathology_linkage_v3` (pathology size linkage)
- `tumor_episode_master_v2`, `tumor_pathology`
- `imaging_nodule_long_v2`, `imaging_nodule_master_v1`
- `imaging_fna_linkage_v3`
- `imaging_exam_summary_v2`
- `fna_episode_master_v2`
- `molecular_test_episode_v2`
- `patient_level_summary_mv`
- `ct_imaging` (pathologic lymph nodes)
- `clinical_notes_long` (exploratory snippet export with **LEFT(note_text, 120)** cap in pipeline)

Exact SQL strings appear in `study_pipeline.py`.

---

## 2. Cohort construction

### 2.1 First qualifying surgery

Implemented via `cohort_logic.first_qualifying_surgeries` on `operative_episode_detail_v2` with `procedure_normalized` in allowed sets (see `run()` for the sequence starting from all first procedures including `unknown`/`other`, then narrowing to hemithyroidectomy + total thyroidectomy).

### 2.2 Preoperative imaging 2.0–4.0 cm cohort

`cohort_logic.preop_imaging_size_cohort` chooses an index nodule and applies:

- Dominant / sort rules as coded (`sort_dom`, lesion candidates)
- `size_cm_max` or `max_dimension_cm` per imaging source branch
- Date constraint: imaging **on or before** `index_surgery_date`

Outputs feed `preop_cohort` and patient-level merges.

### 2.3 Pathology size (parallel arm)

`cohort_logic.add_pathology_size` joins `path_size_cm` from `surgery_pathology_linkage_v3` with tumor pathology fallbacks. The pathology-eligible intermediate cohort may be **empty** after 2–4 cm filter in a given refresh; the **frozen** run documented **N = 0** after strict nodal exclusion.

### 2.4 Strict nodal exclusion

`cohort_logic.strict_ln_exclusion`:

- **CT/MRI:** `pathologic_lymph_nodes` in `ct_imaging` / `mri_imaging` (as wired) with exam date before surgery
- **Malignant node FNA:** Bethesda VI where specimen site indicates node (pipeline logic)

Produces `strict_ln_positive_any` merged into cohort rows; strict cohorts drop rows with flag **TRUE**.

### 2.5 Broad suspicious-node exclusion

`cohort_logic.broad_suspicious_node` merges exam-level and nodule-level suspicion; broad cohorts drop `broad_suspicious_any == TRUE`.

---

## 3. Patient-level analytic frame

`assemble_patient_dataframe` merges:

- Preoperative imaging slice, demographics, procedure flags, completion (`completion_after_lobectomy` output), ultimate extent (`ultimate_extent_total`), preoperative FNA and molecular attaches, tumor episode histology fields, imaging master TIRADS / bilateral indicators.

Binary outcomes and indicators:

- `initial_total`, `initial_lobectomy`
- `bethesda_ge4` constructed in modeling as numeric Bethesda ≥ 4, missing coerced to not-ge4 for indicator in pipeline
- `has_mol` = `preop_molecular_tested`

---

## 4. Statistical methods

### 4.1 Descriptive Table 1

Computed in `run()` by subsetting `initial_lobectomy==1` vs `initial_total==1` and exporting means / proportions to `table1_by_initial_extent.csv`.

### 4.2 Univariable screen

Function `univariable_screen`:

- Age: **scipy.stats.mannwhitneyu** two-sided
- Each binary feature: contingency table vs outcome; **Fisher exact** if 2×2 with expected **&lt; 5** in any cell, else **chi2_contingency**

Output: `univariable_tests.csv`.

### 4.3 Multivariable logistic regression

Function `fit_lr`:

- Drops rows with **any** NA in outcome + predictors
- `statsmodels.api.Logit` with `maxiter=200`
- Reports exponentiated coefficients as OR with **conf_int** exponentiated

Primary labels:

- `primary_parsimonious`, `primary_extended` on N=558 frame
- `broad_nodal_parsimonious` on N=635 frame
- Additional exploratory fits for molecular subsets, ThyroSeq-only, Afirma-only, and completion-after-lobe (see `MANUSCRIPT_GAP_LIST.md` for interpretability limits)

Outputs: `logistic_*.csv`, combined `table2_multivariable_total_vs_lobectomy.csv`.

### 4.4 Concordance tables

`concordance_tables` builds descriptive 2×2 counts for molecular “positive” vs pathology malignant / neoplasm / aggressive frameworks and platform strata; exported to `table6_molecular_pathology_concordance.csv`.

---

## 5. Validation artifact

`write_validation_report` compares `patient_level_dataset.csv` row count and **research_id** set to in-memory dataframe and optionally counts operative rows for cohort IDs; output `validation_report.md`.

---

## 6. Missingness export note

`missingness_summary.csv` in the repository may aggregate missingness summaries beyond a simple `isna().mean()` single frame (see current file structure: separate blocks by cohort label). Methods text in the main manuscript defers to that file for column-level rates.

---

## 7. Random seed

`study_pipeline.py` sets `RNG = 42` / `np.random.seed(42)` for sklearn penalized logistic fallback path (if invoked).
