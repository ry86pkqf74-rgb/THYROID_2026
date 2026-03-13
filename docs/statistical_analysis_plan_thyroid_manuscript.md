# Statistical Analysis Plan -- THYROID_2026

**Document version:** 1.0  
**Date:** 2026-03-13  
**Study database:** THYROID_2026 (Emory University, DTC surgical cohort)  
**Primary data source:** MotherDuck `thyroid_research_2026` (DuckDB)  
**Analysis-grade layer:** scripts 48--55 (`patient_analysis_resolved_v1` and supporting tables)

---

## Table of Contents

- [A. Cohort Definitions](#a-cohort-definitions)
- [B. Primary Descriptive Analyses](#b-primary-descriptive-analyses)
- [C. Thyroid-Specific Scoring Analyses](#c-thyroid-specific-scoring-analyses)
- [D. Primary Inferential Models](#d-primary-inferential-models)
- [E. Sensitivity Analyses](#e-sensitivity-analyses)
- [F. Missing Data Handling](#f-missing-data-handling)
- [G. Model Diagnostics and Robustness](#g-model-diagnostics-and-robustness)
- [H. Output Specification](#h-output-specification)

---

## A. Cohort Definitions

All cohorts derive from the analysis-grade resolved layer (script 48). The
primary key for every table is `research_id` (integer). Unless stated
otherwise, patients appear at most once per cohort. The analysis layer was
built on 2026-03-13 and versioned as `resolved_layer_version = 'v1'`.

### A.1 Full Surgical Cohort

| Property | Value |
|----------|-------|
| **Source table** | `patient_analysis_resolved_v1` |
| **Unit of analysis** | Patient |
| **Estimated N** | 10,871 (after deduplication) |
| **Inclusion** | All patients with a row in the resolved layer |
| **Exclusion** | None |
| **Deduplication** | `patient_analysis_resolved_v1` is one row per `research_id` by construction (enforced by `QUALIFY ROW_NUMBER()` on upstream CTEs; validated by script 55 duplicate-ID assertion) |
| **Provisional fields** | `ata_response_category` (only 35 patients calculable), `biochemical_recurrence_flag` (128 events, simplified definition), `rln_permanent_flag` (0 patients -- data gap) |

**Purpose.** Demographic description and complication-rate denominators. This
cohort includes cancer, benign, and indeterminate surgical patients.

### A.2 Cancer-Specific Cohort

| Property | Value |
|----------|-------|
| **Source table** | `patient_analysis_resolved_v1` |
| **Unit of analysis** | Patient |
| **Estimated N** | ~4,136 |
| **Inclusion** | `analysis_eligible_flag = TRUE` (requires non-NULL `path_histology_raw` AND non-NULL `first_surgery_date`) |
| **Exclusion** | Patients whose only pathology is benign (adenoma, hyperplasia, thyroiditis, Graves) or indeterminate (NIFTP without concurrent cancer) |
| **Histology filter** | `path_histology_raw` NOT IN (`'adenoma'`, `'hyperplasia'`, `'benign'`, `'thyroiditis'`, `'graves'`) AND `path_histology_raw IS NOT NULL` |
| **Deduplication** | Inherits from A.1 |
| **Provisional fields** | Same as A.1 |

**Purpose.** Denominators for staging, molecular, and outcome analyses.

### A.3 Analysis-Eligible Cohort (Primary Analytic Set)

| Property | Value |
|----------|-------|
| **Source table** | `patient_analysis_resolved_v1` |
| **Unit of analysis** | Patient |
| **Estimated N** | ~4,136 |
| **Inclusion** | `analysis_eligible_flag = TRUE` |
| **Exclusion** | Excludes benign histology and patients without surgery date |
| **Deduplication** | Inherits from A.1 |
| **Provisional fields** | Same as A.1 |

**Notes.** This cohort is identical to A.2 and serves as the default denominator
for all inferential models unless a model-specific restriction is stated. When
a model's required predictors or outcome have additional missingness, the
effective analytic N will be smaller (complete-case approach documented in
Section F).

### A.4 Episode-Level Cohort

| Property | Value |
|----------|-------|
| **Source table** | `episode_analysis_resolved_v1` |
| **Unit of analysis** | Surgery episode |
| **Estimated N** | ~9,575 |
| **Inclusion** | All operative episodes |
| **Exclusion** | None |
| **Deduplication** | One row per `surgery_episode_id` (unique by construction from `operative_episode_detail_v2`) |
| **Provisional fields** | `path_link_score_v3` and `fna_link_score_v3` (v3 linkage scores are analysis-grade but the linkage algorithm itself is heuristic; `analysis_eligible_link_flag` requires `linkage_score >= 0.50`) |

**Purpose.** Procedure-level analyses: complication rates per procedure type,
operative technique associations, linkage quality assessment.

### A.5 Lesion-Level Cohort

| Property | Value |
|----------|-------|
| **Source table** | `lesion_analysis_resolved_v1` |
| **Unit of analysis** | Tumor / lesion |
| **Estimated N** | ~11,851 |
| **Inclusion** | All tumors from `tumor_episode_master_v2` |
| **Exclusion** | None (benign lesions retained for descriptive purposes) |
| **Deduplication** | One row per `(research_id, surgery_episode_id, tumor_ordinal)` |
| **Provisional fields** | `imaging_link_score`, `mol_link_score` (heuristic cross-domain linkage) |

**Purpose.** Per-lesion pathologic feature analyses: size, invasion, multifocality,
cross-domain linkage to FNA/molecular/imaging at the nodule level.

### A.6 Longitudinal Lab Cohort

| Property | Value |
|----------|-------|
| **Source table** | `longitudinal_lab_clean_v1` |
| **Unit of analysis** | Lab measurement |
| **Estimated N** | ~38,699 |
| **Inclusion** | All lab values passing plausibility guards |
| **Exclusion** | Values outside plausibility bounds (Tg: 0--100,000 ng/mL; anti-Tg: 0--10,000 IU/mL; TSH: 0--500 mIU/L; PTH: 0.5--500 pg/mL; Ca: 4.0--15.0 mg/dL; ionized Ca: 0.5--2.5 mmol/L); same-day same-value duplicates |
| **Patient coverage** | ~2,569 patients with Tg data (23.5% of surgical cohort) |
| **Date precedence** | `specimen_collect_dt` (priority 1.0) > `entity_date` (0.7) > `note_date` (0.5) |
| **Provisional fields** | `is_below_threshold` for censored "<X" values (treated as the numeric threshold in summary statistics) |

**Per-patient summaries** are available in `longitudinal_lab_patient_summary_v1`:
`tg_nadir`, `tg_last_value`, `tg_peak`, `tg_n_measurements`, `tg_rising_flag`,
`tg_doubling_time_days`, `tg_below_threshold_ever`, `anti_tg_nadir`,
`anti_tg_rising_flag`, `tsh_suppressed_ever`, `pth_nadir`, `calcium_nadir`,
`postop_low_pth_flag`, `postop_low_calcium_flag`, `lab_completeness_score`.

### A.7 TIRADS Imaging Subset

| Property | Value |
|----------|-------|
| **Source table** | `patient_analysis_resolved_v1` WHERE `imaging_tirads_best IS NOT NULL` |
| **Unit of analysis** | Patient |
| **Estimated N** | ~3,474 (32.0% of surgical cohort) |
| **Inclusion** | Patients with at least one validated TIRADS score from `extracted_tirads_validated_v1` |
| **Source data** | Phase 12 TIRADS pipeline: `COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx` (ACR-scored per-nodule data) and `US Nodules TIRADS 12_1_25.xlsx` (radiologist-assigned scores) |
| **Deduplication** | Per-patient worst (highest-risk) TIRADS score selected |
| **Provisional fields** | `imaging_nodule_size_cm` (from Excel structured data; 3,440 of 3,474 patients have size) |

### A.8 Molecular-Tested Subset

| Property | Value |
|----------|-------|
| **Source table** | `patient_analysis_resolved_v1` WHERE `molecular_eligible_flag = TRUE` |
| **Unit of analysis** | Patient |
| **Estimated N** | ~10,022 (92.2% have at least one non-stub molecular test) |
| **Inclusion** | `mol_n_tests > 0` (after excluding placeholder rows where `platform = 'x'`) |
| **Key columns** | `braf_positive_final`, `ras_positive_final`, `tert_positive_final`, `mol_platform`, `mol_braf_variant`, `mol_ras_subtype`, `molecular_risk_tier` |
| **Provisional fields** | None -- molecular flags are source-linked and positivity-confirmed (Phase 11/13 NLP audit with explicit positive-qualifier gate) |

**BRAF prevalence context.** 546 BRAF-positive patients (after removing 113
false positives in Phase 13). 441/~800 molecular-tested cancer patients = 55.1%
(above published 40--45% PTC rate due to surgical cohort enrichment for
suspicious nodules).

### A.9 RAI-Treated Subset

| Property | Value |
|----------|-------|
| **Source table** | `patient_analysis_resolved_v1` WHERE `rai_eligible_flag = TRUE` |
| **Unit of analysis** | Patient |
| **Estimated N** | ~35 (definite_received or likely_received) |
| **Inclusion** | `rai_assertion_status IN ('definite_received', 'likely_received')` from `rai_treatment_episode_v2` |
| **Key columns** | `rai_first_date`, `rai_max_dose_mci`, `rai_assertion_statuses` |
| **Provisional fields** | `rai_max_dose_mci` (average 143.9 mCi among those with dose; 307 total NLP-recovered doses but only ~35 meet the definite/likely assertion threshold) |

**Warning.** The small RAI-eligible N limits inferential analyses stratified by
RAI status. RAI-related models (e.g., time-to-RAI, post-RAI Tg trajectory)
should be reported as exploratory with explicit N and confidence interval widths.

---

## B. Primary Descriptive Analyses

### B.1 Table 1 -- Demographics and Clinical Characteristics

**Cohort:** A.3 (analysis-eligible, N~4,136).

| Variable | Column | Type | Summary statistic |
|----------|--------|------|-------------------|
| Age at surgery | `age_at_surgery` | Continuous | Median (IQR); mean (SD) |
| Sex | `sex` | Binary | N (%); Female vs Male |
| Race | `race` | Categorical | N (%) per group: Black, White, Asian, Hispanic, Other/Unknown |
| Primary histology | `path_histology_raw` | Categorical | N (%) for PTC, FTC, MTC, PDTC, HCC, ATC, other |
| Tumor size (cm) | `path_tumor_size_cm` | Continuous | Median (IQR); N missing |
| Multifocality | `path_multifocal_flag` | Binary | N (%) |
| AJCC8 stage group | `ajcc8_stage_group` | Ordinal | N (%) per stage: I, II, III, IVA, IVB |
| ATA initial risk | `ata_risk_category` | Ordinal | N (%) low / intermediate / high |
| MACIS score | `macis_score` | Continuous | Median (IQR); risk group N (%) |
| ETE grade (final) | `ete_grade_final` | Categorical | N (%) none / microscopic / gross / present_ungraded |
| Vascular invasion (final) | `vascular_invasion_final` | Categorical | N (%) absent / focal / extensive / present_ungraded |
| Margin status (final) | `margin_status_final` | Categorical | N (%) R0, R1, R2, Rx |
| LN positive (final) | `ln_positive_final` | Count | Median (IQR); N (%) with LN+ > 0 |
| LN examined (raw) | `path_ln_examined_raw` | Count | Median (IQR) |
| LN ratio | `ln_ratio` | Continuous | Median (IQR) |
| Procedure type | `surg_procedure_type` | Categorical | N (%) total thyroidectomy / hemithyroidectomy / other |
| RAI received | `rai_received_flag` | Binary | N (%) |
| BRAF V600E | `braf_positive_final` | Binary | N (%) among molecular-tested |
| RAS mutation | `ras_positive_final` | Binary | N (%) among molecular-tested |
| TERT promoter | `tert_positive_final` | Binary | N (%) among molecular-tested |
| Molecular risk tier | `molecular_risk_tier` | Ordinal | N (%) high / intermediate_braf / intermediate_ras / low / unknown |

**Stratification columns for Table 1 subgroup panels:**
- By histology type (PTC vs FTC vs others)
- By AJCC8 stage group (I/II vs III/IV)
- By procedure type (total vs hemi)

**Denominators.** Each variable's N is reported explicitly. For variables with
>5% missingness, report "N evaluated / N total (% available)." For categorical
variables, include an "Unknown/Missing" row.

### B.2 Table 2 -- Surgical Complications

**Cohort:** A.1 (full surgical, N~10,871) for complication rates; A.3 for
stratified cancer-specific rates.

| Complication | Column | Classification | Summary |
|-------------|--------|---------------|---------|
| Hypocalcemia | `hypocalcemia_status` | biochemical_only / treatment_requiring / confirmed_transient / confirmed_permanent / absent / unknown | N (%) per classification |
| Hypoparathyroidism | `hypoparathyroidism_status` | Same classification | N (%) per classification |
| RLN injury | `rln_status` | confirmed_permanent / confirmed_transient / probable / absent / unknown | N (%) per classification |
| RLN transient | `rln_transient_flag` | Binary | N (%) |
| RLN permanent | `rln_permanent_flag` | Binary | N (%) -- **DATA GAP: 0 patients have permanent = TRUE; report as "not ascertainable"** |
| Hematoma | `hematoma_status` | confirmed / absent | N (%) |
| Seroma | `seroma_status` | confirmed / absent | N (%) |
| Chyle leak | `chyle_leak_status` | confirmed / absent | N (%) |
| Wound infection | `wound_infection_status` | confirmed / absent | N (%) |
| Any confirmed complication | `any_confirmed_complication` | Binary | N (%) |
| Calcium supplement required | `calcium_supplement_required` | Binary | N (%) |

**Source hierarchy.** Complication phenotypes from `complication_patient_summary_v1`
(script 52), which applies the Phase 2 refinement pipeline (excludes H&P consent
boilerplate false positives). Raw NLP precision was 3.3% before refinement;
all published rates use the refined counts.

**Stratification:** By procedure type (total thyroidectomy vs hemithyroidectomy),
by central LN dissection (CLN+ vs CLN-), by surgeon volume quintile (if available).

### B.3 Table 3 -- Recurrence and Follow-up

**Cohort:** A.3 (analysis-eligible, N~4,136).

| Variable | Column | Type | Summary |
|----------|--------|------|---------|
| Any recurrence | `any_recurrence_flag` | Binary | N (%) |
| Structural recurrence | `structural_recurrence_flag` | Binary | N (%) |
| Biochemical recurrence | `biochemical_recurrence_flag` | Binary | N (%) -- **PROVISIONAL** |
| Time to recurrence (days) | derived: `recurrence_date - first_surgery_date` | Continuous (censored) | Median (IQR) among recurrent patients |
| Recurrence site | `recurrence_site_primary` | Categorical | N (%) per site |
| Tg nadir | `tg_nadir` | Continuous | Median (IQR); N with Tg data |
| Tg last value | `tg_last_value` | Continuous | Median (IQR) |
| Tg rising flag | `tg_rising_flag` | Binary | N (%) |
| Anti-Tg rising | `anti_tg_rising_flag` | Binary | N (%) |
| Lab completeness score | `lab_completeness_score` | Continuous (0--100) | Mean (SD); median (IQR) |
| ATA response to therapy | `ata_response_category` | Categorical | N (%) -- **PROVISIONAL (N=35 calculable)** |

**Recurrence definitions:**
- Structural recurrence: documented on imaging or pathology (from `extracted_recurrence_refined_v1.recurrence_flag_structural`); 1,818 events.
- Biochemical recurrence: Tg rising > 2x nadir AND Tg > 1.0 ng/mL without structural evidence (simplified definition); 128 events. **Provisional -- does not account for anti-Tg antibody interference or assay change-overs.**

### B.4 Supplementary Table -- FNA and Imaging

**Cohort:** A.7 (TIRADS subset, N~3,474) for imaging; A.3 for FNA.

| Variable | Column | Type | Summary |
|----------|--------|------|---------|
| Bethesda category (worst) | `fna_bethesda_final` | Ordinal I--VI | N (%) per category |
| TIRADS score (best) | `imaging_tirads_best` | Ordinal 1--5 | N (%) per category |
| TIRADS score (worst) | `imaging_tirads_worst` | Ordinal 1--5 | N (%) per category |
| TIRADS category | `imaging_tirads_category` | Categorical (TR1--TR5) | N (%) |
| Nodule size (imaging, cm) | `imaging_nodule_size_cm` | Continuous | Median (IQR) |
| Number of nodule records | `imaging_n_nodule_records` | Count | Median (IQR) |
| Multifocal on imaging | from `imaging_patient_summary_v1.multifocal_flag` | Binary | N (%) |
| Bilateral disease | from `imaging_patient_summary_v1.bilateral_disease_flag` | Binary | N (%) |

### B.5 Supplementary Table -- Longitudinal Labs

**Cohort:** A.6 (longitudinal lab cohort, ~38,699 measurements from ~2,569
patients with Tg data).

| Variable | Column | Type | Summary |
|----------|--------|------|---------|
| Total measurements per patient | `tg_n_measurements` | Count | Median (IQR) |
| Tg nadir | `tg_nadir` | Continuous | Median (IQR) |
| Tg peak | `tg_peak` | Continuous | Median (IQR) |
| Tg doubling time (days) | `tg_doubling_time_days` | Continuous | Median (IQR) among rising |
| Below-threshold measurements | `tg_below_threshold_ever` | Binary | N (%) |
| PTH nadir (post-op) | `pth_nadir` | Continuous | Median (IQR); N with PTH data |
| Calcium nadir (post-op) | `calcium_nadir` | Continuous | Median (IQR); N with Ca data |
| Post-op low PTH flag | `postop_low_pth_flag` | Binary | N (%) |
| Post-op low calcium flag | `postop_low_calcium_flag` | Binary | N (%) |

---

## C. Thyroid-Specific Scoring Analyses

**Source table:** `thyroid_scoring_systems_v1` (script 51) joined to
`patient_analysis_resolved_v1` via `research_id`. Alternatively,
`thyroid_scoring_py_v1` (Python-derived version) may be used.

### C.1 Score Calculability Summary

Report the percentage of the analysis-eligible cohort (A.3) for which each
score is calculable:

| Scoring system | Calculable flag column | Estimated calculable % | Required inputs |
|---------------|----------------------|----------------------|-----------------|
| AJCC 8th Ed | `ajcc8_calculable_flag` | 37.6% | `tumor_size_cm`, `age_at_surgery`, `ete_grade != 'unknown'` |
| ATA 2015 risk | `ata_calculable_flag` | 28.9% | `histology` (non-NULL, non-unknown), `ete_grade` (non-NULL) |
| MACIS | `macis_calculable_flag` | 37.5% | `age_at_surgery`, `tumor_size_cm` |
| AGES | `ages_calculable_flag` | 37.5% | `age_at_surgery`, `tumor_size_cm` |
| AMES | `ames_calculable_flag` | varies | `age_at_surgery`, `tumor_size_cm`, `sex` |
| ATA response | `ata_response_calculable_flag` | ~0.3% (N=35) | `rai_received = TRUE`, `tg_max IS NOT NULL` |
| Molecular risk | `molecular_risk_calculable_flag` | ~8% (molecular-tested cancer) | At least one BRAF/RAS/TERT result |

For each score, report `*_missing_components` to identify which input(s) drove
non-calculability.

### C.2 AJCC 8th Edition Staging

**Reference:** AJCC Cancer Staging Manual, 8th Ed., Chapter 73 (Thyroid -- DTC).

**T Stage derivation:**
- T1a: size <= 1 cm, no gross ETE
- T1b: size > 1--2 cm, no gross ETE
- T2: size > 2--4 cm, no gross ETE
- T3a: size > 4 cm, no ETE
- T3b: gross ETE to perithyroidal soft tissue
- T4a: gross ETE to larynx, trachea, RLN, esophagus
- T4b: gross ETE to prevertebral fascia, carotid, mediastinum
- Microscopic ETE does NOT upstage T1--T2 per AJCC 8th Ed.

**N Stage derivation:**
- N0: no LN metastasis (`ln_positive = 0` with `ln_examined IS NOT NULL`)
- N1a: central compartment (level VI) positive
- N1b: lateral/mediastinal (levels II--V, VII) positive

**Stage group (age-dependent DTC rules):**
- Age < 55: Stage I (any T, any N, M0); Stage II (any T, any N, M1)
- Age >= 55: I = T1-2/N0/M0; II = T1-2/N1a or T3; III = T4a or N1b; IVA = T4b; IVB = M1

**Columns:** `ajcc8_t_stage`, `ajcc8_n_stage`, `ajcc8_m_stage`, `ajcc8_stage_group`.

Report: Stage distribution (N, %) and stage migration analysis versus raw
`path_t_stage_raw` / `path_n_stage_raw` where available.

### C.3 ATA 2015 Initial Recurrence Risk

**Reference:** Haugen BR et al., Thyroid 2016;26:1--133.

**Risk strata:**
- **Low:** PTC without aggressive variant, no vascular invasion, no ETE, no distant mets, LN positive <= 5 with micrometastases (< 0.2 cm).
- **Intermediate:** Aggressive histologic variant (tall cell, hobnail, columnar, diffuse sclerosing, solid, PDTC), vascular invasion (any grade), microscopic ETE, > 5 positive LN or LN deposit 0.2--3 cm, RAI uptake in neck.
- **High:** Gross ETE, incomplete resection (R1/R2), distant mets, Tg suggesting distant mets (Tg > 10 post-RAI), LN deposit > 3 cm, FTC with extensive vascular invasion.

**Column:** `ata_risk_category` (low / intermediate / high / NULL).

Report: N (%) per risk stratum, cross-tabulated with AJCC8 stage group.

### C.4 MACIS Score

**Reference:** Hay ID et al., Surgery 1993;114:1050--8.

**Formula:**

    MACIS = 3.1 * age_factor + 0.3 * tumor_size_cm
            + 1.0 * incomplete_resection + 1.0 * local_invasion
            + 3.0 * distant_mets

    age_factor = 0.08 * age   (if age < 40)
               = 0.22 * age   (if age >= 40)

**Risk groups:** < 6.0 = low; 6.0--6.99 = intermediate; 7.0--7.99 = high; >= 8.0 = very high.

**Column:** `macis_score` (DOUBLE), `macis_risk_group`.

Report: Mean (SD), median (IQR), risk group distribution.

### C.5 AGES and AMES Scores

**AGES** (Hay et al., Surgery 1987):

    AGES = 0.05 * age + grade_points + 1.0 * ETE + 3.0 * distant_mets
           + 0.2 * tumor_size_cm

    grade_points: grade 2 = 1.0; grade 3--4 = 3.0; else 0.0

**Caveat:** `histologic_grade_raw` (from `path_synoptics.tumor_1_histologic_grade`)
is < 30% populated. AGES grade component defaults to 0 when missing. This is
a known underestimation of AGES for poorly differentiated tumors.

**Column:** `ages_score`.

**AMES** (Cady & Rossi, Surgery 1988):

    High risk = older patient (Male > 40 or Female > 50)
                AND (distant mets OR gross ETE OR tumor > 5 cm)
    Low risk  = all others

**Column:** `ames_risk_group` (low / high / NULL).

### C.6 Molecular Risk Composite

**Reference:** Xing M et al., Lancet Oncol 2014;15:1461--8.

| Tier | Definition |
|------|-----------|
| `high` | BRAF V600E + TERT promoter co-mutation |
| `intermediate_braf` | BRAF V600E alone |
| `intermediate_ras` | RAS mutation alone |
| `low` | All tested negative |
| `unknown` | Not tested or insufficient data |

**Column:** `molecular_risk_tier`.

Report: N (%) per tier; cross-tabulation with ATA risk and AJCC8 stage.

### C.7 LN Burden Metrics

- `ln_ratio` = `ln_positive / ln_examined` (NULL when `ln_examined = 0`)
- `ln_burden_band`: N0 / low_burden_1to3 / high_burden_gt3 / unknown

Report: LN ratio distribution by AJCC N stage, central vs lateral dissection
rates (`central_ln_dissected`, `lateral_ln_dissected`).

### C.8 Scoring Concordance Analysis

Compare calculated AJCC8 stage versus raw `path_stage_raw` (pathologist-reported
synoptic stage) where both are available. Report:
- Concordance rate (exact match, %)
- Directional disagreement (upstaged vs downstaged by calculated algorithm)
- Kappa statistic with 95% CI

---

## D. Primary Inferential Models

All inferential models use the analysis-eligible cohort (A.3) unless stated
otherwise. Two-sided tests at alpha = 0.05 throughout. Multiple comparisons
within a model family are corrected by Benjamini-Hochberg FDR. All random seeds
fixed at 42 for reproducibility.

### D.1 Logistic Regression Models

#### D.1.1 Recurrence (Primary)

| Property | Value |
|----------|-------|
| **Outcome** | `any_recurrence_flag` (binary) |
| **Predictors** | `ajcc8_stage_group`, `ata_risk_category`, `braf_positive_final`, `tert_positive_final`, `ete_grade_final`, `vascular_invasion_final`, `ln_positive_final`, `surg_procedure_type` |
| **Method** | Multivariable logistic regression (statsmodels `Logit`) |
| **Complete-case rule** | Exclude patients missing outcome OR any predictor. Report effective N vs total N. |
| **Minimum event threshold** | At least 10 events per predictor variable (EPV >= 10). If EPV < 10, reduce model to univariable or bivariate. |
| **Output** | OR (95% CI), p-value per predictor; Hosmer-Lemeshow goodness-of-fit; c-statistic (AUC). |

#### D.1.2 Structural Recurrence (Sensitivity)

Same specification as D.1.1 but outcome = `structural_recurrence_flag`. This
model excludes the 128 biochemical-only recurrences (provisional definition).

#### D.1.3 Complication (Secondary)

| Property | Value |
|----------|-------|
| **Outcome** | `any_confirmed_complication` (binary) |
| **Predictors** | `age_at_surgery`, `sex`, `surg_procedure_type`, `path_tumor_size_cm`, `ln_positive_final`, `lateral_neck_dissected` |
| **Cohort** | A.1 (full surgical) for maximum N |
| **Method** | Multivariable logistic regression |
| **Complete-case rule** | Same as D.1.1 |
| **Minimum event threshold** | EPV >= 10 |
| **Output** | OR (95% CI), c-statistic |

#### D.1.4 Hypocalcemia-Specific Model

| Property | Value |
|----------|-------|
| **Outcome** | `hypocalcemia_status IN ('treatment_requiring', 'confirmed_transient', 'confirmed_permanent')` vs absent/biochemical_only |
| **Predictors** | `surg_procedure_type`, `age_at_surgery`, `sex`, `lateral_neck_dissected`, `surg_n_procedures` |
| **Method** | Multivariable logistic regression |
| **Note** | Hypocalcemia definition follows script 52: PTH < 15 pg/mL or Ca < 8.0 mg/dL within 30d of surgery |

### D.2 Survival / Time-to-Event Models

#### D.2.1 Kaplan-Meier Estimates

| Property | Value |
|----------|-------|
| **Outcome** | Time from `first_surgery_date` to `recurrence_date` (structural recurrence); censored at last Tg measurement date or last clinical encounter |
| **Strata** | AJCC8 stage group, ATA risk, molecular risk tier, BRAF status, histology type |
| **Method** | Kaplan-Meier with log-rank test for each stratification variable |
| **Cohort** | A.3 (analysis-eligible) with `structural_recurrence_flag IS NOT NULL` |
| **Output** | KM curves (median survival, 5-year and 10-year estimates with 95% CI) |
| **Minimum stratum size** | Report strata with N >= 20; combine smaller strata or report separately as exploratory |

#### D.2.2 Cox Proportional Hazards (Primary)

| Property | Value |
|----------|-------|
| **Outcome** | Time to structural recurrence (same as D.2.1) |
| **Predictors** | `ajcc8_stage_group`, `ata_risk_category`, `braf_positive_final`, `tert_positive_final`, `ete_grade_final`, `ln_positive_final`, `path_tumor_size_cm`, `age_at_surgery` |
| **Method** | Cox PH (`lifelines.CoxPHFitter`, `penalizer=0.01`) |
| **Complete-case rule** | Exclude patients missing time variable, event indicator, or any covariate |
| **Minimum events** | EPV >= 10 per covariate; if insufficient, build reduced models |
| **Output** | HR (95% CI), p-value, concordance index, Schoenfeld residual plots |
| **PH check** | Schoenfeld residual test for each covariate; if p < 0.05, consider stratified Cox or time-varying coefficient |

#### D.2.3 Propensity Score Matching (Exploratory)

| Property | Value |
|----------|-------|
| **Exposure** | ETE (gross vs none/microscopic) OR CLN (performed vs not) |
| **Outcome** | Structural recurrence |
| **Covariates for PSM** | `age_at_surgery`, `path_tumor_size_cm`, `ln_positive_final`, `path_ln_examined_raw` |
| **Method** | Nearest-neighbor 1:1 matching with 0.25xSD caliper; `random_state=42` |
| **Balance check** | Standardized mean difference (SMD) < 0.10 for all covariates |
| **Output** | Matched HR (95% CI), E-value for unmeasured confounding |

### D.3 Longitudinal Lab Models

#### D.3.1 Thyroglobulin Trajectory

| Property | Value |
|----------|-------|
| **Outcome** | `log(tg_value + 0.1)` at each measurement time point |
| **Predictors** | `days_from_surgery` (time), `ajcc8_stage_group`, `rai_received_flag`, `braf_positive_final` |
| **Method** | Linear mixed-effects model (random intercept + random slope for `days_from_surgery` per patient) |
| **Cohort** | A.6 restricted to patients with >= 3 Tg measurements (lab_type = 'thyroglobulin') |
| **Software** | `statsmodels.MixedLM` |
| **Output** | Fixed effects (beta, 95% CI, p), random effects variance, ICC |

#### D.3.2 Tg Doubling Time

| Property | Value |
|----------|-------|
| **Outcome** | `tg_doubling_time_days` from `longitudinal_lab_patient_summary_v1` |
| **Analysis** | Descriptive (median, IQR among patients with `tg_rising_flag = TRUE`); Cox model with doubling time as a time-varying covariate for structural recurrence |
| **Note** | Only applicable to patients with >= 2 Tg measurements showing rising trend |

### D.4 Complication Models

#### D.4.1 RLN Injury Risk Factors

| Property | Value |
|----------|-------|
| **Outcome** | `rln_status IN ('confirmed_permanent', 'confirmed_transient', 'probable')` vs absent |
| **Predictors** | `surg_procedure_type`, `lateral_neck_dissected`, `surg_n_procedures`, `age_at_surgery`, `path_tumor_size_cm` |
| **Cohort** | A.1 (full surgical) |
| **Method** | Multivariable logistic regression |
| **Note** | RLN 3-tier classification from refined pipeline (Tier 1 laryngoscopy-confirmed = 6, Tier 2 chart-documented = 19, Tier 3 NLP-confirmed = 34, Tier 3 suspected = 33; total refined 92 patients, 0.85%) |
| **Data gap** | `rln_permanent_flag` is 0 across all patients. Transient vs permanent distinction is not reliably ascertainable. Report this limitation explicitly. |

---

## E. Sensitivity Analyses

Eight pre-specified sensitivity restrictions. Each restriction is applied to
the primary models (D.1.1, D.2.2) and results reported alongside primary
estimates.

### E.1 Complete Staging Only

**Restriction:** `ajcc8_calculable_flag = TRUE AND ata_calculable_flag = TRUE`

**Rationale:** Limits analysis to patients with full T, N, M staging and ATA
risk inputs. Eliminates bias from missing staging components.

**Expected N:** ~1,200 (intersection of AJCC8 37.6% and ATA 28.9% calculability).

### E.2 Cancer-Histology Confirmed

**Restriction:** `path_histology_raw IN ('PTC_classic', 'PTC_follicular_variant', 'PTC_tall_cell', 'PTC_hobnail', 'PTC_diffuse_sclerosing', 'PTC_columnar', 'FTC', 'HCC_oncocytic', 'MTC', 'ATC', 'PDTC')`

**Rationale:** Excludes NIFTP, adenoma, benign, and unclassifiable histology.
Ensures outcome models are cancer-specific.

### E.3 Structural Recurrence Only

**Restriction:** Replace `any_recurrence_flag` with `structural_recurrence_flag`
as the outcome.

**Rationale:** Removes the 128 biochemical-only recurrences (provisional
definition) to assess robustness of recurrence associations.

### E.4 High Lab Completeness

**Restriction:** `lab_completeness_score >= 40` (from `longitudinal_lab_patient_summary_v1`)

**Rationale:** Patients with adequate follow-up lab coverage. A score of 40
corresponds to at least Tg OR clinical event data available.

**Expected N:** ~2,500 patients.

### E.5 Molecular-Tested Only

**Restriction:** `molecular_eligible_flag = TRUE` (from `patient_analysis_resolved_v1`)

**Rationale:** Restricts molecular-adjusted models to patients where BRAF/RAS/TERT
status is known rather than assumed wild-type.

### E.6 Exclude Provisional ATA Response

**Restriction:** Drop `ata_response_category` from any model that includes it
as a predictor.

**Rationale:** Only 35 patients have calculable ATA response. Including it
in multivariate models introduces extreme sparsity. Models with ATA response
are exploratory only.

### E.7 TIRADS Subset Restriction

**Restriction:** Analysis restricted to patients in A.7 (TIRADS subset,
N~3,474) with TIRADS data available.

**Rationale:** Allows inclusion of `imaging_tirads_best` as a predictor in
recurrence and complication models while maintaining comparable covariate
completeness.

### E.8 Minimum Follow-up Restriction

**Restriction:** Exclude patients with time from surgery to last contact
< 1 year.

**Derivation:** `DATEDIFF('day', first_surgery_date, GREATEST(recurrence_date, tg_latest_date, last_clinical_contact)) >= 365`

**Rationale:** Ensures at least 1 year of follow-up opportunity to capture
recurrence events. Mitigates informative censoring from short follow-up.

---

## F. Missing Data Handling

### F.1 General Strategy

The primary analysis uses **complete-case analysis** per model. Each model's
effective N is reported alongside the total eligible N (cohort A.3 or A.1).
The percentage of exclusions due to missingness is reported.

### F.2 Key Missingness Rates (in analysis-eligible cohort A.3)

| Variable | Column | Approximate fill % | Missing handling |
|----------|--------|-------------------|-----------------|
| Age at surgery | `age_at_surgery` | 99.2% | Complete-case |
| Sex | `sex` | 93.2% | Complete-case |
| Tumor size (cm) | `path_tumor_size_cm` | 38.0% | Complete-case for primary; MICE imputation for sensitivity (see F.4) |
| LN positive | `ln_positive_final` | ~33.8% | Complete-case for primary; MICE for sensitivity |
| LN examined | `path_ln_examined_raw` | ~75.2% | Complete-case |
| ETE grade | `ete_grade_final` | ~95.4% (after Phase 9 'x' resolution) | Complete-case; exclude remaining 49 `present_ungraded` as missing |
| Vascular invasion | `vascular_invasion_final` | ~35.4% | Complete-case |
| AJCC8 stage | `ajcc8_stage_group` | 37.6% calculable | Complete-case |
| ATA risk | `ata_risk_category` | 28.9% calculable | Complete-case |
| MACIS | `macis_score` | 37.5% calculable | Complete-case |
| TIRADS | `imaging_tirads_best` | 32.0% | Excluded from primary model; included in sensitivity E.7 |
| Tg nadir | `tg_nadir` | 23.5% | Complete-case |
| Molecular status | `braf_positive_final` | 92.2% tested | Untested coded as FALSE for primary; tested-only restriction in E.5 |

### F.3 Missing Data Mechanism Assessment

For each variable with > 20% missingness, assess the plausibility of MAR
(missing at random) versus MNAR (missing not at random) by comparing
demographic and clinical characteristics of patients with vs without the
variable available. Report these comparisons in a supplementary table.

Specific known mechanisms:
- **Tumor size / LN data (62--67% missing):** Missing because the patient is
  benign (non-cancer). Among cancer patients, fill rate is much higher
  (~85--90%). This is informative missingness (MNAR conditional on cancer
  status) but MAR within the cancer subset.
- **TIRADS (68% missing):** Missing because ultrasound reports were not
  entered into the structured Excel workbooks or the patient had no pre-op US
  in the study system. Assess whether TIRADS-missing patients differ on
  Bethesda category and tumor size.
- **Tg labs (76.5% missing):** Missing because the patient had no post-operative
  Tg monitoring recorded in the lab system. More likely missing for benign
  surgical patients and those lost to follow-up.

### F.4 Multiple Imputation (Sensitivity)

For the primary logistic model (D.1.1) and Cox model (D.2.2), a pre-specified
sensitivity analysis uses **MICE (Multiple Imputation by Chained Equations)**
with m = 20 imputations to handle `path_tumor_size_cm`, `ln_positive_final`,
and `path_ln_examined_raw`.

**Implementation:** `sklearn.impute.IterativeImputer` with `max_iter=10`,
`random_state=42`. Covariates for imputation model: `age_at_surgery`, `sex`,
`path_histology_raw` (cancer flag).

**Pooling:** Rubin's rules for combining parameter estimates and standard errors
across m imputations (`ThyroidStatisticalAnalyzer.rubins_rules()`).

**Report:** MICE-pooled OR/HR alongside complete-case estimates; fraction of
missing information (FMI) per variable.

### F.5 Provisional Field Policy

Fields marked PROVISIONAL in this SAP (see cohort definitions) are handled as
follows:

| Field | Policy |
|-------|--------|
| `ata_response_category` | Excluded from primary inferential models. Descriptive only (Table 3). N=35 explicitly reported. |
| `biochemical_recurrence_flag` | Used in descriptive Table 3. Excluded from primary recurrence outcome in inferential models (use `structural_recurrence_flag` or `any_recurrence_flag` instead). |
| `rln_permanent_flag` | Reported as "not ascertainable" in Table 2. RLN models use combined `rln_status` (confirmed/probable vs absent). |
| `tg_doubling_time_days` | Exploratory model D.3.2 only. Not included in primary recurrence models. |
| Linkage scores (`path_link_score_v3`, `fna_link_score_v3`, etc.) | Used for quality assessment, not as model covariates. Analysis-eligible linkages require `linkage_score >= 0.50`. |

---

## G. Model Diagnostics and Robustness

### G.1 Logistic Regression Diagnostics

For each logistic model:

1. **Hosmer-Lemeshow test** (10 groups). Report chi-squared statistic and p-value.
   p > 0.05 indicates adequate fit.
2. **C-statistic (AUC)** with 95% CI via bootstrap (n = 1,000).
3. **Calibration plot**: predicted vs observed probabilities across deciles.
4. **Influential observations**: Cook's distance > 4/N threshold. Report count
   and clinical characteristics of influential patients.
5. **Multicollinearity**: Variance inflation factor (VIF) for each predictor.
   Flag VIF > 5.

### G.2 Cox PH Diagnostics

For each Cox model:

1. **Schoenfeld residuals**: Test PH assumption for each covariate. Report
   global test p-value and per-covariate p-values. If any covariate violates
   PH (p < 0.05), fit stratified Cox or add time-interaction.
2. **Concordance index (C-index)** with 95% CI.
3. **Deviance residuals**: Identify outliers (|residual| > 3).
4. **Log-log plot**: Visual check for parallel curves across strata.
5. **Landmark analysis** at 1 year: Re-fit Cox model restricted to patients
   event-free at 1 year to assess early vs late recurrence patterns.

### G.3 PSM Diagnostics

For PSM analyses (D.2.3):

1. **Pre-match balance table**: SMD for all covariates before matching.
2. **Post-match balance table**: SMD < 0.10 for all covariates after matching.
3. **Caliper sensitivity**: Repeat matching at 0.10, 0.15, 0.20, 0.25, 0.30,
   0.50 x SD calipers. Report HR and N pairs at each level.
4. **E-value**: Point estimate and lower 95% CI bound for unmeasured confounding.
5. **Doubly-robust estimation**: Weighted Cox regression using PS weights to
   adjust for residual imbalance.

### G.4 Mixed Model Diagnostics

For longitudinal lab model (D.3.1):

1. **Residual normality**: QQ-plot of level-1 residuals.
2. **Random effects distribution**: Histogram of random intercepts and slopes.
3. **ICC**: Intraclass correlation coefficient.
4. **AIC/BIC** comparison with simpler models (random intercept only).

### G.5 Reproducibility

- All random seeds set to 42.
- Exact software versions recorded in `requirements.txt`.
- Analyses executed via DuckDB on MotherDuck (`thyroid_research_2026`) with
  script version `resolved_layer_version = 'v1'`.
- Data frozen at `resolved_at` timestamp recorded in each analysis-grade table.

---

## H. Output Specification

### H.1 Tables

| Table ID | Title | Location |
|----------|-------|----------|
| Table 1 | Demographics and clinical characteristics | `studies/manuscript_tables/table1_demographics.csv` |
| Table 2 | Surgical complications | `studies/manuscript_tables/table2_complications.csv` |
| Table 3 | Recurrence and follow-up | `studies/manuscript_tables/table3_recurrence.csv` |
| Table S1 | FNA and imaging characteristics | `studies/manuscript_tables/tableS1_fna_imaging.csv` |
| Table S2 | Longitudinal lab summary | `studies/manuscript_tables/tableS2_labs.csv` |
| Table S3 | Score calculability and distribution | `studies/manuscript_tables/tableS3_scoring.csv` |
| Table S4 | Logistic regression results | `studies/manuscript_tables/tableS4_logistic.csv` |
| Table S5 | Cox PH results | `studies/manuscript_tables/tableS5_cox.csv` |
| Table S6 | Sensitivity analysis summary | `studies/manuscript_tables/tableS6_sensitivity.csv` |
| Table S7 | Missing data assessment | `studies/manuscript_tables/tableS7_missingness.csv` |
| Table S8 | MICE vs complete-case comparison | `studies/manuscript_tables/tableS8_mice.csv` |

All tables exported in CSV and LaTeX (booktabs) format. LaTeX files stored in
`exports/latex_tables/`.

### H.2 Figures

| Figure ID | Title | Format |
|-----------|-------|--------|
| Fig 1 | Study cohort flow diagram (CONSORT-style) | PNG 300 DPI + SVG |
| Fig 2 | AJCC8 stage distribution (grouped bar) | PNG 300 DPI + SVG |
| Fig 3 | KM curves by ATA risk stratum | PNG 300 DPI + SVG |
| Fig 4 | KM curves by molecular risk tier | PNG 300 DPI + SVG |
| Fig 5 | Forest plot: multivariable Cox HR | PNG 300 DPI + SVG |
| Fig S1 | Score concordance heatmap (AJCC8 calculated vs pathologist-reported) | PNG 300 DPI |
| Fig S2 | PSM balance plot (pre- and post-match SMD) | PNG 300 DPI |
| Fig S3 | Caliper sensitivity plot | PNG 300 DPI |
| Fig S4 | Tg trajectory by AJCC stage (spaghetti + mean) | PNG 300 DPI |
| Fig S5 | Model diagnostics panel (Schoenfeld, calibration, residuals) | PNG 300 DPI |

All figures stored in `exports/publication_figures_300dpi/` and
`studies/manuscript_draft/figures/`.

### H.3 Data Exports

| Export | Contents | Location |
|--------|----------|----------|
| Analysis dataset | `patient_analysis_resolved_v1` (CSV + Parquet) | `exports/FINAL_PUBLICATION_BUNDLE_*/ ` |
| Scoring dataset | `thyroid_scoring_systems_v1` or `thyroid_scoring_py_v1` | Same bundle |
| Lab dataset | `longitudinal_lab_clean_v1` summary | Same bundle |
| Complication dataset | `complication_patient_summary_v1` | Same bundle |
| Validation report | `val_analysis_resolved_v1` test results | Same bundle |
| Manifest | `manifest.json` with row counts, provenance, git SHA | Same bundle |

### H.4 Software

| Component | Version |
|-----------|---------|
| Python | >= 3.10 |
| DuckDB | <= 1.4.4 (MotherDuck compatibility) |
| pandas | >= 2.0 |
| lifelines | >= 0.27 |
| statsmodels | >= 0.14 |
| scikit-learn | >= 1.3 |
| scikit-survival | >= 0.22 |
| matplotlib | >= 3.8 |
| scipy | >= 1.11 |

### H.5 Alpha Spending and Multiple Comparisons

| Context | Method |
|---------|--------|
| Within a single model | Wald tests at alpha = 0.05 |
| Across models within a hypothesis | Benjamini-Hochberg FDR correction |
| Across hypotheses (H1 CLN, H2 Goiter, primary manuscript) | Report uncorrected p-values with Bonferroni-adjusted significance threshold noted |
| Sensitivity analyses | No additional correction (pre-specified robustness checks) |

---

## Appendix: Column Quick Reference

Key columns from `patient_analysis_resolved_v1` used throughout this SAP:

```
research_id                  -- integer primary key
age_at_surgery               -- numeric (years)
sex                          -- varchar (female/male)
race                         -- varchar (Black/White/Asian/Hispanic/Other/Unknown)
path_histology_raw           -- varchar (PTC_classic, FTC, MTC, etc.)
histology_final              -- varchar (normalized)
path_tumor_size_cm           -- double
ete_grade_final              -- varchar (none/microscopic/gross/present_ungraded)
vascular_invasion_final      -- varchar (absent/focal/extensive/present_ungraded)
margin_status_final          -- varchar (R0/R1/R2/Rx)
ln_positive_final            -- integer
path_ln_examined_raw         -- integer
ln_ratio                     -- double (from thyroid_scoring_systems_v1)
surg_procedure_type          -- varchar (total_thyroidectomy/hemithyroidectomy/other)
lateral_neck_dissected       -- boolean
braf_positive_final          -- boolean
ras_positive_final           -- boolean
tert_positive_final          -- boolean
molecular_risk_tier          -- varchar (high/intermediate_braf/intermediate_ras/low/unknown)
fna_bethesda_final           -- varchar (I--VI)
imaging_tirads_best          -- integer (1--5)
imaging_tirads_category      -- varchar (TR1--TR5)
imaging_nodule_size_cm       -- double
rai_received_flag            -- boolean
rai_max_dose_mci             -- double
ajcc8_t_stage                -- varchar (T1a/T1b/T2/T3a/T3b/T4a/T4b)
ajcc8_n_stage                -- varchar (N0/N1a/N1b)
ajcc8_m_stage                -- varchar (M0/M1/M1_biochemical)
ajcc8_stage_group            -- varchar (I/II/III/IVA/IVB)
ajcc8_calculable_flag        -- boolean
ata_risk_category            -- varchar (low/intermediate/high)
ata_calculable_flag          -- boolean
ata_response_category        -- varchar (excellent/indeterminate/biochemical_incomplete/structural_incomplete)
                             -- **PROVISIONAL** (N=35)
macis_score                  -- double
macis_risk_group             -- varchar (low/intermediate/high/very_high)
ages_score                   -- double
ames_risk_group              -- varchar (low/high)
any_recurrence_flag          -- boolean
structural_recurrence_flag   -- boolean
biochemical_recurrence_flag  -- boolean  **PROVISIONAL**
recurrence_date              -- date
recurrence_type_primary      -- varchar
recurrence_site_primary      -- varchar
hypocalcemia_status          -- varchar (biochemical_only/.../confirmed_permanent/absent/unknown)
rln_status                   -- varchar (confirmed_permanent/.../absent/unknown)
rln_permanent_flag           -- boolean  **DATA GAP (always FALSE)**
rln_transient_flag           -- boolean
any_confirmed_complication   -- boolean
tg_nadir                     -- double
tg_last_value                -- double
tg_rising_flag               -- boolean
lab_completeness_score       -- integer (0--100)
analysis_eligible_flag       -- boolean (primary eligibility gate)
molecular_eligible_flag      -- boolean
rai_eligible_flag            -- boolean
survival_eligible_flag       -- boolean
scoring_ajcc8_flag           -- boolean
scoring_ata_flag             -- boolean
scoring_macis_flag           -- boolean
provenance_confidence        -- integer (0--100)
resolved_layer_version       -- varchar ('v1')
```
