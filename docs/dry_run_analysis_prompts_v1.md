# Dry-Run Statistical Analysis Prompts — Canonical Dataset Verification

**Database:** `thyroid_canonical_publication_v1_0`  
**Schema:** `main` → `canonical_patient_master` (N=10,871; 1,377 columns)  
**Workspace views:** `manuscript_workspace` schema (64 views)  
**Date created:** 2026-04-16  

> **How to use:** Copy-paste one prompt per new Claude Cowork session. Each is self-contained.  
> The agent will connect to MotherDuck, run the analysis, and report findings.  
> These are DRY RUNS — read-only queries, no data modifications.  
> Use findings to flag data quality issues, missing values, or impossible ranges.

---

## PROMPT 1 — Table 1 Generator & Demographics Sanity Check

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients, 1,377 columns).
All queries MUST use the fully qualified three-part name: thyroid_canonical_publication_v1_0.main.canonical_patient_master.
NEVER reference any other database. This is a READ-ONLY dry run — do not create or modify anything.

TASK: Generate a standard "Table 1" for the full cohort and flag any data quality issues.

Run the following analyses:

1. DEMOGRAPHICS SUMMARY:
   - Count and % for: sex, race
   - age_at_surgery: mean, median, SD, min, max, IQR, count of NULLs
   - bmi_combined: mean, median, SD, min, max, count of NULLs, count where BMI < 10 or BMI > 80 (implausible)
   - Flag: any patients with age_at_surgery < 0 or > 110? Any sex values other than 'Male'/'Female'/NULL?

2. SURGERY SUMMARY:
   - surg_procedure_type: count and % for each value
   - surg_total_thyroidectomy vs surg_hemithyroidectomy: crosstab, check they're mutually consistent
   - n_surgeries: distribution (1, 2, 3+)
   - Flag: any patients with surg_total_thyroidectomy=true AND surg_hemithyroidectomy=true simultaneously?

3. PATHOLOGY SUMMARY:
   - histology_final: top 10 values by count with %
   - is_malignant: count and % (true/false/NULL)
   - tumor_size_cm: mean, median, SD, min, max among non-NULL
   - Flag: tumor_size_cm > 15 cm? tumor_size_cm = 0? Negative values?

4. STAGING:
   - ajcc8_stage_group: distribution (I, II, III, IV, IVA, IVB, NULL)
   - ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage: distribution
   - Flag: any M1 patients who have ajcc8_stage_group = 'I'? (staging inconsistency)

5. MISSING DATA HEATMAP:
   - For the following key columns, report NULL count and % NULL:
     age_at_surgery, sex, race, histology_final, is_malignant, tumor_size_cm,
     bethesda_final, ajcc8_stage_group, surg_procedure_type, any_recurrence_flag,
     vital_status, overall_survival_years, followup_years, molecular_tested_confirmed,
     rai_received_flag, ln_total_examined, ete_grade

Present results as formatted tables. At the end, list the top 5 data quality concerns you found, ranked by severity.
```

---

## PROMPT 2 — Molecular Testing Yield & Platform Validation

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients).
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

TASK: Audit the molecular testing data for completeness and internal consistency.

1. TESTING RATES:
   - molecular_tested_confirmed: count true/false/NULL
   - Among tested: mol_platform distribution (Afirma, ThyroSeq, other, NULL)
   - mol_has_afirma, mol_has_thyroseq: counts, overlap (both true?)
   - mol_n_tests: distribution (1, 2, 3+, NULL)

2. MUTATION LANDSCAPE:
   - Among molecular_tested_confirmed=true:
     - braf_positive_final: count true/false/NULL, % positive
     - ras_positive_final: count true/false/NULL
     - tert_positive_final: count true/false/NULL
     - ret_positive_v7: count true/false/NULL
     - any_fusion_positive: count true/false/NULL
   - Flag: any patient with braf_positive_final=true AND ras_positive_final=true? (rare but possible — count them)
   - Flag: any patient with molecular_tested_confirmed=false but braf_positive_final=true? (inconsistency)

3. MOLECULAR × BETHESDA CROSSTAB:
   - Cross-tabulate bethesda_final (1-6) × molecular_tested_confirmed (true/false)
   - Among Bethesda III/IV (indeterminate): what % had molecular testing?
   - Among Bethesda V/VI: what % had molecular testing?

4. MOLECULAR × OUTCOME:
   - Among molecular_tested_confirmed=true: is_malignant rate
   - Among molecular_tested_confirmed=false: is_malignant rate
   - Among BRAF+ vs BRAF-: any_recurrence_flag rate, mean tumor_size_cm

5. TEMPORAL CHECK:
   - mol_first_test_days_from_surg: distribution (negative = preop, positive = postop)
   - Flag: any mol_first_test_days_from_surg > 365? (molecular test >1yr after surgery is unusual)

Present findings as tables. List the top 5 molecular data quality concerns.
```

---

## PROMPT 3 — Survival & Recurrence Integrity Audit

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients).
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

TASK: Validate survival and recurrence data for internal consistency and plausibility.

1. VITAL STATUS:
   - vital_status: distribution of all values
   - death_occurred: count true/false/NULL
   - Flag: vital_status = 'Dead' but death_occurred = false? Or vice versa?
   - overall_survival_years: mean, median, max, min, SD among non-NULL
   - Flag: overall_survival_years < 0? overall_survival_years > 30?

2. FOLLOW-UP COMPLETENESS:
   - followup_years: mean, median, IQR, max, count of NULLs
   - followup_category: distribution
   - followup_completeness_score: distribution (histogram buckets)
   - Flag: any patient with followup_years > 27? (cohort started ~1999)
   - Flag: any patient with death_occurred=true but followup_years is NULL?

3. RECURRENCE:
   - any_recurrence_flag: count true/false/NULL
   - Among recurrence=true: recurrence_type distribution, recurrence_site distribution
   - time_to_recurrence_days: mean, median, min, max among recurrences
   - Flag: time_to_recurrence_days < 0? (recurrence before surgery)
   - Flag: time_to_recurrence_days > followup_days? (recurrence after last contact)
   - biochemical_recurrence_flag vs structural_recurrence_flag: crosstab

4. KAPLAN-MEIER DRY RUN (SQL-based):
   - Calculate 5-year and 10-year recurrence-free survival rates using:
     - Time variable: LEAST(followup_days, time_to_recurrence_days) / 365.25
     - Event: any_recurrence_flag = true
     - Stratify by: surg_procedure_type (total thyroidectomy vs lobectomy)
   - Use a simple life-table approach in SQL (1-year intervals) rather than requiring Python
   - Report the number at risk and cumulative survival at years 1, 3, 5, 10

5. DEATH ANALYSIS:
   - Count deaths, crude mortality rate
   - Among deaths: mean age_at_surgery, histology_final distribution, ajcc8_stage_group distribution
   - Flag: any death with overall_survival_days < 30? (perioperative mortality — verify)

List top 5 survival/recurrence data integrity issues.
```

---

## PROMPT 4 — RAI & Thyroglobulin Kinetics Validation

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients).
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

TASK: Audit RAI treatment data and thyroglobulin kinetics for completeness and consistency.

1. RAI TREATMENT RATES:
   - rai_received_flag: count true/false/NULL
   - Among RAI-treated: n_rai_episodes distribution (1, 2, 3+)
   - rai_dose_v9: mean, median, min, max, SD (mCi)
   - rai_total_cumulative_dose_mci: mean, median, max
   - Flag: rai_dose_v9 > 300 mCi? (unusually high single dose)
   - Flag: rai_received_flag=true but n_rai_episodes=0 or NULL?

2. RAI × SURGERY TYPE:
   - Cross-tabulate rai_received_flag × surg_procedure_type
   - Flag: any lobectomy-only patient who received RAI? (unusual but possible — count)

3. RAI × HISTOLOGY:
   - RAI rate by histology_final (top 8 histologies)
   - Flag: RAI given to benign (is_malignant=false) patients? Count them.

4. THYROGLOBULIN KINETICS:
   - tg_n_measurements: distribution (0, 1-3, 4-10, 10+, NULL)
   - tg_nadir: median, IQR among non-NULL
   - tg_peak: median, max among non-NULL
   - tg_rising_flag: count true/false/NULL
   - tg_trajectory_class: distribution
   - Flag: tg_peak > 10000? (possible assay artifact)
   - Flag: tg_nadir > tg_peak? (impossible if both non-NULL)

5. Tg × RECURRENCE:
   - Among patients with tg_n_measurements >= 2:
     - Mean tg_nadir in recurrence=true vs recurrence=false
     - tg_rising_flag rate in recurrence=true vs recurrence=false
   - Flag: any_recurrence_flag=true but tg_nadir < 0.1 AND tg_rising_flag=false? (unexpected)

6. TgAb INTERFERENCE:
   - tgab_interference_flag: count true/false/NULL
   - Among interference=true: mean tg_n_measurements, tg_trajectory_class distribution

List top 5 RAI/Tg data quality concerns.
```

---

## PROMPT 5 — Lymph Node & Staging Deep Dive

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients).
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

TASK: Validate lymph node data, staging consistency, and extrathyroidal extension.

1. LYMPH NODE BASICS:
   - ln_total_examined: mean, median, max, count of NULL, count of 0
   - ln_total_positive: mean, median, max among examined > 0
   - ln_positive_flag: count (1/0/NULL)
   - ln_ratio: mean, median, max among ln_total_examined > 0
   - Flag: ln_total_positive > ln_total_examined? (impossible)
   - Flag: ln_positive_flag=1 but ln_total_positive=0? Or ln_positive_flag=0 but ln_total_positive > 0?

2. LEVEL-SPECIFIC BREAKDOWN:
   - For levels I through VII (ln_level_i_examined through ln_level_vii_examined):
     - Count of patients with non-NULL data per level
     - Sum of examined, sum of positive per level
   - Central (level VI) vs lateral (levels II-V): how many patients had both?
   - Flag: any level where positive > examined?

3. ENE (EXTRANODAL EXTENSION):
   - ene_positive: count true/false/NULL
   - best_ene_grade: distribution
   - Among ene_positive=true: mean ln_total_positive, mean tumor_size_cm
   - Flag: ene_positive=true but ln_positive_flag != 1?

4. ETE (EXTRATHYROIDAL EXTENSION):
   - ete_grade: distribution (none, microscopic, gross, NULL)
   - gross_ete_flag: count true/false/NULL
   - Cross-tabulate ete_grade × ajcc8_t_stage (gross ETE should map to T3b/T4)
   - Flag: gross_ete_flag=true but ajcc8_t_stage in ('T1a','T1b','T2')? (staging error)

5. STAGING CONSISTENCY:
   - Reconstruct expected ajcc8_stage_group from t_stage + n_stage + m_stage + age:
     - DTC age < 55: Stage I (any T, any N, M0) or Stage II (M1)
     - DTC age >= 55: Stage I (T1-T2, N0, M0), Stage II (T1-T2, N1 or T3, any N, M0), etc.
   - Compare reconstructed vs recorded ajcc8_stage_group
   - Report % concordance and list the discordant patterns with counts

6. MARGIN STATUS:
   - margin_status_final: distribution
   - Cross-tabulate margin_status_final × any_recurrence_flag
   - Flag: margin_status_final = 'R2' or 'positive' but no recurrence and short follow-up?

List top 5 LN/staging data quality concerns.
```

---

## PROMPT 6 — Parathyroid & Calcium/PTH Outcomes

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients).
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

TASK: Audit parathyroid, calcium, and PTH data for completeness and clinical plausibility.

1. PARATHYROID SPECIMEN DATA:
   - para_specimen_included: count true/false/NULL
   - para_has_pathologic_glands: count true/false/NULL
   - para_n_glands_identified: mean, median, max among non-NULL
   - para_abnormality_type: distribution
   - Flag: para_n_glands_identified > 8? (very unusual)
   - Flag: para_has_pathologic_glands=true but para_specimen_included=false?

2. POSTOP CALCIUM:
   - lab_calcium_n_measurements: distribution (0, 1-3, 4-10, 10+)
   - lab_calcium_min: mean, min, count where < 7.0 (severe hypocalcemia)
   - calcium_nadir_30d: mean, median, min
   - postop_calcium_min_value: distribution
   - Flag: lab_calcium_min < 5.0? (likely lab error or critical event)
   - Flag: has_low_calcium_flag=true but lab_calcium_min > 8.5?

3. POSTOP PTH:
   - lab_pth_n_measurements: distribution (0, 1-3, 4-10, 10+)
   - lab_pth_min: mean, median, min
   - pth_nadir_30d: mean, median among non-NULL
   - Flag: lab_pth_min < 0? (impossible)
   - Flag: has_low_pth_flag=true but lab_pth_min > 15?

4. HYPOPARATHYROIDISM:
   - comp_hypoparathyroidism_confirmed: count true/false/NULL
   - comp_hypoparathyroidism_transient vs permanent: counts
   - Cross-tabulate with surg_procedure_type (total vs hemi)
   - Flag: comp_hypoparathyroidism_confirmed=true in lobectomy-only patients? (very rare)

5. CALCIUM × PTH CONCORDANCE:
   - Among patients with both lab_calcium_min and lab_pth_min non-NULL:
     - Correlation between the two
     - Scatter summary: low Ca + low PTH (hypoparathyroidism), low Ca + high PTH (other cause), normal both
   - comp_hypocalcemia_confirmed vs has_low_calcium_flag: concordance rate

6. NSQIP CALCIUM CROSS-VALIDATION:
   - nsqip_hypocalcemia: distribution among nsqip_thyroidectomy_has_data=true
   - Compare nsqip_hypocalcemia_flag with comp_hypocalcemia_confirmed
   - Report concordance/discordance rates

List top 5 parathyroid/calcium data quality concerns.
```

---

## PROMPT 7 — TIRADS & FNA Diagnostic Accuracy

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients).
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

TASK: Validate TIRADS scoring, FNA/Bethesda data, and diagnostic accuracy metrics.

1. TIRADS COVERAGE:
   - tirads_best_category_v12: distribution (TR1-TR5, NULL)
   - tirads_n_sources_v12: distribution
   - tirads_reliability_v12: mean, median, min
   - Flag: tirads_best_score_v12 > 10? (ACR TIRADS max is usually 3+2+3+3+3=14 but most are < 10)
   - Flag: tirads_best_category_v12 not in ('TR1','TR2','TR3','TR4','TR5', NULL)?

2. FNA / BETHESDA:
   - bethesda_final: distribution (1-6, NULL)
   - n_fna_episodes: distribution (0, 1, 2, 3+)
   - fna_path_concordance_category: distribution
   - Flag: bethesda_final NOT IN (1,2,3,4,5,6) and not NULL?
   - Flag: n_fna_episodes = 0 but bethesda_final is not NULL? (got a Bethesda without FNA?)

3. RISK OF MALIGNANCY BY BETHESDA:
   - For each bethesda_final (1-6): count, is_malignant rate, 95% Wilson CI
   - Compare to published Bethesda ROM benchmarks:
     Bethesda I: 1-4%, II: 0-3%, III: 6-18%, IV: 10-40%, V: 45-75%, VI: 94-100%
   - Flag any category where observed ROM falls outside expected range

4. TIRADS × MALIGNANCY:
   - For each tirads_best_category_v12: count, is_malignant rate
   - Expected pattern: TR1-2 very low, TR3 < 5%, TR4 5-20%, TR5 > 35%
   - Flag inversions (e.g., TR3 higher malignancy than TR5)

5. FNA × MOLECULAR × FINAL PATH (triple crossover):
   - Among bethesda_final IN (3,4) AND molecular_tested_confirmed=true:
     - is_malignant rate by braf_positive_final (true vs false)
     - is_malignant rate by ras_positive_final (true vs false)
   - This validates whether molecular results are predictive in our cohort

6. DIAGNOSTIC ACCURACY METRICS (FNA vs final path):
   - Restrict to: bethesda_final IN (2,5,6) AND is_malignant IS NOT NULL
   - Treat Bethesda 2 as "test negative", Bethesda 5-6 as "test positive"
   - Calculate: sensitivity, specificity, PPV, NPV
   - Flag: if NPV < 95% for Bethesda 2, that's a data issue (expected 97-99%)

List top 5 TIRADS/FNA data quality concerns.
```

---

## PROMPT 8 — Complications & Surgical Outcomes

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients).
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

TASK: Audit complication data across multiple domains for rates and internal consistency.

1. COMPLICATION RATES (among all patients):
   For each complication, report confirmed count, %, and transient vs permanent split:
   - comp_hypocalcemia (expected: 20-30% transient after total thyroidectomy)
   - comp_hypoparathyroidism (expected: 15-25% transient)
   - comp_rln_injury (expected: 1-5%)
   - comp_vc_paralysis (expected: 0.5-3%)
   - comp_vc_paresis (expected: 2-5%)
   - comp_hematoma (expected: 1-3%)
   - comp_seroma (expected: 1-5%)
   - comp_wound_infection (expected: 1-2%)
   - comp_chyle_leak (expected: < 1%)
   - Flag: any rate that is 3x above expected range (possible over-coding)
   - Flag: any rate that is 10x below expected range (possible under-ascertainment)

2. COMPLICATION × SURGERY TYPE:
   - For hypocalcemia, RLN injury, and hematoma:
     - Rate in total thyroidectomy vs lobectomy
   - Flag: hypocalcemia rate in lobectomy > 10%? (very unusual)

3. EVIDENCE TIER DISTRIBUTION:
   - For each complication: distribution of evidence_tier (1=strong, 2, 3=weak)
   - What % of confirmed complications are tier 1?

4. TIMING:
   - For each complication: mean, median days_postop among confirmed
   - Flag: any complication with days_postop < 0? (before surgery)
   - Flag: any complication with days_postop > 365?

5. VOICE OUTCOMES:
   - has_voice_data: count
   - voice_outcome_category: distribution
   - rln_classification: distribution
   - Cross-tabulate rln_injury_type × voice_outcome_category
   - Flag: RLN injury confirmed but voice_outcome = 'normal'? (possible but flag for review)

6. NSQIP CROSS-VALIDATION:
   - Among nsqip_thyroidectomy_has_data=true:
     - Compare nsqip_rln_injury_flag with comp_rln_injury_confirmed
     - Compare nsqip_hematoma_flag with comp_hematoma_confirmed
     - Report concordance matrix for each

List top 5 complication data quality concerns.
```

---

## PROMPT 9 — NLP Extraction Coverage & Cross-Source Validation

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients).
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

TASK: Audit NLP-extracted data coverage and cross-validate against structured sources.

1. NLP DOMAIN COVERAGE (for each nlp_*_has_data column, count true/false/NULL):
   - nlp_frozensec_has_data, nlp_path_has_data, nlp_parathyroid_has_data
   - nlp_tg_has_data, nlp_tirads_has_data, nlp_rec_has_data
   - nlp_ln_has_data, nlp_pmhx_has_data, nlp_synoptic_has_data
   - nlp_imaging_has_data, nlp_labs_has_data, nlp_vasc_has_data
   Report as a table: domain, n_with_data, pct_with_data

2. NLP vs STRUCTURED CROSS-VALIDATION:
   a) Frozen section:
      - nlp_frozensec_has_data=true vs syn_frozen_section=true: concordance
      - nlp_frozensec_key_finding='carcinoma' rate vs syn_carcinoma_on_frozen=true rate
   b) Pathology:
      - nlp_path_ete_mentioned=true vs ete_grade != 'none' AND ete_grade IS NOT NULL
      - nlp_path_multifocal_mentioned=true vs multifocal_flag=true
      - nlp_path_ln_positive_mentioned=true vs ln_positive_flag=1
   c) Recurrence:
      - nlp_rec_any_mentioned=true vs any_recurrence_flag=true: concordance
      - nlp_rec_disease_free_mentioned=true vs any_recurrence_flag=false: concordance

3. NLP ENTITY DENSITY:
   - For nlp_path_n_entities, nlp_ln_n_entities, nlp_tg_n_entities:
     mean, median, max
   - Flag: any nlp domain where has_data=true but n_entities=0?

4. OPERATIVE NOTE NLP:
   - op_nlp_nerve_monitoring_used: count true/false/NULL
   - op_nlp_parathyroid_managed: count true/false/NULL
   - op_nlp_reoperative_field: count true/false/NULL, cross-check with op_reoperative_any
   - op_nlp_ebl_ml: mean, median, max among non-NULL
   - Flag: op_nlp_ebl_ml > 2000? (very high for thyroid surgery)

5. MEDICATION NLP:
   - med_nlp_levothyroxine: count true, expected to be high post-thyroidectomy
   - med_nlp_calcium_supplement: count true, cross-check with comp_hypocalcemia_confirmed
   - med_nlp_calcitriol: count true
   - Flag: surg_total_thyroidectomy=true AND med_nlp_levothyroxine=false AND followup_years > 1?
     (should be on levothyroxine)

List top 5 NLP data quality concerns.
```

---

## PROMPT 10 — Imaging & Ultrasound Data Validation

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients).
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

TASK: Audit imaging data (US, CT, MRI, PET, nuclear medicine) for coverage and consistency.

1. ULTRASOUND COVERAGE:
   - n_us_exams: distribution (0, 1-3, 4-10, 10+)
   - n_us_nodules_total: mean, max
   - us_first_exam_days_from_surg: mean, median (negative = preop)
   - us_last_exam_days_from_surg: mean, median, max
   - Flag: n_us_exams = 0 but tirads_best_category_v12 is NOT NULL? (TIRADS without US?)

2. CT COVERAGE:
   - ct_n_exams: distribution (0, 1-3, 4+)
   - ct_substernal_extension_any: count true
   - ct_tracheal_deviation_any: count true
   - ct_ln_suspicious_any: count true, cross-check with ln_positive_flag

3. MRI:
   - mri_has_data: count true/false/NULL
   - mri_n_exams: distribution among mri_has_data=true
   - mri_substernal_any: count, cross-check with ct_substernal_extension_any

4. PET:
   - pet_has_data: count true
   - pet_n_exams: distribution
   - pet_distant_mets_ever: count true, cross-check with ajcc8_m_stage='M1'
   - Flag: pet_distant_mets_ever=true but ajcc8_m_stage != 'M1'?

5. NUCLEAR MEDICINE / RAI SCANS:
   - nucmed_has_rai_scan: count true
   - nucmed_n_scans: distribution
   - nucmed_scan_types: top values
   - Cross-check: rai_received_flag=true should overlap heavily with nucmed_has_rai_scan=true
   - Flag: rai_received_flag=true but nucmed_has_rai_scan=false? (possible — outpatient RAI)

6. SIZE CONCORDANCE ACROSS SOURCES:
   - Compare: tumor_size_cm (path), imaging_nodule_size_cm (imaging), dominant_nodule_size_cm
   - Among patients with all three non-NULL: correlation, mean absolute difference
   - Flag: |path_size - imaging_size| > 3 cm? (large discrepancy)

List top 5 imaging data quality concerns.
```

---

## PROMPT 11 — Comorbidity & PMH Plausibility

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients).
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

TASK: Validate past medical history and comorbidity data for plausibility and completeness.

1. KEY COMORBIDITIES (from pmhx_nlp_* columns):
   For each, report count true, % prevalence:
   - pmhx_nlp_hypertension (expected: 25-40% in surgical cohort)
   - pmhx_nlp_diabetes (expected: 10-20%)
   - pmhx_nlp_obesity (expected: 15-30%)
   - pmhx_nlp_hypothyroidism (expected: 5-15%)
   - pmhx_nlp_hyperthyroidism (expected: 3-10%)
   - pmhx_nlp_depression (expected: 5-15%)
   - pmhx_nlp_cad (expected: 3-8%)
   - pmhx_nlp_ckd (expected: 2-5%)
   - pmhx_nlp_breast_cancer (expected: 1-3%)
   - pmhx_nlp_prior_cancer_hx
   - pmhx_nlp_radiation_exposure
   - pmhx_nlp_family_hx_thyroid
   - Flag any prevalence > 2x or < 0.5x expected range

2. AUTOIMMUNE THYROID:
   - syn_graves: count true
   - syn_hashimoto: count true
   - syn_chronic_thyroiditis: count true
   - pmhx_nlp_autoimmune_thyroid_hx: count true
   - Cross-check: syn_graves OR syn_hashimoto should overlap with pmhx_nlp_autoimmune_thyroid_hx
   - Flag discordance rate

3. SMOKING:
   - pmhx_nlp_smoking_status: distribution
   - nsqip_smoker: distribution among NSQIP patients
   - Cross-check concordance between the two

4. HEREDITARY:
   - pmhx_nlp_men_syndrome: count (MEN syndrome, very rare < 0.5%)
   - pmhx_nlp_family_hx_thyroid: count
   - pmhx_nlp_family_hx_cancer: count
   - Flag: pmhx_nlp_men_syndrome=true but histology_final not MTC? (MEN usually presents with MTC)

5. COMORBIDITY COUNT:
   - pmhx_nlp_n_comorbidities: mean, median, max, distribution
   - Correlation with age_at_surgery (older patients should have more comorbidities)
   - Flag: pmhx_nlp_n_comorbidities > 15? (possible over-extraction)
   - Flag: age > 70 but pmhx_nlp_n_comorbidities = 0? (possible under-extraction)

List top 5 comorbidity data quality concerns.
```

---

## PROMPT 12 — Synoptic Pathology & Gland Weight Validation

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
The main analytic table is: thyroid_canonical_publication_v1_0.main.canonical_patient_master (N=10,871 patients).
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

TASK: Audit synoptic pathology data and gland weight measurements.

1. SYNOPTIC COVERAGE:
   - nlp_synoptic_has_data: count true/false/NULL
   - syn_frozen_section: count true/false/NULL
   - syn_carcinoma_on_frozen: count true among syn_frozen_section=true

2. HISTOLOGY FROM SYNOPTIC:
   - syn_architecture: distribution
   - syn_histologic_grade: distribution (1, 2, 3, NULL)
   - syn_has_second_tumor: count true, and syn_tumor2_histologic_type distribution
   - syn_has_third_plus_tumor: count true
   - Flag: syn_histologic_grade = 3 but histology_final not in aggressive variants?

3. INVASION MARKERS:
   - syn_capsular_invasion_clean: distribution
   - syn_lymphatic_invasion_clean: distribution
   - lvi_grade_final_v13: distribution, cross-check with syn_lymphatic_invasion_clean
   - vasc_grade_final_v13: distribution
   - pni_refined_v6: distribution
   - Flag: lvi_grade_final_v13 says 'extensive' but vasc_grade_final_v13 says 'none'?

4. MARGIN FROM SYNOPTIC:
   - syn_margin_status_synoptic: distribution
   - syn_margin_distance_mm: among non-NULL, mean, median, min
   - Cross-check with margin_status_final: concordance rate
   - Flag: syn_margin_distance_mm = 0 but syn_margin_status_synoptic = 'negative'?

5. GLAND WEIGHT:
   - gland_weight_total_reported_g: mean, median, max, count non-NULL
   - syn_total_weight_g: mean, median, max, count non-NULL
   - Cross-check: correlation between the two among patients with both non-NULL
   - Flag: gland_weight > 500g? (massive goiter territory)
   - Flag: gland_weight < 1g? (possible measurement error)
   - Lobe weights: syn_left_lobe_weight_g + syn_right_lobe_weight_g vs syn_total_weight_g
     (should be approximately equal — flag discrepancies > 20%)

6. BACKGROUND THYROID:
   - syn_multinodular_goiter: count true
   - syn_chronic_thyroiditis: count true (should match pmhx numbers roughly)
   - syn_follicular_adenoma: count true
   - syn_colloid_nodule: count true
   - syn_adenomatoid_nodules: count true
   - syn_hurthle_cell_change: count true

List top 5 synoptic/gland data quality concerns.
```

---

---

# PART 2 — Detail Table Cross-Validation Prompts

> These prompts verify that the one-row-per-patient rollup in `canonical_patient_master`
> accurately reflects the multi-row detail tables. This is where rollup bugs hide:
> wrong counts, dropped rows, stale max/min values, orphan records.
>
> **Key detail tables (rows / patients):**
> `fna_episode_master_v2` (8,119 / 5,266) · `us_nodules_tirads` (10,862 / 10,862)
> `imaging_nodule_long_v2` (19,891 / 3,439) · `molecular_test_episode_v2` (10,126 / 10,026)
> `molecular_variant_long` (1,640 / 703) · `rai_treatment_episode_v2` (1,857 / 862)
> `thyroglobulin_lab_canonical_v1` (76,971 / 3,258) · `longitudinal_lab_canonical_v1` (77,960 / 3,690)
> `synoptic_tumor_long_v1` (11,103 / 8,422) · `tumor_episode_master_v2` (11,691 / 10,871)
> `complication_phenotype_v1` (5,928 / 2,892) · `recurrence_event_clean_v1` (1,946 / 1,946)
> `operative_episode_detail_v2` (9,371 / 9,368) · `ln_master_rollup_v1` (4,290 / 3,986)
> `path_synoptics` (11,688 / 10,871)

---

## PROMPT 13 — FNA Episode Detail ↔ Master Rollup

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
All queries MUST use the fully qualified three-part name: thyroid_canonical_publication_v1_0.main.<table>.
NEVER reference any other database. READ-ONLY dry run.

CONTEXT: The detail table `fna_episode_master_v2` has one row per FNA episode (~8,119 rows, 5,266 patients).
The master rollup `canonical_patient_master` has one row per patient (10,871) with summary columns like
n_fna_episodes, bethesda_final, n_fna_cytology_records.

TASK: Cross-validate FNA detail against the master rollup.

1. COUNT CONCORDANCE:
   - For each patient in fna_episode_master_v2, count FNA episodes.
   - JOIN to canonical_patient_master on research_id.
   - Compare the detail-derived count vs cpm.n_fna_episodes.
   - Report: n patients where counts match, n where detail > master, n where detail < master.
   - List top 10 patients with the largest discrepancy (by research_id, detail count, master count).

2. BETHESDA CONCORDANCE:
   - For each patient in fna_episode_master_v2, compute MAX(bethesda_category) cast as integer.
   - Compare to canonical_patient_master.bethesda_final.
   - Report concordance rate. List discordant patterns with counts.
   - Flag: patients with bethesda_final NOT NULL in master but ZERO rows in fna_episode_master_v2?

3. ORPHAN CHECK:
   - Any research_id in fna_episode_master_v2 that does NOT exist in canonical_patient_master?
   - Any research_id with n_fna_episodes > 0 in master but ZERO rows in fna_episode_master_v2?

4. DATE INTEGRITY:
   - fna_episode_master_v2.resolved_fna_date: count NULLs, count where date_status != 'resolved'
   - Any resolved_fna_date before 1995 or after 2026? (implausible)
   - Cross-check: does the earliest FNA date per patient precede their first_surgery_date in the master?
     (expected for preop FNA — but flag if FNA is >5 years before surgery)

5. LINKAGE COVERAGE:
   - linked_molecular_episode_id: % non-NULL (how many FNAs link to molecular tests?)
   - linked_surgery_episode_id: % non-NULL
   - linked_imaging_nodule_id: % non-NULL
   - Flag: any FNA with bethesda_category IN (3, 4) but linked_molecular_episode_id is NULL?
     (indeterminate nodules often get molecular testing — count the gap)

List top 5 FNA detail-vs-master data integrity concerns.
```

---

## PROMPT 14 — Molecular Detail ↔ Master Rollup

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
All queries MUST use the fully qualified three-part name: thyroid_canonical_publication_v1_0.main.<table>.
NEVER reference any other database. READ-ONLY dry run.

CONTEXT: Two detail tables:
- `molecular_test_episode_v2` (~10,126 rows, 10,026 patients) — one row per molecular test episode
- `molecular_variant_long` (~1,640 rows, 703 patients) — one row per variant call
Master rollup: `canonical_patient_master` with mol_n_tests, molecular_tested_confirmed,
braf_positive_final, ras_positive_final, tert_positive_final, mol_platform, etc.

TASK: Cross-validate molecular detail against master.

1. TEST COUNT CONCORDANCE:
   - Count molecular_test_episode_v2 rows per patient.
   - Compare to cpm.mol_n_tests. Report match/mismatch counts.
   - Flag: patients with mol_n_tests=0 in master but >0 rows in detail?

2. TESTED FLAG CONCORDANCE:
   - Derive: patient has >=1 row in molecular_test_episode_v2 WHERE cancelled_flag=false
     AND inadequate_flag=false → should be molecular_tested_confirmed=true in master.
   - Compare. Report discordance count.

3. MUTATION FLAG CONCORDANCE:
   - From molecular_test_episode_v2: derive braf_flag=true for any episode → should match
     cpm.braf_positive_final=true.
   - Same for ras_flag, tert_flag, ret_flag, fusion_flag.
   - Report concordance for each. List discordant research_ids.

4. VARIANT DETAIL:
   - molecular_variant_long: top 10 gene_symbol values by count.
   - variant_class distribution (SNV, fusion, indel, etc.)
   - Any research_id in molecular_variant_long NOT in canonical_patient_master? (orphans)
   - Any research_id in molecular_variant_long NOT in molecular_test_episode_v2? (broken link)

5. PLATFORM CONCORDANCE:
   - From molecular_test_episode_v2: derive platform per patient (most common if multiple).
   - Compare to cpm.mol_platform. Report concordance rate.
   - mol_has_afirma in master vs COUNT of episodes WHERE platform ILIKE '%afirma%' > 0.
   - mol_has_thyroseq in master vs COUNT WHERE platform ILIKE '%thyroseq%' > 0.

List top 5 molecular detail-vs-master integrity concerns.
```

---

## PROMPT 15 — RAI Episode Detail ↔ Master Rollup

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
All queries MUST use the fully qualified three-part name: thyroid_canonical_publication_v1_0.main.<table>.
NEVER reference any other database. READ-ONLY dry run.

CONTEXT: `rai_treatment_episode_v2` (~1,857 rows, 862 patients) — one row per RAI treatment episode.
Master rollup: `canonical_patient_master` with n_rai_episodes, rai_received_flag, rai_dose_v9,
rai_total_cumulative_dose_mci, rai_max_dose_mci, confirmed_rai_episodes, rai_first_date, etc.

TASK: Cross-validate RAI detail against master.

1. EPISODE COUNT:
   - Count rai_treatment_episode_v2 rows per patient WHERE rai_assertion_status NOT IN ('negated','denied','historical').
   - Compare to cpm.n_rai_episodes and cpm.confirmed_rai_episodes.
   - Report: n patients matching, n mismatched. Top 10 discrepant research_ids.

2. RECEIVED FLAG:
   - Derive: patient has >=1 confirmed episode → rai_received_flag should be true.
   - Report discordance. Specifically flag: rai_received_flag=true in master but 0 confirmed episodes in detail.

3. DOSE CONCORDANCE:
   - From detail: compute MAX(dose_mci) per patient → compare to cpm.rai_max_dose_mci.
   - From detail: compute SUM(dose_mci) per patient → compare to cpm.rai_total_cumulative_dose_mci.
   - Report mean absolute difference for each. Flag patients where |delta| > 50 mCi.

4. DATE CONCORDANCE:
   - From detail: MIN(resolved_rai_date) per patient → compare to cpm.rai_first_date (or rai_first_episode_date).
   - Report: n matches (within 30 days), n mismatches. Flag dates >1 year apart.

5. INTENT & AVIDITY:
   - rai_intent distribution in detail (ablation, adjuvant, therapeutic, diagnostic, etc.)
   - Compare to cpm.rai_intent_v9. Do they map to the same patient-level roll?
   - iodine_avidity_flag in detail vs cpm.rai_avid_flag: concordance.

List top 5 RAI detail-vs-master integrity concerns.
```

---

## PROMPT 16 — Thyroglobulin Lab Detail ↔ Master Rollup

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
All queries MUST use the fully qualified three-part name: thyroid_canonical_publication_v1_0.main.<table>.
NEVER reference any other database. READ-ONLY dry run.

CONTEXT: `thyroglobulin_lab_canonical_v1` (~76,971 rows, 3,258 patients) — one row per Tg/TgAb lab result.
Also `longitudinal_lab_canonical_v1` (~77,960 rows, 3,690 patients) — broader lab results (TSH, PTH, Ca, VitD, etc.).
Master rollup: `canonical_patient_master` with tg_n_measurements, tg_nadir, tg_peak, tg_mean,
tg_rising_flag, lab_tsh_n_measurements, lab_pth_n_measurements, lab_calcium_n_measurements, etc.

TASK: Cross-validate lab detail against master.

1. Tg COUNT CONCORDANCE:
   - From thyroglobulin_lab_canonical_v1: count rows per patient WHERE analyte ILIKE '%thyroglobulin%'
     (exclude TgAb rows).
   - Compare to cpm.tg_n_measurements. Report concordance.
   - Flag: tg_n_measurements > 0 in master but 0 Tg rows in detail? And vice versa.

2. Tg VALUE CONCORDANCE:
   - From detail: MIN(result_numeric) per patient WHERE analyte is Tg → compare to cpm.tg_nadir.
   - From detail: MAX(result_numeric) per patient → compare to cpm.tg_peak.
   - Report mean absolute difference. Flag patients where |nadir_delta| > 1.0 or |peak_delta| > 10.0.

3. TSH/PTH/CALCIUM FROM longitudinal_lab_canonical_v1:
   - For each analyte_group (TSH, PTH, Calcium):
     - Count rows per patient in detail.
     - Compare to cpm.lab_tsh_n_measurements, lab_pth_n_measurements, lab_calcium_n_measurements.
     - Report concordance rate for each.
   - Flag: cpm says 0 measurements but detail has rows (or vice versa).

4. TEMPORAL SPAN:
   - From thyroglobulin_lab_canonical_v1: MIN(days_from_surgery), MAX(days_from_surgery) per patient.
   - Compare to cpm.first_tg_days_from_surg, last_tg_days_from_surg.
   - Flag: any patient where first Tg is > 365 days before surgery (days_from_surgery < -365)?

5. TgAb INTERFERENCE:
   - From detail: count rows per patient WHERE analyte ILIKE '%anti%' OR analyte ILIKE '%tgab%'.
   - Any patient with elevated TgAb (result_numeric > 4.0) but cpm.tgab_interference_flag = false?
   - Report discordance count.

6. ORPHAN CHECK:
   - Any research_id in thyroglobulin_lab_canonical_v1 NOT in canonical_patient_master?
   - Any research_id in longitudinal_lab_canonical_v1 NOT in canonical_patient_master?

List top 5 lab detail-vs-master integrity concerns.
```

---

## PROMPT 17 — Tumor & Synoptic Detail ↔ Master Rollup

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
All queries MUST use the fully qualified three-part name: thyroid_canonical_publication_v1_0.main.<table>.
NEVER reference any other database. READ-ONLY dry run.

CONTEXT: Three detail tables:
- `tumor_episode_master_v2` (~11,691 rows, 10,871 patients) — one row per tumor per surgery
- `synoptic_tumor_long_v1` (~11,103 rows, 8,422 patients) — one row per tumor from synoptic path
- `path_synoptics` (~11,688 rows, 10,871 patients) — full synoptic path report per patient
Master rollup: `canonical_patient_master` with n_tumors, multifocal_flag, tumor_size_cm,
histology_final, ete_grade, margin_status_final, syn_* columns, etc.

TASK: Cross-validate tumor/synoptic detail against master.

1. TUMOR COUNT:
   - From tumor_episode_master_v2: COUNT per patient → compare to cpm.n_tumors.
   - Report concordance. Flag patients where detail shows multifocality (count > 1)
     but cpm.multifocal_flag = false.

2. TUMOR SIZE:
   - From tumor_episode_master_v2: MAX(tumor_size_cm) per patient → compare to cpm.tumor_size_cm.
   - Report mean absolute difference. Flag |delta| > 1.0 cm.
   - From synoptic_tumor_long_v1: MAX(size_greatest_dimension_cm) → compare to same.

3. HISTOLOGY:
   - From tumor_episode_master_v2: primary_histology for tumor_ordinal=1 (or confidence_rank=1).
   - Compare to cpm.histology_final. Report concordance rate.
   - List top 10 discordant patterns (detail histology vs master histology, count).

4. STAGING:
   - From tumor_episode_master_v2: t_stage, n_stage, m_stage for the primary tumor.
   - Compare to cpm.ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage.
   - Report concordance for each. Flag discordant staging.

5. SYNOPTIC INVASION MARKERS:
   - From synoptic_tumor_long_v1: extrathyroidal_extension, angioinvasion, lymphatic_invasion,
     perineural_invasion, margin_status for the primary tumor (tumor_index=1).
   - Compare to cpm.ete_grade, vasc_grade_final_v13, lvi_grade_final_v13,
     pni_refined_v6, margin_status_final.
   - Report concordance for each marker.

6. SYNOPTIC PARATHYROID:
   - From path_synoptics: parathyroid_gland_or_tissue_included_in_resected_specimen (boolean).
   - Compare to cpm.para_specimen_included. Report concordance.
   - From path_synoptics: count patients with any parag_1_location non-NULL.
   - Compare to cpm.para_n_glands_identified > 0. Concordance?

7. ORPHAN CHECK:
   - Any research_id in tumor_episode_master_v2 NOT in canonical_patient_master?
   - Any research_id in synoptic_tumor_long_v1 NOT in canonical_patient_master?

List top 5 tumor/synoptic detail-vs-master integrity concerns.
```

---

## PROMPT 18 — Surgery & Complications Detail ↔ Master Rollup

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
All queries MUST use the fully qualified three-part name: thyroid_canonical_publication_v1_0.main.<table>.
NEVER reference any other database. READ-ONLY dry run.

CONTEXT: Two detail tables:
- `operative_episode_detail_v2` (~9,371 rows, 9,368 patients) — one row per surgery episode
- `complication_phenotype_v1` (~5,928 rows, 2,892 patients) — one row per complication type per patient
Master rollup: `canonical_patient_master` with surg_procedure_type, n_surgeries, surg_total_thyroidectomy,
comp_*_confirmed, comp_*_transient, comp_*_permanent, etc.

TASK: Cross-validate surgery and complication detail against master.

1. SURGERY COUNT:
   - From operative_episode_detail_v2: COUNT per patient → compare to cpm.surg_n_procedures (or n_surgeries).
   - Report concordance. Flag patients with detail count != master count.

2. PROCEDURE TYPE:
   - From operative_episode_detail_v2: procedure_normalized for the earliest surgery.
   - Compare to cpm.surg_procedure_type. Report concordance rate.
   - Flag: detail says 'total thyroidectomy' but cpm.surg_total_thyroidectomy=false?

3. OPERATIVE DETAIL FLAGS:
   - From detail: lateral_neck_dissection_flag=true for any episode.
   - Compare to cpm.lateral_neck_dissected. Concordance?
   - From detail: reoperative_field_flag=true. Compare to cpm.op_reoperative_any. Concordance?
   - From detail: parathyroid_autograft_flag=true. Compare to cpm.op_parathyroid_autograft_any.

4. COMPLICATION CONCORDANCE:
   For each complication entity in complication_phenotype_v1:
   - Derive: confirmed_flag=true → should match cpm.comp_<entity>_confirmed=true.
   - Run this for: hypocalcemia, hypoparathyroidism, rln_injury, vc_paralysis,
     vc_paresis, hematoma, seroma, wound_infection, chyle_leak.
   - Report concordance for each. List entities with >5% discordance.

5. COMPLICATION TIMING:
   - From complication_phenotype_v1: timing_days_post_surgery for confirmed complications.
   - Compare to cpm.comp_<entity>_days_postop for each entity.
   - Report: how many match exactly, how many within 7 days, how many disagree by >30 days.

6. TRANSIENT/PERMANENT:
   - From detail: transient_flag=true → cpm.comp_<entity>_transient=true.
   - From detail: permanent_flag=true → cpm.comp_<entity>_permanent=true.
   - Flag: any patient where detail says permanent but master says transient (or vice versa).

7. ORPHAN CHECK:
   - Any research_id in operative_episode_detail_v2 NOT in canonical_patient_master?
   - Any research_id in complication_phenotype_v1 NOT in canonical_patient_master?

List top 5 surgery/complication detail-vs-master integrity concerns.
```

---

## PROMPT 19 — Imaging & Nodule Detail ↔ Master Rollup

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
All queries MUST use the fully qualified three-part name: thyroid_canonical_publication_v1_0.main.<table>.
NEVER reference any other database. READ-ONLY dry run.

CONTEXT: Three detail tables:
- `imaging_nodule_long_v2` (~19,891 rows, 3,439 patients) — one row per nodule per imaging exam
- `us_nodules_tirads` (~10,862 rows, 10,862 patients) — one row per patient, wide format (up to 14 nodules)
- `ultrasound_reports` (~6,793 rows, 4,074 patients) — one row per US report
Master rollup: `canonical_patient_master` with n_us_exams, imaging_n_nodule_records,
tirads_best_category_v12, tirads_best_score_v12, imaging_nodule_size_cm, dominant_nodule_size_cm, etc.

TASK: Cross-validate imaging/nodule detail against master.

1. US EXAM COUNT:
   - Count ultrasound_reports rows per patient → compare to cpm.n_us_exams (or n_us_reports).
   - Report concordance rate. Flag patients with |delta| > 3.

2. NODULE COUNT:
   - From imaging_nodule_long_v2: COUNT per patient → compare to cpm.imaging_n_nodule_records.
   - Report concordance. Flag large discrepancies.

3. TIRADS CONCORDANCE:
   - From us_nodules_tirads: for each patient, find the max TIRADS across n1_tr through n14_tr.
   - Compare to cpm.tirads_best_score_v12 or tirads_worst_combined.
   - From imaging_nodule_long_v2: MAX(tirads_score) per patient → compare to same master columns.
   - Report concordance for each source.

4. NODULE SIZE:
   - From imaging_nodule_long_v2: MAX(size_cm_max) per patient → compare to cpm.imaging_nodule_size_cm.
   - Report mean absolute difference. Flag |delta| > 2.0 cm.
   - From imaging_nodule_long_v2 WHERE dominant_nodule_flag=true: size_cm_max → compare to
     cpm.dominant_nodule_size_cm.

5. TIRADS COMPONENT DETAIL:
   - From imaging_nodule_long_v2: distribution of composition, echogenicity, shape, margins, calcifications
     among nodules with tirads_score non-NULL.
   - Flag: any nodule with tirads_score > 0 but ALL component columns NULL? (score without basis)
   - Flag: any nodule with tirads_category = 'TR5' but tirads_score < 7? (TR5 requires >=7 points)

6. LATERALITY:
   - From imaging_nodule_long_v2: laterality distribution (left, right, isthmus, bilateral, NULL).
   - Cross-check against cpm.laterality for patients where both are non-NULL.

7. ORPHAN CHECK:
   - Any research_id in imaging_nodule_long_v2 NOT in canonical_patient_master?
   - Any research_id in us_nodules_tirads NOT in canonical_patient_master?

List top 5 imaging detail-vs-master integrity concerns.
```

---

## PROMPT 20 — Recurrence & LN Detail ↔ Master Rollup

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
All queries MUST use the fully qualified three-part name: thyroid_canonical_publication_v1_0.main.<table>.
NEVER reference any other database. READ-ONLY dry run.

CONTEXT: Two detail tables:
- `recurrence_event_clean_v1` (~1,946 rows, 1,946 patients) — one row per recurrence event
- `ln_master_rollup_v1` (~4,290 rows, 3,986 patients) — LN detail per patient
Also: `clinical_note_ln_extracted_v1` (~7,751 rows, 3,588 patients) — NLP-extracted LN mentions.
Master rollup: `canonical_patient_master` with any_recurrence_flag, recurrence_type,
time_to_recurrence_days, ln_total_examined, ln_total_positive, ln_positive_flag, etc.

TASK: Cross-validate recurrence and LN detail against master.

1. RECURRENCE FLAG:
   - From recurrence_event_clean_v1: set of distinct research_ids.
   - Compare to cpm.any_recurrence_flag=true. Report:
     - Patients in detail but any_recurrence_flag=false in master? (detail says yes, master says no)
     - Patients with any_recurrence_flag=true in master but NOT in detail? (master says yes, no detail row)

2. RECURRENCE TYPE:
   - From detail: recurrence_type for event_rank=1 (first event).
   - Compare to cpm.recurrence_type. Report concordance.
   - From detail: structural_recurrence_flag → compare to cpm.structural_recurrence_flag.
   - From detail: biochemical_recurrence_flag → compare to cpm.biochemical_recurrence_flag.

3. RECURRENCE TIMING:
   - From detail: recurrence_date for event_rank=1.
   - Derive days from first_surgery_date → compare to cpm.time_to_recurrence_days.
   - Report: n matches (within 30 days), n mismatches. Flag >90 day discrepancies.

4. LN COUNT CONCORDANCE:
   - From ln_master_rollup_v1: ln_total_examined, ln_total_positive per patient.
   - Compare to cpm.ln_total_examined, cpm.ln_total_positive.
   - Report concordance for each. Flag patients where |delta| > 5 for either.

5. LN FLAG CONCORDANCE:
   - From ln_master_rollup_v1: ln_any_positive → compare to cpm.ln_positive_flag.
   - From ln_master_rollup_v1: ln_extranodal_extension → compare to cpm.ene_positive.
   - Report concordance for each.

6. LN LEVEL DATA:
   - From ln_master_rollup_v1: ln_level_vi_examined, ln_level_vi_positive (central compartment).
   - Compare to cpm.ln_level_vi_examined, cpm.ln_level_vi_positive.
   - Repeat for levels II-V (lateral).
   - Flag: any level where detail positive > detail examined? (impossible)

7. NLP LN CROSS-CHECK:
   - From clinical_note_ln_extracted_v1: COUNT per patient → general density check.
   - Among patients with >5 NLP LN mentions: does ln_positive_flag align with
     nlp_ln_positive_mentioned from master? Report concordance.

8. ORPHAN CHECK:
   - Any research_id in recurrence_event_clean_v1 NOT in canonical_patient_master?
   - Any research_id in ln_master_rollup_v1 NOT in canonical_patient_master?

List top 5 recurrence/LN detail-vs-master integrity concerns.
```

---

## PROMPT 21 — Manuscript Workspace View Integrity

```
You have access to a MotherDuck database: thyroid_canonical_publication_v1_0.
All queries MUST use the fully qualified three-part name.
NEVER reference any other database. READ-ONLY dry run.

CONTEXT: The `manuscript_workspace` schema has 64 views built on top of
`canonical_patient_master` in the `main` schema. One consolidated full-cohort view
(`cohort_descriptive_full_cohort_v1`, N=10,871) and 63 per-manuscript views
(24 thin wrappers on the full-cohort view + 39 dedicated views with WHERE clauses).
A lookup table `manuscript_dive_map_v1` maps 63 manuscripts to their views and Dives.

TASK: Validate that all workspace views are healthy and consistent with main schema.

1. VIEW ROW COUNTS:
   - For EVERY view listed in manuscript_dive_map_v1.cohort_view_name:
     run SELECT COUNT(*) and report the result.
   - Flag: any view returning 0 rows?
   - Flag: any dedicated (non-full-cohort) view returning exactly 10,871 rows?
     (that means the WHERE clause isn't filtering)

2. FULL-COHORT VIEW PARITY:
   - cohort_descriptive_full_cohort_v1: verify COUNT = 10,871 (must match canonical_patient_master).
   - Pick 5 random columns from the full-cohort view. For each, verify:
     COUNT(DISTINCT <col>) matches the same column from canonical_patient_master.

3. THIN WRAPPER INTEGRITY:
   - For 5 sample thin wrappers (e.g., cohort_m048, cohort_m050, cohort_m058, cohort_m060, cohort_m065):
     - Verify row count = 10,871 (they select from full-cohort view with no WHERE).
     - Verify the columns they expose are a strict subset of cohort_descriptive_full_cohort_v1 columns.
     - Flag: any column returning ALL NULLs?

4. DEDICATED VIEW FILTER VALIDATION:
   For 5 sample dedicated views:
   a) cohort_m001_indeterminate_genetics_v1 — should have bethesda_final IN (3, 4) for all rows.
      Run: SELECT COUNT(*) WHERE bethesda_final NOT IN (3, 4). Must be 0.
   b) cohort_m019_rai_outcomes_v1 — should have rai_received_flag = true for all rows.
      Run: SELECT COUNT(*) WHERE rai_received_flag != true. Must be 0.
   c) cohort_m067_tsh_tg_tumorigenesis_v1 — should have tg_n_measurements > 0 for all rows.
      Run: SELECT COUNT(*) WHERE tg_n_measurements <= 0 OR tg_n_measurements IS NULL. Must be 0.
   d) cohort_m016_graves_carcinoma_v1 — should have syn_graves = true for all rows.
      Run: SELECT COUNT(*) WHERE syn_graves != true. Must be 0.
   e) cohort_m082_parathyroid_tumors_v1 — should have para_specimen_included OR para_has_pathologic_glands.
      Run: SELECT COUNT(*) WHERE NOT (para_specimen_included OR para_has_pathologic_glands). Must be 0.

5. MAP TABLE INTEGRITY:
   - manuscript_dive_map_v1: 63 rows expected. Confirm.
   - Every cohort_view_name in the map must exist in information_schema.tables.
     Check for typos or missing views.
   - Every dive_id should be a valid UUID. Flag any NULL or malformed.

6. CROSS-SCHEMA ISOLATION:
   - Verify NO view in manuscript_workspace references any table outside of
     thyroid_canonical_publication_v1_0. (Check view definitions if accessible via
     SHOW CREATE VIEW or information_schema.)

List top 5 workspace view integrity concerns.
```

---

## Notes for All Prompts

**Part 1 (Prompts 1-12):** Exercise `canonical_patient_master` (1,377 columns) in isolation.
Covers: demographics, surgery, pathology, staging, LN, ETE, molecular, RAI, Tg kinetics,
survival, recurrence, complications, voice, TIRADS, FNA, Bethesda, NLP extraction,
imaging (US/CT/MRI/PET/NucMed), parathyroid, calcium/PTH, comorbidities, synoptic
pathology, gland weights, and NSQIP cross-validation.

**Part 2 (Prompts 13-21):** Cross-validate the multi-row detail tables against the
patient-level rollup. This is where rollup bugs hide — wrong counts, stale max/min values,
orphan records, broken linkages. Covers: FNA episodes, molecular tests/variants, RAI episodes,
Tg/lab longitudinal data, tumor/synoptic pathology, surgery episodes, complications,
imaging/nodule detail, recurrence events, LN detail, and manuscript_workspace views.

**Together:** 21 prompts exercising the full master canonical table (1,377 columns),
15+ multi-row detail tables (~300K+ rows), and 64 manuscript_workspace views.

If a prompt surfaces a real data quality issue, create a follow-up prompt to investigate
the specific patients involved (by research_id) and trace back to source columns.
