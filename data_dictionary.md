# Thyroid Master DuckDB Data Dictionary

This document describes the production thyroid cancer research lakehouse in:

- `thyroid_master.duckdb`

Primary grain:

- Patient key: `research_id`
- Master cohort size: **11,673** distinct patients

## Database Layout

### Base Tables (13)

1. `thyroid_sizes`
2. `tumor_pathology`
3. `benign_pathology`
4. `thyroid_weights`
5. `fna_cytology`
6. `frozen_sections`
7. `ultrasound_reports`
8. `ct_imaging`
9. `mri_imaging`
10. `nuclear_med`
11. `thyroglobulin_labs`
12. `anti_thyroglobulin_labs`
13. `parathyroid`

### Existing Views (Phase 1)

1. `master_cohort`
2. `lab_timeline`
3. `imaging_timeline`
4. `data_completeness`

### New Research Views (Phase 2)

1. `ptc_cohort`
2. `recurrence_risk_cohort`
3. `imaging_pathology_correlation`
4. `fna_accuracy_view`
5. `lymph_node_metastasis_view`
6. `benign_vs_malignant_comparison`
7. `longitudinal_lab_view`
8. `data_completeness_by_year`

---

## Core Entity and Join Rules

- **Canonical join key:** `research_id` (string-like ID normalized at ingestion)
- **Recommended cohort anchor:** `master_cohort`
- **Standard join pattern:**
  - `master_cohort mc`
  - `LEFT JOIN tumor_pathology tp ON mc.research_id = tp.research_id`
  - `LEFT JOIN benign_pathology bp ON mc.research_id = bp.research_id`
  - `LEFT JOIN fna_cytology f ON mc.research_id = f.research_id`
  - `LEFT JOIN longitudinal_lab_view llv ON mc.research_id = llv.research_id`

---

## Table-Level Dictionary

## `master_cohort` (view, patient-level)

Purpose:

- One row per patient.
- Demographics and per-domain data availability flags.

Key columns:

- `research_id`: canonical patient identifier
- `age_at_surgery`, `sex`, `surgery_date`: harmonized demographics (primarily from pathology sources)
- `has_*` flags:
  - `has_thyroid_sizes`
  - `has_tumor_pathology`
  - `has_benign_pathology`
  - `has_thyroid_weights`
  - `has_fna_cytology`
  - `has_frozen_sections`
  - `has_ultrasound_reports`
  - `has_ct_imaging`
  - `has_mri_imaging`
  - `has_nuclear_med`
  - `has_thyroglobulin_labs`
  - `has_anti_thyroglobulin_labs`
  - `has_parathyroid`

## `tumor_pathology` (table, malignant pathology)

Purpose:

- Gold-standard malignant disease characterization.

Important variables:

- Histology and staging:
  - `histology_1_type`
  - `histology_1_t_stage_ajcc8`
  - `histology_1_n_stage_ajcc8`
  - `histology_1_m_stage_ajcc8`
  - `histology_1_overall_stage_ajcc8`
- Tumor burden:
  - `histology_1_largest_tumor_cm`
  - `num_tumors_identified`
- Lymph node burden:
  - `histology_1_ln_examined`
  - `histology_1_ln_positive`
  - `histology_1_ln_ratio`
  - level-specific columns (`ln_level_i_*` through `ln_level_vii_*`)
- Invasion and ETE:
  - `tumor_1_extrathyroidal_ext`
  - `tumor_1_gross_ete`
  - `tumor_1_ete_microscopic_only`
  - vascular/lymphatic/perineural invasion columns

## `benign_pathology` (table, benign pathology)

Purpose:

- Benign disease phenotypes and inflammatory/autoimmune findings.

Important variables:

- `multinodular_goiter`
- `diffuse_hyperplasia`
- `colloid_nodule`
- `follicular_adenoma`
- `hurthle_adenoma`
- `hashimoto_thyroiditis`
- `graves_disease`
- `focal_lymphocytic_thyroiditis`

## `fna_cytology` (table, cytology)

Purpose:

- Fine needle aspiration and Bethesda classification over time.

Important variables:

- `fna_index`, `fna_date`
- `specimen_location`
- `bethesda_2010_num`, `bethesda_2010_name`
- `bethesda_2015_num`, `bethesda_2015_name`
- `bethesda_2023_num`, `bethesda_2023_name`
- `confidence`, `reasoning`, `provider`

## `thyroglobulin_labs` / `anti_thyroglobulin_labs` (tables, long-format labs)

Purpose:

- Long-format serum marker trajectories.

Important variables:

- `research_id`
- `lab_index`
- `test_name`
- `specimen_collect_dt`
- `result`
- `units`

## `lab_timeline` (view)

Purpose:

- Unified stack of thyroglobulin + anti-thyroglobulin lab measurements.

Columns:

- `research_id`, `lab_type`, `lab_index`, `test_name`, `specimen_collect_dt`, `result`, `units`

## `ultrasound_reports` (table)

Purpose:

- Detailed ultrasound extraction (multi-nodule, TI-RADS features, gland metrics).

Important variables:

- `ultrasound_date`
- `number_of_nodules`
- `right_lobe_volume_ml`, `left_lobe_volume_ml`, `total_thyroid_volume_ml`
- `nodule_1_*` ... `nodule_n_*` feature families, including:
  - dimensions
  - location
  - `ti_rads`
  - composition
  - echogenicity
  - calcifications
  - margins
  - shape
- `lymph_node_assessment`

## `ct_imaging` (table)

Purpose:

- CT-derived thyroid and nodal findings.

Important variables:

- `date_of_exam`, `exam_type_normalized`, `contrast`
- `thyroid_nodule`, `thyroid_enlarged`, `thyroid_postsurgical`, `goiter_present`
- `pathologic_lymph_nodes`, `lymph_nodes_suspicious`
- `largest_lymph_node_short_axis_mm`
- `lymph_node_locations`

## `mri_imaging` (table)

Purpose:

- MRI-derived thyroid and nodal findings.

Important variables:

- `date_of_exam`, `exam_type_detail`, `contrast`
- `thyroid_nodule`, `thyroid_enlarged`, `substernal_extension`
- `pathologic_lymph_nodes`, `lymph_node_locations`
- nodule location and size fields (`nodule1_*` ... `nodule5_*`)

## `nuclear_med` (table)

Purpose:

- Long-format nuclear medicine studies after wide-to-long melt.

Important variables:

- `research_id`
- `scan_index`
- scan metadata and findings columns (e.g., radiotracer, uptake, impression)

## `thyroid_sizes` (table)

Purpose:

- Structured specimen dimensions/volumes from pathology summaries.

Important variables:

- lobe-level formatted dimensions and volume metrics
- total volume fields

## `thyroid_weights` (table)

Purpose:

- Surgical specimen weights and diagnosis context.

Important variables:

- `date_of_surgery`
- lobe/isthmus/total weights
- `specimen_weight_combined`
- diagnosis text fields

## `frozen_sections` (table)

Purpose:

- Intraoperative frozen section details and concordance with final pathology.

Important variables:

- `frozen_section_obtained`
- `number_of_frozen_sections`
- `fs_result_1...fs_result_3`
- `concordance_with_final`

## `parathyroid` (table)

Purpose:

- Parathyroid tissue involvement and intent annotation.

Important variables:

- `removal_intent`
- `parathyroid_abnormality`
- incidental vs intentional removal fields
- gland-level details (`g1_*`, `g2_*`, etc.)

---

## Phase 2 Research Views (Detailed)

## `ptc_cohort`

Purpose:
- Classic papillary thyroid carcinoma cohort extraction.

Key logic:

- Filters to `histology_1_type = 'PTC'`
- Keeps classic variant or unspecified-variant PTC rows.

Output highlights:

- AJCC stage fields
- largest tumor size
- LN burden
- ETE fields

## `recurrence_risk_cohort`

Purpose:

- Patient-level recurrence risk feature set combining:
  - pathology stage
  - ETE
  - thyroglobulin trend summary

Output highlights:

- `tg_first_value`, `tg_last_value`, `tg_max`, `tg_mean`
- `tg_delta_per_measurement`
- `recurrence_risk_band` (low/intermediate/high)

## `imaging_pathology_correlation`

Purpose:

- Correlates imaging burden/signals with final pathology.

Output highlights:

- modality counts (`us_count`, `ct_count`, `mri_count`)
- max TI-RADS summary
- CT/MRI nodule and pathologic LN flags
- final histology and tumor size

## `fna_accuracy_view`

Purpose:

- Operational diagnostic performance view linking FNA Bethesda to final pathology.

Key logic:

- Test-positive: Bethesda 2023 >= 5
- Gold standard:
  - malignant if tumor pathology exists
  - benign if benign pathology exists and no tumor pathology

Output highlights:

- confusion class per FNA (`TP`, `FP`, `FN`, `TN`)

## `lymph_node_metastasis_view`

Purpose:

- LN metastasis burden and level-wise involvement table.

Output highlights:

- total LN examined/positive
- level-wise examined/positive (I–VII)
- LN ratio
- extranodal extension

## `benign_vs_malignant_comparison`

Purpose:

- Harmonized cohort for comparative analyses.

Output highlights:

- `disease_group` (`benign` vs `malignant`)
- demographics and surgery date
- malignancy markers (histology, size, stage)
- benign phenotypes (Hashimoto, Graves, goiter, adenoma)
- modality/lab availability flags

## `longitudinal_lab_view`

Purpose:

- Time-indexed thyroglobulin and anti-thyroglobulin series.

Output highlights:

- parsed `numeric_result`
- `days_from_first_lab` normalization per patient and lab type

## `data_completeness_by_year`

Purpose:

- Grant-ready year-by-year cohort completeness metrics.

Output highlights:

- patient counts per surgery year
- domain-level counts and percentages (pathology, FNA, imaging, labs)

---

## Data Quality Notes

- Many source fields are free text from extraction pipelines.
- Boolean columns may be represented as string-like values in source.
- Numeric lab values are parsed from mixed strings (e.g., `<0.4`, `3.1 ng/mL`) using regex.
- Date fields are cast with `TRY_CAST`; nulls are expected for incomplete records.

---

## Recommended Starter Queries

1. Cohort size by stage:

```sql
SELECT overall_stage_ajcc8, COUNT(*) AS n
FROM ptc_cohort
GROUP BY overall_stage_ajcc8
ORDER BY n DESC;
```

2. FNA confusion summary:

```sql
SELECT confusion_class, COUNT(*) AS n
FROM fna_accuracy_view
GROUP BY confusion_class
ORDER BY n DESC;
```

3. Annual data completeness for grant tables:

```sql
SELECT *
FROM data_completeness_by_year
ORDER BY surgery_year;
```

---

## Phase 6: Integrated Source Tables (8 New Excel Sources)

### `complications` (table)

Source: `Thyroid all_Complications 12_1_25.xlsx`

Surgical complications with NLP-parsed laryngoscopy notes. Key columns:
`rln_injury_vocal_cord_paralysis`, `seroma`, `hematoma`, `hypocalcemia`,
`hypoparathyroidism`, `vocal_cord_status` (normal/paresis/paralysis),
`affected_side`, `laryngoscopy_date`, `_raw_laryngoscopy_note`.

### `molecular_testing` (table, long format)

Source: `THYROSEQ_AFIRMA_12_5.xlsx`

One row per molecular test per patient (up to 3 tests). Key columns:
`test_index`, `thyroseq_afirma`, `date`, `result`, `mutation`, `detailed_findings`.

### `operative_details` (table)

Source: `Thyroid OP Sheet data.xlsx`

Operative sheet data — BMI, EBL, skin-to-skin time, nerve monitoring,
parathyroid autograft notes, IO tumor appearance.

### `fna_history` (table, long format)

Source: `FNAs 12_5_2025.xlsx`

One row per FNA per patient (up to 12 FNAs). Key columns:
`fna_index`, `date`, `bethesda`, `path`, `path_extended`, `specimen_received`.

### `us_nodules_tirads` (table, long format)

Source: `US Nodules TIRADS 12_1_25.xlsx`

One row per US exam per patient (up to 14 exams). Includes per-nodule
TIRADS scores and nodule descriptions within each exam.

### `serial_imaging_us` (table, long format)

Source: `Imaging_12_1_25.xlsx`

Serial imaging reports across 8 modalities (thyroid_us, ln_us, us_fna,
ct_petct, nuclear_med, mri, cxr, other). Raw report text and impressions.

### `path_synoptics` (table, wide — 275+ cols)

Source: `All Diagnoses & synoptic 12_1_2025.xlsx`

Full AJCC staging, margins, variants, LN details for up to 5 tumors.
Includes synoptic diagnosis text, path diagnosis summary, and benign findings.
Note: contains duplicate research_ids for re-operations.

### `clinical_notes` (table)

Source: `Notes 12_1_25.xlsx`

Combined demographics/summary (Sheet1) + clinical notes (Sheet2).
H&P notes 1-4, OP notes 1-4, discharge summaries 1-4, last endocrine/FM note,
ED notes 1-2. Notes may be truncated at 32,767 characters (Excel limit).

### `clinical_notes_long` (table)

Source: `Notes 12_1_25.xlsx` (Sheet2 + Sheet1 summary folded into long format)

Purpose:

- Store *all* available clinical note text verbatim in a long format for NLP/extraction.

Key columns:

- `research_id`
- `note_type` (HP, OPNOTE, DC_SUM, ED_NOTE, OTHER_HISTORY, OTHER_NOTES, ENDOCRINE_FM, THYROID_CX_HISTORY, DEATH)
- `note_index` (1-4 when applicable)
- `note_text`
- `source_sheet`, `source_column`

### `extracted_clinical_events` (table, long format)

NLP-extracted events from clinical notes. Event types:
- **lab**: TSH, thyroglobulin, anti-Tg, calcium, PTH, vitamin D (with values and units)
- **medication**: levothyroxine (with dose), calcium supplements, calcitriol
- **comorbidity**: hypertension, diabetes, breast/lung cancer, obesity, CAD, etc.
- **treatment**: RAI, EBRT, recurrence, reoperation (with dates when available)
- **follow_up**: follow-up visit dates

### `advanced_features_v2` (view)

Comprehensive analytic view joining `master_cohort` with all Phase 6 tables
plus existing tumor_pathology and benign_pathology. Includes data availability
flags for every domain.

---

## Cross-File Validation Tables (Script 11.5)

Created by `scripts/11.5_cross_file_validation.py`. These tables validate
consistency across multiple source files and flag discrepancies.

### `qa_laterality_mismatches` (table)

Cross-checks operative laterality (`operative_details.side_of_largest_tumor_or_goiter`)
against pathology procedure laterality (inferred from `path_synoptics.thyroid_procedure`).
Joined via `master_timeline` for surgery number.

Key columns:

- `research_id`: patient identifier (INT)
- `operative_side`: side from operative sheet (lowercase)
- `path_procedure`: full procedure name from synoptic report
- `path_side`: inferred laterality (right / left / bilateral / isthmus / NULL)
- `surgery_date`: date of surgery (DATE)
- `surgery_number`: from master_timeline
- `laterality_flag`: MATCH, LATERALITY_MISMATCH, or INCOMPLETE

### `qa_report_matching` (table)

Aggregate match rates for two cross-file linkage checks:

1. **fna_path**: FNA bethesda result ↔ pathology diagnosis (365-day window)
2. **us_operative**: US nodule size ↔ operative sheet size (180-day window)

Key columns:

- `total_pairs`: number of patient-level joins within date window
- `matched`: pairs where both fields are non-NULL
- `match_pct`: percentage of matched pairs
- `check_type`: 'fna_path' or 'us_operative'

### `qa_missing_demographics` (table)

Patients with missing demographic fields. Age and sex sourced from
`patient_level_summary_mv`; race sourced from `path_synoptics` via LEFT JOIN.

Key columns:

- `research_id`: patient identifier (INT)
- `age_at_surgery`: age (NULL if missing)
- `sex`: sex (NULL if missing)
- `race`: race from path_synoptics (NULL if missing or no synoptic record)
- `age_flag`: 'MISSING_AGE' or 'OK'
- `sex_flag`: 'MISSING_SEX' or 'OK'
- `race_flag`: 'MISSING_RACE' or 'OK'
- `source_priority`: data source used for demographics lookup
