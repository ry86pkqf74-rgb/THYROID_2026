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

---

## Phase 7: Clinical Notes Long + Entity Extraction

### `clinical_notes_long` (table)

Source: `raw/Notes 12_1_25.xlsx`, unpivoted via `config/notes_column_map.csv`

One row per note per patient (long format). 11,037 rows from 5,641 patients.

Key columns:

- `note_row_id` (VARCHAR): SHA-1 hash primary key
- `research_id` (INT): patient identifier
- `note_type` (VARCHAR): h_p, op_note, dc_sum, ed_note, endocrine_note, history_summary, other_history, other_notes
- `note_index` (INT): sequence within type (1-4)
- `note_date` (VARCHAR): encounter/service date extracted from note header (YYYY-MM-DD)
- `note_text` (VARCHAR): full note text
- `source_sheet` (VARCHAR): Excel sheet name
- `source_column` (VARCHAR): snake_case column name
- `char_count` (INT): length of note_text

### `note_entities_staging` (table)

AJCC T/N/M and overall stage mentions extracted via regex. 3,807 rows.

### `note_entities_genetics` (table)

Gene/mutation mentions (BRAF, RAS, RET, TERT, NTRK, ALK). 1,738 rows.

### `note_entities_procedures` (table)

Surgical procedure mentions (thyroidectomy variants, neck dissection, etc.). 21,942 rows.

### `note_entities_complications` (table)

Post-operative complication mentions (RLN injury, hypocalcemia, etc.). 9,359 rows.

### `note_entities_medications` (table)

Medication mentions with optional dose (levothyroxine, calcium, etc.). 7,501 rows.

### `note_entities_problem_list` (table)

Comorbidity/diagnosis mentions (hypertension, diabetes, etc.). 11,579 rows.

All six entity tables share a common schema:

- `research_id` (INT): patient identifier
- `note_row_id` (VARCHAR): FK to clinical_notes_long
- `note_type` (VARCHAR): source note category
- `entity_type` (VARCHAR): domain-specific type
- `entity_value_raw` (VARCHAR): raw matched string
- `entity_value_norm` (VARCHAR): normalised value from controlled vocabulary
- `present_or_negated` (VARCHAR): present or negated
- `confidence` (FLOAT): 0.0-1.0
- `evidence_span` (VARCHAR): exact substring from note_text
- `evidence_start` (INT): character offset start
- `evidence_end` (INT): character offset end
- `entity_date` (VARCHAR): date found near entity in note text (YYYY-MM-DD)
- `note_date` (VARCHAR): encounter/service date from note header (YYYY-MM-DD)
- `extraction_method` (VARCHAR): regex or llm_model
- `extracted_at` (VARCHAR): ISO-8601 timestamp

### `notes_entity_summary` (view)

Aggregated entity counts per patient across all domains.

See `docs/notes_extraction_spec.md` for controlled vocabularies and extraction details.

---

## Phase 7: v2 Canonical Episode Tables (scripts 22-26)

### `tumor_episode_master_v2` (table)

One row per tumor per surgery per patient. Reconciles path_synoptics, tumor_pathology, and note-derived staging with confidence-ranked precedence.

- `research_id` (INT): patient identifier
- `surgery_episode_id` (INT): sequential surgery number per patient
- `tumor_ordinal` (INT): tumor index within surgery
- `surgery_date` (DATE): surgery date
- `date_status` (VARCHAR): exact_source_date or unresolved_date
- `date_confidence` (INT): 0-100
- `primary_histology` (VARCHAR): best-available histology (synoptic > tumor_path)
- `histology_variant` (VARCHAR): subtype/variant
- `histology_source` (VARCHAR): provenance of histology value
- `t_stage`, `n_stage`, `m_stage`, `overall_stage` (VARCHAR): AJCC staging
- `tumor_size_cm` (DOUBLE): largest dimension in cm
- `extrathyroidal_extension`, `gross_ete` (VARCHAR): ETE findings
- `vascular_invasion`, `lymphatic_invasion`, `perineural_invasion`, `capsular_invasion` (VARCHAR)
- `margin_status` (VARCHAR)
- `nodal_disease_positive_count`, `nodal_disease_total_count` (INT)
- `extranodal_extension` (VARCHAR)
- `laterality` (VARCHAR): right/left/bilateral/isthmus
- `number_of_tumors` (INT), `multifocality_flag` (BOOL)
- `consult_diagnosis` (VARCHAR), `consult_precedence_flag` (BOOL)
- `histology_discordance_flag`, `t_stage_discordance_flag` (BOOL)
- `confidence_rank` (INT): 1=synoptic, 2=tumor_path, 3=note
- `source_tables` (VARCHAR), `procedure_raw` (VARCHAR)

### `molecular_test_episode_v2` (table)

One row per molecular testing event. Deep-parsed mutation flags and quality indicators.

- `research_id` (INT), `molecular_episode_id` (INT)
- `platform_raw`, `platform` (VARCHAR): ThyroSeq/Afirma/Other
- `test_date_native` (DATE), `resolved_test_date` (VARCHAR)
- `date_status` (VARCHAR), `date_confidence` (INT)
- `overall_result_class` (VARCHAR): positive/negative/suspicious/indeterminate/non_diagnostic/cancelled
- `detailed_findings_raw` (VARCHAR)
- Mutation flags: `braf_flag`, `braf_variant`, `ras_flag`, `ras_subtype`, `ret_flag`, `ret_fusion_flag`, `tert_flag`, `ntrk_flag`, `eif1ax_flag`, `tp53_flag`, `pax8_pparg_flag`, `cna_flag`, `fusion_flag`, `loh_flag`, `alk_flag` (BOOL/VARCHAR)
- `high_risk_marker_flag` (BOOL): composite of BRAF V600E, TERT, TP53, ALK/RET/NTRK fusions
- `inadequate_flag`, `cancelled_flag` (BOOL)
- Linkage: `linked_fna_episode_id`, `linked_surgery_episode_id` (VARCHAR)
- `adjudication_status` (VARCHAR)

### `rai_treatment_episode_v2` (table)

One row per RAI treatment event with assertion status and treatment classification.

- `research_id` (INT), `rai_episode_id` (INT)
- `rai_date_native` (DATE), `resolved_rai_date` (DATE)
- `date_status` (VARCHAR), `date_confidence` (INT)
- `dose_mci` (DOUBLE), `dose_text_raw` (VARCHAR)
- `rai_assertion_status` (VARCHAR): definite_received/likely_received/planned/historical/negated/ambiguous
- `rai_intent` (VARCHAR): remnant_ablation/adjuvant/metastatic_disease/recurrence/unknown
- `completion_status` (VARCHAR): completed/recommended/not_received/uncertain
- `rai_confidence` (DOUBLE)
- Linkage: `linked_surgery_episode_id` (VARCHAR)
- Scan context: `pre_scan_flag`, `post_therapy_scan_flag`, `iodine_avidity_flag` (BOOL)
- Labs: `stimulated_tg`, `stimulated_tsh` (DOUBLE)

### `imaging_nodule_long_v2` (table)

One row per nodule per imaging exam. Multi-modality (US/CT/MRI).

- `research_id` (INT), `imaging_exam_id` (INT), `nodule_id` (VARCHAR)
- `modality` (VARCHAR): US/CT/MRI
- `exam_date_native` (DATE), `resolved_exam_date` (DATE)
- `date_status` (VARCHAR), `date_confidence` (INT)
- `nodule_index_within_exam` (INT)
- `size_cm_max`, `size_cm_x`, `size_cm_y`, `size_cm_z` (DOUBLE)
- `composition`, `echogenicity`, `shape`, `margins`, `calcifications` (VARCHAR)
- `tirads_score` (INT), `tirads_category` (VARCHAR)
- `laterality`, `location_detail` (VARCHAR)
- `suspicious_node_flag`, `growth_flag`, `dominant_nodule_flag` (BOOL)
- Linkage: `linked_fna_episode_id`, `linked_molecular_episode_id` (VARCHAR)

### `imaging_exam_summary_v2` (table)

One row per imaging exam. Aggregates nodule-level data.

- `research_id` (INT), `modality` (VARCHAR), `imaging_exam_id` (INT)
- `exam_date_native` (DATE), `date_status` (VARCHAR)
- `nodule_count` (INT), `max_nodule_size_cm` (DOUBLE), `max_tirads_score` (INT)
- `any_suspicious_node` (BOOL), `any_growth_noted` (BOOL)

### `operative_episode_detail_v2` (table)

One row per surgery episode with detailed operative findings.

- `research_id` (INT), `surgery_episode_id` (INT)
- `surgery_date_native` (DATE), `date_status` (VARCHAR)
- `procedure_raw`, `procedure_normalized` (VARCHAR)
- `laterality` (VARCHAR)
- `central_neck_dissection_flag`, `lateral_neck_dissection_flag` (BOOL)
- `rln_monitoring_flag` (BOOL), `rln_finding_raw` (VARCHAR)
- `parathyroid_autograft_flag` (BOOL), `parathyroid_autograft_count` (INT), `parathyroid_autograft_site` (VARCHAR)
- `parathyroid_resection_flag` (BOOL)
- `gross_ete_flag`, `local_invasion_flag`, `tracheal_involvement_flag`, `esophageal_involvement_flag`, `strap_muscle_involvement_flag`, `reoperative_field_flag` (BOOL)
- `ebl_ml` (DOUBLE), `drain_flag` (BOOL)
- `operative_findings_raw` (VARCHAR)

### `fna_episode_master_v2` (table)

One row per FNA episode with Bethesda and laterality.

- `research_id` (INT), `fna_episode_id` (INT)
- `fna_date_native` (DATE), `resolved_fna_date` (DATE)
- `date_status` (VARCHAR), `date_confidence` (INT)
- `bethesda_raw` (VARCHAR), `bethesda_category` (INT)
- `pathology_diagnosis`, `pathology_extended` (VARCHAR)
- `specimen_site_raw` (VARCHAR), `laterality` (VARCHAR)
- Linkage: `linked_molecular_episode_id`, `linked_surgery_episode_id` (VARCHAR)

### `event_date_audit_v2` (table)

One row per extracted fact across all domains. Used for date quality metrics.

- `domain` (VARCHAR): tumor/molecular/rai/imaging/operative/fna
- `research_id` (INT)
- `native_date`, `resolved_date` (VARCHAR)
- `date_status` (VARCHAR), `date_confidence` (INT)
- `anchor_source`, `source_table` (VARCHAR)

### `patient_cross_domain_timeline_v2` (table)

Union of all episodes ordered chronologically per patient.

- `research_id` (INT), `event_type` (VARCHAR), `domain` (VARCHAR)
- `event_date` (DATE), `episode_id` (INT), `event_detail` (VARCHAR)

### Linkage Tables

- `imaging_fna_linkage_v2`: imaging nodule -> FNA with confidence tier
- `fna_molecular_linkage_v2`: FNA -> molecular test with confidence tier
- `preop_surgery_linkage_v2`: preop event -> surgery with confidence tier
- `surgery_pathology_linkage_v2`: surgery -> pathology tumor
- `pathology_rai_linkage_v2`: pathology -> RAI treatment
- `linkage_summary_v2`: aggregate linkage counts by tier

### Reconciliation Review Views

- `pathology_reconciliation_review_v2`: histology/staging mismatches
- `molecular_linkage_review_v2`: unlinked tests, chronology issues
- `rai_adjudication_review_v2`: dose/chronology/assertion issues
- `imaging_pathology_concordance_review_v2`: laterality/size discrepancies
- `operative_pathology_reconciliation_review_v2`: procedure/specimen mismatches

### QA Tables

- `qa_issues_v2`: all detected issues with check_id, severity, description
- `qa_date_completeness_v2`: date quality metrics per domain
- `qa_summary_by_domain_v2`: aggregated issue counts
- `qa_high_priority_review_v2`: error-severity items only

See `docs/pipeline_architecture_v2.md` for full architecture documentation.

---

## Date Association & Provenance Policy (added 2026-03-10)

### Problem

Note-derived entity tables (`note_entities_*`) have high `entity_date` null rates (61–98%).
Without a systematic fallback policy, time-dependent analyses (recurrence endpoints,
time-to-RAI, genotype–phenotype timelines) lose 30–70% of their data.

### Core Tables Involved

| Table | Date Column | Type | Notes |
|-------|-------------|------|-------|
| `clinical_notes_long` | `note_date` | VARCHAR (YYYY-MM-DD) | Encounter-level anchor; highest-volume fallback |
| `note_entities_*` (6 tables) | `entity_date` | VARCHAR | Native extraction; high null rate |
| `molecular_testing` | `"date"` | VARCHAR | Quoted (reserved word); may be day-level or year-only |
| `genetic_testing` | `"date"` | VARCHAR | Same Excel source as `molecular_testing` |
| `path_synoptics` | `surg_date` | VARCHAR | Surgical anchor; not `surgery_date` |
| `fna_history` | `fna_date_parsed` | VARCHAR (YYYY-MM-DD) | Parsed FNA date; `fna_date` is a computed alias in views |

### Provenance Columns (added to all `note_entities_*` base tables)

Added by `scripts/27_date_provenance_formalization.sql`:

| Column | Type | Description |
|--------|------|-------------|
| `inferred_event_date` | DATE | Best-available date via fallback precedence |
| `date_source` | VARCHAR | Which table/column provided the date |
| `date_granularity` | VARCHAR | `day` or `year` (year = YYYY-01-01 placeholder) |
| `date_confidence` | INTEGER | 0–100 confidence score |

### Precedence Rules

Enforced identically in `scripts/15_date_association_views.sql` (enriched views)
and `scripts/27_date_provenance_formalization.sql` (base table backfill):

| Priority | Source | Confidence | Granularity |
|----------|--------|------------|-------------|
| 1 | `entity_date` (native extraction) | 100 | day |
| 2 | `clinical_notes_long.note_date` | 70 | day |
| 3 | `path_synoptics.surg_date` | 60 | day |
| 4 | `molecular_testing."date"` (day-level) | 60 | day |
| 4b | `molecular_testing."date"` (year-only) | 50 | year |
| 5 | `fna_history.fna_date_parsed` | 55 | day |
| — | No source found | 0 | NULL |

### `date_source` Values

- `entity_date` — extracted directly from note text near entity mention
- `note_date` — encounter/service date from note header
- `surg_date` — primary surgery date from synoptic pathology
- `molecular_testing_date` — test date from ThyroSeq/Afirma records
- `fna_date_parsed` — FNA procedure date
- `unrecoverable` — no date source available; flagged for manual review

### Fallback Chain by Entity Domain

| Domain | Fallback sources |
|--------|-----------------|
| genetics | entity → note → surg → molecular → fna (full 5-source) |
| staging | entity → note → surg |
| procedures | entity → note → surg |
| complications | entity → note → surg |
| medications | entity → note |
| problem_list | entity → note |

### Date Status Taxonomy V3 (Script 17)

Applied to all enriched views via `scripts/17_semantic_cleanup_v3.sql` and `scripts/17_semantic_cleanup_v3_views.sql`:

| Status | Source | Confidence |
|--------|--------|------------|
| `exact_source_date` | `entity_date` (native extraction) | 100 |
| `inferred_day_level_date` | `note_date` fallback | 70 |
| `coarse_anchor_date` | surgery / FNA / genetics year | 35–60 |
| `unresolved_date` | no source found | 0 |

**Standardized provenance columns** (present on all enriched views):

| Column | Type |
|--------|------|
| `date_status` | VARCHAR |
| `date_is_source_native_flag` | BOOLEAN |
| `date_is_inferred_flag` | BOOLEAN |
| `date_requires_manual_review_flag` | BOOLEAN |
| `inferred_event_date` | DATE |

### Related Views

| View | Source | Purpose |
|------|--------|---------|
| `enriched_note_entities_*` (6) | Script 15 | Enriched views with provenance columns computed at query time |
| `missing_date_associations_audit` | Script 15 | Union of all enriched views for audit |
| `date_recovery_summary` | Script 15 | Aggregate rescue stats by domain × source |
| `timeline_rescue_v2_mv` | Script 17 | Genetics rescue view with V3 taxonomy; extend with UNION ALL for other domains |
| `timeline_unresolved_summary_v2_mv` | Script 17 | KPI rollup: row/patient count and % by date_status |
| `validation_failures_v3` | Script 17 | Reclassifies coarse anchor dates from error → info; only truly unresolvable dates remain errors |
| `enriched_master_timeline` | Script 27 | Filtered audit (excludes unrecoverable) |
| `date_rescue_rate_summary` | Script 27 | KPI: rescue rate % and avg confidence per domain |

### Deployment

Script 27 depends on script 15 views (`missing_date_associations_audit`) and all
base tables being present in `thyroid_research_2026`. Run after scripts 15–26.

---

## Legacy Compatibility Layer (Script 27_fix_legacy_episode_compatibility)

**Created:** 2026-03-10  
**Script:** `scripts/27_fix_legacy_episode_compatibility.py`  
**Purpose:** Bridge legacy episode architecture references (scripts 17/18/22/23/26) to the current
modern table stack. Run this script if the dashboard shows "Missing critical tables" errors.

### Legacy → Modern Mapping

| Legacy Table | Source Table(s) | Key Mapped Columns |
|---|---|---|
| `molecular_episode_v3` | `advanced_features_v3` | `braf/ras/ret/tert_mutation_mentioned`, `overall_linkage_confidence`, `molecular_analysis_eligible_flag` |
| `rai_episode_v3` | `extracted_clinical_events_v4` | `rai_assertion_status`, `rai_interval_class`, `rai_treatment_certainty` |
| `validation_failures_v3` | `qa_issues` | `severity` (v3 reclassification: coarse_anchor_date → info), `requires_manual_review_flag` |
| `tumor_episode_master_v2` | `advanced_features_v3` + `master_timeline` | `surgery_date`, `histology_1_type`, `analysis_eligible_flag`, `adjudication_needed_flag` |
| `linkage_summary_v2` | `patient_level_summary_mv` | `linkage_confidence_tier`, `linked_domain_count`, per-domain has_* flags |

### Modern Stack (No Legacy Needed)

| Modern Table | Replaces | Notes |
|---|---|---|
| `extracted_clinical_events_v4` | legacy episode tables | All clinical event extraction |
| `advanced_features_v3` | `molecular_episode_v2/v3` | 60+ engineered features including molecular flags |
| `master_timeline` | `patient_cross_domain_timeline_v2` | Surgery-level timeline, multi-surgery safe |
| `qa_issues` | `validation_failures_v2/v3` | All QA severity levels |
| `patient_level_summary_mv` | `linkage_summary_v2` | Patient-level coverage summary |
| `risk_enriched_mv` | `recurrence_risk_features_mv` | Risk enrichment with PSM-ready features |

### Usage

```bash
# Fix dashboard "Missing critical tables" error:
.venv/bin/python scripts/27_fix_legacy_episode_compatibility.py

# Use local DuckDB instead of MotherDuck:
.venv/bin/python scripts/27_fix_legacy_episode_compatibility.py --local

# Dry-run preview:
.venv/bin/python scripts/27_fix_legacy_episode_compatibility.py --dry-run
```

After running, restart the Streamlit dashboard to clear the cached connection.

---

## Date Association & Provenance Policy — Quick Reference (added 2026-03-10)

### Core Tables & Date Sources
- `clinical_notes_long.note_date` (VARCHAR) — canonical note-level anchor
- `note_entities_*` family (6 tables) — high `entity_date` null rate; now enriched via V3 taxonomy
- `genetic_testing` — `DATE_1_year`, `DATE_2_year`, `DATE_3_year` (BIGINT, year-level only)
- `path_synoptics` / `tumor_pathology` — `surg_date` / `surgery_date`
- `fna_cytology` (or `fna_history`) — `fna_date` / `fna_date_parsed`

### Date Status Taxonomy V3 (applied to all enriched views — created in script 17)
- `exact_source_date` (entity_date, confidence 100)
- `inferred_day_level_date` (note_date fallback, confidence 70)
- `coarse_anchor_date` (surgery/FNA/genetics year, confidence 35–60)
- `unresolved_date` (confidence 0)

### Standardized Provenance Columns (added to all enriched views)
- `date_status` VARCHAR
- `date_is_source_native_flag` BOOLEAN
- `date_is_inferred_flag` BOOLEAN
- `date_requires_manual_review_flag` BOOLEAN
- `inferred_event_date` DATE

### Views (created by `scripts/17_semantic_cleanup_v3.sql`)
- `timeline_rescue_v2_mv`
- `timeline_unresolved_summary_v2_mv`
- `validation_failures_v3` (and `patient_validation_rollup_v2_mv`)
