# Thyroid Master DuckDB Data Dictionary

> **MotherDuck / cloud canonical contract:** For **MotherDuck** schema maps, promotion planes (`main`, `v2_stage`, `qa`, `release_*`), and analyst views, use [`docs/motherduck_database_contract_v1.md`](docs/motherduck_database_contract_v1.md) — not this file alone. This dictionary remains oriented to **local file** `thyroid_master.duckdb` and historical pipeline tables.

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

### `demographics_harmonized_v2` (table, added 2026-03-13)

One row per patient. Cross-source harmonized demographics with full provenance.
Eliminates 715 false-missing age records by backfilling DOB from `stg_dob_excel_recovery`,
`thyroid_weights`, `thyroglobulin_labs`, and `anti_thyroglobulin_labs`, then computing
age via `DATE_DIFF('year', dob, surgery_date)` with birthday correction. Annotated
surgery dates (e.g. "8/11/2014 (MANNUALLY ADDED...)") parsed via `TRY_STRPTIME` +
regex extraction.

Source priority (highest first):
- **Age**: benign_pathology > tumor_pathology > path_synoptics.age > stg_dob_excel_recovery.age > MRN crosswalk > Excel DOB-derived > thyroid_weights DOB > lab DOB
- **Sex**: benign_pathology > tumor_pathology > path_synoptics.gender > stg_dob_excel_recovery.gender > MRN crosswalk > thyroglobulin_labs.gender > anti_tg_labs.gender
- **Race**: path_synoptics.race > stg_dob_excel_recovery.race > MRN crosswalk > thyroglobulin_labs.race > anti_tg_labs.race

Key columns:

- `research_id`: patient identifier (INT)
- `age_at_surgery`: harmonized age (INT, NULL if no source)
- `age_source`: provenance label (benign_pathology|tumor_pathology|path_synoptics|excel_dob_unanimous|excel_dob_derived|thyroid_weights_dob|thyroglobulin_labs_dob|anti_tg_labs_dob)
- `sex`: harmonized sex ('Male'|'Female', NULL if no source)
- `sex_source`: provenance label
- `race`: harmonized race (raw value from best source, NULL if no source)
- `race_source`: provenance label
- `best_surgery_date`: DATE used for DOB-based age calculation
- `best_dob`: DATE from DOB backfill chain (NULL if no DOB in any source)

Coverage (as of 2026-03-13): 11,673 patients; age 99.26% (11,587), sex 98.00% (11,440), race 97.93% (11,431).
86 truly missing age (orphan research_ids absent from all raw Excel files, concentrated in IDs > 9800).
233 missing sex, 242 missing race (patients not in any source file with demographics).
MRN crosswalk recovered 569 sex and 566 race values from patients whose OP Sheet research_id
differed from their All Diagnoses research_id (same EUH_MRN, different research_id assignment).
33 DOB conflicts across sources (majority-vote resolved).

### `stg_dob_excel_recovery` (table, added 2026-03-13)

Cross-file DOB resolution from 6 sources (3 Excel + 3 DB). Majority vote resolves
DOB conflicts; priority tiebreak when no majority: all_diagnoses > op_sheet >
thyroid_weights > notes > thyroglobulin_labs > anti_tg_labs.

- `dob_resolved`: winning DOB after majority vote
- `age_at_surgery`: calculated from DOB + surgery date
- `gender_excel`, `race_excel`: from All Diagnoses Excel
- `dob_n_sources`: number of sources with DOB for this patient
- `dob_concordant`: TRUE if all sources agree
- `dob_resolution`: unanimous | majority_N_of_M | priority_tiebreak_SOURCE

### `stg_mrn_crosswalk_demographics` (table, added 2026-03-13)

MRN-based crosswalk recovering demographics for patients whose OP Sheet `research_id`
differs from their All Diagnoses `research_id` (same `EUH_MRN`). These 570 patients
were invisible to the standard join-by-research_id pipeline. Gender, Race, Age, and
DOB are pulled from All Diagnoses via the matched MRN.

- `research_id`: OP Sheet research_id (INT, used in master_cohort)
- `ad_research_id`: matching All Diagnoses research_id
- `mrn`: shared EUH_MRN that links the two records
- `sex`, `race`, `age_at_surgery`, `dob`: demographics from All Diagnoses

Coverage: 570 patients (569 recoverable sex, 566 recoverable race).

### `us_dominant_nodule_size_v1` (table, added 2026-03-13)

Per-patient dominant (largest) thyroid nodule size from ultrasound, extracted from
TIRADS structured data (`raw_us_tirads_excel_v1`) and NLP from free-text nodule
descriptions in `serial_imaging_us`. Fills the gap where
`serial_imaging_us.dominant_nodule_size_on_us` was entirely NULL.

- `dominant_nodule_size_cm`: largest nodule in cm
- `size_source`: tirads_structured | nlp_detail_text | imaging_excel
- `imaging_excel_cm`, `tirads_structured_cm`, `nlp_extracted_cm`: per-source values

Coverage: 3,440 patients (was 0).

### `qa_missing_demographics` (table)

Residual patients with missing demographics **after** cross-source backfill.
Now reads from `demographics_harmonized_v3`. Includes orphan flagging and linkage method.

Key columns:

- `research_id`: patient identifier (INT)
- `canonical_research_id`: MRN-linked canonical RID from `linkage_master_v1`
- `linkage_method`: 'direct' | 'mrn_crosswalk' | 'identity'
- `is_orphan_flag`: TRUE if patient has no data in any of 21 source tables
- `age_at_surgery`, `sex`, `race`: best values from `demographics_harmonized_v3`
- `age_source`, `sex_source`, `race_source`: provenance labels
- `age_derivation_method`: 'surgery_date' | 'note_date_fallback' | 'lab_specimen_date_fallback' | 'canonical_rid_inheritance'
- `age_flag`: 'MISSING_AGE' or 'OK'
- `sex_flag`: 'MISSING_SEX' or 'OK'
- `race_flag`: 'MISSING_RACE' or 'OK'
- `source_priority`: combined provenance string

### `mrn_crosswalk_v1` (table, added 2026-03-13)

Permanent MRN ↔ research_id crosswalk from 4 raw Excel sources. 33,433 rows
covering 10,078 patients and 9,513 distinct EUH_MRNs. Scans `raw_path_synoptics`,
`raw_clinical_notes`, `raw_complications`, `raw_operative_details`.

Key columns:

- `research_id`, `euh_mrn`, `tec_mrn`: linkage keys
- `canonical_research_id`: the research_id with highest data volume for this MRN
- `dob`, `first_name`, `last_name`: PHI-bearing (raw tables only)
- `gender_raw`, `race_raw`: raw demographics from source
- `source_tables`: list of source tables where this (RID, MRN) pair appears
- `linkage_method`: 'direct' (RID = canonical) or 'mrn_crosswalk' (RID differs)
- `confidence`: 1.0 for direct, 0.95 for crosswalk

### `linkage_master_v1` (table, added 2026-03-13)

Single source of truth mapping every `master_cohort` research_id to its canonical.
11,673 rows (one per patient). Merges `mrn_crosswalk_v1` with legacy
`stg_mrn_crosswalk_demographics`.

Key columns:

- `research_id`: patient identifier (same as `master_cohort`)
- `canonical_research_id`: best-linked RID for this patient
- `euh_mrn`: MRN if available
- `linkage_method`: 'direct' | 'mrn_crosswalk' | 'identity'
- `confidence`: linkage confidence score (1.0 for direct/identity, 0.95 for crosswalk)
- `has_mrn`: TRUE if this patient has any MRN in any raw source

### `demographics_harmonized_v3` (table, added 2026-03-13)

Supersedes `demographics_harmonized_v2`. Joins 13 source CTEs through the MRN
crosswalk with orphan flagging. 11,673 rows.

Enhancements over v2:
- Cross-MRN demographics recovery (P10): sex/race from MRN-linked records
- Lab specimen date fallback (P12): age from DOB + earliest thyroglobulin lab date
- Canonical-RID inheritance (P13): demographics from canonical_research_id for split-RID patients
- Orphan detection: `is_orphan_flag` = TRUE for patients with no data in any source
- Age derivation method tracking: 'surgery_date' | 'note_date_fallback' | 'lab_specimen_date_fallback' | 'canonical_rid_inheritance'

Coverage (final): age 99.28% (84 missing, all orphans), sex 98.01%, race 97.94%.
Non-orphan coverage: age 100%, sex 98.72%, race 98.65%.

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

See `docs/llm_extraction_spec.md` for controlled vocabularies and extraction details.

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
base tables being present in `thyroid_master.duckdb`. Run after scripts 15–26.

---

## Traceability & Date Accuracy Guarantee (added 2026-03-12)

### Strict Lab Date Precedence Rule

Lab collection dates **always** take precedence over note encounter dates.
This rule is enforced in `provenance_enriched_events_v1`:

```sql
-- Canonical date resolution for all clinical events
COALESCE(
    TRY_CAST(specimen_collect_dt AS DATE),  -- 1. Lab collection date   (confidence 1.0)
    TRY_CAST(event_date AS DATE),           -- 2. Entity-extracted date (confidence 0.7)
    followup_date                           -- 3. Note encounter date   (last resort)
) AS event_date_correct
```

### New Provenance Columns (provenance_enriched_events_v1)

| Column | Type | Description |
|--------|------|-------------|
| `specimen_collect_dt` | VARCHAR | Specimen collection date from `thyroglobulin_labs` (NULL for non-lab events) |
| `event_date_correct` | DATE | Best-available date per strict lab-date precedence rule |
| `date_status_final` | VARCHAR | `LAB_DATE_USED` / `ENTITY_DATE_USED` / `ENTITY_DATE_EQUALS_NOTE_DATE` / `NOTE_DATE_FALLBACK` / `NO_DATE` |
| `direct_source_link` | VARCHAR | Pipe-delimited `source_column|research_id|event_subtype|evidence_snippet` |
| `provenance_created_at` | TIMESTAMP | Audit timestamp |

### date_status_final Values

| Value | Meaning | Confidence |
|-------|---------|-----------|
| `LAB_DATE_USED` | `specimen_collect_dt` from structured lab table | 1.0 |
| `ENTITY_DATE_USED` | `entity_date` differs from encounter date | 0.7 |
| `ENTITY_DATE_EQUALS_NOTE_DATE` | Entity date present but equals note encounter date | 0.5 |
| `NOTE_DATE_FALLBACK` | Only note encounter date available (error for labs) | 0.0 |
| `NO_DATE` | No date source found | 0.0 |

### New Tables / Views

| Table | Script | Purpose |
|-------|--------|---------|
| `provenance_enriched_events_v1` | `46_provenance_audit.py` | Clinical events with strict lab-date precedence + `direct_source_link` |
| `lineage_audit_v1` | `46_provenance_audit.py` | Raw → note → extracted → final cohort traceability (one row per patient) |
| `val_provenance_traceability` | `29_validation_engine.py` | 4-check validation: `direct_source_link` completeness + zero-tolerance `NOTE_DATE_FALLBACK` for labs |

### Extraction Pipeline Enhancements (v2026-03-12)

**`utils/text_helpers.py` — `extract_nearby_date()` and `extract_nearby_date_with_confidence()`:**
- Added `_LAB_DATE_KEYWORDS` regex that scans for explicit collection date phrases before any generic date
- Keywords: "collected on", "drawn on", "specimen date:", "result date:", "received:", "reported on", "accession date:"
- Returns `(date, 1.0)` when keyword found, `(date, 0.7)` for generic nearby date

**`llm_extraction/extract_llm.py` — Functional LLM extractor:**
- `_build_prompt()` loads `prompts/lab_date_extraction_v1.txt` system prompt
- Output JSON schema: `{entity_type, entity_value, entity_date, date_confidence, present_or_negated, evidence_text, source_line}`
- Explicit instruction: lab dates > note encounter date; `date_confidence=1.0` for keyword-found dates

**`llm_extraction/run_extraction.py` — Selective re-extraction:**
- `--target DOMAIN` re-extracts only one entity domain (merges with existing parquet)
- `--research-ids FILE` re-extracts only flagged patients (one research_id per line)

### QA Guarantee

Zero tolerance is enforced: any `date_status_final = 'NOTE_DATE_FALLBACK'` for a lab event
is classified as **error severity** in `val_provenance_traceability` and inserted into `qa_issues`.

Run the full audit and validation:
```bash
.venv/bin/python scripts/46_provenance_audit.py --md
.venv/bin/python scripts/29_validation_engine.py --md
```

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

# Use local DuckDB instead of local DuckDB:
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

---

### Date Association & Provenance Policy (added 2026-03-10)

**Core Tables & Date Sources**
- `clinical_notes_long.note_date` (VARCHAR) — canonical note-level anchor
- `note_entities_*` family (6 tables) — high `entity_date` null rate; enriched via V3 taxonomy
- `genetic_testing` — `DATE_1_year`, `DATE_2_year`, `DATE_3_year` (BIGINT)
- `path_synoptics` / `tumor_pathology` — `surg_date` / `surgery_date`
- `fna_cytology` (or `fna_history`) — `fna_date` / `fna_date_parsed`

**Date Status Taxonomy V3** (scripts 17 + 27)
- `exact_source_date` (confidence 100)
- `inferred_day_level_date` (confidence 70)
- `coarse_anchor_date` (confidence 35-60)
- `unresolved_date` (confidence 0)

**Standardized Provenance Columns**
- `date_status` VARCHAR
- `date_is_source_native_flag` BOOLEAN
- `date_is_inferred_flag` BOOLEAN
- `date_requires_manual_review_flag` BOOLEAN
- `inferred_event_date` DATE

**Views** (created by 26/25/29)
- `timeline_rescue_v2_mv`
- `timeline_unresolved_summary_v2_mv`
- `validation_failures_v3`
- `enriched_patient_timeline_v3_mv` — timeline_rescue_v3_mv joined with patient header, first RAI, and per-patient rescue rate; `genetic_year` is coarsened to YYYY-01-01 when used as a date anchor
- `date_rescue_rate_summary` — single-row-per-domain KPI table; overall rescue rate, rescued row count, and average confidence across all 6 note_entities_* domains
- `timeline_rescue_v3_mv` — V3 taxonomy enrichment of all 6 note_entities_* tables; adds date_status, date_is_source_native_flag, date_is_inferred_flag, date_requires_manual_review_flag, inferred_event_date
- `time_to_rai_v3_mv` — per-patient `time_to_rai_days`, `ajcc_stage_grouped`, and `date_rescue_confidence`; uses inferred timeline anchors from `inferred_event_date`
- `recurrence_free_survival_v3_mv` — per-patient `time_to_recurrence_days` with `censoring_flag` and surgery-aligned censor dates for recurrence endpoint analysis
- `genotype_stratified_outcomes_v3_mv` — genotype-stratified (`braf_ras_status`) survival surface combining RAI timing, recurrence-free timing, stage grouping, and rescue-confidence tier

---

## ThyroSeq Workbook Integration (script 41)

**Source:** `Thyroseq Data Complete.xlsx` — 83 rows, 34 columns of ThyroSeq molecular testing results, pathology, demographics, serial Tg/TgAb/TSH follow-up, surgery, RAI, and imaging.

### Staging Tables

| Table | Description |
|-------|-------------|
| `stg_thyroseq_excel_raw` | Raw workbook rows with source metadata, normalized identifiers (`mrn_norm`, `dob_norm`, `name_norm`), and deterministic `row_hash` |
| `stg_thyroseq_match_results` | Patient matching results: `matched_research_id`, `match_method`, `match_confidence`, `review_required`, `conflict_flags` |
| `stg_thyroseq_parsed` | Parsed/normalized fields: mutations, fusions, margins, ETE, lymph nodes, angioinvasion, demographics, surgery, RAI |

### Enrichment Tables (long format)

| Table | Description |
|-------|-------------|
| `thyroseq_molecular_enrichment` | One row per molecular test record: mutation/fusion flags, allele fractions, GEP, CNA |
| `thyroseq_followup_labs` | One row per serial Tg/TgAb/TSH measurement: value, operator, date, stimulated flag |
| `thyroseq_followup_events` | One row per surgery/RAI/imaging event with dates and parsed attributes |

### Audit Tables

| Table | Description |
|-------|-------------|
| `thyroseq_fill_actions` | Field-level audit log of null-fill operations with old/proposed values |
| `thyroseq_review_queue` | Items requiring manual review: match ambiguity, parse failures, structured conflicts |

### Match Methods

| Method | Confidence | Auto-merge |
|--------|-----------|------------|
| `exact_mrn_dob_name` | 1.0 | Yes |
| `exact_mrn_name` | 0.9 | Yes |
| `exact_mrn_only` | 0.7 | Yes |
| `exact_name_dob` | 0.6 | No (review) |
| `mrn_with_discordance` | 0.3 | No (review) |
| `mrn_ambiguous_multi` | 0.2 | No (review) |
| `manual_review_required` | 0.0 | No (review) |

### Fill Policy

Only auto-fill when: target field is NULL, source match confidence >= 0.7, source value is parseable. Conflicts are routed to `thyroseq_review_queue`.

### Run Command

```bash
.venv/bin/python scripts/41_ingest_thyroseq_excel.py \
    --input '/path/to/Thyroseq Data Complete.xlsx' \
    [--md] [--local] [--dry-run]
```

---

## Normalized molecular results layer (governed; script 131)

**Purpose:** Store vendor-neutral, longitudinal molecular **assay** and **variant** facts alongside — not instead of — `molecular_testing`, ThyroSeq enrichment tables, and `molecular_test_episode_v2`. Ingestion is **append-only**; corrections use `superseded_by_molecular_result_id` on new rows. **Linkage to patients is exact** (`research_id` only after deterministic resolution elsewhere); native IDs are kept as provenance columns.

**DDL:** `scripts/sql/131_molecular_results_layer_ddl.sql`  
**Runner:** `scripts/131_molecular_results_layer.py` (`--execute`, optional `--md`)

### Design notes

| Principle | How it is expressed |
|-----------|----------------------|
| Canonical patient key | `research_id` (INTEGER), matching `molecular_testing` / episode tables |
| Source-native identity | `source_patient_id`, `source_specimen_id`, `source_accession` (optional VARCHAR) |
| No source overwrite | New tables only; loaders must INSERT, not UPDATE source tables |
| Exact-match linkage | No fuzzy joins inside this schema; ambiguous rows get `normalization_status = 'quarantine'` or `pending_review` and appear in `molecular_normalization_review_v1` |
| Append-only batches | `ingestion_run_id`, `ingestion_ts`, `lineage_id` (UUID VARCHAR per batch/line) tie rows to a load |
| DuckLake (MotherDuck) | Tables have **no PRIMARY KEY or secondary indexes** (platform limitation); logical uniqueness is `molecular_result_id` / `(domain, source_code)` in `molecular_code_crosswalk`, enforced by loaders |

### `molecular_results`

One row per **assay result envelope** (specimen + order + panel instance). Aligns naming with existing domains where possible: `test_date_native` (VARCHAR, same spirit as `molecular_test_episode_v2.test_date_native`), `platform`, optional `molecular_episode_id` link.

| Column | Type | Description |
|--------|------|-------------|
| `molecular_result_id` | VARCHAR | Surrogate UUID for the result row (loader-generated) |
| `research_id` | INTEGER | Canonical patient key |
| `source_patient_id` | VARCHAR | Vendor/file patient id (e.g. MRN string) |
| `source_specimen_id` | VARCHAR | Accession / specimen id from source |
| `source_accession` | VARCHAR | Alternate accession label when split from specimen id |
| `assay_name` | VARCHAR | Human-readable assay / panel name |
| `panel_version` | VARCHAR | Panel or software version |
| `platform` | VARCHAR | e.g. ThyroSeq, Afirma — consistent with `molecular_test_episode_v2.platform` |
| `vendor` | VARCHAR | Laboratory / vendor when distinct from platform |
| `loinc_code` | VARCHAR | LOINC when known |
| `test_date_native` | VARCHAR | Raw date string from source |
| `test_date_parsed` | DATE | Parsed test date when available |
| `interpretation_summary` | VARCHAR | Report-level interpretation |
| `risk_call` | VARCHAR | Structured risk / tier if provided |
| `canonical_hgvs` | VARCHAR | Single “header” HGVS when report is one-variant |
| `raw_payload_json` | JSON | Full normalized or raw structured payload |
| `payload_checksum` | VARCHAR | SHA-256 hex over canonical serialized payload (loader) |
| `parse_status` | VARCHAR | `pending` / `ok` / `partial` / `failed` (ThyroSeq-style) |
| `normalization_status` | VARCHAR | `raw` / `mapped` / `verified` / `quarantine` / `pending_review` |
| `qc_flags` | JSON | Array or object of QC codes |
| `lineage_id` | VARCHAR | Batch or transformation lineage UUID |
| `ingestion_ts` | TIMESTAMP | Row insert time |
| `ingestion_run_id` | VARCHAR | FK-style reference to `molecular_ingestion_runs` |
| `source_table` | VARCHAR | Origin table name e.g. `thyroseq_molecular_enrichment` |
| `source_row_fingerprint` | VARCHAR | e.g. `source_row_hash` from ThyroSeq staging |
| `molecular_episode_id` | INTEGER | Optional join to `molecular_test_episode_v2` |
| `superseded_by_molecular_result_id` | VARCHAR | New row id that replaces this assertion (append-only corrections) |

### `molecular_variant_long`

One row per **variant call** (SNV, indel, fusion partner set, CNV, etc.).

| Column | Type | Description |
|--------|------|-------------|
| `molecular_variant_id` | VARCHAR | Surrogate UUID |
| `molecular_result_id` | VARCHAR | Parent result |
| `research_id` | INTEGER | Denormalized patient key for simple filters |
| `gene_symbol` | VARCHAR | Gene (partner A for fusions) |
| `transcript_id` | VARCHAR | RefSeq transcript e.g. NM_… |
| `genomic_hgvs` | VARCHAR | g. notation when available |
| `cdna_hgvs` | VARCHAR | c. notation |
| `protein_hgvs` | VARCHAR | p. notation |
| `canonical_hgvs` | VARCHAR | Preferred single HGVS for the call |
| `variant_class` | VARCHAR | `SNV` / `INDEL` / `FUSION` / `CNV` / `OTHER` |
| `allele_fraction` | DOUBLE | VAF or copy-ratio surrogate |
| `zygosity` | VARCHAR | e.g. het / hom / unknown |
| `interpretation_text` | VARCHAR | Variant-level text |
| `risk_call` | VARCHAR | Tier / ACMG bucket when encoded |
| `parse_status` | VARCHAR | Per-variant parse state |
| `normalization_status` | VARCHAR | Mapping / review state |
| `qc_flags` | JSON | Per-variant QC |
| `lineage_id` | VARCHAR | Shared with parent batch |
| `ingestion_ts` | TIMESTAMP | Insert time |
| `partner_gene_symbol` | VARCHAR | Fusion / rearrangement partner |
| `fusion_partner` | VARCHAR | Free-text fusion descriptor |
| `raw_variant_token` | VARCHAR | Opaque source fragment for audit |

### `molecular_assay_dictionary`

Curated reference: `assay_key` (stable string), `assay_name`, `panel_version`, `platform`, `vendor`, `loinc_code`, `loinc_long_name`, validity window, `source_reference`.

### `molecular_code_crosswalk`

Exact **source_code → target_code** map by `domain` (seed includes `variant_class` → SNV/INDEL/FUSION/CNV/OTHER). Idempotent seed via `NOT EXISTS` anti-join.

### `molecular_ingestion_runs` (optional)

`ingestion_run_id`, `started_at`, `completed_at`, `source_system`, `runner_script`, `status`, `notes`.

### Contract views (Streamlit / notebooks)

| View | Role |
|------|------|
| `molecular_results_contract_v1` | Stable column projection over `molecular_results` |
| `molecular_variant_long_contract_v1` | Stable projection over `molecular_variant_long` |
| `molecular_results_enriched_v1` | Results + `n_variants_long` scalar subquery |
| `molecular_normalization_review_v1` | `normalization_status` / `parse_status` review funnel |
| `molecular_fact_long_base_v` | Internal: stacked note genetics + assay envelope + variant rows with precedence flags |
| `molecular_fact_long_v` | Analyst union: note-derived vs `assay_structured_import`, `record_role`, `included_in_primary_analytics`, genetics review overlay |
| `molecular_results_unified_v` | Synonym of `molecular_fact_long_v` |
| `molecular_fact_lineage_qa_duplicate_candidates_v` | QA: note vs structured assay pairs within ±21 days (same patient + `molecular_family`) |

---

## V2 LLM Extraction Entity Tables

Registry-driven extraction domains from `config/extraction_domain_registry.yaml` (schema version `entity_schema_v3_2026-04-03`). Each domain produces a `note_entities_llm_<domain>.parquet` in the v2 fleet directory, staged to `v2_stage` schema in MotherDuck and promoted to `main` after passing the 8-gate promotion pipeline.

### V2 Domain Tables (23 canonical-output domains)

| Table | QA Tier | Linkage Family | Note Scope |
|-------|---------|----------------|------------|
| `note_entities_llm_imaging` | standard | imaging | all |
| `note_entities_llm_tirads_granular` | standard | imaging | all |
| `note_entities_llm_us_nodule_dynamics` | standard | imaging | all |
| `note_entities_llm_labs` | standard | followup | all |
| `note_entities_llm_tg_kinetics` | standard | followup | all |
| `note_entities_llm_pathology` | critical | pathology | path_report |
| `note_entities_llm_synoptic_pathology_enrichment` | critical | pathology | path_report |
| `note_entities_llm_rai_detailed` | critical | rai | all |
| `note_entities_llm_rad_treatment` | standard | rai | all |
| `note_entities_llm_parathyroid_detail` | standard | operative | op_note |
| `note_entities_llm_recurrence` | critical | followup | all |
| `note_entities_llm_survival_followup` | standard | followup | all |
| `note_entities_llm_cervical_ln_detail` | standard | pathology | all |
| `note_entities_llm_functional_outcomes` | informational | followup | all |
| `note_entities_llm_past_medical_hx` | informational | demographics | all |
| `note_entities_llm_past_surgical_hx` | informational | demographics | all |
| `note_entities_llm_presenting_symptoms` | informational | demographics | all |
| `note_entities_llm_physical_exam` | informational | demographics | all |
| `note_entities_llm_vascular_invasion` | critical | pathology | path_report |
| `note_entities_llm_airway_invasion` | standard | operative | op_note |
| `note_entities_llm_frozen_section_detail` | standard | operative | op_note |
| `note_entities_llm_dynamic_risk_response` | standard | followup | all |
| `note_entities_llm_patient_decision_adherence` | informational | followup | all |

### Canonical Fact Tables (v2)

| Table | Description |
|-------|-------------|
| `canonical_extracted_fact_long_v2` | All v1 + v2 domains expanded to entity-level rows. Superset of v1. Columns per `docs/fact_provenance_contract_v1.md`. |
| `canonical_fact_quarantine_v2` | Rows failing quality gates with `quarantine_reason` and `quarantine_date`. |

### Sub-Prompt Parquets (merged into parent domain)

| Parquet Stem | Parent Domain |
|-------------|---------------|
| `note_entities_llm_recurrence_detailed` | recurrence |
| `note_entities_llm_complications_rln_laryngoscopy` | complications |
| `note_entities_llm_medication_management` | medications |
| `note_entities_llm_operative_details` | operative_detail |
| `note_entities_llm_operative_v2_enrichment` | operative_detail |
| `note_entities_llm_parathyroid_per_gland` | parathyroid_detail |
| `note_entities_llm_molecular_thyroseq_afirma` | genetics |

---

## Thyroglobulin Lab Tables

Source: `raw/Thyroid_Thyroglobulin_Lab_20251120.csv` (78,112 raw rows). Ingested by `scripts/113_tg_lab_ingestion.py`.

| Table | Description | Rows |
|-------|-------------|------|
| `thyroglobulin_lab_canonical_v1` | Canonical Tg/TgAb lab results: `research_id`, `analyte` (Tg/TgAb), `result_numeric`, `result_text`, `result_date`, `lab_units`, `reference_range`, `temporal_window` | ~76,971 |
| `tg_lab_review_queue_v1` | Ambiguous Tg+TgAb combo pairs requiring manual disambiguation | ~1,035 |
| `tg_timeline_patient_summary_v1` | Per-patient summary: first/last Tg, nadir Tg, trend direction, surveillance window count | ~3,258 |
| `tg_postop_surveillance_windows_v1` | Temporal surveillance windows with per-window Tg/TgAb statistics | Derived |
| `tg_recurrence_surveillance_linkage_v1` | Join of rising-Tg patients to `extracted_recurrence_refined_v1` | Derived |

### Key Columns in `thyroglobulin_lab_canonical_v1`

- `research_id`: patient identifier (exact match to `master_cohort`)
- `analyte`: `Tg` or `TgAb`
- `result_numeric`: parsed numeric lab value
- `result_date`: `YYYY-MM-DD` normalized
- `temporal_window`: postoperative surveillance window assignment
- `source_row_hash`: deterministic hash for deduplication

---

## QA Schema (MotherDuck)

Schema `qa` in the `Thyroid 2026` catalog. Created by `scripts/114_qa_schema_setup.py`.

| Table | Description |
|-------|-------------|
| `qa.promotion_scorecard` | Gate results per run: `run_label`, `gate_id`, `status`, `detail`, `git_sha` |
| `qa.promotion_review_decisions` | Persisted review decisions: `verification_status`, `reviewer`, `waiver_reason` |
| `qa.concordance_summary` | Per-domain concordance metrics by gate run |
| `qa.domain_validation` | Schema compliance, dup rates, date coverage per gate run |
| `qa.tg_lab_ingestion_qc` | Structured QC from script 113: reconciliation gap, parse rates, patient counts |
| `qa.release_manifest` | Immutable release snapshot metadata: `release_tag`, `git_sha`, `tables_included` |

---

## MotherDuck Schema Layout

| Schema | Purpose |
|--------|---------|
| `main` | Canonical/production tables. Stable contract surfaces for analysis, manuscripts, dashboards. |
| `v2_stage` | Pre-promotion staging. Raw LLM fleet parquets (1 row per note, `result_json`). Not yet gate-validated. |
| `qa` | Validation artifacts, gate scorecards, review decisions, release manifests. |
| `release_YYYYMMDD` | Immutable point-in-time snapshots of canonical tables for manuscript reproducibility. |
