# Imaging Nodule Master Materialization — 2026-03-13

## Problem

`imaging_nodule_master_v1` had **0 rows** on MotherDuck despite
`raw_us_tirads_excel_v1` containing 19,891 source rows (Phase 12 TIRADS
Excel ingestion). The final verification report flagged the imaging domain as
**NOT VERIFIED** with a CRITICAL risk rating.

## Root Cause

Script 50 (`50_multinodule_imaging.py`) expected the raw TIRADS source to be
in **wide format** (one row per patient with columns like `TI_RADS`,
`Nodule 2 Composition`, etc.), matching the original Excel layout. However,
the Phase 12 engine (`extraction_audit_engine_v10.py`) had already unpivoted
and normalized the data before uploading to MotherDuck, producing a
**long-format** schema with columns like `composition_norm`,
`tirads_reported`, `nodule_length_mm`, and `nodule_number`.

The column-detection logic in `build_nodule_long_sql()` could not match
the normalized column names, generated SQL with literal NULL feature
references, and the WHERE clause filtered out all rows — producing a
valid-but-empty table.

## Fix

Added a schema detection path (`_is_normalized_long_format` +
`_build_from_normalized_long`) that recognizes the Phase 12 normalized
schema and maps directly:

| MotherDuck column      | imaging_nodule_master_v1 column |
|------------------------|---------------------------------|
| `tirads_reported`      | `tirads_reported`               |
| `tirads_recalculated`  | `tirads_acr_recalculated`       |
| `composition_norm`     | `composition`                   |
| `echogenicity_norm`    | `echogenicity`                  |
| `shape_norm`           | `shape`                         |
| `margin_norm`          | `margins`                       |
| `calcification_norm`   | `calcifications`                |
| `nodule_length_mm`     | `length_mm`                     |
| `nodule_width_mm`      | `width_mm`                      |
| `nodule_height_mm`     | `height_mm`                     |
| `nodule_location`      | `location_raw` → `laterality`   |

The legacy wide-format unpivot path is preserved for local DuckDB use.

## Results

| Table                          | Before  | After    |
|--------------------------------|---------|----------|
| `imaging_nodule_master_v1`     | 0       | 19,891   |
| `imaging_exam_master_v1`       | 0       | 6,028    |
| `imaging_patient_summary_v1`   | 3,474   | 3,474    |
| `md_imaging_nodule_master_v1`  | (none)  | 19,891   |
| `md_imaging_exam_master_v1`    | (none)  | 6,028    |
| `md_imaging_patient_summary_v1`| (none)  | 3,474    |

### Key metrics for `imaging_nodule_master_v1`

- **19,891** nodule rows across **3,439** patients
- `max_dimension_cm`: 19,891 non-null (avg 1.25 cm, range 0.30–4.33)
- `tirads_category`: 19,891 non-null (TR1=35.5%, TR2=21.9%, TR3=22.7%, TR4=16.9%, TR5=2.9%)
- `suspicious_flag`: 3,945 true (19.8%)
- `composition`: 19,891 non-null
- `echogenicity`: 19,891 non-null
- `tirads_concordant_flag`: 19,572 concordant (98.4%)

### TIRADS category distribution

| Category | Count  | %     |
|----------|--------|-------|
| TR1      | 7,063  | 35.5  |
| TR2      | 4,361  | 21.9  |
| TR3      | 4,522  | 22.7  |
| TR4      | 3,366  | 16.9  |
| TR5      | 579    | 2.9   |

## imaging_nodule_long_v2 Status

`imaging_nodule_long_v2` (10,866 rows / 6,123 patients) retains **all-NULL**
imaging features. This table was built by script 22's NLP extraction from
`clinical_notes_long`, which contains no structured ultrasound radiology
data. Backfilling from `imaging_nodule_master_v1` is **not safe** due to:

1. **Schema mismatch**: v2 `laterality` is DOUBLE (bug), v1 is VARCHAR
2. **Grain mismatch**: v2 is one-row-per-exam-impression, v1 is one-row-per-nodule
3. **Identity mismatch**: no shared nodule/exam IDs between tables

**Recommendation**: Use `imaging_nodule_master_v1` as the canonical imaging
source for all analyses. `imaging_nodule_long_v2` should be treated as a
placeholder pending a v3 rebuild that ingests from the Excel source directly.

## Verdict

**PASS** — The imaging layer is no longer "0 rows / all null."
