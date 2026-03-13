# Imaging Layer — Architecture & Migration Plan

Generated: 2026-03-13

## Current State

### `imaging_nodule_long_v2` (DEPRECATED)

- Created by `scripts/22_canonical_episodes_v2.py`
- 10,866 rows on MotherDuck
- **All data columns NULL** — tirads, composition, echogenicity, shape, margins,
  calcifications, size, location are entirely unpopulated
- Functions as a schema stub only; no analytic value
- Historically populated from `serial_imaging_us.dominant_nodule_size_on_us` which
  is itself entirely NULL (0/18,753 rows)

### `imaging_nodule_master_v1` (CANONICAL)

- Created by `scripts/50_multinodule_imaging.py`
- 19,891 rows on MotherDuck
- Source: `raw_us_tirads_excel_v1` (from `COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx`)
- Unpivots up to 14 nodule groups per exam into long-format rows
- Columns populated: `tirads_reported` (98.4%), `max_dimension_cm` (100%),
  `suspicious_flag` (100%), plus ACR criteria, laterality, concordance flags
- This is the authoritative source for ultrasound imaging data

### Supporting Tables

| Table | Rows | Source |
|-------|------|--------|
| `imaging_exam_master_v1` | per-exam aggregation | script 50 |
| `imaging_patient_summary_v1` | per-patient summary | script 50 |
| `extracted_tirads_validated_v1` | 3,474 patients | Phase 12 engine |
| `raw_us_tirads_excel_v1` | 19,891 | Excel ingestion |
| `raw_us_tirads_scored_v1` | 19,549 | Scored Excel sheets |

## Migration Status

Dashboard modules updated (2026-03-13):
- `app/imaging_nodule_dashboard.py` — prefers `imaging_nodule_master_v1`
- `app/extraction_completeness.py` — domain table switched
- `app/patient_timeline_explorer.py` — fallback chain: master_v1 -> long_v2
- `app/diagnostics.py` — reference updated

`imaging_nodule_long_v2` retained in:
- `scripts/22_canonical_episodes_v2.py` (creation)
- `scripts/23_cross_domain_linkage_v2.py` (linkage UPDATE target)
- `scripts/26_motherduck_materialize_v2.py` (materialization)
- `scripts/29_validation_engine.py` (validation checks)
- `scripts/49_enhanced_linkage_v3.py` (fallback UNION)

These upstream scripts retain the v2 reference because they also create/maintain
the table; removing the reference would break the pipeline. The v2 table
continues to be materialized as a historical artifact.

## Future v3 Design

A unified `imaging_nodule_v3` table would:

1. Replace both `imaging_nodule_long_v2` and `imaging_nodule_master_v1`
2. Source from `raw_us_tirads_excel_v1` (structured) + NLP extraction (from US reports)
3. Include proper FNA linkage via temporal + laterality matching
4. Add CT/MRI/PET imaging modalities (currently only US)
5. Track imaging-pathology concordance natively

### Blocking Requirements

- CT/MRI imaging data exists only in clinical note free-text; no structured source
- PET imaging similarly unstructured
- FNA linkage requires `imaging_fna_linkage_v3` to be non-empty (currently 0 rows)

### Recommended Approach

1. Keep `imaging_nodule_master_v1` as canonical for current analyses
2. Build `imaging_nodule_v3` only when CT/MRI structured data becomes available
3. Deprecate `imaging_nodule_long_v2` in pipeline scripts after v3 is validated
