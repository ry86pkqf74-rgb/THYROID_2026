# Imaging Nodule Master Status Report

**Date:** 2026-03-13

---

## Status: FULLY POPULATED AND VERIFIED

The `imaging_nodule_master_v1` table is correctly populated on MotherDuck with comprehensive
data. No repair was needed.

## Verification Results

| Field | Non-NULL Count | Total | Coverage |
|-------|---------------|-------|----------|
| Total rows | 19,891 | 19,891 | 100% |
| tirads_reported | 19,572 | 19,891 | 98.4% |
| max_dimension_cm | 19,891 | 19,891 | 100% |
| composition | 19,891 | 19,891 | 100% |

## Related Tables

| Table | Rows | Status |
|-------|------|--------|
| `imaging_nodule_master_v1` | 19,891 | POPULATED |
| `imaging_exam_master_v1` | 6,028 | POPULATED |
| `imaging_patient_summary_v1` | 3,474 | POPULATED |
| `imaging_fna_linkage_v3` | 9,024 | REBUILT (this session) |

## Source

Created by `scripts/50_multinodule_imaging.py --md` from:
- `raw_us_tirads_excel_v1` (19,891 rows from COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx)
- `extracted_tirads_validated_v1` (3,474 rows, supplement)

## Known Limitation

`imaging_nodule_long_v2` (the V2 canonical table from script 22) has 10,866 rows but ALL
size/TIRADS columns are NULL. This is because script 22 builds the table from structured
imaging data which was never populated with US measurements. The separately built
`imaging_nodule_master_v1` (from the Excel source via script 50) is the correct canonical
source for imaging data.

Dashboard modules correctly fall back from `imaging_nodule_master_v1` to
`imaging_nodule_long_v2` using `tbl_exists()` + `_resolve_view()` pattern.
