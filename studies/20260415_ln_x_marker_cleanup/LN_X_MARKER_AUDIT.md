# LN 'x' Marker Cross-Verification Audit
**Date:** 2026-04-15
**Database:** MotherDuck "Thyroid 2026".main

## Background
The all-diagnosis pathology spreadsheet uses 'x' as a checkbox-style marker meaning 
"present/involved" in the `tumor_1_ln_involved` column of `path_synoptics`. 

Cross-verification across 4 independent sources was performed to determine which 'x' markers 
are supported by independent evidence:

| Source | Type | Patients covered |
|--------|------|-----------------|
| tumor_pathology | Cleaned Excel (249 cols) | 3,986 |
| path_synoptics | All-diagnosis sheet | 10,871 |
| ln_master_rollup_v1 | Derived rollup | 3,986 |
| note_entities_llm_cervical_ln_detail | Clinical note LLM extraction | 5,641 |

## Results

### 'x' markers with independent confirmation (n=1,655)
For patients with both path_synoptics 'x' AND tumor_pathology data:
- **1,655 / 1,709 (96.8%)** confirmed positive by tumor_pathology
- These patients were already corrected in the patient master via tumor_pathology_corrected source

### 'x' markers contradicted by tumor_pathology (n=36+1)
- 36 patients: path_synoptics 'x' but tumor_pathology says 0 positive
  - These were overwritten by tumor_pathology (the more granular source) during the LN correction
- 1 patient (research_id=5187): Explicitly confirmed as data entry error
  - path_synoptics: ln_involved='1' (1/4 LN positive) — discordant with tumor_pathology
  - tumor_pathology: primary_ln_ln_total_positive=0
  - ln_master_rollup: ln_crossval_status updated to 'confirmed_data_entry_error'
  - master_clinical_v12: ln_total_positive=0 (source=tumor_pathology_corrected) — correct
  - Flagged in ln_x_marker_audit_v1 as 'confirmed_fumble'

### 'x' markers with NO independent source (n=52)
- 52 patients exist only in path_synoptics (no tumor_pathology row)
- No clinical note LN extraction confirms positivity
- No other structured source available
- These remain as ln_total_positive=0/NULL in the patient master
- Flagged in ln_x_marker_audit_v1 as 'unconfirmed_x_only'
- **Disposition: NOT treated as positive — insufficient evidence**

Research IDs (unconfirmed_x_only, n=52):
529, 1167, 2015, 3575, 4017, 5209, 5465, 5466, 5515, 5589, 5746, 5768, 6189, 6212, 6216,
6382, 6421, 6433, 6459, 6490, 6581, 6657, 6768, 6787, 7113, 7311, 7315, 7402, 7464, 7593,
7596, 7924, 8573, 8755, 8956, 9621, 9754, 9897, 9998, 10079, 10176, 10367, 10542, 10684,
10964, 11283, 11295, 11314, 11337, 11484, 12027, 12031

### Validated positive patients (n=2,643)
All 4 available sources agree on LN positive status.

### Validated negative patients (n=2,463)
All available sources agree on LN negative/zero status.

## Artifacts
- `ln_x_marker_audit_v1` table on MotherDuck (53 rows: 52 unconfirmed_x_only + 1 confirmed_fumble)
- `ln_master_rollup_v1.ln_crossval_status` updated to 'confirmed_data_entry_error' for research_id=5187
- This document

## Cross-verification summary
| Agreement pattern | Patients | Action |
|-------------------|----------|--------|
| All sources agree positive | 2,643 | Confirmed positive ✅ |
| All sources agree zero/negative | 2,463 | Confirmed zero ✅ |
| No LN data available | 5,465 | NULL (no surgery/path) |
| Single source only, path_synoptic numeric | 1,361 | Accepted (has counts) |
| path_synoptics 'x' only, unconfirmed | 52 | Flagged, not changed |
| path_synoptics 'x' contradicted | 1 | Confirmed fumble |
| **Total patients** | **12,886** | |

## Key rule
> No patient's `ln_total_positive` was changed by this audit. The 52 unconfirmed patients 
> remain at 0/NULL. This document records WHY, not a correction.
