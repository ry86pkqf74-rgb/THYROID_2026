# Database Inventory Summary — 2026-04-14

## MotherDuck Environment
- **Database:** `Thyroid 2026` (DUCKLAKE)
- **Release validation:** 40 PASS / 5 WARN / 0 FAIL

## Key Tables/Views Verified

| Object | Type | Rows | Status |
|--------|------|------|--------|
| imaging_nodule_master_v1 | TABLE | 37,016 | OK |
| imaging_nodule_long_v2 | TABLE | 19,891 | OK |
| raw_us_tirads_excel_v1 | TABLE | 19,891 | OK |
| raw_us_tirads_scored_v1 | TABLE | 19,549 | OK |
| raw_imaging_12_slots_v1 | TABLE | 21,079 | OK |
| ultrasound_reports | TABLE | 6,793 | OK |
| serial_imaging_us | TABLE | 0 | Empty placeholder |
| imaging_fna_linkage_mm_v1 | TABLE | 7,305 | OK |
| v_imaging_nodule_linkage_classification_v1 | VIEW | 37,016 | OK |
| imaging_pathology_concordance_review_v2 | — | — | MISSING on MotherDuck |
| fna_history | TABLE | 8,119 | OK |
| fna_cytology | TABLE | 8,063 | OK |
| fna_episode_master_v2 | TABLE | 8,119 | OK |
| v_fna_episode_bethesda_resolved_v1 | VIEW | 8,119 | OK |
| v_fna_bethesda_episode_vs_resolved_v1 | VIEW | 8,119 | OK |
| extracted_tirads_validated_v1 | TABLE | 3,439 | OK |

## Linkage State Distribution

| State | Count |
|-------|-------|
| no_eligible_fna | 30,657 |
| linked_to_fna | 6,359 |

### no_eligible_fna Reason Codes

| Reason | Count |
|--------|-------|
| patient_has_no_dated_fna_episode | 8,395 |
| only_fna_beyond_90d_after_index_us | 8,107 |
| all_fna_before_index_us_exam | 7,709 |
| index_us_after_first_surgery | 6,318 |
| preop_fna_window_patient_no_mm_pair_for_this_nodule | 123 |
| fna_calendar_in_window_but_not_preop | 5 |

**Null reason codes: 0**
