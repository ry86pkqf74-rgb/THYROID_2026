# mig_186 NIFTP + uncertain-malignancy exclusion scoping

**Date:** 2026-04-30  
**Run ID:** `mig186_niftp_scoping_20260430`  
**Posture:** READ-ONLY scoping; no MotherDuck DDL/DML executed.  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Prompt:** `cursor_prompts/CURSOR_PROMPT_mig186_niftp_uncertain_exclusion_20260430.md`

## Executive summary

- Affected rows in `main.canonical_path_malignant_events_v1`: **220 events / 202 combined patients**.
- NIFTP-classified affected rows: **213 events / 195 patients**.
- Uncertain-malignant-potential affected rows: **7 events / 7 patients**.
- Recommended disposition remains **R-D hybrid**: snapshot affected rows, move/copy them to an indeterminate provenance table if desired, then exclude them from `canonical_path_malignant_events_v1` after Logan ratifies the rule.
- This run only authored a placeholder apply SQL; it did **not** execute any mutation against MotherDuck.

## 1. Full histology inventory

| primary_histology                                                                                            | histology_variant   | affected_reason               |   n_events |   n_pts |   n_with_t_stage |   n_with_n_stage |
|:-------------------------------------------------------------------------------------------------------------|:--------------------|:------------------------------|-----------:|--------:|-----------------:|-----------------:|
| NIFTP                                                                                                        | nan                 | niftp_who2017_non_malignant   |        154 |     142 |              152 |              113 |
| NIFTP                                                                                                        | follicular          | niftp_who2017_non_malignant   |         23 |      20 |               23 |               23 |
| NIFTP                                                                                                        | microcarcinoma      | niftp_who2017_non_malignant   |         19 |      16 |               19 |               19 |
| NIFTP                                                                                                        | classical           | niftp_who2017_non_malignant   |         11 |      11 |               11 |               11 |
| NIFTP                                                                                                        | minimally invasive  | niftp_who2017_non_malignant   |          3 |       3 |                3 |                3 |
| NIFTP                                                                                                        | oncocytic/hurthle   | niftp_who2017_non_malignant   |          3 |       3 |                3 |                3 |
| Atypical hurthle cell neoplasm                                                                               | oncocytic/hurthle   | uncertain_malignant_potential |          1 |       1 |                1 |                1 |
| Follicular tumor of uncertain malignant potential with focal high-grade features                             | nan                 | uncertain_malignant_potential |          1 |       1 |                1 |                1 |
| HÜRTHLE cell neoplasm of uncertain malignant potential                                                       | microcarcinoma      | uncertain_malignant_potential |          1 |       1 |                1 |                1 |
| Low-risk atypical follicular neoplasm (follicular thyroid neoplasm of uncertain malignant potential, FT-UMP) | nan                 | uncertain_malignant_potential |          1 |       1 |                1 |                1 |
| Oncocytic follicular tumor of uncertain malignant potential                                                  | nan                 | uncertain_malignant_potential |          1 |       1 |                1 |                1 |
| Oncocytic lesion of uncertain malignant potential                                                            | nan                 | uncertain_malignant_potential |          1 |       1 |                1 |                1 |
| Oncocytic tumor of uncertain malignant potential (atypical oncocytic tumor)                                  | nan                 | uncertain_malignant_potential |          1 |       1 |                1 |                1 |

## 2. Cross-table cascade analysis

| table_or_domain                            | metric                                                   | value   | note                                                    |
|:-------------------------------------------|:---------------------------------------------------------|:--------|:--------------------------------------------------------|
| canonical_path_malignant_events_v1         | affected_events                                          | 220     | Rows matching NIFTP/uncertain text filter               |
| canonical_path_malignant_events_v1         | affected_patients                                        | 202     | Distinct affected research_id values                    |
| canonical_path_malignant_patient_rollup_v1 | rows_for_affected_patients                               | 202     | Patient-level join to affected rids                     |
| canonical_path_malignant_patient_rollup_v1 | affected_patients_present                                | 202     | Distinct affected rids present                          |
| canonical_invasion_events_v1               | rows_for_affected_patients                               | 1845    | Patient-level join to affected rids                     |
| canonical_invasion_events_v1               | affected_patients_present                                | 202     | Distinct affected rids present                          |
| canonical_us_lymph_node_patient_rollup_v2  | rows_for_affected_patients                               | 202     | Patient-level join to affected rids                     |
| canonical_us_lymph_node_patient_rollup_v2  | affected_patients_present                                | 202     | Distinct affected rids present                          |
| canonical_patient_master                   | rows_for_affected_patients                               | 202     | Patient-level join to affected rids                     |
| canonical_patient_master                   | affected_patients_present                                | 202     | Distinct affected rids present                          |
| canonical_tumor_characteristics_v1         | table_presence                                           | absent  | Not present in main schema of target DB at scoping time |
| tumor_episode_master_v2                    | table_presence                                           | absent  | Not present in main schema of target DB at scoping time |
| canonical_patient_master                   | affected_patients_with_niftp_in_histologic_types_all     | 195     | CPM text field contains NIFTP                           |
| canonical_patient_master                   | affected_patients_with_uncertain_in_histologic_types_all | 6       | CPM text field contains uncertain                       |
| cohort_impact                              | edge_no_other_path_event_but_pm_malignant                | 115     | Patient-disposition bucket                              |
| cohort_impact                              | mixed_keep_patient_exclude_affected_event_rows           | 87      | Patient-disposition bucket                              |

## 3. Cohort impact: NIFTP-only vs mixed

| proposed_patient_disposition                   |   n_pts |   n_events |
|:-----------------------------------------------|--------:|-----------:|
| edge_no_other_path_event_but_pm_malignant      |     115 |        122 |
| mixed_keep_patient_exclude_affected_event_rows |      87 |         98 |

Interpretation:

- `mixed_keep_patient_exclude_affected_event_rows`: patient has at least one additional path-malignant event outside the NIFTP/uncertain rows; exclude the affected event row(s), but keep the patient in malignant-cohort analyses if the other malignant event remains valid.
- `edge_no_other_path_event_but_pm_malignant`: no other event in `canonical_path_malignant_events_v1`, but `canonical_patient_master.is_malignant` is true; requires rule review before patient-level cohort exclusion.
- `niftp_uncertain_only_candidate_patient_exclusion`: no other path-malignant event and CPM does not mark malignant; likely patient-level exclusion candidate if Logan ratifies WHO-2017 NIFTP/uncertain exclusion.

## 4. Disposition rule comparison

| Rule | Approach | Pros | Cons | Recommendation |
|---|---|---|---|---|
| R-A | Delete affected rows from `canonical_path_malignant_events_v1` | Clean manuscript malignant event table | Requires external audit trail | Accept only with pre-snapshot |
| R-B | Move rows to `canonical_path_indeterminate_events_v1` | Preserves queryable provenance | Adds new canonical surface | Good if downstream consumers need indeterminate events |
| R-C | Add `is_malignant_per_who_2017` flag | Non-destructive | Every consumer must remember filter | Too easy to misuse |
| R-D | Archive + optional indeterminate table + delete from malignant events | Clean malignant semantics and preserved provenance | Requires Logan ratification + downstream rebuild checklist | **Recommended** |

## 5. Manuscript implications

- NIFTP is non-malignant under WHO 2017 terminology; retaining these rows in a malignant event table can inflate malignant tumor/event counts.
- Mixed patients should remain analyzable through their non-NIFTP malignant event(s), but the NIFTP/uncertain event rows should be excluded from malignant-event denominators.
- NIFTP-only or uncertain-only patients should be excluded from malignant-cohort denominators unless a separate ratified malignant criterion exists outside the affected row.
- Any apply must open/track `CF-mig186-WHO-2017-NIFTP-RECLASS` and refresh dependent rollups after deletion.

## 6. Logan spot-check sample

Full inventory CSV: `exports/mig186_niftp_scoping_20260430/niftp_uncertain_inventory.csv`

|   research_id | surgery_episode_id   |   tumor_ordinal | primary_histology                                                                                            |   histology_variant | affected_reason               | proposed_patient_disposition                   |
|--------------:|:---------------------|----------------:|:-------------------------------------------------------------------------------------------------------------|--------------------:|:------------------------------|:-----------------------------------------------|
|         10015 | <NA>                 |               1 | NIFTP                                                                                                        |                 nan | niftp_who2017_non_malignant   | edge_no_other_path_event_but_pm_malignant      |
|         10079 | <NA>                 |               1 | NIFTP                                                                                                        |                 nan | niftp_who2017_non_malignant   | edge_no_other_path_event_but_pm_malignant      |
|         10153 | <NA>                 |               4 | NIFTP                                                                                                        |                 nan | niftp_who2017_non_malignant   | mixed_keep_patient_exclude_affected_event_rows |
|         10176 | <NA>                 |               1 | NIFTP                                                                                                        |                 nan | niftp_who2017_non_malignant   | edge_no_other_path_event_but_pm_malignant      |
|         10282 | <NA>                 |               1 | NIFTP                                                                                                        |                 nan | niftp_who2017_non_malignant   | edge_no_other_path_event_but_pm_malignant      |
|         10558 | <NA>                 |               2 | Low-risk atypical follicular neoplasm (follicular thyroid neoplasm of uncertain malignant potential, FT-UMP) |                 nan | uncertain_malignant_potential | mixed_keep_patient_exclude_affected_event_rows |
|         10986 | <NA>                 |               2 | Oncocytic follicular tumor of uncertain malignant potential                                                  |                 nan | uncertain_malignant_potential | mixed_keep_patient_exclude_affected_event_rows |
|         11191 | <NA>                 |               2 | Follicular tumor of uncertain malignant potential with focal high-grade features                             |                 nan | uncertain_malignant_potential | mixed_keep_patient_exclude_affected_event_rows |
|         11255 | <NA>                 |               3 | Oncocytic tumor of uncertain malignant potential (atypical oncocytic tumor)                                  |                 nan | uncertain_malignant_potential | mixed_keep_patient_exclude_affected_event_rows |
|         11500 | <NA>                 |               3 | Oncocytic lesion of uncertain malignant potential                                                            |                 nan | uncertain_malignant_potential | mixed_keep_patient_exclude_affected_event_rows |

## Deliverables written

- `qc_framework_v1/migrations/186_niftp_uncertain_exclusion_TBD_20260430.sql`
- `qc_framework_v1/reports/mig_186_niftp_uncertain_exclusion_scoping_20260430.md`
- `exports/mig186_niftp_scoping_20260430/niftp_uncertain_inventory.csv`
- `exports/mig186_niftp_scoping_20260430/niftp_uncertain_patient_disposition.csv`
- `exports/mig186_niftp_scoping_20260430/manifest.json`
