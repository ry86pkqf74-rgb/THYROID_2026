# mig_180b NLP UPSTREAM-MISSING lineage investigation

Generated: 2026-04-30T00:17:07.317583+00:00

## Executive summary

- Scope: 12 family-level `CF-mig180-NLP-UPSTREAM-MISSING-*` carry-forwards / 38 `canonical_patient_master` columns from mig_180.
- Source lineage found for **12 / 12** families; closure notes applied to **38 / 38** registry rows.
- Exact derivation-vs-canonical replay: **5** families; source-located but stricter retired PM subset: **7** families.
- No `canonical_patient_master` values were mutated; CPM invariants remained 10,871 rows / 10,871 distinct `research_id`.
- Validation table: `main.val_mig180b_nlp_upstream_lineage_v1` (12 rows).
- Pre-snapshot: `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig180b_20260429` (38 rows).

## Family lineage audit

| family               |   n_cols | source_catalog       | source_schema             | source_table                                                         | source_kind   |   source_rows |   source_patients |   metrics_tested |   mismatch_total | exact_replay_pass   | closure_decision                     |
|:---------------------|---------:|:---------------------|:--------------------------|:---------------------------------------------------------------------|:--------------|--------------:|------------------:|-----------------:|-----------------:|:--------------------|:-------------------------------------|
| nlp_funcoutcome      |        4 | Thyroid 2026 UPdated | archive_pub_v1_0          | note_entities_llm_functional_outcomes_pre251_20260417T012311Z        | rawjson       |         11037 |              5641 |                3 |              956 | False               | closed_source_found_pm_strict_subset |
| nlp_imaging          |        4 | Thyroid 2026 UPdated | archive_pub_v1_0          | note_entities_llm_imaging_pre251_20260417T012311Z                    | rawjson       |         11037 |              5641 |                3 |              390 | False               | closed_source_found_pm_strict_subset |
| nlp_labs             |        4 | Thyroid 2026 UPdated | archive_pub_v1_0          | note_entities_llm_labs_pre251_20260417T012311Z                       | rawjson       |         11037 |              5641 |                3 |              188 | False               | closed_source_found_pm_strict_subset |
| nlp_ne_complications |        2 | Thyroid 2026 UPdated | archive_pub_v1_0          | note_entities_complications_pre364_20260422_050902                   | entities      |          9359 |              2840 |                2 |                0 | True                | closed_exact_replay                  |
| nlp_ne_genetics      |        2 | Thyroid 2026 UPdated | molecular_legacy_20260421 | note_entities_genetics                                               | entities      |          1738 |               605 |                2 |                0 | True                | closed_exact_replay                  |
| nlp_ne_medications   |        2 | Thyroid 2026 UPdated | archive_pub_v1_0          | note_entities_medications_pre365b_20260422_122116                    | entities      |          7501 |              2070 |                2 |                0 | True                | closed_exact_replay                  |
| nlp_ne_problemlist   |        2 | Thyroid 2026 UPdated | archive_pub_v1_0          | note_entities_problem_list_pre365b_20260422_122116                   | entities      |         11579 |              4037 |                2 |                0 | True                | closed_exact_replay                  |
| nlp_ne_staging       |        2 | Thyroid 2026 UPdated | main                      | note_entities_staging_archived_20260422                              | entities      |          3807 |              1639 |                2 |                0 | True                | closed_exact_replay                  |
| nlp_physexam         |        4 | Thyroid 2026 UPdated | archive_pub_v1_0          | note_entities_llm_physical_exam_pre251_20260417T012311Z              | rawjson       |         11037 |              5641 |                3 |              688 | False               | closed_source_found_pm_strict_subset |
| nlp_ptdecision       |        4 | Thyroid 2026 UPdated | archive_pub_v1_0          | note_entities_llm_patient_decision_adherence_pre251_20260417T012311Z | rawjson       |         11037 |              5641 |                3 |              117 | False               | closed_source_found_pm_strict_subset |
| nlp_radtx            |        4 | Thyroid 2026 UPdated | archive_pub_v1_0          | note_entities_llm_rad_treatment_pre251_20260417T012311Z              | rawjson       |         11037 |              5641 |                3 |               17 | False               | closed_source_found_pm_strict_subset |
| nlp_usnodule         |        4 | Thyroid 2026 UPdated | archive_pub_v1_0          | note_entities_llm_us_nodule_dynamics_pre251_20260417T012311Z         | rawjson       |         11037 |              5641 |                3 |                3 | False               | closed_source_found_pm_strict_subset |

## Column-level replay audit

| family               | column_name                   | metric      | replay_kind                         |   pm_non_null |   source_non_null |   pm_sum_or_true |   source_sum_or_true |   n_mismatches | closure_decision                                     |
|:---------------------|:------------------------------|:------------|:------------------------------------|--------------:|------------------:|-----------------:|---------------------:|---------------:|:-----------------------------------------------------|
| nlp_funcoutcome      | nlp_funcoutcome_has_data      | has_data    | source_located_non_exact_raw_replay |          1623 |              1842 |             1623 |                 1842 |            219 | closed_source_found_pm_strict_subset                 |
| nlp_funcoutcome      | nlp_funcoutcome_key_finding   | key_finding | lineage_only_text_metric            |          1534 |               nan |              nan |                  nan |            nan | closed_lineage_source_found_text_metric_not_replayed |
| nlp_funcoutcome      | nlp_funcoutcome_n_entities    | n_entities  | source_located_non_exact_raw_replay |          1623 |              1842 |             2570 |                 3322 |            492 | closed_source_found_pm_strict_subset                 |
| nlp_funcoutcome      | nlp_funcoutcome_n_notes       | n_notes     | source_located_non_exact_raw_replay |          1623 |              1842 |             1882 |                 2130 |            245 | closed_source_found_pm_strict_subset                 |
| nlp_imaging          | nlp_imaging_has_data          | has_data    | source_located_non_exact_raw_replay |          1728 |              1759 |             1728 |                 1759 |             31 | closed_source_found_pm_strict_subset                 |
| nlp_imaging          | nlp_imaging_key_finding       | key_finding | lineage_only_text_metric            |          1727 |               nan |              nan |                  nan |            nan | closed_lineage_source_found_text_metric_not_replayed |
| nlp_imaging          | nlp_imaging_n_entities        | n_entities  | source_located_non_exact_raw_replay |          1728 |              1759 |             7942 |                 8428 |            328 | closed_source_found_pm_strict_subset                 |
| nlp_imaging          | nlp_imaging_n_notes           | n_notes     | source_located_non_exact_raw_replay |          1728 |              1759 |             1848 |                 1879 |             31 | closed_source_found_pm_strict_subset                 |
| nlp_labs             | nlp_labs_has_data             | has_data    | source_located_non_exact_raw_replay |           791 |               841 |              791 |                  841 |             50 | closed_source_found_pm_strict_subset                 |
| nlp_labs             | nlp_labs_key_finding          | key_finding | lineage_only_text_metric            |           784 |               nan |              nan |                  nan |            nan | closed_lineage_source_found_text_metric_not_replayed |
| nlp_labs             | nlp_labs_n_entities           | n_entities  | source_located_non_exact_raw_replay |           791 |               841 |             2168 |                 2462 |             86 | closed_source_found_pm_strict_subset                 |
| nlp_labs             | nlp_labs_n_notes              | n_notes     | source_located_non_exact_raw_replay |           791 |               841 |              795 |                  847 |             52 | closed_source_found_pm_strict_subset                 |
| nlp_ne_complications | nlp_ne_complications_has_data | has_data    | exact_derivation_vs_canonical       |          2840 |              2840 |             2840 |                 2840 |              0 | closed_exact_replay                                  |
| nlp_ne_complications | nlp_ne_complications_n_rows   | n_rows      | exact_derivation_vs_canonical       |          2840 |              2840 |             9359 |                 9359 |              0 | closed_exact_replay                                  |
| nlp_ne_genetics      | nlp_ne_genetics_has_data      | has_data    | exact_derivation_vs_canonical       |           605 |               605 |              605 |                  605 |              0 | closed_exact_replay                                  |
| nlp_ne_genetics      | nlp_ne_genetics_n_rows        | n_rows      | exact_derivation_vs_canonical       |           605 |               605 |             1738 |                 1738 |              0 | closed_exact_replay                                  |
| nlp_ne_medications   | nlp_ne_medications_has_data   | has_data    | exact_derivation_vs_canonical       |          2070 |              2070 |             2070 |                 2070 |              0 | closed_exact_replay                                  |
| nlp_ne_medications   | nlp_ne_medications_n_rows     | n_rows      | exact_derivation_vs_canonical       |          2070 |              2070 |             7501 |                 7501 |              0 | closed_exact_replay                                  |
| nlp_ne_problemlist   | nlp_ne_problemlist_has_data   | has_data    | exact_derivation_vs_canonical       |          4036 |              4036 |             4036 |                 4036 |              0 | closed_exact_replay                                  |
| nlp_ne_problemlist   | nlp_ne_problemlist_n_rows     | n_rows      | exact_derivation_vs_canonical       |          4036 |              4036 |            11577 |                11577 |              0 | closed_exact_replay                                  |
| nlp_ne_staging       | nlp_ne_staging_has_data       | has_data    | exact_derivation_vs_canonical       |          1639 |              1639 |             1639 |                 1639 |              0 | closed_exact_replay                                  |
| nlp_ne_staging       | nlp_ne_staging_n_rows         | n_rows      | exact_derivation_vs_canonical       |          1639 |              1639 |             3807 |                 3807 |              0 | closed_exact_replay                                  |
| nlp_physexam         | nlp_physexam_has_data         | has_data    | source_located_non_exact_raw_replay |           512 |               662 |              512 |                  662 |            150 | closed_source_found_pm_strict_subset                 |
| nlp_physexam         | nlp_physexam_key_finding      | key_finding | lineage_only_text_metric            |           505 |               nan |              nan |                  nan |            nan | closed_lineage_source_found_text_metric_not_replayed |
| nlp_physexam         | nlp_physexam_n_entities       | n_entities  | source_located_non_exact_raw_replay |           512 |               662 |             1160 |                 2025 |            372 | closed_source_found_pm_strict_subset                 |
| nlp_physexam         | nlp_physexam_n_notes          | n_notes     | source_located_non_exact_raw_replay |           512 |               662 |              530 |                  702 |            166 | closed_source_found_pm_strict_subset                 |
| nlp_ptdecision       | nlp_ptdecision_has_data       | has_data    | source_located_non_exact_raw_replay |           367 |               398 |              367 |                  398 |             31 | closed_source_found_pm_strict_subset                 |
| nlp_ptdecision       | nlp_ptdecision_key_finding    | key_finding | lineage_only_text_metric            |           366 |               nan |              nan |                  nan |            nan | closed_lineage_source_found_text_metric_not_replayed |
| nlp_ptdecision       | nlp_ptdecision_n_entities     | n_entities  | source_located_non_exact_raw_replay |           367 |               398 |              583 |                  641 |             53 | closed_source_found_pm_strict_subset                 |
| nlp_ptdecision       | nlp_ptdecision_n_notes        | n_notes     | source_located_non_exact_raw_replay |           367 |               398 |              369 |                  402 |             33 | closed_source_found_pm_strict_subset                 |
| nlp_radtx            | nlp_radtx_has_data            | has_data    | source_located_non_exact_raw_replay |           210 |               213 |              210 |                  213 |              3 | closed_source_found_pm_strict_subset                 |
| nlp_radtx            | nlp_radtx_key_finding         | key_finding | lineage_only_text_metric            |           210 |               nan |              nan |                  nan |            nan | closed_lineage_source_found_text_metric_not_replayed |
| nlp_radtx            | nlp_radtx_n_entities          | n_entities  | source_located_non_exact_raw_replay |           210 |               213 |              568 |                  580 |             11 | closed_source_found_pm_strict_subset                 |
| nlp_radtx            | nlp_radtx_n_notes             | n_notes     | source_located_non_exact_raw_replay |           210 |               213 |              212 |                  215 |              3 | closed_source_found_pm_strict_subset                 |
| nlp_usnodule         | nlp_usnodule_has_data         | has_data    | source_located_non_exact_raw_replay |            18 |                19 |               18 |                   19 |              1 | closed_source_found_pm_strict_subset                 |
| nlp_usnodule         | nlp_usnodule_key_finding      | key_finding | lineage_only_text_metric            |            18 |               nan |              nan |                  nan |            nan | closed_lineage_source_found_text_metric_not_replayed |
| nlp_usnodule         | nlp_usnodule_n_entities       | n_entities  | source_located_non_exact_raw_replay |            18 |                19 |               48 |                   49 |              1 | closed_source_found_pm_strict_subset                 |
| nlp_usnodule         | nlp_usnodule_n_notes          | n_notes     | source_located_non_exact_raw_replay |            18 |                19 |               18 |                   19 |              1 | closed_source_found_pm_strict_subset                 |

## Interpretation

The original mig_180 audit searched only live canonical `main` tables, so archived / legacy Tier-2 NLP sources were reported as upstream-missing. mig_180b widens lineage discovery to the governed archive/legacy schemas without writing to those read-only reference databases.

The five generic `note_entities_*` families replay exactly from their archived/legacy rows. The seven raw JSON LLM families have source lineage present, but the raw entity count is a superset of the stricter retired CPM rollup. Those carry-forwards are closed as `source_found_pm_strict_subset`; exact reproduction would require the retired family-specific filter code, and no CPM value rewrite is warranted in this lane.

`source_patients` / `source_rows` are whole-source counts. Replay comparisons are deliberately scoped to the 10,871-row CPM spine, so non-CPM archived rows are lineage context rather than mismatches.

## Execution summary

|   before_affected_cols |   before_closed_cols |   after_affected_cols |   after_closed_cols |   cpm_rows |   cpm_distinct_research_id |   val_rows |   snapshot_rows |
|-----------------------:|---------------------:|----------------------:|--------------------:|-----------:|---------------------------:|-----------:|----------------:|
|                     38 |                    0 |                    38 |                  38 |      10871 |                      10871 |         12 |              38 |
