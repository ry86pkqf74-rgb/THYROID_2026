# mig_180 PM nlp_* Cluster Audit

Generated: 2026-04-29T23:34:16.608473+00:00

## Executive summary

- Scoped registry rows: **116** `canonical_patient_master.nlp_*` columns with `verification_status='not_started'` and `batch_id IS NULL`.
- Proposed verified: **115**.
- Proposed NA: **1**.
- No writes were executed against MotherDuck by this script; the migration SQL is authored for governed Path-C apply.

## Family summary

| family               |   n_cols |   n_verified |   n_na | source_status     | source_table                            |   source_rows |   source_patients |
|:---------------------|---------:|-------------:|-------:|:------------------|:----------------------------------------|--------------:|------------------:|
| nlp_airway           |        4 |            4 |      0 | live_source_found | note_entities_llm_airway_invasion_v2    |          6054 |              2820 |
| nlp_cervln           |        4 |            4 |      0 | live_source_found | note_entities_llm_cervical_ln_detail    |         10084 |              5106 |
| nlp_dynrisk          |        4 |            4 |      0 | live_source_found | note_entities_llm_dynamic_risk_response |         11037 |              5641 |
| nlp_esoph            |        4 |            4 |      0 | live_source_found | note_entities_llm_esophageal_invasion   |          4409 |              4170 |
| nlp_frozensec        |        4 |            4 |      0 | live_source_found | note_entities_llm_frozen_section_detail |         32408 |             10863 |
| nlp_funcoutcome      |        4 |            4 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_imaging          |        4 |            4 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_labs             |        4 |            4 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_ln               |        5 |            5 |      0 | live_source_found | canonical_us_lymph_node_v2              |          6801 |              4077 |
| nlp_ne_complications |        2 |            2 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_ne_genetics      |        2 |            2 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_ne_medications   |        2 |            2 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_ne_operative     |        2 |            2 |      0 | live_source_found | note_entities_operative_detail          |         12151 |              4032 |
| nlp_ne_problemlist   |        2 |            2 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_ne_staging       |        2 |            2 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_parathyroid      |        4 |            4 |      0 | live_source_found | note_entities_llm_parathyroid_detail_v1 |          8697 |              4443 |
| nlp_path             |       10 |           10 |      0 | live_source_found | note_entities_llm_pathology             |         10084 |              5106 |
| nlp_physexam         |        4 |            4 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_pmhx             |        4 |            4 |      0 | live_source_found | note_entities_llm_past_medical_hx       |         11037 |              5641 |
| nlp_pshx             |        4 |            4 |      0 | live_source_found | note_entities_llm_past_surgical_hx      |         11037 |              5641 |
| nlp_ptdecision       |        4 |            4 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_radtx            |        4 |            4 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_rec              |        8 |            8 |      0 | live_source_found | note_entities_llm_recurrence            |         11037 |              5641 |
| nlp_survfu           |        4 |            4 |      0 | live_source_found | canonical_survival_followup_v1          |         10871 |             10871 |
| nlp_symptoms         |        4 |            4 |      0 | live_source_found | note_entities_llm_presenting_symptoms   |         11037 |              5641 |
| nlp_tg               |        4 |            3 |      1 | live_source_found | tg_postop_surveillance_windows_v1       |         16184 |              3250 |
| nlp_tirads           |        5 |            5 |      0 | live_source_found | note_entities_llm_tirads_granular       |         10084 |              5106 |
| nlp_usnodule         |        4 |            4 |      0 | upstream_missing  | <NA>                                    |           nan |               nan |
| nlp_vasc             |        4 |            4 |      0 | live_source_found | note_entities_llm_vascular_invasion_v2  |          3861 |              3745 |

## Upstream lineage rationale

| family               | source_status     | source_table                            |   source_rows |   source_patients | quality_where          | positivity_expr                                          | rationale                                                                                                                                                     |
|:---------------------|:------------------|:----------------------------------------|--------------:|------------------:|:-----------------------|:---------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| nlp_airway           | live_source_found | note_entities_llm_airway_invasion_v2    |          6054 |              2820 | COALESCE(error, 0) = 0 | TRUE                                                     | Resolved to main.note_entities_llm_airway_invasion_v2; row quality filter `COALESCE(error, 0) = 0`; positivity expression `TRUE`.                             |
| nlp_cervln           | live_source_found | note_entities_llm_cervical_ln_detail    |         10084 |              5106 | TRUE                   | TRUE                                                     | Resolved to main.note_entities_llm_cervical_ln_detail; row quality filter `TRUE`; positivity expression `TRUE`.                                               |
| nlp_dynrisk          | live_source_found | note_entities_llm_dynamic_risk_response |         11037 |              5641 | TRUE                   | TRUE                                                     | Resolved to main.note_entities_llm_dynamic_risk_response; row quality filter `TRUE`; positivity expression `TRUE`.                                            |
| nlp_esoph            | live_source_found | note_entities_llm_esophageal_invasion   |          4409 |              4170 | TRUE                   | TRUE                                                     | Resolved to main.note_entities_llm_esophageal_invasion; row quality filter `TRUE`; positivity expression `TRUE`.                                              |
| nlp_frozensec        | live_source_found | note_entities_llm_frozen_section_detail |         32408 |             10863 | TRUE                   | TRUE                                                     | Resolved to main.note_entities_llm_frozen_section_detail; row quality filter `TRUE`; positivity expression `TRUE`.                                            |
| nlp_funcoutcome      | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_llm_functional_outcomes.                                                                           |
| nlp_imaging          | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_llm_imaging.                                                                                       |
| nlp_labs             | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_llm_labs.                                                                                          |
| nlp_ln               | live_source_found | canonical_us_lymph_node_v2              |          6801 |              4077 | TRUE                   | (suspicious_flag IS TRUE)                                | Resolved to main.canonical_us_lymph_node_v2; row quality filter `TRUE`; positivity expression `(suspicious_flag IS TRUE)`.                                    |
| nlp_ne_complications | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_complications.                                                                                     |
| nlp_ne_genetics      | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_genetics.                                                                                          |
| nlp_ne_medications   | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_medications.                                                                                       |
| nlp_ne_operative     | live_source_found | note_entities_operative_detail          |         12151 |              4032 | TRUE                   | (LOWER(CAST(present_or_negated AS VARCHAR)) = 'present') | Resolved to main.note_entities_operative_detail; row quality filter `TRUE`; positivity expression `(LOWER(CAST(present_or_negated AS VARCHAR)) = 'present')`. |
| nlp_ne_problemlist   | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_problem_list.                                                                                      |
| nlp_ne_staging       | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_staging.                                                                                           |
| nlp_parathyroid      | live_source_found | note_entities_llm_parathyroid_detail_v1 |          8697 |              4443 | COALESCE(error, 0) = 0 | TRUE                                                     | Resolved to main.note_entities_llm_parathyroid_detail_v1; row quality filter `COALESCE(error, 0) = 0`; positivity expression `TRUE`.                          |
| nlp_path             | live_source_found | note_entities_llm_pathology             |         10084 |              5106 | TRUE                   | TRUE                                                     | Resolved to main.note_entities_llm_pathology; row quality filter `TRUE`; positivity expression `TRUE`.                                                        |
| nlp_physexam         | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_llm_physical_exam.                                                                                 |
| nlp_pmhx             | live_source_found | note_entities_llm_past_medical_hx       |         11037 |              5641 | TRUE                   | TRUE                                                     | Resolved to main.note_entities_llm_past_medical_hx; row quality filter `TRUE`; positivity expression `TRUE`.                                                  |
| nlp_pshx             | live_source_found | note_entities_llm_past_surgical_hx      |         11037 |              5641 | TRUE                   | TRUE                                                     | Resolved to main.note_entities_llm_past_surgical_hx; row quality filter `TRUE`; positivity expression `TRUE`.                                                 |
| nlp_ptdecision       | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_llm_patient_decision_adherence.                                                                    |
| nlp_radtx            | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_llm_rad_treatment.                                                                                 |
| nlp_rec              | live_source_found | note_entities_llm_recurrence            |         11037 |              5641 | TRUE                   | TRUE                                                     | Resolved to main.note_entities_llm_recurrence; row quality filter `TRUE`; positivity expression `TRUE`.                                                       |
| nlp_survfu           | live_source_found | canonical_survival_followup_v1          |         10871 |             10871 | TRUE                   | TRUE                                                     | Resolved to main.canonical_survival_followup_v1; row quality filter `TRUE`; positivity expression `TRUE`.                                                     |
| nlp_symptoms         | live_source_found | note_entities_llm_presenting_symptoms   |         11037 |              5641 | TRUE                   | TRUE                                                     | Resolved to main.note_entities_llm_presenting_symptoms; row quality filter `TRUE`; positivity expression `TRUE`.                                              |
| nlp_tg               | live_source_found | tg_postop_surveillance_windows_v1       |         16184 |              3250 | TRUE                   | TRUE                                                     | Resolved to main.tg_postop_surveillance_windows_v1; row quality filter `TRUE`; positivity expression `TRUE`.                                                  |
| nlp_tirads           | live_source_found | note_entities_llm_tirads_granular       |         10084 |              5106 | TRUE                   | TRUE                                                     | Resolved to main.note_entities_llm_tirads_granular; row quality filter `TRUE`; positivity expression `TRUE`.                                                  |
| nlp_usnodule         | upstream_missing  | <NA>                                    |           nan |               nan | FALSE                  | <NA>                                                     | No live source table found among candidates: note_entities_llm_us_nodule, canonical_us_nodule_characteristics_v1.                                             |
| nlp_vasc             | live_source_found | note_entities_llm_vascular_invasion_v2  |          3861 |              3745 | COALESCE(error, 0) = 0 | TRUE                                                     | Resolved to main.note_entities_llm_vascular_invasion_v2; row quality filter `COALESCE(error, 0) = 0`; positivity expression `TRUE`.                           |

## Boolean Type-A / Type-B classification

| col_name                        |   n_true |   n_false |   n_null | classification                 | proposed_status   | carry_forward                                                 |
|:--------------------------------|---------:|----------:|---------:|:-------------------------------|:------------------|:--------------------------------------------------------------|
| nlp_tg_rising_mentioned         |        0 |        49 |    10822 | type_b_placeholder_zero_true   | na                | CF-mig180-NLP-PLACEHOLDER-nlp_tg_rising_mentioned             |
| nlp_airway_has_data             |     1634 |      9237 |        0 | mixed_boolean_verified         | verified          |                                                               |
| nlp_cervln_has_data             |     1643 |      9228 |        0 | mixed_boolean_verified         | verified          |                                                               |
| nlp_cervln_positive_mentioned   |      974 |      9897 |        0 | mixed_boolean_verified         | verified          |                                                               |
| nlp_esoph_has_data              |       60 |     10811 |        0 | mixed_boolean_verified         | verified          |                                                               |
| nlp_esoph_positive_mentioned    |       43 |     10828 |        0 | mixed_boolean_verified         | verified          |                                                               |
| nlp_frozensec_has_data          |     2855 |      8016 |        0 | mixed_boolean_verified         | verified          |                                                               |
| nlp_ln_positive_mentioned       |      148 |       720 |    10003 | mixed_boolean_verified         | verified          |                                                               |
| nlp_parathyroid_has_data        |     3585 |      7286 |        0 | mixed_boolean_verified         | verified          |                                                               |
| nlp_path_has_data               |     3382 |      7489 |        0 | mixed_boolean_verified         | verified          |                                                               |
| nlp_path_ln_positive_mentioned  |      963 |      1831 |     8077 | mixed_boolean_verified         | verified          |                                                               |
| nlp_path_margin_mentioned       |     1214 |      1580 |     8077 | mixed_boolean_verified         | verified          |                                                               |
| nlp_path_multifocal_mentioned   |     1059 |      1735 |     8077 | mixed_boolean_verified         | verified          |                                                               |
| nlp_path_positive_mentioned     |     2855 |      8016 |        0 | mixed_boolean_verified         | verified          |                                                               |
| nlp_path_vasc_inv_mentioned     |      569 |      2225 |     8077 | mixed_boolean_verified         | verified          |                                                               |
| nlp_rec_disease_free_mentioned  |       17 |       116 |    10738 | mixed_boolean_verified         | verified          |                                                               |
| nlp_tg_undetectable_mentioned   |        5 |        44 |    10822 | mixed_boolean_verified         | verified          |                                                               |
| nlp_tirads_has_component_detail |     1061 |       653 |     9157 | mixed_boolean_verified         | verified          |                                                               |
| nlp_vasc_has_data               |      776 |     10095 |        0 | mixed_boolean_verified         | verified          |                                                               |
| nlp_vasc_positive_mentioned     |      776 |     10095 |        0 | mixed_boolean_verified         | verified          |                                                               |
| nlp_dynrisk_has_data            |       25 |         0 |    10846 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_dynrisk_has_data          |
| nlp_funcoutcome_has_data        |     1623 |         0 |     9248 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_funcoutcome_has_data      |
| nlp_imaging_has_data            |     1728 |         0 |     9143 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_imaging_has_data          |
| nlp_labs_has_data               |      791 |         0 |    10080 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_labs_has_data             |
| nlp_ln_has_data                 |      868 |         0 |    10003 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ln_has_data               |
| nlp_ne_complications_has_data   |     2840 |         0 |     8031 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_complications_has_data |
| nlp_ne_genetics_has_data        |      605 |         0 |    10266 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_genetics_has_data      |
| nlp_ne_medications_has_data     |     2070 |         0 |     8801 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_medications_has_data   |
| nlp_ne_operative_has_data       |     4031 |         0 |     6840 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_operative_has_data     |
| nlp_ne_problemlist_has_data     |     4036 |         0 |     6835 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_problemlist_has_data   |
| nlp_ne_staging_has_data         |     1639 |         0 |     9232 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_staging_has_data       |
| nlp_physexam_has_data           |      512 |         0 |    10359 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_physexam_has_data         |
| nlp_pmhx_has_data               |      290 |         0 |    10581 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_pmhx_has_data             |
| nlp_pshx_has_data               |     1864 |         0 |     9007 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_pshx_has_data             |
| nlp_ptdecision_has_data         |      367 |         0 |    10504 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ptdecision_has_data       |
| nlp_radtx_has_data              |      210 |         0 |    10661 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_radtx_has_data            |
| nlp_rec_any_mentioned           |      133 |         0 |    10738 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_rec_any_mentioned         |
| nlp_rec_has_data                |      133 |         0 |    10738 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_rec_has_data              |
| nlp_survfu_has_data             |     2911 |         0 |     7960 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_survfu_has_data           |
| nlp_symptoms_has_data           |      116 |         0 |    10755 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_symptoms_has_data         |
| nlp_tg_has_data                 |       49 |         0 |    10822 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_tg_has_data               |
| nlp_tirads_has_data             |     1715 |         0 |     9156 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_tirads_has_data           |
| nlp_usnodule_has_data           |       18 |         0 |    10853 | type_a_presence_flag_true_only | verified          | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_usnodule_has_data         |

## Non-null/value profile

| col_name                           | data_type   |   non_null |   nulls |   distinct_values |
|:-----------------------------------|:------------|-----------:|--------:|------------------:|
| nlp_airway_has_data                | BOOLEAN     |      10871 |       0 |                 2 |
| nlp_airway_key_finding             | VARCHAR     |       1634 |    9237 |              1171 |
| nlp_airway_n_entities              | BIGINT      |      10871 |       0 |                44 |
| nlp_airway_n_notes                 | BIGINT      |      10871 |       0 |                25 |
| nlp_cervln_confidence_tier         | VARCHAR     |       1643 |    9228 |                 1 |
| nlp_cervln_has_data                | BOOLEAN     |      10871 |       0 |                 2 |
| nlp_cervln_n_entities              | INTEGER     |      10871 |       0 |                21 |
| nlp_cervln_positive_mentioned      | BOOLEAN     |      10871 |       0 |                 2 |
| nlp_dynrisk_has_data               | BOOLEAN     |         25 |   10846 |                 1 |
| nlp_dynrisk_key_finding            | VARCHAR     |         25 |   10846 |                14 |
| nlp_dynrisk_n_entities             | BIGINT      |         25 |   10846 |                 5 |
| nlp_dynrisk_n_notes                | BIGINT      |         25 |   10846 |                 1 |
| nlp_esoph_confidence_tier          | VARCHAR     |         60 |   10811 |                 1 |
| nlp_esoph_has_data                 | BOOLEAN     |      10871 |       0 |                 2 |
| nlp_esoph_n_entities               | INTEGER     |      10871 |       0 |                 9 |
| nlp_esoph_positive_mentioned       | BOOLEAN     |      10871 |       0 |                 2 |
| nlp_frozensec_has_data             | BOOLEAN     |      10871 |       0 |                 2 |
| nlp_frozensec_key_finding          | VARCHAR     |       2855 |    8016 |              1762 |
| nlp_frozensec_n_entities           | BIGINT      |      10871 |       0 |                19 |
| nlp_frozensec_n_notes              | BIGINT      |      10871 |       0 |                 9 |
| nlp_funcoutcome_has_data           | BOOLEAN     |       1623 |    9248 |                 1 |
| nlp_funcoutcome_key_finding        | VARCHAR     |       1534 |    9337 |               745 |
| nlp_funcoutcome_n_entities         | BIGINT      |       1623 |    9248 |                10 |
| nlp_funcoutcome_n_notes            | BIGINT      |       1623 |    9248 |                 5 |
| nlp_imaging_has_data               | BOOLEAN     |       1728 |    9143 |                 1 |
| nlp_imaging_key_finding            | VARCHAR     |       1727 |    9144 |              1617 |
| nlp_imaging_n_entities             | BIGINT      |       1728 |    9143 |                18 |
| nlp_imaging_n_notes                | BIGINT      |       1728 |    9143 |                 4 |
| nlp_labs_has_data                  | BOOLEAN     |        791 |   10080 |                 1 |
| nlp_labs_key_finding               | VARCHAR     |        784 |   10087 |               552 |
| nlp_labs_n_entities                | BIGINT      |        791 |   10080 |                12 |
| nlp_labs_n_notes                   | BIGINT      |        791 |   10080 |                 2 |
| nlp_ln_has_data                    | BOOLEAN     |        868 |   10003 |                 1 |
| nlp_ln_levels_mentioned            | VARCHAR     |        823 |   10048 |               669 |
| nlp_ln_n_entities                  | BIGINT      |        868 |   10003 |                33 |
| nlp_ln_n_notes                     | BIGINT      |        868 |   10003 |                 6 |
| nlp_ln_positive_mentioned          | BOOLEAN     |        868 |   10003 |                 2 |
| nlp_ne_complications_has_data      | BOOLEAN     |       2840 |    8031 |                 1 |
| nlp_ne_complications_n_rows        | BIGINT      |       2840 |    8031 |                28 |
| nlp_ne_genetics_has_data           | BOOLEAN     |        605 |   10266 |                 1 |
| nlp_ne_genetics_n_rows             | BIGINT      |        605 |   10266 |                18 |
| nlp_ne_medications_has_data        | BOOLEAN     |       2070 |    8801 |                 1 |
| nlp_ne_medications_n_rows          | BIGINT      |       2070 |    8801 |                32 |
| nlp_ne_operative_has_data          | BOOLEAN     |       4031 |    6840 |                 1 |
| nlp_ne_operative_n_rows            | BIGINT      |       4031 |    6840 |                18 |
| nlp_ne_problemlist_has_data        | BOOLEAN     |       4036 |    6835 |                 1 |
| nlp_ne_problemlist_n_rows          | BIGINT      |       4036 |    6835 |                16 |
| nlp_ne_staging_has_data            | BOOLEAN     |       1639 |    9232 |                 1 |
| nlp_ne_staging_n_rows              | BIGINT      |       1639 |    9232 |                30 |
| nlp_parathyroid_has_data           | BOOLEAN     |      10871 |       0 |                 2 |
| nlp_parathyroid_key_finding        | VARCHAR     |       3585 |    7286 |               386 |
| nlp_parathyroid_n_entities         | BIGINT      |      10871 |       0 |                19 |
| nlp_parathyroid_n_notes            | BIGINT      |      10871 |       0 |                11 |
| nlp_path_confidence_tier           | VARCHAR     |       3382 |    7489 |                 1 |
| nlp_path_has_data                  | BOOLEAN     |      10871 |       0 |                 2 |
| nlp_path_ln_positive_mentioned     | BOOLEAN     |       2794 |    8077 |                 2 |
| nlp_path_margin_mentioned          | BOOLEAN     |       2794 |    8077 |                 2 |
| nlp_path_multifocal_concordance_v2 | VARCHAR     |       2319 |    8552 |                 4 |
| nlp_path_multifocal_mentioned      | BOOLEAN     |       2794 |    8077 |                 2 |
| nlp_path_n_entities                | BIGINT      |      10871 |       0 |                21 |
| nlp_path_n_notes                   | BIGINT      |       2794 |    8077 |                13 |
| nlp_path_positive_mentioned        | BOOLEAN     |      10871 |       0 |                 2 |
| nlp_path_vasc_inv_mentioned        | BOOLEAN     |       2794 |    8077 |                 2 |
| nlp_physexam_has_data              | BOOLEAN     |        512 |   10359 |                 1 |
| nlp_physexam_key_finding           | VARCHAR     |        505 |   10366 |               422 |
| nlp_physexam_n_entities            | BIGINT      |        512 |   10359 |                10 |
| nlp_physexam_n_notes               | BIGINT      |        512 |   10359 |                 2 |
| nlp_pmhx_has_data                  | BOOLEAN     |        290 |   10581 |                 1 |
| nlp_pmhx_key_finding               | VARCHAR     |        290 |   10581 |               172 |
| nlp_pmhx_n_entities                | BIGINT      |        290 |   10581 |                10 |
| nlp_pmhx_n_notes                   | BIGINT      |        290 |   10581 |                 2 |
| nlp_pshx_has_data                  | BOOLEAN     |       1864 |    9007 |                 1 |
| nlp_pshx_key_finding               | VARCHAR     |       1864 |    9007 |              1282 |
| nlp_pshx_n_entities                | BIGINT      |       1864 |    9007 |                11 |
| nlp_pshx_n_notes                   | BIGINT      |       1864 |    9007 |                 2 |
| nlp_ptdecision_has_data            | BOOLEAN     |        367 |   10504 |                 1 |
| nlp_ptdecision_key_finding         | VARCHAR     |        366 |   10505 |               105 |
| nlp_ptdecision_n_entities          | BIGINT      |        367 |   10504 |                 7 |
| nlp_ptdecision_n_notes             | BIGINT      |        367 |   10504 |                 2 |
| nlp_radtx_has_data                 | BOOLEAN     |        210 |   10661 |                 1 |
| nlp_radtx_key_finding              | VARCHAR     |        210 |   10661 |               193 |
| nlp_radtx_n_entities               | BIGINT      |        210 |   10661 |                11 |
| nlp_radtx_n_notes                  | BIGINT      |        210 |   10661 |                 2 |
| nlp_rec_any_mentioned              | BOOLEAN     |        133 |   10738 |                 1 |
| nlp_rec_confidence_tier            | VARCHAR     |        133 |   10738 |                 1 |
| nlp_rec_disease_free_mentioned     | BOOLEAN     |        133 |   10738 |                 2 |
| nlp_rec_earliest_date              | DATE        |         91 |   10780 |                85 |
| nlp_rec_earliest_days_from_surg    | INTEGER     |         55 |   10816 |                51 |
| nlp_rec_has_data                   | BOOLEAN     |        133 |   10738 |                 1 |
| nlp_rec_n_entities                 | BIGINT      |        133 |   10738 |                 9 |
| nlp_rec_type_worst                 | VARCHAR     |        133 |   10738 |                 4 |
| nlp_survfu_has_data                | BOOLEAN     |       2911 |    7960 |                 1 |
| nlp_survfu_key_finding             | VARCHAR     |       2911 |    7960 |              1065 |
| nlp_survfu_n_entities              | BIGINT      |       2911 |    7960 |                12 |
| nlp_survfu_n_notes                 | BIGINT      |       2911 |    7960 |                 4 |
| nlp_symptoms_has_data              | BOOLEAN     |        116 |   10755 |                 1 |
| nlp_symptoms_key_finding           | VARCHAR     |        116 |   10755 |               110 |
| nlp_symptoms_n_entities            | BIGINT      |        116 |   10755 |                 7 |
| nlp_symptoms_n_notes               | BIGINT      |        116 |   10755 |                 1 |
| nlp_tg_has_data                    | BOOLEAN     |         49 |   10822 |                 1 |
| nlp_tg_n_entities                  | BIGINT      |         49 |   10822 |                 9 |
| nlp_tg_rising_mentioned            | BOOLEAN     |         49 |   10822 |                 1 |
| nlp_tg_undetectable_mentioned      | BOOLEAN     |         49 |   10822 |                 2 |
| nlp_tirads_has_component_detail    | BOOLEAN     |       1714 |    9157 |                 2 |
| nlp_tirads_has_data                | BOOLEAN     |       1715 |    9156 |                 1 |
| nlp_tirads_max_category            | VARCHAR     |       1714 |    9157 |               344 |
| nlp_tirads_n_entities              | BIGINT      |       1715 |    9156 |                66 |
| nlp_tirads_n_notes                 | BIGINT      |       1715 |    9156 |                 7 |
| nlp_usnodule_has_data              | BOOLEAN     |         18 |   10853 |                 1 |
| nlp_usnodule_key_finding           | VARCHAR     |         18 |   10853 |                18 |
| nlp_usnodule_n_entities            | BIGINT      |         18 |   10853 |                 7 |
| nlp_usnodule_n_notes               | BIGINT      |         18 |   10853 |                 1 |
| nlp_vasc_confidence_tier           | VARCHAR     |        776 |   10095 |                 1 |
| nlp_vasc_has_data                  | BOOLEAN     |      10871 |       0 |                 2 |
| nlp_vasc_n_entities                | BIGINT      |      10871 |       0 |                 5 |
| nlp_vasc_positive_mentioned        | BOOLEAN     |      10871 |       0 |                 2 |

## Rederivation coverage summary

| rederivation_kind                           |   n_cols |
|:--------------------------------------------|---------:|
| BOOL_OR_positive_expression                 |       13 |
| COUNT_DISTINCT_note_index                   |        9 |
| COUNT_rows_no_note_id_available             |        2 |
| COUNT_valid_source_rows                     |       17 |
| EXISTS_valid_source_rows                    |       17 |
| documented_lineage_not_replayed_text_metric |       20 |
| no_live_upstream_source                     |       38 |

## Carry-forwards

### Boolean uniformity carry-forwards

| col_name                      | classification                 | carry_forward                                                 |
|:------------------------------|:-------------------------------|:--------------------------------------------------------------|
| nlp_dynrisk_has_data          | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_dynrisk_has_data          |
| nlp_funcoutcome_has_data      | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_funcoutcome_has_data      |
| nlp_imaging_has_data          | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_imaging_has_data          |
| nlp_labs_has_data             | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_labs_has_data             |
| nlp_ln_has_data               | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ln_has_data               |
| nlp_ne_complications_has_data | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_complications_has_data |
| nlp_ne_genetics_has_data      | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_genetics_has_data      |
| nlp_ne_medications_has_data   | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_medications_has_data   |
| nlp_ne_operative_has_data     | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_operative_has_data     |
| nlp_ne_problemlist_has_data   | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_problemlist_has_data   |
| nlp_ne_staging_has_data       | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ne_staging_has_data       |
| nlp_physexam_has_data         | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_physexam_has_data         |
| nlp_pmhx_has_data             | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_pmhx_has_data             |
| nlp_pshx_has_data             | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_pshx_has_data             |
| nlp_ptdecision_has_data       | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_ptdecision_has_data       |
| nlp_radtx_has_data            | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_radtx_has_data            |
| nlp_rec_any_mentioned         | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_rec_any_mentioned         |
| nlp_rec_has_data              | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_rec_has_data              |
| nlp_survfu_has_data           | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_survfu_has_data           |
| nlp_symptoms_has_data         | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_symptoms_has_data         |
| nlp_tg_has_data               | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_tg_has_data               |
| nlp_tg_rising_mentioned       | type_b_placeholder_zero_true   | CF-mig180-NLP-PLACEHOLDER-nlp_tg_rising_mentioned             |
| nlp_tirads_has_data           | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_tirads_has_data           |
| nlp_usnodule_has_data         | type_a_presence_flag_true_only | CF-mig180-NLP-NEAR-UNIFORM-TRUE-nlp_usnodule_has_data         |

### Upstream-missing carry-forwards

| family               | source_table   | rationale                                                                                                         | carry_forward                                   |
|:---------------------|:---------------|:------------------------------------------------------------------------------------------------------------------|:------------------------------------------------|
| nlp_funcoutcome      | <NA>           | No live source table found among candidates: note_entities_llm_functional_outcomes.                               | CF-mig180-NLP-UPSTREAM-MISSING-funcoutcome      |
| nlp_imaging          | <NA>           | No live source table found among candidates: note_entities_llm_imaging.                                           | CF-mig180-NLP-UPSTREAM-MISSING-imaging          |
| nlp_labs             | <NA>           | No live source table found among candidates: note_entities_llm_labs.                                              | CF-mig180-NLP-UPSTREAM-MISSING-labs             |
| nlp_ne_complications | <NA>           | No live source table found among candidates: note_entities_complications.                                         | CF-mig180-NLP-UPSTREAM-MISSING-ne_complications |
| nlp_ne_genetics      | <NA>           | No live source table found among candidates: note_entities_genetics.                                              | CF-mig180-NLP-UPSTREAM-MISSING-ne_genetics      |
| nlp_ne_medications   | <NA>           | No live source table found among candidates: note_entities_medications.                                           | CF-mig180-NLP-UPSTREAM-MISSING-ne_medications   |
| nlp_ne_problemlist   | <NA>           | No live source table found among candidates: note_entities_problem_list.                                          | CF-mig180-NLP-UPSTREAM-MISSING-ne_problemlist   |
| nlp_ne_staging       | <NA>           | No live source table found among candidates: note_entities_staging.                                               | CF-mig180-NLP-UPSTREAM-MISSING-ne_staging       |
| nlp_physexam         | <NA>           | No live source table found among candidates: note_entities_llm_physical_exam.                                     | CF-mig180-NLP-UPSTREAM-MISSING-physexam         |
| nlp_ptdecision       | <NA>           | No live source table found among candidates: note_entities_llm_patient_decision_adherence.                        | CF-mig180-NLP-UPSTREAM-MISSING-ptdecision       |
| nlp_radtx            | <NA>           | No live source table found among candidates: note_entities_llm_rad_treatment.                                     | CF-mig180-NLP-UPSTREAM-MISSING-radtx            |
| nlp_usnodule         | <NA>           | No live source table found among candidates: note_entities_llm_us_nodule, canonical_us_nodule_characteristics_v1. | CF-mig180-NLP-UPSTREAM-MISSING-usnodule         |


## Apply posture

The prompt posture is SQL-only authoring. The companion migration file includes §A-§F, including a pre-snapshot and post-state probes, but it was not executed in this lane.
