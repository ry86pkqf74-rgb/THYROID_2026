# mig_180 NLP Cluster Rederivation Audit

Generated: 2026-04-29T23:34:16.578683+00:00

This is a read-only audit. Mismatch counts are computed only where a generic, source-schema-safe replay is possible. Text/date/key-finding metrics are lineage-documented and status-classified separately in the main audit report.

## Replayed metrics

| col_name                       | family           | metric_kind            | source_table                                 | rederivation_kind               |   n_mismatches |   n_rows |
|:-------------------------------|:-----------------|:-----------------------|:---------------------------------------------|:--------------------------------|---------------:|---------:|
| nlp_airway_has_data            | nlp_airway       | has_data               | main.note_entities_llm_airway_invasion_v2    | EXISTS_valid_source_rows        |           3424 |    10871 |
| nlp_airway_n_entities          | nlp_airway       | n_entities             | main.note_entities_llm_airway_invasion_v2    | COUNT_valid_source_rows         |           3869 |    10871 |
| nlp_airway_n_notes             | nlp_airway       | n_notes                | main.note_entities_llm_airway_invasion_v2    | COUNT_DISTINCT_note_index       |           3797 |    10871 |
| nlp_dynrisk_has_data           | nlp_dynrisk      | has_data               | main.note_entities_llm_dynamic_risk_response | EXISTS_valid_source_rows        |          10846 |    10871 |
| nlp_dynrisk_n_entities         | nlp_dynrisk      | n_entities             | main.note_entities_llm_dynamic_risk_response | COUNT_valid_source_rows         |          10863 |    10871 |
| nlp_dynrisk_n_notes            | nlp_dynrisk      | n_notes                | main.note_entities_llm_dynamic_risk_response | COUNT_DISTINCT_note_index       |          10847 |    10871 |
| nlp_frozensec_has_data         | nlp_frozensec    | has_data               | main.note_entities_llm_frozen_section_detail | EXISTS_valid_source_rows        |           8007 |    10871 |
| nlp_frozensec_n_entities       | nlp_frozensec    | n_entities             | main.note_entities_llm_frozen_section_detail | COUNT_valid_source_rows         |          10512 |    10871 |
| nlp_frozensec_n_notes          | nlp_frozensec    | n_notes                | main.note_entities_llm_frozen_section_detail | COUNT_DISTINCT_note_index       |           8833 |    10871 |
| nlp_ln_has_data                | nlp_ln           | has_data               | main.canonical_us_lymph_node_v2              | EXISTS_valid_source_rows        |          10336 |    10871 |
| nlp_ln_n_entities              | nlp_ln           | n_entities             | main.canonical_us_lymph_node_v2              | COUNT_valid_source_rows         |          10840 |    10871 |
| nlp_ln_n_notes                 | nlp_ln           | n_notes                | main.canonical_us_lymph_node_v2              | COUNT_rows_no_note_id_available |          10608 |    10871 |
| nlp_ln_positive_mentioned      | nlp_ln           | positive_mentioned     | main.canonical_us_lymph_node_v2              | BOOL_OR_positive_expression     |          10151 |    10871 |
| nlp_ne_operative_has_data      | nlp_ne_operative | has_data               | main.note_entities_operative_detail          | EXISTS_valid_source_rows        |           6840 |    10871 |
| nlp_ne_operative_n_rows        | nlp_ne_operative | n_rows                 | main.note_entities_operative_detail          | COUNT_valid_source_rows         |           6840 |    10871 |
| nlp_parathyroid_has_data       | nlp_parathyroid  | has_data               | main.note_entities_llm_parathyroid_detail_v1 | EXISTS_valid_source_rows        |           3062 |    10871 |
| nlp_parathyroid_n_entities     | nlp_parathyroid  | n_entities             | main.note_entities_llm_parathyroid_detail_v1 | COUNT_valid_source_rows         |           4977 |    10871 |
| nlp_parathyroid_n_notes        | nlp_parathyroid  | n_notes                | main.note_entities_llm_parathyroid_detail_v1 | COUNT_DISTINCT_note_index       |           3449 |    10871 |
| nlp_path_has_data              | nlp_path         | has_data               | main.note_entities_llm_pathology             | EXISTS_valid_source_rows        |           1723 |    10871 |
| nlp_path_ln_positive_mentioned | nlp_path         | ln_positive_mentioned  | main.note_entities_llm_pathology             | BOOL_OR_positive_expression     |           9872 |    10871 |
| nlp_path_margin_mentioned      | nlp_path         | margin_mentioned       | main.note_entities_llm_pathology             | BOOL_OR_positive_expression     |           9789 |    10871 |
| nlp_path_multifocal_mentioned  | nlp_path         | multifocal_mentioned   | main.note_entities_llm_pathology             | BOOL_OR_positive_expression     |           9878 |    10871 |
| nlp_path_n_entities            | nlp_path         | n_entities             | main.note_entities_llm_pathology             | COUNT_valid_source_rows         |           4341 |    10871 |
| nlp_path_n_notes               | nlp_path         | n_notes                | main.note_entities_llm_pathology             | COUNT_DISTINCT_note_index       |           9629 |    10871 |
| nlp_path_vasc_inv_mentioned    | nlp_path         | vasc_inv_mentioned     | main.note_entities_llm_pathology             | BOOL_OR_positive_expression     |          10088 |    10871 |
| nlp_pmhx_has_data              | nlp_pmhx         | has_data               | main.note_entities_llm_past_medical_hx       | EXISTS_valid_source_rows        |          10581 |    10871 |
| nlp_pmhx_n_entities            | nlp_pmhx         | n_entities             | main.note_entities_llm_past_medical_hx       | COUNT_valid_source_rows         |          10806 |    10871 |
| nlp_pmhx_n_notes               | nlp_pmhx         | n_notes                | main.note_entities_llm_past_medical_hx       | COUNT_DISTINCT_note_index       |          10591 |    10871 |
| nlp_pshx_has_data              | nlp_pshx         | has_data               | main.note_entities_llm_past_surgical_hx      | EXISTS_valid_source_rows        |           9007 |    10871 |
| nlp_pshx_n_entities            | nlp_pshx         | n_entities             | main.note_entities_llm_past_surgical_hx      | COUNT_valid_source_rows         |          10382 |    10871 |
| nlp_pshx_n_notes               | nlp_pshx         | n_notes                | main.note_entities_llm_past_surgical_hx      | COUNT_DISTINCT_note_index       |           9101 |    10871 |
| nlp_rec_any_mentioned          | nlp_rec          | any_mentioned          | main.note_entities_llm_recurrence            | BOOL_OR_positive_expression     |          10738 |    10871 |
| nlp_rec_disease_free_mentioned | nlp_rec          | disease_free_mentioned | main.note_entities_llm_recurrence            | BOOL_OR_positive_expression     |          10854 |    10871 |
| nlp_rec_has_data               | nlp_rec          | has_data               | main.note_entities_llm_recurrence            | EXISTS_valid_source_rows        |          10738 |    10871 |
| nlp_rec_n_entities             | nlp_rec          | n_entities             | main.note_entities_llm_recurrence            | COUNT_valid_source_rows         |          10840 |    10871 |
| nlp_survfu_has_data            | nlp_survfu       | has_data               | main.canonical_survival_followup_v1          | EXISTS_valid_source_rows        |           7960 |    10871 |
| nlp_survfu_n_entities          | nlp_survfu       | n_entities             | main.canonical_survival_followup_v1          | COUNT_valid_source_rows         |          10306 |    10871 |
| nlp_survfu_n_notes             | nlp_survfu       | n_notes                | main.canonical_survival_followup_v1          | COUNT_rows_no_note_id_available |           8251 |    10871 |
| nlp_symptoms_has_data          | nlp_symptoms     | has_data               | main.note_entities_llm_presenting_symptoms   | EXISTS_valid_source_rows        |          10755 |    10871 |
| nlp_symptoms_n_entities        | nlp_symptoms     | n_entities             | main.note_entities_llm_presenting_symptoms   | COUNT_valid_source_rows         |          10831 |    10871 |
| nlp_symptoms_n_notes           | nlp_symptoms     | n_notes                | main.note_entities_llm_presenting_symptoms   | COUNT_DISTINCT_note_index       |          10762 |    10871 |
| nlp_tg_has_data                | nlp_tg           | has_data               | main.tg_postop_surveillance_windows_v1       | EXISTS_valid_source_rows        |          10825 |    10871 |
| nlp_tg_n_entities              | nlp_tg           | n_entities             | main.tg_postop_surveillance_windows_v1       | COUNT_valid_source_rows         |          10866 |    10871 |
| nlp_tg_rising_mentioned        | nlp_tg           | rising_mentioned       | main.tg_postop_surveillance_windows_v1       | BOOL_OR_positive_expression     |          10868 |    10871 |
| nlp_tg_undetectable_mentioned  | nlp_tg           | undetectable_mentioned | main.tg_postop_surveillance_windows_v1       | BOOL_OR_positive_expression     |          10863 |    10871 |
| nlp_tirads_has_data            | nlp_tirads       | has_data               | main.note_entities_llm_tirads_granular       | EXISTS_valid_source_rows        |           9499 |    10871 |
| nlp_tirads_n_entities          | nlp_tirads       | n_entities             | main.note_entities_llm_tirads_granular       | COUNT_valid_source_rows         |          10852 |    10871 |
| nlp_tirads_n_notes             | nlp_tirads       | n_notes                | main.note_entities_llm_tirads_granular       | COUNT_DISTINCT_note_index       |           9890 |    10871 |
| nlp_vasc_has_data              | nlp_vasc         | has_data               | main.note_entities_llm_vascular_invasion_v2  | EXISTS_valid_source_rows        |           3005 |    10871 |
| nlp_vasc_n_entities            | nlp_vasc         | n_entities             | main.note_entities_llm_vascular_invasion_v2  | COUNT_valid_source_rows         |           3189 |    10871 |
| nlp_vasc_positive_mentioned    | nlp_vasc         | positive_mentioned     | main.note_entities_llm_vascular_invasion_v2  | BOOL_OR_positive_expression     |           3005 |    10871 |
| nlp_path_positive_mentioned    | nlp_path         | positive_mentioned     | main.note_entities_llm_pathology             | BOOL_OR_positive_expression     |           2250 |    10871 |
| nlp_cervln_has_data            | nlp_cervln       | has_data               | main.note_entities_llm_cervical_ln_detail    | EXISTS_valid_source_rows        |           3462 |    10871 |
| nlp_cervln_n_entities          | nlp_cervln       | n_entities             | main.note_entities_llm_cervical_ln_detail    | COUNT_valid_source_rows         |           4828 |    10871 |
| nlp_cervln_positive_mentioned  | nlp_cervln       | positive_mentioned     | main.note_entities_llm_cervical_ln_detail    | BOOL_OR_positive_expression     |           4131 |    10871 |
| nlp_esoph_has_data             | nlp_esoph        | has_data               | main.note_entities_llm_esophageal_invasion   | EXISTS_valid_source_rows        |           4109 |    10871 |
| nlp_esoph_n_entities           | nlp_esoph        | n_entities             | main.note_entities_llm_esophageal_invasion   | COUNT_valid_source_rows         |           4154 |    10871 |
| nlp_esoph_positive_mentioned   | nlp_esoph        | positive_mentioned     | main.note_entities_llm_esophageal_invasion   | BOOL_OR_positive_expression     |           4126 |    10871 |

## Metrics not generically replayed

| col_name                           | family               | metric_kind               | source_table                                 | rederivation_kind                           |
|:-----------------------------------|:---------------------|:--------------------------|:---------------------------------------------|:--------------------------------------------|
| nlp_airway_key_finding             | nlp_airway           | key_finding               | main.note_entities_llm_airway_invasion_v2    | documented_lineage_not_replayed_text_metric |
| nlp_dynrisk_key_finding            | nlp_dynrisk          | key_finding               | main.note_entities_llm_dynamic_risk_response | documented_lineage_not_replayed_text_metric |
| nlp_frozensec_key_finding          | nlp_frozensec        | key_finding               | main.note_entities_llm_frozen_section_detail | documented_lineage_not_replayed_text_metric |
| nlp_funcoutcome_has_data           | nlp_funcoutcome      | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_funcoutcome_key_finding        | nlp_funcoutcome      | key_finding               | MISSING                                      | no_live_upstream_source                     |
| nlp_funcoutcome_n_entities         | nlp_funcoutcome      | n_entities                | MISSING                                      | no_live_upstream_source                     |
| nlp_funcoutcome_n_notes            | nlp_funcoutcome      | n_notes                   | MISSING                                      | no_live_upstream_source                     |
| nlp_imaging_has_data               | nlp_imaging          | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_imaging_key_finding            | nlp_imaging          | key_finding               | MISSING                                      | no_live_upstream_source                     |
| nlp_imaging_n_entities             | nlp_imaging          | n_entities                | MISSING                                      | no_live_upstream_source                     |
| nlp_imaging_n_notes                | nlp_imaging          | n_notes                   | MISSING                                      | no_live_upstream_source                     |
| nlp_labs_has_data                  | nlp_labs             | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_labs_key_finding               | nlp_labs             | key_finding               | MISSING                                      | no_live_upstream_source                     |
| nlp_labs_n_entities                | nlp_labs             | n_entities                | MISSING                                      | no_live_upstream_source                     |
| nlp_labs_n_notes                   | nlp_labs             | n_notes                   | MISSING                                      | no_live_upstream_source                     |
| nlp_ln_levels_mentioned            | nlp_ln               | levels_mentioned          | main.canonical_us_lymph_node_v2              | documented_lineage_not_replayed_text_metric |
| nlp_ne_complications_has_data      | nlp_ne_complications | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_ne_complications_n_rows        | nlp_ne_complications | n_rows                    | MISSING                                      | no_live_upstream_source                     |
| nlp_ne_genetics_has_data           | nlp_ne_genetics      | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_ne_genetics_n_rows             | nlp_ne_genetics      | n_rows                    | MISSING                                      | no_live_upstream_source                     |
| nlp_ne_medications_has_data        | nlp_ne_medications   | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_ne_medications_n_rows          | nlp_ne_medications   | n_rows                    | MISSING                                      | no_live_upstream_source                     |
| nlp_ne_problemlist_has_data        | nlp_ne_problemlist   | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_ne_problemlist_n_rows          | nlp_ne_problemlist   | n_rows                    | MISSING                                      | no_live_upstream_source                     |
| nlp_ne_staging_has_data            | nlp_ne_staging       | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_ne_staging_n_rows              | nlp_ne_staging       | n_rows                    | MISSING                                      | no_live_upstream_source                     |
| nlp_parathyroid_key_finding        | nlp_parathyroid      | key_finding               | main.note_entities_llm_parathyroid_detail_v1 | documented_lineage_not_replayed_text_metric |
| nlp_physexam_has_data              | nlp_physexam         | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_physexam_key_finding           | nlp_physexam         | key_finding               | MISSING                                      | no_live_upstream_source                     |
| nlp_physexam_n_entities            | nlp_physexam         | n_entities                | MISSING                                      | no_live_upstream_source                     |
| nlp_physexam_n_notes               | nlp_physexam         | n_notes                   | MISSING                                      | no_live_upstream_source                     |
| nlp_pmhx_key_finding               | nlp_pmhx             | key_finding               | main.note_entities_llm_past_medical_hx       | documented_lineage_not_replayed_text_metric |
| nlp_pshx_key_finding               | nlp_pshx             | key_finding               | main.note_entities_llm_past_surgical_hx      | documented_lineage_not_replayed_text_metric |
| nlp_ptdecision_has_data            | nlp_ptdecision       | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_ptdecision_key_finding         | nlp_ptdecision       | key_finding               | MISSING                                      | no_live_upstream_source                     |
| nlp_ptdecision_n_entities          | nlp_ptdecision       | n_entities                | MISSING                                      | no_live_upstream_source                     |
| nlp_ptdecision_n_notes             | nlp_ptdecision       | n_notes                   | MISSING                                      | no_live_upstream_source                     |
| nlp_radtx_has_data                 | nlp_radtx            | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_radtx_key_finding              | nlp_radtx            | key_finding               | MISSING                                      | no_live_upstream_source                     |
| nlp_radtx_n_entities               | nlp_radtx            | n_entities                | MISSING                                      | no_live_upstream_source                     |
| nlp_radtx_n_notes                  | nlp_radtx            | n_notes                   | MISSING                                      | no_live_upstream_source                     |
| nlp_rec_confidence_tier            | nlp_rec              | confidence_tier           | main.note_entities_llm_recurrence            | documented_lineage_not_replayed_text_metric |
| nlp_rec_earliest_date              | nlp_rec              | earliest_date             | main.note_entities_llm_recurrence            | documented_lineage_not_replayed_text_metric |
| nlp_rec_type_worst                 | nlp_rec              | type_worst                | main.note_entities_llm_recurrence            | documented_lineage_not_replayed_text_metric |
| nlp_survfu_key_finding             | nlp_survfu           | key_finding               | main.canonical_survival_followup_v1          | documented_lineage_not_replayed_text_metric |
| nlp_symptoms_key_finding           | nlp_symptoms         | key_finding               | main.note_entities_llm_presenting_symptoms   | documented_lineage_not_replayed_text_metric |
| nlp_tirads_has_component_detail    | nlp_tirads           | has_component_detail      | main.note_entities_llm_tirads_granular       | documented_lineage_not_replayed_text_metric |
| nlp_tirads_max_category            | nlp_tirads           | max_category              | main.note_entities_llm_tirads_granular       | documented_lineage_not_replayed_text_metric |
| nlp_usnodule_has_data              | nlp_usnodule         | has_data                  | MISSING                                      | no_live_upstream_source                     |
| nlp_usnodule_key_finding           | nlp_usnodule         | key_finding               | MISSING                                      | no_live_upstream_source                     |
| nlp_usnodule_n_entities            | nlp_usnodule         | n_entities                | MISSING                                      | no_live_upstream_source                     |
| nlp_usnodule_n_notes               | nlp_usnodule         | n_notes                   | MISSING                                      | no_live_upstream_source                     |
| nlp_vasc_confidence_tier           | nlp_vasc             | confidence_tier           | main.note_entities_llm_vascular_invasion_v2  | documented_lineage_not_replayed_text_metric |
| nlp_rec_earliest_days_from_surg    | nlp_rec              | earliest_days_from_surg   | main.note_entities_llm_recurrence            | documented_lineage_not_replayed_text_metric |
| nlp_path_multifocal_concordance_v2 | nlp_path             | multifocal_concordance_v2 | main.note_entities_llm_pathology             | documented_lineage_not_replayed_text_metric |
| nlp_path_confidence_tier           | nlp_path             | confidence_tier           | main.note_entities_llm_pathology             | documented_lineage_not_replayed_text_metric |
| nlp_cervln_confidence_tier         | nlp_cervln           | confidence_tier           | main.note_entities_llm_cervical_ln_detail    | documented_lineage_not_replayed_text_metric |
| nlp_esoph_confidence_tier          | nlp_esoph            | confidence_tier           | main.note_entities_llm_esophageal_invasion   | documented_lineage_not_replayed_text_metric |
