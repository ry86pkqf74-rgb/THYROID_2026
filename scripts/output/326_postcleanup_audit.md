# Post-Cleanup Audit v2 (Script 326)

Generated: 2026-04-21T04:58:17.634888Z

========================================================================
Script 326 — Post-cleanup verification v2
Generated: 2026-04-21T04:58:14.804607Z
========================================================================

## 1. CPM Invariants
  rows=10871 distinct_rid=10871 null_fna=0
  CPM column count: 1538
  PASS

## 2. Tier 2 Completeness
  22/22 domains have Tier 2 tables
  PASS — all domains covered

## 3. Tier 2 Event Table Row Counts
  airway_invasion_event_v1                              11601 rows    1805 pts
  dynamic_risk_response_event_v1                           53 rows      25 pts
  frozen_section_event_v1                                8640 rows    3535 pts
  functional_outcomes_event_v1                           3322 rows    1842 pts
  parathyroid_detail_event_v1                           10130 rows    3706 pts
  past_medical_hx_event_v1                                865 rows     295 pts
  past_surgical_hx_event_v1                              3919 rows    1878 pts
  patient_decision_adherence_event_v1                     641 rows     398 pts
  physical_exam_event_v1                                 2025 rows     662 pts
  presenting_symptoms_event_v1                            280 rows     120 pts
  rad_treatment_event_v1                                  580 rows     213 pts
  vascular_invasion_event_v1                            22800 rows    4215 pts

## 4. Patient-wide Table Row Counts
  airway_invasion_patient_wide_v1                        1805 rows    1805 pts
  dynamic_risk_response_patient_wide_v1                    25 rows      25 pts
  frozen_section_patient_wide_v1                         3535 rows    3535 pts
  functional_outcomes_patient_wide_v1                    1842 rows    1842 pts
  parathyroid_patient_wide_v1                            3706 rows    3706 pts
  past_medical_hx_patient_wide_v1                         295 rows     295 pts
  past_surgical_hx_patient_wide_v1                       1878 rows    1878 pts
  patient_decision_adherence_patient_wide_v1              398 rows     398 pts
  physical_exam_patient_wide_v1                           522 rows     522 pts
  presenting_symptoms_patient_wide_v1                     120 rows     120 pts
  rad_treatment_patient_wide_v1                           213 rows     213 pts
  vascular_invasion_patient_wide_v1                      4215 rows    4215 pts

## 5. Concordance Summaries (all verify tables)
  airway_invasion           extrathyroidal_extension            agree=   90 disagree=  475 pct=0.1593
  frozen_section            frozen_section_performed            agree= 2688 disagree=    0 pct=1.0
  frozen_section            frozen_section_result               agree=    0 disagree= 1642 pct=0.0
  genetics_per_test         platform                            agree=  849 disagree=  376 pct=0.6931
  labs                      tsh_value                           agree=  113 disagree=   88 pct=0.5622
  labs                      tg_value                            agree=    3 disagree=  171 pct=0.0172
  ln                        ln_total                            agree=    0 disagree= 2261 pct=0.0
  ln                        ln_positive                         agree=    0 disagree= 1154 pct=0.0
  operative                 surgery_type                        agree=    0 disagree=    0 pct=None
  operative                 central_neck_dissection_flag        agree=    0 disagree=    0 pct=None
  operative                 lateral_neck_dissection_flag        agree=    0 disagree=    0 pct=None
  parathyroid               n_pt_identified                     agree=  112 disagree=  513 pct=0.1792
  parathyroid               n_pt_autotransplanted               agree=    0 disagree=    3 pct=0.0
  pathology_synoptics       histologic_type                     agree= 4137 disagree=    0 pct=1.0
  pathology_synoptics       size_greatest_dimension_cm          agree= 3469 disagree=  522 pct=0.8692
  pathology_synoptics       extrathyroidal_extension            agree= 3881 disagree=    0 pct=1.0
  pathology_synoptics       margin_status                       agree= 3957 disagree=    0 pct=1.0
  pathology_synoptics       lymphatic_invasion                  agree= 3433 disagree=    0 pct=1.0
  pathology_synoptics       angioinvasion                       agree= 3751 disagree=    0 pct=1.0
  pathology_synoptics       perineural_invasion                 agree= 1487 disagree=    0 pct=1.0
  pathology_synoptics       t_stage                             agree=    0 disagree=    0 pct=None
  pathology_synoptics       n_stage                             agree=    0 disagree=    0 pct=None
  pathology_synoptics       ln_examined                         agree=    0 disagree= 7767 pct=0.0
  pathology_synoptics       ln_involved                         agree=    0 disagree= 3705 pct=0.0
  rai                       rai_dose_mci                        agree=    0 disagree=  197 pct=0.0
  rai                       rai_indication                      agree=    0 disagree=  232 pct=0.0
  recurrence                recurrence_confirmed                agree=    0 disagree=    0 pct=None
  us_nodule                 nodule_count                        agree=  768 disagree= 3306 pct=0.1885
  us_nodule                 max_nodule_size_cm                  agree=    7 disagree= 3432 pct=0.002
  us_nodule                 tirads_category_max                 agree=  613 disagree=  391 pct=0.6106
  vascular_invasion         lymphovascular_invasion             agree=  677 disagree= 2702 pct=0.2004

## 6. Low Concordance Fields (pct_agree < 0.80)
  airway_invasion           extrathyroidal_extension            pct=0.1593
  frozen_section            frozen_section_result               pct=0.0
  genetics_per_test         platform                            pct=0.6931
  labs                      tsh_value                           pct=0.5622
  labs                      tg_value                            pct=0.0172
  ln                        ln_total                            pct=0.0
  ln                        ln_positive                         pct=0.0
  parathyroid               n_pt_identified                     pct=0.1792
  parathyroid               n_pt_autotransplanted               pct=0.0
  pathology_synoptics       ln_examined                         pct=0.0
  pathology_synoptics       ln_involved                         pct=0.0
  rai                       rai_dose_mci                        pct=0.0
  rai                       rai_indication                      pct=0.0
  us_nodule                 nodule_count                        pct=0.1885
  us_nodule                 max_nodule_size_cm                  pct=0.002
  us_nodule                 tirads_category_max                 pct=0.6106
  vascular_invasion         lymphovascular_invasion             pct=0.2004
  Wrote 17 rows to verification_low_concordance_v1

## 7. Archive Move Log
  280_archive_stale_objects                          us_nodules_tirads_placeholder_archive_v1                     3 rows
  292_rebuild_operative_episode_detail_v2            operative_episode_detail_v2                                  9371 rows
  297_archive_stale_objects                          note_entities_llm_synoptic_pathology_enrichment__march2026_broken 11037 rows
  297_archive_stale_objects                          _molecular_patient_rollup_v227                               10025 rows
  297_archive_stale_objects                          canonical_cleanup_audit_v1_snapshot_20260417                 115 rows
  297_archive_stale_objects                          manuscript_dive_map_v1_pre272_snapshot                       63 rows
  297_archive_stale_objects                          view_definitions_snapshot_bigcleanup                         65 rows
  297_archive_stale_objects                          collision_resolution_v265                                    88 rows
  297_archive_stale_objects                          cpm_cols_unmapped_review_v265                                223 rows
  297_archive_stale_objects                          cpm_unmapped_triage_v266a                                    0 rows
  297_archive_stale_objects                          fusion_flag_unparsed_review_v265                             16 rows
  297_archive_stale_objects                          fusion_parse_error_review_v265                               632 rows
  297_archive_stale_objects                          ln_extract_noncohort_orphan_v279                             2 rows
  297_archive_stale_objects                          registry_end_to_end_validation_v273                          820 rows
  297_archive_stale_objects                          registry_v2_resolution_audit_v273                            934 rows
  297_archive_stale_objects                          registry_v2_unresolved_pointers_v273                         0 rows
  297_archive_stale_objects                          thin_wrapper_pi_review_v273                                  24 rows
  297_archive_stale_objects                          vc_paralysis_recalibration_v236                              59 rows
  297_archive_stale_objects                          legacy_column_sweep_v1_1                                     5 rows
  297_archive_stale_objects                          nan_string_audit_v1_1                                        476 rows
  297_archive_stale_objects                          registry_normalization_review_v1_1                           116 rows
  297_archive_stale_objects                          v1_1_finalization_audit_v1                                   71 rows
  297_archive_stale_objects                          v1_1_tech_debt_v1                                            5 rows
  325_archive_duplicates_round2                      tumor_pathology                                              4290 rows
  325_archive_duplicates_round2                      path_size_adjudication_v241                                  96 rows
  325_archive_duplicates_round2                      ret_note_entity_adjudication_v226                            177 rows
  325_archive_duplicates_round2                      ret_patient_adjudicated_v226                                 66 rows
  325_archive_duplicates_round2                      tirads_v2_reports_raw                                        8810 rows
  325_archive_duplicates_round2                      tirads_llm_validation_v2                                     1225 rows

## 8. Final Database State
  main: 166 tables, 24 views
  manuscript_workspace: 40 tables, 68 views
  views_readable: 0 tables, 46 views

## 9. Domain -> Verify Table Mapping
  verify_airway_invasion_v1                             10871 rows
  verify_frozen_section_v1                              10872 rows
  verify_genetics_per_test_v1                           10861 rows
  verify_labs_v1                                         3760 rows
  verify_ln_v1                                          10871 rows
  verify_operative_v1                                   11422 rows
  verify_parathyroid_v1                                 10872 rows
  verify_pathology_synoptics_v1                         10871 rows
  verify_rai_v1                                          1148 rows
  verify_recurrence_v1                                  10871 rows
  verify_us_nodule_v1                                    6126 rows
  verify_vascular_invasion_v1                           10871 rows

========================================================================
Verification complete.
