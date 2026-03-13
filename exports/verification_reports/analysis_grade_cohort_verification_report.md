# Analysis Grade Cohort Verification Report
Generated: 2026-03-13T01:24:47.528334

## Executive Summary

Verification run: 2026-03-13T01:24:47.527781
Environment: local

## Row Counts

| Table | Rows | Status |
|-------|------|--------|
| `patient_analysis_resolved_v1` | 10,871 | NEW |
| `episode_analysis_resolved_v1` | 9,394 | NEW |
| `lesion_analysis_resolved_v1` | 11,707 | NEW |
| `thyroid_scoring_systems_v1` | - | NOT FOUND |
| `imaging_nodule_master_v1` | - | NOT FOUND |
| `imaging_exam_master_v1` | - | NOT FOUND |
| `imaging_patient_summary_v1` | - | NOT FOUND |
| `imaging_fna_linkage_v3` | - | NOT FOUND |
| `fna_molecular_linkage_v3` | - | NOT FOUND |
| `preop_surgery_linkage_v3` | - | NOT FOUND |
| `surgery_pathology_linkage_v3` | - | NOT FOUND |
| `pathology_rai_linkage_v3` | - | NOT FOUND |
| `linkage_summary_v3` | - | NOT FOUND |
| `linkage_ambiguity_review_v1` | - | NOT FOUND |
| `complication_phenotype_v1` | - | NOT FOUND |
| `complication_patient_summary_v1` | - | NOT FOUND |
| `complication_discrepancy_report_v1` | - | NOT FOUND |
| `longitudinal_lab_clean_v1` | - | NOT FOUND |
| `longitudinal_lab_patient_summary_v1` | - | NOT FOUND |
| `recurrence_event_clean_v1` | - | NOT FOUND |
| `val_scoring_systems` | - | NOT FOUND |
| `tumor_episode_master_v2` | 11,691 | SOURCE |
| `operative_episode_detail_v2` | 9,371 | SOURCE |
| `fna_episode_master_v2` | 59,620 | SOURCE |
| `molecular_test_episode_v2` | 10,126 | SOURCE |
| `rai_treatment_episode_v2` | 1,857 | SOURCE |
| `imaging_nodule_long_v2` | 10,866 | SOURCE |
| `imaging_fna_linkage_v2` | - | NOT FOUND |
| `fna_molecular_linkage_v2` | 0 | SOURCE |
| `preop_surgery_linkage_v2` | 0 | SOURCE |
| `surgery_pathology_linkage_v2` | - | NOT FOUND |
| `pathology_rai_linkage_v2` | 0 | SOURCE |
| `patient_refined_master_clinical_v12` | 12,886 | SOURCE |
| `extracted_tirads_validated_v1` | 3,474 | SOURCE |

## Duplicate Checks

| Table | Rows | Duplicate Check |
|-------|------|------------------|
| `patient_analysis_resolved_v1` | 10,871 | PASS |
| `thyroid_scoring_systems_v1` | - | NOT FOUND |
| `complication_patient_summary_v1` | - | NOT FOUND |
| `longitudinal_lab_patient_summary_v1` | - | NOT FOUND |
| `imaging_patient_summary_v1` | - | NOT FOUND |

## Null/Missingness

| Column | NULL Count | NULL % |
|--------|-----------|--------|
| `age_at_surgery` | 0 | 0.0% |
| `ages_score` | 10,871 | 100.0% |
| `ajcc8_calculable_flag` | 10,871 | 100.0% |
| `ajcc8_m_stage` | 10,871 | 100.0% |
| `ajcc8_missing_components` | 10,871 | 100.0% |
| `ajcc8_n_stage` | 10,871 | 100.0% |
| `ajcc8_stage_group` | 10,871 | 100.0% |
| `ajcc8_t_stage` | 10,871 | 100.0% |
| `ames_risk_group` | 10,871 | 100.0% |
| `analysis_eligible_flag` | 0 | 0.0% |
| `anti_tg_nadir` | 10,871 | 100.0% |
| `anti_tg_rising_flag` | 10,871 | 100.0% |
| `any_confirmed_complication` | 0 | 0.0% |
| `any_recurrence_flag` | 0 | 0.0% |
| `ata_calculable_flag` | 10,871 | 100.0% |
| `ata_response_calculable_flag` | 10,871 | 100.0% |
| `ata_response_category` | 10,871 | 100.0% |
| `ata_risk_category` | 10,871 | 100.0% |
| `biochemical_recurrence_flag` | 10,871 | 100.0% |
| `braf_positive_final` | 0 | 0.0% |
| `braf_source` | 10,495 | 96.5% |
| `braf_variant_raw` | 10,698 | 98.4% |
| `calcium_nadir` | 10,871 | 100.0% |
| `calcium_supplement_required` | 0 | 0.0% |
| `chyle_leak_status` | 0 | 0.0% |
| `closest_margin_mm` | 9,992 | 91.9% |
| `date_traceability_status` | 0 | 0.0% |
| `demo_confidence` | 0 | 0.0% |
| `demo_source` | 0 | 0.0% |
| `ete_grade_final` | 6,796 | 62.5% |
| `ete_grade_source` | 0 | 0.0% |
| `first_surgery_date` | 1 | 0.0% |
| `fna_bethesda_confidence` | 5,622 | 51.7% |
| `fna_bethesda_final` | 5,622 | 51.7% |
| `fna_bethesda_source` | 5,622 | 51.7% |
| `hematoma_status` | 0 | 0.0% |
| `histology_final` | 6,734 | 61.9% |
| `histology_source` | 0 | 0.0% |
| `hypocalcemia_status` | 0 | 0.0% |
| `hypoparathyroidism_status` | 0 | 0.0% |
| `imaging_n_nodule_records` | 7,397 | 68.0% |
| `imaging_nodule_size_cm` | 7,432 | 68.4% |
| `imaging_tirads_best` | 7,397 | 68.0% |
| `imaging_tirads_category` | 7,397 | 68.0% |
| `imaging_tirads_source` | 7,397 | 68.0% |
| `imaging_tirads_worst` | 7,397 | 68.0% |
| `lab_completeness_score` | 0 | 0.0% |
| `lateral_neck_dissected` | 0 | 0.0% |
| `ln_burden_band` | 10,871 | 100.0% |
| `ln_positive_final` | 6,811 | 62.7% |
| `ln_ratio` | 10,871 | 100.0% |
| `macis_calculable_flag` | 10,871 | 100.0% |
| `macis_risk_group` | 10,871 | 100.0% |
| `macis_score` | 10,871 | 100.0% |
| `margin_status_final` | 6,914 | 63.6% |
| `mol_n_tests` | 846 | 7.8% |
| `mol_platform` | 846 | 7.8% |
| `mol_test_date` | 10,062 | 92.6% |
| `molecular_eligible_flag` | 846 | 7.8% |
| `molecular_risk_tier` | 10,871 | 100.0% |
| `n_confirmed_complications` | 0 | 0.0% |
| `path_ene_raw` | 9,644 | 88.7% |
| `path_ete_raw` | 6,796 | 62.5% |
| `path_gross_ete_flag` | 9,888 | 91.0% |
| `path_histology_raw` | 6,734 | 61.9% |
| `path_histology_variant_raw` | 7,554 | 69.5% |
| `path_laterality` | 533 | 4.9% |
| `path_ln_examined_raw` | 3,141 | 28.9% |
| `path_ln_positive_raw` | 7,268 | 66.9% |
| `path_lvi_raw` | 7,505 | 69.0% |
| `path_m_stage_raw` | 6,866 | 63.2% |
| `path_margin_raw` | 6,987 | 64.3% |
| `path_multifocal_flag` | 10,871 | 100.0% |
| `path_n_stage_raw` | 6,856 | 63.1% |
| `path_n_tumors` | 10,871 | 100.0% |
| `path_pni_raw` | 9,433 | 86.8% |
| `path_stage_raw` | 10,871 | 100.0% |
| `path_t_stage_raw` | 6,862 | 63.1% |
| `path_tumor_size_cm` | 6,741 | 62.0% |
| `path_vascular_invasion_raw` | 7,189 | 66.1% |
| `postop_low_calcium_flag` | 10,871 | 100.0% |
| `postop_low_pth_flag` | 10,871 | 100.0% |
| `provenance_confidence` | 0 | 0.0% |
| `pth_nadir` | 10,871 | 100.0% |
| `race` | 9 | 0.1% |
| `rai_assertion_statuses` | 10,836 | 99.7% |
| `rai_eligible_flag` | 0 | 0.0% |
| `rai_first_date` | 10,838 | 99.7% |
| `rai_max_dose_mci` | 10,836 | 99.7% |
| `rai_received_flag` | 0 | 0.0% |
| `ras_positive_final` | 0 | 0.0% |
| `ras_subtype_raw` | 10,697 | 98.4% |
| `recurrence_date` | 10,871 | 100.0% |
| `recurrence_site_primary` | 10,871 | 100.0% |
| `recurrence_source` | 10,871 | 100.0% |
| `recurrence_type_primary` | 10,871 | 100.0% |
| `research_id` | 0 | 0.0% |
| `resolved_at` | 0 | 0.0% |
| `resolved_layer_version` | 0 | 0.0% |
| `rln_permanent_flag` | 0 | 0.0% |
| `rln_status` | 0 | 0.0% |
| `rln_transient_flag` | 0 | 0.0% |
| `scoring_ajcc8_flag` | 0 | 0.0% |
| `scoring_ata_flag` | 0 | 0.0% |
| `scoring_macis_flag` | 0 | 0.0% |
| `seroma_status` | 0 | 0.0% |
| `sex` | 0 | 0.0% |
| `source_script` | 0 | 0.0% |
| `structural_recurrence_flag` | 10,871 | 100.0% |
| `surg_first_date` | 2,140 | 19.7% |
| `surg_hemithyroidectomy` | 2,138 | 19.7% |
| `surg_n_procedures` | 2,138 | 19.7% |
| `surg_procedure_type` | 2,138 | 19.7% |
| `surg_total_thyroidectomy` | 2,138 | 19.7% |
| `survival_eligible_flag` | 0 | 0.0% |
| `tert_positive_final` | 0 | 0.0% |
| `tg_below_threshold_ever` | 10,871 | 100.0% |
| `tg_last_value` | 10,871 | 100.0% |
| `tg_n_measurements` | 10,871 | 100.0% |
| `tg_nadir` | 10,871 | 100.0% |
| `tg_peak` | 10,871 | 100.0% |
| `tg_rising_flag` | 10,871 | 100.0% |
| `tsh_suppressed_ever` | 10,871 | 100.0% |
| `vascular_invasion_final` | 7,120 | 65.5% |
| `vascular_vessel_count` | 10,825 | 99.6% |
| `wound_infection_status` | 0 | 0.0% |

## Score Calculability

_thyroid_scoring_systems_v1 not found_

## Linkage Quality

_linkage_summary_v3 not found_

## Complication Phenotype

_complication_discrepancy_report_v1 not found_

## Recurrence Events

_recurrence_event_clean_v1 not found_

## Date Precedence

_longitudinal_lab_clean_v1 not found_

## Concordance

**histology_final concordance with tumor_episode_master_v2:**
 total  concordant  null_in_resolved
 10871     10762.0            6734.0

