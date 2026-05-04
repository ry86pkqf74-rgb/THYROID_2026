# mig_297 disposition — cohort view freshness audit

- Probed views: 64
- Repointed (table-level safe sub): 0
- Needs manual repoint (column-level deprecation or compile/drift fail): 0
- No action: 64

## Substitution rules

| deprecated | replacement | mode |
|---|---|---|
| `canonical_recurrence_resolved_v1` | `canonical_recurrence_patient_rollup_v1` | auto (whole-word table swap) |
| `canonical_recurrence_v1` | `canonical_recurrence_patient_rollup_v1` | auto (whole-word table swap) |
| `recurrence_event_clean_v1` | `canonical_recurrence_patient_rollup_v1` | auto (whole-word table swap) |
| `nlp_tirads_max_category` | `tirads_resolved` | manual (column-level; semantics differ) |

## Per-view disposition

| view | flags | action | pre_rows | pre_cols | post_rows | note |
|---|---|---|---|---|---|---|
| manuscript_workspace.cohort_descriptive_full_cohort_v1 | — | no_change | 10871 | 176 |  |  |
| manuscript_workspace.cohort_m001_indeterminate_genetics_v1 | — | no_change | 1267 | 30 |  |  |
| manuscript_workspace.cohort_m004_graves_hashimoto_cancer_v1 | — | no_change | 1761 | 26 |  |  |
| manuscript_workspace.cohort_m006_molecular_surg_decision_v1 | — | no_change | 1286 | 26 |  |  |
| manuscript_workspace.cohort_m007_rss_reclassification_v1 | — | no_change | 3144 | 27 |  |  |
| manuscript_workspace.cohort_m009_parathyroid_final_path_v1 | — | no_change | 1409 | 38 |  |  |
| manuscript_workspace.cohort_m011_tirads_fna_genetics_v1 | — | no_change | 3282 | 25 |  |  |
| manuscript_workspace.cohort_m016_graves_carcinoma_v1 | — | no_change | 574 | 22 |  |  |
| manuscript_workspace.cohort_m017_eucalcemic_hypopara_v1 | — | no_change | 100 | 38 |  |  |
| manuscript_workspace.cohort_m018_molecular_beth56_v1 | — | no_change | 1494 | 24 |  |  |
| manuscript_workspace.cohort_m019_rai_outcomes_v1 | — | no_change | 862 | 33 |  |  |
| manuscript_workspace.cohort_m023_preop_genetics_v1 | — | no_change | 1286 | 26 |  |  |
| manuscript_workspace.cohort_m025_tirads_performance_v1 | — | no_change | 3375 | 46 |  |  |
| manuscript_workspace.cohort_m028_bethesda_iii_iv_v1 | — | no_change | 1267 | 56 |  |  |
| manuscript_workspace.cohort_m029_fna_concordance_v1 | — | no_change | 2401 | 40 |  |  |
| manuscript_workspace.cohort_m030_genetic_predictive_v1 | — | no_change | 1286 | 58 |  |  |
| manuscript_workspace.cohort_m031_nuclear_medicine_v1 | — | no_change | 1148 | 46 |  |  |
| manuscript_workspace.cohort_m032_descriptive_25yr_v1 | — | no_change | 10871 | 70 |  |  |
| manuscript_workspace.cohort_m033_afirma_thyroseq_v1 | — | no_change | 969 | 58 |  |  |
| manuscript_workspace.cohort_m035_bethesda_v_v1 | — | no_change | 273 | 50 |  |  |
| manuscript_workspace.cohort_m036_ata_risk_comparison_v1 | — | no_change | 4019 | 64 |  |  |
| manuscript_workspace.cohort_m037_ln_metastasis_v1 | — | no_change | 2234 | 83 |  |  |
| manuscript_workspace.cohort_m038_massive_goiter_v1 | — | no_change | 10871 | 153 |  |  |
| manuscript_workspace.cohort_m039_pth_calcium_v1 | — | no_change | 5999 | 89 |  |  |
| manuscript_workspace.cohort_m040_reoperative_v1 | — | no_change | 1470 | 65 |  |  |
| manuscript_workspace.cohort_m042_incidental_parathyroid_v1 | — | no_change | 4798 | 64 |  |  |
| manuscript_workspace.cohort_m043_ln_predictors_v1 | — | no_change | 4019 | 68 |  |  |
| manuscript_workspace.cohort_m044_ajcc_ete_v1 | — | no_change | 4013 | 64 |  |  |
| manuscript_workspace.cohort_m045_multimodal_risk_v1 | — | no_change | 1165 | 44 |  |  |
| manuscript_workspace.cohort_m046_niftp_era_bethesda_v1 | — | no_change | 5026 | 42 |  |  |
| manuscript_workspace.cohort_m047_frozen_section_v1 | — | no_change | 10871 | 42 |  |  |
| manuscript_workspace.cohort_m048_tnm_multifocal_v1 | — | no_change | 10871 | 25 |  |  |
| manuscript_workspace.cohort_m049_pyramidal_lobe_v1 | — | no_change | 10871 | 16 |  |  |
| manuscript_workspace.cohort_m050_tumor_size_volume_v1 | — | no_change | 10871 | 18 |  |  |
| manuscript_workspace.cohort_m051_ete_ln_v1 | — | no_change | 10871 | 26 |  |  |
| manuscript_workspace.cohort_m052_mrlnd_ln_count_v1 | — | no_change | 10871 | 23 |  |  |
| manuscript_workspace.cohort_m053_nondiagnostic_fna_v1 | — | no_change | 10871 | 18 |  |  |
| manuscript_workspace.cohort_m054_niftp_reclass_v1 | — | no_change | 10871 | 19 |  |  |
| manuscript_workspace.cohort_m055_recurrence_rai_v1 | — | no_change | 10871 | 22 |  |  |
| manuscript_workspace.cohort_m056_age_epidemiology_v1 | — | no_change | 10871 | 17 |  |  |
| manuscript_workspace.cohort_m057_risk_stratification_v1 | — | no_change | 10871 | 24 |  |  |
| manuscript_workspace.cohort_m058_thyroid_size_weight_v1 | — | no_change | 10871 | 21 |  |  |
| manuscript_workspace.cohort_m059_prognostic_scoring_v1 | — | no_change | 10871 | 27 |  |  |
| manuscript_workspace.cohort_m060_adenoma_ftump_v1 | — | no_change | 10871 | 22 |  |  |
| manuscript_workspace.cohort_m061_thyroiditis_outcomes_v1 | — | no_change | 10871 | 23 |  |  |
| manuscript_workspace.cohort_m062_incidental_frozen_v1 | — | no_change | 10871 | 19 |  |  |
| manuscript_workspace.cohort_m063_frozen_false_neg_v1 | — | no_change | 10871 | 20 |  |  |
| manuscript_workspace.cohort_m064_frozen_decision_v1 | — | no_change | 10871 | 22 |  |  |
| manuscript_workspace.cohort_m065_frozen_tt_vs_lob_v1 | — | no_change | 10871 | 21 |  |  |
| manuscript_workspace.cohort_m066_parathyroid_id_v1 | — | no_change | 10871 | 32 |  |  |
| manuscript_workspace.cohort_m067_tsh_tg_tumorigenesis_v1 | — | no_change | 2567 | 25 |  |  |
| manuscript_workspace.cohort_m068_mutation_labs_v1 | — | no_change | 10871 | 27 |  |  |
| manuscript_workspace.cohort_m069_graves_hashimoto_v1 | — | no_change | 10871 | 21 |  |  |
| manuscript_workspace.cohort_m070_hereditary_v1 | — | no_change | 10871 | 19 |  |  |
| manuscript_workspace.cohort_m071_immunologic_meds_v1 | — | no_change | 10871 | 20 |  |  |
| manuscript_workspace.cohort_m072_molecular_surg_impact_v1 | — | no_change | 1286 | 25 |  |  |
| manuscript_workspace.cohort_m073_tg_lob_vs_tt_v1 | — | no_change | 2567 | 24 |  |  |
| manuscript_workspace.cohort_m075_tirads_multi_nodule_v1 | — | no_change | 3282 | 22 |  |  |
| manuscript_workspace.cohort_m076_ln_surveillance_v1 | — | no_change | 10871 | 25 |  |  |
| manuscript_workspace.cohort_m078_graves_survival_v1 | — | no_change | 1530 | 23 |  |  |
| manuscript_workspace.cohort_m079_eucalcemic_outcomes_v1 | — | no_change | 100 | 37 |  |  |
| manuscript_workspace.cohort_m080_molecular_beth56_v1 | — | no_change | 306 | 22 |  |  |
| manuscript_workspace.cohort_m081_rai_resistant_v1 | — | no_change | 862 | 29 |  |  |
| manuscript_workspace.cohort_m082_parathyroid_tumors_v1 | — | no_change | 1399 | 34 |  |  |
