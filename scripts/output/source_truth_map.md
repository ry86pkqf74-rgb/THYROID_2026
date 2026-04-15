# THYROID_2026 — Source Truth Map
Generated: 2026-04-15
Database: `thyroid_ete_fix_20260413`
Table: `canonical_patient_master_v1`

---

## Summary
- **Total columns:** 748
- **Total rows:** 10,871
- **Overall mean coverage:** 30.4%
- **Fully populated columns (≥100%):** 93
- **Sparse columns (<10%):** 308

---

## A. Database Hierarchy

| Priority | Database | Role | Notes |
|----------|----------|------|-------|
| 1 | `thyroid_ete_fix_20260413` | **CANONICAL — all reads and writes** | Created Apr 2026 as clean ETE-fix copy |
| 2 | `Thyroid 2026` (DuckLake) | **HISTORICAL READ ONLY** | Origin of `fna_path_outcome` via scripts 115/116 |
| 3 | `thyroid_research_ro_v2` (share) | **HISTORICAL READ ONLY** | Origin of `tirads_llm_extracted_v2`, `ln_master_rollup_v1` |
| 4 | `Thyroid 2026 Molecular *` | **DEPRECATED** | Do not use |
| 5 | `my_db`, `rosflow`, `sample_data` | **UNRELATED** | Ignore |

---

## B. Table Tiers (on `thyroid_ete_fix_20260413`)

| Tier | Tables | Description | In canonical? |
|------|--------|-------------|---------------|
| 0 | `canonical_patient_master_v1` | Single analytical table — 10,871 patients × 748 columns | **IS the canonical** |
| 1 | `gold_master_patient_facts_v1`, `patient_refined_master_clinical_v12`, `tumor_pathology`, `path_synoptics`, `ultrasound_reports`, `ct_imaging`, `nuclear_med`, `fna_cytology`, `fna_episode_master_v2`, `operative_episode_detail_v2`, `imaging_patient_summary_v1`, `longitudinal_lab_canonical_v1`, `molecular_results`, `molecular_test_episode_v2`, `specimen_master_v1`, `clinical_notes_long` | Source structured data from clinical databases | YES (scripts 200–211) |
| 2 | `extracted_tirads_validated_v1`, `extracted_braf_recovery_v1`, `extracted_ras_patient_summary_v1`, `thyroid_scoring_py_v1`, `tg_timeline_patient_summary_v1`, `complication_phenotype_v1`, `recurrence_event_clean_v1`, `survival_cohort_enriched`, `rai_treatment_episode_v2`, `ln_master_rollup_v1`, `extracted_rln_injury_refined_v2`, `extracted_postop_labs_expanded_v1` | LLM or deterministic processing of Tier 1 | YES (scripts 207–211) |
| 3 | `note_entities_llm_*` (23 tables), `note_entities_*` (7 tables) | NLP entity extraction from clinical notes | YES as `nlp_` columns (script 212) |
| 4 | `linkage_*`, `val_*`, `review_queue_*`, `*_backup_*`, `imaging_fna_linkage_*`, `surgery_pathology_linkage_*`, `fna_molecular_linkage_*` | Internal linkage and QC tables | **NO** — internal plumbing |
| 5 | `analysis_*`, `manuscript_cohort_*` | Pre-built analysis subsets | **NO** — may be outdated; rebuild from canonical |
| 6 | `fhir_*`, `stg_thyroseq_*`, `rosflow_*` | Other/deprecated/unrelated | **NO** |

---

## C. Script Lineage

| Script | Commit | Purpose | Columns added / action |
|--------|--------|---------|------------------------|
| 200 | ac41da7 | Canonical diagnosis standardization | `diagnosis_primary`, `diagnosis_variant`, `is_malignant`, `diagnosis_full` |
| 201 | ac41da7 | Canonical survival / follow-up | `followup_days`, `followup_years`, `last_contact_date`, `vital_status` |
| 202 | ac41da7 | Canonical molecular tested | `molecular_tested_confirmed`, `mol_platform`, `mol_n_tests` |
| 203 | ac41da7 | Canonical recurrence | `recurrence_confirmed`, `recurrence_type`, `recurrence_date`, `time_to_recurrence_days` |
| 204 | ac41da7 | Canonical master assembly (original 96 columns) | 96 base columns from all Tier 1 sources |
| 205 | bdb0fdb | Consolidation — FNA, TIRADS, Bethesda, LN | `fna_path_outcome`, `preop_tirads_*`, `bethesda_*`, `tp_ln_*` |
| 206 | 192a352 | Fleet NLP validation + upload (171K JSONL rows) | No canonical columns — NLP to `note_entities_llm_*` |
| 207 | cf12d69 | Full canonical expansion (125 → 362 columns) | 237 columns from gold_master, PRM v12, CT, nuclear, imaging_summary, thyroid_scoring, extracted_tirads |
| 208 | 80ee3cf | LN master rollup integration (362 → 407 columns) | 45 `ln_rollup_*` and `ln_level_*` columns from `ln_master_rollup_v1` |
| 209 | ab751b9 | NLP cross-validation report | QC report only — no canonical columns |
| 210 | d90dcdf (partial) | Database audit + backup | QC artifacts — no canonical columns |
| 211 | d90dcdf | Gap-fill from 8 extracted/episode tables | ~129 columns: complications, RLN, ETE, postop labs, RAI episodes, recurrence events, survival, molecular variants |
| 212 | d90dcdf | NLP entity patient-level rollup | ~212 `nlp_*` columns from 26 note_entities tables |
| 213 | pending | Data dictionary + source truth map | Documentation only — no canonical columns |

---

## D. Domain Coverage Summary

| domain | n_columns | mean_coverage_pct | min_coverage_pct | max_coverage_pct | n_100pct | n_below_10pct | key_columns |
|--------|-----------|-------------------|-----------------|-----------------|---------|--------------|-------------|
| eligibility | 1 | 100.0 | 100.0 | 100.0 | 1 | 0 | analysis_eligible_flag |
| demographics | 6 | 100.0 | 99.9 | 100.0 | 5 | 0 | research_id | age_at_surgery | sex |
| survival | 12 | 86.7 | 16.2 | 100.0 | 7 | 0 | last_contact_date | last_contact_source | followup_days |
| scoring | 25 | 75.3 | 0.3 | 100.0 | 15 | 1 | ajcc8_m_stage | ages_score | ames_risk_group |
| fna | 18 | 51.1 | 47.9 | 100.0 | 1 | 0 | fna_path_outcome | bethesda_final | fna_path_concordance_category |
| imaging_us | 14 | 48.7 | 28.1 | 100.0 | 2 | 0 | imaging_ln_abnormal | imaging_suspicious_unconfirmed | n_us_exams |
| pathology | 31 | 46.5 | 0.0 | 100.0 | 8 | 4 | is_malignant | diagnosis_primary | diagnosis_full |
| recurrence | 16 | 44.2 | 0.0 | 100.0 | 6 | 5 | recurrence_confirmed | recurrence_type | recurrence_definition |
| molecular | 63 | 43.4 | 0.1 | 100.0 | 8 | 33 | molecular_tested_confirmed | braf_positive | ras_positive |
| voice | 15 | 40.4 | 0.1 | 100.0 | 6 | 9 | rln_status | rln_permanent_flag | rln_transient_flag |
| tirads | 17 | 31.9 | 31.6 | 32.0 | 0 | 0 | preop_tirads_best | preop_tirads_worst | preop_tirads_category |
| surgery | 16 | 31.7 | 0.0 | 80.3 | 0 | 9 | first_surgery_date | surg_procedure_type | surg_n_procedures |
| lymph_nodes | 76 | 29.0 | 0.1 | 100.0 | 1 | 12 | ln_lateral_dissected | ln_total_examined | ln_positive_flag |
| nlp_other | 14 | 26.2 | 5.6 | 43.4 | 0 | 2 | nlp_ne_procedures_has_data | nlp_ne_procedures_n_rows | nlp_ne_operative_has_data |
| provenance | 268 | 25.0 | 0.0 | 100.0 | 28 | 123 | hypocalcemia_status | hypoparathyroidism_status | chyle_leak_status |
| imaging_ct | 8 | 23.6 | 8.5 | 28.4 | 0 | 1 | ct_n_exams | ct_tracheal_deviation_any | ct_tracheal_narrowing_any |
| lateral_neck | 5 | 20.8 | 0.8 | 100.0 | 1 | 4 | lateral_neck_dissected_v10 | lateral_detection_method | lateral_source_v10 |
| rai | 25 | 16.1 | 0.0 | 100.0 | 3 | 22 | rai_received_flag | rai_max_dose_mci | rai_eligible_flag |
| labs | 33 | 15.8 | 0.0 | 100.0 | 1 | 16 | calcium_supplement_required | tg_trajectory_class | tgab_interference_flag |
| imaging_nuclear | 4 | 7.9 | 0.0 | 10.6 | 0 | 1 | nucmed_n_scans | nucmed_has_rai_scan | nucmed_scan_types |
| complications | 74 | 5.6 | 0.0 | 26.6 | 0 | 59 | comp_voice_permanence_noted | comp_voice_resolution_noted | comp_hypocalcemia_confirmed |
| completion | 7 | 4.3 | 1.0 | 6.3 | 0 | 7 | completion_reason | completion_reason_confidence | completion_braf_positive |

---

## E. Key Clinical Denominators

| Denominator | Value | Column | Notes |
|-------------|-------|--------|-------|
| Total surgical cohort | 10,871 | — | All patients in canonical |
| Analysis-eligible cancer | ~4,136 | `histology_analysis_eligible_flag` | Confirmed malignancy with eligible staging |
| Molecular tested | ~10,025 | `molecular_tested_confirmed` | Any structured molecular test |
| TIRADS documented | ~3,474 | `tirads_best_combined` | At least one structured TIRADS score |
| RAI received | ~862 | `rai_received_flag` | Any RAI treatment documented |
| Recurrence documented | ~1,986 | `any_recurrence` | Any recurrence flag (structural or biochemical) |

---

## F. Column Naming Conventions

| Prefix | Domain | Example |
|--------|--------|---------|
| `demo_` | Demographics | `demo_age_group` |
| `surg_` | Surgery | `surg_total_thyroidectomy` |
| `op_` | Operative detail (NLP) | `op_rln_monitoring_any` |
| `ajcc8_` | AJCC 8th Ed staging | `ajcc8_t_stage`, `ajcc8_stage_group` |
| `ata_` | ATA risk / response | `ata_risk_category` |
| `macis_` | MACIS score | `macis_score` |
| `ln_` | Lymph nodes | `ln_total_examined`, `ln_ratio` |
| `ln_rollup_` | LN rollup (script 208) | `ln_rollup_central_n_examined` |
| `tp_` | tumor_pathology source | `tp_ln_positive` |
| `ene_` | Extranodal extension | `ene_best_grade` |
| `mol_` | Molecular results | `mol_braf_positive` |
| `braf_`, `ras_`, `tert_` | Specific mutations | `braf_positive_final` |
| `rai_` | RAI treatment | `rai_received_flag`, `rai_dose_mci` |
| `tg_` | Thyroglobulin | `tg_nadir`, `tg_rising_flag` |
| `comp_` | Complications | `comp_rln_status` |
| `surv_` | Survival | `surv_time_days`, `surv_event` |
| `rec_` | Recurrence sub | `rec_detection_category` |
| `nlp_llm_` | LLM NLP rollup | `nlp_llm_pathology_ete_grade` |
| `nlp_ne_` | Non-LLM NLP metadata | `nlp_ne_complications_n_rows` |
| `_flag` suffix | Boolean indicator | `aggressive_variant_flag` |
| `_source` suffix | Provenance field | `ete_source_of_truth` |
| `_confidence` suffix | Confidence score | `tirads_reliability` |
| `_eligible_flag` suffix | Eligibility gate | `histology_analysis_eligible_flag` |

---

*Generated by script 213. Re-run to refresh after any canonical update.*
