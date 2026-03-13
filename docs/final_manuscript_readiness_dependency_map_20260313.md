# Final Manuscript Readiness Dependency Map

**Date:** 2026-03-13
**MotherDuck:** 578 distinct tables in `thyroid_research_2026.main`

---

## Pipeline Execution Order

### Phase 1: Ingestion (Scripts 01-02b)
```
01_ingest_all_files.py → raw Excel → processed/*.parquet
02_build_duckdb_full.py → local thyroid_master.duckdb
02b_register_notes_entities.py → 6 note_entities_* tables
```

### Phase 2: Research Views (Scripts 03-14)
```
03_research_views.py --md → ptc_cohort, recurrence_risk_cohort, genetic_testing_clean, advanced_features_v*
09_motherduck_upload_verify_extract.py → MotherDuck base tables
09b_motherduck_upload_notes_entities.py --confirm → MotherDuck note_entities_*
10_maximize_motherduck_trial.py → patient_level_summary_mv, survival_cohort_ready_mv, master_cohort
11_quality_assurance_crosscheck.py → master_timeline, extracted_clinical_events_v4, qa_issues
11.5_cross_file_validation.py → demographics_harmonized_v2
```

### Phase 3: Reconciliation (Scripts 15-20)
```
15_date_association_audit.py --md → 6 enriched_note_entities_* views
16_reconciliation_v2.py --md → histology_reconciliation_v2, molecular_episode_v2, rai_episode_v2
17_semantic_cleanup_v3.py --md → validation_failures_v3, date status taxonomy
18_adjudication_framework.py --md → molecular_episode_v3, rai_episode_v3, streamlit_* views
19_reviewer_persistence.py --md → adjudication_decisions, post-review overlays
20_manuscript_exports.py --md → manuscript_*_cohort_v views
```

### Phase 4: V2 Canonical (Scripts 22-27)
```
22_canonical_episodes_v2.py --md → 9 canonical episode tables (tumor, molecular, RAI, imaging, operative, FNA)
23_cross_domain_linkage_v2.py --md → 6 linkage tables (imaging-FNA, FNA-mol, preop-surg, surg-path, path-RAI)
24_reconciliation_review_v2.py --md → 5 reconciliation review queues
25_qa_validation_v2.py --md → qa_issues_v2, qa_summary_by_domain_v2
27_fix_legacy_episode_compatibility.py --md → legacy bridge tables
```

### Phase 5: Materialization Hub
```
26_motherduck_materialize_v2.py --md → 209+ md_* materialized tables
```

### Phase 6: Analysis-Grade Resolved Layer (Scripts 48-57)
```
51b_thyroid_scoring_python.py --md → thyroid_scoring_py_v1 (AJCC8, ATA, MACIS, AGES, AMES)
50_multinodule_imaging.py --md → imaging_nodule_master_v1, imaging_exam_master_v1, imaging_patient_summary_v1
49_enhanced_linkage_v3.py --md → 5 v3 linkage tables with numeric scoring
52_complication_phenotyping_v2.py --md → complication_phenotype_v1, complication_patient_summary_v1
53_longitudinal_lab_hardening.py --md → longitudinal_lab_clean_v1, recurrence_event_clean_v1
48_build_analysis_resolved_layer.py --md → patient/episode/lesion_analysis_resolved_v1
57_freeze_manuscript_cohort.py --md → manuscript_cohort_v1
```

### Phase 7: Gap Closure & Hardening (Scripts 70-78)
```
70_canonical_backfill.py --md → RAI dose, RAS, linkage ID backfill
71_operative_nlp_to_motherduck.py --md → operative_episode_detail_v2 NLP enrichment
75_dataset_maturation.py --md --all → val_dataset_integrity_summary_v1, val_provenance_completeness_v2
76_canonical_gap_closure.py --md --phase all → RAI dose provenance, RAS subtypes, linkage IDs, recurrence dates
77_lab_canonical_layer.py --md → longitudinal_lab_canonical_v1
78_final_hardening.py --md --phase all → imaging-FNA linkage, RAI missingness, lab validation
```

### Phase 8: Validation & Verification (Scripts 29, 46, 55, 67)
```
29_validation_engine.py --md → 16 val_* validation tables
46_provenance_audit.py --md → provenance_enriched_events_v1, lineage_audit_v1
55_analysis_validation_suite.py --md → val_analysis_resolved_v1
67_database_hardening_validation.py --md → 9 hardening validation tables
```

### Phase 9: Manuscript Outputs (Scripts 58-66)
```
58_missingness_summary.py --md → missingness report
62_run_primary_descriptives.py --md → Tables 1-3
63_run_primary_models.py --md → logistic regression models
64_run_survival_analyses.py --md → KM + Cox PH
65_generate_manuscript_tables.py → LaTeX + markdown tables
66_generate_manuscript_figures.py --md → 300 DPI publication figures
```

---

## Manuscript-Critical Tables (Tier 1)

| Table | Rows | Script | Key Fields |
|-------|------|--------|------------|
| `manuscript_cohort_v1` | 10,871 | 57 | research_id, age_at_surgery, sex, race, analysis_eligible_flag |
| `patient_analysis_resolved_v1` | 10,871 | 48 | Unified per-patient with source provenance |
| `episode_analysis_resolved_v1_dedup` | 9,368 | 48 | Deduplicated surgery episodes |
| `thyroid_scoring_py_v1` | 10,871 | 51b | AJCC8, ATA, MACIS, AGES, AMES |
| `complication_phenotype_v1` | 5,928 | 52 | Structured complication events |
| `survival_cohort_enriched` | 61,134 | 26 | Survival modeling with covariates |

## Secondary Tables (Tier 2)

| Table | Rows | Script | Purpose |
|-------|------|--------|---------|
| `operative_episode_detail_v2` | 9,371 | 22 | Surgery details + NLP enrichment |
| `imaging_nodule_master_v1` | 19,891 | 50 | Per-nodule US with TIRADS |
| `rai_treatment_episode_v2` | 1,857 | 22 | RAI episodes + dose |
| `molecular_test_episode_v2` | 10,126 | 22 | Molecular testing episodes |
| `longitudinal_lab_canonical_v1` | 39,961 | 77 | Lab timeline (5 analytes) |
| `extracted_recurrence_refined_v1` | 10,871 | Phase 8 | Recurrence with date tiers |
| `patient_refined_master_clinical_v12` | 12,886 | Phase 13 | Master clinical (172 cols) |

## Provenance/Date Fields Enter At

| Domain | Source Script | Provenance Columns | Where Lost |
|--------|-------------|-------------------|------------|
| Pathology | 22 (tumor_episode) | date_status, date_confidence | Preserved through resolved layer |
| Molecular | 22 (molecular_episode) | date_status, molecular_date_raw_class | Preserved |
| RAI | 22 (rai_episode) | date_status, dose_source, dose_confidence | Preserved |
| Recurrence | Phase 8 engine | recurrence_date_status, recurrence_date_confidence | Preserved |
| Labs (Tg) | 77 (lab_canonical) | lab_date_status, source_table | Preserved |
| Labs (non-Tg) | 77 (lab_canonical) | lab_date_status=note_date_fallback | No structured lab date |
| Operative | 22/76 | note_date_source, note_date_confidence | NLP fields at 0% (source limitation) |
