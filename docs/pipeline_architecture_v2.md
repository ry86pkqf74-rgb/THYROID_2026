# Pipeline Architecture v2

## Overview

The THYROID_2026 pipeline processes thyroid cancer research data through extraction, normalization, reconciliation, adjudication, and materialization phases. The v2 upgrade adds 5 new domain-specific parsers, 9 canonical episode-level tables, cross-domain linkage, 10-category QA validation, and 6 new Streamlit dashboard tabs.

## Pipeline Execution Order

```
Phase 1: Ingestion
  01_ingest_all_files.py         Raw Excel -> Parquet
  02_build_duckdb_full.py        Parquet -> DuckDB views
  02b_register_notes_entities.py Note entity parquets

Phase 2: Research Views
  03_research_views.py           ptc_cohort, recurrence views
  04_publication_exports.py      CSV exports

Phase 3: Quality & Extraction
  05_histology_qa.py             Histology standardization
  06_advanced_extraction.py      Nuclear med, pathology mutations
  07_phase3_genetics_specimen.py Genetic testing, specimen, imaging

Phase 4: Integration
  08_integrate_missing_sources.py  Phase 6 integration
  09_motherduck_upload_verify_extract.py  MotherDuck upload
  09b_motherduck_upload_notes_entities.py  Notes upload

Phase 5: Optimization & QA
  10_maximize_motherduck_trial.py  Trial analytics
  11_quality_assurance_crosscheck.py  Master timeline, QA
  11.5_cross_file_validation.py  Cross-file QA
  12_update_streamlit_dashboard.py  Dashboard v3

Phase 6: Adjudication (deploy: 15 -> 16 -> 17 -> 18 -> 19 -> 20)
  15_date_association_audit.py   Date enrichment
  16_reconciliation_v2.py        Episode reconciliation
  17_semantic_cleanup_v3.py      Date status taxonomy
  18_adjudication_framework.py   Adjudication queues
  19_reviewer_persistence.py     Reviewer decisions
  20_manuscript_exports.py       Manuscript views
  21_validation_tests.py         End-to-end validation

Phase 7: v2 Upgrade (deploy: 22 -> 23 -> 24 -> 25 -> 26)
  22_canonical_episodes_v2.py    9 canonical episode tables
  23_cross_domain_linkage_v2.py  Cross-domain linkage
  24_reconciliation_review_v2.py 5 reconciliation review views
  25_qa_validation_v2.py         10-category QA
  26_motherduck_materialize_v2.py  MotherDuck materialization
```

## Canonical Data Model (v2)

### Grain Definitions

| Table | Grain | Primary Key |
|-------|-------|-------------|
| tumor_episode_master_v2 | One row per tumor per surgery | (research_id, surgery_episode_id, tumor_ordinal) |
| molecular_test_episode_v2 | One row per molecular test | (research_id, molecular_episode_id) |
| rai_treatment_episode_v2 | One row per RAI treatment | (research_id, rai_episode_id) |
| imaging_nodule_long_v2 | One row per nodule per exam | (research_id, imaging_exam_id, nodule_id) |
| imaging_exam_summary_v2 | One row per imaging exam | (research_id, modality, imaging_exam_id) |
| operative_episode_detail_v2 | One row per surgery | (research_id, surgery_episode_id) |
| fna_episode_master_v2 | One row per FNA | (research_id, fna_episode_id) |
| event_date_audit_v2 | One row per extracted fact | (domain, research_id, source_table) |
| patient_cross_domain_timeline_v2 | One row per event | (research_id, event_type, event_date) |

### Extraction Precedence Rules

For histology/pathology:
1. Structured synoptic fields (path_synoptics) -- highest confidence
2. Structured tumor pathology fields (tumor_pathology) -- high confidence
3. Expert consult diagnosis -- high confidence, overrides original when present
4. Diagnostic narrative text -- moderate confidence
5. Note-derived mentions -- lowest confidence

For molecular testing:
1. Structured molecular_testing table fields
2. Note-extracted genetics entities

For dates:
1. Source-native date (entity_date) -> exact_source_date, confidence 100
2. Note encounter date (note_date) -> inferred_day_level_date, confidence 70
3. Linked surgery/FNA/molecular date -> coarse_anchor_date, confidence 35-60
4. No available date -> unresolved_date, confidence 0

### Confidence Scoring

| Date Status | Base Confidence | Multi-source Max |
|-------------|----------------|-------------------|
| exact_source_date | 100 | 100 |
| inferred_day_level_date | 70 | 70 |
| coarse_anchor_date | 50 | 60 (boosted by +5 per additional source) |
| unresolved_date | 0 | 0 |

### Cross-Domain Linkage Tiers

| Tier | Criteria |
|------|----------|
| exact_match | Same date AND compatible laterality |
| high_confidence | Date within 7 days, or same date different laterality |
| plausible | Date within 90 days AND compatible laterality |
| weak | Date within 365 days |
| unlinked | No linkable counterpart |

## Extraction Modules (v2)

| Module | Entity Domain | Entity Types |
|--------|--------------|-------------|
| extract_molecular_v2.py | molecular_detail | molecular_platform, specimen_adequacy, result_classification, mutation_*, copy_number_alteration, gene_fusion, loh, classifier_result, risk_probability, bethesda_mention |
| extract_rai_v2.py | rai_detail | rai_dose, rai_intent, rai_completion, rai_pre_scan, rai_post_scan, rai_avidity, rai_scan_finding, rai_stimulated_tg, rai_stimulated_tsh, rai_uptake_pct |
| extract_imaging_v2.py | imaging_detail | nodule_size, composition, echogenicity, nodule_shape, nodule_margins, calcifications, vascularity, tirads_score, suspicious_lymph_node, interval_change, multinodular_goiter, thyroiditis, imaging_ete, dominant_nodule, nodule_laterality |
| extract_operative_v2.py | operative_detail | rln_finding, nerve_monitoring, parathyroid_autograft, parathyroid_management, gross_invasion, strap_muscle, tracheal_involvement, esophageal_involvement, reoperative_field, ebl, drain_placement, specimen_detail, berry_ligament, intraop_complication |
| extract_histology_v2.py | histology_detail | capsular_invasion, perineural_invasion, extranodal_extension, vascular_invasion_detail, lymphatic_invasion_detail, margin_status, consult_diagnosis, histology_subtype, multifocality, niftp, pdtc_features, aggressive_features, tumor_count, lymph_node_count, extrathyroidal_extension_detail |

## QA Validation Categories

1. **Histology reconciliation mismatch** -- conflicting histology/staging across sources
2. **Molecular chronology mismatch** -- post-surgery molecular test without context
3. **RAI chronology mismatch** -- RAI before surgery (non-historical)
4. **Nodule-FNA mismatch** -- imaging-FNA laterality disagreement
5. **Imaging-pathology mismatch** -- laterality/size discrepancy
6. **Op-pathology mismatch** -- procedure vs specimen inconsistency
7. **Parathyroid consistency** -- autograft in notes but not in operative record
8. **Date completeness** -- % exact/inferred/coarse/unresolved per domain
9. **Duplicate event detection** -- same-date duplicates within domain
10. **Missing-but-derivable** -- data available in one source but missing in canonical table

## Review Queue Meanings

| Review View | Issue Types | Severity |
|-------------|------------|----------|
| pathology_reconciliation_review_v2 | histology_mismatch, staging_mismatch, consult_precedence_needed | error/warning/info |
| molecular_linkage_review_v2 | unlinked_test, post_surgery_test, inadequate_specimen, cancelled_test, missing_date | warning/info |
| rai_adjudication_review_v2 | rai_before_surgery, implausible_low_dose, high_dose_review, ambiguous_assertion, recommended_no_completion, missing_date | error/warning/info |
| imaging_pathology_concordance_review_v2 | laterality_and_size, laterality_only, size_only | error/warning |
| operative_pathology_reconciliation_review_v2 | op_path_laterality, cnd_no_nodal_path, nodal_path_no_cnd, bilateral_in_lobectomy | error/warning |

## MotherDuck Materialization

All v2 tables are materialized with `md_` prefix via script 26. The mapping preserves the same schema. Streamlit dashboards query with fallback: try source table name first, then `md_` prefix.

## Streamlit Dashboard Additions (v2)

| Tab | Module | Data Sources |
|-----|--------|-------------|
| Extraction v2 | extraction_completeness.py | qa_date_completeness_v2, event_date_audit_v2, all v2 canonical tables |
| Molecular v2 | molecular_dashboard.py | molecular_test_episode_v2, molecular_linkage_review_v2 |
| RAI v2 | rai_dashboard.py | rai_treatment_episode_v2, rai_adjudication_review_v2 |
| Imaging/Nodule v2 | imaging_nodule_dashboard.py | imaging_nodule_long_v2, imaging_exam_summary_v2, imaging_pathology_concordance_review_v2 |
| Operative v2 | operative_dashboard.py | operative_episode_detail_v2, operative_pathology_reconciliation_review_v2 |
| Adjudication v2 | adjudication_summary.py | qa_summary_by_domain_v2, qa_high_priority_review_v2, linkage_summary_v2, manual_review_queue_summary_v2 |

## Known Limitations

- Imaging nodule unpivot from wide-format ultrasound currently only extracts nodule_1; full multi-nodule support requires additional wide-column discovery logic
- CT/MRI nodule extraction is basic (presence/absence) due to less structured source data
- Operative note parsing relies on regex; complex narrative structures may be missed
- FNA-to-molecular linkage uses temporal proximity; specimen-level identifiers are not available in source data
- RAI dose extraction may miss non-standard dose formats or doses embedded in complex narrative
- Cross-modality nodule matching (same lesion across US and CT) uses temporal + laterality heuristics only
- Pre-1999 events may appear in timelines for patients with historical surgical context

## Data Tier Conventions

- **Dashboard tabs** use pre-adjudication and QA views for review workflows. Reviewers see algorithmic values and can submit adjudication decisions through Review Mode.
- **Manuscript exports** (script 20) prefer post-review views (`histology_post_review_v`, `molecular_post_review_v`, `rai_post_review_v`) when available, falling back to algorithmic analysis cohorts.
- **V2 canonical tables** (scripts 22-26) are independent of Phase 6 adjudication. They read from raw source tables, not from post-review views. If downstream analysis requires V2 grain + adjudicated values, a bridge layer joining V2 tables with `adjudication_decisions` is needed.

## Date Format Coverage

`extract_nearby_date` (in `utils/text_helpers.py`) supports:
- `MM/DD/YYYY` and `MM/DD/YY` (slash-separated)
- `YYYY-MM-DD` (ISO 8601)
- Month-name formats: `January 15, 2024` and `15 January 2024` (full or abbreviated)

Year bounds: 1990-2030. Dates outside this range are discarded. European `DD.MM.YYYY` is not supported.

## Weak Linkage QA Routing

Linkages with `weak` confidence tier are routed to `qa_issues_v2` as `warning` severity. QA check IDs:
- `weak_linkage_imaging_fna`
- `weak_linkage_fna_molecular`
- `weak_linkage_preop_surgery`
- `weak_linkage_pathology_rai`

These should be reviewed by domain experts to confirm or reject the linkage.
