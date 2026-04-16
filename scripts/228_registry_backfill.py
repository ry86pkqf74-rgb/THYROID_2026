#!/usr/bin/env python3
"""
Script 228: detail_table_registry_v1 Backfill
Canonical DB: thyroid_canonical_publication_v1_0
Model: Claude Haiku 4.5

Registers all previously-unregistered drill-down tables in main schema into
manuscript_workspace.detail_table_registry_v1.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from motherduck_client import get_token

import duckdb

token = get_token()
if not token:
    raise RuntimeError("No MotherDuck token found — check motherduck.local.toml")

con = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}")
print(f"Connected to thyroid_canonical_publication_v1_0")

# ── Registry entries ──────────────────────────────────────────────────────────
# (detail_table_name, schema_name, grain, domain, feeds_cols, description)
registry_entries = [
    ('canonical_patient_master', 'main', 'one row per patient (THE MASTER)', 'Master',
     'ALL 1,463 columns',
     'The analytic spine. 10,871 patients x 1,463 cols. Every drill-down in this registry feeds one or more columns here.'),
    ('canonical_diagnosis_unified_v1', 'main', 'one row per patient', 'Diagnosis',
     'diagnosis_primary, diagnosis_variant, diagnosis_full, is_malignant',
     'Unified diagnosis rollup combining benign + malignant sources.'),
    ('canonical_benign_diagnosis_v1', 'main', 'one row per patient', 'Diagnosis',
     'syn_graves, syn_hashimoto, syn_multinodular_goiter, has_follicular_adenoma, other benign flags',
     'Benign diagnosis phenotype flags.'),
    ('canonical_malignant_diagnosis_v1', 'main', 'one row per patient', 'Diagnosis',
     'histology_final, histology_source, aggressive_variant_flag',
     'Canonical malignant histology with variant detail.'),
    ('canonical_molecular_tested_v1', 'main', 'one row per patient', 'Molecular',
     'molecular_tested_confirmed, mol_platform, mol_has_thyroseq, mol_has_afirma, braf_positive_canonical',
     'Patient-level molecular testing summary.'),
    ('canonical_recurrence_v1', 'main', 'one row per patient', 'Recurrence',
     'recurrence_confirmed, recurrence_type, recurrence_site, recurrence_date, recurrence_definition',
     'Canonical recurrence with type and site.'),
    ('canonical_survival_followup_v1', 'main', 'one row per patient', 'Survival',
     'followup_years, followup_days, last_contact_date, vital_status, death_date',
     'Canonical follow-up and vital status.'),
    ('tg_timeline_patient_summary_v1', 'main', 'one row per patient', 'Labs',
     'tg_nadir, tg_peak, tg_mean, tg_rising_flag, tg_trajectory_class, tg_n_measurements',
     'Thyroglobulin kinetics rollup with trajectory class.'),
    ('tg_postop_surveillance_windows_v1', 'main', 'one row per patient', 'Labs',
     'post_rai_tg_nadir, post_rai_tg_last, max_stimulated_tg',
     'Tg windowed values tied to RAI timing.'),
    ('complication_patient_summary_v1', 'main', 'one row per patient', 'Complications',
     'any_confirmed_complication, n_confirmed_complications, n_analysis_eligible_complication',
     'Patient-level complication rollup (input to CPM comp_* columns).'),
    ('imaging_patient_summary_v1', 'main', 'one row per patient', 'Imaging',
     'n_us_exams, bilateral_disease_flag, dominant_nodule_size_cm, longitudinal_assessment_available, max_tirads_ever',
     'Imaging rollup across all modalities.'),
    ('imaging_nodule_master_v1', 'main', 'one row per nodule', 'Imaging',
     'imaging_n_nodule_records, tirads_worst_combined, tirads_best_combined',
     'Nodule-level imaging detail with combined TIRADS.'),
    ('imaging_exam_master_v1', 'main', 'one row per imaging exam', 'Imaging',
     'us_n_reports, exam-level linkage',
     'Exam-level imaging index.'),
    ('imaging_fna_linkage_v3', 'main', 'one row per imaging-FNA link', 'Linkage',
     'crosslink (no direct CPM column)',
     'Imaging <-> FNA crosslink table.'),
    ('tirads_llm_extracted_v2', 'main', 'one row per nodule (LLM)', 'Imaging',
     'tirads_worst_combined, tirads_best_combined (via combined CTE)',
     'LLM-extracted TIRADS scores from ultrasound report text. 5,631 nodules.'),
    ('tirads_llm_validation_v2', 'main', 'one row per validation', 'Imaging',
     'TIRADS validation signals',
     'Validation pairs comparing LLM-extracted vs structured TIRADS.'),
    ('extracted_tirads_validated_v1', 'main', 'one row per nodule', 'Imaging',
     'tirads component scores (composition, echogenicity, shape, margin, foci)',
     'Fully parsed per-component TIRADS. Input to tirads_best_score_v12.'),
    ('extracted_braf_recovery_v1', 'main', 'one row per patient', 'Molecular',
     'braf_recovered_status_v11, braf_recovered_variant_v11, braf_detection_method_v11',
     'BRAF recovery from NLP and secondary sources.'),
    ('extracted_ras_patient_summary_v1', 'main', 'one row per patient', 'Molecular',
     'ras_positive_v11, ras_primary_subtype_v11, ras_allele_freq_v11, ras_protein_change_v11',
     'Patient-level RAS summary with variant detail.'),
    ('extracted_ete_subgraded_v1', 'main', 'one row per patient', 'Pathology',
     'ete_refined_grade, ete_subgrade_method, ete_subgrade_note',
     'ETE subgraded from path text. 3,558 patients.'),
    ('extracted_fna_bethesda_v1', 'main', 'one row per patient', 'FNA',
     'bethesda_final, bethesda_category, fna_bethesda_source, fna_bethesda_confidence',
     'Canonical Bethesda patient-level rollup.'),
    ('extracted_complications_refined_v5', 'main', 'one row per complication mention', 'Complications',
     'upstream for complication_phenotype_v1',
     'NLP-extracted complication mentions, refined.'),
    ('extracted_rln_injury_refined_v2', 'main', 'one row per RLN event', 'Complications',
     'rln_status, rln_injury_type, rln_permanent_flag, rln_transient_flag',
     'RLN injury events with temporality.'),
    ('ete_adjudication_v1', 'main', 'one row per patient', 'Pathology',
     'ete_grade_adjudicated, ete_adjudication_confidence, ete_adjudication_evidence',
     'ETE adjudication decisions.'),
    ('path_outcome_classification_v1', 'main', 'one row per patient', 'Pathology',
     'fna_path_outcome, fna_path_concordant, fna_path_concordance_category',
     'Classification of FNA->path outcome concordance.'),
    ('patient_tumor_rollup_v1', 'main', 'one row per patient', 'Pathology',
     'tumor_size_cm_max, tumor_size_cm_dominant, multifocal_flag_path, has_right_tumor, has_left_tumor, bilateral_path_flag',
     'Patient-level tumor rollup from synoptic_tumor_long_v1.'),
    ('patient_completion_oed_path_linkage_v1', 'main', 'one row per patient', 'Pathology',
     'completion_braf_positive, completion_reason, completion_histology_type, completion_t_stage',
     'Completion thyroidectomy linkage with path.'),
    ('patient_analysis_resolved_v1', 'main', 'one row per patient', 'Analysis',
     'resolved_at, resolved_layer_version, analysis_eligible_flag',
     'Resolved analysis layer with provenance.'),
    ('lesion_analysis_resolved_v1', 'main', 'one row per lesion', 'Analysis',
     'lesion-level (no direct CPM column)',
     'Resolved lesion-level analysis.'),
    ('episode_analysis_resolved_v1_dedup', 'main', 'one row per episode', 'Analysis',
     'episode-level (no direct CPM column)',
     'Resolved episode analysis, deduplicated.'),
    ('manuscript_cohort_v1', 'main', 'one row per patient', 'Manuscript',
     'manuscript-ready cohort',
     'Pre-filtered manuscript cohort with all necessary flags.'),
    ('analysis_molecular_subset_v1', 'main', 'one row per molecular-tested patient', 'Analysis',
     'subset view',
     'Filtered to molecular-tested patients.'),
    ('survival_cohort_enriched', 'main', 'one row per patient', 'Survival',
     'surv_max_time_days, surv_n_events, surv_recurrence_risk_band, surv_tg_annual_log_slope',
     'Enriched survival cohort with time-to-event.'),
    ('nsqip_patient_summary', 'main', 'one row per patient', 'NSQIP',
     'nsqip_* columns (full set of 80+)',
     'NSQIP patient-level summary. Covers 1,410 patients.'),
    ('nsqip_enrichment', 'main', 'one row per NSQIP match', 'NSQIP',
     'upstream for nsqip_patient_summary',
     'NSQIP enrichment with match metadata.'),
    ('fna_history', 'main', 'one row per FNA', 'FNA',
     'upstream source for fna_cytology',
     'FNA historical records. 5,266 patients.'),
    ('ln_crossval_v1', 'main', 'one row per patient', 'Lymph Nodes',
     'ln_rollup_crossval_status, ln_rollup_internal_consistency',
     'LN cross-validation across sources.'),
    ('molecular_results', 'main', 'one row per molecular test result', 'Molecular',
     'upstream for molecular_test_episode_v2',
     'Raw molecular test results.'),
    ('molecular_testing', 'main', 'one row per molecular test', 'Molecular',
     'upstream source',
     'Legacy molecular testing table.'),
    ('molecular_assay_dictionary', 'main', 'reference table', 'Molecular',
     'reference (no direct CPM column)',
     'Assay metadata dictionary.'),
    ('molecular_code_crosswalk', 'main', 'reference table', 'Molecular',
     'reference (no direct CPM column)',
     'Molecular code mapping table.'),
    ('molecular_ingestion_runs', 'main', 'one row per ingestion run', 'Molecular',
     'provenance (no direct CPM column)',
     'Molecular ingestion run log.'),
    ('thyroseq_molecular_enrichment', 'main', 'one row per ThyroSeq test', 'Molecular',
     'preop_sweep_genes_found_v11',
     'ThyroSeq preop enrichment detail.'),
    ('specimen_genomic_assay_v1', 'main', 'one row per specimen-assay', 'Molecular',
     'specimen->assay linkage',
     'Genomic assay linked to specimens.'),
    ('specimen_source_xref_v1', 'main', 'one row per specimen source', 'Pathology',
     'specimen crosswalk',
     'Specimen source crosswalk.'),
    ('clinical_notes_long', 'main', 'one row per clinical note', 'Notes',
     'upstream for all note_entities_* tables',
     'Full clinical notes. 11,050 notes across 8 note types.'),
    ('note_entities_problem_list', 'main', 'one row per problem list entity', 'NLP',
     'pmhx_nlp_* columns (indirectly)',
     'Problem list entities from NLP.'),
    ('note_entities_procedures', 'main', 'one row per procedure entity', 'NLP',
     'proc_nlp_* columns',
     'Procedure entities from NLP.'),
    ('note_entities_staging', 'main', 'one row per staging entity', 'NLP',
     'TNM mentions (indirectly)',
     'TNM staging mentions from NLP.'),
    ('note_entities_llm_airway_invasion', 'main', 'one row per airway entity', 'NLP',
     'nlp_airway_* columns',
     'LLM airway invasion entities.'),
    ('note_entities_llm_dynamic_risk_response', 'main', 'one row per DRR entity', 'NLP',
     'nlp_dynrisk_* columns',
     'Dynamic risk response entities.'),
    ('note_entities_llm_functional_outcomes', 'main', 'one row per func outcome entity', 'NLP',
     'nlp_funcoutcome_* columns',
     'Functional outcome entities.'),
    ('note_entities_llm_imaging', 'main', 'one row per imaging entity', 'NLP',
     'nlp_imaging_* columns',
     'Imaging mentions from NLP.'),
    ('note_entities_llm_labs', 'main', 'one row per lab entity', 'NLP',
     'nlp_labs_* columns',
     'Lab value mentions from NLP.'),
    ('note_entities_llm_past_medical_hx', 'main', 'one row per PMHx entity', 'NLP',
     'pmhx_nlp_* columns (diabetes, HTN, obesity, radiation, etc.)',
     'Past medical history LLM extraction.'),
    ('note_entities_llm_past_surgical_hx', 'main', 'one row per PSHx entity', 'NLP',
     'pshx_nlp_* columns (prior thyroidectomy, FNA, neck surgery, RAI)',
     'Past surgical history LLM extraction.'),
    ('note_entities_llm_patient_decision_adherence', 'main', 'one row per ptdecision entity', 'NLP',
     'nlp_ptdecision_* columns',
     'Patient decision and adherence.'),
    ('note_entities_llm_physical_exam', 'main', 'one row per physexam entity', 'NLP',
     'nlp_physexam_* columns',
     'Physical exam findings.'),
    ('note_entities_llm_presenting_symptoms', 'main', 'one row per symptom entity', 'NLP',
     'sx_nlp_* columns (hoarseness, dysphagia, neck mass, dyspnea)',
     'Presenting symptoms.'),
    ('note_entities_llm_rad_treatment', 'main', 'one row per radtx entity', 'NLP',
     'radtx_nlp_* columns',
     'Radiation treatment entities.'),
    ('note_entities_llm_survival_followup', 'main', 'one row per survival entity', 'NLP',
     'nlp_survfu_* columns',
     'Survival/follow-up mentions.'),
    ('note_entities_llm_synoptic_pathology_enrichment', 'main', 'one row per synoptic entity', 'NLP',
     'nlp_synoptic_* columns',
     'Synoptic pathology enrichment.'),
    ('note_entities_llm_us_nodule_dynamics', 'main', 'one row per US nodule entity', 'NLP',
     'nlp_usnodule_* columns',
     'US nodule dynamics over time.'),
    ('note_entities_llm_vascular_invasion', 'main', 'one row per vasc entity', 'NLP',
     'nlp_vasc_* columns, vascular_invasion_grade',
     'Vascular invasion NLP entities.'),
    ('thyroid_sizes', 'main', 'one row per patient', 'Pathology',
     'us_total_volume_ml, us_left_lobe_volume_ml, us_right_lobe_volume_ml',
     'Standardized thyroid size measurements.'),
    ('thyroid_weights', 'main', 'one row per patient', 'Pathology',
     'gland_weight_final_g, gland_weight_source, gland_weight_left_lobe_g, gland_weight_right_lobe_g',
     'Gland weight data from path + NLP.'),
    ('lab_cross_wave_dedup_map_v1', 'main', 'one row per lab dedup pair', 'Labs',
     'dedup crosswalk (no direct CPM column)',
     'Lab cross-wave dedup map.'),
]

# ── Backfill loop ─────────────────────────────────────────────────────────────
n_registered = 0
n_skipped = 0
n_missing = 0

for (tbl, sch, grain, domain, feeds, desc) in registry_entries:
    exists = con.execute(f"""
        SELECT COUNT(*) FROM manuscript_workspace.detail_table_registry_v1
        WHERE detail_table_name = '{tbl}'
    """).fetchone()[0]
    if exists > 0:
        print(f"  skip (already registered): {tbl}")
        n_skipped += 1
        continue

    # Check table exists in DB before trying to count rows
    tbl_exists = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT table_name AS t FROM duckdb_tables()
            WHERE database_name = 'thyroid_canonical_publication_v1_0'
              AND schema_name = '{sch}' AND table_name = '{tbl}'
            UNION ALL
            SELECT view_name AS t FROM duckdb_views()
            WHERE database_name = 'thyroid_canonical_publication_v1_0'
              AND schema_name = '{sch}' AND view_name = '{tbl}'
        )
    """).fetchone()[0]

    if tbl_exists == 0:
        print(f"  MISSING (not in DB): {tbl}")
        n_missing += 1
        continue

    try:
        cnt = con.execute(f'SELECT COUNT(*), COUNT(DISTINCT research_id) FROM "{sch}"."{tbl}"').fetchone()
        n_rows, n_pts = cnt[0], cnt[1]
    except Exception:
        # Reference tables without research_id
        try:
            n_rows = con.execute(f'SELECT COUNT(*) FROM "{sch}"."{tbl}"').fetchone()[0]
        except Exception as e:
            print(f"  ERROR counting {tbl}: {e}")
            n_missing += 1
            continue
        n_pts = None

    con.execute("""
        INSERT INTO manuscript_workspace.detail_table_registry_v1
        (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
         domain, feeds_master_columns, description, canonical_version)
        VALUES (?, ?, 'research_id', ?, ?, ?, ?, ?, ?, 'v1_0')
    """, [tbl, sch, grain, n_rows, n_pts, domain, feeds, desc])
    print(f"  registered: {tbl} ({n_rows:,} rows, {n_pts} patients)")
    n_registered += 1

print()
print(f"Registered {n_registered} new tables")
print(f"Skipped    {n_skipped} already-registered tables")
if n_missing:
    print(f"Missing    {n_missing} tables (not found in DB)")

# ── Final verification ────────────────────────────────────────────────────────
result = con.execute("""
    SELECT COUNT(DISTINCT detail_table_name), COUNT(*)
    FROM manuscript_workspace.detail_table_registry_v1
""").fetchone()
print(f"\nRegistry total: {result[0]} distinct / {result[1]} total entries")
assert result[0] == result[1], "ERROR: Duplicates detected in registry!"

unregistered = con.execute("""
    WITH base AS (
        SELECT table_name AS table_name FROM duckdb_tables()
        WHERE database_name = 'thyroid_canonical_publication_v1_0'
          AND schema_name = 'main'
          AND table_name NOT LIKE '\_%' ESCAPE '\\'
          AND table_name != '__readme'
        UNION
        SELECT view_name AS table_name FROM duckdb_views()
        WHERE database_name = 'thyroid_canonical_publication_v1_0'
          AND schema_name = 'main'
    )
    SELECT base.table_name FROM base
    LEFT JOIN manuscript_workspace.detail_table_registry_v1 dtr
        ON dtr.detail_table_name = base.table_name AND dtr.schema_name = 'main'
    WHERE dtr.detail_table_name IS NULL
    ORDER BY base.table_name
""").fetchall()

if unregistered:
    print(f"\n{len(unregistered)} tables still unregistered:")
    for (u,) in unregistered:
        print(f"    {u}")
else:
    print("\nEvery base table/view in main schema is registered")

con.close()
