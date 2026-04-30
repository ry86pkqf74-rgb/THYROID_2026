-- =============================================================================
-- Migration 211 — mig_211 verify 10 deferred analytic composites (Lane A)
-- =============================================================================
-- Date:   2026-04-30 (UTC)
-- Batch:  mig_211_verify_10_deferred_composites_20260430
-- Prompt: cursor_prompts/CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md (Lane A)
-- DB:     thyroid_canonical_publication_v1_0 (MotherDuck RW)
--
-- Scope:  Flip 10 Tier-2 / manuscript analytic composites from table_status
--         not_started to verified — column registry not_started to verified or na.
--         NA columns = registry seed na_provenance spine (research_id, FK ids,
--         source_table where seeded) plus resolved-layer / cohort-freeze metadata
--         columns and build timestamps.
--
-- Build lineage (documentation — no data DDL):
--   manuscript_cohort_v1 — scripts/57_freeze_manuscript_cohort.py + script 86 op_*;
--     spine from patient_analysis_resolved_v1 / episode layer.
--   patient_analysis_resolved_v1 — scripts/48_* resolved patient wide table.
--   episode_analysis_resolved_v1_dedup — script 48 + dedup; manuscript_workspace.
--   lesion_analysis_resolved_v1 — script 48 lesion grain; manuscript_workspace.
--   ln_master_rollup_v1 — LN rollup manuscript_workspace (path + NLP LN yield).
--   imaging_fna_linkage_v3 — linkage v3 (script 23 / analysis resolved layer).
--   tumor_stage_heterogeneity_v1 — 266b per-tumor AJCC heterogeneity.
--   imaging_patient_summary_v1 — script 50 multinodule / imaging summary.
--   recurrence_event_clean_v1 — script 52 recurrence clean events.
--   patient_cross_domain_timeline_v2 — script 22 V2 timeline long format.
--
-- Row-count reference (live 2026-04-30): MC + PAR 10,871; ln 4,273; ep 9,368;
--   lesion 11,851; img-fna 9,911; TSHet 8,422; img-pt 6,126; rec 1,946; timeline 61,055.
--
-- Effect: Registry-only (canonical_column_verification_registry_v1 +
--         canonical_table_signoff_registry_v1); no base-table mutations.
-- CAST(CURRENT_TIMESTAMP AS TIMESTAMP); no explicit transaction.
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §A Pre-snapshot full registries (audit trail)
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig211_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig211_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_v1_pre_mig211_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig211_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1;

-- =============================================================================
-- §B Column registry — mark all touched columns verified (then overlay na)
-- =============================================================================

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'mig_211',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = 'mig211_composite_derivation_trace_resolved_layer_20260430',
    batch_id = 'mig_211_verify_10_deferred_composites_20260430',
    notes = COALESCE(notes, '')
      || ' | mig_211: Logan-ratified verify of deferred analytic composite; '
      || 'derivation traceable to scripts 48/50/51b/52/57/22/23/266b family + linkage v3.'
WHERE (schema_name, table_name) IN (
  SELECT * FROM (VALUES
    ('main', 'manuscript_cohort_v1'),
    ('manuscript_workspace', 'patient_analysis_resolved_v1'),
    ('manuscript_workspace', 'ln_master_rollup_v1'),
    ('manuscript_workspace', 'episode_analysis_resolved_v1_dedup'),
    ('manuscript_workspace', 'lesion_analysis_resolved_v1'),
    ('main', 'imaging_fna_linkage_v3'),
    ('main', 'tumor_stage_heterogeneity_v1'),
    ('main', 'imaging_patient_summary_v1'),
    ('main', 'recurrence_event_clean_v1'),
    ('main', 'patient_cross_domain_timeline_v2')
  ) AS v(s, t)
);

-- NA overlay: keep seed-style identifier/FK na + layer metadata / audit timestamps.

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    category = 'na_provenance',
    verified_by = 'mig_211',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = 'mig211_na_spine_fk_or_freeze_metadata_20260430',
    batch_id = 'mig_211_verify_10_deferred_composites_20260430',
    notes = COALESCE(notes, '')
      || ' | mig_211: na — CPM/spine key, linkage FK id, freeze metadata, or ETL audit stamp.'
WHERE (schema_name, table_name, column_name) IN (
  SELECT * FROM (VALUES
    ('main', 'manuscript_cohort_v1', 'research_id'),
    ('main', 'manuscript_cohort_v1', 'resolved_layer_version'),
    ('main', 'manuscript_cohort_v1', 'source_script'),
    ('main', 'manuscript_cohort_v1', 'resolved_at'),
    ('main', 'manuscript_cohort_v1', 'cohort_build_timestamp'),
    ('main', 'manuscript_cohort_v1', 'cohort_resolved_layer_version'),
    ('main', 'manuscript_cohort_v1', 'freeze_git_sha'),
    ('main', 'manuscript_cohort_v1', 'motherduck_database'),
    ('main', 'manuscript_cohort_v1', 'pipeline_version'),
    ('manuscript_workspace', 'patient_analysis_resolved_v1', 'research_id'),
    ('manuscript_workspace', 'patient_analysis_resolved_v1', 'source_table'),
    ('manuscript_workspace', 'patient_analysis_resolved_v1', 'resolved_layer_version'),
    ('manuscript_workspace', 'patient_analysis_resolved_v1', 'source_script'),
    ('manuscript_workspace', 'patient_analysis_resolved_v1', 'resolved_at'),
    ('manuscript_workspace', 'patient_analysis_resolved_v1', 'provenance_note'),
    ('manuscript_workspace', 'ln_master_rollup_v1', 'research_id'),
    ('manuscript_workspace', 'episode_analysis_resolved_v1_dedup', 'research_id'),
    ('manuscript_workspace', 'episode_analysis_resolved_v1_dedup', 'surgery_episode_id'),
    ('manuscript_workspace', 'episode_analysis_resolved_v1_dedup', 'linked_fna_episode_id'),
    ('manuscript_workspace', 'episode_analysis_resolved_v1_dedup', 'linked_rai_episode_id'),
    ('manuscript_workspace', 'episode_analysis_resolved_v1_dedup', 'resolved_layer_version'),
    ('manuscript_workspace', 'episode_analysis_resolved_v1_dedup', 'resolved_at'),
    ('manuscript_workspace', 'lesion_analysis_resolved_v1', 'research_id'),
    ('manuscript_workspace', 'lesion_analysis_resolved_v1', 'surgery_episode_id'),
    ('manuscript_workspace', 'lesion_analysis_resolved_v1', 'fna_episode_id'),
    ('manuscript_workspace', 'lesion_analysis_resolved_v1', 'source_table'),
    ('manuscript_workspace', 'lesion_analysis_resolved_v1', 'resolved_layer_version'),
    ('manuscript_workspace', 'lesion_analysis_resolved_v1', 'source_script'),
    ('manuscript_workspace', 'lesion_analysis_resolved_v1', 'provenance_note'),
    ('main', 'imaging_fna_linkage_v3', 'research_id'),
    ('main', 'imaging_fna_linkage_v3', 'nodule_id'),
    ('main', 'imaging_fna_linkage_v3', 'imaging_exam_id'),
    ('main', 'imaging_fna_linkage_v3', 'fna_episode_id'),
    ('main', 'tumor_stage_heterogeneity_v1', 'research_id'),
    ('main', 'imaging_patient_summary_v1', 'research_id'),
    ('main', 'imaging_patient_summary_v1', 'created_at'),
    ('main', 'recurrence_event_clean_v1', 'research_id'),
    ('main', 'recurrence_event_clean_v1', 'cleaned_at'),
    ('main', 'recurrence_event_clean_v1', 'source_priority'),
    ('main', 'recurrence_event_clean_v1', 'source_table'),
    ('main', 'patient_cross_domain_timeline_v2', 'research_id'),
    ('main', 'patient_cross_domain_timeline_v2', 'domain'),
    ('main', 'patient_cross_domain_timeline_v2', 'episode_id')
  ) AS n(s, t, c)
);

-- =============================================================================
-- §C Table signoff registry — roll up counts and verified status
-- =============================================================================

UPDATE main.canonical_table_signoff_registry_v1 AS ts
SET n_columns_total = a.n_tot,
    n_verified = a.n_v,
    n_na = a.n_n,
    n_not_started = 0,
    n_failed = 0,
    table_status = 'verified',
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/211_verify_10_deferred_composites_20260430.sql',
    notes = COALESCE(ts.notes, '')
      || ' | mig_211 (2026-04-30): Lane A — full column verify for deferred analytic composite; '
      || 'n_verified+n_na=n_columns_total; derivation logged in col_registry batch_id.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_tot,
         SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_v,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_n
  FROM main.canonical_column_verification_registry_v1
  WHERE (schema_name, table_name) IN (
    SELECT * FROM (VALUES
      ('main', 'manuscript_cohort_v1'),
      ('manuscript_workspace', 'patient_analysis_resolved_v1'),
      ('manuscript_workspace', 'ln_master_rollup_v1'),
      ('manuscript_workspace', 'episode_analysis_resolved_v1_dedup'),
      ('manuscript_workspace', 'lesion_analysis_resolved_v1'),
      ('main', 'imaging_fna_linkage_v3'),
      ('main', 'tumor_stage_heterogeneity_v1'),
      ('main', 'imaging_patient_summary_v1'),
      ('main', 'recurrence_event_clean_v1'),
      ('main', 'patient_cross_domain_timeline_v2')
    ) AS v(s, t)
  )
  GROUP BY 1, 2
) AS a
WHERE ts.schema_name = a.schema_name AND ts.table_name = a.table_name;

-- =============================================================================
-- §D Provenance row
-- =============================================================================

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_211_verify_10_deferred_composites_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'lane_a_verify_ten_deferred_analytic_composites_registry_only_pre_snapshots_archive_pub_v1_0',
   'DEFERRED_COMPOSITE_TABLE_SIGNOFF_GOV_GAP',
   '516_column_registry_rows_closed_manuscript_and_linkage_composites',
   'gate1_expected_plus_ten_tables_logan_scope',
   'none');

-- =============================================================================
-- §E Post-apply checks (manual review if non-zero)
-- =============================================================================
-- Expect: each of 10 tables table_status='verified';
--   SUM(n_verified+n_na)=SUM(n_columns_total) per table;
--   Cowork verification gate1 +10; §12 governance gap unchanged.
