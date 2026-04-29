-- =============================================================================
-- Migration 129 — manuscript_workspace tier3_helper batch (Protocol v2 / Lane 21)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Scope:  16 small helper tables in manuscript_workspace (adjudication queues / logs,
--         backfills, QC audits, mapping/gap artifacts, meta __conventions). Not
--         first-class canonical fact tables; pragmatic category-driven verification.
--
-- Live MotherDuck probes (thyroid_canonical_publication_v1_0, pre-apply 2026-04-29):
--   * Row counts: biochemical_concern_backfill_v1 N=1659; nucmed_tgab_max N=2994;
--       registry_v2_cpm_cols_without_registry N=950 distinct column_name keys;
--       hypopara_queue N=4; path_stage_raw N=4083; us_llm_absorption_gap N=60;
--       verification_low_concordance N=17; cpm_ete_self_contradiction_queue N=2790;
--       hypopara_log N=4; qc_event_issues N=6147; us_llm_absorption_deferred_multi_nodule N=825;
--       __conventions N=16; cohort_view_duplicate_review N=8; max_stimulated_tg_backfill N=238;
--       recurrence_path_proven_candidates N=191; tier2_completeness N=22.
--   * cpm_ete_self_contradiction_queue_v1: LEFT JOIN canonical_patient_master on research_id:
--       0 orphan keys (referential integrity vs CPM spine).
--   * verification_low_concordance_v1.pct_agree: observed range [0, 0.6931] (fractional concordance).
--   * tier2_completeness_v1: 0 NULL has_tier2_event_table boolean (derived flag populated).
--   * nucmed_tgab_max_backfill_v1.max_tgab: range [0.4, 25600] plausible lab magnitudes.
--   * biochemical_concern_backfill_v1: 1372/1659 rows join to CPM; 287 rows are non-CPM spill
--       (expected for concern/surveillance candidates — not a CF for helper sign-off).
--
-- Gate 1 effect: ~+16 verified tier3_helper tables (column not_started → verified on 43 cols).
-- Gate 5: unchanged — verified canonical_% date gate does not include manuscript_workspace.*.
--
-- Deferred (next lane / larger n_not_started / out of manuscript_workspace scope):
--   * main.__readme, main.patient_cross_domain_timeline_v2, main.specimen_source_xref_v1
--       and remaining tier3_helper tables with n_not_started > 4 or large analytic tables
--       (episode_analysis_resolved_v1_dedup, patient_analysis_resolved_v1, ln_master_rollup_v1, …).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 129a — Backfill: biochemical_concern_backfill_v1 (DATE plausibility)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'backfill_date_plausibility_probe_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=1659; DATE column non-null coverage reviewed; CPM overlap 1372 '
            || '(287 rows non-cohort spill per concern-backfill scope).'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'biochemical_concern_backfill_v1'
  AND column_name = 'first_concern_date'
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129b — Backfill: nucmed_tgab_max_backfill_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'numeric_plausibility_bounds_audit_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=2994; min/max_tgab [0.4,25600] lab-scale; no neg values.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'nucmed_tgab_max_backfill_v1'
  AND column_name = 'max_tgab'
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129c — Meta audit: registry_v2_cpm_cols_without_registry_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'meta_registry_gap_enumeration_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=950 rows; column_name VARCHAR keys non-empty; gap-list artifact.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'registry_v2_cpm_cols_without_registry_v1'
  AND column_name = 'column_name'
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129d — Adjudication queue: cpm_hypopara_adjudication_queue_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'presence_check_adjudication_discordance_pair_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=4 queue rows; cpm_says vs phenotype_says populated for review.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'cpm_hypopara_adjudication_queue_v1'
  AND column_name IN ('cpm_says', 'phenotype_says')
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129e — Backfill: path_stage_raw_backfill_v1 (proposed strings)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'backfill_proposed_source_string_snapshot_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=4083; proposed path-stage raw strings materialized for governance.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'path_stage_raw_backfill_v1'
  AND column_name IN ('proposed_path_stage_raw', 'proposed_gm_path_stage_raw')
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129f — Audit: us_llm_absorption_gap_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'derived_rowcount_and_timestamp_probe_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=60 gap rows; total_entities HUGEINT; rebuilt_at TIMESTAMP TZ present.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'us_llm_absorption_gap_v1'
  AND column_name IN ('total_entities', 'rebuilt_at')
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129g — Audit: verification_low_concordance_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'audit_concordance_field_and_pct_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=17; pct_agree in [0,~0.69]; field_name vocab clean.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'verification_low_concordance_v1'
  AND column_name IN ('field_name', 'pct_agree')
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129h — Adjudication queue: cpm_ete_self_contradiction_queue_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'referential_integrity_vs_canonical_patient_master_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=2790; 0 orphan research_id vs CPM; ETE/gross/reason snapshot coherent.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'cpm_ete_self_contradiction_queue_v1'
  AND column_name = 'cpm_ete_grade_final_v2'
  AND verification_status = 'not_started';

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'boolean_flag_coherence_adjudication_queue_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: gross_ete_flag boolean column populated per join spine.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'cpm_ete_self_contradiction_queue_v1'
  AND column_name = 'cpm_gross_ete_flag'
  AND verification_status = 'not_started';

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'enum_vocabulary_validation_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: reason text bucketed; contradictions enumerated.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'cpm_ete_self_contradiction_queue_v1'
  AND column_name = 'reason'
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129i — Adjudication log: cpm_hypopara_adjudication_log_v1 (append-only history)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'presence_check_append_only_log_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=4 log rows; action_taken / evidence_summary JSON / decided_at TZ.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'cpm_hypopara_adjudication_log_v1'
  AND column_name IN ('action_taken', 'evidence_summary', 'decided_at')
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129j — QC audit: qc_event_issues_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'qc_event_issue_payload_audit_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=6147; source_pk / details VARCHAR; detected_at TIMESTAMP present.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'qc_event_issues_v1'
  AND column_name IN ('source_pk', 'details', 'detected_at')
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129k — Audit: us_llm_absorption_deferred_multi_nodule_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'derived_count_integrity_deferral_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=825; n_llm_entities vs n_v2_rows BIGINT; deferred_at TZ.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'us_llm_absorption_deferred_multi_nodule_v1'
  AND column_name IN ('n_llm_entities', 'n_v2_rows', 'deferred_at')
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129l — Meta: __conventions
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'meta_documentation_table_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=16 convention rows; category/rule/exemplar/established_in sampled.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = '__conventions'
  AND column_name IN ('category', 'rule', 'exemplar', 'established_in')
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129m — Review queue: cohort_view_duplicate_review_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'duplicate_cluster_review_queue_probe_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=8 cluster pairs; jaccard overlap DOUBLE in [0,1]; IDs VARCHAR.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'cohort_view_duplicate_review_v1'
  AND column_name IN ('cluster_label', 'manuscript_id_a', 'manuscript_id_b', 'jaccard_column_overlap')
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129n — Backfill: max_stimulated_tg_backfill_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'backfill_tg_measurement_bundle_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=238; stimulated Tg + date + note_ref + BIGINT count coherence.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'max_stimulated_tg_backfill_v1'
  AND column_name IN ('max_stimulated_tg', 'max_stimulated_tg_date',
                      'max_stimulated_tg_source_note_ref', 'n_stimulated_tg_measurements')
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129o — Candidates: recurrence_path_proven_candidates_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'candidate_evidence_snapshot_probe_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=191 path-proven SURVEILLANCE candidates; source/date/evidence/priority.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'recurrence_path_proven_candidates_v1'
  AND column_name IN ('path_proven_source', 'path_proven_date',
                      'path_proven_evidence', 'priority')
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- 129p — Registry completeness: tier2_completeness_v1
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'registry_completeness_boolean_snapshot_audit_m129',
    batch_id = 'tier3_helper_workspace_batch_m129_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_129: N=22 LLM-domain rows; has_tier2_event_table fully populated BOOLEAN.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'tier2_completeness_v1'
  AND column_name IN ('llm_source', 'expected_tier2_table',
                      'has_tier2_event_table', 'checked_at')
  AND verification_status = 'not_started';

-- =============================================================================
-- 129q — Table rollups (16 manuscript_workspace helpers → verified)
-- =============================================================================

UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified = subq.n_verified,
    n_not_started = subq.n_not_started,
    n_failed = COALESCE(subq.n_failed, 0),
    n_na = subq.n_na,
    table_status = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/129_manuscript_workspace_helper_batch_signoff_20260429.sql',
    notes = 'Tier3_helper manuscript_workspace batch (Lane 21 mig_129): pragmatic audit/queue/backfill artifacts; rows/columns probed live 2026-04-29; all deferred large-table tier3_helpers remain unchanged.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'manuscript_workspace'
    AND table_name IN (
      'biochemical_concern_backfill_v1',
      'nucmed_tgab_max_backfill_v1',
      'registry_v2_cpm_cols_without_registry_v1',
      'cpm_hypopara_adjudication_queue_v1',
      'path_stage_raw_backfill_v1',
      'us_llm_absorption_gap_v1',
      'verification_low_concordance_v1',
      'cpm_ete_self_contradiction_queue_v1',
      'cpm_hypopara_adjudication_log_v1',
      'qc_event_issues_v1',
      'us_llm_absorption_deferred_multi_nodule_v1',
      '__conventions',
      'cohort_view_duplicate_review_v1',
      'max_stimulated_tg_backfill_v1',
      'recurrence_path_proven_candidates_v1',
      'tier2_completeness_v1'
    )
  GROUP BY schema_name, table_name
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- =============================================================================
-- end migration 129 — manuscript_workspace tier3_helper batch (16 tables, 43 cols)
-- Deferred: remaining tier3_helper + main.__readme / cross-domain timeline / specimen_xref
-- =============================================================================
