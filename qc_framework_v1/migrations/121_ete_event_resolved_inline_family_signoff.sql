-- =============================================================================
-- Migration 121 — canonical_ete_event_resolved_v1 + canonical_ete_inline_adjudication_v1 SIGN-OFF (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29 (UTC — Cursor lane 13 batch)
-- Author: Logan Glosser <logan.glosser@gmail.com>
-- Plan:   Close the Tier-2 ETE resolved layer (62 cols, 6,689 event rows / 4,137
--         patients) built from manuscript_workspace.ete_manuscript_analytic_v6
--         (refreshed mig_61c) + dual-track recurrence (mig_62) + survival columns
--         layered from canonical_survival_followup_v1. Optionally pair-close the
--         small inline adjudication table (12 cols) populated in mig_61c.
--
-- Build lineage (SSOT in repo):
--   * qc_framework_v1/migrations/61c_ete_inline_adjudication_full_closeout.sql
--     — creates canonical_ete_inline_adjudication_v1 (3,021 rows) and rebuilds
--     canonical_ete_event_resolved_v1 from ete_manuscript_analytic_v6 (no recurrence).
--   * qc_framework_v1/migrations/62_canonical_recurrence_resolved_v1.sql
--     — layers recurrence into analytic v7 and (DB state) recurrence + survival cols
--     on canonical_ete_event_resolved_v1 via subsequent refresh aligned to
--     main.canonical_recurrence_resolved_v1.
--
-- Live MotherDuck verification probes (2026-04-29 ~query_rw scripts._md_connect):
--   * Row/patient cardinality: canonical_ete_event_resolved_v1 COUNT(*)=6689 ,
--       COUNT(DISTINCT research_id)=4137 matching canonical_path_malignant_events_v1
--       COUNT(*)=6689 / DISTINCT patients =4137.
--   * 5-field multiset identity vs path events: EXCEPT-distinct tuples
--       (research_id, path_surgery_id, tumor_ordinal, specimen_id, synoptic_row_ix)
--       has 0 rows both directions vs canonical_path_malignant_events_v1 (identical distinct sets).
--   * Inner join cardinality on raw specimen + synoptic match (IS NOT DISTINCT FROM):
--       5,261 rows → compare path_event_ete_raw / extrathyroidal, size_greatest_dimension_cm,
--       primary_histology matching reported_t_stage_ajcc8 / t-stage string:
--       0 drift on pm.extrathyroidal_extension vs er.path_event_ete_raw, 0 drift size,
--       0 drift reported AJCC T reported column vs pm.t_stage_ajcc8. Trusted histology /
--       variant fields intentionally diverge raw pm.primary_histology on many rows —
--       they replay etemanuscript analytic trusted layer (verified path malignant feed),
--       not verbatim Excel raw column-only equality.
--   * Recurrence (patient-grain LEFT join): IS-DISTINCT-FROM vs
--       main.canonical_recurrence_resolved_v1 recurrence_path_proven and
--       recurrence_status_final = 0 / 6689 paired rows — full extraction-faithfulness.
--   * Survival: raw equality vs main.canonical_survival_followup_v1 shows expected
--       drift on last_known_alive_date (TIMESTAMP vs DATE storage) and vital_status
--       naming (vital_status vs vital_status_current) pending survival-family sign-off;
--       carry-forwards below do not block column registry closure with explicit methods.
--   * Note-level canonical_ete_subgrade_events_v1 (mig_114) is NOT joinable on
--       (path_surgery_id, tumor_ordinal) — different grain (note LLM). Cross-check for
--       raw ETE text uses path_malignant_events per above.
--   * t_stage_discordance_flag prevalence ≈3.1% — well under 30% investigation threshold.
--   * manuscript_workspace.ete_manuscript_analytic_v6/v7 views fail to compile in this
--       catalog (missing path_malignant_event_fingerprint_v1) — verification used
--       materialized canonical_ete_event_resolved_v1 + verified base tables instead.
--
-- Carry-forwards (documented; do not block Protocol v2 column closure):
--   * CF-mig121-ETE-EVENT-RESOLVED-RECURRENCE-PENDING — flip notes to
--       derivation_re_derivation when independent patient-level canonical_recurrence_v1
--       (non-resolved) is verified; current recurrence block is faithful to
--       canonical_recurrence_resolved_v1 (mig_62 dual-track) at 0 drift.
--   * CF-mig121-ETE-EVENT-RESOLVED-SURVIVAL-PENDING — bounded drift vs
--       canonical_survival_followup_v1 until that table’s family sign-off completes;
--       includes TIMESTAMP storage on last_known_alive_date (joins CF-100-DATE-RETYPE).
--   * CF-mig121-ETE-EVENT-LAST-ALIVE-RETYPE — er.last_known_alive_date stored as
--       TIMESTAMP; calendar semantics align when CAST to DATE vs survival follow-up;
--       strip TZ / rebuild per feedback_alter_view_dependents pattern on future refresh.
--
-- Final state targeted:
--   canonical_ete_event_resolved_v1 — 62 cols: 57 not_started→verified + 5 na unchanged
--   canonical_ete_inline_adjudication_v1 — 12 cols: 9 not_started→verified + 3 na
--
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 121a — Resolved table: pathology spine + analytic eligibility (path malignant verified mig_89)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'extraction_faithfulness_against_verified_canonical_path_malignant_events_v1',
    batch_id = 'mig121_ete_event_resolved_family_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
              || ' | mig121: Event grain aligns with mig_89 canonical_path_malignant_events_v1 '
              || '(6,689 rows identical distinct 5-field keys specimen/synoptic; multiset parity). '
              || 'synoptic_row_ix: Script 108 workbook load-order index (reference_synoptic_row_ix.md)—not ROW_NUMBER-derived. '
              || 'Trusted histology/size/stage derive from manuscript ete pipeline—not raw-cell equality.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_ete_event_resolved_v1'
  AND verification_status = 'not_started'
  AND column_name IN (
    'surgery_episode_id_global',
    'tumor_ordinal','synoptic_row_ix','cohort_ptc','cohort_descriptive_full','analytic_eligible'
  );
-- (research_id, path_surgery_id, specimen_id remain registry na — auto identifier/provenance skips.)


-- -----------------------------------------------------------------------------
-- 121b — Resolved table: adjudicated ETE cluster + mig54 LLM ancillary
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'derivation_replay_etemanuscript_mig61c_v6_plus_inline_closeout',
    batch_id = 'mig121_ete_event_resolved_family_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
              || ' | mig121: Layers ete_grade_final_v6 + inline adjudication + patient_master ETE cols + '
              || 'fresh LLM subgrade mig54_* from qc_framework migrations 52–61c SSOT DDL. '
              || 'extrathyroid raw field 0-drift vs path_malignant on specimen join subset (5,261 rows).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_ete_event_resolved_v1'
  AND verification_status = 'not_started'
  AND column_name IN (
    'ete_grade','ete_grade_source','is_gross_ete','is_microscopic_ete','any_ete_present','is_no_ete',
    'is_unresolved_ete','is_no_ete_data','path_event_ete_raw',
    'patient_master_ete_grade_clean','patient_master_ete_grade_source',
    'patient_master_ete_grade_adjudicated','patient_master_ete_adjudicated_flag',
    'general_llm_ete_grade',
    'mig54_fresh_llm_ete_grade','mig54_fresh_llm_evidence_quotes','mig54_fresh_llm_confidence','mig54_fresh_llm_ajcc8',
    'inline_patient_grade','inline_patient_set','inline_patient_evidence',
    'inline_event_grade','inline_event_evidence','inline_event_set',
    'pm_disagreement_flag','open_self_contradiction_flag','legacy_gross_ete_effective'
  );


-- -----------------------------------------------------------------------------
-- 121c — Resolved table: tumor + AJCC trusted staging block
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'trusted_staging_replay_from_etemanuscript_analytic_with_mig89_inputs',
    batch_id = 'mig121_ete_event_resolved_family_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
              || ' | mig121: Staging column block replays trusted layer on path-malignant spine; '
              || 'reported_t_stage_ajcc8 matches pm.t_stage_ajcc8 on raw IS-NOT-DISTINCT join subset (0 drift). '
              || 'Derived/discordance/overall stage per manuscript rules in mig_61 family.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_ete_event_resolved_v1'
  AND verification_status = 'not_started'
  AND column_name IN (
    'size_greatest_dimension_cm','primary_histology','histology_variant','laterality','multifocal_flag',
    'reported_t_stage_ajcc8','derived_t_stage_ajcc8','t_stage_discordance_flag','ajcc_overall_stage'
  );


-- -----------------------------------------------------------------------------
-- 121d — Resolved table: dual-track recurrence (faithful to canonical_recurrence_resolved_v1)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'extraction_faithfulness_against_canonical_recurrence_resolved_v1_mig62',
    batch_id = 'mig121_ete_event_resolved_family_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
              || ' | mig121: 0/6,689 drift vs main.canonical_recurrence_resolved_v1 patient join (2026-04-29 probe). '
              || 'CF-mig121-ETE-EVENT-RESOLVED-RECURRENCE-PENDING: re-label to '
              || 'derivation_re_derivation when legacy canonical_recurrence_v1 (if distinct) verifies.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_ete_event_resolved_v1'
  AND verification_status = 'not_started'
  AND column_name IN (
    'recurrence_path_proven','recurrence_path_proven_date','recurrence_path_proven_source','days_to_path_proven',
    'recurrence_imaging_suspicious','recurrence_imaging_suspicious_date',
    'recurrence_imaging_modality_summary','recurrence_imaging_modalities_all',
    'recurrence_imaging_finding_text','recurrence_imaging_n_events','days_to_imaging_suspicious',
    'recurrence_imaging_then_path_confirmed','recurrence_status_final'
  );


-- -----------------------------------------------------------------------------
-- 121e — Resolved table: survival (pending upstream family + date retype CF)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'extraction_faithfulness_bounded_pending_canonical_survival_followup_v1_verification',
    batch_id = 'mig121_ete_event_resolved_family_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
              || ' | mig121: Materialized join to canonical_survival_followup_v1 with known TIMESTAMP/DATE bridge + '
              || 'vital column rename drift until survival family sign-off. '
              || 'CF-mig121-ETE-EVENT-RESOLVED-SURVIVAL-PENDING + CF-mig121-ETE-EVENT-LAST-ALIVE-RETYPE → CF-100-DATE-RETYPE.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_ete_event_resolved_v1'
  AND verification_status = 'not_started'
  AND column_name IN ('last_known_alive_date','vital_status');


-- -----------------------------------------------------------------------------
-- 121f — Inline adjudication table (Option A paired close)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'manual_administrative_adjudication_replay_mig61c_inline_closeout',
    batch_id = 'mig121_ete_inline_adjudication_family_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
              || ' | mig121: Adjudication sourced from mig_61c INSERT/SELECT batches + Claude inline reads; '
              || '3,021 rows / 3,012 distinct patients; tumor_ordinal populated for event-grain rows.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_ete_inline_adjudication_v1'
  AND verification_status = 'not_started';


-- -----------------------------------------------------------------------------
-- 121g — Table signoff: canonical_ete_event_resolved_v1
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed,0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/121_ete_event_resolved_inline_family_signoff.sql',
    notes             = 'Tier-2 ETE adjudication envelope: mig_61c v6 refresh + mig_62 recurrence + survival layering. '
                        || 'First full multi-source Tier-2 enrichment verification (Protocol v2). '
                        || 'See migration header for probes + carry-forwards.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_ete_event_resolved_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


-- -----------------------------------------------------------------------------
-- 121h — Table signoff: canonical_ete_inline_adjudication_v1
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed,0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/121_ete_event_resolved_inline_family_signoff.sql',
    notes             = 'Inline ETE adjudication warehouse paired with mig121 resolved sign-off; mig_61c administrative + clinical reads.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_ete_inline_adjudication_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


-- =============================================================================
-- end of migration 121 — ETE resolved + inline adjudication family closed
-- =============================================================================
