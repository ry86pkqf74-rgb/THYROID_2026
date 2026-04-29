-- =============================================================================
-- Migration 140 — canonical_patient_master ETE CLUSTER sign-off (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane 29 — ETE thematic slice (~36 cols per Cowork predicate).
-- batch_id: mig_140_patient_master_ete_cluster_20260429
--
-- Pre-apply probes (MotherDuck RW thyroid_canonical_publication_v1_0, 2026-04-29):
--   * Predicate cardinality: exactly 36 cols matching substring probe excluding already-verified
--     registry rows + completeness/detection/undetectable/ct_thyroid_heterogeneous exclusions.
--   * Cohort parity: canonical_patient_master = 10,871 rows / distinct research_id (connect_locked).
--   * BOOLEAN uniformity sweep (all ETE booleans in lane): documented below; degenerate-handling:
--       any_ete_present_not_further_specified_in_imaging → n_true=0 → disposition **na**
--       (upstream NFS-imaging signal absent — CF-mig140-EXPAND-UPSTREAM-IMAGING-NFS-ETE).
--       microscopic_ete_t3b_corrected → n_true=0 all-FALSE cohort → Type A invariant (AJCC8 T3b
--       microscopic-correction rule yields zero positives in production cohort) —
--       verified + CF-mig140-COHORT-INVARIANT-microscopic_ete_t3b_corrected.
--   * Gate 4 (verified requires verified_by + verification_method + batch_id + verified_ts): 0
--     violations pre-apply on existing verified CPM rows.
--   * Non-degenerate booleans (examples): any_ete_anywhere true=1277; any_microscopic_ete_anywhere
--     true=318; prm_ete_path_confirmed true=3850; gross_ete_flag true=1056.
--
-- SSOT lineage (verification contract — multi-feeder):
--   * **canonical_ete_event_resolved_v1** — mig_121 Tier-2 family (57/62 cols verified); CAST(rid AS VARCHAR)
--     cross-checks per feedback_etevent_resolved_cross_check.md.
--   * **canonical_ete_subgrade_events_v1 / rollup** — mig_114 (note-grain LLM + worst-mention rollup).
--   * **canonical_path_malignant_events_v1** — mig_89 + **canonical_invasion_events_v1** — mig_95/
--     qc_framework_v1/migrations/95_ete_taxonomy_and_invasion_rollups.sql.
--   * **PRM rule chain** — pm_margin_with_gross_ete / prm_ete_* categorical passes vs AJCC staging overlays;
--     prm_ete_imaging_path_concordance often insufficient_data pending structured imaging-source ETE —
--     CF-mig140-PM-ETE-IMAGING-UPSTREAM-PENDING (informational; VARCHAR).
--
-- Acceptance disposition:
--   * 35 cols → verified (Gate 4 metadata complete).
--   * 1 col → na (upstream imaging NFS axis): any_ete_present_not_further_specified_in_imaging.
--   * Total cluster closure = 36 cols moved off **not_started**.
--
-- Active parallel lanes (do not touch): recurrence-response mig_138, survival mig_141, RAI mig_142,
-- small-clusters mig_143.
--
-- Executed on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 140a — 6 cols — patient-level BOOL_OR aggregates across axes (excluding NFS-imaging lane col)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_aggregate_ete_per_axis',
    batch_id            = 'mig_140_patient_master_ete_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_140 ETE cluster (Lane 29). Aggregate BOOL_OR replay vs canonical '
                          || 'path/invasion/imaging feeders (mig_89/mig_95); cohort uniformity sweep '
                          || '2026-04-29 — non-zero TRUE where structured imaging/path/op substrate exists.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'any_ete_anywhere',
    'any_ete_in_imaging',
    'any_ete_in_op_or_path',
    'any_ete_present_not_further_specified_anywhere',
    'any_ete_present_not_further_specified_in_op_or_path',
    'any_microscopic_ete_anywhere'
  );


-- -----------------------------------------------------------------------------
-- 140b — 1 col — imaging NFS axis degenerate at cohort grain → na (upstream pending)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verified_by         = 'logan',
    verification_method = 'upstream_imaging_nfs_ete_pending',
    batch_id            = 'mig_140_patient_master_ete_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_140 ETE cluster (Lane 29). Cohort uniformity n_true=0 on NFS-imaging '
                          || 'BOOLEAN — Type B upstream axis not populated (CF-mig140-EXPAND-UPSTREAM-'
                          || 'IMAGING-NFS-ETE); defer verified until structured imaging NFS ETE exists.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name='any_ete_present_not_further_specified_in_imaging';


-- -----------------------------------------------------------------------------
-- 140c — 5 cols — adjudication chain (Tier-2 resolved family + patient adjudication overlays)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_ete_event_resolved_v1',
    batch_id            = 'mig_140_patient_master_ete_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_140 ETE cluster (Lane 29). Patient-grain adjudication fields replay '
                          || 'vs mig121 canonical_ete_event_resolved_v1 / ete_adjudication_v1 overlays '
                          || '(Scripts 233/390/265 lineage); VARCHAR provenance cols — no DATE CF.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'ete_adjudicated_flag',
    'ete_adjudication_confidence',
    'ete_adjudication_evidence',
    'ete_adjudication_reasoning',
    'ete_adjudication_t_adjustment'
  );


-- -----------------------------------------------------------------------------
-- 140d1 — 11 cols — resolved grade ladder + ordinal + originals (multi-feeder; resolved spine SSOT)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_ete_event_resolved_v1',
    batch_id            = 'mig_140_patient_master_ete_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_140 ETE cluster (Lane 29). Grade ladder vs mig121 Tier-2 resolved '
                          || '+ adjudication precedence per canonical_finalization mapping (ete_grade_final '
                          || 'vs ete_grade_final_v2 generations); INT ete_ordinal_worst ordinal-aligned.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'ete_grade',
    'ete_grade_adjudicated',
    'ete_grade_clean',
    'ete_grade_final',
    'ete_grade_final_v2',
    'ete_grade_source',
    'ete_op_note_confidence',
    'ete_op_note_grade',
    'ete_ordinal_worst',
    'ete_original_grade',
    'ete_original_source'
  );


-- -----------------------------------------------------------------------------
-- 140d2 — 2 cols — Phase subgrade / v10 worst pinned from ete_subgrade SSOT
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_ete_subgrade_events_v1',
    batch_id            = 'mig_140_patient_master_ete_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_140 ETE cluster (Lane 29). worst_ete_v10 + ete_refined_grade pinned '
                          || 'to mig114 canonical_ete_subgrade_events_v1 / rollup worst-mention logic.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'ete_refined_grade',
    'worst_ete_v10'
  );


-- -----------------------------------------------------------------------------
-- 140e1 — 5 cols — path malignant + invasion substrate (structured path/op replay)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_path_malignant_events_v1',
    batch_id            = 'mig_140_patient_master_ete_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_140 ETE cluster (Lane 29). Path-aligned BOOL/VARCHAR replay vs mig_89 '
                          || 'canonical_path_malignant_events_v1 gross/raw/NLP mirrors.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'ete_any_present_path',
    'gm_path_ete_raw',
    'gross_ete_flag',
    'nlp_path_ete_mentioned',
    'microscopic_ete_t3b_corrected'
  );


-- -----------------------------------------------------------------------------
-- 140e2 — 2 cols — subgrade narrative metadata (note-tier extraction family)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_ete_subgrade_events_v1',
    batch_id            = 'mig_140_patient_master_ete_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_140 ETE cluster (Lane 29). Subgrade method/note passthrough vs mig114 '
                          || 'note_entities_llm_ete_subgrade canonical tier.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'ete_subgrade_method',
    'ete_subgrade_note'
  );


-- -----------------------------------------------------------------------------
-- 140f — 4 cols — PRM concordance / rule stamps (AJCC staging overlays vs margin+gross ETE)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'prm_rule_concordance_chain',
    batch_id            = 'mig_140_patient_master_ete_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_140 ETE cluster (Lane 29). PRM categorical chain replay vs publication '
                          || 'PM build (prm_ete_rule_applied x_to_microscopic/present_to_microscopic; '
                          || 'prm_ete_imaging_path_concordance often insufficient_data — imaging-path '
                          || 'concordance deferred CF-mig140-PM-ETE-IMAGING-UPSTREAM-PENDING).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'prm_ete_imaging_path_concordance',
    'prm_ete_path_confirmed',
    'prm_ete_rule_applied',
    'prm_margin_with_gross_ete'
  );


-- -----------------------------------------------------------------------------
-- 140 — refresh canonical_patient_master row on canonical_table_signoff_registry_v1
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 AS ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_140: ETE thematic cluster CLOSED (36 cols disposition: 35 verified + 1 na).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'       THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'           THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


-- -----------------------------------------------------------------------------
-- 140g — informational CF tagging (registry notes only)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig140-COHORT-INVARIANT-microscopic_ete_t3b_corrected: cohort uniformity '
            || 'all-FALSE — Logan AJCC8 microscopic-vs-T3b correction gate yields zero positives '
            || '(staging-derived-from-finding per feedback_findings_vs_staging.md).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='verified'
  AND batch_id='mig_140_patient_master_ete_cluster_20260429'
  AND column_name='microscopic_ete_t3b_corrected';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig140-PM-ETE-IMAGING-UPSTREAM-PENDING: prm_ete_imaging_path_concordance relies '
            || 'on mixed imaging/path substrate; insufficient_data prevalence expected until imaging '
            || 'ETE SSOT structured.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='verified'
  AND batch_id='mig_140_patient_master_ete_cluster_20260429'
  AND column_name='prm_ete_imaging_path_concordance';


COMMIT;


-- =============================================================================
-- end migration 140 — CPM ETE cluster (35 verified + 1 na; 36 cols closed off not_started)
-- =============================================================================
