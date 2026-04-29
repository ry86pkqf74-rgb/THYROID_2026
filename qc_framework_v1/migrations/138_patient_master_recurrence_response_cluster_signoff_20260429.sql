-- =============================================================================
-- Migration 138 -- canonical_patient_master RECURRENCE + RESPONSE CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   28 — Protocol v2 patient_master derivation verification (Cowork recurrence/
--         response-to-therapy / Tg-trajectory cluster).
--
-- batch_id: mig_138_patient_master_recurrence_response_cluster_20260429
--
-- Predicate (fixed LIKE + ESCAPE '\' for literal underscores):
--   recurrence% , rec_% , time_to_recurrence% , biochemical_tg% , tg_% , %response_to_therapy%
--   excluding columns already flipped in registry (verification_status <> not_started).
--
-- Live MotherDuck probes (RW, 2026-04-29):
--   **48** columns match predicate total (information_schema ∩ CPM registry).
--   **40** flipped here (not_started → verified): this migration only.
--   **7** already verified (overlap with prior thematic lanes — tg_nadir/tg_peak/tg_mean/
--       tg_n_measurements/tg_last_value/tg_rising_flag/biochemical_tg_nadir_after_surgery).
--   **1** `na`: recurrence_pathology_specimen_id — out of Lane 28 scope (non-actionable tier).
--
-- SSOT lineage (verification contract — multi-feeder; NOT naive single-table equality row-by-row):
--   * **canonical_recurrence_v1** — mig_123 rebuild (`scripts/203b_canonical_recurrence_harmonized_20260429.py`);
--     deterministic one-row spine with ROW_NUMBER priority (confirmed first, severity by recurrence_type).
--   * **canonical_recurrence_resolved_v1** — mig_125 rollup; **`recurrence_status_final`** ENUM lives here only
--     (**none** / **imaging_only_unconfirmed** / **path_proven** — NEVER `imaging_only_suspicious`).
--   * **classification / rec_\*** overlays — lineage from recurrence consolidation-era ETL (Script **224**/288/289 —
--     `recurrence_classification_v1` feeders where present; archival column mapping when table retired).
--   * **Histology-at-recurrence** — `recurrence_histology_v2` from classification spine; **`recurrence_histology`**
--     mirrored with precedence v1←v2 (Script **289**) per Lane 22 deferral clearance.
--
-- Cross-check doctrine (Cowork references):
--   * Event-/spine-aware joins use **CAST(research_id AS VARCHAR)** INNER JOIN probes where patient-grain
--     Cartesian drift documented (`feedback_etevent_resolved_cross_check.md` pattern adapted for recurrence).
--   * **`recurrence_imaging_n_events`** NULL semantics preserved elsewhere (**NULL≠0**) —
--     (`feedback_recurrence_imaging_n_events_null.md` — not on CPM; resolved_v1 owns the field).
--
-- Drift probes vs live `canonical_recurrence_v1` dedup spine (same ROW_NUMBER tier as builders):
--   * `recurrence_confirmed` mismatches vs COALESCE(playbook) — **462 / 10,871 (~4.25%)**.
--     Acceptance per Protocol v2: **≤5% with documentation** — attributable to CPM materialization cadence lagging
--     post–mig_123 recurrence_v1 RW refresh; flagged as **CF-mig138-CPM-RECURRENCE-SPINE-RESYNC-PENDING** pending
--     explicit downstream CPM rebuild (do not silently rewrite CPM recurrence core in-registry verification).
--
-- Date-type ledger (clinical calendar discipline):
--   * `canonical_recurrence_v1.recurrence_date` TIMESTAMP in live publication DB (≠ registry DATE drift from
--     mig_121 series) — mirrored on CPM; **CF-mig138-PM-RECURRENCE-DATE-RETYPE** / umbrella **CF-100-DATE-RETYPE**.
--
-- Acceptance:
--   * 40 cols flipped `not_started` → `verified`; Gate 4 satisfied (verified_by, verification_method, batch_id, verified_ts).
--   * `n_verified` on `canonical_table_signoff_registry_v1` increments by exactly **40**.
--
-- Do not rerun canonical_recurrence_v1 mig_123 in this lane; sibling exclusions per prompt.
--
-- Applied on MotherDuck RW (`thyroid_canonical_publication_v1_0.main`): 2026-04-29 — 40 cols batch_id mig_138.
-- =============================================================================

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 138a — 9 cols — canonical_recurrence_v1 spine (203b mig_123 rebuild playbook)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_recurrence_v1_mig_123_rebuild',
    batch_id            = 'mig_138_patient_master_recurrence_response_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_138 recurrence cluster (Lane 28). Spine: canonical_recurrence_v1 '
                          || '(Script 203b / mig_123). ≤5% post-rebuild lineage drift flagged '
                          || 'CF-mig138-CPM-RECURRENCE-SPINE-RESYNC-PENDING — CPM resync gated explicitly.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'biochemical_tg_at_recurrence',
    'recurrence_confirmed',
    'recurrence_date',
    'recurrence_definition',
    'recurrence_evidence_source',
    'recurrence_histology',
    'recurrence_site',
    'recurrence_type',
    'time_to_recurrence_days'
  );


-- -----------------------------------------------------------------------------
-- 138b — 5 cols — rec_* event-classification overlay (historical recurrence_classification lineage)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'cross_check_event_grain_inner_join_cast_varchar',
    batch_id            = 'mig_138_patient_master_recurrence_response_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_138 recurrence cluster (Lane 28). rec_* provenance tiers from recurrence '
                          || 'classification overlays (Script 224 lineage / patient-spine merges); INNER JOIN probes '
                          || 'CAST(rid AS VARCHAR) per recurrence family cross-check doctrine.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'rec_detection_method',
    'rec_event_rank',
    'rec_source_priority',
    'rec_source_table',
    'rec_structural_flag'
  );


-- -----------------------------------------------------------------------------
-- 138c — 19 cols — extended recurrence_rollups — resolved_v2/site/history + days-from-surgery fields
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_recurrence_resolved_v1',
    batch_id            = 'mig_138_patient_master_recurrence_response_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_138 recurrence cluster (Lane 28). Extended recurrence_site_* / pathology / '
                          || 'staging rollups sourced from mig_125 resolved family + pathology spine; '
                          || '`recurrence_histology_v2` cleared from Lane 22 pathology defer bucket.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'recurrence_data_confidence',
    'recurrence_date_source',
    'recurrence_date_v2',
    'recurrence_days_from_surg',
    'recurrence_days_from_surg_1',
    'recurrence_days_from_surg_quarantined',
    'recurrence_evidence_type',
    'recurrence_flag_scoring',
    'recurrence_flag_v2',
    'recurrence_histology_v2',
    'recurrence_laterality',
    'recurrence_pathology_source_table',
    'recurrence_site_primary',
    'recurrence_site_raw',
    'recurrence_site_source',
    'recurrence_site_text',
    'recurrence_site_v2',
    'recurrence_source',
    'recurrence_type_primary'
  );


-- -----------------------------------------------------------------------------
-- 138d — 7 cols — Tg trajectory / availability / caveat flags (ATA response context; NOT tg_lab_*)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'response_to_therapy_ata_classification_logic',
    batch_id            = 'mig_138_patient_master_recurrence_response_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_138 recurrence cluster (Lane 28). Tg trajectory + limitation flags underpin '
                          || 'structured response-to-therapy phenotyping; labs-lane `tg_lab_*` columns verified '
                          || 'separately (mig_134). ATA composite logic references longitudinal lab + recurrence SSOT.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'tg_below_threshold_ever',
    'tg_data_available',
    'tg_last_censored',
    'tg_limitation_note',
    'tg_nadir_suspect_preablation',
    'tg_peak_source',
    'tg_trajectory_class'
  );


-- -----------------------------------------------------------------------------
-- 138e — refresh canonical_table_signoff_registry_v1 for CPM (+40 verified)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
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
                        || ' | mig_138: recurrence+response thematic cluster CLOSED (40 cols).'
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
-- 138f — Carry-forward CF tagging (DATE/TIMESTAMP + spine resync backlog)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig138-PM-RECURRENCE-DATE-RETYPE: recurrence_date TIMESTAMP on CPM; '
            || 'calendar comparisons vs survival / surgery use CAST(... AS DATE) per AGENTS.md mig121/'
            || 'clinical_date_retype_20260428; umbrella CF-100-DATE-RETYPE.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='verified'
  AND batch_id='mig_138_patient_master_recurrence_response_cluster_20260429'
  AND column_name='recurrence_date';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig138-CPM-RECURRENCE-SPINE-RESYNC-PENDING: LIVE dedup-vs-CPM confirmed drift '
            || '~4.25% recurrence_confirmed mismatch post–mig_123 until CPM materialization replays recurrence_v1 spine.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='verified'
  AND batch_id='mig_138_patient_master_recurrence_response_cluster_20260429'
  AND column_name='recurrence_confirmed';


COMMIT;


-- =============================================================================
-- end migration 138 — CPM recurrence-response cluster verified (40 cols flipped this lane)
-- =============================================================================
