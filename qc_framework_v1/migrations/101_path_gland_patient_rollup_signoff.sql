-- =============================================================================
-- Migration 101 -- canonical_path_gland_patient_rollup_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Rebuild + verify path_gland rollup post-Cursor's events repair
--         (06f88ae). Rollup was built 2026-04-21 by Script 361 — STALE relative
--         to the 2026-04-28 events repair which renormalized parathyroid
--         gland_position taxonomy.
--
-- Methodology: Derivation re-derivation against verified events, then rebuild
--   in place using the same Script 361 logic to restore rollup ↔ events
--   consistency. Pattern from mig_95b (rollup derivation signoff).
--
-- Pre-rebuild diff (mig_101 probe, 2026-04-28):
--   Re-derivation against current canonical_path_gland_events_v1 vs stored
--   rollup. 10,731 patient rows, 9 of 10 derived cols match 100%; 1 drifted:
--     n_parathyroid_glands_documented : 565 / 10,731 patients differ.
--   Reason: rollup uses COUNT(DISTINCT gland_position) for parathyroid; the
--   pre-repair events had ~4,025 free-text positions (e.g. 'right superior',
--   'rt sup', 'right_superior_parathyroid'); post-repair events collapse to
--   the 7-value taxonomy + NULL, so the COUNT-DISTINCT result is now correctly
--   smaller. Other 9 cols don't depend on gland_position so they stayed clean.
--
-- Side-fix: rollup.build_ts was TIMESTAMP WITH TIME ZONE (legacy DuckDB
--   CURRENT_TIMESTAMP default). Per reference_duckdb_timestamp_tz.md, all
--   build_ts cols should be plain TIMESTAMP. Recast on rebuild.
--
-- Sign-off scope (post-rebuild):
--   10 not_started cols flipped to 'verified' via derivation_re_derivation:
--     any_thyroid_lobe_measured, total_thyroid_weight_g, left_lobe_max_dim_cm,
--     right_lobe_max_dim_cm, any_isthmus_documented, any_parathyroid_documented,
--     n_parathyroid_glands_documented, n_parathyroid_glands_abnormal,
--     any_parathyroid_hyperplasia, any_parathyroid_adenoma
--   3 already-na cols: research_id, build_script, build_ts
--   Final: 10 verified + 3 na = 13 / 13 closed
--
-- Total path_gland family closed: events (Cursor mig path_gland_repair_20260428,
-- 06f88ae) + rollup (mig_101, this file). 19th canonical table closed under v2.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 101a: defensive snapshot of pre-rebuild rollup into archive_pub_v1_0
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_gland_patient_rollup_v1_pre_mig101_20260428 AS
SELECT * FROM main.canonical_path_gland_patient_rollup_v1;

-- 101b: rebuild rollup using Script 361 logic against post-repair events
CREATE OR REPLACE TABLE main.canonical_path_gland_patient_rollup_v1 AS
WITH ev AS (
  SELECT * FROM main.canonical_path_gland_events_v1
),
latest_surgery AS (
  SELECT research_id, MAX(path_date) AS latest_path_date
  FROM ev WHERE gland_type='thyroid_lobe' GROUP BY research_id
),
thy_latest AS (
  SELECT e.research_id, SUM(e.gland_weight_g) AS total_thyroid_weight_g_latest
  FROM ev e JOIN latest_surgery ls
    ON e.research_id=ls.research_id AND e.path_date=ls.latest_path_date
  WHERE e.gland_type='thyroid_lobe'
  GROUP BY e.research_id
)
SELECT
  ev.research_id,
  BOOL_OR(ev.gland_type='thyroid_lobe')                               AS any_thyroid_lobe_measured,
  ANY_VALUE(t.total_thyroid_weight_g_latest)                          AS total_thyroid_weight_g,
  MAX(CASE WHEN ev.gland_type='thyroid_lobe' AND ev.gland_position='left'
           THEN ev.gland_length_cm END)                                AS left_lobe_max_dim_cm,
  MAX(CASE WHEN ev.gland_type='thyroid_lobe' AND ev.gland_position='right'
           THEN ev.gland_length_cm END)                                AS right_lobe_max_dim_cm,
  BOOL_OR(ev.gland_type='thyroid_lobe' AND ev.gland_position='isthmus'
          AND (ev.gland_length_cm IS NOT NULL OR ev.gland_weight_g IS NOT NULL))
                                                                       AS any_isthmus_documented,
  BOOL_OR(ev.gland_type='parathyroid')                                AS any_parathyroid_documented,
  COUNT(DISTINCT CASE WHEN ev.gland_type='parathyroid' THEN ev.gland_position END)
                                                                       AS n_parathyroid_glands_documented,
  COUNT(DISTINCT CASE WHEN ev.gland_type='parathyroid' AND (
      LOWER(COALESCE(ev.gland_pathology,'')) LIKE '%hyperplasia%'
      OR LOWER(COALESCE(ev.gland_pathology,'')) LIKE '%adenoma%'
      OR LOWER(COALESCE(ev.gland_pathology,'')) LIKE '%hypercellular%')
    THEN ev.gland_position END)                                        AS n_parathyroid_glands_abnormal,
  BOOL_OR(ev.gland_type='parathyroid' AND LOWER(COALESCE(ev.gland_pathology,'')) LIKE '%hyperplasia%')
                                                                       AS any_parathyroid_hyperplasia,
  BOOL_OR(ev.gland_type='parathyroid' AND LOWER(COALESCE(ev.gland_pathology,'')) LIKE '%adenoma%')
                                                                       AS any_parathyroid_adenoma,
  '361_via_mig_101'::VARCHAR                                          AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                AS build_ts
FROM ev
LEFT JOIN thy_latest t ON t.research_id=ev.research_id
GROUP BY ev.research_id;

-- 101c: flip 10 not_started cols to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_post_events_repair',
    batch_id            = 'mig_101_path_gland_rollup_signoff_20260428',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_101: rebuilt rollup via CREATE OR REPLACE '
                          || 'using Script 361 derivation logic against current '
                          || 'canonical_path_gland_events_v1 (post Cursor repair '
                          || '06f88ae). Pre-rebuild diff: 9/10 cols matched '
                          || '100%; n_parathyroid_glands_documented drifted on '
                          || '565/10,731 pts because parathyroid taxonomy was '
                          || 'renormalized in events. Post-rebuild: rollup '
                          || 'consistent with events. build_ts retyped TZ->TS.'
WHERE schema_name='main'
  AND table_name='canonical_path_gland_patient_rollup_v1'
  AND verification_status='not_started';

-- 101d: recompute table_signoff_registry counts and sign off
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
    signed_off_ts   = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/101_path_gland_patient_rollup_signoff.sql',
    notes           = 'Rebuilt via Script 361 derivation logic post-Cursor '
                      || 'events repair (06f88ae). All 10 derived cols match '
                      || 'fresh re-derivation. Pre-rebuild diff isolated to '
                      || 'n_parathyroid_glands_documented (parathyroid taxonomy '
                      || 'renormalization side-effect). build_ts retyped from '
                      || 'TIMESTAMP WITH TIME ZONE to TIMESTAMP.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_path_gland_patient_rollup_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 101 -- canonical_path_gland_patient_rollup_v1 closed
-- 19th table verified under Protocol v2; path_gland family complete.
-- =============================================================================
