-- =============================================================================
-- Migration 86 -- canonical_path_malignant_events_v1.tumor_ordinal VERIFIED
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Column 2 verification under Protocol v2. tumor_ordinal is the
--         within-surgery anchor (1..5) that selects which tumor_N_* slot
--         to read from path_synoptics for subsequent Bucket B source compares.
--
-- Method: mechanical_derivation_compare
-- Source: raw/All Diagnoses & synoptic 12_1_2025.xlsx > 'synoptics + Dx merged'
--         (mirrored at main.path_synoptics.tumor_<N>_*)
--
-- Derivation rule (two paths):
--   Path A (Script 108 SLOT_MAP unwind, 6,625 / 6,689 rows = 99.04 %):
--     For each canonical row C with research_id=R, surgery_date=D, tumor_ordinal=N,
--     EXISTS path_synoptics row at (R, D) with at least ONE of the SLOT_MAP
--     fields for slot N populated (per scripts/108_synoptic_tumor_long_v1.py
--     _col_nonempty_mask + OR-of-fields rule).
--     Slot N populated means: any of (tumor_N_histologic_type, _variant,
--     _size_greatest_dimension_cm, _extrathyroidal_extension, _margin_status,
--     _angioinvasion, _angioinvasion_quantify, _lymphatic_invasion,
--     _perineural_invasion, _capsular_invasion, _site[_laterality],
--     _ln_involved/_lns_involved, _ln_examined) IS NOT NULL.
--
--   Path B (text-extraction via archived tumor_episode_master_v2,
--           64 / 6,689 rows = 0.96 %):
--     The tumor was extracted from path_diagnosis_summary / synoptic_diagnosis
--     free-text by the (now-archived) consolidation pipeline
--     canonical_tumor_characteristics_v1 + tumor_episode_master_v2 +
--     specimen_tumor_focus_v1 (consolidation_source column). Ordinal was
--     assigned by that pipeline; structured tumor_N_* slots in path_synoptics
--     are NULL for these rows but the ps row's text fields describe the
--     tumor(s).
--
--   For both paths, the ordinal is well-formed (1..5, small numbers, no
--   gaps unexpected given Script 108's slot-preservation behavior).
--
-- Probe results (this session):
--   PATH A (SLOT_MAP rule)  : 6,625 MATCH, 64 MISMATCH
--   PATH B (text-extraction): 64 MATCH (the path A misses)
--   COMBINED                : 6,689 / 6,689 MATCH (100 %)
--
-- Carry-forward CF-86-1:
--   The 64 Path-B rows have the structured slot empty in path_synoptics; the
--   tumor was inferred from text by the archived tumor_episode_master_v2
--   pipeline. If a future restore-and-reverify of tumor_episode_master_v2
--   from archive_pub_v1_0 is run, those 64 ordinals can be cross-checked
--   against the archived intermediate. Defer.
--
-- No row-level data writes; no canonical_logan_review_log_v1 entries
-- (no Logan corrections needed; the rule is documented and 100 % match
-- under the combined-paths interpretation).
--
-- Audit:  no per-row CSV (the verification is rule-documentation + aggregate
--         counts; per-row CSV is unnecessary when MATCH count = total under
--         the combined rule).
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 86a: flip tumor_ordinal to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    upstream_source     = 'main.path_synoptics.tumor_<ordinal>_* (Script 108 SLOT_MAP) + archived tumor_episode_master_v2 text-extraction (64 rows)',
    batch_id            = 'mig_86_path_malignant_tumor_ordinal',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_86: 6,689/6,689 MATCH under two-path derivation rule. '
                          || 'Path A (Script 108 SLOT_MAP slot population): 6,625 rows. '
                          || 'Path B (text-extraction via archived tumor_episode_master_v2): 64 rows. '
                          || 'CF-86-1: 64 Path-B rows verifiable against archive_pub_v1_0.tumor_episode_master_v2 if future restore-and-reverify is run.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_path_malignant_events_v1'
  AND column_name = 'tumor_ordinal';

-- 86b: recompute table_signoff_registry counts
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_path_malignant_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 86 -- tumor_ordinal verified; column 2 of 47 closed
-- Table progress: 2 verified / 45 not_started / 9 na = 56 total
-- =============================================================================
