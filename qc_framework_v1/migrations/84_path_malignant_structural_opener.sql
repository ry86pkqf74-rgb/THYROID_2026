-- =============================================================================
-- Migration 84 -- canonical_path_malignant_events_v1 STRUCTURAL OPENER (Step A)
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Open Protocol v2 verification of canonical_path_malignant_events_v1.
--         Three structural moves before per-column work begins:
--
--   84a:   Drop 11 dependent QA fingerprint views that explicitly hash the
--          deprecated columns by name in their MD5 expression. These were
--          built during the 2026-04-21 -- 2026-04-23 cleanup push as
--          change-detection hashes; not analytic deliverables. Per Logan
--          (this session): "Drop the 11 fingerprint views in mig_84." Per
--          memory feedback_alter_view_dependents.md: dependent views with
--          hard-coded column names break on next recompile when the column
--          is dropped, so they must be reconciled in the same migration.
--          Views dropped (all in manuscript_workspace):
--            _path_malignant_row_fingerprint_v1
--            path_malignant_event_fingerprint_v1
--            path_event_discordance_dedup_ete_v1
--            path_malignant_overlay_ete_clean_w_fp_v1
--            path_malignant_overlay_global_epi_w_fp_v1
--            path_malignant_overlay_histology_w_fp_v1
--            path_malignant_overlay_invasion_w_fp_v1
--            path_malignant_overlay_laterality_w_fp_v1
--            path_malignant_overlay_ln_denom_w_fp_v1
--            path_malignant_overlay_size_flag_w_fp_v1
--            path_malignant_overlay_variant_w_fp_v1
--          The 8 underlying `*_clean` views and `_keyed` view are KEPT --
--          they SELECT p.* and auto-shrink when columns drop.
--
--   84b/c: Drop 4 *_deprecated_un_versioned_20260417 staging columns.
--          Per Logan: "Drop in mig_84 (FNA pilot precedent)."
--          - overall_stage_deprecated: 0 / 6,689 populated (entirely NULL)
--          - t_stage_deprecated      : 5,089 / 6,689 populated, but agreement
--            with current AJCC8 is 207 / 6,689 (3.1 %)
--          - n_stage_deprecated      : 5,093 / 6,689, ajcc8 agreement 429 (6.4 %)
--          - m_stage_deprecated      : 5,079 / 6,689, ajcc8 agreement 1,089 (16 %)
--          These are stale Script-266c-era staging from before the AJCC7/AJCC8
--          split. Saves 4 columns of probe + CSV + Logan review for stale data.
--          FNA pilot precedent: mig_78b dropped is_index_fna inline.
--          (Note: FNA's drop had zero dependent views; this drop required
--          the additional 84a step.)
--
--   84d:   Reclass 3 cols from adjudicated -> na_provenance, with
--          verification_method='auto_no_source_counterpart'. Status remains
--          'not_started' (Step D batch flip). Joins the 9 already-na cols for
--          a 12-col Step D batch.
--            - synoptic_row_ix : Script 108 pandas-load-order global index, not
--                                SQL-reproducible (memory: reference_synoptic_row_ix.md).
--            - histology_source: pipeline-trace ("which source the histology
--                                came from"); no upstream source counterpart.
--            - resolution_rule : pipeline-trace describing how the row was
--                                assembled by the consolidation script.
--
--   84e:   Recompute table_signoff_registry counts (table goes 60 -> 56 cols).
--
-- Per-column verification begins in mig_85, starting with surgery_date as the
-- natural-key anchor (mechanical_source_compare against path_synoptics.surg_date).
--
-- Logan-ratified decisions (this session):
--   1. Drop the 4 deprecated cols.
--   2. AJCC7/AJCC8 staging cols (10) will be verified via
--      mechanical_derivation_compare from findings, per the airway-invasion
--      precedent and feedback_findings_vs_staging.md.
--
-- Final post-mig_84 column tally for canonical_path_malignant_events_v1:
--   A  auto_no_source_counterpart : 12  (Step D batch flip)
--   B  mechanical_source_compare  : 16  (path_synoptics tumor_N_*)
--   C  mechanical_derivation_compare: 24  (rollups + 10 AJCC7/8 staging cols)
--   D  manual_source_review       :  4  (staging_source_note,
--                                        stage_migration_7_to_8,
--                                        discordance_notes,
--                                        linkage_confidence_tier)
--   total                          : 56
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 84a: drop 11 dependent QA fingerprint views (must precede column drops)
DROP VIEW IF EXISTS manuscript_workspace.path_malignant_overlay_variant_w_fp_v1;
DROP VIEW IF EXISTS manuscript_workspace.path_malignant_overlay_size_flag_w_fp_v1;
DROP VIEW IF EXISTS manuscript_workspace.path_malignant_overlay_invasion_w_fp_v1;
DROP VIEW IF EXISTS manuscript_workspace.path_malignant_overlay_ete_clean_w_fp_v1;
DROP VIEW IF EXISTS manuscript_workspace.path_malignant_overlay_global_epi_w_fp_v1;
DROP VIEW IF EXISTS manuscript_workspace.path_malignant_overlay_laterality_w_fp_v1;
DROP VIEW IF EXISTS manuscript_workspace.path_malignant_overlay_ln_denom_w_fp_v1;
DROP VIEW IF EXISTS manuscript_workspace.path_malignant_overlay_histology_w_fp_v1;
DROP VIEW IF EXISTS manuscript_workspace.path_event_discordance_dedup_ete_v1;
DROP VIEW IF EXISTS manuscript_workspace.path_malignant_event_fingerprint_v1;
DROP VIEW IF EXISTS manuscript_workspace._path_malignant_row_fingerprint_v1;

-- 84b: drop 4 deprecated staging columns
ALTER TABLE main.canonical_path_malignant_events_v1
  DROP COLUMN t_stage_deprecated_un_versioned_20260417;
ALTER TABLE main.canonical_path_malignant_events_v1
  DROP COLUMN n_stage_deprecated_un_versioned_20260417;
ALTER TABLE main.canonical_path_malignant_events_v1
  DROP COLUMN m_stage_deprecated_un_versioned_20260417;
ALTER TABLE main.canonical_path_malignant_events_v1
  DROP COLUMN overall_stage_deprecated_un_versioned_20260417;

-- 84c: delete corresponding registry rows
DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name  = 'canonical_path_malignant_events_v1'
  AND column_name IN (
    't_stage_deprecated_un_versioned_20260417',
    'n_stage_deprecated_un_versioned_20260417',
    'm_stage_deprecated_un_versioned_20260417',
    'overall_stage_deprecated_un_versioned_20260417'
  );

-- 84d: reclass 3 cols (adjudicated -> na_provenance, method=auto_no_source_counterpart)
UPDATE main.canonical_column_verification_registry_v1
SET category            = 'na_provenance',
    verification_method = 'auto_no_source_counterpart',
    notes               = COALESCE(notes,'')
                          || ' | mig_84c: reclassed adjudicated -> na_provenance. '
                          || 'synoptic_row_ix: Script 108 pandas-load-order (memory: reference_synoptic_row_ix.md). '
                          || 'histology_source / resolution_rule: pipeline-trace, no upstream source counterpart. '
                          || 'Status remains not_started; will flip at Step D batch flip.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_path_malignant_events_v1'
  AND column_name IN ('synoptic_row_ix', 'histology_source', 'resolution_rule');

-- 84e: recompute table_signoff_registry counts (60 -> 56 cols)
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
-- end of migration 84 -- structural opener; mig_85 begins per-column work
-- =============================================================================
