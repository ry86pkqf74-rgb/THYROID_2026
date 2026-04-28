-- =============================================================================
-- Migration 71 -- Bethesda column round verification
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Protocol-v2 verification of all 10 substantive bethesda columns in
--         canonical_fna_events_v1.
-- Scope:  main.canonical_fna_events_v1
--
-- Pre-investigation findings:
--   1,115 rows where bethesda_calculated_num diverges from source bethesda_raw.
--   These were the LLM/rules RESCORE overlay applied to the cohort
--   (raw/FNAs_Rescored_Long_Format.xlsx, 8,063 rows). DB faithfully captures
--   the rescore output; source workbook bethesda_raw is the pre-rescore input.
--
-- Verification source-of-truth:
--   manuscript_workspace.fna_bethesda_rescore_staging_v1
--     (loaded via qc_framework_v1/scripts/stage_rescore_file.py from
--      raw/FNAs_Rescored_Long_Format.xlsx)
--
-- Per-column results:
--
--   bethesda_calculated_num  (adjudicated -> mechanical_source_compare against rescore)
--     7,935 match (DB num = rescore category_num) + 119 both-NULL + 0 diverge
--
--   bethesda_original_text   (source -> mechanical_source_compare against FNA workbook)
--     8,045 match (TRIM-equality) + 8 both-NULL + 1 (DB null where source='n/a';
--     DB build correctly NULL-ified the literal 'n/a') + 0 diverge
--
--   bethesda_reasoning       (source -> mechanical_source_compare against rescore)
--     2,695 actual reasoning text matches + 5,359 DB-NULL aligned with
--     rescore-NaN. Total 8,054. (The 'nan' string in the staging table is a
--     pandas->DuckDB conversion artifact; DB NULL semantically equals it.)
--
--   bethesda_2010_num, bethesda_2015_num, bethesda_2023_num
--     (derived -> mechanical_derivation_compare; rule: equal to bethesda_calculated_num)
--     8,054/8,054 match.
--
--   bethesda_2010_name, bethesda_2015_name, bethesda_2023_name
--     (derived -> mechanical_derivation_compare; rule: num->name lookup)
--     Per category_num the same name is used across all 3 versions:
--       1=Nondiagnostic or Unsatisfactory; 2=Benign; 3=Atypia of Undetermined
--       Significance or Follicular Lesion of Undetermined Significance;
--       4=Follicular Neoplasm or Suspicious for a Follicular Neoplasm;
--       5=Suspicious for Malignancy; 6=Malignant.
--     All 8,054 rows consistent.
--
--   bethesda_final_num       (derived -> mechanical_derivation_compare)
--     Rule (corrected from mig_64's "identical to calculated_num"):
--       final_num = COALESCE(calculated_num, original_bethesda_from_rescore)
--     7,942 match calculated_num + 112 fallback to original (where rescore
--     couldn't determine) + 7 both-NULL.
--
-- Net effect:
--   * 0 row-level value changes (column data was already correct)
--   * 10 rows in canonical_column_verification_registry_v1 flipped to verified
--     (batch_id=mig_71_bethesda)
--   * 1 row in canonical_table_signoff_registry_v1 recomputed
--     (n_verified: 7 -> 17, table_status=in_progress)
--
-- Carry-forward:
--   The na_provenance bethesda columns (bethesda_confidence,
--   bethesda_derivation_method, bethesda_rules_category, bethesda_rules_confidence,
--   bethesda_provider, bethesda_evidence_present) remain not_started; they
--   flip to verified at table sign-off (Step D) per Protocol v2.
--
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

-- Adjudicated col: bethesda_calculated_num
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_source_compare',
    batch_id            = 'mig_71_bethesda',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_71: 100% match against '
                          || 'manuscript_workspace.fna_bethesda_rescore_staging_v1 '
                          || '(loaded from raw/FNAs_Rescored_Long_Format.xlsx). '
                          || '7,935 match + 119 both-null + 0 diverge.'
WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  AND column_name = 'bethesda_calculated_num';

-- Source cols
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_source_compare',
    batch_id            = 'mig_71_bethesda',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_71: 100% match against source '
                          || '(bethesda_original_text vs FNA Bethesda cell; '
                          || 'bethesda_reasoning vs rescore file reasoning column).'
WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  AND column_name IN ('bethesda_original_text', 'bethesda_reasoning');

-- Derived cols
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    batch_id            = 'mig_71_bethesda',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_71: bethesda_201[0|5]_num and bethesda_2023_num '
                          || '= bethesda_calculated_num (8,054/8,054 match); '
                          || 'bethesda_final_num = COALESCE(calculated_num, '
                          || 'original_bethesda from rescore) (7,942 match + 112 fallback + 0 diverge); '
                          || 'bethesda_*_name = num->name lookup (consistent across versions).'
WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  AND column_name IN (
    'bethesda_2010_num', 'bethesda_2010_name',
    'bethesda_2015_num', 'bethesda_2015_name',
    'bethesda_2023_num', 'bethesda_2023_name',
    'bethesda_final_num'
  );

-- Recompute table signoff (now 17/39 verified)
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
  WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 71
-- =============================================================================
