-- =============================================================================
-- Migration 128 — tier3_extraction: last 5 raw LLM mirror tables, `error` col
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork / Cursor Lane 20)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Goal:   Flip the lone remaining `not_started` column (`error`, INTEGER — LLM
--         extractor exit code) on each of five `note_entities_llm_*` raw-output
--         mirrors to verified under Protocol v2 raw-mirror-exempt extension
--         (same posture as mig_109 for the prior 10 tier3_extraction mirrors).
--
-- Probes executed 2026-04-29 against thyroid_canonical_publication_v1_0.main
-- (MotherDuck, query_rw immediately before authoring this migration):
--
--   table                          rows   error_zero  error_nonzero  error_null  distinct_vals
--   ----------------------------  ------  ----------  -------------  ----------  -------------
--   note_entities_llm_airway_invasion_v2      6054       6054           0            0           0
--   note_entities_llm_ete_subgrade_v1          287        287           0            0           0
--   note_entities_llm_parathyroid_detail_v1   8697       8697           0            0           0
--   note_entities_llm_t4b_invasion_v1          944        944           0            0           0
--   note_entities_llm_vascular_invasion_v2    3861       3861           0            0           0
--
-- All distinct error values observed: {0} only. Interpretation: 0 = successful
-- extraction pipeline run; nonzero would signal loader/LLM failure (none present).
--
-- Verification method: raw_llm_mirror_error_distribution_audit (distribution
-- review only — no row-by-row content verification; extractor QC occurs upstream).
--
-- Gate 1 effect: verified_tables_total 66 → +5 = 71 (tier3 tier3_extraction).
-- Gate 5: unchanged — audit filters verified tables with LIKE 'canonical_%'
--         only (mig_127 refinement); these note_entities_llm_* tables do not appear.
--
-- Sign-off artifact: qc_framework_v1/migrations/128_tier3_extraction_error_col_batch_signoff_20260429.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 128a–e: flip `error` on each raw mirror table
-- ---------------------------------------------------------------------------

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'raw_llm_mirror_error_distribution_audit',
    batch_id = 'tier3_extraction_error_col_batch_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_128: airway_invasion_v2 probe N=6054 rows; error code 0 '
            || 'only (0 nonzero, 0 NULL). Raw LLM mirror; signoff_migration '
            || '= this file.'
WHERE schema_name = 'main'
  AND table_name = 'note_entities_llm_airway_invasion_v2'
  AND column_name = 'error'
  AND verification_status = 'not_started';

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'raw_llm_mirror_error_distribution_audit',
    batch_id = 'tier3_extraction_error_col_batch_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_128: ete_subgrade_v1 probe N=287 rows; error code 0 '
            || 'only (0 nonzero, 0 NULL). Raw LLM mirror; signoff_migration '
            || '= this file.'
WHERE schema_name = 'main'
  AND table_name = 'note_entities_llm_ete_subgrade_v1'
  AND column_name = 'error'
  AND verification_status = 'not_started';

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'raw_llm_mirror_error_distribution_audit',
    batch_id = 'tier3_extraction_error_col_batch_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_128: parathyroid_detail_v1 probe N=8697 rows; error code 0 '
            || 'only (0 nonzero, 0 NULL). Raw LLM mirror; signoff_migration '
            || '= this file.'
WHERE schema_name = 'main'
  AND table_name = 'note_entities_llm_parathyroid_detail_v1'
  AND column_name = 'error'
  AND verification_status = 'not_started';

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'raw_llm_mirror_error_distribution_audit',
    batch_id = 'tier3_extraction_error_col_batch_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_128: t4b_invasion_v1 probe N=944 rows; error code 0 '
            || 'only (0 nonzero, 0 NULL). Raw LLM mirror; signoff_migration '
            || '= this file.'
WHERE schema_name = 'main'
  AND table_name = 'note_entities_llm_t4b_invasion_v1'
  AND column_name = 'error'
  AND verification_status = 'not_started';

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'raw_llm_mirror_error_distribution_audit',
    batch_id = 'tier3_extraction_error_col_batch_20260429',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
            || ' | mig_128: vascular_invasion_v2 probe N=3861 rows; error code 0 '
            || 'only (0 nonzero, 0 NULL). Raw LLM mirror; signoff_migration '
            || '= this file.'
WHERE schema_name = 'main'
  AND table_name = 'note_entities_llm_vascular_invasion_v2'
  AND column_name = 'error'
  AND verification_status = 'not_started';

-- ---------------------------------------------------------------------------
-- Table rollups — not_started → verified (14 cols each = 13 na + 1 verified)
-- ---------------------------------------------------------------------------

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
    signoff_migration = 'qc_framework_v1/migrations/128_tier3_extraction_error_col_batch_signoff_20260429.sql',
    notes = 'Tier3_extraction raw LLM-output mirror per Protocol v2. All 13 provenance/metadata cols na; mig_128 verifies INTEGER error column via distribution audit (same posture as mig_109 ten-table batch). Probe 2026-04-29: all rows error=0.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'note_entities_llm_airway_invasion_v2'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

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
    signoff_migration = 'qc_framework_v1/migrations/128_tier3_extraction_error_col_batch_signoff_20260429.sql',
    notes = 'Tier3_extraction raw LLM-output mirror per Protocol v2. All 13 provenance/metadata cols na; mig_128 verifies INTEGER error column via distribution audit (same posture as mig_109 ten-table batch). Probe 2026-04-29: all rows error=0.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'note_entities_llm_ete_subgrade_v1'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

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
    signoff_migration = 'qc_framework_v1/migrations/128_tier3_extraction_error_col_batch_signoff_20260429.sql',
    notes = 'Tier3_extraction raw LLM-output mirror per Protocol v2. All 13 provenance/metadata cols na; mig_128 verifies INTEGER error column via distribution audit (same posture as mig_109 ten-table batch). Probe 2026-04-29: all rows error=0.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'note_entities_llm_parathyroid_detail_v1'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

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
    signoff_migration = 'qc_framework_v1/migrations/128_tier3_extraction_error_col_batch_signoff_20260429.sql',
    notes = 'Tier3_extraction raw LLM-output mirror per Protocol v2. All 13 provenance/metadata cols na; mig_128 verifies INTEGER error column via distribution audit (same posture as mig_109 ten-table batch). Probe 2026-04-29: all rows error=0.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'note_entities_llm_t4b_invasion_v1'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

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
    signoff_migration = 'qc_framework_v1/migrations/128_tier3_extraction_error_col_batch_signoff_20260429.sql',
    notes = 'Tier3_extraction raw LLM-output mirror per Protocol v2. All 13 provenance/metadata cols na; mig_128 verifies INTEGER error column via distribution audit (same posture as mig_109 ten-table batch). Probe 2026-04-29: all rows error=0.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'note_entities_llm_vascular_invasion_v2'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- =============================================================================
-- end migration 128 — all 15 tier3_extraction note_entities_llm_* tables closed
-- =============================================================================
