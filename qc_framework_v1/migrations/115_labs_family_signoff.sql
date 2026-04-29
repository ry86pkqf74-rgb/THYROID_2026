-- =============================================================================
-- Migration 115 -- labs family Protocol v2 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session)
-- Author: Logan Glosser <logan.glosser@gmail.com>
-- Scope:  Close five Script-347 per-analyte canonical lab tables:
--           main.canonical_labs_thyroglobulin_v1
--           main.canonical_labs_calcium_v1
--           main.canonical_labs_pth_v1
--           main.canonical_labs_tsh_v1
--           main.canonical_labs_vitamin_d_v1
--
-- Methodology: structured_source_compare_with_normalizer
--   * Build context: Script 347 consolidated the lab layer into five per-
--     analyte canonical tables from longitudinal / thyroglobulin sources and
--     used scripts/_lab_value_normalizer.py as the single source of truth.
--   * Normalizer regression suite: tests/test_lab_value_normalizer.py passed
--     45/45 tests before sign-off.
--   * Per-row replay: re-ran normalize_lab_value(value_raw, analyte) against
--     every current row in the five lab tables and compared value_numeric,
--     is_censored, and value_correction_note with 1e-9 numeric tolerance and
--     pandas-NA-aware note comparison. Result: 0 mismatches in all five tables.
--   * Unit/source checks: expected canonical units and source enums are clean.
--   * Date policy: lab_datetime is TIMESTAMP and is intentionally retained as
--     TIMESTAMP. Current rows are all midnight-valued, reflecting the available
--     source timestamps at build time; this sign-off does not retype labs to
--     DATE because lab measurement timestamps remain clinically valid when
--     present in future feeds.
--
-- Pre-signoff probe (2026-04-29):
--   Table                                rows   patients  value/censor/note drift
--   canonical_labs_thyroglobulin_v1     53,006   3,124   0 / 0 / 0
--   canonical_labs_calcium_v1              187     166   0 / 0 / 0
--   canonical_labs_pth_v1                  200     184   0 / 0 / 0
--   canonical_labs_tsh_v1                  556     449   0 / 0 / 0
--   canonical_labs_vitamin_d_v1             86      82   0 / 0 / 0
--
-- Sanity-range carry-forwards (informational, not sign-off blockers):
--   * Calcium has 13 non-censored rows outside the conservative 6-15 mg/dL
--     prompt range, but all are accepted by the Script-347 normalizer's broader
--     4-20 mg/dL plausibility range after deterministic correction where
--     needed (e.g. 500 -> 5.0, 200 -> 20.0).
--   * PTH has one row at 1399 pg/mL; accepted by the normalizer's 0-3000
--     plausibility range.
--   * TSH has six non-censored rows outside 0.01-100 mIU/L; accepted by the
--     normalizer's 0-150 plausibility range, including one exact 0.
--   These are preserved as observed clinical/lab values, not registry failures.
--
-- Final expected registry state:
--   canonical_labs_thyroglobulin_v1: 11 verified + 1 na = 12/12 closed
--   Each non-Tg lab table:           9 verified + 1 na = 10/10 closed
-- =============================================================================

BEGIN TRANSACTION;

-- 115a: canonical_labs_thyroglobulin_v1
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'structured_source_compare_with_normalizer',
    batch_id            = 'mig_115_labs_family_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_115: Script 347 lab canonical replay. '
                          || 'normalize_lab_value(value_raw, analyte) compared '
                          || 'per-row against stored value_numeric, is_censored, '
                          || 'and value_correction_note; 53,006/53,006 rows '
                          || 'match with 0 drift. Unit vocab clean (Tg=ng/mL, '
                          || 'TgAb=IU/mL), source enum clean, 3,124 patients. '
                          || 'lab_datetime retained as TIMESTAMP per lab policy.'
WHERE schema_name='main'
  AND table_name='canonical_labs_thyroglobulin_v1'
  AND verification_status='not_started';

-- 115b: canonical_labs_calcium_v1
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'structured_source_compare_with_normalizer',
    batch_id            = 'mig_115_labs_family_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_115: Script 347 lab canonical replay. '
                          || 'normalize_lab_value(value_raw, calcium) compared '
                          || 'per-row against stored value_numeric, is_censored, '
                          || 'and value_correction_note; 187/187 rows match '
                          || 'with 0 drift. Unit vocab clean (mg/dL), source '
                          || 'enum clean, 166 patients. 13 conservative-range '
                          || 'outliers remain within Script-347 normalizer '
                          || 'plausibility/correction policy. lab_datetime '
                          || 'retained as TIMESTAMP per lab policy.'
WHERE schema_name='main'
  AND table_name='canonical_labs_calcium_v1'
  AND verification_status='not_started';

-- 115c: canonical_labs_pth_v1
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'structured_source_compare_with_normalizer',
    batch_id            = 'mig_115_labs_family_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_115: Script 347 lab canonical replay. '
                          || 'normalize_lab_value(value_raw, pth) compared '
                          || 'per-row against stored value_numeric, is_censored, '
                          || 'and value_correction_note; 200/200 rows match '
                          || 'with 0 drift. Unit vocab clean (pg/mL), source '
                          || 'enum clean, 184 patients. One value (1399 pg/mL) '
                          || 'is above the conservative prompt range but within '
                          || 'Script-347 normalizer plausibility. lab_datetime '
                          || 'retained as TIMESTAMP per lab policy.'
WHERE schema_name='main'
  AND table_name='canonical_labs_pth_v1'
  AND verification_status='not_started';

-- 115d: canonical_labs_tsh_v1
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'structured_source_compare_with_normalizer',
    batch_id            = 'mig_115_labs_family_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_115: Script 347 lab canonical replay. '
                          || 'normalize_lab_value(value_raw, tsh) compared '
                          || 'per-row against stored value_numeric, is_censored, '
                          || 'and value_correction_note; 556/556 rows match '
                          || 'with 0 drift. Unit vocab clean (mIU/L), source '
                          || 'enum clean, 449 patients. Six conservative-range '
                          || 'outliers remain within Script-347 normalizer '
                          || 'plausibility policy. lab_datetime retained as '
                          || 'TIMESTAMP per lab policy.'
WHERE schema_name='main'
  AND table_name='canonical_labs_tsh_v1'
  AND verification_status='not_started';

-- 115e: canonical_labs_vitamin_d_v1
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'structured_source_compare_with_normalizer',
    batch_id            = 'mig_115_labs_family_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_115: Script 347 lab canonical replay. '
                          || 'normalize_lab_value(value_raw, vitamin_d) compared '
                          || 'per-row against stored value_numeric, is_censored, '
                          || 'and value_correction_note; 86/86 rows match with '
                          || '0 drift. Unit vocab clean (ng/mL), source enum '
                          || 'clean, 82 patients. lab_datetime retained as '
                          || 'TIMESTAMP per lab policy.'
WHERE schema_name='main'
  AND table_name='canonical_labs_vitamin_d_v1'
  AND verification_status='not_started';

-- 115f: recompute sign-off counts for all five lab canonical tables.
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
    signoff_migration = 'qc_framework_v1/migrations/115_labs_family_signoff.sql',
    notes             = 'Labs family closed under Protocol v2 via Script 347 '
                        || 'structured-source/normalizer replay. 45/45 '
                        || 'normalizer tests passed; per-row replay found 0 '
                        || 'value_numeric, is_censored, or value_correction_note '
                        || 'drift across all five tables. Unit vocab and source '
                        || 'enum checks clean. lab_datetime remains TIMESTAMP '
                        || '(all current rows midnight-valued from available '
                        || 'source timestamps, but labs retain timestamp type '
                        || 'for future draw-time fidelity). Conservative range '
                        || 'exceptions documented as normalizer-plausible values.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main'
    AND table_name IN (
      'canonical_labs_thyroglobulin_v1',
      'canonical_labs_calcium_v1',
      'canonical_labs_pth_v1',
      'canonical_labs_tsh_v1',
      'canonical_labs_vitamin_d_v1'
    )
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

COMMIT;

-- =============================================================================
-- end of migration 115 -- labs family closed (5 Tier-2 canonicals)
-- =============================================================================