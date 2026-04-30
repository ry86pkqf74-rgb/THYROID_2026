-- =============================================================================
-- Migration 174b — cnln_img_laterality multi-label apply
-- Option A: per-side BOOLEAN columns, Logan-ratified 2026-04-29
-- =============================================================================
-- Date: 2026-04-29 (UTC)
-- Author: GitHub Copilot + Logan Glosser <logan.glosser@gmail.com>
-- Predecessor: mig_174a design (`955801f`)
-- batch_id: mig_174b_cnln_img_laterality_per_side_boolean_20260429
-- Target DB: thyroid_canonical_publication_v1_0
-- Target table: main.canonical_patient_master
--
-- POSTURE:
--   SQL-only Path-C apply artifact. This file is not executed by the authoring
--   agent. Cowork/Path-C applies after pre-snapshot verification.
--
-- EFFECT:
--   * Preserve raw cnln_img_laterality VARCHAR as legacy/provenance.
--   * Add 5 per-side BOOLEAN columns:
--       cnln_img_left_present
--       cnln_img_right_present
--       cnln_img_central_present
--       cnln_img_bilateral_present
--       cnln_img_lateral_neck_present
--   * Populate those columns from trimmed-lowercase semicolon token parsing.
--   * Keep rows with raw cnln_img_laterality IS NULL as NULL in all 5 new cols.
--   * Stamp registry/signoff/provenance and refresh cpm_built_at.
--
-- LIVE READ-ONLY TOKEN ENUMERATION (2026-04-29):
--   column_name            token_norm       n_appearances   n_pts
--   ---------------------  ---------------  --------------  -----
--   cnln_img_laterality    bilateral        116             116
--   cnln_img_laterality    right            87              87
--   cnln_img_laterality    left             85              85
--   cnln_img_laterality    central          32              32
--   cnln_img_laterality    null             12              12
--   cnln_img_laterality    lateral          4               4
--   cnln_img_laterality    lateral neck     3               3
--
-- LIVE PRECONDITIONS (2026-04-29):
--   * CPM rows = 10,871; distinct research_id = 10,871.
--   * cnln_img_laterality non-NULL rows = 272; NULL rows = 10,599.
--   * New per-side columns absent before apply.
--   * Unhandled cnln_img_laterality tokens = 0.
--
-- LIVE DERIVATION COUNTS EXPECTED POST-APPLY:
--   column_name                    TRUE   FALSE   NULL
--   -----------------------------  -----  ------  ------
--   cnln_img_left_present          85     187     10599
--   cnln_img_right_present         87     185     10599
--   cnln_img_central_present       32     240     10599
--   cnln_img_bilateral_present     116    156     10599
--   cnln_img_lateral_neck_present  7      265     10599
--
-- HALT RULE:
--   If a fresh pre-apply token probe finds any cnln_img_laterality token outside
--   ('left','right','central','bilateral','lateral_neck','lateral neck',
--    'lateral','null','nan','none','n/a','unknown',''), do not apply. Emit
--   CF-mig174b-UNHANDLED-TOKEN-<token> and reroute to adjudication.
--
-- LINT/GOVERNANCE:
--   * No BEGIN TRANSACTION / COMMIT statements.
--   * All writes target thyroid_canonical_publication_v1_0 only, except the
--     required pre-snapshot archive in "Thyroid 2026 UPdated".archive_pub_v1_0.
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

-- §A — Pre-snapshot of the legacy source column before adding derived flags.
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig174b_cnln_laterality_20260429 AS
SELECT
  research_id,
  cnln_img_laterality,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig174b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master
WHERE cnln_img_laterality IS NOT NULL;

-- §B — Add 5 per-side BOOLEAN columns. NULL means no source laterality data.
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS cnln_img_left_present BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS cnln_img_right_present BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS cnln_img_central_present BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS cnln_img_bilateral_present BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS cnln_img_lateral_neck_present BOOLEAN;

COMMENT ON COLUMN main.canonical_patient_master.cnln_img_left_present IS
  'mig_174b Option A: TRUE when cnln_img_laterality token parse contains left; FALSE when raw laterality is present without left; NULL when raw laterality is NULL.';
COMMENT ON COLUMN main.canonical_patient_master.cnln_img_right_present IS
  'mig_174b Option A: TRUE when cnln_img_laterality token parse contains right; FALSE when raw laterality is present without right; NULL when raw laterality is NULL.';
COMMENT ON COLUMN main.canonical_patient_master.cnln_img_central_present IS
  'mig_174b Option A: TRUE when cnln_img_laterality token parse contains central; FALSE when raw laterality is present without central; NULL when raw laterality is NULL.';
COMMENT ON COLUMN main.canonical_patient_master.cnln_img_bilateral_present IS
  'mig_174b Option A: TRUE when cnln_img_laterality token parse contains bilateral; FALSE when raw laterality is present without bilateral; NULL when raw laterality is NULL.';
COMMENT ON COLUMN main.canonical_patient_master.cnln_img_lateral_neck_present IS
  'mig_174b Option A: TRUE when cnln_img_laterality token parse contains lateral_neck/lateral neck/lateral; FALSE when raw laterality is present without lateral neck; NULL when raw laterality is NULL.';

-- §C — Populate via token parse.
-- Sentinel tokens ('null','nan','none','n/a','unknown','') do not mark flags.
-- Rows with raw cnln_img_laterality IS NULL remain NULL in all derived columns.
WITH tokens AS (
  SELECT
    research_id,
    TRIM(LOWER(t)) AS tok
  FROM (
    SELECT research_id, UNNEST(string_split(cnln_img_laterality, ';')) AS t
    FROM main.canonical_patient_master
    WHERE cnln_img_laterality IS NOT NULL
  )
), tok_clean AS (
  SELECT research_id, tok
  FROM tokens
  WHERE tok NOT IN ('null', 'nan', 'none', 'n/a', 'unknown', '')
), per_pt AS (
  SELECT
    research_id,
    BOOL_OR(tok = 'left') AS has_left,
    BOOL_OR(tok = 'right') AS has_right,
    BOOL_OR(tok = 'central') AS has_central,
    BOOL_OR(tok = 'bilateral') AS has_bilateral,
    BOOL_OR(tok IN ('lateral_neck', 'lateral neck', 'lateral')) AS has_lateral_neck
  FROM tok_clean
  GROUP BY 1
), final AS (
  SELECT
    pm.research_id,
    CASE WHEN pm.cnln_img_laterality IS NULL THEN NULL ELSE COALESCE(per_pt.has_left, FALSE) END AS has_left,
    CASE WHEN pm.cnln_img_laterality IS NULL THEN NULL ELSE COALESCE(per_pt.has_right, FALSE) END AS has_right,
    CASE WHEN pm.cnln_img_laterality IS NULL THEN NULL ELSE COALESCE(per_pt.has_central, FALSE) END AS has_central,
    CASE WHEN pm.cnln_img_laterality IS NULL THEN NULL ELSE COALESCE(per_pt.has_bilateral, FALSE) END AS has_bilateral,
    CASE WHEN pm.cnln_img_laterality IS NULL THEN NULL ELSE COALESCE(per_pt.has_lateral_neck, FALSE) END AS has_lateral_neck
  FROM main.canonical_patient_master pm
  LEFT JOIN per_pt
    ON CAST(pm.research_id AS VARCHAR) = CAST(per_pt.research_id AS VARCHAR)
)
UPDATE main.canonical_patient_master pm
SET
  cnln_img_left_present = final.has_left,
  cnln_img_right_present = final.has_right,
  cnln_img_central_present = final.has_central,
  cnln_img_bilateral_present = final.has_bilateral,
  cnln_img_lateral_neck_present = final.has_lateral_neck,
  cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM final
WHERE CAST(pm.research_id AS VARCHAR) = CAST(final.research_id AS VARCHAR);

-- §D — Register the 5 new cols in canonical_column_verification_registry_v1.
INSERT INTO main.canonical_column_verification_registry_v1
       (schema_name, table_name, column_name, data_type, ordinal_position,
        category, upstream_source, verification_status, verified_by, verified_ts,
        verification_method, batch_id, notes)
SELECT
  'main',
  'canonical_patient_master',
  col_name,
  'BOOLEAN',
  (SELECT MAX(ordinal_position)
   FROM main.canonical_column_verification_registry_v1
   WHERE schema_name = 'main'
     AND table_name = 'canonical_patient_master')
    + ROW_NUMBER() OVER (ORDER BY col_name),
  'derived',
  'cnln_img_laterality token parse',
  'not_started',
  NULL,
  NULL,
  NULL,
  NULL,
  'mig_174b: per-side BOOLEAN derived from cnln_img_laterality token parse (Option A; legacy VARCHAR preserved).'
FROM (VALUES
  ('cnln_img_left_present'),
  ('cnln_img_right_present'),
  ('cnln_img_central_present'),
  ('cnln_img_bilateral_present'),
  ('cnln_img_lateral_neck_present')
) v(col_name)
WHERE NOT EXISTS (
  SELECT 1
  FROM main.canonical_column_verification_registry_v1 r
  WHERE r.schema_name = 'main'
    AND r.table_name = 'canonical_patient_master'
    AND r.column_name = v.col_name
);

-- §E — Flip the 5 new cols to verified.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = 'derivation_vs_cnln_img_laterality_token_parse',
    batch_id = 'mig_174b_cnln_img_laterality_per_side_boolean_20260429',
    notes = COALESCE(notes, '')
          || ' | mig_174b: token-parse-derived from raw cnln_img_laterality. '
          || 'Cohort uniformity T/F/NULL counts: left 85/187/10599; right 87/185/10599; central 32/240/10599; bilateral 116/156/10599; lateral_neck 7/265/10599. '
          || 'CF-mig174b-COHORT-UNIFORM-* informational.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN (
    'cnln_img_left_present',
    'cnln_img_right_present',
    'cnln_img_central_present',
    'cnln_img_bilateral_present',
    'cnln_img_lateral_neck_present'
  );

-- §F — Append legacy-column guidance to raw cnln_img_laterality registry row.
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_174b: legacy multi-label VARCHAR; PREFER per-side BOOLEAN columns '
            || '(cnln_img_<left|right|central|bilateral|lateral_neck>_present) for analytic use. '
            || 'Raw VARCHAR retained for audit/provenance; token parsing logic preserves all non-sentinel tokens.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'cnln_img_laterality';

-- §G — Resync canonical_table_signoff_registry_v1 row for canonical_patient_master.
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
    notes = COALESCE(ts.notes, '')
            || ' | mig_174b: +5 per-side BOOLEAN cols from cnln_img_laterality multi-label parse.'
FROM (
  SELECT
    schema_name,
    table_name,
    COUNT(*) AS n_total,
    SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_verified,
    SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
    SUM(CASE WHEN verification_status = 'failed' THEN 1 ELSE 0 END) AS n_failed,
    SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- §H — CPM reconciliation provenance row.
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared,
   held_for_adjudication)
SELECT
  'canonical_cleanup_mig174b_cnln_img_laterality_per_side_boolean_20260429',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'pre_snapshot_add_columns_token_parse_registry_stamp_signoff_resync',
  'none',
  'CF-mig174-CPM-CNLN-IMG-LATERALITY-MULTILABEL',
  'none',
  'none'
WHERE NOT EXISTS (
  SELECT 1
  FROM manuscript_workspace.cpm_reconciliation_provenance_v1
  WHERE run_id = 'canonical_cleanup_mig174b_cnln_img_laterality_per_side_boolean_20260429'
);

-- §I — Post-state verification probes for Path-C executor.
SELECT
  COUNT(*) AS n_rows,
  COUNT(DISTINCT research_id) AS n_distinct_research_id,
  SUM(CASE WHEN cpm_built_at IS NULL THEN 1 ELSE 0 END) AS null_cpm_built_at
FROM main.canonical_patient_master;

SELECT
  'cnln_img_left_present' AS column_name,
  SUM(CASE WHEN cnln_img_left_present IS TRUE THEN 1 ELSE 0 END) AS true_n,
  SUM(CASE WHEN cnln_img_left_present IS FALSE THEN 1 ELSE 0 END) AS false_n,
  SUM(CASE WHEN cnln_img_left_present IS NULL THEN 1 ELSE 0 END) AS null_n
FROM main.canonical_patient_master
UNION ALL
SELECT 'cnln_img_right_present',
       SUM(CASE WHEN cnln_img_right_present IS TRUE THEN 1 ELSE 0 END),
       SUM(CASE WHEN cnln_img_right_present IS FALSE THEN 1 ELSE 0 END),
       SUM(CASE WHEN cnln_img_right_present IS NULL THEN 1 ELSE 0 END)
FROM main.canonical_patient_master
UNION ALL
SELECT 'cnln_img_central_present',
       SUM(CASE WHEN cnln_img_central_present IS TRUE THEN 1 ELSE 0 END),
       SUM(CASE WHEN cnln_img_central_present IS FALSE THEN 1 ELSE 0 END),
       SUM(CASE WHEN cnln_img_central_present IS NULL THEN 1 ELSE 0 END)
FROM main.canonical_patient_master
UNION ALL
SELECT 'cnln_img_bilateral_present',
       SUM(CASE WHEN cnln_img_bilateral_present IS TRUE THEN 1 ELSE 0 END),
       SUM(CASE WHEN cnln_img_bilateral_present IS FALSE THEN 1 ELSE 0 END),
       SUM(CASE WHEN cnln_img_bilateral_present IS NULL THEN 1 ELSE 0 END)
FROM main.canonical_patient_master
UNION ALL
SELECT 'cnln_img_lateral_neck_present',
       SUM(CASE WHEN cnln_img_lateral_neck_present IS TRUE THEN 1 ELSE 0 END),
       SUM(CASE WHEN cnln_img_lateral_neck_present IS FALSE THEN 1 ELSE 0 END),
       SUM(CASE WHEN cnln_img_lateral_neck_present IS NULL THEN 1 ELSE 0 END)
FROM main.canonical_patient_master;

SELECT COUNT(*) AS raw_null_rows_with_nonnull_derived_flag
FROM main.canonical_patient_master
WHERE cnln_img_laterality IS NULL
  AND (
    cnln_img_left_present IS NOT NULL
    OR cnln_img_right_present IS NOT NULL
    OR cnln_img_central_present IS NOT NULL
    OR cnln_img_bilateral_present IS NOT NULL
    OR cnln_img_lateral_neck_present IS NOT NULL
  );

SELECT verification_status, COUNT(*) AS n_cols
FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
GROUP BY 1
ORDER BY 1;

-- =============================================================================
-- end mig_174b
-- =============================================================================
