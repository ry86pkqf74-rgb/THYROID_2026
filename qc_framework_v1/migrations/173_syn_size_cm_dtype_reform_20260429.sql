-- =============================================================================
-- Migration 173 — syn_*_size_cm 3-axis dtype reform
-- =============================================================================
-- Batch:   mig_173_syn_size_cm_dtype_reform_20260429
-- Lane:    62
-- Date:    2026-04-29
-- Target:  thyroid_canonical_publication_v1_0
-- Posture: Path-C schema reform artifact. Cowork/user applies section-by-section.
--
-- Purpose
--   Decompose three canonical_patient_master VARCHAR size strings into typed
--   3-axis numeric columns plus rectangular volume and parse-status fields:
--     * syn_right_lobe_size_cm
--     * syn_left_lobe_size_cm
--     * syn_isthmus_size_cm
--
-- Governance
--   1. Pre-snapshot legacy raw values before structural change.
--   2. ADD 15 typed/parse-status columns.
--   3. Populate via conservative DuckDB regex cascade.
--   4. Rename legacy VARCHAR columns to *_legacy_raw; never DROP raw text.
--   5. Resync verification registries and CPM provenance.
--   6. Run commented verification probes after each section.
--
-- Notes
--   * DuckDB regexp_extract returns '' on no match; every capture is wrapped
--     in NULLIF(..., '') before TRY_CAST.
--   * Volume is length_cm * width_cm * height_cm. No ellipsoid 0.524 factor is
--     applied in this migration. See CF-mig173-VOLUME-CALC-NO-ELLIPSOID-FACTOR.
--   * Parser is intentionally conservative: rare narrative strings that do not
--     match the cascade remain unparsed for future review rather than guessed.
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- Section A — Pre-snapshot legacy raw columns
-- =============================================================================

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_syn_right_lobe_size_cm_pre_mig173_20260429 AS
SELECT
  CAST(research_id AS VARCHAR) AS research_id,
  syn_right_lobe_size_cm AS syn_right_lobe_size_cm_legacy_raw,
  cpm_built_at,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts,
  'mig_173_syn_size_cm_dtype_reform_20260429' AS batch_id
FROM main.canonical_patient_master;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_syn_left_lobe_size_cm_pre_mig173_20260429 AS
SELECT
  CAST(research_id AS VARCHAR) AS research_id,
  syn_left_lobe_size_cm AS syn_left_lobe_size_cm_legacy_raw,
  cpm_built_at,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts,
  'mig_173_syn_size_cm_dtype_reform_20260429' AS batch_id
FROM main.canonical_patient_master;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_syn_isthmus_size_cm_pre_mig173_20260429 AS
SELECT
  CAST(research_id AS VARCHAR) AS research_id,
  syn_isthmus_size_cm AS syn_isthmus_size_cm_legacy_raw,
  cpm_built_at,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts,
  'mig_173_syn_size_cm_dtype_reform_20260429' AS batch_id
FROM main.canonical_patient_master;

-- Verification A (run after Section A):
-- SELECT 'right' AS source_col, COUNT(*) AS n_rows, COUNT(DISTINCT research_id) AS n_patients
-- FROM "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_syn_right_lobe_size_cm_pre_mig173_20260429
-- UNION ALL
-- SELECT 'left', COUNT(*), COUNT(DISTINCT research_id)
-- FROM "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_syn_left_lobe_size_cm_pre_mig173_20260429
-- UNION ALL
-- SELECT 'isthmus', COUNT(*), COUNT(DISTINCT research_id)
-- FROM "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_syn_isthmus_size_cm_pre_mig173_20260429;

-- =============================================================================
-- Section B — Add typed axis, volume, and parse-status columns
-- =============================================================================

ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_right_lobe_length_cm DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_right_lobe_width_cm DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_right_lobe_height_cm DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_right_lobe_volume_cc DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_right_lobe_size_parse_status VARCHAR;

ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_left_lobe_length_cm DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_left_lobe_width_cm DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_left_lobe_height_cm DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_left_lobe_volume_cc DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_left_lobe_size_parse_status VARCHAR;

ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_isthmus_length_cm DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_isthmus_width_cm DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_isthmus_height_cm DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_isthmus_volume_cc DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS syn_isthmus_size_parse_status VARCHAR;

COMMENT ON COLUMN main.canonical_patient_master.syn_right_lobe_volume_cc IS
  'mig_173: rectangular volume = length_cm * width_cm * height_cm; no ellipsoid 0.524 factor.';
COMMENT ON COLUMN main.canonical_patient_master.syn_left_lobe_volume_cc IS
  'mig_173: rectangular volume = length_cm * width_cm * height_cm; no ellipsoid 0.524 factor.';
COMMENT ON COLUMN main.canonical_patient_master.syn_isthmus_volume_cc IS
  'mig_173: rectangular volume = length_cm * width_cm * height_cm; no ellipsoid 0.524 factor.';

-- Verification B (run after Section B):
-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_catalog='thyroid_canonical_publication_v1_0'
--   AND table_schema='main'
--   AND table_name='canonical_patient_master'
--   AND column_name IN (
--     'syn_right_lobe_length_cm','syn_right_lobe_width_cm','syn_right_lobe_height_cm','syn_right_lobe_volume_cc','syn_right_lobe_size_parse_status',
--     'syn_left_lobe_length_cm','syn_left_lobe_width_cm','syn_left_lobe_height_cm','syn_left_lobe_volume_cc','syn_left_lobe_size_parse_status',
--     'syn_isthmus_length_cm','syn_isthmus_width_cm','syn_isthmus_height_cm','syn_isthmus_volume_cc','syn_isthmus_size_parse_status'
--   )
-- ORDER BY ordinal_position;

-- =============================================================================
-- Section C — Populate typed columns via regex parser
-- =============================================================================
-- Parse cascade:
--   pattern_x3       : A x B x C, optional cm, optional trailing ')' / punctuation
--   pattern_by3      : A cm ... by B cm ... by C cm ...
--   pattern_three_cm : first three cm-valued numbers in prose
--   pattern_x2       : two-axis partial values only
--   pattern_one      : one-axis partial values only
-- =============================================================================

WITH source_values AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         'right' AS side_key,
         syn_right_lobe_size_cm AS raw_value
  FROM main.canonical_patient_master
  UNION ALL
  SELECT CAST(research_id AS VARCHAR),
         'left',
         syn_left_lobe_size_cm
  FROM main.canonical_patient_master
  UNION ALL
  SELECT CAST(research_id AS VARCHAR),
         'isthmus',
         syn_isthmus_size_cm
  FROM main.canonical_patient_master
), normalized AS (
  SELECT
    research_id,
    side_key,
    raw_value,
    LOWER(REGEXP_REPLACE(TRIM(CAST(raw_value AS VARCHAR)), '\s+', ' ', 'g')) AS raw_norm
  FROM source_values
), captured AS (
  SELECT
    research_id,
    side_key,
    raw_value,
    raw_norm,
    raw_norm IN ('n/s','ns','none','null','','x','c/a','-') AS is_sentinel,

    NULLIF(REGEXP_EXTRACT(raw_norm, '^\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*[x×]\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*[x×]\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*\)?\s*\.?\s*$', 1), '') AS x3_l,
    NULLIF(REGEXP_EXTRACT(raw_norm, '^\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*[x×]\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*[x×]\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*\)?\s*\.?\s*$', 2), '') AS x3_w,
    NULLIF(REGEXP_EXTRACT(raw_norm, '^\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*[x×]\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*[x×]\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*\)?\s*\.?\s*$', 3), '') AS x3_h,

    NULLIF(REGEXP_EXTRACT(raw_norm, '([0-9]+\.?[0-9]*)\s*cm[^0-9]+by\s*([0-9]+\.?[0-9]*)\s*cm[^0-9]+by\s*([0-9]+\.?[0-9]*)\s*cm', 1), '') AS by3_l,
    NULLIF(REGEXP_EXTRACT(raw_norm, '([0-9]+\.?[0-9]*)\s*cm[^0-9]+by\s*([0-9]+\.?[0-9]*)\s*cm[^0-9]+by\s*([0-9]+\.?[0-9]*)\s*cm', 2), '') AS by3_w,
    NULLIF(REGEXP_EXTRACT(raw_norm, '([0-9]+\.?[0-9]*)\s*cm[^0-9]+by\s*([0-9]+\.?[0-9]*)\s*cm[^0-9]+by\s*([0-9]+\.?[0-9]*)\s*cm', 3), '') AS by3_h,

    NULLIF(REGEXP_EXTRACT(raw_norm, '([0-9]+\.?[0-9]*)\s*cm[^0-9]+([0-9]+\.?[0-9]*)\s*cm[^0-9]+([0-9]+\.?[0-9]*)\s*cm', 1), '') AS cm3_l,
    NULLIF(REGEXP_EXTRACT(raw_norm, '([0-9]+\.?[0-9]*)\s*cm[^0-9]+([0-9]+\.?[0-9]*)\s*cm[^0-9]+([0-9]+\.?[0-9]*)\s*cm', 2), '') AS cm3_w,
    NULLIF(REGEXP_EXTRACT(raw_norm, '([0-9]+\.?[0-9]*)\s*cm[^0-9]+([0-9]+\.?[0-9]*)\s*cm[^0-9]+([0-9]+\.?[0-9]*)\s*cm', 3), '') AS cm3_h,

    NULLIF(REGEXP_EXTRACT(raw_norm, '^\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*[x×]\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*$', 1), '') AS x2_l,
    NULLIF(REGEXP_EXTRACT(raw_norm, '^\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*[x×]\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*$', 2), '') AS x2_w,
    NULLIF(REGEXP_EXTRACT(raw_norm, '^\s*([0-9]+\.?[0-9]*)\s*[a-z]*\s*$', 1), '') AS one_l
  FROM normalized
), parsed AS (
  SELECT
    research_id,
    side_key,
    raw_value,
    raw_norm,
    is_sentinel,
    CASE WHEN raw_value IS NULL OR is_sentinel THEN NULL ELSE COALESCE(
      TRY_CAST(x3_l AS DOUBLE), TRY_CAST(by3_l AS DOUBLE), TRY_CAST(cm3_l AS DOUBLE), TRY_CAST(x2_l AS DOUBLE), TRY_CAST(one_l AS DOUBLE)
    ) END AS length_cm,
    CASE WHEN raw_value IS NULL OR is_sentinel THEN NULL ELSE COALESCE(
      TRY_CAST(x3_w AS DOUBLE), TRY_CAST(by3_w AS DOUBLE), TRY_CAST(cm3_w AS DOUBLE), TRY_CAST(x2_w AS DOUBLE)
    ) END AS width_cm,
    CASE WHEN raw_value IS NULL OR is_sentinel THEN NULL ELSE COALESCE(
      TRY_CAST(x3_h AS DOUBLE), TRY_CAST(by3_h AS DOUBLE), TRY_CAST(cm3_h AS DOUBLE)
    ) END AS height_cm
  FROM captured
), statused AS (
  SELECT
    research_id,
    side_key,
    length_cm,
    width_cm,
    height_cm,
    CASE
      WHEN raw_value IS NULL THEN NULL
      WHEN is_sentinel THEN 'sentinel'
      WHEN length_cm IS NOT NULL AND width_cm IS NOT NULL AND height_cm IS NOT NULL THEN 'parsed_3axis'
      WHEN length_cm IS NOT NULL OR width_cm IS NOT NULL OR height_cm IS NOT NULL THEN 'parsed_partial'
      ELSE 'unparsed'
    END AS parse_status,
    CASE
      WHEN length_cm IS NOT NULL AND width_cm IS NOT NULL AND height_cm IS NOT NULL
        THEN length_cm * width_cm * height_cm
      ELSE NULL
    END AS volume_cc
  FROM parsed
), wide AS (
  SELECT
    research_id,
    MAX(CASE WHEN side_key='right' THEN length_cm END) AS right_length_cm,
    MAX(CASE WHEN side_key='right' THEN width_cm END) AS right_width_cm,
    MAX(CASE WHEN side_key='right' THEN height_cm END) AS right_height_cm,
    MAX(CASE WHEN side_key='right' THEN volume_cc END) AS right_volume_cc,
    MAX(CASE WHEN side_key='right' THEN parse_status END) AS right_parse_status,

    MAX(CASE WHEN side_key='left' THEN length_cm END) AS left_length_cm,
    MAX(CASE WHEN side_key='left' THEN width_cm END) AS left_width_cm,
    MAX(CASE WHEN side_key='left' THEN height_cm END) AS left_height_cm,
    MAX(CASE WHEN side_key='left' THEN volume_cc END) AS left_volume_cc,
    MAX(CASE WHEN side_key='left' THEN parse_status END) AS left_parse_status,

    MAX(CASE WHEN side_key='isthmus' THEN length_cm END) AS isthmus_length_cm,
    MAX(CASE WHEN side_key='isthmus' THEN width_cm END) AS isthmus_width_cm,
    MAX(CASE WHEN side_key='isthmus' THEN height_cm END) AS isthmus_height_cm,
    MAX(CASE WHEN side_key='isthmus' THEN volume_cc END) AS isthmus_volume_cc,
    MAX(CASE WHEN side_key='isthmus' THEN parse_status END) AS isthmus_parse_status
  FROM statused
  GROUP BY research_id
)
UPDATE main.canonical_patient_master pm
SET syn_right_lobe_length_cm = w.right_length_cm,
    syn_right_lobe_width_cm = w.right_width_cm,
    syn_right_lobe_height_cm = w.right_height_cm,
    syn_right_lobe_volume_cc = w.right_volume_cc,
    syn_right_lobe_size_parse_status = w.right_parse_status,
    syn_left_lobe_length_cm = w.left_length_cm,
    syn_left_lobe_width_cm = w.left_width_cm,
    syn_left_lobe_height_cm = w.left_height_cm,
    syn_left_lobe_volume_cc = w.left_volume_cc,
    syn_left_lobe_size_parse_status = w.left_parse_status,
    syn_isthmus_length_cm = w.isthmus_length_cm,
    syn_isthmus_width_cm = w.isthmus_width_cm,
    syn_isthmus_height_cm = w.isthmus_height_cm,
    syn_isthmus_volume_cc = w.isthmus_volume_cc,
    syn_isthmus_size_parse_status = w.isthmus_parse_status,
    cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM wide w
WHERE CAST(pm.research_id AS VARCHAR) = w.research_id;

-- Verification C (run after Section C):
-- WITH status_counts AS (
--   SELECT 'right' AS source_col, syn_right_lobe_size_parse_status AS parse_status, COUNT(*) AS n
--   FROM main.canonical_patient_master GROUP BY 1,2
--   UNION ALL
--   SELECT 'left', syn_left_lobe_size_parse_status, COUNT(*)
--   FROM main.canonical_patient_master GROUP BY 1,2
--   UNION ALL
--   SELECT 'isthmus', syn_isthmus_size_parse_status, COUNT(*)
--   FROM main.canonical_patient_master GROUP BY 1,2
-- )
-- SELECT * FROM status_counts ORDER BY source_col, parse_status;

-- =============================================================================
-- Section D — Rename legacy raw VARCHAR columns
-- =============================================================================
-- Run Section D only after Section C parse counts are accepted. These RENAMEs
-- are intentionally one-time operations; if this migration is rerun after a
-- successful rename, skip this section.

ALTER TABLE main.canonical_patient_master RENAME COLUMN syn_right_lobe_size_cm TO syn_right_lobe_size_cm_legacy_raw;
ALTER TABLE main.canonical_patient_master RENAME COLUMN syn_left_lobe_size_cm TO syn_left_lobe_size_cm_legacy_raw;
ALTER TABLE main.canonical_patient_master RENAME COLUMN syn_isthmus_size_cm TO syn_isthmus_size_cm_legacy_raw;

COMMENT ON COLUMN main.canonical_patient_master.syn_right_lobe_size_cm_legacy_raw IS
  'mig_173: preserved raw VARCHAR 3-axis lobe-size string; use typed syn_right_lobe_{length,width,height}_cm for analysis.';
COMMENT ON COLUMN main.canonical_patient_master.syn_left_lobe_size_cm_legacy_raw IS
  'mig_173: preserved raw VARCHAR 3-axis lobe-size string; use typed syn_left_lobe_{length,width,height}_cm for analysis.';
COMMENT ON COLUMN main.canonical_patient_master.syn_isthmus_size_cm_legacy_raw IS
  'mig_173: preserved raw VARCHAR 3-axis lobe-size string; use typed syn_isthmus_{length,width,height}_cm for analysis.';

-- Verification D (run after Section D):
-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_catalog='thyroid_canonical_publication_v1_0'
--   AND table_schema='main'
--   AND table_name='canonical_patient_master'
--   AND column_name IN (
--     'syn_right_lobe_size_cm','syn_left_lobe_size_cm','syn_isthmus_size_cm',
--     'syn_right_lobe_size_cm_legacy_raw','syn_left_lobe_size_cm_legacy_raw','syn_isthmus_size_cm_legacy_raw'
--   )
-- ORDER BY ordinal_position;

-- =============================================================================
-- Section E — Registry resync + CPM provenance
-- =============================================================================

-- E1: Rename the three pre-existing registry rows to legacy_raw and reclassify
--     as source-preservation rows not used for typed numeric analysis.
UPDATE main.canonical_column_verification_registry_v1
SET column_name = CASE column_name
      WHEN 'syn_right_lobe_size_cm' THEN 'syn_right_lobe_size_cm_legacy_raw'
      WHEN 'syn_left_lobe_size_cm' THEN 'syn_left_lobe_size_cm_legacy_raw'
      WHEN 'syn_isthmus_size_cm' THEN 'syn_isthmus_size_cm_legacy_raw'
      ELSE column_name
    END,
    data_type = 'VARCHAR',
    category = 'source',
    upstream_source = COALESCE(upstream_source, 'canonical_patient_master legacy synoptic lobe-size raw string'),
    verification_status = 'na',
    verified_by = 'cowork_mig_173',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = 'legacy_raw_preserved_after_typed_decomposition',
    batch_id = 'mig_173_syn_size_cm_dtype_reform_20260429',
    notes = COALESCE(notes,'')
      || ' | mig_173: raw VARCHAR preserved as *_legacy_raw after 3-axis numeric decomposition. '
      || 'Not for numeric manuscript analysis; use typed axis/volume columns. '
      || 'Closes CF-mig169-DTYPE-VARCHAR-WITH-UNITS and CF-mig168-VOCAB-DRIFT-SYN-SIZE-3AXIS-VARCHAR for this legacy raw field.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND column_name IN ('syn_right_lobe_size_cm','syn_left_lobe_size_cm','syn_isthmus_size_cm');

-- E2: Register the 15 new typed/parse-status columns if not already present.
INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position,
   category, upstream_source, verification_status, verified_by, verified_ts,
   verification_method, batch_id, notes, registered_ts)
SELECT
  'main' AS schema_name,
  'canonical_patient_master' AS table_name,
  i.column_name,
  i.data_type,
  i.ordinal_position,
  'derived' AS category,
  CASE
    WHEN i.column_name LIKE 'syn_right_lobe_%' THEN 'syn_right_lobe_size_cm_legacy_raw via mig_173 parser cascade'
    WHEN i.column_name LIKE 'syn_left_lobe_%' THEN 'syn_left_lobe_size_cm_legacy_raw via mig_173 parser cascade'
    WHEN i.column_name LIKE 'syn_isthmus_%' THEN 'syn_isthmus_size_cm_legacy_raw via mig_173 parser cascade'
    ELSE 'mig_173 parser cascade'
  END AS upstream_source,
  'not_started' AS verification_status,
  NULL AS verified_by,
  NULL AS verified_ts,
  NULL AS verification_method,
  'mig_173_syn_size_cm_dtype_reform_20260429' AS batch_id,
  CASE
    WHEN i.column_name LIKE '%volume_cc' THEN
      'mig_173: NEW typed derived column. Rectangular volume = length_cm * width_cm * height_cm. CF-mig173-VOLUME-CALC-NO-ELLIPSOID-FACTOR remains informational.'
    WHEN i.column_name LIKE '%parse_status' THEN
      'mig_173: NEW parser audit/status column with values parsed_3axis, parsed_partial, sentinel, unparsed. CF-mig173-PARSE-COVERAGE-LT-100PCT-PER-COL tracks residual unparsed rows.'
    ELSE
      'mig_173: NEW typed DOUBLE axis column parsed from preserved synoptic lobe-size raw string. Closes corresponding mig_169 VARCHAR-with-units finding after verification.'
  END AS notes,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS registered_ts
FROM information_schema.columns i
WHERE i.table_catalog='thyroid_canonical_publication_v1_0'
  AND i.table_schema='main'
  AND i.table_name='canonical_patient_master'
  AND i.column_name IN (
    'syn_right_lobe_length_cm','syn_right_lobe_width_cm','syn_right_lobe_height_cm','syn_right_lobe_volume_cc','syn_right_lobe_size_parse_status',
    'syn_left_lobe_length_cm','syn_left_lobe_width_cm','syn_left_lobe_height_cm','syn_left_lobe_volume_cc','syn_left_lobe_size_parse_status',
    'syn_isthmus_length_cm','syn_isthmus_width_cm','syn_isthmus_height_cm','syn_isthmus_volume_cc','syn_isthmus_size_parse_status'
  )
  AND NOT EXISTS (
    SELECT 1
    FROM main.canonical_column_verification_registry_v1 r
    WHERE r.schema_name='main'
      AND r.table_name='canonical_patient_master'
      AND r.column_name=i.column_name
  );

-- E3: Resync table-level counts. Table status is expected to become in_progress
--     unless a later Cowork signoff flips all new columns to verified/na.
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
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/173_syn_size_cm_dtype_reform_20260429.sql',
    notes = COALESCE(ts.notes,'')
      || ' | mig_173: syn_*_size_cm VARCHAR fields decomposed to typed 3-axis DOUBLE columns + volume/status; legacy raw strings preserved as *_legacy_raw. New derived columns await post-parse verification.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main'
    AND table_name='canonical_patient_master'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- E4: CPM reconciliation provenance. All columns are VARCHAR-compatible by
--     legacy convention except timestamps.
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared,
   held_for_adjudication)
VALUES
  ('canonical_cleanup_mig173_syn_size_cm_dtype_reform_20260429',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'pre_snapshot_add_columns_parser_update_legacy_rename_registry_resync',
   'none',
   'CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_right_lobe_size_cm;CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_left_lobe_size_cm;CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_isthmus_size_cm;CF-mig168-VOCAB-DRIFT-SYN-SIZE-3AXIS-VARCHAR',
   'none',
   'CF-mig173-PARSE-COVERAGE-LT-100PCT-PER-COL;CF-mig173-VOLUME-CALC-NO-ELLIPSOID-FACTOR');

-- =============================================================================
-- Section F — Post-state verification probes (run after Sections A-E)
-- =============================================================================

-- F1: CPM invariant.
-- SELECT COUNT(*) AS n_rows,
--        COUNT(DISTINCT CAST(research_id AS VARCHAR)) AS n_distinct_research_id,
--        COUNT(*) FILTER (WHERE cpm_built_at IS NULL) AS n_null_cpm_built_at
-- FROM main.canonical_patient_master;

-- F2: Parse coverage by column/status.
-- WITH status_counts AS (
--   SELECT 'syn_right_lobe_size_cm' AS legacy_col, syn_right_lobe_size_parse_status AS parse_status, COUNT(*) AS n
--   FROM main.canonical_patient_master GROUP BY 1,2
--   UNION ALL
--   SELECT 'syn_left_lobe_size_cm', syn_left_lobe_size_parse_status, COUNT(*)
--   FROM main.canonical_patient_master GROUP BY 1,2
--   UNION ALL
--   SELECT 'syn_isthmus_size_cm', syn_isthmus_size_parse_status, COUNT(*)
--   FROM main.canonical_patient_master GROUP BY 1,2
-- )
-- SELECT * FROM status_counts ORDER BY legacy_col, parse_status;

-- F3: Coverage percentage among non-sentinel non-null values.
-- SELECT
--   'right' AS source_col,
--   COUNT(*) FILTER (WHERE syn_right_lobe_size_parse_status IN ('parsed_3axis','parsed_partial')) AS n_parsed_any,
--   COUNT(*) FILTER (WHERE syn_right_lobe_size_parse_status='parsed_3axis') AS n_parsed_3axis,
--   COUNT(*) FILTER (WHERE syn_right_lobe_size_parse_status='unparsed') AS n_unparsed,
--   COUNT(*) FILTER (WHERE syn_right_lobe_size_parse_status IN ('parsed_3axis','parsed_partial','unparsed')) AS n_parse_attempted,
--   ROUND(100.0 * COUNT(*) FILTER (WHERE syn_right_lobe_size_parse_status='parsed_3axis') / NULLIF(COUNT(*) FILTER (WHERE syn_right_lobe_size_parse_status IN ('parsed_3axis','parsed_partial','unparsed')), 0), 2) AS pct_3axis
-- FROM main.canonical_patient_master
-- UNION ALL
-- SELECT 'left',
--   COUNT(*) FILTER (WHERE syn_left_lobe_size_parse_status IN ('parsed_3axis','parsed_partial')),
--   COUNT(*) FILTER (WHERE syn_left_lobe_size_parse_status='parsed_3axis'),
--   COUNT(*) FILTER (WHERE syn_left_lobe_size_parse_status='unparsed'),
--   COUNT(*) FILTER (WHERE syn_left_lobe_size_parse_status IN ('parsed_3axis','parsed_partial','unparsed')),
--   ROUND(100.0 * COUNT(*) FILTER (WHERE syn_left_lobe_size_parse_status='parsed_3axis') / NULLIF(COUNT(*) FILTER (WHERE syn_left_lobe_size_parse_status IN ('parsed_3axis','parsed_partial','unparsed')), 0), 2)
-- FROM main.canonical_patient_master
-- UNION ALL
-- SELECT 'isthmus',
--   COUNT(*) FILTER (WHERE syn_isthmus_size_parse_status IN ('parsed_3axis','parsed_partial')),
--   COUNT(*) FILTER (WHERE syn_isthmus_size_parse_status='parsed_3axis'),
--   COUNT(*) FILTER (WHERE syn_isthmus_size_parse_status='unparsed'),
--   COUNT(*) FILTER (WHERE syn_isthmus_size_parse_status IN ('parsed_3axis','parsed_partial','unparsed')),
--   ROUND(100.0 * COUNT(*) FILTER (WHERE syn_isthmus_size_parse_status='parsed_3axis') / NULLIF(COUNT(*) FILTER (WHERE syn_isthmus_size_parse_status IN ('parsed_3axis','parsed_partial','unparsed')), 0), 2)
-- FROM main.canonical_patient_master;

-- F4: Volume sanity. Max < 1000 cc is a conservative thyroid-lobe plausibility guard.
-- SELECT 'right' AS source_col,
--        MIN(syn_right_lobe_volume_cc) AS min_volume_cc,
--        MAX(syn_right_lobe_volume_cc) AS max_volume_cc,
--        COUNT(*) FILTER (WHERE syn_right_lobe_volume_cc < 0) AS n_negative,
--        COUNT(*) FILTER (WHERE syn_right_lobe_volume_cc >= 1000) AS n_ge_1000
-- FROM main.canonical_patient_master
-- UNION ALL
-- SELECT 'left', MIN(syn_left_lobe_volume_cc), MAX(syn_left_lobe_volume_cc),
--        COUNT(*) FILTER (WHERE syn_left_lobe_volume_cc < 0),
--        COUNT(*) FILTER (WHERE syn_left_lobe_volume_cc >= 1000)
-- FROM main.canonical_patient_master
-- UNION ALL
-- SELECT 'isthmus', MIN(syn_isthmus_volume_cc), MAX(syn_isthmus_volume_cc),
--        COUNT(*) FILTER (WHERE syn_isthmus_volume_cc < 0),
--        COUNT(*) FILTER (WHERE syn_isthmus_volume_cc >= 1000)
-- FROM main.canonical_patient_master;

-- F5: Registry status for affected fields.
-- SELECT column_name, data_type, category, verification_status, verification_method, batch_id
-- FROM main.canonical_column_verification_registry_v1
-- WHERE schema_name='main'
--   AND table_name='canonical_patient_master'
--   AND column_name IN (
--     'syn_right_lobe_size_cm_legacy_raw','syn_left_lobe_size_cm_legacy_raw','syn_isthmus_size_cm_legacy_raw',
--     'syn_right_lobe_length_cm','syn_right_lobe_width_cm','syn_right_lobe_height_cm','syn_right_lobe_volume_cc','syn_right_lobe_size_parse_status',
--     'syn_left_lobe_length_cm','syn_left_lobe_width_cm','syn_left_lobe_height_cm','syn_left_lobe_volume_cc','syn_left_lobe_size_parse_status',
--     'syn_isthmus_length_cm','syn_isthmus_width_cm','syn_isthmus_height_cm','syn_isthmus_volume_cc','syn_isthmus_size_parse_status'
--   )
-- ORDER BY ordinal_position;

-- F6: Sample residual unparsed raw strings for future review.
-- SELECT 'right' AS source_col, syn_right_lobe_size_cm_legacy_raw AS raw_value, COUNT(*) AS n
-- FROM main.canonical_patient_master
-- WHERE syn_right_lobe_size_parse_status='unparsed'
-- GROUP BY 1,2 ORDER BY n DESC, raw_value LIMIT 30;
-- Repeat with left/isthmus as needed.

-- End mig_173.