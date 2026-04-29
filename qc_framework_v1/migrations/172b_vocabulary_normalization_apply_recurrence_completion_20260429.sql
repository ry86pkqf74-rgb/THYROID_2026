-- mig_172b — vocabulary normalization apply for recurrence/completion histology family
-- Date authored: 2026-04-29
-- Batch: mig_172b_vocabulary_normalization_apply_recurrence_completion_20260429
-- Target DB: thyroid_canonical_publication_v1_0
-- Primary table: main.canonical_patient_master
--
-- Posture: AUTHORING ARTIFACT ONLY. Do not execute casually.
-- Cowork/Path C should execute with pre-snapshot review and one-statement-at-a-time logging.
-- This SQL intentionally does not touch the four mig_178-covered histology surfaces:
--   path_histology_raw, path_histology_variant_raw, histologic_types_all, histologic_variants_all.
--
-- Key decision: Logan rejected synthetic mtc_ptc_mixed per CF-mig172; the single ratified
-- source raw value is remapped to the mig_178 mixed-label convention: MTC | PTC.

USE "thyroid_canonical_publication_v1_0";
USE "thyroid_canonical_publication_v1_0".main;

-- §3 Pre-flight probes — run and retain output before §A/§B/§C apply steps.
-- Expected pre-state on 2026-04-29 read-only probe:
--   recurrence_histology=42; recurrence_histology_v2=26;
--   completion_prior_histology=15; completion_histology_type=11.
SELECT
  COUNT(DISTINCT recurrence_histology)        AS recurrence_histology_n,
  COUNT(DISTINCT recurrence_histology_v2)     AS recurrence_histology_v2_n,
  COUNT(DISTINCT completion_prior_histology)  AS completion_prior_histology_n,
  COUNT(DISTINCT completion_histology_type)   AS completion_histology_type_n
FROM main.canonical_patient_master;

SELECT
  COUNT(*) FILTER (WHERE histologic_types_all ILIKE '%mtc_ptc_mixed%') AS hta_mtc_ptc_mixed,
  COUNT(*) FILTER (WHERE histologic_variants_all ILIKE '%mtc_ptc_mixed%') AS hva_mtc_ptc_mixed
FROM main.canonical_patient_master;
-- Expect post-mig_178: 0 / 0.

SELECT COUNT(*) AS n_rows, COUNT(DISTINCT research_id) AS n_rids
FROM main.canonical_patient_master;
-- Expect: 10871 / 10871.

-- §A — pre-snapshots, one per target column.
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_recurrence_histology_pre_mig172b_20260429 AS
SELECT research_id, recurrence_histology, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig172b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_recurrence_histology_v2_pre_mig172b_20260429 AS
SELECT research_id, recurrence_histology_v2, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig172b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_completion_prior_histology_pre_mig172b_20260429 AS
SELECT research_id, completion_prior_histology, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig172b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_completion_histology_type_pre_mig172b_20260429 AS
SELECT research_id, completion_histology_type, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig172b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

-- §B — load post-mig_178 rewritten CSV into exact-match map table.
CREATE OR REPLACE TABLE main.histology_vocab_normalization_map_v1 AS
SELECT
  raw_value::VARCHAR AS raw_value,
  canonical_code::VARCHAR AS canonical_code,
  display_label::VARCHAR AS display_label,
  source_col::VARCHAR AS source_col
FROM (VALUES
  ('PTC', 'ptc', 'PTC', 'recurrence_histology'),
  ('PTC ', 'ptc', 'PTC', 'recurrence_histology'),
  ('metastatic PTC', 'ptc', 'PTC', 'recurrence_histology'),
  ('Metastatic PTC', 'ptc', 'PTC', 'recurrence_histology'),
  ('metastatic pTC', 'ptc', 'PTC', 'recurrence_histology'),
  ('follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'recurrence_histology'),
  ('Follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'recurrence_histology'),
  ('metastatic MTC', 'mtc', 'MTC', 'recurrence_histology'),
  ('Metastatic MTC', 'mtc', 'MTC', 'recurrence_histology'),
  ('NIFTP', 'niftp', 'NIFTP', 'recurrence_histology'),
  ('MTC', 'mtc', 'MTC', 'recurrence_histology'),
  ('metastatic PTC classical', 'ptc', 'PTC, Classical', 'recurrence_histology'),
  ('metastatic follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'recurrence_histology'),
  ('metastatic PTC tall cell variant', 'ptc', 'PTC, Tall Cell Variant', 'recurrence_histology'),
  ('Metastatic PTC tall cell variant', 'ptc', 'PTC, Tall Cell Variant', 'recurrence_histology'),
  ('Poorly differentiated thyroid carcinoma', 'pdtc', 'PDTC', 'recurrence_histology'),
  ('poorly differentiated thyroid carcinoma', 'pdtc', 'PDTC', 'recurrence_histology'),
  ('Metastatic Carcinoma', 'dtc_nos', 'Metastatic Carcinoma (NOS)', 'recurrence_histology'),
  ('metastatic carcinoma', 'dtc_nos', 'Metastatic Carcinoma (NOS)', 'recurrence_histology'),
  ('Recurrent/metastatic follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'recurrence_histology'),
  ('recurrent/metastatic follicular Carcinoma', 'ftc', 'Follicular Carcinoma', 'recurrence_histology'),
  ('recurrent/metastatic PTC', 'ptc', 'PTC', 'recurrence_histology'),
  ('angiosarcoma of the thyroid', 'angiosarcoma', 'Angiosarcoma of the Thyroid', 'recurrence_histology'),
  ('differentiated high grade thyroid carcinoma', 'dhgtc', 'DHGTC', 'recurrence_histology'),
  ('differentiated thyroid carcinoma', 'dtc_nos', 'DTC (NOS)', 'recurrence_histology'),
  ('FTUMP', 'ftump', 'FTUMP', 'recurrence_histology'),
  ('metastatic high grade neuroendocrine tumor', 'neuroendocrine', 'Neuroendocrine Tumor, High Grade', 'recurrence_histology'),
  ('metastatic poorly differentiated thyroid carcinoma', 'pdtc', 'PDTC', 'recurrence_histology'),
  ('metastatic PTC calssical with focal tall cell features (<5%)', 'ptc', 'PTC, Classical with Focal Tall Cell', 'recurrence_histology'),
  ('metastatic PTC classic', 'ptc', 'PTC, Classical', 'recurrence_histology'),
  ('metastatic PTC classic subtype with tall cell component ~25%', 'ptc', 'PTC, Classical with ~25% Tall Cell', 'recurrence_histology'),
  ('metastatic PTC
classic subtype with tall cell component ~25%', 'ptc', 'PTC, Classical with ~25% Tall Cell', 'recurrence_histology'),
  ('metastatic PTC follicular', 'ptc', 'PTC, Follicular Variant', 'recurrence_histology'),
  ('metastatic PTC high grade', 'ptc', 'PTC, High Grade', 'recurrence_histology'),
  ('metastatic PTC onocytic classical', 'ptc', 'PTC, Oncocytic Classical', 'recurrence_histology'),
  ('metastatic PTC with focal tall cell features', 'ptc', 'PTC, Classical with Focal Tall Cell', 'recurrence_histology'),
  ('metastatic PTC with tall cell features', 'ptc', 'PTC, Tall Cell', 'recurrence_histology'),
  ('metastatic PTC?', 'ptc', 'PTC', 'recurrence_histology'),
  ('metastatic thyroid carcinoma', 'dtc_nos', 'Metastatic Thyroid Carcinoma (NOS)', 'recurrence_histology'),
  ('metastatic/recurrent PTC', 'ptc', 'PTC', 'recurrence_histology'),
  ('metastatic/recurrent PTC classical', 'ptc', 'PTC, Classical', 'recurrence_histology'),
  ('recurrent anaplastic carcinoma', 'atc', 'Anaplastic Thyroid Carcinoma', 'recurrence_histology'),
  ('recurrent differentiated high grade thyroid carcinoma metastatic', 'dhgtc', 'DHGTC', 'recurrence_histology'),
  ('metastatic PTC', 'ptc', 'PTC', 'recurrence_histology_v2'),
  ('Metastatic PTC', 'ptc', 'PTC', 'recurrence_histology_v2'),
  ('metastatic pTC', 'ptc', 'PTC', 'recurrence_histology_v2'),
  ('follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'recurrence_histology_v2'),
  ('metastatic MTC', 'mtc', 'MTC', 'recurrence_histology_v2'),
  ('Metastatic MTC', 'mtc', 'MTC', 'recurrence_histology_v2'),
  ('metastatic PTC classical', 'ptc', 'PTC, Classical', 'recurrence_histology_v2'),
  ('metastatic PTC tall cell variant', 'ptc', 'PTC, Tall Cell Variant', 'recurrence_histology_v2'),
  ('Metastatic PTC tall cell variant', 'ptc', 'PTC, Tall Cell Variant', 'recurrence_histology_v2'),
  ('metastatic follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'recurrence_histology_v2'),
  ('differentiated thyroid carcinoma', 'dtc_nos', 'DTC (NOS)', 'recurrence_histology_v2'),
  ('metastatic carcinoma', 'dtc_nos', 'Metastatic Carcinoma (NOS)', 'recurrence_histology_v2'),
  ('metastatic high grade neuroendocrine tumor', 'neuroendocrine', 'Neuroendocrine Tumor, High Grade', 'recurrence_histology_v2'),
  ('metastatic poorly differentiated thyroid carcinoma', 'pdtc', 'PDTC', 'recurrence_histology_v2'),
  ('metastatic PTC calssical with focal tall cell features (<5%)', 'ptc', 'PTC, Classical with Focal Tall Cell', 'recurrence_histology_v2'),
  ('metastatic PTC classic', 'ptc', 'PTC, Classical', 'recurrence_histology_v2'),
  ('metastatic PTC with focal tall cell features', 'ptc', 'PTC, Classical with Focal Tall Cell', 'recurrence_histology_v2'),
  ('metastatic PTC with tall cell features', 'ptc', 'PTC, Tall Cell', 'recurrence_histology_v2'),
  ('metastatic PTC?', 'ptc', 'PTC', 'recurrence_histology_v2'),
  ('metastatic thyroid carcinoma', 'dtc_nos', 'Metastatic Thyroid Carcinoma (NOS)', 'recurrence_histology_v2'),
  ('metastatic/recurrent PTC', 'ptc', 'PTC', 'recurrence_histology_v2'),
  ('metastatic/recurrent PTC classical', 'ptc', 'PTC, Classical', 'recurrence_histology_v2'),
  ('Poorly differentiated thyroid carcinoma', 'pdtc', 'PDTC', 'recurrence_histology_v2'),
  ('recurrent anaplastic carcinoma', 'atc', 'Anaplastic Thyroid Carcinoma', 'recurrence_histology_v2'),
  ('recurrent differentiated high grade thyroid carcinoma metastatic', 'dhgtc', 'DHGTC', 'recurrence_histology_v2'),
  ('Recurrent/metastatic follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'recurrence_histology_v2'),
  ('PTC', 'ptc', 'PTC', 'completion_prior_histology'),
  ('PTC ', 'ptc', 'PTC', 'completion_prior_histology'),
  ('follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'completion_prior_histology'),
  ('Follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'completion_prior_histology'),
  ('MTC', 'mtc', 'MTC', 'completion_prior_histology'),
  ('poorly differentiated thyroid carcinoma', 'pdtc', 'PDTC', 'completion_prior_histology'),
  ('Poorly differentiated thyroid carcinoma', 'pdtc', 'PDTC', 'completion_prior_histology'),
  ('NIFTP', 'niftp', 'NIFTP', 'completion_prior_histology'),
  ('differentiated high grade thyroid carcinoma', 'dhgtc', 'DHGTC', 'completion_prior_histology'),
  ('FTUMP', 'ftump', 'FTUMP', 'completion_prior_histology'),
  ('metastatic PTC', 'ptc', 'PTC', 'completion_prior_histology'),
  ('Atypical hurthle cell neoplasm', 'atypical_hurthle_neoplasm', 'Atypical Hurthle Cell Neoplasm', 'completion_prior_histology'),
  ('metastatic follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'completion_prior_histology'),
  ('metastatic thyroid carcinoma', 'dtc_nos', 'Metastatic Thyroid Carcinoma (NOS)', 'completion_prior_histology'),
  ('MTC PTC mixed composit', 'MTC | PTC', 'MTC | PTC', 'completion_prior_histology'),
  ('MTC
PTC mixed composit', 'MTC | PTC', 'MTC | PTC', 'completion_prior_histology'),
  ('PTC', 'ptc', 'PTC', 'completion_histology_type'),
  ('PTC ', 'ptc', 'PTC', 'completion_histology_type'),
  ('metastatic PTC', 'ptc', 'PTC', 'completion_histology_type'),
  ('follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'completion_histology_type'),
  ('Follicular carcinoma', 'ftc', 'Follicular Carcinoma', 'completion_histology_type'),
  ('MTC', 'mtc', 'MTC', 'completion_histology_type'),
  ('NIFTP', 'niftp', 'NIFTP', 'completion_histology_type'),
  ('Metastatic MTC', 'mtc', 'MTC', 'completion_histology_type'),
  ('metastatic MTC', 'mtc', 'MTC', 'completion_histology_type'),
  ('FTUMP', 'ftump', 'FTUMP', 'completion_histology_type'),
  ('metastatic/recurrent PTC', 'ptc', 'PTC', 'completion_histology_type')
) AS v(raw_value, canonical_code, display_label, source_col);

-- §B validation — rejected code must not be present in the loaded map.
SELECT COUNT(*) AS mtc_ptc_mixed_map_rows
FROM main.histology_vocab_normalization_map_v1
WHERE LOWER(TRIM(canonical_code)) = 'mtc_ptc_mixed';
-- Expect: 0.

-- §B validation — no unmapped live raw values should remain before update.
SELECT 'recurrence_histology' AS source_col, pm.recurrence_histology AS unmapped_raw_value, COUNT(*) AS n_rows
FROM main.canonical_patient_master AS pm
LEFT JOIN main.histology_vocab_normalization_map_v1 AS m
  ON m.source_col = 'recurrence_histology'
 AND pm.recurrence_histology = m.raw_value
WHERE pm.recurrence_histology IS NOT NULL
  AND m.raw_value IS NULL
GROUP BY 1, 2
ORDER BY n_rows DESC, unmapped_raw_value;
SELECT 'recurrence_histology_v2' AS source_col, pm.recurrence_histology_v2 AS unmapped_raw_value, COUNT(*) AS n_rows
FROM main.canonical_patient_master AS pm
LEFT JOIN main.histology_vocab_normalization_map_v1 AS m
  ON m.source_col = 'recurrence_histology_v2'
 AND pm.recurrence_histology_v2 = m.raw_value
WHERE pm.recurrence_histology_v2 IS NOT NULL
  AND m.raw_value IS NULL
GROUP BY 1, 2
ORDER BY n_rows DESC, unmapped_raw_value;
SELECT 'completion_prior_histology' AS source_col, pm.completion_prior_histology AS unmapped_raw_value, COUNT(*) AS n_rows
FROM main.canonical_patient_master AS pm
LEFT JOIN main.histology_vocab_normalization_map_v1 AS m
  ON m.source_col = 'completion_prior_histology'
 AND pm.completion_prior_histology = m.raw_value
WHERE pm.completion_prior_histology IS NOT NULL
  AND m.raw_value IS NULL
GROUP BY 1, 2
ORDER BY n_rows DESC, unmapped_raw_value;
SELECT 'completion_histology_type' AS source_col, pm.completion_histology_type AS unmapped_raw_value, COUNT(*) AS n_rows
FROM main.canonical_patient_master AS pm
LEFT JOIN main.histology_vocab_normalization_map_v1 AS m
  ON m.source_col = 'completion_histology_type'
 AND pm.completion_histology_type = m.raw_value
WHERE pm.completion_histology_type IS NOT NULL
  AND m.raw_value IS NULL
GROUP BY 1, 2
ORDER BY n_rows DESC, unmapped_raw_value;

-- §C — normalize remaining four recurrence/completion histology columns.
UPDATE main.canonical_patient_master AS pm
SET recurrence_histology = m.canonical_code
FROM main.histology_vocab_normalization_map_v1 AS m
WHERE m.source_col = 'recurrence_histology'
  AND pm.recurrence_histology = m.raw_value
  AND pm.recurrence_histology IS DISTINCT FROM m.canonical_code;
UPDATE main.canonical_patient_master AS pm
SET recurrence_histology_v2 = m.canonical_code
FROM main.histology_vocab_normalization_map_v1 AS m
WHERE m.source_col = 'recurrence_histology_v2'
  AND pm.recurrence_histology_v2 = m.raw_value
  AND pm.recurrence_histology_v2 IS DISTINCT FROM m.canonical_code;
UPDATE main.canonical_patient_master AS pm
SET completion_prior_histology = m.canonical_code
FROM main.histology_vocab_normalization_map_v1 AS m
WHERE m.source_col = 'completion_prior_histology'
  AND pm.completion_prior_histology = m.raw_value
  AND pm.completion_prior_histology IS DISTINCT FROM m.canonical_code;
UPDATE main.canonical_patient_master AS pm
SET completion_histology_type = m.canonical_code
FROM main.histology_vocab_normalization_map_v1 AS m
WHERE m.source_col = 'completion_histology_type'
  AND pm.completion_histology_type = m.raw_value
  AND pm.completion_histology_type IS DISTINCT FROM m.canonical_code;

-- §D — registry note appendix for the four normalized columns; guarded to avoid duplicate appends.
UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE
    WHEN COALESCE(notes, '') ILIKE '%mig_172b: vocab_normalization_apply%'
      THEN notes
    ELSE CONCAT(COALESCE(notes, ''), CASE WHEN COALESCE(notes, '') = '' THEN '' ELSE ' | ' END, 'mig_172b: vocab_normalization_apply — recurrence/completion histology raw values normalized via histology_vocab_normalization_map_v1; mtc_ptc_mixed dropped (Logan-rejected per CF-mig172); mig_178 convention MTC | PTC applied')
  END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('recurrence_histology', 'recurrence_histology_v2', 'completion_prior_histology', 'completion_histology_type');

-- Governance: refresh CPM provenance timestamp after this CPM mutation batch.
UPDATE main.canonical_patient_master
SET cpm_built_at = CURRENT_TIMESTAMP;

-- Governance: append cpm_reconciliation_provenance_v1 row for this phase.
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1 (
  run_id,
  started_at,
  ended_at,
  phases_applied,
  critical_findings_cleared,
  high_findings_cleared,
  med_findings_cleared,
  held_for_adjudication
)
SELECT
  'mig_172b_vocabulary_normalization_apply_recurrence_completion_20260429',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP,
  'rewrite_histology_vocab_map_and_normalize_recurrence_completion_histology_family',
  '0',
  '1',
  '4',
  '0';

-- §E — post-state verification probes.
-- Expected simulated post-state from read-only probe with the rewritten CSV:
--   recurrence_histology=11; recurrence_histology_v2=8;
--   completion_prior_histology=10; completion_histology_type=5.
SELECT
  COUNT(DISTINCT recurrence_histology)        AS recurrence_histology_n,
  COUNT(DISTINCT recurrence_histology_v2)     AS recurrence_histology_v2_n,
  COUNT(DISTINCT completion_prior_histology)  AS completion_prior_histology_n,
  COUNT(DISTINCT completion_histology_type)   AS completion_histology_type_n
FROM main.canonical_patient_master;

SELECT COUNT(*) AS remaining_mtc_ptc_mixed_in_target_cols
FROM main.canonical_patient_master
WHERE LOWER(TRIM(COALESCE(recurrence_histology, ''))) = 'mtc_ptc_mixed'
   OR LOWER(TRIM(COALESCE(recurrence_histology_v2, ''))) = 'mtc_ptc_mixed'
   OR LOWER(TRIM(COALESCE(completion_prior_histology, ''))) = 'mtc_ptc_mixed'
   OR LOWER(TRIM(COALESCE(completion_histology_type, ''))) = 'mtc_ptc_mixed';
-- Expect: 0.

SELECT COUNT(*) AS n_rows, COUNT(DISTINCT research_id) AS n_rids
FROM main.canonical_patient_master;
-- Expect: 10871 / 10871.

SELECT COUNT(*) FILTER (WHERE cpm_built_at IS NULL) AS null_cpm_built_at
FROM main.canonical_patient_master;
-- Expect: 0.
