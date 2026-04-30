-- =============================================================================
-- Migration 181 — PM syn_*_size 15 not_started cols verify + apply
-- =============================================================================
-- Date: 2026-04-29
-- Batch: mig_181_pm_syn_size_cols_verify_apply_20260429
-- Target DB: thyroid_canonical_publication_v1_0
-- Primary table touched: main.canonical_column_verification_registry_v1
-- Data tables touched: NONE (registry/signoff/provenance only)
--
-- Scope: verify and flip the 15 typed synoptic lobe-size columns added by
-- mig_173b from not_started to verified. The 3 *_legacy_raw columns remain na.
-- =============================================================================

-- §0 — pre-flight invariants (read-only)
SELECT COUNT(*) AS cpm_rows,
       COUNT(DISTINCT research_id) AS cpm_distinct_research_id,
       COUNT(*) FILTER (WHERE cpm_built_at IS NULL) AS null_cpm_built_at
FROM main.canonical_patient_master;

SELECT verification_status, COUNT(*) AS n_cols
FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
GROUP BY 1
ORDER BY 1;

-- §A — pre-snapshot of affected mig_173 registry rows (15 in-scope + 3 legacy_raw na)
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig181_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig181_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND batch_id = 'mig_173_syn_size_cm_dtype_reform_20260429'
  AND column_name IN (
    'syn_right_lobe_length_cm','syn_right_lobe_width_cm','syn_right_lobe_height_cm','syn_right_lobe_volume_cc','syn_right_lobe_size_parse_status',
    'syn_left_lobe_length_cm','syn_left_lobe_width_cm','syn_left_lobe_height_cm','syn_left_lobe_volume_cc','syn_left_lobe_size_parse_status',
    'syn_isthmus_length_cm','syn_isthmus_width_cm','syn_isthmus_height_cm','syn_isthmus_volume_cc','syn_isthmus_size_parse_status',
    'syn_right_lobe_size_cm_legacy_raw','syn_left_lobe_size_cm_legacy_raw','syn_isthmus_size_cm_legacy_raw'
  );

-- §B — Path-C stamp on 15 typed/status columns
UPDATE main.canonical_column_verification_registry_v1
SET verified_by = 'Logan Glosser <logan.glosser@gmail.com>',
    batch_id = 'mig_181_pm_syn_size_cols_verify_apply_20260429',
    verification_method = 'derivation_vs_syn_size_legacy_raw_parse_pipeline',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = CASE
      WHEN notes IS NULL OR TRIM(notes) = '' THEN
        'mig_181 verified against preserved *_legacy_raw parse pipeline: parsed_3axis axis extraction, rectangular volume formula, parse_status multi-state enum, deterministic spot-checks. CF-mig181-SYN-SIZE-ZERO-AXIS-EDGECASE documents 3 source strings with literal 0 axis values preserved by parser.'
      WHEN POSITION('mig_181 verified against preserved *_legacy_raw parse pipeline' IN notes) > 0 THEN notes
      ELSE notes || ' | ' ||
        'mig_181 verified against preserved *_legacy_raw parse pipeline: parsed_3axis axis extraction, rectangular volume formula, parse_status multi-state enum, deterministic spot-checks. CF-mig181-SYN-SIZE-ZERO-AXIS-EDGECASE documents 3 source strings with literal 0 axis values preserved by parser.'
    END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'syn_right_lobe_length_cm','syn_right_lobe_width_cm','syn_right_lobe_height_cm','syn_right_lobe_volume_cc','syn_right_lobe_size_parse_status',
    'syn_left_lobe_length_cm','syn_left_lobe_width_cm','syn_left_lobe_height_cm','syn_left_lobe_volume_cc','syn_left_lobe_size_parse_status',
    'syn_isthmus_length_cm','syn_isthmus_width_cm','syn_isthmus_height_cm','syn_isthmus_volume_cc','syn_isthmus_size_parse_status'
  );

-- §C — status flips: 15 typed/status columns -> verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'syn_right_lobe_length_cm','syn_right_lobe_width_cm','syn_right_lobe_height_cm','syn_right_lobe_volume_cc','syn_right_lobe_size_parse_status',
    'syn_left_lobe_length_cm','syn_left_lobe_width_cm','syn_left_lobe_height_cm','syn_left_lobe_volume_cc','syn_left_lobe_size_parse_status',
    'syn_isthmus_length_cm','syn_isthmus_width_cm','syn_isthmus_height_cm','syn_isthmus_volume_cc','syn_isthmus_size_parse_status'
  );

-- §D — parse_status enum appendix for the 3 status columns
UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE
  WHEN notes IS NULL OR TRIM(notes) = '' THEN
    'mig_181 parse_status enum: parsed_3axis, parsed_partial, sentinel, unparsed, NULL. Multi-valued audit/status field, not a Type-A near-uniform flag and not a Type-B placeholder.'
  WHEN POSITION('mig_181 parse_status enum:' IN notes) > 0 THEN notes
  ELSE notes || ' | ' ||
    'mig_181 parse_status enum: parsed_3axis, parsed_partial, sentinel, unparsed, NULL. Multi-valued audit/status field, not a Type-A near-uniform flag and not a Type-B placeholder.'
END
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN (
    'syn_right_lobe_size_parse_status',
    'syn_left_lobe_size_parse_status',
    'syn_isthmus_size_parse_status'
  );

-- §E — table-level signoff resync for canonical_patient_master
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
    signoff_migration = 'qc_framework_v1/migrations/181_pm_syn_size_cols_verify_apply_20260429.sql',
    notes = CASE
      WHEN ts.notes IS NULL OR TRIM(ts.notes) = '' THEN
        'mig_181: verified 15 syn_*_size typed/status columns against mig_173 parse pipeline. PM not_started reduced 16 to 1.'
      WHEN POSITION('mig_181: verified 15 syn_*_size typed/status columns' IN ts.notes) > 0 THEN ts.notes
      ELSE ts.notes || ' | ' ||
        'mig_181: verified 15 syn_*_size typed/status columns against mig_173 parse pipeline. PM not_started reduced 16 to 1.'
    END
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

-- §F — CPM reconciliation provenance row for registry-only closure
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared,
   held_for_adjudication)
SELECT
  'canonical_cleanup_mig181_pm_syn_size_cols_verify_apply_20260429',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'pre_snapshot_derivation_audit_registry_stamp_status_flip_signoff_resync',
  'none',
  'CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_right_lobe_size_cm|CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_left_lobe_size_cm|CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_isthmus_size_cm',
  'CF-mig181-SYN-SIZE-VOLUME-FORMULA-RECTANGULAR',
  'CF-mig181-SYN-SIZE-ZERO-AXIS-EDGECASE'
WHERE NOT EXISTS (
  SELECT 1
  FROM manuscript_workspace.cpm_reconciliation_provenance_v1
  WHERE run_id = 'canonical_cleanup_mig181_pm_syn_size_cols_verify_apply_20260429'
);

-- §G — post-state verification probes
SELECT verification_status, COUNT(*) AS n_cols
FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
GROUP BY 1
ORDER BY 1;

SELECT table_status, n_columns_total, n_verified, n_not_started, n_failed, n_na, signoff_migration
FROM main.canonical_table_signoff_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master';
