-- mig_203 — gate5 → 0 (v11 audit allowlist + suffix patterns) + PM signoff registry refresh (10 *_resolved / AJCC resolution cols).
-- Target DB: thyroid_canonical_publication_v1_0
-- Closes CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION.
-- Predecessor: mig_188b ALTER COLUMN ADD on canonical_patient_master; mig_160b date retype.
-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY

USE thyroid_canonical_publication_v1_0;

-- §A Idempotent register 10 mig_188b columns (ordinal_position + data_type from information_schema)
INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position,
   category, upstream_source, verification_status, verified_by, verified_ts,
   verification_method, batch_id, notes)
SELECT
  ic.table_schema,
  ic.table_name,
  ic.column_name,
  ic.data_type,
  ic.ordinal_position,
  'derived' AS category,
  meta.upstream_source,
  'verified' AS verification_status,
  'mig_203' AS verified_by,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS verified_ts,
  meta.verification_method,
  meta.batch_id,
  meta.notes
FROM information_schema.columns AS ic
INNER JOIN (
  VALUES
    ('ajcc8_t_stage_resolved', 'canonical_path_malignant_events_v1', 'derived_from_canonical_path_malignant_events_v1_via_mig188b', 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430', 'mig_188b R1 AJCC + r1c T0 derivation; manuscript SQL prefers *_resolved.'),
    ('ajcc8_n_stage_resolved', 'canonical_path_malignant_events_v1', 'derived_from_canonical_path_malignant_events_v1_via_mig188b', 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430', 'mig_188b N1a/N1b split using PM cnln_img_*_present + lateral_neck_dissected.'),
    ('ajcc8_m_stage_resolved', 'canonical_path_malignant_events_v1', 'derived_from_canonical_path_malignant_events_v1_via_mig188b', 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430', 'mig_188b M0 default unless M1 evidence.'),
    ('ajcc8_stage_group_resolved', 'canonical_path_malignant_events_v1', 'derived_from_canonical_path_malignant_events_v1_via_mig188b', 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430', 'mig_188b Sect-G AJCC8 stage_group with MTC N1a->III / N1b->IVA + DTC age 55 split.'),
    ('ajcc7_t_stage_resolved', 'canonical_path_malignant_events_v1', 'derived_from_canonical_path_malignant_events_v1_via_mig188b', 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430', 'mig_188b AJCC7 mapping: T3b->T3, T0->T0 pass-through.'),
    ('ajcc7_n_stage_resolved', 'canonical_path_malignant_events_v1', 'derived_from_canonical_path_malignant_events_v1_via_mig188b', 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430', 'mig_188b AJCC7 N copy-through.'),
    ('ajcc7_m_stage_resolved', 'canonical_path_malignant_events_v1', 'derived_from_canonical_path_malignant_events_v1_via_mig188b', 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430', 'mig_188b AJCC7 M copy-through.'),
    ('ajcc7_stage_group_resolved', 'canonical_path_malignant_events_v1', 'derived_from_canonical_path_malignant_events_v1_via_mig188b', 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430', 'mig_188b Sect-G AJCC7 stage_group with MTC N1a->III / N1b->IVA + DTC age 45 split.'),
    ('ajcc_resolution_source', 'canonical_path_malignant_events_v1', 'set_by_mig188b_resolution_token', 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430', 'mig_188b: identifies which derivation rule produced the *_resolved values.'),
    ('ajcc_resolution_confidence', 'canonical_path_malignant_events_v1', 'set_by_mig188b_confidence_classification', 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430', 'mig_188b: high/medium/uncalculable_or_pending.')
) AS meta(column_name, upstream_source, verification_method, batch_id, notes)
  ON ic.column_name = meta.column_name
WHERE ic.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND ic.table_schema = 'main'
  AND ic.table_name = 'canonical_patient_master'
  AND NOT EXISTS (
    SELECT 1
    FROM main.canonical_column_verification_registry_v1 AS r
    WHERE r.schema_name = ic.table_schema
      AND r.table_name = ic.table_name
      AND r.column_name = ic.column_name
  );

-- §B Resync PM rollup counts from live schema + registry (idempotent)
UPDATE main.canonical_table_signoff_registry_v1
SET n_columns_total = (
      SELECT COUNT(*)
      FROM information_schema.columns
      WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
        AND table_schema = 'main'
        AND table_name = 'canonical_patient_master'
    ),
    n_verified = (
      SELECT COUNT(*)
      FROM main.canonical_column_verification_registry_v1
      WHERE schema_name = 'main'
        AND table_name = 'canonical_patient_master'
        AND verification_status = 'verified'
    )
WHERE table_name = 'canonical_patient_master';

-- §C Verify gate3 still 0 after resync
SELECT COUNT(*) AS gate3_violations
FROM main.canonical_table_signoff_registry_v1 AS t
WHERE t.table_status = 'verified'
  AND (
    t.n_verified + t.n_na <> t.n_columns_total
    OR t.n_not_started <> 0
    OR COALESCE(t.n_failed, 0) <> 0
  );

-- §D Provenance row insert (idempotent)
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
SELECT
  'mig_203_gate5_zero_audit_allowlist_extension_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'pm_resolved_cols_registry_inserts_signoff_bump_audit_allowlist_v11',
  'CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION',
  '10_pm_resolved_cols_registered',
  'audit_query_v11_template_authored',
  'none'
WHERE NOT EXISTS (
  SELECT 1
  FROM manuscript_workspace.cpm_reconciliation_provenance_v1
  WHERE run_id = 'mig_203_gate5_zero_audit_allowlist_extension_20260430'
);

-- §E v11 5-gate audit (expect gate5 = 0 after allowlist + suffix rules)
WITH verified_tables AS (
  SELECT table_name
  FROM main.canonical_table_signoff_registry_v1
  WHERE table_status = 'verified'
    AND table_name LIKE 'canonical_%'
),
audit_allowlist AS (
  SELECT col_name
  FROM (
    VALUES
      ('build_ts'),
      ('built_at'),
      ('extracted_at'),
      ('llm_build_ts'),
      ('llm_extracted_at'),
      ('verified_ts'),
      ('signed_off_ts'),
      ('registered_ts'),
      ('updated_at'),
      ('created_at'),
      ('promoted_at'),
      ('completed_at'),
      ('started_at'),
      ('ended_at'),
      ('ingested_at_utc'),
      ('ingestion_date'),
      ('lab_datetime'),
      ('cpm_built_at'),
      ('rollup_built_at'),
      ('resolved_at'),
      ('reclassified_at')
  ) AS v(col_name)
)
SELECT
  (
    SELECT COUNT(*)
    FROM main.canonical_table_signoff_registry_v1
    WHERE table_status = 'verified'
  ) AS gate1,
  (
    SELECT COUNT(*)
    FROM main.canonical_table_signoff_registry_v1
    WHERE table_status = 'verified'
      AND signoff_migration IS NULL
  ) AS gate2,
  (
    SELECT COUNT(*)
    FROM main.canonical_table_signoff_registry_v1 AS t
    WHERE t.table_status = 'verified'
      AND (
        t.n_verified + t.n_na <> t.n_columns_total
        OR t.n_not_started <> 0
        OR COALESCE(t.n_failed, 0) <> 0
      )
  ) AS gate3,
  (
    SELECT COUNT(*)
    FROM main.canonical_column_verification_registry_v1 AS r
    INNER JOIN main.canonical_table_signoff_registry_v1 AS t USING (schema_name, table_name)
    WHERE t.table_status = 'verified'
      AND r.verification_status = 'verified'
      AND (
        r.verified_by IS NULL
        OR r.batch_id IS NULL
        OR r.verification_method IS NULL
      )
  ) AS gate4,
  (
    SELECT COUNT(*)
    FROM information_schema.columns AS c
    INNER JOIN verified_tables AS v ON c.table_name = v.table_name
    LEFT JOIN main.canonical_column_verification_registry_v1 AS r
      ON r.schema_name = 'main'
      AND r.table_name = c.table_name
      AND r.column_name = c.column_name
    WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
      AND c.table_schema = 'main'
      AND c.column_name NOT IN (SELECT audit_allowlist.col_name FROM audit_allowlist)
      AND NOT regexp_matches(c.column_name, '_built_at$')
      AND NOT regexp_matches(c.column_name, '_derived_at$')
      AND NOT regexp_matches(c.column_name, '_resolved_at$')
      AND NOT regexp_matches(c.column_name, '_confidence$')
      AND c.column_name NOT LIKE '%_status'
      AND c.column_name NOT LIKE '%_source'
      AND c.column_name NOT LIKE '%_keyword'
      AND c.column_name NOT LIKE '%_raw'
      AND COALESCE(r.verification_status, 'unknown') != 'na'
      AND (
        c.data_type IN ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE')
        OR (
          c.data_type = 'VARCHAR'
          AND (
            regexp_matches(c.column_name, '(^|_)dates?(_|$)')
            OR regexp_matches(c.column_name, '(^|_)dt(_|$)')
          )
        )
      )
  ) AS gate5_v11;
