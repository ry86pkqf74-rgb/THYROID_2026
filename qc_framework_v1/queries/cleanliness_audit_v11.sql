-- v11 5-gate cleanliness audit for thyroid_canonical_publication_v1_0.main canonical_* verified tables.
-- mig_203: extended audit_allowlist + suffix-pattern exclusions (_built_at, _derived_at, _resolved_at, _confidence).
-- Expected post-mig_203 + mig_160b chain: gate1=172, gate2=0, gate3=0, gate4=0, gate5=0.

USE thyroid_canonical_publication_v1_0;

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
  ) AS gate5;
