-- mig_340 validation queries — BigQuery (thyroid-canonical-pub-2026).
-- Run after: mig_340_thyroglobulin_analyst_bq_rebuild.py --csv ... --apply

-- A: Full patient coverage (expect 3298 distinct research_id values)
SELECT COUNT(DISTINCT research_id) AS distinct_patients
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`;

-- B: Combined-test split — expect both analytes plus inferred_combo_* assignment methods.
SELECT analyte, analyte_assignment_method, COUNT(*) AS n
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`
GROUP BY 1, 2
ORDER BY 1, 2;

-- C: The 40 “never-ingested” patients — expect DISTINCT count = 40
WITH missing AS (
  SELECT rid FROM UNNEST([
    '3430', '5984', '5985', '6558', '6681', '6860', '7099', '7311', '8760', '9338',
    '10317', '10420', '10514', '10588', '10621', '10726', '10797', '10872', '10992',
    '11024', '11025', '11036', '11037', '11134', '11189', '11216', '11242', '11281',
    '11475', '11481', '11486', '11634', '11644', '11660', '11753', '11795', '11880',
    '12006', '12061', '12146'
  ]) AS rid
)
SELECT COUNT(DISTINCT t.research_id) AS n_present_in_missing_list
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1` t
JOIN missing m ON CAST(t.research_id AS STRING) = m.rid;

-- D: No regression — legacy (research_id, analyte, lab_datetime) triples still covered (expect 0).
SELECT COUNT(*) AS n_legacy_triples_absent_from_new
FROM (
  SELECT SAFE_CAST(TRIM(CAST(research_id AS STRING)) AS INT64) AS research_id, analyte, lab_datetime
  FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.thyroglobulin_lab_canonical_v1`
  EXCEPT DISTINCT
  SELECT research_id, analyte, lab_datetime
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`
);

-- E (manual): longitudinal regression vs pub_canonical.longitudinal_lab_canonical_v1 —
-- Join on CAST(research_id AS INT64), analyte mapping, TIMESTAMP_TRUNC(ts, SECOND);
-- Require value_numeric and is_censored parity on overlapping thyroid rows only.
