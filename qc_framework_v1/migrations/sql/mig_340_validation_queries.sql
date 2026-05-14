-- mig_340 validation queries — BigQuery (thyroid-canonical-pub-2026).
-- Run after: mig_340_thyroglobulin_analyst_bq_rebuild.py --csv ... --apply
--
-- Labeling matches Prompt 18:
--   A — patient coverage
--   B — 40 never-ingested RIDs (Prompt 6 list)
--   C — analyte × analyte_assignment_method (combined-test split + COMMENT on table)
--   D — no regression vs pub_legacy thyroglobulin_lab_canonical_v1 triples
--   E — normalization vs pub_canonical.longitudinal_lab_canonical_v1 (paired by value_raw + date)
--   F — archive snapshot rowcount (expect equals pre-replace canonical rowcount; for audit)

-- -----------------------------------------------------------------------------
-- A: Full patient coverage (expect 3298 distinct research_id after mig_340)
-- -----------------------------------------------------------------------------
SELECT
  'A_distinct_patients' AS check_id,
  COUNT(DISTINCT research_id) AS distinct_patients
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`;

-- -----------------------------------------------------------------------------
-- B: The 40 “never-ingested” patients — expect COUNT(DISTINCT) = 40
-- -----------------------------------------------------------------------------
WITH missing AS (
  SELECT rid FROM UNNEST([
    '3430', '5984', '5985', '6558', '6681', '6860', '7099', '7311', '8760', '9338',
    '10317', '10420', '10514', '10588', '10621', '10726', '10797', '10872', '10992',
    '11024', '11025', '11036', '11037', '11134', '11189', '11216', '11242', '11281',
    '11475', '11481', '11486', '11634', '11644', '11660', '11753', '11795', '11880',
    '12006', '12061', '12146'
  ]) AS rid
)
SELECT
  'B_never_ingested_list' AS check_id,
  COUNT(DISTINCT t.research_id) AS n_present_in_missing_list
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1` t
JOIN missing m ON CAST(t.research_id AS STRING) = m.rid;

-- -----------------------------------------------------------------------------
-- C: Combined-test split (expect Tg + TgAb; ~34.5k combo source rows split into
--    inferred_* assignment methods; rule documented on canonical table OPTIONS)
-- -----------------------------------------------------------------------------
SELECT
  'C_analyte_assignment' AS check_id,
  analyte,
  analyte_assignment_method,
  COUNT(*) AS n
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`
GROUP BY 1, 2
ORDER BY analyte, analyte_assignment_method;

-- -----------------------------------------------------------------------------
-- D: No regression — legacy triples absent from new (expect 0)
--    Legacy uses specimen_collect_dt (not lab_datetime). Keys aligned to SECOND.
-- -----------------------------------------------------------------------------
SELECT
  'D_legacy_triples_missing' AS check_id,
  COUNT(*) AS n_legacy_triples_absent_from_new
FROM (
  SELECT
    TRIM(CAST(research_id AS STRING)) AS research_id,
    analyte,
    TIMESTAMP_TRUNC(
      TIMESTAMP_MICROS(CAST(DIV(specimen_collect_dt, 1000) AS INT64)),
      SECOND
    ) AS lab_datetime
  FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.thyroglobulin_lab_canonical_v1`
  EXCEPT DISTINCT
  SELECT
    TRIM(CAST(research_id AS STRING)) AS research_id,
    analyte,
    TIMESTAMP_TRUNC(lab_datetime, SECOND) AS lab_datetime
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`
);

-- -----------------------------------------------------------------------------
-- E: Normalization regression — join thyroid_tumor_markers longitudinal rows to
--    rebuilt canonical on (research_id, calendar day, mapped analyte, TRIM(value_raw)).
--    Expect mismatch_count = 0 after rebuild.
--    Note: pairing is 1:1 when value_raw matches EHR extract string; if 0 pairs,
--    investigate value_raw drift (whitespace) before interpreting.
-- -----------------------------------------------------------------------------
WITH canon AS (
  SELECT
    research_id,
    analyte,
    lab_datetime,
    TRIM(COALESCE(value_raw, "")) AS value_raw_trim,
    value_numeric AS canon_vn,
    is_censored AS canon_ic
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`
),
ll AS (
  SELECT
    research_id,
    lab_date,
    lab_name_standardized,
    TRIM(COALESCE(value_raw, "")) AS value_raw_trim,
    value_numeric AS ll_vn,
    is_censored AS ll_ic
  FROM `thyroid-canonical-pub-2026.pub_canonical.longitudinal_lab_canonical_v1`
  WHERE analyte_group = "thyroid_tumor_markers"
    AND lab_name_standardized IN ("thyroglobulin", "anti_thyroglobulin")
),
paired AS (
  SELECT
    c.research_id,
    c.analyte,
    c.lab_datetime,
    c.canon_vn,
    c.canon_ic,
    l.ll_vn,
    l.ll_ic
  FROM canon c
  INNER JOIN ll l
    ON CAST(c.research_id AS STRING) = l.research_id
    AND DATE(c.lab_datetime) = l.lab_date
    AND l.lab_name_standardized = IF(c.analyte = "Tg", "thyroglobulin", "anti_thyroglobulin")
    AND c.value_raw_trim = l.value_raw_trim
)
SELECT
  'E_longitudinal_normalization' AS check_id,
  COUNT(*) AS paired_rows,
  COUNTIF(
    NOT (
      (canon_vn IS NULL AND ll_vn IS NULL)
      OR (
        canon_vn IS NOT NULL
        AND ll_vn IS NOT NULL
        AND ABS(canon_vn - ll_vn) < 1e-9
      )
    )
    OR canon_ic IS DISTINCT FROM ll_ic
  ) AS mismatch_count
FROM paired;

-- -----------------------------------------------------------------------------
-- F: Archive snapshot (created immediately before canonical replace; expect rowcount > 0)
-- -----------------------------------------------------------------------------
SELECT
  'F_archive_snapshot_rows' AS check_id,
  COUNT(*) AS snapshot_rows
FROM `thyroid-canonical-pub-2026.pub_archive.canonical_labs_thyroglobulin_v1_pre_tgrebuild_20260514`;
