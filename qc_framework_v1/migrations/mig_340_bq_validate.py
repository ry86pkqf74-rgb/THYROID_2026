#!/usr/bin/env python3
"""Run mig_340 BigQuery validation checks A–F and print copy-pastable output."""
from __future__ import annotations

from google.api_core.exceptions import NotFound
from google.cloud import bigquery as bq

PROJECT = "thyroid-canonical-pub-2026"
LOCATION = "us-central1"

SQL_A = """
SELECT 'A_distinct_patients' AS check_id, COUNT(DISTINCT research_id) AS distinct_patients
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`
"""

SQL_B = """
WITH missing AS (
  SELECT rid FROM UNNEST([
    '3430', '5984', '5985', '6558', '6681', '6860', '7099', '7311', '8760', '9338',
    '10317', '10420', '10514', '10588', '10621', '10726', '10797', '10872', '10992',
    '11024', '11025', '11036', '11037', '11134', '11189', '11216', '11242', '11281',
    '11475', '11481', '11486', '11634', '11644', '11660', '11753', '11795', '11880',
    '12006', '12061', '12146'
  ]) AS rid
)
SELECT 'B_never_ingested_list' AS check_id, COUNT(DISTINCT t.research_id) AS n_present
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1` t
JOIN missing m ON CAST(t.research_id AS STRING) = m.rid
"""

SQL_C = """
SELECT analyte, analyte_assignment_method, COUNT(*) AS n
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`
GROUP BY 1, 2
ORDER BY 1, 2
"""

SQL_D = """
SELECT
  'D_legacy_triples_missing' AS check_id,
  COUNT(*) AS n_missing
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
)
"""

SQL_E = """
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
FROM paired
"""

SQL_F = """
SELECT 'F_archive_snapshot_rows' AS check_id, COUNT(*) AS snapshot_rows
FROM `thyroid-canonical-pub-2026.pub_archive.canonical_labs_thyroglobulin_v1_pre_tgrebuild_20260514`
"""


def _has_column(client: bq.Client, fq_table: str, column: str) -> bool:
    proj, ds, tname = fq_table.split(".")
    q = f"""
    SELECT COUNT(*) AS n
    FROM `{proj}.{ds}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @t AND column_name = @c
    """
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter("t", "STRING", tname),
            bq.ScalarQueryParameter("c", "STRING", column),
        ],
    )
    return int(list(client.query(q, job_config=job_config).result())[0].n) > 0


def main() -> None:
    client = bq.Client(project=PROJECT, location=LOCATION)
    print("=== mig_340 BigQuery validation (Prompt 18) ===\n")

    for label, sql in [
        ("A — DISTINCT patients", SQL_A),
        ("B — 40 never-ingested list present", SQL_B),
        ("D — Legacy triples missing from new", SQL_D),
        ("E — Longitudinal normalization (value_raw + date pairing)", SQL_E),
    ]:
        print(f"--- {label} ---")
        for row in client.query(sql).result():
            print(dict(row))
        print()

    canon_tbl = f"{PROJECT}.pub_canonical.canonical_labs_thyroglobulin_v1"
    if _has_column(client, canon_tbl, "analyte_assignment_method"):
        print("--- C — analyte × analyte_assignment_method ---")
        for row in client.query(SQL_C).result():
            print(f"  {row.analyte}\t{row.analyte_assignment_method}\t{row.n}")
        print()
    else:
        print(
            "--- C — analyte × analyte_assignment_method ---\n"
            "  SKIPPED: column analyte_assignment_method not present yet "
            "(run mig_340 --apply; post-rebuild table includes assignment audit column).\n"
        )

    print("--- F — Archive snapshot (skipped if table missing) ---")
    try:
        for row in client.query(SQL_F).result():
            print(dict(row))
    except NotFound:
        print(
            "  SKIPPED: pub_archive.canonical_labs_thyroglobulin_v1_pre_tgrebuild_20260514 "
            "not found (expected before first mig_340 --apply)."
        )
    print()
    print("Done.")


if __name__ == "__main__":
    main()
