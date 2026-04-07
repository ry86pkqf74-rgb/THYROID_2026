#!/usr/bin/env python3
"""
build_fna_history_from_fnas_detailed.py

Melt processed/fnas_detailed.parquet (wide FNA workbook) into long
processed/fna_history.parquet expected by scripts/22_canonical_episodes_v2.py.

fna_index uses the same ordering as script 22:
  ROW_NUMBER PARTITION BY research_id
  ORDER BY COALESCE(TRY_CAST(fna_date_parsed AS DATE), TRY_CAST(date AS DATE), DATE '2099-01-01'), source_slot

Run after 01_ingest_all_files (FNAs 12_5_2025.xlsx → fnas_detailed).

Usage (repo THYROID_2026 root):
  python scripts/build_fna_history_from_fnas_detailed.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
FNAS_PQ = PROCESSED / "fnas_detailed.parquet"
OUT_PQ = PROCESSED / "fna_history.parquet"


def _specimen_col(slot: int) -> str:
    if slot in (1, 2):
        raise ValueError("use preop column for slots 1–2")
    if 3 <= slot <= 8:
        return f"fna_{slot}_specimen_received"
    return f"fna_{slot}_specimen"


def _branch_slot1() -> str:
    return """
    SELECT
        TRY_CAST(regexp_replace(CAST(research_id AS VARCHAR), '\\.0$', '') AS INTEGER) AS research_id,
        1 AS source_slot,
        CAST("1_preop_fna_date" AS VARCHAR) AS date,
        CAST("1_preop_fna_date" AS VARCHAR) AS fna_date_parsed,
        CAST(bethesda AS VARCHAR) AS bethesda,
        CAST(preop_fna_history AS VARCHAR) AS path,
        CAST(fna1_path_extended AS VARCHAR) AS path_extended,
        CAST(preop_specimen_received_fna_location AS VARCHAR) AS preop_specimen_received_fna_location,
        NULL::VARCHAR AS specimen,
        CAST(preop_specimen_received_fna_location AS VARCHAR) AS specimen_received
    FROM raw
    """


def _branch_slot2() -> str:
    return """
    SELECT
        TRY_CAST(regexp_replace(CAST(research_id AS VARCHAR), '\\.0$', '') AS INTEGER) AS research_id,
        2 AS source_slot,
        CAST(preop_fna_2_date AS VARCHAR) AS date,
        CAST(preop_fna_2_date AS VARCHAR) AS fna_date_parsed,
        CAST(bethesda_2 AS VARCHAR) AS bethesda,
        CAST(fna_2_history AS VARCHAR) AS path,
        CAST(fna_2_path_extended AS VARCHAR) AS path_extended,
        NULL::VARCHAR AS preop_specimen_received_fna_location,
        CAST(fna_2_specimen_received AS VARCHAR) AS specimen,
        CAST(fna_2_specimen_received AS VARCHAR) AS specimen_received
    FROM raw
    """


def _branch_slot_n(n: int) -> str:
    sc = _specimen_col(n)
    return f"""
    SELECT
        TRY_CAST(regexp_replace(CAST(research_id AS VARCHAR), '\\.0$', '') AS INTEGER) AS research_id,
        {n} AS source_slot,
        CAST(fna_{n}_date AS VARCHAR) AS date,
        CAST(fna_{n}_date AS VARCHAR) AS fna_date_parsed,
        CAST(fna_{n}_bethesda AS VARCHAR) AS bethesda,
        CAST(fna_{n}_path AS VARCHAR) AS path,
        CAST(fna_{n}_history AS VARCHAR) AS path_extended,
        NULL::VARCHAR AS preop_specimen_received_fna_location,
        CAST({sc} AS VARCHAR) AS specimen,
        CAST({sc} AS VARCHAR) AS specimen_received
    FROM raw
    """


def build_sql() -> str:
    branches = [_branch_slot1(), _branch_slot2()]
    branches.extend(_branch_slot_n(n) for n in range(3, 13))
    unions = " UNION ALL ".join(b.strip() for b in branches)
    return f"""
CREATE OR REPLACE TABLE fna_history_out AS
WITH raw AS (
    SELECT * FROM read_parquet('{FNAS_PQ.as_posix()}')
),
stacked AS (
{unions}
),
filtered AS (
    SELECT *
    FROM stacked
    WHERE research_id IS NOT NULL
      AND (
          (date IS NOT NULL AND CAST(date AS VARCHAR) NOT IN ('', 'none', 'None'))
          OR (bethesda IS NOT NULL AND CAST(bethesda AS VARCHAR) NOT IN ('', 'none', 'None'))
          OR (path IS NOT NULL AND CAST(path AS VARCHAR) NOT IN ('', 'none', 'None'))
          OR (path_extended IS NOT NULL AND CAST(path_extended AS VARCHAR) NOT IN ('', 'none', 'None'))
          OR (specimen IS NOT NULL AND CAST(specimen AS VARCHAR) NOT IN ('', 'none', 'None'))
          OR (preop_specimen_received_fna_location IS NOT NULL
              AND CAST(preop_specimen_received_fna_location AS VARCHAR) NOT IN ('', 'none', 'None'))
      )
),
parsed AS (
    SELECT
        *,
        COALESCE(
            TRY_CAST(date AS DATE),
            CAST(
                try_strptime(
                    trim(regexp_replace(CAST(date AS VARCHAR), '[\\n\\r]+', '', 'g')),
                    '%m/%d/%Y'
                ) AS DATE
            ),
            CAST(
                try_strptime(
                    trim(regexp_replace(CAST(date AS VARCHAR), '[\\n\\r]+', '', 'g')),
                    '%m/%d/%y'
                ) AS DATE
            ),
            CAST(
                try_strptime(
                    regexp_extract(
                        trim(regexp_replace(CAST(date AS VARCHAR), '[\\n\\r]+', '', 'g')),
                        '^([0-9]{1,2}/[0-9]{1,2}/[0-9]{{4}})',
                        1
                    ),
                    '%m/%d/%Y'
                ) AS DATE
            )
        ) AS _resolved_date
    FROM filtered
)
SELECT
    research_id,
    ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY
            COALESCE(_resolved_date, DATE '2099-01-01'),
            source_slot
    )::INTEGER AS fna_index,
    CASE
        WHEN _resolved_date IS NOT NULL THEN strftime(_resolved_date, '%Y-%m-%d')
        ELSE CAST(date AS VARCHAR)
    END AS date,
    CASE
        WHEN _resolved_date IS NOT NULL THEN strftime(_resolved_date, '%Y-%m-%d')
        ELSE CAST(fna_date_parsed AS VARCHAR)
    END AS fna_date_parsed,
    bethesda,
    path,
    path_extended,
    preop_specimen_received_fna_location,
    specimen,
    specimen_received
FROM parsed
"""


def main() -> None:
    if not FNAS_PQ.exists():
        print(f"  FATAL: {FNAS_PQ} not found. Run scripts/01_ingest_all_files.py first.", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect(":memory:")
    con.execute(build_sql())
    con.execute(f"COPY fna_history_out TO '{OUT_PQ.as_posix()}' (FORMAT PARQUET)")
    n = con.execute("SELECT COUNT(*) FROM fna_history_out").fetchone()[0]
    p = con.execute("SELECT COUNT(DISTINCT research_id) FROM fna_history_out").fetchone()[0]
    print(f"  Wrote {OUT_PQ.name}: {n:,} rows, {p:,} patients")


if __name__ == "__main__":
    main()
