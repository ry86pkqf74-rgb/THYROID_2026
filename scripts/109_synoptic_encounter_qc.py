#!/usr/bin/env python3
"""Synoptic encounter isolation QC for path_synoptics.

Creates:
  - VIEW path_synoptics_encounter_qc_v1 — base synoptic row + surg_date_canonical,
    surg_date_parse_tier (see utils.surg_date_canonical), encounter_synoptic_row_ix
    (row number within research_id + canonical surgery date; use to disambiguate
    multiple synoptic lines on the same calendar day).
  - TABLE val_path_synoptic_encounter_isolation_v1 — encounter keys where either
    surg_date_canonical is NULL or multiple synoptic rows share the same key with
    conflicting tumor_1 LN examined/involved (trimmed VARCHAR distinct counts).

Linkage policy (manuscript / joins): prefer (research_id, surgery_date_canonical);
when multiple rows exist, pair using encounter_synoptic_row_ix with operative /
tumor-level tables (e.g. synoptic_tumor_long_v1.synoptic_row_ix, surgery_episode_id).

Usage:
  .venv/bin/python scripts/109_synoptic_encounter_qc.py
  .venv/bin/python scripts/109_synoptic_encounter_qc.py --local ./thyroid_master_local.duckdb
  LOCAL_DB_PATH=... .venv/bin/python scripts/109_synoptic_encounter_qc.py --md
  .venv/bin/python scripts/109_synoptic_encounter_qc.py --md --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402

from local DuckDB_client import get_token, resolve_database_for_env  # noqa: E402
from utils.surg_date_canonical import (  # noqa: E402
    surgery_date_canonical_sql,
    surgery_date_parse_tier_sql,
)


def _ddl() -> tuple[str, str]:
    canon = surgery_date_canonical_sql("p.surg_date")
    tier = surgery_date_parse_tier_sql("p.surg_date")
    view_sql = f"""
CREATE OR REPLACE VIEW path_synoptics_encounter_qc_v1 AS
WITH base AS (
  SELECT
    p.*,
    {canon} AS surg_date_canonical,
    {tier} AS surg_date_parse_tier
  FROM path_synoptics p
  WHERE p.research_id IS NOT NULL
)
SELECT
  b.*,
  ROW_NUMBER() OVER (
    PARTITION BY b.research_id, b.surg_date_canonical
    ORDER BY
      TRIM(CAST(b.tumor_1_ln_examined AS VARCHAR)),
      TRIM(CAST(b.tumor_1_ln_involved AS VARCHAR)),
      TRIM(CAST(b.tumor_1_histologic_type AS VARCHAR)),
      TRIM(CAST(b.thyroid_procedure AS VARCHAR))
  ) AS encounter_synoptic_row_ix
FROM base b
""".strip()
    val_sql = """
CREATE OR REPLACE TABLE val_path_synoptic_encounter_isolation_v1 AS
WITH enc AS (
  SELECT * FROM path_synoptics_encounter_qc_v1
),
grp AS (
  SELECT
    research_id,
    surg_date_canonical,
    COUNT(*)::BIGINT AS n_synoptic_rows,
    COUNT(DISTINCT TRIM(CAST(tumor_1_ln_examined AS VARCHAR)))::BIGINT
      AS n_distinct_ln_examined,
    COUNT(DISTINCT TRIM(CAST(tumor_1_ln_involved AS VARCHAR)))::BIGINT
      AS n_distinct_ln_involved
  FROM enc
  GROUP BY 1, 2
)
SELECT
  research_id,
  surg_date_canonical,
  n_synoptic_rows,
  n_distinct_ln_examined,
  n_distinct_ln_involved,
  (n_synoptic_rows > 1
    AND (n_distinct_ln_examined > 1 OR n_distinct_ln_involved > 1))
    AS ln_mismatch_same_encounter,
  (surg_date_canonical IS NULL) AS encounter_date_unresolved,
  TRUE AS qc_attention_flag
FROM grp
WHERE (n_synoptic_rows > 1
       AND (n_distinct_ln_examined > 1 OR n_distinct_ln_involved > 1))
   OR surg_date_canonical IS NULL
""".strip()
    return view_sql, val_sql


def _mirror_md_sql() -> tuple[str, str]:
    return (
        "CREATE OR REPLACE TABLE md_path_synoptics_encounter_qc_v1 AS "
        "SELECT * FROM path_synoptics_encounter_qc_v1",
        "CREATE OR REPLACE TABLE md_val_path_synoptic_encounter_isolation_v1 AS "
        "SELECT * FROM val_path_synoptic_encounter_isolation_v1",
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Deploy path_synoptics encounter QC objects")
    ap.add_argument("--local", type=str, default=None, help="Local DuckDB path")
    ap.add_argument("--md", action="store_true", help="Apply to local DuckDB (token required)")
    ap.add_argument("--sa", action="store_true", help="Prefer LOCAL_DB_PATH for local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Print SQL only")
    args = ap.parse_args()

    view_sql, val_sql = _ddl()
    to_run: list[str] = [view_sql, val_sql]
    if args.md:
        to_run.extend(_mirror_md_sql())

    if args.dry_run:
        for s in to_run:
            print(s)
            print(";")
        return 0

    if args.md:
        tok = get_token(prefer_service_account=args.sa)
        if not tok:
            print("Missing LOCAL_DB_PATH / LOCAL_DB_PATH for --md", file=sys.stderr)
            return 1
        for k in ("USE_LOCAL_DUCKDB", "use_local_duckdb"):
            os.environ.pop(k, None)
        db = resolve_database_for_env(os.getenv("LOCAL_DB_ENV", "prod"))
        uri = f"thyroid_master.duckdb"
        con: duckdb.DuckDBPyConnection = duckdb.connect(uri)
        label = f"thyroid_master.duckdb"
    else:
        path = Path(args.local or DB_PATH).expanduser()
        if not path.is_file():
            print(f"Local database not found: {path}", file=sys.stderr)
            return 1
        con = duckdb.connect(str(path))
        label = str(path)

    try:
        for s in to_run:
            con.execute(s)
    finally:
        con.close()

    print(f"109_synoptic_encounter_qc: applied to {label} ({len(to_run)} statements)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
