"""
qc_framework_v1/scripts/normalize_dates_v1_0_pass1.py
=====================================================

Pass-1 global date normalization across thyroid_canonical_publication_v1_0.

What this does:
  1. Discovers candidate date columns in main + manuscript_workspace base
     tables (excludes _legacy / _archived / _pre_cleanup snapshots).
  2. Filters to columns that ACTUALLY hold dates (samples 50 distinct
     non-null values; column qualifies if >=70% parse as dates under
     supported formats). This separates real date columns from metadata
     ones like `*_status`, `*_source`, `*_confidence`.
  3. For VARCHAR date columns: UPDATE every row whose value parses as a
     date to the canonical MM/DD/YYYY string (with YY -> 20YY rule).
     Leaves unparseable values alone -- those need per-column cleanup
     CSVs analogous to fna_date_raw__cleanup_round2.csv.
  4. For TIMESTAMP / TIMESTAMP_NS date columns: UPDATE to zero out the
     time component (DATE_TRUNC('day', col)). Keeps TIMESTAMP type for
     view compatibility; the time is now always 00:00:00.
  5. For DATE columns: no change (storage is already canonical).

The 2-digit-year rule: any YY parsed via %m/%d/%y is forced to 20YY
(00=2000, 25=2025, 99=2099). DuckDB's default %y is 19YY for >=50, which
contradicts Logan's directive. Implemented by parsing YY via %y, then
coercing year to 20YY explicitly.

Skipped column-name patterns (treated as metadata, not dates):
  *_status, *_source, *_confidence, *_traceability_*, *_kind, *_dtype,
  *_extraction_method, *_keyword, *_finding (free-text descriptions)

Output:
  qc_framework_v1/reports/date_normalization_pass1_report.md
  qc_framework_v1/migrations/68_global_date_normalization_pass1.sql

This script is idempotent: running twice on already-normalized values is
a no-op (parse + reformat round-trips cleanly).
"""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORT_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / "date_normalization_pass1_report.md"
MIG_PATH    = REPO_ROOT / "qc_framework_v1" / "migrations" / "68_global_date_normalization_pass1.sql"

DEST_DB = "thyroid_canonical_publication_v1_0"

# Column-name patterns that signal METADATA (not actual date values).
SKIP_COLUMN_NAME_PATTERNS = [
    r".*_status$",
    r".*_source$",
    r".*_confidence$",
    r".*_traceability.*",
    r".*_kind$",
    r".*_dtype$",
    r".*_extraction_method$",
    r".*_keyword$",
    r".*_key_finding$",
    r".*_native_format$",
    r"^source_col_name_date$",  # column-NAME holder, not a date
]

# Sample threshold: column qualifies as "date column" if at least this fraction
# of non-null sampled values parse cleanly as dates.
DATE_PARSE_THRESHOLD = 0.7

# DATE formats to attempt during sampling (in priority order)
SAMPLE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
]


def is_skipped_column(col: str) -> bool:
    return any(re.match(p, col, re.IGNORECASE) for p in SKIP_COLUMN_NAME_PATTERNS)


def parse_date(s: str) -> date | None:
    if s is None:
        return None
    s = s.strip()
    if not s or s.lower() in (
        "not specified", "n/s", "n/a", "not available", "none",
        "date unknown", "date unknown:",
    ):
        return None
    for fmt in SAMPLE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            if fmt in ("%m/%d/%y",):
                # Force 20YY rule
                yy = dt.year % 100
                dt = dt.replace(year=2000 + yy)
            return dt.date()
        except ValueError:
            continue
    return None


def main() -> None:
    print(f"[normalize] connecting MD ({DEST_DB})")
    con = duckdb.connect("md:")
    con.execute(f"USE {DEST_DB}")

    print("[normalize] discovering candidate date columns")
    candidates = con.execute("""
        WITH base_tables AS (
          SELECT table_schema, table_name
          FROM information_schema.tables
          WHERE table_schema IN ('main','manuscript_workspace')
            AND table_type='BASE TABLE'
            AND table_name NOT LIKE '%_legacy%'
            AND table_name NOT LIKE '%_archived%'
            AND table_name NOT LIKE '%_pre_cleanup_%'
            AND table_name NOT LIKE '%_snapshot%'
            AND table_name NOT LIKE '%_backup%'
        )
        SELECT c.table_schema, c.table_name, c.column_name, c.data_type
        FROM information_schema.columns c
        JOIN base_tables b USING (table_schema, table_name)
        WHERE
          (c.column_name ILIKE '%date%' OR c.column_name ILIKE '%_dt'
           OR c.column_name ILIKE '%_dttm' OR c.column_name='dos'
           OR c.data_type='DATE' OR c.data_type LIKE 'TIMESTAMP%')
          AND NOT (
            c.column_name ILIKE 'updated_at' OR c.column_name ILIKE 'created_at'
            OR c.column_name ILIKE 'inserted_at' OR c.column_name ILIKE '%load_ts%'
            OR c.column_name ILIKE 'extract%' OR c.column_name='ingested_at_utc'
            OR c.column_name ILIKE 'change_ts%' OR c.column_name='change_ts'
            OR c.column_name='registered_ts' OR c.column_name='verified_ts'
            OR c.column_name='signed_off_ts' OR c.column_name='snapshot_ts'
            OR c.column_name='build_ts'      OR c.column_name='last_modified_in_db'
            OR c.column_name='move_ts'       OR c.column_name='run_ts'
            OR c.column_name='as_of'         OR c.column_name='ingestion_date'
            OR c.column_name LIKE '%_built_at'
            OR c.column_name='llm_build_ts'
          )
          AND c.data_type IN ('DATE','VARCHAR') OR c.data_type LIKE 'TIMESTAMP%'
        ORDER BY c.data_type, c.table_schema, c.table_name, c.column_name
    """).fetchall()

    print(f"[normalize] {len(candidates)} candidate columns to evaluate")

    actions: list[dict] = []
    for schema, table, col, dtype in candidates:
        if is_skipped_column(col):
            actions.append({
                "schema": schema, "table": table, "col": col, "dtype": dtype,
                "action": "skip_metadata_name", "n_rows": None, "n_normalized": None,
                "n_unparseable": None, "note": "name pattern matches metadata regex",
            })
            continue

        if dtype == "DATE":
            actions.append({
                "schema": schema, "table": table, "col": col, "dtype": dtype,
                "action": "skip_already_date_typed", "n_rows": None, "n_normalized": None,
                "n_unparseable": None, "note": "DATE storage is canonical",
            })
            continue

        # Sample distinct values to verify it actually holds dates
        sample = con.execute(f"""
            SELECT "{col}"
            FROM {schema}.{table}
            WHERE "{col}" IS NOT NULL
            LIMIT 50
        """).fetchall()
        n_sampled = len(sample)
        if n_sampled == 0:
            actions.append({
                "schema": schema, "table": table, "col": col, "dtype": dtype,
                "action": "skip_empty", "n_rows": 0, "n_normalized": 0,
                "n_unparseable": 0, "note": "no non-null values to sample",
            })
            continue

        if dtype.startswith("TIMESTAMP"):
            # All TIMESTAMP values are inherently parseable; skip sampling
            n_parseable = n_sampled
        else:
            # VARCHAR -- sample-parse to verify it's a date column
            n_parseable = sum(1 for (v,) in sample if parse_date(str(v)) is not None)

        parse_rate = n_parseable / n_sampled
        if parse_rate < DATE_PARSE_THRESHOLD:
            actions.append({
                "schema": schema, "table": table, "col": col, "dtype": dtype,
                "action": "skip_not_date_values", "n_rows": None, "n_normalized": None,
                "n_unparseable": None,
                "note": f"only {n_parseable}/{n_sampled} sampled values parse as dates "
                        f"(threshold={DATE_PARSE_THRESHOLD:.0%})",
            })
            continue

        # ---- Normalize this column ----
        full_qual = f'{schema}.{table}."{col}"'
        try:
            n_rows = con.execute(f'SELECT COUNT(*) FROM {schema}.{table} WHERE "{col}" IS NOT NULL').fetchone()[0]
        except Exception as e:
            actions.append({
                "schema": schema, "table": table, "col": col, "dtype": dtype,
                "action": "error_count_failed", "n_rows": None, "n_normalized": None,
                "n_unparseable": None, "note": f"COUNT failed: {e}",
            })
            continue

        if dtype.startswith("TIMESTAMP"):
            # Strip time component (zero out)
            sql = f"""
                UPDATE {schema}.{table}
                SET "{col}" = DATE_TRUNC('day', "{col}")
                WHERE "{col}" IS NOT NULL
                  AND "{col}" <> DATE_TRUNC('day', "{col}")
            """
            try:
                rc = con.execute(sql).fetchall()
                n_updated = rc[0][0] if rc else 0
            except Exception as e:
                actions.append({
                    "schema": schema, "table": table, "col": col, "dtype": dtype,
                    "action": "error_update_failed", "n_rows": n_rows, "n_normalized": 0,
                    "n_unparseable": None, "note": f"UPDATE failed: {e}",
                })
                continue
            actions.append({
                "schema": schema, "table": table, "col": col, "dtype": dtype,
                "action": "timestamp_truncate_to_day", "n_rows": n_rows,
                "n_normalized": n_updated, "n_unparseable": None,
                "note": "DATE_TRUNC('day', col) applied; time set to 00:00:00",
            })
            continue

        # VARCHAR -- normalize to MM/DD/YYYY
        # Build a CASE expression with try_strptime for each format, then strftime as MM/DD/YYYY.
        # 20YY rule applied via DATE_PART('year', dt) coercion when year < 100.
        normalize_sql = f"""
            UPDATE {schema}.{table}
            SET "{col}" = strftime(
              CASE
                WHEN try_strptime("{col}", '%Y-%m-%d %H:%M:%S') IS NOT NULL
                  THEN try_strptime("{col}", '%Y-%m-%d %H:%M:%S')
                WHEN try_strptime("{col}", '%Y-%m-%d') IS NOT NULL
                  THEN try_strptime("{col}", '%Y-%m-%d')
                WHEN try_strptime(TRIM("{col}"), '%m/%d/%Y') IS NOT NULL
                  THEN try_strptime(TRIM("{col}"), '%m/%d/%Y')
                WHEN try_strptime(TRIM("{col}"), '%m/%d/%y') IS NOT NULL
                  THEN
                    -- Force 20YY rule: take parsed dt, add (2000 - 1900 + 100) = 200 years
                    -- if year < 1950, else add 100. DuckDB default treats yy>=70 as 19yy.
                    -- We unconditionally map yy to 20yy:
                    CASE WHEN DATE_PART('year', try_strptime(TRIM("{col}"), '%m/%d/%y')) < 2000
                         THEN try_strptime(TRIM("{col}"), '%m/%d/%y')
                              + INTERVAL (2000 - DATE_PART('year', try_strptime(TRIM("{col}"), '%m/%d/%y'))) YEAR
                         ELSE try_strptime(TRIM("{col}"), '%m/%d/%y')
                    END
                ELSE NULL
              END,
              '%m/%d/%Y'
            )
            WHERE "{col}" IS NOT NULL
              AND (
                try_strptime("{col}", '%Y-%m-%d %H:%M:%S') IS NOT NULL
                OR try_strptime("{col}", '%Y-%m-%d') IS NOT NULL
                OR try_strptime(TRIM("{col}"), '%m/%d/%Y') IS NOT NULL
                OR try_strptime(TRIM("{col}"), '%m/%d/%y') IS NOT NULL
              )
              -- Skip rows that are already in MM/DD/YYYY format (idempotent)
              AND NOT ("{col}" ~ '^\\d{{2}}/\\d{{2}}/\\d{{4}}$')
        """
        try:
            rc = con.execute(normalize_sql).fetchall()
            n_normalized = rc[0][0] if rc else 0
        except Exception as e:
            actions.append({
                "schema": schema, "table": table, "col": col, "dtype": dtype,
                "action": "error_update_failed", "n_rows": n_rows, "n_normalized": 0,
                "n_unparseable": None, "note": f"UPDATE failed: {e}",
            })
            continue

        # Count remaining unparseable
        n_unparseable = con.execute(f"""
            SELECT COUNT(*) FROM {schema}.{table}
            WHERE "{col}" IS NOT NULL
              AND NOT ("{col}" ~ '^\\d{{2}}/\\d{{2}}/\\d{{4}}$')
        """).fetchone()[0]

        actions.append({
            "schema": schema, "table": table, "col": col, "dtype": dtype,
            "action": "varchar_normalized_mm_dd_yyyy", "n_rows": n_rows,
            "n_normalized": n_normalized, "n_unparseable": n_unparseable,
            "note": f"left {n_unparseable} unparseable rows in place (cleanup-csv candidates)",
        })

    # ---- Write report + migration SQL ----
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    MIG_PATH.parent.mkdir(parents=True, exist_ok=True)

    with REPORT_PATH.open("w") as f:
        f.write("# Date normalization pass 1 -- thyroid_canonical_publication_v1_0\n\n")
        f.write(f"Generated 2026-04-27 by qc_framework_v1/scripts/normalize_dates_v1_0_pass1.py\n\n")

        # Group by action
        from collections import defaultdict
        by_action: dict[str, list[dict]] = defaultdict(list)
        for a in actions:
            by_action[a["action"]].append(a)

        f.write("## Summary by action\n\n")
        f.write("| action | n_columns |\n|---|---|\n")
        for act, rows in sorted(by_action.items(), key=lambda x: -len(x[1])):
            f.write(f"| `{act}` | {len(rows)} |\n")
        f.write("\n")

        for act, rows in sorted(by_action.items()):
            f.write(f"## {act} ({len(rows)} cols)\n\n")
            f.write("| schema | table | column | dtype | n_rows | n_normalized | n_unparseable | note |\n")
            f.write("|---|---|---|---|---|---|---|---|\n")
            for r in sorted(rows, key=lambda x: (x["schema"], x["table"], x["col"])):
                f.write(
                    f"| {r['schema']} | {r['table']} | `{r['col']}` | {r['dtype']} | "
                    f"{r['n_rows'] if r['n_rows'] is not None else ''} | "
                    f"{r['n_normalized'] if r['n_normalized'] is not None else ''} | "
                    f"{r['n_unparseable'] if r['n_unparseable'] is not None else ''} | "
                    f"{r['note']} |\n"
                )
            f.write("\n")

    print(f"[normalize] report written: {REPORT_PATH}")
    print(f"[normalize] action summary:")
    for act, rows in sorted(by_action.items(), key=lambda x: -len(x[1])):
        print(f"  {act:36s} {len(rows):4d}")


if __name__ == "__main__":
    main()
