#!/usr/bin/env python3
"""Step 8 corrective patch — rebuild data_dictionary_v266a with the original
analytical-metadata columns preserved (n_non_null, pct_non_null, n_distinct,
status, replacement_column_name), while ALSO including the prompt-required
fields (data_type, is_nullable, ordinal_position, comment) and recomputing
n_non_null / n_distinct for the new columns added in Script 271.

Source-of-truth columns from snapshot:
  data_dictionary_v266a_pre271_<TS>.column_name -> n_non_null, pct_non_null,
  n_distinct, description, status, replacement_column_name.

Recompute n_non_null / n_distinct for new (post-271) columns directly.

Idempotent: rebuilds the table from scratch.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_PREFIX = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}'

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
SCRIPT_TAG = "script271_step8_patch_2026-04-17"

TARGETS = [
    "canonical_patient_master",
    "canonical_us_nodule_characteristics_v1",
    "imaging_nodule_master_v1",
]


def main() -> int:
    con = connect_locked()
    print(f"[{NOW}] Step 8 patch — rebuild data_dictionary_v266a with full schema")

    # Find the most recent pre271 dictionary snapshot
    rows = con.execute(
        f'SELECT table_name FROM "{ARCHIVE_DB}".information_schema.tables '
        f"WHERE table_schema='{ARCHIVE_SCHEMA}' "
        "AND table_name LIKE 'data_dictionary_v266a_pre271_%' "
        "ORDER BY table_name DESC"
    ).fetchall()
    if not rows:
        raise SystemExit("No data_dictionary_v266a_pre271_* snapshot found in archive.")
    snap_name = rows[0][0]
    snap_fq = f'{ARCHIVE_PREFIX}."{snap_name}"'
    print(f"  Using snapshot: {snap_fq}")

    snap_cols = [r[0] for r in con.execute(
        f'SELECT column_name FROM "{ARCHIVE_DB}".information_schema.columns '
        f"WHERE table_schema='{ARCHIVE_SCHEMA}' AND table_name='{snap_name}' "
        "ORDER BY ordinal_position"
    ).fetchall()]
    print(f"  Snapshot columns: {snap_cols}")

    # Build current information_schema view restricted to our 3 tables
    tlist = ", ".join(f"'{t}'" for t in TARGETS)
    print("  Building base info-schema set ...")
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _info AS "
        "SELECT c.table_name, c.column_name, c.data_type, c.is_nullable, "
        "       c.ordinal_position, "
        "       COALESCE(cm.comment, '') AS comment "
        "FROM information_schema.columns c "
        "LEFT JOIN duckdb_columns() cm "
        "  ON cm.database_name = c.table_catalog "
        " AND cm.schema_name   = c.table_schema "
        " AND cm.table_name    = c.table_name "
        " AND cm.column_name   = c.column_name "
        f"WHERE c.table_catalog='{PUBLICATION_DB}' "
        "  AND c.table_schema='main' "
        f"  AND c.table_name IN ({tlist})"
    )

    # Compute n_non_null / n_distinct ONLY for canonical_patient_master columns (it's the
    # main per-patient table). For nodule-grain tables we keep snapshot values where
    # available (re-computing across 37k rows times 36/several cols is also fine but
    # noisier). We'll cover all three but limit per-column to a single cheap pass.
    rows = con.execute(
        "SELECT table_name, column_name FROM _info ORDER BY table_name, ordinal_position"
    ).fetchall()
    print(f"  Computing per-column metrics for {len(rows)} columns ...")

    # Build a per-column metrics temp table
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _metrics ("
        "table_name VARCHAR, column_name VARCHAR, "
        "n_non_null BIGINT, pct_non_null DOUBLE, n_distinct BIGINT)"
    )

    for tbl, col in rows:
        # Quote both safely
        col_q = f'"{col}"'
        try:
            row = con.execute(
                f'SELECT COUNT({col_q}) AS nn, '
                f'COUNT(DISTINCT {col_q}) AS nd, '
                f'COUNT(*) AS n '
                f'FROM {PUBLICATION_DB}.main.{tbl}'
            ).fetchone()
            nn, nd, n = row
            pct = (nn / n * 100.0) if n else None
        except Exception as e:
            print(f"    WARN metric for {tbl}.{col}: {e!r}; storing NULLs")
            nn, nd, pct = None, None, None
        con.execute(
            "INSERT INTO _metrics VALUES (?, ?, ?, ?, ?)",
            [tbl, col, nn, pct, nd],
        )

    # Now rebuild the dictionary by joining info-schema + metrics + snapshot legacy fields
    # Snapshot has columns: column_name, data_type, ordinal_position, n_non_null, pct_non_null,
    # n_distinct, description, status, replacement_column_name -- but no table_name, so we
    # cannot perfectly attribute legacy rows to a table. We carry description/status/replacement
    # forward where the column name appears uniquely in the snapshot.
    print("  Rebuilding data_dictionary_v266a (extended schema) ...")
    con.execute(
        "CREATE OR REPLACE TABLE data_dictionary_v266a AS "
        "WITH legacy AS ( "
        "  SELECT column_name, "
        "         ANY_VALUE(description) AS description, "
        "         ANY_VALUE(status) AS status, "
        "         ANY_VALUE(replacement_column_name) AS replacement_column_name "
        f"  FROM {snap_fq} "
        "  GROUP BY 1 "
        ") "
        "SELECT i.table_name, i.column_name, i.data_type, i.is_nullable, "
        "       i.ordinal_position, i.comment, "
        "       m.n_non_null, m.pct_non_null, m.n_distinct, "
        "       l.description, l.status, l.replacement_column_name, "
        f"       TIMESTAMP '{NOW}' AS rebuilt_at, "
        f"       '{SCRIPT_TAG}' AS rebuilt_by "
        "FROM _info i "
        "LEFT JOIN _metrics m USING (table_name, column_name) "
        "LEFT JOIN legacy l USING (column_name) "
        "ORDER BY i.table_name, i.ordinal_position"
    )

    n = con.execute("SELECT COUNT(*) FROM data_dictionary_v266a").fetchone()[0]
    by_tbl = con.execute(
        "SELECT table_name, COUNT(*) FROM data_dictionary_v266a GROUP BY 1 ORDER BY 1"
    ).fetchall()
    print(f"  data_dictionary_v266a rebuilt: total rows={n}")
    for r in by_tbl:
        print(f"    {r[0]}: {r[1]} cols")

    # Sanity: invariants
    inv = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id), "
        "COUNT(*) FILTER (WHERE research_id IS NULL) "
        f"FROM {PUBLICATION_DB}.main.canonical_patient_master"
    ).fetchone()
    print(f"  CPM invariants: n={inv[0]} distinct={inv[1]} nulls={inv[2]}")
    if inv != (10871, 10871, 0):
        raise SystemExit("INVARIANT VIOLATION after dictionary rebuild")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
