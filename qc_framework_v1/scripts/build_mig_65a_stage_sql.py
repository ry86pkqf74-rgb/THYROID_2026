"""
qc_framework_v1/scripts/build_mig_65a_stage_sql.py
==================================================

Builds the SQL chunks needed to stage the FNA-pilot source-side date data
in MotherDuck for Step B mechanical_source_compare against fna_date_raw.

Why a slim staging table: the full _source_long.parquet has 5 raw text
fields (date / specimen / path / history / bethesda) per FNA episode.
For fna_date_raw alone we only need the date field. We'll widen the
staging table when we hit the specimen / path / history / bethesda
column compares.

Output:
  qc_framework_v1/migrations/65a_stage_fna_source_long_date_v1.sql
    CREATE TABLE + N x INSERT VALUES (chunked at CHUNK_ROWS)

  qc_framework_v1/migrations/_chunks/65a_chunk_NN.sql  (one file per chunk)
    Same SQL split into chunk files so query_rw doesn't choke on a
    multi-megabyte single statement.

The full file is the audit record (replayable as one SQL script).
The chunk files are what get sent through query_rw.
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_PARQUET = REPO_ROOT / "verification_csvs" / "canonical_fna_events_v1" / "_source_long.parquet"
MIG_DIR = REPO_ROOT / "qc_framework_v1" / "migrations"
CHUNK_DIR = MIG_DIR / "_chunks"
FULL_SQL_OUT = MIG_DIR / "65a_stage_fna_source_long_date_v1.sql"

CHUNK_ROWS = 500

DEST_TABLE = "manuscript_workspace.fna_source_long_date_v1_step_b"

HEADER = """\
-- =============================================================================
-- Migration 65a -- stage FNA source-side date data for Protocol v2 Step B
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   qc_framework_v1/MASTER_VERIFICATION_PLAN.md (Protocol v2 Step B)
-- Scope:  manuscript_workspace.fna_source_long_date_v1_step_b
--
-- Source: raw/FNAs 12_5_2025.xlsx > sheet 'FNA Bethesda'
-- Extractor: qc_framework_v1/scripts/extract_fna_source_long.py
-- Long-form intermediate: verification_csvs/canonical_fna_events_v1/_source_long.parquet
--
-- This migration creates a slim staging table containing one row per
-- (research_id, fna_index) where the source workbook holds *any* FNA-episode
-- field (date, specimen, path, history, bethesda). It carries only the
-- columns needed for the fna_date_raw mechanical_source_compare:
--
--   research_id      VARCHAR
--   fna_index        INTEGER  (1..12)
--   source_row       INTEGER  (1-based Excel row, header=1, data starts at 2)
--   date_raw         VARCHAR  (raw cell value from the per-FNA Date column)
--
-- The staging table will be widened with the other 4 source fields when
-- specimen_location / pathology_extended / pathology_diagnosis /
-- bethesda_original_text come up in Protocol v2 Step B for this table.
-- The companion source_col / source_workbook / source_sheet / source_col_name
-- attributes are deterministic by (fna_index) and re-derived in the
-- canary-CSV query rather than stored here.
--
-- All writes routed through Cowork's mcp__motherduck__query_rw tool 2026-04-27.
-- This file is the canonical record of the change for replay/audit.
-- =============================================================================
"""

CREATE_SQL = f"""\
-- DROP first to allow safe re-stage in the same session.
DROP TABLE IF EXISTS {DEST_TABLE};

CREATE TABLE {DEST_TABLE} (
  research_id  VARCHAR,
  fna_index    INTEGER,
  source_row   INTEGER,
  date_raw     VARCHAR
);
"""

FOOTER = """\
-- =============================================================================
-- end of migration 65a
-- =============================================================================
"""


def sql_literal(v) -> str:
    """Render a Python value as a DuckDB SQL literal."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "NULL"
    if pd.isna(v):
        return "NULL"
    s = str(v)
    s = s.replace("'", "''")
    return f"'{s}'"


def main() -> None:
    df = pd.read_parquet(SRC_PARQUET, columns=["research_id", "fna_index", "source_row", "date_raw"])
    df = df.sort_values(["research_id", "fna_index"]).reset_index(drop=True)
    n_total = len(df)
    n_chunks = math.ceil(n_total / CHUNK_ROWS)

    MIG_DIR.mkdir(parents=True, exist_ok=True)
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    for old in CHUNK_DIR.glob("65a_chunk_*.sql"):
        old.unlink()

    full_chunks_sql: list[str] = [HEADER, CREATE_SQL]

    # Write CREATE as chunk_00 so the runner can replay in one pass.
    create_chunk_path = CHUNK_DIR / "65a_chunk_00_create.sql"
    create_chunk_path.write_text(CREATE_SQL)

    for ci in range(n_chunks):
        sub = df.iloc[ci * CHUNK_ROWS : (ci + 1) * CHUNK_ROWS]
        rows_sql = []
        for _, row in sub.iterrows():
            rows_sql.append(
                "  ("
                + ", ".join(
                    [
                        sql_literal(row["research_id"]),
                        str(int(row["fna_index"])),
                        str(int(row["source_row"])),
                        sql_literal(row["date_raw"]),
                    ]
                )
                + ")"
            )
        insert_sql = (
            f"-- chunk {ci+1:02d}/{n_chunks:02d} -- rows {ci*CHUNK_ROWS+1}-{ci*CHUNK_ROWS + len(sub)}\n"
            f"INSERT INTO {DEST_TABLE} (research_id, fna_index, source_row, date_raw) VALUES\n"
            + ",\n".join(rows_sql)
            + ";\n"
        )
        chunk_path = CHUNK_DIR / f"65a_chunk_{ci+1:02d}_insert.sql"
        chunk_path.write_text(insert_sql)
        full_chunks_sql.append(insert_sql)

    full_chunks_sql.append(FOOTER)
    FULL_SQL_OUT.write_text("\n".join(full_chunks_sql))

    print(f"[build_mig_65a] rows: {n_total}, chunks: {n_chunks}")
    print(f"[build_mig_65a] full SQL: {FULL_SQL_OUT}  ({FULL_SQL_OUT.stat().st_size:,} bytes)")
    for p in sorted(CHUNK_DIR.glob("65a_chunk_*.sql")):
        print(f"  {p.name}  ({p.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
