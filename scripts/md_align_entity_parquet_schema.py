#!/usr/bin/env python3
"""Verify (and optionally fix) MotherDuck ``main`` entity / run telemetry column parity.

Compares ``main.note_entities*`` tables to ``ENTITY_SCHEMA_COLUMNS`` and
``main.note_extraction_runs`` to ``NOTE_EXTRACTION_RUNS_EXPECTED_COLUMNS``.
With ``--apply``, adds missing columns via ``ALTER TABLE ... ADD COLUMN`` so
existing promoted tables tolerate new parquet fields without a full reload.

Loads ``.env.motherduck`` when present (same pattern as promotion gate).

Usage:
  python3 scripts/md_align_entity_parquet_schema.py --md
  python3 scripts/md_align_entity_parquet_schema.py --md --apply
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

_env_md = ROOT / ".env.motherduck"
if _env_md.exists():
    try:
        from dotenv import load_dotenv

        load_dotenv(_env_md, override=False)
    except ImportError:
        for line in _env_md.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from llm_extraction.run_telemetry import (  # noqa: E402
    NOTE_EXTRACTION_RUNS_EXPECTED_COLUMNS,
    note_extraction_runs_sql_type,
)
from llm_extraction.vocab import ENTITY_SCHEMA_COLUMNS, ENTITY_SCHEMA_DTYPES  # noqa: E402
from utils.md_connect import connect_md_or_file  # noqa: E402

DEFAULT_DB_PATH = ROOT / "thyroid_master.duckdb"

# New transport/metadata fields only — for MotherDuck tables that are not full ENTITY_SCHEMA-shaped
# (e.g. legacy wide/json v2 loads) so we do not add dozens of empty entity columns.
MINIMAL_LLM_PROVENANCE_COLUMNS: frozenset[str] = frozenset({
    "entity_domain",
    "llm_provider",
    "llm_base_url",
    "llm_sdk",
    "llm_sdk_version",
    "provider_returned_model",
    "provider_system_fingerprint",
})


def _entity_sql_type(col: str) -> str:
    dt = ENTITY_SCHEMA_DTYPES.get(col, "string")
    if dt == "Int64":
        return "BIGINT"
    if dt == "float64":
        return "DOUBLE"
    return "VARCHAR"


def _table_columns(con, table: str) -> set[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM duckdb_columns()
        WHERE database_name = current_database()
          AND schema_name = 'main'
          AND table_name = ?
        ORDER BY column_index
        """,
        [table],
    ).fetchall()
    return {str(r[0]) for r in rows}


def _main_tables(con) -> list[str]:
    rows = con.execute(
        """
        SELECT DISTINCT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_type = 'BASE TABLE'
          AND (
            table_name LIKE 'note_entities%'
            OR table_name = 'note_extraction_runs'
          )
        ORDER BY table_name
        """
    ).fetchall()
    return [str(r[0]) for r in rows]


def _alignment_want_columns(table: str, have: set[str]) -> set[str]:
    """Return the column names we expect on *table* given existing *have* columns."""
    if table == "note_extraction_runs":
        return set(NOTE_EXTRACTION_RUNS_EXPECTED_COLUMNS)
    shaped = (
        {"entity_type", "entity_value_norm"}.issubset(have)
        or ("extraction_method" in have and "entity_value_raw" in have)
    )
    if shaped:
        return set(ENTITY_SCHEMA_COLUMNS)
    if table.startswith("note_entities"):
        return set(MINIMAL_LLM_PROVENANCE_COLUMNS)
    return set()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--md", action="store_true", help="Connect to MotherDuck (fail-closed).")
    p.add_argument(
        "--apply",
        action="store_true",
        help="Run ALTER TABLE ADD COLUMN for each missing column (nullable).",
    )
    p.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Local fallback DB path.")
    args = p.parse_args()

    if not args.md:
        print("  This utility is MotherDuck-only. Pass --md.")
        sys.exit(2)

    con = connect_md_or_file(Path(args.db_path), md=True, fail_closed=True)
    try:
        tables = _main_tables(con)
        if not tables:
            print("  No matching main.note_entities* / note_extraction_runs tables found.")
            sys.exit(0)

        total_missing = 0
        alters: list[str] = []

        for tbl in tables:
            have = _table_columns(con, tbl)
            want = _alignment_want_columns(tbl, have)
            if not want:
                print(f"  {tbl}: skip (unknown alignment target)")
                continue
            missing = sorted(want - have)
            if tbl == "note_extraction_runs":
                for col in missing:
                    sql_t = note_extraction_runs_sql_type(col)
                    alters.append(
                        f'ALTER TABLE main."{tbl}" ADD COLUMN "{col}" {sql_t}'
                    )
            else:
                for col in missing:
                    sql_t = _entity_sql_type(col)
                    alters.append(
                        f'ALTER TABLE main."{tbl}" ADD COLUMN "{col}" {sql_t}'
                    )
            if missing:
                if tbl == "note_extraction_runs":
                    mode = "telemetry"
                elif want == MINIMAL_LLM_PROVENANCE_COLUMNS:
                    mode = "provenance-only"
                else:
                    mode = "full entity schema"
                print(f"  {tbl}: missing {len(missing)} column(s) [{mode}]: {missing}")
                total_missing += len(missing)
            else:
                print(f"  {tbl}: OK ({len(have)} columns)")

        if not alters:
            print(f"\n  All {len(tables)} table(s) aligned with repo schema.")
            return

        print(f"\n  Total missing column definitions: {total_missing}")
        if not args.apply:
            print("  Dry-run only. Re-run with --apply to execute ALTERs.")
            return

        for stmt in alters:
            con.execute(stmt + ";")
        print(f"  Applied {len(alters)} ALTER statement(s).")
    finally:
        con.close()


if __name__ == "__main__":
    main()
