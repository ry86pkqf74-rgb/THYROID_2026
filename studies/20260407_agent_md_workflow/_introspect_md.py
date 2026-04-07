#!/usr/bin/env python3
"""One-off introspection for agent workflow — run from repo root."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402
from motherduck_client import get_token  # noqa: E402
from urllib.parse import quote_plus  # noqa: E402
import os  # noqa: E402


def connect_md() -> duckdb.DuckDBPyConnection:
    token = get_token(prefer_service_account=False)
    if not token:
        raise SystemExit("No MotherDuck token")
    qs = f"motherduck_token={quote_plus(token)}"
    ua = os.getenv("MOTHERDUCK_CUSTOM_USER_AGENT", "THYROID_2026_agent_introspect/1.0")
    if ua:
        qs = f"{qs}&custom_user_agent={quote_plus(ua)}"
    return duckdb.connect(f"md:?{qs}")


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


TABLES = (
    "molecular_testing",
    "thyroseq_molecular_enrichment",
    "thyroseq_followup_labs",
    "thyroseq_followup_events",
    "note_entities_genetics",
    "molecular_results",
    "molecular_variant_long",
    "molecular_assay_dictionary",
    "molecular_code_crosswalk",
)

VIEWS = (
    "molecular_results_contract_v",
    "molecular_fact_long_v",
    "canonical_extracted_fact_long_v2",
)


def main() -> None:
    prod = "Thyroid 2026"
    con = connect_md()
    try:
        con.execute(f"USE {quote_ident(prod)}")
        print(f"=== {prod} (current_database={con.execute('SELECT current_database()').fetchone()[0]}) ===\n")
        for name in TABLES:
            row = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_catalog = current_database() AND table_schema = 'main' "
                "AND table_name = ?",
                [name],
            ).fetchone()
            if not row or row[0] == 0:
                print(f"-- {name}: NOT IN main")
                continue
            kind = con.execute(
                "SELECT table_type FROM information_schema.tables "
                "WHERE table_catalog = current_database() AND table_schema = 'main' "
                "AND table_name = ?",
                [name],
            ).fetchone()[0]
            n = con.execute(f"SELECT COUNT(*) FROM {quote_ident('main')}.{quote_ident(name)}").fetchone()[0]
            print(f"-- {name} ({kind}) rows={n:,}")
            cols = con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_catalog = current_database() AND table_schema = 'main' "
                "AND table_name = ? ORDER BY ordinal_position",
                [name],
            ).fetchall()
            for c, t in cols[:40]:
                print(f"     {c}  {t}")
            if len(cols) > 40:
                print(f"     ... +{len(cols) - 40} columns")
            print()
        print("=== views (main) ===\n")
        for name in VIEWS:
            row = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_catalog = current_database() AND table_schema = 'main' "
                "AND table_name = ?",
                [name],
            ).fetchone()
            if not row or row[0] == 0:
                print(f"-- {name}: NOT IN main")
                continue
            print(f"-- {name}: present")
    finally:
        con.close()


if __name__ == "__main__":
    main()
