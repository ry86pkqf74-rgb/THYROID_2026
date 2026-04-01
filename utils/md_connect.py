"""Connect to MotherDuck (md:) or local file DuckDB for script --md flags."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

# Default catalog name (override if your MotherDuck database uses another name).
_DEFAULT_MD_DB = "thyroid_master"


def _md_database_name() -> str:
    return (
        os.environ.get("MOTHERDUCK_DATABASE")
        or os.environ.get("MOTHERDUCK_DB")
        or _DEFAULT_MD_DB
    ).strip()


def connect_md_or_file(db_path: Path, *, md: bool) -> duckdb.DuckDBPyConnection:
    if md:
        token = os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")
        if token:
            md_db = _md_database_name()
            try:
                con = duckdb.connect(f"md:{md_db}?motherduck_token={token}")
                print(f"  Connected to MotherDuck (md:{md_db})")
                return con
            except Exception as e:
                print(f"  MotherDuck unavailable: {e} — using file {db_path}")
                print(
                    "  Hint: set MOTHERDUCK_DATABASE to your MotherDuck DB name, or run "
                    "`duckdb.connect('md:?motherduck_token=...')` then `SHOW DATABASES` "
                    "to list catalogs."
                )
        else:
            print(f"  Using file DB (--md, no MOTHERDUCK_TOKEN): {db_path}")
        return duckdb.connect(str(db_path))

    con = duckdb.connect(str(db_path))
    print(f"  Using local file DB: {db_path}")
    return con
