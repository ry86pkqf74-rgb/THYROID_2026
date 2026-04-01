"""Connect to MotherDuck (md:) or local file DuckDB for script --md flags."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

# Matches scripts/29_validation_engine.py and dashboard MotherDuck usage.
_MD_DB = "thyroid_master"


def connect_md_or_file(db_path: Path, *, md: bool) -> duckdb.DuckDBPyConnection:
    if md:
        token = os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")
        if token:
            try:
                con = duckdb.connect(f"md:{_MD_DB}?motherduck_token={token}")
                print(f"  Connected to MotherDuck (md:{_MD_DB})")
                return con
            except Exception as e:
                print(f"  MotherDuck unavailable: {e} — using file {db_path}")
        else:
            print(f"  Using file DB (--md, no MOTHERDUCK_TOKEN): {db_path}")
        return duckdb.connect(str(db_path))

    con = duckdb.connect(str(db_path))
    print(f"  Using local file DB: {db_path}")
    return con
