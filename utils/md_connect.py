"""Connect to MotherDuck (md:) or local file DuckDB for script --md flags."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from motherduck_client import MotherDuckClient, MotherDuckConfig, get_token


def connect_md_or_file(db_path: Path, *, md: bool) -> duckdb.DuckDBPyConnection:
    if md:
        token = get_token(prefer_service_account=False) or get_token(
            prefer_service_account=True
        )
        if token:
            try:
                client = MotherDuckClient(MotherDuckConfig())
                con = client.connect_rw()
                attach = (
                    os.environ.get("MOTHERDUCK_DATABASE")
                    or os.environ.get("MOTHERDUCK_DB")
                    or ""
                ).strip() or client.config.database
                print(f"  Connected to MotherDuck (md:{attach})")
                return con
            except Exception as e:
                print(f"  MotherDuck unavailable: {e} — using file {db_path}")
                print(
                    "  Hint: set MOTHERDUCK_DATABASE if your DB name differs; "
                    "md:?motherduck_token=… then SHOW DATABASES lists catalogs."
                )
        else:
            print(f"  Using file DB (--md, no MotherDuck token in env): {db_path}")
        return duckdb.connect(str(db_path))

    con = duckdb.connect(str(db_path))
    print(f"  Using local file DB: {db_path}")
    return con
