"""Connect to MotherDuck (md:) or local file DuckDB for script --md flags.

This is the single canonical entry-point for MotherDuck connections.
All scripts with --md should call ``connect_md_or_file`` rather than
rolling their own token resolution from toml / env vars.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from motherduck_client import MotherDuckClient, get_token


def _resolve_md_token() -> str | None:
    """Unified MotherDuck token resolution — single source of truth.

    Priority:
      1. MOTHERDUCK_TOKEN env var
      2. MD_SA_TOKEN env var (service-account / CI)
      3. LOCAL_DB_PATH env var (when it looks like a JWT / md_ PAT)
      4. .streamlit/secrets.toml keys (MOTHERDUCK_TOKEN → MD_SA_TOKEN → LOCAL_DB_PATH)
    """
    token = get_token(prefer_service_account=False)
    if token:
        return token
    token = get_token(prefer_service_account=True)
    if token:
        return token
    return None


def connect_md_or_file(
    db_path: Path,
    *,
    md: bool,
    env: str | None = None,
) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection — MotherDuck when *md* is True, else local file.

    Parameters
    ----------
    db_path : Path
        Fallback local DuckDB file (used when ``md`` is False or connection fails).
    md : bool
        When True, attempt MotherDuck connection before falling back to *db_path*.
    env : str | None
        Optional MotherDuck environment ("dev", "qa", "prod").  Defaults to prod.
    """
    if md:
        token = _resolve_md_token()
        if token:
            try:
                client = MotherDuckClient.for_env(env, use_service_account=False)
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
