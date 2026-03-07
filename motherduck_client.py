#!/usr/bin/env python3
"""
Simple MotherDuck client for thyroid research data.

Security:
- Never hard-code tokens.
- Set MOTHERDUCK_TOKEN in your shell or .env loader.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import duckdb


@dataclass(frozen=True)
class MotherDuckConfig:
    database: str = "thyroid_research_2026"
    token_env_var: str = "MOTHERDUCK_TOKEN"
    # Set this for collaborator read-only usage, e.g.:
    # md:_share/thyroid_research_ro/<share-uuid>
    share_path: str | None = None


class MotherDuckClient:
    def __init__(self, config: MotherDuckConfig | None = None) -> None:
        self.config = config or MotherDuckConfig()

    def _require_token(self) -> str:
        token = os.getenv(self.config.token_env_var)
        if not token:
            raise RuntimeError(
                f"Missing {self.config.token_env_var}. "
                "Export your MotherDuck token before connecting."
            )
        return token

    def connect_rw(self) -> duckdb.DuckDBPyConnection:
        token = self._require_token()
        return duckdb.connect(f"md:{self.config.database}?motherduck_token={token}")

    def connect_ro_share(self) -> duckdb.DuckDBPyConnection:
        token = self._require_token()
        if not self.config.share_path:
            raise RuntimeError(
                "share_path is not configured. Set MotherDuckConfig.share_path "
                "to your read-only share URL path."
            )
        return duckdb.connect(f"{self.config.share_path}?motherduck_token={token}")

    @staticmethod
    def query_one(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[Any, ...]:
        return con.execute(sql).fetchone()

    @staticmethod
    def query_all(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[Any, ...]]:
        return con.execute(sql).fetchall()


if __name__ == "__main__":
    client = MotherDuckClient()
    with client.connect_rw() as con:
        n_patients = client.query_one(
            con, "SELECT COUNT(DISTINCT research_id) FROM master_cohort"
        )[0]
        print(f"master_cohort patients: {n_patients}")
