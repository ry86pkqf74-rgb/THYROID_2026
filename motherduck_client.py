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


LOCAL_DUCKDB_PATH = os.getenv(
    "LOCAL_DUCKDB_PATH", "thyroid_master_local.duckdb"
)


@dataclass(frozen=True)
class MotherDuckConfig:
    database: str = "thyroid_research_2026"
    token_env_var: str = "MOTHERDUCK_TOKEN"
    share_path: str | None = None
    use_local: bool = False


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
        if self.config.use_local or os.getenv("USE_LOCAL_DUCKDB", "").lower() in ("1", "true", "yes"):
            return duckdb.connect(LOCAL_DUCKDB_PATH)
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
    def query_one(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[Any, ...] | None:
        return con.execute(sql).fetchone()

    @staticmethod
    def query_all(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[Any, ...]]:
        return con.execute(sql).fetchall()


if __name__ == "__main__":
    client = MotherDuckClient()
    with client.connect_rw() as con:
        row = client.query_one(
            con, "SELECT COUNT(DISTINCT research_id) FROM master_cohort"
        )
        if row is None:
            raise RuntimeError("Expected a result from master_cohort count query")
        print(f"master_cohort patients: {row[0]}")
