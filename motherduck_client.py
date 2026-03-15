#!/usr/bin/env python3
"""
MotherDuck client for thyroid research data.

Authentication hierarchy
────────────────────────
1. Service-account token  MD_SA_TOKEN      ← automated pipelines / CI
2. Personal token         MOTHERDUCK_TOKEN ← interactive development
3. Secrets file           .streamlit/secrets.toml (dashboard only)

Environment selection
─────────────────────
Set MOTHERDUCK_ENV to "dev", "qa", or "prod" (default: "prod").
The matching database name is loaded from config/motherduck_environments.yml.

Security
────────
- Never hard-code tokens.
- Service-account tokens must be stored in GitHub Actions secrets
  (or your secret manager) and injected via env vars.
- The RO share path is public metadata; the token authenticates access.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb


LOCAL_DUCKDB_PATH = os.getenv(
    "LOCAL_DUCKDB_PATH", "thyroid_master_local.duckdb"
)

# Default environment databases (overridden by environments.yml when present)
_ENV_DATABASES: dict[str, str] = {
    "dev":  "thyroid_research_2026_dev",
    "qa":   "thyroid_research_2026_qa",
    "prod": "thyroid_research_2026",
}

_SHARE_PATH_PROD = "md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c"


def _load_env_databases() -> dict[str, str]:
    """Load database names from config/motherduck_environments.yml if available."""
    cfg_path = Path(__file__).resolve().parent / "config" / "motherduck_environments.yml"
    if not cfg_path.exists():
        return _ENV_DATABASES.copy()
    try:
        import yaml  # type: ignore
        with cfg_path.open() as fh:
            data = yaml.safe_load(fh)
        envs = data.get("environments", {})
        return {
            k: envs[k]["database"]
            for k in ("dev", "qa", "prod")
            if k in envs and "database" in envs[k]
        }
    except Exception:
        return _ENV_DATABASES.copy()


def resolve_database_for_env(env: str | None = None) -> str:
    """Return the MotherDuck database name for the given environment.

    Falls back to MOTHERDUCK_ENV env var, then defaults to 'prod'.
    """
    env = (env or os.getenv("MOTHERDUCK_ENV", "prod")).lower().strip()
    return _load_env_databases().get(env, _ENV_DATABASES["prod"])


def get_token(prefer_service_account: bool = False) -> str | None:
    """Resolve a MotherDuck token.

    Priority:
      1. MD_SA_TOKEN          – service-account token for CI/automation
      2. MOTHERDUCK_TOKEN     – personal developer token
      3. .streamlit/secrets.toml (MOTHERDUCK_TOKEN key)

    Set prefer_service_account=True in automated scripts; leave False
    (default) for interactive sessions that should use the personal token.
    """
    if prefer_service_account:
        sa = os.getenv("MD_SA_TOKEN")
        if sa:
            return sa
    token = os.getenv("MOTHERDUCK_TOKEN")
    if token:
        return token
    # Streamlit secrets fallback (dashboard / Streamlit Cloud)
    secrets_path = Path(".streamlit") / "secrets.toml"
    if secrets_path.exists():
        try:
            import toml  # type: ignore
            return toml.load(str(secrets_path)).get("MOTHERDUCK_TOKEN")
        except Exception:
            pass
    return None


@dataclass(frozen=True)
class MotherDuckConfig:
    database: str = "thyroid_research_2026"
    token_env_var: str = "MOTHERDUCK_TOKEN"
    share_path: str | None = None
    use_local: bool = False
    # When True, prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN (for CI/automation)
    use_service_account: bool = False


class MotherDuckClient:
    def __init__(self, config: MotherDuckConfig | None = None) -> None:
        self.config = config or MotherDuckConfig()

    # ── Token resolution ──────────────────────────────────────────────────

    def _require_token(self) -> str:
        token = get_token(prefer_service_account=self.config.use_service_account)
        if not token:
            raise RuntimeError(
                "No MotherDuck token found. Set MD_SA_TOKEN (automation) or "
                "MOTHERDUCK_TOKEN (interactive) before connecting."
            )
        return token

    # ── Connection helpers ────────────────────────────────────────────────

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

    # ── Environment-aware factory ─────────────────────────────────────────

    @classmethod
    def for_env(
        cls,
        env: str | None = None,
        *,
        use_service_account: bool = False,
    ) -> "MotherDuckClient":
        """Return a client configured for the target environment.

        Usage::

            # Interactive development (dev DB, personal token)
            client = MotherDuckClient.for_env("dev")

            # CI promotion gate (prod DB, service-account token)
            client = MotherDuckClient.for_env("prod", use_service_account=True)
        """
        db = resolve_database_for_env(env)
        share = _SHARE_PATH_PROD if (env or "prod").lower() == "prod" else None
        cfg = MotherDuckConfig(
            database=db,
            share_path=share,
            use_service_account=use_service_account,
        )
        return cls(cfg)

    # ── Query helpers ─────────────────────────────────────────────────────

    @staticmethod
    def query_one(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[Any, ...] | None:
        return con.execute(sql).fetchone()

    @staticmethod
    def query_all(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[Any, ...]]:
        return con.execute(sql).fetchall()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Quick MotherDuck connectivity check")
    parser.add_argument("--env", default="prod", choices=["dev", "qa", "prod"])
    parser.add_argument("--sa", action="store_true", help="Use service-account token")
    args = parser.parse_args()

    client = MotherDuckClient.for_env(args.env, use_service_account=args.sa)
    con = client.connect_rw()
    row = client.query_one(con, "SELECT COUNT(DISTINCT research_id) FROM master_cohort")
    if row is None:
        raise RuntimeError("Expected a result from master_cohort count query")
    print(f"[{args.env}] master_cohort patients: {row[0]:,}")
    con.close()
