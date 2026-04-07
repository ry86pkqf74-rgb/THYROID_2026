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
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import duckdb


LOCAL_DUCKDB_PATH = os.getenv(
    "LOCAL_DUCKDB_PATH", "thyroid_master_local.duckdb"
)

# Default environment databases (overridden by environments.yml when present)
_ENV_DATABASES: dict[str, str] = {
    "dev":  "Thyroid 2026",
    "qa":   "Thyroid 2026",
    "prod": "Thyroid 2026",
}

_SHARE_PATH_PROD = "md:_share/thyroid_research_ro_v2/2558f066-1c5d-46a5-afbc-800fd5f7568d"


def _jwt_like(value: str | None) -> str | None:
    """Treat LOCAL_DB_PATH / similar as a token when it looks like a JWT or md_ PAT."""
    if not value:
        return None
    v = value.strip()
    if v.startswith("eyJ") or v.startswith("md_"):
        return v
    return None


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

    If MOTHERDUCK_DATABASE or MOTHERDUCK_DB is set, it wins (single-tenant override).
    Otherwise falls back to MOTHERDUCK_ENV / prod mapping.
    """
    override = (os.environ.get("MOTHERDUCK_DATABASE") or os.environ.get("MOTHERDUCK_DB") or "").strip()
    if override:
        return override
    env = (env or os.getenv("MOTHERDUCK_ENV", "prod")).lower().strip()
    return _load_env_databases().get(env, _ENV_DATABASES["prod"])


def get_token(prefer_service_account: bool = False) -> str | None:
    """Resolve a MotherDuck token.

    Priority (when *prefer_service_account* is True):
      1. MD_SA_TOKEN              – service-account / team token
      2. MOTHERDUCK_TOKEN         – personal developer token
      3. .streamlit/secrets.toml  – MD_SA_TOKEN key, then MOTHERDUCK_TOKEN key

    Priority (when *prefer_service_account* is False — the default):
      1. MOTHERDUCK_TOKEN         – personal developer token
      2. MD_SA_TOKEN              – service-account fallback
      3. .streamlit/secrets.toml  – MOTHERDUCK_TOKEN key, then MD_SA_TOKEN key

    Set prefer_service_account=True in CI / automated scripts; leave False
    for interactive development that should use the personal token.
    """
    if prefer_service_account:
        sa = os.getenv("MD_SA_TOKEN")
        if sa:
            return sa
        lp = _jwt_like(os.getenv("LOCAL_DB_PATH"))
        if lp:
            return lp
        personal = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")
        if personal:
            return personal
    else:
        personal = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")
        if personal:
            return personal
        sa = os.getenv("MD_SA_TOKEN")
        if sa:
            return sa
        lp = _jwt_like(os.getenv("LOCAL_DB_PATH"))
        if lp:
            return lp

    # Streamlit secrets fallback (dashboard / Streamlit Cloud)
    secrets_path = Path(".streamlit") / "secrets.toml"
    if secrets_path.exists():
        try:
            import toml  # type: ignore
            data = toml.load(str(secrets_path))
            if prefer_service_account:
                return data.get("MD_SA_TOKEN") or data.get("MOTHERDUCK_TOKEN")
            return data.get("MOTHERDUCK_TOKEN") or data.get("MD_SA_TOKEN")
        except Exception:
            pass
    return None


def token_mode() -> str:
    """Return a human-readable label describing the active token source.

    Returns one of:
      'env:MD_SA_TOKEN'                – service-account env var
      'env:MOTHERDUCK_TOKEN'           – personal env var
      'secrets.toml:MD_SA_TOKEN'       – service-account in Streamlit secrets
      'secrets.toml:MOTHERDUCK_TOKEN'  – personal in Streamlit secrets
      'none'                           – no token found

    Never exposes the token value itself.
    """
    if os.getenv("MD_SA_TOKEN"):
        return "env:MD_SA_TOKEN"
    if os.getenv("MOTHERDUCK_TOKEN"):
        return "env:MOTHERDUCK_TOKEN"
    if os.getenv("motherduck_token"):
        return "env:motherduck_token"
    if _jwt_like(os.getenv("LOCAL_DB_PATH")):
        return "env:LOCAL_DB_PATH"
    secrets_path = Path(".streamlit") / "secrets.toml"
    if secrets_path.exists():
        try:
            import toml  # type: ignore
            data = toml.load(str(secrets_path))
            if data.get("MD_SA_TOKEN"):
                return "secrets.toml:MD_SA_TOKEN"
            if data.get("MOTHERDUCK_TOKEN"):
                return "secrets.toml:MOTHERDUCK_TOKEN"
        except Exception:
            pass
    return "none"


@dataclass(frozen=True)
class MotherDuckConfig:
    database: str = "Thyroid 2026"
    token_env_var: str = "MOTHERDUCK_TOKEN"
    share_path: str | None = None
    use_local: bool = False
    # When True, prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN (for CI/automation)
    use_service_account: bool = False
    # MotherDuck / DuckDB connection attribution (query history, integrations)
    custom_user_agent: str | None = None
    motherduck_session_hint: str | None = None


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
        db = (os.environ.get("MOTHERDUCK_DATABASE") or os.environ.get("MOTHERDUCK_DB") or "").strip()
        attach = db or self.config.database
        # Connection string query (MotherDuck: motherduck_token, custom_user_agent)
        q_tok = quote_plus(token)
        extra = [f"motherduck_token={q_tok}"]
        ua = self.config.custom_user_agent or os.getenv("MOTHERDUCK_CUSTOM_USER_AGENT")
        if ua:
            extra.append(f"custom_user_agent={quote_plus(ua)}")
        qs = "&".join(extra)

        def _apply_session_hint(con: duckdb.DuckDBPyConnection) -> None:
            hint = self.config.motherduck_session_hint or (
                os.getenv("MOTHERDUCK_SESSION_HINT") or ""
            ).strip()
            if not hint:
                return
            safe = hint.replace("'", "''")
            try:
                con.execute(f"SET motherduck_session_hint='{safe}'")
            except Exception:
                pass  # Older drivers may not support; attribution still has custom_user_agent

        if " " in attach:
            con = duckdb.connect(f"md:?{qs}")
            con.execute(f'USE "{attach}"')
            _apply_session_hint(con)
            return con
        con = duckdb.connect(f"md:{attach}?{qs}")
        _apply_session_hint(con)
        return con

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
        custom_user_agent: str | None = None,
        motherduck_session_hint: str | None = None,
    ) -> "MotherDuckClient":
        """Return a client configured for the target environment.

        Usage::

            # Interactive development (dev DB, personal token)
            client = MotherDuckClient.for_env("dev")

            # CI promotion gate (prod DB, service-account token)
            client = MotherDuckClient.for_env("prod", use_service_account=True)
        """
        db = resolve_database_for_env(env)
        share = _SHARE_PATH_PROD if (_SHARE_PATH_PROD and (env or "prod").lower() == "prod") else None
        cfg = MotherDuckConfig(
            database=db,
            share_path=share,
            use_service_account=use_service_account,
            custom_user_agent=custom_user_agent,
            motherduck_session_hint=motherduck_session_hint,
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
    db = con.execute("SELECT current_database()").fetchone()
    print(f"[{args.env}] database: {db[0] if db else '?'}")
    tables = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog = current_database() AND table_schema = 'main'"
    ).fetchone()
    print(f"[{args.env}] main schema tables: {tables[0] if tables else 0}")
    con.close()
