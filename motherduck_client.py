#!/usr/bin/env python3
"""
MotherDuck client for thyroid research data.

Read/write tokens (staging, attach, promotion, validators)
──────────────────────────────────────────────────────────
1. Service-account token  MD_SA_TOKEN         ← CI / automation (``prefer_service_account=True``)
2. Personal token         MOTHERDUCK_TOKEN    ← interactive development
3. Official alias         motherduck_token   ← same as MOTHERDUCK_TOKEN where supported
4. Secrets file           .streamlit/secrets.toml — ``MD_SA_TOKEN`` / ``MOTHERDUCK_TOKEN``

Read-scaling token (dashboard read-only / Business scale-out)
─────────────────────────────────────────────────────────────
Use **only** ``MD_READ_SCALING_TOKEN`` (alias ``MOTHERDUCK_READ_SCALING_TOKEN``) for
attach-as-read workloads. Never pass this token into ``connect_rw()``, ``connect_md_fail_closed``,
or promotion scripts — those paths require a read/write token above.

Optional: ``MD_READ_SCALING_SESSION_HINT`` (or per-call ``session_hint``) for stable
MotherDuck user-duckling affinity on read-scaling connections.

Environment selection
─────────────────────
Set MOTHERDUCK_ENV to "dev", "qa", or "prod" (default: "prod").
The matching database name is loaded from config/motherduck_environments.yml.

**Catalog note:** dev / qa / prod map to a single MotherDuck database name today; logical
isolation is by schema (see ``docs/motherduck_database_contract_v1.md``).

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

_READ_SCALING_SECRET_KEYS = ("MD_READ_SCALING_TOKEN", "MOTHERDUCK_READ_SCALING_TOKEN")


class ReadScalingTokenForbiddenError(RuntimeError):
    """Raised when a read-scaling-only environment is used for read/write MotherDuck paths."""


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
    env_name = env if env is not None else os.getenv("MOTHERDUCK_ENV", "prod")
    env_key = str(env_name or "prod").lower().strip()
    return _load_env_databases().get(env_key, _ENV_DATABASES["prod"])


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


def get_read_scaling_token() -> str | None:
    """Return a MotherDuck **read-scaling** token (Business / dashboard read-only).

    Resolution order:
      1. ``MD_READ_SCALING_TOKEN``
      2. ``MOTHERDUCK_READ_SCALING_TOKEN``
      3. ``.streamlit/secrets.toml`` — same keys

    This token is intentionally **not** part of :func:`get_token` so that CI and
    promotion flows never pick it up as a read/write credential.
    """
    for key in _READ_SCALING_SECRET_KEYS:
        v = os.getenv(key)
        if v and str(v).strip():
            return str(v).strip()
    secrets_path = Path(".streamlit") / "secrets.toml"
    if secrets_path.exists():
        try:
            import toml  # type: ignore
            data = toml.load(str(secrets_path))
            for key in _READ_SCALING_SECRET_KEYS:
                val = data.get(key)
                if val and str(val).strip():
                    return str(val).strip()
        except Exception:
            pass
    return None


def read_scaling_token_mode() -> str:
    """Label for read-scaling token source (never exposes the secret)."""
    if os.getenv("MD_READ_SCALING_TOKEN"):
        return "env:MD_READ_SCALING_TOKEN"
    if os.getenv("MOTHERDUCK_READ_SCALING_TOKEN"):
        return "env:MOTHERDUCK_READ_SCALING_TOKEN"
    secrets_path = Path(".streamlit") / "secrets.toml"
    if secrets_path.exists():
        try:
            import toml  # type: ignore
            data = toml.load(str(secrets_path))
            if data.get("MD_READ_SCALING_TOKEN"):
                return "secrets.toml:MD_READ_SCALING_TOKEN"
            if data.get("MOTHERDUCK_READ_SCALING_TOKEN"):
                return "secrets.toml:MOTHERDUCK_READ_SCALING_TOKEN"
        except Exception:
            pass
    return "none"


def is_read_scaling_only_environment() -> bool:
    """True when a read-scaling token is configured but no read/write token is available."""
    if _jwt_like(os.getenv("LOCAL_DB_PATH")):
        return False
    if get_token(prefer_service_account=False) or get_token(prefer_service_account=True):
        return False
    return get_read_scaling_token() is not None


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

    def _apply_session_hint(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        session_hint: str | None = None,
        hint_profile: str = "rw",
    ) -> None:
        """Set ``motherduck_session_hint`` when a value is available.

        *hint_profile* ``read_scaling`` inserts ``MD_READ_SCALING_SESSION_HINT`` /
        ``MOTHERDUCK_READ_SCALING_SESSION_HINT`` before the generic ``MOTHERDUCK_SESSION_HINT``.
        """
        hints: list[str] = []
        if session_hint and str(session_hint).strip():
            hints.append(str(session_hint).strip())
        if hint_profile == "read_scaling":
            hints.append((os.getenv("MD_READ_SCALING_SESSION_HINT") or "").strip())
            hints.append((os.getenv("MOTHERDUCK_READ_SCALING_SESSION_HINT") or "").strip())
        if self.config.motherduck_session_hint:
            hints.append(str(self.config.motherduck_session_hint).strip())
        hints.append((os.getenv("MOTHERDUCK_SESSION_HINT") or "").strip())
        hint = next((h for h in hints if h), "")
        if not hint:
            return
        safe = hint.replace("'", "''")
        try:
            con.execute(f"SET motherduck_session_hint='{safe}'")
        except Exception:
            pass  # Older drivers may not support; attribution still has custom_user_agent

    def _connect_md_attached(
        self,
        token: str,
        *,
        session_hint: str | None = None,
        hint_profile: str = "rw",
    ) -> duckdb.DuckDBPyConnection:
        """Open MotherDuck with an explicit token (read/write or read-scaling)."""
        db = (os.environ.get("MOTHERDUCK_DATABASE") or os.environ.get("MOTHERDUCK_DB") or "").strip()
        attach = db or self.config.database
        q_tok = quote_plus(token)
        extra = [f"motherduck_token={q_tok}"]
        ua = self.config.custom_user_agent or os.getenv("MOTHERDUCK_CUSTOM_USER_AGENT")
        if ua:
            extra.append(f"custom_user_agent={quote_plus(ua)}")
        qs = "&".join(extra)
        if " " in attach:
            con = duckdb.connect(f"md:?{qs}")
            con.execute(f'USE "{attach}"')
            self._apply_session_hint(con, session_hint=session_hint, hint_profile=hint_profile)
            return con
        con = duckdb.connect(f"md:{attach}?{qs}")
        self._apply_session_hint(con, session_hint=session_hint, hint_profile=hint_profile)
        return con

    # ── Connection helpers ────────────────────────────────────────────────

    def connect_rw(self) -> duckdb.DuckDBPyConnection:
        if self.config.use_local or os.getenv("USE_LOCAL_DUCKDB", "").lower() in ("1", "true", "yes"):
            return duckdb.connect(LOCAL_DUCKDB_PATH)
        if is_read_scaling_only_environment():
            raise ReadScalingTokenForbiddenError(
                "MotherDuck read/write connection refused: only read-scaling credentials are set "
                "(MD_READ_SCALING_TOKEN / MOTHERDUCK_READ_SCALING_TOKEN). "
                "Use MOTHERDUCK_TOKEN or MD_SA_TOKEN for staging, promotion, validators, and "
                "attach/write paths. For dashboard-only reads, use connect_read_scaling()."
            )
        token = self._require_token()
        return self._connect_md_attached(
            token,
            session_hint=self.config.motherduck_session_hint,
            hint_profile="rw",
        )

    def connect_read_scaling(
        self,
        *,
        session_hint: str | None = None,
    ) -> duckdb.DuckDBPyConnection:
        """Connect to MotherDuck with a **read-scaling** token only.

        Use for analyst dashboards and read replicas — never for promotion or staging writes.
        """
        if self.config.use_local or os.getenv("USE_LOCAL_DUCKDB", "").lower() in ("1", "true", "yes"):
            raise RuntimeError("connect_read_scaling does not support USE_LOCAL_DUCKDB / local file mode.")
        token = get_read_scaling_token()
        if not token:
            raise RuntimeError(
                "No read-scaling MotherDuck token. Set MD_READ_SCALING_TOKEN (or "
                "MOTHERDUCK_READ_SCALING_TOKEN), optionally with MD_READ_SCALING_SESSION_HINT."
            )
        call_hint = session_hint if session_hint is not None else self.config.motherduck_session_hint
        return self._connect_md_attached(token, session_hint=call_hint, hint_profile="read_scaling")

    def connect_ro_share(self, *, token: str | None = None) -> duckdb.DuckDBPyConnection:
        """Attach the configured read-only share path.

        When *token* is None, uses :func:`get_token` (read/write identity). Pass an explicit
        *token* to authenticate with a read-scaling token if the share ACL allows it.
        """
        tok = token if token is not None else self._require_token()
        if not self.config.share_path:
            raise RuntimeError(
                "share_path is not configured. Set MotherDuckConfig.share_path "
                "to your read-only share URL path."
            )
        q_tok = quote_plus(tok)
        con = duckdb.connect(f"{self.config.share_path}?motherduck_token={q_tok}")
        profile = "read_scaling" if token is not None and tok == get_read_scaling_token() else "rw"
        self._apply_session_hint(con, session_hint=self.config.motherduck_session_hint, hint_profile=profile)
        return con

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
