#!/usr/bin/env python3
"""
MotherDuck client for thyroid research data.

Read/write tokens (staging, attach, promotion, validators)
──────────────────────────────────────────────────────────
1. Service-account token  MD_SA_TOKEN         ← CI / automation (wins when both SA and personal are set)
2. Personal token         MOTHERDUCK_TOKEN    ← interactive development
3. Official alias         motherduck_token    ← same family as MOTHERDUCK_TOKEN
4. Legacy guard           LOCAL_DB_PATH       ← only when value looks like a JWT / ``md_`` PAT
5. Repo-root TOML         ``motherduck.local.toml`` (gitignored) — same RW key order as secrets
6. Secrets file           .streamlit/secrets.toml — same key order as above
7. Repo-root ``.env``     Optional; loaded at import via ``python-dotenv`` (``override=False``) so
   ``MD_READ_SCALING_TOKEN`` / ``MOTHERDUCK_TOKEN`` / etc. can live next to other local env (see ``.env.motherduck.example``).

Read-scaling token (dashboard read-only / Business scale-out)
─────────────────────────────────────────────────────────────
Use **only** ``MD_READ_SCALING_TOKEN`` (alias ``MOTHERDUCK_READ_SCALING_TOKEN``) for
attach-as-read workloads. Never pass this token into ``connect_rw()``, ``connect_md_fail_closed``,
or promotion scripts — those paths require a read/write token above.

Optional: ``MD_READ_SCALING_SESSION_HINT`` (or per-call ``session_hint``) for stable
MotherDuck user-duckling affinity on read-scaling connections.

Streamlit dashboard (opt-in, **default off** for attach):

* ``MOTHERDUCK_DASHBOARD_PREFER_READ_SCALING_TOKEN`` / ``THYROID_DASHBOARD_PREFER_READ_SCALING_TOKEN``
  — try read-scaling token before RW for the RO share.
* ``MOTHERDUCK_DASHBOARD_ALLOW_READ_SCALING_ATTACH`` / ``THYROID_DASHBOARD_ALLOW_READ_SCALING_ATTACH``
  — allow ``connect_read_scaling()`` to the primary DB when share paths fail.

See ``docs/motherduck_read_scaling_dashboard.md``.

Environment selection
─────────────────────
Set MOTHERDUCK_ENV to "dev", "qa", or "prod" (default: "prod").
The matching database name is loaded from config/motherduck_environments.yml.

**Catalog note:** dev / qa / prod map to separate MotherDuck database names by default
(see ``config/motherduck_environments.yml`` and ``docs/motherduck_sandbox_clone_runbook.md``).
Scoped schemas (``main``, ``v2_stage``, ``qa``, ``release_*``) still apply within each DB.

Security
────────
- Never hard-code tokens.
- Service-account tokens must be stored in GitHub Actions secrets
  (or your secret manager) and injected via env vars.
- The RO share path is public metadata; the token authenticates access.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import duckdb

REPO_ROOT = Path(__file__).resolve().parent
# Back-compat alias — some imports expect ``_REPO_ROOT``.
_REPO_ROOT = REPO_ROOT

# Optional repo-root TOML for MotherDuck tokens (same keys as ``.streamlit/secrets.toml``).
# Tests may monkeypatch this path. Not committed — see ``motherduck.local.toml.example``.
LOCAL_MOTHERDUCK_TOML_PATH = REPO_ROOT / "motherduck.local.toml"


def _load_repo_dotenv() -> None:
    """Load repo-root env files when ``python-dotenv`` is available.

    Loads ``.env`` then ``.env.motherduck`` (both optional) with ``override=False`` so
    shell/CI-injected variables and earlier keys stay authoritative.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for name in (".env", ".env.motherduck"):
        env_path = _REPO_ROOT / name
        if env_path.is_file():
            load_dotenv(env_path, override=False)


_load_repo_dotenv()

LOCAL_DUCKDB_PATH = os.getenv(
    "LOCAL_DUCKDB_PATH", "thyroid_master_local.duckdb"
)

# Default environment databases (overridden by environments.yml when present)
_ENV_DATABASES: dict[str, str] = {
    "dev":  "Thyroid 2026 Molecular Dev 20260407",
    "qa":   "Thyroid 2026 Molecular QA 20260407",
    "prod": "Thyroid 2026",
}

_SHARE_PATH_PROD = "md:_share/thyroid_research_ro_v2/2558f066-1c5d-46a5-afbc-800fd5f7568d"

_READ_SCALING_SECRET_KEYS = ("MD_READ_SCALING_TOKEN", "MOTHERDUCK_READ_SCALING_TOKEN")


def _env_truthy(*names: str) -> bool:
    for n in names:
        v = (os.getenv(n) or "").strip().lower()
        if v in ("1", "true", "yes", "on"):
            return True
    return False


def dashboard_prefer_read_scaling_token_for_share() -> bool:
    """Opt-in: try read-scaling token before RW token when attaching the RO share (Streamlit).

    Default False — existing dashboards that rely on the RW token for share ACL keep behavior.
    """
    return _env_truthy(
        "MOTHERDUCK_DASHBOARD_PREFER_READ_SCALING_TOKEN",
        "THYROID_DASHBOARD_PREFER_READ_SCALING_TOKEN",
    )


def dashboard_allow_read_scaling_attach() -> bool:
    """Opt-in: allow ``connect_read_scaling()`` fallback to the primary database catalog.

    Default False — avoids silently attaching readers to the primary DB replica when only
    RO share + RW paths are intended. Enable for Business read-scaling attach workflows.
    """
    return _env_truthy(
        "MOTHERDUCK_DASHBOARD_ALLOW_READ_SCALING_ATTACH",
        "THYROID_DASHBOARD_ALLOW_READ_SCALING_ATTACH",
    )


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


def _load_toml_path(path: Path) -> dict[str, Any]:
    """Load a TOML file. Prefer stdlib ``tomllib`` (Py 3.11+); fall back to PyPI ``toml`` if installed."""
    if not path.is_file():
        return {}
    raw_bytes = path.read_bytes()
    try:
        import tomllib

        data = tomllib.loads(raw_bytes.decode())
        return data if isinstance(data, dict) else {}
    except Exception:
        pass
    try:
        import toml  # type: ignore

        data = toml.loads(raw_bytes.decode())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _local_motherduck_toml_dict() -> dict[str, Any]:
    """Parse ``LOCAL_MOTHERDUCK_TOML_PATH`` when present; ignore parse errors."""
    return _load_toml_path(LOCAL_MOTHERDUCK_TOML_PATH)


def get_token(prefer_service_account: bool = False) -> str | None:
    """Resolve a MotherDuck read/write token.

    Fixed precedence (env, then fallbacks) — matches ``docs/motherduck_database_contract_v1.md`` §8:

      1. ``MD_SA_TOKEN``
      2. ``MOTHERDUCK_TOKEN``
      3. ``motherduck_token`` (env alias)
      4. ``LOCAL_DB_PATH`` when it looks like a JWT / ``md_`` PAT (misconfig guard)
      5. Repo-root ``motherduck.local.toml`` — ``MD_SA_TOKEN``, then ``MOTHERDUCK_TOKEN``, then ``motherduck_token``
      6. ``.streamlit/secrets.toml`` — same key order as (5)

    *prefer_service_account* is ignored (kept for backward-compatible call sites).
    """
    _ = prefer_service_account  # API compatibility only; ordering is always SA → personal → alias.
    sa = (os.getenv("MD_SA_TOKEN") or "").strip()
    if sa:
        return sa
    personal = (os.getenv("MOTHERDUCK_TOKEN") or "").strip()
    if personal:
        return personal
    alias = (os.getenv("motherduck_token") or "").strip()
    if alias:
        return alias
    lp = _jwt_like(os.getenv("LOCAL_DB_PATH"))
    if lp:
        return lp

    local = _local_motherduck_toml_dict()
    for key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
        val = local.get(key)
        if val and str(val).strip():
            return str(val).strip()

    secrets_path = Path(".streamlit") / "secrets.toml"
    data = _load_toml_path(secrets_path)
    for key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
        val = data.get(key)
        if val and str(val).strip():
            return str(val).strip()
    return None


def get_read_scaling_token() -> str | None:
    """Return a MotherDuck **read-scaling** token (Business / dashboard read-only).

    Resolution order:
      1. ``MD_READ_SCALING_TOKEN``
      2. ``MOTHERDUCK_READ_SCALING_TOKEN``
      3. Repo-root ``motherduck.local.toml`` — same keys
      4. ``.streamlit/secrets.toml`` — same keys

    This token is intentionally **not** part of :func:`get_token` so that CI and
    promotion flows never pick it up as a read/write credential.
    """
    for key in _READ_SCALING_SECRET_KEYS:
        v = os.getenv(key)
        if v and str(v).strip():
            return str(v).strip()
    local = _local_motherduck_toml_dict()
    for key in _READ_SCALING_SECRET_KEYS:
        val = local.get(key)
        if val and str(val).strip():
            return str(val).strip()
    secrets_path = Path(".streamlit") / "secrets.toml"
    data = _load_toml_path(secrets_path)
    for key in _READ_SCALING_SECRET_KEYS:
        val = data.get(key)
        if val and str(val).strip():
            return str(val).strip()
    return None


def read_scaling_token_mode() -> str:
    """Label for read-scaling token source (never exposes the secret)."""
    if os.getenv("MD_READ_SCALING_TOKEN"):
        return "env:MD_READ_SCALING_TOKEN"
    if os.getenv("MOTHERDUCK_READ_SCALING_TOKEN"):
        return "env:MOTHERDUCK_READ_SCALING_TOKEN"
    loc = _local_motherduck_toml_dict()
    if loc.get("MD_READ_SCALING_TOKEN"):
        return "motherduck.local.toml:MD_READ_SCALING_TOKEN"
    if loc.get("MOTHERDUCK_READ_SCALING_TOKEN"):
        return "motherduck.local.toml:MOTHERDUCK_READ_SCALING_TOKEN"
    secrets_path = Path(".streamlit") / "secrets.toml"
    data = _load_toml_path(secrets_path)
    if data.get("MD_READ_SCALING_TOKEN"):
        return "secrets.toml:MD_READ_SCALING_TOKEN"
    if data.get("MOTHERDUCK_READ_SCALING_TOKEN"):
        return "secrets.toml:MOTHERDUCK_READ_SCALING_TOKEN"
    return "none"


def is_read_scaling_only_environment() -> bool:
    """True when a read-scaling token is configured but no read/write token is available."""
    if _jwt_like(os.getenv("LOCAL_DB_PATH")):
        return False
    if get_token():
        return False
    return get_read_scaling_token() is not None


def token_mode() -> str:
    """Return a human-readable label describing the active **read/write** token source.

    Read-scaling dashboard tokens (``MD_READ_SCALING_TOKEN`` /
    ``MOTHERDUCK_READ_SCALING_TOKEN``) are **not** considered here — use
    :func:`read_scaling_token_mode` for those. Staging, promotion, validators, and
    :func:`connect_md_fail_closed` require a credential from this RW resolution path.

    Returns one of:
      'env:MD_SA_TOKEN'                – service-account env var
      'env:MOTHERDUCK_TOKEN'           – personal env var
      'env:motherduck_token'           – legacy personal env alias
      'env:LOCAL_DB_PATH'              – JWT-like token carried in LOCAL_DB_PATH
      'motherduck.local.toml:MD_SA_TOKEN' / 'motherduck.local.toml:MOTHERDUCK_TOKEN' – repo-root TOML
      'secrets.toml:MD_SA_TOKEN'       – service-account in Streamlit secrets
      'secrets.toml:MOTHERDUCK_TOKEN'  – personal in Streamlit secrets
      'none'                           – no read/write token found

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
    loc = _local_motherduck_toml_dict()
    if loc.get("MD_SA_TOKEN"):
        return "motherduck.local.toml:MD_SA_TOKEN"
    if loc.get("MOTHERDUCK_TOKEN"):
        return "motherduck.local.toml:MOTHERDUCK_TOKEN"
    if loc.get("motherduck_token"):
        return "motherduck.local.toml:motherduck_token"
    secrets_path = Path(".streamlit") / "secrets.toml"
    data = _load_toml_path(secrets_path)
    if data.get("MD_SA_TOKEN"):
        return "secrets.toml:MD_SA_TOKEN"
    if data.get("MOTHERDUCK_TOKEN"):
        return "secrets.toml:MOTHERDUCK_TOKEN"
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

    def _resolve_session_hint(
        self,
        *,
        session_hint: str | None = None,
        hint_profile: str = "rw",
    ) -> str | None:
        """Pick the first non-empty session hint (routing / duckling affinity).

        Precedence matches :meth:`_apply_session_hint`:

        - explicit *session_hint* argument (per-call wins)
        - for *read_scaling* profile: ``MD_READ_SCALING_SESSION_HINT``,
          ``MOTHERDUCK_READ_SCALING_SESSION_HINT``
        - :attr:`MotherDuckConfig.motherduck_session_hint`
        - ``MOTHERDUCK_SESSION_HINT``

        MotherDuck documents ``session_hint`` as a **connection-string** query
        parameter; we also ``SET motherduck_session_hint`` after connect for
        drivers that only honor the pragma.
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
        return hint or None

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
        hint = self._resolve_session_hint(
            session_hint=session_hint, hint_profile=hint_profile
        )
        if not hint:
            return
        safe = hint.replace("'", "''")
        try:
            con.execute(f"SET motherduck_session_hint='{safe}'")
        except Exception:
            pass  # Older drivers may not support; attribution still has custom_user_agent

    @staticmethod
    def _is_ducklake_attach_conflict(exc: BaseException) -> bool:
        msg = str(exc).lower()
        return "write-write conflict" in msg or "catalog write-write conflict" in msg

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
        hint_for_url = self._resolve_session_hint(
            session_hint=session_hint, hint_profile=hint_profile
        )
        if hint_for_url:
            extra.append(f"session_hint={quote_plus(hint_for_url)}")
        qs = "&".join(extra)
        max_attempts = max(
            1,
            int((os.getenv("MOTHERDUCK_ATTACH_RETRY_ATTEMPTS") or "3").strip() or "3"),
        )
        base_delay = float((os.getenv("MOTHERDUCK_ATTACH_RETRY_DELAY_S") or "0.45").strip() or "0.45")
        for attempt in range(max_attempts):
            con: duckdb.DuckDBPyConnection | None = None
            try:
                if " " in attach:
                    con = duckdb.connect(f"md:?{qs}")
                    con.execute(f'USE "{attach}"')
                else:
                    con = duckdb.connect(f"md:{attach}?{qs}")
                self._apply_session_hint(con, session_hint=session_hint, hint_profile=hint_profile)
                return con
            except Exception as e:
                if con is not None:
                    try:
                        con.close()
                    except Exception:
                        pass
                if attempt < max_attempts - 1 and self._is_ducklake_attach_conflict(e):
                    time.sleep(base_delay * (attempt + 1))
                    continue
                raise
        raise RuntimeError("_connect_md_attached: retry loop exited without connect")

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
        return self._connect_md_attached(token, session_hint=None, hint_profile="rw")

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
        profile = "read_scaling" if token is not None and tok == get_read_scaling_token() else "rw"
        q_tok = quote_plus(tok)
        extra = [f"motherduck_token={q_tok}"]
        ua = self.config.custom_user_agent or os.getenv("MOTHERDUCK_CUSTOM_USER_AGENT")
        if ua:
            extra.append(f"custom_user_agent={quote_plus(ua)}")
        hint_for_url = self._resolve_session_hint(session_hint=None, hint_profile=profile)
        if hint_for_url:
            extra.append(f"session_hint={quote_plus(hint_for_url)}")
        qs = "&".join(extra)
        con = duckdb.connect(f"{self.config.share_path}?{qs}")
        self._apply_session_hint(con, session_hint=None, hint_profile=profile)
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
