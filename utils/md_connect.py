"""Connect to MotherDuck (md:) or local file DuckDB for script --md flags.

This is the single canonical entry-point for MotherDuck connections.
All scripts with --md should call ``connect_md_or_file`` rather than
rolling their own token resolution from toml / env vars.

Fail-closed mode
----------------
Pass ``fail_closed=True`` when the script *must* land on MotherDuck and
silently falling back to the local file would be incorrect (e.g. staging
canonical v2 outputs).  With ``fail_closed=True`` the helper runs a
``PRAGMA database_list`` verification after connecting; if no ``md:`` path
appears in the result it prints a clear diagnostic and exits with code 1.

Scripts that only need the fail-closed behaviour can import the convenience
alias ``connect_md_fail_closed`` instead of passing ``fail_closed=True``
every call site.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Repo-safe import: ensure repository root (parent of ``utils/``) is on ``sys.path``
# so ``motherduck_client`` resolves when this module is imported without a prior
# ``sys.path`` tweak (e.g. some pytest entrypoints or ``python -c`` one-liners).
_ROOT = Path(__file__).resolve().parent.parent
_root_s = str(_ROOT)
if _root_s not in sys.path:
    sys.path.insert(0, _root_s)

import duckdb

from motherduck_client import (
    MotherDuckClient,
    ReadScalingTokenForbiddenError,
    get_token,
    is_read_scaling_only_environment,
)


def _resolve_md_token(*, prefer_service_account: bool = False) -> str | None:
    """Resolve a MotherDuck read/write token (single precedence; see ``get_token``)."""
    _ = prefer_service_account  # passed through for API compatibility with callers
    return get_token()


def _verify_md_connection(con: duckdb.DuckDBPyConnection) -> bool:
    """Return True when *con* is genuinely connected to MotherDuck.

    Accepts either the old 'md:' path prefix (older driver versions) or the
    presence of 'md_information_schema' (newer MotherDuck driver which drops
    the 'md:' prefix from PRAGMA database_list output).
    """
    try:
        dbs = con.execute("PRAGMA database_list").fetchall()
        return any("md:" in str(r) or "md_information_schema" in str(r) for r in dbs)
    except Exception:
        return False


def connect_md_or_file(
    db_path: Path,
    *,
    md: bool,
    env: str | None = None,
    fail_closed: bool = False,
    prefer_service_account: bool = False,
    custom_user_agent: str | None = None,
    motherduck_session_hint: str | None = None,
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
    fail_closed : bool
        When True *and* ``md`` is True, verify the connection actually reached
        MotherDuck via ``PRAGMA database_list``.  If the verification fails the
        helper closes the connection and exits with code 1.  Use this for scripts
        that must write to MotherDuck and must never silently fall back to a local
        file.  Has no effect when ``md`` is False.
    prefer_service_account : bool
        Kept for API compatibility with older call sites. Token order is fixed in
        :func:`motherduck_client.get_token` (``MD_SA_TOKEN`` before ``MOTHERDUCK_TOKEN``);
        this flag does not change resolution.
    custom_user_agent / motherduck_session_hint :
        Passed to ``MotherDuckClient`` for query-history attribution.  When None,
        ``MOTHERDUCK_CUSTOM_USER_AGENT`` / ``MOTHERDUCK_SESSION_HINT`` env vars apply.
    """
    if md:
        token = _resolve_md_token(prefer_service_account=prefer_service_account)
        if token:
            try:
                client = MotherDuckClient.for_env(
                    env,
                    use_service_account=prefer_service_account,
                    custom_user_agent=custom_user_agent,
                    motherduck_session_hint=motherduck_session_hint,
                )
                con = client.connect_rw()
                attach = (
                    os.environ.get("MOTHERDUCK_DATABASE")
                    or os.environ.get("MOTHERDUCK_DB")
                    or ""
                ).strip() or client.config.database
                print(f"  Connected to MotherDuck (md:{attach})")
                if fail_closed:
                    if not _verify_md_connection(con):
                        con.close()
                        print("  FATAL: --md requested but PRAGMA database_list shows no md: path.")
                        print("  Connection appeared to succeed but landed on a local file.")
                        print("  Check MOTHERDUCK_DATABASE / MOTHERDUCK_DB env vars.")
                        sys.exit(1)
                    print("  MotherDuck connection verified (fail-closed gate passed)")
                return con
            except ReadScalingTokenForbiddenError as e:
                if fail_closed:
                    print(f"  FATAL: {e}")
                    sys.exit(1)
                raise
            except SystemExit:
                raise
            except Exception as e:
                if fail_closed:
                    print(f"  FATAL: MotherDuck connection failed: {e}")
                    print("  --md with fail_closed=True: refusing to fall back to local file.")
                    print("  Ensure MD_SA_TOKEN or MOTHERDUCK_TOKEN is set and the service is reachable.")
                    sys.exit(1)
                print(f"  MotherDuck unavailable: {e} — using file {db_path}")
                print(
                    "  Hint: set MOTHERDUCK_DATABASE if your DB name differs; "
                    "md:?motherduck_token=… then SHOW DATABASES lists catalogs."
                )
        else:
            if fail_closed:
                if is_read_scaling_only_environment():
                    print(
                        "  FATAL: --md requested but only MD_READ_SCALING_TOKEN (read-only) is set."
                    )
                    print(
                        "  Staging, promotion, and validators require MD_SA_TOKEN or MOTHERDUCK_TOKEN."
                    )
                else:
                    print("  FATAL: --md requested but no MotherDuck RW token resolved.")
                    print(
                        "  Set MD_SA_TOKEN / MOTHERDUCK_TOKEN in env, or motherduck.local.toml / .streamlit/secrets.toml."
                    )
                sys.exit(1)
            print(f"  Using file DB (--md, no MotherDuck token in env): {db_path}")
        return duckdb.connect(str(db_path))

    con = duckdb.connect(str(db_path))
    print(f"  Using local file DB: {db_path}")
    return con


def connect_md_fail_closed(
    db_path: Path,
    *,
    env: str | None = None,
    prefer_service_account: bool = False,
    custom_user_agent: str | None = None,
    motherduck_session_hint: str | None = None,
) -> duckdb.DuckDBPyConnection:
    """Convenience alias: always attempt MotherDuck and exit 1 if unreachable.

    Equivalent to ``connect_md_or_file(db_path, md=True, fail_closed=True)``.
    Use in scripts where landing on a local file when ``--md`` was requested
    would be a silent data-routing error.
    """
    return connect_md_or_file(
        db_path,
        md=True,
        env=env,
        fail_closed=True,
        prefer_service_account=prefer_service_account,
        custom_user_agent=custom_user_agent,
        motherduck_session_hint=motherduck_session_hint,
    )
