#!/usr/bin/env python3
"""
MotherDuck Auth Diagnostic
==========================
Lightweight probe that reports the active token mode, reachable databases /
shares, and basic connectivity without ever printing secrets.

Usage
-----
    # Default — prefers SA token if available
    .venv/bin/python scripts/99_motherduck_auth_diagnostic.py

    # Force personal-token precedence
    .venv/bin/python scripts/99_motherduck_auth_diagnostic.py --personal

    # Only check the RO share
    .venv/bin/python scripts/99_motherduck_auth_diagnostic.py --ro-only

Exit codes
----------
    0  All requested probes passed.
    1  At least one probe failed.
    2  No token could be resolved at all.
"""
from __future__ import annotations

import argparse
import sys
import textwrap
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure the repo root is on sys.path so motherduck_client is importable
# even when invoked as  python scripts/99_motherduck_auth_diagnostic.py
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROD_DB = "thyroid_research_2026"
RO_SHARE = "md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c"
CANARY_TABLE = "patient_analysis_resolved_v1"   # must exist in prod
CANARY_RO_TABLE = "path_synoptics"               # must exist in RO share


def _banner(msg: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {msg}")
    print(f"{'─' * 60}")


def _ok(label: str, detail: str = "") -> None:
    line = f"  ✅ {label}"
    if detail:
        line += f"  ({detail})"
    print(line)


def _fail(label: str, detail: str = "") -> None:
    line = f"  ❌ {label}"
    if detail:
        line += f"  ({detail})"
    print(line)


# ---------------------------------------------------------------------------
# Probes
# ---------------------------------------------------------------------------
def probe_token(prefer_sa: bool) -> str | None:
    """Resolve a token and report mode. Returns the token or None."""
    _banner("Token Resolution")
    tok = get_token(prefer_service_account=prefer_sa)
    mode = token_mode(prefer_service_account=prefer_sa)
    if tok:
        masked = tok[:4] + "…" + tok[-4:] if len(tok) > 12 else "****"
        _ok(f"Token resolved via {mode}", f"masked: {masked}")
    else:
        _fail("No token found in any source")
        print(textwrap.indent(
            "Checked: MD_SA_TOKEN, MOTHERDUCK_TOKEN env vars, "
            ".streamlit/secrets.toml keys MD_SA_TOKEN & MOTHERDUCK_TOKEN.",
            "         ",
        ))
    return tok


def probe_prod(token: str) -> bool:
    """Connect to prod and run a canary query."""
    import duckdb

    _banner(f"Prod Database — {PROD_DB}")
    try:
        t0 = time.time()
        con = duckdb.connect(f"md:{PROD_DB}?motherduck_token={token}")
        cur_db = con.execute("SELECT current_database()").fetchone()[0]
        elapsed = time.time() - t0
        _ok(f"Connected in {elapsed:.2f}s", f"current_database = {cur_db}")
    except Exception as exc:
        _fail("Connection failed", str(exc)[:120])
        return False

    # Canary table
    try:
        row = con.execute(
            f"SELECT COUNT(*) FROM {CANARY_TABLE}"
        ).fetchone()
        _ok(f"Canary table '{CANARY_TABLE}'", f"{row[0]:,} rows")
    except Exception as exc:
        _fail(f"Canary table '{CANARY_TABLE}' inaccessible", str(exc)[:120])
        con.close()
        return False

    # List databases visible
    try:
        dbs = [
            r[0]
            for r in con.execute(
                "SELECT DISTINCT database_name FROM information_schema.schemata"
            ).fetchall()
        ]
        _ok("Visible databases", ", ".join(sorted(dbs)))
    except Exception:
        pass  # non-fatal

    con.close()
    return True


def probe_ro_share(token: str) -> bool:
    """Attach the RO share and check a canary table."""
    import duckdb

    _banner("Read-Only Share")
    try:
        t0 = time.time()
        con = duckdb.connect(f"md:?motherduck_token={token}")
        con.execute(f"ATTACH '{RO_SHARE}' AS thyroid_ro (READ_ONLY)")
        elapsed = time.time() - t0
        _ok(f"Attached RO share in {elapsed:.2f}s")
    except Exception as exc:
        _fail("RO share ATTACH failed", str(exc)[:120])
        return False

    try:
        row = con.execute(
            f"SELECT COUNT(*) FROM thyroid_ro.main.{CANARY_RO_TABLE}"
        ).fetchone()
        _ok(f"Canary RO table '{CANARY_RO_TABLE}'", f"{row[0]:,} rows")
    except Exception as exc:
        _fail(f"Canary RO table '{CANARY_RO_TABLE}' inaccessible", str(exc)[:120])
        con.close()
        return False

    con.close()
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="MotherDuck auth diagnostic — never prints secrets."
    )
    parser.add_argument(
        "--personal",
        action="store_true",
        help="Prefer personal token (MOTHERDUCK_TOKEN) over SA.",
    )
    parser.add_argument(
        "--ro-only",
        action="store_true",
        help="Only probe the RO share, skip prod RW check.",
    )
    args = parser.parse_args()

    prefer_sa = not args.personal

    print("MotherDuck Auth Diagnostic")
    print(f"  prefer_service_account = {prefer_sa}")

    tok = probe_token(prefer_sa)
    if not tok:
        return 2

    ok = True
    if not args.ro_only:
        if not probe_prod(tok):
            ok = False
    if not probe_ro_share(tok):
        ok = False

    _banner("Summary")
    if ok:
        _ok("All probes passed")
    else:
        _fail("One or more probes failed — review output above")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
