"""Shared Snowflake client with PAT auth workaround for connector v4.4.0."""
import os, json
from pathlib import Path

import snowflake.connector
import snowflake.connector.network as _net


def _load_repo_dotenv_for_pat() -> None:
    """Populate os.environ from repo-root .env (gitignored) if python-dotenv is available."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    repo = Path(__file__).resolve().parents[2]
    load_dotenv(repo / ".env", override=False)


def _ensure_snowflake_pat() -> None:
    """Resolve SNOWFLAKE_PAT from env, ``SNOWFLAKE_PAT_FILE``, or ~/.snowflake/config.toml."""
    if (os.environ.get("SNOWFLAKE_PAT") or "").strip():
        return
    path = (os.environ.get("SNOWFLAKE_PAT_FILE") or "").strip()
    if path:
        fp = Path(path).expanduser()
        if fp.is_file():
            os.environ["SNOWFLAKE_PAT"] = fp.read_text(encoding="utf-8").strip("\n\r\t ")
            return
    cfg = Path.home() / ".snowflake" / "config.toml"
    if cfg.is_file():
        try:
            import tomllib

            data = tomllib.loads(cfg.read_text(encoding="utf-8"))
            pw = (
                data.get("connections", {})
                .get("thyroid_2026", {})
                .get("password")
            )
            if pw:
                os.environ["SNOWFLAKE_PAT"] = str(pw).strip()
        except Exception:
            pass


_load_repo_dotenv_for_pat()
_ensure_snowflake_pat()

DOTTED = "qcc02515.us-east-1"

_orig = _net.SnowflakeRestful._post_request
def _patched(self, url, headers, body, *args, **kwargs):
    if "/session/v1/login-request" in url:
        try:
            d = json.loads(body) if isinstance(body, str) else json.loads(body.decode())
            d["data"]["ACCOUNT_NAME"] = DOTTED
            if not d["data"].get("TOKEN"):
                d["data"]["TOKEN"] = os.environ.get("SNOWFLAKE_PAT") or ""
            body = json.dumps(d)
        except Exception:
            pass
    return _orig(self, url, headers, body, *args, **kwargs)
_net.SnowflakeRestful._post_request = _patched


def get_cursor():
    _ensure_snowflake_pat()
    pat = os.environ.get("SNOWFLAKE_PAT")
    if not pat:
        raise RuntimeError("SNOWFLAKE_PAT is not set")
    ctx = snowflake.connector.connect(
        account="qcc02515", host=f"{DOTTED}.snowflakecomputing.com",
        user="LGLOSSE13", password=pat,
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        warehouse="COMPUTE_WH", database="THYROID_VALIDATION",
        schema="PUBLIC", role="ACCOUNTADMIN")
    return ctx, ctx.cursor()


def deploy_histology_lookup_ssot(cur) -> None:
    """Deploy mig_267 mirror table (idempotent CREATE OR REPLACE)."""
    seed = Path(__file__).resolve().parent.parent / "sql" / "canonical_histology_lookup_v1_seed.sql"
    if not seed.is_file():
        return
    cur.execute(seed.read_text(encoding="utf-8"))


def md_table(rows, cols, max_rows=None):
    """Format result rows as a Markdown table."""
    out = ["| " + " | ".join(str(c) for c in cols) + " |"]
    out.append("| " + " | ".join("---" for _ in cols) + " |")
    n = max_rows if max_rows else len(rows)
    for r in rows[:n]:
        out.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return "\n".join(out) + "\n"
