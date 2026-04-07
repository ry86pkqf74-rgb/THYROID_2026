"""MotherDuck observability helpers (diagnostics SQL, safe execution, audit rows).

Does not load or print tokens. Queries against ``MD_INFORMATION_SCHEMA`` require
organization-admin / Business-plan access; failures are returned as messages.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

# --- Diagnostics (reference: MotherDuck docs, QUERY_HISTORY / RECENT_QUERIES) ---

SQL_CATALOGS = """
SELECT name, uuid, created_ts, transient, historical_snapshot_retention, type
FROM MD_INFORMATION_SCHEMA.DATABASES
ORDER BY name;
"""

SQL_CURRENT_CONTEXT = """
SELECT current_database() AS database_name, current_schema() AS schema_name;
"""

SQL_SNAPSHOTS_THIS_DB = """
SELECT snapshot_id, snapshot_name, database_name, created_ts, active_bytes, bytes_written
FROM MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS
WHERE database_name = current_database()
ORDER BY created_ts DESC
LIMIT 25;
"""

SQL_QUERY_HISTORY_SAMPLE = """
SELECT query_id, start_time, end_time, total_elapsed_time, error_type, error_message,
       user_agent, session_name, query_type, instance_type,
       substring(query_text, 1, 240 AS len) AS query_text_preview
FROM MD_INFORMATION_SCHEMA.QUERY_HISTORY
ORDER BY start_time DESC
LIMIT 15;
"""

SQL_RECENT_QUERIES_SAMPLE = """
SELECT query_id, start_time, end_time, total_elapsed_time, error_type,
       user_agent, session_name, query_type,
       substring(query_text, 1, 240 AS len) AS query_text_preview
FROM MD_INFORMATION_SCHEMA.RECENT_QUERIES
ORDER BY start_time DESC
LIMIT 15;
"""

# Molecular / lineage / ingest attribution and objects
SQL_RECENT_MOLECULAR = """
SELECT query_id, start_time, end_time, total_elapsed_time, error_type, error_message,
       user_agent, session_name, query_type,
       substring(query_text, 1, 320 AS len) AS query_text_preview
FROM MD_INFORMATION_SCHEMA.QUERY_HISTORY
WHERE start_time >= now() - INTERVAL '14 days'
  AND (
    query_text ILIKE '%molecular%'
    OR query_text ILIKE '%afirma%'
    OR query_text ILIKE '%thyroseq%'
    OR query_text ILIKE '%molecular_code_crosswalk%'
    OR user_agent ILIKE '%THYROID_2026_molecular%'
    OR user_agent ILIKE '%ingest_afirma%'
    OR user_agent ILIKE '%ingest_thyroseq%'
    OR user_agent ILIKE '%molecular_fact_lineage%'
    OR user_agent ILIKE '%molecular_contract%'
  )
ORDER BY start_time DESC
LIMIT 50;
"""


def _fix_substring_syntax(sql: str) -> str:
    """DuckDB substring(text, start, length) — not substring(..., AS len)."""
    return (
        sql.replace("substring(query_text, 1, 240 AS len)", "substring(query_text, 1, 240)")
        .replace("substring(query_text, 1, 320 AS len)", "substring(query_text, 1, 320)")
    )


SQL_QUERY_HISTORY_SAMPLE = _fix_substring_syntax(SQL_QUERY_HISTORY_SAMPLE)
SQL_RECENT_QUERIES_SAMPLE = _fix_substring_syntax(SQL_RECENT_QUERIES_SAMPLE)
SQL_RECENT_MOLECULAR = _fix_substring_syntax(SQL_RECENT_MOLECULAR)


@dataclass
class SafeQueryResult:
    name: str
    ok: bool
    dataframe: pd.DataFrame | None
    error: str | None


def run_safe(con: duckdb.DuckDBPyConnection, name: str, sql: str) -> SafeQueryResult:
    """Execute *sql*; capture failures without raising (e.g. permission / missing view)."""
    try:
        df = con.execute(sql).fetchdf()
        return SafeQueryResult(name=name, ok=True, dataframe=df, error=None)
    except Exception as e:
        return SafeQueryResult(name=name, ok=False, dataframe=None, error=str(e))


def run_diagnostics(con: duckdb.DuckDBPyConnection) -> list[SafeQueryResult]:
    return [
        run_safe(con, "current_context", SQL_CURRENT_CONTEXT),
        run_safe(con, "md_information_schema.databases", SQL_CATALOGS),
        run_safe(con, "database_snapshots (this database)", SQL_SNAPSHOTS_THIS_DB),
        run_safe(con, "query_history_sample", SQL_QUERY_HISTORY_SAMPLE),
        run_safe(con, "recent_queries_sample", SQL_RECENT_QUERIES_SAMPLE),
    ]


def run_recent_molecular(con: duckdb.DuckDBPyConnection) -> SafeQueryResult:
    return run_safe(con, "recent_molecular_queries", SQL_RECENT_MOLECULAR)


def format_console_block(title: str, df: pd.DataFrame | None, err: str | None) -> str:
    lines = [f"=== {title} ==="]
    if err:
        lines.append(f"(skipped: {err})")
    elif df is not None and len(df):
        lines.append(df.to_string(index=False))
    elif df is not None:
        lines.append("(0 rows)")
    else:
        lines.append("(no data)")
    lines.append("")
    return "\n".join(lines)


AUDIT_DDL_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "sql" / "135_molecular_pipeline_audit_ddl.sql"
)


def apply_audit_ddl(con: duckdb.DuckDBPyConnection) -> None:
    if not AUDIT_DDL_PATH.is_file():
        raise FileNotFoundError(str(AUDIT_DDL_PATH))
    con.execute(AUDIT_DDL_PATH.read_text(encoding="utf-8"))


def insert_audit_row(
    con: duckdb.DuckDBPyConnection,
    *,
    pipeline_name: str,
    git_sha: str,
    database_name: str,
    schema_name: str,
    row_counts: dict[str, Any],
    runtime_seconds: float | None,
    validation_status: str,
    custom_user_agent: str | None = None,
    session_hint: str | None = None,
    notes: str | None = None,
) -> str:
    """Insert one governance row into ``qa.molecular_pipeline_run_audit``; returns audit_id."""
    audit_id = str(uuid.uuid4())
    payload = json.dumps(row_counts, separators=(",", ":"), default=str)
    con.execute(
        """
        INSERT INTO qa.molecular_pipeline_run_audit (
            audit_id, pipeline_name, git_sha, database_name, schema_name,
            row_counts_json, runtime_seconds, validation_status,
            custom_user_agent, session_hint, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            audit_id,
            pipeline_name,
            git_sha,
            database_name,
            schema_name,
            payload,
            runtime_seconds,
            validation_status,
            custom_user_agent,
            session_hint,
            notes,
        ],
    )
    return audit_id


def append_audit_markdown(
    repo_root: Path,
    *,
    pipeline_name: str,
    git_sha: str,
    database_name: str,
    schema_name: str,
    row_counts: dict[str, Any],
    runtime_seconds: float | None,
    validation_status: str,
    audit_id: str | None = None,
) -> Path:
    """Append a single markdown row to studies/molecular_pipeline_audit_log.md."""
    log_path = repo_root / "studies" / "molecular_pipeline_audit_log.md"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
    rows_s = json.dumps(row_counts, separators=(",", ":"), default=str)
    line = (
        f"| {ts} | {pipeline_name} | `{git_sha}` | `{database_name}` | `{schema_name}` | "
        f"{runtime_seconds!s} | {validation_status} | `{audit_id or ''}` | {rows_s} |\n"
    )
    if not log_path.exists():
        hdr = (
            "# Molecular pipeline audit log (no secrets)\n\n"
            "| recorded_utc | pipeline | git | database | schema | runtime_s | validation | audit_id | row_counts_json |\n"
            "|---|---|---|---|---|---:|---|---|---|\n"
        )
        log_path.write_text(hdr, encoding="utf-8")
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(line)
    return log_path
