"""
Shared helpers for the round-2 LLM-extraction integration scripts
(369 / 382 / 383 / 384 / 385 / 386).

Purpose
-------
Encapsulate the commonly-needed plumbing — logging, gate failures,
MotherDuck connection, registry / __readme upserts — so the per-script
ETL bodies stay focused on their domain SQL.

PHI rule
--------
NEVER pass clinical note text, evidence_text, result_json, or source_*
column values into log() — the buffer is flushed to ``scripts/output/<NNN>_run.log``
and committed.  Counts, RIDs, entity types, and gate names only.

DuckDB notes
------------
* TIMESTAMP with TZ trap: every build_ts column uses
  ``CAST(CURRENT_TIMESTAMP AS TIMESTAMP)`` to avoid TIMESTAMPTZ→pytz pull-in.
* ``research_id`` is VARCHAR on canonical_patient_master; cast on every join.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
REGISTRY_SCHEMA = "manuscript_workspace"
REGISTRY_TABLE = "detail_table_registry_v1"
README_TABLE = "__readme"


class GateFailure(SystemExit):
    """Raised on a gate failure; subclass of SystemExit so caller exits non-zero."""

    def __init__(self, msg: str) -> None:
        super().__init__(1)
        self.msg = msg


class RunLogger:
    """Buffered logger that writes once at flush time (atomic file ops)."""

    def __init__(self, log_path: Path) -> None:
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._buf: list[str] = []

    def log(self, msg: str) -> None:
        line = f"[{datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]}Z] {msg}"
        print(line, flush=True)
        self._buf.append(line)

    def flush(self) -> None:
        mode = "a" if self.log_path.exists() else "w"
        with self.log_path.open(mode) as fh:
            fh.write("\n".join(self._buf) + "\n")
        self._buf.clear()

    def gate(self, cond: bool, msg: str) -> None:
        if not cond:
            self.log(f"  GATE FAILED: {msg}")
            self.flush()
            sys.exit(1)
        self.log(f"  gate OK: {msg}")


def connect_md(logger: RunLogger) -> Any:
    """Open a MotherDuck connection using the canonical resolution order."""
    import duckdb

    from motherduck_client import get_token, token_mode

    token = get_token()
    mode = token_mode()
    logger.log(f"  Connecting to MotherDuck ({mode}) …")
    con = duckdb.connect(f"md:?motherduck_token={token}")
    con.execute(f"USE {CANONICAL_DB}")
    logger.log(f"  Connected. DB: {CANONICAL_DB}")
    return con


def table_exists(con: Any, schema: str, table: str) -> bool:
    return bool(
        con.execute(
            """SELECT 1 FROM information_schema.tables
               WHERE table_catalog=? AND table_schema=? AND table_name=? LIMIT 1""",
            [CANONICAL_DB, schema, table],
        ).fetchone()
    )


def column_exists(con: Any, schema: str, table: str, column: str) -> bool:
    return bool(
        con.execute(
            """SELECT 1 FROM information_schema.columns
               WHERE table_catalog=? AND table_schema=? AND table_name=? AND column_name=? LIMIT 1""",
            [CANONICAL_DB, schema, table, column],
        ).fetchone()
    )


def add_cpm_columns_if_missing(
    con: Any,
    logger: RunLogger,
    cols: dict[str, str],
) -> None:
    """ALTER canonical_patient_master to add 4-col Tier-2 nlp_<dom>_* cols if missing."""
    for col, decl in cols.items():
        if not column_exists(con, "main", "canonical_patient_master", col):
            con.execute(f"ALTER TABLE main.canonical_patient_master ADD COLUMN {col} {decl}")
            logger.log(f"  added CPM column: {col} {decl}")
        else:
            logger.log(f"  CPM column already present (preserved): {col}")


def archive_table_if_present(
    con: Any,
    logger: RunLogger,
    src_schema: str,
    src_table: str,
    archive_table: str,
) -> int | None:
    """CTAS the source table into archive_pub_v1_0 if it exists; return row count or None."""
    if not table_exists(con, src_schema, src_table):
        logger.log(f"  no pre-existing {src_schema}.{src_table}; archive skipped")
        return None
    cur_rows = con.execute(f"SELECT COUNT(*) FROM {src_schema}.{src_table}").fetchone()[0]
    logger.log(
        f"  archiving {cur_rows:,} rows {src_schema}.{src_table} -> "
        f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{archive_table}'
    )
    con.execute(
        f'CREATE OR REPLACE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}."{archive_table}" AS '
        f"SELECT * FROM {src_schema}.{src_table}"
    )
    archived = con.execute(
        f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}."{archive_table}"'
    ).fetchone()[0]
    logger.gate(archived == cur_rows, f"archive row-count parity ({archived:,})")
    return archived


def upsert_registry(
    con: Any,
    logger: RunLogger,
    *,
    detail_table_name: str,
    schema_name: str,
    join_key: str,
    grain: str,
    total_rows: int,
    total_patients: int,
    domain: str,
    feeds_master_columns: str,
    description: str,
    canonical_version: str,
    feeds_master_columns_array: list[str] | None = None,
) -> None:
    """Idempotent upsert into manuscript_workspace.detail_table_registry_v1.

    Probes the registry's actual columns and writes only what's present in
    its current schema (per project rule: probe before INSERT).
    """
    cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=? AND table_name=?",
            [REGISTRY_SCHEMA, REGISTRY_TABLE],
        ).fetchall()
    }
    payload: dict[str, Any] = {
        "detail_table_name": detail_table_name,
        "schema_name": schema_name,
        "join_key": join_key,
        "grain": grain,
        "total_rows": total_rows,
        "total_patients": total_patients,
        "domain": domain,
        "feeds_master_columns": feeds_master_columns,
        "description": description,
        "canonical_version": canonical_version,
    }
    if "feeds_master_columns_array" in cols:
        payload["feeds_master_columns_array"] = feeds_master_columns_array or []
    if "needs_manual_review" in cols:
        payload["needs_manual_review"] = False
    use_cols = [c for c in payload.keys() if c in cols]
    placeholders = ",".join("?" for _ in use_cols)
    values = [payload[c] for c in use_cols]

    con.execute(
        f"DELETE FROM {REGISTRY_SCHEMA}.{REGISTRY_TABLE} WHERE detail_table_name=?",
        [detail_table_name],
    )
    con.execute(
        f"INSERT INTO {REGISTRY_SCHEMA}.{REGISTRY_TABLE} ({','.join(use_cols)}) "
        f"VALUES ({placeholders})",
        values,
    )
    logger.log(
        f"  registry upsert: {detail_table_name} (rows={total_rows:,}, patients={total_patients:,})"
    )


def append_readme(con: Any, logger: RunLogger, *, script: str, content: str) -> None:
    """Append a single new row to main.__readme using its actual 4-col schema.

    Schema (probed): content VARCHAR, updated_at TIMESTAMP, git_sha VARCHAR, script VARCHAR.
    """
    con.execute(
        f"INSERT INTO main.{README_TABLE} (content, updated_at, git_sha, script) "
        f"VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), NULL, ?)",
        [content, script],
    )
    logger.log(f"  __readme append: script={script}")


def assert_unchanged(
    con: Any,
    logger: RunLogger,
    *,
    schema: str,
    table: str,
    expected_rows: int,
    label: str,
) -> None:
    actual = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
    logger.gate(
        actual == expected_rows,
        f"{label}: {schema}.{table} row count == {expected_rows:,} (got {actual:,})",
    )


CPM_EXPECTED_ROWS = 10_871


def assert_cpm_intact(con: Any, logger: RunLogger) -> None:
    rows = con.execute("SELECT COUNT(*) FROM main.canonical_patient_master").fetchone()[0]
    logger.gate(rows == CPM_EXPECTED_ROWS, f"CPM row count == {CPM_EXPECTED_ROWS:,} (got {rows:,})")
