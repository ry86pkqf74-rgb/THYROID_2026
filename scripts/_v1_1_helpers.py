"""
Shared helpers for the v1_1 finalization scripts (252-259).

Provides:
  - timestamp + logging helpers
  - snapshot_table(): copy table to "Thyroid 2026 UPdated".archive_pub_v1_0
    with COMMENT ON TABLE explaining provenance
  - ensure_audit_table(): create manuscript_workspace.v1_1_finalization_audit_v1
    if missing
  - record_audit(): persist before/after counts for a given finding

Conventions:
  - Archive DB / schema: "Thyroid 2026 UPdated".archive_pub_v1_0
  - Snapshot suffix: _pre<scriptnum>_<UTC YYYYMMDDTHHMMSSZ>
  - All cross-table joins use TRY_CAST(research_id AS INTEGER)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_QUALIFIED = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'

PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
AUDIT_TABLE = "manuscript_workspace.v1_1_finalization_audit_v1"


def utc_ts() -> str:
    """Return UTC timestamp in YYYYMMDDTHHMMSSZ format (snapshot suffix)."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_short() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.") + \
        f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


def make_logger(log_path: Path):
    """Return (log_fn, file_handle). file_handle.close() at end."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fh = log_path.open("a", encoding="utf-8")

    def log(msg: str) -> None:
        line = f"[{utc_short()}] {msg}"
        print(line, flush=True)
        fh.write(line + "\n")
        fh.flush()
    return log, fh


def ensure_archive_schema(con) -> None:
    """Ensure archive_pub_v1_0 schema exists in the archive DB."""
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"')


def snapshot_table(con, source_qualified: str, dest_name: str,
                   script_tag: str, reason: str) -> str:
    """
    Snapshot a fully-qualified source table to archive_pub_v1_0 with the
    given dest_name. Adds a COMMENT ON TABLE describing the snapshot.

    Returns the fully-qualified destination identifier.
    """
    ensure_archive_schema(con)
    dest_full = f'{ARCHIVE_QUALIFIED}."{dest_name}"'
    con.execute(f"DROP TABLE IF EXISTS {dest_full}")
    con.execute(f"CREATE TABLE {dest_full} AS SELECT * FROM {source_qualified}")
    comment = (
        f"{script_tag} pre-mutation snapshot of {source_qualified}. "
        f"Reason: {reason}. "
        f"Created at {datetime.now(timezone.utc).isoformat()}."
    )
    safe = comment.replace("'", "''")
    con.execute(f"COMMENT ON TABLE {dest_full} IS '{safe}'")
    return dest_full


def ensure_audit_table(con) -> None:
    """Create manuscript_workspace.v1_1_finalization_audit_v1 if missing."""
    con.execute("CREATE SCHEMA IF NOT EXISTS manuscript_workspace")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
            run_ts TIMESTAMP,
            script_num VARCHAR,
            finding_id VARCHAR,
            metric VARCHAR,
            count_before BIGINT,
            count_after BIGINT,
            target_after BIGINT,
            status VARCHAR,
            notes VARCHAR
        )
    """)


def record_audit(con, script_num: str, finding_id: str, metric: str,
                 count_before: int, count_after: int,
                 target_after: int = 0, status: str = "OK",
                 notes: str = "") -> None:
    """Insert a row into the v1_1 finalization audit table."""
    ensure_audit_table(con)
    con.execute(
        f"""INSERT INTO {AUDIT_TABLE}
            (run_ts, script_num, finding_id, metric,
             count_before, count_after, target_after, status, notes)
            VALUES (?,?,?,?,?,?,?,?,?)""",
        [datetime.now(timezone.utc), script_num, finding_id, metric,
         int(count_before) if count_before is not None else None,
         int(count_after) if count_after is not None else None,
         int(target_after) if target_after is not None else None,
         status, notes],
    )


def write_decision_log(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
