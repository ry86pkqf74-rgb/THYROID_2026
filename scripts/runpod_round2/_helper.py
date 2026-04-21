"""Shared helper for the 2026-04-21 RunPod qwen2.5-32b extraction round.

Exposes:
  * connect(): returns an authenticated DuckDB/MotherDuck connection
  * audit_parquet(): reads an output parquet and returns {rows, rids, dup_nrids}
  * archive_current_table(): snapshots an existing MD table to
    archive_pub_v1_0 before overwrite
  * load_parquet_to_md(): CREATE OR REPLACE the target MD table from the
    parquet, synthesizing provenance columns to match sibling (282-285)
    schema (23 cols total)
  * parity_check_md_vs_parquet(): confirm MD rowcount == parquet rowcount
    and note_row_id sets match
  * run_rollup_sql(): execute an UPDATE statement against CPM and report
    rowcount
  * snapshot_cpm(): take a pre-mutation CPM snapshot to archive_pub_v1_0
  * summary_json(): write a run summary JSON under scripts/output/
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token  # noqa: E402

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Provenance defaults (match vasc/airway/frozensec/parathyroid sibling schemas).
SYNTH_LLM_PROVIDER = "vllm"
SYNTH_LLM_SDK = "openai"
SYNTH_PROVIDER_RETURNED_MODEL = "qwen2.5-32b"
SYNTH_LLM_BASE_URL = "https://pmza5juk7ru2xl-8000.proxy.runpod.net/v1"
SYNTH_LLM_MODEL = "qwen2.5-32b"

# 23 columns, same order as Script 282/285.
LOADED_COLUMNS = [
    "note_row_id",
    "domain",
    "llm_model",
    "llm_base_url",
    "extracted_at",
    "result_json",
    "research_id",
    "note_type",
    "note_date",
    "linkage_date",
    "source_workbook",
    "source_sheet",
    "source_column",
    "note_index",
    "preprocess_batch_id",
    "preprocessed_at_utc",
    "preprocess_script_version",
    "entity_domain",
    "llm_provider",
    "llm_sdk",
    "llm_sdk_version",
    "provider_returned_model",
    "provider_system_fingerprint",
]


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utcnow_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def log(msg: str) -> None:
    print(f"[{utcnow_iso()}] {msg}", flush=True)


def connect() -> duckdb.DuckDBPyConnection:
    """Connect to MotherDuck with the canonical publication DB attached."""
    tok = get_token()
    if not tok:
        raise SystemExit("No MotherDuck token available; check motherduck.local.toml")
    log(f"connecting to MotherDuck '{CANONICAL_DB}'")
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")
    return con


def attach_archive(con: duckdb.DuckDBPyConnection) -> None:
    """Attach the archive DB (literal name has a space)."""
    con.execute(f'ATTACH IF NOT EXISTS \'md:{ARCHIVE_DB}\' AS "{ARCHIVE_DB}";')


def audit_parquet(parquet_path: Path) -> dict[str, Any]:
    """Return row count, rid count, dup note_row_id groups, and a stable hash."""
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
    ).fetchone()[0]
    rids = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM read_parquet('{parquet_path}')"
    ).fetchone()[0]
    dup = con.execute(
        f"""
        SELECT COUNT(*) FROM (
          SELECT note_row_id FROM read_parquet('{parquet_path}')
           GROUP BY note_row_id HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    # MD5 over concatenation of note_row_id | research_id (sorted).
    nrid_rid_hash_row = con.execute(
        f"""
        SELECT md5(string_agg(concat_ws('|', note_row_id, research_id), '||' ORDER BY note_row_id))
          FROM read_parquet('{parquet_path}')
        """
    ).fetchone()
    nrid_rid_hash = nrid_rid_hash_row[0] if nrid_rid_hash_row else None
    return {
        "parquet_path": str(parquet_path),
        "rows": int(rows),
        "rids": int(rids),
        "dup_note_row_id_groups": int(dup),
        "nrid_rid_hash": nrid_rid_hash,
        "audited_at_utc": utcnow_iso(),
    }


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
         WHERE table_catalog=? AND table_schema=? AND table_name=?
        """,
        [CANONICAL_DB, schema, table],
    ).fetchone()
    return row is not None


def archive_current_table(
    con: duckdb.DuckDBPyConnection, source_table: str
) -> str | None:
    """Snapshot main.<source_table> to archive DB. Returns the archive name,
    or None if the source table does not exist."""
    if not table_exists(con, "main", source_table):
        log(f"  archive: main.{source_table} does not exist — skipping")
        return None
    attach_archive(con)
    archive_name = f"{source_table}_qwen3_snapshot_{utcnow_compact()}"
    con.execute(
        f"""
        CREATE OR REPLACE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{archive_name} AS
        SELECT * FROM {CANONICAL_DB}.main.{source_table};
        """
    )
    n_rows = con.execute(
        f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{archive_name}'
    ).fetchone()[0]
    log(f"  archive: main.{source_table} -> {archive_name} ({n_rows:,} rows)")
    return archive_name


def load_parquet_to_md(
    con: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    target_table: str,
    entity_domain: str,
) -> int:
    """CREATE OR REPLACE main.<target_table> from parquet, synthesizing
    provenance columns to match sibling (Script 282-285) 23-col schema."""
    pq = str(parquet_path)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE main.{target_table} AS
        WITH raw AS (
            SELECT * FROM read_parquet('{pq}')
        ),
        dedup AS (
            SELECT * EXCLUDE(rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY note_row_id ORDER BY extracted_at DESC
                ) AS rn
                FROM raw
            )
            WHERE rn = 1
        )
        SELECT
            note_row_id::VARCHAR                     AS note_row_id,
            COALESCE(domain, '{entity_domain}')::VARCHAR       AS domain,
            COALESCE(llm_model, '{SYNTH_LLM_MODEL}')::VARCHAR  AS llm_model,
            COALESCE(llm_base_url, '{SYNTH_LLM_BASE_URL}')::VARCHAR AS llm_base_url,
            extracted_at::VARCHAR                    AS extracted_at,
            result_json::VARCHAR                     AS result_json,
            research_id::VARCHAR                     AS research_id,
            note_type::VARCHAR                       AS note_type,
            note_date::VARCHAR                       AS note_date,
            NULL::VARCHAR                            AS linkage_date,
            source_workbook::VARCHAR                 AS source_workbook,
            source_sheet::VARCHAR                    AS source_sheet,
            source_column::VARCHAR                   AS source_column,
            note_index::VARCHAR                      AS note_index,
            preprocess_batch_id::VARCHAR             AS preprocess_batch_id,
            preprocessed_at_utc::VARCHAR             AS preprocessed_at_utc,
            preprocess_script_version::VARCHAR       AS preprocess_script_version,
            '{entity_domain}'::VARCHAR               AS entity_domain,
            '{SYNTH_LLM_PROVIDER}'::VARCHAR          AS llm_provider,
            '{SYNTH_LLM_SDK}'::VARCHAR               AS llm_sdk,
            NULL::VARCHAR                            AS llm_sdk_version,
            '{SYNTH_PROVIDER_RETURNED_MODEL}'::VARCHAR AS provider_returned_model,
            NULL::VARCHAR                            AS provider_system_fingerprint
        FROM dedup;
        """
    )
    loaded = con.execute(f"SELECT COUNT(*) FROM main.{target_table}").fetchone()[0]
    log(f"  load: main.{target_table} = {loaded:,} rows")
    return int(loaded)


def parity_check_md_vs_parquet(
    con: duckdb.DuckDBPyConnection, target_table: str, parquet_path: Path
) -> dict[str, Any]:
    pq = str(parquet_path)
    md_rows = con.execute(f"SELECT COUNT(*) FROM main.{target_table}").fetchone()[0]
    pq_rows_row = con.execute(
        f"""
        SELECT COUNT(*) FROM (
          SELECT * EXCLUDE(rn) FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY note_row_id
              ORDER BY extracted_at DESC) AS rn
            FROM read_parquet('{pq}')
          ) WHERE rn = 1
        )
        """
    ).fetchone()
    pq_rows = pq_rows_row[0] if pq_rows_row else 0
    # note_row_id set symmetric difference
    diff = con.execute(
        f"""
        WITH md_n AS (SELECT note_row_id FROM main.{target_table}),
             pq_n AS (SELECT note_row_id FROM (
                       SELECT * EXCLUDE(rn) FROM (
                         SELECT *, ROW_NUMBER() OVER (PARTITION BY note_row_id
                           ORDER BY extracted_at DESC) AS rn
                         FROM read_parquet('{pq}')
                       ) WHERE rn=1
                     ))
        SELECT
          (SELECT COUNT(*) FROM (SELECT note_row_id FROM md_n EXCEPT SELECT note_row_id FROM pq_n)) AS md_only,
          (SELECT COUNT(*) FROM (SELECT note_row_id FROM pq_n EXCEPT SELECT note_row_id FROM md_n)) AS pq_only
        """
    ).fetchone()
    out = {
        "md_rows": int(md_rows),
        "parquet_rows_deduped": int(pq_rows),
        "md_only_note_row_ids": int(diff[0]),
        "parquet_only_note_row_ids": int(diff[1]),
    }
    out["ok"] = (
        out["md_rows"] == out["parquet_rows_deduped"]
        and out["md_only_note_row_ids"] == 0
        and out["parquet_only_note_row_ids"] == 0
    )
    log(f"  parity: {out}")
    return out


def snapshot_cpm(con: duckdb.DuckDBPyConnection, label: str) -> str:
    """Take a full CPM snapshot to archive_pub_v1_0."""
    attach_archive(con)
    ts = utcnow_compact()
    name = f"canonical_patient_master_snapshot_{label}_{ts}"
    con.execute(
        f"""
        CREATE OR REPLACE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{name} AS
        SELECT * FROM {CANONICAL_DB}.main.canonical_patient_master;
        """
    )
    log(f"  cpm snapshot: {name}")
    return name


def write_summary(prefix: str, payload: dict[str, Any]) -> Path:
    path = OUTPUT_DIR / f"{prefix}_{utcnow_compact()}.json"
    path.write_text(json.dumps(payload, indent=2, default=str))
    log(f"  summary: {path}")
    return path


def short_digest(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:12]
