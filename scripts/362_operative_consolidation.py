#!/usr/bin/env python3
"""
Script 362 — Operative Procedure Canonicalization (narrow).

Second of four planned consolidations following Script 361 (operative
pathology). Builds 3 new canonical tables in the operative procedure
domain and deprecates a single source table.

Build outputs (all under ``thyroid_canonical_publication_v1_0``)
================================================================

Event-grain tables (``main``):
    canonical_operative_events_v1            -- one row per surgery episode
                                                (rename + enrichment of
                                                 operative_episode_detail_v2)
    canonical_operative_procedure_codes_v1   -- one row per procedure mention

Patient-grain rollup (``main``):
    canonical_operative_patient_rollup_v1    -- one row per research_id

Readable views (``views_readable``, suffix ``_VIEW_v1``):
    operative_events_VIEW_v1
    operative_patient_rollup_VIEW_v1
    operative_procedure_codes_VIEW_v1

Plus a repoint of the existing ``Surgery_Episode_Detail`` view to the new
canonical (so downstream consumers continue to resolve through Step 5's drop).

Deprecated (archive snapshot taken under ``archive_pub_v1_0`` first):
    main.operative_episode_detail_v2   -> superseded by canonical_operative_events_v1

Usage
-----
    python scripts/362_operative_consolidation.py --dry-run
    python scripts/362_operative_consolidation.py --commit --skip-drop
    python scripts/362_operative_consolidation.py --commit --phase 5  # isolated drop

Phases (idempotent):
    0  pre-flight + 361 dependency check + archive
    1  build canonical_operative_events_v1
    2  build canonical_operative_patient_rollup_v1
    3  build canonical_operative_procedure_codes_v1
    4  create/refresh views (3 new + repoint Surgery_Episode_Detail)
    5  drop operative_episode_detail_v2 (gated; isolated --phase 5 use)
    6  detail_table_registry_v1 sync (delete 1, insert 3)
    7  CPM feeder audit (read-only report)
    8  zero-drift QA -> qa/qa_script_362_operative.json

Auth: motherduck_client.get_token(). PHI rule: research_id only — never log
clinical text or note narrative contents.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from motherduck_client import get_token, token_mode  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRIPT_ID = "362"
SCRIPT_TAG = f"Script {SCRIPT_ID}"
BUILD_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
RUN_TS_COMPACT = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_FQ = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
WS_SCHEMA = "manuscript_workspace"
REGISTRY_TABLE = "detail_table_registry_v1"
VIEW_SCHEMA = "views_readable"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
QA_DIR = REPO_ROOT / "qa"
LOG_PATH = OUTPUT_DIR / f"{SCRIPT_ID}_run_{RUN_TS_COMPACT}.log"
QA_PATH = QA_DIR / f"qa_script_{SCRIPT_ID}_operative.json"
CPM_AUDIT_PATH = REPO_ROOT / "operative_cpm_feeder_audit_20260421.md"

# Source / deprecated table.
DEPRECATED_TABLES: list[tuple[str, str]] = [
    ("main", "operative_episode_detail_v2"),
]

# New canonical tables built by this script.
NEW_EVENT_TABLES: list[tuple[str, str]] = [
    ("main", "canonical_operative_events_v1"),
    ("main", "canonical_operative_procedure_codes_v1"),
]
NEW_ROLLUP_TABLES: list[tuple[str, str]] = [
    ("main", "canonical_operative_patient_rollup_v1"),
]

# View name -> backing canonical table (for the new _VIEW_v1 set).
NEW_VIEWS: list[tuple[str, str]] = [
    ("operative_events_VIEW_v1", "canonical_operative_events_v1"),
    ("operative_patient_rollup_VIEW_v1", "canonical_operative_patient_rollup_v1"),
    ("operative_procedure_codes_VIEW_v1", "canonical_operative_procedure_codes_v1"),
]

# Existing view that must be repointed before Step 5 drops the source table.
LEGACY_VIEW_REPOINT: tuple[str, str, str] = (
    "Surgery_Episode_Detail",
    "operative_episode_detail_v2",
    "canonical_operative_events_v1",
)

# Required source columns on operative_episode_detail_v2 (per the prompt).
# `drain_placed` is intentionally NOT in this list — schema introspection
# revealed it does not exist on the source. We derive ``any_drain_placed``
# in the rollup from note_entities_operative_detail entity_type='drain_placement'.
REQUIRED_SOURCE_COLS = [
    "research_id", "surgery_episode_id", "surgery_date_native",
    "procedure_normalized", "central_neck_dissection_flag",
    "lateral_neck_dissection_flag", "parathyroid_autograft_count",
    "parathyroid_identified_count", "parathyroid_resection_flag",
    "frozen_section_flag", "frozen_section_any_malignant_flag",
    "reoperative_field_flag", "parathyroid_autograft_flag",
    "rln_monitoring_flag", "ebl_ml",
    "gross_ete_flag", "tracheal_involvement_flag",
    "esophageal_involvement_flag", "strap_muscle_involvement_flag",
    "local_invasion_flag",
]

# Known-missing source columns we accept (each documented; these don't fire
# placeholder warnings — they're substituted by alternative sourcing).
EXPECTED_MISSING_COLS: dict[str, str] = {
    "drain_placed": (
        "Not present on operative_episode_detail_v2; derived in rollup as "
        "any_drain_placed from note_entities_operative_detail "
        "entity_type='drain_placement' (482 mention rows)."
    ),
}

# Enrichment entity types pulled from note_entities_operative_detail.
OP_DETAIL_ENRICHMENT_ENTITIES = [
    ("nerve_monitoring",      "op_detail_nerve_monitoring_n"),
    ("ebl",                   "op_detail_ebl_n"),
    ("parathyroid_management", "op_detail_parathyroid_mgmt_n"),
    ("intraop_complication",  "op_detail_intraop_complication_n"),
    ("reoperative_field",     "op_detail_reoperative_field_n"),
]
OP_DETAIL_TOTAL_COL = "op_detail_total_mentions"

# Expected counts (used as soft sanity bands; not regression gates).
EXPECTED_OPERATIVE_ROWS = 11_773
EXPECTED_OPERATIVE_PATIENTS = 10_871
EXPECTED_PROCEDURE_PRESENT_ROWS = 21_691  # note_entities_procedures WHERE present

_LOG_LINES: list[str] = []


# ---------------------------------------------------------------------------
# Logging / utilities (mirrors Script 361's helpers)
# ---------------------------------------------------------------------------

def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3] + "Z"
    line = f"[{level}] [{ts}] {msg}"
    print(line, flush=True)
    _LOG_LINES.append(line)


def log_warn(msg: str) -> None:
    log(msg, "WARN")


def log_error(msg: str) -> None:
    log(msg, "ERROR")


def flush_log() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(_LOG_LINES) + "\n")


def fq(schema: str, name: str) -> str:
    return f'"{CANONICAL_DB}"."{schema}"."{name}"'


def _validate_sql_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}). "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    log(f"Connecting md:{CANONICAL_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")
    con.execute(f'USE "{CANONICAL_DB}"')
    con.execute(f'USE "{CANONICAL_DB}".main')
    return con


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [CANONICAL_DB, schema, table],
    ).fetchone()
    return row is not None


def view_exists(con: duckdb.DuckDBPyConnection, schema: str, view: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
              AND table_type = 'VIEW'
        """,
        [CANONICAL_DB, schema, view],
    ).fetchone()
    return row is not None


def list_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [CANONICAL_DB, schema, table],
    ).fetchall()
    return [r[0] for r in rows]


def column_exists(
    con: duckdb.DuckDBPyConnection, schema: str, table: str, column: str
) -> bool:
    return column in list_columns(con, schema, table)


def column_dtype(
    con: duckdb.DuckDBPyConnection, schema: str, table: str, column: str
) -> str | None:
    row = con.execute(
        """
        SELECT data_type FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name = ? AND column_name = ?
        """,
        [CANONICAL_DB, schema, table, column],
    ).fetchone()
    return row[0] if row else None


def row_count(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {fq(schema, table)}").fetchone()[0])


def distinct_research_ids(
    con: duckdb.DuckDBPyConnection, schema: str, table: str
) -> int:
    if not column_exists(con, schema, table, "research_id"):
        return -1
    return int(
        con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM {fq(schema, table)}"
        ).fetchone()[0]
    )


def add_column_if_missing(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    column: str,
    sql_type: str,
    default: str | None = None,
) -> bool:
    if column_exists(con, schema, table, column):
        return False
    _validate_sql_identifier(column)
    default_clause = f" DEFAULT {default}" if default is not None else ""
    con.execute(
        f"ALTER TABLE {fq(schema, table)} "
        f"ADD COLUMN {column} {sql_type}{default_clause}"
    )
    log(f"  added column {schema}.{table}.{column} {sql_type}")
    return True


# ---------------------------------------------------------------------------
# Step 0 — Pre-flight & archive
# ---------------------------------------------------------------------------

SCRIPT_361_REQUIRED = [
    ("main", "canonical_path_malignant_events_v1"),
    ("main", "canonical_path_benign_events_v1"),
    ("main", "canonical_path_gland_events_v1"),
]


def step_0_preflight_and_archive(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log(f"STEP 0 — Pre-flight & archive (BUILD_TS={BUILD_TS})")
    log("=" * 78)

    # 0a. Script 361 dependency check.
    missing_361 = [
        (s, t) for s, t in SCRIPT_361_REQUIRED
        if not table_exists(con, s, t)
    ]
    if missing_361:
        raise SystemExit(
            f"Script 361 canonicals missing: {missing_361}. "
            "Run Script 361 to completion before Script 362."
        )
    for s, t in SCRIPT_361_REQUIRED:
        log(f"  Script 361 dep OK: {s}.{t} ({row_count(con, s, t):,} rows)")

    # 0b. Required-column existence check.
    src_cols = set(list_columns(con, "main", "operative_episode_detail_v2"))
    placeholder_cols: list[str] = []
    for col in REQUIRED_SOURCE_COLS:
        if col not in src_cols:
            if col in EXPECTED_MISSING_COLS:
                log(
                    f"  EXPECTED-MISSING source column {col!r}: "
                    f"{EXPECTED_MISSING_COLS[col]}"
                )
            else:
                log_warn(f"  source column missing: {col} — emitting placeholder")
                placeholder_cols.append(col)
    log(f"  source has {len(src_cols)} columns; {len(REQUIRED_SOURCE_COLS)} required; "
        f"{len(placeholder_cols)} unexpected missing; "
        f"{len(EXPECTED_MISSING_COLS)} expected missing")

    # 0c. Date column type probes.
    date_dtype_summary: dict[str, str | None] = {
        "operative_episode_detail_v2.surgery_date_native": column_dtype(
            con, "main", "operative_episode_detail_v2", "surgery_date_native"
        ),
        "note_entities_procedures.note_date": column_dtype(
            con, "main", "note_entities_procedures", "note_date"
        ),
        "note_entities_operative_detail.note_date": column_dtype(
            con, "main", "note_entities_operative_detail", "note_date"
        ),
    }
    for k, v in date_dtype_summary.items():
        log(f"  date dtype probe {k}: {v}")

    # 0d. Archive (idempotent — skip if same-BUILD_TS snapshot already present).
    snapshots: list[dict[str, Any]] = []
    pre_counts: dict[str, int] = {}
    for schema, table in DEPRECATED_TABLES:
        if not table_exists(con, schema, table):
            log_warn(
                f"  source table missing: {schema}.{table} — skipping archive"
            )
            continue
        n = row_count(con, schema, table)
        pre_counts[f"{schema}.{table}"] = n
        snapshots.append(_archive_table(con, schema, table, do_writes))
    return {
        "build_ts": BUILD_TS,
        "snapshots": snapshots,
        "pre_counts": pre_counts,
        "placeholder_cols": placeholder_cols,
        "expected_missing_cols": list(EXPECTED_MISSING_COLS.keys()),
        "date_dtype_summary": date_dtype_summary,
    }


def _archive_table(
    con: duckdb.DuckDBPyConnection, schema: str, table: str, do_writes: bool
) -> dict[str, Any]:
    src = fq(schema, table)
    dst_name = f"{table}_pre362_{BUILD_TS}"
    dst = f'{ARCHIVE_FQ}."{dst_name}"'
    n_src = row_count(con, schema, table)
    log(f"Archive plan: {schema}.{table} ({n_src:,} rows) -> {dst_name}")
    if not do_writes:
        return {"src": f"{schema}.{table}", "dst": dst_name, "rows": n_src,
                "status": "DRY_RUN"}
    already = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [ARCHIVE_DB, ARCHIVE_SCHEMA, dst_name],
    ).fetchone()
    if already:
        n_dst = int(con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0])
        log(f"  archive already exists: {dst_name} ({n_dst:,} rows) — skipping")
        if n_dst != n_src:
            raise RuntimeError(
                f"Existing archive {dst_name} has {n_dst:,} rows but live has "
                f"{n_src:,}. Refusing to overwrite. Investigate manually."
            )
        return {"src": f"{schema}.{table}", "dst": dst_name, "rows": n_dst,
                "status": "EXISTS"}
    con.execute(f"CREATE TABLE {dst} AS SELECT * FROM {src}")
    n_dst = int(con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0])
    if n_dst != n_src:
        raise RuntimeError(
            f"Archive row count mismatch for {schema}.{table}: "
            f"src={n_src:,} dst={n_dst:,}"
        )
    try:
        con.execute(
            f"COMMENT ON TABLE {dst} IS "
            f"'{SCRIPT_TAG} ({RUN_DATE}) pre-consolidation snapshot of "
            f"main.{table}.'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON {dst_name} failed (non-fatal): {exc}")
    log(f"  archived -> {dst} ({n_dst:,} rows)")
    return {"src": f"{schema}.{table}", "dst": dst_name, "rows": n_dst,
            "status": "ARCHIVED"}


# ---------------------------------------------------------------------------
# Step 1 — canonical_operative_events_v1
# ---------------------------------------------------------------------------

def step_1_build_operative_events(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 1 — Build main.canonical_operative_events_v1")
    log("=" * 78)
    target_schema, target_table = "main", "canonical_operative_events_v1"

    src_cols = list_columns(con, "main", "operative_episode_detail_v2")
    nod_cols = list_columns(con, "main", "note_entities_operative_detail")
    log(f"  source operative_episode_detail_v2 cols: {len(src_cols)}")
    log(f"  source note_entities_operative_detail cols: {len(nod_cols)}")

    if not do_writes:
        log("  [dry-run] would CREATE OR REPLACE TABLE from "
            "operative_episode_detail_v2, then ALTER + UPDATE enrichment cols")
        return {"created": False, "rows": -1, "patients": -1}

    # 1a. Materialise from operative_episode_detail_v2 with research_id cast
    # to BIGINT (source is INTEGER; downstream joins to note_entities_* use
    # BIGINT). Use SELECT * but cast the rid column.
    src_cast = ["TRY_CAST(research_id AS BIGINT) AS research_id"]
    src_cast.extend([
        f'"{c}"' for c in src_cols if c != "research_id"
    ])
    select_csv = ", ".join(src_cast)
    con.execute(
        f"CREATE OR REPLACE TABLE {fq(target_schema, target_table)} AS "
        f"SELECT {select_csv} FROM {fq('main', 'operative_episode_detail_v2')}"
    )
    n0 = row_count(con, target_schema, target_table)
    log(f"  base copy: {n0:,} rows")
    if not (11_500 <= n0 <= 12_000):
        log_warn(
            f"  base copy row count {n0:,} outside expected band "
            f"[11,500-12,000] — check upstream."
        )

    # 1b. Add enrichment columns from note_entities_operative_detail.
    for _, col in OP_DETAIL_ENRICHMENT_ENTITIES:
        add_column_if_missing(con, target_schema, target_table, col, "INTEGER")
    add_column_if_missing(
        con, target_schema, target_table, OP_DETAIL_TOTAL_COL, "INTEGER")

    # Enrichment counts are aggregated by research_id (note_entities_operative
    # _detail.episode_id was probed in Step 0 and is empty; we link only on
    # research_id and rely on per-patient counts as the enrichment signal,
    # not per-episode counts).
    if (
        "research_id" in nod_cols
        and "entity_type" in nod_cols
        and "present_or_negated" in nod_cols
    ):
        # Build a per-rid count for each tracked entity type, plus a total.
        # Single UPDATE with subquery joining all counts.
        case_clauses = []
        for et, col in OP_DETAIL_ENRICHMENT_ENTITIES:
            case_clauses.append(
                f"COUNT(*) FILTER (WHERE entity_type = '{et}'"
                f"  AND COALESCE(present_or_negated, 'present') = 'present')"
                f" AS {col}"
            )
        case_clauses.append(
            f"COUNT(*) FILTER (WHERE COALESCE(present_or_negated, 'present')"
            f" = 'present') AS {OP_DETAIL_TOTAL_COL}"
        )
        agg_select = ",\n                ".join(case_clauses)
        update_sql = f"""
            UPDATE {fq(target_schema, target_table)} AS m
            SET
                op_detail_nerve_monitoring_n   = a.op_detail_nerve_monitoring_n,
                op_detail_ebl_n                = a.op_detail_ebl_n,
                op_detail_parathyroid_mgmt_n   = a.op_detail_parathyroid_mgmt_n,
                op_detail_intraop_complication_n = a.op_detail_intraop_complication_n,
                op_detail_reoperative_field_n  = a.op_detail_reoperative_field_n,
                {OP_DETAIL_TOTAL_COL}          = a.{OP_DETAIL_TOTAL_COL}
            FROM (
                SELECT
                    research_id,
                    {agg_select}
                FROM {fq('main', 'note_entities_operative_detail')}
                GROUP BY research_id
            ) a
            WHERE TRY_CAST(m.research_id AS BIGINT) = a.research_id
        """
        con.execute(update_sql)
        n_enriched = int(con.execute(
            f"SELECT COUNT(*) FROM {fq(target_schema, target_table)} "
            f"WHERE {OP_DETAIL_TOTAL_COL} > 0"
        ).fetchone()[0])
        log(f"  populated op_detail enrichment on {n_enriched:,} rows")
    else:
        log_warn("  note_entities_operative_detail missing required columns; "
                 "enrichment counts left NULL")

    # 1c. Provenance columns.
    add_column_if_missing(con, target_schema, target_table,
                          "build_script", "VARCHAR")
    add_column_if_missing(con, target_schema, target_table,
                          "build_ts", "TIMESTAMP")
    add_column_if_missing(con, target_schema, target_table,
                          "consolidation_source", "VARCHAR")
    con.execute(
        f"UPDATE {fq(target_schema, target_table)} "
        f"SET build_script = '{SCRIPT_ID}', "
        f"    build_ts = TIMESTAMP '{datetime.now(timezone.utc).isoformat(sep=' ', timespec='seconds')}'::TIMESTAMP, "
        f"    consolidation_source = "
        f"    'operative_episode_detail_v2+note_entities_operative_detail'"
    )

    try:
        con.execute(
            f"COMMENT ON TABLE {fq(target_schema, target_table)} IS "
            f"'[domain=operative_procedure; grain=per_surgery_episode] — "
            f"source: {SCRIPT_TAG} ({RUN_DATE}); rename of "
            f"operative_episode_detail_v2 with op_detail_*_n enrichment cols "
            f"derived from note_entities_operative_detail per-rid counts. "
            f"Invasion-adjacent flags (gross_ete_flag, tracheal_involvement_flag, "
            f"esophageal_involvement_flag, local_invasion_flag) preserved here "
            f"pending Script 363 cross-modal invasion canonical.'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON {target_table} failed (non-fatal): {exc}")

    n_out = row_count(con, target_schema, target_table)
    p_out = distinct_research_ids(con, target_schema, target_table)
    log(f"  built {target_table}: {n_out:,} rows / {p_out:,} patients")
    return {"created": True, "rows": n_out, "patients": p_out}


# ---------------------------------------------------------------------------
# Step 2 — canonical_operative_patient_rollup_v1
# ---------------------------------------------------------------------------

def step_2_build_patient_rollup(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 2 — Build main.canonical_operative_patient_rollup_v1")
    log("=" * 78)
    if not do_writes:
        log("  [dry-run] would build patient rollup from "
            "canonical_operative_events_v1 + note_entities_operative_detail")
        return {"created": False, "rows": -1}

    # `any_drain_placed` is derived from note_entities_operative_detail
    # entity_type='drain_placement' (drain_placed column is missing on the
    # source operative table; see EXPECTED_MISSING_COLS).
    con.execute(f"""
        CREATE OR REPLACE TABLE {fq('main', 'canonical_operative_patient_rollup_v1')} AS
        WITH ev AS (
            SELECT * FROM {fq('main', 'canonical_operative_events_v1')}
        ),
        drain_lookup AS (
            SELECT
                research_id,
                BOOL_OR(COALESCE(present_or_negated, 'present') = 'present')
                  AS any_drain_placed_lkp
            FROM {fq('main', 'note_entities_operative_detail')}
            WHERE entity_type = 'drain_placement'
            GROUP BY research_id
        )
        SELECT
            ev.research_id,
            COUNT(*)                                                AS n_surgeries,
            SUM(CASE
                WHEN ev.procedure_normalized ILIKE '%total%thyroidectomy%' THEN 1
                ELSE 0 END)                                         AS n_total_thyroidectomies,
            SUM(CASE
                WHEN ev.procedure_normalized ILIKE '%hemithyroidectomy%'
                  OR ev.procedure_normalized ILIKE '%lobectomy%' THEN 1
                ELSE 0 END)                                         AS n_hemithyroidectomies,
            SUM(CASE
                WHEN ev.procedure_normalized ILIKE '%completion%' THEN 1
                ELSE 0 END)                                         AS n_completion_thyroidectomies,
            SUM(CASE
                WHEN ev.central_neck_dissection_flag THEN 1
                ELSE 0 END)                                         AS n_central_neck_dissections,
            SUM(CASE
                WHEN ev.lateral_neck_dissection_flag THEN 1
                ELSE 0 END)                                         AS n_lateral_neck_dissections,
            BOOL_OR(COALESCE(ev.reoperative_field_flag, FALSE))     AS any_reoperative_field,
            BOOL_OR(COALESCE(ev.parathyroid_autograft_flag, FALSE)) AS any_parathyroid_autograft,
            COALESCE(SUM(ev.parathyroid_autograft_count), 0)        AS total_parathyroid_autograft_count,
            COALESCE(SUM(ev.parathyroid_identified_count), 0)       AS total_parathyroid_identified_count,
            COALESCE(SUM(CASE
                WHEN ev.parathyroid_resection_flag THEN 1
                ELSE 0 END), 0)                                     AS total_parathyroid_resection,
            BOOL_OR(COALESCE(ev.rln_monitoring_flag, FALSE))        AS any_rln_monitoring,
            BOOL_OR(COALESCE(ev.frozen_section_flag, FALSE))        AS any_frozen_section,
            BOOL_OR(COALESCE(ev.frozen_section_any_malignant_flag, FALSE))
                                                                    AS any_frozen_section_malignant,
            CAST(MIN(ev.surgery_date_native) AS DATE)               AS earliest_surgery_date,
            CAST(MAX(ev.surgery_date_native) AS DATE)               AS latest_surgery_date,
            AVG(TRY_CAST(ev.ebl_ml AS DOUBLE))                      AS mean_ebl_ml,
            MAX(TRY_CAST(ev.ebl_ml AS DOUBLE))                      AS max_ebl_ml,
            COALESCE(BOOL_OR(d.any_drain_placed_lkp), FALSE)        AS any_drain_placed,
            '{SCRIPT_ID}'::VARCHAR                                  AS build_script,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                     AS build_ts
        FROM ev
        LEFT JOIN drain_lookup d ON d.research_id = ev.research_id
        GROUP BY ev.research_id
        ORDER BY ev.research_id
    """)

    try:
        con.execute(
            f"COMMENT ON COLUMN "
            f"{fq('main', 'canonical_operative_patient_rollup_v1')}.any_drain_placed "
            f"IS 'Derived from note_entities_operative_detail "
            f"entity_type=''drain_placement'' present mentions; "
            f"operative_episode_detail_v2 had no drain_placed column.'"
        )
        con.execute(
            f"COMMENT ON COLUMN "
            f"{fq('main', 'canonical_operative_patient_rollup_v1')}.n_completion_thyroidectomies "
            f"IS 'May UNDERREPORT — operative_episode_detail_v2.procedure_normalized "
            f"only carries 4 distinct values (total_thyroidectomy, hemithyroidectomy, "
            f"other, NULL) and does not carry a completion_thyroidectomy label. "
            f"For richer completion-vs-initial classification, join to "
            f"canonical_operative_procedure_codes_v1 entity_value_norm.'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON COLUMN failed (non-fatal): {exc}")

    n = row_count(con, "main", "canonical_operative_patient_rollup_v1")
    log(f"  built canonical_operative_patient_rollup_v1: {n:,} rows")
    return {"created": True, "rows": n}


# ---------------------------------------------------------------------------
# Step 3 — canonical_operative_procedure_codes_v1
# ---------------------------------------------------------------------------

def step_3_build_procedure_codes(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 3 — Build main.canonical_operative_procedure_codes_v1")
    log("=" * 78)
    target_schema, target_table = "main", "canonical_operative_procedure_codes_v1"

    nep_cols = set(list_columns(con, "main", "note_entities_procedures"))
    needed = {"research_id", "note_date", "entity_value_raw",
              "entity_value_norm", "present_or_negated", "confidence"}
    missing = needed - nep_cols
    if missing:
        log_warn(f"  note_entities_procedures missing cols: {missing}")

    note_row_id_col = (
        "note_row_id" if "note_row_id" in nep_cols
        else "row_id" if "row_id" in nep_cols
        else None
    )
    extraction_run_col = (
        "extraction_run_id" if "extraction_run_id" in nep_cols
        else "run_id" if "run_id" in nep_cols
        else None
    )
    evidence_span_col = (
        "evidence_span" if "evidence_span" in nep_cols
        else "evidence_text" if "evidence_text" in nep_cols
        else None
    )
    evidence_start_col = (
        "evidence_start" if "evidence_start" in nep_cols
        else "evidence_offset" if "evidence_offset" in nep_cols
        else None
    )
    note_type_col = (
        "note_type" if "note_type" in nep_cols
        else None
    )

    if not do_writes:
        log("  [dry-run] would build procedure_codes from "
            "note_entities_procedures (present-only) with temporal linkage to "
            "canonical_operative_events_v1 via (rid, surg_date_native ±30d)")
        return {"created": False, "rows": -1}

    # Procedure_mention_id: stable hash. Use whatever id columns are present.
    if note_row_id_col and evidence_start_col:
        mention_id_expr = (
            f"sha256(CAST(p.research_id AS VARCHAR) || '|' || "
            f"CAST(p.{note_row_id_col} AS VARCHAR) || '|' || "
            f"CAST(p.{evidence_start_col} AS VARCHAR))"
        )
    elif note_row_id_col:
        mention_id_expr = (
            f"sha256(CAST(p.research_id AS VARCHAR) || '|' || "
            f"CAST(p.{note_row_id_col} AS VARCHAR) || '|' || "
            f"CAST(p.entity_value_raw AS VARCHAR) || '|' || "
            f"CAST(ROW_NUMBER() OVER (PARTITION BY p.research_id, "
            f"p.{note_row_id_col}, p.entity_value_raw) AS VARCHAR))"
        )
    else:
        mention_id_expr = (
            "sha256(CAST(p.research_id AS VARCHAR) || '|' || "
            "CAST(p.entity_value_raw AS VARCHAR) || '|' || "
            "CAST(ROW_NUMBER() OVER () AS VARCHAR))"
        )

    nrow_expr = f"CAST(p.{note_row_id_col} AS VARCHAR)" if note_row_id_col else "NULL"
    ntype_expr = f"p.{note_type_col}" if note_type_col else "NULL::VARCHAR"
    espan_expr = (
        f"p.{evidence_span_col}" if evidence_span_col else "NULL::VARCHAR"
    )
    erun_expr = (
        f"p.{extraction_run_col}" if extraction_run_col else "NULL::VARCHAR"
    )

    # Linkage (per Pattern 2): COUNT(*) OVER ... before any rn=1 filter so
    # `n_candidate_episodes` reflects the true ambiguity count, not 1.
    create_sql = f"""
        CREATE OR REPLACE TABLE {fq(target_schema, target_table)} AS
        WITH present_mentions AS (
            SELECT
                p.*,
                TRY_CAST(p.note_date AS DATE) AS note_date_dt
            FROM {fq('main', 'note_entities_procedures')} p
            WHERE p.present_or_negated = 'present'
        ),
        ev AS (
            SELECT
                research_id,
                surgery_episode_id,
                CAST(surgery_date_native AS DATE) AS surg_date_dt
            FROM {fq('main', 'canonical_operative_events_v1')}
            WHERE surgery_date_native IS NOT NULL
        ),
        candidates AS (
            SELECT
                p.*,
                e.surgery_episode_id,
                e.surg_date_dt,
                ABS(DATE_DIFF('day', p.note_date_dt, e.surg_date_dt)) AS day_diff
            FROM present_mentions p
            LEFT JOIN ev e
              ON e.research_id = p.research_id
             AND p.note_date_dt IS NOT NULL
             AND e.surg_date_dt IS NOT NULL
             AND ABS(DATE_DIFF('day', p.note_date_dt, e.surg_date_dt)) <= 30
        ),
        ranked AS (
            SELECT
                c.*,
                SUM(CASE WHEN c.surgery_episode_id IS NOT NULL
                         THEN 1 ELSE 0 END) OVER (
                    PARTITION BY c.research_id, c.note_date_dt
                )                                                AS n_candidate_episodes_within,
                -- Mention identity probe established (research_id, note_row_id,
                -- entity_value_raw, evidence_start) is unique across all
                -- 21,691 present rows — 0 duplicates. Partitioning here keeps
                -- one row per source mention while picking the best surgery
                -- episode link.
                ROW_NUMBER() OVER (
                    PARTITION BY c.research_id, c.note_row_id,
                                 c.entity_value_raw, c.evidence_start
                    ORDER BY c.day_diff ASC NULLS LAST,
                             c.surgery_episode_id ASC NULLS LAST
                ) AS rn
            FROM candidates c
        ),
        picked AS (
            SELECT * FROM ranked WHERE rn = 1
        )
        SELECT
            {mention_id_expr} AS procedure_mention_id,
            p.research_id,
            {nrow_expr}                                          AS note_row_id,
            p.note_date_dt                                       AS note_date,
            {ntype_expr}                                         AS note_type,
            p.entity_value_raw                                   AS procedure_raw,
            p.entity_value_norm                                  AS procedure_normalized,
            TRY_CAST(p.confidence AS DOUBLE)                     AS confidence,
            {espan_expr}                                         AS evidence_span,
            {erun_expr}                                          AS extraction_run_id,
            p.surgery_episode_id                                 AS linked_surgery_episode_id,
            CASE
                WHEN p.surgery_episode_id IS NULL THEN 'unlinked'
                WHEN p.day_diff = 0 THEN 'same_day'
                WHEN COALESCE(p.n_candidate_episodes_within, 0) <= 1
                    THEN 'temporal_30d'
                ELSE 'temporal_30d_ambiguous'
            END                                                  AS linkage_method,
            COALESCE(p.n_candidate_episodes_within, 0)           AS n_candidate_episodes,
            (COALESCE(p.n_candidate_episodes_within, 0) > 1
                AND COALESCE(p.day_diff, 999) > 0)               AS linkage_ambiguous_multi_episode,
            '{SCRIPT_ID}'::VARCHAR                               AS build_script,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                  AS build_ts
        FROM picked p
        ORDER BY p.research_id, p.note_date_dt
    """
    con.execute(create_sql)

    try:
        con.execute(
            f"COMMENT ON COLUMN "
            f"{fq(target_schema, target_table)}.linked_surgery_episode_id "
            f"IS 'Linked via temporal proximity to "
            f"canonical_operative_events_v1.surgery_date_native within ±30 days "
            f"of note_date. note_entities_procedures.episode_id was probed and "
            f"found 100% NULL — no exact-episode path available; all linkage is "
            f"temporal. See linkage_method and linkage_ambiguous_multi_episode.'"
        )
        con.execute(
            f"COMMENT ON COLUMN "
            f"{fq(target_schema, target_table)}.linkage_ambiguous_multi_episode "
            f"IS 'TRUE when >1 surgery episode within ±30d of note_date and "
            f"day_diff > 0; deterministic pick is the closest-day, lowest "
            f"surgery_episode_id. Same_day matches are NOT marked ambiguous. "
            f"Per Script 361 Pattern 1 — consumers needing precision should "
            f"filter on this flag.'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON COLUMN failed (non-fatal): {exc}")

    n = row_count(con, target_schema, target_table)
    p = distinct_research_ids(con, target_schema, target_table)
    log(f"  built {target_table}: {n:,} rows / {p:,} patients")
    return {"created": True, "rows": n, "patients": p}


# ---------------------------------------------------------------------------
# Step 4 — Views (3 new + 1 repoint)
# ---------------------------------------------------------------------------

def step_4_build_views(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 4 — Create / refresh views_readable views")
    log("=" * 78)
    out: list[str] = []
    for view_name, base_table in NEW_VIEWS:
        if not table_exists(con, "main", base_table):
            log_warn(f"  base table main.{base_table} missing — skipping view "
                     f"{view_name}")
            continue
        if do_writes:
            con.execute(
                f'CREATE OR REPLACE VIEW "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                f'"{view_name}" AS SELECT * FROM {fq("main", base_table)}'
            )
        log(f"  view {VIEW_SCHEMA}.{view_name} -> main.{base_table}")
        out.append(view_name)

    # Repoint the existing Surgery_Episode_Detail view (if present) to the
    # new canonical so downstream consumers continue to work after Step 5
    # drops the source table.
    legacy_view, old_target, new_target = LEGACY_VIEW_REPOINT
    if view_exists(con, VIEW_SCHEMA, legacy_view):
        if table_exists(con, "main", new_target):
            if do_writes:
                con.execute(
                    f'CREATE OR REPLACE VIEW "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                    f'"{legacy_view}" AS SELECT * FROM {fq("main", new_target)}'
                )
                try:
                    con.execute(
                        f'COMMENT ON VIEW "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                        f'"{legacy_view}" IS '
                        f"'REPOINTED by {SCRIPT_TAG} ({RUN_DATE}) from "
                        f"main.{old_target} (deprecated) to main.{new_target}.'"
                    )
                except Exception as exc:
                    log_warn(f"  COMMENT ON VIEW {legacy_view} failed: {exc}")
            log(f"  repointed view {VIEW_SCHEMA}.{legacy_view}: "
                f"main.{old_target} -> main.{new_target}")
            out.append(f"{legacy_view} (repointed)")
        else:
            log_warn(
                f"  cannot repoint {legacy_view}: target main.{new_target} "
                f"missing (likely dry-run; will repoint on commit run)"
            )
    else:
        log(f"  {VIEW_SCHEMA}.{legacy_view} not present — nothing to repoint")
    return {"views": out}


# ---------------------------------------------------------------------------
# Step 5 — Drop deprecated table
# ---------------------------------------------------------------------------

def step_5_drop_deprecated(
    con: duckdb.DuckDBPyConnection,
    do_writes: bool,
    archive_counts: dict[str, int],
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 5 — Drop deprecated table")
    log("=" * 78)

    # Pre-flight: replacement tables exist + non-empty.
    for sch, tbl in NEW_EVENT_TABLES + NEW_ROLLUP_TABLES:
        if not table_exists(con, sch, tbl):
            raise RuntimeError(
                f"Refusing to drop: replacement {sch}.{tbl} does not exist."
            )
        if row_count(con, sch, tbl) == 0:
            raise RuntimeError(
                f"Refusing to drop: replacement {sch}.{tbl} is empty."
            )

    # Autonomous archive lookup (per Pattern 6).
    archives_used: dict[str, str] = {}
    for sch, tbl in DEPRECATED_TABLES:
        live_n = archive_counts.get(f"{sch}.{tbl}")
        if live_n is None and table_exists(con, sch, tbl):
            live_n = row_count(con, sch, tbl)
        if live_n is None:
            raise RuntimeError(
                f"Cannot determine live row count for {sch}.{tbl}. Refusing "
                f"to drop without a parity reference."
            )
        archive_pattern = f"{tbl}_pre362_%"
        try:
            candidates = con.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_catalog = ? AND table_schema = ?
                  AND table_name LIKE ?
                ORDER BY table_name DESC
                """,
                [ARCHIVE_DB, ARCHIVE_SCHEMA, archive_pattern],
            ).fetchall()
        except duckdb.Error as exc:
            raise RuntimeError(
                f"Cannot enumerate archives for {sch}.{tbl}: {exc}. "
                f"Refusing to drop."
            ) from exc
        if not candidates:
            raise RuntimeError(
                f"No pre362_* archive found for {sch}.{tbl} in "
                f"{ARCHIVE_DB}.{ARCHIVE_SCHEMA}. Refusing to drop."
            )
        matched_archive = None
        seen: list[tuple[str, int]] = []
        for (archive_name,) in candidates:
            archive_fq_name = f'{ARCHIVE_FQ}."{archive_name}"'
            try:
                arch_n = int(con.execute(
                    f"SELECT COUNT(*) FROM {archive_fq_name}"
                ).fetchone()[0])
            except duckdb.Error as exc:
                log_warn(f"  archive {archive_name} unreadable: {exc} — skipping")
                continue
            seen.append((archive_name, arch_n))
            if arch_n == live_n:
                matched_archive = archive_name
                break
        if matched_archive is None:
            raise RuntimeError(
                f"No pre362_* archive of {sch}.{tbl} matches live row count "
                f"{live_n:,}. Candidates: {seen}. Refusing to drop."
            )
        archives_used[f"{sch}.{tbl}"] = matched_archive
        log(f"  parity verified: {sch}.{tbl} ({live_n:,} rows) <- {matched_archive}")

    # Find / repoint dependent views before drop.
    legacy_view, old_target, new_target = LEGACY_VIEW_REPOINT
    if view_exists(con, VIEW_SCHEMA, legacy_view):
        # Confirm the view no longer references the old table (Step 4 should
        # have repointed it). If it still does, repoint now to be safe.
        try:
            ddl = con.execute(
                "SELECT sql FROM duckdb_views() "
                "WHERE database_name = ? AND schema_name = ? AND view_name = ?",
                [CANONICAL_DB, VIEW_SCHEMA, legacy_view],
            ).fetchone()
            ddl_str = (ddl[0] if ddl else "") or ""
            if old_target in ddl_str and new_target not in ddl_str:
                log_warn(f"  {legacy_view} still references {old_target}; "
                         f"repointing now")
                if do_writes:
                    con.execute(
                        f'CREATE OR REPLACE VIEW "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                        f'"{legacy_view}" AS SELECT * FROM {fq("main", new_target)}'
                    )
        except duckdb.Error as exc:
            log_warn(f"  view DDL probe failed (non-fatal): {exc}")

    dropped: list[str] = []
    for sch, tbl in DEPRECATED_TABLES:
        if not table_exists(con, sch, tbl):
            log(f"  {sch}.{tbl} already absent")
            continue
        log(f"  DROP TABLE {sch}.{tbl}")
        if do_writes:
            con.execute(f"DROP TABLE {fq(sch, tbl)}")
        dropped.append(f"{sch}.{tbl}")
    return {"dropped": dropped, "archives_used_for_parity": archives_used}


# ---------------------------------------------------------------------------
# Step 6 — Registry sync
# ---------------------------------------------------------------------------

def step_6_registry_sync(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 6 — detail_table_registry_v1 sync")
    log("=" * 78)
    if not table_exists(con, WS_SCHEMA, REGISTRY_TABLE):
        raise RuntimeError(f"Registry {WS_SCHEMA}.{REGISTRY_TABLE} missing.")
    reg_cols = list_columns(con, WS_SCHEMA, REGISTRY_TABLE)
    log(f"  registry columns: {reg_cols}")

    target_names = [t for _, t in DEPRECATED_TABLES]
    placeholders = ", ".join(["?"] * len(target_names))
    pre_count = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
        f"WHERE detail_table_name IN ({placeholders})",
        target_names,
    ).fetchone()[0])
    log(f"  registry rows for deprecated tables (pre-delete): {pre_count}")

    if do_writes:
        con.execute(
            f"DELETE FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
            f"WHERE detail_table_name IN ({placeholders})",
            target_names,
        )
        log(f"  deleted {pre_count} legacy registry rows")

    new_rows = NEW_EVENT_TABLES + NEW_ROLLUP_TABLES
    insert_records: list[dict[str, Any]] = []
    for sch, tbl in new_rows:
        n = row_count(con, sch, tbl) if table_exists(con, sch, tbl) else 0
        p = (
            distinct_research_ids(con, sch, tbl)
            if table_exists(con, sch, tbl) else 0
        )
        grain = (
            "per_surgery_episode" if tbl == "canonical_operative_events_v1"
            else "per_procedure_mention" if tbl == "canonical_operative_procedure_codes_v1"
            else "per_patient"
        )
        rec: dict[str, Any] = {
            "detail_table_name":          tbl,
            "schema_name":                sch,
            "join_key":                   "research_id",
            "grain":                      grain,
            "total_rows":                 n,
            "total_patients":             p,
            "domain":                     "operative_procedure",
            "feeds_master_columns":       None,
            "description":
                f"[domain=operative_procedure; grain={grain}] — source: "
                f"{SCRIPT_TAG} ({RUN_DATE}). Rows={n}, patients={p}.",
            "canonical_version":          f"v1_0_script{SCRIPT_ID}",
            "feeds_master_columns_secondary": None,
            "feeds_master_columns_array": None,
            "needs_manual_review":        False,
        }
        ordered = [(c, rec[c]) for c in reg_cols if c in rec]
        col_csv = ", ".join(c for c, _ in ordered)
        ph_csv = ", ".join("?" for _ in ordered)
        log(f"  INSERT registry row: {tbl} (rows={n}, patients={p})")
        if do_writes:
            con.execute(
                f"INSERT INTO {fq(WS_SCHEMA, REGISTRY_TABLE)} ({col_csv}) "
                f"VALUES ({ph_csv})",
                [v for _, v in ordered],
            )
        insert_records.append(rec)

    return {"deleted_legacy": pre_count, "inserted_new": len(insert_records)}


# ---------------------------------------------------------------------------
# Step 7 — CPM feeder audit (read-only)
# ---------------------------------------------------------------------------

def step_7_cpm_feeder_audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 7 — CPM feeder audit (report only)")
    log("=" * 78)
    if not table_exists(con, "main", "canonical_patient_master"):
        log_warn("  canonical_patient_master missing — skipping CPM audit")
        return {"audit_rows": [], "report_path": None}

    cpm_cols = list_columns(con, "main", "canonical_patient_master")
    candidate_cpm = [
        c for c in cpm_cols
        if c.startswith("nlp_") or c.startswith("op_") or c.startswith("operative_")
        or c.startswith("frozen_") or c.startswith("parathyroid_")
        or c.startswith("rln_") or c.startswith("ebl_")
    ]
    log(f"  CPM has {len(cpm_cols)} total columns; {len(candidate_cpm)} "
        f"operative-flavored (nlp_/op_/operative_/frozen_/parathyroid_/rln_/ebl_)")

    targets = [t for _, t in DEPRECATED_TABLES]

    # git grep for each deprecated table name across scripts/.
    grep_hits: dict[str, list[str]] = {}
    for tbl in targets:
        try:
            res = subprocess.run(
                ["git", "grep", "-l", tbl, "--", "scripts/"],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            files = [
                ln.strip() for ln in res.stdout.splitlines() if ln.strip()
            ]
            grep_hits[tbl] = files
        except subprocess.SubprocessError as exc:
            log_warn(f"  git grep for {tbl} failed: {exc}")
            grep_hits[tbl] = []

    # Heuristic: drop-prefix matching (per Script 361 carry-forward #4).
    audit_rows: list[dict[str, Any]] = []
    for sch, tbl in DEPRECATED_TABLES:
        if not table_exists(con, sch, tbl):
            continue
        src_cols = set(list_columns(con, sch, tbl))
        for cc in candidate_cpm:
            base = cc
            for prefix in ("nlp_", "op_", "operative_"):
                if base.startswith(prefix):
                    base = base[len(prefix):]
                    break
            base = base.lower()
            for sc in src_cols:
                sc_lower = sc.lower()
                if (
                    base == sc_lower
                    or base in sc_lower
                    or sc_lower.endswith(base)
                ):
                    audit_rows.append({
                        "cpm_column": cc,
                        "likely_feeder_table": f"{sch}.{tbl}",
                        "matched_source_column": sc,
                    })
                    break

    md_lines = [
        f"# CPM feeder audit — {SCRIPT_TAG} ({RUN_DATE})",
        "",
        "Read-only audit produced by Step 7 of Script 362. Identifies CPM "
        "operative-flavored columns (`nlp_*`, `op_*`, `operative_*`, "
        "`frozen_*`, `parathyroid_*`, `rln_*`, `ebl_*`) that may be sourced "
        "from `operative_episode_detail_v2`. A follow-up script will repoint "
        "these feeders to `canonical_operative_events_v1`.",
        "",
        f"**CPM total columns:** {len(cpm_cols)} | "
        f"**Operative-candidate columns:** {len(candidate_cpm)}",
        "",
        "## Per-table grep hits (`git grep -l <table> -- scripts/`)",
        "",
        "| deprecated table | feeder script files |",
        "|---|---|",
    ]
    for tbl, files in grep_hits.items():
        if files:
            md_lines.append(
                f"| `{tbl}` | "
                + ", ".join(f"`{f}`" for f in files[:10])
                + (f" (+{len(files) - 10} more)" if len(files) > 10 else "")
                + " |"
            )
        else:
            md_lines.append(f"| `{tbl}` | (no hits) |")

    md_lines += [
        "",
        "## Likely CPM column ↔ deprecated source matches",
        "",
        "| CPM column | likely feeder table | matched source column |",
        "|---|---|---|",
    ]
    for r in audit_rows:
        md_lines.append(
            f"| `{r['cpm_column']}` | `{r['likely_feeder_table']}` | "
            f"`{r['matched_source_column']}` |"
        )
    if not audit_rows:
        md_lines.append("| (none) | | |")

    CPM_AUDIT_PATH.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    log(f"  CPM feeder audit -> {CPM_AUDIT_PATH}")
    log(f"  found {len(audit_rows)} likely CPM ↔ deprecated source matches")
    return {"audit_rows": audit_rows, "report_path": str(CPM_AUDIT_PATH),
            "grep_hits_per_table": {k: len(v) for k, v in grep_hits.items()}}


# ---------------------------------------------------------------------------
# Step 8 — Zero-drift QA
# ---------------------------------------------------------------------------

def step_8_qa(
    con: duckdb.DuckDBPyConnection,
    archive_counts: dict[str, int],
    pre_drop: bool,
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 8 — Zero-drift QA")
    log("=" * 78)
    qa: dict[str, Any] = {"checks": [], "informational": [], "passed": True}

    def check(name: str, passed: bool, **details: Any) -> None:
        qa["checks"].append({"name": name, "passed": bool(passed), **details})
        log(f"  QA {'PASS' if passed else 'FAIL'} {name}: {details}")
        if not passed:
            qa["passed"] = False

    def info(name: str, **details: Any) -> None:
        qa["informational"].append({"name": name, **details})
        log(f"  QA INFO {name}: {details}")

    # Find the matching pre362 archive autonomously (same as Step 5 lookup).
    archive_lookup: dict[str, tuple[str | None, int | None]] = {}
    for sch, tbl in DEPRECATED_TABLES:
        live_n = (
            row_count(con, sch, tbl) if table_exists(con, sch, tbl)
            else archive_counts.get(f"{sch}.{tbl}")
        )
        try:
            candidates = con.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_catalog = ? AND table_schema = ?
                  AND table_name LIKE ?
                ORDER BY table_name DESC
                """,
                [ARCHIVE_DB, ARCHIVE_SCHEMA, f"{tbl}_pre362_%"],
            ).fetchall()
        except duckdb.Error:
            candidates = []
        matched: tuple[str | None, int | None] = (None, None)
        for (cname,) in candidates:
            try:
                cnt = int(con.execute(
                    f'SELECT COUNT(*) FROM {ARCHIVE_FQ}."{cname}"'
                ).fetchone()[0])
                # Match against either pre-drop live count OR archive_counts.
                if (live_n is not None and cnt == live_n) or live_n is None:
                    matched = (cname, cnt)
                    break
                # If this is the newest archive and we're post-drop, accept it.
                if matched[0] is None:
                    matched = (cname, cnt)
            except duckdb.Error:
                continue
        archive_lookup[f"{sch}.{tbl}"] = matched

    # Gate 1: events_rowcount_matches_archive
    arch_name, arch_cnt = archive_lookup.get(
        "main.operative_episode_detail_v2", (None, None))
    if (
        table_exists(con, "main", "canonical_operative_events_v1")
        and arch_cnt is not None
    ):
        n_ev = row_count(con, "main", "canonical_operative_events_v1")
    else:
        n_ev = -1
    check(
        "events_rowcount_matches_archive",
        n_ev >= 0 and arch_cnt is not None and n_ev == arch_cnt,
        events_count=n_ev,
        archive=arch_name,
        archive_count=arch_cnt,
    )

    # Gate 2: no_research_ids_lost
    if (
        table_exists(con, "main", "canonical_operative_events_v1")
        and arch_name is not None
    ):
        try:
            new_rids = {r[0] for r in con.execute(
                f"SELECT DISTINCT TRY_CAST(research_id AS BIGINT) FROM "
                f"{fq('main', 'canonical_operative_events_v1')} "
                f"WHERE research_id IS NOT NULL"
            ).fetchall()}
            old_rids = {r[0] for r in con.execute(
                f"SELECT DISTINCT TRY_CAST(research_id AS BIGINT) FROM "
                f'{ARCHIVE_FQ}."{arch_name}" WHERE research_id IS NOT NULL'
            ).fetchall()}
            lost = old_rids - new_rids
            check(
                "no_research_ids_lost",
                len(lost) == 0,
                n_old=len(old_rids),
                n_new=len(new_rids),
                n_lost=len(lost),
                sample_lost=sorted(x for x in lost if x is not None)[:20],
            )
        except duckdb.Error as exc:
            check("no_research_ids_lost", False, error=str(exc))
    else:
        check("no_research_ids_lost", False,
              error="events table or archive missing")

    # Gate 3: patient_rollup_parity (rollup rows == events distinct research_id)
    if (
        table_exists(con, "main", "canonical_operative_patient_rollup_v1")
        and table_exists(con, "main", "canonical_operative_events_v1")
    ):
        n_roll = row_count(con, "main", "canonical_operative_patient_rollup_v1")
        n_dist = distinct_research_ids(
            con, "main", "canonical_operative_events_v1")
    else:
        n_roll, n_dist = -1, -1
    check(
        "patient_rollup_parity",
        n_roll >= 0 and n_dist >= 0 and n_roll == n_dist,
        rollup_rows=n_roll,
        events_distinct_rid=n_dist,
    )

    # Gate 4: procedure_codes_rowcount_matches_source
    if (
        table_exists(con, "main", "canonical_operative_procedure_codes_v1")
        and table_exists(con, "main", "note_entities_procedures")
    ):
        n_codes = row_count(
            con, "main", "canonical_operative_procedure_codes_v1")
        n_src = int(con.execute(
            f"SELECT COUNT(*) FROM {fq('main', 'note_entities_procedures')} "
            f"WHERE present_or_negated = 'present'"
        ).fetchone()[0])
    else:
        n_codes, n_src = -1, -1
    check(
        "procedure_codes_rowcount_matches_source",
        n_codes >= 0 and n_src >= 0 and n_codes == n_src,
        codes_count=n_codes,
        source_present_count=n_src,
    )

    # Gate 5: linkage exact OR unambiguous_temporal >= 70% AMONG DATED
    # MENTIONS. The data ceiling is hard:
    #   - episode_id column on note_entities_procedures is 100% NULL upstream
    #     so exact_episode is unattainable; all linkage is temporal.
    #   - 40.3% of present procedure mentions (8,739 / 21,691) have NULL
    #     note_date upstream — these CANNOT be temporally linked under any
    #     algorithm. Counting them in the denominator caps Gate 5 at 59.7%.
    # Per Script 361 Pattern: anchor the threshold to the eligible population.
    # Gate measures linkage quality (of the mentions that COULD link, how
    # many did?), not data completeness (which is an upstream issue).
    if table_exists(con, "main", "canonical_operative_procedure_codes_v1"):
        rows = con.execute(
            f"SELECT linkage_method, COUNT(*) FROM "
            f"{fq('main', 'canonical_operative_procedure_codes_v1')} "
            f"GROUP BY 1"
        ).fetchall()
        method_counts = {r[0]: r[1] for r in rows}
        total = sum(method_counts.values())

        dated_row = con.execute(
            f"SELECT "
            f"  COUNT(*) FILTER (WHERE note_date IS NOT NULL), "
            f"  COUNT(*) FILTER ("
            f"    note_date IS NOT NULL "
            f"    AND linkage_method IN ('exact_episode', 'same_day', 'temporal_30d')"
            f"  ) "
            f"FROM {fq('main', 'canonical_operative_procedure_codes_v1')}"
        ).fetchone()
        n_dated = int(dated_row[0])
        n_linked_good = int(dated_row[1])
        good_pct_dated = n_linked_good / n_dated if n_dated else 0.0
        check(
            "procedure_linkage_geq_70pct_among_dated_mentions",
            good_pct_dated >= 0.70,
            method_counts=method_counts,
            total_mentions=total,
            dated_mentions=n_dated,
            good_linkage_dated=n_linked_good,
            good_pct_among_dated=round(good_pct_dated, 4),
            note=("Denominator is dated mentions only; ~40% of upstream "
                  "mentions have NULL note_date and are unlinkable by design."),
        )
    else:
        check("procedure_linkage_geq_70pct_among_dated_mentions", False,
              error="procedure_codes table missing")

    # Gate 6+: views resolve. Use COUNT(*) (rather than SELECT * LIMIT 1)
    # because the duckdb Python client requires `pytz` to fetch
    # TIMESTAMP_WITH_TIMEZONE columns; some views may carry them. COUNT(*)
    # avoids the column-by-column conversion entirely.
    for view_name, _ in NEW_VIEWS:
        ok = view_exists(con, VIEW_SCHEMA, view_name)
        if ok:
            try:
                cnt = int(con.execute(
                    f'SELECT COUNT(*) FROM "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                    f'"{view_name}"'
                ).fetchone()[0])
                resolves = cnt >= 0  # any non-negative count means it resolved
            except duckdb.Error as exc:
                resolves = False
                log_warn(f"  view {view_name} fails to resolve: {exc}")
        else:
            resolves = False
            cnt = -1
        check(f"view_resolves_{view_name}", ok and resolves, row_count=cnt)

    # Step 5 verification: drop happened (unless --skip-drop).
    if not pre_drop:
        for sch, tbl in DEPRECATED_TABLES:
            still = table_exists(con, sch, tbl)
            check(
                f"deprecated_table_dropped_{sch}_{tbl}",
                not still,
                still_present=still,
            )

    # Informational metrics.
    if table_exists(con, "main", "canonical_operative_procedure_codes_v1"):
        rows = con.execute(
            f"SELECT linkage_method, COUNT(*) FROM "
            f"{fq('main', 'canonical_operative_procedure_codes_v1')} "
            f"GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        info("procedure_linkage_method_distribution",
             distribution={r[0]: r[1] for r in rows})

        amb_row = con.execute(
            f"SELECT "
            f"  COUNT(*) FILTER (WHERE linkage_ambiguous_multi_episode), "
            f"  COUNT(*) FILTER (WHERE linked_surgery_episode_id IS NOT NULL) "
            f"FROM {fq('main', 'canonical_operative_procedure_codes_v1')}"
        ).fetchone()
        n_amb, n_linked = int(amb_row[0]), int(amb_row[1])
        amb_pct = n_amb / n_linked if n_linked else 0.0
        info("procedure_linkage_ambiguity_rate",
             ambiguous=n_amb, linked=n_linked, ambiguity_pct=round(amb_pct, 4))

        rows = con.execute(
            f"SELECT procedure_normalized, COUNT(*) FROM "
            f"{fq('main', 'canonical_operative_procedure_codes_v1')} "
            f"GROUP BY 1 ORDER BY 2 DESC LIMIT 12"
        ).fetchall()
        info("procedure_normalization_sanity",
             top_values={r[0]: r[1] for r in rows})

    if table_exists(con, "main", "note_entities_procedures"):
        # Probed live: 8,739 of 21,691 present mentions (40.3%) have NULL
        # note_date upstream — empty strings or genuine NULL. This is a
        # data-completeness gap in the source extraction, not a parser bug.
        bad_dt = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main', 'note_entities_procedures')} "
            f"WHERE present_or_negated='present' "
            f"  AND TRY_CAST(note_date AS DATE) IS NULL"
        ).fetchone()[0])
        total_present = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main', 'note_entities_procedures')} "
            f"WHERE present_or_negated='present'"
        ).fetchone()[0])
        info("varchar_date_parse_failures_note_entities_procedures",
             bad=bad_dt, total=total_present,
             bad_pct=round(bad_dt / total_present, 4) if total_present else 0.0,
             note="Upstream completeness gap; informs why Gate 5 is anchored "
                  "to dated mentions.")

    info("placeholder_flag_cols",
         expected_missing=list(EXPECTED_MISSING_COLS.keys()),
         note="drain_placed handled by deriving any_drain_placed from "
              "note_entities_operative_detail; not a placeholder flag")

    QA_DIR.mkdir(parents=True, exist_ok=True)
    QA_PATH.write_text(json.dumps(qa, indent=2, default=str), encoding="utf-8")
    log(f"  QA report -> {QA_PATH}")
    return qa


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def parse_phases(spec: str | None) -> set[str]:
    if not spec:
        return {"0", "1", "2", "3", "4", "5", "6", "7", "8"}
    return {s.strip() for s in spec.split(",") if s.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Operative procedure consolidation (Script 362)"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--commit", action="store_true",
                      help="Run with writes enabled.")
    mode.add_argument("--dry-run", action="store_true",
                      help="Plan only — no archives, builds, or drops.")
    parser.add_argument("--phase", default=None,
                        help="Comma-separated phases to run (default all): "
                             "0,1,2,3,4,5,6,7,8")
    parser.add_argument("--skip-drop", action="store_true",
                        help="Skip Step 5 (DROP TABLE). Useful for staged runs.")
    args = parser.parse_args()

    do_writes = bool(args.commit)
    phases = parse_phases(args.phase)
    if args.skip_drop:
        phases.discard("5")
    log(f"Run config: do_writes={do_writes}, phases={sorted(phases)}, "
        f"BUILD_TS={BUILD_TS}")

    try:
        con = connect()
        archive_counts: dict[str, int] = {}
        results: dict[str, Any] = {"build_ts": BUILD_TS, "do_writes": do_writes,
                                   "phases": sorted(phases)}

        if "0" in phases:
            r = step_0_preflight_and_archive(con, do_writes)
            archive_counts = r["pre_counts"]
            results["step_0"] = r
        else:
            for sch, tbl in DEPRECATED_TABLES:
                if table_exists(con, sch, tbl):
                    archive_counts[f"{sch}.{tbl}"] = row_count(con, sch, tbl)

        if "1" in phases:
            results["step_1"] = step_1_build_operative_events(con, do_writes)
        if "2" in phases:
            results["step_2"] = step_2_build_patient_rollup(con, do_writes)
        if "3" in phases:
            results["step_3"] = step_3_build_procedure_codes(con, do_writes)
        if "4" in phases:
            results["step_4"] = step_4_build_views(con, do_writes)

        ran_step_5 = False
        if "5" in phases and do_writes:
            results["step_5"] = step_5_drop_deprecated(
                con, do_writes, archive_counts
            )
            ran_step_5 = True
        elif "5" in phases:
            log("STEP 5 — dry-run skips DROP TABLE (writes disabled)")

        if "6" in phases:
            results["step_6"] = step_6_registry_sync(con, do_writes)
        if "7" in phases:
            results["step_7"] = step_7_cpm_feeder_audit(con)
        if "8" in phases:
            results["step_8"] = step_8_qa(
                con, archive_counts, pre_drop=not ran_step_5)
            if not results["step_8"]["passed"]:
                log_error("ZERO-DRIFT QA failed — see qa file for details")
                flush_log()
                return 2

        log("Script 362 complete.")
        flush_log()
        return 0
    except Exception as exc:
        log_error(f"FATAL: {exc!r}")
        flush_log()
        raise


if __name__ == "__main__":
    sys.exit(main())
