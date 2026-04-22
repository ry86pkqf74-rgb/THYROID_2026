#!/usr/bin/env python3
"""
Script 361 — Operative Pathology Consolidation.

Consolidates the operative pathology layer into 3 canonical event-grain tables
plus 3 patient-grain rollups, with thin readable views, deprecating 7 legacy
tables. Aligns with the close-out pattern of Scripts 347 (labs) and 360
(frozen section).

Build outputs (all under ``thyroid_canonical_publication_v1_0``)
================================================================

Event-grain tables (``main``):
    canonical_path_malignant_events_v1   -- one row per tumor per surgery
    canonical_path_benign_events_v1      -- one row per synoptic report
    canonical_path_gland_events_v1       -- one row per anatomical gland (long)

Patient-grain rollups (``main``):
    canonical_path_malignant_patient_rollup_v1
    canonical_path_benign_patient_rollup_v1
    canonical_path_gland_patient_rollup_v1

Readable views (``views_readable``, suffix ``_VIEW_v1``):
    path_malignant_events_VIEW_v1
    path_benign_events_VIEW_v1
    path_gland_events_VIEW_v1
    path_malignant_patient_rollup_VIEW_v1
    path_benign_patient_rollup_VIEW_v1
    path_gland_patient_rollup_VIEW_v1

Deprecated (archive snapshots taken under ``archive_pub_v1_0`` first):
    main.canonical_tumor_characteristics_v1   -> superseded by malignant_events
    main.canonical_benign_diagnosis_v1
    main.canonical_malignant_diagnosis_v1
    main.canonical_diagnosis_unified_v1
    main.tumor_episode_master_v2
    main.synoptic_tumor_long_v1
    main.path_outcome_classification_v1

Usage
-----
    python scripts/361_op_path_consolidation.py --dry-run
    python scripts/361_op_path_consolidation.py --commit
    python scripts/361_op_path_consolidation.py --commit --phase 0,1,2,3
    python scripts/361_op_path_consolidation.py --commit --skip-drop  # Steps 0-6 + 8-10

Phases (idempotent):
    0  pre-flight inventory + archive snapshots
    1  build canonical_path_malignant_events_v1
    2  build canonical_path_benign_events_v1
    3  build canonical_path_gland_events_v1
    5  build the 3 patient-grain rollups
    6  create/refresh the 6 views_readable views
    7  drop deprecated tables (gated behind --commit and verified row counts)
    8  detail_table_registry_v1 sync (delete 7, insert 6)
    9  CPM feeder audit (read-only report)
    10 zero-drift QA -> qa/qa_script_361_op_path_consolidation.json

Auth: motherduck_client.get_token(). PHI rule: research_id only — never log
clinical text or path report contents.
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

SCRIPT_ID = "361"
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
QA_PATH = QA_DIR / f"qa_script_{SCRIPT_ID}_op_path_consolidation.json"
CPM_AUDIT_PATH = REPO_ROOT / "op_path_cpm_feeder_audit_20260421.md"

# Tables that get archived in Step 0 / dropped in Step 7.
DEPRECATED_TABLES: list[tuple[str, str]] = [
    ("main", "canonical_tumor_characteristics_v1"),
    ("main", "canonical_benign_diagnosis_v1"),
    ("main", "canonical_malignant_diagnosis_v1"),
    ("main", "canonical_diagnosis_unified_v1"),
    ("main", "tumor_episode_master_v2"),
    ("main", "synoptic_tumor_long_v1"),
    ("main", "path_outcome_classification_v1"),
]

# New canonical tables built by this script.
NEW_EVENT_TABLES: list[tuple[str, str]] = [
    ("main", "canonical_path_malignant_events_v1"),
    ("main", "canonical_path_benign_events_v1"),
    ("main", "canonical_path_gland_events_v1"),
]
NEW_ROLLUP_TABLES: list[tuple[str, str]] = [
    ("main", "canonical_path_malignant_patient_rollup_v1"),
    ("main", "canonical_path_benign_patient_rollup_v1"),
    ("main", "canonical_path_gland_patient_rollup_v1"),
]

# View name -> backing canonical table.
NEW_VIEWS: list[tuple[str, str]] = [
    ("path_malignant_events_VIEW_v1", "canonical_path_malignant_events_v1"),
    ("path_benign_events_VIEW_v1", "canonical_path_benign_events_v1"),
    ("path_gland_events_VIEW_v1", "canonical_path_gland_events_v1"),
    ("path_malignant_patient_rollup_VIEW_v1",
     "canonical_path_malignant_patient_rollup_v1"),
    ("path_benign_patient_rollup_VIEW_v1",
     "canonical_path_benign_patient_rollup_v1"),
    ("path_gland_patient_rollup_VIEW_v1",
     "canonical_path_gland_patient_rollup_v1"),
]

# Output column name (as requested in the prompt) -> path_synoptics raw column
# name. The prompt names use an ``nlp_*`` prefix to align with CPM convention,
# but path_synoptics carries the bare column name. Missing keys are tolerated:
# the build step logs a warning and emits a constant FALSE for that column
# (per the prompt's "validate ... coalesce to FALSE" instruction).
BENIGN_FLAG_MAP: dict[str, str] = {
    # neoplastic / structural benign
    "nlp_mng": "multinodular_goiter",
    "nlp_multinodular_goiter": "multinodular_goiter",
    "nlp_substernal_mng": "substernal_multinodular_goiter",
    "nlp_follicular_adenoma": "follicular_adenoma",
    "nlp_hurthle_cell_adenoma": "hurthle_cell_oncocytic_adenoma",
    "nlp_hurthle_cell_change": "hurthle_cell_change",
    "nlp_hurthle_cell_metaplasia": "hurthle_cell_metaplasia",
    "nlp_hurthle_cell_nodule": "hurthle_cell_nodule",
    "nlp_adenomatoid_nodule": "adenomatoid_nodules",
    "nlp_colloid_nodule": "colloid_nodule",
    "nlp_colloid_cyst": "colloid_cyst",
    "nlp_cystic_change": "cystic_degeneration",
    "nlp_follicular_nodule": "follicular_nodule",
    "nlp_hyperplastic_nodules": "hyperplastic_nodules",
    "nlp_atypical_adenoma": "atypical_adenomas",
    "nlp_hyalinizing_trabecular_tumor": "hyalinizing_trabecular_tumor_adenoma",
    # Graves / thyroglossal / other
    "nlp_graves": "graves",
    "nlp_graves_disease": "graves",
    "nlp_thyroglossal_duct_cyst": "thyroglossal_duct_cyst",
    # NIFTP family — not present in path_synoptics; emitted as FALSE via fallback
    "nlp_nifcp": None,
    "nlp_nifp": None,
    "nlp_nifpt": None,
    # hyperplasia
    "nlp_hyperplasia": "hyperplastic_change_follicular_hyperplasia",
    "nlp_normal_thyroid": None,  # not captured as a discrete flag
    "nlp_nodular_hyperplasia": "adenomatous_hyperplasia",
    "nlp_papillary_hyperplasia": "papillary_hyperplasia",
    "nlp_c_cell_hyperplasia": "c_cell_hyperplasia",
}

THYROIDITIS_FLAG_MAP: dict[str, str] = {
    "nlp_hashimotos_thyroiditis": "hashimoto_thyroiditis",
    "nlp_hashimotos": "hashimoto_thyroiditis",
    "nlp_lymphocytic_thyroiditis": "lymphocytic_thyroiditis",
    "nlp_chronic_lymphocytic_thyroiditis": "chronic_lymphocytic_thyroiditis",
    "nlp_chronic_thyroiditis": "chronic_thyroiditis",
    "nlp_riedels_thyroiditis": "riedels_fibrosing_thyroiditis",
    "nlp_de_quervains_thyroiditis":
        "de_quervain_thyroiditis_granulomatous_thyroiditis_or_giant_cell_thyroiditis",
    "nlp_granulomatous_thyroiditis":
        "de_quervain_thyroiditis_granulomatous_thyroiditis_or_giant_cell_thyroiditis",
    "nlp_palpation_thyroiditis": "palpation_thyroiditis",
    "nlp_autoimmune_thyroiditis": "autoimmune_thyroiditis",
    "nlp_chronic_inflammation": "chronic_inflammation",
}

# Expected counts (used as soft sanity checks; not regression gates).
EXPECTED_MALIGNANT_ROWS = 11_106  # from canonical_tumor_characteristics_v1
EXPECTED_MALIGNANT_PATIENTS = 4_137  # from canonical_malignant_diagnosis_v1
EXPECTED_BENIGN_ROWS = 11_688  # from path_synoptics
EXPECTED_BENIGN_PATIENTS = 10_871

_LOG_LINES: list[str] = []


# ---------------------------------------------------------------------------
# Logging / utilities
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


def archive_table(
    con: duckdb.DuckDBPyConnection, schema: str, table: str, do_writes: bool
) -> dict[str, Any]:
    src = fq(schema, table)
    dst_name = f"{table}_pre361_{BUILD_TS}"
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

def step_0_preflight_and_archive(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log(f"STEP 0 — Pre-flight & archive (BUILD_TS={BUILD_TS})")
    log("=" * 78)
    snapshots: list[dict[str, Any]] = []
    pre_counts: dict[str, int] = {}
    for schema, table in DEPRECATED_TABLES:
        if not table_exists(con, schema, table):
            log_warn(
                f"  source table missing: {schema}.{table} — skipping archive "
                f"(may already be deprecated)"
            )
            continue
        n = row_count(con, schema, table)
        pre_counts[f"{schema}.{table}"] = n
        snapshots.append(archive_table(con, schema, table, do_writes))
    # Verify each archive equals live count (already checked inside archive_table).
    return {
        "build_ts": BUILD_TS,
        "snapshots": snapshots,
        "pre_counts": pre_counts,
    }


# ---------------------------------------------------------------------------
# Step 1 — canonical_path_malignant_events_v1
# ---------------------------------------------------------------------------

def step_1_build_malignant_events(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 1 — Build main.canonical_path_malignant_events_v1")
    log("=" * 78)
    target_schema, target_table = "main", "canonical_path_malignant_events_v1"

    # Validate source schema before doing anything destructive.
    ctc_cols = list_columns(con, "main", "canonical_tumor_characteristics_v1")
    if not ctc_cols:
        raise RuntimeError(
            "main.canonical_tumor_characteristics_v1 missing — cannot build "
            "malignant events."
        )
    tem_cols = list_columns(con, "main", "tumor_episode_master_v2")
    stf_cols = list_columns(con, "main", "specimen_tumor_focus_v1")
    log(f"  source CTC v1 cols: {len(ctc_cols)}")
    log(f"  source TEM v2 cols: {len(tem_cols)}")
    log(f"  source STF v1 cols: {len(stf_cols)}")

    # 1a. Materialise from canonical_tumor_characteristics_v1 (idempotent).
    if not do_writes:
        log("  [dry-run] would CREATE OR REPLACE TABLE from "
            "canonical_tumor_characteristics_v1")
        return {"created": False, "rows": -1, "patients": -1}

    con.execute(
        f"CREATE OR REPLACE TABLE {fq(target_schema, target_table)} AS "
        f"SELECT * FROM {fq('main', 'canonical_tumor_characteristics_v1')}"
    )
    n0 = row_count(con, target_schema, target_table)
    log(f"  base copy: {n0:,} rows")
    if n0 != EXPECTED_MALIGNANT_ROWS:
        log_warn(
            f"  base copy row count {n0:,} differs from expected "
            f"{EXPECTED_MALIGNANT_ROWS:,} — check upstream."
        )

    # 1b. Add discordance columns from tumor_episode_master_v2.
    # Discordance flag column names. NOTE on naming (per code review):
    # tumor_episode_master_v2.t_stage_discordance_flag conflates size, ETE, and
    # multifocality differences (anything that affects the AJCC T category).
    # We name our column discordance_t_stage_flag (NOT discordance_size_flag)
    # to be honest about the upstream semantics. discordance_laterality_flag
    # is added per the prompt's column contract but stays NULL — no upstream
    # source provides it.
    add_column_if_missing(con, target_schema, target_table,
                          "discordance_histology_flag", "BOOLEAN")
    add_column_if_missing(con, target_schema, target_table,
                          "discordance_t_stage_flag", "BOOLEAN")
    add_column_if_missing(con, target_schema, target_table,
                          "discordance_laterality_flag", "BOOLEAN")
    add_column_if_missing(con, target_schema, target_table,
                          "discordance_notes", "VARCHAR")

    # Map TEM v2 -> our names. TEM v2 carries:
    #   histology_discordance_flag, t_stage_discordance_flag (BOOLEAN),
    #   consult_precedence_flag (BOOLEAN), consult_diagnosis (VARCHAR).
    join_cols_present = (
        "surgery_episode_id" in ctc_cols and "tumor_ordinal" in ctc_cols
        and "surgery_episode_id" in tem_cols and "tumor_ordinal" in tem_cols
    )
    if join_cols_present:
        update_sql = f"""
            UPDATE {fq(target_schema, target_table)} AS m
            SET
                discordance_histology_flag = t.histology_discordance_flag,
                discordance_t_stage_flag   = t.t_stage_discordance_flag,
                discordance_notes          =
                    CASE
                        WHEN t.consult_precedence_flag IS TRUE
                             OR t.consult_diagnosis IS NOT NULL
                        THEN COALESCE(
                                NULLIF(
                                    TRIM(CAST(t.consult_diagnosis AS VARCHAR)),
                                    ''
                                ),
                                'consult_precedence_flag=TRUE')
                        ELSE NULL
                    END
            FROM {fq('main', 'tumor_episode_master_v2')} AS t
            WHERE TRY_CAST(m.research_id AS BIGINT) =
                  TRY_CAST(t.research_id AS BIGINT)
              AND m.surgery_episode_id = t.surgery_episode_id
              AND m.tumor_ordinal      = t.tumor_ordinal
        """
        con.execute(update_sql)
        n_disc = int(con.execute(
            f"SELECT COUNT(*) FROM {fq(target_schema, target_table)} "
            f"WHERE discordance_histology_flag IS NOT NULL "
            f"   OR discordance_t_stage_flag IS NOT NULL "
            f"   OR discordance_notes IS NOT NULL"
        ).fetchone()[0])
        log(f"  populated discordance columns on {n_disc:,} rows from TEM v2")
        # Document the t_stage conflation directly on the column so anyone
        # querying knows discordance_t_stage_flag is NOT pure size discord.
        try:
            con.execute(
                f"COMMENT ON COLUMN {fq(target_schema, target_table)}"
                f".discordance_t_stage_flag IS "
                f"'Sourced from tumor_episode_master_v2.t_stage_discordance_flag. "
                f"This is an AJCC T-category discordance, which conflates size, "
                f"extrathyroidal extension, and multifocality differences between "
                f"path-report and consult-precedence sources. NOT a pure size "
                f"discordance.'"
            )
            con.execute(
                f"COMMENT ON COLUMN {fq(target_schema, target_table)}"
                f".discordance_laterality_flag IS "
                f"'Reserved for future use. Currently NULL — no upstream source "
                f"in tumor_episode_master_v2 or canonical_tumor_characteristics_v1 "
                f"provides a laterality discordance signal.'"
            )
        except Exception as exc:
            log_warn(f"  COMMENT ON COLUMN failed (non-fatal): {exc}")
    else:
        log_warn(
            "  TEM v2 / CTC v1 missing surgery_episode_id+tumor_ordinal; "
            "discordance columns left NULL"
        )

    # 1c. Pull linkage cols from specimen_tumor_focus_v1.
    add_column_if_missing(con, target_schema, target_table,
                          "specimen_focus_id", "VARCHAR")
    add_column_if_missing(con, target_schema, target_table,
                          "linkage_confidence_tier", "VARCHAR")
    add_column_if_missing(con, target_schema, target_table,
                          "linkage_score", "DECIMAL(29,3)")
    if (
        "surgery_episode_id" in stf_cols
        and "tumor_ordinal" in stf_cols
        and "surgery_episode_id" in ctc_cols
        and "tumor_ordinal" in ctc_cols
    ):
        # specimen_tumor_focus_v1 may have multiple foci per tumor; take the
        # highest-confidence single match per (rid, surgery_episode_id,
        # tumor_ordinal) to keep the join 1:1.
        link_sql = f"""
            WITH ranked AS (
                SELECT
                    research_id,
                    surgery_episode_id,
                    tumor_ordinal,
                    specimen_focus_id,
                    linkage_confidence_tier,
                    linkage_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY research_id, surgery_episode_id,
                                     tumor_ordinal
                        ORDER BY linkage_score DESC NULLS LAST,
                                 specimen_focus_id
                    ) AS rn
                FROM {fq('main', 'specimen_tumor_focus_v1')}
                WHERE surgery_episode_id IS NOT NULL
                  AND tumor_ordinal IS NOT NULL
            ),
            picked AS (
                SELECT * FROM ranked WHERE rn = 1
            )
            UPDATE {fq(target_schema, target_table)} AS m
            SET
                specimen_focus_id       = p.specimen_focus_id,
                linkage_confidence_tier = p.linkage_confidence_tier,
                linkage_score           = p.linkage_score
            FROM picked p
            WHERE TRY_CAST(m.research_id AS BIGINT) =
                  TRY_CAST(p.research_id AS BIGINT)
              AND m.surgery_episode_id = p.surgery_episode_id
              AND m.tumor_ordinal      = p.tumor_ordinal
        """
        con.execute(link_sql)
        n_linked = int(con.execute(
            f"SELECT COUNT(*) FROM {fq(target_schema, target_table)} "
            f"WHERE specimen_focus_id IS NOT NULL"
        ).fetchone()[0])
        log(f"  populated specimen_focus_id on {n_linked:,} rows from STF v1")
    else:
        log_warn("  STF v1 missing required join keys; linkage cols left NULL")

    # 1d. Provenance columns.
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
        f"    'canonical_tumor_characteristics_v1+tumor_episode_master_v2+specimen_tumor_focus_v1'"
    )

    try:
        con.execute(
            f"COMMENT ON TABLE {fq(target_schema, target_table)} IS "
            f"'[domain=operative_pathology; grain=per_tumor_per_surgery] — "
            f"source: {SCRIPT_TAG} ({RUN_DATE}); successor of "
            f"canonical_tumor_characteristics_v1 with discordance flags from "
            f"tumor_episode_master_v2 and linkage cols from "
            f"specimen_tumor_focus_v1.'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON {target_table} failed (non-fatal): {exc}")

    n_out = row_count(con, target_schema, target_table)
    p_out = distinct_research_ids(con, target_schema, target_table)
    log(f"  built {target_table}: {n_out:,} rows / {p_out:,} patients")
    return {"created": True, "rows": n_out, "patients": p_out}


# ---------------------------------------------------------------------------
# Step 2 — canonical_path_benign_events_v1
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Linkage helpers
# ---------------------------------------------------------------------------
#
# IMPORTANT NAMING NOTE (per code review).
#
# `path_synoptics` has NO native row pointer. The canonical `synoptic_row_ix`
# carried by `specimen_master_v1` and `specimen_tumor_focus_v1` was built in
# Script 108 as a global 1..N pandas-load-order index over the entire
# `path_synoptics` table (`np.arange(1, len(ps)+1)` after `reset_index`).
# That ordering is NOT a per-patient ROW_NUMBER and CANNOT be reproduced from
# SQL alone — re-loading or re-sorting `path_synoptics` would invalidate it.
#
# We therefore (a) NEVER synthesize a fake `synoptic_row_ix` and JOIN on it
# (the previous draft did this and silently misjoined every row), and (b)
# expose only `synoptic_row_ord` — a deterministic within-patient sequence
# that is clearly NOT the canonical `synoptic_row_ix`. Linkage to
# `specimen_master_v1` is done via `(research_id, surg_date)` with a
# deterministic 1-row pick per (rid, date).
#
# The `synoptic_row_ord` ORDER BY uses three tie-breakers
# (surg_date, content hash of source_text_type, then a stable column hash of
# the row's diagnostic fields) so the same path_synoptics row is reliably
# assigned the same `synoptic_row_ord` across re-runs.

# Deterministic tie-breaker columns for path_synoptics row ordering. Picked
# for (a) being present on every row (we include COALESCE wrappers below for
# defensiveness) and (b) carrying enough cardinality that ties are rare.
_PS_ROW_ORDER_TIE_COLS = [
    "synoptic_diagnosis",
    "tumor_1_histologic_type",
    "tumor_1_size_greatest_dimension_cm",
    "thyroid_procedure",
    "fs_pathology_frozen_section",
]


def _ps_row_order_sql(alias: str = "ps") -> str:
    """Build a deterministic ORDER BY clause for ROW_NUMBER over path_synoptics.

    The first column is always ``surg_date``; subsequent columns are the
    canonical tie-breakers (only those that exist on the live table — caller
    is responsible for ensuring this list is filtered).
    """
    parts = [f"{alias}.surg_date NULLS LAST"]
    for c in _PS_ROW_ORDER_TIE_COLS:
        parts.append(
            f"COALESCE(TRIM(CAST({alias}.{c} AS VARCHAR)), '') ASC"
        )
    return ", ".join(parts)


def _filter_existing_tie_cols(
    ps_cols: set[str],
) -> list[str]:
    """Drop tie-breaker columns the live table doesn't have (defensive)."""
    present = [c for c in _PS_ROW_ORDER_TIE_COLS if c in ps_cols]
    missing = [c for c in _PS_ROW_ORDER_TIE_COLS if c not in ps_cols]
    if missing:
        log_warn(
            f"  path_synoptics tie-breaker columns missing (ordering still "
            f"deterministic via remaining cols): {missing}"
        )
    return present


def _ps_row_order_sql_filtered(alias: str, present_tie_cols: list[str]) -> str:
    parts = [f"{alias}.surg_date NULLS LAST"]
    for c in present_tie_cols:
        parts.append(
            f"COALESCE(TRIM(CAST({alias}.{c} AS VARCHAR)), '') ASC"
        )
    return ", ".join(parts)


def _specimen_master_picked_cte_sql() -> str:
    """One row per (research_id, surg_date) from specimen_master_v1.

    When multiple specimens were collected on the same date, we pick the row
    with the lowest specimen_id (stable, deterministic). This loses some
    multi-specimen detail but eliminates the silent misjoin from the
    previous draft.

    NOTE: ``specimen_master_v1.procedure_date_day`` is VARCHAR and contains
    empty strings on some rows; live probe surfaced ``Conversion Error: invalid
    date field format: ""`` when using ``CAST``. We use ``TRY_CAST`` and filter
    out NULL results so unparseable rows are dropped from the picked set
    rather than aborting the whole build.
    """
    # NOTE on sm_n_specimens_for_date: a previous draft computed COUNT(*) AFTER
    # filtering to rn=1, which always returned 1. We use a window COUNT() OVER
    # the same partition so the value reflects the true number of specimens
    # collected on that (research_id, date), even after we keep only the
    # picked row.
    return f"""
        SELECT
            research_id,
            sm_date,
            surgery_episode_id,
            specimen_id,
            accession_or_source_id,
            sm_n_specimens_for_date
        FROM (
            SELECT
                sm.research_id,
                TRY_CAST(sm.procedure_date_day AS DATE)         AS sm_date,
                sm.surgery_episode_id,
                sm.specimen_id,
                sm.accession_or_source_id,
                COUNT(*) OVER (
                    PARTITION BY sm.research_id,
                                 TRY_CAST(sm.procedure_date_day AS DATE)
                )                                               AS sm_n_specimens_for_date,
                ROW_NUMBER() OVER (
                    PARTITION BY sm.research_id,
                                 TRY_CAST(sm.procedure_date_day AS DATE)
                    ORDER BY sm.specimen_id ASC NULLS LAST
                )                                               AS rn
            FROM {fq('main', 'specimen_master_v1')} sm
            WHERE sm.procedure_date_day IS NOT NULL
              AND TRY_CAST(sm.procedure_date_day AS DATE) IS NOT NULL
        ) ranked
        WHERE rn = 1
    """


def _placeholder_array_literal(cols: list[str]) -> str:
    """Render a Python list[str] as a DuckDB ``VARCHAR[]`` literal.

    Used to attach a per-row queryable list of placeholder-FALSE flag column
    names to the benign_events table (constant on every row but exposed so
    consumers can detect "this flag is structurally FALSE, not observed
    FALSE" without reading column comments).
    """
    if not cols:
        return "CAST([] AS VARCHAR[])"
    safe = []
    for c in cols:
        _validate_sql_identifier(c)
        safe.append(f"'{c}'")
    return f"CAST([{', '.join(safe)}] AS VARCHAR[])"


def _benign_flag_select(
    out_col: str, source_col: str | None, ps_alias: str = "ps"
) -> str:
    """Build the SELECT expression for one benign flag (Yes/No VARCHAR -> BOOL)."""
    _validate_sql_identifier(out_col)
    if source_col is None:
        return f"FALSE AS {out_col}"
    _validate_sql_identifier(source_col)
    return (
        f"CASE WHEN UPPER(TRIM(CAST({ps_alias}.{source_col} AS VARCHAR))) "
        f"IN ('Y','YES','TRUE','1','POS','POSITIVE') THEN TRUE "
        f"WHEN UPPER(TRIM(CAST({ps_alias}.{source_col} AS VARCHAR))) "
        f"IN ('N','NO','FALSE','0','NEG','NEGATIVE') THEN FALSE "
        f"WHEN {ps_alias}.{source_col} IS NULL "
        f"  OR TRIM(CAST({ps_alias}.{source_col} AS VARCHAR)) = '' THEN FALSE "
        f"ELSE FALSE END AS {out_col}"
    )


def step_2_build_benign_events(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 2 — Build main.canonical_path_benign_events_v1")
    log("=" * 78)
    target_schema, target_table = "main", "canonical_path_benign_events_v1"

    ps_cols = set(list_columns(con, "main", "path_synoptics"))
    if not ps_cols:
        raise RuntimeError("main.path_synoptics missing.")
    sm_cols = set(list_columns(con, "main", "specimen_master_v1"))

    # Validate every requested flag column; emit FALSE constant for any that
    # aren't on path_synoptics so downstream queries don't break. Track the
    # placeholder set so we can (a) log it, (b) attach per-column COMMENTs
    # that distinguish PLACEHOLDER-FALSE from observed-FALSE, and (c) emit a
    # queryable `placeholder_flag_cols VARCHAR[]` column on every row.
    resolved_benign: list[tuple[str, str | None]] = []
    placeholder_cols: list[str] = []
    for out_col, src in BENIGN_FLAG_MAP.items():
        if src is None:
            log_warn(
                f"  benign flag {out_col} has no path_synoptics source — "
                f"emitting PLACEHOLDER constant FALSE"
            )
            resolved_benign.append((out_col, None))
            placeholder_cols.append(out_col)
        elif src not in ps_cols:
            log_warn(
                f"  benign flag {out_col}: path_synoptics column {src!r} "
                f"missing — emitting PLACEHOLDER constant FALSE"
            )
            resolved_benign.append((out_col, None))
            placeholder_cols.append(out_col)
        else:
            resolved_benign.append((out_col, src))

    resolved_thy: list[tuple[str, str | None]] = []
    for out_col, src in THYROIDITIS_FLAG_MAP.items():
        if src is None or src not in ps_cols:
            log_warn(
                f"  thyroiditis flag {out_col}: source {src!r} missing — "
                f"emitting PLACEHOLDER constant FALSE"
            )
            resolved_thy.append((out_col, None))
            placeholder_cols.append(out_col)
        else:
            resolved_thy.append((out_col, src))

    if placeholder_cols:
        log(
            f"  benign_events placeholder columns "
            f"({len(placeholder_cols)}): {placeholder_cols}"
        )

    # Build the SELECT list.
    flag_select_parts: list[str] = []
    for out_col, src in resolved_benign + resolved_thy:
        flag_select_parts.append("            " + _benign_flag_select(out_col, src))

    # Linkage to specimen_master_v1 is via (research_id, surg_date) only — see
    # the IMPORTANT NAMING NOTE above. We never synthesize a fake
    # synoptic_row_ix and JOIN on it. `synoptic_row_ord` is exposed as a
    # within-patient sequence, NOT the canonical synoptic_row_ix.
    has_specimen_link = (
        "research_id" in sm_cols
        and "procedure_date_day" in sm_cols
        and "surgery_episode_id" in sm_cols
        and "specimen_id" in sm_cols
    )

    if not do_writes:
        log("  [dry-run] would CREATE OR REPLACE TABLE benign_events from "
            "path_synoptics (+ specimen_master_v1 link via rid+date)")
        return {"created": False, "rows": -1, "patients": -1}

    flag_sql = ",\n".join(flag_select_parts)
    present_tie_cols = _filter_existing_tie_cols(ps_cols)
    order_sql = _ps_row_order_sql_filtered("ps", present_tie_cols)

    if has_specimen_link:
        sm_cte = f"""
        sm_picked AS (
            {_specimen_master_picked_cte_sql()}
        ),"""
        sm_join_clause = (
            "LEFT JOIN sm_picked sm "
            "ON sm.research_id = TRY_CAST(ps.research_id AS BIGINT) "
            "AND sm.sm_date = CAST(ps.surg_date AS DATE)"
        )
        sm_select = (
            "sm.surgery_episode_id, sm.specimen_id, "
            "sm.accession_or_source_id, sm.sm_n_specimens_for_date"
        )
    else:
        sm_cte = ""
        sm_join_clause = ""
        sm_select = (
            "NULL::BIGINT AS surgery_episode_id, "
            "NULL::VARCHAR AS specimen_id, "
            "NULL::VARCHAR AS accession_or_source_id, "
            "NULL::BIGINT AS sm_n_specimens_for_date"
        )

    create_sql = f"""
        CREATE OR REPLACE TABLE {fq(target_schema, target_table)} AS
        WITH ps_keyed AS (
            SELECT
                ps.*,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(ps.research_id AS VARCHAR)
                    ORDER BY {order_sql}
                ) AS synoptic_row_ord
            FROM {fq('main', 'path_synoptics')} ps
        ),{sm_cte}
        joined AS (
            SELECT
                TRY_CAST(ps.research_id AS BIGINT) AS research_id,
                ps.synoptic_row_ord                AS synoptic_row_ord,
                {sm_select},
                CAST(ps.surg_date AS DATE)         AS path_date,
{flag_sql}
            FROM ps_keyed ps
            {sm_join_clause}
        )
        SELECT
            j.research_id,
            CAST(NULL AS BIGINT)                             AS synoptic_row_ix,
            j.synoptic_row_ord,
            j.surgery_episode_id,
            j.specimen_id,
            j.accession_or_source_id,
            j.path_date,
            j.sm_n_specimens_for_date,
            CASE
                WHEN j.surgery_episode_id IS NOT NULL
                     AND j.specimen_id IS NOT NULL THEN 'full'
                WHEN j.surgery_episode_id IS NOT NULL THEN 'specimen_only'
                WHEN j.specimen_id IS NOT NULL THEN 'synoptic_only'
                ELSE 'unlinked'
            END                                              AS linkage_quality,
            CASE
                WHEN COALESCE(j.sm_n_specimens_for_date, 0) > 1 THEN TRUE
                ELSE FALSE
            END                                              AS linkage_ambiguous_multi_specimen,
            'path_synoptics'::VARCHAR                        AS source_table,
            'path_synoptics_row'::VARCHAR                    AS source_text_type,
            CAST(j.synoptic_row_ord AS VARCHAR)              AS source_report_id,
            j.* EXCLUDE (
                research_id, synoptic_row_ord, surgery_episode_id, specimen_id,
                accession_or_source_id, sm_n_specimens_for_date, path_date
            ),
            {_placeholder_array_literal(placeholder_cols)}    AS placeholder_flag_cols,
            FALSE                                            AS has_concomitant_malignant_event,
            '{SCRIPT_ID}'::VARCHAR                           AS build_script,
            CURRENT_TIMESTAMP                                AS build_ts
        FROM joined j
        ORDER BY j.research_id, j.synoptic_row_ord
    """
    con.execute(create_sql)

    # Per-column COMMENT on each placeholder flag so introspection (data
    # dictionary, dbt docs, anyone reading information_schema) can tell at a
    # glance that FALSE here is "no upstream source" not "observed FALSE".
    for placeholder in placeholder_cols:
        try:
            con.execute(
                f"COMMENT ON COLUMN {fq(target_schema, target_table)}"
                f".{placeholder} IS "
                f"'PLACEHOLDER: no upstream column on path_synoptics provides "
                f"this signal. Emitted as constant FALSE on every row to keep "
                f"the table schema stable. NOT an observed-FALSE measurement; "
                f"do not interpret as a negative finding. See also "
                f"placeholder_flag_cols on this table.'"
            )
        except Exception as exc:
            log_warn(
                f"  COMMENT ON COLUMN {placeholder} failed (non-fatal): {exc}"
            )

    # Document why synoptic_row_ix is NULL on this table + the placeholder set.
    try:
        con.execute(
            f"COMMENT ON COLUMN {fq(target_schema, target_table)}"
            f".synoptic_row_ix IS "
            f"'Reserved for the canonical global pandas-load-order index used "
            f"by specimen_master_v1 / specimen_tumor_focus_v1 (built in Script "
            f"108). NULL here because that ordering cannot be reproduced from "
            f"SQL alone; use synoptic_row_ord for within-patient ordering and "
            f"join via (research_id, path_date) to specimen_master_v1.'"
        )
        con.execute(
            f"COMMENT ON COLUMN {fq(target_schema, target_table)}"
            f".synoptic_row_ord IS "
            f"'Within-patient deterministic synoptic sequence "
            f"(1..n per research_id), ordered by surg_date with content "
            f"tie-breakers. NOT the canonical synoptic_row_ix from Script 108.'"
        )
        con.execute(
            f"COMMENT ON COLUMN {fq(target_schema, target_table)}"
            f".linkage_ambiguous_multi_specimen IS "
            f"'TRUE when specimen_master_v1 had >1 specimen on (research_id, "
            f"path_date); we picked the lowest specimen_id deterministically "
            f"but the surgery_episode_id/specimen_id assignment is one of "
            f"several possible.'"
        )
        con.execute(
            f"COMMENT ON COLUMN {fq(target_schema, target_table)}"
            f".placeholder_flag_cols IS "
            f"'VARCHAR[] of nlp_* flag column names that were emitted as "
            f"PLACEHOLDER constant FALSE because no upstream column on "
            f"path_synoptics provides the signal. Same value on every row of "
            f"the table. Use ''<col>'' = ANY(placeholder_flag_cols) to test; "
            f"FALSE on a placeholder column does NOT mean observed-negative.'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON COLUMN failed (non-fatal): {exc}")

    # Update has_concomitant_malignant_event from the freshly-built malignant
    # events table.
    if table_exists(con, "main", "canonical_path_malignant_events_v1"):
        con.execute(
            f"UPDATE {fq(target_schema, target_table)} AS b "
            f"SET has_concomitant_malignant_event = TRUE "
            f"WHERE EXISTS ("
            f"  SELECT 1 FROM {fq('main', 'canonical_path_malignant_events_v1')} m "
            f"  WHERE TRY_CAST(m.research_id AS BIGINT) = b.research_id "
            f"    AND m.surgery_episode_id = b.surgery_episode_id "
            f"    AND b.surgery_episode_id IS NOT NULL"
            f")"
        )
        n_concomitant = int(con.execute(
            f"SELECT COUNT(*) FROM {fq(target_schema, target_table)} "
            f"WHERE has_concomitant_malignant_event"
        ).fetchone()[0])
        log(f"  flagged {n_concomitant:,} benign rows with concomitant malignancy")
    else:
        log_warn("  malignant events table missing; concomitant flag left FALSE")

    try:
        con.execute(
            f"COMMENT ON TABLE {fq(target_schema, target_table)} IS "
            f"'[domain=operative_pathology; grain=per_synoptic_report] — "
            f"source: {SCRIPT_TAG} ({RUN_DATE}); benign histology and "
            f"thyroiditis flags carried wide from path_synoptics with linkage "
            f"to specimen_master_v1 where available. has_concomitant_malignant_event "
            f"flags reports that share a surgery_episode_id with malignant_events.'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON {target_table} failed (non-fatal): {exc}")

    n_out = row_count(con, target_schema, target_table)
    p_out = distinct_research_ids(con, target_schema, target_table)
    log(f"  built {target_table}: {n_out:,} rows / {p_out:,} patients")
    return {"created": True, "rows": n_out, "patients": p_out}


# ---------------------------------------------------------------------------
# Step 3 — canonical_path_gland_events_v1 (unified thyroid + parathyroid)
# ---------------------------------------------------------------------------

def step_3_build_gland_events(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 3 — Build main.canonical_path_gland_events_v1 (unified)")
    log("=" * 78)
    target_schema, target_table = "main", "canonical_path_gland_events_v1"

    ps_cols = set(list_columns(con, "main", "path_synoptics"))
    sm_cols = set(list_columns(con, "main", "specimen_master_v1"))

    # Thyroid lobe slots: (position, size_cm_col, weight_g_col).
    thyroid_slots = [
        ("right",   "rl_size_cm",       "rl_g"),
        ("left",    "ll_size_cm",       "ll_g"),
        ("isthmus", "isthmus_size_cm",  "isthmus_g"),
        ("pyramidal", "pyramidal_lobe_cm", "pyramidal_g"),
        ("substernal", "substernal_goiter_size_cm", "substernal_g"),
        ("total",   "total_thyroid_size", "weight_total"),
    ]
    # Parathyroid slots 1..6: (position, size_col, weight_g_col, pathology_col,
    # location_col, biopsy_col, desc_col).
    parathyroid_slots = []
    for i in range(1, 7):
        parathyroid_slots.append((
            str(i),
            f"parag_{i}_size",
            f"parag_{i}_weight_grams",
            f"parag_{i}_cellularity",
            f"parag_{i}_location",
            f"parag_{i}_parathyroidectomy_excisional",
            f"parag_{i}_description",
        ))

    # Validate every slot column up-front and log misses (Issue 7 fix).
    # Same discipline as the benign-flag validation in Step 2: surface every
    # missing source column at build time so silent NULL emission can't hide
    # data-on-the-floor bugs.
    missing_thyroid: list[tuple[str, str, str]] = []
    for position, size_col, weight_col in thyroid_slots:
        if size_col not in ps_cols:
            missing_thyroid.append((position, "size", size_col))
        if weight_col not in ps_cols:
            missing_thyroid.append((position, "weight", weight_col))
    missing_para: list[tuple[str, str, str]] = []
    for slot in parathyroid_slots:
        position = slot[0]
        labels = ["size", "weight_g", "cellularity", "location",
                  "biopsy_or_excisional", "description"]
        for label, col in zip(labels, slot[1:]):
            if col not in ps_cols:
                missing_para.append((position, label, col))
    if missing_thyroid:
        log_warn(
            f"  thyroid-lobe slot columns missing on path_synoptics "
            f"({len(missing_thyroid)} missing) — those measurements will be "
            f"NULL for the affected slot:"
        )
        for pos, label, col in missing_thyroid:
            log_warn(f"    thyroid_lobe[{pos}].{label}: {col!r} not on path_synoptics")
    if missing_para:
        log_warn(
            f"  parathyroid slot columns missing on path_synoptics "
            f"({len(missing_para)} missing) — those measurements will be NULL "
            f"for the affected slot:"
        )
        for pos, label, col in missing_para:
            log_warn(f"    parathyroid[{pos}].{label}: {col!r} not on path_synoptics")
    if not missing_thyroid and not missing_para:
        log("  all gland slot columns validated against path_synoptics ✓")

    if not do_writes:
        log("  [dry-run] would build gland events long table from path_synoptics")
        return {"created": False, "rows": -1, "patients": -1,
                "missing_thyroid_slot_cols": len(missing_thyroid),
                "missing_parathyroid_slot_cols": len(missing_para)}

    # Linkage via (research_id, surg_date) — see IMPORTANT NAMING NOTE.
    has_link = (
        "research_id" in sm_cols
        and "procedure_date_day" in sm_cols
        and "surgery_episode_id" in sm_cols
        and "specimen_id" in sm_cols
    )
    if has_link:
        sm_cte = f"""
        sm_picked AS (
            {_specimen_master_picked_cte_sql()}
        ),"""
        sm_join_clause = (
            "LEFT JOIN sm_picked sm "
            "ON sm.research_id = TRY_CAST(ps.research_id AS BIGINT) "
            "AND sm.sm_date = CAST(ps.surg_date AS DATE)"
        )
        sm_join_select = "sm.surgery_episode_id, sm.specimen_id"
    else:
        sm_cte = ""
        sm_join_clause = ""
        sm_join_select = (
            "NULL::BIGINT AS surgery_episode_id, NULL::VARCHAR AS specimen_id"
        )

    present_tie_cols = _filter_existing_tie_cols(ps_cols)
    order_sql = _ps_row_order_sql_filtered("ps", present_tie_cols)

    # Build per-slot SELECTs.
    thyroid_unions: list[str] = []
    for position, size_col, weight_col in thyroid_slots:
        size_present = size_col in ps_cols
        weight_present = weight_col in ps_cols
        size_expr = (
            f"TRY_CAST(REGEXP_EXTRACT(CAST(ps.{size_col} AS VARCHAR), "
            f"'([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)"
            if size_present else "NULL::DOUBLE"
        )
        weight_expr = (
            f"TRY_CAST(REGEXP_EXTRACT(CAST(ps.{weight_col} AS VARCHAR), "
            f"'([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)"
            if weight_present else "NULL::DOUBLE"
        )
        notes_parts = []
        if size_present:
            notes_parts.append(
                f"CASE WHEN ps.{size_col} IS NOT NULL THEN "
                f"'size_raw=' || CAST(ps.{size_col} AS VARCHAR) END"
            )
        if weight_present:
            notes_parts.append(
                f"CASE WHEN ps.{weight_col} IS NOT NULL THEN "
                f"'weight_raw=' || CAST(ps.{weight_col} AS VARCHAR) END"
            )
        # NULLIF on CONCAT_WS — without this, an all-NULL parts list yields
        # '' (not NULL) and the downstream "all NULL → drop" WHERE filter
        # leaves the row in. Caused the 140K-row overcount in the first run.
        notes_expr = (
            f"NULLIF(CONCAT_WS('; ', {', '.join(notes_parts)}), '')"
            if notes_parts else "NULL::VARCHAR"
        )
        thyroid_unions.append(f"""
            SELECT
                TRY_CAST(ps.research_id AS BIGINT)             AS research_id,
                {sm_join_select},
                ps.synoptic_row_ord                            AS synoptic_row_ord,
                CAST(ps.surg_date AS DATE)                     AS path_date,
                'thyroid_lobe'::VARCHAR                        AS gland_type,
                '{position}'::VARCHAR                          AS gland_position,
                {size_expr}                                    AS gland_length_cm,
                NULL::DOUBLE                                   AS gland_width_cm,
                NULL::DOUBLE                                   AS gland_depth_cm,
                {weight_expr}                                  AS gland_weight_g,
                NULL::DOUBLE                                   AS gland_weight_mg,
                NULL::VARCHAR                                  AS gland_pathology,
                {notes_expr}                                   AS gland_notes,
                'operative'::VARCHAR                           AS specimen_type,
                'path_synoptics'::VARCHAR                      AS source_table
            FROM ps_keyed ps
            {sm_join_clause}
        """)

    parathyroid_unions: list[str] = []
    for slot in parathyroid_slots:
        position, size_col, weight_col, path_col, loc_col, biopsy_col, desc_col = slot
        size_present = size_col in ps_cols
        weight_present = weight_col in ps_cols
        path_present = path_col in ps_cols
        loc_present = loc_col in ps_cols
        biopsy_present = biopsy_col in ps_cols
        desc_present = desc_col in ps_cols
        size_expr = (
            f"TRY_CAST(REGEXP_EXTRACT(CAST(ps.{size_col} AS VARCHAR), "
            f"'([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)"
            if size_present else "NULL::DOUBLE"
        )
        weight_g_expr = "NULL::DOUBLE"
        weight_mg_expr = (
            f"TRY_CAST(REGEXP_EXTRACT(CAST(ps.{weight_col} AS VARCHAR), "
            f"'([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)"
            if weight_present else "NULL::DOUBLE"
        )
        pathology_expr = (
            f"NULLIF(TRIM(CAST(ps.{path_col} AS VARCHAR)), '')"
            if path_present else "NULL::VARCHAR"
        )
        notes_parts = []
        if loc_present:
            notes_parts.append(
                f"CASE WHEN ps.{loc_col} IS NOT NULL THEN "
                f"'location=' || CAST(ps.{loc_col} AS VARCHAR) END"
            )
        if biopsy_present:
            notes_parts.append(
                f"CASE WHEN ps.{biopsy_col} IS NOT NULL THEN "
                f"'procedure=' || CAST(ps.{biopsy_col} AS VARCHAR) END"
            )
        if desc_present:
            notes_parts.append(
                f"CASE WHEN ps.{desc_col} IS NOT NULL THEN "
                f"'desc=' || CAST(ps.{desc_col} AS VARCHAR) END"
            )
        notes_expr = (
            f"NULLIF(CONCAT_WS('; ', {', '.join(notes_parts)}), '')"
            if notes_parts else "NULL::VARCHAR"
        )
        parathyroid_unions.append(f"""
            SELECT
                TRY_CAST(ps.research_id AS BIGINT)             AS research_id,
                {sm_join_select},
                ps.synoptic_row_ord                            AS synoptic_row_ord,
                CAST(ps.surg_date AS DATE)                     AS path_date,
                'parathyroid'::VARCHAR                         AS gland_type,
                '{position}'::VARCHAR                          AS gland_position,
                {size_expr}                                    AS gland_length_cm,
                NULL::DOUBLE                                   AS gland_width_cm,
                NULL::DOUBLE                                   AS gland_depth_cm,
                {weight_g_expr}                                AS gland_weight_g,
                {weight_mg_expr}                               AS gland_weight_mg,
                {pathology_expr}                               AS gland_pathology,
                {notes_expr}                                   AS gland_notes,
                'operative'::VARCHAR                           AS specimen_type,
                'path_synoptics'::VARCHAR                      AS source_table
            FROM ps_keyed ps
            {sm_join_clause}
        """)

    all_unions = "\nUNION ALL\n".join(thyroid_unions + parathyroid_unions)
    create_sql = f"""
        CREATE OR REPLACE TABLE {fq(target_schema, target_table)} AS
        WITH ps_keyed AS (
            SELECT
                ps.*,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(ps.research_id AS VARCHAR)
                    ORDER BY {order_sql}
                ) AS synoptic_row_ord
            FROM {fq('main', 'path_synoptics')} ps
        ),{sm_cte}
        unioned AS (
            {all_unions}
        )
        SELECT
            research_id,
            surgery_episode_id,
            CAST(NULL AS BIGINT)        AS synoptic_row_ix,
            synoptic_row_ord,
            specimen_id,
            path_date,
            CASE
                WHEN surgery_episode_id IS NOT NULL
                     AND specimen_id IS NOT NULL THEN 'full'
                WHEN surgery_episode_id IS NOT NULL THEN 'specimen_only'
                WHEN specimen_id IS NOT NULL THEN 'synoptic_only'
                ELSE 'unlinked'
            END AS linkage_quality,
            gland_type,
            gland_position,
            gland_length_cm,
            gland_width_cm,
            gland_depth_cm,
            gland_weight_g,
            gland_weight_mg,
            gland_pathology,
            gland_notes,
            specimen_type,
            source_table,
            '{SCRIPT_ID}'::VARCHAR AS build_script,
            CURRENT_TIMESTAMP      AS build_ts
        FROM unioned
        WHERE NOT (
            gland_length_cm IS NULL
            AND gland_width_cm IS NULL
            AND gland_depth_cm IS NULL
            AND gland_weight_g IS NULL
            AND gland_weight_mg IS NULL
            AND gland_pathology IS NULL
            AND gland_notes IS NULL
        )
        ORDER BY research_id, synoptic_row_ord, gland_type, gland_position
    """
    con.execute(create_sql)

    # Same column-comment treatment as benign_events.
    try:
        con.execute(
            f"COMMENT ON COLUMN {fq(target_schema, target_table)}"
            f".synoptic_row_ix IS "
            f"'Reserved for the canonical global pandas-load-order index used "
            f"by specimen_master_v1 / specimen_tumor_focus_v1 (built in Script "
            f"108). NULL here because that ordering cannot be reproduced from "
            f"SQL alone; use synoptic_row_ord for within-patient ordering and "
            f"join via (research_id, path_date) to specimen_master_v1.'"
        )
        con.execute(
            f"COMMENT ON COLUMN {fq(target_schema, target_table)}"
            f".synoptic_row_ord IS "
            f"'Within-patient deterministic synoptic sequence "
            f"(1..n per research_id), ordered by surg_date with content "
            f"tie-breakers. NOT the canonical synoptic_row_ix from Script 108.'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON COLUMN failed (non-fatal): {exc}")

    # Validate gland_type domain.
    bad = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(target_schema, target_table)} "
        f"WHERE gland_type NOT IN ('thyroid_lobe', 'parathyroid')"
    ).fetchone()[0])
    if bad:
        raise RuntimeError(
            f"gland_events: {bad:,} rows have gland_type outside the allowed "
            f"domain. Aborting."
        )

    try:
        con.execute(
            f"COMMENT ON TABLE {fq(target_schema, target_table)} IS "
            f"'[domain=operative_pathology; grain=per_gland_per_surgery] — "
            f"source: {SCRIPT_TAG} ({RUN_DATE}); long-format unification of "
            f"thyroid lobes (left/right/isthmus/pyramidal/substernal/total) and "
            f"parathyroid glands 1-6 from path_synoptics. Rows where every "
            f"measurement and pathology field is NULL are dropped.'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON {target_table} failed (non-fatal): {exc}")

    n_out = row_count(con, target_schema, target_table)
    p_out = distinct_research_ids(con, target_schema, target_table)
    n_thy = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(target_schema, target_table)} "
        f"WHERE gland_type = 'thyroid_lobe'"
    ).fetchone()[0])
    n_par = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(target_schema, target_table)} "
        f"WHERE gland_type = 'parathyroid'"
    ).fetchone()[0])
    log(f"  built {target_table}: {n_out:,} rows / {p_out:,} patients "
        f"(thyroid={n_thy:,}, parathyroid={n_par:,})")
    return {"created": True, "rows": n_out, "patients": p_out,
            "thyroid_rows": n_thy, "parathyroid_rows": n_par}


# ---------------------------------------------------------------------------
# Step 5 — Patient rollups
# ---------------------------------------------------------------------------

def step_5_build_rollups(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 5 — Build the 3 patient-grain rollup tables")
    log("=" * 78)
    if not do_writes:
        log("  [dry-run] would build 3 patient rollups")
        return {"created": False}

    out: dict[str, Any] = {}

    # 5a. Malignant rollup with merge from path_outcome_classification_v1.
    poc_present = table_exists(con, "main", "path_outcome_classification_v1")
    poc_join = ""
    poc_select = ""
    if poc_present:
        poc_select = """,
            poc.bethesda_max                AS bethesda_final,
            poc.bethesda_final_name,
            poc.regex_classification        AS regex_path_outcome,
            poc.tumor_1_histologic_type     AS poc_tumor_1_histologic_type"""
        poc_join = f"""
        LEFT JOIN (
            SELECT
                research_id,
                MAX(bethesda_final)             AS bethesda_max,
                ANY_VALUE(bethesda_final_name)  AS bethesda_final_name,
                ANY_VALUE(regex_classification) AS regex_classification,
                ANY_VALUE(tumor_1_histologic_type) AS tumor_1_histologic_type
            FROM {fq('main', 'path_outcome_classification_v1')}
            GROUP BY research_id
        ) poc ON poc.research_id = ev.research_id
        """
    else:
        log_warn("  path_outcome_classification_v1 not found — bethesda fields "
                 "will be omitted from malignant rollup")

    con.execute(f"""
        CREATE OR REPLACE TABLE {fq('main', 'canonical_path_malignant_patient_rollup_v1')} AS
        WITH ev AS (
            SELECT
                TRY_CAST(research_id AS BIGINT)         AS research_id,
                surgery_episode_id,
                surgery_date,
                primary_histology,
                extrathyroidal_extension,
                gross_ete,
                stage_group_ajcc7,
                stage_group_ajcc8
            FROM {fq('main', 'canonical_path_malignant_events_v1')}
        ),
        agg AS (
            SELECT
                research_id,
                TRUE                                    AS any_malignant_event,
                COUNT(DISTINCT surgery_episode_id)      AS n_malignant_surgeries,
                COUNT(*)                                AS n_tumors_total,
                MIN(surgery_date)                       AS earliest_malignant_path_date,
                MAX(surgery_date)                       AS latest_malignant_path_date,
                MAX(stage_group_ajcc8)                  AS highest_stage_ajcc8,
                MAX(stage_group_ajcc7)                  AS highest_stage_ajcc7,
                BOOL_OR(
                    -- gross_ete is BIGINT (NULL or 1)
                    COALESCE(gross_ete, 0) = 1
                    -- extrathyroidal_extension is VARCHAR; positive values
                    -- include 'present', 'minimal', 'microscopic', 'yes',
                    -- 'c/a' (continuous activity); 'x' = not assessed and
                    -- 'false'/'no'/'none'/empty = negative
                    OR LOWER(COALESCE(CAST(extrathyroidal_extension AS VARCHAR), ''))
                       IN ('present', 'minimal', 'microscopic', 'yes', 'c/a',
                           'gross', 'macroscopic')
                )                                       AS any_ett,
                mode(primary_histology)                 AS dominant_histology
            FROM ev
            GROUP BY research_id
        )
        SELECT
            ev.research_id,
            ev.any_malignant_event,
            ev.n_malignant_surgeries,
            ev.n_tumors_total,
            ev.earliest_malignant_path_date,
            ev.latest_malignant_path_date,
            ev.highest_stage_ajcc8,
            ev.highest_stage_ajcc7,
            ev.any_ett,
            FALSE                                       AS any_metastasis,
            ev.dominant_histology{poc_select},
            '{SCRIPT_ID}'::VARCHAR                      AS build_script,
            CURRENT_TIMESTAMP                           AS build_ts
        FROM agg ev{poc_join}
    """)
    n_m = row_count(con, 'main', 'canonical_path_malignant_patient_rollup_v1')
    log(f"  built canonical_path_malignant_patient_rollup_v1: {n_m:,} rows")
    out["malignant_rollup_rows"] = n_m

    # 5b. Benign rollup.
    con.execute(f"""
        CREATE OR REPLACE TABLE {fq('main', 'canonical_path_benign_patient_rollup_v1')} AS
        WITH ev AS (
            SELECT * FROM {fq('main', 'canonical_path_benign_events_v1')}
        )
        SELECT
            research_id,
            BOOL_OR(
                COALESCE(nlp_mng, FALSE)
                OR COALESCE(nlp_multinodular_goiter, FALSE)
                OR COALESCE(nlp_follicular_adenoma, FALSE)
                OR COALESCE(nlp_hurthle_cell_adenoma, FALSE)
                OR COALESCE(nlp_hashimotos, FALSE)
                OR COALESCE(nlp_hashimotos_thyroiditis, FALSE)
                OR COALESCE(nlp_lymphocytic_thyroiditis, FALSE)
                OR COALESCE(nlp_chronic_lymphocytic_thyroiditis, FALSE)
                OR COALESCE(nlp_graves, FALSE)
                OR COALESCE(nlp_graves_disease, FALSE)
                OR COALESCE(nlp_adenomatoid_nodule, FALSE)
                OR COALESCE(nlp_colloid_nodule, FALSE)
                OR COALESCE(nlp_hyperplasia, FALSE)
                OR COALESCE(nlp_nodular_hyperplasia, FALSE)
            )                                                        AS any_benign_event,
            COUNT(*)                                                 AS n_benign_synoptics,
            BOOL_OR(COALESCE(nlp_mng, FALSE)
                    OR COALESCE(nlp_multinodular_goiter, FALSE))     AS any_mng,
            BOOL_OR(COALESCE(nlp_hashimotos, FALSE)
                    OR COALESCE(nlp_hashimotos_thyroiditis, FALSE))  AS any_hashimotos,
            BOOL_OR(COALESCE(nlp_lymphocytic_thyroiditis, FALSE)
                    OR COALESCE(nlp_chronic_lymphocytic_thyroiditis, FALSE))
                                                                     AS any_lymphocytic_thyroiditis,
            BOOL_OR(COALESCE(nlp_graves, FALSE)
                    OR COALESCE(nlp_graves_disease, FALSE))          AS any_graves,
            BOOL_OR(COALESCE(nlp_follicular_adenoma, FALSE))         AS any_follicular_adenoma,
            MIN(path_date)                                           AS earliest_benign_path_date,
            MAX(path_date)                                           AS latest_benign_path_date,
            BOOL_OR(COALESCE(has_concomitant_malignant_event, FALSE)) AS any_concomitant_malignant,
            '{SCRIPT_ID}'::VARCHAR                                   AS build_script,
            CURRENT_TIMESTAMP                                        AS build_ts
        FROM ev
        GROUP BY research_id
    """)
    n_b = row_count(con, 'main', 'canonical_path_benign_patient_rollup_v1')
    log(f"  built canonical_path_benign_patient_rollup_v1: {n_b:,} rows")
    out["benign_rollup_rows"] = n_b

    # 5c. Gland rollup.
    con.execute(f"""
        CREATE OR REPLACE TABLE {fq('main', 'canonical_path_gland_patient_rollup_v1')} AS
        WITH ev AS (
            SELECT * FROM {fq('main', 'canonical_path_gland_events_v1')}
        ),
        latest_surgery AS (
            SELECT
                research_id,
                MAX(path_date) AS latest_path_date
            FROM ev
            WHERE gland_type = 'thyroid_lobe'
            GROUP BY research_id
        ),
        thy_latest AS (
            SELECT
                e.research_id,
                SUM(e.gland_weight_g) AS total_thyroid_weight_g_latest
            FROM ev e
            JOIN latest_surgery ls
              ON e.research_id = ls.research_id
             AND e.path_date   = ls.latest_path_date
            WHERE e.gland_type = 'thyroid_lobe'
            GROUP BY e.research_id
        )
        SELECT
            ev.research_id,
            BOOL_OR(ev.gland_type = 'thyroid_lobe')                  AS any_thyroid_lobe_measured,
            ANY_VALUE(t.total_thyroid_weight_g_latest)               AS total_thyroid_weight_g,
            MAX(CASE WHEN ev.gland_type = 'thyroid_lobe'
                          AND ev.gland_position = 'left'
                     THEN ev.gland_length_cm END)                    AS left_lobe_max_dim_cm,
            MAX(CASE WHEN ev.gland_type = 'thyroid_lobe'
                          AND ev.gland_position = 'right'
                     THEN ev.gland_length_cm END)                    AS right_lobe_max_dim_cm,
            BOOL_OR(ev.gland_type = 'thyroid_lobe'
                    AND ev.gland_position = 'isthmus'
                    AND (ev.gland_length_cm IS NOT NULL
                         OR ev.gland_weight_g IS NOT NULL))          AS any_isthmus_documented,
            BOOL_OR(ev.gland_type = 'parathyroid')                   AS any_parathyroid_documented,
            COUNT(DISTINCT CASE WHEN ev.gland_type = 'parathyroid'
                                THEN ev.gland_position END)          AS n_parathyroid_glands_documented,
            COUNT(DISTINCT CASE WHEN ev.gland_type = 'parathyroid'
                                     AND (
                                         LOWER(COALESCE(ev.gland_pathology, '')) LIKE '%hyperplasia%'
                                         OR LOWER(COALESCE(ev.gland_pathology, '')) LIKE '%adenoma%'
                                         OR LOWER(COALESCE(ev.gland_pathology, '')) LIKE '%hypercellular%'
                                     )
                                THEN ev.gland_position END)          AS n_parathyroid_glands_abnormal,
            BOOL_OR(ev.gland_type = 'parathyroid'
                    AND LOWER(COALESCE(ev.gland_pathology, '')) LIKE '%hyperplasia%')
                                                                     AS any_parathyroid_hyperplasia,
            BOOL_OR(ev.gland_type = 'parathyroid'
                    AND LOWER(COALESCE(ev.gland_pathology, '')) LIKE '%adenoma%')
                                                                     AS any_parathyroid_adenoma,
            '{SCRIPT_ID}'::VARCHAR                                   AS build_script,
            CURRENT_TIMESTAMP                                        AS build_ts
        FROM ev
        LEFT JOIN thy_latest t ON t.research_id = ev.research_id
        GROUP BY ev.research_id
    """)
    n_g = row_count(con, 'main', 'canonical_path_gland_patient_rollup_v1')
    log(f"  built canonical_path_gland_patient_rollup_v1: {n_g:,} rows")
    out["gland_rollup_rows"] = n_g

    return out


# ---------------------------------------------------------------------------
# Step 6 — Views
# ---------------------------------------------------------------------------

def step_6_build_views(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 6 — Create / refresh views_readable views")
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
    return {"views": out}


# ---------------------------------------------------------------------------
# Step 7 — Drop deprecated tables
# ---------------------------------------------------------------------------

def step_7_drop_deprecated(
    con: duckdb.DuckDBPyConnection,
    do_writes: bool,
    archive_counts: dict[str, int],
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 7 — Drop deprecated tables")
    log("=" * 78)

    # Pre-flight: confirm all replacement tables exist with non-zero rows.
    for sch, tbl in NEW_EVENT_TABLES + NEW_ROLLUP_TABLES:
        if not table_exists(con, sch, tbl):
            raise RuntimeError(
                f"Refusing to drop deprecated tables: replacement {sch}.{tbl} "
                f"does not exist."
            )
        n = row_count(con, sch, tbl)
        if n == 0:
            raise RuntimeError(
                f"Refusing to drop: replacement {sch}.{tbl} is empty."
            )

    # Pre-flight: confirm archive copies exist with matching counts.
    for sch, tbl in DEPRECATED_TABLES:
        live_n = archive_counts.get(f"{sch}.{tbl}")
        archive_name = f"{tbl}_pre361_{BUILD_TS}"
        archive_fq_name = f'{ARCHIVE_FQ}."{archive_name}"'
        try:
            arch_n = int(con.execute(
                f"SELECT COUNT(*) FROM {archive_fq_name}"
            ).fetchone()[0])
        except duckdb.Error as exc:
            raise RuntimeError(
                f"Archive {archive_name} unreadable: {exc}. Refusing to drop."
            ) from exc
        if live_n is not None and arch_n != live_n:
            raise RuntimeError(
                f"Archive {archive_name} has {arch_n:,} rows but pre-archive "
                f"live had {live_n:,}. Refusing to drop {sch}.{tbl}."
            )

    # Find dependent views in views_readable that reference the targets.
    dep_views: list[tuple[str, str]] = []
    targets = {tbl for _, tbl in DEPRECATED_TABLES}
    try:
        rows = con.execute(
            "SELECT view_schema, view_name FROM information_schema.view_table_usage "
            "WHERE view_catalog = ? AND table_schema = 'main'",
            [CANONICAL_DB],
        ).fetchall()
        for vs, vn in rows:
            # Re-inspect each view definition to filter to ones using a target.
            try:
                ddl = con.execute(
                    "SELECT sql FROM duckdb_views() "
                    "WHERE database_name = ? AND schema_name = ? AND view_name = ?",
                    [CANONICAL_DB, vs, vn],
                ).fetchone()
                ddl_str = (ddl[0] if ddl else "") or ""
                if any(t in ddl_str for t in targets):
                    dep_views.append((vs, vn))
            except duckdb.Error:
                continue
    except duckdb.Error as exc:
        log_warn(f"  view dependency lookup failed (non-fatal): {exc}")

    if dep_views:
        log(f"  dependent views to drop: {len(dep_views)}")
        for vs, vn in dep_views:
            # Skip our own freshly-created views.
            if vs == VIEW_SCHEMA and vn in {v[0] for v in NEW_VIEWS}:
                continue
            log(f"    DROP VIEW {vs}.{vn}")
            if do_writes:
                con.execute(
                    f'DROP VIEW IF EXISTS "{CANONICAL_DB}"."{vs}"."{vn}"'
                )

    dropped: list[str] = []
    for sch, tbl in DEPRECATED_TABLES:
        if not table_exists(con, sch, tbl):
            log(f"  {sch}.{tbl} already absent")
            continue
        log(f"  DROP TABLE {sch}.{tbl}")
        if do_writes:
            con.execute(f"DROP TABLE {fq(sch, tbl)}")
        dropped.append(f"{sch}.{tbl}")
    return {"dropped": dropped, "dependent_views": [f"{s}.{v}" for s, v in dep_views]}


# ---------------------------------------------------------------------------
# Step 8 — Registry sync
# ---------------------------------------------------------------------------

def step_8_registry_sync(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 8 — detail_table_registry_v1 sync")
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

    # Build INSERT for the 6 new canonicals.
    new_rows = NEW_EVENT_TABLES + NEW_ROLLUP_TABLES
    insert_records: list[dict[str, Any]] = []
    for sch, tbl in new_rows:
        n = row_count(con, sch, tbl) if table_exists(con, sch, tbl) else 0
        p = (
            distinct_research_ids(con, sch, tbl)
            if table_exists(con, sch, tbl) else 0
        )
        grain = (
            "per_tumor_per_surgery" if "malignant_events" in tbl
            else "per_synoptic_report" if "benign_events" in tbl
            else "per_gland_per_surgery" if "gland_events" in tbl
            else "per_patient"
        )
        rec: dict[str, Any] = {
            "detail_table_name":          tbl,
            "schema_name":                sch,
            "join_key":                   "research_id",
            "grain":                      grain,
            "total_rows":                 n,
            "total_patients":             p,
            "domain":                     "operative_pathology",
            "feeds_master_columns":       None,
            "description":
                f"[domain=operative_pathology; grain={grain}] — source: "
                f"{SCRIPT_TAG} ({RUN_DATE}). Rows={n}, patients={p}.",
            "canonical_version":          f"v1_0_script{SCRIPT_ID}",
            "feeds_master_columns_secondary": None,
            "feeds_master_columns_array": None,
            "needs_manual_review":        False,
        }
        # Fill only columns the registry actually has, in registry order.
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
# Step 9 — CPM feeder audit (read-only)
# ---------------------------------------------------------------------------

def step_9_cpm_feeder_audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 9 — CPM feeder audit (report only)")
    log("=" * 78)
    if not table_exists(con, "main", "canonical_patient_master"):
        log_warn("  canonical_patient_master missing — skipping CPM audit")
        return {"audit_rows": [], "report_path": None}

    cpm_cols = list_columns(con, "main", "canonical_patient_master")
    nlp_cols = [c for c in cpm_cols if c.startswith("nlp_")]
    log(f"  CPM has {len(cpm_cols)} total columns, {len(nlp_cols)} nlp_* columns")

    # Heuristic mapping of CPM nlp_* names -> deprecated source table that
    # likely fed them (based on column naming alignment).
    targets = [t for _, t in DEPRECATED_TABLES]
    audit_rows: list[dict[str, Any]] = []

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

    # Match nlp_* columns to deprecated table columns by name overlap.
    for sch, tbl in DEPRECATED_TABLES:
        if not table_exists(con, sch, tbl):
            # Already dropped — try archive name from this build.
            continue
        src_cols = set(list_columns(con, sch, tbl))
        for nc in nlp_cols:
            base = nc[len("nlp_"):]
            for sc in src_cols:
                if base == sc.lower() or base in sc.lower() or sc.lower().endswith(base):
                    audit_rows.append({
                        "cpm_column": nc,
                        "likely_feeder_table": f"{sch}.{tbl}",
                        "matched_source_column": sc,
                    })
                    break

    # Write report.
    md_lines = [
        f"# CPM feeder audit — {SCRIPT_TAG} ({RUN_DATE})",
        "",
        "Read-only audit produced by Step 9 of Script 361. Identifies CPM "
        "`nlp_*` columns that may be sourced from one of the 7 deprecated "
        "operative-pathology tables. A follow-up script must repoint these "
        "feeders to the new canonical event/rollup tables.",
        "",
        f"**CPM total columns:** {len(cpm_cols)} | **nlp_\\* columns:** {len(nlp_cols)}",
        "",
        "## Per-table grep hits (`git grep -l <table> -- scripts/`)",
        "",
        "| deprecated table | feeder script files |",
        "|---|---|",
    ]
    for tbl, files in grep_hits.items():
        if files:
            md_lines.append(f"| `{tbl}` | {', '.join(f'`{f}`' for f in files[:10])}"
                            + (f" (+{len(files)-10} more)" if len(files) > 10 else "")
                            + " |")
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
# Step 10 — Zero-drift QA
# ---------------------------------------------------------------------------

def step_10_qa(
    con: duckdb.DuckDBPyConnection,
    archive_counts: dict[str, int],
    pre_drop: bool,
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 10 — Zero-drift QA")
    log("=" * 78)
    qa: dict[str, Any] = {"checks": [], "passed": True}

    def check(name: str, passed: bool, **details: Any) -> None:
        qa["checks"].append({"name": name, "passed": bool(passed), **details})
        log(f"  QA {'PASS' if passed else 'FAIL'} {name}: {details}")
        if not passed:
            qa["passed"] = False

    # Patient denominators: malignant rollup any_malignant_event TRUE count
    # should match prior canonical_malignant_diagnosis_v1 patient count exactly.
    if table_exists(con, "main", "canonical_path_malignant_patient_rollup_v1"):
        n_mp = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main', 'canonical_path_malignant_patient_rollup_v1')} "
            f"WHERE any_malignant_event"
        ).fetchone()[0])
    else:
        n_mp = -1
    legacy_mp = archive_counts.get("main.canonical_malignant_diagnosis_v1")
    # Use distinct research_id from the archive to be exact.
    archive_mp = None
    arch_name = f"canonical_malignant_diagnosis_v1_pre361_{BUILD_TS}"
    try:
        arch_fq = f'{ARCHIVE_FQ}."{arch_name}"'
        archive_mp = int(con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM {arch_fq}"
        ).fetchone()[0])
    except duckdb.Error:
        archive_mp = None
    expected_mp = archive_mp if archive_mp is not None else legacy_mp
    check(
        "malignant_patient_count_matches_legacy",
        n_mp >= 0 and expected_mp is not None and n_mp == expected_mp,
        rollup_count=n_mp,
        archive_count=archive_mp,
        legacy_row_count=legacy_mp,
    )

    # Benign rollup any_benign_event TRUE >= legacy benign_diagnosis patients.
    if table_exists(con, "main", "canonical_path_benign_patient_rollup_v1"):
        n_bp = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main', 'canonical_path_benign_patient_rollup_v1')} "
            f"WHERE any_benign_event"
        ).fetchone()[0])
    else:
        n_bp = -1
    archive_bp = None
    arch_b = f"canonical_benign_diagnosis_v1_pre361_{BUILD_TS}"
    try:
        archive_bp = int(con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM "
            f'{ARCHIVE_FQ}."{arch_b}"'
        ).fetchone()[0])
    except duckdb.Error:
        archive_bp = None
    check(
        "benign_patient_count_geq_legacy",
        n_bp >= 0 and archive_bp is not None and n_bp >= archive_bp,
        rollup_count=n_bp,
        archive_count=archive_bp,
    )

    # Row count on malignant_events == archive of canonical_tumor_characteristics_v1.
    if table_exists(con, "main", "canonical_path_malignant_events_v1"):
        n_me = row_count(con, "main", "canonical_path_malignant_events_v1")
    else:
        n_me = -1
    arch_t = f"canonical_tumor_characteristics_v1_pre361_{BUILD_TS}"
    try:
        archive_me = int(con.execute(
            f'SELECT COUNT(*) FROM {ARCHIVE_FQ}."{arch_t}"'
        ).fetchone()[0])
    except duckdb.Error:
        archive_me = None
    check(
        "malignant_events_rowcount_matches_ctc_archive",
        n_me >= 0 and archive_me is not None and n_me == archive_me,
        events_count=n_me,
        archive_count=archive_me,
    )

    # No research_id values lost: union of distinct rids before vs after.
    try:
        # "Before": research_ids from the seven archived tables.
        archived_tables_sql = " UNION ALL ".join(
            f'SELECT TRY_CAST(research_id AS BIGINT) AS rid FROM '
            f'{ARCHIVE_FQ}."{tbl}_pre361_{BUILD_TS}"'
            for _, tbl in DEPRECATED_TABLES
        )
        before_rids = {
            r[0] for r in con.execute(
                f"SELECT DISTINCT rid FROM ({archived_tables_sql}) WHERE rid IS NOT NULL"
            ).fetchall()
        }
        # "After": research_ids from the six new canonicals.
        new_tables_sql = " UNION ALL ".join(
            f'SELECT TRY_CAST(research_id AS BIGINT) AS rid '
            f'FROM {fq(sch, tbl)}'
            for sch, tbl in NEW_EVENT_TABLES + NEW_ROLLUP_TABLES
            if table_exists(con, sch, tbl)
        )
        after_rids = {
            r[0] for r in con.execute(
                f"SELECT DISTINCT rid FROM ({new_tables_sql}) WHERE rid IS NOT NULL"
            ).fetchall()
        }
        lost = before_rids - after_rids
        check(
            "no_research_ids_lost",
            len(lost) == 0,
            n_before=len(before_rids),
            n_after=len(after_rids),
            n_lost=len(lost),
            sample_lost=sorted(lost)[:20],
        )
    except duckdb.Error as exc:
        check("no_research_ids_lost", False, error=str(exc))

    # Path-outcome preservation: bethesda_final non-null on malignant rollup
    # equals count from archived path_outcome_classification_v1.
    arch_poc = f"path_outcome_classification_v1_pre361_{BUILD_TS}"
    try:
        archive_poc_cnt = int(con.execute(
            f'SELECT COUNT(*) FROM {ARCHIVE_FQ}."{arch_poc}" '
            f"WHERE bethesda_final IS NOT NULL"
        ).fetchone()[0])
    except duckdb.Error:
        archive_poc_cnt = None
    if (
        archive_poc_cnt is not None
        and table_exists(con, "main", "canonical_path_malignant_patient_rollup_v1")
        and column_exists(con, "main",
                          "canonical_path_malignant_patient_rollup_v1",
                          "bethesda_final")
    ):
        live_poc = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main', 'canonical_path_malignant_patient_rollup_v1')} "
            f"WHERE bethesda_final IS NOT NULL"
        ).fetchone()[0])
    else:
        live_poc = -1
    check(
        "bethesda_preserved_in_malignant_rollup",
        archive_poc_cnt is not None
        and live_poc >= 0
        and live_poc == archive_poc_cnt,
        archive_count=archive_poc_cnt,
        rollup_count=live_poc,
    )

    # Linkage column population on malignant_events.
    if (
        table_exists(con, "main", "canonical_path_malignant_events_v1")
        and column_exists(con, "main",
                          "canonical_path_malignant_events_v1",
                          "specimen_focus_id")
    ):
        n_link = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main', 'canonical_path_malignant_events_v1')} "
            f"WHERE specimen_focus_id IS NOT NULL"
        ).fetchone()[0])
    else:
        n_link = -1
    check(
        "malignant_linkage_population",
        n_link >= 9_000,
        rows_with_specimen_focus_id=n_link,
        target_threshold=9000,
    )

    # Benign-events linkage rate (Issue 2 follow-up gate). The decision report
    # said 77% of specimens have full surgery+synoptic linkage; the new
    # (rid,date) join should land in roughly that ballpark — 60% acts as a
    # red-flag floor.
    if table_exists(con, "main", "canonical_path_benign_events_v1"):
        n_total = row_count(con, "main", "canonical_path_benign_events_v1")
        n_full = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main', 'canonical_path_benign_events_v1')} "
            f"WHERE linkage_quality = 'full'"
        ).fetchone()[0])
        n_amb = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main', 'canonical_path_benign_events_v1')} "
            f"WHERE COALESCE(linkage_ambiguous_multi_specimen, FALSE)"
        ).fetchone()[0])
        full_pct = n_full / n_total if n_total else 0.0
        check(
            "benign_events_full_linkage_rate_geq_60pct",
            full_pct >= 0.60,
            n_total=n_total,
            n_full=n_full,
            full_pct=round(full_pct, 4),
            n_ambiguous_multi_specimen=n_amb,
        )

    # Step 7 verification: drops happened (unless --skip-drop was set).
    if not pre_drop:
        for sch, tbl in DEPRECATED_TABLES:
            still = table_exists(con, sch, tbl)
            check(
                f"deprecated_table_dropped_{sch}_{tbl}",
                not still,
                still_present=still,
            )

    # All 6 views resolve.
    for view_name, _ in NEW_VIEWS:
        ok = view_exists(con, VIEW_SCHEMA, view_name)
        if ok:
            try:
                _ = con.execute(
                    f'SELECT * FROM "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                    f'"{view_name}" LIMIT 0'
                ).fetchall()
                resolves = True
            except duckdb.Error as exc:
                resolves = False
                log_warn(f"  view {view_name} fails to resolve: {exc}")
        else:
            resolves = False
        check(f"view_resolves_{view_name}", ok and resolves)

    QA_DIR.mkdir(parents=True, exist_ok=True)
    QA_PATH.write_text(json.dumps(qa, indent=2, default=str), encoding="utf-8")
    log(f"  QA report -> {QA_PATH}")
    return qa


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def parse_phases(spec: str | None) -> set[str]:
    if not spec:
        return {"0", "1", "2", "3", "5", "6", "7", "8", "9", "10"}
    return {s.strip() for s in spec.split(",") if s.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Operative pathology consolidation (Script 361)"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--commit", action="store_true",
                      help="Run with writes enabled.")
    mode.add_argument("--dry-run", action="store_true",
                      help="Plan only — no archives, builds, or drops.")
    parser.add_argument("--phase", default=None,
                        help="Comma-separated phases to run (default all): "
                             "0,1,2,3,5,6,7,8,9,10")
    parser.add_argument("--skip-drop", action="store_true",
                        help="Skip Step 7 (DROP TABLE). Useful for staged runs.")
    args = parser.parse_args()

    do_writes = bool(args.commit)
    phases = parse_phases(args.phase)
    if args.skip_drop:
        phases.discard("7")
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
            # If we skip Step 0 but need archive counts later, look them up
            # from live tables (best effort).
            for sch, tbl in DEPRECATED_TABLES:
                if table_exists(con, sch, tbl):
                    archive_counts[f"{sch}.{tbl}"] = row_count(con, sch, tbl)

        if "1" in phases:
            results["step_1"] = step_1_build_malignant_events(con, do_writes)
        if "2" in phases:
            results["step_2"] = step_2_build_benign_events(con, do_writes)
        if "3" in phases:
            results["step_3"] = step_3_build_gland_events(con, do_writes)
        if "5" in phases:
            results["step_5"] = step_5_build_rollups(con, do_writes)
        if "6" in phases:
            results["step_6"] = step_6_build_views(con, do_writes)

        # Step 7 needs the archive counts to verify pre-drop safety.
        ran_step_7 = False
        if "7" in phases and do_writes:
            results["step_7"] = step_7_drop_deprecated(
                con, do_writes, archive_counts
            )
            ran_step_7 = True
        elif "7" in phases:
            log("STEP 7 — dry-run skips DROP TABLE (writes disabled)")

        if "8" in phases:
            results["step_8"] = step_8_registry_sync(con, do_writes)
        if "9" in phases:
            results["step_9"] = step_9_cpm_feeder_audit(con)
        if "10" in phases:
            results["step_10"] = step_10_qa(con, archive_counts,
                                            pre_drop=not ran_step_7)
            if not results["step_10"]["passed"]:
                log_error("ZERO-DRIFT QA failed — see qa file for details")
                flush_log()
                return 2

        log("Script 361 complete.")
        flush_log()
        return 0
    except Exception as exc:
        log_error(f"FATAL: {exc!r}")
        flush_log()
        raise


if __name__ == "__main__":
    sys.exit(main())
