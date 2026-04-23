#!/usr/bin/env python3
"""Script 396 — DTC NULL-N (Nx) stage_group fill: 4-row surgical cohort on CPM.

Updates only main.canonical_patient_master.ajcc8_stage_group for research_ids
2480, 2837, 4245, 6772. Single-transaction snapshot + UPDATEs + __readme.

Phases
------
* default / --phase 0 — Phase 0 probe + H1–H7; writes probe markdown +
  PROBE_REPORT_SHA256 (hashed region excludes footer after ---HASH-BOUNDARY---).
* --apply — idempotency (NO-OP if snapshot + __readme + rows already filled);
  else requires --i-approve matching probe hash; Phase 2 apply; Phase 3 verify.

Token: motherduck_client.get_token() (never printed).
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))

from _md_script_runner_utils import normalize_i_approve, probe_report_sha256  # noqa: E402
from motherduck_client import get_token, token_mode  # noqa: E402

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PUB_DB = "thyroid_canonical_publication_v1_0"
ARC_SCHEMA = "archive_pub_v1_0"
MAIN_SCHEMA = "main"
MS_SCHEMA = "manuscript_workspace"
CPM_TABLE = "canonical_patient_master"
README_TABLE = "__readme"
QUEUE_TABLE = "cpm_stage_group_manual_review_v1"

SCRIPT_FILE = "apply_dtc_null_n_stage_group_fill.py"
SCRIPT_TAG = "script_396"
SNAPSHOT_PREFIX = "cpm_pre_dtc_null_n_stage_group_fill_"

TARGET_RIDS = ("2480", "2837", "4245", "6772")
PLAN_STAGE: dict[str, str] = {
    "2480": "I",
    "2837": "I",
    "4245": "II",
    "6772": "II",
}
RATIONALE: dict[str, str] = {
    "2480": "derive_t_v2_nx_convention",
    "2837": "builder_sync_age_lt_55_m0",
    "4245": "builder_sync_t3a_t3b_both_yield_ii",
    "6772": "m1_primary_path_corroborated",
}

HASH_BOUNDARY = "---HASH-BOUNDARY---"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PROBE_PATH = OUTPUT_DIR / "apply_dtc_null_n_stage_group_fill_probe.md"
RUN_LOG_PATH = OUTPUT_DIR / "apply_dtc_null_n_stage_group_fill_run.log"
CLOSE_OUT_PATH = REPO_ROOT / "cursor_prompts" / "CLOSE_OUT_396.md"
PROMPT_PATH = REPO_ROOT / "cursor_prompts" / (
    "CURSOR_PROMPT_DTC_NULL_N_STAGE_GROUP_FILL_20260423_SCRIPT_396.md"
)

CPM_EXPECTED = 10_871
DTC_DX = ("PTC", "FTC", "HCC")

TAG_PREFIX = "v1_0-dtc-null-n-stage-groups-filled-"


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #


def log(msg: str, *, also_print: bool = True) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    if also_print:
        print(line, flush=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(RUN_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def fresh_log() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPO_ROOT.joinpath("cursor_prompts").mkdir(parents=True, exist_ok=True)
    RUN_LOG_PATH.write_text("", encoding="utf-8")


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #


def _connect_with_token(*, log_connect: bool) -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}).  "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    if log_connect:
        log(f"Connecting md:{PUB_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{PUB_DB}?motherduck_token={tok}")
    con.execute(f'USE "{PUB_DB}"')
    con.execute(f'USE "{PUB_DB}".main')
    dbs = {r[0] for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()}
    if PUB_DB not in dbs:
        raise SystemExit(f"PUB DB '{PUB_DB}' not attached")
    return con


def connect() -> duckdb.DuckDBPyConnection:
    return _connect_with_token(log_connect=True)


def connect_quiet() -> duckdb.DuckDBPyConnection:
    return _connect_with_token(log_connect=False)


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [PUB_DB, schema, name],
    ).fetchone()
    return row is not None


def readme_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    if not table_exists(con, MAIN_SCHEMA, README_TABLE):
        return set()
    rows = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [PUB_DB, MAIN_SCHEMA, README_TABLE],
    ).fetchall()
    return {r[0] for r in rows}


def count_snapshots_396(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(
        f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name LIKE '{SNAPSHOT_PREFIX}%'
        """,
        [PUB_DB, ARC_SCHEMA],
    ).fetchone()[0]


def find_snapshot_396(con: duckdb.DuckDBPyConnection) -> str | None:
    rows = con.execute(
        f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name LIKE '{SNAPSHOT_PREFIX}%'
        ORDER BY table_name DESC
        LIMIT 1
        """,
        [PUB_DB, ARC_SCHEMA],
    ).fetchall()
    return rows[0][0] if rows else None


def fq_snap(snap_name: str) -> str:
    return f'"{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}"'


def snapshot_run_stamp(snap_table: str) -> str:
    """Strip `cpm_pre_dtc_null_n_stage_group_fill_` → `YYYYMMDD_HHMMSS`."""
    if not snap_table.startswith(SNAPSHOT_PREFIX):
        raise ValueError(f"unexpected snapshot table name: {snap_table!r}")
    return snap_table[len(SNAPSHOT_PREFIX) :]


def cpm_fq() -> str:
    return f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'


def readme_fq() -> str:
    return f'"{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"'


def queue_fq() -> str:
    return f'"{PUB_DB}"."{MS_SCHEMA}"."{QUEUE_TABLE}"'


def readme_396_rowcount(con: duckdb.DuckDBPyConnection) -> int:
    cols = readme_columns(con)
    if not cols:
        return 0
    if "script_tag" in cols:
        return con.execute(
            f'SELECT COUNT(*) FROM {readme_fq()} WHERE script_tag = ?',
            [SCRIPT_TAG],
        ).fetchone()[0]
    return con.execute(
        f"""
        SELECT COUNT(*) FROM {readme_fq()}
        WHERE content LIKE 'Script 396:%'
        """
    ).fetchone()[0]


def rows_match_plan(con: duckdb.DuckDBPyConnection) -> bool:
    cpm = cpm_fq()
    for rid, sg in PLAN_STAGE.items():
        got = con.execute(
            f"""
            SELECT ajcc8_stage_group FROM {cpm}
            WHERE CAST(research_id AS VARCHAR) = ?
            """,
            [rid],
        ).fetchone()
        if not got or got[0] != sg:
            return False
    return True


def idempotency_complete(con: duckdb.DuckDBPyConnection) -> bool:
    snap = find_snapshot_396(con)
    if not snap:
        return False
    if readme_396_rowcount(con) < 1:
        return False
    return rows_match_plan(con)


def idempotency_partial(con: duckdb.DuckDBPyConnection) -> tuple[bool, str | None]:
    snap = find_snapshot_396(con)
    n_readme = readme_396_rowcount(con)
    if snap and n_readme == 0:
        return True, "snapshot without __readme row"
    if n_readme and not snap:
        return True, "__readme without snapshot"
    return False, None


# --------------------------------------------------------------------------- #
# Probe & gates
# --------------------------------------------------------------------------- #


def run_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cpm = cpm_fq()
    q = f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE diagnosis_primary IN ('PTC','FTC','HCC')
          AND ajcc8_stage_group IS NULL
    """
    dtc_null_stage = con.execute(q).fetchone()[0]

    q_dtc = f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE diagnosis_primary IN ('PTC','FTC','HCC')
    """
    dtc_total = con.execute(q_dtc).fetchone()[0]

    h1 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE diagnosis_primary IN ('PTC','FTC','HCC')
          AND ajcc8_stage_group IS NULL
          AND ajcc8_n_stage IS NULL
          AND CAST(research_id AS VARCHAR) IN ('2480','2837','4245','6772')
        """
    ).fetchone()[0]

    h2 = 0
    if table_exists(con, MS_SCHEMA, QUEUE_TABLE):
        h2 = con.execute(
            f"""
            SELECT COUNT(*) FROM {queue_fq()}
            WHERE CAST(research_id AS VARCHAR) IN ('2480','2837','4245','6772')
            """
        ).fetchone()[0]

    h3 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]

    h7 = count_snapshots_396(con)

    per_row = con.execute(
        f"""
        SELECT
          CAST(research_id AS VARCHAR) AS rid,
          diagnosis_primary,
          age_at_surgery,
          ajcc8_t_stage,
          ajcc8_t_stage_v2,
          dominant_tumor_ajcc8_t_stage,
          ajcc8_n_stage,
          ajcc8_n_stage_v2,
          ajcc8_m_stage,
          ajcc8_m_stage_v2,
          dominant_tumor_ajcc8_m_stage,
          ajcc8_stage_group,
          ajcc8_stage_group_corrected,
          path_stage_raw
        FROM {cpm}
        WHERE CAST(research_id AS VARCHAR) IN ('2480','2837','4245','6772')
        ORDER BY rid
        """
    ).fetchall()

    h6 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE diagnosis_primary IN ('PTC','FTC','HCC')
          AND age_at_surgery >= 55
          AND ajcc8_m_stage = 'M0'
          AND CAST(ajcc8_t_stage AS VARCHAR) IN ('T1','T1a','T1b','T2')
          AND ajcc8_n_stage IS NULL
          AND ajcc8_stage_group = 'I'
        """
    ).fetchone()[0]

    return {
        "dtc_null_stage": dtc_null_stage,
        "dtc_total": dtc_total,
        "h1": h1,
        "h2": h2,
        "h3": h3,
        "h6": h6,
        "h7": h7,
        "per_row": per_row,
    }


def _row_dict(per_row: list) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for r in per_row:
        rid = str(r[0])
        out[rid] = {
            "diagnosis_primary": r[1],
            "age_at_surgery": r[2],
            "ajcc8_t_stage": r[3],
            "ajcc8_t_stage_v2": r[4],
            "dominant_tumor_ajcc8_t_stage": r[5],
            "ajcc8_n_stage": r[6],
            "ajcc8_n_stage_v2": r[7],
            "ajcc8_m_stage": r[8],
            "ajcc8_m_stage_v2": r[9],
            "dominant_tumor_ajcc8_m_stage": r[10],
            "ajcc8_stage_group": r[11],
            "ajcc8_stage_group_corrected": r[12],
            "path_stage_raw": r[13],
        }
    return out


def check_h4(rd: dict[str, Any]) -> list[str]:
    errs: list[str] = []

    def req(rid: str, **kwargs: Any) -> None:
        row = rd.get(rid)
        if not row:
            errs.append(f"H4: missing row {rid}")
            return
        for col, exp in kwargs.items():
            got = row.get(col)
            if exp is None:
                if got is not None:
                    errs.append(f"H4: {rid} {col} expected NULL got {got!r}")
            elif got != exp:
                errs.append(f"H4: {rid} {col} expected {exp!r} got {got!r}")

    req(
        "2480",
        diagnosis_primary="PTC",
        age_at_surgery=63,
        ajcc8_t_stage=None,
        ajcc8_t_stage_v2="T1",
        ajcc8_n_stage=None,
        ajcc8_n_stage_v2="Nx",
        ajcc8_m_stage="M0",
        ajcc8_m_stage_v2="M0",
        ajcc8_stage_group_corrected=None,
    )
    req(
        "2837",
        diagnosis_primary="PTC",
        age_at_surgery=15,
        ajcc8_t_stage=None,
        ajcc8_t_stage_v2="T1a",
        ajcc8_n_stage=None,
        ajcc8_n_stage_v2="Nx",
        ajcc8_m_stage="M0",
        ajcc8_stage_group_corrected="I",
        path_stage_raw="I",
    )
    req(
        "4245",
        diagnosis_primary="PTC",
        age_at_surgery=69,
        ajcc8_t_stage="T3b",
        ajcc8_t_stage_v2="T3a",
        ajcc8_n_stage=None,
        ajcc8_n_stage_v2="Nx",
        ajcc8_m_stage="M0",
        ajcc8_stage_group_corrected="II",
    )
    req(
        "6772",
        diagnosis_primary="PTC",
        age_at_surgery=49,
        ajcc8_t_stage=None,
        ajcc8_t_stage_v2="T3a",
        ajcc8_n_stage=None,
        ajcc8_n_stage_v2="Nx",
        ajcc8_m_stage="M1",
        ajcc8_m_stage_v2="M0",
        dominant_tumor_ajcc8_m_stage="M1",
        ajcc8_stage_group_corrected="II",
        path_stage_raw="II",
    )

    for rid in TARGET_RIDS:
        row = rd.get(rid)
        if row and row.get("ajcc8_stage_group") is not None:
            errs.append(f"H4: {rid} ajcc8_stage_group expected NULL pre-apply got {row['ajcc8_stage_group']!r}")
    return errs


def derive_stage_rule(row: dict[str, Any]) -> str | None:
    rid = None
    age = row["age_at_surgery"]
    m = row["ajcc8_m_stage"]
    tv2 = row.get("ajcc8_t_stage_v2")
    tprim = row.get("ajcc8_t_stage")

    if m == "M0" and age is not None and age < 55:
        return "I"
    if m == "M1" and age is not None and age < 55:
        return "II"
    if m == "M0" and age is not None and age >= 55:
        teff = tprim if tprim is not None else tv2
        if teff in ("T1", "T1a", "T1b", "T2"):
            return "I"
        if teff in ("T3a", "T3b"):
            return "II"
    return None


def check_h5(rd: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    for rid in TARGET_RIDS:
        row = rd[rid]
        derived = derive_stage_rule(row)
        planned = PLAN_STAGE[rid]
        if derived != planned:
            errs.append(
                f"H5: {rid} rule-derived {derived!r} != planned {planned!r}"
            )
        corr = row.get("ajcc8_stage_group_corrected")
        if corr is not None and corr != planned:
            errs.append(
                f"H5: {rid} ajcc8_stage_group_corrected {corr!r} != planned {planned!r}"
            )
    return errs


def halt_gate(probe: dict[str, Any]) -> tuple[bool, list[str]]:
    errs: list[str] = []
    rd = _row_dict(probe["per_row"])

    if probe["h1"] != 4:
        errs.append(f"H1: scope count={probe['h1']}, expected 4")
    if probe["h2"] > 0:
        errs.append(f"H2: target rids in manual-review queue: count={probe['h2']}, expected 0")
    if probe["h3"] != CPM_EXPECTED:
        errs.append(f"H3: CPM count={probe['h3']}, expected {CPM_EXPECTED}")
    if probe["h6"] < 50:
        errs.append(f"H6: Nx convention precedent count={probe['h6']}, expected >=50")
    if probe["h7"] > 1:
        errs.append(f"H7: snapshot prefix occurrences={probe['h7']}, expected <=1")

    if len(rd) != 4:
        errs.append(f"H4: per-row fetch count={len(rd)}, expected 4")

    errs.extend(check_h4(rd))
    errs.extend(check_h5(rd))

    return (len(errs) == 0, errs)


def format_probe_hashed_body(probe: dict[str, Any], gate_ok: bool, gate_errs: list[str]) -> str:
    rd = _row_dict(probe["per_row"])
    lines = [
        "# Script 396 — Phase 0 probe (DTC NULL-N stage_group fill)",
        "",
        "## Cohort summary",
        "",
        "| metric | value |",
        "|---|---:|",
        f"| dtc_total | {probe['dtc_total']} |",
        f"| dtc_null_stage_group | {probe['dtc_null_stage']} |",
        f"| H1 scope (NULL N in write set, NULL stage_group) | {probe['h1']} |",
        f"| H3 CPM total | {probe['h3']} |",
        f"| H6 convention precedent (age>=55 M0 T1-T2 N NULL stage I) | {probe['h6']} |",
        f"| H7 snapshot prefix table count | {probe['h7']} |",
        "",
        "## Halt gates (H1–H7)",
        "",
        f"| all_pass | {gate_ok} |",
        "",
    ]
    if gate_errs:
        lines.append("### Failures")
        lines.append("")
        for e in gate_errs:
            lines.append(f"- {e}")
        lines.append("")

    lines.extend(
        [
            "## Per-row current state",
            "",
            "| research_id | age | t_prim | t_v2 | n_prim | n_v2 | m_prim | m_v2 | m_dom | "
            "stage_group | corrected | path_raw | planned_write | rationale |",
            "|---:|---:|---|---|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for rid in sorted(rd.keys(), key=int):
        row = rd[rid]
        def s(x: Any) -> str:
            return "NULL" if x is None else str(x)

        lines.append(
            "| {rid} | {age} | {tp} | {tv} | {np} | {nv} | {mp} | {mv} | {md} | {sg} | {sc} | {pr} | {pw} | {rat} |".format(
                rid=rid,
                age=row["age_at_surgery"],
                tp=s(row["ajcc8_t_stage"]),
                tv=s(row["ajcc8_t_stage_v2"]),
                np=s(row["ajcc8_n_stage"]),
                nv=s(row["ajcc8_n_stage_v2"]),
                mp=s(row["ajcc8_m_stage"]),
                mv=s(row["ajcc8_m_stage_v2"]),
                md=s(row["dominant_tumor_ajcc8_m_stage"]),
                sg=s(row["ajcc8_stage_group"]),
                sc=s(row["ajcc8_stage_group_corrected"]),
                pr=s(row["path_stage_raw"]),
                pw=PLAN_STAGE[rid],
                rat=RATIONALE[rid],
            )
        )

    lines.extend(
        [
            "",
            "## Planned UPDATEs (stage_group only)",
            "",
            "| research_id | SET ajcc8_stage_group | rationale_code |",
            "|---|---|---|",
            "| 2480 | I | derive_t_v2_nx_convention |",
            "| 2837 | I | builder_sync_age_lt_55_m0 |",
            "| 4245 | II | builder_sync_t3a_t3b_both_yield_ii |",
            "| 6772 | II | m1_primary_path_corroborated |",
            "",
            "## Projected completeness",
            "",
            "DTC NULL stage_group: 6 → 2 (remaining 1404, 12198 in manual review).",
            "DTC completeness: 3736/3742 (99.8397%) → 3740/3742 (99.9466%) if denominators unchanged.",
            "",
        ]
    )
    return "\n".join(lines)


def write_probe_file(hashed_body: str) -> str:
    footer_lines = [
        "",
        HASH_BOUNDARY,
        "",
        "## Generation footer (excluded from PROBE_REPORT_SHA256)",
        "",
        f"Written UTC: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]
    PROBE_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROBE_PATH.write_text(hashed_body + "\n".join(footer_lines), encoding="utf-8")
    return probe_report_sha256(hashed_body)


# --------------------------------------------------------------------------- #
# Apply
# --------------------------------------------------------------------------- #


def insert_readme_396(
    con: duckdb.DuckDBPyConnection,
    *,
    snap_fqn: str,
    probe_sha: str,
    run_stamp: str,
) -> None:
    cols = readme_columns(con)
    summary = (
        f"Script 396: apply_dtc_null_n_stage_group_fill — 4 rows updated (2×I, 2×II). "
        f"Rationales: {', '.join(f'{k}:{RATIONALE[k]}' for k in TARGET_RIDS)}. "
        "Disagreements logged: "
        "rid_4245_t_disagreement:T3b_primary_vs_T3a_v2_both_yield_II; "
        "rid_6772_m_disagreement:M1_primary_vs_M0_v2_path_stage_II_and_dominant_M1_corroborate_M1. "
        f"Snapshot: {snap_fqn}. Probe SHA256 (consumed): {probe_sha}."
    )
    if {"script_name", "script_tag", "run_timestamp", "summary"}.issubset(cols):
        con.execute(
            f"""
            INSERT INTO {readme_fq()} (script_name, script_tag, run_timestamp, summary)
            VALUES (?, ?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?)
            """,
            [SCRIPT_FILE, SCRIPT_TAG, summary],
        )
        return
    insert_cols = ["content", "updated_at"]
    value_exprs = ["?", "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)"]
    content = (
        f"Script 396: {SCRIPT_TAG} {SCRIPT_FILE}. {summary}"
    )
    params: list[Any] = [content]
    if "script" in cols:
        insert_cols.append("script")
        value_exprs.append("?")
        params.append(SCRIPT_TAG)
    con.execute(
        f"INSERT INTO {readme_fq()} ({', '.join(insert_cols)}) "
        f"VALUES ({', '.join(value_exprs)})",
        params,
    )


def apply_transaction(
    con: duckdb.DuckDBPyConnection,
    run_stamp: str,
    probe_sha: str,
) -> str:
    cpm = cpm_fq()
    snap_name = f"{SNAPSHOT_PREFIX}{run_stamp}"
    snap_sql = fq_snap(snap_name)

    if count_snapshots_396(con) != 0:
        raise SystemExit(
            "2A: fresh apply requires zero existing cpm_pre_dtc_null_n_stage_group_fill_* tables"
        )

    con.execute("BEGIN TRANSACTION")
    try:
        log("2A: CREATE TABLE snapshot (4 rows)", also_print=True)
        con.execute(
            f"""
            CREATE TABLE {snap_sql} AS
            SELECT * FROM {cpm}
            WHERE CAST(research_id AS VARCHAR) IN ('2480','2837','4245','6772')
            """
        )
        n_snap = con.execute(f"SELECT COUNT(*) FROM {snap_sql}").fetchone()[0]
        if n_snap != 4:
            raise SystemExit(f"2A: snapshot rows={n_snap}, expected 4")

        updates: list[tuple[str, str, str]] = [
            (
                "2480",
                "I",
                "diagnosis_primary = 'PTC' AND age_at_surgery = 63 "
                "AND ajcc8_m_stage = 'M0' AND ajcc8_stage_group IS NULL",
            ),
            (
                "2837",
                "I",
                "diagnosis_primary = 'PTC' AND age_at_surgery = 15 "
                "AND ajcc8_m_stage = 'M0' AND ajcc8_stage_group IS NULL "
                "AND ajcc8_stage_group_corrected = 'I'",
            ),
            (
                "4245",
                "II",
                "diagnosis_primary = 'PTC' AND age_at_surgery = 69 "
                "AND ajcc8_m_stage = 'M0' AND ajcc8_stage_group IS NULL "
                "AND ajcc8_stage_group_corrected = 'II'",
            ),
            (
                "6772",
                "II",
                "diagnosis_primary = 'PTC' AND age_at_surgery = 49 "
                "AND ajcc8_m_stage = 'M1' AND ajcc8_stage_group IS NULL "
                "AND ajcc8_stage_group_corrected = 'II'",
            ),
        ]
        for rid, sg, pred in updates:
            rows = con.execute(
                f"""
                UPDATE {cpm}
                SET ajcc8_stage_group = ?
                WHERE CAST(research_id AS VARCHAR) = ?
                  AND {pred}
                RETURNING research_id
                """,
                [sg, rid],
            ).fetchall()
            if len(rows) != 1:
                raise SystemExit(f"2B: UPDATE {rid} expected 1 row, got {len(rows)}")

        snap_fqn = f"{PUB_DB}.{ARC_SCHEMA}.{snap_name}"
        insert_readme_396(con, snap_fqn=snap_fqn, probe_sha=probe_sha, run_stamp=run_stamp)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return snap_name


# --------------------------------------------------------------------------- #
# Verify (Phase 3)
# --------------------------------------------------------------------------- #


def run_verify(con: duckdb.DuckDBPyConnection, snap_name: str) -> tuple[bool, list[str], dict[str, Any]]:
    cpm = cpm_fq()
    snap = fq_snap(snap_name)
    errs: list[str] = []
    info: dict[str, Any] = {}

    p1 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]
    if p1 != CPM_EXPECTED:
        errs.append(f"P1: CPM={p1}, expected {CPM_EXPECTED}")

    for rid, exp in PLAN_STAGE.items():
        got = con.execute(
            f"SELECT ajcc8_stage_group FROM {cpm} WHERE CAST(research_id AS VARCHAR) = ?",
            [rid],
        ).fetchone()
        if not got or got[0] != exp:
            errs.append(f"P2: {rid} stage_group={got}, expected {exp}")

    p3 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE diagnosis_primary IN ('PTC','FTC','HCC')
          AND ajcc8_stage_group IS NULL
        """
    ).fetchone()[0]
    if p3 != 2:
        errs.append(f"P3: DTC NULL stage_group count={p3}, expected 2")

    p3_rids = con.execute(
        f"""
        SELECT CAST(research_id AS VARCHAR) FROM {cpm}
        WHERE diagnosis_primary IN ('PTC','FTC','HCC')
          AND ajcc8_stage_group IS NULL
        ORDER BY 1
        """
    ).fetchall()
    rids_null = [r[0] for r in p3_rids]
    if set(rids_null) != {"1404", "12198"}:
        errs.append(f"P3: NULL stage rids={rids_null}, expected ['1404','12198']")

    if table_exists(con, MS_SCHEMA, QUEUE_TABLE):
        for rid in ("1404", "12198"):
            nq = con.execute(
                f"""
                SELECT COUNT(*) FROM {queue_fq()}
                WHERE CAST(research_id AS VARCHAR) = ?
                """,
                [rid],
            ).fetchone()[0]
            if nq < 1:
                errs.append(f"P3: research_id {rid} not in manual-review queue")

    n_readme = readme_396_rowcount(con)
    if n_readme != 1:
        errs.append(f"P4: __readme script_396 rows={n_readme}, expected 1")

    p5 = con.execute(f"SELECT COUNT(*) FROM {snap}").fetchone()[0]
    if p5 != 4:
        errs.append(f"P5: snapshot rows={p5}, expected 4")

    snap_cols = [
        r[0]
        for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [PUB_DB, ARC_SCHEMA, snap_name],
        ).fetchall()
    ]
    compare_cols = [c for c in snap_cols if c.lower() != "ajcc8_stage_group"]
    if compare_cols:
        ors = [f'c."{c}" IS DISTINCT FROM s."{c}"' for c in compare_cols]
        p6_bad = con.execute(
            f"""
            SELECT COUNT(*) FROM {snap} s
            INNER JOIN {cpm} c
              ON CAST(s.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
            WHERE {' OR '.join(ors)}
            """
        ).fetchone()[0]
        if p6_bad > 0:
            errs.append(
                f"P6: {p6_bad} row(s) differ from snapshot outside ajcc8_stage_group"
            )

    info.update(
        {
            "p1": p1,
            "p3": p3,
            "p5": p5,
            "readme_n": n_readme,
        }
    )
    return (len(errs) == 0, errs, info)


def format_close_out(
    *,
    commit_sha_note: str,
    tag_name: str,
    utc_ts: str,
    probe_sha: str,
    snap_name: str,
    gate_table: str,
    before_after: str,
    verify_ok: bool,
    verify_errs: list[str],
) -> str:
    lines = [
        "# Script 396 — Close-out",
        "",
        f"- **Git commit / SHA:** {commit_sha_note}",
        f"- **Tag:** `{tag_name}`",
        f"- **UTC timestamp:** {utc_ts}",
        f"- **Probe SHA256 (consumed):** `{probe_sha}`",
        f"- **Snapshot FQN:** `{PUB_DB}.{ARC_SCHEMA}.{snap_name}`",
        "",
        "## Halt-gate verdicts (Phase 0)",
        "",
        gate_table,
        "",
        "## Writes — stage_group before → after",
        "",
        before_after,
        "",
        "## Disagreement log",
        "",
        "- rid_4245_t_disagreement: T3b_primary_vs_T3a_v2_both_yield_II",
        "- rid_6772_m_disagreement: M1_primary_vs_M0_v2_path_stage_II_and_dominant_M1_corroborate_M1",
        "",
        "## DTC completeness",
        "",
        "- Before: 3736/3742 (99.8397%)",
        "- After: 3740/3742 (99.9466%)",
        "",
        "## Remaining NULL DTC rows",
        "",
        "research_ids **1404** and **12198** — confirmed in `manuscript_workspace.cpm_stage_group_manual_review_v1` (AJCC-edition adjudication).",
        "",
        "## CF-396 followups (proposed)",
        "",
        "- **CF-396-1:** rid 4245 T3b/T3a primary-vs-v2 — root-cause in 240-builder's T-column source precedence.",
        "- **CF-396-2:** rid 6772 M1/M0 primary-vs-v2 — root-cause in M-column builder (primary M1 correct per path_raw + dominant; v2 M0 anomalous).",
        "- **CF-396-3:** 240-builder COALESCE fallback from `ajcc8_t_stage_v2` when `ajcc8_t_stage IS NULL` (structural fix deferred).",
        "",
        "## Phase 3 verification",
        "",
        f"- **all_pass:** {verify_ok}",
    ]
    if verify_errs:
        lines.extend(["", "### Failures", ""])
        lines.extend(f"- {e}" for e in verify_errs)
    lines.append("")
    return "\n".join(lines)


def git_commit_and_tag(run_stamp: str) -> tuple[str, str]:
    """Phase 4: surgical add, commit (if staged diff), tag, push HEAD + tag."""
    tag_name = f"{TAG_PREFIX}{run_stamp}"
    paths = [
        REPO_ROOT / "scripts" / SCRIPT_FILE,
        PROBE_PATH,
        RUN_LOG_PATH,
        PROMPT_PATH,
        CLOSE_OUT_PATH,
    ]
    for p in paths:
        if not p.exists():
            raise SystemExit(f"Phase 4: missing file {p}")
    msg = "Script 396: DTC NULL-N stage_group fill (4 rows; 2x Stage I, 2x Stage II)"
    root = str(REPO_ROOT)
    for p in paths:
        add_cmd = ["git", "-C", root, "add", "--", str(p)]
        if p == RUN_LOG_PATH:
            # Run log lives under scripts/output/ which may be gitignored for *.log.
            add_cmd = ["git", "-C", root, "add", "-f", "--", str(p)]
        subprocess.run(add_cmd, check=True)
    diff = subprocess.run(["git", "-C", root, "diff", "--staged", "--quiet"])
    if diff.returncode != 0:
        subprocess.run(["git", "-C", root, "commit", "-m", msg], check=True)
    else:
        log("Phase 4: no staged changes — skipping commit (tree already matches index)")
    r = subprocess.run(
        ["git", "-C", root, "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    commit_sha = r.stdout.strip()
    # Idempotent local tag → current HEAD (re-close 396 if re-run).
    subprocess.run(["git", "-C", root, "tag", "-f", tag_name, commit_sha], check=True)
    out_head = subprocess.run(
        ["git", "-C", root, "push", "origin", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    log(f"Phase 4: git push origin HEAD ok — {out_head.stdout.strip() or out_head.stderr.strip() or 'done'}")
    out_tag = subprocess.run(
        ["git", "-C", root, "push", "origin", tag_name],
        check=True,
        capture_output=True,
        text=True,
    )
    log(f"Phase 4: git push origin {tag_name} ok — {out_tag.stdout.strip() or out_tag.stderr.strip() or 'done'}")
    return commit_sha, tag_name


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--phase", type=int, default=0, help="0=probe (default)")
    ap.add_argument("--apply", action="store_true", help="Run apply after gates + approval")
    ap.add_argument(
        "--i-approve",
        default="",
        metavar="SHA256",
        help="SHA256 of hashed probe region (required for fresh --apply)",
    )
    ap.add_argument(
        "--force-stamp",
        default="",
        help="Override RUN_STAMP for snapshot/tag suffix (UTC YYYYMMDD_HHMMSS)",
    )
    ap.add_argument(
        "--phase4",
        action="store_true",
        help="After successful apply+verify, run git commit/tag/push (optional)",
    )
    args = ap.parse_args()

    if args.phase != 0:
        print("Only --phase 0 is implemented.", flush=True)
        return 1

    if args.apply:
        con0 = connect_quiet()
        try:
            sn0 = find_snapshot_396(con0)
            nr0 = readme_396_rowcount(con0)
            rmp0 = rows_match_plan(con0)
            if sn0 and nr0 >= 1 and not rmp0:
                print(
                    "HALT: corrupt — script_396 snapshot + __readme but CPM rows "
                    "do not match planned stage_group",
                    flush=True,
                )
                return 2
            if idempotency_complete(con0):
                line = (
                    "NO-OP: snapshot + script_396 __readme + planned stage_group values present — "
                    "running Phase 3 verify only."
                )
                print(line, flush=True)
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                with open(RUN_LOG_PATH, "a", encoding="utf-8") as f:
                    f.write(f"{datetime.now(timezone.utc).isoformat()} {line}\n")
                snap = find_snapshot_396(con0)
                if not snap:
                    print("HALT: idempotency inconsistency — missing snapshot", flush=True)
                    return 2
                ok, errs, _ = run_verify(con0, snap)
                if not ok:
                    print("\n--- PHASE 3 VERIFY FAILED ---\n" + "\n".join(errs), flush=True)
                    return 2
                print("Phase 3 verify OK (NO-OP).", flush=True)
                if args.phase4:
                    rs = snapshot_run_stamp(snap)
                    log(f"Phase 4: NO-OP path — git using snapshot stamp {rs}")
                    commit_sha, tag_n = git_commit_and_tag(rs)
                    log(f"Phase 4: commit_sha={commit_sha} tag={tag_n}")
                    print(f"commit_sha={commit_sha} tag={tag_n}", flush=True)
                return 0

            partial, reason = idempotency_partial(con0)
            if partial:
                print(f"HALT: partial apply — {reason}", flush=True)
                return 2
        finally:
            con0.close()

    fresh_log()
    con = connect()
    try:
        probe = run_probe(con)
        ok, errs = halt_gate(probe)
        hashed_body = format_probe_hashed_body(probe, ok, errs)
        digest = write_probe_file(hashed_body)
        log(f"Wrote {PROBE_PATH}")
        print(f"PROBE_REPORT_SHA256={digest}", flush=True)

        if not ok:
            for e in errs:
                log(f"HALT: {e}")
            print("\n--- HALT GATE FAILED ---\n" + "\n".join(errs), flush=True)
            return 1

        if not args.apply:
            log("Probe only — re-run with --apply --i-approve=<sha256> after approval.")
            return 0

        approved = normalize_i_approve(args.i_approve)
        if not approved:
            log("HALT: --apply requires --i-approve=<sha256>")
            print(
                f"\nAfter approval:\n  python3 scripts/{SCRIPT_FILE} --apply --i-approve={digest}\n",
                flush=True,
            )
            return 5
        if approved != digest:
            log(f"HALT: --i-approve mismatch (expected {digest}, got {approved})")
            print(
                f"Re-run probe or pass: --i-approve={digest}",
                flush=True,
            )
            return 3

        run_stamp = args.force_stamp.strip() or datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S"
        )

        snx = find_snapshot_396(con)
        nrx = readme_396_rowcount(con)
        rmpx = rows_match_plan(con)
        if rmpx and not (snx and nrx >= 1):
            log("HALT: CPM matches plan but snapshot or script_396 __readme missing")
            print("HALT: inconsistent artifact state — manual recovery required", flush=True)
            return 2
        if snx and nrx >= 1 and not rmpx:
            log("HALT: snapshot + __readme present but CPM does not match plan")
            return 2
        if count_snapshots_396(con) != 0:
            log("HALT: existing 396 snapshot table(s) block fresh apply")
            return 2

        cpm = cpm_fq()
        before_rows = con.execute(
            f"""
            SELECT CAST(research_id AS VARCHAR), ajcc8_stage_group
            FROM {cpm}
            WHERE CAST(research_id AS VARCHAR) IN ('2480','2837','4245','6772')
            ORDER BY 1
            """
        ).fetchall()

        snap_name = apply_transaction(con, run_stamp, digest)
        ok3, verr, _ = run_verify(con, snap_name)
        if not ok3:
            for e in verr:
                log(f"VERIFY: {e}")
            print("\n--- PHASE 3 VERIFY FAILED ---\n" + "\n".join(verr), flush=True)
            return 2

        ba_lines = [
            "| research_id | before | after |",
            "|---:|---|---|",
        ]
        for rid, before in before_rows:
            ba_lines.append(f"| {rid} | {before!s} | {PLAN_STAGE[rid]} |")
        before_after_md = "\n".join(ba_lines)

        gate_md = "\n".join(
            [
                "| gate | verdict |",
                "|---|---|",
                "| H1 | PASS (4) |",
                "| H2 | PASS |",
                "| H3 | PASS |",
                "| H4 | PASS |",
                "| H5 | PASS |",
                "| H6 | PASS |",
                "| H7 | PASS |",
            ]
        )

        utc_ts = datetime.now(timezone.utc).isoformat()
        tag_name = f"{TAG_PREFIX}{run_stamp}"
        sha_note = (
            "`(after git commit)` — a blob cannot embed its own full SHA; "
            f"resolve with `git rev-parse {tag_name}` once tagged."
        )
        close_body = format_close_out(
            commit_sha_note=sha_note,
            tag_name=tag_name,
            utc_ts=utc_ts,
            probe_sha=digest,
            snap_name=snap_name,
            gate_table=gate_md,
            before_after=before_after_md,
            verify_ok=True,
            verify_errs=[],
        )
        CLOSE_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        CLOSE_OUT_PATH.write_text(close_body, encoding="utf-8")
        log(f"Wrote {CLOSE_OUT_PATH}")

        commit_sha = "(not committed)"
        if args.phase4:
            commit_sha, tag_name = git_commit_and_tag(run_stamp)
            log(f"Phase 4: commit_sha={commit_sha} (also: git rev-parse {tag_name})")

        log("Script 396 apply + verify complete.")
        print(f"commit_sha={commit_sha} tag={tag_name} snapshot={snap_name}", flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
