#!/usr/bin/env python3
"""Script 395 — DTC T-sync + stage_group fill (T-requiring residual, CPM).

Surgical UPDATE on main.canonical_patient_master: sync ajcc8_t_stage from
ajcc8_t_stage_v2, derive ajcc8_stage_group / _corrected for 11 rows; route 2 rows
to manuscript_workspace.cpm_stage_group_manual_review_v1.

Phases
------
* default    — Phase 0 Q0-A..G; writes scripts/output/395_prestate_probe_report.md;
               prints PROBE_REPORT_SHA256=<hex> for --i-approve on apply.
* --apply    — idempotency (NO-OP if snapshot + __readme present); else requires
               --i-approve matching probe body; 2A-2E apply; Phase 3 verify.

Token: motherduck_client.get_token() (never printed).
"""
from __future__ import annotations

import argparse
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

PRE394_SNAPSHOT = "cpm_stage_group_pre394_20260423_000452"
SNAPSHOT_PREFIX = "cpm_t_sync_pre395_"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PRESTATE_PATH = OUTPUT_DIR / "395_prestate_probe_report.md"
RUN_LOG_PATH = OUTPUT_DIR / "395_run.log"
CLOSE_OUT_PATH = OUTPUT_DIR / "395_close_out_report.md"

CPM_EXPECTED = 10_871


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #


def log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    print(line, flush=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(RUN_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def fresh_log() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
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
    """Same as connect() without log line — for idempotency NO-OP check before fresh_log()."""
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


def find_pre395_snapshot(con: duckdb.DuckDBPyConnection) -> str | None:
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


def fq_pre394() -> str:
    return f'"{PUB_DB}"."{ARC_SCHEMA}"."{PRE394_SNAPSHOT}"'


def readme_395_present(con: duckdb.DuckDBPyConnection) -> bool:
    if not table_exists(con, MAIN_SCHEMA, README_TABLE):
        return False
    row = con.execute(
        f"""
        SELECT 1 FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
        WHERE content LIKE 'Script 395:%'
        LIMIT 1
        """
    ).fetchone()
    return row is not None


def run_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cpm = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'
    pre394 = fq_pre394()

    n_q0a = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE ajcc8_t_stage IS NULL
          AND ajcc8_n_stage IS NOT NULL
          AND ajcc8_m_stage IS NOT NULL
          AND ajcc8_stage_group IS NULL
          AND diagnosis_primary IN ('PTC','FTC','HCC')
        """
    ).fetchone()[0]

    n_q0b = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm} cpm
        JOIN {pre394} s USING (research_id)
        WHERE s.cohort_tag = '395_deferred_needs_T'
          AND cpm.ajcc8_t_stage IS NULL
          AND cpm.ajcc8_stage_group IS NULL
        """
    ).fetchone()[0]

    q0c = con.execute(
        f"""
        SELECT
          CASE WHEN cpm.ajcc8_t_stage_v2 IS NOT NULL THEN 'T_syncable' ELSE 'manual_review' END AS route,
          COUNT(*) AS n
        FROM {cpm} cpm
        JOIN {pre394} s USING (research_id)
        WHERE s.cohort_tag = '395_deferred_needs_T'
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()

    q0d = con.execute(
        f"""
        SELECT
          cpm.research_id,
          cpm.ajcc8_t_stage_v2 AS t_v2,
          COALESCE(CAST(cpm.dominant_tumor_ajcc8_t_stage AS VARCHAR), '·') AS t_dom,
          CASE
            WHEN cpm.dominant_tumor_ajcc8_t_stage IS NULL THEN 'v2_only_ok'
            WHEN CAST(cpm.dominant_tumor_ajcc8_t_stage AS VARCHAR)
                 = CAST(cpm.ajcc8_t_stage_v2 AS VARCHAR) THEN 'corroborated'
            ELSE 'DISAGREEMENT_halt'
          END AS corroboration
        FROM {cpm} cpm
        JOIN {pre394} s USING (research_id)
        WHERE s.cohort_tag = '395_deferred_needs_T'
          AND cpm.ajcc8_t_stage_v2 IS NOT NULL
        ORDER BY cpm.research_id
        """
    ).fetchall()

    q0e = con.execute(
        f"""
        SELECT
          CASE
            WHEN cpm.ajcc8_t_stage_v2 IN ('T1','T1a','T1b','T2') AND cpm.ajcc8_n_stage = 'N0' THEN 'I'
            WHEN cpm.ajcc8_t_stage_v2 IN ('T1','T1a','T1b','T2') AND cpm.ajcc8_n_stage LIKE 'N1%' THEN 'II'
            WHEN cpm.ajcc8_t_stage_v2 IN ('T3a','T3b') THEN 'II'
            WHEN cpm.ajcc8_t_stage_v2 = 'T4a' THEN 'III'
            WHEN cpm.ajcc8_t_stage_v2 = 'T4b' THEN 'IVA'
            ELSE 'UNHANDLED_halt'
          END AS projected,
          COUNT(*) AS n
        FROM {cpm} cpm
        JOIN {pre394} s USING (research_id)
        WHERE s.cohort_tag = '395_deferred_needs_T'
          AND cpm.ajcc8_t_stage_v2 IS NOT NULL
          AND cpm.age_at_surgery >= 55
          AND cpm.ajcc8_m_stage = 'M0'
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()

    q0f = con.execute(
        f"""
        SELECT research_id, path_stage_raw, gm_path_stage_raw, ajcc8_n_stage, ajcc8_m_stage, age_at_surgery
        FROM {cpm}
        WHERE research_id IN ('1404', '12198')
        ORDER BY research_id
        """
    ).fetchall()

    n_q0g = con.execute(
        f"""
        SELECT COUNT(*) AS n_tbls
        FROM information_schema.tables
        WHERE table_catalog = '{PUB_DB}'
          AND table_schema = '{MS_SCHEMA}'
          AND table_name = '{QUEUE_TABLE}'
        """
    ).fetchone()[0]

    n_queue_395 = 0
    if n_q0g:
        n_queue_395 = con.execute(
            f"""
            SELECT COUNT(*) FROM "{PUB_DB}"."{MS_SCHEMA}"."{QUEUE_TABLE}"
            WHERE source_script = '395'
            """
        ).fetchone()[0]

    return {
        "n_q0a": n_q0a,
        "n_q0b": n_q0b,
        "q0c": q0c,
        "q0d": q0d,
        "q0e": q0e,
        "q0f": q0f,
        "n_q0g": n_q0g,
        "n_queue_395_pre": n_queue_395,
    }


def format_probe_md(probe: dict[str, Any]) -> str:
    """Stable body (no timestamps) so PROBE_REPORT_SHA256 matches across probe/apply runs."""
    q0c = probe["q0c"]
    q0e = probe["q0e"]
    lines = [
        "# Script 395 — Pre-state probe (Phase 0)",
        "",
        "## Q0-A. n_remaining_null_t_orphans",
        "",
        "| n_remaining_null_t_orphans |",
        "|---:|",
        f"| {probe['n_q0a']} |",
        "",
        "## Q0-B. n_match_394_deferred",
        "",
        "| n_match_394_deferred |",
        "|---:|",
        f"| {probe['n_q0b']} |",
        "",
        "## Q0-C. Route split (T_syncable vs manual_review)",
        "",
        "| route | n |",
        "|---|---:|",
    ]
    for route, n in q0c:
        lines.append(f"| {route} | {n} |")
    lines.extend(
        [
            "",
            "## Q0-D. T-column corroboration (11 T_syncable rows)",
            "",
            "| research_id | t_v2 | t_dom | corroboration |",
            "|---:|---|---|---|",
        ]
    )
    for rid, tv2, tdom, corr in probe["q0d"]:
        tv2s = tv2 if tv2 is not None else "NULL"
        lines.append(f"| {rid} | {tv2s} | {tdom} | {corr} |")
    lines.extend(
        [
            "",
            "## Q0-E. Projected stage_group distribution (age≥55 M0, t_v2 populated)",
            "",
            "| projected | n |",
            "|---|---:|",
        ]
    )
    for proj, n in q0e:
        ps = proj if proj is not None else "NULL"
        lines.append(f"| {ps} | {n} |")
    lines.extend(
        [
            "",
            "## Q0-F. Manual-review rows path stage",
            "",
            "| research_id | path_stage_raw | gm_path_stage_raw | N | M | age |",
            "|---:|---|---|---|---|---:|",
        ]
    )
    for r in probe["q0f"]:
        rid, psr, gmpsr, nst, mst, age = r
        psr_s = psr if psr is not None else "NULL"
        gm_s = gmpsr if gmpsr is not None else "NULL"
        lines.append(f"| {rid} | {psr_s} | {gm_s} | {nst} | {mst} | {age} |")
    lines.extend(
        [
            "",
            "## Q0-G. Manual-review queue table",
            "",
            "| n_tbls (information_schema) | n_rows source_script=395 (if table exists) |",
            "|---:|---:|",
            f"| {probe['n_q0g']} | {probe['n_queue_395_pre']} |",
            "",
        ]
    )
    return "\n".join(lines)


def halt_gate(probe: dict[str, Any]) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if probe["n_q0a"] != 13:
        errs.append(f"Q0-A: n={probe['n_q0a']}, expected 13")
    if probe["n_q0b"] != 13:
        errs.append(f"Q0-B: n={probe['n_q0b']}, expected 13")

    q0c_map = {r[0]: int(r[1]) for r in probe["q0c"]}
    if q0c_map.get("T_syncable") != 11 or q0c_map.get("manual_review") != 2:
        errs.append(f"Q0-C: split {q0c_map}, expected T_syncable=11, manual_review=2")

    for rid, _tv2, _tdom, corr in probe["q0d"]:
        if corr == "DISAGREEMENT_halt":
            errs.append(f"Q0-D: DISAGREEMENT_halt on research_id {rid}")

    dist = {r[0]: int(r[1]) for r in probe["q0e"]}
    if "UNHANDLED_halt" in dist:
        errs.append(f"Q0-E: UNHANDLED_halt present: {dist}")
    if dist.get("I") != 4 or dist.get("II") != 7:
        errs.append(f"Q0-E: distribution {dist}, expected I=4, II=7")

    for r in probe["q0f"]:
        rid, psr, gmpsr, _n, _m, _age = r
        psr_n = str(psr).strip() if psr is not None else ""
        gm_n = str(gmpsr).strip() if gmpsr is not None else ""
        if psr_n != "III" or gm_n != "III":
            errs.append(f"Q0-F: research_id {rid} path_stage_raw={psr!r} gm={gmpsr!r}, expected III/III")

    if len(probe["q0f"]) != 2:
        errs.append(f"Q0-F: row count {len(probe['q0f'])}, expected 2")

    return (len(errs) == 0, errs)


def idempotency_state(con: duckdb.DuckDBPyConnection) -> tuple[str | None, bool]:
    return find_pre395_snapshot(con), readme_395_present(con)


def apply_script(con: duckdb.DuckDBPyConnection, run_stamp: str) -> str:
    snap_name = f"{SNAPSHOT_PREFIX}{run_stamp}"
    snap_fq = f'"{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}"'
    cpm = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'
    readme = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"'
    pre394 = fq_pre394()
    queue_fq = f'"{PUB_DB}"."{MS_SCHEMA}"."{QUEUE_TABLE}"'

    log(f"2A: CREATE OR REPLACE {snap_fq} AS ...")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {snap_fq} AS
        SELECT research_id, diagnosis_primary, age_at_surgery,
               ajcc8_t_stage, ajcc8_t_stage_v2,
               dominant_tumor_ajcc8_t_stage,
               ajcc8_n_stage, ajcc8_m_stage,
               ajcc8_stage_group, ajcc8_stage_group_corrected,
               path_stage_raw, gm_path_stage_raw,
               CASE
                 WHEN ajcc8_t_stage_v2 IS NOT NULL THEN '395_t_synced'
                 ELSE '395_manual_review'
               END AS cohort_tag,
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
        FROM {cpm} cpm
        WHERE research_id IN (
          SELECT research_id FROM {pre394}
          WHERE cohort_tag = '395_deferred_needs_T'
        )
        """
    )
    n_snap = con.execute(f"SELECT COUNT(*) FROM {snap_fq}").fetchone()[0]
    if n_snap != 13:
        raise SystemExit(f"2A: snapshot row count {n_snap}, expected 13")
    log(f"2A OK — {n_snap} rows")

    log("2B: UPDATE ajcc8_t_stage FROM ajcc8_t_stage_v2 (11 rows) ...")
    n2b = len(
        con.execute(
            f"""
        UPDATE {cpm}
        SET ajcc8_t_stage = ajcc8_t_stage_v2
        WHERE ajcc8_t_stage IS NULL
          AND ajcc8_t_stage_v2 IS NOT NULL
          AND ajcc8_n_stage IS NOT NULL
          AND ajcc8_m_stage IS NOT NULL
          AND ajcc8_stage_group IS NULL
          AND diagnosis_primary IN ('PTC','FTC','HCC')
          AND research_id IN (
            SELECT research_id FROM {pre394}
            WHERE cohort_tag = '395_deferred_needs_T'
          )
          AND (
            dominant_tumor_ajcc8_t_stage IS NULL
            OR CAST(dominant_tumor_ajcc8_t_stage AS VARCHAR)
               = CAST(ajcc8_t_stage_v2 AS VARCHAR)
          )
        RETURNING research_id
        """
        ).fetchall()
    )
    log(f"2B RETURNING count={n2b}")
    if n2b != 11:
        raise SystemExit(f"2B: expected 11 rows updated, got {n2b}")

    log("2C: UPDATE ajcc8_stage_group + _corrected ...")
    n2c = len(
        con.execute(
            f"""
        UPDATE {cpm}
        SET ajcc8_stage_group = CASE
              WHEN ajcc8_t_stage IN ('T1','T1a','T1b','T2') AND ajcc8_n_stage = 'N0'      THEN 'I'
              WHEN ajcc8_t_stage IN ('T1','T1a','T1b','T2') AND ajcc8_n_stage LIKE 'N1%'  THEN 'II'
              WHEN ajcc8_t_stage IN ('T3a','T3b')                                         THEN 'II'
              WHEN ajcc8_t_stage = 'T4a'                                                  THEN 'III'
              WHEN ajcc8_t_stage = 'T4b'                                                  THEN 'IVA'
            END,
            ajcc8_stage_group_corrected = CASE
              WHEN ajcc8_t_stage IN ('T1','T1a','T1b','T2') AND ajcc8_n_stage = 'N0'      THEN 'I'
              WHEN ajcc8_t_stage IN ('T1','T1a','T1b','T2') AND ajcc8_n_stage LIKE 'N1%'  THEN 'II'
              WHEN ajcc8_t_stage IN ('T3a','T3b')                                         THEN 'II'
              WHEN ajcc8_t_stage = 'T4a'                                                  THEN 'III'
              WHEN ajcc8_t_stage = 'T4b'                                                  THEN 'IVA'
            END
        WHERE ajcc8_stage_group IS NULL
          AND ajcc8_t_stage IS NOT NULL
          AND ajcc8_n_stage IS NOT NULL
          AND ajcc8_m_stage = 'M0'
          AND age_at_surgery >= 55
          AND diagnosis_primary IN ('PTC','FTC','HCC')
          AND research_id IN (
            SELECT research_id FROM {pre394}
            WHERE cohort_tag = '395_deferred_needs_T'
          )
        RETURNING research_id
        """
        ).fetchall()
    )
    log(f"2C RETURNING count={n2c}")
    if n2c != 11:
        raise SystemExit(f"2C: expected 11 rows updated, got {n2c}")

    log("2D: CREATE queue table if needed + INSERT manual-review rows ...")
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{PUB_DB}"."{MS_SCHEMA}"')
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {queue_fq} (
          research_id VARCHAR,
          reason VARCHAR,
          path_stage_raw VARCHAR,
          gm_path_stage_raw VARCHAR,
          ajcc8_n_stage VARCHAR,
          ajcc8_m_stage VARCHAR,
          age_at_surgery INTEGER,
          diagnosis_primary VARCHAR,
          source_script VARCHAR,
          inserted_at TIMESTAMP
        )
        """
    )
    n_ins = len(
        con.execute(
            f"""
        INSERT INTO {queue_fq}
          (research_id, reason, path_stage_raw, gm_path_stage_raw,
           ajcc8_n_stage, ajcc8_m_stage, age_at_surgery, diagnosis_primary,
           source_script, inserted_at)
        SELECT CAST(cpm.research_id AS VARCHAR),
               'no_T_signal_path_stage_raw_III_ajcc_edition_unknown',
               CAST(cpm.path_stage_raw AS VARCHAR),
               CAST(cpm.gm_path_stage_raw AS VARCHAR),
               cpm.ajcc8_n_stage, cpm.ajcc8_m_stage,
               cpm.age_at_surgery, cpm.diagnosis_primary,
               '395', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
        FROM {cpm} cpm
        WHERE CAST(cpm.research_id AS VARCHAR) IN ('1404', '12198')
          AND NOT EXISTS (
            SELECT 1 FROM {queue_fq} q
            WHERE q.research_id = CAST(cpm.research_id AS VARCHAR)
              AND q.source_script = '395'
          )
        RETURNING research_id
        """
        ).fetchall()
    )
    log(f"2D INSERT RETURNING count={n_ins}")
    if n_ins != 2:
        raise SystemExit(f"2D: expected 2 inserts, got {n_ins}")

    prov = (
        "Script 395: canonical_patient_master T-sync + stage_group fill for 394-deferred cohort — "
        "11 rows ajcc8_t_stage<-ajcc8_t_stage_v2 (corroborated by dominant_tumor_ajcc8_t_stage where "
        "present) with ajcc8_stage_group derived via AJCC8 DTC age>=55 M0 rules (4xI, 7xII); "
        "ajcc8_stage_group_corrected mirrored. 2 rows (research_ids 1404, 12198) routed to "
        "manuscript_workspace.cpm_stage_group_manual_review_v1 as "
        "no_T_signal_path_stage_raw_III_ajcc_edition_unknown (need chart review to resolve T4a vs T3). "
        f"Snapshot: {ARC_SCHEMA}.{snap_name}."
    )
    log("2E: __readme")
    con.execute(
        f"""
        INSERT INTO {readme}(content, updated_at) VALUES
        (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP))
        """,
        [prov],
    )
    return snap_name


def run_verify(con: duckdb.DuckDBPyConnection, snap_name: str) -> dict[str, Any]:
    cpm = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'
    snap = f'"{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}"'
    readme = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"'
    queue_fq = f'"{PUB_DB}"."{MS_SCHEMA}"."{QUEUE_TABLE}"'
    pre394 = fq_pre394()

    v1 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE ajcc8_stage_group IS NULL
          AND ajcc8_n_stage IS NOT NULL
          AND ajcc8_m_stage IS NOT NULL
          AND diagnosis_primary IN ('PTC','FTC','HCC')
        """
    ).fetchone()[0]

    v2 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm} cpm
        JOIN {snap} s USING (research_id)
        WHERE s.cohort_tag = '395_t_synced'
          AND cpm.ajcc8_t_stage IS NOT NULL
          AND cpm.ajcc8_stage_group IS NOT NULL
          AND cpm.ajcc8_stage_group_corrected IS NOT NULL
        """
    ).fetchone()[0]

    v3 = con.execute(
        f"""
        SELECT cpm.ajcc8_stage_group, COUNT(*) AS n
        FROM {cpm} cpm
        JOIN {snap} s USING (research_id)
        WHERE s.cohort_tag = '395_t_synced'
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()

    v4 = con.execute(
        f"""
        SELECT SUM(CASE
          WHEN CAST(cpm.ajcc8_t_stage AS VARCHAR) != CAST(cpm.ajcc8_t_stage_v2 AS VARCHAR)
          THEN 1 ELSE 0 END)
        FROM {cpm} cpm
        JOIN {snap} s USING (research_id)
        WHERE s.cohort_tag = '395_t_synced'
        """
    ).fetchone()[0]

    v5 = con.execute(
        f"""
        SELECT SUM(CASE
          WHEN cpm.ajcc8_stage_group != cpm.ajcc8_stage_group_corrected
          THEN 1 ELSE 0 END)
        FROM {cpm} cpm
        JOIN {snap} s USING (research_id)
        WHERE s.cohort_tag = '395_t_synced'
        """
    ).fetchone()[0]

    v6 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm} cpm
        JOIN {snap} s USING (research_id)
        WHERE s.cohort_tag = '395_manual_review'
          AND cpm.ajcc8_t_stage IS NULL
          AND cpm.ajcc8_stage_group IS NULL
        """
    ).fetchone()[0]

    v7 = con.execute(
        f"SELECT COUNT(*) FROM {queue_fq} WHERE source_script = '395'"
    ).fetchone()[0]

    v8 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]

    v9 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE ajcc8_t_stage = 'T3b'
          AND ajcc8_n_stage IS NOT NULL AND ajcc8_m_stage IS NOT NULL
          AND ajcc8_stage_group IS NULL
          AND diagnosis_primary IN ('PTC','FTC','HCC')
        """
    ).fetchone()[0]

    v10 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm} cpm
        JOIN {pre394} s USING (research_id)
        WHERE s.cohort_tag = '394_fillable'
          AND cpm.ajcc8_stage_group IS NULL
        """
    ).fetchone()[0]

    v11 = con.execute(
        f"SELECT COUNT(*) FROM {readme} WHERE content LIKE 'Script 395:%'"
    ).fetchone()[0]

    dist = {r[0]: int(r[1]) for r in v3}
    dist_ok = dist == {"I": 4, "II": 7}

    all_ok = (
        v1 == 2
        and v2 == 11
        and v4 == 0
        and v5 == 0
        and v6 == 2
        and v7 == 2
        and v8 == CPM_EXPECTED
        and v9 == 0
        and v10 == 0
        and v11 == 1
        and dist_ok
    )

    return {
        "v1": v1,
        "v2": v2,
        "v3": v3,
        "v4": v4,
        "v5": v5,
        "v6": v6,
        "v7": v7,
        "v8": v8,
        "v9": v9,
        "v10": v10,
        "v11": v11,
        "dist_ok": dist_ok,
        "all_ok": all_ok,
    }


def format_close_out(verify: dict[str, Any], snap_name: str) -> str:
    return "\n".join(
        [
            "# Script 395 — Close-out (Phase 3)",
            "",
            f"**Snapshot:** `{ARC_SCHEMA}.{snap_name}`",
            "",
            "## Verification summary",
            "",
            "| Check | Value |",
            "|---|---|",
            f"| V1 n_orphans_remaining (DTC N/M set, stage_group NULL) | {verify['v1']} (expect 2) |",
            f"| V2 n_fully_filled (395_t_synced) | {verify['v2']} (expect 11) |",
            f"| V3 distribution | {verify['v3']} (expect I=4, II=7) |",
            f"| V3 dist_ok | {verify['dist_ok']} |",
            f"| V4 n_mismatch T vs T_v2 | {verify['v4']} (expect 0) |",
            f"| V5 n_mismatch stage vs corrected | {verify['v5']} (expect 0) |",
            f"| V6 manual_review still NULL T+stage | {verify['v6']} (expect 2) |",
            f"| V7 queue rows source_script=395 | {verify['v7']} (expect 2) |",
            f"| V8 n_cpm | {verify['v8']} (expect {CPM_EXPECTED}) |",
            f"| V9 T3b DTC orphans | {verify['v9']} (expect 0) |",
            f"| V10 394_fillable stage_group lost | {verify['v10']} (expect 0) |",
            f"| V11 __readme Script 395 rows | {verify['v11']} (expect 1) |",
            "",
            f"**Phase 3 pass:** {verify['all_ok']}",
            "",
            "## Carry-forward",
            "",
            "- **CF-395-1:** research_ids 1404, 12198 in manual-review queue — chart review for AJCC edition.",
            "- **CF-395-2:** Builder should COALESCE/fallback T from ajcc8_t_stage_v2 to avoid future gaps.",
            "",
        ]
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Run idempotency, apply, verify (default is probe only)",
    )
    ap.add_argument(
        "--force-stamp",
        default="",
        help="Override RUN_STAMP for snapshot name (default: UTC YYYYMMDD_HHMMSS)",
    )
    ap.add_argument(
        "--i-approve",
        default="",
        metavar="SHA256",
        help="Required for fresh --apply: SHA256 from probe-only PROBE_REPORT_SHA256 output.",
    )
    args = ap.parse_args()

    if args.apply:
        con0 = connect_quiet()
        try:
            snap, readme = idempotency_state(con0)
            if snap and readme:
                line = (
                    f"{datetime.now(timezone.utc).isoformat()} "
                    f"NO-OP: snapshot {snap} and Script 395 __readme present — "
                    "idempotent success (probe/close-out files untouched)."
                )
                print(line, flush=True)
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                with open(RUN_LOG_PATH, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
                return 0
            if snap and not readme:
                print("HALT: partial apply — snapshot exists but no __readme", flush=True)
                return 2
            if readme and not snap:
                print("HALT: missing snapshot — __readme without snapshot", flush=True)
                return 3
        finally:
            con0.close()

    fresh_log()
    con = connect()

    probe = run_probe(con)
    PRESTATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    prestate_body = format_probe_md(probe)
    PRESTATE_PATH.write_text(prestate_body, encoding="utf-8")
    log(f"Wrote {PRESTATE_PATH}")
    digest = probe_report_sha256(prestate_body)
    print(f"PROBE_REPORT_SHA256={digest}", flush=True)

    ok, errs = halt_gate(probe)
    if not ok:
        for e in errs:
            log(f"HALT: {e}")
        print("\n--- HALT GATE FAILED ---\n" + "\n".join(errs), flush=True)
        return 1

    if args.apply:
        approved = normalize_i_approve(args.i_approve)
        if not approved:
            log("HALT: fresh --apply requires --i-approve=<sha256> from probe-only output")
            print(
                "\nAfter greenlight, run:\n"
                f"  python3 scripts/395_dtc_t_sync_stage_group_fill.py --apply "
                f"--i-approve={digest}\n",
                flush=True,
            )
            return 5
        if approved != digest:
            log(
                f"HALT: --i-approve mismatch (file {digest}, got {approved}) — "
                "probe drift or wrong hash"
            )
            return 5

    if not args.apply:
        log("Probe only — re-run with --apply --i-approve=<sha256> after greenlight.")
        return 0

    run_stamp = args.force_stamp.strip() or datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S"
    )
    snap_name = apply_script(con, run_stamp)
    verify = run_verify(con, snap_name)
    CLOSE_OUT_PATH.write_text(format_close_out(verify, snap_name), encoding="utf-8")
    log(f"Wrote {CLOSE_OUT_PATH}")
    if not verify["all_ok"]:
        log("VERIFY FAILED — see close-out report")
        return 4
    log("Script 395 apply + verify complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
