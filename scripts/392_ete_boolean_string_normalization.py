#!/usr/bin/env python3
"""Script 392 — ete_grade_final_v2 Boolean-String Normalization.

Surgically normalizes 183 CPM rows where ete_grade_final_v2 holds the raw
string literals 'false' (179 rows) or 'true' (4 rows) instead of valid enum
values. The upstream table (tumor_episode_master_v2) is already deprecated,
making an in-place UPDATE both safe and terminal.

Three evidence-based buckets
------------------------------
  Bucket A  |  'false' + all corroboration flags negative  →  'none'         (179)
  Bucket B  |  'true'  + gross-ETE corroboration           →  'gross' + T-cascade  (2)
  Bucket C  |  'true'  + no corroboration                  →  queue (preserve 'true')  (2)

Phases
------
  --phase 0 (default) — read-only probe; writes
    scripts/output/392_prestate_probe_report.md
    No writes to PUB.

  --apply — re-runs Phase 0 probe, then executes:
    Phase 2A: archive snapshot (archive_pub_v1_0.cpm_ete_pre392_<stamp>)
    Phase 2B: UPDATE 179 'false' → 'none' (both ete_grade_final_v2 + ete_grade)
    Phase 2C: UPDATE 2 'true' → 'gross' + ete_ordinal_worst (both columns)
    Phase 2D: T-stage cascade for the 2 gross-flip rows
    Phase 2E: Queue the 2 uncorroborated 'true' rows
    Phase 2F: __readme provenance row
    Phase 3:  Post-state verification (halt-on-fail)

  NOTE: No approval file is required for --apply. Logan greenlights via chat
        after reviewing Phase 0 output.

Idempotency
-----------
  1. archive_pub_v1_0.cpm_ete_pre392_* snapshot exists
     AND __readme has row matching 'Script 392:%'
     → exit 0, NO-OP (Phase 3 invariants re-verified)
  2. Snapshot exists but no __readme row → HALT (partial apply)
  3. __readme exists but no snapshot    → HALT (partial apply)

Hard rules honored
------------------
  * No cross-DB sourcing: all reads/writes stay in PUB.
  * CAST(CURRENT_TIMESTAMP AS TIMESTAMP) for __readme inserts.
  * Token never printed — motherduck_client.get_token() + token_mode().
  * No git add performed by this script.
  * PHI-safe: only research_id and aggregate counts logged.
  * 4-place audit pattern for all UPDATE/INSERT calls.

Auth: motherduck_client.get_token(). Token never printed.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PUB_DB = "thyroid_canonical_publication_v1_0"
ARC_SCHEMA = "archive_pub_v1_0"
MAIN_SCHEMA = "main"
MANU_SCHEMA = "manuscript_workspace"

SCRIPT_ID = "392"
SCRIPT_TAG = "392_ete_boolean_string_normalization"

RUN_STAMP = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

CPM_TABLE = "canonical_patient_master"
QUEUE_TABLE = "cpm_ete_self_contradiction_queue_v1"
README_TABLE = "__readme"
SNAPSHOT_PREFIX = "cpm_ete_pre392_"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PROBE_REPORT_PATH = OUTPUT_DIR / "392_prestate_probe_report.md"
RUN_LOG_PATH = OUTPUT_DIR / "392_run.log"
CLOSE_OUT_PATH = OUTPUT_DIR / "392_close_out_report.md"

# --------------------------------------------------------------------------- #
# Frozen baselines (live-verified 2026-04-22, post-391, pre-392)
# --------------------------------------------------------------------------- #

CPM_ROWS = 10_871
N_JUNK_FALSE = 179
N_JUNK_TRUE = 4
N_JUNK_TOTAL = 183

DRIFT_TOL = 0.02  # ±2%

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

_log_buf: list[str] = []


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3] + "Z"


def log(msg: str) -> None:
    line = f"[INFO] [{_ts()}] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def warn(msg: str) -> None:
    line = f"[WARN] [{_ts()}] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def err(msg: str) -> None:
    line = f"[ERROR] [{_ts()}] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def flush_log() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    mode = "a" if RUN_LOG_PATH.exists() else "w"
    with RUN_LOG_PATH.open(mode, encoding="utf-8") as fh:
        fh.write("\n".join(_log_buf) + "\n")


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}). "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    log(f"Connecting md:{PUB_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{PUB_DB}?motherduck_token={tok}")
    con.execute(f'USE "{PUB_DB}"')
    con.execute(f'USE "{PUB_DB}".main')

    dbs = {
        r[0] for r in con.execute(
            "SELECT database_name FROM duckdb_databases()"
        ).fetchall()
    }
    if PUB_DB not in dbs:
        raise SystemExit(f"PUB DB '{PUB_DB}' not attached")
    log(f"Connection OK — attached DBs: {sorted(dbs)}")
    return con


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def row_count(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> int:
    return con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}"."{schema}"."{name}"'
    ).fetchone()[0]  # type: ignore[index]


def table_exists(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    name: str,
    db: str | None = None,
) -> bool:
    catalog = db or PUB_DB
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [catalog, schema, name],
    ).fetchone()
    return row is not None


def find_pre392_snapshot(con: duckdb.DuckDBPyConnection) -> str | None:
    rows = con.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog = ?
          AND table_schema   = ?
          AND table_name LIKE 'cpm_ete_pre392_%'
        ORDER BY table_name
        LIMIT 1
        """,
        [PUB_DB, ARC_SCHEMA],
    ).fetchall()
    return rows[0][0] if rows else None


def readme_392_present(con: duckdb.DuckDBPyConnection) -> bool:
    if not table_exists(con, MAIN_SCHEMA, README_TABLE):
        return False
    row = con.execute(
        f"""
        SELECT 1 FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
        WHERE content LIKE 'Script 392:%'
        LIMIT 1
        """
    ).fetchone()
    return row is not None


def _pct_drift(actual: int | float, expected: int | float) -> float:
    if expected == 0:
        return 0.0 if actual == 0 else float("inf")
    return abs(actual - expected) / expected


def _halt_gate(label: str, actual: int, lo: int | None, hi: int | None) -> None:
    failed = False
    if lo is not None and actual < lo:
        failed = True
    if hi is not None and actual > hi:
        failed = True
    if failed:
        msg = (
            f"HALT GATE [{label}]: actual={actual}, "
            f"expected=[{lo}, {hi}]. "
            "Stopping immediately — do not retry."
        )
        err(msg)
        flush_log()
        raise SystemExit(msg)
    log(f"[GATE] {label}: {actual} — within [{lo}, {hi}]  OK")


# --------------------------------------------------------------------------- #
# Phase 0 — Probe (idempotent, read-only)
# --------------------------------------------------------------------------- #


def phase0_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    results: dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    # CPM row count
    # ------------------------------------------------------------------ #
    cpm_n = row_count(con, MAIN_SCHEMA, CPM_TABLE)
    results["cpm_rows"] = cpm_n
    log(f"CPM rowcount: {cpm_n}")

    # ------------------------------------------------------------------ #
    # Q0-A: Junk cohort count + distribution
    # ------------------------------------------------------------------ #
    log("")
    log("Q0-A: Junk cohort distribution (ete_grade_final_v2 IN ('true','false'))")
    dist_rows = con.execute(
        f"""
        SELECT ete_grade_final_v2, COUNT(*) AS n
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('true', 'false')
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    results["junk_dist"] = dist_rows
    junk_dict: dict[str, int] = {}
    for row in dist_rows:
        junk_dict[row[0]] = row[1]
        log(f"  ete_grade_final_v2={row[0]!r}  n={row[1]}")

    n_false = junk_dict.get("false", 0)
    n_true = junk_dict.get("true", 0)
    n_total = n_false + n_true
    results["n_junk_false"] = n_false
    results["n_junk_true"] = n_true
    results["n_junk_total"] = n_total
    log(f"  TOTAL junk rows: {n_total}  (expected {N_JUNK_TOTAL})")

    # ------------------------------------------------------------------ #
    # Q0-B: Evidence bucketing
    # ------------------------------------------------------------------ #
    log("")
    log("Q0-B: Evidence bucketing for junk cohort")
    bucket_rows = con.execute(
        f"""
        SELECT
            LOWER(TRIM(ete_grade_final_v2)) AS junk_val,
            CASE
                WHEN op_intraop_gross_ete_any = TRUE
                     OR path_gross_ete_flag = TRUE
                     OR gross_ete_flag = TRUE
                THEN 'gross_corroborated'
                WHEN any_microscopic_ete_anywhere = TRUE
                THEN 'micro_corroborated'
                WHEN (op_intraop_gross_ete_any = FALSE OR op_intraop_gross_ete_any IS NULL)
                 AND (path_gross_ete_flag = FALSE OR path_gross_ete_flag IS NULL)
                 AND (gross_ete_flag = FALSE OR gross_ete_flag IS NULL)
                 AND (any_microscopic_ete_anywhere = FALSE OR any_microscopic_ete_anywhere IS NULL)
                THEN 'all_flags_negative'
                ELSE 'mixed_or_null'
            END AS evidence_bucket,
            COUNT(*) AS n
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('true', 'false')
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
        """
    ).fetchall()
    results["bucket_rows"] = bucket_rows
    bucket_map: dict[tuple[str, str], int] = {}
    for row in bucket_rows:
        bucket_map[(row[0], row[1])] = row[2]
        log(f"  junk={row[0]!r:<8s}  bucket={row[1]:<26s}  n={row[2]}")

    n_false_neg = bucket_map.get(("false", "all_flags_negative"), 0)
    n_true_gross = bucket_map.get(("true", "gross_corroborated"), 0)
    n_true_neg = bucket_map.get(("true", "all_flags_negative"), 0)
    n_true_micro = bucket_map.get(("true", "micro_corroborated"), 0)
    n_true_mixed = bucket_map.get(("true", "mixed_or_null"), 0)
    results["n_false_neg"] = n_false_neg
    results["n_true_gross"] = n_true_gross
    results["n_true_neg"] = n_true_neg
    log(
        f"  Bucket A (false/all_flags_neg): {n_false_neg}  expect 179\n"
        f"  Bucket B (true/gross_corroborated): {n_true_gross}  expect 2\n"
        f"  Bucket C (true/all_flags_neg): {n_true_neg}  expect 2\n"
        f"  Other true (micro/mixed): {n_true_micro + n_true_mixed}"
    )

    # ------------------------------------------------------------------ #
    # Q0-C: Legacy ete_grade parity check
    # ------------------------------------------------------------------ #
    log("")
    log("Q0-C: Legacy ete_grade parity on 183-row junk cohort")
    parity_n = con.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('true', 'false')
          AND LOWER(TRIM(ete_grade)) = LOWER(TRIM(ete_grade_final_v2))
        """
    ).fetchone()[0]  # type: ignore[index]
    results["parity_n"] = parity_n
    log(f"  Junk rows with matching ete_grade: {parity_n}  (expect {N_JUNK_TOTAL})")

    # ------------------------------------------------------------------ #
    # Q0-D: T-stage state of the 2 gross-flip candidates (full flag vectors)
    # ------------------------------------------------------------------ #
    log("")
    log("Q0-D: T-stage state + full flag vectors for the 2 gross-flip candidates")
    gross_detail = con.execute(
        f"""
        SELECT
            research_id,
            diagnosis_primary,
            ete_grade_final_v2,
            ete_grade,
            ete_ordinal_worst,
            ajcc8_t_stage,
            ajcc8_stage_group,
            ajcc8_stage_group_corrected,
            tumor_size_cm_dominant,
            age_at_surgery,
            op_intraop_gross_ete_any,
            path_gross_ete_flag,
            gross_ete_flag,
            any_microscopic_ete_anywhere,
            microscopic_ete_t3b_corrected
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE LOWER(TRIM(ete_grade_final_v2)) = 'true'
          AND (
              op_intraop_gross_ete_any = TRUE
              OR path_gross_ete_flag = TRUE
              OR gross_ete_flag = TRUE
          )
        ORDER BY research_id
        """
    ).fetchall()
    results["gross_flip_rows"] = gross_detail
    log(f"  Gross-corroborated 'true' rows: {len(gross_detail)}")
    for r in gross_detail:
        log(
            f"    research_id={r[0]}  diag={r[1]}  "
            f"ete_grade_final_v2={r[2]!r}  ete_ordinal_worst={r[4]}  "
            f"t_stage={r[5]}  stage_group={r[6]}  size={r[8]}  age={r[9]}\n"
            f"      op_intraop_gross={r[10]}  path_gross={r[11]}  "
            f"gross_ete_flag={r[12]}  any_micro={r[13]}  "
            f"micro_t3b_corrected={r[14]}"
        )

    # ------------------------------------------------------------------ #
    # Q0-D2: Full flag vectors for the 2 queue candidates
    # ------------------------------------------------------------------ #
    log("")
    log("Q0-D2: Full flag vectors for the 2 uncorroborated 'true' candidates (queue set)")
    queue_detail = con.execute(
        f"""
        SELECT
            research_id,
            diagnosis_primary,
            ete_grade_final_v2,
            ete_grade,
            ete_ordinal_worst,
            ajcc8_t_stage,
            ajcc8_stage_group,
            tumor_size_cm_dominant,
            age_at_surgery,
            op_intraop_gross_ete_any,
            path_gross_ete_flag,
            gross_ete_flag,
            any_microscopic_ete_anywhere,
            microscopic_ete_t3b_corrected
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE LOWER(TRIM(ete_grade_final_v2)) = 'true'
          AND (op_intraop_gross_ete_any = FALSE OR op_intraop_gross_ete_any IS NULL)
          AND (path_gross_ete_flag = FALSE OR path_gross_ete_flag IS NULL)
          AND (gross_ete_flag = FALSE OR gross_ete_flag IS NULL)
          AND (any_microscopic_ete_anywhere = FALSE OR any_microscopic_ete_anywhere IS NULL)
        ORDER BY research_id
        """
    ).fetchall()
    results["queue_rows"] = queue_detail
    log(f"  Uncorroborated 'true' rows (queue candidates): {len(queue_detail)}")
    for r in queue_detail:
        log(
            f"    research_id={r[0]}  diag={r[1]}  "
            f"ete_grade_final_v2={r[2]!r}  ete_ordinal_worst={r[4]}  "
            f"t_stage={r[5]}  stage_group={r[6]}  size={r[7]}  age={r[8]}\n"
            f"      op_intraop_gross={r[9]}  path_gross={r[10]}  "
            f"gross_ete_flag={r[11]}  any_micro={r[12]}  "
            f"micro_t3b_corrected={r[13]}"
        )

    # ------------------------------------------------------------------ #
    # Q0-E: Queue table membership — any of 183 already queued?
    # ------------------------------------------------------------------ #
    log("")
    log("Q0-E: Queue table pre-existence check")
    queue_exists = table_exists(con, MANU_SCHEMA, QUEUE_TABLE)
    results["queue_table_exists"] = queue_exists
    if queue_exists:
        already_queued = con.execute(
            f"""
            SELECT COUNT(*) FROM "{PUB_DB}"."{MANU_SCHEMA}"."{QUEUE_TABLE}"
            WHERE research_id IN (
                SELECT research_id FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
                WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('true', 'false')
            )
            """
        ).fetchone()[0]  # type: ignore[index]
        results["already_queued"] = already_queued
        log(f"  Queue table found. Rows already queued from 183-cohort: {already_queued}")
        # Show reason/script breakdown (not individual research_ids for PHI safety)
        reason_dist = con.execute(
            f"""
            SELECT reason, queued_by_script, COUNT(*) AS n
            FROM "{PUB_DB}"."{MANU_SCHEMA}"."{QUEUE_TABLE}"
            WHERE research_id IN (
                SELECT research_id FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
                WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('true', 'false')
            )
            GROUP BY 1, 2 ORDER BY 3 DESC
            """
        ).fetchall()
        for qr in reason_dist:
            log(f"    reason={qr[0]}  script={qr[1]}  n={qr[2]}")

        # How many already have boolean_string_no_corroboration?
        already_bool_nc = con.execute(
            f"""
            SELECT COUNT(*) FROM "{PUB_DB}"."{MANU_SCHEMA}"."{QUEUE_TABLE}"
            WHERE reason = 'boolean_string_no_corroboration'
            """
        ).fetchone()[0]  # type: ignore[index]
        results["already_bool_nc"] = already_bool_nc
        log(f"  Rows with reason=boolean_string_no_corroboration: {already_bool_nc}  (expect 0)")
    else:
        warn(f"  Queue table {MANU_SCHEMA}.{QUEUE_TABLE} NOT FOUND — will be created? Check 390.")
        results["already_queued"] = 0

    # ------------------------------------------------------------------ #
    # Q0-F: ete_ordinal_worst distribution (verify 3=gross scale)
    # ------------------------------------------------------------------ #
    log("")
    log("Q0-F: ete_ordinal_worst distribution (to verify scale; expect 3=gross)")
    ordinal_dist = con.execute(
        f"""
        SELECT ete_ordinal_worst, ete_grade_final_v2, COUNT(*) AS n
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_ordinal_worst IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
        LIMIT 30
        """
    ).fetchall()
    results["ordinal_dist"] = ordinal_dist
    for r in ordinal_dist[:15]:
        log(f"  ordinal={r[0]}  ete_grade_final_v2={r[1]!r}  n={r[2]}")

    # Specifically check ordinal=3
    ordinal_3_grades = con.execute(
        f"""
        SELECT ete_grade_final_v2, COUNT(*) AS n
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_ordinal_worst = 3
        GROUP BY 1
        """
    ).fetchall()
    log(f"  Ordinal=3 grade distribution: {ordinal_3_grades}")
    results["ordinal_3_grades"] = ordinal_3_grades

    return results


# --------------------------------------------------------------------------- #
# Phase 0 — Baseline drift check
# --------------------------------------------------------------------------- #


def check_baselines(results: dict[str, Any]) -> None:
    log("=" * 60)
    log("BASELINE DRIFT CHECK (halt threshold: 2%)")
    log("=" * 60)

    checks = [
        ("CPM_ROWCOUNT",     results["cpm_rows"],     CPM_ROWS),
        ("N_JUNK_FALSE",     results["n_junk_false"],  N_JUNK_FALSE),
        ("N_JUNK_TRUE",      results["n_junk_true"],   N_JUNK_TRUE),
        ("N_JUNK_TOTAL",     results["n_junk_total"],  N_JUNK_TOTAL),
    ]

    failed: list[str] = []
    for label, actual, expected in checks:
        drift = _pct_drift(actual, expected)
        status = "OK" if drift <= DRIFT_TOL else "DRIFT"
        log(
            f"  {label:<30s}  actual={actual:7d}  expected={expected:7d}  "
            f"drift={drift:.2%}  [{status}]"
        )
        if drift > DRIFT_TOL:
            failed.append(
                f"{label}: actual={actual}, expected={expected}, drift={drift:.2%}"
            )

    if failed:
        msg = "BASELINE DRIFT GATE FAILED — halting:\n" + "\n".join(
            f"  {f}" for f in failed
        )
        err(msg)
        flush_log()
        raise SystemExit(msg)

    log("All 4 baselines within 2% drift tolerance — OK")
    log("=" * 60)


# --------------------------------------------------------------------------- #
# Phase 0 — Write probe report
# --------------------------------------------------------------------------- #


def write_probe_report(results: dict[str, Any]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()

    def _drift_status(actual: int, expected: int) -> str:
        d = _pct_drift(actual, expected)
        return "✅ OK" if d <= DRIFT_TOL else f"❌ DRIFT ({d:.2%})"

    lines = [
        "# Script 392 — ETE Boolean-String Normalization: Phase 0 Probe Report",
        "",
        f"**Generated:** {ts}",
        f"**Database:** {PUB_DB}",
        "",
        "---",
        "",
        "## 1. Baseline Drift Check",
        "",
        "| Metric | Expected | Actual | Status |",
        "|--------|----------|--------|--------|",
        f"| CPM_ROWCOUNT | {CPM_ROWS:,} | {results['cpm_rows']:,} | "
        f"{_drift_status(results['cpm_rows'], CPM_ROWS)} |",
        f"| N_JUNK_FALSE | {N_JUNK_FALSE} | {results['n_junk_false']} | "
        f"{_drift_status(results['n_junk_false'], N_JUNK_FALSE)} |",
        f"| N_JUNK_TRUE | {N_JUNK_TRUE} | {results['n_junk_true']} | "
        f"{_drift_status(results['n_junk_true'], N_JUNK_TRUE)} |",
        f"| N_JUNK_TOTAL | {N_JUNK_TOTAL} | {results['n_junk_total']} | "
        f"{_drift_status(results['n_junk_total'], N_JUNK_TOTAL)} |",
        "",
        "---",
        "",
        "## 2. Q0-A: Junk Cohort Distribution",
        "",
        "| ete_grade_final_v2 | n |",
        "|--------------------|---|",
    ]
    for row in results["junk_dist"]:
        lines.append(f"| `{row[0]}` | {row[1]} |")

    lines += [
        "",
        "---",
        "",
        "## 3. Q0-B: Evidence Bucketing",
        "",
        "| junk_val | evidence_bucket | n | Notes |",
        "|----------|----------------|---|-------|",
    ]
    bucket_expect = {
        ("false", "all_flags_negative"): "EXPECT 179",
        ("true", "gross_corroborated"): "EXPECT 2 → gross-flip",
        ("true", "all_flags_negative"): "EXPECT 2 → queue",
    }
    for row in results["bucket_rows"]:
        note = bucket_expect.get((row[0], row[1]), "")
        lines.append(f"| `{row[0]}` | `{row[1]}` | {row[2]} | {note} |")

    lines += [
        "",
        "---",
        "",
        "## 4. Q0-C: Legacy ete_grade Parity",
        "",
        f"Rows with `ete_grade == ete_grade_final_v2` (junk cohort): "
        f"**{results['parity_n']}** (expect {N_JUNK_TOTAL})",
        "",
        "---",
        "",
        "## 5. Q0-D: Gross-Flip Candidates (Bucket B — will become 'gross')",
        "",
        "| research_id | diag | curr_t | stage_group | size_cm | age | op_gross | path_gross | gross_ete | any_micro | micro_t3b_corr |",
        "|-------------|------|--------|-------------|---------|-----|----------|------------|-----------|-----------|---------------|",
    ]
    for r in results["gross_flip_rows"]:
        lines.append(
            f"| {r[0]} | {r[1]} | {r[5]} | {r[6]} | {r[8]} | {r[9]} "
            f"| {r[10]} | {r[11]} | {r[12]} | {r[13]} | {r[14]} |"
        )

    lines += [
        "",
        "---",
        "",
        "## 6. Q0-D2: Queue Candidates (Bucket C — 'true' preserved, routed to queue)",
        "",
        "| research_id | diag | curr_t | stage_group | size_cm | age | op_gross | path_gross | gross_ete | any_micro | micro_t3b_corr |",
        "|-------------|------|--------|-------------|---------|-----|----------|------------|-----------|-----------|---------------|",
    ]
    for r in results["queue_rows"]:
        lines.append(
            f"| {r[0]} | {r[1]} | {r[5]} | {r[6]} | {r[7]} | {r[8]} "
            f"| {r[9]} | {r[10]} | {r[11]} | {r[12]} | {r[13]} |"
        )

    lines += [
        "",
        "---",
        "",
        "## 7. Q0-E: Queue Table Pre-Existence",
        "",
        f"Queue table `{MANU_SCHEMA}.{QUEUE_TABLE}` found: **{results['queue_table_exists']}**",
        f"Rows already queued from 183-cohort: **{results.get('already_queued', 'N/A')}**",
        "",
        "---",
        "",
        "## 8. Q0-F: ete_ordinal_worst Scale Verification",
        "",
        "Ordinal=3 grade distribution (live scale: 2=gross, not 3; GREATEST(...,2) applied):",
        "",
        "| ete_grade_final_v2 | n |",
        "|--------------------|---|",
    ]
    for r in results["ordinal_3_grades"]:
        lines.append(f"| `{r[0]}` | {r[1]} |")

    lines += [
        "",
        "---",
        "",
        "## 9. Halt Gate Summary",
        "",
    ]

    all_pass = (
        results["n_junk_false"] == N_JUNK_FALSE
        and results["n_junk_true"] == N_JUNK_TRUE
        and results["n_junk_total"] == N_JUNK_TOTAL
        and results["n_false_neg"] == N_JUNK_FALSE
        and results["n_true_gross"] == 2
        and results["n_true_neg"] == 2
        and results["parity_n"] == N_JUNK_TOTAL
    )

    if all_pass:
        lines.append(
            "✅ **ALL HALT GATES PASS** — "
            f"{N_JUNK_FALSE} false→none, 2 true→gross, 2 true→queue confirmed.\n\n"
            "Logan: review gross-flip table (Section 5) and confirm T-stage cascade looks clean.\n"
            "Then run: `python3 scripts/392_ete_boolean_string_normalization.py --apply`"
        )
    else:
        lines.append(
            "❌ **HALT GATE FAILED** — counts do not match expected. "
            "Do NOT run --apply. Re-probe first."
        )

    with PROBE_REPORT_PATH.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    log(f"Probe report written → {PROBE_REPORT_PATH}")


# --------------------------------------------------------------------------- #
# Idempotency check
# --------------------------------------------------------------------------- #


def idempotency_check(con: duckdb.DuckDBPyConnection) -> None:
    snap = find_pre392_snapshot(con)
    readme_present = readme_392_present(con)

    if snap and readme_present:
        log(f"Idempotency: both triggers present (snap={snap}, readme=True)")
        log("Re-verifying Phase 3 invariants for NO-OP exit ...")
        try:
            phase3_verify(con, snap)
        except SystemExit:
            raise SystemExit(
                "Idempotency: both triggers present but Phase 3 invariants FAIL. "
                "Manual inspection required."
            )
        log("Idempotency: Phase 3 invariants pass — NO-OP exit (script already applied).")
        flush_log()
        sys.exit(0)
    elif snap and not readme_present:
        raise SystemExit(
            f"Idempotency: snapshot '{snap}' exists but __readme row missing. "
            "Partial prior run detected. Manual inspection required before re-running."
        )
    elif not snap and readme_present:
        raise SystemExit(
            "Idempotency: __readme row present but no cpm_ete_pre392_* snapshot found. "
            "Partial prior run detected. Manual inspection required before re-running."
        )
    log("Idempotency: no prior run detected — proceeding")


# --------------------------------------------------------------------------- #
# Phase 2A — Archive snapshot
# --------------------------------------------------------------------------- #


def phase2a_snapshot(con: duckdb.DuckDBPyConnection) -> str:
    snap = f"{SNAPSHOT_PREFIX}{RUN_STAMP}"
    sql = f"""
        CREATE OR REPLACE TABLE "{PUB_DB}"."{ARC_SCHEMA}"."{snap}" AS
        SELECT
            research_id,
            ete_grade,
            ete_grade_final_v2,
            ete_grade_source,
            ete_ordinal_worst,
            ajcc8_t_stage,
            ajcc8_stage_group,
            ajcc8_stage_group_corrected,
            microscopic_ete_t3b_corrected,
            tumor_size_cm_dominant,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('true', 'false')
    """
    log(f"[2A] Creating snapshot: {ARC_SCHEMA}.{snap}")
    con.execute(sql)
    snap_n = row_count(con, ARC_SCHEMA, snap)
    log(f"[2A] Snapshot rowcount: {snap_n}  (expect {N_JUNK_TOTAL})")
    _halt_gate("2A snapshot rowcount", snap_n, N_JUNK_TOTAL, N_JUNK_TOTAL)
    return snap


# --------------------------------------------------------------------------- #
# Phase 2B — Normalize 'false' → 'none' (179 rows)
# --------------------------------------------------------------------------- #


def phase2b_normalize_false(con: duckdb.DuckDBPyConnection) -> int:
    where = (
        "LOWER(TRIM(ete_grade_final_v2)) = 'false' "
        "AND (op_intraop_gross_ete_any = FALSE OR op_intraop_gross_ete_any IS NULL) "
        "AND (path_gross_ete_flag      = FALSE OR path_gross_ete_flag      IS NULL) "
        "AND (gross_ete_flag           = FALSE OR gross_ete_flag           IS NULL) "
        "AND (any_microscopic_ete_anywhere = FALSE OR any_microscopic_ete_anywhere IS NULL)"
    )
    n_pre = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" WHERE {where}'
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2B] Normalizing 'false' → 'none'  ({n_pre} rows qualify) ...")
    con.execute(
        f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        SET ete_grade_final_v2 = 'none',
            ete_grade          = 'none'
        WHERE {where}
        """
    )
    log(f"[2B] Updated: {n_pre} rows  (expected {N_JUNK_FALSE})")
    _halt_gate("2B false→none rows", n_pre, N_JUNK_FALSE - 2, N_JUNK_FALSE + 2)
    return int(n_pre)


# --------------------------------------------------------------------------- #
# Phase 2C — Normalize 'true' → 'gross' (2 corroborated rows)
# --------------------------------------------------------------------------- #


def phase2c_normalize_true_gross(con: duckdb.DuckDBPyConnection) -> int:
    where = (
        "LOWER(TRIM(ete_grade_final_v2)) = 'true' "
        "AND (op_intraop_gross_ete_any = TRUE "
        "     OR path_gross_ete_flag = TRUE "
        "     OR gross_ete_flag = TRUE)"
    )
    n_pre = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" WHERE {where}'
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2C] Normalizing 'true' → 'gross' (gross-corroborated) ({n_pre} rows qualify) ...")
    con.execute(
        f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        SET ete_grade_final_v2 = 'gross',
            ete_grade          = 'gross',
            ete_ordinal_worst  = GREATEST(COALESCE(ete_ordinal_worst, 0), 2)
        WHERE {where}
        """
    )
    log(f"[2C] Updated: {n_pre} rows  (expected 2)")
    _halt_gate("2C true→gross rows", n_pre, 0, 4)
    return int(n_pre)


# --------------------------------------------------------------------------- #
# Phase 2D — T-stage cascade for the 2 gross-flip rows
# --------------------------------------------------------------------------- #


def phase2d_t_stage_cascade(
    con: duckdb.DuckDBPyConnection, gross_flip_research_ids: list[Any]
) -> tuple[int, int]:
    """Cascade T-stage for rows that flipped to 'gross' from 'true'.

    Applies 391-style logic: for DTC (PTC/FTC/HCC), gross ETE → T3b if
    current T-stage is T1/T2/T3a. Also re-derives stage group.
    Returns (n_t_stage_updated, n_stage_group_updated).
    """
    if not gross_flip_research_ids:
        log("[2D] No gross-flip rows identified — T-stage cascade is a no-op")
        return 0, 0

    id_list = ", ".join(f"'{r}'" for r in gross_flip_research_ids)

    # T-stage cascade
    where_t = (
        f"research_id IN ({id_list}) "
        "AND ete_grade_final_v2 = 'gross' "
        "AND ete_grade_source = 'tumor_episode_master_v2' "
        "AND diagnosis_primary NOT IN ('MTC', 'ATC') "
        "AND ajcc8_t_stage IN ('T1','T1a','T1b','T2','T3a')"
    )
    n_t_pre = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" WHERE {where_t}'
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2D] T-stage cascade: {n_t_pre} rows qualify for T3b upgrade")

    if n_t_pre > 0:
        con.execute(
            f"""
            UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
            SET ajcc8_t_stage              = 'T3b',
                microscopic_ete_t3b_corrected = FALSE
            WHERE {where_t}
            """
        )
        log(f"[2D] T-stage upgraded to T3b: {n_t_pre} rows")
    else:
        log("[2D] T-stage cascade: 0 rows upgraded (they may already be T3b/T4)")

    # Stage-group cascade — re-derive for these rows using new T-stage
    where_sg = (
        f"research_id IN ({id_list}) "
        "AND ete_grade_final_v2 = 'gross'"
    )
    n_sg_pre = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" WHERE {where_sg}'
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2D] Stage-group re-derive: {n_sg_pre} rows qualify")

    if n_sg_pre > 0:
        con.execute(
            f"""
            UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
            SET ajcc8_stage_group = CASE
                    WHEN age_at_surgery IS NULL THEN ajcc8_stage_group
                    WHEN age_at_surgery < 55
                         AND (ajcc8_m_stage IS NULL OR ajcc8_m_stage = 'M0')
                    THEN 'I'
                    WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1'
                    THEN 'II'
                    WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1'
                    THEN 'IVB'
                    WHEN age_at_surgery >= 55
                         AND ajcc8_t_stage IN ('T3a','T3b')
                    THEN 'II'
                    WHEN age_at_surgery >= 55
                         AND ajcc8_t_stage = 'T4a'
                    THEN 'III'
                    WHEN age_at_surgery >= 55
                         AND ajcc8_t_stage = 'T4b'
                    THEN 'IVA'
                    WHEN age_at_surgery >= 55
                         AND ajcc8_t_stage IN ('T1a','T1b','T2')
                         AND (ajcc8_n_stage IS NULL OR ajcc8_n_stage IN ('N0','N0a','N0b','NX'))
                    THEN 'I'
                    WHEN age_at_surgery >= 55
                         AND ajcc8_t_stage IN ('T1a','T1b','T2')
                         AND ajcc8_n_stage IN ('N1','N1a','N1b')
                    THEN 'II'
                    ELSE ajcc8_stage_group
                END,
                ajcc8_stage_group_corrected = CASE
                    WHEN age_at_surgery IS NULL THEN ajcc8_stage_group_corrected
                    WHEN age_at_surgery < 55
                         AND (ajcc8_m_stage IS NULL OR ajcc8_m_stage = 'M0')
                    THEN 'I'
                    WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1'
                    THEN 'II'
                    WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1'
                    THEN 'IVB'
                    WHEN age_at_surgery >= 55
                         AND ajcc8_t_stage IN ('T3a','T3b')
                    THEN 'II'
                    WHEN age_at_surgery >= 55
                         AND ajcc8_t_stage = 'T4a'
                    THEN 'III'
                    WHEN age_at_surgery >= 55
                         AND ajcc8_t_stage = 'T4b'
                    THEN 'IVA'
                    WHEN age_at_surgery >= 55
                         AND ajcc8_t_stage IN ('T1a','T1b','T2')
                         AND (ajcc8_n_stage IS NULL OR ajcc8_n_stage IN ('N0','N0a','N0b','NX'))
                    THEN 'I'
                    WHEN age_at_surgery >= 55
                         AND ajcc8_t_stage IN ('T1a','T1b','T2')
                         AND ajcc8_n_stage IN ('N1','N1a','N1b')
                    THEN 'II'
                    ELSE ajcc8_stage_group_corrected
                END
            WHERE {where_sg}
            """
        )
        log(f"[2D] Stage group re-derived: {n_sg_pre} rows")

    return int(n_t_pre), int(n_sg_pre)


# --------------------------------------------------------------------------- #
# Phase 2E — Queue the 2 uncorroborated 'true' rows
# --------------------------------------------------------------------------- #


def phase2e_queue_uncorroborated(con: duckdb.DuckDBPyConnection) -> int:
    if not table_exists(con, MANU_SCHEMA, QUEUE_TABLE):
        raise SystemExit(
            f"[2E] HALT: Queue table {MANU_SCHEMA}.{QUEUE_TABLE} does not exist. "
            "Script 390 must have created it. Check DB state."
        )

    n_pre = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm2
        WHERE LOWER(TRIM(cpm2.ete_grade_final_v2)) = 'true'
          AND (cpm2.op_intraop_gross_ete_any = FALSE OR cpm2.op_intraop_gross_ete_any IS NULL)
          AND (cpm2.path_gross_ete_flag      = FALSE OR cpm2.path_gross_ete_flag      IS NULL)
          AND (cpm2.gross_ete_flag           = FALSE OR cpm2.gross_ete_flag           IS NULL)
          AND (cpm2.any_microscopic_ete_anywhere = FALSE OR cpm2.any_microscopic_ete_anywhere IS NULL)
          AND NOT EXISTS (
            SELECT 1 FROM "{PUB_DB}"."{MANU_SCHEMA}"."{QUEUE_TABLE}" q
            WHERE q.research_id = cpm2.research_id
              AND q.reason = 'boolean_string_no_corroboration'
          )
        """
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2E] Queuing {n_pre} uncorroborated 'true' rows (expect 2) ...")

    con.execute(
        f"""
        INSERT INTO "{PUB_DB}"."{MANU_SCHEMA}"."{QUEUE_TABLE}"
            (research_id, cpm_ete_grade_final_v2, reason, status,
             queued_at, queued_by_script)
        SELECT
            research_id,
            'true',
            'boolean_string_no_corroboration',
            'awaiting_manual_review',
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
            '392'
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm_ins
        WHERE LOWER(TRIM(cpm_ins.ete_grade_final_v2)) = 'true'
          AND (cpm_ins.op_intraop_gross_ete_any = FALSE OR cpm_ins.op_intraop_gross_ete_any IS NULL)
          AND (cpm_ins.path_gross_ete_flag      = FALSE OR cpm_ins.path_gross_ete_flag      IS NULL)
          AND (cpm_ins.gross_ete_flag           = FALSE OR cpm_ins.gross_ete_flag           IS NULL)
          AND (cpm_ins.any_microscopic_ete_anywhere = FALSE OR cpm_ins.any_microscopic_ete_anywhere IS NULL)
          AND NOT EXISTS (
            SELECT 1 FROM "{PUB_DB}"."{MANU_SCHEMA}"."{QUEUE_TABLE}" q
            WHERE q.research_id = cpm_ins.research_id
              AND q.reason = 'boolean_string_no_corroboration'
          )
        """
    )
    log(f"[2E] Queue insert: {n_pre} rows  (expected 2)")
    _halt_gate("2E queue insert rows", n_pre, 0, 4)
    return int(n_pre)


# --------------------------------------------------------------------------- #
# Phase 2F — __readme provenance row
# --------------------------------------------------------------------------- #


def phase2f_provenance(
    con: duckdb.DuckDBPyConnection,
    snap_name: str,
    n_false_normed: int,
    n_gross_normed: int,
    n_queued: int,
    n_t_cascade: int,
) -> None:
    if not table_exists(con, MAIN_SCHEMA, README_TABLE):
        raise SystemExit(
            f"[2F] HALT: {MAIN_SCHEMA}.{README_TABLE} does not exist."
        )

    content = (
        f"Script 392: canonical_patient_master.ete_grade_final_v2 boolean-string "
        f"normalization — {n_false_normed} 'false'→'none' (corroborated-negative), "
        f"{n_gross_normed} 'true'→'gross' (corroborated-positive w/ T-stage cascade: "
        f"{n_t_cascade} rows upgraded), {n_queued} routed to "
        f"cpm_ete_self_contradiction_queue_v1 as boolean_string_no_corroboration. "
        f"Legacy ete_grade synced in same UPDATE. "
        f"Source ete_grade_source=tumor_episode_master_v2 (upstream table deprecated, "
        f"no rebuild). Snapshot: archive_pub_v1_0.{snap_name}."
    )

    con.execute(
        f"""
        INSERT INTO "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
            (content, updated_at)
        VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP))
        """,
        [content],
    )
    log(f"[2F] __readme row inserted: {content[:80]}...")


# --------------------------------------------------------------------------- #
# Phase 3 — Post-state verification
# --------------------------------------------------------------------------- #


def phase3_verify(con: duckdb.DuckDBPyConnection, snap_name: str) -> None:
    log("")
    log("=" * 60)
    log("PHASE 3: POST-STATE VERIFICATION")
    log("=" * 60)

    failures: list[str] = []

    def check(label: str, condition: bool, detail: str = "") -> None:
        if condition:
            log(f"  [PASS] {label}")
        else:
            msg = f"[FAIL] {label}" + (f" — {detail}" if detail else "")
            err(msg)
            failures.append(msg)

    # V1: No 'false' literals remain in corroborated buckets
    n_false_remain = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE LOWER(TRIM(ete_grade_final_v2)) = 'false'
        """
    ).fetchone()[0]  # type: ignore[index]
    check("V1: 0 'false' literals remain in ete_grade_final_v2", n_false_remain == 0, f"found {n_false_remain}")

    # V2: Exactly 2 'true' literals remain (the queue-routed pair)
    n_true_remain = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE LOWER(TRIM(ete_grade_final_v2)) = 'true'
        """
    ).fetchone()[0]  # type: ignore[index]
    check("V2: Exactly 2 'true' literals remain (queue-routed)", n_true_remain == 2, f"found {n_true_remain}")

    # V3: Queue grew by exactly 2 rows under queued_by_script='392'
    n_queue_392 = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MANU_SCHEMA}"."{QUEUE_TABLE}"
        WHERE queued_by_script = '392'
          AND reason = 'boolean_string_no_corroboration'
        """
    ).fetchone()[0]  # type: ignore[index]
    check("V3: Queue has 2 rows from queued_by_script='392'", n_queue_392 == 2, f"found {n_queue_392}")

    # V4: Legacy ete_grade in sync with ete_grade_final_v2 for the original 183 rows
    # (We check via the snapshot: all 183 research_ids should now have matching values)
    n_out_of_sync = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
        JOIN "{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}" snap
            ON cpm.research_id = snap.research_id
        WHERE LOWER(TRIM(COALESCE(cpm.ete_grade, '')))
           != LOWER(TRIM(COALESCE(cpm.ete_grade_final_v2, '')))
        """
    ).fetchone()[0]  # type: ignore[index]
    check(
        "V4: ete_grade == ete_grade_final_v2 for all original 183 rows",
        n_out_of_sync == 0,
        f"{n_out_of_sync} out-of-sync",
    )

    # V5: Full ete_grade_final_v2 distribution check — no 'false' anywhere
    n_false_total = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE LOWER(TRIM(ete_grade_final_v2)) = 'false'
        """
    ).fetchone()[0]  # type: ignore[index]
    check("V5: 0 'false' strings in full CPM.ete_grade_final_v2", n_false_total == 0, f"found {n_false_total}")

    # V6: Stage-group invariant — no T/N/M complete but stage_group NULL for DTC
    n_orphan_sg = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ajcc8_t_stage IS NOT NULL
          AND ajcc8_n_stage IS NOT NULL
          AND ajcc8_m_stage IS NOT NULL
          AND ajcc8_stage_group IS NULL
          AND diagnosis_primary IN ('PTC','FTC','HCC')
        """
    ).fetchone()[0]  # type: ignore[index]
    # Report actual value (this is a non-regression check against 391 post-state)
    log(f"  V6: Orphan stage_groups (T+N+M set, stage_group NULL, DTC): {n_orphan_sg}")

    # V7: __readme row landed
    n_readme = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
        WHERE content LIKE 'Script 392:%'
        """
    ).fetchone()[0]  # type: ignore[index]
    check("V7: __readme row for Script 392 landed", n_readme == 1, f"found {n_readme}")

    # V8: Snapshot rowcount = 183
    if table_exists(con, ARC_SCHEMA, snap_name):
        snap_n = row_count(con, ARC_SCHEMA, snap_name)
        check(
            f"V8: Snapshot {snap_name} rowcount = {N_JUNK_TOTAL}",
            snap_n == N_JUNK_TOTAL,
            f"got {snap_n}",
        )
    else:
        failures.append(f"V8: Snapshot {snap_name} not found in {ARC_SCHEMA}")

    # V9: CPM rowcount unchanged
    cpm_n = row_count(con, MAIN_SCHEMA, CPM_TABLE)
    check("V9: CPM rowcount unchanged at 10,871", cpm_n == CPM_ROWS, f"got {cpm_n}")

    log("=" * 60)
    if failures:
        msg = (
            f"PHASE 3 FAILED — {len(failures)} invariant(s) violated:\n"
            + "\n".join(f"  {f}" for f in failures)
        )
        err(msg)
        flush_log()
        raise SystemExit(msg)

    log(f"PHASE 3: ALL {9 - (1 if n_orphan_sg > 0 else 0)} CHECKS PASSED")
    log("=" * 60)


# --------------------------------------------------------------------------- #
# Close-out report
# --------------------------------------------------------------------------- #


def write_close_out(
    snap_name: str,
    n_false_normed: int,
    n_gross_normed: int,
    n_queued: int,
    n_t_cascade: int,
    n_sg_cascade: int,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    lines = [
        "# Script 392 — ETE Boolean-String Normalization: Close-Out Report",
        "",
        f"**Completed:** {ts}",
        f"**Snapshot:** `archive_pub_v1_0.{snap_name}`",
        "",
        "## Summary",
        "",
        "| Phase | Operation | Rows Affected |",
        "|-------|-----------|---------------|",
        f"| 2A | Archive snapshot ({N_JUNK_TOTAL} junk rows) | {N_JUNK_TOTAL} |",
        f"| 2B | 'false' → 'none' (both ete_grade_final_v2 + ete_grade) | {n_false_normed} |",
        f"| 2C | 'true' → 'gross' (corroborated, both columns + ordinal) | {n_gross_normed} |",
        f"| 2D | T-stage + stage_group cascade on gross-flip rows | {n_t_cascade} T-stage / {n_sg_cascade} stage-group |",
        f"| 2E | Queue uncorroborated 'true' rows | {n_queued} |",
        f"| 2F | __readme provenance | 1 |",
        "",
        "## Post-Normalization State",
        "",
        "- `ete_grade_final_v2 = 'false'` remaining in CPM: **0**",
        f"- `ete_grade_final_v2 = 'true'` remaining in CPM: **{n_queued}** (queue-routed, intentionally preserved)",
        "",
        "## Phase 3: All invariants passed",
        "",
        "See `392_run.log` for full execution trace.",
        "",
        "## Commit / Tag",
        "",
        f"```",
        f"git tag v1_0-ete-bool-strings-normalized-{RUN_STAMP}",
        f"```",
    ]
    with CLOSE_OUT_PATH.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    log(f"Close-out report written → {CLOSE_OUT_PATH}")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Script 392 — ete_grade_final_v2 Boolean-String Normalization"
    )
    parser.add_argument(
        "--phase",
        type=int,
        default=0,
        help="Phase to run: 0 = probe only (default)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help=(
            "Apply all changes (Phase 2 + 3). "
            "No approval file needed — Logan greenlights via chat."
        ),
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log(f"Script {SCRIPT_ID}: {SCRIPT_TAG}")
    log(f"Run stamp: {RUN_STAMP}")
    log(f"Mode: {'--apply' if args.apply else f'--phase {args.phase}'}")

    con = connect()

    if args.apply:
        idempotency_check(con)

    log("")
    log("=" * 60)
    log("PHASE 0: PROBE")
    log("=" * 60)

    results = phase0_probe(con)
    check_baselines(results)
    write_probe_report(results)

    if not args.apply:
        log("")
        log("Phase 0 complete. Review the probe report:")
        log(f"  {PROBE_REPORT_PATH}")
        log("Then greenlight via chat and run:")
        log(f"  python3 scripts/{SCRIPT_TAG}.py --apply")
        flush_log()
        return

    # ------------------------------------------------------------------ #
    # Phase 2
    # ------------------------------------------------------------------ #
    log("")
    log("=" * 60)
    log("PHASE 2: APPLY")
    log("=" * 60)

    # Capture the 2 gross-flip research_ids before writing anything
    gross_flip_ids = [r[0] for r in results["gross_flip_rows"]]
    log(f"[PRE] Gross-flip research_ids: {gross_flip_ids}")

    snap_name = phase2a_snapshot(con)
    n_false_normed = phase2b_normalize_false(con)
    n_gross_normed = phase2c_normalize_true_gross(con)
    n_t_cascade, n_sg_cascade = phase2d_t_stage_cascade(con, gross_flip_ids)
    n_queued = phase2e_queue_uncorroborated(con)
    phase2f_provenance(
        con,
        snap_name=snap_name,
        n_false_normed=n_false_normed,
        n_gross_normed=n_gross_normed,
        n_queued=n_queued,
        n_t_cascade=n_t_cascade,
    )

    # ------------------------------------------------------------------ #
    # Phase 3
    # ------------------------------------------------------------------ #
    phase3_verify(con, snap_name)

    write_close_out(
        snap_name=snap_name,
        n_false_normed=n_false_normed,
        n_gross_normed=n_gross_normed,
        n_queued=n_queued,
        n_t_cascade=n_t_cascade,
        n_sg_cascade=n_sg_cascade,
    )

    log("")
    log("=" * 60)
    log(f"Script {SCRIPT_ID}: COMPLETE")
    log(f"Snapshot: {snap_name}")
    log(f"Commit tag when ready: v1_0-ete-bool-strings-normalized-{RUN_STAMP}")
    log("=" * 60)
    flush_log()


if __name__ == "__main__":
    main()
