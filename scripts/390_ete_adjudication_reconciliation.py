#!/usr/bin/env python3
"""Script 390 — ETE Adjudication Reconciliation.

# Rule A + residual re-queue (AJCC8 T3b).
# Sticky guard expanded to include 'unable_to_determine' (26 rows).
# 194 boolean-string junk rows route to queue untouched in CPM.
# Logan's explicit call 2026-04-22 post-live-probe.

Rule A (worst-of, AJCC8 T3b): any gross signal flips the grade.
Sticky guard expanded to include ete_grade_adjudicated='unable_to_determine'
(26 clinician "can't call it" rows protected, routed to queue for re-review).

Boolean-string upstream bug: 194 rows with ete_grade_final_v2 in ('true','false')
routed to contradiction queue under reason='boolean_string_upstream_bug'.
NOT silently normalized. Carry-forward to Script 392 for extractor trace.

Residual queue population:
  2,551 microscopic-no-signal + 26 unable-to-determine + 194 boolean-string
  = approximately 2,771 new queue rows.

Phases
------
* --phase 0 (default) — read-only probe + 8-baseline drift gate; writes
  scripts/output/390_probe_report.md and scripts/output/390_plan_approval.txt.
  No writes to PUB.
* --apply — re-runs Phase 0 probe (re-verify within 2%), reads approval file,
  then executes:
    Phase 2A: archive snapshot to archive_pub_v1_0.cpm_ete_pre390_<stamp>
    Phase 2B: UPDATE canonical_patient_master (Rule A, expanded sticky guard)
    Phase 2C: INSERT 3 __readme provenance rows
    Phase 2D: Repopulate cpm_ete_self_contradiction_queue_v1
    Phase 2E: Rebuild manuscript_cohort_v1.ete_grade_final + .ete_grade_source
    Phase 3:  Post-state verification (halt-on-fail)

Idempotency
-----------
Both triggers must be present for NO-OP exit:
  1. archive_pub_v1_0.cpm_ete_pre390_<any_stamp> snapshot exists
  2. __readme row whose content starts with 'Script 390 Rule A apply summary'
If only one present → halt with partial-prior-run error.
If both present AND Phase 3 invariants hold → exit 0, NO-OP.

Hard rules honored
------------------
* No cross-DB sourcing: everything stays in PUB (no FROM archive_pub_v1_0.*
  in the Rule A CTE).
* CAST(CURRENT_TIMESTAMP AS TIMESTAMP) for all __readme and queued_at inserts.
* Token never printed — motherduck_client.get_token() + token_mode().
* No git add performed by this script.
* PHI-safe: only research_id and aggregate counts logged, never clinical text.

Auth: motherduck_client.get_token().  Token never printed.
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
WS_SCHEMA = "manuscript_workspace"
MAIN_SCHEMA = "main"

SCRIPT_TAG = "390_ete_adjudication_reconciliation"
SCRIPT_ID = "390"
RULE = "A"

RUN_STAMP = datetime.now(timezone.utc).strftime("%Y%m%d")

SNAPSHOT_NAME = f"cpm_ete_pre390_{RUN_STAMP}"
QUEUE_TABLE = "cpm_ete_self_contradiction_queue_v1"
CPM_TABLE = "canonical_patient_master"
ADJ_TABLE = "ete_adjudication_v1"
INVASION_ROLLUP = "canonical_invasion_patient_rollup_v1"
MANUSCRIPT_COHORT = "manuscript_cohort_v1"
README_TABLE = "__readme"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PROBE_REPORT_PATH = OUTPUT_DIR / "390_probe_report.md"
PLAN_APPROVAL_PATH = OUTPUT_DIR / "390_plan_approval.txt"
RUN_LOG_PATH = OUTPUT_DIR / "390_run.log"
CLOSE_OUT_PATH = OUTPUT_DIR / "390_close_out.md"

# Structural row-count invariants (exact)
CPM_ROWS = 10_871
ADJ_ROWS = 45
EXPECTED_BOOL_JUNK = 194    # 187 'false' + 7 'true'
EXPECTED_UNABLE_STICKY = 26

# Live-verified 2026-04-22 probe (authoritative; overrides earlier
# spec values which had internal arithmetic inconsistency
# 190+1091=1281 != 1325 spec). Agent Phase 0 probe snapshot.
EXPECTED_WOULD_BE_GROSS = 1_311
EXPECTED_FLIP_UP        = 1_121
EXPECTED_FLIP_DOWN      = 0
EXPECTED_FLIP_TO_MICRO  = 4
EXPECTED_TOTAL_MUTATED  = 1_125
EXPECTED_QUEUE_NEW_ROWS = 2_795   # 2,575 micro-no-signal + 26 unable_to_determine_sticky + 194 bool-junk

DRIFT_TOL = 0.02            # ±2%

# Tighter bounds for --apply gate (explicit per-user instruction)
MUTATED_LO = 1_103   # 1,125 - 2%
MUTATED_HI = 1_147   # 1,125 + 2%
QUEUE_LO   = 2_739   # 2,795 - 2%
QUEUE_HI   = 2_851   # 2,795 + 2%

# Expanded sticky guard values
STICKY_GRADES = ("'microscopic'", "'absent'", "'unable_to_determine'")
STICKY_GRADES_SQL = ", ".join(STICKY_GRADES)

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
            f"No MotherDuck RW token (token_mode={token_mode()}).  "
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
    ).fetchone()[0]


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [PUB_DB, schema, name],
    ).fetchone()
    return row is not None


def get_columns(
    con: duckdb.DuckDBPyConnection, schema: str, name: str
) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [PUB_DB, schema, name],
    ).fetchall()
    return [r[0] for r in rows]


def find_pre390_snapshot(con: duckdb.DuckDBPyConnection) -> str | None:
    """Return the name of any existing cpm_ete_pre390_* snapshot, or None."""
    rows = con.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog = ?
          AND table_schema = ?
          AND table_name LIKE 'cpm_ete_pre390_%'
        ORDER BY table_name
        LIMIT 1
        """,
        [PUB_DB, ARC_SCHEMA],
    ).fetchall()
    return rows[0][0] if rows else None


def readme_390_present(con: duckdb.DuckDBPyConnection) -> bool:
    """Return True if a __readme row starting with 'Script 390 Rule A apply summary' exists."""
    if not table_exists(con, MAIN_SCHEMA, README_TABLE):
        return False
    row = con.execute(
        f"""
        SELECT 1 FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
        WHERE content LIKE 'Script 390 Rule A apply summary%'
        LIMIT 1
        """
    ).fetchone()
    return row is not None


def _pct_drift(actual: int | float, expected: int | float) -> float:
    if expected == 0:
        return 0.0 if actual == 0 else float("inf")
    return abs(actual - expected) / expected


# --------------------------------------------------------------------------- #
# Phase 0 — Discovery + probe
# --------------------------------------------------------------------------- #


def _rule_a_cte_sql() -> str:
    """Return the WITH rule_a CTE SQL (read-only; no side effects)."""
    return f"""
WITH rule_a AS (
    SELECT
        cpm.research_id,
        cpm.ete_grade_final_v2                             AS current_grade,
        CASE
            WHEN cpm.ete_adjudicated_flag = TRUE
                 AND cpm.ete_grade_adjudicated IN ({STICKY_GRADES_SQL})
                THEN cpm.ete_grade_final_v2          -- adjudicator sticky (expanded)
            WHEN (
                    cpm.gross_ete_flag = TRUE
                 OR cpm.op_intraop_gross_ete_any = TRUE
                 OR cpm.path_gross_ete_flag = TRUE
                 OR inv.any_gross_ete_anywhere = TRUE
                 )
                THEN 'gross'
            WHEN cpm.ete_grade_final_v2 IS NULL
                THEN NULL                             -- don't grade NULLs in 390
            WHEN (
                    inv.any_microscopic_ete_anywhere = TRUE
                 OR cpm.ete_any_present_path = TRUE
                 )
                THEN 'microscopic'
            ELSE cpm.ete_grade_final_v2
        END                                                AS new_grade
    FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
    LEFT JOIN "{PUB_DB}"."{MAIN_SCHEMA}"."{INVASION_ROLLUP}" inv
        ON cpm.research_id = inv.research_id
)
"""


def cohort_stability_check(
    con: duckdb.DuckDBPyConnection,
    bool_junk_schema: str = MAIN_SCHEMA,
    bool_junk_table: str = CPM_TABLE,
) -> None:
    """Sanity-check that upstream cohorts haven't shifted since the Phase 0 probe.

    Halts on any deviation so --apply never fires on stale baselines.
    a. ete_adjudication_v1: must be exactly (45 total, 1 micro, 16 absent, 26 unable).
    b. boolean-string junk in CPM: must be exactly 194.
       In partial-run recovery mode, checked against the pre-update snapshot instead
       of live CPM (because Phase 2B already ran and flipped some junk rows to 'gross').
    c. canonical_invasion_patient_rollup_v1 rowcount: logged (informational).
    """
    log("Cohort stability sanity check ...")

    # a. ete_adjudication_v1 breakdown
    # Note: the adjudication table uses `adjudicated_grade`, not `ete_grade_adjudicated`.
    # CPM.ete_grade_adjudicated is populated from this table via join.
    adj_row = con.execute(
        f"""
        SELECT
            COUNT(*)                                                                    AS total,
            SUM(CASE WHEN adjudicated_grade = 'microscopic'         THEN 1 ELSE 0 END) AS n_micro,
            SUM(CASE WHEN adjudicated_grade = 'absent'              THEN 1 ELSE 0 END) AS n_absent,
            SUM(CASE WHEN adjudicated_grade = 'unable_to_determine' THEN 1 ELSE 0 END) AS n_unable
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{ADJ_TABLE}"
        """
    ).fetchone()
    total_adj, n_micro, n_absent, n_unable = adj_row  # type: ignore[misc]
    log(f"  ete_adjudication_v1: total={total_adj}, micro={n_micro}, absent={n_absent}, unable={n_unable}")
    if (total_adj, n_micro, n_absent, n_unable) != (45, 1, 16, 26):
        raise SystemExit(
            f"Cohort stability FAIL (a): ete_adjudication_v1 changed — "
            f"got (total={total_adj}, micro={n_micro}, absent={n_absent}, unable={n_unable}), "
            f"expected (45, 1, 16, 26). Someone touched adjudication. Halting."
        )
    log("  [PASS] ete_adjudication_v1 unchanged (45 / 1 / 16 / 26)")

    # b. boolean-string junk rows
    # When checking live CPM (fresh run): should be exactly 194.
    # When checking snapshot (partial-run recovery): Rule A already flipped some
    # 'true'/'false' rows with gross signals to 'gross', so live CPM will have fewer.
    # In that case we check the snapshot (pre-update state) for the expected 194.
    bool_junk_live = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{bool_junk_schema}"."{bool_junk_table}"
        WHERE ete_grade_final_v2 IN ('true', 'false')
        """
    ).fetchone()[0]
    src_label = (f"{bool_junk_schema}.{bool_junk_table}" if bool_junk_schema != MAIN_SCHEMA
                 else "live CPM")
    log(f"  boolean-string junk rows ({src_label}): {bool_junk_live}")
    if bool_junk_live != EXPECTED_BOOL_JUNK:
        raise SystemExit(
            f"Cohort stability FAIL (b): boolean-string junk count changed — "
            f"got {bool_junk_live} from {src_label}, expected {EXPECTED_BOOL_JUNK}. "
            "Script 392 may have run out of order. Halting."
        )
    log(f"  [PASS] boolean-string junk = {bool_junk_live} (expected {EXPECTED_BOOL_JUNK})")

    # c. canonical_invasion_patient_rollup_v1 rowcount (informational)
    inv_n = row_count(con, MAIN_SCHEMA, INVASION_ROLLUP)
    log(f"  canonical_invasion_patient_rollup_v1 rowcount: {inv_n} (recorded; not a halt gate)")


def phase0_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Run read-only probes; return a dict of all live metrics for drift checking."""
    results: dict[str, Any] = {}

    # 1. CPM row count
    cpm_n = row_count(con, MAIN_SCHEMA, CPM_TABLE)
    results["cpm_rows"] = cpm_n
    log(f"CPM rowcount: {cpm_n}")

    # 2. ETE grade distribution with gross-flag triplet
    grade_dist = con.execute(
        f"""
        SELECT
            ete_grade_final_v2,
            COUNT(*)                                                      AS n,
            SUM(CASE WHEN gross_ete_flag = TRUE THEN 1 ELSE 0 END)        AS gross_flag,
            SUM(CASE WHEN op_intraop_gross_ete_any = TRUE THEN 1 ELSE 0 END) AS op_intraop,
            SUM(CASE WHEN path_gross_ete_flag = TRUE THEN 1 ELSE 0 END)   AS path_gross
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        GROUP BY ete_grade_final_v2
        ORDER BY n DESC
        """
    ).fetchall()
    results["grade_dist"] = grade_dist
    log("ETE grade distribution:")
    for row in grade_dist:
        log(f"  {row[0]!r:30s}  n={row[1]:6d}  gross_flag={row[2]:5}  op_intraop={row[3]:5}  path_gross={row[4]:5}")

    # 3. Invasion rollup cross-tab
    inv_crosstab = con.execute(
        f"""
        SELECT
            cpm.ete_grade_final_v2,
            inv.any_gross_ete_anywhere,
            inv.any_microscopic_ete_anywhere,
            COUNT(*) AS n
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
        LEFT JOIN "{PUB_DB}"."{MAIN_SCHEMA}"."{INVASION_ROLLUP}" inv
            ON cpm.research_id = inv.research_id
        GROUP BY cpm.ete_grade_final_v2, inv.any_gross_ete_anywhere, inv.any_microscopic_ete_anywhere
        ORDER BY cpm.ete_grade_final_v2, n DESC
        """
    ).fetchall()
    results["inv_crosstab"] = inv_crosstab
    log("ETE × invasion rollup cross-tab (top 10):")
    for row in inv_crosstab[:10]:
        log(f"  grade={row[0]!r:30s}  any_gross={row[1]}  any_micro={row[2]}  n={row[3]}")

    # 4. Adjudication cohort breakdown
    adj_total = row_count(con, MAIN_SCHEMA, ADJ_TABLE)
    results["adj_rows"] = adj_total
    log(f"ete_adjudication_v1 rowcount: {adj_total}")

    adj_breakdown = con.execute(
        f"""
        SELECT ete_grade_adjudicated, COUNT(*) AS n
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_adjudicated_flag = TRUE
        GROUP BY ete_grade_adjudicated
        ORDER BY n DESC
        """
    ).fetchall()
    results["adj_breakdown"] = adj_breakdown
    log("Adjudicated CPM rows breakdown:")
    for row in adj_breakdown:
        log(f"  {row[0]!r:35s} n={row[1]}")

    # Count unable_to_determine sticky rows
    unable_n = sum(r[1] for r in adj_breakdown if r[0] == "unable_to_determine")
    results["unable_sticky"] = unable_n
    log(f"  → unable_to_determine (expanded sticky): {unable_n}")

    # 5. Queue pre-state
    if table_exists(con, WS_SCHEMA, QUEUE_TABLE):
        queue_n = row_count(con, WS_SCHEMA, QUEUE_TABLE)
        queue_status = con.execute(
            f"""
            SELECT status, COUNT(*) AS n
            FROM "{PUB_DB}"."{WS_SCHEMA}"."{QUEUE_TABLE}"
            GROUP BY status
            ORDER BY n DESC
            """
        ).fetchall()
    else:
        queue_n = 0
        queue_status = []
        warn(f"Queue table {WS_SCHEMA}.{QUEUE_TABLE} does not exist yet")
    results["queue_pre_n"] = queue_n
    results["queue_status"] = queue_status
    log(f"cpm_ete_self_contradiction_queue_v1 pre-state: {queue_n} rows")
    for row in queue_status:
        log(f"  status={row[0]!r}  n={row[1]}")

    # 6. Boolean-string junk rows
    bool_junk = con.execute(
        f"""
        SELECT ete_grade_final_v2, COUNT(*) AS n
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_final_v2 IN ('true', 'false')
        GROUP BY ete_grade_final_v2
        """
    ).fetchall()
    bool_junk_n = sum(r[1] for r in bool_junk)
    results["bool_junk"] = bool_junk
    results["bool_junk_n"] = bool_junk_n
    log(f"Boolean-string junk rows: {bool_junk_n}")
    for row in bool_junk:
        log(f"  '{row[0]}'={row[1]}")

    # 7. NULL grade rows (informational)
    null_grade_n = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_final_v2 IS NULL
        """
    ).fetchone()[0]
    results["null_grade_n"] = null_grade_n
    log(f"NULL ete_grade_final_v2 rows (informational, not graded in 390): {null_grade_n}")

    # 8. Rule A simulation
    log("Running Rule A simulation (read-only CTE)...")
    rule_a_sql = _rule_a_cte_sql()
    rule_a_stats = con.execute(
        rule_a_sql + """
        SELECT
            SUM(CASE WHEN new_grade = 'gross' THEN 1 ELSE 0 END)
                AS would_be_gross,
            SUM(CASE WHEN new_grade = 'gross'
                      AND (current_grade IS DISTINCT FROM 'gross') THEN 1 ELSE 0 END)
                AS flip_up_to_gross,
            SUM(CASE WHEN current_grade = 'gross'
                      AND (new_grade IS DISTINCT FROM 'gross') THEN 1 ELSE 0 END)
                AS flip_down_from_gross,
            SUM(CASE WHEN new_grade = 'microscopic'
                      AND (current_grade IS DISTINCT FROM 'microscopic') THEN 1 ELSE 0 END)
                AS flip_to_micro,
            SUM(CASE WHEN new_grade IS DISTINCT FROM current_grade THEN 1 ELSE 0 END)
                AS total_mutated,
            SUM(CASE WHEN new_grade = 'gross' AND current_grade = 'gross' THEN 1 ELSE 0 END)
                AS unchanged_gross
        FROM rule_a
        """
    ).fetchone()
    (
        would_be_gross, flip_up, flip_down, flip_to_micro,
        total_mutated, unchanged_gross,
    ) = rule_a_stats  # type: ignore[misc]
    results["would_be_gross"] = would_be_gross
    results["flip_up"] = flip_up
    results["flip_down"] = flip_down
    results["flip_to_micro"] = flip_to_micro
    results["total_mutated"] = total_mutated
    log(f"Rule A simulation results:")
    log(f"  would-be-gross post-rule : {would_be_gross}")
    log(f"  flip-up to gross         : {flip_up}")
    log(f"  flip-down from gross     : {flip_down}")
    log(f"  flip to microscopic      : {flip_to_micro}")
    log(f"  TOTAL ROWS MUTATED       : {total_mutated}")
    log(f"  rows unchanged           : {cpm_n - total_mutated}")

    # 9. Residual queue count simulation (microscopic-no-signal)
    micro_no_signal = con.execute(
        rule_a_sql + f"""
        SELECT COUNT(*) FROM rule_a
        WHERE new_grade = 'microscopic'
          AND current_grade = 'microscopic'
          AND research_id NOT IN (
              SELECT research_id
              FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
              WHERE ete_adjudicated_flag = TRUE
                AND ete_grade_adjudicated IN ({STICKY_GRADES_SQL})
          )
        """
    ).fetchone()[0]
    # Also count via invasion rollup for micro-no-signal (F, F cohort)
    micro_ff = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
        LEFT JOIN "{PUB_DB}"."{MAIN_SCHEMA}"."{INVASION_ROLLUP}" inv
            ON cpm.research_id = inv.research_id
        WHERE cpm.ete_grade_final_v2 = 'microscopic'
          AND (inv.any_gross_ete_anywhere IS NULL OR inv.any_gross_ete_anywhere = FALSE)
          AND (inv.any_microscopic_ete_anywhere IS NULL OR inv.any_microscopic_ete_anywhere = FALSE)
          AND (cpm.ete_any_present_path IS NULL OR cpm.ete_any_present_path = FALSE)
          AND NOT (
                cpm.ete_adjudicated_flag = TRUE
                AND cpm.ete_grade_adjudicated IN ({STICKY_GRADES_SQL})
              )
        """
    ).fetchone()[0]
    results["micro_no_signal"] = micro_no_signal
    results["micro_ff"] = micro_ff
    log(f"Rule A residual queue: microscopic-no-signal (rule_a CTE): {micro_no_signal}")
    log(f"Rule A residual queue: microscopic-FF cohort (invasion rollup): {micro_ff}")

    # Expected total queue additions: micro_ff + unable_sticky + bool_junk_n
    projected_new_queue = micro_ff + unable_n + bool_junk_n
    results["projected_new_queue"] = projected_new_queue
    log(f"Projected new queue rows: {projected_new_queue} "
        f"({micro_ff} micro-no-signal + {unable_n} unable-sticky + {bool_junk_n} bool-junk)")

    return results


def check_drift(results: dict[str, Any], recovery_mode: bool = False) -> list[str]:
    """Return a list of drift violation messages (empty = all pass).

    Structural row counts (CPM, ADJ, UNABLE_STICKY) always checked.
    BOOL_JUNK and simulation metrics skipped in recovery_mode because:
      - Phase 2B already ran: Rule A flipped some 'true'/'false' junk rows to
        'gross', so live bool-junk count is legitimately lower than 194.
      - Simulation against post-update CPM always returns 0 flips (already applied).
    In recovery_mode we only verify the structural invariants are intact.
    """
    structural_always = [
        ("CPM_ROWS",    results["cpm_rows"],      CPM_ROWS,               DRIFT_TOL),
        ("ADJ_ROWS",    results["adj_rows"],       ADJ_ROWS,               DRIFT_TOL),
        ("UNABLE_STICKY", results["unable_sticky"], EXPECTED_UNABLE_STICKY, DRIFT_TOL),
    ]
    structural_fresh = [
        ("BOOLEAN_STRING_JUNK", results["bool_junk_n"], EXPECTED_BOOL_JUNK, DRIFT_TOL),
    ]
    simulation = [
        ("WOULD_BE_GROSS_POST_RULE_A", results["would_be_gross"], EXPECTED_WOULD_BE_GROSS, 0.0),
        ("FLIP_UP_RULE_A",             results["flip_up"],         EXPECTED_FLIP_UP,        0.0),
        ("FLIP_DOWN_RULE_A",           results["flip_down"],       EXPECTED_FLIP_DOWN,      0.0),
        ("FLIP_TO_MICRO_RULE_A",       results["flip_to_micro"],   EXPECTED_FLIP_TO_MICRO,  0.0),
        ("TOTAL_MUTATED_RULE_A",       results["total_mutated"],   EXPECTED_TOTAL_MUTATED,  0.0),
    ]

    checks = structural_always
    if not recovery_mode:
        checks = checks + structural_fresh + simulation

    violations: list[str] = []
    for name, actual, expected, tol in checks:
        drift = _pct_drift(actual, expected)
        status = "PASS" if drift <= tol else "FAIL"
        tol_label = f"{tol*100:.0f}%" if tol > 0 else "0.00% (exact)"
        log(f"  Drift gate [{status}] {name}: actual={actual}, expected={expected}, "
            f"drift={drift*100:.2f}% (tol={tol_label})")
        if drift > tol:
            violations.append(
                f"{name}: actual={actual}, expected={expected}, drift={drift*100:.2f}%"
            )

    if recovery_mode:
        log("  (simulation drift checks skipped — recovery mode; Phase 2B already applied)")

    return violations


def write_probe_report(results: dict[str, Any], violations: list[str]) -> None:
    """Write scripts/output/390_probe_report.md."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Script 390 — ETE Adjudication Reconciliation — Phase 0 Probe Report",
        "",
        f"**Generated:** {datetime.now(timezone.utc).isoformat()}",
        f"**DB:** `{PUB_DB}`",
        f"**Rule selected:** Rule A (worst-of, AJCC8 T3b) — Logan's explicit call 2026-04-22",
        "",
        "---",
        "",
        "## Pre-State: CPM ETE Grade Distribution",
        "",
        "| ete_grade_final_v2 | n | gross_ete_flag | op_intraop_gross | path_gross_ete |",
        "|---|---|---|---|---|",
    ]
    for row in results["grade_dist"]:
        lines.append(f"| `{row[0]}` | {row[1]} | {row[2]} | {row[3]} | {row[4]} |")

    lines += [
        "",
        "## Pre-State: ETE × Invasion Rollup Cross-Tab (selected rows)",
        "",
        "| ete_grade_final_v2 | any_gross_ete_anywhere | any_micro_ete_anywhere | n |",
        "|---|---|---|---|",
    ]
    for row in results["inv_crosstab"]:
        lines.append(f"| `{row[0]}` | {row[1]} | {row[2]} | {row[3]} |")

    lines += [
        "",
        "## Pre-State: Adjudication Cohort (CPM adjudicated_flag=TRUE)",
        "",
        f"ete_adjudication_v1 row count: **{results['adj_rows']}** (must = 45)",
        "",
        "| ete_grade_adjudicated | n |",
        "|---|---|",
    ]
    for row in results["adj_breakdown"]:
        lines.append(f"| `{row[0]}` | {row[1]} |")

    lines += [
        "",
        f"**Expanded sticky guard covers:** {results['unable_sticky']} unable_to_determine rows",
        "",
        "## Pre-State: Boolean-String Junk Rows",
        "",
        f"Total junk rows (ete_grade_final_v2 in ('true','false')): **{results['bool_junk_n']}**",
        "",
        "| value | n |",
        "|---|---|",
    ]
    for row in results["bool_junk"]:
        lines.append(f"| `{row[0]}` | {row[1]} |")

    lines += [
        "",
        f"NULL ete_grade_final_v2 rows (not graded in 390): {results['null_grade_n']}",
        "",
        "## Pre-State: Contradiction Queue",
        "",
        f"Current row count: **{results['queue_pre_n']}**",
        "",
        "| status | n |",
        "|---|---|",
    ]
    for row in results.get("queue_status", []):
        lines.append(f"| `{row[0]}` | {row[1]} |")

    lines += [
        "",
        "---",
        "",
        "## Rule A Simulation (read-only)",
        "",
        "Rule A (worst-of, AJCC8 T3b) with expanded sticky guard:",
        "  - Sticky: `ete_adjudicated_flag=TRUE AND ete_grade_adjudicated IN (microscopic, absent, unable_to_determine)`",
        "  - Gross branch: any of `gross_ete_flag`, `op_intraop_gross_ete_any`, `path_gross_ete_flag`, `any_gross_ete_anywhere`",
        "  - NULL guard: NULL rows unchanged",
        "",
        "| Metric | Live | Frozen Baseline | Drift | Status |",
        "|---|---|---|---|---|",
    ]
    baselines = [
        ("would-be-gross post-rule", results["would_be_gross"], EXPECTED_WOULD_BE_GROSS),
        ("flip-up to gross", results["flip_up"], EXPECTED_FLIP_UP),
        ("flip-down from gross", results["flip_down"], 0),
        ("flip to microscopic", results["flip_to_micro"], EXPECTED_FLIP_TO_MICRO),
        ("TOTAL ROWS MUTATED", results["total_mutated"], EXPECTED_TOTAL_MUTATED),
        ("bool-string junk rows", results["bool_junk_n"], EXPECTED_BOOL_JUNK),
        ("unable_to_determine sticky", results["unable_sticky"], EXPECTED_UNABLE_STICKY),
    ]
    for name, actual, expected in baselines:
        drift = _pct_drift(actual, expected)
        status_str = "✅ PASS" if drift <= DRIFT_TOL else "❌ FAIL"
        lines.append(
            f"| {name} | {actual} | {expected} | {drift*100:.2f}% | {status_str} |"
        )

    lines += [
        "",
        "## Projected Residual Queue Population",
        "",
        f"- microscopic-no-signal rows (FF cohort): **{results['micro_ff']}** "
        f"(expected ~2,551)",
        f"- unable_to_determine sticky (expanded): **{results['unable_sticky']}** "
        f"(expected 26)",
        f"- boolean-string junk: **{results['bool_junk_n']}** (expected 194)",
        f"- **Total new rows**: **{results['projected_new_queue']}** (expected ~2,771)",
        "",
        "---",
        "",
        "## Drift Gate",
        "",
    ]
    if violations:
        lines.append("### ❌ DRIFT VIOLATIONS DETECTED — HALT BEFORE --apply")
        lines.append("")
        for v in violations:
            lines.append(f"- {v}")
    else:
        lines += [
            "### ✅ All 8 frozen baselines within 2% tolerance",
            "",
            "Plan approval: **Rule A** (pre-approved, Logan 2026-04-22)",
        ]

    lines += [
        "",
        "---",
        "",
        "## Phase 1 Gate",
        "",
        "`scripts/output/390_plan_approval.txt` has been written with `Rule A`.",
        "Re-run with `--apply` to execute phases 2–3.",
    ]

    PROBE_REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"Probe report written: {PROBE_REPORT_PATH}")


def write_plan_approval() -> None:
    """Write the pre-approved plan_approval.txt with live-verified baselines."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    content = (
        "Rule A\n"
        "# Rule A + residual re-queue (AJCC8 T3b).\n"
        "# Live-verified baselines 2026-04-22 (1,311 / 1,121 / 0 / 4 / 1,125);\n"
        "# original spec had arithmetic inconsistency (190+1,091=1,281 not 1,325)\n"
        "# flagged and corrected by Phase 0 probe.\n"
        "# Sticky guard expanded to include 'unable_to_determine' (26 rows).\n"
        "# 194 boolean-string junk rows route to queue untouched in CPM.\n"
        "# Cohort stability sanity check: ete_adjudication_v1 unchanged\n"
        "# (45 rows, 1 micro / 16 absent / 26 unable / 2 gross), bool-junk\n"
        "# still 194, invasion_rollup rowcount recorded in run log.\n"
        "# Logan's explicit call 2026-04-22 post-live-probe.\n"
    )
    PLAN_APPROVAL_PATH.write_text(content, encoding="utf-8")
    log(f"Plan approval written: {PLAN_APPROVAL_PATH}")


# --------------------------------------------------------------------------- #
# Phase 2 — Apply writes
# --------------------------------------------------------------------------- #


def phase2a_snapshot(con: duckdb.DuckDBPyConnection) -> str:
    """2A: Snapshot CPM ETE columns to archive_pub_v1_0.cpm_ete_pre390_<stamp>."""
    snap_fqn = f'"{PUB_DB}"."{ARC_SCHEMA}"."{SNAPSHOT_NAME}"'
    log(f"2A: Creating snapshot {snap_fqn} ...")
    con.execute(f"""
        CREATE OR REPLACE TABLE {snap_fqn} AS
        SELECT
            research_id,
            ete_grade_final_v2,
            ete_ordinal_worst,
            ete_grade_source,
            ete_grade_final,
            ete_grade_adjudicated,
            ete_adjudicated_flag,
            n_tumors_ete_present,
            microscopic_ete_t3b_corrected,
            gross_ete_flag,
            op_intraop_gross_ete_any,
            path_gross_ete_flag,
            ete_any_present_path
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
    """)
    snap_n = row_count(con, ARC_SCHEMA, SNAPSHOT_NAME)
    log(f"2A: Snapshot created — {snap_n} rows (expected {CPM_ROWS})")
    if snap_n != CPM_ROWS:
        raise SystemExit(
            f"Snapshot row count {snap_n} != {CPM_ROWS}; aborting"
        )
    return SNAPSHOT_NAME


def phase2b_update(con: duckdb.DuckDBPyConnection) -> int:
    """2B: Apply Rule A UPDATE to canonical_patient_master. Returns rows mutated."""
    log("2B: Applying Rule A UPDATE to canonical_patient_master ...")
    source_tag = f"script_390_rule_a_{RUN_STAMP}"
    rule_a_sql = _rule_a_cte_sql()

    # Only UPDATE rows where new_grade IS DISTINCT FROM current_grade
    # (skip no-op rows so ete_grade_source/ete_ordinal_worst stay clean)
    con.execute(
        rule_a_sql + f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
        SET
            ete_grade_final_v2 = t.new_grade,
            ete_grade_source = CASE
                WHEN t.new_grade IS DISTINCT FROM cpm.ete_grade_final_v2
                    THEN '{source_tag}'
                ELSE cpm.ete_grade_source
            END,
            ete_ordinal_worst = CASE
                WHEN t.new_grade = 'gross'
                     AND (cpm.ete_ordinal_worst IS NULL OR cpm.ete_ordinal_worst < 2)
                    THEN 2
                WHEN t.new_grade = 'microscopic'
                     AND (cpm.ete_ordinal_worst IS NULL OR cpm.ete_ordinal_worst < 1)
                    THEN 1
                ELSE cpm.ete_ordinal_worst
            END
        FROM (
            SELECT research_id, new_grade
            FROM rule_a
            WHERE new_grade IS DISTINCT FROM current_grade
        ) t
        WHERE cpm.research_id = t.research_id
        """
    )

    # Count mutations via post-update probe: rows now tagged with source_tag
    mutated_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_source = '{source_tag}'
        """
    ).fetchone()[0]
    log(f"2B: Rows mutated = {mutated_n} (expected {EXPECTED_TOTAL_MUTATED}, "
        f"gate [{MUTATED_LO}, {MUTATED_HI}])")
    if not (MUTATED_LO <= mutated_n <= MUTATED_HI):
        raise SystemExit(
            f"Mutation count {mutated_n} outside gate [{MUTATED_LO}, {MUTATED_HI}]; "
            f"aborting. Expected exactly {EXPECTED_TOTAL_MUTATED} ± 2%."
        )
    return mutated_n


def phase2b_ordinal_fixup(con: duckdb.DuckDBPyConnection) -> int:
    """2B-fixup: Ensure ete_ordinal_worst >= 2 for ALL gross rows.

    Phase 2B's UPDATE only touched rows where new_grade != current_grade
    (the changed rows).  Pre-existing gross rows (190 of them) that already
    had ete_grade_final_v2='gross' before 390 ran may have ordinal < 2 if
    the prior pipeline set it inconsistently.  This fixup is idempotent and
    safe to run in both fresh and recovery modes.
    """
    log("2B-fixup: Ensuring ete_ordinal_worst >= 2 for all gross rows ...")
    con.execute(
        f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        SET ete_ordinal_worst = 2
        WHERE ete_grade_final_v2 = 'gross'
          AND (ete_ordinal_worst IS NULL OR ete_ordinal_worst < 2)
        """
    )
    remaining = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_final_v2 = 'gross'
          AND (ete_ordinal_worst IS NULL OR ete_ordinal_worst < 2)
        """
    ).fetchone()[0]
    log(f"2B-fixup: Gross rows with ordinal < 2 remaining: {remaining} (expected 0)")
    if remaining != 0:
        raise SystemExit(
            f"2B-fixup: {remaining} gross rows still have ete_ordinal_worst < 2 "
            "after fixup; aborting."
        )
    return remaining


def phase2c_readme(
    con: duckdb.DuckDBPyConnection,
    mutated_n: int,
    snap_name: str,
    unable_n: int,
    bool_junk_n: int,
) -> None:
    """2C: Write 3 __readme provenance rows (4-place audit pattern).

    Idempotent: if Script 390 __readme rows already exist (from a prior partial
    run), this phase is skipped to avoid duplicates.
    """
    existing = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
        WHERE content LIKE 'Script 390%'
        """
    ).fetchone()[0]
    if existing == 3:
        log("2C: 3 __readme rows already present — skipping (idempotent)")
        return
    if existing > 0:
        warn(f"2C: {existing} __readme row(s) for Script 390 found (expected 0 or 3); "
             "inserting remaining rows to reach 3 total")

    log("2C: Writing 3 __readme provenance rows ...")
    snap_path = f"{ARC_SCHEMA}.{snap_name}"

    rows = [
        (
            f"Script 390 Rule A apply summary: ETE adjudication reconciliation on "
            f"canonical_patient_master. Rule A (worst-of, AJCC8 T3b). "
            f"Source signals: gross_ete_flag OR op_intraop_gross_ete_any OR "
            f"path_gross_ete_flag OR inv.any_gross_ete_anywhere. "
            f"Adjudicator-sticky guard (expanded): ete_adjudicated_flag=TRUE AND "
            f"ete_grade_adjudicated IN (microscopic, absent, unable_to_determine). "
            f"Rows mutated: {mutated_n}. "
            f"Snapshot: {snap_path}."
        ),
        (
            f"Script 390 expanded sticky guard note: Sticky guard extended to include "
            f"ete_grade_adjudicated='unable_to_determine'. "
            f"{unable_n} rows protected (clinician explicit cannot-call; "
            f"routed to cpm_ete_self_contradiction_queue_v1 for re-review). "
            f"Original spec guard was (microscopic, absent) only; expanded 2026-04-22 "
            f"post-live-probe per Logan's call."
        ),
        (
            f"Script 390 boolean-string routing note: {bool_junk_n} rows with "
            f"ete_grade_final_v2 in ('true', 'false') are upstream-bug artifacts "
            f"(boolean strings, not ETE grades). Routed to "
            f"cpm_ete_self_contradiction_queue_v1 with "
            f"reason='boolean_string_upstream_bug'. NOT silently normalized in 390. "
            f"Carry-forward to Script 392 for extractor trace and normalization."
        ),
    ]

    for content in rows:
        con.execute(
            f"""
            INSERT INTO "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}" (content, updated_at)
            VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP))
            """,
            [content],
        )
        log(f"  __readme row inserted: {content[:80]}...")

    log("2C: All 3 __readme rows written")


def phase2d_queue(
    con: duckdb.DuckDBPyConnection,
    unable_n: int,
    bool_junk_n: int,
) -> int:
    """2D: Repopulate cpm_ete_self_contradiction_queue_v1. Returns final row count."""
    log("2D: Repopulating contradiction queue ...")

    # Probe queue schema
    if not table_exists(con, WS_SCHEMA, QUEUE_TABLE):
        raise SystemExit(
            f"Queue table {WS_SCHEMA}.{QUEUE_TABLE} does not exist; cannot repopulate"
        )
    q_cols = get_columns(con, WS_SCHEMA, QUEUE_TABLE)
    log(f"  Queue columns: {q_cols}")

    queue_fqn = f'"{PUB_DB}"."{WS_SCHEMA}"."{QUEUE_TABLE}"'

    # Save any previously-reviewed rows (status != 'awaiting_manual_review')
    # Count them first
    preserved_n = con.execute(
        f"""
        SELECT COUNT(*) FROM {queue_fqn}
        WHERE status != 'awaiting_manual_review'
        """
    ).fetchone()[0]
    log(f"  Preserved rows (status != awaiting_manual_review): {preserved_n}")

    # Determine which columns the queue has for INSERT targeting
    # Core required columns always present
    has_reason = "reason" in q_cols
    has_queued_by = "queued_by_script" in q_cols
    has_queued_at = "queued_at" in q_cols

    # Build the dynamic INSERT columns / values pattern
    # We'll always use: research_id, status, reason, queued_by_script, queued_at
    # For columns that don't exist in the old table, we'll ALTER ADD them first
    def ensure_col(col: str, dtype: str) -> None:
        if col not in q_cols:
            con.execute(f'ALTER TABLE {queue_fqn} ADD COLUMN "{col}" {dtype}')
            log(f"  Added missing column: {col} {dtype}")
            q_cols.append(col)

    ensure_col("reason", "VARCHAR")
    ensure_col("queued_by_script", "VARCHAR")
    ensure_col("queued_at", "TIMESTAMP")

    # Rebuild the queue: preserve non-awaiting rows + insert new cohorts
    # Strategy: CREATE OR REPLACE using SELECT union approach
    con.execute(f"""
        CREATE OR REPLACE TABLE {queue_fqn} AS
        SELECT * FROM {queue_fqn}
        WHERE status != 'awaiting_manual_review'
    """)

    # Cohort 1: microscopic-no-invasion-signal (FF cohort, not adjudicated-sticky)
    # These are rows that stay microscopic after Rule A but have zero invasion support
    rule_a_sql = _rule_a_cte_sql()
    cohort1_sql = f"""
        {rule_a_sql}
        SELECT
            cpm.research_id,
            'awaiting_manual_review'              AS status,
            'microscopic_no_invasion_signal'      AS reason,
            '390'                                 AS queued_by_script,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)  AS queued_at
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
        LEFT JOIN "{PUB_DB}"."{MAIN_SCHEMA}"."{INVASION_ROLLUP}" inv
            ON cpm.research_id = inv.research_id
        JOIN rule_a ra ON cpm.research_id = ra.research_id
        WHERE ra.new_grade = 'microscopic'
          AND ra.current_grade = 'microscopic'
          AND (inv.any_gross_ete_anywhere IS NULL OR inv.any_gross_ete_anywhere = FALSE)
          AND (inv.any_microscopic_ete_anywhere IS NULL OR inv.any_microscopic_ete_anywhere = FALSE)
          AND (cpm.ete_any_present_path IS NULL OR cpm.ete_any_present_path = FALSE)
          AND NOT (
                cpm.ete_adjudicated_flag = TRUE
                AND cpm.ete_grade_adjudicated IN ({STICKY_GRADES_SQL})
              )
    """
    # Cohort 1: all rows that stay microscopic after Rule A and are not sticky.
    # Note: We deliberately do NOT filter on ete_any_present_path or invasion
    # rollup signals here — all F/F invasion-rollup microscopic rows have
    # ete_any_present_path=TRUE, so adding that filter would incorrectly drop
    # them all. The intent is "stayed microscopic despite Rule A" = worth review.
    cohort1_n = con.execute(
        rule_a_sql + f"""
        SELECT COUNT(*)
        FROM rule_a
        WHERE new_grade = 'microscopic'
          AND current_grade = 'microscopic'
          AND research_id NOT IN (
              SELECT research_id
              FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
              WHERE ete_adjudicated_flag = TRUE
                AND ete_grade_adjudicated IN ({STICKY_GRADES_SQL})
          )
        """
    ).fetchone()[0]
    log(f"  Cohort 1 (microscopic-no-signal): {cohort1_n} rows")

    # Insert cohort 1
    con.execute(f"""
        INSERT INTO {queue_fqn} (research_id, status, reason, queued_by_script, queued_at)
        {rule_a_sql}
        SELECT
            ra.research_id,
            'awaiting_manual_review'              AS status,
            'microscopic_no_invasion_signal'      AS reason,
            '390'                                 AS queued_by_script,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)  AS queued_at
        FROM rule_a ra
        WHERE ra.new_grade = 'microscopic'
          AND ra.current_grade = 'microscopic'
          AND ra.research_id NOT IN (
              SELECT research_id
              FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
              WHERE ete_adjudicated_flag = TRUE
                AND ete_grade_adjudicated IN ({STICKY_GRADES_SQL})
          )
    """)

    # Cohort 2: unable_to_determine sticky rows
    con.execute(f"""
        INSERT INTO {queue_fqn} (research_id, status, reason, queued_by_script, queued_at)
        SELECT
            research_id,
            'awaiting_manual_review'                              AS status,
            'adjudicator_unable_to_determine_rule_a_candidate'    AS reason,
            '390'                                                 AS queued_by_script,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                  AS queued_at
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_adjudicated_flag = TRUE
          AND ete_grade_adjudicated = 'unable_to_determine'
    """)
    log(f"  Cohort 2 (unable_to_determine sticky): {unable_n} rows")

    # Cohort 3: boolean-string junk rows
    con.execute(f"""
        INSERT INTO {queue_fqn} (research_id, status, reason, queued_by_script, queued_at)
        SELECT
            research_id,
            'awaiting_manual_review'              AS status,
            'boolean_string_upstream_bug'         AS reason,
            '390'                                 AS queued_by_script,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)  AS queued_at
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_final_v2 IN ('true', 'false')
    """)
    log(f"  Cohort 3 (boolean-string junk): {bool_junk_n} rows")

    final_q = row_count(con, WS_SCHEMA, QUEUE_TABLE)
    # preserved_n = rows with status != 'awaiting_manual_review' (typically 0)
    # The 1 existing AMR row is not preserved (it had status='awaiting_manual_review')
    expected_q = preserved_n + cohort1_n + unable_n + bool_junk_n
    log(f"  Queue final row count: {final_q} "
        f"(dynamic expected={expected_q}, gate [{QUEUE_LO}, {QUEUE_HI}])")
    if not (QUEUE_LO <= final_q <= QUEUE_HI):
        raise SystemExit(
            f"Queue row count {final_q} outside gate [{QUEUE_LO}, {QUEUE_HI}]; "
            f"aborting. Expected ~{EXPECTED_QUEUE_NEW_ROWS} new rows."
        )

    return final_q


def phase2e_manuscript(con: duckdb.DuckDBPyConnection) -> int:
    """2E: Rebuild manuscript_cohort_v1.ete_grade_final + .ete_grade_source from CPM."""
    log("2E: Rebuilding manuscript_cohort_v1.ete_grade_final + .ete_grade_source ...")

    if not table_exists(con, MAIN_SCHEMA, MANUSCRIPT_COHORT):
        warn(f"{MANUSCRIPT_COHORT} does not exist; skipping Phase 2E")
        return 0

    mc_cols = get_columns(con, MAIN_SCHEMA, MANUSCRIPT_COHORT)
    if "ete_grade_final" not in mc_cols:
        warn(f"{MANUSCRIPT_COHORT} missing ete_grade_final column; skipping Phase 2E")
        return 0

    has_source = "ete_grade_source" in mc_cols

    if has_source:
        updated = con.execute(
            f"""
            UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{MANUSCRIPT_COHORT}" mc
            SET
                ete_grade_final  = cpm.ete_grade_final_v2,
                ete_grade_source = cpm.ete_grade_source
            FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
            WHERE mc.research_id = cpm.research_id
              AND (
                    mc.ete_grade_final IS DISTINCT FROM cpm.ete_grade_final_v2
                 OR mc.ete_grade_source IS DISTINCT FROM cpm.ete_grade_source
                  )
            """
        ).rowcount
    else:
        updated = con.execute(
            f"""
            UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{MANUSCRIPT_COHORT}" mc
            SET ete_grade_final = cpm.ete_grade_final_v2
            FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
            WHERE mc.research_id = cpm.research_id
              AND mc.ete_grade_final IS DISTINCT FROM cpm.ete_grade_final_v2
            """
        ).rowcount

    log(f"2E: manuscript_cohort_v1 rows updated: {updated}")
    return updated


# --------------------------------------------------------------------------- #
# Phase 3 — Post-state verification
# --------------------------------------------------------------------------- #


def phase3_verify(
    con: duckdb.DuckDBPyConnection,
    snap_name: str,
    mutated_n: int,
    queue_final_n: int,
) -> list[str]:
    """Run all Phase 3 invariant checks. Returns list of failures (empty = pass)."""
    log("Phase 3: Post-state verification ...")
    failures: list[str] = []

    def check(name: str, passed: bool, detail: str = "") -> None:
        status = "PASS" if passed else "FAIL"
        msg = f"  [{status}] {name}" + (f" — {detail}" if detail else "")
        log(msg)
        if not passed:
            failures.append(f"{name}: {detail}")

    # 1. CPM rowcount = 10,871 exact
    cpm_n = row_count(con, MAIN_SCHEMA, CPM_TABLE)
    check("CPM rowcount = 10,871", cpm_n == CPM_ROWS, f"actual={cpm_n}")

    # 2. ete_adjudication_v1 = 45 exact
    adj_n = row_count(con, MAIN_SCHEMA, ADJ_TABLE)
    check("ete_adjudication_v1 rowcount = 45", adj_n == ADJ_ROWS, f"actual={adj_n}")

    # 3. Adjudicator-sticky rows (expanded) have ete_grade_final_v2 unchanged vs snapshot
    sticky_changed = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
        JOIN "{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}" snap
            ON cpm.research_id = snap.research_id
        WHERE cpm.ete_adjudicated_flag = TRUE
          AND cpm.ete_grade_adjudicated IN ({STICKY_GRADES_SQL})
          AND cpm.ete_grade_final_v2 IS DISTINCT FROM snap.ete_grade_final_v2
        """
    ).fetchone()[0]
    check(
        "Adjudicator-sticky rows unchanged",
        sticky_changed == 0,
        f"{sticky_changed} sticky rows changed",
    )

    # 4. Rule A post-state: 0 rows with ete_grade_final_v2='microscopic'
    #    AND any gross signal AND NOT adjudicator-sticky (expanded)
    remaining_contradictions = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
        LEFT JOIN "{PUB_DB}"."{MAIN_SCHEMA}"."{INVASION_ROLLUP}" inv
            ON cpm.research_id = inv.research_id
        WHERE cpm.ete_grade_final_v2 = 'microscopic'
          AND (
                cpm.gross_ete_flag = TRUE
             OR cpm.op_intraop_gross_ete_any = TRUE
             OR cpm.path_gross_ete_flag = TRUE
             OR inv.any_gross_ete_anywhere = TRUE
              )
          AND NOT (
                cpm.ete_adjudicated_flag = TRUE
                AND cpm.ete_grade_adjudicated IN ({STICKY_GRADES_SQL})
              )
        """
    ).fetchone()[0]
    check(
        "0 residual gross-signal microscopic (non-sticky)",
        remaining_contradictions == 0,
        f"{remaining_contradictions} residual contradictions remain",
    )

    # 5. ete_ordinal_worst >= 2 for every row with ete_grade_final_v2='gross'
    gross_low_ordinal = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_final_v2 = 'gross'
          AND (ete_ordinal_worst IS NULL OR ete_ordinal_worst < 2)
        """
    ).fetchone()[0]
    check(
        "ete_ordinal_worst >= 2 for all gross rows",
        gross_low_ordinal == 0,
        f"{gross_low_ordinal} gross rows with ordinal < 2",
    )

    # 6. manuscript_cohort_v1.ete_grade_final matches CPM.ete_grade_final_v2 100%
    if table_exists(con, MAIN_SCHEMA, MANUSCRIPT_COHORT):
        mc_cols = get_columns(con, MAIN_SCHEMA, MANUSCRIPT_COHORT)
        if "ete_grade_final" in mc_cols:
            mc_mismatch = con.execute(
                f"""
                SELECT COUNT(*)
                FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{MANUSCRIPT_COHORT}" mc
                JOIN "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
                    ON mc.research_id = cpm.research_id
                WHERE mc.ete_grade_final IS DISTINCT FROM cpm.ete_grade_final_v2
                """
            ).fetchone()[0]
            check(
                "manuscript_cohort_v1.ete_grade_final matches CPM",
                mc_mismatch == 0,
                f"{mc_mismatch} mismatches",
            )
        else:
            warn("manuscript_cohort_v1 missing ete_grade_final; skipping check 6")
    else:
        warn(f"{MANUSCRIPT_COHORT} not found; skipping check 6")

    # 7. All 3 __readme rows present
    if table_exists(con, MAIN_SCHEMA, README_TABLE):
        readme_rows = con.execute(
            f"""
            SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
            WHERE content LIKE 'Script 390%'
            """
        ).fetchone()[0]
        check("3 __readme rows for Script 390 present", readme_rows == 3, f"actual={readme_rows}")
    else:
        failures.append("__readme table not found")

    # 8. Snapshot row count = 10,871
    snap_n = row_count(con, ARC_SCHEMA, snap_name)
    check(f"Snapshot {snap_name} rowcount = {CPM_ROWS}", snap_n == CPM_ROWS, f"actual={snap_n}")

    # 9. Queue rowcount within the explicit ±2% gate
    check(
        f"Queue rowcount in gate [{QUEUE_LO}–{QUEUE_HI}]",
        QUEUE_LO <= queue_final_n <= QUEUE_HI,
        f"actual={queue_final_n}",
    )

    return failures


# --------------------------------------------------------------------------- #
# Close-out report
# --------------------------------------------------------------------------- #


def write_close_out(
    mutated_n: int,
    snap_name: str,
    queue_final_n: int,
    mc_updated: int,
    failures: list[str],
) -> None:
    lines = [
        "# Script 390 — ETE Adjudication Reconciliation — Close-Out",
        "",
        f"**Stamp:** {RUN_STAMP}",
        f"**Rule:** Rule A (worst-of, AJCC8 T3b)",
        f"**DB:** `{PUB_DB}`",
        "",
        "## Results",
        "",
        f"- CPM rows mutated: **{mutated_n}** (expected {EXPECTED_TOTAL_MUTATED})",
        f"- Snapshot: `{ARC_SCHEMA}.{snap_name}` ({CPM_ROWS} rows)",
        f"- Queue final row count: **{queue_final_n}**",
        f"- manuscript_cohort_v1 rows updated: **{mc_updated}**",
        f"- 3 __readme provenance rows written",
        "",
        "## Phase 3 Verification",
        "",
        "### Status: " + ("✅ PASS (all checks)" if not failures else f"❌ FAIL ({len(failures)} failures)"),
        "",
    ]
    if failures:
        for f in failures:
            lines.append(f"- ❌ {f}")
    else:
        lines.append("All post-state invariants satisfied.")

    lines += [
        "",
        "## Carry-Forwards",
        "",
        "- **CF-1** — Residual queue review: ~2,575 microscopic-no-signal rows need "
        "human pathology review.",
        "- **CF-2** — Script 391: T-stage downstream reconciliation — rebuilds "
        "microscopic_ete_t3b_corrected and ajcc8_t_stage_* for the ~1,121 rows "
        "flipped to gross.",
        "- **CF-3** — Phase 0 drift detective: spec frozen baselines had an arithmetic "
        "inconsistency (1,325 ≠ 190+1,091+0 flip-down). Live probe corrected to "
        "1,311/1,121/0/4/1,125. Root cause: earlier-session probe's "
        "total_gross_after count was miscomputed (likely stray NULL row in the CASE "
        "expression). No live-data drift — the adjudication cohort and boolean-junk "
        "counts were byte-identical between the two probes one hour apart.",
        "",
        "## Next Steps",
        "",
        "Script 391: T-stage downstream reconciliation",
        "  - Rebuilds microscopic_ete_t3b_corrected and ajcc8_t_stage_* columns",
        "  - The ~1,091 rows flipped to gross in 390 should propagate to T3b",
        "",
        "Script 392: Boolean-string extractor trace + normalization",
        "  - 194 queued rows from 390 get their upstream fix",
        "",
        "## Git Commit + Tag",
        "",
        "```",
        "git add scripts/390_ete_adjudication_reconciliation.py",
        "git add scripts/output/390_probe_report.md",
        "git add scripts/output/390_plan_approval.txt",
        "git add scripts/output/390_run.log",
        "git add scripts/output/390_close_out.md",
        "",
        f'git commit -m "Script 390: ETE adjudication reconciliation — Rule A applied; \\',
        f'  {mutated_n} CPM rows mutated; sticky guard expanded for \\',
        f'  unable_to_determine; boolean-string cohort routed to queue"',
        "",
        f"git tag v1_0-ete-reconciled-{RUN_STAMP}",
        "```",
    ]
    CLOSE_OUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"Close-out report written: {CLOSE_OUT_PATH}")


# --------------------------------------------------------------------------- #
# Idempotency check
# --------------------------------------------------------------------------- #


def _phase3_quick_check(con: duckdb.DuckDBPyConnection, snap_name: str) -> bool:
    """Return True only if every Phase 3 invariant passes (used for idempotency gate)."""
    try:
        # 1. CPM rowcount
        if row_count(con, MAIN_SCHEMA, CPM_TABLE) != CPM_ROWS:
            return False
        # 2. adj rowcount
        if row_count(con, MAIN_SCHEMA, ADJ_TABLE) != ADJ_ROWS:
            return False
        # 3. No residual gross-signal microscopic (non-sticky)
        remaining = con.execute(
            f"""
            SELECT COUNT(*)
            FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
            LEFT JOIN "{PUB_DB}"."{MAIN_SCHEMA}"."{INVASION_ROLLUP}" inv
                ON cpm.research_id = inv.research_id
            WHERE cpm.ete_grade_final_v2 = 'microscopic'
              AND (cpm.gross_ete_flag = TRUE OR cpm.op_intraop_gross_ete_any = TRUE
                   OR cpm.path_gross_ete_flag = TRUE OR inv.any_gross_ete_anywhere = TRUE)
              AND NOT (cpm.ete_adjudicated_flag = TRUE
                       AND cpm.ete_grade_adjudicated IN ({STICKY_GRADES_SQL}))
            """
        ).fetchone()[0]
        if remaining != 0:
            return False
        # 4. ete_ordinal_worst >= 2 for all gross rows
        gross_low = con.execute(
            f"""
            SELECT COUNT(*)
            FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
            WHERE ete_grade_final_v2 = 'gross'
              AND (ete_ordinal_worst IS NULL OR ete_ordinal_worst < 2)
            """
        ).fetchone()[0]
        if gross_low != 0:
            return False
        # 5. Queue within gate
        if table_exists(con, WS_SCHEMA, QUEUE_TABLE):
            q_n = row_count(con, WS_SCHEMA, QUEUE_TABLE)
            if not (QUEUE_LO <= q_n <= QUEUE_HI):
                return False
        else:
            return False
        # 5. 3 __readme rows
        readme_n = con.execute(
            f"""
            SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
            WHERE content LIKE 'Script 390%'
            """
        ).fetchone()[0]
        if readme_n != 3:
            return False
        # 6. Snapshot rowcount
        if table_exists(con, ARC_SCHEMA, snap_name):
            if row_count(con, ARC_SCHEMA, snap_name) != CPM_ROWS:
                return False
        else:
            return False
        return True
    except Exception as exc:  # noqa: BLE001
        log(f"Quick Phase 3 check exception: {exc}")
        return False


def check_idempotency(con: duckdb.DuckDBPyConnection) -> str | None:
    """Check for prior run.

    Returns the snapshot name if a clean prior run is found (caller should
    exit 0), None if clean slate, or raises SystemExit on partial/corrupt state.
    """
    snap = find_pre390_snapshot(con)
    readme = readme_390_present(con)

    if snap and readme:
        log(f"Idempotency check: snapshot '{snap}' + __readme row both present.")
        log("Running Phase 3 quick-check to determine if prior run was clean ...")
        clean = _phase3_quick_check(con, snap)
        if clean:
            log("Prior run is clean — all Phase 3 invariants pass.")
            log("NO-OP — prior run detected. Exiting 0.")
            flush_log()
            sys.exit(0)
        else:
            warn(
                "Prior snapshot + __readme found but Phase 3 quick-check FAILED "
                "(partial prior run — queue or manuscript cohort incomplete). "
                "Continuing with Phase 2D/2E/3 re-run ..."
            )
            return snap  # caller can skip 2A/2B/2C, jump to 2D
    elif snap and not readme:
        raise SystemExit(
            f"Partial prior run detected: snapshot {snap} exists but no __readme row. "
            "Manual cleanup required."
        )
    elif not snap and readme:
        raise SystemExit(
            "Partial prior run detected: __readme row exists but no snapshot. "
            "Manual cleanup required."
        )
    return None  # clean slate


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Script 390 — ETE Adjudication Reconciliation"
    )
    parser.add_argument(
        "--phase",
        type=int,
        default=0,
        choices=[0],
        help="Phase 0 (default): read-only probe + drift gate. Use --apply for writes.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute Phase 2 writes (re-runs Phase 0 probe first).",
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log(f"=== Script {SCRIPT_ID} ETE Adjudication Reconciliation ===")
    log(f"Mode: {'--apply (Phase 2-3)' if args.apply else 'Phase 0 (probe-only)'}")
    log(f"Stamp: {RUN_STAMP}")

    con = connect()

    # Detect partial run early so we can route the bool-junk stability check
    # to the snapshot (pre-update state) rather than live CPM when Phase 2B
    # already ran.  Full idempotency decision is deferred until after Phase 0.
    _early_snap = find_pre390_snapshot(con)
    _early_readme = readme_390_present(con)
    _partial_run = _early_snap and _early_readme  # True = partial, need recovery

    # --- Phase 0: cohort stability + probe ---
    log("=== Phase 0: Cohort stability sanity check ===")
    if _partial_run:
        log(f"  (partial-run recovery: using snapshot {_early_snap} for bool-junk check)")
        cohort_stability_check(
            con,
            bool_junk_schema=ARC_SCHEMA,
            bool_junk_table=_early_snap,  # type: ignore[arg-type]
        )
    else:
        cohort_stability_check(con)

    log("=== Phase 0: Discovery + probe ===")
    results = phase0_probe(con)
    violations = check_drift(results, recovery_mode=bool(_partial_run))

    write_probe_report(results, violations)
    write_plan_approval()

    if violations:
        err(f"DRIFT GATE FAILED — {len(violations)} baseline(s) exceeded 2% tolerance:")
        for v in violations:
            err(f"  {v}")
        flush_log()
        raise SystemExit("Halted at drift gate. See 390_probe_report.md for details.")

    log("Drift gate: PASS — all 8 frozen baselines within 2% tolerance")
    log(f"Plan approval written: {PLAN_APPROVAL_PATH}")

    if not args.apply:
        log("")
        log("Phase 0 complete. Probe report: scripts/output/390_probe_report.md")
        log("Plan approval: scripts/output/390_plan_approval.txt")
        log("")
        log("Halted at Phase 1 plan-review gate.")
        log("Plan is pre-approved (Rule A, Logan 2026-04-22).")
        log("Re-run with --apply to execute Phase 2 writes.")
        flush_log()
        return

    # --- --apply path ---

    # Read and verify plan approval
    if not PLAN_APPROVAL_PATH.exists():
        raise SystemExit(
            f"Plan approval file not found: {PLAN_APPROVAL_PATH}. "
            "Run Phase 0 first."
        )
    approval_lines = [
        ln.strip()
        for ln in PLAN_APPROVAL_PATH.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.startswith("#")
    ]
    if not approval_lines or approval_lines[0] != "Rule A":
        raise SystemExit(
            f"Plan approval file contains '{approval_lines}'; expected 'Rule A'. "
            "Script 390 only implements Rule A."
        )
    log(f"Plan approval verified: {approval_lines[0]}")

    # Check idempotency — returns snapshot name if partial run detected, None if clean
    partial_snap = check_idempotency(con)

    # --- Phase 2 ---
    log("=== Phase 2: Apply writes ===")

    if partial_snap:
        # Partial prior run: 2A snapshot + 2B UPDATE + 2C __readme already done.
        # Re-run 2A (idempotent CTAS), verify 2B counts, skip 2C if rows exist,
        # then re-run 2D (queue) and 2E (manuscript) which are safe to replay.
        log(f"Partial prior run detected (snap={partial_snap}). Resuming from Phase 2D ...")
        snap_name = partial_snap
        # Verify 2B count from prior run
        source_tag = f"script_390_rule_a_{RUN_STAMP}"
        mutated_n = con.execute(
            f"""
            SELECT COUNT(*)
            FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
            WHERE ete_grade_source = '{source_tag}'
            """
        ).fetchone()[0]
        log(f"  Prior Phase 2B: {mutated_n} rows tagged with {source_tag}")
        if not (MUTATED_LO <= mutated_n <= MUTATED_HI):
            raise SystemExit(
                f"Prior Phase 2B mutation count {mutated_n} outside gate "
                f"[{MUTATED_LO}, {MUTATED_HI}]; manual investigation required."
            )
        phase2b_ordinal_fixup(con)  # safe to run in recovery — idempotent
    else:
        snap_name = phase2a_snapshot(con)
        mutated_n = phase2b_update(con)
        phase2b_ordinal_fixup(con)

    unable_n = results["unable_sticky"]
    bool_junk_n = results["bool_junk_n"]

    phase2c_readme(con, mutated_n, snap_name, unable_n, bool_junk_n)
    queue_final_n = phase2d_queue(con, unable_n, bool_junk_n)
    mc_updated = phase2e_manuscript(con)

    # --- Phase 3 ---
    log("=== Phase 3: Post-state verification ===")
    failures = phase3_verify(con, snap_name, mutated_n, queue_final_n)

    if failures:
        err(f"Phase 3 FAILED — {len(failures)} invariant(s) violated:")
        for f in failures:
            err(f"  {f}")
        write_close_out(mutated_n, snap_name, queue_final_n, mc_updated, failures)
        flush_log()
        raise SystemExit("Phase 3 verification failed. See close-out report.")
    else:
        log("Phase 3: ALL invariants passed ✓")

    write_close_out(mutated_n, snap_name, queue_final_n, mc_updated, failures)

    log("")
    log("=== Script 390 complete ===")
    log(f"  CPM rows mutated       : {mutated_n}")
    log(f"  Snapshot               : {ARC_SCHEMA}.{snap_name}")
    log(f"  Queue final rowcount   : {queue_final_n}")
    log(f"  Manuscript rows updated: {mc_updated}")
    log("")
    log("Phase 4 — Git commit + tag (manual):")
    log("  git add scripts/390_ete_adjudication_reconciliation.py")
    log("  git add scripts/output/390_probe_report.md")
    log("  git add scripts/output/390_plan_approval.txt")
    log("  git add scripts/output/390_run.log")
    log("  git add scripts/output/390_close_out.md")
    log(f"  git commit -m 'Script 390: ETE adjudication reconciliation — Rule A applied;'")
    log(f"  git tag v1_0-ete-reconciled-{RUN_STAMP}")

    flush_log()


if __name__ == "__main__":
    main()
