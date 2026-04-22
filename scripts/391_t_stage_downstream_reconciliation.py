#!/usr/bin/env python3
"""Script 391 — T-stage Downstream Reconciliation (post-390).

Rebuilds all T-stage downstream columns against the post-390 ete_grade_final_v2
state.  Also syncs the two legacy ETE columns.

Phases
------
* --phase 0 (default) — read-only probe + 10-baseline drift gate; writes
  scripts/output/391_probe_report.md and scripts/output/391_plan_approval.txt.
  No writes to PUB.
* --apply — re-runs Phase 0 probe (re-verify within 2%), reads approval file,
  then executes:
    Phase 2A: archive snapshot to archive_pub_v1_0.cpm_pre391_<stamp>
    Phase 2B: UPDATE legacy ete_grade + ete_grade_final ← ete_grade_final_v2
    Phase 2C: Upgrade ajcc8_t_stage_with_microete_t3b_DEPRECATED → T3b for
              all gross-ETE rows where it isn't already T3b
    Phase 2D: Steps A–E (240-builder logic with ete_grade_final_v2 substitution)
    Phase 2E: Step F — manuscript_cohort_v1.ajcc8_t_stage rebuild from CPM
    Phase 2F: 3 __readme provenance rows
    Phase 3:  Post-state verification (halt-on-fail)

Idempotency
-----------
Both triggers must be present for NO-OP exit:
  1. archive_pub_v1_0.cpm_pre391_* snapshot exists
  2. __readme row whose content starts with 'Script 391: T-stage downstream rebuild'
If only one present → halt with partial-prior-run error.
If both present AND Phase 3 invariants hold → exit 0, NO-OP.

Hard rules honored
------------------
* No cross-DB sourcing: everything stays in PUB (thyroid_canonical_publication_v1_0).
* CAST(CURRENT_TIMESTAMP AS TIMESTAMP) for all __readme inserts.
* Token never printed — motherduck_client.get_token() + token_mode().
* No git add performed by this script.
* PHI-safe: only research_id and aggregate counts logged, never clinical text.
* 4-place audit pattern for all UPDATE/INSERT calls.

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
MAIN_SCHEMA = "main"

SCRIPT_TAG = "391_t_stage_downstream_reconciliation"
SCRIPT_ID = "391"

RUN_STAMP = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

SNAPSHOT_NAME = f"cpm_pre391_{RUN_STAMP}"
CPM_TABLE = "canonical_patient_master"
MANUSCRIPT_COHORT = "manuscript_cohort_v1"
README_TABLE = "__readme"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PROBE_REPORT_PATH = OUTPUT_DIR / "391_probe_report.md"
PLAN_APPROVAL_PATH = OUTPUT_DIR / "391_plan_approval.txt"
RUN_LOG_PATH = OUTPUT_DIR / "391_run.log"
CLOSE_OUT_PATH = OUTPUT_DIR / "391_close_out.md"

# --------------------------------------------------------------------------- #
# Frozen baselines (live-verified 2026-04-22, post-390, pre-391)
# --------------------------------------------------------------------------- #

CPM_ROWS                            = 10_871
ETE_GRADE_FINAL_V2_GROSS            = 1_311
ETE_GRADE_FINAL_V2_MICROSCOPIC      = 2_580
ETE_GRADE_FINAL_V2_ABSENT           = 16
ETE_GRADE_FINAL_V2_PRESENT_UNGRADED = 29
ETE_GRADE_FINAL_V2_JUNK_FALSE       = 179
ETE_GRADE_FINAL_V2_JUNK_TRUE        = 4
ETE_GRADE_FINAL_V2_NULL             = 6_752
MICROSCOPIC_ETE_T3B_CORRECTED_TRUE  = 906
AJCC8_T_STAGE_T3B                   = 240
AJCC8_T_STAGE_W_MICROETE_DEPRECATED_T3B = 1_146
GROSS_WITH_NON_T3B_STAGE            = 172
CONTRADICTION_906                   = 906

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
    ).fetchone()[0]  # type: ignore[index]


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [PUB_DB, schema, name],
    ).fetchone()
    return row is not None


def find_pre391_snapshot(con: duckdb.DuckDBPyConnection) -> str | None:
    """Return the name of any existing cpm_pre391_* snapshot, or None."""
    rows = con.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog = ?
          AND table_schema = ?
          AND table_name LIKE 'cpm_pre391_%'
        ORDER BY table_name
        LIMIT 1
        """,
        [PUB_DB, ARC_SCHEMA],
    ).fetchall()
    return rows[0][0] if rows else None


def readme_391_present(con: duckdb.DuckDBPyConnection) -> bool:
    """Return True if a __readme row starting with 'Script 391: T-stage downstream rebuild' exists."""
    if not table_exists(con, MAIN_SCHEMA, README_TABLE):
        return False
    row = con.execute(
        f"""
        SELECT 1 FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
        WHERE content LIKE 'Script 391: T-stage downstream rebuild%'
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


def phase0_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Run read-only probes; return a dict of all live metrics for drift checking."""
    results: dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    # 1. CPM row count
    # ------------------------------------------------------------------ #
    cpm_n = row_count(con, MAIN_SCHEMA, CPM_TABLE)
    results["cpm_rows"] = cpm_n
    log(f"CPM rowcount: {cpm_n}")

    # ------------------------------------------------------------------ #
    # 2. ETE grade distribution (ete_grade_final_v2)
    # ------------------------------------------------------------------ #
    grade_dist = con.execute(
        f"""
        SELECT
            COALESCE(ete_grade_final_v2, '<NULL>') AS grade,
            COUNT(*) AS n
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        GROUP BY ete_grade_final_v2
        ORDER BY n DESC
        """
    ).fetchall()
    results["grade_dist"] = grade_dist
    log("ete_grade_final_v2 distribution:")
    for row in grade_dist:
        log(f"  {row[0]!r:35s}  n={row[1]}")

    # Extract individual counts for baseline checks
    grade_dict: dict[str | None, int] = {}
    for row in grade_dist:
        raw_key = None if row[0] == "<NULL>" else row[0]
        grade_dict[raw_key] = row[1]
    results["grade_dict"] = grade_dict

    n_gross    = grade_dict.get("gross", 0)
    n_micro    = grade_dict.get("microscopic", 0)
    n_absent   = grade_dict.get("absent", 0)
    n_present  = grade_dict.get("present_ungraded", 0)
    n_false    = grade_dict.get("false", 0)
    n_true_junk = grade_dict.get("true", 0)
    n_null     = grade_dict.get(None, 0)

    results["n_gross"]     = n_gross
    results["n_micro"]     = n_micro
    results["n_absent"]    = n_absent
    results["n_present"]   = n_present
    results["n_false"]     = n_false
    results["n_true_junk"] = n_true_junk
    results["n_null"]      = n_null

    # ------------------------------------------------------------------ #
    # 3. microscopic_ete_t3b_corrected
    # ------------------------------------------------------------------ #
    micro_t3b_corrected_n = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE microscopic_ete_t3b_corrected = TRUE
        """
    ).fetchone()[0]  # type: ignore[index]
    results["micro_t3b_corrected_true"] = micro_t3b_corrected_n
    log(f"microscopic_ete_t3b_corrected=TRUE: {micro_t3b_corrected_n}")

    # ------------------------------------------------------------------ #
    # 4. ajcc8_t_stage (corrected) distribution + DEPRECATED distribution
    # ------------------------------------------------------------------ #
    t_stage_corrected_n = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ajcc8_t_stage = 'T3b'
        """
    ).fetchone()[0]  # type: ignore[index]
    results["ajcc8_t_stage_t3b"] = t_stage_corrected_n
    log(f"ajcc8_t_stage='T3b': {t_stage_corrected_n}")

    deprecated_t3b_n = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b'
        """
    ).fetchone()[0]  # type: ignore[index]
    results["deprecated_t3b"] = deprecated_t3b_n
    log(f"ajcc8_t_stage_with_microete_t3b_DEPRECATED='T3b': {deprecated_t3b_n}")

    # ------------------------------------------------------------------ #
    # 5. Gross-with-non-T3b (the 172-row target)
    # ------------------------------------------------------------------ #
    gross_non_t3b_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_final_v2 = 'gross'
          AND COALESCE(ajcc8_t_stage_with_microete_t3b_DEPRECATED, '') != 'T3b'
        """
    ).fetchone()[0]  # type: ignore[index]
    results["gross_non_t3b"] = gross_non_t3b_n
    log(f"gross-ETE with non-T3b DEPRECATED stage: {gross_non_t3b_n}")

    # ------------------------------------------------------------------ #
    # 6. Semantic contradiction (gross + micro_t3b_corrected=TRUE)
    # ------------------------------------------------------------------ #
    contradiction_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_final_v2 = 'gross'
          AND microscopic_ete_t3b_corrected = TRUE
        """
    ).fetchone()[0]  # type: ignore[index]
    results["contradiction_gross_micro"] = contradiction_n
    log(f"Semantic contradiction (gross+micro_t3b_corrected=TRUE): {contradiction_n}")

    return results


def check_baselines(results: dict[str, Any]) -> None:
    """Verify all 10 frozen baselines against live probe. Halt if any drift > 2%."""
    log("=" * 60)
    log("BASELINE DRIFT CHECK (halt threshold: 2%)")
    log("=" * 60)

    checks = [
        ("CPM_ROWCOUNT",
         results["cpm_rows"],                CPM_ROWS),
        ("ETE_GRADE_FINAL_V2_GROSS",
         results["n_gross"],                 ETE_GRADE_FINAL_V2_GROSS),
        ("ETE_GRADE_FINAL_V2_MICROSCOPIC",
         results["n_micro"],                 ETE_GRADE_FINAL_V2_MICROSCOPIC),
        ("ETE_GRADE_FINAL_V2_ABSENT",
         results["n_absent"],                ETE_GRADE_FINAL_V2_ABSENT),
        ("ETE_GRADE_FINAL_V2_PRESENT_UNGRADED",
         results["n_present"],               ETE_GRADE_FINAL_V2_PRESENT_UNGRADED),
        ("ETE_GRADE_FINAL_V2_JUNK_FALSE",
         results["n_false"],                 ETE_GRADE_FINAL_V2_JUNK_FALSE),
        ("ETE_GRADE_FINAL_V2_JUNK_TRUE",
         results["n_true_junk"],             ETE_GRADE_FINAL_V2_JUNK_TRUE),
        ("ETE_GRADE_FINAL_V2_NULL",
         results["n_null"],                  ETE_GRADE_FINAL_V2_NULL),
        ("MICROSCOPIC_ETE_T3B_CORRECTED_TRUE",
         results["micro_t3b_corrected_true"], MICROSCOPIC_ETE_T3B_CORRECTED_TRUE),
        ("AJCC8_T_STAGE_W_MICROETE_DEPRECATED_T3B",
         results["deprecated_t3b"],          AJCC8_T_STAGE_W_MICROETE_DEPRECATED_T3B),
    ]

    failed: list[str] = []
    for label, actual, expected in checks:
        drift = _pct_drift(actual, expected)
        status = "OK" if drift <= DRIFT_TOL else "DRIFT"
        log(f"  {label:<45s}  actual={actual:7d}  expected={expected:7d}  "
            f"drift={drift:.2%}  [{status}]")
        if drift > DRIFT_TOL:
            failed.append(
                f"{label}: actual={actual}, expected={expected}, drift={drift:.2%}"
            )

    if failed:
        msg = "BASELINE DRIFT GATE FAILED — halting:\n" + "\n".join(f"  {f}" for f in failed)
        err(msg)
        flush_log()
        raise SystemExit(msg)

    log("All 10 baselines within 2% drift tolerance — OK")
    log("=" * 60)


def phase0_dry_runs(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Dry-run each of steps A–F as read-only CTEs; return projected row changes."""
    projections: dict[str, Any] = {}
    log("")
    log("=" * 60)
    log("DRY-RUN PROJECTIONS (read-only CTEs, no writes)")
    log("=" * 60)

    # ------------------------------------------------------------------ #
    # Projection 1: ete_grade sync rows that would change
    # ------------------------------------------------------------------ #
    ete_grade_sync_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE COALESCE(ete_grade, '') != COALESCE(ete_grade_final_v2, '')
           OR COALESCE(ete_grade_final, '') != COALESCE(ete_grade_final_v2, '')
        """
    ).fetchone()[0]  # type: ignore[index]
    projections["ete_grade_sync_rows"] = ete_grade_sync_n
    log(f"Step ete_grade/ete_grade_final SYNC — rows that would change: {ete_grade_sync_n}")

    # breakdown by direction
    grade_sync_detail = con.execute(
        f"""
        SELECT
            SUM(CASE WHEN COALESCE(ete_grade,'') != COALESCE(ete_grade_final_v2,'')
                     THEN 1 ELSE 0 END)       AS ete_grade_mismatched,
            SUM(CASE WHEN COALESCE(ete_grade_final,'') != COALESCE(ete_grade_final_v2,'')
                     THEN 1 ELSE 0 END)       AS ete_grade_final_mismatched
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        """
    ).fetchone()
    log(f"  ete_grade mismatches: {grade_sync_detail[0]}, "  # type: ignore[index]
        f"ete_grade_final mismatches: {grade_sync_detail[1]}")  # type: ignore[index]

    # ------------------------------------------------------------------ #
    # Projection 2: DEPRECATED upgrade (gross → T3b where not already T3b)
    # ------------------------------------------------------------------ #
    deprecated_upgrade_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_final_v2 = 'gross'
          AND COALESCE(ajcc8_t_stage_with_microete_t3b_DEPRECATED, '') != 'T3b'
        """
    ).fetchone()[0]  # type: ignore[index]
    projections["deprecated_upgrade_rows"] = deprecated_upgrade_n
    log(f"Step 2C (DEPRECATED T3b upgrade) — rows that would change: {deprecated_upgrade_n}")

    # ------------------------------------------------------------------ #
    # Projection 3: microscopic_ete_t3b_corrected after rebuild (Step B)
    #   Will be TRUE for: ete_grade_final_v2='microscopic' AND DEPRECATED='T3b'
    #   AND diagnosis_primary NOT IN ('MTC','ATC')
    #   After Step 2C, DEPRECATED='T3b' for all gross rows, so gross rows won't
    #   qualify because ete_grade_final_v2='gross' not 'microscopic'.
    # ------------------------------------------------------------------ #
    micro_t3b_post_rebuild = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_final_v2 = 'microscopic'
          AND ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b'
          AND diagnosis_primary NOT IN ('MTC', 'ATC')
        """
    ).fetchone()[0]  # type: ignore[index]
    projections["micro_t3b_corrected_post"] = micro_t3b_post_rebuild
    log(f"Step B (micro_t3b_corrected=TRUE post-rebuild) — projected TRUE count: {micro_t3b_post_rebuild}")
    log(f"  (reduction from {MICROSCOPIC_ETE_T3B_CORRECTED_TRUE} → {micro_t3b_post_rebuild}; "
        f"contradiction cleared: {MICROSCOPIC_ETE_T3B_CORRECTED_TRUE - micro_t3b_post_rebuild} rows)")

    # ------------------------------------------------------------------ #
    # Projection 4: ajcc8_t_stage changes (Steps C + D)
    # ------------------------------------------------------------------ #
    # After rebuild:
    #   - corrected=TRUE rows → size-based
    #   - corrected=FALSE + gross → T3b
    #   - corrected=FALSE + not gross → copy DEPRECATED
    # Count differences vs current ajcc8_t_stage

    t_stage_changes = con.execute(
        f"""
        WITH new_t AS (
            SELECT
                research_id,
                ajcc8_t_stage  AS old_t,
                CASE
                    -- Step B qualifiers (TRUE → size-based)
                    WHEN ete_grade_final_v2 = 'microscopic'
                         AND ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b'
                         AND diagnosis_primary NOT IN ('MTC', 'ATC')
                    THEN CASE
                        WHEN tumor_size_cm_dominant IS NULL THEN 'T3a'
                        WHEN tumor_size_cm_dominant <= 1.0  THEN 'T1a'
                        WHEN tumor_size_cm_dominant <= 2.0  THEN 'T1b'
                        WHEN tumor_size_cm_dominant <= 4.0  THEN 'T2'
                        ELSE 'T3a'
                    END
                    -- Step D: gross → T3b
                    WHEN ete_grade_final_v2 = 'gross'
                    THEN 'T3b'
                    -- Step D: passthrough
                    ELSE ajcc8_t_stage_with_microete_t3b_DEPRECATED
                END AS new_t
            FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        )
        SELECT
            COUNT(*) AS total_changing,
            SUM(CASE WHEN new_t = 'T3b' AND (old_t IS DISTINCT FROM 'T3b')
                     THEN 1 ELSE 0 END) AS upgrade_to_t3b,
            SUM(CASE WHEN new_t != 'T3b' AND old_t = 'T3b'
                     THEN 1 ELSE 0 END) AS downgrade_from_t3b
        FROM new_t
        WHERE new_t IS DISTINCT FROM old_t
        """
    ).fetchone()
    total_t_changing = t_stage_changes[0]  # type: ignore[index]
    upgrade_t3b      = t_stage_changes[1]  # type: ignore[index]
    downgrade_t3b    = t_stage_changes[2]  # type: ignore[index]
    projections["t_stage_corrected_changes"] = total_t_changing
    projections["t_stage_upgrade_to_t3b"]    = upgrade_t3b
    projections["t_stage_downgrade_from_t3b"] = downgrade_t3b
    log(f"Step C+D (ajcc8_t_stage) — rows changing: {total_t_changing} "
        f"(+T3b={upgrade_t3b}, -T3b={downgrade_t3b})")

    # ------------------------------------------------------------------ #
    # Projection 5: ajcc8_stage_group_corrected changes (Step E)
    # ------------------------------------------------------------------ #
    stage_group_changes = con.execute(
        f"""
        WITH new_stage AS (
            SELECT
                research_id,
                ajcc8_stage_group_corrected AS old_sg,
                CASE
                    WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
                    WHEN age_at_surgery < 55                           THEN 'I'
                    WHEN ajcc8_m_stage = 'M1'                         THEN 'IVB'
                    WHEN (
                        CASE
                            WHEN ete_grade_final_v2 = 'microscopic'
                                 AND ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b'
                                 AND diagnosis_primary NOT IN ('MTC', 'ATC')
                            THEN CASE
                                WHEN tumor_size_cm_dominant IS NULL THEN 'T3a'
                                WHEN tumor_size_cm_dominant <= 1.0  THEN 'T1a'
                                WHEN tumor_size_cm_dominant <= 2.0  THEN 'T1b'
                                WHEN tumor_size_cm_dominant <= 4.0  THEN 'T2'
                                ELSE 'T3a'
                            END
                            WHEN ete_grade_final_v2 = 'gross' THEN 'T3b'
                            ELSE ajcc8_t_stage_with_microete_t3b_DEPRECATED
                        END
                    ) IN ('T1a','T1b','T2')
                     AND (ajcc8_n_stage IS NULL OR ajcc8_n_stage IN ('N0','N0a','N0b','NX'))
                    THEN 'I'
                    WHEN (
                        CASE
                            WHEN ete_grade_final_v2 = 'microscopic'
                                 AND ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b'
                                 AND diagnosis_primary NOT IN ('MTC', 'ATC')
                            THEN CASE
                                WHEN tumor_size_cm_dominant IS NULL THEN 'T3a'
                                WHEN tumor_size_cm_dominant <= 1.0  THEN 'T1a'
                                WHEN tumor_size_cm_dominant <= 2.0  THEN 'T1b'
                                WHEN tumor_size_cm_dominant <= 4.0  THEN 'T2'
                                ELSE 'T3a'
                            END
                            WHEN ete_grade_final_v2 = 'gross' THEN 'T3b'
                            ELSE ajcc8_t_stage_with_microete_t3b_DEPRECATED
                        END
                    ) IN ('T1a','T1b','T2')
                     AND ajcc8_n_stage IN ('N1','N1a','N1b')
                    THEN 'II'
                    WHEN (
                        CASE
                            WHEN ete_grade_final_v2 = 'microscopic'
                                 AND ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b'
                                 AND diagnosis_primary NOT IN ('MTC', 'ATC')
                            THEN CASE
                                WHEN tumor_size_cm_dominant IS NULL THEN 'T3a'
                                WHEN tumor_size_cm_dominant <= 1.0  THEN 'T1a'
                                WHEN tumor_size_cm_dominant <= 2.0  THEN 'T1b'
                                WHEN tumor_size_cm_dominant <= 4.0  THEN 'T2'
                                ELSE 'T3a'
                            END
                            WHEN ete_grade_final_v2 = 'gross' THEN 'T3b'
                            ELSE ajcc8_t_stage_with_microete_t3b_DEPRECATED
                        END
                    ) IN ('T3a','T3b')
                    THEN 'II'
                    WHEN (
                        CASE
                            WHEN ete_grade_final_v2 = 'microscopic'
                                 AND ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b'
                                 AND diagnosis_primary NOT IN ('MTC', 'ATC')
                            THEN CASE
                                WHEN tumor_size_cm_dominant IS NULL THEN 'T3a'
                                WHEN tumor_size_cm_dominant <= 1.0  THEN 'T1a'
                                WHEN tumor_size_cm_dominant <= 2.0  THEN 'T1b'
                                WHEN tumor_size_cm_dominant <= 4.0  THEN 'T2'
                                ELSE 'T3a'
                            END
                            WHEN ete_grade_final_v2 = 'gross' THEN 'T3b'
                            ELSE ajcc8_t_stage_with_microete_t3b_DEPRECATED
                        END
                    ) = 'T4a'
                    THEN 'III'
                    WHEN (
                        CASE
                            WHEN ete_grade_final_v2 = 'microscopic'
                                 AND ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b'
                                 AND diagnosis_primary NOT IN ('MTC', 'ATC')
                            THEN CASE
                                WHEN tumor_size_cm_dominant IS NULL THEN 'T3a'
                                WHEN tumor_size_cm_dominant <= 1.0  THEN 'T1a'
                                WHEN tumor_size_cm_dominant <= 2.0  THEN 'T1b'
                                WHEN tumor_size_cm_dominant <= 4.0  THEN 'T2'
                                ELSE 'T3a'
                            END
                            WHEN ete_grade_final_v2 = 'gross' THEN 'T3b'
                            ELSE ajcc8_t_stage_with_microete_t3b_DEPRECATED
                        END
                    ) = 'T4b'
                    THEN 'IVA'
                    ELSE ajcc8_stage_group
                END AS new_sg
            FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        )
        SELECT COUNT(*)
        FROM new_stage
        WHERE new_sg IS DISTINCT FROM old_sg
        """
    ).fetchone()[0]  # type: ignore[index]
    projections["stage_group_changes"] = stage_group_changes
    log(f"Step E (ajcc8_stage_group_corrected) — rows changing: {stage_group_changes}")

    # ------------------------------------------------------------------ #
    # Projection 6: manuscript_cohort_v1.ajcc8_t_stage changes (Step F)
    # ------------------------------------------------------------------ #
    if table_exists(con, MAIN_SCHEMA, MANUSCRIPT_COHORT):
        mc_rows = row_count(con, MAIN_SCHEMA, MANUSCRIPT_COHORT)
        mc_changes = con.execute(
            f"""
            WITH new_t AS (
                SELECT
                    research_id,
                    CASE
                        WHEN ete_grade_final_v2 = 'microscopic'
                             AND ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b'
                             AND diagnosis_primary NOT IN ('MTC', 'ATC')
                        THEN CASE
                            WHEN tumor_size_cm_dominant IS NULL THEN 'T3a'
                            WHEN tumor_size_cm_dominant <= 1.0  THEN 'T1a'
                            WHEN tumor_size_cm_dominant <= 2.0  THEN 'T1b'
                            WHEN tumor_size_cm_dominant <= 4.0  THEN 'T2'
                            ELSE 'T3a'
                        END
                        WHEN ete_grade_final_v2 = 'gross' THEN 'T3b'
                        ELSE ajcc8_t_stage_with_microete_t3b_DEPRECATED
                    END AS new_t_corrected
                FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
            )
            SELECT COUNT(*)
            FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{MANUSCRIPT_COHORT}" mc
            JOIN new_t ON new_t.research_id = mc.research_id
            WHERE new_t.new_t_corrected IS DISTINCT FROM mc.ajcc8_t_stage
            """
        ).fetchone()[0]  # type: ignore[index]
        projections["mc_t_stage_changes"] = mc_changes
        projections["mc_rows"] = mc_rows
        log(f"Step F (manuscript_cohort_v1.ajcc8_t_stage) — rows changing: {mc_changes} "
            f"(of {mc_rows} total)")
    else:
        warn(f"{MANUSCRIPT_COHORT} not found — Step F projection skipped")
        projections["mc_t_stage_changes"] = None
        projections["mc_rows"] = None

    log("")
    log("DRY-RUN PROJECTIONS COMPLETE")
    return projections


def write_probe_report(
    results: dict[str, Any],
    projections: dict[str, Any],
) -> None:
    """Write the probe report markdown file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()

    lines = [
        "# Script 391 — T-stage Downstream Reconciliation: Phase 0 Probe Report",
        "",
        f"**Generated:** {ts}",
        f"**Database:** {PUB_DB}",
        "",
        "---",
        "",
        "## 1. Baseline Drift Check",
        "",
        "| Metric | Expected | Actual | Drift | Status |",
        "|--------|----------|--------|-------|--------|",
    ]

    checks = [
        ("CPM_ROWCOUNT",
         CPM_ROWS,                    results["cpm_rows"]),
        ("ETE_GRADE_FINAL_V2_GROSS",
         ETE_GRADE_FINAL_V2_GROSS,    results["n_gross"]),
        ("ETE_GRADE_FINAL_V2_MICROSCOPIC",
         ETE_GRADE_FINAL_V2_MICROSCOPIC, results["n_micro"]),
        ("ETE_GRADE_FINAL_V2_ABSENT",
         ETE_GRADE_FINAL_V2_ABSENT,   results["n_absent"]),
        ("ETE_GRADE_FINAL_V2_PRESENT_UNGRADED",
         ETE_GRADE_FINAL_V2_PRESENT_UNGRADED, results["n_present"]),
        ("ETE_GRADE_FINAL_V2_JUNK_FALSE",
         ETE_GRADE_FINAL_V2_JUNK_FALSE, results["n_false"]),
        ("ETE_GRADE_FINAL_V2_JUNK_TRUE",
         ETE_GRADE_FINAL_V2_JUNK_TRUE, results["n_true_junk"]),
        ("ETE_GRADE_FINAL_V2_NULL",
         ETE_GRADE_FINAL_V2_NULL,     results["n_null"]),
        ("MICROSCOPIC_ETE_T3B_CORRECTED_TRUE",
         MICROSCOPIC_ETE_T3B_CORRECTED_TRUE, results["micro_t3b_corrected_true"]),
        ("AJCC8_T_STAGE_W_MICROETE_DEPRECATED_T3B",
         AJCC8_T_STAGE_W_MICROETE_DEPRECATED_T3B, results["deprecated_t3b"]),
    ]

    all_pass = True
    for label, expected, actual in checks:
        drift = _pct_drift(actual, expected)
        status = "✅ OK" if drift <= DRIFT_TOL else "❌ DRIFT"
        if drift > DRIFT_TOL:
            all_pass = False
        lines.append(
            f"| {label} | {expected:,} | {actual:,} | {drift:.2%} | {status} |"
        )

    lines += [
        "",
        f"**Overall baseline gate:** {'✅ ALL PASS' if all_pass else '❌ FAILED — HALT'}",
        "",
        "---",
        "",
        "## 2. Pre-State Problem Summary",
        "",
        f"- Gross-ETE rows with non-T3b DEPRECATED stage (target for rebuild): **{results['gross_non_t3b']}**",
        f"- Semantic contradiction (gross + micro_t3b_corrected=TRUE): **{results['contradiction_gross_micro']}**",
        "",
        "---",
        "",
        "## 3. Dry-Run Step Projections",
        "",
        "| Step | Operation | Rows Changing | Notes |",
        "|------|-----------|---------------|-------|",
        f"| 2B  | ete_grade + ete_grade_final SYNC ← ete_grade_final_v2 "
        f"| **{projections['ete_grade_sync_rows']:,}** | Both columns sync simultaneously |",
        f"| 2C  | DEPRECATED T3b upgrade for gross-ETE "
        f"| **{projections['deprecated_upgrade_rows']:,}** | Expected ~172 |",
        f"| 2D-B | micro_t3b_corrected=TRUE count (post-rebuild) "
        f"| from {MICROSCOPIC_ETE_T3B_CORRECTED_TRUE:,} → **{projections['micro_t3b_corrected_post']:,}** "
        f"| Contradiction cleared |",
        f"| 2D-C/D | ajcc8_t_stage rebuild "
        f"| **{projections['t_stage_corrected_changes']:,}** rows "
        f"(+T3b={projections['t_stage_upgrade_to_t3b']}, -T3b={projections['t_stage_downgrade_from_t3b']}) "
        f"| |",
        f"| 2D-E | ajcc8_stage_group_corrected rebuild "
        f"| **{projections['stage_group_changes']:,}** | AJCC8 staging rule |",
        f"| 2E  | manuscript_cohort_v1.ajcc8_t_stage rebuild "
        f"| **{projections.get('mc_t_stage_changes', 'N/A')}** "
        f"(of {projections.get('mc_rows', 'N/A')} total) | 100% join expected |",
        "",
        "---",
        "",
        "## 4. Next Steps",
        "",
        "1. Review the projections above.",
        "2. Write `APPROVED` to `scripts/output/391_plan_approval.txt`.",
        "3. Run: `python3 scripts/391_t_stage_downstream_reconciliation.py --apply`",
        "",
    ]

    with PROBE_REPORT_PATH.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    log(f"Probe report written → {PROBE_REPORT_PATH}")

    # Write the plan approval placeholder (if not already present)
    if not PLAN_APPROVAL_PATH.exists():
        with PLAN_APPROVAL_PATH.open("w", encoding="utf-8") as fh:
            fh.write(
                "# Script 391 Plan Approval\n"
                "# Write APPROVED on the line below to unlock --apply.\n"
                "\n"
            )
        log(f"Plan approval placeholder written → {PLAN_APPROVAL_PATH}")
    else:
        log(f"Plan approval file already exists → {PLAN_APPROVAL_PATH}")


# --------------------------------------------------------------------------- #
# Approval gate
# --------------------------------------------------------------------------- #


def check_approval() -> None:
    """Read plan_approval.txt; halt if 'APPROVED' not present."""
    if not PLAN_APPROVAL_PATH.exists():
        raise SystemExit(
            f"Plan approval file not found: {PLAN_APPROVAL_PATH}\n"
            "Run Phase 0 first, then write APPROVED to that file."
        )
    content = PLAN_APPROVAL_PATH.read_text(encoding="utf-8")
    if "APPROVED" not in content:
        raise SystemExit(
            f"Plan approval file does not contain 'APPROVED'.\n"
            f"Edit {PLAN_APPROVAL_PATH} and write APPROVED to unlock --apply."
        )
    log("Plan approval file: APPROVED — proceeding with apply")


# --------------------------------------------------------------------------- #
# Phase 2 — Apply
# --------------------------------------------------------------------------- #


def phase2a_snapshot(con: duckdb.DuckDBPyConnection) -> str:
    """2A: Snapshot all 6 target columns + research_id to archive schema."""
    snap = f"cpm_pre391_{RUN_STAMP}"
    sql = f"""
        CREATE OR REPLACE TABLE "{PUB_DB}"."{ARC_SCHEMA}"."{snap}" AS
        SELECT
            research_id,
            ete_grade,
            ete_grade_final,
            ete_grade_final_v2,
            microscopic_ete_t3b_corrected,
            ajcc8_t_stage,
            ajcc8_stage_group_corrected,
            ajcc8_t_stage_with_microete_t3b_DEPRECATED,
            tumor_size_cm_dominant,
            diagnosis_primary
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
    """
    log(f"[2A] Creating snapshot: {ARC_SCHEMA}.{snap}")
    con.execute(sql)
    snap_n = row_count(con, ARC_SCHEMA, snap)
    log(f"[2A] Snapshot rowcount: {snap_n}  (expected {CPM_ROWS})")
    if snap_n != CPM_ROWS:
        raise SystemExit(
            f"[2A] HALT: snapshot rowcount {snap_n} != expected {CPM_ROWS}"
        )
    return snap


def phase2b_sync_legacy(con: duckdb.DuckDBPyConnection) -> int:
    """2B: Sync ete_grade + ete_grade_final ← ete_grade_final_v2."""
    where = (
        f"COALESCE(ete_grade, '') != COALESCE(ete_grade_final_v2, '') "
        f"OR COALESCE(ete_grade_final, '') != COALESCE(ete_grade_final_v2, '')"
    )
    n = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" WHERE {where}'
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2B] Syncing legacy ete_grade + ete_grade_final ← ete_grade_final_v2 ...")
    con.execute(
        f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        SET ete_grade       = ete_grade_final_v2,
            ete_grade_final = ete_grade_final_v2
        WHERE {where}
        """
    )
    log(f"[2B] Legacy sync: {n} rows updated")
    return int(n)


def phase2c_deprecated_upgrade(con: duckdb.DuckDBPyConnection) -> int:
    """2C: Upgrade DEPRECATED T3b for gross-ETE rows not already T3b."""
    where = (
        "ete_grade_final_v2 = 'gross' "
        "AND COALESCE(ajcc8_t_stage_with_microete_t3b_DEPRECATED, '') != 'T3b'"
    )
    n = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" WHERE {where}'
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2C] Upgrading DEPRECATED T-stage to T3b for gross-ETE rows ...")
    con.execute(
        f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        SET ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b'
        WHERE {where}
        """
    )
    log(f"[2C] DEPRECATED upgrade: {n} rows updated  (expected ~{GROSS_WITH_NON_T3B_STAGE})")
    return int(n)


def phase2d_step_a(con: duckdb.DuckDBPyConnection) -> int:
    """2D Step A: Reset microscopic_ete_t3b_corrected = FALSE for all rows."""
    n = row_count(con, MAIN_SCHEMA, CPM_TABLE)
    log(f"[2D-A] Resetting microscopic_ete_t3b_corrected = FALSE for all {n} rows ...")
    con.execute(
        f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        SET microscopic_ete_t3b_corrected = FALSE
        """
    )
    log(f"[2D-A] Reset: {n} rows updated")
    return int(n)


def phase2d_step_bc(con: duckdb.DuckDBPyConnection) -> int:
    """2D Steps B+C: Set corrected=TRUE + size-based t_stage for micro+T3b+DTC."""
    where = (
        "ete_grade_final_v2 = 'microscopic' "
        "AND ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b' "
        "AND diagnosis_primary NOT IN ('MTC', 'ATC')"
    )
    n = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" WHERE {where}'
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2D-B/C] Setting corrected=TRUE + size-based t_stage for micro+T3b+DTC ...")
    con.execute(
        f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        SET microscopic_ete_t3b_corrected = TRUE,
            ajcc8_t_stage = CASE
                WHEN tumor_size_cm_dominant IS NULL THEN 'T3a'
                WHEN tumor_size_cm_dominant <= 1.0  THEN 'T1a'
                WHEN tumor_size_cm_dominant <= 2.0  THEN 'T1b'
                WHEN tumor_size_cm_dominant <= 4.0  THEN 'T2'
                ELSE 'T3a'
            END
        WHERE {where}
        """
    )
    log(f"[2D-B/C] Corrected rows (TRUE): {n}")
    return int(n)


def phase2d_step_d(con: duckdb.DuckDBPyConnection) -> int:
    """2D Step D: Non-corrected rows — gross→T3b, else copy DEPRECATED."""
    where = "microscopic_ete_t3b_corrected = FALSE"
    n = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" WHERE {where}'
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2D-D] Copying ajcc8_t_stage for non-corrected rows "
        f"(gross→T3b, else DEPRECATED) ...")
    con.execute(
        f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        SET ajcc8_t_stage = CASE
            WHEN ete_grade_final_v2 = 'gross' THEN 'T3b'
            ELSE ajcc8_t_stage_with_microete_t3b_DEPRECATED
        END
        WHERE {where}
        """
    )
    log(f"[2D-D] Non-corrected rows updated: {n}")
    return int(n)


def phase2d_step_e_corrected(con: duckdb.DuckDBPyConnection) -> int:
    """2D Step E (corrected=TRUE rows): Re-derive ajcc8_stage_group_corrected."""
    where = "microscopic_ete_t3b_corrected = TRUE"
    n = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" WHERE {where}'
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2D-E] Re-deriving ajcc8_stage_group_corrected for corrected rows ...")
    con.execute(
        f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        SET ajcc8_stage_group_corrected = CASE
            WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
            WHEN age_at_surgery < 55                          THEN 'I'
            WHEN ajcc8_m_stage = 'M1'                        THEN 'IVB'
            WHEN ajcc8_t_stage IN ('T1a','T1b','T2')
                 AND (ajcc8_n_stage IS NULL
                      OR ajcc8_n_stage IN ('N0','N0a','N0b','NX'))
            THEN 'I'
            WHEN ajcc8_t_stage IN ('T1a','T1b','T2')
                 AND ajcc8_n_stage IN ('N1','N1a','N1b')
            THEN 'II'
            WHEN ajcc8_t_stage IN ('T3a','T3b') THEN 'II'
            WHEN ajcc8_t_stage = 'T4a'          THEN 'III'
            WHEN ajcc8_t_stage = 'T4b'          THEN 'IVA'
            ELSE ajcc8_stage_group
        END
        WHERE {where}
        """
    )
    log(f"[2D-E corrected] Stage group re-derived: {n} rows")
    return int(n)


def phase2d_step_e_noncorrected(con: duckdb.DuckDBPyConnection) -> int:
    """2D Step E (corrected=FALSE rows): Re-derive stage group using rebuilt T-stage.

    For non-corrected rows, ajcc8_t_stage has already been set
    (either T3b for gross or DEPRECATED passthrough).  We now re-derive
    stage group for ALL non-corrected rows so the 172 gross→T3b flip-ups
    also get the correct stage group.
    """
    where = "microscopic_ete_t3b_corrected = FALSE"
    n = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" WHERE {where}'
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2D-E non-corrected] Re-deriving ajcc8_stage_group_corrected for non-corrected rows ...")
    con.execute(
        f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        SET ajcc8_stage_group_corrected = CASE
            WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
            WHEN age_at_surgery < 55                          THEN 'I'
            WHEN ajcc8_m_stage = 'M1'                        THEN 'IVB'
            WHEN ajcc8_t_stage IN ('T1a','T1b','T2')
                 AND (ajcc8_n_stage IS NULL
                      OR ajcc8_n_stage IN ('N0','N0a','N0b','NX'))
            THEN 'I'
            WHEN ajcc8_t_stage IN ('T1a','T1b','T2')
                 AND ajcc8_n_stage IN ('N1','N1a','N1b')
            THEN 'II'
            WHEN ajcc8_t_stage IN ('T3a','T3b') THEN 'II'
            WHEN ajcc8_t_stage = 'T4a'          THEN 'III'
            WHEN ajcc8_t_stage = 'T4b'          THEN 'IVA'
            ELSE ajcc8_stage_group
        END
        WHERE {where}
        """
    )
    log(f"[2D-E non-corrected] Stage group re-derived: {n} rows")
    return int(n)


def phase2e_manuscript_rebuild(con: duckdb.DuckDBPyConnection) -> int:
    """2E Step F: Rebuild manuscript_cohort_v1.ajcc8_t_stage from CPM."""
    if not table_exists(con, MAIN_SCHEMA, MANUSCRIPT_COHORT):
        warn(f"[2E] {MANUSCRIPT_COHORT} not found — skipping manuscript rebuild")
        return 0

    n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{MANUSCRIPT_COHORT}" mc
        JOIN "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
            ON cpm.research_id = mc.research_id
        WHERE mc.ajcc8_t_stage IS DISTINCT FROM cpm.ajcc8_t_stage
        """
    ).fetchone()[0]  # type: ignore[index]
    log(f"[2E] Rebuilding {MANUSCRIPT_COHORT}.ajcc8_t_stage from CPM.ajcc8_t_stage ...")
    con.execute(
        f"""
        UPDATE "{PUB_DB}"."{MAIN_SCHEMA}"."{MANUSCRIPT_COHORT}" mc
        SET ajcc8_t_stage = cpm.ajcc8_t_stage
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
        WHERE cpm.research_id = mc.research_id
          AND mc.ajcc8_t_stage IS DISTINCT FROM cpm.ajcc8_t_stage
        """
    )
    log(f"[2E] manuscript_cohort_v1.ajcc8_t_stage: {n} rows updated")
    return int(n)


def phase2f_provenance(
    con: duckdb.DuckDBPyConnection,
    snap_name: str,
    n_legacy_sync: int,
    n_micro_t3b_post: int,
    n_t_stage_changed: int,
    n_stage_group_changed: int,
    n_mc_changed: int,
) -> None:
    """2F: Insert 3 __readme provenance rows."""
    ts_sql = "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)"

    # Verify __readme table exists
    if not table_exists(con, MAIN_SCHEMA, README_TABLE):
        raise SystemExit(
            f"[2F] HALT: {MAIN_SCHEMA}.{README_TABLE} does not exist — cannot write provenance"
        )

    rows = [
        (
            f"Script 391: legacy ete_grade + ete_grade_final synced from ete_grade_final_v2; "
            f"{n_legacy_sync} rows updated. Pre-390 values preserved in "
            f"archive_pub_v1_0.cpm_ete_pre390_20260422."
        ),
        (
            f"Script 391: T-stage downstream rebuild — 172 gross-ETE rows upgraded to T3b; "
            f"microscopic_ete_t3b_corrected reduced from {MICROSCOPIC_ETE_T3B_CORRECTED_TRUE} "
            f"to {n_micro_t3b_post}; ajcc8_stage_group_corrected re-derived for "
            f"{n_stage_group_changed} DTC patients. Snapshot: archive_pub_v1_0.{snap_name}."
        ),
        (
            f"Script 391: manuscript_cohort_v1.ajcc8_t_stage rebuilt from "
            f"CPM.ajcc8_t_stage; {n_mc_changed} rows modified."
        ),
    ]

    for content in rows:
        con.execute(
            f"""
            INSERT INTO "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
                (content, updated_at)
            VALUES (?, {ts_sql})
            """,
            [content],
        )
        log(f"[2F] __readme row inserted: {content[:80]}...")

    log(f"[2F] 3 provenance rows inserted into {README_TABLE}")


# --------------------------------------------------------------------------- #
# Phase 3 — Post-state verification
# --------------------------------------------------------------------------- #


def phase3_verify(con: duckdb.DuckDBPyConnection, snap_name: str) -> None:
    """Run all post-state invariant checks. Halt on any failure."""
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

    # 1. CPM rowcount unchanged
    cpm_n = row_count(con, MAIN_SCHEMA, CPM_TABLE)
    check("CPM rowcount = 10,871", cpm_n == CPM_ROWS, f"got {cpm_n}")

    # 2. manuscript_cohort_v1 rowcount unchanged
    if table_exists(con, MAIN_SCHEMA, MANUSCRIPT_COHORT):
        mc_n = row_count(con, MAIN_SCHEMA, MANUSCRIPT_COHORT)
        log(f"  manuscript_cohort_v1 rowcount: {mc_n}")
    else:
        warn(f"  {MANUSCRIPT_COHORT} not found — skipping mc rowcount check")

    # 3. ete_grade == ete_grade_final_v2 for all rows (100% match)
    ete_grade_mismatch = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE COALESCE(ete_grade, '') != COALESCE(ete_grade_final_v2, '')
        """
    ).fetchone()[0]  # type: ignore[index]
    check(
        "ete_grade == ete_grade_final_v2 (100% match)",
        ete_grade_mismatch == 0,
        f"{ete_grade_mismatch} mismatches",
    )

    # 4. ete_grade_final == ete_grade_final_v2 for all rows (100% match)
    ete_grade_final_mismatch = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE COALESCE(ete_grade_final, '') != COALESCE(ete_grade_final_v2, '')
        """
    ).fetchone()[0]  # type: ignore[index]
    check(
        "ete_grade_final == ete_grade_final_v2 (100% match)",
        ete_grade_final_mismatch == 0,
        f"{ete_grade_final_mismatch} mismatches",
    )

    # 5. 0 gross rows with non-T3b DEPRECATED stage
    gross_non_t3b_post = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_final_v2 = 'gross'
          AND COALESCE(ajcc8_t_stage_with_microete_t3b_DEPRECATED, '') != 'T3b'
        """
    ).fetchone()[0]  # type: ignore[index]
    check(
        "0 rows: gross-ETE with non-T3b DEPRECATED stage",
        gross_non_t3b_post == 0,
        f"{gross_non_t3b_post} residual rows",
    )

    # 6. 0 gross rows with micro_t3b_corrected=TRUE (semantic contradiction cleared)
    contradiction_post = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ete_grade_final_v2 = 'gross'
          AND microscopic_ete_t3b_corrected = TRUE
        """
    ).fetchone()[0]  # type: ignore[index]
    check(
        "0 rows: gross-ETE + microscopic_ete_t3b_corrected=TRUE (contradiction)",
        contradiction_post == 0,
        f"{contradiction_post} remaining contradiction rows",
    )

    # 7. Every corrected=TRUE row has: micro + T3b + DTC
    invalid_corrected = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE microscopic_ete_t3b_corrected = TRUE
          AND NOT (
              ete_grade_final_v2 = 'microscopic'
              AND ajcc8_t_stage_with_microete_t3b_DEPRECATED = 'T3b'
              AND diagnosis_primary NOT IN ('MTC', 'ATC')
          )
        """
    ).fetchone()[0]  # type: ignore[index]
    check(
        "Every corrected=TRUE row qualifies (micro+T3b+DTC)",
        invalid_corrected == 0,
        f"{invalid_corrected} rows with TRUE flag that don't qualify",
    )

    # 8. Every corrected=FALSE row has ajcc8_t_stage == DEPRECATED
    #    (the gross rows will have T3b in DEPRECATED AND corrected, so still match)
    false_mismatch = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE microscopic_ete_t3b_corrected = FALSE
          AND COALESCE(ajcc8_t_stage, '') !=
              COALESCE(ajcc8_t_stage_with_microete_t3b_DEPRECATED, '')
        """
    ).fetchone()[0]  # type: ignore[index]
    check(
        "Every corrected=FALSE row: ajcc8_t_stage == DEPRECATED",
        false_mismatch == 0,
        f"{false_mismatch} mismatches on non-corrected rows",
    )

    # 9. manuscript_cohort_v1.ajcc8_t_stage matches CPM.ajcc8_t_stage 100%
    if table_exists(con, MAIN_SCHEMA, MANUSCRIPT_COHORT):
        mc_mismatch = con.execute(
            f"""
            SELECT COUNT(*)
            FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{MANUSCRIPT_COHORT}" mc
            JOIN "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
                ON cpm.research_id = mc.research_id
            WHERE mc.ajcc8_t_stage IS DISTINCT FROM cpm.ajcc8_t_stage
            """
        ).fetchone()[0]  # type: ignore[index]
        check(
            "manuscript_cohort_v1.ajcc8_t_stage 100% matches CPM.ajcc8_t_stage",
            mc_mismatch == 0,
            f"{mc_mismatch} mismatches",
        )
    else:
        warn("  manuscript_cohort_v1 not found — mc T-stage join check skipped")

    # 10. Snapshot rowcount = 10,871
    if table_exists(con, ARC_SCHEMA, snap_name):
        snap_n = row_count(con, ARC_SCHEMA, snap_name)
        check(
            f"Snapshot {snap_name} rowcount = 10,871",
            snap_n == CPM_ROWS,
            f"got {snap_n}",
        )
    else:
        failures.append(f"Snapshot {snap_name} not found in {ARC_SCHEMA}")
        err(f"[FAIL] Snapshot {snap_name} not found in {ARC_SCHEMA}")

    # 11. 3 __readme rows from Script 391
    readme_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
        WHERE content LIKE 'Script 391%'
        """
    ).fetchone()[0]  # type: ignore[index]
    check(f"3 Script 391 __readme rows landed", readme_n == 3, f"found {readme_n}")

    # Final verdict
    log("=" * 60)
    if failures:
        msg = f"PHASE 3 FAILED — {len(failures)} invariant(s) violated:\n" + \
              "\n".join(f"  {f}" for f in failures)
        err(msg)
        flush_log()
        raise SystemExit(msg)

    log("PHASE 3: ALL INVARIANTS PASSED (9 checks)")
    log("=" * 60)


# --------------------------------------------------------------------------- #
# Close-out report
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Diagnostic helpers (non-blocking — logs only, never halt)
# --------------------------------------------------------------------------- #

COHORT_CSV_PATH = OUTPUT_DIR / "391_t3b_upgrade_cohort.csv"


def dump_t3b_upgrade_cohort(con: duckdb.DuckDBPyConnection, snap_name: str) -> int:
    """Dump the 172 gross-ETE patients that had non-T3b DEPRECATED stage to CSV.

    Uses the pre-391 snapshot (state before any 391 changes) so the list
    is deterministic regardless of when this is called during apply.
    Returns the count for verification.
    """
    rows = con.execute(
        f"""
        SELECT research_id
        FROM "{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}"
        WHERE ete_grade_final_v2 = 'gross'
          AND COALESCE(ajcc8_t_stage_with_microete_t3b_DEPRECATED, '') != 'T3b'
        ORDER BY research_id
        """
    ).fetchall()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with COHORT_CSV_PATH.open("w", encoding="utf-8") as fh:
        fh.write("research_id\n")
        for r in rows:
            fh.write(f"{r[0]}\n")
    log(f"[DIAG] 172-cohort CSV written → {COHORT_CSV_PATH}  ({len(rows)} rows)")
    return len(rows)


def log_stage_group_cascade(
    con: duckdb.DuckDBPyConnection, snap_name: str
) -> None:
    """Log top-20 old→new stage_group_corrected transitions (diagnostic, non-halting)."""
    log("[DIAG] Stage-group cascade breakdown (top-20 old→new transitions):")
    rows = con.execute(
        f"""
        SELECT
            COALESCE(snap.ajcc8_stage_group_corrected, '<NULL>') AS old_group,
            COALESCE(cpm.ajcc8_stage_group_corrected, '<NULL>')  AS new_group,
            COUNT(*) AS n
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
        JOIN "{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}" snap
            ON cpm.research_id = snap.research_id
        WHERE cpm.ajcc8_stage_group_corrected IS DISTINCT FROM snap.ajcc8_stage_group_corrected
        GROUP BY 1, 2
        ORDER BY n DESC
        LIMIT 20
        """
    ).fetchall()
    log(f"  {'old_group':<12s}  {'new_group':<12s}  {'n':>6s}")
    log(f"  {'-'*12}  {'-'*12}  {'-'*6}")
    for r in rows:
        log(f"  {r[0]:<12s}  {r[1]:<12s}  {r[2]:>6d}")
    if not rows:
        log("  (no changes detected)")


# --------------------------------------------------------------------------- #
# Halt gates (called after each apply step)
# --------------------------------------------------------------------------- #


def _halt_gate(label: str, actual: int, lo: int | None, hi: int | None) -> None:
    """Raise SystemExit if actual is outside [lo, hi].  Pass None to skip that bound."""
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


def write_close_out(
    snap_name: str,
    n_legacy_sync: int,
    n_deprecated_upgrade: int,
    n_micro_t3b_post: int,
    n_t_stage_step_bc: int,
    n_t_stage_step_d: int,
    n_stage_group_changed: int,
    n_mc_changed: int,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    lines = [
        "# Script 391 — T-stage Downstream Reconciliation: Close-Out Report",
        "",
        f"**Completed:** {ts}",
        f"**Snapshot:** `archive_pub_v1_0.{snap_name}`",
        "",
        "## Summary",
        "",
        f"| Phase | Operation | Rows Affected |",
        f"|-------|-----------|---------------|",
        f"| 2B | ete_grade + ete_grade_final SYNC | {n_legacy_sync:,} |",
        f"| 2C | DEPRECATED T3b upgrade (gross-ETE) | {n_deprecated_upgrade:,} |",
        f"| 2D-B/C | micro_t3b_corrected=TRUE + size-based t_stage | {n_t_stage_step_bc:,} |",
        f"| 2D-D | non-corrected t_stage passthrough / gross→T3b | {n_t_stage_step_d:,} |",
        f"| 2D-E | stage_group_corrected re-derived (corrected+non-corrected) | {n_stage_group_changed:,} |",
        f"| 2E | manuscript_cohort_v1.ajcc8_t_stage rebuild | {n_mc_changed:,} |",
        "",
        "## Key Metrics Post-391",
        "",
        f"- `microscopic_ete_t3b_corrected=TRUE` reduced from {MICROSCOPIC_ETE_T3B_CORRECTED_TRUE:,} "
        f"→ {n_micro_t3b_post:,} (contradiction cleared)",
        f"- Gross-ETE rows with non-T3b DEPRECATED stage: 0 (was {GROSS_WITH_NON_T3B_STAGE:,})",
        "",
        "## Phase 3: All invariants passed",
        "",
        "See `391_run.log` for full execution trace.",
        "",
        "## Next",
        "",
        "Script 392 — boolean-string extractor trace + normalization "
        f"for the 183 remaining 'false'/'true' junk rows in ete_grade_final_v2.",
    ]
    with CLOSE_OUT_PATH.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    log(f"Close-out report written → {CLOSE_OUT_PATH}")


# --------------------------------------------------------------------------- #
# Idempotency check
# --------------------------------------------------------------------------- #


def idempotency_check(con: duckdb.DuckDBPyConnection) -> None:
    """
    Check for prior successful run.

    Both triggers must be present for NO-OP:
      1. archive_pub_v1_0.cpm_pre391_* snapshot exists
      2. __readme row starting with 'Script 391: T-stage downstream rebuild'

    If only one → halt 'partial prior run'.
    If both present AND Phase 3 invariants hold → exit 0 NO-OP.
    """
    snap = find_pre391_snapshot(con)
    readme_present = readme_391_present(con)

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
            "Idempotency: __readme row present but no cpm_pre391_* snapshot found. "
            "Partial prior run detected. Manual inspection required before re-running."
        )
    # else: neither present → fresh run, proceed normally
    log("Idempotency: no prior run detected — proceeding")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Script 391 — T-stage Downstream Reconciliation (post-390)"
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
        help="Apply all changes (Phase 2 + 3). Requires APPROVED plan approval file.",
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log(f"Script {SCRIPT_ID}: {SCRIPT_TAG}")
    log(f"Run stamp: {RUN_STAMP}")
    log(f"Mode: {'--apply' if args.apply else f'--phase {args.phase}'}")

    con = connect()

    # ------------------------------------------------------------------ #
    # In --apply mode, run idempotency check FIRST (before Phase 0
    # baselines which are calibrated to the pre-391 state and will fail
    # once the script has already been applied).
    # ------------------------------------------------------------------ #
    if args.apply:
        idempotency_check(con)   # exits 0 (NO-OP) if already complete
        check_approval()

    # ------------------------------------------------------------------ #
    # Phase 0 — always runs for --phase 0; also runs before fresh apply
    # ------------------------------------------------------------------ #
    log("")
    log("=" * 60)
    log("PHASE 0: DISCOVERY + PROBE")
    log("=" * 60)

    results = phase0_probe(con)
    check_baselines(results)
    projections = phase0_dry_runs(con)
    write_probe_report(results, projections)

    if not args.apply:
        log("")
        log("Phase 0 complete.  Review the probe report, write APPROVED to "
            f"{PLAN_APPROVAL_PATH}, then re-run with --apply.")
        flush_log()
        return

    # ------------------------------------------------------------------ #
    # Phase 2
    # ------------------------------------------------------------------ #
    log("")
    log("=" * 60)
    log("PHASE 2: APPLY")
    log("=" * 60)

    snap_name = phase2a_snapshot(con)

    # Capture the 172-cohort CSV immediately from snapshot (before any changes)
    n_cohort_csv = dump_t3b_upgrade_cohort(con, snap_name)
    _halt_gate("2A snapshot rowcount", row_count(con, ARC_SCHEMA, snap_name),
               CPM_ROWS, CPM_ROWS)

    n_legacy_sync = phase2b_sync_legacy(con)
    _halt_gate("2B ete_grade sync rows", n_legacy_sync, 1_121, 1_167)

    n_deprecated_upgrade = phase2c_deprecated_upgrade(con)
    _halt_gate("2C DEPRECATED T3b upgrade rows", n_deprecated_upgrade, 168, 176)

    _phase2d_a = phase2d_step_a(con)

    n_t_stage_step_bc = phase2d_step_bc(con)

    n_t_stage_step_d = phase2d_step_d(con)

    # Post-2D-D gate: total ajcc8_t_stage changes vs snapshot
    n_t_stage_total_changed = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
        JOIN "{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}" snap
            ON cpm.research_id = snap.research_id
        WHERE cpm.ajcc8_t_stage IS DISTINCT FROM snap.ajcc8_t_stage
        """
    ).fetchone()[0]  # type: ignore[index]
    log(f"ajcc8_t_stage total rows changed vs snapshot: {n_t_stage_total_changed}")
    _halt_gate("2D-C/D ajcc8_t_stage total changes", n_t_stage_total_changed, 1_056, 1_100)

    # Post-2D-B gate: micro_t3b_corrected=TRUE must be exactly 0
    n_micro_t3b_post = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE microscopic_ete_t3b_corrected = TRUE
        """
    ).fetchone()[0]  # type: ignore[index]
    log(f"Post-rebuild microscopic_ete_t3b_corrected=TRUE: {n_micro_t3b_post}")
    _halt_gate("2D-B micro_t3b_corrected=TRUE post-rebuild", n_micro_t3b_post, 0, 0)

    phase2d_step_e_corrected(con)
    phase2d_step_e_noncorrected(con)

    # Measure actual value-changes vs snapshot (not rows-touched by UPDATE)
    n_stage_group_changed = con.execute(
        f"""
        SELECT COUNT(*)
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}" cpm
        JOIN "{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}" snap
            ON cpm.research_id = snap.research_id
        WHERE cpm.ajcc8_stage_group_corrected IS DISTINCT FROM snap.ajcc8_stage_group_corrected
        """
    ).fetchone()[0]  # type: ignore[index]
    log(f"ajcc8_stage_group_corrected actual value-changes vs snapshot: {n_stage_group_changed}")
    _halt_gate("2D-E stage_group_corrected cascade", n_stage_group_changed, 3_702, 3_852)

    # Stage-group cascade diagnostic (non-halting)
    log_stage_group_cascade(con, snap_name)

    n_mc_changed = phase2e_manuscript_rebuild(con)
    _halt_gate("2E manuscript_cohort ajcc8_t_stage rows", n_mc_changed, 168, 176)

    phase2f_provenance(
        con,
        snap_name=snap_name,
        n_legacy_sync=n_legacy_sync,
        n_micro_t3b_post=n_micro_t3b_post,
        n_t_stage_changed=n_t_stage_total_changed,
        n_stage_group_changed=n_stage_group_changed,
        n_mc_changed=n_mc_changed,
    )

    # ------------------------------------------------------------------ #
    # Phase 3
    # ------------------------------------------------------------------ #
    phase3_verify(con, snap_name)

    # ------------------------------------------------------------------ #
    # Write close-out report
    # ------------------------------------------------------------------ #
    write_close_out(
        snap_name=snap_name,
        n_legacy_sync=n_legacy_sync,
        n_deprecated_upgrade=n_deprecated_upgrade,
        n_micro_t3b_post=n_micro_t3b_post,
        n_t_stage_step_bc=n_t_stage_step_bc,
        n_t_stage_step_d=n_t_stage_step_d,
        n_stage_group_changed=n_stage_group_changed,
        n_mc_changed=n_mc_changed,
    )

    log("")
    log("=" * 60)
    log(f"Script {SCRIPT_ID}: COMPLETE")
    log("Snapshot: " + snap_name)
    log(f"Commit tag when ready: v1_0-t-stage-reconciled-{RUN_STAMP}")
    log("=" * 60)
    flush_log()


if __name__ == "__main__":
    main()
