#!/usr/bin/env python3
"""
Script 221c — Patch tirads_v2_report_patient_rollup_v1 to three-valued semantics.

Background: 221b re-extraction (gpt-5.2) introduced a real NULL value-state
on tirads_v2_reports_raw.suspicious_ln_present (genuine radiologist
uncertainty, distinct from "no LN mention" silence). The original rollup
formula was binary (TRUE/FALSE only) and collapsed any non-TRUE input to
FALSE via MAX(CASE...END), which masked the new NULL bucket and pushed
~150-300 patients with only-NULL reports into FALSE incorrectly.

This patch:
  1. Rebuilds main.tirads_v2_report_patient_rollup_v1 with three-valued
     precedence TRUE > FALSE > NULL (Logan 2026-04-19):
       - TRUE  if any report is TRUE
       - FALSE if no TRUE and any report is FALSE
       - NULL  if no TRUE, no FALSE (only NULLs / silent / hedged)
  2. NULL-out canonical_patient_master.tirads_v2_any_suspicious_ln_on_us
     and refresh from the patched rollup so NULL values propagate.
  3. Updates COMMENT on the CPM column to document the three-valued
     semantics explicitly (per Logan's verbatim text).
  4. Stamps cpm_built_at on touched RIDs and inserts a provenance row.
  5. Prints the new patient-level distribution and re-validates Phase 6
     invariants.

Idempotent: safe to re-run. Touches ONLY the rollup table and the single
CPM column tirads_v2_any_suspicious_ln_on_us. No raw-table writes. No
git/commit operations.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
RAW_TABLE = "tirads_v2_reports_raw"
ROLLUP_TABLE = "tirads_v2_report_patient_rollup_v1"
CPM_COL = "tirads_v2_any_suspicious_ln_on_us"
EXPECTED_CPM_ROWS = 10_871

SCRIPT_TAG = "scripts/221c_rollup_threevalue_patch.py"
RUN_TS_ISO = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
RUN_DATE = RUN_TS_ISO[:10]

OUT_DIR = REPO_ROOT / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
DECISIONS_PATH = OUT_DIR / "221c_rollup_threevalue_patch.json"
LOG_PATH = OUT_DIR / "221c_rollup_threevalue_patch.log"

CPM_COMMENT = (
    "Patient-level rollup of tirads_v2_reports_raw.suspicious_ln_present "
    "with precedence TRUE > FALSE > NULL across all reports per RID. "
    "Three-valued: TRUE = >=1 report flagged suspicious LN; FALSE = no "
    "TRUE reports AND >=1 report explicitly cleared LNs; NULL = no "
    "conclusive radiologist call on any report (all reports either "
    "silent on LNs or used genuine uncertainty language: indeterminate, "
    "cannot exclude, of uncertain significance). For analysis: filter "
    "IS NOT NULL to restrict to assessed patients. Re-extracted "
    "2026-04-19 via GPT-5.2 with tightened prompt; original Qwen "
    "extraction had ~36% FP rate. See suspicious_ln_rationale on the "
    "raw report table for per-report justification."
)

_log_buf: list[str] = []


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]}Z] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def main() -> int:
    tok = get_token()
    if not tok:
        raise SystemExit(f"No MotherDuck token (token_mode={token_mode()}).")
    log(f"connecting to MotherDuck '{CANONICAL_DB}' (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")

    decisions: dict = {
        "script": SCRIPT_TAG,
        "ts": RUN_TS_ISO,
    }

    try:
        # Pre-state snapshot for diff
        log("=== PRE-PATCH STATE ===")
        pre_dist = con.execute(f"""
            SELECT
              {CPM_COL}, COUNT(*)
            FROM canonical_patient_master
            GROUP BY 1
            ORDER BY 1 NULLS FIRST
        """).fetchall()
        log("  CPM distribution (all patients):")
        for v, n in pre_dist:
            label = "NULL" if v is None else str(bool(v)).upper()
            log(f"    {label:<5s}  n={n:,}")

        pre_with_reports = con.execute(f"""
            SELECT
              COUNT(*) FILTER (WHERE {CPM_COL} = TRUE)  AS n_true,
              COUNT(*) FILTER (WHERE {CPM_COL} = FALSE) AS n_false,
              COUNT(*) FILTER (WHERE {CPM_COL} IS NULL) AS n_null,
              COUNT(*)                                  AS total
            FROM canonical_patient_master
            WHERE tirads_v2_n_reports IS NOT NULL
        """).fetchone()
        decisions["pre_patch_with_reports"] = {
            "n_true":  int(pre_with_reports[0]),
            "n_false": int(pre_with_reports[1]),
            "n_null":  int(pre_with_reports[2]),
            "total":   int(pre_with_reports[3]),
        }
        log(f"  Patients with reports: TRUE={pre_with_reports[0]:,}  "
            f"FALSE={pre_with_reports[1]:,}  NULL={pre_with_reports[2]:,}  "
            f"total={pre_with_reports[3]:,}")

        # Step 1: rebuild rollup with three-valued logic
        log("")
        log("=== STEP 1 — rebuild rollup with three-valued precedence (TRUE > FALSE > NULL) ===")
        con.execute(f"""
            CREATE OR REPLACE TABLE {ROLLUP_TABLE} AS
            SELECT
              research_id,
              COUNT(*) AS tirads_v2_n_reports,
              CASE
                WHEN MAX(CASE WHEN suspicious_ln_present = TRUE  THEN 1 ELSE 0 END) = 1 THEN TRUE
                WHEN MAX(CASE WHEN suspicious_ln_present = FALSE THEN 1 ELSE 0 END) = 1 THEN FALSE
                ELSE NULL
              END                                                                       AS tirads_v2_any_suspicious_ln_on_us,
              MAX(CASE WHEN overall_recommendation = 'fna' THEN 1 ELSE 0 END)::BOOLEAN  AS tirads_v2_any_fna_recommended_report,
              MIN(follow_up_interval_months)                                            AS tirads_v2_shortest_followup_months
            FROM {RAW_TABLE}
            GROUP BY research_id
        """)
        n_rl_rows, n_rl_rids = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {ROLLUP_TABLE}"
        ).fetchone()
        log(f"  rebuilt {ROLLUP_TABLE}: {n_rl_rows:,} rows / {n_rl_rids:,} RIDs")
        if n_rl_rows != n_rl_rids:
            raise SystemExit(f"rollup invariant violated: rows={n_rl_rows} rids={n_rl_rids}")

        rl_dist = con.execute(f"""
            SELECT tirads_v2_any_suspicious_ln_on_us, COUNT(*)
            FROM {ROLLUP_TABLE}
            GROUP BY 1
            ORDER BY 1 NULLS FIRST
        """).fetchall()
        log(f"  rollup distribution after patch:")
        for v, n in rl_dist:
            label = "NULL" if v is None else str(bool(v)).upper()
            log(f"    {label:<5s}  n={n:,}")
        decisions["rollup_dist"] = [
            {"flag": ("NULL" if v is None else bool(v)), "n": int(n)} for v, n in rl_dist
        ]

        # Step 2: refresh CPM column
        log("")
        log("=== STEP 2 — refresh canonical_patient_master.tirads_v2_any_suspicious_ln_on_us ===")
        con.execute(f"UPDATE canonical_patient_master SET {CPM_COL} = NULL")
        con.execute(f"""
            UPDATE canonical_patient_master AS m
            SET {CPM_COL} = r.{CPM_COL}
            FROM {ROLLUP_TABLE} AS r
            WHERE m.research_id = r.research_id
        """)

        post_dist = con.execute(f"""
            SELECT {CPM_COL}, COUNT(*)
            FROM canonical_patient_master
            GROUP BY 1
            ORDER BY 1 NULLS FIRST
        """).fetchall()
        log(f"  CPM distribution after refresh (all patients):")
        for v, n in post_dist:
            label = "NULL" if v is None else str(bool(v)).upper()
            log(f"    {label:<5s}  n={n:,}")

        post_with_reports = con.execute(f"""
            SELECT
              COUNT(*) FILTER (WHERE {CPM_COL} = TRUE)  AS n_true,
              COUNT(*) FILTER (WHERE {CPM_COL} = FALSE) AS n_false,
              COUNT(*) FILTER (WHERE {CPM_COL} IS NULL) AS n_null,
              COUNT(*)                                  AS total
            FROM canonical_patient_master
            WHERE tirads_v2_n_reports IS NOT NULL
        """).fetchone()
        decisions["post_patch_with_reports"] = {
            "n_true":  int(post_with_reports[0]),
            "n_false": int(post_with_reports[1]),
            "n_null":  int(post_with_reports[2]),
            "total":   int(post_with_reports[3]),
        }
        log("")
        log(f"  Patients with reports — POST-PATCH:")
        log(f"    TRUE  = {post_with_reports[0]:,}   (was {pre_with_reports[0]:,})")
        log(f"    FALSE = {post_with_reports[1]:,}   (was {pre_with_reports[1]:,})")
        log(f"    NULL  = {post_with_reports[2]:,}   (was {pre_with_reports[2]:,})")
        log(f"    total = {post_with_reports[3]:,}   (should remain 4,073)")
        delta_true = post_with_reports[0] - pre_with_reports[0]
        delta_false = post_with_reports[1] - pre_with_reports[1]
        delta_null = post_with_reports[2] - pre_with_reports[2]
        log(f"    deltas: TRUE {delta_true:+,d}  FALSE {delta_false:+,d}  NULL {delta_null:+,d}")
        decisions["deltas"] = {
            "true": delta_true, "false": delta_false, "null": delta_null,
        }

        # Step 3: bump cpm_built_at + provenance row
        log("")
        log("=== STEP 3 — bump cpm_built_at and insert provenance ===")
        if con.execute(
            "SELECT 1 FROM information_schema.columns "
            f"WHERE table_catalog='{CANONICAL_DB}' AND table_schema='main' "
            "AND table_name='canonical_patient_master' AND column_name='cpm_built_at'"
        ).fetchone():
            con.execute(f"""
                UPDATE canonical_patient_master AS m
                SET cpm_built_at = CURRENT_TIMESTAMP
                WHERE m.research_id IN (SELECT research_id FROM {ROLLUP_TABLE})
            """)
            log("  cpm_built_at stamped on rollup-touched RIDs.")
        try:
            con.execute(
                "INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1 "
                "(run_id, started_at, ended_at, phases_applied, "
                " critical_findings_cleared, high_findings_cleared, "
                " med_findings_cleared, held_for_adjudication) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    f"tirads_v2_ln_rollup_threevalue_patch_{RUN_DATE.replace('-', '')}",
                    RUN_TS_ISO,
                    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "tirads_v2_ln_rollup:three_valued_TRUE_gt_FALSE_gt_NULL",
                    "0",
                    "0",
                    str(post_with_reports[2]),  # NULLs surfaced as 'med' findings cleared
                    "0",
                ],
            )
            log("  provenance row inserted into cpm_reconciliation_provenance_v1")
        except Exception as e:
            log(f"  provenance insert skipped: {e!r}")

        # Step 4: COMMENT on CPM column
        log("")
        log("=== STEP 4 — refresh COMMENT ON COLUMN ===")
        try:
            safe = CPM_COMMENT.replace("'", "''")
            con.execute(f"COMMENT ON COLUMN canonical_patient_master.{CPM_COL} IS '{safe}'")
            log(f"  COMMENT applied to canonical_patient_master.{CPM_COL}")
        except Exception as e:
            log(f"  COMMENT skipped: {e!r}")

        # Step 5: Phase 6 invariants
        log("")
        log("=== STEP 5 — Phase 6 invariants ===")
        n_cpm, n_dist, n_null = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT research_id), "
            "COUNT(*) FILTER (WHERE research_id IS NULL) "
            "FROM canonical_patient_master"
        ).fetchone()
        log(f"  CPM rows={n_cpm:,}  distinct={n_dist:,}  null_rid={n_null}")
        if n_cpm != EXPECTED_CPM_ROWS or n_dist != EXPECTED_CPM_ROWS or n_null != 0:
            raise SystemExit(
                f"CPM invariants violated: rows={n_cpm} distinct={n_dist} null_rid={n_null}"
            )
        decisions["invariants"] = {
            "cpm_rows": int(n_cpm),
            "cpm_distinct_rids": int(n_dist),
            "null_rid": int(n_null),
            "ok": True,
        }
        log("  ✓ invariants pass")

        return 0
    finally:
        DECISIONS_PATH.write_text(json.dumps(decisions, indent=2, default=str))
        LOG_PATH.write_text("\n".join(_log_buf) + "\n")
        try:
            con.close()
        except Exception:
            pass
        log(f"decisions → {DECISIONS_PATH}")
        log(f"log       → {LOG_PATH}")


if __name__ == "__main__":
    sys.exit(main())
