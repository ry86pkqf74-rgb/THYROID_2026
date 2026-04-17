#!/usr/bin/env python3
"""
Script 252 — Recompute max_tirads_ever / imaging_tirads_worst / preop_tirads_best

v1_1 finalization pass (audit §1.1, 1,503 patients affected).

Source of truth: main.canonical_us_nodule_characteristics_v1 (built by Script
246), per-(exam, nodule) grain. The authoritative TIRADS value per row is
GREATEST(tirads_reported, tirads_acr_recalculated). The legacy CPM rollup
ignored tirads_acr_recalculated and read only tirads_reported, undercounting
TR5 in 1,503 patients.

This script:
  1. snapshots canonical_patient_master to archive_pub_v1_0
  2. UPDATEs three rollup columns from the authoritative drill-down:
       max_tirads_ever       = MAX(GREATEST(reported, recalc))   per patient
       imaging_tirads_worst  = MAX(GREATEST(reported, recalc))   per patient
       preop_tirads_best     = MIN(GREATEST(reported, recalc))   per patient,
                               restricted to exam_date < first_surgery_date
                               (when both available)
  3. Re-annotates the legacy `us_nodules_tirads.tirads_worst_category_v12`
     row in data_dictionary_v240 as status='legacy', replacement='max_tirads_ever'.
  4. Self-verifies that the §1.1 replay query returns 0.
  5. Records before/after counts in manuscript_workspace.v1_1_finalization_audit_v1.

Invariants (asserted at end):
  - canonical_patient_master rows == 10,871
  - audit §1.1 replay COUNT(*) == 0

Default mode is --dry-run; pass --apply to mutate.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    AUDIT_TABLE,
    ensure_audit_table, ensure_archive_schema, make_logger,
    record_audit, snapshot_table, utc_ts, write_decision_log,
)

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
RUN_LOG = OUTPUT_DIR / "252_run.log"
DECISION_LOG = OUTPUT_DIR / "252_decision_log.json"
SCRIPT_TAG = "Script 252"
SCRIPT_NUM = "252"
RUN_DATE = "2026-04-16"
CPM_FQ = f'{PUBLICATION_DB}.main.canonical_patient_master'
DETAIL_FQ = f'{PUBLICATION_DB}.main.canonical_us_nodule_characteristics_v1'

# ---------------------------------------------------------------------------
# Replay query for audit §1.1: number of CPM rows where max_tirads_ever
# under-reports the per-patient detail max.
# ---------------------------------------------------------------------------
REPLAY_SQL = f"""
WITH detail AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid,
         GREATEST(COALESCE(MAX(tirads_reported), 0),
                  COALESCE(MAX(tirads_acr_recalculated), 0)) AS detail_max
  FROM {DETAIL_FQ}
  WHERE research_id IS NOT NULL
  GROUP BY 1
)
SELECT COUNT(*)
FROM {CPM_FQ} cpm
JOIN detail d ON TRY_CAST(cpm.research_id AS INTEGER) = d.rid
WHERE d.detail_max > COALESCE(cpm.max_tirads_ever, 0)
"""


def replay_count(con) -> int:
    return int(con.execute(REPLAY_SQL).fetchone()[0])


def update_rollups(con, log) -> dict:
    """
    Update the three rollups in a single CTE-driven pass.

    GREATEST(reported, recalc) is computed at the row grain so a row with
    only one populated value still contributes its non-null value.
    """
    # Use CASE WHEN for greatest of two nullable columns (DuckDB GREATEST
    # treats NULL as bigger in some versions; safer to handle explicitly).
    detail_cte = f"""
    WITH detail_row AS (
      SELECT TRY_CAST(research_id AS INTEGER) AS rid,
             exam_date,
             CASE
               WHEN tirads_reported IS NULL AND tirads_acr_recalculated IS NULL THEN NULL
               WHEN tirads_reported IS NULL THEN tirads_acr_recalculated
               WHEN tirads_acr_recalculated IS NULL THEN tirads_reported
               WHEN tirads_acr_recalculated > tirads_reported THEN tirads_acr_recalculated
               ELSE tirads_reported
             END AS tr_value
      FROM {DETAIL_FQ}
      WHERE research_id IS NOT NULL
    ),
    per_pt AS (
      SELECT rid,
             MAX(tr_value) AS max_tirads,
             MIN(tr_value) AS min_tirads
      FROM detail_row
      WHERE tr_value IS NOT NULL
      GROUP BY rid
    )
    """

    # 1. max_tirads_ever + imaging_tirads_worst (= worst-ever)
    con.execute(f"""
        {detail_cte}
        UPDATE {CPM_FQ} AS cpm
        SET max_tirads_ever      = p.max_tirads,
            imaging_tirads_worst = p.max_tirads
        FROM per_pt AS p
        WHERE TRY_CAST(cpm.research_id AS INTEGER) = p.rid
    """)
    n_rows_max = con.execute(f"""
        SELECT COUNT(*) FROM {CPM_FQ}
        WHERE max_tirads_ever IS NOT NULL
    """).fetchone()[0]
    log(f"  max_tirads_ever populated rows: {n_rows_max}")

    # 2. preop_tirads_best — restrict to exam_date < first_surgery_date
    # (fall back to all-time best when first_surgery_date is null)
    con.execute(f"""
        WITH detail_row AS (
          SELECT TRY_CAST(d.research_id AS INTEGER) AS rid,
                 d.exam_date,
                 CASE
                   WHEN d.tirads_reported IS NULL AND d.tirads_acr_recalculated IS NULL THEN NULL
                   WHEN d.tirads_reported IS NULL THEN d.tirads_acr_recalculated
                   WHEN d.tirads_acr_recalculated IS NULL THEN d.tirads_reported
                   WHEN d.tirads_acr_recalculated > d.tirads_reported THEN d.tirads_acr_recalculated
                   ELSE d.tirads_reported
                 END AS tr_value
          FROM {DETAIL_FQ} d
          WHERE d.research_id IS NOT NULL
        ),
        cpm_dates AS (
          SELECT TRY_CAST(research_id AS INTEGER) AS rid,
                 first_surgery_date::DATE AS first_surg_d
          FROM {CPM_FQ}
        ),
        preop AS (
          SELECT dr.rid, MIN(dr.tr_value) AS min_pre
          FROM detail_row dr
          LEFT JOIN cpm_dates cd ON cd.rid = dr.rid
          WHERE dr.tr_value IS NOT NULL
            AND (cd.first_surg_d IS NULL OR dr.exam_date IS NULL
                 OR dr.exam_date < cd.first_surg_d)
          GROUP BY dr.rid
        )
        UPDATE {CPM_FQ} AS cpm
        SET preop_tirads_best = p.min_pre
        FROM preop AS p
        WHERE TRY_CAST(cpm.research_id AS INTEGER) = p.rid
    """)
    n_rows_preop = con.execute(f"""
        SELECT COUNT(*) FROM {CPM_FQ}
        WHERE preop_tirads_best IS NOT NULL
    """).fetchone()[0]
    log(f"  preop_tirads_best populated rows: {n_rows_preop}")

    return {"max_populated": n_rows_max, "preop_populated": n_rows_preop}


def annotate_legacy_v12(con, log) -> int:
    """
    Re-annotate `tirads_worst_category_v12` in data_dictionary_v240 as
    status='legacy' with replacement='max_tirads_ever'. Preserve the column
    in CPM (it stays in the wide-format us_nodules_tirads workbook).
    """
    n = con.execute(f"""
        SELECT COUNT(*) FROM {PUBLICATION_DB}.main.data_dictionary_v240
        WHERE column_name = 'tirads_worst_category_v12'
    """).fetchone()[0]
    if n == 0:
        log("  data_dictionary_v240 has no row for tirads_worst_category_v12 (skip)")
        return 0
    con.execute(f"""
        UPDATE {PUBLICATION_DB}.main.data_dictionary_v240
        SET status = 'legacy',
            replacement_column_name = 'max_tirads_ever'
        WHERE column_name = 'tirads_worst_category_v12'
    """)
    log("  re-annotated tirads_worst_category_v12 -> status=legacy, "
        "replacement=max_tirads_ever")
    return 1


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes; default is dry-run.")
    args = ap.parse_args()
    do_writes = bool(args.apply)
    mode = "APPLY" if do_writes else "DRY-RUN"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log, fh = make_logger(RUN_LOG)
    t0 = time.time()
    log("=" * 78)
    log(f"=== START {Path(__file__).name}  mode={mode}")
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")
    run_ts = utc_ts()

    decision: dict = {
        "script": SCRIPT_NUM, "run_ts": run_ts, "run_date": RUN_DATE,
        "mode": mode, "phases": {},
    }

    try:
        # ---- preflight ----
        n_cpm = con.execute(f"SELECT COUNT(*) FROM {CPM_FQ}").fetchone()[0]
        if n_cpm != 10871:
            raise RuntimeError(f"CPM rows {n_cpm} != 10871; aborting")
        before = replay_count(con)
        log(f"PREFLIGHT  CPM rows={n_cpm}  audit_§1.1 replay BEFORE: {before}")
        decision["phases"]["preflight"] = {"cpm_rows": n_cpm,
                                           "replay_before": before}

        if not do_writes:
            log("DRY-RUN — no mutations performed")
            log("=" * 78)
            log(f"=== END  elapsed={time.time()-t0:.1f}s")
            write_decision_log(DECISION_LOG, decision)
            fh.close()
            return

        # ---- snapshot CPM ----
        ensure_archive_schema(con)
        snap_name = f"canonical_patient_master_pre252_{run_ts}"
        snap_full = snapshot_table(
            con, CPM_FQ, snap_name, SCRIPT_TAG,
            "Pre-rebuild snapshot for max_tirads_ever / imaging_tirads_worst / "
            "preop_tirads_best from canonical_us_nodule_characteristics_v1.",
        )
        log(f"SNAPSHOT  {snap_full}")
        decision["phases"]["snapshot"] = snap_full

        # ---- ensure audit table ----
        ensure_audit_table(con)
        log(f"audit table ready: {AUDIT_TABLE}")

        # ---- mutate ----
        log("MUTATE  rolling up max_tirads_ever, imaging_tirads_worst, preop_tirads_best")
        upd = update_rollups(con, log)
        decision["phases"]["update"] = upd

        # ---- legacy dictionary annotation ----
        log("DICTIONARY  re-annotating tirads_worst_category_v12 as legacy")
        n_dict = annotate_legacy_v12(con, log)
        decision["phases"]["dict_annotated"] = n_dict

        # ---- self-verify ----
        after = replay_count(con)
        log(f"VERIFY  audit_§1.1 replay AFTER: {after} (target=0)")
        decision["phases"]["replay_after"] = after

        record_audit(
            con, SCRIPT_NUM, "audit_1_1",
            "max_tirads_ever_undercount",
            count_before=before, count_after=after, target_after=0,
            status="OK" if after == 0 else "FAIL",
            notes=f"snapshot={snap_full}",
        )

        if after != 0:
            raise RuntimeError(
                f"Self-verify FAILED: §1.1 replay returned {after} (expected 0)"
            )

        # ---- post invariants ----
        n_after = con.execute(f"SELECT COUNT(*) FROM {CPM_FQ}").fetchone()[0]
        if n_after != 10871:
            raise RuntimeError(f"CPM row count drifted: {n_after} != 10871")
        log(f"INVARIANT  CPM rows still = {n_after}")

        log("ALL ASSERTIONS PASS")
    except Exception as exc:
        log(f"FATAL: {exc!r}")
        decision["error"] = str(exc)
        write_decision_log(DECISION_LOG, decision)
        fh.close()
        raise

    write_decision_log(DECISION_LOG, decision)
    log("=" * 78)
    log(f"=== END  elapsed={time.time()-t0:.1f}s")
    fh.close()


if __name__ == "__main__":
    main()
