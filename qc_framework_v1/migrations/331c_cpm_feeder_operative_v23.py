#!/usr/bin/env python3
"""mig_331c — Phase 4 CPM feeder: propagate OED v2.3 NLP columns to CPM.

ADD and UPDATE canonical_patient_master columns from operative_episode_detail_v2
(the view backed by pub_legacy_source_20260416).  This is the patient-level
aggregation step that was previously handled inline by older pipeline scripts.

New CPM columns added and populated:
  proc_nlp_central_neck_dissection  BOOL
  op_nlp_ligasure_used              BOOL
  op_nlp_harmonic_used              BOOL
  op_nlp_rln_signal_status          STRING
  op_nlp_trach_concurrent           BOOL

Existing CPM columns refreshed from v2.3 OED:
  op_nlp_nerve_monitoring_used      (picks up ~58 new v2.3 positives)
  proc_nlp_lateral_neck_dissection  (picks up new lateral dissection NLP)

Usage:
  .venv/bin/python qc_framework_v1/migrations/331c_cpm_feeder_operative_v23.py --dry-run
  .venv/bin/python qc_framework_v1/migrations/331c_cpm_feeder_operative_v23.py --apply
"""
from __future__ import annotations

import argparse
import datetime
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

PROJECT = "thyroid-canonical-pub-2026"
DATASET = "pub_canonical"
CPM = f"`{PROJECT}.{DATASET}.canonical_patient_master`"
OED = f"`{PROJECT}.{DATASET}.operative_episode_detail_v2`"


def log(msg: str) -> None:
    ts = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def bq_query(sql: str, dry_run: bool = False) -> None:
    if dry_run:
        log(f"  DRY-RUN: {sql[:140]}")
        return
    cmd = ["bq", "query", "--nouse_legacy_sql", sql]
    log(f"  BQ: {sql[:120]}")
    rc = subprocess.call(cmd)
    if rc != 0:
        raise SystemExit(f"BQ query failed (rc={rc})")


def get_cpm_columns() -> set[str]:
    import json
    result = subprocess.run(
        ["bq", "show", "--schema", f"{PROJECT}:{DATASET}.canonical_patient_master"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return set()
    try:
        return {c["name"] for c in json.loads(result.stdout)}
    except Exception:
        return set()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--dry-run", action="store_true")
    grp.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    dry = args.dry_run

    existing = get_cpm_columns()
    log(f"CPM existing cols: {len(existing)}")

    # ── Step 1: ADD new columns ─────────────────────────────────────────────
    new_bool_cols = {
        "proc_nlp_central_neck_dissection": "BOOL",
        "op_nlp_ligasure_used": "BOOL",
        "op_nlp_harmonic_used": "BOOL",
        "op_nlp_energy_device_other_used": "BOOL",
        "op_nlp_suture_ligation_only": "BOOL",
        "op_nlp_trach_concurrent": "BOOL",
    }
    new_str_cols = {
        "op_nlp_rln_signal_status": "STRING",
    }
    all_new = {**new_bool_cols, **new_str_cols}

    for col, dtype in all_new.items():
        if col not in existing:
            sql = f"ALTER TABLE {CPM} ADD COLUMN IF NOT EXISTS `{col}` {dtype}"
            bq_query(sql, dry_run=dry)
        else:
            log(f"  {col} already exists — skip ADD")

    if not dry:
        import time; time.sleep(3)

    # ── Step 2: CREATE patient-level OED aggregation temp view ─────────────
    # (BQ doesn't support WITH ... UPDATE in one statement; use MERGE instead)
    agg_sql = f"""
MERGE {CPM} cpm
USING (
  SELECT
    CAST(research_id AS INT64)                        AS research_id,
    LOGICAL_OR(rln_monitoring_flag)                   AS rln_monitoring_any,
    LOGICAL_OR(central_neck_dissection_flag)          AS central_nd_any,
    LOGICAL_OR(lateral_neck_dissection_flag)          AS lateral_nd_any,
    LOGICAL_OR(ligasure_used_nlp)                     AS ligasure_nlp_any,
    LOGICAL_OR(harmonic_used_nlp)                     AS harmonic_nlp_any,
    LOGICAL_OR(energy_device_other_used_nlp)          AS energy_other_any,
    LOGICAL_OR(suture_ligation_only_nlp)              AS suture_only_any,
    MAX(trach_concurrent_evidence)                    AS trach_conc,
    LOGICAL_OR(trach_concurrent_evidence IS NOT NULL
               AND trach_concurrent_evidence != '')   AS trach_conc_any,
    MAX(rln_signal_status_nlp)                        AS rln_signal
  FROM {OED}
  WHERE research_id IS NOT NULL
  GROUP BY 1
) oed
ON CAST(cpm.research_id AS INT64) = oed.research_id
WHEN MATCHED THEN UPDATE SET
  cpm.op_nlp_nerve_monitoring_used              = COALESCE(oed.rln_monitoring_any, cpm.op_nlp_nerve_monitoring_used),
  cpm.proc_nlp_central_neck_dissection          = COALESCE(oed.central_nd_any, FALSE),
  cpm.proc_nlp_lateral_neck_dissection          = COALESCE(oed.lateral_nd_any, cpm.proc_nlp_lateral_neck_dissection),
  cpm.op_nlp_ligasure_used                      = COALESCE(oed.ligasure_nlp_any, FALSE),
  cpm.op_nlp_harmonic_used                      = COALESCE(oed.harmonic_nlp_any, FALSE),
  cpm.op_nlp_energy_device_other_used           = COALESCE(oed.energy_other_any, FALSE),
  cpm.op_nlp_suture_ligation_only               = COALESCE(oed.suture_only_any, FALSE),
  cpm.op_nlp_trach_concurrent                   = COALESCE(oed.trach_conc_any, FALSE),
  cpm.op_nlp_rln_signal_status                  = COALESCE(oed.rln_signal, cpm.op_nlp_rln_signal_status)
"""
    log("Step 2: MERGE CPM from OED patient-level aggregation")
    bq_query(agg_sql, dry_run=dry)

    # ── Step 3: Verify ──────────────────────────────────────────────────────
    if not dry:
        log("Step 3: Post-feeder verification")
        bq_query(f"""
SELECT
  COUNTIF(op_nlp_nerve_monitoring_used IS TRUE)     AS n_nerve_monitoring,
  COUNTIF(proc_nlp_central_neck_dissection IS TRUE) AS n_central,
  COUNTIF(proc_nlp_lateral_neck_dissection IS TRUE) AS n_lateral,
  COUNTIF(op_nlp_ligasure_used IS TRUE)             AS n_ligasure,
  COUNTIF(op_nlp_harmonic_used IS TRUE)             AS n_harmonic,
  COUNTIF(op_nlp_rln_signal_status IS NOT NULL)     AS n_rln_signal
FROM {CPM}
""", dry_run=False)

    log("=== mig_331c COMPLETE ===")


if __name__ == "__main__":
    main()
