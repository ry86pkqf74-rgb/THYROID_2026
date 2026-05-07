#!/usr/bin/env python3
"""Apply mig_334 — F6/F7/F8/F9 follow-up rollups.

Usage:
  .venv/bin/python qc_framework_v1/migrations/apply_mig334.py --dry-run
  .venv/bin/python qc_framework_v1/migrations/apply_mig334.py --apply
"""
from __future__ import annotations

import argparse
import datetime
import subprocess
import time
from pathlib import Path

PROJECT = "thyroid-canonical-pub-2026"
CPM = f"`{PROJECT}.pub_canonical.canonical_patient_master`"


def log(msg: str) -> None:
    ts = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def bq(sql: str, dry_run: bool = False) -> None:
    if dry_run:
        log(f"  DRY: {sql[:130]}")
        return
    cmd = ["bq", "query", "--nouse_legacy_sql", sql]
    log(f"  BQ: {sql[:120]}")
    rc = subprocess.call(cmd)
    if rc != 0:
        raise SystemExit(f"BQ failed (rc={rc})")


def main() -> None:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--dry-run", action="store_true")
    grp.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    dry = args.dry_run

    # ── Step 1: ADD columns (only those not yet added by mig_331c) ──────────
    log("Step 1: ADD new columns to CPM")
    add_stmts = [
        f"ALTER TABLE {CPM} ADD COLUMN IF NOT EXISTS `op_nlp_op_time_min` FLOAT64",
        f"ALTER TABLE {CPM} ADD COLUMN IF NOT EXISTS `op_nlp_los_days` FLOAT64",
        # these were added by mig_331c but IF NOT EXISTS makes them safe
        f"ALTER TABLE {CPM} ADD COLUMN IF NOT EXISTS `op_nlp_ligasure_used` BOOL",
        f"ALTER TABLE {CPM} ADD COLUMN IF NOT EXISTS `op_nlp_harmonic_used` BOOL",
        f"ALTER TABLE {CPM} ADD COLUMN IF NOT EXISTS `op_nlp_energy_device_other_used` BOOL",
        f"ALTER TABLE {CPM} ADD COLUMN IF NOT EXISTS `op_nlp_suture_ligation_only` BOOL",
        f"ALTER TABLE {CPM} ADD COLUMN IF NOT EXISTS `cpm_ligasure_used` BOOL",
        f"ALTER TABLE {CPM} ADD COLUMN IF NOT EXISTS `cpm_ligasure_source` STRING",
        f"ALTER TABLE {CPM} ADD COLUMN IF NOT EXISTS `cpm_op_time_min_source` STRING",
        f"ALTER TABLE {CPM} ADD COLUMN IF NOT EXISTS `cpm_los_days_source` STRING",
    ]
    for stmt in add_stmts:
        bq(stmt, dry_run=dry)
        if not dry:
            time.sleep(2)  # avoid ALTER TABLE rate limit (5 per 5s)

    if not dry:
        log("Waiting 5s for schema propagation...")
        time.sleep(5)

    # ── Step 2: F8 drain rollup fix ─────────────────────────────────────────
    log("Step 2: F8 — update op_drain_placed_any (OR-fold NLP + NSQIP)")
    bq(f"""
UPDATE {CPM} cpm
SET op_drain_placed_any = (
  COALESCE(op_drain_placed_any, FALSE)
  OR COALESCE(op_nlp_drain_placed, FALSE)
  OR LOWER(IFNULL(nsqip_drain_usage, '')) = 'yes'
)
WHERE (op_drain_placed_any IS NOT TRUE)
  AND (op_nlp_drain_placed IS TRUE
       OR LOWER(IFNULL(nsqip_drain_usage,'')) = 'yes')
""", dry_run=dry)

    # ── Step 3: F6 — cpm_op_time_min NLP fallback ───────────────────────────
    log("Step 3: F6 — cpm_op_time_min: backfill from op_nlp_op_time_min")
    bq(f"""
UPDATE {CPM}
SET
  cpm_op_time_min = op_nlp_op_time_min,
  cpm_op_time_min_source = 'op_nlp_op_time_min'
WHERE cpm_op_time_min IS NULL
  AND op_nlp_op_time_min IS NOT NULL
""", dry_run=dry)

    # ── Step 4: F7 — cpm_los_days NLP fallback ──────────────────────────────
    log("Step 4: F7 — cpm_los_days: backfill from op_nlp_los_days")
    bq(f"""
UPDATE {CPM}
SET
  cpm_los_days = op_nlp_los_days,
  cpm_los_days_source = 'op_nlp_los_days'
WHERE cpm_los_days IS NULL
  AND op_nlp_los_days IS NOT NULL
""", dry_run=dry)

    # ── Step 5: F9 — cpm_ligasure_used combined rollup ──────────────────────
    log("Step 5: F9 — cpm_ligasure_used combined rollup (NLP + NSQIP)")
    bq(f"""
UPDATE {CPM}
SET
  cpm_ligasure_used = (
    COALESCE(op_nlp_ligasure_used, FALSE)
    OR (
      LOWER(IFNULL(nsqip_vessel_sealant, '')) = 'yes'
      AND COALESCE(op_nlp_harmonic_used, FALSE) IS FALSE
    )
  ),
  cpm_ligasure_source = CASE
    WHEN op_nlp_ligasure_used IS TRUE
         AND LOWER(IFNULL(nsqip_vessel_sealant,'')) = 'yes'
      THEN 'op_nlp_v23_and_nsqip'
    WHEN op_nlp_ligasure_used IS TRUE
      THEN 'op_nlp_v23_only'
    WHEN op_nlp_harmonic_used IS TRUE
      THEN 'op_nlp_v23_harmonic_not_ligasure'
    WHEN LOWER(IFNULL(nsqip_vessel_sealant,'')) = 'yes'
      THEN 'nsqip_inferred_ligasure'
    WHEN op_nlp_suture_ligation_only IS TRUE
      THEN 'op_nlp_v23_suture_only'
    ELSE NULL
  END
WHERE op_nlp_ligasure_used IS NOT NULL
   OR op_nlp_harmonic_used IS NOT NULL
   OR nsqip_vessel_sealant IS NOT NULL
   OR op_nlp_suture_ligation_only IS NOT NULL
""", dry_run=dry)

    # ── Step 6: Verify ───────────────────────────────────────────────────────
    log("Step 6: Verification")
    bq(f"""
SELECT
  COUNTIF(op_drain_placed_any)              AS n_drain_combined,
  COUNTIF(cpm_op_time_min IS NOT NULL)      AS n_op_time_combined,
  COUNTIF(cpm_los_days IS NOT NULL)         AS n_los_combined,
  COUNTIF(op_nlp_ligasure_used IS TRUE)     AS n_ligasure_nlp,
  COUNTIF(op_nlp_harmonic_used IS TRUE)     AS n_harmonic_nlp,
  COUNTIF(cpm_ligasure_used IS TRUE)        AS n_ligasure_combined
FROM {CPM}
""", dry_run=dry)

    log("=== mig_334 apply_mig334.py COMPLETE ===")


if __name__ == "__main__":
    main()
