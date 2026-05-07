#!/usr/bin/env python3
"""mig_331b — BQ sync for operative NLP v2.3 changes (THY-28).

Two operations:
  1. Replace pub_canonical.note_entities_operative_detail with v2.3 parquet
     (adds ligasure/harmonic/op_time/length_of_stay entity types).
  2. ADD new columns to pub_canonical.operative_episode_detail_v2 and populate
     them from the local DuckDB export parquet.

Usage:
  .venv/bin/python qc_framework_v1/migrations/331b_bq_sync_operative_v23.py --dry-run
  .venv/bin/python qc_framework_v1/migrations/331b_bq_sync_operative_v23.py --apply
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

PROJECT = "thyroid-canonical-pub-2026"
DATASET = "pub_canonical"
BASE_DATASET = "pub_legacy_source_20260416"  # backing table for pub_canonical views
LOCATION = "us-central1"

NOTE_ENT_PARQUET = _REPO / "processed" / "note_entities_operative_detail.parquet"
OED_UPDATE_PARQUET = _REPO / "exports" / "bq_sync_mig331" / "oed_v23_update_cols.parquet"

NEW_BOOL_COLS = [
    "central_neck_dissection_flag",
    "lateral_neck_dissection_flag",
    "rln_signal_status_nlp",
    "trach_concurrent_evidence",
    "trach_nonperioperative_evidence",
    "op_time_nlp_present",
    "los_nlp_present",
    "ligasure_used_nlp",
    "harmonic_used_nlp",
    "energy_device_other_used_nlp",
    "suture_ligation_only_nlp",
]


def log(msg: str) -> None:
    import datetime
    ts = datetime.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def bq_query(sql: str, dry_run: bool = False) -> None:
    if dry_run:
        log(f"  DRY-RUN SQL: {sql[:120]}")
        return
    cmd = ["bq", "query", "--nouse_legacy_sql", sql]
    log(f"  BQ: {sql[:100]}")
    rc = subprocess.call(cmd)
    if rc != 0:
        raise SystemExit(f"BQ query failed (rc={rc}): {sql[:200]}")


def bq_load(table: str, parquet: Path, replace: bool = True, dry_run: bool = False) -> None:
    dest = f"{PROJECT}:{DATASET}.{table}"
    cmd = [
        "bq",
        f"--location={LOCATION}",
        "load",
        "--replace" if replace else "--noreplace",
        "--source_format=PARQUET",
        dest,
        str(parquet.resolve()),
    ]
    log(f"  {'DRY-RUN ' if dry_run else ''}bq load {dest} from {parquet.name}")
    if not dry_run:
        rc = subprocess.call(cmd)
        if rc != 0:
            raise SystemExit(f"bq load failed (rc={rc})")


def get_bq_columns(table: str, dataset: str = DATASET) -> set[str]:
    """Return set of column names in a BQ table."""
    import json
    result = subprocess.run(
        ["bq", "show", "--schema", f"{PROJECT}:{dataset}.{table}"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        log(f"  WARNING: could not get schema for {table}: {result.stderr}")
        return set()
    try:
        return {c["name"] for c in json.loads(result.stdout)}
    except Exception:
        return set()


def add_missing_bq_cols(table: str, dry_run: bool) -> None:
    """ADD any OED columns missing from the BQ base table."""
    existing = get_bq_columns(table, dataset=BASE_DATASET)
    col_defs = {
        "rln_signal_status_nlp": "STRING",
        "trach_concurrent_evidence": "STRING",
        "trach_nonperioperative_evidence": "STRING",
        "op_time_nlp_present": "BOOL",
        "los_nlp_present": "BOOL",
        "ligasure_used_nlp": "BOOL",
        "harmonic_used_nlp": "BOOL",
        "energy_device_other_used_nlp": "BOOL",
        "suture_ligation_only_nlp": "BOOL",
    }
    for col, dtype in col_defs.items():
        if col not in existing:
            sql = (
                f"ALTER TABLE `{PROJECT}.{BASE_DATASET}.operative_episode_detail_v2` "
                f"ADD COLUMN IF NOT EXISTS `{col}` {dtype}"
            )
            bq_query(sql, dry_run=dry_run)
        else:
            log(f"  Column {col} already exists — skip ADD")


def update_oed_from_parquet(dry_run: bool) -> None:
    """Load OED update parquet to temp BQ table then UPDATE main table."""
    from google.cloud import bigquery  # type: ignore

    client = bigquery.Client(project=PROJECT)
    temp_table = f"{PROJECT}.{DATASET}._oed_v23_update_tmp"

    if not dry_run:
        import pandas as pd
        log("  Loading OED update parquet to BQ temp table...")
        df = pd.read_parquet(OED_UPDATE_PARQUET)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )
        job = client.load_table_from_dataframe(
            df, temp_table, job_config=job_config
        )
        job.result()
        log(f"  Temp table loaded: {len(df)} rows")

    oed = f"`{PROJECT}.{BASE_DATASET}.operative_episode_detail_v2`"
    tmp = f"`{temp_table}`"

    # Update all new NLP columns — cast string columns explicitly since NULL values
    # in local DuckDB parquet lose their type hint
    string_cols = {"rln_signal_status_nlp", "trach_concurrent_evidence", "trach_nonperioperative_evidence"}
    bool_cols = {
        "central_neck_dissection_flag", "lateral_neck_dissection_flag",
        "op_time_nlp_present", "los_nlp_present", "ligasure_used_nlp",
        "harmonic_used_nlp", "energy_device_other_used_nlp", "suture_ligation_only_nlp",
    }
    update_cols = [
        "central_neck_dissection_flag",
        "lateral_neck_dissection_flag",
        "rln_signal_status_nlp",
        "trach_concurrent_evidence",
        "trach_nonperioperative_evidence",
        "op_time_nlp_present",
        "los_nlp_present",
        "ligasure_used_nlp",
        "harmonic_used_nlp",
        "energy_device_other_used_nlp",
        "suture_ligation_only_nlp",
    ]

    def col_expr(col: str) -> str:
        if col in string_cols:
            return f"CAST(s.{col} AS STRING)"
        elif col in bool_cols:
            return f"CAST(s.{col} AS BOOL)"
        return f"s.{col}"

    set_clause = ",\n      ".join(
        f"t.{col} = {col_expr(col)}" for col in update_cols
    )
    update_sql = f"""
UPDATE {oed} t
SET
  {set_clause}
FROM {tmp} s
WHERE CAST(t.research_id AS INT64) = CAST(s.research_id AS INT64)
  AND t.surgery_episode_id = s.surgery_episode_id
"""
    bq_query(update_sql, dry_run=dry_run)

    if not dry_run:
        # Drop temp table
        client.delete_table(temp_table, not_found_ok=True)
        log("  Temp table dropped")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--skip-note-entities", action="store_true",
                    help="Skip note_entities replace (if already done)")
    args = ap.parse_args()

    if not args.dry_run and not args.apply:
        ap.print_help()
        raise SystemExit("Pass --dry-run or --apply")

    dry = args.dry_run

    # ── Step 1: Replace note_entities_operative_detail ─────────────────────
    if not args.skip_note_entities:
        log("Step 1: Replace note_entities_operative_detail (v2.2→v2.3)")
        if not NOTE_ENT_PARQUET.is_file():
            raise SystemExit(f"Missing: {NOTE_ENT_PARQUET}")
        bq_load("note_entities_operative_detail", NOTE_ENT_PARQUET, replace=True, dry_run=dry)
        if not dry:
            log("  Waiting 5s for BQ consistency...")
            time.sleep(5)
    else:
        log("Step 1: SKIPPED (--skip-note-entities)")

    # ── Step 2: Add new columns to operative_episode_detail_v2 ─────────────
    log("Step 2: ADD new columns to operative_episode_detail_v2")
    add_missing_bq_cols("operative_episode_detail_v2", dry_run=dry)

    if not dry:
        log("  Waiting 5s for schema propagation...")
        time.sleep(5)

    # ── Step 3: UPDATE BQ OED with new NLP values ───────────────────────────
    log("Step 3: UPDATE operative_episode_detail_v2 with v2.3 NLP columns")
    if not OED_UPDATE_PARQUET.is_file():
        raise SystemExit(f"Missing: {OED_UPDATE_PARQUET}")
    update_oed_from_parquet(dry_run=dry)

    log("=== mig_331b COMPLETE ===")
    if not dry:
        log("Next: run 364_cpm_feeder_repoint.py (Phase 4)")


if __name__ == "__main__":
    main()
