#!/usr/bin/env python3
"""
Script 383 — round-2 tirads_granular LLM extraction merge + MD-load + 376/377/378 chain.

CONTEXT
=======
The tirads_granular batch (10,084 notes) uses a DIFFERENT result_json shape
from the other round-2 domains — instead of ``{"entities": [...]}`` it emits
``{"nodules": [{...}, ...], "report_level": {...}}`` with 30+ structured fields
per nodule.

Per cursor prompt 2026-04-22 §1B / D5, this script does NOT build a new
canonical.  The existing US v2 absorb pipeline is the canonical surface:

  * ``main.canonical_us_nodule_v2`` (37,579 baseline rows)

The pipeline is wired through Scripts 376 → 377 → 378 → 379, which expect
``main.note_entities_llm_tirads_granular`` to be live in PUB.main.  Our job
is to land the parquet there and trigger the chain.

PHASES
======
  --phase merge   Read ckpt JSONL → dedup → write parquet (READ-ONLY to MD)
  --phase 0       Parquet audit: count nodules & non-trivial report_level (READ-ONLY)
  --phase 1       Archive current main.note_entities_llm_tirads_granular if present
  --phase 2       Load merged parquet (CREATE OR REPLACE)
  --phase 3       Post-load parity
  --phase 4       Trigger 376 → 377 → 378 → 379 (each with --commit)
  --phase 5       Final invariants — us_nodule_v2 monotonic + LN/frozen unchanged
  --phase all     merge → 0 → 5

NOTE: phases 4/5 run subprocesses against the same MD instance.  No new
canonicals are created by this script.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts._round2_helpers import (  # noqa: E402
    ARCHIVE_SCHEMA,
    CANONICAL_DB,
    RunLogger,
    append_readme,
    archive_table_if_present,
    assert_cpm_intact,
    assert_unchanged,
    connect_md,
    table_exists,
    upsert_registry,
)

DOMAIN = "tirads_granular"
SOURCE_TABLE = "note_entities_llm_tirads_granular"
ARCHIVE_NAME = f"{SOURCE_TABLE}_pre383_20260422"

# Hard-guard baselines (probed 2026-04-22)
US_NODULE_V2_BASELINE = 37_579   # us_nodule_v2 must be >= this after the chain
US_LN_BASELINE = 6_801
FROZEN_SECTION_BASELINE = 7_081

CKPT_PATH = REPO_ROOT / "runs" / "round2_20260421" / DOMAIN / "output" / f"note_entities_llm_{DOMAIN}.ckpt.jsonl"
PARQUET_PATH = REPO_ROOT / "runs" / "round2_20260421" / DOMAIN / "output" / f"note_entities_llm_{DOMAIN}.parquet"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
LOG_PATH = OUTPUT_DIR / "383_run.log"
PREFLIGHT_PATH = OUTPUT_DIR / "383_preflight.json"

EXPECTED_LOADED_COLUMNS = [
    "note_row_id", "domain", "llm_model", "llm_base_url", "extracted_at",
    "result_json", "research_id", "note_type", "note_date", "linkage_date",
    "source_workbook", "source_sheet", "source_column", "note_index",
    "preprocess_batch_id", "preprocessed_at_utc", "preprocess_script_version",
]

# Downstream chain (Scripts 376/377/378/379) is intentionally NOT triggered.
#
# 2026-04-22 reality check: all chain prerequisites have been archived:
#   * main.tirads_v2_nodules_raw  -> "Thyroid 2026 UPdated".us_legacy_20260421.archived_tirads_v2_nodules_raw
#   * main.note_entities_llm_us_nodule_dynamics -> archived_note_entities_llm_us_nodule_dynamics
#   * main.note_entities_llm_imaging            -> archived_note_entities_llm_imaging
#
# Running 376/377/378 today would either no-op (376/377: tirads result_json
# is {nodules,report_level} not {entities}, so they'd see 0 entity types
# from this table) or hard-fail (378: source main.tirads_v2_nodules_raw is
# gone). The prompt's expectation that the chain auto-absorbs into
# canonical_us_nodule_v2 was based on a pre-archive state.
#
# Scope of this script: land the parquet only. A future script (suggest 388)
# should reconstitute the absorb pipeline against the new tirads_granular
# source. See carry-forward in 386 close-out.
CHAIN_SCRIPTS: tuple[tuple[str, list[str]], ...] = ()

logger = RunLogger(LOG_PATH)
log = logger.log
gate = logger.gate


def phase_merge() -> None:
    log("=" * 70)
    log(f"PHASE MERGE — read ckpt JSONL → dedup → write parquet ({DOMAIN})")
    log("=" * 70)
    gate(CKPT_PATH.exists(), f"ckpt exists: {CKPT_PATH.name}")

    raw: list[dict] = []
    with CKPT_PATH.open() as f:
        for line in f:
            line = line.strip()
            if line:
                raw.append(json.loads(line))
    log(f"  Raw checkpoint rows: {len(raw):,}")

    by_nrid: dict[str, dict] = {}
    for row in raw:
        nrid = row["note_row_id"]
        if nrid not in by_nrid or row["extracted_at"] > by_nrid[nrid]["extracted_at"]:
            by_nrid[nrid] = row
    deduped = list(by_nrid.values())
    log(f"  After dedup: {len(deduped):,} unique note_row_ids ({len(raw) - len(deduped):,} dups removed)")

    df = pd.DataFrame(deduped)
    for col in EXPECTED_LOADED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[EXPECTED_LOADED_COLUMNS]

    # Audit: count nodules vs trivial report_level (PHI-safe — counts only).
    n_with_nodules = 0
    total_nodules = 0
    n_nontrivial_report_level = 0
    for r in deduped:
        rj = r.get("result_json", "")
        if not isinstance(rj, str):
            continue
        try:
            data = json.loads(rj)
        except Exception:
            continue
        nodules = data.get("nodules", []) or []
        if nodules:
            n_with_nodules += 1
            total_nodules += len(nodules)
        rl = data.get("report_level", {}) or {}
        # "Non-trivial" = at least one structured field is non-null/non-empty
        if any(
            v not in (None, "", 0, 0.0)
            for k, v in rl.items()
            if k not in ("date_confidence", "source_line", "evidence_text")
        ):
            n_nontrivial_report_level += 1

    log(f"  Notes with >=1 nodule: {n_with_nodules:,} / {len(deduped):,} "
        f"({100 * n_with_nodules / len(deduped):.1f}%)")
    log(f"  Total nodules: {total_nodules:,}")
    log(f"  Notes with non-trivial report_level: {n_nontrivial_report_level:,}")
    distinct_models = sorted({r.get("llm_model", "?") for r in deduped})
    log(f"  Source-tag llm_model values (preserved as-is): {distinct_models}")

    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PARQUET_PATH, index=False)
    log(f"  Written: {PARQUET_PATH.name} ({len(df):,} rows, {PARQUET_PATH.stat().st_size / 1_048_576:.2f} MB)")

    PREFLIGHT_PATH.write_text(json.dumps({
        "domain": DOMAIN,
        "raw_rows": len(raw),
        "deduped_rows": len(deduped),
        "notes_with_nodules": n_with_nodules,
        "total_nodules": total_nodules,
        "notes_with_nontrivial_report_level": n_nontrivial_report_level,
        "source_llm_model_tags": distinct_models,
        "merged_at": datetime.now(timezone.utc).isoformat(),
        "parquet_path": str(PARQUET_PATH),
    }, indent=2))
    log(f"  Preflight: {PREFLIGHT_PATH.name}")
    log("PHASE MERGE complete")
    logger.flush()


def phase_0(con: Any) -> None:
    log("=" * 70)
    log(f"PHASE 0 — parquet audit ({DOMAIN}) (READ-ONLY)")
    log("=" * 70)
    gate(PARQUET_PATH.exists(), f"merged parquet exists: {PARQUET_PATH}")

    import duckdb as _duckdb
    local = _duckdb.connect()
    rel = f"read_parquet('{PARQUET_PATH}')"
    n_rows, n_nrids, n_rids = local.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT note_row_id), COUNT(DISTINCT research_id) FROM {rel}"
    ).fetchone()
    log(f"  Rows: {n_rows:,} | distinct note_row_ids: {n_nrids:,} | distinct RIDs: {n_rids:,}")
    gate(n_rows == n_nrids, "no duplicate note_row_ids in parquet")
    gate(n_rows >= 9_000, f"parquet row count >= 9,000 (got {n_rows:,})")

    # Verify shape: result_json has 'nodules' or 'report_level' key
    has_shape = local.execute(
        f"""SELECT COUNT(*) FROM {rel}
            WHERE CAST(result_json AS VARCHAR) LIKE '%"nodules"%'
               OR CAST(result_json AS VARCHAR) LIKE '%"report_level"%'"""
    ).fetchone()[0]
    gate(has_shape > 0, f"result_json has tirads_granular shape (got {has_shape:,} matching rows)")
    local.close()

    if table_exists(con, "main", SOURCE_TABLE):
        cur = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
        log(f"  Current MD main.{SOURCE_TABLE}: {cur:,} rows")
    else:
        log(f"  Current MD main.{SOURCE_TABLE}: NOT PRESENT (will be created in phase 2)")

    log("PHASE 0 complete")
    logger.flush()


def phase_1(con: Any) -> None:
    log("=" * 70)
    log(f"PHASE 1 — archive current main.{SOURCE_TABLE} → {ARCHIVE_SCHEMA} (if present)")
    log("=" * 70)
    archive_table_if_present(con, logger, "main", SOURCE_TABLE, ARCHIVE_NAME)
    log("PHASE 1 complete")
    logger.flush()


def phase_2(con: Any) -> None:
    log("=" * 70)
    log(f"PHASE 2 — load merged parquet → main.{SOURCE_TABLE}")
    log("=" * 70)
    gate(PARQUET_PATH.exists(), "merged parquet exists")

    import duckdb as _duckdb
    local = _duckdb.connect()
    df = local.execute(f"SELECT * FROM read_parquet('{PARQUET_PATH}')").df()
    local.close()
    log(f"  Loaded parquet: {len(df):,} rows")

    con.execute(f"USE {CANONICAL_DB}")
    con.execute("CREATE OR REPLACE TEMP TABLE _r2_tirads_stage AS SELECT * FROM df")
    staged = con.execute("SELECT COUNT(*) FROM _r2_tirads_stage").fetchone()[0]
    gate(staged == len(df), f"staging parity ({staged:,} == {len(df):,})")

    con.execute(f"CREATE OR REPLACE TABLE main.{SOURCE_TABLE} AS SELECT * FROM _r2_tirads_stage")
    loaded = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    gate(loaded == len(df), f"MD load parity ({loaded:,} == {len(df):,})")
    log(f"  Loaded {loaded:,} rows into main.{SOURCE_TABLE}")
    log("PHASE 2 complete")
    logger.flush()


def phase_3(con: Any) -> None:
    log("=" * 70)
    log("PHASE 3 — post-load parity")
    log("=" * 70)
    md_rows, md_nrids, md_rids = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT note_row_id), COUNT(DISTINCT research_id) FROM main.{SOURCE_TABLE}"
    ).fetchone()
    import duckdb as _duckdb
    local = _duckdb.connect()
    pq_rows, pq_rids = local.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM read_parquet('{PARQUET_PATH}')"
    ).fetchone()
    local.close()
    log(f"  MD: {md_rows:,} rows | {md_rids:,} RIDs | {md_nrids:,} note_row_ids")
    log(f"  Parquet: {pq_rows:,} rows | {pq_rids:,} RIDs")
    gate(md_rows == pq_rows, f"row-count parity ({md_rows:,})")
    gate(md_rids == pq_rids, f"RID-count parity ({md_rids:,})")
    gate(md_rows == md_nrids, "no duplicate note_row_ids in MD")
    log("PHASE 3 complete")
    logger.flush()


def phase_4(con: Any) -> None:
    log("=" * 70)
    log("PHASE 4 — absorb-chain trigger (intentionally SKIPPED — see header)")
    log("=" * 70)
    pre = con.execute("SELECT COUNT(*) FROM main.canonical_us_nodule_v2").fetchone()[0]
    log(f"  canonical_us_nodule_v2 row count: {pre:,} (no change expected)")

    # Chain prerequisites probe (read-only) — show what's actually live.
    for tbl in (
        "tirads_v2_nodules_raw",
        "note_entities_llm_us_nodule_dynamics",
        "note_entities_llm_imaging",
    ):
        present = table_exists(con, "main", tbl)
        log(f"  main.{tbl} present: {present}")

    # Conflict-log size signals (informational only)
    for tbl in (
        "us_llm_absorption_deferred_multi_nodule_v1",
        "us_raw_index0_conflict_v1",
        "us_raw_index_mismatch_v1",
        "us_llm_absorption_mapping_v1",
    ):
        try:
            n = con.execute(f"SELECT COUNT(*) FROM manuscript_workspace.{tbl}").fetchone()[0]
            log(f"  manuscript_workspace.{tbl}: {n:,} rows")
        except Exception as e:
            log(f"  manuscript_workspace.{tbl}: not present ({e!s})")

    log("  Chain skipped — see CHAIN_SCRIPTS comment for rationale.")
    log("  Carry-forward: a future script (suggest 388) should rebuild the absorb")
    log("  pipeline against the new tirads_granular source.")
    log("PHASE 4 complete")
    logger.flush()


# Subprocess import is intentionally retained for future re-enablement.
_ = subprocess  # keep import live


def phase_5(con: Any) -> None:
    log("=" * 70)
    log("PHASE 5 — final invariants (CPM intact + LN/frozen guards)")
    log("=" * 70)
    assert_cpm_intact(con, logger)
    assert_unchanged(
        con, logger,
        schema="main", table="canonical_us_lymph_node_v2",
        expected_rows=US_LN_BASELINE,
        label="US LN domain untouched",
    )
    assert_unchanged(
        con, logger,
        schema="main", table="canonical_frozen_section_events_v1",
        expected_rows=FROZEN_SECTION_BASELINE,
        label="frozen-section domain untouched",
    )
    n_us_nodule = con.execute("SELECT COUNT(*) FROM main.canonical_us_nodule_v2").fetchone()[0]
    # Chain skipped (see PHASE 4) — us_nodule_v2 must equal baseline exactly,
    # not merely "monotonic >= baseline".  Any drift here would indicate
    # something else mutated the canonical between phases.
    gate(n_us_nodule == US_NODULE_V2_BASELINE,
         f"us_nodule_v2 == baseline {US_NODULE_V2_BASELINE:,} (got {n_us_nodule:,}); "
         f"chain skipped, no monotonic growth expected")

    # Registry + __readme
    n_src = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    n_src_rids = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM main.{SOURCE_TABLE}").fetchone()[0]
    upsert_registry(
        con, logger,
        detail_table_name=SOURCE_TABLE, schema_name="main", join_key="note_row_id",
        grain="one row per clinical note (round-2 LLM tirads_granular extraction)",
        total_rows=n_src, total_patients=n_src_rids,
        domain="LLM source (tirads_granular)",
        feeds_master_columns="canonical_us_nodule_v2 (via Scripts 376/377/378)",
        description=(
            "Raw LLM output (round-2 batch, 2026-04-21). result_json shape is "
            '{"nodules":[...],"report_level":{...}} — NOT entities-array. Absorbed '
            "into canonical_us_nodule_v2 via the 376→377→378 chain; no new canonical."
        ),
        canonical_version="v1_0",
        feeds_master_columns_array=["canonical_us_nodule_v2"],
    )
    append_readme(
        con, logger,
        script="383_tirads_granular_merge_load",
        content=(
            f"[Script 383 2026-04-22] Loaded round-2 tirads_granular ({n_src:,} notes) "
            f"into main.{SOURCE_TABLE}. NO new canonical built (D5). The 376/377/378 "
            f"absorb chain was NOT triggered: its prerequisites "
            "(tirads_v2_nodules_raw, note_entities_llm_us_nodule_dynamics, "
            "note_entities_llm_imaging) were archived to us_legacy_20260421 / "
            f"archive_pub_v1_0 on 2026-04-21. canonical_us_nodule_v2 unchanged at "
            f"{n_us_nodule:,} rows. A future script (suggest 388) should rebuild the "
            "absorb pipeline against this tirads_granular source. See 386 close-out."
        ),
    )
    log("PHASE 5 complete")
    logger.flush()


def main() -> None:
    ap = argparse.ArgumentParser(description="Script 383 — round-2 tirads_granular merge+load+chain")
    ap.add_argument("--phase", required=True,
                    choices=["merge", "0", "1", "2", "3", "4", "5", "all"])
    args = ap.parse_args()

    log(f"Script 383 — phase={args.phase} — {datetime.now(timezone.utc).isoformat()}")

    if args.phase == "merge":
        phase_merge()
        return

    con = connect_md(logger)
    phases = {
        "0": phase_0, "1": phase_1, "2": phase_2,
        "3": phase_3, "4": phase_4, "5": phase_5,
    }
    if args.phase == "all":
        phase_merge()
        con = connect_md(logger)
        for p in ["0", "1", "2", "3", "4", "5"]:
            phases[p](con)
    else:
        phases[args.phase](con)

    logger.flush()
    log("Done.")


if __name__ == "__main__":
    main()
