#!/usr/bin/env python3
"""
Script 386b — patch round-2 source tables' ``llm_model`` tag from
``qwen2.5-32b`` -> ``openai/gpt-oss-120b``.

CONTEXT
=======
Carry-forward 6.I from ``scripts/output/386_close_out.md``: the round-2 ckpt
JSONL files (pathology / cervical_ln_detail / tirads_granular / esophageal_invasion)
are tagged ``llm_model = "qwen2.5-32b"``, but per the prompt evaluator stats
(row counts and entity-type distributions match exactly) these are the
gpt-oss-120b RunPod outputs.  The misleading tag is a stale launcher-config
value, not the true model.

Why fix it now (pre-Script 388)
-------------------------------
Script 388's era classifier keys on ``llm_model`` to assign each
``note_entities_llm_*`` table a CURRENT_LIVE / LEGACY_REPLACED bucket.  If the
round-2 tables remain mis-tagged, 388 will mis-bucket them as legacy qwen
output, which is the wrong era.  Single UPDATE per table is the right fix at
the source (rather than papering over it in the classifier).

Cross-source corroboration
--------------------------
* All 4 round-2 tables share the same ``llm_base_url``
  (``https://pmza5juk7ru2xl-8000.proxy.runpod.net/v1``) as the vascular v2
  batch (Script 368), which is correctly tagged ``openai/gpt-oss-120b``.
* Row counts match the prompt's gpt-oss-120b stats exactly:
  pathology=10,084 / cervical_ln=10,084 / tirads=10,084 / esophageal=4,409.
* Entity schemas are the v2 prompt's expected types (e.g. ``ln_level``,
  ``esophageal_invasion_present``, ``fna_cytology``), not the older qwen3
  prompt's narrower set.

Audit trail
-----------
1. Pre-mutation snapshot (``llm_model`` distribution per table) -> archive_pub_v1_0.
2. UPDATE each table's llm_model to the corrected value.
3. Verify-after.
4. Append a __readme entry per table documenting the correction.

Idempotent: re-running this script is a no-op (UPDATE WHERE current value
matches the OLD tag; if already corrected, 0 rows affected).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts._round2_helpers import (  # noqa: E402
    ARCHIVE_DB,
    ARCHIVE_SCHEMA,
    RunLogger,
    append_readme,
    connect_md,
    table_exists,
)

OLD_TAG = "qwen2.5-32b"
NEW_TAG = "openai/gpt-oss-120b"

# Round-2 source tables to patch (vascular intentionally NOT included — its
# tag was correct from the start).
ROUND2_TABLES = (
    "note_entities_llm_pathology",
    "note_entities_llm_cervical_ln_detail",
    "note_entities_llm_tirads_granular",
    "note_entities_llm_esophageal_invasion",
)

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
LOG_PATH = OUTPUT_DIR / "386b_run.log"
RESULT_PATH = OUTPUT_DIR / "386b_fix_result.json"

logger = RunLogger(LOG_PATH)
log = logger.log
gate = logger.gate


def _model_dist(con: Any, table: str) -> list[tuple[str, int]]:
    return [
        (r[0], int(r[1]))
        for r in con.execute(
            f"SELECT llm_model, COUNT(*) FROM main.{table} GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
    ]


def main() -> None:
    ap = argparse.ArgumentParser(description="Script 386b — fix round-2 llm_model tag")
    ap.add_argument("--apply", action="store_true",
                    help="Actually run the UPDATE (omit for dry-run).")
    args = ap.parse_args()

    log(f"Script 386b — fix round-2 llm_model tag — {datetime.now(timezone.utc).isoformat()}")
    log(f"  apply={args.apply}  OLD_TAG={OLD_TAG!r}  NEW_TAG={NEW_TAG!r}")

    con = connect_md(logger)
    result: dict[str, Any] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "apply": args.apply,
        "old_tag": OLD_TAG,
        "new_tag": NEW_TAG,
        "tables": {},
    }

    log("=" * 70)
    log("PRE — current llm_model distribution per table")
    log("=" * 70)
    for tbl in ROUND2_TABLES:
        gate(table_exists(con, "main", tbl), f"main.{tbl} present")
        pre = _model_dist(con, tbl)
        result["tables"][tbl] = {"pre": pre}
        log(f"  main.{tbl}:")
        for m, n in pre:
            log(f"    {m!r:35s}  {n:>6,}")

    if not args.apply:
        log("=" * 70)
        log("DRY-RUN — pass --apply to commit. No mutation performed.")
        log("=" * 70)
        RESULT_PATH.write_text(json.dumps(result, indent=2))
        log(f"  result: {RESULT_PATH.name}")
        logger.flush()
        return

    # Per-table no-op short-circuit: only do snapshot + UPDATE + __readme when
    # there are still OLD-tagged rows to flip.  Makes the script truly
    # idempotent on re-run (no extra archive snapshots, no misleading
    # "Patched X rows" __readme rows when X is actually 0).
    snap_stem = f"llm_model_pre386b_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    tables_to_patch: list[tuple[str, int]] = []
    for tbl in ROUND2_TABLES:
        n_old = con.execute(
            f"SELECT COUNT(*) FROM main.{tbl} WHERE llm_model = ?", [OLD_TAG]
        ).fetchone()[0]
        result["tables"][tbl]["rows_to_patch"] = int(n_old)
        if n_old == 0:
            log(f"  main.{tbl}: 0 rows on {OLD_TAG!r} — already patched, skipping")
        else:
            tables_to_patch.append((tbl, n_old))

    if not tables_to_patch:
        log("=" * 70)
        log("NO-OP — every round-2 table is already on the corrected tag")
        log("=" * 70)
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        result["status"] = "noop"
        RESULT_PATH.write_text(json.dumps(result, indent=2))
        log(f"  result: {RESULT_PATH.name}")
        log("Done.")
        logger.flush()
        return

    log("=" * 70)
    log("SNAPSHOT — pre-mutation llm_model snapshot per table -> archive_pub_v1_0")
    log("=" * 70)
    for tbl, n_old in tables_to_patch:
        snap = f"{snap_stem}__{tbl}"
        con.execute(
            f'CREATE OR REPLACE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}."{snap}" AS '
            f"SELECT note_row_id, llm_model FROM main.{tbl}"
        )
        n = con.execute(
            f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}."{snap}"'
        ).fetchone()[0]
        log(f"  snapshotted main.{tbl} ({n:,} rows) -> {snap}")

    log("=" * 70)
    log(f"UPDATE — set llm_model = {NEW_TAG!r} where llm_model = {OLD_TAG!r}")
    log("=" * 70)
    for tbl, n_old in tables_to_patch:
        con.execute(
            f"UPDATE main.{tbl} SET llm_model = ? WHERE llm_model = ?",
            [NEW_TAG, OLD_TAG],
        )
        post = _model_dist(con, tbl)
        result["tables"][tbl]["post"] = post
        log(f"  main.{tbl} post-UPDATE:")
        for m, n in post:
            log(f"    {m!r:35s}  {n:>6,}")

        still_old = con.execute(
            f"SELECT COUNT(*) FROM main.{tbl} WHERE llm_model = ?", [OLD_TAG]
        ).fetchone()[0]
        gate(still_old == 0, f"no rows remain on {OLD_TAG!r} in main.{tbl} (got {still_old:,})")
        n_new = con.execute(
            f"SELECT COUNT(*) FROM main.{tbl} WHERE llm_model = ?", [NEW_TAG]
        ).fetchone()[0]
        gate(n_new > 0, f"some rows on {NEW_TAG!r} in main.{tbl} (got {n_new:,})")

    log("=" * 70)
    log("__readme — append provenance row per patched table")
    log("=" * 70)
    for tbl, n_old in tables_to_patch:
        append_readme(
            con, logger,
            script="386b_fix_round2_llm_model_tag",
            content=(
                f"[Script 386b 2026-04-22] Patched main.{tbl}.llm_model from "
                f"{OLD_TAG!r} -> {NEW_TAG!r} ({n_old:,} rows actually flipped). "
                "The original ckpt JSONL tag was a stale launcher-config value; "
                "corroborating evidence (shared RunPod base_url with vasc v2, "
                "exact-match row counts to evaluator's gpt-oss-120b stats, v2 prompt "
                "entity schemas) confirms the true model. Pre-mutation snapshot at "
                f"archive_pub_v1_0.{snap_stem}__{tbl}. Carry-forward 6.I from 386 closes."
            ),
        )

    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    RESULT_PATH.write_text(json.dumps(result, indent=2))
    log(f"  result: {RESULT_PATH.name}")
    log("Done.")
    logger.flush()


if __name__ == "__main__":
    main()
