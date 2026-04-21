#!/usr/bin/env python3
"""Script 373 — manuscript_workspace + views_readable cleanup (Phase 4).

The 5 dependent views from Phase 1 (US_Nodules_Characteristics,
US_Nodules_Index, US_TIRADS_Reextraction_Queue, imaging_nodule_master_clean_v1,
tirads_llm_haiku_vs_qwen_v1) were already dropped by Script 370.

This script handles the remaining workspace tables:
  manuscript_workspace.us_nodules_tirads_vs_inm_v1_discordance_v1
      (1,722 rows — QA from v1 era, superseded by us_nodule_conflict_queue_v1)
      → drop outright (no archive — superseded equivalent exists)
  manuscript_workspace.tirads_v1_v2_discordance_v1
      (493 rows — historical v1↔v2 transition state)
      → archive as archived_tirads_v1_v2_discordance_v1 then drop
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
ARCH_DB = "Thyroid 2026 UPdated"
ARCH_SCHEMA = "us_legacy_20260421"
SCRIPT_TAG = "Script 373"

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"373_workspace_cleanup_{RUN_TS}.json"

DROP_OUTRIGHT: list[str] = [
    "us_nodules_tirads_vs_inm_v1_discordance_v1",
]
ARCHIVE_AND_DROP: list[str] = [
    "tirads_v1_v2_discordance_v1",
]


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def table_exists(con, db: str, schema: str, name: str) -> bool:
    return bool(con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [db, schema, name],
    ).fetchone()[0])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    plan: list[dict] = []
    for t in DROP_OUTRIGHT + ARCHIVE_AND_DROP:
        present = table_exists(con, PUB, "manuscript_workspace", t)
        n = (
            con.execute(
                f'SELECT COUNT(*) FROM {PUB}.manuscript_workspace."{t}"'
            ).fetchone()[0] if present else None
        )
        plan.append({"table": t, "present": present, "rows": n,
                     "action": "archive+drop" if t in ARCHIVE_AND_DROP
                     else "drop"})
    for p in plan:
        log(f"  {p['table']:55s} present={p['present']} rows={p['rows']} "
            f"action={p['action']}")

    if not args.commit:
        log("dry-run only.")
        return 0

    log(f'ensure archive schema "{ARCH_DB}"."{ARCH_SCHEMA}"')
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{ARCH_DB}"."{ARCH_SCHEMA}"')

    results: list[dict] = []
    for t in ARCHIVE_AND_DROP:
        if not table_exists(con, PUB, "manuscript_workspace", t):
            log(f"  SKIP {t} (already absent)")
            results.append({"table": t, "status": "absent"})
            continue
        src = f'{PUB}.manuscript_workspace."{t}"'
        dst = f'"{ARCH_DB}"."{ARCH_SCHEMA}"."archived_{t}"'
        n_src = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
        if not table_exists(con, ARCH_DB, ARCH_SCHEMA, f"archived_{t}"):
            log(f"  archive {t} → archived_{t} (rows={n_src})")
            con.execute(f"CREATE TABLE {dst} AS SELECT * FROM {src}")
        else:
            log(f"  archive_{t} already exists")
        log(f"  DROP TABLE manuscript_workspace.{t}")
        con.execute(f"DROP TABLE {src}")
        con.execute(
            f"DELETE FROM {PUB}.manuscript_workspace.detail_table_registry_v1 "
            f"WHERE detail_table_name = ?", [t],
        )
        results.append({"table": t, "status": "archived+dropped",
                        "rows": n_src})

    for t in DROP_OUTRIGHT:
        if not table_exists(con, PUB, "manuscript_workspace", t):
            log(f"  SKIP {t} (already absent)")
            results.append({"table": t, "status": "absent"})
            continue
        src = f'{PUB}.manuscript_workspace."{t}"'
        n_src = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
        log(f"  DROP TABLE manuscript_workspace.{t}  (rows={n_src})")
        con.execute(f"DROP TABLE {src}")
        con.execute(
            f"DELETE FROM {PUB}.manuscript_workspace.detail_table_registry_v1 "
            f"WHERE detail_table_name = ?", [t],
        )
        results.append({"table": t, "status": "dropped", "rows": n_src})

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "plan": plan, "results": results,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    log(f"summary: {len([r for r in results if 'dropped' in r['status']])} "
        "tables dropped from manuscript_workspace")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
