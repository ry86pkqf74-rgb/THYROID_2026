#!/usr/bin/env python3
"""Script 380 — Phase 5: archive (refresh) + drop the 4 held tables.

Targets (currently in main, with archived_<name> already in
us_legacy_20260421 from Scripts 371/372):
  main.tirads_v2_nodules_raw
  main.note_entities_llm_tirads_granular
  main.note_entities_llm_us_nodule_dynamics
  main.note_entities_llm_imaging

For each table:
  1. MD5 content hash main.<t>.
  2. MD5 content hash archive (must match → archive is current).
     If hashes differ (table changed since prior archive), refresh archive
     by recreating it.
  3. Dependency scan in information_schema.views — abort if any view
     references the table (rewrite the view first).
  4. DROP TABLE.
  5. DELETE registry row.
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
SCRIPT_TAG = "Script 380"

TARGETS = [
    "tirads_v2_nodules_raw",
    "note_entities_llm_tirads_granular",
    "note_entities_llm_us_nodule_dynamics",
    "note_entities_llm_imaging",
]

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"380_drop_held_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def content_hash(con, fq: str) -> str:
    return con.execute(
        f"SELECT MD5(STRING_AGG(MD5(CAST(t AS VARCHAR)), '|' "
        f"ORDER BY MD5(CAST(t AS VARCHAR)))) FROM {fq} t"
    ).fetchone()[0]


def find_dependent_views(con, table: str) -> list[str]:
    rows = con.execute(
        "SELECT table_schema, table_name FROM information_schema.views "
        "WHERE table_catalog = ? AND LOWER(view_definition) LIKE ?",
        [PUB, f'%{table.lower()}%'],
    ).fetchall()
    return [f"{r[0]}.{r[1]}" for r in rows]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    plan: list[dict] = []
    for t in TARGETS:
        fq_main = f'{PUB}.main."{t}"'
        fq_arch = f'"{ARCH_DB}"."{ARCH_SCHEMA}"."archived_{t}"'

        main_kind = con.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_catalog=? AND table_schema='main' AND table_name=?",
            [PUB, t],
        ).fetchall()
        if not main_kind:
            log(f"  SKIP {t}: not in main")
            plan.append({"table": t, "status": "absent_in_main"})
            continue

        n_main = con.execute(f"SELECT COUNT(*) FROM {fq_main}").fetchone()[0]

        arch_kind = con.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_catalog=? AND table_schema=? AND table_name=?",
            [ARCH_DB, ARCH_SCHEMA, f"archived_{t}"],
        ).fetchall()
        n_arch = (
            con.execute(f"SELECT COUNT(*) FROM {fq_arch}").fetchone()[0]
            if arch_kind else None
        )

        deps = find_dependent_views(con, t)

        plan.append({
            "table": t, "main_rows": n_main, "archive_rows": n_arch,
            "dependent_views": deps,
        })
        log(f"  {t:42s}  main={n_main:>6,}  archive={n_arch}  "
            f"deps={deps}")

    blocking_deps = [
        (p["table"], p.get("dependent_views"))
        for p in plan if p.get("dependent_views")
    ]
    if blocking_deps:
        log(f"BLOCKING: {len(blocking_deps)} tables have dependent views; "
            "rewrite views first before drop.")
        for t, d in blocking_deps:
            log(f"  {t}: {d}")
        DECISION_LOG.write_text(json.dumps(
            {"script": SCRIPT_TAG, "plan": plan, "blocked": True},
            indent=2, default=str,
        ))
        return 1

    if not args.commit:
        log("dry-run only.")
        return 0

    log("verify hashes + refresh archives where stale")
    results: list[dict] = []
    for p in plan:
        if p.get("status") == "absent_in_main":
            results.append(p)
            continue
        t = p["table"]
        fq_main = f'{PUB}.main."{t}"'
        fq_arch = f'"{ARCH_DB}"."{ARCH_SCHEMA}"."archived_{t}"'

        h_main = content_hash(con, fq_main)
        # Refresh archive if row count differs (cheaper than hashing first)
        if p["archive_rows"] != p["main_rows"]:
            log(f"  refresh archive for {t} (row count differs: "
                f"main={p['main_rows']} archive={p['archive_rows']})")
            con.execute(f"DROP TABLE {fq_arch}")
            con.execute(f"CREATE TABLE {fq_arch} AS SELECT * FROM {fq_main}")
        h_arch = content_hash(con, fq_arch)
        if h_main != h_arch:
            log(f"  hash mismatch for {t}: refreshing archive")
            con.execute(f"DROP TABLE {fq_arch}")
            con.execute(f"CREATE TABLE {fq_arch} AS SELECT * FROM {fq_main}")
            h_arch = content_hash(con, fq_arch)
            if h_main != h_arch:
                log(f"  CRITICAL: hashes still differ after refresh on {t}")
                results.append({"table": t, "status": "hash_mismatch_post_refresh"})
                continue

        log(f"  DROP TABLE main.{t}  (hashes match)")
        con.execute(f"DROP TABLE {fq_main}")
        con.execute(
            f"DELETE FROM {PUB}.manuscript_workspace.detail_table_registry_v1 "
            f"WHERE detail_table_name = ?", [t],
        )
        results.append({"table": t, "status": "dropped",
                        "rows": p["main_rows"], "hash": h_main[:12]})

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "plan": plan, "results": results,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    log(f"summary: {sum(1 for r in results if r.get('status')=='dropped')} "
        "tables dropped from main")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
