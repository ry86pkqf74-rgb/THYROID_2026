#!/usr/bin/env python3
"""Script 372 — Archive + drop raw LLM entity tables (Phase 3).

Targets:
  main.note_entities_llm_tirads_granular     (11,037 rows / 5,641 pts)
  main.note_entities_llm_us_nodule_dynamics  (11,037 rows / 5,641 pts)
  main.note_entities_llm_imaging             (11,037 rows / 5,641 pts)

Probe finding (2026-04-21): every result_json sampled is "{\"entities\": []}"
— these tables were extraction scaffolds that produced no entities. The
verification below confirms this empirically before drop:
  * fraction of rows with non-empty result_json
  * for non-empty rows: surface a sample to the audit table
    manuscript_workspace.us_llm_absorption_gap_v1 so any genuine entities
    are visible before drop.

If any non-trivial entities are found AND not yet absorbed into
canonical_us_nodule_v2, abort. Otherwise archive + drop.

The prompt's "split note_entities_llm_imaging by US/non-US" step is moot
here because no entities exist to split.
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
SCRIPT_TAG = "Script 372"
TARGETS = [
    "note_entities_llm_tirads_granular",
    "note_entities_llm_us_nodule_dynamics",
    "note_entities_llm_imaging",
]
GAP_TABLE = (
    f"{PUB}.manuscript_workspace.us_llm_absorption_gap_v1"
)

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"372_us_llm_entities_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def verify_emptiness(con, table: str) -> dict:
    fq = f'{PUB}.main."{table}"'
    n_total = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
    n_with_json = con.execute(
        f"SELECT COUNT(*) FROM {fq} WHERE result_json IS NOT NULL "
        f"AND TRIM(result_json) <> ''"
    ).fetchone()[0]
    # Empty entities: result_json = '{"entities": []}' or similar pattern
    n_empty = con.execute(
        f"SELECT COUNT(*) FROM {fq} "
        f"WHERE result_json IS NULL "
        f"   OR TRIM(result_json) = '' "
        f"   OR result_json LIKE '%\"entities\":%[]%' "
        f"   OR result_json LIKE '%\"entities\": []%' "
        f"   OR result_json = '{{}}' "
    ).fetchone()[0]
    n_nontrivial = n_total - n_empty
    return {
        "table": table,
        "rows_total": n_total,
        "rows_with_json_text": n_with_json,
        "rows_empty_or_null": n_empty,
        "rows_nontrivial": n_nontrivial,
    }


def archive_one(con, table: str) -> dict:
    src = f'{PUB}.main."{table}"'
    dst = f'"{ARCH_DB}"."{ARCH_SCHEMA}"."archived_{table}"'
    n_src = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]

    exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [ARCH_DB, ARCH_SCHEMA, f"archived_{table}"],
    ).fetchone()[0] > 0
    if exists:
        n_dst = con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0]
        if n_dst == n_src:
            log(f"  archive {table} already exists (rows={n_dst})")
            return {"table": table, "archive_status": "exists",
                    "rows": n_src}
        log(f"  archive {table} stale; recreating")
        con.execute(f"DROP TABLE {dst}")

    con.execute(f"CREATE TABLE {dst} AS SELECT * FROM {src}")
    n_dst = con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0]
    if n_dst != n_src:
        raise SystemExit(f"archive count mismatch for {table}")
    return {"table": table, "archive_status": "created", "rows": n_src}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true",
                    help="Apply archive + drop (only safe when verification "
                         "shows zero non-trivial entities).")
    ap.add_argument("--archive-only", action="store_true",
                    help="Archive to us_legacy_20260421 but do NOT drop "
                         "from main. Use when verification finds non-trivial "
                         "entities not yet absorbed into v2 — preserves data "
                         "for a follow-up absorption pass.")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}  "
        f"archive_only={args.archive_only}")
    con = connect_locked()

    log("verify result_json emptiness across LLM entity tables")
    verifications = [verify_emptiness(con, t) for t in TARGETS]
    for v in verifications:
        log(f"  {v['table']:40s} total={v['rows_total']:>6,}  "
            f"empty={v['rows_empty_or_null']:>6,}  "
            f"nontrivial={v['rows_nontrivial']:>6,}")

    nontrivial_tables = [v for v in verifications if v["rows_nontrivial"] > 0]
    if nontrivial_tables and not args.archive_only:
        # Write gap audit so Logan can review before any drop
        log("nontrivial entity rows detected — writing audit and aborting drop")
        if args.commit:
            con.execute(
                f"CREATE OR REPLACE TABLE {GAP_TABLE} ("
                f"  source_table VARCHAR, "
                f"  sample_row_count BIGINT, "
                f"  sample_result_json VARCHAR, "
                f"  detected_at TIMESTAMP)"
            )
            for v in nontrivial_tables:
                src = f'{PUB}.main."{v["table"]}"'
                samples = con.execute(
                    f"SELECT result_json FROM {src} "
                    f"WHERE result_json IS NOT NULL "
                    f"  AND TRIM(result_json) <> '' "
                    f"  AND result_json NOT LIKE '%\"entities\": []%' "
                    f"LIMIT 5"
                ).fetchall()
                for s in samples:
                    con.execute(
                        f"INSERT INTO {GAP_TABLE} VALUES (?, ?, ?, "
                        f"CURRENT_TIMESTAMP)",
                        [v["table"], v["rows_nontrivial"],
                         (s[0] or "")[:2000]],
                    )
            log(f"  audit written to {GAP_TABLE}")
        log("ABORT — re-run with --archive-only to preserve and skip drop.")
        DECISION_LOG.write_text(json.dumps(
            {"script": SCRIPT_TAG, "verifications": verifications,
             "aborted_due_to_nontrivial_entities": True}, indent=2,
            default=str))
        return 1

    if nontrivial_tables and args.archive_only:
        log("nontrivial entities present — archiving WITHOUT drop "
            "(per --archive-only)")
    else:
        log("all 3 tables are empty extractions — safe to archive + drop")

    if not args.commit:
        log("dry-run only — pass --commit to perform archive+drop.")
        DECISION_LOG.write_text(json.dumps(
            {"script": SCRIPT_TAG, "verifications": verifications,
             "commit": False}, indent=2, default=str))
        return 0

    log(f'ensure archive schema "{ARCH_DB}"."{ARCH_SCHEMA}"')
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{ARCH_DB}"."{ARCH_SCHEMA}"')

    archives = [archive_one(con, t) for t in TARGETS]

    drops: list[dict] = []
    if args.archive_only:
        log("HOLD all 3 tables (archive-only mode); no main drops")
        for t in TARGETS:
            drops.append({"table": t, "status": "held"})
    else:
        log("DROP each archived LLM table + DELETE registry row")
        for t in TARGETS:
            log(f"  DROP TABLE main.{t}")
            con.execute(f'DROP TABLE {PUB}.main."{t}"')
            con.execute(
                f"DELETE FROM {PUB}.manuscript_workspace.detail_table_registry_v1 "
                f"WHERE detail_table_name = ?", [t],
            )
            drops.append({"table": t, "status": "dropped"})

    # Also write a gap audit table so the absorption gap is durable
    if args.archive_only and any(v["rows_nontrivial"] > 0 for v in verifications):
        con.execute(
            f"CREATE OR REPLACE TABLE {GAP_TABLE} ("
            f"  source_table VARCHAR, "
            f"  sample_row_count BIGINT, "
            f"  sample_result_json VARCHAR, "
            f"  detected_at TIMESTAMP)"
        )
        for v in [v for v in verifications if v["rows_nontrivial"] > 0]:
            src = f'{PUB}.main."{v["table"]}"'
            samples = con.execute(
                f"SELECT result_json FROM {src} "
                f"WHERE result_json IS NOT NULL "
                f"  AND TRIM(result_json) <> '' "
                f"  AND result_json NOT LIKE '%\"entities\": []%' "
                f"LIMIT 5"
            ).fetchall()
            for s in samples:
                con.execute(
                    f"INSERT INTO {GAP_TABLE} VALUES (?, ?, ?, "
                    f"CURRENT_TIMESTAMP)",
                    [v["table"], v["rows_nontrivial"], (s[0] or "")[:2000]],
                )
        log(f"  gap audit written to {GAP_TABLE}")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "verifications": verifications, "archives": archives, "drops": drops,
        "archive_only": args.archive_only,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    log(f"summary: {sum(1 for d in drops if d['status']=='dropped')} dropped, "
        f"{sum(1 for d in drops if d['status']=='held')} held, "
        f"{len(archives)} archived")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
