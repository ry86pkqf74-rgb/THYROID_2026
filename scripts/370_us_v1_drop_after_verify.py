#!/usr/bin/env python3
"""Script 370 — Verify-then-drop already-archived US v1 tables (Phase 1).

Per-table sequence:
  1. Existence check (skip if already gone).
  2. Row-count match between main.<t> and "Thyroid 2026 UPdated".us_legacy_20260421.<t>.
  3. Column-set match (information_schema.columns name set).
  4. Deterministic content hash match (MD5 over sorted CAST(row AS VARCHAR)).
  5. Drop dependent views first (Phase 1b inline).
  6. DROP TABLE.
  7. DELETE from manuscript_workspace.detail_table_registry_v1.

Fail loud on any mismatch. Targets are the 9 tables Script 361 archived to
"Thyroid 2026 UPdated".us_legacy_20260421.

Probe identified 5 dependent views. They are dropped before any base-table drop:
  manuscript_workspace.tirads_llm_haiku_vs_qwen_v1
  manuscript_workspace.imaging_nodule_master_clean_v1
  views_readable.US_TIRADS_Reextraction_Queue
  views_readable.US_Nodules_Characteristics
  views_readable.US_Nodules_Index
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
SCRIPT_TAG = "Script 370"

# (schema, table) — order matters: workspace last so base tables go first
TARGETS: list[tuple[str, str]] = [
    ("main", "canonical_us_nodule_master_v1"),
    ("main", "canonical_us_nodule_characteristics_v1"),
    ("main", "imaging_nodule_master_v1"),
    ("main", "canonical_us_exam_master_v1"),
    ("main", "canonical_us_patient_master_v1"),
    ("main", "tirads_llm_extracted_v2"),
    ("main", "serial_imaging_us"),
    ("manuscript_workspace", "tirads_granular_parsed_v1"),
    ("manuscript_workspace", "us_nodule_dynamics_parsed_v1"),
]

# Views to drop first (Phase 1b inline). Order does not matter; CASCADE
# would also work but explicit drops let us log each.
DEPENDENT_VIEWS: list[tuple[str, str]] = [
    ("manuscript_workspace", "tirads_llm_haiku_vs_qwen_v1"),
    ("manuscript_workspace", "imaging_nodule_master_clean_v1"),
    ("views_readable", "US_TIRADS_Reextraction_Queue"),
    ("views_readable", "US_Nodules_Characteristics"),
    ("views_readable", "US_Nodules_Index"),
]

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"370_us_v1_drop_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def table_kind(con, db: str, schema: str, name: str) -> str | None:
    rows = con.execute(
        "SELECT table_type FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [db, schema, name],
    ).fetchall()
    return rows[0][0] if rows else None


def col_set(con, db: str, schema: str, name: str) -> set[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [db, schema, name],
    ).fetchall()
    return {r[0] for r in rows}


def content_hash(con, fq: str) -> str:
    """Deterministic MD5 of sorted CAST(row AS VARCHAR) join."""
    return con.execute(
        f"SELECT MD5(STRING_AGG(MD5(CAST(t AS VARCHAR)), '|' "
        f"ORDER BY MD5(CAST(t AS VARCHAR)))) FROM {fq} t"
    ).fetchone()[0]


def verify_one(con, schema: str, table: str) -> dict:
    main_kind = table_kind(con, PUB, schema, table)
    if main_kind is None:
        return {"status": "absent", "table": table, "schema": schema}

    fq_main = f'{PUB}.{schema}."{table}"'
    fq_arch = f'"{ARCH_DB}"."{ARCH_SCHEMA}"."{table}"'

    arch_kind = table_kind(con, ARCH_DB, ARCH_SCHEMA, table)
    if arch_kind is None:
        return {
            "status": "fail",
            "table": table,
            "reason": f"archive table missing: {fq_arch}",
        }

    n_main = con.execute(f"SELECT COUNT(*) FROM {fq_main}").fetchone()[0]
    n_arch = con.execute(f"SELECT COUNT(*) FROM {fq_arch}").fetchone()[0]
    if n_main != n_arch:
        return {
            "status": "fail",
            "table": table,
            "reason": f"row count mismatch main={n_main} arch={n_arch}",
        }

    cs_main = col_set(con, PUB, schema, table)
    cs_arch = col_set(con, ARCH_DB, ARCH_SCHEMA, table)
    if cs_main != cs_arch:
        only_main = sorted(cs_main - cs_arch)
        only_arch = sorted(cs_arch - cs_main)
        return {
            "status": "fail",
            "table": table,
            "reason": f"col-set mismatch only_main={only_main} only_arch={only_arch}",
        }

    h_main = content_hash(con, fq_main)
    h_arch = content_hash(con, fq_arch)
    if h_main != h_arch:
        return {
            "status": "fail",
            "table": table,
            "reason": f"content hash mismatch main={h_main} arch={h_arch}",
        }

    return {
        "status": "ok",
        "table": table,
        "schema": schema,
        "rows": n_main,
        "hash": h_main,
    }


def drop_dependent_views(con) -> list[dict]:
    out: list[dict] = []
    for schema, name in DEPENDENT_VIEWS:
        kind = table_kind(con, PUB, schema, name)
        if kind is None:
            log(f"  view {schema}.{name} already absent")
            out.append({"view": f"{schema}.{name}", "status": "absent"})
            continue
        log(f"  DROP VIEW IF EXISTS {schema}.{name}")
        con.execute(f'DROP VIEW IF EXISTS {PUB}.{schema}."{name}"')
        out.append({"view": f"{schema}.{name}", "status": "dropped"})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true",
                    help="Default = dry-run (verify only).")
    args = ap.parse_args()

    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    log("verify each target against its us_legacy_20260421 archive")
    verifications = [verify_one(con, sch, tbl) for sch, tbl in TARGETS]
    for v in verifications:
        log(f"  {v['table']:45s} {v['status']:6s} "
            f"{v.get('reason') or v.get('rows','-')}")

    fails = [v for v in verifications if v["status"] == "fail"]
    if fails:
        DECISION_LOG.write_text(json.dumps(
            {"script": SCRIPT_TAG, "verifications": verifications,
             "fails": fails}, indent=2, default=str))
        log(f"FAIL: {len(fails)} verification failures; aborting before drop.")
        log(f"decision log: {DECISION_LOG}")
        return 1

    if not args.commit:
        log("dry-run only — pass --commit to perform drops.")
        DECISION_LOG.write_text(json.dumps(
            {"script": SCRIPT_TAG, "verifications": verifications,
             "commit": False}, indent=2, default=str))
        return 0

    log("drop dependent views first (Phase 1b inline)")
    view_results = drop_dependent_views(con)

    log("DROP each verified table + DELETE registry row")
    drop_results: list[dict] = []
    for v in verifications:
        if v["status"] != "ok":
            continue
        sch, tbl = v["schema"], v["table"]
        fq = f'{PUB}.{sch}."{tbl}"'
        log(f"  DROP TABLE {fq}")
        con.execute(f"DROP TABLE {fq}")
        con.execute(
            f"DELETE FROM {PUB}.manuscript_workspace.detail_table_registry_v1 "
            f"WHERE detail_table_name = ?",
            [tbl],
        )
        drop_results.append({"table": tbl, "schema": sch, "status": "dropped"})

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "verifications": verifications,
        "view_drops": view_results,
        "table_drops": drop_results,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    log(f"summary: dropped {len(drop_results)} tables, {len(view_results)} views")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
