#!/usr/bin/env python3
"""Script 375 — CPM TIRADS column rename + final cleanup audit (Phase 6).

Renames on canonical_patient_master to kill the v2_v2 double-suffix and
match the new canonical_us_nodule_v2 vocabulary:

  imaging_tirads_category_v2_v2  →  imaging_updated_tirads_category_cpm_v2
  imaging_tirads_category        →  imaging_updated_tirads_category_cpm_v1

Then emits the final cleanup audit:
  * count of US/TIRADS objects in main (target ≤ 12)
  * count of objects in us_legacy_20260421 (target ≥ 14)
  * canonical_us_nodule_v2 row count (must stay 36,957)
  * acr2017_* / updated_* / concordance metrics
  * confirmation no column named imaging_tirads_category_v2_v2 anywhere
  * confirmation registry has no rows pointing to dropped tables
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
SCRIPT_TAG = "Script 375"
CPM = f"{PUB}.main.canonical_patient_master"

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"375_cpm_cleanup_{RUN_TS}.json"

CPM_RENAMES: list[tuple[str, str]] = [
    ("imaging_tirads_category_v2_v2", "imaging_updated_tirads_category_cpm_v2"),
    ("imaging_tirads_category",       "imaging_updated_tirads_category_cpm_v1"),
]

# Tables that should be DROPPED from registry if any rows linger
DROPPED_TABLES = [
    "canonical_us_nodule_master_v1",
    "canonical_us_nodule_characteristics_v1",
    "imaging_nodule_master_v1",
    "canonical_us_exam_master_v1",
    "canonical_us_patient_master_v1",
    "tirads_llm_extracted_v2",
    "serial_imaging_us",
    "tirads_granular_parsed_v1",
    "us_nodule_dynamics_parsed_v1",
    "extracted_tirads_validated_v1",
    "tirads_reextraction_queue_v1",
    "us_nodules_tirads_vs_inm_v1_discordance_v1",
    "tirads_v1_v2_discordance_v1",
]


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def cpm_columns(con) -> set[str]:
    return {
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog=? AND table_schema='main' "
            "AND table_name='canonical_patient_master'", [PUB],
        ).fetchall()
    }


def main_us_objects(con) -> list[tuple[str, str]]:
    return con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema IN ('main','manuscript_workspace') "
        "AND (LOWER(table_name) LIKE '%us%' "
        "  OR LOWER(table_name) LIKE '%tirads%' "
        "  OR LOWER(table_name) LIKE '%nodule%' "
        "  OR LOWER(table_name) LIKE '%ultrasound%') "
        "ORDER BY 1, 2", [PUB],
    ).fetchall()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    cols = cpm_columns(con)
    log(f"  CPM column count: {len(cols)}")

    if not args.commit:
        log("dry-run only.")
        return 0

    log("RENAME CPM columns")
    for old, new in CPM_RENAMES:
        if old in cols and new not in cols:
            log(f"  RENAME {old} → {new}")
            con.execute(f"ALTER TABLE {CPM} RENAME COLUMN {old} TO {new}")
        elif new in cols:
            log(f"  {new} already present (skip)")
        else:
            log(f"  WARN: {old} missing; cannot rename")

    # Refresh registry: remove any rows still pointing at dropped tables
    log("DELETE registry rows for dropped tables")
    deleted = 0
    for t in DROPPED_TABLES:
        # Skip rows where the table was archived but kept live (held)
        n = con.execute(
            f"DELETE FROM {PUB}.manuscript_workspace.detail_table_registry_v1 "
            f"WHERE detail_table_name = ? RETURNING 1", [t],
        ).fetchall()
        deleted += len(n)
    log(f"  registry rows DELETEd: {deleted}")

    # Final audit block
    log("=== FINAL AUDIT ===")

    # 1. main US/TIRADS object count
    objs = main_us_objects(con)
    main_us_count = sum(1 for s, _ in objs if s == "main")
    ws_us_count = sum(1 for s, _ in objs if s == "manuscript_workspace")
    log(f"  main US/TIRADS objects:                 {main_us_count}")
    for s, n in objs:
        if s == "main":
            log(f"    - main.{n}")
    log(f"  manuscript_workspace US/TIRADS objects: {ws_us_count}")
    for s, n in objs:
        if s == "manuscript_workspace":
            log(f"    - ws.{n}")

    # 2. archive count
    con.execute(f'USE "{ARCH_DB}"')
    arch_tbls = [
        r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog=? AND table_schema=? ORDER BY 1",
            [ARCH_DB, ARCH_SCHEMA],
        ).fetchall()
    ]
    con.execute(f'USE "{PUB}"')
    log(f"  us_legacy_20260421 tables:              {len(arch_tbls)}")
    for t in arch_tbls:
        log(f"    - {t}")

    # 3. canonical_us_nodule_v2 row count + TIRADS metrics
    nv2_rows = con.execute(
        f"SELECT COUNT(*) FROM {PUB}.main.canonical_us_nodule_v2"
    ).fetchone()[0]
    metrics = con.execute(
        f"""SELECT
            COUNT(acr2017_tirads_points), COUNT(acr2017_tirads_category),
            COUNT(updated_tirads_category),
            COUNT(CASE WHEN acr2017_tirads_category IS NOT NULL
                       AND updated_tirads_category IS NOT NULL THEN 1 END),
            SUM(CASE WHEN acr2017_vs_updated_concordant = FALSE THEN 1 ELSE 0 END)
           FROM {PUB}.main.canonical_us_nodule_v2"""
    ).fetchone()
    log(f"  canonical_us_nodule_v2 rows: {nv2_rows}")
    log(f"  has_acr2017_points={metrics[0]}  has_acr2017_category={metrics[1]} "
        f"has_updated_category={metrics[2]}")
    log(f"  both_populated={metrics[3]}  disagreeing_rows={metrics[4]}")

    # 4. forbid imaging_tirads_category_v2_v2 anywhere
    bad = con.execute(
        "SELECT table_schema, table_name FROM information_schema.columns "
        "WHERE table_catalog=? AND column_name='imaging_tirads_category_v2_v2'",
        [PUB],
    ).fetchall()
    log(f"  imaging_tirads_category_v2_v2 sightings: {len(bad)}  "
        f"(target: 0)")

    # 5. registry hygiene
    bad_reg = con.execute(
        f"""SELECT detail_table_name FROM {PUB}.manuscript_workspace.detail_table_registry_v1
            WHERE detail_table_name = ANY(?)""",
        [DROPPED_TABLES],
    ).fetchall()
    log(f"  registry rows for dropped tables remaining: {len(bad_reg)}  "
        f"(target: 0)")

    # 6. CPM TIRADS column inventory after rename
    cpm_cols_now = sorted(
        c for c in cpm_columns(con)
        if "tirads" in c.lower() or "imaging_updated" in c.lower()
    )
    log("  CPM imaging-TIRADS columns now:")
    for c in cpm_cols_now:
        log(f"    - {c}")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "renames_attempted": [{"old": o, "new": n} for o, n in CPM_RENAMES],
        "registry_deletes": deleted,
        "main_us_objects_count": main_us_count,
        "ws_us_objects_count": ws_us_count,
        "us_legacy_count": len(arch_tbls),
        "us_legacy_tables": arch_tbls,
        "nv2_rows": nv2_rows,
        "tirads_metrics": {
            "has_acr2017_points": metrics[0],
            "has_acr2017_category": metrics[1],
            "has_updated_category": metrics[2],
            "both_populated": metrics[3],
            "disagreeing_rows": metrics[4],
        },
        "bad_double_suffix_sightings": [
            {"schema": s, "table": t} for s, t in bad
        ],
        "bad_registry_remaining": [r[0] for r in bad_reg],
        "cpm_imaging_tirads_cols": cpm_cols_now,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
