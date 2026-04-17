#!/usr/bin/env python3
"""
Script 256 — Rebuild any_confirmed_complication_flag (audit §5.3, 174 patients)

The aggregate flag was rolling up only 3 of the 9 complication entities
(hypocalcemia, hypoparathyroidism, rln_injury). Patients with a confirmed
hematoma, seroma, chyle_leak, wound_infection, vocal_cord_paralysis, or
vocal_cord_paresis but no rollup hit registered as FALSE.

Complication entity → existing CPM column map (both names already exist):

  hypocalcemia          -> comp_hypocalcemia_confirmed
  hypoparathyroidism    -> comp_hypoparathyroidism_confirmed
  rln_injury            -> comp_rln_injury_confirmed
  hematoma              -> comp_hematoma_confirmed
  seroma                -> comp_seroma_confirmed
  chyle_leak            -> comp_chyle_leak_confirmed
  wound_infection       -> comp_wound_infection_confirmed
  vocal_cord_paralysis  -> comp_vc_paralysis_confirmed
  vocal_cord_paresis    -> comp_vc_paresis_confirmed

This script:
  1. Snapshots CPM.
  2. Idempotently rebuilds each comp_<entity>_confirmed BOOLEAN from the full
     complication_phenotype_v1 grouping per (research_id, complication_entity).
  3. Rebuilds any_confirmed_complication_flag = BOOL_OR(confirmed_flag=TRUE)
     across all 9 entities.
  4. Rebuilds n_confirmed_complications = COUNT(DISTINCT complication_entity
     WHERE confirmed_flag).
  5. Updates data_dictionary_v240 status='authoritative' for the 9 per-entity
     columns and for the aggregate.
  6. Self-verifies: 0 patients with phenotype-confirmed event but
     any_confirmed_complication_flag=FALSE.

Default --dry-run; pass --apply.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    ensure_audit_table, ensure_archive_schema, make_logger,
    record_audit, snapshot_table, utc_ts, write_decision_log,
)

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
RUN_LOG = OUTPUT_DIR / "256_run.log"
DECISION_LOG = OUTPUT_DIR / "256_decision_log.json"
SCRIPT_TAG = "Script 256"
SCRIPT_NUM = "256"
RUN_DATE = "2026-04-16"

CPM = f'{PUBLICATION_DB}.main.canonical_patient_master'
PHEN = f'{PUBLICATION_DB}.main.complication_phenotype_v1'
DICT = f'{PUBLICATION_DB}.main.data_dictionary_v240'

# Map complication_entity value -> CPM column.
ENTITY_COL_MAP = {
    "hypocalcemia":         "comp_hypocalcemia_confirmed",
    "hypoparathyroidism":   "comp_hypoparathyroidism_confirmed",
    "rln_injury":           "comp_rln_injury_confirmed",
    "hematoma":             "comp_hematoma_confirmed",
    "seroma":               "comp_seroma_confirmed",
    "chyle_leak":           "comp_chyle_leak_confirmed",
    "wound_infection":      "comp_wound_infection_confirmed",
    "vocal_cord_paralysis": "comp_vc_paralysis_confirmed",
    "vocal_cord_paresis":   "comp_vc_paresis_confirmed",
}

REPLAY_SQL = f"""
WITH cp AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid,
         BOOL_OR(confirmed_flag = TRUE) AS any_cp
  FROM {PHEN} GROUP BY 1
)
SELECT COUNT(*) FROM cp
JOIN {CPM} p ON TRY_CAST(p.research_id AS INTEGER) = cp.rid
WHERE cp.any_cp = TRUE AND COALESCE(p.any_confirmed_complication_flag, FALSE) = FALSE
"""


def replay_count(con) -> int:
    return int(con.execute(REPLAY_SQL).fetchone()[0])


def upsert_dict_entry(con, column_name: str, status: str,
                      replacement: str | None, description: str) -> None:
    n = con.execute(
        f"SELECT COUNT(*) FROM {DICT} WHERE column_name = ?",
        [column_name],
    ).fetchone()[0]
    if n == 0:
        cols = [r[0] for r in con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='data_dictionary_v240'
            ORDER BY ordinal_position
        """).fetchall()]
        value_map = {
            "column_name": column_name,
            "status": status,
            "replacement_column_name": replacement,
            "description": description,
        }
        col_list = [c for c in cols if c in value_map]
        placeholders = ",".join(["?"] * len(col_list))
        con.execute(
            f"INSERT INTO {DICT} ({','.join(col_list)}) VALUES ({placeholders})",
            [value_map[c] for c in col_list],
        )
    else:
        con.execute(
            f"""UPDATE {DICT} SET status=?, replacement_column_name=?,
                description = COALESCE(description, '') ||
                  CASE WHEN COALESCE(description,'')='' THEN '' ELSE ' ' END || ?
                WHERE column_name=?""",
            [status, replacement, description, column_name],
        )


def rebuild_per_entity_and_aggregate(con, log) -> dict:
    # Build a wide per-patient rollup of confirmed flags per entity.
    case_clauses = ",\n             ".join(
        f"BOOL_OR(complication_entity='{ent}' AND confirmed_flag=TRUE) AS confirmed_{ent}"
        for ent in ENTITY_COL_MAP
    )
    confirmed_pairs = " OR ".join(f"COALESCE(confirmed_{ent}, FALSE)" for ent in ENTITY_COL_MAP)
    n_count_terms = " + ".join(f"CASE WHEN COALESCE(confirmed_{ent}, FALSE) THEN 1 ELSE 0 END"
                               for ent in ENTITY_COL_MAP)
    set_clauses = ",\n        ".join(
        f"{cpm_col} = COALESCE(p.confirmed_{ent}, FALSE)"
        for ent, cpm_col in ENTITY_COL_MAP.items()
    )

    sql = f"""
    WITH per_pt AS (
      SELECT TRY_CAST(research_id AS INTEGER) AS rid,
             {case_clauses}
      FROM {PHEN}
      GROUP BY 1
    ),
    agg AS (
      SELECT rid,
             ({confirmed_pairs}) AS any_conf,
             ({n_count_terms})    AS n_conf,
             {", ".join(f"confirmed_{ent}" for ent in ENTITY_COL_MAP)}
      FROM per_pt
    )
    UPDATE {CPM} AS cpm
    SET {set_clauses},
        any_confirmed_complication_flag = COALESCE(p.any_conf, FALSE),
        any_confirmed_complication      = COALESCE(p.any_conf, FALSE),
        n_confirmed_complications       = COALESCE(p.n_conf, 0)
    FROM agg AS p
    WHERE TRY_CAST(cpm.research_id AS INTEGER) = p.rid
    """
    con.execute(sql)

    # Patients with NO complication_phenotype_v1 row stay with their
    # existing CPM values (likely FALSE/NULL/0). Force any_*=FALSE,
    # n_*=0 only where they remain NULL to keep schema-clean defaults.
    con.execute(f"""
        UPDATE {CPM}
        SET any_confirmed_complication_flag = FALSE
        WHERE any_confirmed_complication_flag IS NULL
    """)
    con.execute(f"""
        UPDATE {CPM}
        SET any_confirmed_complication = FALSE
        WHERE any_confirmed_complication IS NULL
    """)
    con.execute(f"""
        UPDATE {CPM}
        SET n_confirmed_complications = 0
        WHERE n_confirmed_complications IS NULL
    """)

    n_any = int(con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE any_confirmed_complication_flag = TRUE"
    ).fetchone()[0])
    log(f"  patients with any_confirmed_complication_flag=TRUE: {n_any}")
    per_entity = {}
    for ent, col in ENTITY_COL_MAP.items():
        n = int(con.execute(
            f"SELECT COUNT(*) FROM {CPM} WHERE {col} = TRUE"
        ).fetchone()[0])
        per_entity[col] = n
        log(f"    {col:38s} TRUE: {n}")
    return {"any_confirmed_true": n_any, "per_entity_true": per_entity}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes; default is dry-run.")
    args = ap.parse_args()
    do_writes = bool(args.apply)
    mode = "APPLY" if do_writes else "DRY-RUN"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log, fh = make_logger(RUN_LOG)
    t0 = time.time()
    log("=" * 78)
    log(f"=== START {Path(__file__).name}  mode={mode}")
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")
    run_ts = utc_ts()
    decision: dict = {
        "script": SCRIPT_NUM, "run_ts": run_ts, "run_date": RUN_DATE,
        "mode": mode, "phases": {},
    }

    try:
        n_cpm = int(con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0])
        if n_cpm != 10871:
            raise RuntimeError(f"CPM rows {n_cpm} != 10871")
        before = replay_count(con)
        log(f"PREFLIGHT  CPM rows={n_cpm}  audit_§5.3 replay BEFORE: {before}")
        decision["phases"]["preflight"] = {"cpm_rows": n_cpm, "replay_before": before}

        if not do_writes:
            log("DRY-RUN — no mutations performed")
            log(f"=== END  elapsed={time.time()-t0:.1f}s")
            write_decision_log(DECISION_LOG, decision)
            fh.close()
            return

        ensure_archive_schema(con)
        ensure_audit_table(con)
        snap = f"canonical_patient_master_pre256_{run_ts}"
        snap_full = snapshot_table(
            con, CPM, snap, SCRIPT_TAG,
            "Pre-rebuild snapshot of any_confirmed_complication_flag and 9 "
            "per-entity comp_*_confirmed columns from full complication_phenotype_v1.",
        )
        log(f"SNAPSHOT  {snap_full}")
        decision["phases"]["snapshot"] = snap_full

        log("REBUILD  per-entity confirmed flags + aggregate")
        rebuild = rebuild_per_entity_and_aggregate(con, log)
        decision["phases"]["rebuild"] = rebuild

        log("DICTIONARY  promote per-entity + aggregate to status=authoritative")
        for col in ENTITY_COL_MAP.values():
            upsert_dict_entry(
                con, col, "authoritative", None,
                f"{SCRIPT_TAG} ({RUN_DATE}). Confirmed-flag rollup from "
                "complication_phenotype_v1 per (research_id, complication_entity). "
                "TRUE iff any phenotype row for this entity is confirmed.",
            )
        upsert_dict_entry(
            con, "any_confirmed_complication_flag", "authoritative", None,
            f"{SCRIPT_TAG} ({RUN_DATE}). Aggregate over all 9 complication "
            "entities — TRUE iff BOOL_OR(confirmed_flag=TRUE) per patient. "
            "Earlier rollup considered only 3 of 9 entities; rebuilt by Script 256.",
        )
        upsert_dict_entry(
            con, "n_confirmed_complications", "authoritative", None,
            f"{SCRIPT_TAG} ({RUN_DATE}). Distinct count of complication entities "
            "whose confirmed_flag=TRUE per patient.",
        )

        after = replay_count(con)
        log(f"VERIFY  audit_§5.3 replay AFTER: {after} (target=0)")
        decision["phases"]["replay_after"] = after

        record_audit(
            con, SCRIPT_NUM, "audit_5_3",
            "any_confirmed_complication_flag_undercount",
            count_before=before, count_after=after, target_after=0,
            status="OK" if after == 0 else "FAIL",
            notes=f"snapshot={snap_full}",
        )

        if after != 0:
            raise RuntimeError(
                f"Self-verify FAILED: §5.3 replay returned {after} (expected 0)"
            )

        n_after = int(con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0])
        if n_after != 10871:
            raise RuntimeError(f"CPM row count drifted: {n_after} != 10871")
        log(f"INVARIANT  CPM rows = {n_after}")
        log("ALL ASSERTIONS PASS")

    except Exception as exc:
        log(f"FATAL: {exc!r}")
        decision["error"] = str(exc)
        write_decision_log(DECISION_LOG, decision)
        fh.close()
        raise

    write_decision_log(DECISION_LOG, decision)
    log(f"=== END  elapsed={time.time()-t0:.1f}s")
    fh.close()


if __name__ == "__main__":
    main()
