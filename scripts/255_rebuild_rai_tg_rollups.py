#!/usr/bin/env python3
"""
Script 255 — Rebuild RAI dose + Tg lab rollups (audit §3.1, 3.3, 3.4)

Single-pass rebuild covering all three audit findings:

  - rai_max_dose_mci         (audit §3.1, 213 patients with CPM=0 vs detail >0)
      precedence: MAX(rai_treatment_episode_v2.dose_mci) -> rai_dose_v9
      provenance recorded in NEW column rai_max_dose_source

  - n_tg_measurements_structured / n_tgab_measurements (audit §3.3,
      1,637 / 1,755 patients undercounted)
      rebuild: COUNT(*) FILTER analyte={Tg, TgAb} per patient

  - tg_peak / tg_nadir / tg_mean (audit §3.4, 505/537 patients mismatched)
      rebuild: MAX/MIN/AVG(result_numeric) FILTER analyte='Tg'
      provenance recorded in NEW column tg_peak_source

Adds these CPM columns (idempotent):
  rai_max_dose_source  VARCHAR
  tg_peak_source       VARCHAR

Updates `data_dictionary_v240` rows so future consumers can stratify.

Self-verification asserts each of the four replay queries returns 0.

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
RUN_LOG = OUTPUT_DIR / "255_run.log"
DECISION_LOG = OUTPUT_DIR / "255_decision_log.json"
SCRIPT_TAG = "Script 255"
SCRIPT_NUM = "255"
RUN_DATE = "2026-04-16"

CPM = f'{PUBLICATION_DB}.main.canonical_patient_master'
RAI_EP = f'{PUBLICATION_DB}.main.rai_treatment_episode_v2'
TG = f'{PUBLICATION_DB}.main.thyroglobulin_lab_VIEW_v1'
DICT = f'{PUBLICATION_DB}.main.data_dictionary_v240'

REPLAY_RAI = f"""
WITH e AS (SELECT TRY_CAST(research_id AS INTEGER) AS rid, MAX(dose_mci) AS max_dose
           FROM {RAI_EP} GROUP BY 1)
SELECT COUNT(*) FROM {CPM} cpm
JOIN e ON TRY_CAST(cpm.research_id AS INTEGER) = e.rid
WHERE (cpm.rai_max_dose_mci = 0 OR cpm.rai_max_dose_mci IS NULL) AND e.max_dose > 0
"""
REPLAY_TG_COUNTS = f"""
WITH t AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid,
         COUNT(*) FILTER (WHERE analyte='Tg') AS n_tg,
         COUNT(*) FILTER (WHERE analyte='TgAb') AS n_tgab
  FROM {TG} GROUP BY 1
)
SELECT
  COUNT(*) FILTER (WHERE COALESCE(cpm.n_tg_measurements_structured,-1) <> COALESCE(t.n_tg,-1)),
  COUNT(*) FILTER (WHERE COALESCE(cpm.n_tgab_measurements,-1)         <> COALESCE(t.n_tgab,-1))
FROM {CPM} cpm
JOIN t ON TRY_CAST(cpm.research_id AS INTEGER) = t.rid
"""
REPLAY_TG_PEAK_NADIR = f"""
WITH t AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid,
         MAX(result_numeric) FILTER (WHERE analyte='Tg') AS tg_peak_c,
         MIN(result_numeric) FILTER (WHERE analyte='Tg') AS tg_nadir_c
  FROM {TG} GROUP BY 1
)
SELECT
  COUNT(*) FILTER (WHERE cpm.tg_peak  IS DISTINCT FROM t.tg_peak_c),
  COUNT(*) FILTER (WHERE cpm.tg_nadir IS DISTINCT FROM t.tg_nadir_c)
FROM {CPM} cpm
JOIN t ON TRY_CAST(cpm.research_id AS INTEGER) = t.rid
"""


def add_provenance_cols(con, log) -> dict:
    added = {}
    for col in ["rai_max_dose_source", "tg_peak_source"]:
        has = con.execute(f"""
            SELECT 1 FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='canonical_patient_master' AND column_name='{col}'
        """).fetchone()
        if has:
            log(f"  {col} already exists (idempotent)")
            added[col] = False
            continue
        con.execute(f"ALTER TABLE {CPM} ADD COLUMN {col} VARCHAR")
        log(f"  added column {col} VARCHAR")
        added[col] = True
    con.execute(f"""
        COMMENT ON COLUMN {CPM}.rai_max_dose_source IS
            '{SCRIPT_TAG} ({RUN_DATE}). Source contributing rai_max_dose_mci.
             Allowed: episode_v2 | rai_dose_v9 | none.'
    """)
    con.execute(f"""
        COMMENT ON COLUMN {CPM}.tg_peak_source IS
            '{SCRIPT_TAG} ({RUN_DATE}). Source contributing tg_peak.
             Currently always thyroglobulin_lab_VIEW_v1 when populated.'
    """)
    return added


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


def rebuild_rai_dose(con, log) -> int:
    """Rebuild rai_max_dose_mci using episode -> rai_dose_v9 fallback."""
    sql = f"""
    WITH e AS (
      SELECT TRY_CAST(research_id AS INTEGER) AS rid, MAX(dose_mci) AS ep_max
      FROM {RAI_EP} GROUP BY 1
    )
    UPDATE {CPM} AS cpm
    SET rai_max_dose_mci = CASE
            WHEN COALESCE(e.ep_max, 0) > 0 THEN e.ep_max
            WHEN cpm.rai_dose_v9 IS NOT NULL AND cpm.rai_dose_v9 > 0 THEN cpm.rai_dose_v9
            ELSE 0
        END,
        rai_max_dose_source = CASE
            WHEN COALESCE(e.ep_max, 0) > 0 THEN 'episode_v2'
            WHEN cpm.rai_dose_v9 IS NOT NULL AND cpm.rai_dose_v9 > 0 THEN 'rai_dose_v9'
            ELSE 'none'
        END
    FROM e
    WHERE TRY_CAST(cpm.research_id AS INTEGER) = e.rid
    """
    con.execute(sql)
    n = int(con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE rai_max_dose_mci > 0"
    ).fetchone()[0])
    log(f"  rai_max_dose_mci > 0 patients: {n}")
    return n


def rebuild_tg_rollups(con, log) -> dict:
    """Rebuild Tg/TgAb counts + tg_peak/nadir/mean."""
    sql = f"""
    WITH t AS (
      SELECT TRY_CAST(research_id AS INTEGER) AS rid,
             COUNT(*) FILTER (WHERE analyte='Tg')   AS n_tg,
             COUNT(*) FILTER (WHERE analyte='TgAb') AS n_tgab,
             MAX(result_numeric) FILTER (WHERE analyte='Tg') AS tg_peak_c,
             MIN(result_numeric) FILTER (WHERE analyte='Tg') AS tg_nadir_c,
             AVG(result_numeric) FILTER (WHERE analyte='Tg') AS tg_mean_c
      FROM {TG} GROUP BY 1
    )
    UPDATE {CPM} AS cpm
    SET n_tg_measurements_structured = COALESCE(t.n_tg, 0),
        n_tgab_measurements          = COALESCE(t.n_tgab, 0),
        tg_peak  = t.tg_peak_c,
        tg_nadir = t.tg_nadir_c,
        tg_mean  = t.tg_mean_c,
        tg_peak_source = CASE WHEN t.tg_peak_c IS NOT NULL
                              THEN 'thyroglobulin_lab_VIEW_v1'
                              ELSE NULL END
    FROM t
    WHERE TRY_CAST(cpm.research_id AS INTEGER) = t.rid
    """
    con.execute(sql)
    # Patients with NO entries in the lab canonical: zero out counts cleanly
    con.execute(f"""
        UPDATE {CPM}
        SET n_tg_measurements_structured = COALESCE(n_tg_measurements_structured, 0),
            n_tgab_measurements          = COALESCE(n_tgab_measurements,          0)
    """)
    n_tg_pop = int(con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE n_tg_measurements_structured > 0"
    ).fetchone()[0])
    n_peak_set = int(con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE tg_peak IS NOT NULL"
    ).fetchone()[0])
    log(f"  patients with n_tg > 0: {n_tg_pop}")
    log(f"  patients with tg_peak set: {n_peak_set}")
    return {"n_tg_populated": n_tg_pop, "n_peak_populated": n_peak_set}


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
        rai_b = int(con.execute(REPLAY_RAI).fetchone()[0])
        tg_c_b = con.execute(REPLAY_TG_COUNTS).fetchone()
        tg_pn_b = con.execute(REPLAY_TG_PEAK_NADIR).fetchone()
        log(f"PREFLIGHT  RAI dose mismatches: {rai_b}")
        log(f"           Tg count mismatches: tg={tg_c_b[0]} tgab={tg_c_b[1]}")
        log(f"           Tg peak/nadir mismatches: peak={tg_pn_b[0]} nadir={tg_pn_b[1]}")
        decision["phases"]["preflight"] = {
            "rai_dose_mismatch": rai_b,
            "tg_count_mismatch": list(tg_c_b),
            "tg_peak_nadir_mismatch": list(tg_pn_b),
        }

        if not do_writes:
            log("DRY-RUN — no mutations performed")
            log(f"=== END  elapsed={time.time()-t0:.1f}s")
            write_decision_log(DECISION_LOG, decision)
            fh.close()
            return

        ensure_archive_schema(con)
        ensure_audit_table(con)
        snap = f"canonical_patient_master_pre255_{run_ts}"
        snap_full = snapshot_table(
            con, CPM, snap, SCRIPT_TAG,
            "Pre-rebuild snapshot of RAI dose + Tg lab rollups "
            "(rai_max_dose_mci, n_tg_measurements_structured, "
            "n_tgab_measurements, tg_peak, tg_nadir, tg_mean) and addition of "
            "provenance columns rai_max_dose_source, tg_peak_source.",
        )
        log(f"SNAPSHOT  {snap_full}")
        decision["phases"]["snapshot"] = snap_full

        log("SCHEMA  add provenance cols if missing")
        added = add_provenance_cols(con, log)
        decision["phases"]["added_cols"] = added

        log("REBUILD  RAI dose with episode_v2 -> rai_dose_v9 fallback")
        n_rai = rebuild_rai_dose(con, log)
        decision["phases"]["rai"] = {"n_rai_dose_set": n_rai}

        log("REBUILD  Tg/TgAb counts + tg_peak/nadir/mean")
        tg_res = rebuild_tg_rollups(con, log)
        decision["phases"]["tg"] = tg_res

        log("DICTIONARY  upsert provenance + canonical-source rows")
        upsert_dict_entry(
            con, "rai_max_dose_source", "authoritative", None,
            f"{SCRIPT_TAG} ({RUN_DATE}). Source contributing rai_max_dose_mci. "
            "Allowed: episode_v2, rai_dose_v9, none.",
        )
        upsert_dict_entry(
            con, "tg_peak_source", "authoritative", None,
            f"{SCRIPT_TAG} ({RUN_DATE}). Source contributing tg_peak. "
            "Always thyroglobulin_lab_VIEW_v1 when populated.",
        )

        rai_a = int(con.execute(REPLAY_RAI).fetchone()[0])
        tg_c_a = con.execute(REPLAY_TG_COUNTS).fetchone()
        tg_pn_a = con.execute(REPLAY_TG_PEAK_NADIR).fetchone()
        log(f"VERIFY  RAI dose mismatches AFTER:        {rai_a}")
        log(f"        Tg count mismatches AFTER:        tg={tg_c_a[0]} tgab={tg_c_a[1]}")
        log(f"        Tg peak/nadir mismatches AFTER:   peak={tg_pn_a[0]} nadir={tg_pn_a[1]}")
        decision["phases"]["replay_after"] = {
            "rai_dose_mismatch": rai_a,
            "tg_count_mismatch": list(tg_c_a),
            "tg_peak_nadir_mismatch": list(tg_pn_a),
        }

        record_audit(con, SCRIPT_NUM, "audit_3_1", "rai_max_dose_mci_zero_gt0",
                     count_before=rai_b, count_after=rai_a,
                     target_after=0,
                     status="OK" if rai_a == 0 else "FAIL",
                     notes=f"snapshot={snap_full}")
        record_audit(con, SCRIPT_NUM, "audit_3_3", "n_tg_measurements_mismatch",
                     count_before=tg_c_b[0], count_after=tg_c_a[0],
                     target_after=0,
                     status="OK" if tg_c_a[0] == 0 else "FAIL")
        record_audit(con, SCRIPT_NUM, "audit_3_3", "n_tgab_measurements_mismatch",
                     count_before=tg_c_b[1], count_after=tg_c_a[1],
                     target_after=0,
                     status="OK" if tg_c_a[1] == 0 else "FAIL")
        record_audit(con, SCRIPT_NUM, "audit_3_4", "tg_peak_mismatch",
                     count_before=tg_pn_b[0], count_after=tg_pn_a[0],
                     target_after=0,
                     status="OK" if tg_pn_a[0] == 0 else "FAIL")
        record_audit(con, SCRIPT_NUM, "audit_3_4", "tg_nadir_mismatch",
                     count_before=tg_pn_b[1], count_after=tg_pn_a[1],
                     target_after=0,
                     status="OK" if tg_pn_a[1] == 0 else "FAIL")

        if rai_a != 0 or tg_c_a[0] != 0 or tg_c_a[1] != 0 \
                or tg_pn_a[0] != 0 or tg_pn_a[1] != 0:
            raise RuntimeError(
                f"Self-verify FAILED: rai={rai_a}, tg_counts={tg_c_a}, "
                f"tg_peak_nadir={tg_pn_a} (all expected 0)"
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
