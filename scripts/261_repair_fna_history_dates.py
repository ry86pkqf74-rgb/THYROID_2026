#!/usr/bin/env python3
"""
Script 261 - Repair fna_history two-digit-year dates.

Per Prompt 13 dry-run finding: 26 fna_history rows have parsed dates outside
[1995-01-01, CURRENT_DATE], all 26 propagate into fna_episode_master_v2.

Repair rule:
  yy = year % 100
  cutoff = CURRENT_YEAR - 2000  (= 26 in 2026)
  if year is missing/zero (BC) -> NULL + date_status='unresolved_date'
  elif yy <= cutoff -> 2000+yy
  else if 1900+yy >= 1995 -> 1900+yy
  else -> NULL + date_status='unresolved_date'

The 2029-11-17 row is post-CURRENT_DATE; its yy=29 > 26 yields 1929 < 1995 ->
NULL + unresolved_date (no signal to disambiguate).

Mutations (apply mode):
  fna_history.fna_date_parsed: replace with YYYY-MM-DD repaired string or NULL.
  fna_history.date: same repair (it is the source string).
  fna_episode_master_v2.resolved_fna_date: rewrite for the 26 affected episodes.
    fna_date_native is left untouched per spec.
    date_status='unresolved_date' for episodes with NULL repaired date.
  canonical_patient_master.prm_first_fna_date / prm_last_fna_date and the two
    *_days_from_surg derivatives are re-rolled-up from the canonical detail.

Snapshots: fna_history, fna_episode_master_v2, canonical_patient_master.
Per-row audit: scripts/output/261_date_repair.json.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    ensure_audit_table, ensure_archive_schema, make_logger,
    record_audit, snapshot_table, utc_ts, write_decision_log,
)

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
RUN_LOG = OUTPUT_DIR / "261_run.log"
DECISION_LOG = OUTPUT_DIR / "261_decision_log.json"
AUDIT_JSON = OUTPUT_DIR / "261_date_repair.json"
SCRIPT_TAG = "Script 261"
SCRIPT_NUM = "261"
RUN_DATE = "2026-04-17"

CPM = f'{PUBLICATION_DB}.main.canonical_patient_master'
FEM = f'{PUBLICATION_DB}.main.fna_episode_master_v2'
FHIST = f'{PUBLICATION_DB}.main.fna_history'
EXPECTED_FEM_ROWS = 8119
EXPECTED_CPM_ROWS = 10871
CURRENT_YEAR = datetime.now(timezone.utc).year


# ---------------------------------------------------------------------------
# Repair logic (pure python, applied row-by-row)
# ---------------------------------------------------------------------------
def repair_year(year: int | None, yy_cutoff: int) -> tuple[int | None, str]:
    """Return (new_year, rule). new_year is None when no signal."""
    if year is None:
        return None, "null_year_no_repair"
    if year == 0:
        return None, "year_zero_BC_unresolved"
    yy = year % 100
    if yy <= yy_cutoff:
        candidate = 2000 + yy
        return candidate, f"yy_{yy:02d}_le_cutoff_{yy_cutoff:02d}_2000"
    candidate = 1900 + yy
    if candidate >= 1995:
        return candidate, f"yy_{yy:02d}_gt_cutoff_{yy_cutoff:02d}_1900"
    return None, f"yy_{yy:02d}_resolves_pre1995_unresolved"


def parse_iso_date(s: str | None):
    if s is None:
        return None
    s = s.strip()
    if not s or s.startswith("0000"):
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def build_audit(con, log) -> list[dict]:
    """Pull bad fna_history rows + matching fna_episode_master_v2 rows."""
    rows = con.execute(f"""
        SELECT h.research_id, h.fna_index, h.date AS old_date_str,
               h.fna_date_parsed AS old_parsed_str
        FROM {FHIST} h
        WHERE TRY_CAST(h.fna_date_parsed AS DATE) IS NOT NULL
          AND (EXTRACT(year FROM TRY_CAST(h.fna_date_parsed AS DATE)) < 1995
               OR TRY_CAST(h.fna_date_parsed AS DATE) > CURRENT_DATE)
        ORDER BY h.research_id, h.fna_index
    """).fetchall()

    yy_cutoff = CURRENT_YEAR - 2000
    log(f"  yy_cutoff = CURRENT_YEAR ({CURRENT_YEAR}) - 2000 = {yy_cutoff:02d}")
    log(f"  found {len(rows)} bad fna_history rows")

    audit = []
    for rid, fi, old_date_str, old_parsed_str in rows:
        old = parse_iso_date(old_parsed_str) or parse_iso_date(old_date_str)
        old_year_full = None
        if old is not None:
            old_year_full = old.year
        elif old_parsed_str and old_parsed_str.startswith("0000"):
            old_year_full = 0

        if old is None and old_year_full is None:
            new_year, rule = None, "no_parseable_date"
            new_iso = None
        else:
            new_year, rule = repair_year(old_year_full, yy_cutoff)
            if new_year is None:
                new_iso = None
            else:
                mm = old.month if old is not None else int(old_parsed_str[5:7])
                dd = old.day if old is not None else int(old_parsed_str[8:10])
                try:
                    new_iso = datetime(new_year, mm, dd).date().isoformat()
                except ValueError:
                    new_iso = None
                    rule += "_invalid_md_drop"

        audit.append({
            "research_id": int(rid) if rid is not None else None,
            "fna_index": int(fi) if fi is not None else None,
            "old_date_str": old_date_str,
            "old_parsed_str": old_parsed_str,
            "old_year_full": old_year_full,
            "new_iso_date": new_iso,
            "rule_applied": rule,
        })
    return audit


def get_episode_ids_for_audit(con, audit: list[dict]) -> list[dict]:
    """For each audit row, look up the matching fna_episode_master_v2 row(s)."""
    out = []
    for a in audit:
        rid = a["research_id"]
        # fem rows whose source_table='fna_history' and current resolved_fna_date is bad
        rows = con.execute(f"""
            SELECT fna_episode_id,
                   CAST(resolved_fna_date AS VARCHAR) AS rfd,
                   CAST(fna_date_native   AS VARCHAR) AS fdn
              FROM {FEM}
             WHERE TRY_CAST(research_id AS INTEGER) = ?
               AND source_table = 'fna_history'
               AND (EXTRACT(year FROM resolved_fna_date) < 1995
                    OR resolved_fna_date > CURRENT_DATE)
        """, [rid]).fetchall()
        a["affected_fna_episode_ids"] = [int(r[0]) for r in rows]
        a["pre_resolved"] = [r[1] for r in rows]
        a["pre_native"] = [r[2] for r in rows]
        out.append(a)
    return out


def apply_repairs(con, log, audit: list[dict]) -> dict:
    """Apply fna_history + fna_episode_master_v2 repairs."""
    n_hist_set = 0
    n_hist_null = 0
    n_fem_set = 0
    n_fem_null = 0
    for a in audit:
        rid = a["research_id"]
        fi = a["fna_index"]
        new_iso = a["new_iso_date"]
        if new_iso is not None:
            con.execute(
                f"UPDATE {FHIST} SET fna_date_parsed = ?, date = ? "
                "WHERE research_id = ? AND fna_index = ?",
                [new_iso, new_iso, rid, fi],
            )
            n_hist_set += 1
        else:
            con.execute(
                f"UPDATE {FHIST} SET fna_date_parsed = NULL "
                "WHERE research_id = ? AND fna_index = ?",
                [rid, fi],
            )
            n_hist_null += 1

        for ep_id in a.get("affected_fna_episode_ids", []):
            if new_iso is not None:
                con.execute(
                    f"UPDATE {FEM} SET resolved_fna_date = CAST(? AS DATE) "
                    "WHERE fna_episode_id = ?",
                    [new_iso, ep_id],
                )
                n_fem_set += 1
            else:
                con.execute(
                    f"UPDATE {FEM} SET resolved_fna_date = NULL, "
                    "    date_status = 'unresolved_date' "
                    "WHERE fna_episode_id = ?",
                    [ep_id],
                )
                n_fem_null += 1
    log(f"  fna_history rows updated:           set={n_hist_set} nulled={n_hist_null}")
    log(f"  fna_episode_master_v2 rows updated: set={n_fem_set} nulled={n_fem_null}")
    return {"hist_set": n_hist_set, "hist_null": n_hist_null,
            "fem_set": n_fem_set, "fem_null": n_fem_null}


def rebuild_cpm_fna_dates(con, log) -> dict:
    """Re-roll-up prm_first_fna_date / prm_last_fna_date and *_days_from_surg."""
    sql = f"""
    WITH ep AS (
      SELECT TRY_CAST(research_id AS INTEGER) AS rid,
             MIN(resolved_fna_date) AS first_dt,
             MAX(resolved_fna_date) AS last_dt
        FROM {FEM}
       GROUP BY 1
    )
    UPDATE {CPM} AS cpm
       SET prm_first_fna_date = ep.first_dt,
           prm_last_fna_date  = ep.last_dt,
           prm_first_fna_days_from_surg = CASE
             WHEN ep.first_dt IS NULL OR cpm.first_surgery_date IS NULL THEN NULL
             ELSE CAST(ep.first_dt AS DATE)
                  - CAST(cpm.first_surgery_date AS DATE)
           END,
           prm_last_fna_days_from_surg = CASE
             WHEN ep.last_dt IS NULL OR cpm.first_surgery_date IS NULL THEN NULL
             ELSE CAST(ep.last_dt AS DATE)
                  - CAST(cpm.first_surgery_date AS DATE)
           END
      FROM ep
     WHERE TRY_CAST(cpm.research_id AS INTEGER) = ep.rid
    """
    con.execute(sql)
    pop = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE prm_first_fna_date IS NOT NULL) AS first_pop,
          COUNT(*) FILTER (WHERE prm_last_fna_date  IS NOT NULL) AS last_pop,
          COUNT(*) FILTER (WHERE prm_first_fna_days_from_surg IS NOT NULL) AS first_dfs_pop,
          COUNT(*) FILTER (WHERE prm_last_fna_days_from_surg  IS NOT NULL) AS last_dfs_pop
        FROM {CPM}
    """).fetchone()
    log(f"  prm_first_fna_date pop:           {pop[0]}")
    log(f"  prm_last_fna_date  pop:           {pop[1]}")
    log(f"  prm_first_fna_days_from_surg pop: {pop[2]}")
    log(f"  prm_last_fna_days_from_surg  pop: {pop[3]}")
    return {"first_pop": int(pop[0]), "last_pop": int(pop[1]),
            "first_dfs_pop": int(pop[2]), "last_dfs_pop": int(pop[3])}


def assert_invariants(con, log) -> dict:
    rem_fem = con.execute(f"""
        SELECT COUNT(*) FROM {FEM}
        WHERE resolved_fna_date IS NOT NULL
          AND (EXTRACT(year FROM resolved_fna_date) < 1995
               OR resolved_fna_date > CURRENT_DATE)
    """).fetchone()[0]
    rem_hist = con.execute(f"""
        SELECT COUNT(*) FROM {FHIST}
        WHERE TRY_CAST(fna_date_parsed AS DATE) IS NOT NULL
          AND (EXTRACT(year FROM TRY_CAST(fna_date_parsed AS DATE)) < 1995
               OR TRY_CAST(fna_date_parsed AS DATE) > CURRENT_DATE)
    """).fetchone()[0]
    n_fem = con.execute(f"SELECT COUNT(*) FROM {FEM}").fetchone()[0]
    n_cpm = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    if rem_fem != 0:
        raise RuntimeError(f"FEM still has {rem_fem} bad resolved_fna_date rows")
    if rem_hist != 0:
        raise RuntimeError(f"fna_history still has {rem_hist} bad fna_date_parsed rows")
    if n_fem != EXPECTED_FEM_ROWS:
        raise RuntimeError(f"FEM rowcount drifted: {n_fem} != {EXPECTED_FEM_ROWS}")
    if n_cpm != EXPECTED_CPM_ROWS:
        raise RuntimeError(f"CPM rowcount drifted: {n_cpm} != {EXPECTED_CPM_ROWS}")
    minmax = con.execute(f"""
        SELECT MIN(resolved_fna_date), MAX(resolved_fna_date) FROM {FEM}
    """).fetchone()
    log(f"  FEM rows={n_fem}; CPM rows={n_cpm}; "
        f"resolved_fna_date min={minmax[0]} max={minmax[1]}")
    return {"rem_fem": rem_fem, "rem_hist": rem_hist,
            "fem_rows": n_fem, "cpm_rows": n_cpm,
            "min": minmax[0], "max": minmax[1]}


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
        log("PREFLIGHT  build per-row repair plan")
        audit = build_audit(con, log)
        audit = get_episode_ids_for_audit(con, audit)
        decision["phases"]["preflight"] = {
            "n_audit_rows": len(audit),
            "n_unresolved": sum(1 for a in audit if a["new_iso_date"] is None),
        }
        AUDIT_JSON.write_text(
            json.dumps({"audit": audit, "yy_cutoff": CURRENT_YEAR - 2000,
                        "run_ts": run_ts}, indent=2, default=str),
            encoding="utf-8",
        )
        log(f"  per-row audit written to {AUDIT_JSON.relative_to(REPO)}")
        for a in audit[:5]:
            log(f"    rid={a['research_id']} idx={a['fna_index']} "
                f"old={a['old_parsed_str']} -> new={a['new_iso_date']} "
                f"rule={a['rule_applied']}")

        if not do_writes:
            log("DRY-RUN -- no mutations performed")
            log(f"=== END  elapsed={time.time()-t0:.1f}s")
            write_decision_log(DECISION_LOG, decision)
            fh.close()
            return

        ensure_archive_schema(con)
        ensure_audit_table(con)
        snap_h = snapshot_table(
            con, FHIST, f"fna_history_pre261_{run_ts}", SCRIPT_TAG,
            "Pre-mutation snapshot of fna_history before two-digit-year date repair "
            "(26 rows out-of-range).")
        snap_e = snapshot_table(
            con, FEM, f"fna_episode_master_v2_pre261_{run_ts}", SCRIPT_TAG,
            "Pre-mutation snapshot of fna_episode_master_v2 before resolved_fna_date "
            "rewrite for 26 episodes derived from fna_history bad-year rows.")
        snap_c = snapshot_table(
            con, CPM, f"canonical_patient_master_pre261_{run_ts}", SCRIPT_TAG,
            "Pre-mutation snapshot of canonical_patient_master before re-rolling "
            "prm_first_fna_date / prm_last_fna_date / prm_*_days_from_surg from "
            "the repaired fna_episode_master_v2.")
        log(f"SNAPSHOTS  fhist={snap_h}")
        log(f"           fem  ={snap_e}")
        log(f"           cpm  ={snap_c}")
        decision["phases"]["snapshots"] = {"fhist": snap_h, "fem": snap_e, "cpm": snap_c}

        log("REPAIR  apply per-row date corrections")
        apply_meta = apply_repairs(con, log, audit)
        decision["phases"]["apply"] = apply_meta

        log("REROLL  re-derive CPM prm_first/last_fna_date + *_days_from_surg")
        cpm_meta = rebuild_cpm_fna_dates(con, log)
        decision["phases"]["cpm_reroll"] = cpm_meta

        log("INVARIANTS")
        inv = assert_invariants(con, log)
        decision["phases"]["invariants"] = inv

        record_audit(
            con, SCRIPT_NUM, "prompt13_fna_history_dates",
            "fem_bad_dates_remaining",
            count_before=26, count_after=int(inv["rem_fem"]),
            target_after=0, status="OK",
            notes=f"snap={snap_e}; n_unresolved={decision['phases']['preflight']['n_unresolved']}",
        )
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
