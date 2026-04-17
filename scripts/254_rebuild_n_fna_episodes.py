#!/usr/bin/env python3
"""
Script 254 — Rebuild n_fna_episodes from fna_episode_master_v2 (audit §2.2)

The current CPM rollup has a broadcast/cartesian leak — 5,012 of 5,266 FNA
patients show n_fna_episodes ∈ {11, 12} when the actual per-patient COUNT
is almost always 1-3. Same pass also rebuilds n_fna_cytology_records,
prm_first_fna_date, prm_last_fna_date directly from the canonical sources.

Per audit §2.2 recommendation: ADD COLUMN `worst_bethesda_source` VARCHAR
to track which source produced `worst_bethesda_num` (without changing the
existing value). Allowed values: episode_master | cytology | nlp | adjudicated.

Mutations:
  ALTER TABLE canonical_patient_master ADD COLUMN IF NOT EXISTS worst_bethesda_source VARCHAR;
  UPDATE canonical_patient_master  -- one row per patient, four rollups
    SET n_fna_episodes,
        n_fna_cytology_records,
        prm_first_fna_date,
        prm_last_fna_date,
        worst_bethesda_source

Self-verification queries assert:
  - 0 patients where n_fna_episodes != COUNT from fna_episode_master_v2
  - CPM rows still 10,871

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
RUN_LOG = OUTPUT_DIR / "254_run.log"
DECISION_LOG = OUTPUT_DIR / "254_decision_log.json"
SCRIPT_TAG = "Script 254"
SCRIPT_NUM = "254"
RUN_DATE = "2026-04-16"
CPM = f'{PUBLICATION_DB}.main.canonical_patient_master'
EPI = f'{PUBLICATION_DB}.main.fna_episode_master_v2'
CYT = f'{PUBLICATION_DB}.main.fna_cytology'
DICT = f'{PUBLICATION_DB}.main.data_dictionary_v240'

REPLAY_SQL = f"""
WITH ep AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid, COUNT(*) AS n_ep
  FROM {EPI} GROUP BY 1
)
SELECT COUNT(*)
FROM {CPM} cpm
JOIN ep ON TRY_CAST(cpm.research_id AS INTEGER) = ep.rid
WHERE COALESCE(cpm.n_fna_episodes, -1) <> ep.n_ep
"""


def replay_count(con) -> int:
    return int(con.execute(REPLAY_SQL).fetchone()[0])


def add_bethesda_source_col(con, log) -> bool:
    has = con.execute(f"""
        SELECT 1 FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
          AND column_name='worst_bethesda_source'
    """).fetchone()
    if has:
        log("  worst_bethesda_source column already exists (idempotent)")
        return False
    con.execute(f"ALTER TABLE {CPM} ADD COLUMN worst_bethesda_source VARCHAR")
    con.execute(f"""
        COMMENT ON COLUMN {CPM}.worst_bethesda_source IS
            '{SCRIPT_TAG} ({RUN_DATE}). Source contributing the value of
             worst_bethesda_num. Allowed: episode_master | cytology |
             cytology_2010 | cytology_2015 | cytology_2023 | none.
             Added per audit §2.2 recommendation; existing worst_bethesda_num
             values are NOT modified.'
    """)
    log("  added column worst_bethesda_source VARCHAR + comment")
    return True


def upsert_dict_entry(con, column_name: str, status: str,
                      replacement: str | None, description: str) -> None:
    """Insert or update a row in data_dictionary_v240 for the given column."""
    n = con.execute(
        f"SELECT COUNT(*) FROM {DICT} WHERE column_name = ?",
        [column_name],
    ).fetchone()[0]
    if n == 0:
        # Need to discover existing schema; insert minimally.
        cols = [r[0] for r in con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='data_dictionary_v240'
            ORDER BY ordinal_position
        """).fetchall()]
        # Build a minimal value map keyed by likely column names.
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
            f"""UPDATE {DICT} SET status = ?,
                replacement_column_name = ?,
                description = COALESCE(description, '') ||
                  CASE WHEN COALESCE(description,'') = '' THEN '' ELSE ' ' END || ?
                WHERE column_name = ?""",
            [status, replacement, description, column_name],
        )


def rebuild_rollups(con, log) -> dict:
    """Single UPDATE that rebuilds 4 rollups + populates worst_bethesda_source."""
    sql = f"""
    WITH ep AS (
      SELECT TRY_CAST(research_id AS INTEGER) AS rid,
             COUNT(*) AS n_ep,
             MIN(COALESCE(resolved_fna_date, fna_date_native)) AS first_dt,
             MAX(COALESCE(resolved_fna_date, fna_date_native)) AS last_dt,
             MAX(bethesda_category) AS max_eth_episode
      FROM {EPI} GROUP BY 1
    ),
    cyt AS (
      SELECT TRY_CAST(research_id AS INTEGER) AS rid,
             COUNT(*) AS n_cyt,
             MAX(GREATEST(
               COALESCE(bethesda_2023_num, 0),
               COALESCE(bethesda_2015_num, 0),
               COALESCE(bethesda_2010_num, 0),
               COALESCE(category_num, 0)
             )) AS max_eth_cyt
      FROM {CYT} GROUP BY 1
    ),
    j AS (
      SELECT COALESCE(ep.rid, cyt.rid) AS rid,
             COALESCE(ep.n_ep, 0) AS n_ep,
             COALESCE(cyt.n_cyt, 0) AS n_cyt,
             ep.first_dt, ep.last_dt,
             COALESCE(ep.max_eth_episode, 0) AS max_eth_episode,
             COALESCE(cyt.max_eth_cyt, 0)    AS max_eth_cyt
      FROM ep
      FULL OUTER JOIN cyt ON ep.rid = cyt.rid
    )
    UPDATE {CPM} AS cpm
    SET n_fna_episodes        = j.n_ep,
        n_fna_cytology_records = j.n_cyt,
        prm_first_fna_date    = j.first_dt,
        prm_last_fna_date     = j.last_dt,
        worst_bethesda_source = CASE
          WHEN cpm.worst_bethesda_num IS NULL THEN NULL
          WHEN j.max_eth_episode = j.max_eth_cyt
               AND j.max_eth_episode = cpm.worst_bethesda_num
               AND j.max_eth_episode > 0 THEN 'episode_master+cytology'
          WHEN j.max_eth_episode = cpm.worst_bethesda_num AND j.max_eth_episode > 0 THEN 'episode_master'
          WHEN j.max_eth_cyt     = cpm.worst_bethesda_num AND j.max_eth_cyt     > 0 THEN 'cytology'
          ELSE 'other'
        END
    FROM j
    WHERE TRY_CAST(cpm.research_id AS INTEGER) = j.rid
    """
    con.execute(sql)
    # Patients who don't appear in either source: zero out the counts
    # (preserving NULL dates is semantically OK).
    con.execute(f"""
        UPDATE {CPM}
        SET n_fna_episodes = COALESCE(n_fna_episodes, 0),
            n_fna_cytology_records = COALESCE(n_fna_cytology_records, 0)
        WHERE n_fna_episodes IS NULL OR n_fna_cytology_records IS NULL
    """)

    n_eps = int(con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE n_fna_episodes > 0"
    ).fetchone()[0])
    n_src_set = int(con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE worst_bethesda_source IS NOT NULL"
    ).fetchone()[0])
    log(f"  patients with n_fna_episodes > 0: {n_eps}")
    log(f"  patients with worst_bethesda_source populated: {n_src_set}")
    return {"n_with_episodes": n_eps, "n_bethesda_source_set": n_src_set}


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
            raise RuntimeError(f"CPM rows {n_cpm} != 10871; aborting")
        before = replay_count(con)
        log(f"PREFLIGHT  CPM rows={n_cpm}  audit_§2.2 replay BEFORE: {before}")
        decision["phases"]["preflight"] = {"cpm_rows": n_cpm, "replay_before": before}

        if not do_writes:
            log("DRY-RUN — no mutations performed")
            log(f"=== END  elapsed={time.time()-t0:.1f}s")
            write_decision_log(DECISION_LOG, decision)
            fh.close()
            return

        ensure_archive_schema(con)
        ensure_audit_table(con)
        snap = f"canonical_patient_master_pre254_{run_ts}"
        snap_full = snapshot_table(
            con, CPM, snap, SCRIPT_TAG,
            "Pre-rebuild snapshot of FNA rollups (n_fna_episodes, "
            "n_fna_cytology_records, prm_first_fna_date, prm_last_fna_date) "
            "and addition of worst_bethesda_source provenance column.",
        )
        log(f"SNAPSHOT  {snap_full}")
        decision["phases"]["snapshot"] = snap_full

        log("SCHEMA  add worst_bethesda_source column if missing")
        added = add_bethesda_source_col(con, log)
        decision["phases"]["added_col"] = added

        log("REBUILD  4 FNA rollups + worst_bethesda_source")
        upd = rebuild_rollups(con, log)
        decision["phases"]["rebuild"] = upd

        log("DICTIONARY  upsert worst_bethesda_source row")
        upsert_dict_entry(
            con, "worst_bethesda_source",
            status="authoritative",
            replacement=None,
            description=f"{SCRIPT_TAG} ({RUN_DATE}). Source contributing "
                        "worst_bethesda_num. Allowed: episode_master, cytology, "
                        "episode_master+cytology, other, NULL when "
                        "worst_bethesda_num is NULL.",
        )

        after = replay_count(con)
        log(f"VERIFY  audit_§2.2 replay AFTER: {after} (target=0)")
        decision["phases"]["replay_after"] = after

        record_audit(
            con, SCRIPT_NUM, "audit_2_2",
            "n_fna_episodes_mismatch",
            count_before=before, count_after=after, target_after=0,
            status="OK" if after == 0 else "FAIL",
            notes=f"snapshot={snap_full}",
        )

        if after != 0:
            raise RuntimeError(
                f"Self-verify FAILED: §2.2 replay returned {after} (expected 0)"
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
