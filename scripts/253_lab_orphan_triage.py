#!/usr/bin/env python3
"""
Script 253 — Triage 537 lab-orphan patients (audit §2.1)

These 537 research_ids appear in `thyroglobulin_lab_canonical_v1` and
`longitudinal_lab_canonical_v1` but NOT in `canonical_patient_master`.
Per §7.3 of the audit, the same non-cancer-cohort test that exonerated
the 635 operative orphans applies here:

  for each orphan research_id, check presence in every cancer-evidence table
  (fna_episode_master_v2, tumor_episode_master_v2, synoptic_tumor_long_v1,
   path_synoptics, imaging_nodule_master_v1, operative_episode_detail_v2)

Decision:
  - ZERO cancer evidence  -> archive the lab rows + DELETE from publication DB
  - ANY cancer evidence   -> surface in manuscript_workspace.lab_orphan_cohort_review_v1
                              (one row per patient with summary), do NOT delete

This script does both pieces in a single pass:
  1. Build the per-orphan cancer-evidence matrix.
  2. Snapshot both lab tables AND the orphan slice to archive_pub_v1_0.
  3. DELETE zero-evidence orphans from both lab tables.
  4. CREATE OR REPLACE manuscript_workspace.lab_orphan_cohort_review_v1
     for any-evidence orphans (human review).
  5. Re-run audit replay; persist before/after counts.

Default mode is --dry-run. Pass --apply to mutate.
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
    ARCHIVE_QUALIFIED, ensure_archive_schema, ensure_audit_table,
    make_logger, record_audit, snapshot_table, utc_ts, write_decision_log,
)

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
RUN_LOG = OUTPUT_DIR / "253_run.log"
DECISION_LOG = OUTPUT_DIR / "253_decision_log.json"
SCRIPT_TAG = "Script 253"
SCRIPT_NUM = "253"
RUN_DATE = "2026-04-16"

CPM = f'{PUBLICATION_DB}.main.canonical_patient_master'
TG = f'{PUBLICATION_DB}.main.thyroglobulin_lab_canonical_v1'
LONG_LAB = f'{PUBLICATION_DB}.main.longitudinal_lab_canonical_v1'
EVIDENCE_TABLES = [
    f"{PUBLICATION_DB}.main.fna_episode_master_v2",
    f"{PUBLICATION_DB}.main.tumor_episode_master_v2",
    f"{PUBLICATION_DB}.main.synoptic_tumor_long_v1",
    f"{PUBLICATION_DB}.main.path_synoptics",
    f"{PUBLICATION_DB}.main.imaging_nodule_master_v1",
    f"{PUBLICATION_DB}.main.operative_episode_detail_v2",
]


def orphan_count(con, lab_table: str) -> int:
    return int(con.execute(f"""
        SELECT COUNT(DISTINCT TRY_CAST(research_id AS INTEGER))
        FROM {lab_table}
        WHERE TRY_CAST(research_id AS INTEGER) NOT IN (
          SELECT TRY_CAST(research_id AS INTEGER) FROM {CPM}
          WHERE research_id IS NOT NULL
        )
    """).fetchone()[0])


def build_evidence_matrix(con) -> list[tuple]:
    """
    Return list of (rid, fna, tumor, syn, path, imaging, op) tuples for
    every orphan research_id (union across both lab tables).
    """
    sql = f"""
    WITH cpm_rids AS (
      SELECT TRY_CAST(research_id AS INTEGER) AS rid
      FROM {CPM} WHERE research_id IS NOT NULL
    ),
    orphan_rids AS (
      SELECT DISTINCT rid FROM (
        SELECT TRY_CAST(research_id AS INTEGER) AS rid FROM {TG}
        UNION
        SELECT TRY_CAST(research_id AS INTEGER) AS rid FROM {LONG_LAB}
      ) u
      WHERE rid IS NOT NULL
        AND rid NOT IN (SELECT rid FROM cpm_rids)
    )
    SELECT o.rid,
           EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[0]} x WHERE TRY_CAST(x.research_id AS INTEGER) = o.rid) AS fna,
           EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[1]} x WHERE TRY_CAST(x.research_id AS INTEGER) = o.rid) AS tum,
           EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[2]} x WHERE TRY_CAST(x.research_id AS INTEGER) = o.rid) AS syn,
           EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[3]} x WHERE TRY_CAST(x.research_id AS INTEGER) = o.rid) AS ps,
           EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[4]} x WHERE TRY_CAST(x.research_id AS INTEGER) = o.rid) AS img,
           EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[5]} x WHERE TRY_CAST(x.research_id AS INTEGER) = o.rid) AS op
    FROM orphan_rids o
    ORDER BY o.rid
    """
    return con.execute(sql).fetchall()


def write_review_view(con, any_evidence_rids: list[int], log) -> int:
    """Create manuscript_workspace.lab_orphan_cohort_review_v1 with summary rows."""
    if not any_evidence_rids:
        log("  no any-evidence orphans — view will be empty (created with 0 rows)")
        rid_filter = "1=0"
    else:
        rid_csv = ",".join(str(int(r)) for r in any_evidence_rids)
        rid_filter = f"TRY_CAST(t.research_id AS INTEGER) IN ({rid_csv})"

    con.execute("CREATE SCHEMA IF NOT EXISTS manuscript_workspace")
    con.execute("DROP VIEW IF EXISTS manuscript_workspace.lab_orphan_cohort_review_v1")
    con.execute("DROP TABLE IF EXISTS manuscript_workspace.lab_orphan_cohort_review_v1")

    # Materialize as TABLE (not view) so the snapshot stays stable even if
    # downstream lab tables change.
    con.execute(f"""
        CREATE TABLE manuscript_workspace.lab_orphan_cohort_review_v1 AS
        WITH tg AS (
          SELECT TRY_CAST(research_id AS INTEGER) AS rid,
                 COUNT(*) AS n_tg_rows,
                 MIN(specimen_collect_dt) AS first_tg_dt,
                 MAX(specimen_collect_dt) AS last_tg_dt
          FROM {TG} t
          WHERE {rid_filter}
          GROUP BY 1
        ),
        ll AS (
          SELECT TRY_CAST(research_id AS INTEGER) AS rid,
                 COUNT(*) AS n_long_rows
          FROM {LONG_LAB} t
          WHERE {rid_filter}
          GROUP BY 1
        ),
        evidence AS (
          SELECT
            t1.research_id::INTEGER AS rid,
            EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[0]} x WHERE TRY_CAST(x.research_id AS INTEGER) = t1.research_id::INTEGER) AS has_fna,
            EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[1]} x WHERE TRY_CAST(x.research_id AS INTEGER) = t1.research_id::INTEGER) AS has_tumor,
            EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[2]} x WHERE TRY_CAST(x.research_id AS INTEGER) = t1.research_id::INTEGER) AS has_syn,
            EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[3]} x WHERE TRY_CAST(x.research_id AS INTEGER) = t1.research_id::INTEGER) AS has_path,
            EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[4]} x WHERE TRY_CAST(x.research_id AS INTEGER) = t1.research_id::INTEGER) AS has_imaging,
            EXISTS (SELECT 1 FROM {EVIDENCE_TABLES[5]} x WHERE TRY_CAST(x.research_id AS INTEGER) = t1.research_id::INTEGER) AS has_op
          FROM (SELECT DISTINCT TRY_CAST(t.research_id AS INTEGER) AS research_id
                FROM {TG} t WHERE {rid_filter}) t1
        )
        SELECT e.rid AS research_id,
               e.has_fna, e.has_tumor, e.has_syn,
               e.has_path, e.has_imaging, e.has_op,
               COALESCE(tg.n_tg_rows, 0) AS n_tg_rows,
               COALESCE(ll.n_long_rows, 0) AS n_long_lab_rows,
               tg.first_tg_dt, tg.last_tg_dt,
               'PENDING_HUMAN_REVIEW' AS triage_status,
               CURRENT_TIMESTAMP AS surfaced_at
        FROM evidence e
        LEFT JOIN tg ON tg.rid = e.rid
        LEFT JOIN ll ON ll.rid = e.rid
        ORDER BY e.rid
    """)
    con.execute(f"""
        COMMENT ON TABLE manuscript_workspace.lab_orphan_cohort_review_v1 IS
            '{SCRIPT_TAG} ({RUN_DATE}). Lab orphans (research_ids in lab canonicals
             but not in canonical_patient_master) that have ANY cancer-evidence
             record (FNA, tumor episode, synoptic, path, imaging, operative).
             These were NOT auto-archived; they require human cohort decision
             before re-admission to CPM or deletion.'
    """)
    n = int(con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.lab_orphan_cohort_review_v1"
    ).fetchone()[0])
    log(f"  manuscript_workspace.lab_orphan_cohort_review_v1 rows: {n}")
    return n


def archive_orphan_slice(con, lab_table: str, dest_name: str,
                         rids_to_delete: list[int], log) -> str:
    """
    Snapshot the orphan-slice rows (the rows we're about to delete) to
    archive_pub_v1_0.<dest_name>.
    """
    ensure_archive_schema(con)
    dest_full = f'{ARCHIVE_QUALIFIED}."{dest_name}"'
    con.execute(f"DROP TABLE IF EXISTS {dest_full}")
    if not rids_to_delete:
        # Empty snapshot — still create it for traceability
        con.execute(f"CREATE TABLE {dest_full} AS SELECT * FROM {lab_table} WHERE 1=0")
        log(f"  EMPTY snapshot {dest_full} (no zero-evidence orphans)")
        return dest_full
    rid_csv = ",".join(str(int(r)) for r in rids_to_delete)
    con.execute(f"""
        CREATE TABLE {dest_full} AS
        SELECT * FROM {lab_table}
        WHERE TRY_CAST(research_id AS INTEGER) IN ({rid_csv})
    """)
    n = int(con.execute(f"SELECT COUNT(*) FROM {dest_full}").fetchone()[0])
    con.execute(f"""
        COMMENT ON TABLE {dest_full} IS
            '{SCRIPT_TAG} ({RUN_DATE}). Pre-DELETE snapshot of orphan rows
             (research_ids not in canonical_patient_master AND zero
             cancer-evidence rows in any of the 6 evidence tables).
             Source: {lab_table}. Rows: {n}.'
    """)
    log(f"  snapshot {dest_full} ({n} rows)")
    return dest_full


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
        # ---- preflight ----
        n_tg_orphans = orphan_count(con, TG)
        n_long_orphans = orphan_count(con, LONG_LAB)
        log(f"PREFLIGHT  Tg orphans: {n_tg_orphans}  longitudinal orphans: {n_long_orphans}")
        decision["phases"]["preflight"] = {
            "tg_orphans": n_tg_orphans,
            "long_orphans": n_long_orphans,
        }

        # ---- evidence matrix ----
        log("EVIDENCE  building per-orphan cancer-evidence matrix")
        matrix = build_evidence_matrix(con)
        zero_evidence = [r[0] for r in matrix
                         if not any([r[1], r[2], r[3], r[4], r[5], r[6]])]
        any_evidence = [r[0] for r in matrix
                        if any([r[1], r[2], r[3], r[4], r[5], r[6]])]
        log(f"  total orphans: {len(matrix)}")
        log(f"  zero-evidence (will be archived/deleted): {len(zero_evidence)}")
        log(f"  any-evidence (routed to review view): {len(any_evidence)}")
        decision["phases"]["evidence"] = {
            "n_total_orphans": len(matrix),
            "n_zero_evidence": len(zero_evidence),
            "n_any_evidence": len(any_evidence),
            "any_evidence_sample_ids": [int(r) for r in any_evidence[:20]],
        }

        if not do_writes:
            log("DRY-RUN — no mutations performed")
            log(f"=== END  elapsed={time.time()-t0:.1f}s")
            write_decision_log(DECISION_LOG, decision)
            fh.close()
            return

        # ---- snapshot orphan slices ----
        ensure_archive_schema(con)
        ensure_audit_table(con)

        tg_snap = f"thyroglobulin_lab_canonical_v1_orphans_pre253_{run_ts}"
        ll_snap = f"longitudinal_lab_canonical_v1_orphans_pre253_{run_ts}"
        tg_full = archive_orphan_slice(con, TG, tg_snap, zero_evidence, log)
        ll_full = archive_orphan_slice(con, LONG_LAB, ll_snap, zero_evidence, log)
        decision["phases"]["snapshots"] = {"tg_archive": tg_full,
                                           "long_archive": ll_full}

        # ---- write review view (any-evidence orphans) ----
        log("REVIEW  building manuscript_workspace.lab_orphan_cohort_review_v1")
        n_review = write_review_view(con, any_evidence, log)
        decision["phases"]["review_view_rows"] = n_review

        # ---- delete zero-evidence orphans from both lab tables ----
        if zero_evidence:
            rid_csv = ",".join(str(int(r)) for r in zero_evidence)
            tg_rows = int(con.execute(
                f"SELECT COUNT(*) FROM {TG} WHERE TRY_CAST(research_id AS INTEGER) IN ({rid_csv})"
            ).fetchone()[0])
            ll_rows = int(con.execute(
                f"SELECT COUNT(*) FROM {LONG_LAB} WHERE TRY_CAST(research_id AS INTEGER) IN ({rid_csv})"
            ).fetchone()[0])
            log(f"DELETE  {TG}: {tg_rows} rows for {len(zero_evidence)} orphan rids")
            con.execute(
                f"DELETE FROM {TG} WHERE TRY_CAST(research_id AS INTEGER) IN ({rid_csv})"
            )
            log(f"DELETE  {LONG_LAB}: {ll_rows} rows for {len(zero_evidence)} orphan rids")
            con.execute(
                f"DELETE FROM {LONG_LAB} WHERE TRY_CAST(research_id AS INTEGER) IN ({rid_csv})"
            )
            decision["phases"]["delete"] = {"tg_rows": tg_rows, "long_rows": ll_rows}
        else:
            log("DELETE  no zero-evidence orphans to delete")
            decision["phases"]["delete"] = {"tg_rows": 0, "long_rows": 0}

        # ---- self-verify ----
        n_tg_after = orphan_count(con, TG)
        n_long_after = orphan_count(con, LONG_LAB)
        log(f"VERIFY  Tg orphans AFTER: {n_tg_after}  longitudinal AFTER: {n_long_after}")
        # After deletion, residual orphans == count(any_evidence)
        decision["phases"]["replay_after"] = {
            "tg_orphans_after": n_tg_after,
            "long_orphans_after": n_long_after,
            "expected_residual_any_evidence": len(any_evidence),
        }

        record_audit(
            con, SCRIPT_NUM, "audit_2_1",
            "thyroglobulin_lab_orphans",
            count_before=n_tg_orphans, count_after=n_tg_after,
            target_after=len(any_evidence),
            status="OK" if n_tg_after == len(any_evidence) else "FAIL",
            notes=f"any_evidence_routed_to=lab_orphan_cohort_review_v1 ({len(any_evidence)})",
        )
        record_audit(
            con, SCRIPT_NUM, "audit_2_1",
            "longitudinal_lab_orphans",
            count_before=n_long_orphans, count_after=n_long_after,
            target_after=len(any_evidence),
            status="OK" if n_long_after == len(any_evidence) else "FAIL",
            notes=f"any_evidence_routed_to=lab_orphan_cohort_review_v1 ({len(any_evidence)})",
        )

        if n_tg_after != len(any_evidence) or n_long_after != len(any_evidence):
            raise RuntimeError(
                "Self-verify FAILED: residual orphans != any-evidence count "
                f"(tg={n_tg_after}, long={n_long_after}, expected={len(any_evidence)})"
            )

        # CPM invariant
        n_cpm = int(con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0])
        if n_cpm != 10871:
            raise RuntimeError(f"CPM row count drifted: {n_cpm} != 10871")
        log(f"INVARIANT  CPM rows = {n_cpm}")
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
