"""Step 1 — Generalized scope check for tumor_size_cm_max under-report.

Find ALL multi-surgery patients where the true max tumor focus across
surgeries exceeds CPM.tumor_size_cm_max, including the "hidden" cases
where path_tumor_size_cm ALSO under-reports (so the invariant view stays
clean and the bug hides).

TEM uses column `tumor_size_cm` per probe; surgery is identified by
`surgery_episode_id`. We use TEM's `surgery_date` order to identify
later surgeries (more robust than `surgery_episode_id > 1`).

Read-only. Writes only studies/canonical_cleanup_20260417_resume/
phase4ii_scope_check.{json,csv}.
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

JSON_PATH = HERE / "phase4ii_scope_check.json"
CSV_PATH = HERE / "phase4ii_scope_check.csv"
LOG = HERE / "phase4ii_scope_check.log"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG.open("a") as f:
        f.write(line + "\n")


def main() -> int:
    LOG.write_text("")
    con = connect_locked()
    log("Phase 4 (ii) Step 1 — generalized TEM-based scope check (read-only).")

    # Aggregate counts (Logan's query, adapted: TEM size col is tumor_size_cm,
    # rid is INTEGER; CPM rid is VARCHAR; "later surgery" = any surgery whose
    # date strictly post-dates the patient's earliest surgery date).
    log("Computing aggregate scope...")
    agg = con.execute(
        """
        WITH tem AS (
          SELECT
            research_id,
            tumor_size_cm,
            surgery_episode_id,
            surgery_date
          FROM main.tumor_episode_master_v2
          WHERE tumor_size_cm IS NOT NULL
        ),
        per_pt AS (
          SELECT
            research_id,
            COUNT(DISTINCT surgery_episode_id) AS n_surg_episodes,
            MIN(surgery_date) AS earliest_surg_date,
            MAX(tumor_size_cm) AS true_max_across_all_surgeries
          FROM tem
          GROUP BY research_id
          HAVING COUNT(DISTINCT surgery_episode_id) > 1
        ),
        per_pt_later AS (
          SELECT
            t.research_id,
            MAX(t.tumor_size_cm) AS max_later_surg
          FROM tem t
          JOIN per_pt p USING (research_id)
          WHERE t.surgery_date > p.earliest_surg_date
          GROUP BY t.research_id
        )
        SELECT
          COUNT(*) AS multi_surgery_pts,
          SUM(CASE WHEN cpm.tumor_size_cm_max < p.true_max_across_all_surgeries - 0.01
                   THEN 1 ELSE 0 END) AS cpm_under_reports_true_max,
          SUM(CASE WHEN cpm.tumor_size_cm_max < p.true_max_across_all_surgeries - 0.01
                     AND cpm.path_tumor_size_cm >= p.true_max_across_all_surgeries - 0.01
                   THEN 1 ELSE 0 END) AS caught_by_invariant,
          SUM(CASE WHEN cpm.tumor_size_cm_max < p.true_max_across_all_surgeries - 0.01
                     AND (cpm.path_tumor_size_cm IS NULL
                          OR cpm.path_tumor_size_cm < p.true_max_across_all_surgeries - 0.01)
                   THEN 1 ELSE 0 END) AS hidden_both_under,
          SUM(CASE WHEN cpm.tumor_size_cm_max < p.true_max_across_all_surgeries - 0.01
                     AND cpm.path_tumor_size_cm IS NULL
                   THEN 1 ELSE 0 END) AS hidden_path_null
        FROM main.canonical_patient_master cpm
        JOIN per_pt p ON TRY_CAST(cpm.research_id AS INTEGER) = p.research_id
        LEFT JOIN per_pt_later l ON l.research_id = p.research_id
        """
    ).fetchone()
    keys = [d[0] for d in con.description]
    summary = dict(zip(keys, agg))
    log(f"  aggregate: {summary}")

    # Pull every individual rid in cpm_under_reports_true_max for queue use
    log("Pulling per-rid detail (all under-report cases)...")
    rows = con.execute(
        """
        WITH tem AS (
          SELECT research_id, tumor_size_cm, surgery_episode_id, surgery_date
          FROM main.tumor_episode_master_v2
          WHERE tumor_size_cm IS NOT NULL
        ),
        per_pt AS (
          SELECT research_id,
                 COUNT(DISTINCT surgery_episode_id) AS n_surg_episodes,
                 MIN(surgery_date) AS earliest_surg_date,
                 MAX(tumor_size_cm) AS true_max_across_all_surgeries
          FROM tem
          GROUP BY research_id
          HAVING COUNT(DISTINCT surgery_episode_id) > 1
        )
        SELECT
          CAST(p.research_id AS VARCHAR) AS research_id,
          p.n_surg_episodes,
          p.true_max_across_all_surgeries,
          cpm.path_tumor_size_cm AS current_path_tumor_size_cm,
          cpm.tumor_size_cm_max AS current_tumor_size_cm_max,
          ROUND(p.true_max_across_all_surgeries - cpm.tumor_size_cm_max, 2)
            AS max_under_report_delta_cm,
          (cpm.path_tumor_size_cm IS NULL
            OR cpm.path_tumor_size_cm < p.true_max_across_all_surgeries - 0.01
          ) AS hidden_both_under_flag,
          (cpm.path_tumor_size_cm > cpm.tumor_size_cm_max + 0.01)
            AS caught_by_invariant_flag
        FROM main.canonical_patient_master cpm
        JOIN per_pt p ON TRY_CAST(cpm.research_id AS INTEGER) = p.research_id
        WHERE cpm.tumor_size_cm_max < p.true_max_across_all_surgeries - 0.01
        ORDER BY max_under_report_delta_cm DESC
        """
    ).fetchall()
    detail_keys = [d[0] for d in con.description]
    detail = [dict(zip(detail_keys, r)) for r in rows]
    log(f"  per-rid detail rows: {len(detail)}")

    # Sanity: caught_by_invariant should match Phase 4 (ii) F-bucket count
    n_caught = sum(1 for r in detail if r["caught_by_invariant_flag"])
    n_hidden = sum(1 for r in detail if r["hidden_both_under_flag"])
    log(
        f"  reconciliation: caught_by_invariant_via_query={n_caught}, "
        f"hidden_both_under={n_hidden}, F_bucket_from_phase4ii=60"
    )

    # Write outputs
    JSON_PATH.write_text(
        json.dumps({"aggregate": summary, "detail": detail}, indent=2, default=str)
    )
    if detail:
        with CSV_PATH.open("w", newline="") as fp:
            w = csv.DictWriter(fp, fieldnames=detail_keys)
            w.writeheader()
            for d in detail:
                w.writerow(d)
        log(f"  CSV -> {CSV_PATH}")
    log(f"  JSON -> {JSON_PATH}")

    # CPM invariant
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit(f"CPM invariant regressed: {n_rows}/{n_distinct}")
    log(f"  CPM invariant re-asserted: {n_rows}/{n_distinct}")
    log("Step 1 scope check complete (read-only).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
