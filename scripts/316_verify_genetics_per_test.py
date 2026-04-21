"""
Script 316 — Build verify_genetics_per_test_v1 + summary.

Grain: per (research_id) — joins molecular_results (Excel) with
genetics_per_test_master_v1 (Script 302).

Usage:
    python 316_verify_genetics_per_test.py            # dry-run
    python 316_verify_genetics_per_test.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "316_verify_genetics_per_test"
FIELDS = ["platform"]


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def cpm_invariants(con, label=""):
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
          FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        raise SystemExit("CPM invariant violation")


VERIFY_SQL = """
WITH excel_agg AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(assay_name) AS platform_excel
    FROM main.molecular_results
    GROUP BY research_id
),
llm_agg AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(platform) AS platform_llm
    FROM main.genetics_per_test_master_v1
    GROUP BY research_id
)
SELECT
    COALESCE(e.research_id, l.research_id) AS research_id,

    e.platform_excel, l.platform_llm,
    CASE WHEN e.platform_excel IS NULL AND l.platform_llm IS NULL THEN 'both_null'
         WHEN e.platform_excel IS NULL THEN 'llm_only'
         WHEN l.platform_llm IS NULL THEN 'excel_only'
         WHEN LOWER(TRIM(e.platform_excel)) = LOWER(TRIM(l.platform_llm)) THEN 'agree'
         ELSE 'disagree' END AS platform_concordance

FROM excel_agg e
FULL OUTER JOIN llm_agg l ON l.research_id = e.research_id
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()
    log("=" * 72)
    log(f"Script 316 — verify_genetics_per_test {'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)
    cpm_invariants(con, "pre")

    preview = con.execute(f"SELECT COUNT(*) FROM ({VERIFY_SQL}) t").fetchone()
    log(f"  Verify preview: {preview[0]} rows")

    if not args.commit:
        cpm_invariants(con, "post-dryrun")
        log("(dry-run)")
        return

    con.execute(f"CREATE OR REPLACE TABLE main.verify_genetics_per_test_v1 AS {VERIFY_SQL}")
    log(f"  Created verify_genetics_per_test_v1: {preview[0]} rows")

    summary_parts = []
    for f in FIELDS:
        summary_parts.append(f"""
            SELECT 'genetics_per_test' AS domain, '{f}' AS field_name,
                   SUM(CASE WHEN {f}_concordance='agree' THEN 1 ELSE 0 END) AS n_agree,
                   SUM(CASE WHEN {f}_concordance='disagree' THEN 1 ELSE 0 END) AS n_disagree,
                   SUM(CASE WHEN {f}_concordance='excel_only' THEN 1 ELSE 0 END) AS n_excel_only,
                   SUM(CASE WHEN {f}_concordance='llm_only' THEN 1 ELSE 0 END) AS n_llm_only,
                   ROUND(SUM(CASE WHEN {f}_concordance='agree' THEN 1.0 ELSE 0 END) /
                         NULLIF(SUM(CASE WHEN {f}_concordance IN ('agree','disagree') THEN 1 ELSE 0 END), 0), 4) AS pct_agree
            FROM main.verify_genetics_per_test_v1""")
    con.execute(f"CREATE OR REPLACE TABLE main.verify_genetics_per_test_summary_v1 AS {' UNION ALL '.join(summary_parts)}")

    rows = con.execute("SELECT * FROM main.verify_genetics_per_test_summary_v1").fetchall()
    for r in rows:
        log(f"    {r[1]:30s} agree={r[2]:>5} disagree={r[3]:>5} pct={r[6]}")

    cpm_invariants(con, "post")
    log("Script 316 complete.")


if __name__ == "__main__":
    main()
