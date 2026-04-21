"""
Script 315 — Build verify_us_nodule_v1 + summary.

Grain: per (research_id) — joins ultrasound_reports (Excel) with
canonical_us_nodule_master_v1 (LLM-integrated, Script 299).

Usage:
    python 315_verify_us_nodule.py            # dry-run
    python 315_verify_us_nodule.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "315_verify_us_nodule"

FIELDS = ["nodule_count", "max_nodule_size_cm", "tirads_category_max"]


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
           COUNT(*) AS nodule_count_excel,
           MAX(CAST(nodule_1_length_mm AS VARCHAR)) AS max_nodule_size_cm_excel,
           MAX(nodule_1_ti_rads) AS tirads_category_max_excel
    FROM main.ultrasound_reports
    GROUP BY research_id
),
llm_agg AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           CAST(COUNT(*) AS VARCHAR) AS nodule_count_llm,
           MAX(CAST(size_cm AS VARCHAR)) AS max_nodule_size_cm_llm,
           MAX(tirads_level_2017) AS tirads_category_max_llm
    FROM main.canonical_us_nodule_master_v1
    GROUP BY research_id
)
SELECT
    COALESCE(e.research_id, l.research_id) AS research_id,

    CAST(e.nodule_count_excel AS VARCHAR) AS nodule_count_excel,
    l.nodule_count_llm,
    CASE WHEN e.nodule_count_excel IS NULL AND l.nodule_count_llm IS NULL THEN 'both_null'
         WHEN e.nodule_count_excel IS NULL THEN 'llm_only'
         WHEN l.nodule_count_llm IS NULL THEN 'excel_only'
         WHEN CAST(e.nodule_count_excel AS VARCHAR) = l.nodule_count_llm THEN 'agree'
         ELSE 'disagree' END AS nodule_count_concordance,

    e.max_nodule_size_cm_excel, l.max_nodule_size_cm_llm,
    CASE WHEN e.max_nodule_size_cm_excel IS NULL AND l.max_nodule_size_cm_llm IS NULL THEN 'both_null'
         WHEN e.max_nodule_size_cm_excel IS NULL THEN 'llm_only'
         WHEN l.max_nodule_size_cm_llm IS NULL THEN 'excel_only'
         WHEN TRIM(e.max_nodule_size_cm_excel) = TRIM(l.max_nodule_size_cm_llm) THEN 'agree'
         ELSE 'disagree' END AS max_nodule_size_cm_concordance,

    e.tirads_category_max_excel, l.tirads_category_max_llm,
    CASE WHEN e.tirads_category_max_excel IS NULL AND l.tirads_category_max_llm IS NULL THEN 'both_null'
         WHEN e.tirads_category_max_excel IS NULL THEN 'llm_only'
         WHEN l.tirads_category_max_llm IS NULL THEN 'excel_only'
         WHEN LOWER(TRIM(e.tirads_category_max_excel)) = LOWER(TRIM(l.tirads_category_max_llm)) THEN 'agree'
         ELSE 'disagree' END AS tirads_category_max_concordance

FROM excel_agg e
FULL OUTER JOIN llm_agg l ON l.research_id = e.research_id
"""


def build_summary_sql(fields):
    parts = []
    for f in fields:
        parts.append(f"""
            SELECT 'us_nodule' AS domain, '{f}' AS field_name,
                   COUNT(*) AS n_total,
                   SUM(CASE WHEN {f}_concordance='agree' THEN 1 ELSE 0 END) AS n_agree,
                   SUM(CASE WHEN {f}_concordance='disagree' THEN 1 ELSE 0 END) AS n_disagree,
                   SUM(CASE WHEN {f}_concordance='excel_only' THEN 1 ELSE 0 END) AS n_excel_only,
                   SUM(CASE WHEN {f}_concordance='llm_only' THEN 1 ELSE 0 END) AS n_llm_only,
                   SUM(CASE WHEN {f}_concordance='both_null' THEN 1 ELSE 0 END) AS n_both_null,
                   SUM(CASE WHEN {f}_excel IS NOT NULL THEN 1 ELSE 0 END) AS n_excel_nonnull,
                   SUM(CASE WHEN {f}_llm IS NOT NULL THEN 1 ELSE 0 END) AS n_llm_nonnull,
                   ROUND(SUM(CASE WHEN {f}_concordance='agree' THEN 1.0 ELSE 0 END) /
                         NULLIF(SUM(CASE WHEN {f}_concordance IN ('agree','disagree') THEN 1 ELSE 0 END), 0), 4) AS pct_agree
            FROM main.verify_us_nodule_v1
        """)
    return " UNION ALL ".join(parts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 315 — verify_us_nodule "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    preview = con.execute(f"SELECT COUNT(*) FROM ({VERIFY_SQL}) t").fetchone()
    log(f"  Verify preview: {preview[0]} rows")

    if not args.commit:
        cpm_invariants(con, "post-dryrun")
        log("(dry-run — re-run with --commit to apply)")
        return

    con.execute(f"CREATE OR REPLACE TABLE main.verify_us_nodule_v1 AS {VERIFY_SQL}")
    log(f"  Created verify_us_nodule_v1: {preview[0]} rows")

    con.execute(f"CREATE OR REPLACE TABLE main.verify_us_nodule_summary_v1 AS {build_summary_sql(FIELDS)}")
    rows = con.execute("SELECT field_name, n_agree, n_disagree, n_excel_only, n_llm_only, pct_agree FROM main.verify_us_nodule_summary_v1").fetchall()
    for r in rows:
        log(f"    {r[0]:35s} agree={r[1]:>5} disagree={r[2]:>5} excel_only={r[3]:>5} llm_only={r[4]:>5} pct={r[5]}")

    cpm_invariants(con, "post")
    log("Script 315 complete.")


if __name__ == "__main__":
    main()
