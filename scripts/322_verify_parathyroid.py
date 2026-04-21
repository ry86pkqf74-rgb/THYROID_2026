"""
Script 322 — verify_parathyroid_v1.
Usage: python 322_verify_parathyroid.py [--commit]
"""
from __future__ import annotations
import argparse, datetime as dt
from _md_connect import connect_locked
SCRIPT = "322_verify_parathyroid"
FIELDS = ["n_pt_identified", "n_pt_autotransplanted"]

def log(msg): print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)
def cpm_invariants(con, label=""):
    r = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id), SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) FROM main.canonical_patient_master").fetchone()
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0: raise SystemExit("CPM invariant violation")

VERIFY_SQL = """
WITH excel AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(CAST(parathyroid_glands AS VARCHAR)) AS n_pt_identified_excel,
           MAX(CAST(parathyroid_operation AS VARCHAR)) AS n_pt_autotransplanted_excel
    FROM main.path_synoptics GROUP BY research_id
),
llm AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(CAST(n_pt_identified AS VARCHAR)) AS n_pt_identified_llm,
           MAX(CAST(n_pt_autotransplanted AS VARCHAR)) AS n_pt_autotransplanted_llm
    FROM main.parathyroid_patient_wide_v1 GROUP BY research_id
)
SELECT COALESCE(e.research_id, l.research_id) AS research_id,
    e.n_pt_identified_excel, l.n_pt_identified_llm,
    CASE WHEN e.n_pt_identified_excel IS NULL AND l.n_pt_identified_llm IS NULL THEN 'both_null'
         WHEN e.n_pt_identified_excel IS NULL THEN 'llm_only' WHEN l.n_pt_identified_llm IS NULL THEN 'excel_only'
         WHEN TRIM(e.n_pt_identified_excel) = TRIM(l.n_pt_identified_llm) THEN 'agree' ELSE 'disagree' END AS n_pt_identified_concordance,
    e.n_pt_autotransplanted_excel, l.n_pt_autotransplanted_llm,
    CASE WHEN e.n_pt_autotransplanted_excel IS NULL AND l.n_pt_autotransplanted_llm IS NULL THEN 'both_null'
         WHEN e.n_pt_autotransplanted_excel IS NULL THEN 'llm_only' WHEN l.n_pt_autotransplanted_llm IS NULL THEN 'excel_only'
         WHEN TRIM(e.n_pt_autotransplanted_excel) = TRIM(l.n_pt_autotransplanted_llm) THEN 'agree' ELSE 'disagree' END AS n_pt_autotransplanted_concordance
FROM excel e FULL OUTER JOIN llm l ON l.research_id = e.research_id
"""

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--commit", action="store_true"); args = ap.parse_args()
    con = connect_locked()
    log(f"Script 322 — verify_parathyroid {'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    cpm_invariants(con, "pre")
    preview = con.execute(f"SELECT COUNT(*) FROM ({VERIFY_SQL}) t").fetchone()
    log(f"  Preview: {preview[0]} rows")
    if not args.commit: cpm_invariants(con, "post-dryrun"); return
    con.execute(f"CREATE OR REPLACE TABLE main.verify_parathyroid_v1 AS {VERIFY_SQL}")
    parts = [f"SELECT 'parathyroid' AS domain, '{f}' AS field_name, SUM(CASE WHEN {f}_concordance='agree' THEN 1 ELSE 0 END) AS n_agree, SUM(CASE WHEN {f}_concordance='disagree' THEN 1 ELSE 0 END) AS n_disagree, SUM(CASE WHEN {f}_concordance='excel_only' THEN 1 ELSE 0 END) AS n_excel_only, SUM(CASE WHEN {f}_concordance='llm_only' THEN 1 ELSE 0 END) AS n_llm_only, ROUND(SUM(CASE WHEN {f}_concordance='agree' THEN 1.0 ELSE 0 END)/NULLIF(SUM(CASE WHEN {f}_concordance IN ('agree','disagree') THEN 1 ELSE 0 END),0),4) AS pct_agree FROM main.verify_parathyroid_v1" for f in FIELDS]
    con.execute(f"CREATE OR REPLACE TABLE main.verify_parathyroid_summary_v1 AS {' UNION ALL '.join(parts)}")
    for r in con.execute("SELECT * FROM main.verify_parathyroid_summary_v1").fetchall():
        log(f"    {r[1]:35s} agree={r[2]:>5} disagree={r[3]:>5} pct={r[6]}")
    cpm_invariants(con, "post")
    log("Script 322 complete.")

if __name__ == "__main__": main()
