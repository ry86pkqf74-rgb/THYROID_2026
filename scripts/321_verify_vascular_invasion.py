"""
Script 321 — verify_vascular_invasion_v1.
Usage: python 321_verify_vascular_invasion.py [--commit]
"""
from __future__ import annotations
import argparse, datetime as dt
from _md_connect import connect_locked
SCRIPT = "321_verify_vascular_invasion"
FIELDS = ["lymphovascular_invasion"]

def log(msg): print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)
def cpm_invariants(con, label=""):
    r = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id), SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) FROM main.canonical_patient_master").fetchone()
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0: raise SystemExit("CPM invariant violation")

VERIFY_SQL = """
WITH excel AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(tumor_1_lymphatic_invasion) AS lymphovascular_invasion_excel
    FROM main.path_synoptics GROUP BY research_id
),
llm AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(CASE WHEN vi_present='Y' THEN 'present' WHEN vi_present='N' THEN 'absent' ELSE vi_present END) AS lymphovascular_invasion_llm
    FROM main.vascular_invasion_event_v1 GROUP BY research_id
)
SELECT COALESCE(e.research_id, l.research_id) AS research_id,
    e.lymphovascular_invasion_excel, l.lymphovascular_invasion_llm,
    CASE WHEN e.lymphovascular_invasion_excel IS NULL AND l.lymphovascular_invasion_llm IS NULL THEN 'both_null'
         WHEN e.lymphovascular_invasion_excel IS NULL THEN 'llm_only' WHEN l.lymphovascular_invasion_llm IS NULL THEN 'excel_only'
         WHEN LOWER(TRIM(e.lymphovascular_invasion_excel)) = LOWER(TRIM(l.lymphovascular_invasion_llm)) THEN 'agree' ELSE 'disagree' END AS lymphovascular_invasion_concordance
FROM excel e FULL OUTER JOIN llm l ON l.research_id = e.research_id
"""

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--commit", action="store_true"); args = ap.parse_args()
    con = connect_locked()
    log(f"Script 321 — verify_vascular_invasion {'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    cpm_invariants(con, "pre")
    preview = con.execute(f"SELECT COUNT(*) FROM ({VERIFY_SQL}) t").fetchone()
    log(f"  Preview: {preview[0]} rows")
    if not args.commit: cpm_invariants(con, "post-dryrun"); return
    con.execute(f"CREATE OR REPLACE TABLE main.verify_vascular_invasion_v1 AS {VERIFY_SQL}")
    parts = [f"SELECT 'vascular_invasion' AS domain, '{f}' AS field_name, SUM(CASE WHEN {f}_concordance='agree' THEN 1 ELSE 0 END) AS n_agree, SUM(CASE WHEN {f}_concordance='disagree' THEN 1 ELSE 0 END) AS n_disagree, SUM(CASE WHEN {f}_concordance='excel_only' THEN 1 ELSE 0 END) AS n_excel_only, SUM(CASE WHEN {f}_concordance='llm_only' THEN 1 ELSE 0 END) AS n_llm_only, ROUND(SUM(CASE WHEN {f}_concordance='agree' THEN 1.0 ELSE 0 END)/NULLIF(SUM(CASE WHEN {f}_concordance IN ('agree','disagree') THEN 1 ELSE 0 END),0),4) AS pct_agree FROM main.verify_vascular_invasion_v1" for f in FIELDS]
    con.execute(f"CREATE OR REPLACE TABLE main.verify_vascular_invasion_summary_v1 AS {' UNION ALL '.join(parts)}")
    for r in con.execute("SELECT * FROM main.verify_vascular_invasion_summary_v1").fetchall():
        log(f"    {r[1]:35s} agree={r[2]:>5} disagree={r[3]:>5} pct={r[6]}")
    cpm_invariants(con, "post")
    log("Script 321 complete.")

if __name__ == "__main__": main()
