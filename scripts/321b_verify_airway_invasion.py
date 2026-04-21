"""
Script 321b — verify_airway_invasion_v1.
Usage: python 321b_verify_airway_invasion.py [--commit]
"""
from __future__ import annotations
import argparse, datetime as dt
from _md_connect import connect_locked
SCRIPT = "321b_verify_airway_invasion"
FIELDS = ["extrathyroidal_extension"]

def log(msg): print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)
def cpm_invariants(con, label=""):
    r = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id), SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) FROM main.canonical_patient_master").fetchone()
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0: raise SystemExit("CPM invariant violation")

VERIFY_SQL = """
WITH excel AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(tumor_1_extrathyroidal_extension) AS extrathyroidal_extension_excel
    FROM main.path_synoptics GROUP BY research_id
),
llm AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(CASE WHEN ai_present='Y' THEN 'present' WHEN ai_present='N' THEN 'absent' ELSE ai_present END) AS extrathyroidal_extension_llm
    FROM main.airway_invasion_event_v1 GROUP BY research_id
)
SELECT COALESCE(e.research_id, l.research_id) AS research_id,
    e.extrathyroidal_extension_excel, l.extrathyroidal_extension_llm,
    CASE WHEN e.extrathyroidal_extension_excel IS NULL AND l.extrathyroidal_extension_llm IS NULL THEN 'both_null'
         WHEN e.extrathyroidal_extension_excel IS NULL THEN 'llm_only' WHEN l.extrathyroidal_extension_llm IS NULL THEN 'excel_only'
         WHEN LOWER(TRIM(e.extrathyroidal_extension_excel)) = LOWER(TRIM(l.extrathyroidal_extension_llm)) THEN 'agree' ELSE 'disagree' END AS extrathyroidal_extension_concordance
FROM excel e FULL OUTER JOIN llm l ON l.research_id = e.research_id
"""

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--commit", action="store_true"); args = ap.parse_args()
    con = connect_locked()
    log(f"Script 321b — verify_airway_invasion {'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    cpm_invariants(con, "pre")
    preview = con.execute(f"SELECT COUNT(*) FROM ({VERIFY_SQL}) t").fetchone()
    log(f"  Preview: {preview[0]} rows")
    if not args.commit: cpm_invariants(con, "post-dryrun"); return
    con.execute(f"CREATE OR REPLACE TABLE main.verify_airway_invasion_v1 AS {VERIFY_SQL}")
    parts = [f"SELECT 'airway_invasion' AS domain, '{f}' AS field_name, SUM(CASE WHEN {f}_concordance='agree' THEN 1 ELSE 0 END) AS n_agree, SUM(CASE WHEN {f}_concordance='disagree' THEN 1 ELSE 0 END) AS n_disagree, SUM(CASE WHEN {f}_concordance='excel_only' THEN 1 ELSE 0 END) AS n_excel_only, SUM(CASE WHEN {f}_concordance='llm_only' THEN 1 ELSE 0 END) AS n_llm_only, ROUND(SUM(CASE WHEN {f}_concordance='agree' THEN 1.0 ELSE 0 END)/NULLIF(SUM(CASE WHEN {f}_concordance IN ('agree','disagree') THEN 1 ELSE 0 END),0),4) AS pct_agree FROM main.verify_airway_invasion_v1" for f in FIELDS]
    con.execute(f"CREATE OR REPLACE TABLE main.verify_airway_invasion_summary_v1 AS {' UNION ALL '.join(parts)}")
    for r in con.execute("SELECT * FROM main.verify_airway_invasion_summary_v1").fetchall():
        log(f"    {r[1]:35s} agree={r[2]:>5} disagree={r[3]:>5} pct={r[6]}")
    cpm_invariants(con, "post")
    log("Script 321b complete.")

if __name__ == "__main__": main()
