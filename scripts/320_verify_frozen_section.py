"""
Script 320 — verify_frozen_section_v1. Joins path_synoptics frozen cols with frozen_section_event_v1.
Usage: python 320_verify_frozen_section.py [--commit]
"""
from __future__ import annotations
import argparse, datetime as dt
from _md_connect import connect_locked
SCRIPT = "320_verify_frozen_section"
FIELDS = ["frozen_section_performed", "frozen_section_result"]

def log(msg): print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)
def cpm_invariants(con, label=""):
    r = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id), SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) FROM main.canonical_patient_master").fetchone()
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0: raise SystemExit("CPM invariant violation")

VERIFY_SQL = """
WITH excel AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(frozen_section_obtained) AS frozen_section_performed_excel,
           MAX(fs_pathology_frozen_section) AS frozen_section_result_excel
    FROM main.path_synoptics GROUP BY research_id
),
llm AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           'Y' AS frozen_section_performed_llm,
           MAX(COALESCE(frozen_section_result_raw, frozen_section_result))
                AS frozen_section_result_llm
    FROM tier2.frozen_section_event_v1 GROUP BY research_id
)
SELECT COALESCE(e.research_id, l.research_id) AS research_id,
    e.frozen_section_performed_excel, l.frozen_section_performed_llm,
    CASE WHEN e.frozen_section_performed_excel IS NULL AND l.frozen_section_performed_llm IS NULL THEN 'both_null'
         WHEN e.frozen_section_performed_excel IS NULL THEN 'llm_only' WHEN l.frozen_section_performed_llm IS NULL THEN 'excel_only'
         ELSE 'agree' END AS frozen_section_performed_concordance,
    e.frozen_section_result_excel, l.frozen_section_result_llm,
    CASE WHEN e.frozen_section_result_excel IS NULL AND l.frozen_section_result_llm IS NULL THEN 'both_null'
         WHEN e.frozen_section_result_excel IS NULL THEN 'llm_only' WHEN l.frozen_section_result_llm IS NULL THEN 'excel_only'
         WHEN LOWER(TRIM(CAST(e.frozen_section_result_excel AS VARCHAR))) = LOWER(TRIM(l.frozen_section_result_llm)) THEN 'agree' ELSE 'disagree' END AS frozen_section_result_concordance
FROM excel e FULL OUTER JOIN llm l ON l.research_id = e.research_id
"""

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--commit", action="store_true"); args = ap.parse_args()
    con = connect_locked()
    log(f"Script 320 — verify_frozen_section {'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    cpm_invariants(con, "pre")
    preview = con.execute(f"SELECT COUNT(*) FROM ({VERIFY_SQL}) t").fetchone()
    log(f"  Preview: {preview[0]} rows")
    if not args.commit: cpm_invariants(con, "post-dryrun"); return
    con.execute(f"CREATE OR REPLACE TABLE main.verify_frozen_section_v1 AS {VERIFY_SQL}")
    parts = [f"SELECT 'frozen_section' AS domain, '{f}' AS field_name, SUM(CASE WHEN {f}_concordance='agree' THEN 1 ELSE 0 END) AS n_agree, SUM(CASE WHEN {f}_concordance='disagree' THEN 1 ELSE 0 END) AS n_disagree, SUM(CASE WHEN {f}_concordance='excel_only' THEN 1 ELSE 0 END) AS n_excel_only, SUM(CASE WHEN {f}_concordance='llm_only' THEN 1 ELSE 0 END) AS n_llm_only, ROUND(SUM(CASE WHEN {f}_concordance='agree' THEN 1.0 ELSE 0 END)/NULLIF(SUM(CASE WHEN {f}_concordance IN ('agree','disagree') THEN 1 ELSE 0 END),0),4) AS pct_agree FROM main.verify_frozen_section_v1" for f in FIELDS]
    con.execute(f"CREATE OR REPLACE TABLE main.verify_frozen_section_summary_v1 AS {' UNION ALL '.join(parts)}")
    for r in con.execute("SELECT * FROM main.verify_frozen_section_summary_v1").fetchall():
        log(f"    {r[1]:35s} agree={r[2]:>5} disagree={r[3]:>5} pct={r[6]}")
    cpm_invariants(con, "post")
    log("Script 320 complete.")

if __name__ == "__main__": main()
