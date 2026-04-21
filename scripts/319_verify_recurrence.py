"""
Script 319 — verify_recurrence_v1. Mostly LLM-only since Excel source is limited.
Usage: python 319_verify_recurrence.py [--commit]
"""
from __future__ import annotations
import argparse, datetime as dt
from _md_connect import connect_locked
SCRIPT = "319_verify_recurrence"

def log(msg): print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)
def cpm_invariants(con, label=""):
    r = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id), SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) FROM main.canonical_patient_master").fetchone()
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0: raise SystemExit("CPM invariant violation")

VERIFY_SQL = """
SELECT CAST(research_id AS VARCHAR) AS research_id,
       recurrence_confirmed AS recurrence_confirmed_llm,
       recurrence_date AS recurrence_date_llm,
       recurrence_site AS recurrence_site_llm,
       NULL::VARCHAR AS recurrence_confirmed_excel,
       NULL::VARCHAR AS recurrence_date_excel,
       NULL::VARCHAR AS recurrence_site_excel,
       'llm_only' AS recurrence_confirmed_concordance,
       'llm_only' AS recurrence_date_concordance,
       'llm_only' AS recurrence_site_concordance
FROM main.canonical_recurrence_v1
WHERE recurrence_confirmed IS NOT NULL OR recurrence_date IS NOT NULL
"""

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--commit", action="store_true"); args = ap.parse_args()
    con = connect_locked()
    log(f"Script 319 — verify_recurrence {'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    cpm_invariants(con, "pre")
    preview = con.execute(f"SELECT COUNT(*) FROM ({VERIFY_SQL}) t").fetchone()
    log(f"  Preview: {preview[0]} rows")
    if not args.commit: cpm_invariants(con, "post-dryrun"); return
    con.execute(f"CREATE OR REPLACE TABLE main.verify_recurrence_v1 AS {VERIFY_SQL}")
    con.execute("""CREATE OR REPLACE TABLE main.verify_recurrence_summary_v1 AS
        SELECT 'recurrence' AS domain, 'recurrence_confirmed' AS field_name,
               0::BIGINT AS n_agree, 0::BIGINT AS n_disagree,
               0::BIGINT AS n_excel_only,
               COUNT(*)::BIGINT AS n_llm_only,
               NULL::DOUBLE AS pct_agree
        FROM main.verify_recurrence_v1""")
    log(f"  Created verify_recurrence_v1 + summary")
    cpm_invariants(con, "post")
    log("Script 319 complete.")

if __name__ == "__main__": main()
