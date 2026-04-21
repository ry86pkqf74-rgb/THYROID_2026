"""
Script 324 — verify_labs_v1. Compares longitudinal_lab_canonical_v1 flowsheet with LLM lab entities.
Usage: python 324_verify_labs.py [--commit]
"""
from __future__ import annotations
import argparse, datetime as dt
from _md_connect import connect_locked
SCRIPT = "324_verify_labs"
FIELDS = ["tsh_value", "tg_value"]

def log(msg): print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)
def cpm_invariants(con, label=""):
    r = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id), SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) FROM main.canonical_patient_master").fetchone()
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0: raise SystemExit("CPM invariant violation")

VERIFY_SQL = """
WITH excel AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(CASE WHEN LOWER(lab_name_standardized) ILIKE '%tsh%' THEN CAST(value_numeric AS VARCHAR) END) AS tsh_value_excel,
           MAX(CASE WHEN LOWER(lab_name_standardized) ILIKE '%thyroglobulin%' AND LOWER(lab_name_standardized) NOT ILIKE '%antibod%'
                    THEN CAST(value_numeric AS VARCHAR) END) AS tg_value_excel
    FROM main.longitudinal_lab_canonical_v1
    GROUP BY research_id
),
llm AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(CASE WHEN json_extract_string(ent_json, '$.entity_type') ILIKE '%tsh%'
                    THEN json_extract_string(ent_json, '$.entity_value') END) AS tsh_value_llm,
           MAX(CASE WHEN json_extract_string(ent_json, '$.entity_type') ILIKE '%thyroglobulin%'
                    AND json_extract_string(ent_json, '$.entity_type') NOT ILIKE '%antibod%'
                    THEN json_extract_string(ent_json, '$.entity_value') END) AS tg_value_llm
    FROM (
        SELECT research_id,
               UNNEST(CAST(json_extract(result_json, '$.entities') AS JSON[])) AS ent_json
        FROM main.note_entities_llm_labs
        WHERE result_json IS NOT NULL
          AND json_array_length(json_extract(result_json, '$.entities')) > 0
    ) t
    GROUP BY research_id
)
SELECT COALESCE(e.research_id, l.research_id) AS research_id,
    e.tsh_value_excel, l.tsh_value_llm,
    CASE WHEN e.tsh_value_excel IS NULL AND l.tsh_value_llm IS NULL THEN 'both_null'
         WHEN e.tsh_value_excel IS NULL THEN 'llm_only' WHEN l.tsh_value_llm IS NULL THEN 'excel_only'
         WHEN TRIM(e.tsh_value_excel) = TRIM(l.tsh_value_llm) THEN 'agree' ELSE 'disagree' END AS tsh_value_concordance,
    e.tg_value_excel, l.tg_value_llm,
    CASE WHEN e.tg_value_excel IS NULL AND l.tg_value_llm IS NULL THEN 'both_null'
         WHEN e.tg_value_excel IS NULL THEN 'llm_only' WHEN l.tg_value_llm IS NULL THEN 'excel_only'
         WHEN TRIM(e.tg_value_excel) = TRIM(l.tg_value_llm) THEN 'agree' ELSE 'disagree' END AS tg_value_concordance
FROM excel e FULL OUTER JOIN llm l ON l.research_id = e.research_id
"""

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--commit", action="store_true"); args = ap.parse_args()
    con = connect_locked()
    log(f"Script 324 — verify_labs {'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    cpm_invariants(con, "pre")
    preview = con.execute(f"SELECT COUNT(*) FROM ({VERIFY_SQL}) t").fetchone()
    log(f"  Preview: {preview[0]} rows")
    if not args.commit: cpm_invariants(con, "post-dryrun"); return
    con.execute(f"CREATE OR REPLACE TABLE main.verify_labs_v1 AS {VERIFY_SQL}")
    parts = [f"SELECT 'labs' AS domain, '{f}' AS field_name, SUM(CASE WHEN {f}_concordance='agree' THEN 1 ELSE 0 END) AS n_agree, SUM(CASE WHEN {f}_concordance='disagree' THEN 1 ELSE 0 END) AS n_disagree, SUM(CASE WHEN {f}_concordance='excel_only' THEN 1 ELSE 0 END) AS n_excel_only, SUM(CASE WHEN {f}_concordance='llm_only' THEN 1 ELSE 0 END) AS n_llm_only, ROUND(SUM(CASE WHEN {f}_concordance='agree' THEN 1.0 ELSE 0 END)/NULLIF(SUM(CASE WHEN {f}_concordance IN ('agree','disagree') THEN 1 ELSE 0 END),0),4) AS pct_agree FROM main.verify_labs_v1" for f in FIELDS]
    con.execute(f"CREATE OR REPLACE TABLE main.verify_labs_summary_v1 AS {' UNION ALL '.join(parts)}")
    for r in con.execute("SELECT * FROM main.verify_labs_summary_v1").fetchall():
        log(f"    {r[1]:30s} agree={r[2]:>5} disagree={r[3]:>5} pct={r[6]}")
    cpm_invariants(con, "post")
    log("Script 324 complete.")

if __name__ == "__main__": main()
