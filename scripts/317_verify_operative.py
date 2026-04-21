"""
Script 317 — Build verify_operative_v1 + summary.

Joins operative_episode_detail_v2 Excel-origin cols with
note_entities_operative_detail (pre-parsed 47-col table).

Usage:
    python 317_verify_operative.py [--commit]
"""
from __future__ import annotations
import argparse, datetime as dt
from _md_connect import connect_locked

SCRIPT = "317_verify_operative"
FIELDS = ["surgery_type", "central_neck_dissection_flag", "lateral_neck_dissection_flag"]

def log(msg):
    print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)

def cpm_invariants(con, label=""):
    r = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id), SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) FROM main.canonical_patient_master").fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        raise SystemExit("CPM invariant violation")

VERIFY_SQL = """
WITH excel AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(procedure_normalized) AS surgery_type_excel,
           MAX(CAST(central_neck_dissection_flag AS VARCHAR)) AS central_neck_dissection_flag_excel,
           MAX(CAST(lateral_neck_dissection_flag AS VARCHAR)) AS lateral_neck_dissection_flag_excel
    FROM main.operative_episode_detail_v2
    GROUP BY research_id
),
llm AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(CASE WHEN entity_type ILIKE '%procedure%' OR entity_type ILIKE '%surgery_type%' THEN entity_value_norm END) AS surgery_type_llm,
           MAX(CASE WHEN entity_type ILIKE '%central%dissect%' THEN entity_value_norm END) AS central_neck_dissection_flag_llm,
           MAX(CASE WHEN entity_type ILIKE '%lateral%dissect%' THEN entity_value_norm END) AS lateral_neck_dissection_flag_llm
    FROM main.note_entities_operative_detail
    GROUP BY research_id
)
SELECT COALESCE(e.research_id, l.research_id) AS research_id,
    e.surgery_type_excel, l.surgery_type_llm,
    CASE WHEN e.surgery_type_excel IS NULL AND l.surgery_type_llm IS NULL THEN 'both_null'
         WHEN e.surgery_type_excel IS NULL THEN 'llm_only' WHEN l.surgery_type_llm IS NULL THEN 'excel_only'
         WHEN LOWER(TRIM(e.surgery_type_excel)) = LOWER(TRIM(l.surgery_type_llm)) THEN 'agree' ELSE 'disagree' END AS surgery_type_concordance,
    e.central_neck_dissection_flag_excel, l.central_neck_dissection_flag_llm,
    CASE WHEN e.central_neck_dissection_flag_excel IS NULL AND l.central_neck_dissection_flag_llm IS NULL THEN 'both_null'
         WHEN e.central_neck_dissection_flag_excel IS NULL THEN 'llm_only' WHEN l.central_neck_dissection_flag_llm IS NULL THEN 'excel_only'
         WHEN LOWER(TRIM(e.central_neck_dissection_flag_excel)) = LOWER(TRIM(l.central_neck_dissection_flag_llm)) THEN 'agree' ELSE 'disagree' END AS central_neck_dissection_flag_concordance,
    e.lateral_neck_dissection_flag_excel, l.lateral_neck_dissection_flag_llm,
    CASE WHEN e.lateral_neck_dissection_flag_excel IS NULL AND l.lateral_neck_dissection_flag_llm IS NULL THEN 'both_null'
         WHEN e.lateral_neck_dissection_flag_excel IS NULL THEN 'llm_only' WHEN l.lateral_neck_dissection_flag_llm IS NULL THEN 'excel_only'
         WHEN LOWER(TRIM(e.lateral_neck_dissection_flag_excel)) = LOWER(TRIM(l.lateral_neck_dissection_flag_llm)) THEN 'agree' ELSE 'disagree' END AS lateral_neck_dissection_flag_concordance
FROM excel e FULL OUTER JOIN llm l ON l.research_id = e.research_id
"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()
    log(f"Script 317 — verify_operative {'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    cpm_invariants(con, "pre")
    preview = con.execute(f"SELECT COUNT(*) FROM ({VERIFY_SQL}) t").fetchone()
    log(f"  Preview: {preview[0]} rows")
    if not args.commit:
        cpm_invariants(con, "post-dryrun"); return
    con.execute(f"CREATE OR REPLACE TABLE main.verify_operative_v1 AS {VERIFY_SQL}")
    log(f"  Created verify_operative_v1")
    parts = [f"SELECT 'operative' AS domain, '{f}' AS field_name, SUM(CASE WHEN {f}_concordance='agree' THEN 1 ELSE 0 END) AS n_agree, SUM(CASE WHEN {f}_concordance='disagree' THEN 1 ELSE 0 END) AS n_disagree, SUM(CASE WHEN {f}_concordance='excel_only' THEN 1 ELSE 0 END) AS n_excel_only, SUM(CASE WHEN {f}_concordance='llm_only' THEN 1 ELSE 0 END) AS n_llm_only, ROUND(SUM(CASE WHEN {f}_concordance='agree' THEN 1.0 ELSE 0 END)/NULLIF(SUM(CASE WHEN {f}_concordance IN ('agree','disagree') THEN 1 ELSE 0 END),0),4) AS pct_agree FROM main.verify_operative_v1" for f in FIELDS]
    con.execute(f"CREATE OR REPLACE TABLE main.verify_operative_summary_v1 AS {' UNION ALL '.join(parts)}")
    for r in con.execute("SELECT * FROM main.verify_operative_summary_v1").fetchall():
        log(f"    {r[1]:40s} agree={r[2]:>5} disagree={r[3]:>5} pct={r[6]}")
    cpm_invariants(con, "post")
    log("Script 317 complete.")

if __name__ == "__main__":
    main()
