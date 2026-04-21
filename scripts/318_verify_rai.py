"""
Script 318 — Build verify_rai_v1 + summary.

Joins rai_treatment_episode_v2 (Excel cols) with
note_entities_llm_rai_detailed (LLM entities).

Usage:
    python 318_verify_rai.py [--commit]
"""
from __future__ import annotations
import argparse, datetime as dt
from _md_connect import connect_locked

SCRIPT = "318_verify_rai"
FIELDS = ["rai_dose_mci", "rai_indication"]

def log(msg):
    print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)

def cpm_invariants(con, label=""):
    r = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id), SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) FROM main.canonical_patient_master").fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0: raise SystemExit("CPM invariant violation")

VERIFY_SQL = """
WITH excel AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(CAST(dose_mci AS VARCHAR)) AS rai_dose_mci_excel,
           MAX(dose_text_raw) AS rai_indication_excel
    FROM main.rai_treatment_episode_v2
    GROUP BY research_id
),
llm AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           MAX(CASE WHEN json_extract_string(ent_json, '$.entity_type') ILIKE '%dose%'
                    THEN json_extract_string(ent_json, '$.entity_value') END) AS rai_dose_mci_llm,
           MAX(CASE WHEN json_extract_string(ent_json, '$.entity_type') ILIKE '%indication%'
                    THEN json_extract_string(ent_json, '$.entity_value') END) AS rai_indication_llm
    FROM (
        SELECT research_id,
               UNNEST(CAST(json_extract(result_json, '$.entities') AS JSON[])) AS ent_json
        FROM main.note_entities_llm_rai_detailed
        WHERE result_json IS NOT NULL
          AND json_array_length(json_extract(result_json, '$.entities')) > 0
    ) t
    GROUP BY research_id
)
SELECT COALESCE(e.research_id, l.research_id) AS research_id,
    e.rai_dose_mci_excel, l.rai_dose_mci_llm,
    CASE WHEN e.rai_dose_mci_excel IS NULL AND l.rai_dose_mci_llm IS NULL THEN 'both_null'
         WHEN e.rai_dose_mci_excel IS NULL THEN 'llm_only' WHEN l.rai_dose_mci_llm IS NULL THEN 'excel_only'
         WHEN TRIM(e.rai_dose_mci_excel) = TRIM(l.rai_dose_mci_llm) THEN 'agree' ELSE 'disagree' END AS rai_dose_mci_concordance,
    e.rai_indication_excel, l.rai_indication_llm,
    CASE WHEN e.rai_indication_excel IS NULL AND l.rai_indication_llm IS NULL THEN 'both_null'
         WHEN e.rai_indication_excel IS NULL THEN 'llm_only' WHEN l.rai_indication_llm IS NULL THEN 'excel_only'
         WHEN LOWER(TRIM(e.rai_indication_excel)) = LOWER(TRIM(l.rai_indication_llm)) THEN 'agree' ELSE 'disagree' END AS rai_indication_concordance
FROM excel e FULL OUTER JOIN llm l ON l.research_id = e.research_id
"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()
    log(f"Script 318 — verify_rai {'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    cpm_invariants(con, "pre")
    preview = con.execute(f"SELECT COUNT(*) FROM ({VERIFY_SQL}) t").fetchone()
    log(f"  Preview: {preview[0]} rows")
    if not args.commit: cpm_invariants(con, "post-dryrun"); return
    con.execute(f"CREATE OR REPLACE TABLE main.verify_rai_v1 AS {VERIFY_SQL}")
    log(f"  Created verify_rai_v1")
    parts = [f"SELECT 'rai' AS domain, '{f}' AS field_name, SUM(CASE WHEN {f}_concordance='agree' THEN 1 ELSE 0 END) AS n_agree, SUM(CASE WHEN {f}_concordance='disagree' THEN 1 ELSE 0 END) AS n_disagree, SUM(CASE WHEN {f}_concordance='excel_only' THEN 1 ELSE 0 END) AS n_excel_only, SUM(CASE WHEN {f}_concordance='llm_only' THEN 1 ELSE 0 END) AS n_llm_only, ROUND(SUM(CASE WHEN {f}_concordance='agree' THEN 1.0 ELSE 0 END)/NULLIF(SUM(CASE WHEN {f}_concordance IN ('agree','disagree') THEN 1 ELSE 0 END),0),4) AS pct_agree FROM main.verify_rai_v1" for f in FIELDS]
    con.execute(f"CREATE OR REPLACE TABLE main.verify_rai_summary_v1 AS {' UNION ALL '.join(parts)}")
    for r in con.execute("SELECT * FROM main.verify_rai_summary_v1").fetchall():
        log(f"    {r[1]:30s} agree={r[2]:>5} disagree={r[3]:>5} pct={r[6]}")
    cpm_invariants(con, "post")
    log("Script 318 complete.")

if __name__ == "__main__":
    main()
