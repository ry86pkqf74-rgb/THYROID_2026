#!/usr/bin/env python3
"""
Script 232: Adjudicate 45 "present_ungraded" ETE cases using Claude Haiku 4.5

For 45 PTC patients, ete_grade is "present_ungraded" or "true". We pull the path
synoptic ETE field + gross description + diagnosis comment + microscopic
description and ask Claude Haiku to classify each as microscopic vs gross ETE
per AJCC 8th ed criteria.

AJCC 8 criteria:
  - Microscopic ETE: tumor extension into perithyroidal soft tissue visible only
    on microscopic exam, no macroscopic/clinical invasion. Now EXCLUDED from T3
    staging (AJCC 8 removed mETE as T3 criterion).
  - Gross ETE (T3b/T4a/T4b): invasion into strap muscles (T3b), RLN/trachea/
    esophagus/subcutaneous tissue (T4a), or prevertebral fascia/carotid/
    mediastinum (T4b).

Output: creates table `ete_adjudication_v1` with:
  - research_id
  - adjudicated_grade: 'microscopic' | 'gross' | 'absent' | 'unable_to_determine'
  - adjudicated_confidence: 'high' | 'medium' | 'low'
  - evidence_quote (verbatim snippet from source)
  - reasoning (brief rationale)
  - ajcc8_t_adjustment (suggested T stage if different from current)
  - raw_llm_response

After review, script 233 applies these to canonical_patient_master.

Defensive guards: locked search path + fully qualified table refs.

Usage:
    export ANTHROPIC_API_KEY=sk-ant-...
    python scripts/232_ete_adjudication.py
    python scripts/232_ete_adjudication.py --dry-run
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime

import pandas as pd
from anthropic import Anthropic

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB

FQ = f"{PUBLICATION_DB}.main"
MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 1000
TEMPERATURE = 0.0

SYSTEM_PROMPT = """You are a pathology data adjudicator for a thyroid cancer research database.

For each PTC patient, you will see:
  - The raw extrathyroidal extension field value (already known to be "present" but ungraded)
  - The gross description, microscopic description, diagnosis comment, and other
    pathology text
  - The tumor size

Your job is to determine whether the ETE is MICROSCOPIC or GROSS per AJCC 8th ed.

AJCC 8 definitions (2017 onward):
  - MICROSCOPIC ETE: tumor extension into immediate perithyroidal soft tissue
    (fibroadipose tissue) seen only on microscopic exam. No gross/clinical invasion.
    NOTE: mETE was REMOVED as a T3 criterion in AJCC 8 (no longer upstages to T3).
  - GROSS ETE (T3b+): macroscopic invasion into:
      T3b: strap muscles (sternothyroid, sternohyoid, omohyoid, thyrohyoid)
      T4a: RLN, subcutaneous soft tissue, larynx, trachea, esophagus
      T4b: prevertebral fascia, mediastinal vessels, carotid artery encasement
  - ABSENT: despite the field saying "present", the full text shows no ETE

Keywords suggesting MICROSCOPIC ETE:
  "perithyroidal fat", "perithyroidal soft tissue", "perithyroidal fibroadipose",
  "microscopic focus", "focal perithyroidal", "minimal extension", "focally
  extending", "only microscopically present"

Keywords suggesting GROSS ETE:
  "strap muscle", "sternothyroid", "sternohyoid", "omohyoid", "gross invasion",
  "macroscopic extension", "visible gross invasion", "invading trachea",
  "invading esophagus", "recurrent laryngeal nerve invasion", "visible tumor
  extension", "invasion into [named anatomic structure beyond perithyroidal fat]"

Return STRICT JSON with these exact fields:
{
  "adjudicated_grade": "microscopic" | "gross" | "absent" | "unable_to_determine",
  "adjudicated_confidence": "high" | "medium" | "low",
  "evidence_quote": "<verbatim quote from source text, max 200 chars>",
  "reasoning": "<1-2 sentence rationale>",
  "ajcc8_t_adjustment": "<suggested T stage, e.g., 'T1b', 'T3b', 'T4a', or 'no change'>"
}

If the text is ambiguous or does not clearly support grading, use
"unable_to_determine" with confidence "low". Do not guess."""


def fetch_cases(con):
    q = f"""
    WITH ungraded AS (
      SELECT research_id FROM {FQ}.canonical_patient_master
      WHERE diagnosis_primary='PTC' AND ete_grade IN ('present_ungraded','true')
    ),
    ps AS (
      SELECT
        research_id,
        MAX(path_extended_gross_path) AS gross_description,
        MAX(microscopic_description) AS microscopic_description,
        MAX(path_diagnosis_comment) AS diagnosis_comment,
        MAX(tumor_1_extrathyroidal_extension) AS tumor_1_ete_raw,
        MAX(tumor_1_histology_comment) AS tumor_1_histology_comment,
        MAX(tumor_1_margin_angiolymphatic_invasion_comment) AS tumor_1_margin_comment,
        MAX(tumor_1_size_greatest_dimension_cm) AS tumor_1_size_cm,
        MAX(tumor_1_capsular_invasion) AS tumor_1_capsular
      FROM {FQ}.path_synoptics GROUP BY research_id
    )
    SELECT u.research_id, cpm.ete_grade, cpm.path_ete_raw, cpm.ajcc8_t_stage,
           cpm.ajcc8_stage_group, cpm.gross_ete_flag,
           ps.gross_description, ps.microscopic_description, ps.diagnosis_comment,
           ps.tumor_1_ete_raw, ps.tumor_1_histology_comment,
           ps.tumor_1_margin_comment, ps.tumor_1_size_cm, ps.tumor_1_capsular
    FROM ungraded u
    LEFT JOIN {FQ}.canonical_patient_master cpm USING (research_id)
    LEFT JOIN ps USING (research_id)
    ORDER BY u.research_id
    """
    df = con.execute(q).df()

    # Invariant: exactly one row per ungraded patient (no dup joins)
    if len(df) != df['research_id'].nunique():
        raise SystemExit(
            f"DUPLICATE JOIN DETECTED: {len(df)} rows, "
            f"{df['research_id'].nunique()} distinct patients"
        )
    return df


def build_user_prompt(row):
    def clip(s, n):
        return (s[:n] + "...[truncated]") if s and len(s) > n else (s or "")
    parts = [
        f"Research ID: {row['research_id']}",
        f"Current ete_grade: {row['ete_grade']}",
        f"Raw path_ete_raw: {row['path_ete_raw']}",
        f"Synoptic tumor_1_extrathyroidal_extension: {row['tumor_1_ete_raw']}",
        f"Current AJCC 8 T stage: {row['ajcc8_t_stage']}",
        f"Tumor size (cm): {row['tumor_1_size_cm']}",
        f"Capsular invasion: {row['tumor_1_capsular']}",
        f"gross_ete_flag: {row['gross_ete_flag']}",
        "",
        "--- GROSS DESCRIPTION ---",
        clip(row['gross_description'], 2500),
        "",
        "--- MICROSCOPIC DESCRIPTION ---",
        clip(row['microscopic_description'], 2500),
        "",
        "--- DIAGNOSIS COMMENT ---",
        clip(row['diagnosis_comment'], 1000),
        "",
        "--- TUMOR 1 HISTOLOGY COMMENT ---",
        clip(row['tumor_1_histology_comment'], 800),
        "",
        "--- TUMOR 1 MARGIN/ANGIOLYMPHATIC COMMENT ---",
        clip(row['tumor_1_margin_comment'], 800),
    ]
    return "\n".join(parts)


def adjudicate_case(client, row):
    user_msg = build_user_prompt(row)
    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            temperature=TEMPERATURE,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}]
        )
        txt = resp.content[0].text.strip()
        if txt.startswith("```"):
            txt = txt.split("```", 2)[1]
            if txt.startswith("json"):
                txt = txt[4:]
            txt = txt.strip()
        result = json.loads(txt)
        result["raw_llm_response"] = txt
        result["error"] = None
    except Exception as e:
        result = {
            "adjudicated_grade": "unable_to_determine",
            "adjudicated_confidence": "low",
            "evidence_quote": "",
            "reasoning": f"LLM error: {e}",
            "ajcc8_t_adjustment": "no change",
            "raw_llm_response": "",
            "error": str(e),
        }
    return result


def save_to_md(con, results_df):
    print(f"\nWriting {len(results_df)} adjudications to ete_adjudication_v1...")
    con.register("df_in", results_df)
    con.execute(f"DROP TABLE IF EXISTS {FQ}.ete_adjudication_v1")
    con.execute(f"CREATE TABLE {FQ}.ete_adjudication_v1 AS SELECT * FROM df_in")
    con.execute(f"""
        COMMENT ON TABLE {FQ}.ete_adjudication_v1 IS
        'LLM adjudication (Claude Haiku 4.5) of 45 present_ungraded ETE cases. '
        'Built by script 232. Review before applying to canonical_patient_master.'
    """)
    con.unregister("df_in")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Show prompt for first case only, no API calls")
    ap.add_argument("--limit", type=int, default=None,
                    help="Only adjudicate first N cases (for testing)")
    args = ap.parse_args()

    print(f"Script 232: Adjudicate ungraded ETE cases")
    print(f"Database: {PUBLICATION_DB}")
    print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")

    con = connect_locked()
    cases = fetch_cases(con)
    print(f"\nFetched {len(cases)} ungraded ETE cases")

    if args.dry_run:
        print("\n--- PROMPT FOR CASE 1 ---")
        print(SYSTEM_PROMPT)
        print("\n--- USER MESSAGE ---")
        print(build_user_prompt(cases.iloc[0]))
        return

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise SystemExit("ANTHROPIC_API_KEY not set")
    client = Anthropic(api_key=api_key)

    if args.limit:
        cases = cases.head(args.limit)

    results = []
    for i, row in cases.iterrows():
        rid = row["research_id"]
        print(f"[{i+1}/{len(cases)}] Adjudicating RID {rid}...", end=" ", flush=True)
        t0 = time.time()
        result = adjudicate_case(client, row)
        dt = time.time() - t0
        print(f"→ {result['adjudicated_grade']} ({result['adjudicated_confidence']}) {dt:.1f}s")
        results.append({"research_id": rid, **result})
        time.sleep(0.3)

    out = pd.DataFrame(results)

    print("\n" + "="*72)
    print("ADJUDICATION SUMMARY")
    print("="*72)
    print(out.groupby(["adjudicated_grade", "adjudicated_confidence"]).size().to_string())

    save_to_md(con, out)
    csv_path = f"ete_adjudication_v1_{datetime.now():%Y%m%d_%H%M%S}.csv"
    out.to_csv(csv_path, index=False)

    print(f"\n✓ Wrote ete_adjudication_v1 table and {csv_path}")
    print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("\nNEXT: Review low-confidence adjudications, then run 233_apply_ete_adjudication.py")


if __name__ == "__main__":
    main()
