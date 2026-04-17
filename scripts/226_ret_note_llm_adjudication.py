#!/usr/bin/env python3
"""
THYROID_2026 — Script 226: RET Note-Entity LLM Re-Adjudication

Re-adjudicates 171 regex-flagged RET mentions in note_entities_genetics using
Claude Haiku 4.5 (primary pass) + Claude Sonnet 4.5 (reconsideration on
low-confidence rows).  Writes:
  - ret_note_entity_adjudication_v226   (mention-level, one row per RET mention)
  - ret_patient_adjudicated_v226        (patient-level rollup)
  - canonical_patient_master            (4 new columns + ret_positive_unified)
  - manuscript_workspace.detail_table_registry_v1 (2 new rows)

SAFE TO RUN IN PARALLEL with Script 225 (writes to new tables only; does NOT
touch ret_positive_v7 or note_entities_genetics).

Usage:
    .venv/bin/python scripts/226_ret_note_llm_adjudication.py
    .venv/bin/python scripts/226_ret_note_llm_adjudication.py --dry-run
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

import duckdb
import pandas as pd
from anthropic import Anthropic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from motherduck_client import get_token  # noqa: E402

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Script 226 — RET note LLM adjudication")
parser.add_argument("--dry-run", action="store_true", help="Build input table only; skip LLM calls and writes")
parser.add_argument("--step", type=int, default=0, help="Start at step N (1-7); 0=run all")
args = parser.parse_args()

DRY_RUN = args.dry_run
START_STEP = args.step

print(f"=== Script 226: RET Note-Entity LLM Re-Adjudication  {datetime.now():%Y-%m-%d %H:%M:%S} ===")
if DRY_RUN:
    print("  [DRY-RUN] — LLM calls and DB writes are skipped.")

# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------
token = get_token()
con = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}")
client = Anthropic()   # reads ANTHROPIC_API_KEY from env

HAIKU_MODEL  = "claude-haiku-4-5-20251001"
SONNET_MODEL = "claude-sonnet-4-5"

# ---------------------------------------------------------------------------
# STEP 1 — Build context-rich input table
# ---------------------------------------------------------------------------
if START_STEP <= 1:
    print("\n--- STEP 1: Building _ret_adjudication_input_v226 ---")

    sql_create = """
    CREATE OR REPLACE TABLE _ret_adjudication_input_v226 AS
    WITH mapping(entity_type, cnl_type) AS (VALUES
      ('h_p','HP'), ('op_note','OPNOTE'), ('endocrine_note','ENDOCRINE_FM'),
      ('other_history','OTHER_HISTORY'), ('history_summary','OTHER_HISTORY'),
      ('other_notes','OTHER_NOTES')
    )
    SELECT
      neg.research_id,
      neg.note_row_id,
      neg.note_type   AS entity_note_type,
      m.cnl_type,
      neg.evidence_start,
      neg.evidence_end,
      SUBSTRING(cnl.note_text, GREATEST(1, neg.evidence_start - 125), 250)  AS context_250,
      SUBSTRING(cnl.note_text, GREATEST(1, neg.evidence_start - 200), 500)  AS context_500,
      CASE
        WHEN cnl.note_text ILIKE '%RET/PTC%not detected%'
             THEN 'hint_fp_not_detected'
        WHEN cnl.note_text ILIKE '%RET/PTC%REARRANGEMENT NOT DETECTED%'
             THEN 'hint_fp_rearr_not_detected'
        WHEN cnl.note_text ILIKE '%negative for%RET%'
             THEN 'hint_fp_negative_for'
        WHEN cnl.note_text ILIKE '%MEN%2%' AND cnl.note_text ILIKE '%RET%'
             THEN 'hint_tp_men2'
        WHEN cnl.note_text ILIKE '%medullary%RET%'
          OR cnl.note_text ILIKE '%RET%medullary%'
             THEN 'hint_tp_mtc'
        WHEN cnl.note_text ILIKE '%selpercatinib%'
          OR cnl.note_text ILIKE '%LOXO-292%'
          OR cnl.note_text ILIKE '%RETEVMO%'
             THEN 'hint_tp_targeted_therapy'
        WHEN cnl.note_text ILIKE '%codon 609%'
          OR cnl.note_text ILIKE '%codon 533%'
          OR cnl.note_text ILIKE '%M918T%'
             THEN 'hint_tp_known_variant'
        ELSE 'needs_review'
      END AS auto_triage
    FROM note_entities_genetics neg
    JOIN mapping m ON m.entity_type = neg.note_type
    JOIN clinical_notes_long cnl
      ON CAST(cnl.research_id AS VARCHAR) = CAST(neg.research_id AS VARCHAR)
     AND cnl.note_type = m.cnl_type
    WHERE UPPER(neg.entity_value_norm) = 'RET'
      AND neg.present_or_negated = 'present'
      AND cnl.note_text IS NOT NULL
      AND LENGTH(cnl.note_text) > neg.evidence_start
    """

    if not DRY_RUN:
        con.execute(sql_create)

    row_count = con.execute("SELECT COUNT(*) FROM _ret_adjudication_input_v226").fetchone()[0]
    print(f"  Input rows: {row_count}  (expected ≈171)")
    if row_count < 150:
        print("  WARNING: fewer rows than expected — check note_type mapping for drops")

    triage = con.execute(
        "SELECT auto_triage, COUNT(*) AS n FROM _ret_adjudication_input_v226 GROUP BY 1 ORDER BY 2 DESC"
    ).fetchdf()
    print("  Auto-triage breakdown:")
    print(triage.to_string(index=False))

# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------
SYSTEM = """You are a clinical NLP adjudicator for thyroid cancer research.

Task: Classify each RET gene mention as TRUE_POSITIVE or FALSE_POSITIVE based on ±125 char context.

A mention is TRUE_POSITIVE if the context indicates the patient HAS a RET alteration:
- Germline RET mutation (MEN2A, MEN2B, familial MTC, codon 609/533/634/918, M918T, specific variants)
- Somatic RET fusion detected (RET/PTC1, RET/PTC3 DETECTED / POSITIVE / IDENTIFIED)
- On RET-targeted therapy (selpercatinib, LOXO-292, RETEVMO, TPX-0046, pralsetinib)
- Phrases: "RET positive", "positive for RET", "RET mutation positive", "harboring RET"
- Medullary thyroid cancer with RET mentioned as cause/driver

A mention is FALSE_POSITIVE if RET is mentioned but NOT detected:
- Testing panel descriptions alone: "Molecular testing (BRAF, RET/PTC 1/3, PAX8/PPARgamma and RAS panel)"
- Explicit negation: "RET/PTC1 REARRANGEMENT NOT DETECTED", "RET/PTC3 NOT DETECTED"
- "Negative for RET", "No RET alteration", "RET wild-type"
- Family history of someone ELSE having RET (not the patient)

When ambiguous, use alteration_type='unknown' and confidence<0.7.

Return strict JSON only:
{
  "classification": "TRUE_POSITIVE" | "FALSE_POSITIVE",
  "alteration_type": "germline_men2a" | "germline_men2b" | "germline_fmtc" | "somatic_fusion" | "somatic_mutation" | "therapy_only" | "negated" | "panel_description_only" | "family_history_only" | "unknown",
  "specific_variant": "<e.g., p.G533C, M918T, codon 609, RET/PTC3> or null",
  "confidence": 0.0-1.0,
  "reasoning": "<one sentence>"
}"""


def _parse_llm_response(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        # parts[1] is the code block content
        text = parts[1].lstrip("json\n").rstrip("`").strip()
    return json.loads(text)


def classify_haiku(context_250: str) -> dict:
    msg = client.messages.create(
        model=HAIKU_MODEL,
        max_tokens=400,
        system=SYSTEM,
        messages=[{
            "role": "user",
            "content": f"Context around 'RET' mention:\n\n{context_250}\n\nClassify this mention."
        }]
    )
    return _parse_llm_response(msg.content[0].text)


def classify_sonnet(context: str) -> dict:
    msg = client.messages.create(
        model=SONNET_MODEL,
        max_tokens=400,
        system=SYSTEM,
        messages=[{
            "role": "user",
            "content": f"Extended context:\n\n{context}\n\nClassify this mention."
        }]
    )
    return _parse_llm_response(msg.content[0].text)


# ---------------------------------------------------------------------------
# STEP 2 — Haiku primary pass
# ---------------------------------------------------------------------------
if START_STEP <= 2:
    print(f"\n--- STEP 2: Haiku 4.5 primary pass ({HAIKU_MODEL}) ---")

    rows = con.execute("SELECT * FROM _ret_adjudication_input_v226").fetchdf()
    n_total = len(rows)
    results = []
    n_errors = 0

    if DRY_RUN:
        print(f"  [DRY-RUN] Would classify {n_total} rows with Haiku 4.5 — skipping.")
    else:
        t0 = time.time()
        for i, row in rows.iterrows():
            if i % 20 == 0:
                elapsed = time.time() - t0
                print(f"  {i}/{n_total}  ({elapsed:.0f}s elapsed)")
            try:
                r = classify_haiku(row["context_250"])
                r["research_id"]      = row["research_id"]
                r["note_row_id"]      = row["note_row_id"]
                r["evidence_start"]   = row["evidence_start"]
                r["entity_note_type"] = row["entity_note_type"]
                r["auto_triage"]      = row["auto_triage"]
                r["context_250"]      = row["context_250"]
                r["context_500"]      = row["context_500"]
                r["llm_model"]        = HAIKU_MODEL
                results.append(r)
            except Exception as e:
                n_errors += 1
                print(f"  Row {i} (rid={row['research_id']}) failed: {e}")
                results.append({
                    "research_id":      row["research_id"],
                    "note_row_id":      row["note_row_id"],
                    "evidence_start":   row["evidence_start"],
                    "entity_note_type": row["entity_note_type"],
                    "auto_triage":      row["auto_triage"],
                    "context_250":      row["context_250"],
                    "context_500":      row["context_500"],
                    "llm_model":        HAIKU_MODEL,
                    "error":            str(e),
                    "classification":   None,
                    "alteration_type":  None,
                    "specific_variant": None,
                    "confidence":       None,
                    "reasoning":        None,
                })
            time.sleep(0.1)

        df = pd.DataFrame(results)
        con.register("ret_llm_df", df)
        con.execute(
            "CREATE OR REPLACE TABLE ret_note_entity_adjudication_v226 AS SELECT * FROM ret_llm_df"
        )

        n_tp = (df["classification"] == "TRUE_POSITIVE").sum()
        n_fp = (df["classification"] == "FALSE_POSITIVE").sum()
        print(f"\n  Haiku pass complete: {n_tp} TP  |  {n_fp} FP  |  {n_errors} errors  ({n_total} total)")

# ---------------------------------------------------------------------------
# STEP 3 — Sonnet reconsideration on low-confidence rows
# ---------------------------------------------------------------------------
if START_STEP <= 3:
    print(f"\n--- STEP 3: Sonnet 4.5 reconsideration ({SONNET_MODEL}) ---")

    lowconf = con.execute("""
        SELECT * FROM ret_note_entity_adjudication_v226
        WHERE (confidence < 0.85 OR alteration_type = 'unknown')
          AND classification IS NOT NULL
    """).fetchdf()

    n_lowconf = len(lowconf)
    print(f"  Low-confidence rows: {n_lowconf}")

    if DRY_RUN:
        print("  [DRY-RUN] Skipping Sonnet reconsideration.")
    elif n_lowconf == 0:
        print("  All rows high-confidence — no Sonnet reconsideration needed.")
    else:
        n_reclassified = 0
        for i, row in lowconf.iterrows():
            context = row.get("context_500") or row.get("context_250") or ""
            try:
                r = classify_sonnet(context)
                # Track whether Sonnet changed the call
                if r["classification"] != row.get("classification"):
                    n_reclassified += 1
                con.execute("""
                    UPDATE ret_note_entity_adjudication_v226
                    SET classification    = ?,
                        alteration_type   = ?,
                        specific_variant  = ?,
                        confidence        = ?,
                        reasoning         = ?,
                        llm_model         = 'claude-sonnet-4-5+haiku'
                    WHERE research_id    = ?
                      AND note_row_id    = ?
                      AND evidence_start = ?
                """, [
                    r["classification"], r["alteration_type"], r.get("specific_variant"),
                    r["confidence"], r["reasoning"],
                    row["research_id"], row["note_row_id"], row["evidence_start"]
                ])
            except Exception as e:
                print(f"  Sonnet row {i} failed: {e}")
            time.sleep(0.15)

        print(f"  Sonnet reconsideration: {n_lowconf} escalated, {n_reclassified} reclassified")

# ---------------------------------------------------------------------------
# STEP 4 — Patient-level rollup
# ---------------------------------------------------------------------------
if START_STEP <= 4:
    print("\n--- STEP 4: Patient-level rollup ---")

    sql_rollup = """
    CREATE OR REPLACE TABLE ret_patient_adjudicated_v226 AS
    SELECT
      CAST(research_id AS VARCHAR) AS research_id,
      BOOL_OR(classification = 'TRUE_POSITIVE') AS ret_note_true_positive,
      STRING_AGG(DISTINCT alteration_type, '; ')
        FILTER (WHERE classification = 'TRUE_POSITIVE')                           AS alteration_types,
      STRING_AGG(DISTINCT specific_variant, '; ')
        FILTER (WHERE classification = 'TRUE_POSITIVE' AND specific_variant IS NOT NULL) AS variants_reported,
      MAX(confidence) FILTER (WHERE classification = 'TRUE_POSITIVE')             AS max_tp_confidence,
      COUNT(*)        FILTER (WHERE classification = 'TRUE_POSITIVE')             AS n_tp_mentions,
      COUNT(*)        FILTER (WHERE classification = 'FALSE_POSITIVE')            AS n_fp_mentions,
      COUNT(*)                                                                    AS n_total_mentions
    FROM ret_note_entity_adjudication_v226
    WHERE classification IS NOT NULL
    GROUP BY 1
    """

    if not DRY_RUN:
        con.execute(sql_rollup)

    summary = con.execute("""
        SELECT
          COUNT(*) AS total_patients_reviewed,
          COUNT(*) FILTER (WHERE ret_note_true_positive)      AS patients_confirmed_ret_pos,
          COUNT(*) FILTER (WHERE NOT ret_note_true_positive)  AS patients_confirmed_ret_neg
        FROM ret_patient_adjudicated_v226
    """).fetchdf()
    print("  Patient rollup:")
    print(summary.to_string(index=False))

# ---------------------------------------------------------------------------
# STEP 5 — Propagate into canonical_patient_master
# ---------------------------------------------------------------------------
if START_STEP <= 5:
    print("\n--- STEP 5: Propagating into canonical_patient_master ---")

    if not DRY_RUN:
        for ddl in [
            "ALTER TABLE canonical_patient_master ADD COLUMN IF NOT EXISTS ret_note_adjudicated_positive BOOLEAN DEFAULT FALSE",
            "ALTER TABLE canonical_patient_master ADD COLUMN IF NOT EXISTS ret_note_alteration_types VARCHAR",
            "ALTER TABLE canonical_patient_master ADD COLUMN IF NOT EXISTS ret_note_variants_reported VARCHAR",
            "ALTER TABLE canonical_patient_master ADD COLUMN IF NOT EXISTS ret_note_confidence DOUBLE",
        ]:
            con.execute(ddl)

        con.execute("""
            UPDATE canonical_patient_master cpm
            SET ret_note_adjudicated_positive = rpa.ret_note_true_positive,
                ret_note_alteration_types     = rpa.alteration_types,
                ret_note_variants_reported    = rpa.variants_reported,
                ret_note_confidence           = rpa.max_tp_confidence
            FROM ret_patient_adjudicated_v226 rpa
            WHERE cpm.research_id = rpa.research_id
        """)

        # Unified flag (Script 225 ret_positive_v7 + note adjudication)
        con.execute(
            "ALTER TABLE canonical_patient_master ADD COLUMN IF NOT EXISTS ret_positive_unified BOOLEAN"
        )
        con.execute("""
            UPDATE canonical_patient_master
            SET ret_positive_unified =
                COALESCE(ret_positive_v7, FALSE) OR COALESCE(ret_note_adjudicated_positive, FALSE)
        """)

    verify = con.execute("""
        SELECT
          COUNT(*) FILTER (WHERE ret_positive_v7)                              AS n_variant_long,
          COUNT(*) FILTER (WHERE ret_note_adjudicated_positive)                AS n_note_adjudicated,
          COUNT(*) FILTER (WHERE ret_positive_unified)                         AS n_unified,
          COUNT(*) FILTER (WHERE ret_positive_v7 AND ret_note_adjudicated_positive) AS n_both
        FROM canonical_patient_master
    """).fetchdf()
    print("  CPM unified RET counts:")
    print(verify.to_string(index=False))

# ---------------------------------------------------------------------------
# STEP 6 — Register in detail_table_registry_v1
# ---------------------------------------------------------------------------
if START_STEP <= 6:
    print("\n--- STEP 6: Registering tables in detail_table_registry_v1 ---")

    if not DRY_RUN:
        con.execute("""
            INSERT INTO manuscript_workspace.detail_table_registry_v1
              (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
               domain, feeds_master_columns, description, canonical_version)
            VALUES
              ('ret_note_entity_adjudication_v226', 'main', 'research_id',
               'one row per RET note mention',
               (SELECT COUNT(*)              FROM ret_note_entity_adjudication_v226),
               (SELECT COUNT(DISTINCT research_id) FROM ret_note_entity_adjudication_v226),
               'Molecular/NLP',
               'ret_note_adjudicated_positive, ret_note_alteration_types, ret_note_variants_reported, ret_note_confidence',
               'LLM-adjudicated RET gene mentions from clinical notes. Haiku 4.5 primary pass + Sonnet 4.5 reconsideration on low-confidence. Built by Script 226.',
               'v1_0'),
              ('ret_patient_adjudicated_v226', 'main', 'research_id',
               'one row per patient',
               (SELECT COUNT(*) FROM ret_patient_adjudicated_v226),
               (SELECT COUNT(*) FROM ret_patient_adjudicated_v226),
               'Molecular/NLP',
               'ret_note_adjudicated_positive, ret_positive_unified',
               'Patient-level rollup of LLM-adjudicated RET mentions. Complements ret_positive_v7 (from molecular_variant_long) with germline/clinical-narrative RET signal (MTC, MEN2A, targeted therapy).',
               'v1_0')
        """)
        print("  Registered 2 tables in detail_table_registry_v1.")
    else:
        print("  [DRY-RUN] Skipping registry insert.")

# ---------------------------------------------------------------------------
# STEP 7 — Save summary JSON
# ---------------------------------------------------------------------------
if START_STEP <= 7:
    print("\n--- STEP 7: Writing summary JSON ---")

    try:
        mention_stats = con.execute("""
            SELECT
              COUNT(*) FILTER (WHERE classification='TRUE_POSITIVE')  AS haiku_tp,
              COUNT(*) FILTER (WHERE classification='FALSE_POSITIVE') AS haiku_fp,
              COUNT(*) FILTER (WHERE classification IS NULL)          AS errors,
              COUNT(*) AS total
            FROM ret_note_entity_adjudication_v226
        """).fetchone()

        sonnet_stats = con.execute("""
            SELECT COUNT(*) AS escalated,
                   COUNT(*) FILTER (WHERE llm_model='claude-sonnet-4-5+haiku') AS updated_by_sonnet
            FROM ret_note_entity_adjudication_v226
        """).fetchone()

        patient_stats = con.execute("""
            SELECT
              COUNT(*) FILTER (WHERE ret_note_adjudicated_positive) AS notes_only_or_both,
              COUNT(*) FILTER (WHERE ret_positive_v7 AND ret_note_adjudicated_positive) AS n_both,
              COUNT(*) FILTER (WHERE ret_positive_v7 AND NOT COALESCE(ret_note_adjudicated_positive,FALSE)) AS variant_only,
              COUNT(*) FILTER (WHERE NOT COALESCE(ret_positive_v7,FALSE) AND ret_note_adjudicated_positive) AS notes_only,
              COUNT(*) FILTER (WHERE ret_positive_unified) AS unified_total
            FROM canonical_patient_master
        """).fetchone()

        summary_json = {
            "script": "226",
            "timestamp": datetime.now().isoformat(),
            "dry_run": DRY_RUN,
            "haiku_pass": {
                "model": HAIKU_MODEL,
                "true_positive": int(mention_stats[0]) if mention_stats else None,
                "false_positive": int(mention_stats[1]) if mention_stats else None,
                "errors": int(mention_stats[2]) if mention_stats else None,
                "total": int(mention_stats[3]) if mention_stats else None,
            },
            "sonnet_reconsideration": {
                "model": SONNET_MODEL,
                "escalated": int(sonnet_stats[1]) if sonnet_stats else None,
            },
            "patient_yield": {
                "notes_adjudicated_positive": int(patient_stats[0]) if patient_stats else None,
                "both_signals": int(patient_stats[1]) if patient_stats else None,
                "variant_long_only": int(patient_stats[2]) if patient_stats else None,
                "notes_only": int(patient_stats[3]) if patient_stats else None,
                "ret_positive_unified_total": int(patient_stats[4]) if patient_stats else None,
            },
        }

        out_path = os.path.join(os.path.dirname(__file__), "output", "226_summary.json")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(summary_json, f, indent=2)
        print(f"  Summary written → {out_path}")
        print(json.dumps(summary_json, indent=2))

    except Exception as e:
        print(f"  Summary generation failed (non-fatal): {e}")

print(f"\n=== Script 226 complete  {datetime.now():%Y-%m-%d %H:%M:%S} ===")
