#!/usr/bin/env python3
"""
THYROID_2026 — Script 216b: Phase B LLM Extraction
Database: thyroid_ete_fix_20260413

Phase B LLM extraction tasks:
  B1: PET/CT structured extraction (422 exams, 291 patients)
  B2: MRI impression extraction (715 exams, 462 patients)
  B3: BMI extraction from H&P clinical notes (~2,447 patients)
  REBUILD: Canonical rebuild with B1/B2/B3 outputs

Run:
  .venv/bin/python scripts/216b_llm_extraction.py [--phase B1|B2|B3|REBUILD|all] [--dry-run]

Cost estimate:
  B1: ~422 reports × ~400 tok = ~169K tokens → ~$0.05
  B2: ~715 reports × ~250 tok = ~180K tokens → ~$0.05
  B3: ~2,447 notes × ~200 tok = ~490K tokens → ~$0.12
  Total: ~$0.25-0.50 with claude-haiku-3-5-20241022
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

DB = "thyroid_ete_fix_20260413"
CANONICAL = "canonical_patient_master_v1"
TOTAL_ROWS = 10871
MODEL = "claude-haiku-4-5"
MAX_TOKENS_OUT = 800
RATE_LIMIT_PAUSE = 0.15   # seconds between calls
MAX_RETRIES = 4

OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

B1_PARQUET = OUT_DIR / "_pet_extraction_216b.parquet"
B2_PARQUET = OUT_DIR / "_mri_impression_216b.parquet"
B3_PARQUET = OUT_DIR / "_bmi_extraction_216b.parquet"
B1_ROLLUP_PARQUET = OUT_DIR / "_pet_rollup_ready_216b.parquet"
B2_ROLLUP_PARQUET = OUT_DIR / "_mri_rollup_ready_216b.parquet"


# ======================================================================
# Connection
# ======================================================================

def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        print("[216b] ERROR: No MotherDuck token found.")
        sys.exit(1)
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


def get_existing_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(f"""
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchall()
    return {r[0] for r in rows}


def get_canonical_rids(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {str(r[0]) for r in con.execute(f"SELECT research_id FROM {CANONICAL}").fetchall()}


def check_invariants(con: duckdb.DuckDBPyConnection, table: str, label: str) -> bool:
    inv = con.execute(f"""
        SELECT COUNT(*) AS n, COUNT(DISTINCT research_id) AS d,
               COUNT(*) FILTER (WHERE research_id IS NULL) AS null_r,
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) AS null_fna
        FROM {table}
    """).fetchone()
    print(f"[216b] {label}: {inv[0]} rows, {inv[1]} distinct, {inv[2]} null RIDs, {inv[3]} null FNA")
    ok = (inv[0] == TOTAL_ROWS and inv[0] == inv[1] and inv[2] == 0)
    if not ok:
        if inv[0] != TOTAL_ROWS:
            print(f"  ERROR: Row count {inv[0]} != {TOTAL_ROWS}")
        if inv[0] != inv[1]:
            print(f"  ERROR: Duplicates: {inv[0] - inv[1]}")
    return ok


# ======================================================================
# LLM helper
# ======================================================================

def make_anthropic_client():
    import anthropic
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    return anthropic.Anthropic(api_key=api_key)


def call_llm(
    client,
    system_prompt: str,
    user_text: str,
    label: str = "",
) -> dict | None:
    """Call Anthropic API and return parsed JSON or None on failure."""
    for attempt in range(MAX_RETRIES):
        try:
            msg = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS_OUT,
                system=system_prompt,
                messages=[{"role": "user", "content": user_text}],
            )
            raw = msg.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.strip()
            return json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"  [{label}] JSON parse error (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(1.0)
        except Exception as e:
            err_str = str(e)
            if "rate_limit" in err_str.lower() or "529" in err_str or "overloaded" in err_str.lower():
                wait = 30 * (attempt + 1)
                print(f"  [{label}] Rate limit / overload — waiting {wait}s (attempt {attempt + 1})")
                time.sleep(wait)
            elif "timeout" in err_str.lower():
                print(f"  [{label}] Timeout (attempt {attempt + 1})")
                time.sleep(5)
            else:
                print(f"  [{label}] API error (attempt {attempt + 1}): {e}")
                time.sleep(2)
    return None


# ======================================================================
# B1: PET/CT structured extraction
# ======================================================================

PET_SYSTEM = """You are a nuclear medicine NLP extraction system. Given a PET, PET/CT, or related report text, extract structured findings.

Return ONLY valid JSON with exactly these keys:
{
  "exam_type": "PET/CT" or "PET" or "PET/MR" or "PET_other",
  "radiotracer": "FDG" or "DOTATATE" or "I-131" or "GA68" or "other" or null,
  "thyroid_bed": {
    "fdg_avid": true or false or null,
    "suv_max": number or null,
    "description": "brief string" or null
  },
  "cervical_lymph_nodes": {
    "fdg_avid": true or false or null,
    "suv_max": number or null,
    "locations": "string" or null
  },
  "distant_metastases": {
    "present": true or false or null,
    "sites": ["lung","bone","liver","mediastinal_ln","brain","other"] (empty array if none),
    "description": "brief string" or null
  },
  "impression_brief": "1-3 sentence summary of impression/conclusion" or null,
  "overall_assessment": "no_evidence_of_disease" or "local_recurrence" or "regional_metastasis" or "distant_metastasis" or "mixed" or "indeterminate" or "not_thyroid_related"
}
If the report is not about thyroid or is unrelated, set overall_assessment to "not_thyroid_related".
Extract only what is explicitly stated. Use null for missing values."""


def task_b1_pet_extraction(con: duckdb.DuckDBPyConnection, dry_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 70)
    print("[216b] TASK B1: PET/CT Structured Extraction")
    print("=" * 70)

    df_pet = con.execute("""
        SELECT
            research_id,
            CAST(research_id AS VARCHAR) AS rid,
            exam_type_normalized,
            indication,
            CAST(date_of_exam AS VARCHAR) AS exam_date,
            CAST(original_report AS VARCHAR) AS report_text
        FROM ct_imaging
        WHERE LOWER(exam_type_normalized) LIKE '%pet%'
          AND original_report IS NOT NULL
        ORDER BY CAST(research_id AS VARCHAR), date_of_exam
    """).fetchdf()
    print(f"  PET exams to process: {len(df_pet)}")

    if B1_PARQUET.exists():
        done_df = pd.read_parquet(B1_PARQUET)
        done_keys = set(zip(done_df["rid"].astype(str), done_df["exam_date"].astype(str)))
        df_pet["exam_date_str"] = df_pet["exam_date"].astype(str)
        remaining = df_pet[~df_pet.apply(
            lambda r: (str(r["rid"]), r["exam_date_str"]) in done_keys, axis=1
        )]
        print(f"  Resuming: {len(done_df)} done, {len(remaining)} remaining")
    else:
        done_df = pd.DataFrame()
        remaining = df_pet

    if dry_run:
        print(f"  [DRY RUN] Would process {len(remaining)} PET reports")
        return done_df if not done_df.empty else pd.DataFrame()

    client = make_anthropic_client()
    results = []
    for idx, row in remaining.iterrows():
        label = f"PET rid={row['rid']} {row['exam_date']}"
        report_trunc = (row["report_text"] or "")[:3000]
        user_msg = f"INDICATION: {row['indication'] or 'not specified'}\n\nREPORT:\n{report_trunc}"

        parsed = call_llm(client, PET_SYSTEM, user_msg, label)
        time.sleep(RATE_LIMIT_PAUSE)

        rec = {
            "rid": str(row["rid"]),
            "exam_date": str(row["exam_date"]),
            "exam_type_normalized": row["exam_type_normalized"],
            "indication": row["indication"],
            "llm_exam_type": None,
            "llm_radiotracer": None,
            "llm_thyroid_bed_avid": None,
            "llm_thyroid_bed_suv_max": None,
            "llm_thyroid_bed_description": None,
            "llm_cervical_ln_avid": None,
            "llm_cervical_ln_suv_max": None,
            "llm_cervical_ln_locations": None,
            "llm_distant_mets": None,
            "llm_distant_met_sites": None,
            "llm_distant_met_description": None,
            "llm_impression_brief": None,
            "llm_overall_assessment": None,
            "llm_parse_ok": False,
        }

        if parsed:
            rec["llm_exam_type"] = parsed.get("exam_type")
            rec["llm_radiotracer"] = parsed.get("radiotracer")
            tb = parsed.get("thyroid_bed") or {}
            rec["llm_thyroid_bed_avid"] = tb.get("fdg_avid")
            rec["llm_thyroid_bed_suv_max"] = tb.get("suv_max")
            rec["llm_thyroid_bed_description"] = tb.get("description")
            cl = parsed.get("cervical_lymph_nodes") or {}
            rec["llm_cervical_ln_avid"] = cl.get("fdg_avid")
            rec["llm_cervical_ln_suv_max"] = cl.get("suv_max")
            rec["llm_cervical_ln_locations"] = cl.get("locations")
            dm = parsed.get("distant_metastases") or {}
            rec["llm_distant_mets"] = dm.get("present")
            sites = dm.get("sites") or []
            rec["llm_distant_met_sites"] = ",".join(sites) if sites else None
            rec["llm_distant_met_description"] = dm.get("description")
            rec["llm_impression_brief"] = parsed.get("impression_brief")
            rec["llm_overall_assessment"] = parsed.get("overall_assessment")
            rec["llm_parse_ok"] = True

        results.append(rec)

        if len(results) % 50 == 0:
            partial = pd.concat([done_df, pd.DataFrame(results)], ignore_index=True) if not done_df.empty else pd.DataFrame(results)
            partial.to_parquet(B1_PARQUET, index=False)
            ok_count = sum(r["llm_parse_ok"] for r in results)
            print(f"  Progress: {len(results)}/{len(remaining)} — {ok_count} parsed OK")

    all_df = pd.concat([done_df, pd.DataFrame(results)], ignore_index=True) if not done_df.empty else pd.DataFrame(results)
    all_df.to_parquet(B1_PARQUET, index=False)
    ok = all_df["llm_parse_ok"].sum() if "llm_parse_ok" in all_df.columns else 0
    print(f"  ✓ B1 complete: {len(all_df)} PET exams, {ok} parsed OK ({ok/len(all_df)*100:.1f}%)")
    return all_df


def _pet_rollup(pet_df: pd.DataFrame) -> pd.DataFrame:
    """Roll up PET extraction to patient level."""
    if pet_df.empty:
        return pd.DataFrame(columns=["rid"])

    pet_df = pet_df.copy()
    pet_df["llm_thyroid_bed_avid"] = pet_df["llm_thyroid_bed_avid"].apply(
        lambda x: bool(x) if x is not None and str(x).lower() not in ("none", "nan", "") else None
    )

    grp = pet_df.groupby("rid")

    def any_true(series):
        vals = series.dropna()
        return True if any(str(v).lower() == "true" or v is True or v == 1 for v in vals) else (None if vals.empty else False)

    rollup_rows = []
    for rid, g in grp:
        g = g.sort_values("exam_date")
        first = g.iloc[0]
        last = g.iloc[-1]
        assessments = g["llm_overall_assessment"].dropna().tolist()
        worst = "no_evidence_of_disease"
        hierarchy = ["distant_metastasis", "mixed", "regional_metastasis",
                     "local_recurrence", "indeterminate", "not_thyroid_related",
                     "no_evidence_of_disease"]
        for level in hierarchy:
            if level in assessments:
                worst = level
                break

        all_sites = []
        for sites_str in g["llm_distant_met_sites"].dropna():
            for s in str(sites_str).split(","):
                s = s.strip()
                if s:
                    all_sites.append(s)
        unique_sites = sorted(set(all_sites))

        thyroid_avid = any_true(g["llm_thyroid_bed_avid"])
        cervical_avid = any_true(g["llm_cervical_ln_avid"])
        distant_present_vals = g["llm_distant_mets"].dropna()
        distant_any = True if any(str(v).lower() == "true" or v is True for v in distant_present_vals) else (None if distant_present_vals.empty else False)

        suv_thyroid = g["llm_thyroid_bed_suv_max"].dropna()
        suv_cervical = g["llm_cervical_ln_suv_max"].dropna()

        rollup_rows.append({
            "rid": str(rid),
            "pet_n_exams": len(g),
            "pet_has_data": True,
            "pet_first_date": g["exam_date"].min(),
            "pet_last_date": g["exam_date"].max(),
            "pet_indication_first": str(first["indication"]) if pd.notna(first["indication"]) else None,
            "pet_impression_last": str(last["llm_impression_brief"]) if pd.notna(last["llm_impression_brief"]) else None,
            "pet_radiotracer_primary": str(first["llm_radiotracer"]) if pd.notna(first["llm_radiotracer"]) else None,
            "pet_fdg_avid_thyroid_bed_ever": thyroid_avid,
            "pet_fdg_avid_cervical_ln_ever": cervical_avid,
            "pet_distant_mets_ever": distant_any,
            "pet_suv_max_thyroid_bed": float(suv_thyroid.max()) if not suv_thyroid.empty else None,
            "pet_suv_max_cervical_ln": float(suv_cervical.max()) if not suv_cervical.empty else None,
            "pet_distant_met_sites": ",".join(unique_sites) if unique_sites else None,
            "pet_overall_worst": worst,
        })

    result = pd.DataFrame(rollup_rows)
    print(f"  PET rollup: {len(result)} patients")
    return result


# ======================================================================
# B2: MRI impression extraction
# ======================================================================

MRI_SYSTEM = """You are a radiology NLP extraction system. Given an MRI report, extract the impression/conclusion and key findings.

Return ONLY valid JSON with exactly these keys:
{
  "impression": "the IMPRESSION or CONCLUSION section text (verbatim or closely paraphrased, max 300 chars)" or null,
  "key_findings": ["array of up to 4 key findings as brief strings"],
  "thyroid_assessment": "normal" or "abnormal" or "post_surgical" or "not_visualized" or "mass_present" or null,
  "recommendation": "any follow-up recommendation mentioned" or null,
  "has_thyroid_finding": true or false
}
Extract only what is explicitly stated. Use null for missing values."""


def task_b2_mri_impressions(con: duckdb.DuckDBPyConnection, dry_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 70)
    print("[216b] TASK B2: MRI Impression Extraction")
    print("=" * 70)

    df_mri = con.execute("""
        SELECT
            research_id,
            CAST(research_id AS VARCHAR) AS rid,
            CAST(date_of_exam AS VARCHAR) AS exam_date,
            exam_type_detail,
            indication,
            CAST(original_report AS VARCHAR) AS report_text
        FROM mri_imaging
        WHERE original_report IS NOT NULL
        ORDER BY CAST(research_id AS VARCHAR), date_of_exam
    """).fetchdf()
    print(f"  MRI exams to process: {len(df_mri)}")

    if B2_PARQUET.exists():
        done_df = pd.read_parquet(B2_PARQUET)
        done_keys = set(zip(done_df["rid"].astype(str), done_df["exam_date"].astype(str)))
        remaining = df_mri[~df_mri.apply(
            lambda r: (str(r["rid"]), str(r["exam_date"])) in done_keys, axis=1
        )]
        print(f"  Resuming: {len(done_df)} done, {len(remaining)} remaining")
    else:
        done_df = pd.DataFrame()
        remaining = df_mri

    if dry_run:
        print(f"  [DRY RUN] Would process {len(remaining)} MRI reports")
        return done_df if not done_df.empty else pd.DataFrame()

    client = make_anthropic_client()
    results = []

    for idx, row in remaining.iterrows():
        label = f"MRI rid={row['rid']} {row['exam_date']}"
        report_trunc = (row["report_text"] or "")[:2500]
        user_msg = f"INDICATION: {row['indication'] or 'not specified'}\n\nREPORT:\n{report_trunc}"

        parsed = call_llm(client, MRI_SYSTEM, user_msg, label)
        time.sleep(RATE_LIMIT_PAUSE)

        rec = {
            "rid": str(row["rid"]),
            "exam_date": str(row["exam_date"]),
            "exam_type_detail": row["exam_type_detail"],
            "llm_impression": None,
            "llm_key_findings": None,
            "llm_thyroid_assessment": None,
            "llm_recommendation": None,
            "llm_has_thyroid_finding": None,
            "llm_parse_ok": False,
        }
        if parsed:
            rec["llm_impression"] = parsed.get("impression")
            findings = parsed.get("key_findings") or []
            rec["llm_key_findings"] = "; ".join(str(f) for f in findings[:4]) if findings else None
            rec["llm_thyroid_assessment"] = parsed.get("thyroid_assessment")
            rec["llm_recommendation"] = parsed.get("recommendation")
            rec["llm_has_thyroid_finding"] = parsed.get("has_thyroid_finding")
            rec["llm_parse_ok"] = True

        results.append(rec)

        if len(results) % 100 == 0:
            partial = pd.concat([done_df, pd.DataFrame(results)], ignore_index=True) if not done_df.empty else pd.DataFrame(results)
            partial.to_parquet(B2_PARQUET, index=False)
            ok_count = sum(r["llm_parse_ok"] for r in results)
            print(f"  Progress: {len(results)}/{len(remaining)} — {ok_count} parsed OK")

    all_df = pd.concat([done_df, pd.DataFrame(results)], ignore_index=True) if not done_df.empty else pd.DataFrame(results)
    all_df.to_parquet(B2_PARQUET, index=False)
    ok = all_df["llm_parse_ok"].sum() if "llm_parse_ok" in all_df.columns else 0
    print(f"  ✓ B2 complete: {len(all_df)} MRI exams, {ok} parsed OK ({ok/len(all_df)*100:.1f}%)")
    return all_df


def _mri_impression_rollup(mri_df: pd.DataFrame) -> pd.DataFrame:
    """Roll up MRI impression to patient level."""
    if mri_df.empty:
        return pd.DataFrame(columns=["rid"])

    rows = []
    for rid, g in mri_df.groupby("rid"):
        g = g.sort_values("exam_date")
        first = g.iloc[0]
        last = g.iloc[-1]
        rows.append({
            "rid": str(rid),
            "mri_impression_first": str(first["llm_impression"]) if pd.notna(first.get("llm_impression")) else None,
            "mri_impression_last": str(last["llm_impression"]) if pd.notna(last.get("llm_impression")) else None,
            "mri_key_findings_last": str(last["llm_key_findings"]) if pd.notna(last.get("llm_key_findings")) else None,
            "mri_recommendation_last": str(last["llm_recommendation"]) if pd.notna(last.get("llm_recommendation")) else None,
            "mri_thyroid_assessment_worst": _worst_mri_assessment(g["llm_thyroid_assessment"].dropna().tolist()),
        })
    result = pd.DataFrame(rows)
    print(f"  MRI impression rollup: {len(result)} patients")
    return result


def _worst_mri_assessment(assessments: list[str]) -> str | None:
    hierarchy = ["mass_present", "abnormal", "post_surgical", "not_visualized", "normal"]
    for level in hierarchy:
        if level in assessments:
            return level
    return assessments[0] if assessments else None


# ======================================================================
# B3: BMI extraction from H&P clinical notes
# ======================================================================

BMI_SYSTEM = """You are a clinical NLP extraction system. Extract vital signs/anthropometrics from a clinical note.

Return ONLY valid JSON with exactly these keys:
{
  "bmi": number or null,
  "weight_value": number or null,
  "weight_unit": "kg" or "lbs" or null,
  "height_value": number or null,
  "height_unit": "cm" or "in" or "ft_in" or null,
  "height_ft": number or null,
  "height_in_part": number or null,
  "bmi_source_phrase": "the exact text snippet where BMI was found" or null
}
Rules:
- Only extract values explicitly stated in the text. Do NOT calculate BMI.
- BMI must be a plausible value (10-80). Ignore implausible values.
- Weight: plausible range 30-350 lbs or 15-160 kg.
- Height: plausible range 48-84 inches or 120-220 cm.
- If the note says "BMI 28.5" extract 28.5. If it says "BMI: 28.5 kg/m2" extract 28.5.
- Use null if the value is not present or not plausible."""


def task_b3_bmi_extraction(con: duckdb.DuckDBPyConnection, dry_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 70)
    print("[216b] TASK B3: BMI Extraction from H&P Notes")
    print("=" * 70)

    df_notes = con.execute("""
        WITH ranked AS (
            SELECT
                CAST(n.research_id AS VARCHAR) AS rid,
                n.note_text,
                n.note_index,
                n.source_column,
                ROW_NUMBER() OVER (
                    PARTITION BY n.research_id
                    ORDER BY n.note_index ASC
                ) AS rn
            FROM clinical_notes_long n
            WHERE n.source_column LIKE 'h_p%'
              AND (LOWER(n.note_text) LIKE '%bmi%' OR LOWER(n.note_text) LIKE '%body mass index%')
              AND CAST(n.research_id AS VARCHAR) IN (
                  SELECT research_id FROM canonical_patient_master_v1
                  WHERE nsqip_bmi IS NULL AND ops_bmi IS NULL
              )
        )
        SELECT rid, note_text, note_index, source_column
        FROM ranked
        WHERE rn = 1
        ORDER BY rid
    """).fetchdf()
    print(f"  H&P notes to process (1 per patient): {len(df_notes)}")

    if B3_PARQUET.exists():
        done_df = pd.read_parquet(B3_PARQUET)
        done_rids = set(done_df["rid"].astype(str))
        remaining = df_notes[~df_notes["rid"].astype(str).isin(done_rids)]
        print(f"  Resuming: {len(done_df)} done, {len(remaining)} remaining")
    else:
        done_df = pd.DataFrame()
        remaining = df_notes

    if dry_run:
        print(f"  [DRY RUN] Would process {len(remaining)} H&P notes for BMI")
        return done_df if not done_df.empty else pd.DataFrame()

    client = make_anthropic_client()
    results = []

    for idx, row in remaining.iterrows():
        label = f"BMI rid={row['rid']}"
        note_trunc = (row["note_text"] or "")[:2000]
        user_msg = f"CLINICAL NOTE:\n{note_trunc}"

        parsed = call_llm(client, BMI_SYSTEM, user_msg, label)
        time.sleep(RATE_LIMIT_PAUSE)

        rec = {
            "rid": str(row["rid"]),
            "note_index": row["note_index"],
            "source_column": row["source_column"],
            "bmi_extracted": None,
            "weight_value": None,
            "weight_unit": None,
            "height_value": None,
            "height_unit": None,
            "height_ft": None,
            "height_in_part": None,
            "bmi_source_phrase": None,
            "llm_parse_ok": False,
        }

        if parsed:
            bmi = parsed.get("bmi")
            if bmi is not None:
                try:
                    bmi_f = float(bmi)
                    if 10 <= bmi_f <= 80:
                        rec["bmi_extracted"] = bmi_f
                except (ValueError, TypeError):
                    pass
            wt = parsed.get("weight_value")
            if wt is not None:
                try:
                    wt_f = float(wt)
                    wu = parsed.get("weight_unit")
                    if wu == "lbs" and 30 <= wt_f <= 700:
                        rec["weight_value"] = wt_f
                        rec["weight_unit"] = "lbs"
                    elif wu == "kg" and 15 <= wt_f <= 315:
                        rec["weight_value"] = wt_f
                        rec["weight_unit"] = "kg"
                except (ValueError, TypeError):
                    pass
            ht = parsed.get("height_value")
            if ht is not None:
                try:
                    ht_f = float(ht)
                    hu = parsed.get("height_unit")
                    if hu in ("in", "cm", "ft_in") and ht_f > 0:
                        rec["height_value"] = ht_f
                        rec["height_unit"] = hu
                except (ValueError, TypeError):
                    pass
            rec["height_ft"] = parsed.get("height_ft")
            rec["height_in_part"] = parsed.get("height_in_part")
            rec["bmi_source_phrase"] = str(parsed.get("bmi_source_phrase") or "")[:200] or None
            rec["llm_parse_ok"] = True

        results.append(rec)

        if len(results) % 200 == 0:
            partial = pd.concat([done_df, pd.DataFrame(results)], ignore_index=True) if not done_df.empty else pd.DataFrame(results)
            partial.to_parquet(B3_PARQUET, index=False)
            ok_count = sum(r["llm_parse_ok"] for r in results)
            found_bmi = sum(1 for r in results if r["bmi_extracted"] is not None)
            print(f"  Progress: {len(results)}/{len(remaining)} — {ok_count} parsed, {found_bmi} with BMI")

    all_df = pd.concat([done_df, pd.DataFrame(results)], ignore_index=True) if not done_df.empty else pd.DataFrame(results)
    all_df.to_parquet(B3_PARQUET, index=False)
    ok = all_df["llm_parse_ok"].sum() if "llm_parse_ok" in all_df.columns else 0
    found = all_df["bmi_extracted"].notna().sum() if "bmi_extracted" in all_df.columns else 0
    print(f"  ✓ B3 complete: {len(all_df)} notes processed, {ok} parsed OK, {found} with BMI extracted ({found/len(all_df)*100:.1f}%)")
    return all_df


def _bmi_rollup(bmi_df: pd.DataFrame) -> pd.DataFrame:
    """Roll up BMI extraction to patient level."""
    if bmi_df.empty:
        return pd.DataFrame(columns=["rid"])

    valid = bmi_df[bmi_df["bmi_extracted"].notna()].copy()
    rollup = valid.groupby("rid").agg(
        bmi_note_extracted=("bmi_extracted", "first"),
        bmi_note_source=("source_column", "first"),
    ).reset_index()

    # Compute weight_kg for those with valid weight
    def to_kg(row):
        if row["weight_value"] is None or pd.isna(row["weight_value"]):
            return None
        try:
            wt = float(row["weight_value"])
            if str(row.get("weight_unit", "")) == "lbs":
                return round(wt * 0.453592, 1)
            elif str(row.get("weight_unit", "")) == "kg":
                return wt
        except (ValueError, TypeError):
            pass
        return None

    bmi_df = bmi_df.copy()
    bmi_df["weight_kg_calc"] = bmi_df.apply(to_kg, axis=1)

    wt_rollup = bmi_df[bmi_df["weight_kg_calc"].notna()].groupby("rid").agg(
        weight_kg_note=("weight_kg_calc", "first"),
    ).reset_index()

    result = rollup.merge(wt_rollup, on="rid", how="left")
    print(f"  BMI rollup: {len(result)} patients with extracted BMI")
    return result


# ======================================================================
# REBUILD: Canonical rebuild
# ======================================================================

def task_rebuild(
    con: duckdb.DuckDBPyConnection,
    pet_rollup: pd.DataFrame,
    mri_rollup: pd.DataFrame,
    bmi_rollup: pd.DataFrame,
    dry_run: bool,
) -> int:
    print("\n" + "=" * 70)
    print("[216b] TASK REBUILD: Canonical Rebuild with B1/B2/B3")
    print("=" * 70)

    existing = get_existing_columns(con)
    cur_count = len(existing)
    print(f"  Current canonical: {TOTAL_ROWS} x {cur_count} columns")

    tmp_dir = REPO / "scripts" / "output"
    cte_parts = []
    select_parts = []
    join_parts = []

    # ── PET rollup ──
    if not pet_rollup.empty:
        pet_tmp = tmp_dir / "_pet_rollup_216b.parquet"
        pet_rollup.to_parquet(pet_tmp, index=False)
        con.execute(f"CREATE OR REPLACE TEMP TABLE pet_rollup AS SELECT * FROM read_parquet('{pet_tmp}')")

        pet_cols_map = {
            "pet_n_exams": "INTEGER", "pet_has_data": "BOOLEAN",
            "pet_first_date": "VARCHAR", "pet_last_date": "VARCHAR",
            "pet_indication_first": "VARCHAR", "pet_impression_last": "VARCHAR",
            "pet_radiotracer_primary": "VARCHAR",
            "pet_fdg_avid_thyroid_bed_ever": "BOOLEAN",
            "pet_fdg_avid_cervical_ln_ever": "BOOLEAN",
            "pet_distant_mets_ever": "BOOLEAN",
            "pet_suv_max_thyroid_bed": "DOUBLE",
            "pet_suv_max_cervical_ln": "DOUBLE",
            "pet_distant_met_sites": "VARCHAR",
            "pet_overall_worst": "VARCHAR",
        }
        new_pet = [c for c in pet_cols_map if c not in existing]
        if new_pet:
            select_parts.extend([f'pt."{c}"' for c in new_pet])
            join_parts.append("LEFT JOIN pet_rollup pt ON c.research_id = pt.rid")
            print(f"  PET: {len(new_pet)} new columns")

    # ── MRI impression rollup ──
    if not mri_rollup.empty:
        mri_tmp = tmp_dir / "_mri_rollup_216b.parquet"
        mri_rollup.to_parquet(mri_tmp, index=False)
        con.execute(f"CREATE OR REPLACE TEMP TABLE mri_impression_rollup AS SELECT * FROM read_parquet('{mri_tmp}')")

        mri_imp_cols = ["mri_impression_first", "mri_impression_last",
                        "mri_key_findings_last", "mri_recommendation_last",
                        "mri_thyroid_assessment_worst"]
        new_mri_imp = [c for c in mri_imp_cols if c not in existing]
        if new_mri_imp:
            select_parts.extend([f'mi."{c}"' for c in new_mri_imp])
            join_parts.append("LEFT JOIN mri_impression_rollup mi ON c.research_id = mi.rid")
            print(f"  MRI impression: {len(new_mri_imp)} new columns")

    # ── BMI rollup ──
    if not bmi_rollup.empty:
        bmi_tmp = tmp_dir / "_bmi_rollup_216b.parquet"
        bmi_rollup.to_parquet(bmi_tmp, index=False)
        con.execute(f"CREATE OR REPLACE TEMP TABLE bmi_rollup AS SELECT * FROM read_parquet('{bmi_tmp}')")

        bmi_cols_needed = ["bmi_note_extracted", "bmi_note_source", "weight_kg_note",
                           "bmi_combined", "bmi_source"]
        new_bmi = [c for c in bmi_cols_needed[:3] if c not in existing]

        if "bmi_combined" not in existing or "bmi_source" not in existing:
            select_parts.append("""
COALESCE(c.nsqip_bmi, c.ops_bmi, br.bmi_note_extracted) AS bmi_combined
""".strip())
            select_parts.append("""
CASE
  WHEN c.nsqip_bmi IS NOT NULL THEN 'nsqip'
  WHEN c.ops_bmi IS NOT NULL THEN 'op_sheet'
  WHEN br.bmi_note_extracted IS NOT NULL THEN 'clinical_note_llm'
  ELSE NULL
END AS bmi_source
""".strip())

        if new_bmi:
            select_parts.extend([f'br."{c}"' for c in new_bmi])

        if new_bmi or "bmi_combined" not in existing:
            join_parts.append("LEFT JOIN bmi_rollup br ON c.research_id = br.rid")
            print(f"  BMI: {len(new_bmi) + 2} new columns (incl. bmi_combined, bmi_source)")

    if not select_parts:
        print("  No new columns to add — skipping rebuild")
        return cur_count

    if dry_run:
        print(f"  [DRY RUN] Would add {len(select_parts)} new columns/expressions")
        return cur_count

    select_block = ",\n    ".join(select_parts)
    join_block = "\n".join(join_parts)

    rebuild_sql = f"""
SELECT
    c.*,
    {select_block}
FROM {CANONICAL} c
{join_block}
"""
    staging = f"{CANONICAL}_staging_216b"
    print(f"  Creating staging table {staging}...")
    t0 = time.time()
    con.execute(f"DROP TABLE IF EXISTS {staging}")
    con.execute(f"CREATE TABLE {staging} AS {rebuild_sql}")

    if not check_invariants(con, staging, "Staging"):
        print("  ABORTING — invariant failure")
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        sys.exit(1)

    print("  Invariants passed — swapping tables...")
    con.execute(f"DROP TABLE IF EXISTS {CANONICAL}")
    con.execute(f"ALTER TABLE {staging} RENAME TO {CANONICAL}")

    for tmp_file in [tmp_dir / "_pet_rollup_216b.parquet",
                     tmp_dir / "_mri_rollup_216b.parquet",
                     tmp_dir / "_bmi_rollup_216b.parquet"]:
        try:
            tmp_file.unlink()
        except Exception:
            pass

    new_count = len(get_existing_columns(con))
    elapsed = time.time() - t0
    print(f"  ✓ Rebuild complete: {TOTAL_ROWS} x {new_count} columns (+{new_count - cur_count}) in {elapsed:.1f}s")
    return new_count


# ======================================================================
# Final validation
# ======================================================================

def final_validation(con: duckdb.DuckDBPyConnection) -> None:
    print("\n" + "=" * 70)
    print("[216b] FINAL VALIDATION")
    print("=" * 70)

    if not check_invariants(con, CANONICAL, "Final"):
        print("  INVARIANT FAILURE")
        return

    new_cols = [
        "pet_n_exams", "pet_has_data", "pet_fdg_avid_thyroid_bed_ever",
        "pet_distant_mets_ever", "pet_overall_worst",
        "mri_impression_first", "mri_impression_last", "mri_thyroid_assessment_worst",
        "bmi_combined", "bmi_source", "bmi_note_extracted",
    ]
    cols = get_existing_columns(con)
    print("\n  Coverage report:")
    for col in new_cols:
        if col not in cols:
            print(f"    {col:45s}: NOT IN CANONICAL")
            continue
        row = con.execute(f"""
            SELECT COUNT(*) FILTER (WHERE "{col}" IS NOT NULL) AS n,
                   ROUND(COUNT(*) FILTER (WHERE "{col}" IS NOT NULL) * 100.0 / {TOTAL_ROWS}, 1) AS pct
            FROM {CANONICAL}
        """).fetchone()
        print(f"    {col:45s}: {row[0]:>6,} ({row[1]:>5.1f}%)")

    total_cols = len(cols)
    print(f"\n  FINAL: {CANONICAL} = {TOTAL_ROWS} rows × {total_cols} columns")


# ======================================================================
# Main
# ======================================================================

def main():
    parser = argparse.ArgumentParser(description="Script 216b: Phase B LLM Extraction")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--phase", default="all",
                        help="B1|B2|B3|REBUILD|all (comma-separated OK)")
    args = parser.parse_args()

    phases = set(p.strip().upper() for p in args.phase.split(",")) if args.phase != "all" else {
        "B1", "B2", "B3", "REBUILD"
    }
    print(f"[216b] Connected to {DB}")
    print(f"[216b] Dry run: {args.dry_run} | Phases: {sorted(phases)}")

    con = connect()
    cur_rows = con.execute(f"SELECT COUNT(*) FROM {CANONICAL}").fetchone()[0]
    cur_cols = len(get_existing_columns(con))
    print(f"[216b] Current canonical: {cur_rows} rows × {cur_cols} columns")

    pet_df = pd.DataFrame()
    mri_imp_df = pd.DataFrame()
    bmi_df = pd.DataFrame()

    if "B1" in phases:
        pet_df = task_b1_pet_extraction(con, args.dry_run)

    if "B2" in phases:
        mri_imp_df = task_b2_mri_impressions(con, args.dry_run)

    if "B3" in phases:
        bmi_df = task_b3_bmi_extraction(con, args.dry_run)

    # Load from parquet if phase was skipped but file exists (for REBUILD)
    if "B1" not in phases and B1_PARQUET.exists() and "REBUILD" in phases:
        pet_df = pd.read_parquet(B1_PARQUET)
        print(f"  Loaded cached B1: {len(pet_df)} PET exams")
    if "B2" not in phases and B2_PARQUET.exists() and "REBUILD" in phases:
        mri_imp_df = pd.read_parquet(B2_PARQUET)
        print(f"  Loaded cached B2: {len(mri_imp_df)} MRI exams")
    if "B3" not in phases and B3_PARQUET.exists() and "REBUILD" in phases:
        bmi_df = pd.read_parquet(B3_PARQUET)
        print(f"  Loaded cached B3: {len(bmi_df)} BMI notes")

    # Use pre-computed rollups if available (avoids re-processing raw extractions)
    if B1_ROLLUP_PARQUET.exists() and pet_df.empty:
        pet_rollup = pd.read_parquet(B1_ROLLUP_PARQUET)
        print(f"  Using pre-computed PET rollup: {len(pet_rollup)} patients")
    else:
        pet_rollup = _pet_rollup(pet_df) if not pet_df.empty else pd.DataFrame()

    if B2_ROLLUP_PARQUET.exists() and mri_imp_df.empty:
        mri_rollup = pd.read_parquet(B2_ROLLUP_PARQUET)
        print(f"  Using pre-computed MRI rollup: {len(mri_rollup)} patients")
    else:
        mri_rollup = _mri_impression_rollup(mri_imp_df) if not mri_imp_df.empty else pd.DataFrame()

    bmi_rollup = _bmi_rollup(bmi_df) if not bmi_df.empty else pd.DataFrame()

    if "REBUILD" in phases:
        task_rebuild(con, pet_rollup, mri_rollup, bmi_rollup, args.dry_run)
        if not args.dry_run:
            final_validation(con)

    print("\n[216b] Done.")
    con.close()


if __name__ == "__main__":
    main()
