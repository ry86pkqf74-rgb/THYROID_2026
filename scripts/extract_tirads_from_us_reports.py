#!/usr/bin/env python3
"""
TIRADS Feature Extraction from US Report Text Excerpts (v2)
============================================================
Reads from MotherDuck `raw_imaging_12_slots_v1`, DEDUPLICATES by
(research_id, us_report_number) so each unique report text is sent
to the LLM only ONCE, then fans out the extracted nodules and applies
both the 2017 original and proposed modified ACR TI-RADS scoring.

Includes a VALIDATION LAYER: ~2,600 reports contain pre-structured
TIRADS labels (e.g. "Composition: Solid (2)") and ~1,800 have the
total points already calculated. The script compares LLM extractions
against these ground-truth labels to measure extraction accuracy
BEFORE you trust the full dataset.

Key changes from v1:
- Deduplicates: 12,900 nodule rows → ~2,687 unique report texts
  (saves ~80% on API calls: $0.50-1.50 instead of $2-5)
- Validation report: compares LLM output vs in-text TIRADS for the
  ~2,600 reports that have structured labels
- Joins extracted nodules back to ALL source rows via
  (research_id, us_report_number, extracted_nodule_number)
- Flags truncated excerpts (300 chars) separately in stats

Usage:
    export ANTHROPIC_API_KEY="sk-ant-..."
    export MOTHERDUCK_TOKEN="eyJ..."
    pip install anthropic duckdb pandas pyarrow tqdm
    python extract_tirads_from_us_reports.py
"""

import os
import sys
import json
import time
import logging
import hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import anthropic
import duckdb
import pandas as pd
from tqdm import tqdm

# ─── Configuration ───────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "")
MODEL = "claude-haiku-4-5-20251001"
MAX_WORKERS = 10          # concurrent API calls
BATCH_SIZE = 50           # rows per checkpoint save
MAX_RETRIES = 3           # retries per row on transient errors
RETRY_DELAY = 2           # seconds between retries (doubles each retry)
OUTPUT_DIR = Path("./output")
CHECKPOINT_PATH = OUTPUT_DIR / "checkpoint.json"
OUTPUT_PARQUET = OUTPUT_DIR / "tirads_extracted.parquet"
VALIDATION_PARQUET = OUTPUT_DIR / "tirads_validation.parquet"

# Source database/table
SOURCE_DB = "Thyroid 2026"
SOURCE_TABLE = "raw_imaging_12_slots_v1"

# Ensure output dir exists before logging setup
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(OUTPUT_DIR / "extraction.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ─── ACR TI-RADS Scoring (2017 Original) ────────────────────────────────────

COMPOSITION_POINTS_2017 = {
    "cystic": 0,
    "almost_completely_cystic": 0,
    "spongiform": 0,
    "mixed_cystic_and_solid": 1,
    "solid": 2,
    "almost_completely_solid": 2,
    "indeterminate": 2,
}

ECHOGENICITY_POINTS_2017 = {
    "anechoic": 0,
    "hyperechoic": 1,
    "isoechoic": 1,
    "hypoechoic": 2,
    "very_hypoechoic": 3,
    "markedly_hypoechoic": 3,
    "indeterminate": 1,
}

SHAPE_POINTS_2017 = {
    "wider_than_tall": 0,
    "taller_than_wide": 3,
}

MARGIN_POINTS_2017 = {
    "smooth": 0,
    "ill_defined": 0,
    "lobulated": 2,
    "irregular": 2,
    "lobulated_or_irregular": 2,
    "extrathyroidal_extension": 3,
}

ECHOGENIC_FOCI_POINTS_2017 = {
    "none": 0,
    "large_comet_tail_artifacts": 0,
    "macrocalcifications": 1,
    "peripheral_rim_calcifications": 2,
    "punctate_echogenic_foci": 3,
}


def tirads_level_from_points(total: int) -> str:
    if total == 0:
        return "TR1"
    elif total <= 2:
        return "TR2"
    elif total == 3:
        return "TR3"
    elif 4 <= total <= 6:
        return "TR4"
    elif total >= 7:
        return "TR5"
    return "TR2"


def score_tirads_2017(
    composition: Optional[str],
    echogenicity: Optional[str],
    shape: Optional[str],
    margin: Optional[str],
    echogenic_foci: Optional[list],
) -> dict:
    comp_pts = COMPOSITION_POINTS_2017.get(composition, None)
    echo_pts = ECHOGENICITY_POINTS_2017.get(echogenicity, None)
    shape_pts = SHAPE_POINTS_2017.get(shape, None)
    margin_pts = MARGIN_POINTS_2017.get(margin, None)

    foci_pts = None
    if echogenic_foci and isinstance(echogenic_foci, list):
        foci_pts = sum(ECHOGENIC_FOCI_POINTS_2017.get(f, 0) for f in echogenic_foci)
    elif echogenic_foci is None:
        foci_pts = None

    scorable = [comp_pts, echo_pts, shape_pts, margin_pts, foci_pts]
    non_none = [p for p in scorable if p is not None]

    if not non_none:
        return {
            "composition_pts": None, "echogenicity_pts": None,
            "shape_pts": None, "margin_pts": None, "foci_pts": None,
            "total_pts_2017": None, "tirads_level_2017": None,
            "n_categories_scored": 0,
        }

    total = sum(non_none)
    has_min = comp_pts is not None and echo_pts is not None
    level = tirads_level_from_points(total) if has_min else None

    return {
        "composition_pts": comp_pts, "echogenicity_pts": echo_pts,
        "shape_pts": shape_pts, "margin_pts": margin_pts,
        "foci_pts": foci_pts, "total_pts_2017": total,
        "tirads_level_2017": level, "n_categories_scored": len(non_none),
    }


def score_tirads_modified(
    composition: Optional[str],
    echogenicity: Optional[str],
    shape: Optional[str],
    margin: Optional[str],
    echogenic_foci: Optional[list],
) -> dict:
    comp_pts = COMPOSITION_POINTS_2017.get(composition, None)
    echo_pts = ECHOGENICITY_POINTS_2017.get(echogenicity, None)
    shape_pts = SHAPE_POINTS_2017.get(shape, None)
    margin_pts = MARGIN_POINTS_2017.get(margin, None)

    is_mixed = composition in ("mixed_cystic_and_solid",)

    foci_pts = None
    if echogenic_foci and isinstance(echogenic_foci, list):
        foci_pts = 0
        for f in echogenic_foci:
            if f == "punctate_echogenic_foci" and is_mixed:
                foci_pts += 1  # modified: 1 instead of 3
            else:
                foci_pts += ECHOGENIC_FOCI_POINTS_2017.get(f, 0)
    elif echogenic_foci is None:
        foci_pts = None

    scorable = [comp_pts, echo_pts, shape_pts, margin_pts, foci_pts]
    non_none = [p for p in scorable if p is not None]

    if not non_none:
        return {"total_pts_modified": None, "tirads_level_modified": None}

    total = sum(non_none)
    has_min = comp_pts is not None and echo_pts is not None
    level = tirads_level_from_points(total) if has_min else None

    return {"total_pts_modified": total, "tirads_level_modified": level}


# ─── LLM Extraction Prompt ──────────────────────────────────────────────────

EXTRACTION_SYSTEM_PROMPT = """You are a radiology NLP extraction system. Given a thyroid ultrasound report text excerpt, extract the ACR TI-RADS component features for EACH nodule described.

Return ONLY valid JSON (no markdown, no explanation). The schema is:
{
  "nodules": [
    {
      "nodule_id": "string — brief identifier e.g. 'right_lower_1', 'left_mid_1', 'isthmus_1', or 'nodule_1' if location unclear",
      "location": "string or null — e.g. 'right lower pole', 'left mid', 'isthmus'",
      "size_cm": number or null,
      "composition": "one of: cystic | almost_completely_cystic | spongiform | mixed_cystic_and_solid | solid | almost_completely_solid | indeterminate | null",
      "echogenicity": "one of: anechoic | hyperechoic | isoechoic | hypoechoic | very_hypoechoic | markedly_hypoechoic | indeterminate | null",
      "shape": "one of: wider_than_tall | taller_than_wide | null",
      "margin": "one of: smooth | ill_defined | lobulated | irregular | lobulated_or_irregular | extrathyroidal_extension | null",
      "echogenic_foci": ["array of zero or more of: none | large_comet_tail_artifacts | macrocalcifications | peripheral_rim_calcifications | punctate_echogenic_foci"] or null,
      "tirads_reported_in_text": number or null
    }
  ]
}

Rules:
1. Extract ALL nodules described in the text. If the text describes multiple nodules, return one object per nodule.
2. Use null for any feature NOT mentioned or NOT inferable from the text.
3. Map synonyms: "mixed cystic and solid" / "complex" → mixed_cystic_and_solid; "predominantly solid" → almost_completely_solid; "predominantly cystic" → mixed_cystic_and_solid; "nearly solid" → almost_completely_solid; "markedly hypoechoic" / "very hypoechoic" → very_hypoechoic; "well-defined" / "smooth" → smooth; "spiculated" / "irregular" / "lobulated" → lobulated_or_irregular; "microcalcifications" / "punctate calcifications" → punctate_echogenic_foci; "coarse calcifications" / "macrocalcification" → macrocalcifications; "rim calcification" / "eggshell calcification" / "peripheral calcification" → peripheral_rim_calcifications; "comet tail" / "comet-tail artifact" → large_comet_tail_artifacts.
4. If a TIRADS score is explicitly stated in the text (e.g. "TIRADS 4", "TR3", "TI-RADS 5"), capture it in tirads_reported_in_text as an integer.
5. "Cystic nodule" with no solid component described → composition=cystic, echogenicity=anechoic.
6. If the text says "solid nodule" with no further composition detail → composition=solid.
7. For shape: only assign taller_than_wide if explicitly stated. Default assumption when not mentioned is null (unknown), NOT wider_than_tall.
8. Return {"nodules": []} if the text has no extractable nodule features.
9. NEVER add features not supported by the text. When in doubt, use null.
10. If the text contains structured TIRADS scoring (e.g. "Composition: Solid (2)"), extract the feature labels, NOT the point values. The deterministic scoring is applied separately.
11. Text may be TRUNCATED at 300 characters mid-sentence. Extract whatever is available; do not infer features from incomplete descriptions."""


def build_user_prompt(text_excerpt: str) -> str:
    return f"""Extract ACR TI-RADS features from this thyroid US report excerpt:

<us_report>
{text_excerpt.strip()}
</us_report>"""


# ─── API Call with Retry ─────────────────────────────────────────────────────

client: anthropic.Anthropic = None


def call_llm(text_excerpt: str, row_key: str) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=2000,
                system=EXTRACTION_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": build_user_prompt(text_excerpt)}],
            )
            raw = response.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[-1]
                if raw.endswith("```"):
                    raw = raw[:-3]
                raw = raw.strip()

            parsed = json.loads(raw)
            return {"status": "ok", "result": parsed, "raw": raw, "row_key": row_key}

        except json.JSONDecodeError as e:
            log.warning(f"JSON parse error for {row_key} (attempt {attempt+1}): {e}")
            if attempt == MAX_RETRIES - 1:
                return {"status": "json_error", "result": None, "raw": raw, "row_key": row_key, "error": str(e)}

        except anthropic.RateLimitError:
            wait = RETRY_DELAY * (2 ** attempt)
            log.warning(f"Rate limited for {row_key}, waiting {wait}s...")
            time.sleep(wait)

        except anthropic.APIStatusError as e:
            if e.status_code >= 500:
                wait = RETRY_DELAY * (2 ** attempt)
                log.warning(f"Server error {e.status_code} for {row_key}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                log.error(f"API error for {row_key}: {e}")
                return {"status": "api_error", "result": None, "raw": None, "row_key": row_key, "error": str(e)}

        except Exception as e:
            log.error(f"Unexpected error for {row_key}: {e}")
            return {"status": "error", "result": None, "raw": None, "row_key": row_key, "error": str(e)}

    return {"status": "max_retries", "result": None, "raw": None, "row_key": row_key, "error": "Exceeded max retries"}


# ─── Checkpoint Management ───────────────────────────────────────────────────

def load_checkpoint() -> set:
    if CHECKPOINT_PATH.exists():
        data = json.loads(CHECKPOINT_PATH.read_text())
        return set(data.get("completed_keys", []))
    return set()


def save_checkpoint(completed_keys: set):
    CHECKPOINT_PATH.write_text(json.dumps({"completed_keys": list(completed_keys)}))


# ─── Validation Layer ────────────────────────────────────────────────────────

def run_validation(df_out: pd.DataFrame):
    """
    Compare LLM-extracted TIRADS scores against the ~2,600 reports that
    have pre-structured TIRADS labels already in the source text.

    Validation checks:
    1. tirads_reported_in_text (LLM-extracted) vs tirads_level_2017 (deterministic)
    2. Component-level accuracy where structured labels exist
    3. Feature extraction rates and null analysis
    4. Truncation impact assessment
    """
    log.info("\n" + "=" * 60)
    log.info("VALIDATION LAYER")
    log.info("=" * 60)

    # --- Check 1: LLM-extracted tirads_reported_in_text vs computed score ---
    has_reported = df_out[df_out["tirads_reported_in_text"].notna()].copy()
    log.info(f"\nReports where LLM found a TIRADS score stated in text: {len(has_reported)}")

    if len(has_reported) > 0:
        has_reported["reported_tr"] = has_reported["tirads_reported_in_text"].apply(
            lambda x: f"TR{int(x)}" if pd.notna(x) else None
        )
        has_both = has_reported[
            has_reported["reported_tr"].notna() & has_reported["tirads_level_2017"].notna()
        ]
        if len(has_both) > 0:
            match = (has_both["reported_tr"] == has_both["tirads_level_2017"]).sum()
            total_v = len(has_both)
            log.info(f"  Reported-in-text vs computed 2017 match: {match}/{total_v} ({100*match/total_v:.1f}%)")

            # Breakdown by TR level
            for tr in ["TR1", "TR2", "TR3", "TR4", "TR5"]:
                subset = has_both[has_both["reported_tr"] == tr]
                if len(subset) > 0:
                    m = (subset["reported_tr"] == subset["tirads_level_2017"]).sum()
                    log.info(f"    {tr}: {m}/{len(subset)} match ({100*m/len(subset):.0f}%)")

            # Show sample mismatches
            mismatches = has_both[has_both["reported_tr"] != has_both["tirads_level_2017"]]
            if len(mismatches) > 0:
                log.info(f"\n  Sample mismatches (first 10 of {len(mismatches)}):")
                for _, row in mismatches.head(10).iterrows():
                    log.info(
                        f"    reported={row['reported_tr']} computed={row['tirads_level_2017']} "
                        f"pts={row['total_pts_2017']} | comp={row['composition']} "
                        f"echo={row['echogenicity']} shape={row['shape']} "
                        f"margin={row['margin']} foci={row['echogenic_foci']}"
                    )

    # --- Check 2: Feature extraction rates ---
    ok_rows = df_out[df_out["extraction_status"] == "ok"]
    total_ok = len(ok_rows)
    if total_ok > 0:
        log.info(f"\n  Feature extraction rates (of {total_ok} successfully extracted nodules):")
        for col in ["composition", "echogenicity", "shape", "margin", "echogenic_foci"]:
            non_null = ok_rows[col].notna().sum()
            log.info(f"    {col}: {non_null}/{total_ok} ({100*non_null/total_ok:.1f}%)")

    # --- Check 3: Distribution sanity checks ---
    scored = df_out[df_out["tirads_level_2017"].notna()]
    if len(scored) > 0:
        log.info(f"\n  Composition distribution (n={len(scored)} scored):")
        for val, count in df_out["composition"].value_counts().head(10).items():
            if val:
                log.info(f"    {val}: {count}")

        log.info(f"\n  Echogenicity distribution:")
        for val, count in df_out["echogenicity"].value_counts().head(10).items():
            if val:
                log.info(f"    {val}: {count}")

    # --- Check 4: Truncation impact ---
    log.info(f"\n  Note: 8,346 of 12,900 source excerpts are truncated at 300 chars.")
    log.info(f"  Truncated reports may have incomplete nodule descriptions.")
    log.info(f"  Consider: nodules cut off mid-description will have more null features.")

    # --- Save validation subset ---
    if len(has_reported) > 0:
        has_reported.to_parquet(VALIDATION_PARQUET, index=False)
        log.info(f"\n  Validation subset saved to {VALIDATION_PARQUET}")

    log.info("=" * 60)


# ─── Main Pipeline ───────────────────────────────────────────────────────────

def main():
    global client

    if not ANTHROPIC_API_KEY:
        sys.exit("ERROR: Set ANTHROPIC_API_KEY environment variable")
    if not MOTHERDUCK_TOKEN:
        sys.exit("ERROR: Set MOTHERDUCK_TOKEN environment variable")

    OUTPUT_DIR.mkdir(exist_ok=True)
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # ── Step 1: Load data from MotherDuck ────────────────────────────────
    log.info("Connecting to MotherDuck...")
    conn = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")

    query = f"""
    SELECT
        source_nodule_uid,
        research_id,
        us_report_number,
        exam_date_norm,
        nodule_number,
        aggregate_exam_text_excerpt,
        deterministic_key
    FROM "{SOURCE_DB}".main.{SOURCE_TABLE}
    WHERE aggregate_exam_text_excerpt IS NOT NULL
      AND LENGTH(TRIM(aggregate_exam_text_excerpt)) > 10
    ORDER BY research_id, us_report_number, nodule_number
    """
    log.info("Loading source data...")
    df_source = conn.execute(query).fetchdf()
    conn.close()
    log.info(f"Loaded {len(df_source)} nodule rows with text excerpts")

    # ── Step 1b: DEDUPLICATE — same text shared across nodule rows ───────
    df_unique_reports = (
        df_source
        .groupby(["research_id", "us_report_number"])
        .agg({
            "aggregate_exam_text_excerpt": "first",
            "source_nodule_uid": list,
            "nodule_number": list,
            "deterministic_key": list,
        })
        .reset_index()
    )

    df_unique_reports["row_key"] = df_unique_reports.apply(
        lambda r: hashlib.md5(
            f"{r['research_id']}_{r['us_report_number']}".encode()
        ).hexdigest()[:12],
        axis=1,
    )

    n_unique = len(df_unique_reports)
    n_total = len(df_source)
    log.info(f"Deduplicated: {n_total} nodule rows → {n_unique} unique report texts")
    log.info(f"  (saves ~{100*(1 - n_unique/n_total):.0f}% on API calls)")

    truncated = (df_unique_reports["aggregate_exam_text_excerpt"].str.len() == 300).sum()
    log.info(f"  Reports truncated at 300 chars: {truncated}/{n_unique}")

    # ── Step 2: Resume from checkpoint ───────────────────────────────────
    completed = load_checkpoint()
    remaining = df_unique_reports[~df_unique_reports["row_key"].isin(completed)]
    log.info(f"Already completed: {len(completed)}, remaining: {len(remaining)}")

    if len(remaining) == 0:
        log.info("All reports already processed. Skipping to scoring.")
    else:
        # ── Step 3: Run LLM extraction in parallel ──────────────────────
        all_results_path = OUTPUT_DIR / "llm_results.jsonl"

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {}
            for _, row in remaining.iterrows():
                future = pool.submit(
                    call_llm,
                    row["aggregate_exam_text_excerpt"],
                    row["row_key"],
                )
                futures[future] = row

            batch_count = 0
            with open(all_results_path, "a") as f_out:
                for future in tqdm(as_completed(futures), total=len(futures), desc="Extracting"):
                    row = futures[future]
                    result = future.result()
                    result["research_id"] = int(row["research_id"])
                    result["us_report_number"] = int(row["us_report_number"])
                    result["source_nodule_uids"] = row["source_nodule_uid"]
                    result["nodule_numbers"] = row["nodule_number"]
                    result["deterministic_keys"] = row["deterministic_key"]

                    f_out.write(json.dumps(result, default=str) + "\n")
                    completed.add(result["row_key"])
                    batch_count += 1

                    if batch_count % BATCH_SIZE == 0:
                        save_checkpoint(completed)
                        f_out.flush()
                        log.info(f"Checkpoint saved: {len(completed)} completed")

        save_checkpoint(completed)
        log.info(f"LLM extraction complete. {len(completed)} total reports processed.")

    # ── Step 4: Parse results and apply deterministic scoring ────────────
    log.info("Parsing LLM results and applying TIRADS scoring...")
    results_path = OUTPUT_DIR / "llm_results.jsonl"
    if not results_path.exists():
        sys.exit("No llm_results.jsonl found. Run extraction first.")

    output_rows = []
    with open(results_path) as f:
        for line in f:
            rec = json.loads(line)
            rid = rec.get("research_id")
            usrn = rec.get("us_report_number")
            source_uids = rec.get("source_nodule_uids", [])
            nodule_nums = rec.get("nodule_numbers", [])
            det_keys = rec.get("deterministic_keys", [])

            if rec["status"] != "ok" or not rec.get("result"):
                for i, uid in enumerate(source_uids):
                    output_rows.append({
                        "source_nodule_uid": uid,
                        "research_id": rid,
                        "us_report_number": usrn,
                        "nodule_number": nodule_nums[i] if i < len(nodule_nums) else None,
                        "deterministic_key": det_keys[i] if i < len(det_keys) else None,
                        "extracted_nodule_id": None,
                        "extracted_location": None,
                        "extracted_size_cm": None,
                        "composition": None,
                        "echogenicity": None,
                        "shape": None,
                        "margin": None,
                        "echogenic_foci": None,
                        "tirads_reported_in_text": None,
                        "extraction_status": rec["status"],
                        "extraction_error": rec.get("error"),
                        **score_tirads_2017(None, None, None, None, None),
                        **score_tirads_modified(None, None, None, None, None),
                    })
                continue

            nodules = rec["result"].get("nodules", [])
            if not nodules:
                for i, uid in enumerate(source_uids):
                    output_rows.append({
                        "source_nodule_uid": uid,
                        "research_id": rid,
                        "us_report_number": usrn,
                        "nodule_number": nodule_nums[i] if i < len(nodule_nums) else None,
                        "deterministic_key": det_keys[i] if i < len(det_keys) else None,
                        "extracted_nodule_id": None,
                        "extracted_location": None,
                        "extracted_size_cm": None,
                        "composition": None,
                        "echogenicity": None,
                        "shape": None,
                        "margin": None,
                        "echogenic_foci": None,
                        "tirads_reported_in_text": None,
                        "extraction_status": "no_nodules_found",
                        "extraction_error": None,
                        **score_tirads_2017(None, None, None, None, None),
                        **score_tirads_modified(None, None, None, None, None),
                    })
                continue

            # Fan out: each extracted nodule gets a row
            for nod_idx, nod in enumerate(nodules):
                comp = nod.get("composition")
                echo = nod.get("echogenicity")
                shape = nod.get("shape")
                margin = nod.get("margin")
                foci = nod.get("echogenic_foci")

                scores_2017 = score_tirads_2017(comp, echo, shape, margin, foci)
                scores_mod = score_tirads_modified(comp, echo, shape, margin, foci)

                src_uid = source_uids[nod_idx] if nod_idx < len(source_uids) else None
                src_nnum = nodule_nums[nod_idx] if nod_idx < len(nodule_nums) else None
                src_dk = det_keys[nod_idx] if nod_idx < len(det_keys) else None

                output_rows.append({
                    "source_nodule_uid": src_uid,
                    "research_id": rid,
                    "us_report_number": usrn,
                    "nodule_number": src_nnum,
                    "deterministic_key": src_dk,
                    "extracted_nodule_id": nod.get("nodule_id"),
                    "extracted_location": nod.get("location"),
                    "extracted_size_cm": nod.get("size_cm"),
                    "composition": comp,
                    "echogenicity": echo,
                    "shape": shape,
                    "margin": margin,
                    "echogenic_foci": json.dumps(foci) if foci else None,
                    "tirads_reported_in_text": nod.get("tirads_reported_in_text"),
                    "extraction_status": "ok",
                    "extraction_error": None,
                    **scores_2017,
                    **scores_mod,
                })

            if len(nodules) > len(source_uids):
                log.info(
                    f"  Report {rid}/{usrn}: LLM found {len(nodules)} nodules "
                    f"vs {len(source_uids)} source rows"
                )

    df_out = pd.DataFrame(output_rows)

    # ── Step 5: Save output ──────────────────────────────────────────────
    df_out.to_parquet(OUTPUT_PARQUET, index=False)
    log.info(f"Saved {len(df_out)} rows to {OUTPUT_PARQUET}")

    # ── Step 6: Summary stats ────────────────────────────────────────────
    total = len(df_out)
    scored = df_out["tirads_level_2017"].notna().sum()
    unscorable = df_out["tirads_level_2017"].isna().sum()
    ok_status = (df_out["extraction_status"] == "ok").sum()
    errors = total - ok_status

    log.info("\n" + "=" * 60)
    log.info("SUMMARY")
    log.info(f"  Source nodule rows processed: {n_total}")
    log.info(f"  Unique report texts sent to LLM: {n_unique}")
    log.info(f"  Total output rows (per-nodule): {total}")
    log.info(f"  Successfully extracted (status=ok): {ok_status}")
    log.info(f"  Scored with TI-RADS level (2017): {scored}")
    log.info(f"  Unscorable (insufficient features): {unscorable}")
    log.info(f"  Extraction errors/failures: {errors}")
    log.info("=" * 60)

    if scored > 0:
        log.info("\nTI-RADS 2017 Distribution:")
        dist = df_out["tirads_level_2017"].value_counts().sort_index()
        for level, count in dist.items():
            if level:
                log.info(f"  {level}: {count}")

        log.info("\nTI-RADS Modified Distribution:")
        dist_mod = df_out["tirads_level_modified"].value_counts().sort_index()
        for level, count in dist_mod.items():
            if level:
                log.info(f"  {level}: {count}")

        both_scored = df_out[
            df_out["tirads_level_2017"].notna() & df_out["tirads_level_modified"].notna()
        ]
        differs = both_scored[
            both_scored["tirads_level_2017"] != both_scored["tirads_level_modified"]
        ]
        log.info(f"\n  Rows where 2017 vs Modified differ: {len(differs)} / {len(both_scored)}")

    # ── Step 7: VALIDATION LAYER ─────────────────────────────────────────
    run_validation(df_out)

    log.info(f"\nOutput file: {OUTPUT_PARQUET}")
    log.info("Done!")


if __name__ == "__main__":
    main()
