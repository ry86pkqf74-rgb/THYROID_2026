#!/usr/bin/env python3
"""
Re-extraction of 134 TIRADS mismatches using full source_description
from ultrasound_reports table + Claude Sonnet for higher accuracy.
Patches output/tirads_extracted.parquet in-place.
"""
import os, sys, json, time, toml, hashlib
import duckdb, anthropic, pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tqdm import tqdm

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MD_TOKEN = toml.load("/Users/loganglosser/THYROID_2026/motherduck.local.toml")["MOTHERDUCK_TOKEN"]

# Use Sonnet for the re-run (better reasoning on ambiguous cases)
MODEL = "claude-sonnet-4-5"
MAX_WORKERS = 5
MAX_RETRIES = 3
OUTPUT_DIR = Path("./output")

# ── ACR scoring tables (same as main script) ─────────────────────────────────
COMP_PTS = {
    "cystic": 0, "almost_completely_cystic": 0, "spongiform": 0,
    "mixed_cystic_and_solid": 1, "solid": 2, "almost_completely_solid": 2, "indeterminate": 2,
}
ECHO_PTS = {
    "anechoic": 0, "hyperechoic": 1, "isoechoic": 1,
    "hypoechoic": 2, "very_hypoechoic": 3, "markedly_hypoechoic": 3, "indeterminate": 1,
}
SHAPE_PTS  = {"wider_than_tall": 0, "taller_than_wide": 3}
MARGIN_PTS = {"smooth": 0, "ill_defined": 0, "lobulated": 2, "irregular": 2,
              "lobulated_or_irregular": 2, "extrathyroidal_extension": 3}
FOCI_PTS   = {"none": 0, "large_comet_tail_artifacts": 0,
              "macrocalcifications": 1, "peripheral_rim_calcifications": 2,
              "punctate_echogenic_foci": 3}

def score(comp, echo, shape, margin, foci):
    c = COMP_PTS.get(comp); e = ECHO_PTS.get(echo)
    s = SHAPE_PTS.get(shape); m = MARGIN_PTS.get(margin)
    f = sum(FOCI_PTS.get(x, 0) for x in foci) if isinstance(foci, list) else None
    parts = [x for x in [c,e,s,m,f] if x is not None]
    if not parts:
        return None, None, 0
    total = sum(parts)
    level = ("TR1" if total==0 else "TR2" if total<=2 else "TR3" if total==3
             else "TR4" if total<=6 else "TR5") if (c is not None and e is not None) else None
    return total, level, len(parts)

SYSTEM_PROMPT = """You are a radiology NLP extraction system. Extract ACR TI-RADS features for EACH nodule described.

Return ONLY valid JSON:
{
  "nodules": [{
    "nodule_id": "string",
    "location": "string or null",
    "size_cm": number or null,
    "composition": "cystic|almost_completely_cystic|spongiform|mixed_cystic_and_solid|solid|almost_completely_solid|indeterminate|null",
    "echogenicity": "anechoic|hyperechoic|isoechoic|hypoechoic|very_hypoechoic|markedly_hypoechoic|indeterminate|null",
    "shape": "wider_than_tall|taller_than_wide|null",
    "margin": "smooth|ill_defined|lobulated|irregular|lobulated_or_irregular|extrathyroidal_extension|null",
    "echogenic_foci": ["none|large_comet_tail_artifacts|macrocalcifications|peripheral_rim_calcifications|punctate_echogenic_foci"] or null,
    "tirads_reported_in_text": number or null
  }]
}

Rules:
1. Extract ALL nodules. Use null for features not mentioned.
2. Synonyms: "mixed"/"complex"→mixed_cystic_and_solid; "predominantly solid"→almost_completely_solid; "markedly/very hypoechoic"→very_hypoechoic; "spiculated/irregular/lobulated"→lobulated_or_irregular; "microcalcifications/punctate"→punctate_echogenic_foci; "coarse/macrocalcification"→macrocalcifications; "rim/eggshell/peripheral calcification"→peripheral_rim_calcifications; "comet tail"→large_comet_tail_artifacts.
3. Capture explicit TIRADS scores (e.g. "TIRADS 4", "TR3") in tirads_reported_in_text.
4. For shape: only taller_than_wide if explicitly stated; null otherwise.
5. NEVER infer features not in the text.
6. Structured labels like "Composition: Solid (2)" → extract the label (solid), not the point value."""

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

def call_llm(text: str, key: str) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.messages.create(
                model=MODEL, max_tokens=3000,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": f"Extract ACR TI-RADS features:\n\n<us_report>\n{text.strip()}\n</us_report>"}],
            )
            raw = resp.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[-1]
                if raw.endswith("```"): raw = raw[:-3]
            return {"status": "ok", "result": json.loads(raw.strip()), "key": key}
        except json.JSONDecodeError as ex:
            if attempt == MAX_RETRIES - 1:
                return {"status": "json_error", "key": key, "error": str(ex)}
        except anthropic.RateLimitError:
            time.sleep(4 * (2 ** attempt))
        except Exception as ex:
            return {"status": "error", "key": key, "error": str(ex)}
    return {"status": "max_retries", "key": key, "error": "exceeded retries"}

def build_full_text(row: dict) -> str:
    """Reconstruct a rich prompt from all available ultrasound_reports columns for one nodule row."""
    parts = []
    for n in range(1, 15):
        desc = row.get(f"nodule_{n}_source_description") or ""
        comp = row.get(f"nodule_{n}_composition") or ""
        echo = row.get(f"nodule_{n}_echogenicity") or ""
        calc = row.get(f"nodule_{n}_calcifications") or ""
        marg = row.get(f"nodule_{n}_margins") or ""
        shap = row.get(f"nodule_{n}_shape") or ""
        loc  = row.get(f"nodule_{n}_location") or ""
        tirads = row.get(f"nodule_{n}_ti_rads") or ""
        dims = row.get(f"nodule_{n}_dimensions") or ""
        
        tokens = [x for x in [desc, comp, echo, calc, marg, shap, loc, tirads, dims] if x.strip()]
        if tokens:
            parts.append(f"Nodule {n} ({loc}): " + "; ".join(tokens))
    
    impression = row.get("clinical_impression") or ""
    if impression:
        parts.append(f"Impression: {impression}")
    
    return "\n".join(parts) if parts else ""

def main():
    if not ANTHROPIC_API_KEY:
        sys.exit("Set ANTHROPIC_API_KEY")

    # ── Load mismatch keys ────────────────────────────────────────────────────
    keys_df = pd.read_csv("output/mismatch_keys.csv")
    print(f"Mismatch report keys to re-run: {len(keys_df)}")

    # ── Pull full data from ultrasound_reports ────────────────────────────────
    print("Fetching full report data from MotherDuck...")
    conn = duckdb.connect(f"md:?motherduck_token={MD_TOKEN}")
    
    # Build IN clause
    pairs = [f"({int(r.research_id)}, {int(r.us_report_number)})" for _, r in keys_df.iterrows()]
    pairs_sql = ", ".join(pairs)
    
    df_full = conn.execute(f"""
        SELECT *
        FROM "Thyroid 2026".main.ultrasound_reports
        WHERE (CAST(research_id AS BIGINT), us_report_number) IN ({pairs_sql})
    """).fetchdf()
    conn.close()
    
    print(f"Fetched {len(df_full)} rows from ultrasound_reports")
    
    if len(df_full) == 0:
        # Fallback: use the raw_imaging_12_slots_v1 full text (no truncation limit)
        print("No rows found in ultrasound_reports — falling back to raw_imaging_12_slots_v1 without truncation limit...")
        conn = duckdb.connect(f"md:?motherduck_token={MD_TOKEN}")
        pairs_sql2 = ", ".join([f"({int(r.research_id)}, {int(r.us_report_number)})" for _, r in keys_df.iterrows()])
        df_full = conn.execute(f"""
            SELECT research_id, us_report_number, nodule_number, 
                   aggregate_exam_text_excerpt AS full_text,
                   deterministic_key, source_nodule_uid
            FROM "Thyroid 2026".main.raw_imaging_12_slots_v1
            WHERE (CAST(research_id AS BIGINT), us_report_number) IN ({pairs_sql2})
        """).fetchdf()
        conn.close()
        use_fallback = True
        print(f"Fallback fetched {len(df_full)} rows")
    else:
        use_fallback = False

    # ── Build per-report text prompts ─────────────────────────────────────────
    if not use_fallback:
        # Group by report and build rich text
        report_texts = {}
        for _, row in df_full.iterrows():
            rid = int(row["research_id"])
            usrn = int(row["us_report_number"])
            key = f"{rid}_{usrn}"
            text = build_full_text(row.to_dict())
            if text.strip():
                report_texts[key] = {"text": text, "research_id": rid, "us_report_number": usrn}
    else:
        # Fallback: deduplicate by report, take the text as-is
        report_texts = {}
        for _, row in df_full.iterrows():
            rid = int(row["research_id"])
            usrn = int(row["us_report_number"])
            key = f"{rid}_{usrn}"
            if key not in report_texts:
                report_texts[key] = {"text": row["full_text"], "research_id": rid, "us_report_number": usrn}

    print(f"Reports with extractable text: {len(report_texts)}")

    # ── Run LLM in parallel ───────────────────────────────────────────────────
    print(f"\nRe-extracting with {MODEL}...")
    llm_results = {}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(call_llm, v["text"], k): k
            for k, v in report_texts.items()
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Re-extracting"):
            key = futures[future]
            result = future.result()
            llm_results[key] = result

    ok_count = sum(1 for r in llm_results.values() if r["status"] == "ok")
    print(f"\nLLM done: {ok_count}/{len(llm_results)} succeeded")

    # ── Patch main parquet ────────────────────────────────────────────────────
    print("Patching tirads_extracted.parquet...")
    df_main = pd.read_parquet("output/tirads_extracted.parquet")
    
    # Build mismatch key set from original validation
    val = pd.read_parquet("output/tirads_validation.parquet")
    val["reported_tr"] = val["tirads_reported_in_text"].apply(
        lambda x: f"TR{int(x)}" if pd.notna(x) else None
    )
    has_both = val[val["reported_tr"].notna() & val["tirads_level_2017"].notna()]
    mismatch_pairs = set(
        zip(has_both[has_both["reported_tr"] != has_both["tirads_level_2017"]]["research_id"],
            has_both[has_both["reported_tr"] != has_both["tirads_level_2017"]]["us_report_number"])
    )
    
    patch_rows = []
    for key, res in llm_results.items():
        if res["status"] != "ok":
            continue
        rid = report_texts[key]["research_id"]
        usrn = report_texts[key]["us_report_number"]
        nodules = res["result"].get("nodules", [])
        
        for nod_idx, nod in enumerate(nodules):
            comp = nod.get("composition"); echo = nod.get("echogenicity")
            shp  = nod.get("shape");      marg = nod.get("margin")
            foci = nod.get("echogenic_foci")
            total_pts, level, n_scored = score(comp, echo, shp, marg, foci)
            
            # Modified scoring (punctate in mixed = 1pt)
            is_mixed = comp == "mixed_cystic_and_solid"
            foci_mod_pts = None
            if isinstance(foci, list):
                foci_mod_pts = sum((1 if (f == "punctate_echogenic_foci" and is_mixed) else FOCI_PTS.get(f, 0)) for f in foci)
            mod_parts = [COMP_PTS.get(comp), ECHO_PTS.get(echo), SHAPE_PTS.get(shp), MARGIN_PTS.get(marg), foci_mod_pts]
            mod_parts = [x for x in mod_parts if x is not None]
            mod_total = sum(mod_parts) if mod_parts else None
            c2 = COMP_PTS.get(comp); e2 = ECHO_PTS.get(echo)
            mod_level = (("TR1" if mod_total==0 else "TR2" if mod_total<=2 else "TR3" if mod_total==3
                         else "TR4" if mod_total<=6 else "TR5") if (c2 is not None and e2 is not None) else None) if mod_total is not None else None
            
            patch_rows.append({
                "research_id": rid,
                "us_report_number": usrn,
                "nodule_idx": nod_idx,
                "extracted_nodule_id": nod.get("nodule_id"),
                "extracted_location": nod.get("location"),
                "extracted_size_cm": nod.get("size_cm"),
                "composition": comp, "echogenicity": echo, "shape": shp,
                "margin": marg,
                "echogenic_foci": json.dumps(foci) if foci else None,
                "tirads_reported_in_text": nod.get("tirads_reported_in_text"),
                "extraction_status": "ok", "extraction_error": None,
                "composition_pts": COMP_PTS.get(comp),
                "echogenicity_pts": ECHO_PTS.get(echo),
                "shape_pts": SHAPE_PTS.get(shp),
                "margin_pts": MARGIN_PTS.get(marg),
                "foci_pts": FOCI_PTS.get(foci[0]) if isinstance(foci, list) and foci else None,
                "total_pts_2017": total_pts, "tirads_level_2017": level,
                "n_categories_scored": n_scored,
                "total_pts_modified": mod_total, "tirads_level_modified": mod_level,
                "_rerun_source": "sonnet_full_text",
            })

    print(f"Patch rows built: {len(patch_rows)}")

    # Replace mismatch rows in main parquet
    df_patch = pd.DataFrame(patch_rows)
    df_main["_rerun_source"] = df_main.get("_rerun_source", None)

    # Mark old mismatch rows for replacement
    mismatch_mask = df_main.apply(
        lambda r: (r["research_id"], r["us_report_number"]) in mismatch_pairs, axis=1
    )
    df_keep = df_main[~mismatch_mask].copy()
    
    # Build final DataFrame: keep non-mismatch rows + patched rows
    # Align columns
    all_cols = list(df_main.columns) + ["_rerun_source"]
    for col in all_cols:
        if col not in df_keep.columns:
            df_keep[col] = None
        if col not in df_patch.columns:
            df_patch[col] = None

    df_final = pd.concat([df_keep, df_patch[all_cols]], ignore_index=True)
    df_final.to_parquet("output/tirads_extracted.parquet", index=False)
    print(f"Saved patched parquet: {len(df_final)} total rows")

    # ── Post-patch validation ─────────────────────────────────────────────────
    print("\n" + "="*60)
    print("POST-PATCH VALIDATION")
    patch_only = df_patch[df_patch["tirads_reported_in_text"].notna() & df_patch["tirads_level_2017"].notna()].copy()
    patch_only["reported_tr"] = patch_only["tirads_reported_in_text"].apply(lambda x: f"TR{int(x)}" if pd.notna(x) else None)
    patch_only = patch_only[patch_only["reported_tr"].notna()]
    if len(patch_only) > 0:
        match = (patch_only["reported_tr"] == patch_only["tirads_level_2017"]).sum()
        total_v = len(patch_only)
        print(f"Re-run match rate: {match}/{total_v} ({100*match/total_v:.1f}%)")
        for tr in ["TR1","TR2","TR3","TR4","TR5"]:
            sub = patch_only[patch_only["reported_tr"] == tr]
            if len(sub) > 0:
                m = (sub["reported_tr"] == sub["tirads_level_2017"]).sum()
                print(f"  {tr}: {m}/{len(sub)} ({100*m/len(sub):.0f}%)")
    print("="*60)
    print("Done! tirads_extracted.parquet has been patched.")

if __name__ == "__main__":
    main()
