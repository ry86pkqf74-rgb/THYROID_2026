"""
Extraction Audit Engine v10 — Phase 12: TIRADS Excel Ingestion & ACR Validation
================================================================================
Ingests structured US reports from two Excel sources, recalculates TI-RADS
from ACR criteria, cross-validates reported vs recalculated scores, and
builds per-patient TIRADS + nodule characteristics tables.

Sources:
  1. COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx — 6,793 reports, 4,074 pts
     (fully structured: composition, echogenicity, shape, margins, calcifications)
  2. US Nodules TIRADS 12_1_25.xlsx — 14 sheets, ~10,862 pts each
     (TIRADS scores only, no criteria detail)
  3. clinical_notes_long NLP (Phase 11 extracted_us_tirads_v1)

Source hierarchy:
  excel_complete_structured (1.0) > excel_tirads_scored (0.90) >
  nlp_clinical_note (0.75)

Usage:
    from notes_extraction.extraction_audit_engine_v10 import audit_and_refine_phase12
    results = audit_and_refine_phase12(con)

CLI:
    .venv/bin/python notes_extraction/extraction_audit_engine_v10.py --md
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from notes_extraction.extraction_audit_engine_v3 import _get_connection

PHASE12_SOURCE_HIERARCHY = {
    "excel_complete_structured": 1.0,
    "excel_tirads_scored": 0.90,
    "nlp_endocrine_note": 0.85,
    "nlp_other_history": 0.80,
    "nlp_h_p": 0.70,
    "nlp_op_note": 0.65,
}

TIRADS_CATEGORY_MAP = {
    1: "TR1_Benign",
    2: "TR2_Not_Suspicious",
    3: "TR3_Mildly_Suspicious",
    4: "TR4_Moderately_Suspicious",
    5: "TR5_Highly_Suspicious",
}

# ---------------------------------------------------------------------------
# ACR TI-RADS Point Calculator (hard-coded rules per Tessler et al. 2017)
# ---------------------------------------------------------------------------
COMPOSITION_POINTS = {
    "cystic": 0,
    "spongiform": 0,
    "anechoic": 0,
    "mixed cystic and solid": 1,
    "mixed": 1,
    "mixed cystic-solid": 1,
    "predominantly cystic": 1,
    "solid": 2,
    "predominantly solid": 2,
}

ECHOGENICITY_POINTS = {
    "anechoic": 0,
    "hyperechoic": 1,
    "isoechoic": 1,
    "hypoechoic": 2,
    "very hypoechoic": 3,
    "markedly hypoechoic": 3,
}

SHAPE_POINTS = {
    "wider than tall": 0,
    "wider-than-tall": 0,
    "taller than wide": 3,
    "taller-than-wide": 3,
}

MARGIN_POINTS = {
    "smooth": 0,
    "ill-defined": 0,
    "ill defined": 0,
    "lobulated": 2,
    "microlobulated": 2,
    "irregular": 2,
    "extra-thyroidal extension": 3,
    "extrathyroidal extension": 3,
    "ete": 3,
}

ECHOGENIC_FOCI_POINTS = {
    "none": 0,
    "no calcifications": 0,
    "large comet-tail": 0,
    "large comet-tail artifacts": 0,
    "comet tail": 0,
    "macrocalcifications": 1,
    "macrocalcification": 1,
    "peripheral calcifications": 2,
    "peripheral": 2,
    "rim calcifications": 2,
    "punctate echogenic foci": 3,
    "punctate": 3,
    "microcalcifications": 3,
    "microcalcification": 3,
}


def _score_to_tirads(total_points: int) -> int:
    """Convert ACR point total to TI-RADS category (1-5)."""
    if total_points == 0:
        return 1
    elif total_points <= 2:
        return 2
    elif total_points <= 3:
        return 3
    elif total_points <= 6:
        return 4
    else:
        return 5


def _normalize_feature(val: str | None, point_map: dict) -> tuple[int | None, str | None]:
    """Normalize a feature value and return (points, normalized_value)."""
    if val is None or str(val).strip() in ("", "nan", "None", "NaN"):
        return None, None
    v = str(val).strip().lower()
    if v in point_map:
        return point_map[v], v
    for key in point_map:
        if key in v or v in key:
            return point_map[key], key
    return None, v


class ACRTIRADSCalculator:
    """Recalculate TI-RADS score from 5 ACR criteria."""

    def calculate(
        self,
        composition: str | None,
        echogenicity: str | None,
        shape: str | None,
        margins: str | None,
        calcifications: str | None,
    ) -> dict:
        comp_pts, comp_norm = _normalize_feature(composition, COMPOSITION_POINTS)
        echo_pts, echo_norm = _normalize_feature(echogenicity, ECHOGENICITY_POINTS)
        shape_pts, shape_norm = _normalize_feature(shape, SHAPE_POINTS)
        margin_pts, margin_norm = _normalize_feature(margins, MARGIN_POINTS)
        foci_pts, foci_norm = _normalize_feature(calcifications, ECHOGENIC_FOCI_POINTS)

        known_pts = [p for p in [comp_pts, echo_pts, shape_pts, margin_pts, foci_pts] if p is not None]
        n_criteria = len(known_pts)

        if n_criteria == 0:
            return {
                "tirads_recalculated": None,
                "total_points": None,
                "n_criteria_available": 0,
                "recalc_confidence": 0.0,
                "composition_pts": comp_pts,
                "echogenicity_pts": echo_pts,
                "shape_pts": shape_pts,
                "margin_pts": margin_pts,
                "foci_pts": foci_pts,
                "composition_norm": comp_norm,
                "echogenicity_norm": echo_norm,
                "shape_norm": shape_norm,
                "margin_norm": margin_norm,
                "foci_norm": foci_norm,
            }

        total = sum(known_pts)
        tirads = _score_to_tirads(total)

        conf_base = 0.60
        conf_per_criterion = 0.08
        confidence = min(conf_base + n_criteria * conf_per_criterion, 1.0)

        return {
            "tirads_recalculated": tirads,
            "total_points": total,
            "n_criteria_available": n_criteria,
            "recalc_confidence": round(confidence, 2),
            "composition_pts": comp_pts,
            "echogenicity_pts": echo_pts,
            "shape_pts": shape_pts,
            "margin_pts": margin_pts,
            "foci_pts": foci_pts,
            "composition_norm": comp_norm,
            "echogenicity_norm": echo_norm,
            "shape_norm": shape_norm,
            "margin_norm": margin_norm,
            "foci_norm": foci_norm,
        }


# ---------------------------------------------------------------------------
# Excel Ingestors
# ---------------------------------------------------------------------------
_TIRADS_TR_RE = re.compile(r"TR(\d)", re.IGNORECASE)


def _parse_tr_value(v) -> int | None:
    """Parse 'TR4', 4.0, '4' etc. to integer TIRADS score."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip()
    if s.lower() in ("", "nan", "none", "not_scored", "not scored"):
        return None
    m = _TIRADS_TR_RE.search(s)
    if m:
        return int(m.group(1))
    try:
        val = int(float(s))
        if 1 <= val <= 5:
            return val
    except (ValueError, TypeError):
        pass
    return None


def ingest_complete_us_excel(excel_path: str | Path) -> pd.DataFrame:
    """Ingest COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx into long-format per-nodule rows."""
    df = pd.read_excel(str(excel_path), sheet_name="All_Ultrasound_Reports")
    calculator = ACRTIRADSCalculator()
    rows = []

    for _, report in df.iterrows():
        rid = report.get("Research_ID")
        if pd.isna(rid):
            continue
        rid = int(rid)
        us_date = report.get("Ultrasound_Date")
        us_num = report.get("US_Report_Number")
        sheet = report.get("Sheet_Name", "")
        n_nodules = report.get("Number_of_Nodules")
        impression = report.get("Source_US_Impression", "")
        ln_assessment = report.get("Lymph_Node_Assessment", "")
        recommendation = report.get("Recommendation", "")

        for nod in range(1, 15):
            tirads_col = f"Nodule_{nod}_TI_RADS"
            if tirads_col not in df.columns:
                break
            tirads_raw = report.get(tirads_col)
            tirads_reported = _parse_tr_value(tirads_raw)

            comp = report.get(f"Nodule_{nod}_Composition")
            echo = report.get(f"Nodule_{nod}_Echogenicity")
            shp = report.get(f"Nodule_{nod}_Shape")
            marg = report.get(f"Nodule_{nod}_Margins")
            calc = report.get(f"Nodule_{nod}_Calcifications")
            dims = report.get(f"Nodule_{nod}_Dimensions")
            loc = report.get(f"Nodule_{nod}_Location")
            length_mm = report.get(f"Nodule_{nod}_Length_mm")
            width_mm = report.get(f"Nodule_{nod}_Width_mm")
            height_mm = report.get(f"Nodule_{nod}_Height_mm")
            volume = report.get(f"Nodule_{nod}_Volume")

            if tirads_reported is None and pd.isna(comp):
                continue

            recalc = calculator.calculate(
                composition=comp if not pd.isna(comp) else None,
                echogenicity=echo if not pd.isna(echo) else None,
                shape=shp if not pd.isna(shp) else None,
                margins=marg if not pd.isna(marg) else None,
                calcifications=calc if not pd.isna(calc) else None,
            )

            tirads_final = tirads_reported
            concordance = None
            discrepancy_reason = None
            if tirads_reported is not None and recalc["tirads_recalculated"] is not None:
                if tirads_reported == recalc["tirads_recalculated"]:
                    concordance = "match"
                else:
                    concordance = "mismatch"
                    diff = tirads_reported - recalc["tirads_recalculated"]
                    discrepancy_reason = f"reported_TR{tirads_reported}_vs_recalc_TR{recalc['tirads_recalculated']}_diff_{diff:+d}"
                    if recalc["recalc_confidence"] >= 0.90:
                        tirads_final = recalc["tirads_recalculated"]
            elif tirads_reported is None and recalc["tirads_recalculated"] is not None:
                tirads_final = recalc["tirads_recalculated"]
                concordance = "recalc_only"

            size_max_mm = None
            for dim_val in [length_mm, width_mm, height_mm]:
                if dim_val is not None and not (isinstance(dim_val, float) and np.isnan(dim_val)):
                    v = float(dim_val)
                    if size_max_mm is None or v > size_max_mm:
                        size_max_mm = v

            rows.append({
                "research_id": rid,
                "us_report_number": int(us_num) if not pd.isna(us_num) else None,
                "us_date": str(us_date)[:10] if not pd.isna(us_date) else None,
                "nodule_number": nod,
                "nodule_location": str(loc) if not pd.isna(loc) else None,
                "tirads_reported": tirads_reported,
                "tirads_recalculated": recalc["tirads_recalculated"],
                "tirads_final": tirads_final,
                "tirads_category": TIRADS_CATEGORY_MAP.get(tirads_final),
                "concordance_flag": concordance,
                "discrepancy_reason": discrepancy_reason,
                "total_acr_points": recalc["total_points"],
                "n_criteria_available": recalc["n_criteria_available"],
                "recalc_confidence": recalc["recalc_confidence"],
                "composition_raw": str(comp) if not pd.isna(comp) else None,
                "composition_norm": recalc["composition_norm"],
                "composition_pts": recalc["composition_pts"],
                "echogenicity_raw": str(echo) if not pd.isna(echo) else None,
                "echogenicity_norm": recalc["echogenicity_norm"],
                "echogenicity_pts": recalc["echogenicity_pts"],
                "shape_raw": str(shp) if not pd.isna(shp) else None,
                "shape_norm": recalc["shape_norm"],
                "shape_pts": recalc["shape_pts"],
                "margin_raw": str(marg) if not pd.isna(marg) else None,
                "margin_norm": recalc["margin_norm"],
                "margin_pts": recalc["margin_pts"],
                "calcification_raw": str(calc) if not pd.isna(calc) else None,
                "calcification_norm": recalc["foci_norm"],
                "calcification_pts": recalc["foci_pts"],
                "nodule_size_max_mm": round(size_max_mm, 1) if size_max_mm else None,
                "nodule_length_mm": float(length_mm) if not pd.isna(length_mm) else None,
                "nodule_width_mm": float(width_mm) if not pd.isna(width_mm) else None,
                "nodule_height_mm": float(height_mm) if not pd.isna(height_mm) else None,
                "nodule_volume_str": str(volume) if not pd.isna(volume) else None,
                "nodule_dimensions_str": str(dims) if not pd.isna(dims) else None,
                "source_category": "excel_complete_structured",
                "source_reliability": 1.0,
                "source_sheet": str(sheet) if not pd.isna(sheet) else None,
                "ln_assessment": str(ln_assessment)[:200] if not pd.isna(ln_assessment) else None,
                "recommendation": str(recommendation)[:200] if not pd.isna(recommendation) else None,
            })

    return pd.DataFrame(rows)


def ingest_tirads_scored_excel(excel_path: str | Path) -> pd.DataFrame:
    """Ingest US Nodules TIRADS 12_1_25.xlsx (14 sheets) into long format."""
    xl = pd.ExcelFile(str(excel_path))
    rows = []

    for sheet_name in xl.sheet_names:
        us_match = re.match(r"US-(\d+)", sheet_name)
        if not us_match:
            continue
        us_num = int(us_match.group(1))
        df = pd.read_excel(str(excel_path), sheet_name=sheet_name)

        rid_col = "Research ID number"
        if rid_col not in df.columns:
            continue

        date_candidates = [c for c in df.columns if "date" in c.lower()]
        date_col = date_candidates[0] if date_candidates else None

        for _, row in df.iterrows():
            rid = row.get(rid_col)
            if pd.isna(rid):
                continue
            rid = int(rid)
            us_date = row.get(date_col) if date_col else None

            for nod in range(1, 15):
                tr_candidates = [
                    f"N{nod} TR",
                    f"N{nod}_TR",
                ]
                tr_val = None
                for tc in tr_candidates:
                    if tc in df.columns and not pd.isna(row.get(tc)):
                        tr_val = row.get(tc)
                        break
                if tr_val is None:
                    continue

                score = _parse_tr_value(tr_val)
                if score is None:
                    continue

                nod_desc_cols = [f"Nodule {nod}", f"nodule {nod}", f"N{nod}"]
                nod_text = None
                for nc in nod_desc_cols:
                    if nc in df.columns and not pd.isna(row.get(nc)):
                        nod_text = str(row.get(nc))[:500]
                        break

                rows.append({
                    "research_id": rid,
                    "us_report_number": us_num,
                    "us_date": str(us_date)[:10] if not pd.isna(us_date) else None,
                    "nodule_number": nod,
                    "tirads_reported": score,
                    "tirads_category": TIRADS_CATEGORY_MAP.get(score),
                    "nodule_description": nod_text,
                    "source_category": "excel_tirads_scored",
                    "source_reliability": 0.90,
                    "source_sheet": sheet_name,
                })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Cross-Source Reconciler
# ---------------------------------------------------------------------------
def reconcile_tirads(
    complete_df: pd.DataFrame,
    scored_df: pd.DataFrame,
    nlp_df: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    Reconcile TIRADS across sources per patient, selecting best score
    according to source hierarchy.
    """
    patient_records: dict[int, list[dict]] = {}

    for _, r in complete_df.iterrows():
        rid = int(r["research_id"])
        patient_records.setdefault(rid, []).append({
            "tirads_score": r.get("tirads_final"),
            "tirads_reported": r.get("tirads_reported"),
            "tirads_recalculated": r.get("tirads_recalculated"),
            "tirads_category": r.get("tirads_category"),
            "concordance_flag": r.get("concordance_flag"),
            "n_criteria": r.get("n_criteria_available", 0),
            "source": "excel_complete_structured",
            "reliability": 1.0,
            "us_report_number": r.get("us_report_number"),
            "nodule_number": r.get("nodule_number"),
            "nodule_size_max_mm": r.get("nodule_size_max_mm"),
        })

    for _, r in scored_df.iterrows():
        rid = int(r["research_id"])
        patient_records.setdefault(rid, []).append({
            "tirads_score": r.get("tirads_reported"),
            "tirads_reported": r.get("tirads_reported"),
            "tirads_recalculated": None,
            "tirads_category": r.get("tirads_category"),
            "concordance_flag": None,
            "n_criteria": 0,
            "source": "excel_tirads_scored",
            "reliability": 0.90,
            "us_report_number": r.get("us_report_number"),
            "nodule_number": r.get("nodule_number"),
            "nodule_size_max_mm": None,
        })

    if nlp_df is not None and len(nlp_df) > 0:
        for _, r in nlp_df.iterrows():
            rid = int(r["research_id"])
            patient_records.setdefault(rid, []).append({
                "tirads_score": r.get("tirads_score"),
                "tirads_reported": r.get("tirads_score"),
                "tirads_recalculated": None,
                "tirads_category": r.get("tirads_category"),
                "concordance_flag": None,
                "n_criteria": 0,
                "source": "nlp_clinical_note",
                "reliability": r.get("confidence", 0.70),
                "us_report_number": None,
                "nodule_number": None,
                "nodule_size_max_mm": None,
            })

    patient_rows = []
    for rid, records in patient_records.items():
        valid = [r for r in records if r["tirads_score"] is not None]
        if not valid:
            continue

        valid.sort(key=lambda x: (-x["reliability"], -(x["n_criteria"] or 0)))
        best = valid[0]

        worst_score = max(r["tirads_score"] for r in valid)
        n_sources = len(set(r["source"] for r in valid))
        n_nodules_any = len(set((r.get("us_report_number"), r.get("nodule_number")) for r in valid))

        has_complete = any(r["source"] == "excel_complete_structured" for r in valid)
        has_scored = any(r["source"] == "excel_tirads_scored" for r in valid)
        has_nlp = any(r["source"] == "nlp_clinical_note" for r in valid)

        concordance_records = [r for r in valid if r.get("concordance_flag") is not None]
        concordant_count = sum(1 for r in concordance_records if r["concordance_flag"] == "match")
        mismatch_count = sum(1 for r in concordance_records if r["concordance_flag"] == "mismatch")

        patient_rows.append({
            "research_id": rid,
            "tirads_best_score": best["tirads_score"],
            "tirads_worst_score": worst_score,
            "tirads_best_category": TIRADS_CATEGORY_MAP.get(best["tirads_score"]),
            "tirads_worst_category": TIRADS_CATEGORY_MAP.get(worst_score),
            "tirads_source": best["source"],
            "tirads_reliability": best["reliability"],
            "has_acr_recalculation": has_complete,
            "has_scored_excel": has_scored,
            "has_nlp": has_nlp,
            "n_sources": n_sources,
            "n_nodule_records": n_nodules_any,
            "concordant_count": concordant_count,
            "mismatch_count": mismatch_count,
            "nodule_size_max_mm": best.get("nodule_size_max_mm"),
        })

    return pd.DataFrame(patient_rows)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def audit_and_refine_phase12(con, dry_run: bool = False) -> dict:
    """Full Phase 12 pipeline: ingest, validate, reconcile, deploy."""
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    results: dict = {"timestamp": ts, "tables_deployed": []}

    complete_excel = PROJECT_ROOT / "raw" / "COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx"
    tirads_excel = PROJECT_ROOT / "raw" / "US Nodules TIRADS 12_1_25.xlsx"

    # --- 1. Ingest COMPLETE structured US reports ---
    print("\n[Phase 12] Ingesting COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx ...")
    complete_df = ingest_complete_us_excel(complete_excel)
    print(f"  → {len(complete_df)} nodule-level rows, {complete_df.research_id.nunique()} patients")

    conc = complete_df["concordance_flag"].value_counts()
    print(f"  → Concordance: {dict(conc)}")
    results["complete_nodules"] = len(complete_df)
    results["complete_patients"] = int(complete_df.research_id.nunique())
    results["concordance_distribution"] = dict(conc)

    # --- 2. Ingest TIRADS-scored Excel ---
    print("\n[Phase 12] Ingesting US Nodules TIRADS 12_1_25.xlsx ...")
    scored_df = ingest_tirads_scored_excel(tirads_excel)
    print(f"  → {len(scored_df)} nodule-level rows, {scored_df.research_id.nunique()} patients")
    results["scored_nodules"] = len(scored_df)
    results["scored_patients"] = int(scored_df.research_id.nunique())

    # --- 3. Pull existing NLP TIRADS ---
    nlp_df = None
    try:
        nlp_df = con.execute("SELECT * FROM extracted_us_tirads_v1").fetchdf()
        print(f"\n[Phase 12] Existing NLP TIRADS: {len(nlp_df)} rows, {nlp_df.research_id.nunique()} patients")
        results["nlp_patients"] = int(nlp_df.research_id.nunique())
    except Exception:
        print("\n[Phase 12] No existing extracted_us_tirads_v1 found, skipping NLP source.")
        results["nlp_patients"] = 0

    # --- 4. Reconcile per-patient ---
    print("\n[Phase 12] Reconciling across sources ...")
    patient_df = reconcile_tirads(complete_df, scored_df, nlp_df)
    print(f"  → {len(patient_df)} patients with TIRADS")
    results["reconciled_patients"] = len(patient_df)

    src_dist = patient_df["tirads_source"].value_counts()
    print(f"  → Source distribution: {dict(src_dist)}")
    results["source_distribution"] = dict(src_dist)

    cat_dist = patient_df["tirads_best_category"].value_counts()
    print(f"  → Category distribution: {dict(cat_dist)}")
    results["category_distribution"] = dict(cat_dist)

    # --- 5. Concordance audit summary ---
    if len(complete_df) > 0:
        conc_rows = complete_df[complete_df["concordance_flag"].notna()]
        n_match = (conc_rows["concordance_flag"] == "match").sum()
        n_mismatch = (conc_rows["concordance_flag"] == "mismatch").sum()
        n_recalc_only = (conc_rows["concordance_flag"] == "recalc_only").sum()
        total_eval = n_match + n_mismatch
        concordance_pct = round(n_match / total_eval * 100, 1) if total_eval > 0 else 0
        print(f"\n[Phase 12] ACR Concordance: {concordance_pct}% ({n_match}/{total_eval})")
        print(f"  Mismatches: {n_mismatch}, Recalc-only: {n_recalc_only}")
        results["concordance_pct"] = concordance_pct
        results["n_concordant"] = int(n_match)
        results["n_discordant"] = int(n_mismatch)
        results["n_recalc_only"] = int(n_recalc_only)

        if n_mismatch > 0:
            mismatch_detail = complete_df[complete_df["concordance_flag"] == "mismatch"]
            diff = mismatch_detail["tirads_reported"] - mismatch_detail["tirads_recalculated"]
            print(f"  Mismatch direction: mean diff = {diff.mean():.2f}")
            results["mismatch_mean_diff"] = round(float(diff.mean()), 2)

    if dry_run:
        print("\n[Phase 12] DRY RUN — skipping table deployment.")
        return results

    # --- 6. Deploy tables ---
    print("\n[Phase 12] Deploying tables ...")

    con.execute("DROP TABLE IF EXISTS raw_us_tirads_excel_v1")
    con.register("_tmp_complete", complete_df)
    con.execute("CREATE TABLE raw_us_tirads_excel_v1 AS SELECT * FROM _tmp_complete")
    n = con.execute("SELECT COUNT(*) FROM raw_us_tirads_excel_v1").fetchone()[0]
    print(f"  → raw_us_tirads_excel_v1: {n} rows")
    results["tables_deployed"].append(("raw_us_tirads_excel_v1", n))

    con.execute("DROP TABLE IF EXISTS raw_us_tirads_scored_v1")
    con.register("_tmp_scored", scored_df)
    con.execute("CREATE TABLE raw_us_tirads_scored_v1 AS SELECT * FROM _tmp_scored")
    n = con.execute("SELECT COUNT(*) FROM raw_us_tirads_scored_v1").fetchone()[0]
    print(f"  → raw_us_tirads_scored_v1: {n} rows")
    results["tables_deployed"].append(("raw_us_tirads_scored_v1", n))

    con.execute("DROP TABLE IF EXISTS extracted_tirads_validated_v1")
    con.register("_tmp_patient", patient_df)
    con.execute("CREATE TABLE extracted_tirads_validated_v1 AS SELECT * FROM _tmp_patient")
    n = con.execute("SELECT COUNT(*) FROM extracted_tirads_validated_v1").fetchone()[0]
    print(f"  → extracted_tirads_validated_v1: {n} rows")
    results["tables_deployed"].append(("extracted_tirads_validated_v1", n))

    # --- 7. Summary view ---
    con.execute("""
        DROP TABLE IF EXISTS vw_us_nodule_tirads_validated;
        CREATE TABLE vw_us_nodule_tirads_validated AS
        SELECT
            tirads_best_category as category,
            COUNT(*) as n_patients,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct,
            SUM(has_acr_recalculation::INT) as with_acr_recalc,
            SUM(has_scored_excel::INT) as with_scored_excel,
            SUM(has_nlp::INT) as with_nlp,
            ROUND(AVG(n_sources), 1) as avg_sources,
            SUM(concordant_count) as total_concordant,
            SUM(mismatch_count) as total_mismatched
        FROM extracted_tirads_validated_v1
        GROUP BY tirads_best_category
        ORDER BY category
    """)
    vw = con.execute("SELECT * FROM vw_us_nodule_tirads_validated").fetchdf()
    print(f"\n  → vw_us_nodule_tirads_validated:\n{vw.to_string(index=False)}")
    results["tables_deployed"].append(("vw_us_nodule_tirads_validated", len(vw)))

    # --- 8. Build patient_refined_master_clinical_v11 ---
    print("\n[Phase 12] Building patient_refined_master_clinical_v11 ...")
    _build_master_v11(con)
    n = con.execute("SELECT COUNT(*) FROM patient_refined_master_clinical_v11").fetchone()[0]
    print(f"  → patient_refined_master_clinical_v11: {n} rows")
    results["tables_deployed"].append(("patient_refined_master_clinical_v11", n))

    # --- 9. Build advanced_features_v5 ---
    print("\n[Phase 12] Building advanced_features_v5 ...")
    _build_advanced_features_v5(con)
    n = con.execute("SELECT COUNT(*) FROM advanced_features_v5").fetchone()[0]
    print(f"  → advanced_features_v5: {n} rows")
    results["tables_deployed"].append(("advanced_features_v5", n))

    # --- 10. New fill rate ---
    new_fill = con.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN tirads_best_score_v12 IS NOT NULL THEN 1 END) as has_tirads,
            ROUND(COUNT(CASE WHEN tirads_best_score_v12 IS NOT NULL THEN 1 END)*100.0/COUNT(*), 2) as pct
        FROM patient_refined_master_clinical_v11
    """).fetchone()
    results["new_fill_rate"] = float(new_fill[2])
    results["new_tirads_count"] = int(new_fill[1])
    print(f"\n[Phase 12] NEW TIRADS fill rate: {new_fill[2]}% ({new_fill[1]}/{new_fill[0]})")

    # --- 11. Save results JSON ---
    out_dir = PROJECT_ROOT / "notes_extraction"
    results_path = out_dir / f"phase12_results_{ts}.json"
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n[Phase 12] Results saved to {results_path}")

    return results


def _build_master_v11(con):
    """Extend patient_refined_master_clinical_v10 with Phase 12 TIRADS columns."""
    con.execute("""
        DROP TABLE IF EXISTS patient_refined_master_clinical_v11;
        CREATE TABLE patient_refined_master_clinical_v11 AS
        SELECT
            m.*,
            t.tirads_best_score       AS tirads_best_score_v12,
            t.tirads_worst_score      AS tirads_worst_score_v12,
            t.tirads_best_category    AS tirads_best_category_v12,
            t.tirads_worst_category   AS tirads_worst_category_v12,
            t.tirads_source           AS tirads_source_v12,
            t.tirads_reliability      AS tirads_reliability_v12,
            t.has_acr_recalculation   AS tirads_has_acr_recalc_v12,
            t.n_sources               AS tirads_n_sources_v12,
            t.n_nodule_records        AS tirads_n_nodule_records_v12,
            t.concordant_count        AS tirads_concordant_count_v12,
            t.mismatch_count          AS tirads_mismatch_count_v12,
            t.nodule_size_max_mm      AS tirads_nodule_size_max_mm_v12
        FROM patient_refined_master_clinical_v10 m
        LEFT JOIN extracted_tirads_validated_v1 t
            ON m.research_id = t.research_id
    """)


def _build_advanced_features_v5(con):
    """Extend advanced_features_v4 with Phase 12 TIRADS for analytic models."""
    try:
        con.execute("SELECT 1 FROM advanced_features_v4 LIMIT 1")
        base = "advanced_features_v4"
    except Exception:
        try:
            con.execute("SELECT 1 FROM advanced_features_v4_sorted LIMIT 1")
            base = "advanced_features_v4_sorted"
        except Exception:
            print("  WARNING: No advanced_features_v4 found, creating from master v11")
            con.execute("""
                DROP TABLE IF EXISTS advanced_features_v5;
                CREATE TABLE advanced_features_v5 AS
                SELECT * FROM patient_refined_master_clinical_v11
            """)
            return

    con.execute(f"""
        DROP TABLE IF EXISTS advanced_features_v5;
        CREATE TABLE advanced_features_v5 AS
        SELECT
            a.*,
            t.tirads_best_score       AS tirads_best_score_v12,
            t.tirads_worst_score      AS tirads_worst_score_v12,
            t.tirads_best_category    AS tirads_best_category_v12,
            t.tirads_source           AS tirads_source_v12,
            t.has_acr_recalculation   AS tirads_has_acr_recalc_v12,
            t.concordant_count        AS tirads_concordant_count_v12,
            t.mismatch_count          AS tirads_mismatch_count_v12
        FROM {base} a
        LEFT JOIN extracted_tirads_validated_v1 t
            ON a.research_id = t.research_id
    """)


# ---------------------------------------------------------------------------
# Validation view for script 29
# ---------------------------------------------------------------------------
def deploy_validation_view(con):
    """Create val_phase12_tirads_validation table."""
    con.execute("""
        DROP TABLE IF EXISTS val_phase12_tirads_validation;
        CREATE TABLE val_phase12_tirads_validation AS
        SELECT
            'tirads_excel_complete' AS variable,
            (SELECT COUNT(DISTINCT research_id) FROM raw_us_tirads_excel_v1) AS input_count,
            (SELECT COUNT(*) FROM extracted_tirads_validated_v1
             WHERE tirads_source = 'excel_complete_structured') AS refined_count,
            (SELECT COUNT(*) FROM extracted_tirads_validated_v1
             WHERE concordant_count > 0) AS concordant_patients,
            (SELECT COUNT(*) FROM extracted_tirads_validated_v1
             WHERE mismatch_count > 0) AS discordant_patients
        UNION ALL
        SELECT
            'tirads_excel_scored',
            (SELECT COUNT(DISTINCT research_id) FROM raw_us_tirads_scored_v1),
            (SELECT COUNT(*) FROM extracted_tirads_validated_v1
             WHERE has_scored_excel),
            NULL, NULL
        UNION ALL
        SELECT
            'tirads_nlp_notes',
            (SELECT COUNT(*) FROM extracted_us_tirads_v1),
            (SELECT COUNT(*) FROM extracted_tirads_validated_v1
             WHERE has_nlp),
            NULL, NULL
        UNION ALL
        SELECT
            'tirads_total_patients',
            (SELECT COUNT(*) FROM extracted_tirads_validated_v1),
            (SELECT COUNT(*) FROM patient_refined_master_clinical_v11
             WHERE tirads_best_score_v12 IS NOT NULL),
            NULL, NULL
    """)
    print("  → val_phase12_tirads_validation deployed")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Phase 12: TIRADS Excel Ingestion & ACR Validation")
    parser.add_argument("--md", action="store_true", help="Deploy to MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Dry run — no table deployment")
    args = parser.parse_args()

    if args.local:
        import duckdb
        con = duckdb.connect(str(PROJECT_ROOT / "thyroid_master.duckdb"))
    else:
        con = _get_connection(use_md=args.md)
    results = audit_and_refine_phase12(con, dry_run=args.dry_run)

    if not args.dry_run:
        deploy_validation_view(con)

    print("\n" + "=" * 80)
    print("  PHASE 12 COMPLETE")
    print("=" * 80)
    print(f"  New TIRADS fill rate: {results.get('new_fill_rate', 'N/A')}%")
    print(f"  Concordance (reported vs recalculated): {results.get('concordance_pct', 'N/A')}%")
    print(f"  Tables deployed: {len(results.get('tables_deployed', []))}")
    print(f"  Total patients with TIRADS: {results.get('reconciled_patients', 'N/A')}")
    print()

    con.close()


if __name__ == "__main__":
    main()
