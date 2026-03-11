#!/usr/bin/env python3
"""
41_ingest_thyroseq_excel.py — ThyroSeq workbook integration pipeline.

Ingests 'Thyroseq Data Complete.xlsx', matches patients to existing
research_id spine, parses molecular/labs/imaging/treatment content,
fills only missing canonical values, and generates QA/review outputs.

Usage:
    .venv/bin/python scripts/41_ingest_thyroseq_excel.py \\
        --input '/path/to/Thyroseq Data Complete.xlsx' \\
        [--md]    # target MotherDuck instead of local DuckDB
        [--local] # force local DuckDB
        [--dry-run] # parse + match only, no DB writes

Outputs:
    exports/thyroseq_integration_YYYYMMDD_HHMM/
        matched_rows.csv
        unmatched_rows.csv
        conflict_rows.csv
        fill_actions.csv
        parse_failures.csv
        molecular_enrichment.csv
        followup_labs.csv
        followup_events.csv
        manifest.json
    docs/THYROSEQ_INTEGRATION_REPORT.md
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.thyroseq_helpers import (
    compute_row_hash,
    normalize_angioinvasion,
    normalize_dob,
    normalize_ete,
    normalize_hashimoto_graves,
    normalize_lymph_nodes,
    normalize_margins,
    normalize_mrn,
    normalize_multifocal,
    normalize_name,
    normalize_race,
    normalize_sex,
    normalize_tobacco,
    parse_cna,
    parse_days_to_tg,
    parse_fusion_text,
    parse_gep,
    parse_imaging_text,
    parse_mutation_text,
    parse_rai_text,
    parse_surgery_text,
    parse_tg_panel,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

BATCH_ID = str(uuid.uuid4())[:12]
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")

TG_PANEL_COLS = [
    "Tg/Tg Ab/TSH (date)",
    "Tg/Tg Ab/TSH(date)",
    "Tg/Tg Ab/TSH (date).1",
    "Tg/Tg Ab/TSH (date).2",
    "Tg/Tg Ab/TSH (date).3",
    "Tg/Tg Ab/TSH (date).4",
    "Tg/Tg Ab/TSH (date).5",
]

CROSSWALK_FILES = [
    "All Diagnoses & synoptic 12_1_2025.xlsx",
    "Notes 12_1_25.xlsx",
    "Thyroid OP Sheet data.xlsx",
]


# ═══════════════════════════════════════════════════════════════════════════
# Database connection
# ═══════════════════════════════════════════════════════════════════════════

def connect(use_md: bool = False, use_local: bool = False) -> duckdb.DuckDBPyConnection:
    if use_local or os.environ.get("USE_LOCAL_DUCKDB"):
        path = os.environ.get("LOCAL_DUCKDB_PATH", str(ROOT / "thyroid_master_local.duckdb"))
        log.info(f"Connecting to local DuckDB: {path}")
        return duckdb.connect(path)
    if use_md:
        token = os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            import toml
            try:
                token = toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
            except Exception:
                pass
        if not token:
            raise RuntimeError("MOTHERDUCK_TOKEN not found")
        log.info("Connecting to MotherDuck")
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    path = str(ROOT / "thyroid_master_local.duckdb")
    log.info(f"Connecting to local DuckDB: {path}")
    return duckdb.connect(path)


# ═══════════════════════════════════════════════════════════════════════════
# Phase 1: Raw ingest
# ═══════════════════════════════════════════════════════════════════════════

def ingest_raw(excel_path: str) -> pd.DataFrame:
    log.info(f"Phase 1: Loading {excel_path}")
    df = pd.read_excel(excel_path, sheet_name=0)
    log.info(f"  {len(df)} rows, {len(df.columns)} columns")

    df["source_file"] = Path(excel_path).name
    df["source_sheet"] = "Sheet1"
    df["source_row_number"] = range(2, len(df) + 2)  # 1-indexed, skip header
    df["ingestion_batch_id"] = BATCH_ID
    df["imported_at"] = datetime.now().isoformat()

    name_parts = df["Req Patient/Source Name"].apply(normalize_name)
    df["mrn_norm"] = df["Pt. MRN"].apply(normalize_mrn)
    df["dob_norm"] = df["Date of Birth"].apply(normalize_dob)
    df["name_norm"] = name_parts.apply(lambda d: d["name_norm"])
    df["last_name_norm"] = name_parts.apply(lambda d: d["last_name_norm"])
    df["first_name_norm"] = name_parts.apply(lambda d: d["first_name_norm"])

    df["row_hash"] = df.apply(lambda r: compute_row_hash(r.to_dict()), axis=1)

    n_dup = df.duplicated(subset="row_hash").sum()
    if n_dup:
        log.info(f"  Exact duplicate rows detected: {n_dup}")
        df = df.drop_duplicates(subset="row_hash", keep="first").copy()
        log.info(f"  After dedup: {len(df)} rows")

    return df


# ═══════════════════════════════════════════════════════════════════════════
# Phase 2: Build MRN → research_id crosswalk
# ═══════════════════════════════════════════════════════════════════════════

def build_crosswalk(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Build MRN+DOB+Name → research_id crosswalk from raw Excel files."""
    log.info("Phase 2: Building MRN→research_id crosswalk")
    raw_dir = ROOT / "raw"
    records = []

    for fname in CROSSWALK_FILES:
        fpath = raw_dir / fname
        if not fpath.exists():
            log.warning(f"  Crosswalk file not found: {fname}")
            continue
        log.info(f"  Reading {fname}")
        try:
            df = pd.read_excel(fpath)
        except Exception as e:
            log.warning(f"  Failed to read {fname}: {e}")
            continue

        cols_lower = {c.lower(): c for c in df.columns}
        rid_col = None
        for alias in ("research id number", "research_id", "research_id#", "researchid"):
            if alias in cols_lower:
                rid_col = cols_lower[alias]
                break
        if rid_col is None:
            continue

        mrn_col = None
        for alias in ("euh_mrn", "tec_mrn", "patient_mrn", "mrn"):
            if alias in cols_lower:
                mrn_col = cols_lower[alias]
                break

        dob_col = None
        for alias in ("dob", "date_of_birth"):
            if alias in cols_lower:
                dob_col = cols_lower[alias]
                break

        name_cols = {}
        for alias in ("patient_first_nm", "patient_first_name"):
            if alias in cols_lower:
                name_cols["first"] = cols_lower[alias]
                break
        for alias in ("patient_last_nm", "patient_last_name"):
            if alias in cols_lower:
                name_cols["last"] = cols_lower[alias]
                break

        for _, row in df.iterrows():
            rid_val = row.get(rid_col)
            if pd.isna(rid_val):
                continue
            try:
                rid = int(float(str(rid_val).strip().replace(".0", "")))
            except (ValueError, TypeError):
                continue

            mrn = normalize_mrn(row.get(mrn_col)) if mrn_col else None
            dob = normalize_dob(row.get(dob_col)) if dob_col else None

            first_nm = None
            last_nm = None
            if "first" in name_cols:
                v = row.get(name_cols["first"])
                first_nm = str(v).upper().strip() if pd.notna(v) else None
            if "last" in name_cols:
                v = row.get(name_cols["last"])
                last_nm = str(v).upper().strip() if pd.notna(v) else None

            if mrn or (first_nm and last_nm):
                records.append({
                    "research_id": rid,
                    "mrn": mrn,
                    "dob": dob,
                    "first_name": first_nm,
                    "last_name": last_nm,
                    "source_file": fname,
                })

    xw = pd.DataFrame(records)
    if xw.empty:
        log.warning("  Crosswalk is empty — will attempt DB-only matching")
        return xw

    xw = xw.drop_duplicates(subset=["research_id", "mrn"]).copy()
    log.info(f"  Crosswalk: {len(xw)} rows, {xw['research_id'].nunique()} unique patients")
    return xw


# ═══════════════════════════════════════════════════════════════════════════
# Phase 3: Match patients
# ═══════════════════════════════════════════════════════════════════════════

def match_patients(raw: pd.DataFrame, xw: pd.DataFrame) -> pd.DataFrame:
    log.info("Phase 3: Matching patients to existing research_id")
    results = []

    mrn_to_rids: dict[str, set[int]] = {}
    dob_name_index: dict[tuple, set[int]] = {}
    for _, r in xw.iterrows():
        if r["mrn"]:
            mrn_to_rids.setdefault(r["mrn"], set()).add(r["research_id"])
        if r["dob"] and r["last_name"]:
            key = (str(r["dob"]), r["last_name"])
            dob_name_index.setdefault(key, set()).add(r["research_id"])

    mrn_meta: dict[str, dict] = {}
    for _, r in xw.iterrows():
        if r["mrn"] and r["mrn"] not in mrn_meta:
            mrn_meta[r["mrn"]] = {
                "dob": r["dob"], "first_name": r["first_name"], "last_name": r["last_name"]
            }

    for _, row in raw.iterrows():
        rh = row["row_hash"]
        mrn = row["mrn_norm"]
        dob = row["dob_norm"]
        last = row["last_name_norm"]
        first = row["first_name_norm"]

        match = {
            "row_hash": rh,
            "matched_research_id": None,
            "match_method": "manual_review_required",
            "match_confidence": 0.0,
            "review_required": True,
            "conflict_flags": "",
            "review_reason": "",
        }

        if mrn and mrn in mrn_to_rids:
            rids = mrn_to_rids[mrn]
            meta = mrn_meta.get(mrn, {})

            dob_ok = (dob is None or meta.get("dob") is None
                      or str(dob) == str(meta.get("dob")))
            name_compat = True
            conflicts = []

            if not dob_ok:
                conflicts.append("dob_mismatch")
                name_compat = False
            if last and meta.get("last_name") and last != meta["last_name"]:
                conflicts.append("last_name_mismatch")
                name_compat = False

            if len(rids) == 1:
                rid = next(iter(rids))
                if dob_ok and name_compat:
                    if dob and meta.get("dob") and last and meta.get("last_name"):
                        match.update(matched_research_id=rid,
                                     match_method="exact_mrn_dob_name",
                                     match_confidence=1.0, review_required=False)
                    elif last and meta.get("last_name"):
                        match.update(matched_research_id=rid,
                                     match_method="exact_mrn_name",
                                     match_confidence=0.9, review_required=False)
                    else:
                        match.update(matched_research_id=rid,
                                     match_method="exact_mrn_only",
                                     match_confidence=0.7, review_required=False)
                else:
                    match.update(matched_research_id=rid,
                                 match_method="mrn_with_discordance",
                                 match_confidence=0.3, review_required=True,
                                 conflict_flags=";".join(conflicts),
                                 review_reason="MRN matched but identifier discordance")
            else:
                match.update(matched_research_id=int(min(rids)),
                             match_method="mrn_ambiguous_multi",
                             match_confidence=0.2, review_required=True,
                             review_reason=f"MRN maps to {len(rids)} research_ids: {sorted(rids)}")

        elif dob and last:
            key = (str(dob), last)
            if key in dob_name_index:
                rids = dob_name_index[key]
                if len(rids) == 1:
                    match.update(matched_research_id=next(iter(rids)),
                                 match_method="exact_name_dob",
                                 match_confidence=0.6, review_required=True,
                                 review_reason="Matched by DOB+last_name only (no MRN)")
                else:
                    match.update(review_reason=f"DOB+name maps to {len(rids)} RIDs: {sorted(rids)}")

        if match["matched_research_id"] is None:
            match["review_reason"] = "No crosswalk match found"

        results.append(match)

    df = pd.DataFrame(results)
    stats = df["match_method"].value_counts()
    for method, n in stats.items():
        log.info(f"  {method}: {n}")
    log.info(f"  Total matched (confidence >= 0.6): {(df['match_confidence'] >= 0.6).sum()}")
    log.info(f"  Review required: {df['review_required'].sum()}")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Phase 4: Parse all fields
# ═══════════════════════════════════════════════════════════════════════════

def parse_all_fields(raw: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    log.info("Phase 4: Parsing all fields")
    parsed_rows = []

    for idx, row in raw.iterrows():
        match_row = matches.loc[matches["row_hash"] == row["row_hash"]]
        rid = match_row["matched_research_id"].values[0] if len(match_row) else None

        mut = parse_mutation_text(row.get("Thyroseq Mutation"))
        fus = parse_fusion_text(row.get("Gene Fusions"))
        ln = normalize_lymph_nodes(row.get("lymph nodes"))
        hg = normalize_hashimoto_graves(row.get("Hashimotos/Graves"))
        surg = parse_surgery_text(row.get("Surgery"))
        rai = parse_rai_text(row.get("RAI Ablation"))

        parse_notes = []
        if mut["parse_status"] != "ok" and mut["parse_status"] != "null_input":
            parse_notes.append(f"mutation:{mut['parse_status']}")
        if fus["parse_status"] != "ok" and fus["parse_status"] != "null_input":
            parse_notes.append(f"fusion:{fus['parse_status']}")
        if surg["parse_status"] not in ("ok", "null_input"):
            parse_notes.append(f"surgery:{surg['parse_status']}")
        if rai["parse_status"] not in ("ok", "null_input"):
            parse_notes.append(f"rai:{rai['parse_status']}")

        rec = {
            "row_hash": row["row_hash"],
            "matched_research_id": rid,
            "pathology_raw": row.get("Pathology"),
            "mutation_raw": mut["mutation_raw"],
            "fusion_raw": fus["fusion_raw"],
            "gep_raw": row.get("Gene Expression Profile"),
            "cna_raw": row.get("Copy Number Alterations"),
            "gep_norm": parse_gep(row.get("Gene Expression Profile")),
            "cna_norm": parse_cna(row.get("Copy Number Alterations")),
            "mitotic_rate_raw": row.get("Mitotic Rate"),
            "ki67_raw": row.get("Ki67 Index"),
            "margins_norm": normalize_margins(row.get("Margins")),
            "ete_norm": normalize_ete(row.get("ETE")),
            "ln_status": ln["ln_status"],
            "ln_raw": ln.get("ln_raw"),
            "angioinvasion_norm": normalize_angioinvasion(row.get("angioinvasion")),
            "multifocal_norm": normalize_multifocal(row.get("multifocal")),
            "sex_norm": normalize_sex(row.get("Gender")),
            "age_at_diagnosis": row.get("Age at diagnosis"),
            "race_norm": normalize_race(row.get("Race")),
            "tobacco_norm": normalize_tobacco(row.get("Tobacco")),
            "autoimmune_raw": hg["autoimmune_raw"],
            "hashimoto_flag": hg["hashimoto_flag"],
            "graves_flag": hg["graves_flag"],
            "surgery_raw": surg["surgery_raw"],
            "total_thyroidectomy_flag": surg["total_thyroidectomy_flag"],
            "completion_thyroidectomy_flag": surg["completion_thyroidectomy_flag"],
            "hemithyroidectomy_flag": surg["hemithyroidectomy_flag"],
            "surgery_laterality": surg["laterality"],
            "surgery_dates_json": json.dumps(surg["surgery_dates"]),
            "rai_raw": rai["rai_raw"],
            "rai_given_flag": rai["rai_given_flag"],
            "rai_dates_json": json.dumps(rai["rai_dates"]),
            "rai_status": rai["rai_status"],
            "braf_flag": mut["braf_flag"] or False,
            "ras_flag": mut["ras_flag"] or False,
            "tert_flag": mut["tert_flag"] or False,
            "tp53_flag": mut["tp53_flag"] or False,
            "ret_flag": fus["ret_flag"] or False,
            "ntrk_flag": fus["ntrk_flag"] or False,
            "alk_flag": fus["alk_flag"] or False,
            "pparg_flag": fus["pparg_flag"] or False,
            "fusion_flag": fus["fusion_flag"] or False,
            "parse_status": "ok" if not parse_notes else "partial",
            "parse_notes": "; ".join(parse_notes) if parse_notes else None,
        }
        parsed_rows.append(rec)

    df = pd.DataFrame(parsed_rows)
    ok = (df["parse_status"] == "ok").sum()
    log.info(f"  Parse results: {ok} ok, {len(df) - ok} partial/failed")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Phase 5: Build molecular enrichment table
# ═══════════════════════════════════════════════════════════════════════════

def build_molecular_table(raw: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    log.info("Phase 5: Building molecular enrichment table")
    rows = []
    for _, r in raw.iterrows():
        mrow = matches.loc[matches["row_hash"] == r["row_hash"]]
        rid = mrow["matched_research_id"].values[0] if len(mrow) else None
        if rid is None or pd.isna(rid):
            continue

        mut = parse_mutation_text(r.get("Thyroseq Mutation"))
        fus = parse_fusion_text(r.get("Gene Fusions"))

        rows.append({
            "research_id": int(rid),
            "source_row_hash": r["row_hash"],
            "source_file": r.get("source_file", ""),
            "molecular_platform": "ThyroSeq",
            "mutation_raw": mut["mutation_raw"],
            "fusion_raw": fus["fusion_raw"],
            "gep_raw": r.get("Gene Expression Profile"),
            "cna_raw": r.get("Copy Number Alterations"),
            "braf_flag": mut["braf_flag"],
            "ras_flag": mut["ras_flag"],
            "tert_flag": mut["tert_flag"],
            "tp53_flag": mut["tp53_flag"],
            "pik3ca_flag": mut["pik3ca_flag"],
            "tshr_flag": mut["tshr_flag"],
            "ret_flag": fus["ret_flag"],
            "ntrk_flag": fus["ntrk_flag"],
            "alk_flag": fus["alk_flag"],
            "pparg_flag": fus["pparg_flag"],
            "fusion_flag": fus["fusion_flag"],
            "fusion_genes_json": json.dumps(fus["fusion_genes"]) if fus["fusion_genes"] else None,
            "allele_fractions_json": json.dumps(mut["allele_fractions"]) if mut["allele_fractions"] else None,
            "gep_norm": parse_gep(r.get("Gene Expression Profile")),
            "cna_norm": parse_cna(r.get("Copy Number Alterations")),
            "pathology_raw": r.get("Pathology"),
            "imported_at": datetime.now().isoformat(),
        })

    df = pd.DataFrame(rows)
    log.info(f"  Molecular rows: {len(df)}")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Phase 6: Build follow-up labs long table
# ═══════════════════════════════════════════════════════════════════════════

def build_followup_labs(raw: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    log.info("Phase 6: Building follow-up labs table")
    rows = []

    for _, r in raw.iterrows():
        mrow = matches.loc[matches["row_hash"] == r["row_hash"]]
        rid = mrow["matched_research_id"].values[0] if len(mrow) else None
        if rid is None or pd.isna(rid):
            continue

        days_to_tg = parse_days_to_tg(r.get("Days to Tg from Surgery"))

        for seq, col in enumerate(TG_PANEL_COLS, start=1):
            val = r.get(col)
            if pd.isna(val) or val is None:
                continue
            panel = parse_tg_panel(val)
            if panel["parse_status"] == "null_input":
                continue

            rows.append({
                "research_id": int(rid),
                "source_row_hash": r["row_hash"],
                "sequence_number": seq,
                "raw_panel_text": panel["raw_text"],
                "panel_date": panel["panel_date"],
                "panel_date_precision": panel.get("panel_date_precision"),
                "thyroglobulin_value": panel["thyroglobulin_value"],
                "thyroglobulin_operator": panel["thyroglobulin_operator"],
                "anti_tg_value": panel["anti_tg_value"],
                "anti_tg_operator": panel["anti_tg_operator"],
                "tsh_value": panel["tsh_value"],
                "tsh_operator": panel["tsh_operator"],
                "stimulated_flag": panel["stimulated_flag"],
                "suffix_notes": panel["suffix"],
                "days_from_index_surgery": days_to_tg if seq == 1 else None,
                "parse_status": panel["parse_status"],
                "parse_notes": None,
            })

    df = pd.DataFrame(rows)
    ok = (df["parse_status"] == "ok").sum() if len(df) else 0
    log.info(f"  Lab rows: {len(df)} ({ok} parsed ok)")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Phase 7: Build follow-up events long table
# ═══════════════════════════════════════════════════════════════════════════

def build_followup_events(raw: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    log.info("Phase 7: Building follow-up events table")
    rows = []

    for _, r in raw.iterrows():
        mrow = matches.loc[matches["row_hash"] == r["row_hash"]]
        rid = mrow["matched_research_id"].values[0] if len(mrow) else None
        if rid is None or pd.isna(rid):
            continue

        surg = parse_surgery_text(r.get("Surgery"))
        for d in surg.get("surgery_dates", []):
            rows.append({
                "research_id": int(rid),
                "source_row_hash": r["row_hash"],
                "event_type": "surgery",
                "event_date": d,
                "event_date_precision": "day",
                "raw_text": surg["surgery_raw"],
                "parsed_attributes_json": json.dumps({
                    k: surg[k] for k in ("total_thyroidectomy_flag",
                                         "completion_thyroidectomy_flag",
                                         "hemithyroidectomy_flag",
                                         "laterality", "outside_surgery_flag")
                }),
                "parse_status": surg["parse_status"],
            })

        rai = parse_rai_text(r.get("RAI Ablation"))
        for d in rai.get("rai_dates", []):
            rows.append({
                "research_id": int(rid),
                "source_row_hash": r["row_hash"],
                "event_type": "rai",
                "event_date": d,
                "event_date_precision": rai.get("rai_date_precision", "day"),
                "raw_text": rai["rai_raw"],
                "parsed_attributes_json": json.dumps({
                    k: rai[k] for k in ("rai_given_flag", "rai_status",
                                        "multiple_rai_flag", "outside_rai_flag")
                }),
                "parse_status": rai["parse_status"],
            })

        for modality, col in [("nm_scan", "NM Uptake Scan"), ("pet_ct", "PET CT"),
                               ("ultrasound", "Ultrasound"), ("ct", "CT")]:
            img = parse_imaging_text(r.get(col), modality)
            if img["parse_status"] == "null_input":
                continue
            for d in img.get("imaging_dates", [None]):
                rows.append({
                    "research_id": int(rid),
                    "source_row_hash": r["row_hash"],
                    "event_type": f"imaging_{modality}",
                    "event_date": d,
                    "event_date_precision": img.get("imaging_date_precision"),
                    "raw_text": img["imaging_raw"],
                    "parsed_attributes_json": json.dumps({
                        k: img[k] for k in ("negative_flag", "thyroid_bed_uptake",
                                            "focal_uptake", "pulmonary_findings",
                                            "suspicious_nodal_disease", "residual_tissue")
                    }),
                    "parse_status": img["parse_status"],
                })

    df = pd.DataFrame(rows)
    if len(df):
        log.info(f"  Event rows: {len(df)}")
        log.info(f"  By type: {df['event_type'].value_counts().to_dict()}")
    else:
        log.info("  No events extracted")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Phase 8: Fill missing canonical values
# ═══════════════════════════════════════════════════════════════════════════

FILL_MAP = {
    "sex_norm": ("patient_level_summary_mv", "sex"),
    "race_norm": ("path_synoptics", "race"),
}


def fill_missing_values(
    parsed: pd.DataFrame,
    matches: pd.DataFrame,
    con: duckdb.DuckDBPyConnection,
    dry_run: bool = False,
) -> pd.DataFrame:
    """Fill NULL canonical fields from ThyroSeq data.  Returns fill action log."""
    log.info("Phase 8: Fill missing canonical values")
    fill_rows = []

    high_conf = matches[
        (matches["match_confidence"] >= 0.7) & (~matches["review_required"])
    ].copy()
    if high_conf.empty:
        log.info("  No high-confidence matches — skipping fills")
        return pd.DataFrame(fill_rows)

    merged = parsed.merge(
        high_conf[["row_hash", "matched_research_id"]],
        on="row_hash", how="inner", suffixes=("", "_match"),
    )
    if "matched_research_id_match" in merged.columns:
        merged["matched_research_id"] = merged["matched_research_id_match"]
        merged.drop(columns=["matched_research_id_match"], inplace=True)

    for src_col, (tgt_table, tgt_col) in FILL_MAP.items():
        try:
            existing = con.execute(
                f"SELECT research_id, {tgt_col} FROM {tgt_table}"
            ).fetchdf()
        except Exception:
            log.warning(f"  Cannot read {tgt_table}.{tgt_col} — skipping")
            continue

        for _, row in merged.iterrows():
            rid = row["matched_research_id"]
            proposed = row.get(src_col)
            if pd.isna(proposed) or proposed is None:
                continue

            ex = existing.loc[existing["research_id"] == rid]
            if ex.empty:
                continue
            current = ex[tgt_col].values[0]

            if pd.isna(current) or current is None or str(current).strip() == "":
                action = "filled" if not dry_run else "would_fill"
                fill_rows.append({
                    "research_id": int(rid),
                    "target_table": tgt_table,
                    "target_column": tgt_col,
                    "old_value": None,
                    "proposed_value": str(proposed),
                    "action_taken": action,
                    "rationale": f"ThyroSeq {src_col} fills NULL {tgt_col}",
                    "source_row_hash": row["row_hash"],
                    "match_method": high_conf.loc[
                        high_conf["row_hash"] == row["row_hash"], "match_method"
                    ].values[0] if len(high_conf.loc[high_conf["row_hash"] == row["row_hash"]]) else "unknown",
                    "integration_batch_id": BATCH_ID,
                    "action_timestamp": datetime.now().isoformat(),
                })
            elif str(current).strip().lower() != str(proposed).strip().lower():
                fill_rows.append({
                    "research_id": int(rid),
                    "target_table": tgt_table,
                    "target_column": tgt_col,
                    "old_value": str(current),
                    "proposed_value": str(proposed),
                    "action_taken": "conflict_routed_to_review",
                    "rationale": f"ThyroSeq {src_col} conflicts with existing {tgt_col}",
                    "source_row_hash": row["row_hash"],
                    "match_method": high_conf.loc[
                        high_conf["row_hash"] == row["row_hash"], "match_method"
                    ].values[0] if len(high_conf.loc[high_conf["row_hash"] == row["row_hash"]]) else "unknown",
                    "integration_batch_id": BATCH_ID,
                    "action_timestamp": datetime.now().isoformat(),
                })

    log.info(f"  Fill actions: {len(fill_rows)}")
    if fill_rows:
        actions = pd.DataFrame(fill_rows)["action_taken"].value_counts().to_dict()
        for a, n in actions.items():
            log.info(f"    {a}: {n}")

    return pd.DataFrame(fill_rows)


# ═══════════════════════════════════════════════════════════════════════════
# Phase 9: Build review queue
# ═══════════════════════════════════════════════════════════════════════════

def build_review_queue(
    raw: pd.DataFrame,
    matches: pd.DataFrame,
    parsed: pd.DataFrame,
    fill_actions: pd.DataFrame,
) -> pd.DataFrame:
    log.info("Phase 9: Building review queue")
    rows = []
    now = datetime.now().isoformat()

    for _, m in matches[matches["review_required"]].iterrows():
        rows.append({
            "source_row_hash": m["row_hash"],
            "suspected_research_ids": str(m["matched_research_id"]) if pd.notna(m["matched_research_id"]) else None,
            "issue_type": "match_review",
            "issue_detail": m["review_reason"],
            "recommended_action": m["match_method"],
            "created_at": now,
        })

    for _, p in parsed[parsed["parse_status"] != "ok"].iterrows():
        if p["parse_notes"]:
            rows.append({
                "source_row_hash": p["row_hash"],
                "suspected_research_ids": str(p["matched_research_id"]) if pd.notna(p["matched_research_id"]) else None,
                "issue_type": "parse_failure",
                "issue_detail": p["parse_notes"],
                "recommended_action": "manual_parse_review",
                "created_at": now,
            })

    if not fill_actions.empty:
        conflicts = fill_actions[fill_actions["action_taken"] == "conflict_routed_to_review"]
        for _, c in conflicts.iterrows():
            rows.append({
                "source_row_hash": c["source_row_hash"],
                "suspected_research_ids": str(c["research_id"]),
                "issue_type": "structured_conflict",
                "issue_detail": f"{c['target_table']}.{c['target_column']}: "
                                f"existing='{c['old_value']}' vs proposed='{c['proposed_value']}'",
                "recommended_action": "manual_review",
                "created_at": now,
            })

    df = pd.DataFrame(rows)
    if len(df):
        log.info(f"  Review queue: {len(df)} items")
        log.info(f"  By type: {df['issue_type'].value_counts().to_dict()}")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Phase 10: Write to DuckDB
# ═══════════════════════════════════════════════════════════════════════════

def write_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    raw: pd.DataFrame,
    matches: pd.DataFrame,
    parsed: pd.DataFrame,
    molecular: pd.DataFrame,
    labs: pd.DataFrame,
    events: pd.DataFrame,
    fill_actions: pd.DataFrame,
    review_queue: pd.DataFrame,
):
    log.info("Phase 10: Writing tables to DuckDB")

    raw_safe = raw.copy()
    for c in raw_safe.select_dtypes(include=["object"]).columns:
        raw_safe[c] = raw_safe[c].apply(lambda x: str(x) if pd.notna(x) and x is not None else None)
    date_cols = [c for c in raw_safe.columns if raw_safe[c].dtype == "object"
                 and c.endswith("_norm") and "dob" in c]
    for c in date_cols:
        raw_safe[c] = raw_safe[c].astype(str).replace("None", None)

    tables = {
        "stg_thyroseq_excel_raw": raw_safe,
        "stg_thyroseq_match_results": matches,
        "stg_thyroseq_parsed": parsed,
        "thyroseq_molecular_enrichment": molecular,
        "thyroseq_followup_labs": labs,
        "thyroseq_followup_events": events,
        "thyroseq_fill_actions": fill_actions,
        "thyroseq_review_queue": review_queue,
    }

    for name, df in tables.items():
        if df.empty:
            log.info(f"  {name}: skipped (empty)")
            continue
        df_safe = df.copy()
        for c in df_safe.columns:
            if df_safe[c].dtype == object:
                df_safe[c] = df_safe[c].apply(
                    lambda x: str(x) if pd.notna(x) and x is not None else None
                )
            elif str(df_safe[c].dtype).startswith("float"):
                df_safe[c] = df_safe[c].where(pd.notna(df_safe[c]), None)

        try:
            con.execute(f"DROP TABLE IF EXISTS {name}")
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM df_safe")
            count = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            log.info(f"  {name}: {count} rows")
        except Exception as e:
            log.error(f"  Failed to write {name}: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# Phase 11: Export CSVs + manifest
# ═══════════════════════════════════════════════════════════════════════════

def export_outputs(
    out_dir: Path,
    raw: pd.DataFrame,
    matches: pd.DataFrame,
    parsed: pd.DataFrame,
    molecular: pd.DataFrame,
    labs: pd.DataFrame,
    events: pd.DataFrame,
    fill_actions: pd.DataFrame,
    review_queue: pd.DataFrame,
) -> dict:
    log.info(f"Phase 11: Exporting to {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    matched = matches[matches["match_confidence"] >= 0.6].merge(
        raw[["row_hash", "Req Patient/Source Name", "mrn_norm", "Pathology"]],
        on="row_hash", how="left",
    )
    unmatched = matches[matches["matched_research_id"].isna()].merge(
        raw[["row_hash", "Req Patient/Source Name", "mrn_norm", "Pathology"]],
        on="row_hash", how="left",
    )
    conflict = review_queue[review_queue["issue_type"] == "structured_conflict"]
    parse_fail = parsed[parsed["parse_status"] != "ok"]

    exports = {
        "matched_rows.csv": matched,
        "unmatched_rows.csv": unmatched,
        "conflict_rows.csv": conflict,
        "fill_actions.csv": fill_actions,
        "parse_failures.csv": parse_fail,
        "molecular_enrichment.csv": molecular,
        "followup_labs.csv": labs,
        "followup_events.csv": events,
        "review_queue.csv": review_queue,
    }

    for fname, df in exports.items():
        df.to_csv(out_dir / fname, index=False)

    manifest = {
        "pipeline": "41_ingest_thyroseq_excel",
        "batch_id": BATCH_ID,
        "timestamp": TIMESTAMP,
        "git_sha": _git_sha(),
        "source_rows_ingested": len(raw),
        "exact_duplicates_removed": 0,
        "high_confidence_matches": int((matches["match_confidence"] >= 0.7).sum()),
        "review_required": int(matches["review_required"].sum()),
        "unmatched_rows": int(matches["matched_research_id"].isna().sum()),
        "molecular_rows": len(molecular),
        "lab_rows": len(labs),
        "event_rows": len(events),
        "fill_actions": len(fill_actions),
        "conflicts": len(conflict),
        "parse_failures": len(parse_fail),
        "review_queue_items": len(review_queue),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    log.info(f"  Manifest: {json.dumps(manifest, indent=2)}")
    return manifest


def _git_sha() -> str:
    try:
        import subprocess
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(ROOT), stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return "unknown"


# ═══════════════════════════════════════════════════════════════════════════
# Phase 12: Integration report
# ═══════════════════════════════════════════════════════════════════════════

def write_integration_report(out_dir: Path, manifest: dict, matches: pd.DataFrame,
                              review_queue: pd.DataFrame, fill_actions: pd.DataFrame):
    log.info("Phase 12: Writing integration report")
    report_path = ROOT / "docs" / "THYROSEQ_INTEGRATION_REPORT.md"

    match_summary = matches["match_method"].value_counts().to_dict()
    review_summary = review_queue["issue_type"].value_counts().to_dict() if len(review_queue) else {}

    lines = [
        "# ThyroSeq Integration Report",
        "",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}  ",
        f"**Batch ID:** `{BATCH_ID}`  ",
        f"**Git SHA:** `{manifest.get('git_sha', 'unknown')}`  ",
        "",
        "## Summary Metrics",
        "",
        f"| Metric | Count |",
        f"|--------|-------|",
        f"| Source rows ingested | {manifest['source_rows_ingested']} |",
        f"| High-confidence matches | {manifest['high_confidence_matches']} |",
        f"| Manual review required | {manifest['review_required']} |",
        f"| Unmatched rows | {manifest['unmatched_rows']} |",
        f"| Molecular enrichment rows | {manifest['molecular_rows']} |",
        f"| Follow-up lab rows | {manifest['lab_rows']} |",
        f"| Follow-up event rows | {manifest['event_rows']} |",
        f"| Fill actions | {manifest['fill_actions']} |",
        f"| Conflicts | {manifest['conflicts']} |",
        f"| Parse failures | {manifest['parse_failures']} |",
        "",
        "## Match Method Breakdown",
        "",
        "| Method | Count |",
        "|--------|-------|",
    ]
    for method, n in sorted(match_summary.items()):
        lines.append(f"| {method} | {n} |")

    if review_summary:
        lines += [
            "",
            "## Review Queue Summary",
            "",
            "| Issue Type | Count |",
            "|------------|-------|",
        ]
        for issue, n in sorted(review_summary.items()):
            lines.append(f"| {issue} | {n} |")

    if not fill_actions.empty:
        by_col = fill_actions.groupby("target_column")["action_taken"].value_counts()
        lines += [
            "",
            "## Fill Actions by Column",
            "",
            "| Column | Action | Count |",
            "|--------|--------|-------|",
        ]
        for (col, action), n in by_col.items():
            lines.append(f"| {col} | {action} | {n} |")

    lines += [
        "",
        "## Output Tables",
        "",
        "| Table | Description |",
        "|-------|-------------|",
        "| `stg_thyroseq_excel_raw` | Raw staging with all original columns + identifiers |",
        "| `stg_thyroseq_match_results` | Patient matching results |",
        "| `stg_thyroseq_parsed` | Parsed/normalized fields |",
        "| `thyroseq_molecular_enrichment` | Molecular findings (long format) |",
        "| `thyroseq_followup_labs` | Serial Tg/TgAb/TSH values (long format) |",
        "| `thyroseq_followup_events` | Surgery/RAI/imaging events (long format) |",
        "| `thyroseq_fill_actions` | Audit log of field fills |",
        "| `thyroseq_review_queue` | Items requiring manual review |",
        "",
        f"## Export Directory",
        "",
        f"`{out_dir.relative_to(ROOT)}/`",
        "",
    ]

    report_path.write_text("\n".join(lines))
    log.info(f"  Report: {report_path}")


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="ThyroSeq workbook integration pipeline")
    ap.add_argument("--input", required=True, help="Path to Thyroseq Data Complete.xlsx")
    ap.add_argument("--md", action="store_true", help="Use MotherDuck")
    ap.add_argument("--local", action="store_true", help="Force local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Parse/match only, no DB writes")
    args = ap.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        log.error(f"Input file not found: {input_path}")
        sys.exit(1)

    con = connect(use_md=args.md, use_local=args.local)

    # Phase 1: Raw ingest
    raw = ingest_raw(str(input_path))

    # Phase 2: Build crosswalk
    xw = build_crosswalk(con)

    # Phase 3: Match
    matches = match_patients(raw, xw)

    # Phase 4: Parse
    parsed = parse_all_fields(raw, matches)

    # Phase 5: Molecular table
    molecular = build_molecular_table(raw, matches)

    # Phase 6: Follow-up labs
    labs = build_followup_labs(raw, matches)

    # Phase 7: Follow-up events
    events = build_followup_events(raw, matches)

    # Phase 8: Fill missing values
    fill_actions = fill_missing_values(parsed, matches, con, dry_run=args.dry_run)

    # Phase 9: Review queue
    review_queue = build_review_queue(raw, matches, parsed, fill_actions)

    # Phase 10: Write to DB
    if not args.dry_run:
        write_to_duckdb(con, raw, matches, parsed, molecular, labs, events,
                        fill_actions, review_queue)

    # Phase 11: Export
    out_dir = ROOT / "exports" / f"thyroseq_integration_{TIMESTAMP}"
    manifest = export_outputs(out_dir, raw, matches, parsed, molecular, labs,
                              events, fill_actions, review_queue)

    # Phase 12: Report
    write_integration_report(out_dir, manifest, matches, review_queue, fill_actions)

    con.close()
    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
