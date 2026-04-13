#!/usr/bin/env python3
"""
US nodule + TI-RADS + downstream linkage audit (source vs canonical).

Recomputes inventories from raw Excel + MotherDuck; writes required CSVs + verdict.md.

Run (MotherDuck RW token via motherduck_client.get_token — motherduck.local.toml ok):
  .venv/bin/python studies/20260413_us_nodule_tirads_linkage_audit/run_us_nodule_tirads_linkage_audit.py

Optional:
  --local   use thyroid_master.duckdb instead of MotherDuck (no token)
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from llm_extraction.extraction_audit_engine_v10 import (  # noqa: E402
    ACRTIRADSCalculator,
    _parse_tr_value,
    ingest_complete_us_excel,
    ingest_tirads_scored_excel,
)
from motherduck_client import get_token, token_mode  # noqa: E402
from utils.md_connect import connect_md_or_file  # noqa: E402
from utils.imaging_12_slots import (  # noqa: E402
    norm_date_str,
    parse_imaging_12_exam_slots,
    stable_key,
)

LOCAL_DB = ROOT / "thyroid_master.duckdb"
CMD_LOG = OUT / "commands_run.log"
DATESTAMP = "20260413"


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    with CMD_LOG.open("a", encoding="utf-8") as fh:
        fh.write(f"{ts} {msg}\n")


def _excel_row_ix(ix: object) -> int:
    if isinstance(ix, (int, np.integer)):
        return int(ix)
    return int(str(ix))


def parse_complete_with_provenance(path: Path) -> pd.DataFrame:
    """All_Ultrasound_Reports: one row per extracted nodule + excel row index."""
    df = pd.read_excel(str(path), sheet_name="All_Ultrasound_Reports")
    calc = ACRTIRADSCalculator()
    rows = []
    for row_ix, report in df.iterrows():
        rxi = _excel_row_ix(row_ix)
        rid = report.get("Research_ID")
        if pd.isna(rid):
            continue
        rid = int(rid)
        us_date = report.get("Ultrasound_Date")
        us_num = report.get("US_Report_Number")
        sheet = report.get("Sheet_Name", "")
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
            calcv = report.get(f"Nodule_{nod}_Calcifications")
            loc = report.get(f"Nodule_{nod}_Location")
            length_mm = report.get(f"Nodule_{nod}_Length_mm")
            width_mm = report.get(f"Nodule_{nod}_Width_mm")
            height_mm = report.get(f"Nodule_{nod}_Height_mm")

            if tirads_reported is None and pd.isna(comp):
                continue

            recalc = calc.calculate(
                composition=comp if not pd.isna(comp) else None,
                echogenicity=echo if not pd.isna(echo) else None,
                shape=shp if not pd.isna(shp) else None,
                margins=marg if not pd.isna(marg) else None,
                calcifications=calcv if not pd.isna(calcv) else None,
            )

            size_max_mm = None
            for dim_val in [length_mm, width_mm, height_mm]:
                if dim_val is not None and not (isinstance(dim_val, float) and np.isnan(dim_val)):
                    v = float(dim_val)
                    if size_max_mm is None or v > size_max_mm:
                        size_max_mm = v

            dnorm = norm_date_str(us_date)
            usn_i = int(us_num) if not pd.isna(us_num) else None
            src_uid = hashlib.sha256(
                f"COMPLETE|{path.name}|All_Ultrasound_Reports|row{rxi}|N{nod}|{rid}".encode()
            ).hexdigest()[:20]

            rows.append(
                {
                    "source_system": "COMPLETE_MULTI_SHEET",
                    "source_workbook": path.name,
                    "source_sheet": "All_Ultrasound_Reports",
                    "excel_row_index": rxi,
                    "source_cell_region": f"row {rxi + 2} excel / nodule slot {nod}",
                    "source_nodule_uid": src_uid,
                    "research_id": rid,
                    "us_report_number": usn_i,
                    "exam_date_norm": dnorm,
                    "nodule_number": nod,
                    "tirads_reported": tirads_reported,
                    "tirads_recalculated": recalc["tirads_recalculated"],
                    "n_criteria_available": recalc["n_criteria_available"],
                    "composition_raw": str(comp) if not pd.isna(comp) else None,
                    "nodule_location": str(loc) if not pd.isna(loc) else None,
                    "nodule_size_max_mm": round(size_max_mm, 2) if size_max_mm else None,
                    "ln_assessment_excerpt": str(ln_assessment)[:120] if not pd.isna(ln_assessment) else None,
                    "recommendation_excerpt": str(recommendation)[:120] if not pd.isna(recommendation) else None,
                    "source_sheet_name_cell": str(sheet) if not pd.isna(sheet) else None,
                    "deterministic_key": stable_key(rid, dnorm, nod),
                }
            )
    return pd.DataFrame(rows)


def parse_scored_with_provenance(path: Path) -> pd.DataFrame:
    """US Nodules TIRADS: preserve sheet + row index."""
    xl = pd.ExcelFile(str(path))
    rows = []
    for sheet_name in xl.sheet_names:
        us_match = re.match(r"US-(\d+)", str(sheet_name))
        if not us_match:
            continue
        us_num = int(us_match.group(1))
        df = pd.read_excel(str(path), sheet_name=sheet_name)
        rid_col = "Research ID number"
        if rid_col not in df.columns:
            continue
        date_candidates = [c for c in df.columns if "date" in c.lower()]
        date_col = date_candidates[0] if date_candidates else None

        for row_ix, row in df.iterrows():
            rxi = _excel_row_ix(row_ix)
            rid = row.get(rid_col)
            if pd.isna(rid):
                continue
            rid = int(rid)
            us_date = row.get(date_col) if date_col else None
            dnorm = norm_date_str(us_date)

            for nod in range(1, 15):
                tr_candidates = [f"N{nod} TR", f"N{nod}_TR"]
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
                src_uid = hashlib.sha256(
                    f"SCORED|{path.name}|{sheet_name}|row{rxi}|N{nod}|{rid}".encode()
                ).hexdigest()[:20]
                rows.append(
                    {
                        "source_system": "US_NODULES_TIRADS_SCORED",
                        "source_workbook": path.name,
                        "source_sheet": sheet_name,
                        "excel_row_index": rxi,
                        "source_cell_region": f"row {rxi + 2} excel / N{nod} TR",
                        "source_nodule_uid": src_uid,
                        "research_id": rid,
                        "us_report_number": us_num,
                        "exam_date_norm": dnorm,
                        "nodule_number": nod,
                        "tirads_reported": score,
                        "tirads_recalculated": None,
                        "n_criteria_available": 0,
                        "nodule_description_excerpt": nod_text,
                        "deterministic_key": stable_key(rid, dnorm, nod),
                    }
                )
    return pd.DataFrame(rows)


def try_fetch(con, sql: str) -> pd.DataFrame | None:
    try:
        return con.execute(sql).fetchdf()
    except Exception as e:
        _log(f"skip query: {e}")
        return None


def table_exists(con, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true", help="Use local thyroid_master.duckdb")
    args = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    if CMD_LOG.exists():
        CMD_LOG.unlink()
    _log("start run_us_nodule_tirads_linkage_audit")
    tok_ok = bool(get_token())
    _log(f"token_mode={token_mode()} token={'SET' if tok_ok else 'MISSING'} local={args.local}")

    raw_complete = ROOT / "raw" / "COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx"
    raw_scored = ROOT / "raw" / "US Nodules TIRADS 12_1_25.xlsx"
    raw_imaging12 = ROOT / "raw" / "Imaging_12_1_25.xlsx"
    for p in (raw_complete, raw_scored, raw_imaging12):
        if not p.is_file():
            print(f"FATAL: missing {p}")
            return 1

    # --- 1) Source inventories ---
    complete_prov = parse_complete_with_provenance(raw_complete)
    scored_prov = parse_scored_with_provenance(raw_scored)
    imaging12 = parse_imaging_12_exam_slots(raw_imaging12)

    # Legacy ingest row counts (sanity vs Phase12 functions)
    complete_legacy = ingest_complete_us_excel(raw_complete)
    scored_legacy = ingest_tirads_scored_excel(raw_scored)
    _log(f"complete_prov={len(complete_prov)} complete_legacy={len(complete_legacy)}")
    _log(f"scored_prov={len(scored_prov)} scored_legacy={len(scored_legacy)}")
    _log(f"imaging12_slots={len(imaging12)}")

    # Exam counts
    rep_wide = pd.read_excel(str(raw_complete), sheet_name="All_Ultrasound_Reports")
    n_exams_complete = rep_wide["Research_ID"].notna().sum()
    n_exams_imaging12 = (
        imaging12.groupby(["research_id", "us_report_number", "exam_date_norm"]).ngroups
        if len(imaging12)
        else 0
    )

    source_parts = []
    for dfp in (complete_prov, scored_prov, imaging12):
        if len(dfp):
            source_parts.append(dfp)
    source_nodule_inventory = pd.concat(source_parts, ignore_index=True)
    source_nodule_inventory.to_csv(OUT / "source_nodule_inventory.csv", index=False)

    # --- 2) DB ---
    if args.local:
        con = connect_md_or_file(LOCAL_DB, md=False)
    else:
        con = connect_md_or_file(LOCAL_DB, md=True, fail_closed=True)
    _log("connected")

    def q(sql: str) -> pd.DataFrame | None:
        return try_fetch(con, sql)

    img_master = q("SELECT * FROM imaging_nodule_master_v1")
    img_long = q("SELECT * FROM imaging_nodule_long_v2")
    raw_excel = q("SELECT * FROM raw_us_tirads_excel_v1")
    ext_val = q("SELECT * FROM extracted_tirads_validated_v1")
    link_mm = q("SELECT * FROM imaging_fna_linkage_mm_v1")
    rev_q = q("SELECT * FROM review_queue_imaging_fna_mm_v1")
    us_rep = q(
        "SELECT research_id, us_report_number, CAST(ultrasound_date AS VARCHAR) AS udate "
        "FROM ultrasound_reports"
    )
    serial_us = (
        q("SELECT COUNT(*) AS row_count FROM serial_imaging_us")
        if table_exists(con, "serial_imaging_us")
        else None
    )
    path_conc = None
    if table_exists(con, "imaging_pathology_concordance_review_v2"):
        path_conc = q(
            "SELECT research_id, nodule_id FROM imaging_pathology_concordance_review_v2 LIMIT 100000"
        )
    fna_counts = (
        q("SELECT research_id, COUNT(*) AS n_fna FROM fna_episode_master_v2 GROUP BY 1")
        if table_exists(con, "fna_episode_master_v2")
        else None
    )

    canon_frames = []
    if img_master is not None and len(img_master):
        m = img_master.copy()
        m["origin_table"] = "imaging_nodule_master_v1"
        canon_frames.append(m)
    if img_long is not None and len(img_long):
        m = img_long.copy()
        m["origin_table"] = "imaging_nodule_long_v2"
        canon_frames.append(m)
    if raw_excel is not None and len(raw_excel):
        m = raw_excel.copy()
        m["origin_table"] = "raw_us_tirads_excel_v1"
        canon_frames.append(m)
    raw_i12 = (
        q("SELECT * FROM raw_imaging_12_slots_v1")
        if table_exists(con, "raw_imaging_12_slots_v1")
        else None
    )
    if raw_i12 is not None and len(raw_i12):
        m = raw_i12.copy()
        m["origin_table"] = "raw_imaging_12_slots_v1"
        canon_frames.append(m)

    canonical_nodule_inventory = pd.concat(canon_frames, ignore_index=True) if canon_frames else pd.DataFrame()
    canonical_nodule_inventory.to_csv(OUT / "canonical_nodule_inventory.csv", index=False)

    # Keys for master (preferred)
    if img_master is None or not len(img_master):
        print("FATAL: imaging_nodule_master_v1 missing or empty")
        con.close()
        return 1

    img = img_master.copy()
    for c in ("exam_date",):
        if c in img.columns:
            img[f"{c}_s"] = img[c].map(norm_date_str)
    if "exam_date_s" not in img.columns:
        img["exam_date_s"] = img["exam_date"].map(norm_date_str) if "exam_date" in img.columns else None

    img["deterministic_key"] = img.apply(
        lambda r: stable_key(
            int(r["research_id"]),
            r.get("exam_date_s"),
            int(r["nodule_number"]) if pd.notna(r.get("nodule_number")) else 0,
        ),
        axis=1,
    )
    if img["deterministic_key"].nunique() != len(img):
        img = img.drop_duplicates(subset=["deterministic_key"], keep="first")

    # Primary linkage
    prim = pd.DataFrame()
    if link_mm is not None and len(link_mm) and "is_primary_link" in link_mm.columns:
        prim = link_mm[link_mm["is_primary_link"] == True].copy()  # noqa: E712
    link_by_nodule = prim.groupby("nodule_id").first() if len(prim) else pd.DataFrame()

    rev_nodule_ids: set[str] = set()
    if rev_q is not None and len(rev_q) and "nodule_id" in rev_q.columns:
        rev_nodule_ids = set(rev_q["nodule_id"].astype(str))

    path_by_nodule = (
        path_conc.drop_duplicates("nodule_id").set_index("nodule_id")
        if path_conc is not None and len(path_conc) and "nodule_id" in path_conc.columns
        else pd.DataFrame()
    )

    fna_n = {}
    if fna_counts is not None and len(fna_counts):
        fna_n = dict(zip(fna_counts["research_id"].astype(int), fna_counts["n_fna"].astype(int)))
    img_idx = img.set_index("deterministic_key", drop=False)
    # Index canonical rows by (research_id, nodule_number) for ±30d alignment (non-COMPLETE sources)
    by_rid_nod: dict[tuple[int, int], list[tuple[str, str]]] = {}
    for _, r in img.iterrows():
        rid_i = int(r["research_id"])
        nod_i = int(r["nodule_number"]) if pd.notna(r.get("nodule_number")) else 0
        ds = r.get("exam_date_s")
        if not ds:
            continue
        dsn = norm_date_str(ds)
        if not dsn:
            continue
        k = r["deterministic_key"]
        by_rid_nod.setdefault((rid_i, nod_i), []).append((dsn, k))

    def heuristic_key_pm1(rid, d, nod):
        if not d:
            return None
        ds = norm_date_str(d) if d is not None else None
        if not ds:
            return None
        try:
            base = date.fromisoformat(ds)
        except ValueError:
            return None
        for delta in (-1, 1):
            d2 = (base + timedelta(days=delta)).isoformat()
            kk = stable_key(rid, d2, nod)
            if kk in img_idx.index:
                return kk, "heuristic_pm1"
        return None

    def heuristic_key_pm30(rid, d, nod):
        """Closest canonical row within 30d (same rid+nod)."""
        ds = norm_date_str(d) if d is not None else None
        if not ds:
            return None
        try:
            src_d = date.fromisoformat(ds)
        except ValueError:
            return None
        cands = by_rid_nod.get((int(rid), int(nod)), [])
        best: tuple[int, str] | None = None
        for cand_date, cand_key in cands:
            try:
                cd = date.fromisoformat(cand_date)
            except ValueError:
                continue
            gap = abs((src_d - cd).days)
            if gap <= 30 and (best is None or gap < best[0]):
                best = (gap, cand_key)
        if best:
            return best[1], f"heuristic_pm30_gap{best[0]}"
        return None

    # --- Match matrix ---
    match_rows = []
    for _, s in source_nodule_inventory.iterrows():
        k = s["deterministic_key"]
        match_type = "missing"
        canon_key = None
        nodule_id = None
        src_sys = str(s.get("source_system") or "")
        if k in img_idx.index:
            match_type = "exact"
            canon_key = k
            row = img_idx.loc[k]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            nodule_id = row.get("nodule_id")
        else:
            hk = heuristic_key_pm1(
                int(s["research_id"]),
                s.get("exam_date_norm"),
                int(s["nodule_number"]),
            )
            if hk:
                canon_key, match_type = hk
                row = img_idx.loc[canon_key]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[0]
                nodule_id = row.get("nodule_id")
            elif src_sys in ("US_NODULES_TIRADS_SCORED", "IMAGING_12_1_25"):
                hw = heuristic_key_pm30(
                    int(s["research_id"]),
                    s.get("exam_date_norm"),
                    int(s["nodule_number"]),
                )
                if hw:
                    canon_key, match_type = hw
                    row = img_idx.loc[canon_key]
                    if isinstance(row, pd.DataFrame):
                        row = row.iloc[0]
                    nodule_id = row.get("nodule_id")

        extraction_status = (
            "exact"
            if match_type == "exact"
            else "heuristic"
            if str(match_type).startswith("heuristic")
            else "missing"
        )

        # TI-RADS status (source row)
        n_crit = int(s.get("n_criteria_available") or 0)
        explicit = s.get("tirads_reported")
        explicit_ok = explicit is not None and not (isinstance(explicit, float) and np.isnan(explicit))
        recalc = s.get("tirads_recalculated")
        recalc_ok = recalc is not None and not (isinstance(recalc, float) and np.isnan(recalc))

        if explicit_ok:
            tirads_src = "explicit_in_source"
        elif recalc_ok:
            tirads_src = "recomputable_in_source"
        elif n_crit >= 5:
            tirads_src = "recomputable_in_source"
        elif n_crit > 0:
            tirads_src = "partial_acr_only"
        else:
            tirads_src = "insufficient_source_detail"

        db_tr = None
        db_acr = None
        if canon_key is not None:
            row = img_idx.loc[canon_key]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            db_tr = row.get("tirads_reported")
            db_acr = row.get("tirads_acr_recalculated")

        missing_unexplained = False
        if extraction_status != "missing" and n_crit >= 5:
            if pd.isna(db_tr) and pd.isna(db_acr):
                missing_unexplained = True

        if extraction_status == "missing":
            tirads_status = "missing_unexplained"
        elif missing_unexplained:
            tirads_status = "missing_unexplained"
        elif explicit_ok or recalc_ok or n_crit >= 5:
            if pd.notna(db_tr) or pd.notna(db_acr):
                tirads_status = "canonical_present"
            else:
                tirads_status = "insufficient_source_detail"
        else:
            tirads_status = "insufficient_source_detail"

        provenance_status = extraction_status

        # Downstream linkage
        downstream = "unresolved"
        note = ""
        rid = int(s["research_id"])
        if extraction_status == "missing":
            downstream = "unresolved"
            note = "no canonical row for deterministic/heuristic key"
        else:
            nid = str(nodule_id) if nodule_id is not None else ""
            if nid and nid in path_by_nodule.index:
                downstream = "linked_to_pathology"
            elif nid and nid in link_by_nodule.index:
                lr = link_by_nodule.loc[nid]
                if isinstance(lr, pd.DataFrame):
                    lr = lr.iloc[0]
                feid = lr.get("fna_episode_id")
                if feid is not None and pd.notna(feid):
                    downstream = "linked_to_fna"
                else:
                    downstream = "candidate_only"
            else:
                if fna_n.get(rid, 0) == 0:
                    downstream = "no_eligible_fna"
                    note = "zero fna_episode_master_v2 rows for patient"
                elif nid and str(nid) in rev_nodule_ids:
                    downstream = "unresolved"
                    note = "in review_queue_imaging_fna_mm_v1"
                else:
                    downstream = "unresolved"
                    note = "no primary imaging_fna_linkage_mm_v1 row for nodule_id"

        inv_note = ""
        if extraction_status == "missing":
            if src_sys == "COMPLETE_MULTI_SHEET":
                inv_note = "FAIL: COMPLETE unpivot row absent from imaging_nodule_master_v1 (unexpected)"
            elif src_sys == "US_NODULES_TIRADS_SCORED":
                inv_note = (
                    "No canonical nodule after exact key + ±1d + ±30d (same rid+nodule); "
                    "scored workbook US date may not match COMPLETE Ultrasound_Date used for master."
                )
            elif src_sys == "IMAGING_12_1_25":
                inv_note = (
                    "Inferred from Imaging_12 exam-slot text; no canonical row after script 50 "
                    "(COMPLETE + scored + Imaging_12 supplements) and ±30d match."
                )

        match_rows.append(
            {
                "source_system": s.get("source_system"),
                "source_nodule_uid": s.get("source_nodule_uid"),
                "deterministic_key": k,
                "match_type": match_type,
                "canonical_deterministic_key": canon_key,
                "canonical_nodule_id": nodule_id,
                "extraction_status": extraction_status,
                "tirads_source_class": tirads_src,
                "n_acr_criteria_source": n_crit,
                "db_tirads_reported": db_tr,
                "db_tirads_acr_recalculated": db_acr,
                "tirads_status": tirads_status,
                "missing_canonical_tirads_despite_sufficient_source": missing_unexplained,
                "provenance_status": provenance_status,
                "downstream_linkage_state": downstream,
                "downstream_note": note[:500],
                "investigation_note": inv_note[:800],
            }
        )

    match_df = pd.DataFrame(match_rows)
    match_df.to_csv(OUT / "nodule_match_matrix.csv", index=False)

    ss_rows = []
    for src_sys_name in sorted(match_df["source_system"].dropna().unique()):
        sub = match_df[match_df["source_system"] == src_sys_name]
        ss_rows.append(
            {
                "source_system": src_sys_name,
                "n_rows": len(sub),
                "n_exact": int((sub["extraction_status"] == "exact").sum()),
                "n_heuristic": int((sub["extraction_status"] == "heuristic").sum()),
                "n_missing": int((sub["extraction_status"] == "missing").sum()),
            }
        )
    pd.DataFrame(ss_rows).to_csv(OUT / "source_system_summary.csv", index=False)

    # TI-RADS recompute comparison (COMPLETE source only)
    tri_rows = []
    for _, s in complete_prov.iterrows():
        k = s["deterministic_key"]
        db_tr = db_acr = None
        if k in img_idx.index:
            row = img_idx.loc[k]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            db_tr = row.get("tirads_reported")
            db_acr = row.get("tirads_acr_recalculated")
        tri_rows.append(
            {
                "deterministic_key": k,
                "research_id": s["research_id"],
                "source_tirads_reported": s.get("tirads_reported"),
                "source_tirads_recalculated": s.get("tirads_recalculated"),
                "source_n_criteria": s.get("n_criteria_available"),
                "canonical_tirads_reported": db_tr,
                "canonical_tirads_acr_recalculated": db_acr,
                "delta_reported_minus_canonical": (
                    float(s["tirads_reported"]) - float(db_tr)
                    if pd.notna(s.get("tirads_reported"))
                    and db_tr is not None
                    and not pd.isna(db_tr)
                    else None
                ),
            }
        )
    pd.DataFrame(tri_rows).to_csv(OUT / "tirads_recompute_comparison.csv", index=False)

    # Linkage state matrix (canonical nodules)
    link_state_rows = []
    for _, r in img.iterrows():
        link_nid: object = r.get("nodule_id")
        link_nid_s = str(link_nid) if link_nid is not None else ""
        st = "unresolved"
        note = ""
        rid = int(r["research_id"])
        if link_nid_s and link_nid_s in path_by_nodule.index:
            st = "linked_to_pathology"
        elif link_nid_s and link_nid_s in link_by_nodule.index:
            lr = link_by_nodule.loc[link_nid_s]
            if isinstance(lr, pd.DataFrame):
                lr = lr.iloc[0]
            if lr.get("fna_episode_id") is not None and pd.notna(lr.get("fna_episode_id")):
                st = "linked_to_fna"
            else:
                st = "candidate_only"
        else:
            if fna_n.get(rid, 0) == 0:
                st = "no_eligible_fna"
                note = "no FNA episodes for patient"
            else:
                st = "unresolved"
        link_state_rows.append(
            {
                "nodule_id": link_nid,
                "research_id": rid,
                "deterministic_key": r.get("deterministic_key"),
                "downstream_linkage_state": st,
                "note": note,
            }
        )
    pd.DataFrame(link_state_rows).to_csv(OUT / "linkage_state_matrix.csv", index=False)

    # Unmatched source
    unmatched = match_df[match_df["extraction_status"] == "missing"]
    unmatched.to_csv(OUT / "unmatched_source_nodules.csv", index=False)

    # Canonical without source (keys in DB not in union of source keys)
    src_keys = set(source_nodule_inventory["deterministic_key"].astype(str))
    orphan = img[~img["deterministic_key"].isin(src_keys)]
    orphan.to_csv(OUT / "canonical_without_source.csv", index=False)

    # Extracted patient-level
    n_ext_pat = len(ext_val) if ext_val is not None else 0

    con.close()

    ss_lines = "\n".join(
        f"| {r['source_system']} | {r['n_rows']} | {r['n_exact']} | {r['n_heuristic']} | {r['n_missing']} |"
        for r in ss_rows
    )
    comp_missing = next((r["n_missing"] for r in ss_rows if r["source_system"] == "COMPLETE_MULTI_SHEET"), None)

    # --- verdict.md ---
    n_src = len(source_nodule_inventory)
    n_exact = int((match_df["extraction_status"] == "exact").sum())
    n_heur = int((match_df["extraction_status"] == "heuristic").sum())
    n_miss = int((match_df["extraction_status"] == "missing").sum())
    n_explicit_src = source_nodule_inventory["tirads_reported"].notna().sum()
    ncrit = source_nodule_inventory["n_criteria_available"].fillna(0) if "n_criteria_available" in source_nodule_inventory.columns else pd.Series(0, index=source_nodule_inventory.index)
    trc = source_nodule_inventory["tirads_recalculated"].notna() if "tirads_recalculated" in source_nodule_inventory.columns else pd.Series(False, index=source_nodule_inventory.index)
    n_recomp_src = int(((ncrit >= 5) | trc).sum())
    canon_tr_mask = img["tirads_reported"].notna() | img["tirads_acr_recalculated"].notna()
    n_canon_tr = int(canon_tr_mask.sum()) if len(img) else 0
    n_miss_tirads = int(match_df["missing_canonical_tirads_despite_sufficient_source"].sum())
    n_prov_exact = int((match_df["provenance_status"] == "exact").sum())
    n_linked_fna = int((match_df["downstream_linkage_state"] == "linked_to_fna").sum())
    n_no_fna = int((match_df["downstream_linkage_state"] == "no_eligible_fna").sum())
    n_unres = int((match_df["downstream_linkage_state"] == "unresolved").sum())

    unmatched_empty = len(unmatched) == 0

    complete_ok = (
        unmatched_empty
        and (len(orphan) == 0)
        and (n_miss_tirads == 0)
    )

    md = f"""# US nodule / TI-RADS / linkage audit verdict

**Generated (UTC):** {datetime.now(timezone.utc).isoformat()}
**MotherDuck token:** `{token_mode()}` (secret not printed)
**Connection:** `{"local file" if args.local else "MotherDuck fail-closed"}`

## Claims (evidence in CSVs in this folder)

1. **Every source ultrasound nodule was extracted into canonical tables**  
   - Deterministic + heuristic match rates are in `nodule_match_matrix.csv`.  
   - `unmatched_source_nodules.csv` row count: **{len(unmatched)}** (must be 0 for “complete”).

2. **Every source nodule with enough detail received TI-RADS (explicit or ACR-recomputable)**  
   - Source-side explicit TR: **{int(n_explicit_src)}** rows with `tirads_reported` not null.  
   - Source-side recomputable (≥5 ACR fields or recalc present): see `source_nodule_inventory.csv`.  
   - Gaps: `missing_canonical_tirads_despite_sufficient_source` count: **{n_miss_tirads}**.

3. **Provenance + downstream linkage**  
   - `provenance_status` is exact/heuristic/missing per matched row.  
   - Downstream states: linked_to_fna **{n_linked_fna}**, no_eligible_fna **{n_no_fna}**, unresolved **{n_unres}** (source-aligned rows in match matrix).

## Per-source extraction vs `imaging_nodule_master_v1`

| source_system | n_rows | exact | heuristic | missing |
|---------------|-------:|------:|----------:|--------:|
{ss_lines}

**Interpretation:** `imaging_nodule_master_v1` is built from `raw_us_tirads_excel_v1` unpivot, then supplemented from `raw_us_tirads_scored_v1` and `Imaging_12_1_25.xlsx` (``utils/imaging_12_slots.py``) via ``scripts/50_multinodule_imaging.py`` (±30d dedup vs existing rows).  
- **COMPLETE_MULTI_SHEET** missing count **{comp_missing}** — if 0, claim (1) holds for the structured COMPLETE corpus.  
- Remaining gaps are usually dates beyond ±30d vs any canonical row with the same `research_id` + `nodule_number`, or Excel/audit parser drift.

## Recomputed source counts (this run)

| Metric | Value |
|--------|------:|
| COMPLETE workbook exam rows (All_Ultrasound_Reports) | {int(n_exams_complete)} |
| COMPLETE structured nodules (ingest rows) | {len(complete_prov)} |
| US Nodules TIRADS scored nodules | {len(scored_prov)} |
| Imaging_12_1_25 inferred nodule rows (exam slots / measurement splits) | {len(imaging12)} |
| Imaging_12 unique exam groups (rid+slot+date) | {int(n_exams_imaging12)} |
| **Total source_nodule_inventory rows** | **{n_src}** |
| serial_imaging_us rows (if queried) | {int(serial_us.iloc[0]["row_count"]) if serial_us is not None and len(serial_us) else "N/A"} |
| ultrasound_reports rows | {len(us_rep) if us_rep is not None else "N/A"} |
| raw_us_tirads_excel_v1 rows | {len(raw_excel) if raw_excel is not None else "N/A"} |
| imaging_nodule_master_v1 rows | {len(img)} |
| imaging_nodule_long_v2 rows | {len(img_long) if img_long is not None else "N/A"} |
| extracted_tirads_validated_v1 rows (patient-level) | {n_ext_pat} |
| imaging_fna_linkage_mm_v1 rows | {len(link_mm) if link_mm is not None else "N/A"} |

## Verdict counts (required)

| Metric | Count |
|--------|------:|
| Total source nodules | {n_src} |
| Exact extracted | {n_exact} |
| Heuristic extracted | {n_heur} |
| Missing | {n_miss} |
| Explicit TI-RADS in source (non-null tirads_reported) | {int(n_explicit_src)} |
| Recomputable TI-RADS from source (≥5 criteria or source recalc) | {n_recomp_src} |
| Canonical TI-RADS present (master: reported OR ACR not null) | {n_canon_tr} |
| Missing canonical TI-RADS despite sufficient source detail | {n_miss_tirads} |
| Nodules with exact provenance (match matrix) | {n_prov_exact} |
| Nodules with downstream linked_to_fna (match matrix) | {n_linked_fna} |
| Nodules with no_eligible_fna | {n_no_fna} |
| Nodules unresolved | {n_unres} |

## Overall completeness rule

- `unmatched_source_nodules.csv` empty: **{unmatched_empty}**
- `canonical_without_source.csv` rows: **{len(orphan)}** (must be 0 or documented duplicates)
- Missing TI-RADS despite sufficient source: **{n_miss_tirads}**

**Overall:** `{"COMPLETE" if complete_ok else "NOT_COMPLETE"}`

### If NOT_COMPLETE

See per-row notes in `nodule_match_matrix.csv` and unmatched/orphan CSVs. Imaging_12 rows are **inferred** from exam-slot text (measurement regex split); they are not duplicate COMPLETE rows.

## Methods

1. Parsed `COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx` (All_Ultrasound_Reports) with excel row index + nodule slot.  
2. Parsed `US Nodules TIRADS 12_1_25.xlsx` per sheet with row index.  
3. Parsed `Imaging_12_1_25.xlsx` per US-1..14 slot when date present; nodule rows inferred from measurement counts in nodule text (min 1).  
4. Canonical keys (same grain as `imaging_nodule_master_v1` / script 50): `research_id|YYYY-MM-DD|nodule_number` — **no** US report number in key.  
5. Heuristic: ±1 calendar day; then for `US_NODULES_TIRADS_SCORED` and `IMAGING_12_1_25` only, closest canonical row within **±30 days** with same `research_id` + `nodule_number`.  
6. Linkage: primary `imaging_fna_linkage_mm_v1`; pathology via `imaging_pathology_concordance_review_v2.nodule_id` when present; no FNA episodes ⇒ `no_eligible_fna`.

Per-source extraction parity: see `source_system_summary.csv`.

---
`run_us_nodule_tirads_linkage_audit.py`
"""
    (OUT / "verdict.md").write_text(md, encoding="utf-8")
    _log("done")
    print(f"Wrote artifacts to {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
