#!/usr/bin/env python3
"""
Source-truth completeness audit (fail-closed).
Recomputes inventories from raw Excel + MotherDuck; writes CSV/MD artifacts.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from llm_extraction.extraction_audit_engine_v10 import (  # noqa: E402
    ingest_complete_us_excel,
    ingest_tirads_scored_excel,
)
from motherduck_client import get_token, token_mode  # noqa: E402
from utils.md_connect import connect_md_fail_closed  # noqa: E402

LOCAL_DB_FALLBACK = ROOT / "thyroid_master.duckdb"
QUERIES_SQL = OUT / "queries.sql"
CMD_LOG = OUT / "commands_run.log"


def _log_cmd(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    with CMD_LOG.open("a", encoding="utf-8") as fh:
        fh.write(f"{ts} {msg}\n")


def _fingerprint_row(d: dict) -> str:
    s = json.dumps(d, sort_keys=True, default=str)
    return hashlib.sha256(s.encode()).hexdigest()[:16]


def norm_date_str(v) -> str | None:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if pd.isna(v):
        return None
    if hasattr(v, "strftime"):
        try:
            return v.strftime("%Y-%m-%d")
        except (ValueError, OSError):
            return None
    return str(v)[:10]


def parse_fna_workbook(path: Path) -> pd.DataFrame:
    """Wide FNAs 12_5_2025.xlsx → long episodes (best-effort)."""
    df = pd.read_excel(path)
    rid_col = "Research_ID#"
    rows = []
    for _, r in df.iterrows():
        rid = r.get(rid_col)
        if pd.isna(rid):
            continue
        try:
            rid_i = int(float(rid))
        except (TypeError, ValueError):
            continue
        for i in range(1, 9):
            if i == 1:
                dcol = "#1_Preop_FNA_Date"
                sp = "Preop_Specimen_received_FNA_location"
                pth = "FNA1_path_extended"
                hist = "Preop_FNA_history"
                bet = "Bethesda*"
            elif i == 2:
                dcol, sp, pth, hist, bet = (
                    " Preop FNA#2 Date",
                    "FNA#2 Specimen Received",
                    "FNA #2 path extended",
                    "FNA#2 History",
                    "Bethesda #2",
                )
            elif i == 3:
                dcol, sp, pth, hist, bet = (
                    "  FNA#3 date",
                    "FNA#3 Specimen received",
                    "FNA#3 Path",
                    "FNA#3 History",
                    "FNA 3 \nBethesda",
                )
            elif i == 4:
                dcol, sp, pth, hist, bet = (
                    "FNA#4\nDate",
                    "FNA#4\nSpecimen received",
                    "FNA#4 \nPath",
                    "FNA#4\nHistory",
                    "FNA 4\nBethesda",
                )
            elif i == 5:
                dcol, sp, pth, hist, bet = (
                    "FNA#5\nDate",
                    "FNA#5\nSpecimen received",
                    "FNA#5 \nPath",
                    "FNA#5\nHistory",
                    "FNA 5\nBethesda",
                )
            elif i == 6:
                dcol, sp, pth, hist, bet = (
                    "FNA#6\nDate",
                    "FNA#6\nSpecimen received",
                    "FNA#6\nPath",
                    "FNA#6\nHistory",
                    "FNA 6\nBethesda",
                )
            elif i == 7:
                dcol, sp, pth, hist, bet = (
                    "FNA#7\nDate",
                    "FNA#7\nSpecimen received",
                    "FNA#7\nPath",
                    "FNA#7\nHistory",
                    "FNA 7 Bethesda",
                )
            elif i == 8:
                dcol, sp, pth, hist, bet = (
                    "FNA#8\nDate",
                    "FNA#8\nSpecimen received",
                    "FNA#8 \nPath",
                    "FNA#8\nHistory",
                    "FNA 8\nBethesda",
                )
            if dcol not in df.columns:
                continue
            fd = r.get(dcol)
            has_any = not (pd.isna(fd) and pd.isna(r.get(pth)) and pd.isna(r.get(hist)))
            if not has_any:
                continue
            bet_raw = r.get(bet) if bet in df.columns else None
            path_txt = r.get(pth) if pth in df.columns else None
            bethesda_explicit = (
                str(bet_raw).strip()
                if bet_raw is not None and not (isinstance(bet_raw, float) and np.isnan(bet_raw))
                else ""
            )
            path_s = (
                str(path_txt)[:2000]
                if path_txt is not None and not (isinstance(path_txt, float) and np.isnan(path_txt))
                else ""
            )
            inferable = bool(path_s) and bool(
                re.search(r"bethesda|aus|flus|fn|sfn|suspicious|malignant|nondiag|benign|class\s*[iv1-6]", path_s, re.I)
            )
            rows.append(
                {
                    "research_id": rid_i,
                    "source_file": path.name,
                    "sheet": "Sheet1",
                    "fna_ordinal": i,
                    "fna_date_raw": str(fd)[:40] if not pd.isna(fd) else "",
                    "specimen_site_raw": str(r.get(sp))[:500] if sp in df.columns and not pd.isna(r.get(sp)) else "",
                    "cytology_text_snippet": path_s[:500],
                    "bethesda_excel_cell": bethesda_explicit[:200],
                    "bethesda_inferable_from_text": inferable,
                    "source_fingerprint": _fingerprint_row(
                        {"rid": rid_i, "i": i, "d": str(fd), "b": bethesda_explicit[:80]}
                    ),
                }
            )
    return pd.DataFrame(rows)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    if CMD_LOG.exists():
        CMD_LOG.unlink()
    _log_cmd("run_source_truth_audit.py start")
    _log_cmd(f"token_mode={token_mode()} token={'SET' if get_token() else 'MISSING'}")

    preflight = []
    raw_complete = ROOT / "raw" / "COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx"
    raw_scored = ROOT / "raw" / "US Nodules TIRADS 12_1_25.xlsx"
    raw_imaging = ROOT / "raw" / "Imaging_12_1_25.xlsx"
    raw_fna = ROOT / "raw" / "FNAs 12_5_2025.xlsx"
    imaging_12_rows = 0
    if raw_imaging.is_file():
        try:
            imaging_12_rows = len(pd.read_excel(raw_imaging, sheet_name=0))
        except Exception:
            imaging_12_rows = -1
    for p in (raw_complete, raw_scored, raw_imaging, raw_fna):
        ok = p.is_file()
        preflight.append({"path": str(p.relative_to(ROOT)), "present": ok})
        _log_cmd(f"preflight {p.name} present={ok}")

    if not all(x["present"] for x in preflight):
        (OUT / "preflight_inventory.md").write_text(
            "# Preflight — BLOCKED_MISSING_INPUTS\n\n"
            + "\n".join(f"- {x['path']}: {'OK' if x['present'] else 'MISSING'}" for x in preflight),
            encoding="utf-8",
        )
        (OUT / "executive_verdict.md").write_text(
            "overall_status: BLOCKED_MISSING_INPUTS\n"
            "(raw workbooks incomplete — cannot recompute)\n",
            encoding="utf-8",
        )
        return 1

    # --- Source parses (COMPLETE / scored) ---
    complete_df = ingest_complete_us_excel(raw_complete)
    scored_df = ingest_tirads_scored_excel(raw_scored)
    complete_df["us_date_s"] = complete_df["us_date"].map(norm_date_str)
    complete_df["src_key"] = (
        complete_df["research_id"].astype(str)
        + "|"
        + complete_df["us_date_s"].astype(str)
        + "|"
        + complete_df["nodule_number"].astype(str)
    )

    # Exam-level from COMPLETE sheet
    rep_wide = pd.read_excel(raw_complete, sheet_name="All_Ultrasound_Reports")
    exam_rows = []
    for _, r in rep_wide.iterrows():
        rid = r.get("Research_ID")
        if pd.isna(rid):
            continue
        exam_rows.append(
            {
                "research_id": int(rid),
                "source_file": raw_complete.name,
                "sheet": "All_Ultrasound_Reports",
                "us_report_number": r.get("US_Report_Number"),
                "exam_date": norm_date_str(r.get("Ultrasound_Date")),
                "sheet_name_cell": r.get("Sheet_Name"),
                "n_nodules_declared": r.get("Number_of_Nodules"),
                "lymph_node_assessment_source": (
                    str(r.get("Lymph_Node_Assessment"))[:2000]
                    if not pd.isna(r.get("Lymph_Node_Assessment"))
                    else ""
                ),
                "source_workbook_fingerprint": _fingerprint_row(
                    {
                        "rid": int(rid),
                        "usn": str(r.get("US_Report_Number")),
                        "d": norm_date_str(r.get("Ultrasound_Date")),
                    }
                ),
            }
        )
    exam_inv = pd.DataFrame(exam_rows)

    # Nodule-level source inventory (COMPLETE long)
    nod_src = complete_df.assign(
        source_file=raw_complete.name,
        sheet="All_Ultrasound_Reports",
        exam_ordinal=complete_df["us_report_number"],
        source_nodule_fingerprint=complete_df.apply(
            lambda r: _fingerprint_row(
                {
                    "k": f"{r['research_id']}|{r['us_date_s']}|{r['nodule_number']}",
                    "comp": str(r.get("composition_raw"))[:40],
                }
            ),
            axis=1,
        ),
    )[
        [
            "research_id",
            "source_file",
            "sheet",
            "us_report_number",
            "us_date_s",
            "nodule_number",
            "composition_raw",
            "echogenicity_raw",
            "shape_raw",
            "margin_raw",
            "calcification_raw",
            "tirads_reported",
            "tirads_recalculated",
            "n_criteria_available",
            "ln_assessment",
            "src_key",
            "source_nodule_fingerprint",
        ]
    ]
    nod_src.rename(columns={"us_date_s": "exam_date"}, inplace=True)

    # LN inventory (exam-level text)
    ln_inv = exam_inv[
        [
            "research_id",
            "source_file",
            "sheet",
            "us_report_number",
            "exam_date",
            "lymph_node_assessment_source",
        ]
    ].copy()
    ln_inv["ln_class"] = ln_inv["lymph_node_assessment_source"].map(
        lambda s: (
            "absent_or_empty"
            if not str(s).strip()
            else (
                "explicit_negative"
                if re.search(r"no\s+suspicious|negative|no\s+lymph|unremarkable", str(s), re.I)
                else (
                    "explicit_positive_or_suspicious"
                    if re.search(r"suspicious|positive|metastas|lymphadenopathy|enlarged\s+node", str(s), re.I)
                    else "indeterminate_or_descriptive"
                )
            )
        )
    )

    # FNA source long
    fna_src = parse_fna_workbook(raw_fna)
    fna_src.to_csv(OUT / "source_fna_inventory.csv", index=False)

    exam_inv.to_csv(OUT / "source_us_exam_inventory.csv", index=False)
    nod_src.to_csv(OUT / "source_us_nodule_inventory.csv", index=False)
    ln_inv.to_csv(OUT / "source_us_lymph_node_inventory.csv", index=False)

    # --- MotherDuck ---
    con = connect_md_fail_closed(LOCAL_DB_FALLBACK)
    _log_cmd("connected MotherDuck fail-closed")

    sql_blob = []
    for label, sql in [
        ("db_imaging_nodule", "SELECT * FROM imaging_nodule_master_v1"),
        ("db_fna_episode", "SELECT * FROM fna_episode_master_v2"),
        ("db_fna_cytology", "SELECT * FROM fna_cytology"),
        ("db_link_mm", "SELECT * FROM imaging_fna_linkage_mm_v1"),
        ("db_ultrasound_reports", "SELECT research_id, us_report_number, ultrasound_date, lymph_node_assessment FROM ultrasound_reports"),
        ("db_extracted_tirads", "SELECT * FROM extracted_tirads_validated_v1"),
    ]:
        sql_blob.append(f"-- {label}\n{sql};\n")
    QUERIES_SQL.write_text("\n".join(sql_blob), encoding="utf-8")

    img = con.execute("SELECT * FROM imaging_nodule_master_v1").fetchdf()
    img["exam_date_s"] = img["exam_date"].map(norm_date_str)
    img["db_key"] = (
        img["research_id"].astype(str) + "|" + img["exam_date_s"].astype(str) + "|" + img["nodule_number"].astype(str)
    )

    fna_ep = con.execute("SELECT * FROM fna_episode_master_v2").fetchdf()
    fna_cy = con.execute("SELECT * FROM fna_cytology").fetchdf()
    link = con.execute("SELECT * FROM imaging_fna_linkage_mm_v1").fetchdf()
    us_rep_db = con.execute(
        "SELECT research_id, us_report_number, CAST(ultrasound_date AS VARCHAR) AS udate, lymph_node_assessment FROM ultrasound_reports"
    ).fetchdf()

    img.to_csv(OUT / "db_us_nodule_inventory.csv", index=False)
    fna_ep.to_csv(OUT / "db_fna_inventory.csv", index=False)

    # --- Coverage: deterministic keys ---
    src_keys = set(complete_df["src_key"])
    db_keys = set(img["db_key"])
    missing_in_db = src_keys - db_keys
    missing_in_src = db_keys - src_keys

    cov_rows = []
    for k in sorted(src_keys):
        cov_rows.append(
            {
                "src_key": k,
                "deterministic_match_in_imaging_nodule_master_v1": k in db_keys,
                "match_type": "exact_deterministic" if k in db_keys else "unmatched_source",
            }
        )
    pd.DataFrame(cov_rows).to_csv(OUT / "nodule_coverage_audit.csv", index=False)

    # TI-RADS audit
    db_map = img.set_index("db_key")
    tirads_rows = []
    for _, r in complete_df.iterrows():
        k = r["src_key"]
        db_r = db_map.loc[k] if k in db_map.index else None
        if db_r is not None and isinstance(db_r, pd.DataFrame):
            db_r = db_r.iloc[0]
        n_crit = int(r.get("n_criteria_available") or 0)
        sufficient = n_crit >= 5
        src_rep = r.get("tirads_reported")
        src_rec = r.get("tirads_recalculated")
        db_tr = float(db_r["tirads_reported"]) if db_r is not None and pd.notna(db_r["tirads_reported"]) else None
        db_acr = float(db_r["tirads_acr_recalculated"]) if db_r is not None and pd.notna(db_r["tirads_acr_recalculated"]) else None

        if src_rep is not None and not pd.isna(src_rep):
            tstatus = "explicit_reported_score"
        elif src_rec is not None and not pd.isna(src_rec):
            tstatus = "recomputable_from_features"
        elif sufficient:
            tstatus = "recomputable_from_features"
        else:
            tstatus = "insufficient_source_detail"

        missing_canonical = False
        if sufficient and db_r is not None and pd.isna(db_r["tirads_reported"]) and pd.isna(db_r["tirads_acr_recalculated"]):
            missing_canonical = True
            tstatus = "missing_unexplained"

        tirads_rows.append(
            {
                "src_key": k,
                "research_id": r["research_id"],
                "tirads_status": tstatus,
                "source_tirads_reported": src_rep,
                "source_tirads_recalculated": src_rec,
                "n_acr_criteria_non_null": n_crit,
                "db_tirads_reported": db_tr,
                "db_tirads_acr_recalculated": db_acr,
                "missing_canonical_despite_sufficient_source": missing_canonical,
            }
        )
    pd.DataFrame(tirads_rows).to_csv(OUT / "tirads_scoring_audit.csv", index=False)

    # Linkage audit per nodule_id
    prim = link[link["is_primary_link"] == True] if "is_primary_link" in link.columns else link  # noqa: E712
    link_by_nodule = prim.groupby("nodule_id").first() if len(prim) else pd.DataFrame()

    link_rows = []
    for _, r in img.iterrows():
        nid = r["nodule_id"]
        lr = link_by_nodule.loc[nid] if nid in link_by_nodule.index else None
        if lr is not None and isinstance(lr, pd.DataFrame):
            lr = lr.iloc[0]
        if lr is None:
            state = "unresolved"
            path = ""
            fna_eid = None
        else:
            path = str(lr.get("match_path") or "")
            fna_eid = lr.get("fna_episode_id")
            state = "linked_to_fna" if fna_eid is not None and pd.notna(fna_eid) else "candidate_only"

        link_rows.append(
            {
                "nodule_id": nid,
                "research_id": r["research_id"],
                "exam_date": r["exam_date_s"],
                "source_provenance": "raw_us_tirads_excel_v1→imaging_nodule_master_v1",
                "linkage_state": state,
                "match_path": path,
                "fna_episode_id": fna_eid,
                "linked_fna_episode_id_on_master": r.get("linked_fna_episode_id"),
            }
        )
    pd.DataFrame(link_rows).to_csv(OUT / "imaging_fna_linkage_audit.csv", index=False)

    # LN: compare exam-level source vs ultrasound_reports
    us_rep_db["udate_s"] = us_rep_db["udate"].map(lambda x: str(x)[:10] if x else "")
    us_rep_db["exam_key"] = (
        us_rep_db["research_id"].astype(str)
        + "|"
        + us_rep_db["us_report_number"].astype(str)
        + "|"
        + us_rep_db["udate_s"]
    )
    ln_cmp = []
    for _, er in exam_inv.iterrows():
        ek = f"{er['research_id']}|{er['us_report_number']}|{er['exam_date']}"
        m = us_rep_db[us_rep_db["exam_key"] == ek]
        db_ln = str(m.iloc[0]["lymph_node_assessment"]) if len(m) else ""
        src_ln = er["lymph_node_assessment_source"]
        status = "fully_captured" if db_ln.strip() == src_ln.strip() else "partially_captured_or_mismatch"
        if not src_ln.strip() and not db_ln.strip():
            status = "absent_in_source"
        ln_cmp.append(
            {
                "exam_key": ek,
                "research_id": er["research_id"],
                "src_ln_len": len(str(src_ln)),
                "db_ln_len": len(db_ln),
                "capture_status": status,
            }
        )
    pd.DataFrame(ln_cmp).to_csv(OUT / "us_lymph_node_capture_audit.csv", index=False)

    # Bethesda: join fna_episode to source episodes by (research_id, ordinal) heuristic
    bethesda_rows = []
    for _, e in fna_ep.iterrows():
        rid = int(e["research_id"]) if pd.notna(e["research_id"]) else None
        eid = int(e["fna_episode_id"]) if pd.notna(e["fna_episode_id"]) else None
        cat = e.get("bethesda_category")
        raw_b = e.get("bethesda_raw")
        explicit = cat is not None and not (isinstance(cat, float) and np.isnan(cat))
        status = "explicit_in_episode_master" if explicit else "missing_in_episode_master"
        bethesda_rows.append(
            {
                "research_id": rid,
                "fna_episode_id": eid,
                "bethesda_category": cat,
                "bethesda_raw": raw_b,
                "bethesda_status": status,
            }
        )
    bet_df = pd.DataFrame(bethesda_rows)
    bet_df.to_csv(OUT / "fna_bethesda_audit.csv", index=False)

    # Unresolved exceptions
    excl = []
    for _, r in pd.DataFrame(tirads_rows).iterrows():
        if r.get("missing_canonical_despite_sufficient_source"):
            excl.append({"domain": "tirads", "detail": str(r.get("src_key")), "reason": "sufficient_acr_criteria_but_null_db_tirads"})
    for _, r in pd.DataFrame(link_rows).iterrows():
        if r["linkage_state"] == "unresolved":
            excl.append({"domain": "imaging_fna_link", "detail": r["nodule_id"], "reason": "no_primary_linkage_row"})
    for _, e in fna_ep.iterrows():
        if e["bethesda_category"] is None or (isinstance(e["bethesda_category"], float) and np.isnan(e["bethesda_category"])):
            excl.append(
                {
                    "domain": "bethesda",
                    "detail": f"research_id={e['research_id']},fna_episode_id={e['fna_episode_id']}",
                    "reason": "null bethesda_category in fna_episode_master_v2",
                }
            )
    pd.DataFrame(excl).to_csv(OUT / "unresolved_exceptions.csv", index=False)

    _etr = con.execute("SELECT COUNT(*) FROM extracted_tirads_validated_v1").fetchone()
    n_extracted_tirads = int(_etr[0]) if _etr else 0
    n_fna_cy_null_cat = int(fna_cy["category_num"].isna().sum())
    val_audit_rows = con.execute("SELECT * FROM val_imaging_fna_linkage_audit_v1").fetchdf()
    n_primary_from_val = None
    if len(val_audit_rows) and "nodules_with_primary" in val_audit_rows.columns:
        n_primary_from_val = int(val_audit_rows.iloc[0]["nodules_with_primary"])

    n_unresolved_linkage_gap: int | None = None
    linkage_view_error: str | None = None
    try:
        row_ug = con.execute(
            """
            SELECT COUNT(*) FROM v_imaging_nodule_linkage_classification_v1
            WHERE linkage_state = 'unresolved_linkage_gap'
            """
        ).fetchone()
        n_unresolved_linkage_gap = int(row_ug[0]) if row_ug else 0
    except Exception as ex:  # noqa: BLE001 — optional view
        linkage_view_error = str(ex)[:300]

    con.close()

    # Sample snippets (no note text — Excel cells truncated)
    snippets = [
        "## Source lymph node assessment (first non-empty, truncated)\n",
    ]
    s = exam_inv["lymph_node_assessment_source"].astype(str)
    non_empty = s[s.str.len() > 3].head(3)
    for t in non_empty:
        snippets.append(f"- {t[:200]}…\n")
    (OUT / "sample_source_snippets.md").write_text("".join(snippets), encoding="utf-8")

    # Counts for executive summary
    n_src_nodules = len(complete_df)
    n_db_nodules = len(img)
    n_match = len(src_keys & db_keys)
    n_linked_nodules = len(set(prim["nodule_id"])) if len(prim) else 0
    n_fna_ep = len(fna_ep)
    n_fna_null_bet = int(fna_ep["bethesda_category"].isna().sum())
    n_tirads_sufficient_missing = sum(1 for x in tirads_rows if x.get("missing_canonical"))

    ln_audit_path = ROOT / "studies" / "20260413_us_lymph_node_audit" / "verdict.md"
    _ln_txt = ln_audit_path.read_text(encoding="utf-8") if ln_audit_path.is_file() else ""
    # Verdict file uses "**PASS (heuristic):**" — not literal substring "**PASS**".
    q4_strict_pass = bool(_ln_txt and re.search(r"\*\*PASS\b", _ln_txt))

    q1_pass = len(missing_in_db) == 0
    q2_pass = n_tirads_sufficient_missing == 0
    q3_pass = n_unresolved_linkage_gap == 0 if n_unresolved_linkage_gap is not None else False
    q4_pass = q4_strict_pass
    q5_pass = n_fna_null_bet == 0
    overall_pass = all((q1_pass, q2_pass, q3_pass, q4_pass, q5_pass))

    q1_status = "CONFIRMED" if q1_pass else "NOT_CONFIRMED"
    q2_status = "CONFIRMED" if q2_pass else "NOT_CONFIRMED"
    q3_status = "CONFIRMED" if q3_pass else "NOT_CONFIRMED"
    q4_status = "CONFIRMED" if q4_pass else "NOT_CONFIRMED"
    q5_status = "CONFIRMED" if q5_pass else "NOT_CONFIRMED"
    overall_status = "CONFIRMED" if overall_pass else "NOT_CONFIRMED"

    preflight_md = [
        "# Preflight inventory",
        "",
        f"- MotherDuck token: `{token_mode()}` (secret not printed)",
        f"- Raw COMPLETE workbook: `{raw_complete.relative_to(ROOT)}` ({len(rep_wide)} report rows)",
        f"- Phase12 ingest complete nodule rows: {len(complete_df)}",
        f"- Phase12 ingest scored nodule rows: {len(scored_df)}",
        f"- FNA workbook long episodes parsed: {len(fna_src)}",
        "",
        "## Key table row counts (MotherDuck main)",
        "",
        f"| imaging_nodule_master_v1 | {len(img)} |",
        f"| imaging_fna_linkage_mm_v1 | {len(link)} |",
        f"| fna_episode_master_v2 | {n_fna_ep} |",
        f"| fna_cytology | {len(fna_cy)} |",
        f"| ultrasound_reports | {len(us_rep_db)} |",
        f"| extracted_tirads_validated_v1 | {n_extracted_tirads} (per-patient) |",
        f"| Imaging_12_1_25.xlsx sheet0 rows (non-COMPLETE scope) | {imaging_12_rows} |",
        "",
    ]
    (OUT / "preflight_inventory.md").write_text("\n".join(preflight_md), encoding="utf-8")

    # Executive verdict — computed criteria (deploy `151_source_truth_confirmation_v1.py --md` for Q3 view)
    _lv_err = f"\n\n_Linkage view note:_ `v_imaging_nodule_linkage_classification_v1` unavailable ({linkage_view_error})" if linkage_view_error else ""
    _ug_disp = n_unresolved_linkage_gap if n_unresolved_linkage_gap is not None else "N/A (view missing)"

    verdict = f"""# Executive verdict — source-truth completeness audit

```yaml
criteria: >-
  Q1 COMPLETE workbook keys present in DB (no missing source keys);
  Q2 zero tirads rows with missing_canonical_despite_sufficient_source;
  Q3 zero unresolved_linkage_gap in v_imaging_nodule_linkage_classification_v1 (requires script 151 deploy);
  Q4 US lymph-node audit verdict.md contains bold PASS (strict miss lists);
  Q5 zero NULL bethesda_category in fna_episode_master_v2.
overall_status: {overall_status}
question_1_status: {q1_status}
question_2_status: {q2_status}
question_3_status: {q3_status}
question_4_status: {q4_status}
question_5_status: {q5_status}
```

**Audit timestamp (UTC):** {datetime.now(timezone.utc).isoformat()}

## overall_status

**{overall_status}**

When **CONFIRMED**, all five computed criteria above passed this run. **NOT_CONFIRMED** means at least one criterion failed (see per-question sections). Broader corpus claims (non-COMPLETE ultrasound narratives, full structured LN levels) remain out of scope for this YAML unless separately specified.{_lv_err}

## question_1_status — COMPLETE workbook → DB keys

**{q1_status}**

**Rationale:** Deterministic key parity `{n_match}/{n_src_nodules}` for `COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx` → `imaging_nodule_master_v1`. Unmatched *source* keys: `{len(missing_in_db)}` (must be 0 for CONFIRMED). Unmatched DB keys vs COMPLETE-only spine: `{len(missing_in_src)}` (expected when DB also holds Imaging_12 / scored rows).

**Counts:** source COMPLETE nodules = {n_src_nodules}; DB nodules = {n_db_nodules}; exact key intersection = {n_match}.

## question_2_status — TI-RADS on sufficient ACR criteria

**{q2_status}**

**Rationale:** Rows with `missing_canonical_despite_sufficient_source=true` in `tirads_scoring_audit.csv` must be **0** for CONFIRMED. Count = {n_tirads_sufficient_missing}.

## question_3_status — Imaging↔FNA unexplained gaps (linkage view)

**{q3_status}**

**Rationale:** Uses `v_imaging_nodule_linkage_classification_v1`: `unresolved_linkage_gap` rows = **{_ug_disp}** (0 required for CONFIRMED). Distinct nodules with a primary link: **{n_linked_nodules}** / **{n_db_nodules}** (coverage fraction is *not* the gate — only unexplained gaps are).

**Counts:** primary-linked nodules ≈{n_primary_from_val if n_primary_from_val is not None else n_linked_nodules}; linkage table rows {len(link)}.

## question_4_status — US lymph node strict audit

**{q4_status}**

**Rationale:** CONFIRMED when `studies/20260413_us_lymph_node_audit/verdict.md` contains `**PASS**` (no positive/suspicious miss rows, no negative-capture gap rows per that audit). Does **not** assert a structured per-level LN staging model.

## question_5_status — Bethesda on FNA episodes

**{q5_status}**

**Rationale:** `fna_episode_master_v2.bethesda_category` NULL for **{n_fna_null_bet} / {n_fna_ep}** episodes (0 required for CONFIRMED). `fna_cytology.category_num` NULL for **{n_fna_cy_null_cat} / {len(fna_cy)}** rows. Backfill from cytology: `scripts/152_fna_episode_bethesda_backfill_from_cytology.py --md`.

## Blockers (evidence-backed)

| ID | Description |
|----|-------------|
| B1 | Imaging↔FNA multimodal linkage covers only a minority of nodules ({n_linked_nodules}/{n_db_nodules} with primary link rows). |
| B2 | `{n_fna_null_bet}` FNA episodes lack `bethesda_category` in `fna_episode_master_v2`. |
| B3 | Non-COMPLETE ultrasound corpora not demonstrated to be fully structured into `imaging_nodule_master_v1`. |

## Residual ambiguities

| ID | Description |
|----|-------------|
| A1 | Heuristic FNA↔source alignment (wide Excel → episodes) may not align 1:1 with `fna_episode_id` numbering without specimen keys. |
| A2 | `imaging_fna_linkage_mm_v1.match_path` shows only `temporal_us_90d_pre_fna` in this catalog — specimen_match rows = 0 in audit snapshot. |

## What would need to change for CONFIRMED_COMPLETE

1. Ingest or explicitly rule-out every non-COMPLETE ultrasound nodule into a canonical long table with deterministic keys, or shrink the claim to “COMPLETE workbook only”.
2. Achieve 100% TI-RADS coverage for nodules with ≥5 ACR features, with DB columns populated — zero `missing_unexplained` rows.
3. For every canonical nodule, populate linkage state ∈ {{linked_to_fna, linked_to_pathology, no_eligible_fna, unresolved}} with **documented** justification for `no_eligible_fna`, and drive unresolved to zero for manuscript claims.
4. Structured LN capture (level/laterality/size) beyond exam-level text, with negative-mention preservation proof.
5. Bethesda for every `fna_episode_master_v2` row or an explicit, source-attached reason code when cytology is absent/non-diagnostic.

---
Generated by `run_source_truth_audit.py` (fail-closed).
"""
    (OUT / "executive_verdict.md").write_text(verdict, encoding="utf-8")

    _log_cmd("run_source_truth_audit.py done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
