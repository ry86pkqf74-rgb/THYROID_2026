#!/usr/bin/env python3
"""
Ultrasound lymph-node extraction completeness audit.

Reads source narratives from MotherDuck raw tables (+ optional local Excel if present),
compares to ultrasound_reports.lymph_node_assessment and related text columns.

Run:
  .venv/bin/python studies/20260413_us_lymph_node_audit/run_us_lymph_node_audit.py

Uses motherduck_client.get_token() via utils.md_connect (motherduck.local.toml ok).
Optional: --local  use thyroid_master.duckdb instead of MotherDuck.
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_or_file  # noqa: E402

DATESTAMP = "20260413"
CMD_LOG = OUT / "commands_run.log"


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    with CMD_LOG.open("a", encoding="utf-8") as fh:
        fh.write(f"{ts} {msg}\n")


# --- LN text classification (deterministic regex) --------------------------------

_LN_ANY = re.compile(
    r"(lymph\s*node|lymphadenopathy|adenopath|cervical\s+node|neck\s+node|"
    r"supraclavicular|paratracheal|jugular\s+chain|level\s*[ivx]{1,4}\b|"
    r"level\s+[1-6]\b|station\s+vi|central\s+neck|lateral\s+neck|"
    r"fatty\s+hilum|hilum|extranodal|e\.?n\.?e\.?|perinodal)",
    re.IGNORECASE,
)

_EXPLICIT_NEGATIVE = re.compile(
    r"(no\s+(suspicious|pathologic|abnormal|enlarged|malignant)\s+(lymph|cervical|neck\s+)?(nodes?|adenopath\w*)"
    r"|no\s+evidence\s+of\s+(metastatic\s+)?(lymph|adenopath)"
    r"|lymph\s+nodes?\s+(are\s+)?(normal|benign|unremarkable|negative|not\s+enlarged)"
    r"|no\s+lymphadenopathy"
    r"|without\s+lymphadenopathy"
    r"|negative\s+for\s+(cervical\s+)?(lymph|adenopath)"
    r"|no\s+metastatic\s+adenopathy"
    r"|unremarkable\s+(cervical\s+)?(lymph|nodes?)"
    r"|benign\s+(appearing\s+)?(cervical\s+)?lymph)",
    re.IGNORECASE,
)

_EXPLICIT_POSITIVE = re.compile(
    r"(suspicious\s+(for\s+malignancy\s+)?(lymph|cervical\s+node|adenopath)"
    r"|pathologic\s+(lymph|nodes?|adenopath)"
    r"|metastatic\s+(lymph|adenopath|disease\s+to\s+nodes?)"
    r"|abnormal\s+(cervical\s+)?(lymph|nodes?|adenopath)"
    r"|enlarged\s+(pathologic\s+)?(lymph|nodes?)"
    r"|biopsy(?:sed)?\s+(?:of\s+)?(?:the\s+)?(?:cervical\s+)?(?:lymph|nodes?)"
    r"|malignant\s+appearing\s+(lymph|nodes?)"
    r"|loss\s+of\s+(the\s+)?fatty\s+hilum"
    r"|rounded\s+(lymph\s+)?nodes?"
    r"|hypervascular(ity)?\s+(?:within\s+)?(?:the\s+)?(?:cervical\s+)?(?:lymph|nodes?)"
    r"|cystic\s+(?:change\s+in\s+)?(?:a\s+)?(?:cervical\s+)?(?:lymph|node)"
    r"|microcalcif\w*\s+(?:within\s+)?(?:a\s+)?(?:lymph|node)"
    r"|extranodal\s+extension|perinodal\s+spread"
    r"|fna\s+of\s+(?:the\s+)?(?:cervical\s+)?(?:lymph|nodes?))",
    re.IGNORECASE,
)

_DETAIL_TAGS = {
    "laterality": re.compile(
        r"\b(right|left|bilateral|unilateral|ipsilateral|contralateral)\b", re.I
    ),
    "neck_level": re.compile(
        r"(level\s*[ivx]{1,4}|level\s+[1-6]|station\s+vi|central\s+compartment|"
        r"lateral\s+neck|paratracheal|pretracheal|delphian|jugular)",
        re.I,
    ),
    "size_mm_cm": re.compile(
        r"\b(\d+(?:\.\d+)?)\s*(mm|cm)\b", re.I
    ),
    "cystic": re.compile(r"cystic", re.I),
    "calcification": re.compile(r"calcif|microcalcif|echogenic\s+foci", re.I),
    "hilum": re.compile(r"hilum|fatty\s+hilum", re.I),
    "rounded": re.compile(r"rounded|round\s+mor", re.I),
    "vascularity": re.compile(
        r"hypervascular|increased\s+vascularity|vascularity|color\s+doppler", re.I
    ),
    "ene": re.compile(r"extranodal|e\.?n\.?e\.?|perinodal", re.I),
}


def classify_ln_narrative(text: str | None) -> tuple[str, str]:
    """Return (ln_state, excerpt) where ln_state is one of four categories."""
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return "no_ln_content", ""
    s = str(text).strip()
    if not s or s.lower() in ("nan", "none"):
        return "no_ln_content", ""
    low = s.lower()
    if not _LN_ANY.search(low):
        return "no_ln_content", s[:500]

    neg_hit = _EXPLICIT_NEGATIVE.search(low)
    pos_hit = _EXPLICIT_POSITIVE.search(low)

    if pos_hit and neg_hit:
        # Prefer positive if both (e.g. "no nodules; suspicious LN" — take full context)
        sp, ep = pos_hit.span()
        sn, en = neg_hit.span()
        if sp < sn and ep < sn:
            return "explicit_positive_or_suspicious", s[:500]
        if sn < sp and en < sp:
            return "explicit_negative", s[:500]
        return "explicit_positive_or_suspicious", s[:500]

    if pos_hit:
        return "explicit_positive_or_suspicious", s[:500]
    if neg_hit:
        return "explicit_negative", s[:500]
    return "indeterminate_reference", s[:500]


def extract_detail_flags(text: str | None) -> str:
    if not text:
        return ""
    low = str(text).lower()
    hits = [k for k, pat in _DETAIL_TAGS.items() if pat.search(low)]
    return "|".join(hits)


def norm_date_iso(v) -> str | None:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if pd.isna(v):
        return None
    if hasattr(v, "strftime"):
        try:
            return v.strftime("%Y-%m-%d")
        except Exception:
            pass
    s = str(v).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def exam_key(rid: int, d: str | None, usn: int | None) -> str:
    return f"{rid}|{d or ''}|{usn if usn is not None else ''}"


def structured_ln_text(row: pd.Series) -> str:
    parts = []
    for c in (
        "lymph_node_assessment",
        "clinical_impression",
        "source_us_impression",
        "recommendation",
    ):
        if c in row.index and pd.notna(row[c]):
            parts.append(str(row[c]))
    return " ".join(parts).strip()


def structured_positive_ok(txt: str) -> bool:
    if not txt or len(txt.strip()) < 3:
        return False
    low = txt.lower()
    return bool(_EXPLICIT_POSITIVE.search(low)) or (
        "suspicious" in low and "lymph" in low
    )


def structured_negative_ok(txt: str) -> bool:
    if not txt or len(txt.strip()) < 3:
        return False
    low = txt.lower()
    return bool(_EXPLICIT_NEGATIVE.search(low)) or (
        "unremarkable" in low and "lymph" in low
    )


def load_complete_exams_from_raw(con) -> pd.DataFrame:
    """One row per exam from raw_us_tirads_excel_v1 (COMPLETE workbook)."""
    q = """
    SELECT research_id,
           TRY_CAST(us_date AS DATE) AS us_date,
           us_report_number,
           ANY_VALUE(ln_assessment) AS ln_assessment_src,
           ANY_VALUE(recommendation) AS recommendation_src,
           STRING_AGG(DISTINCT COALESCE(ln_assessment, ''), ' ||| ')
             FILTER (WHERE COALESCE(TRIM(CAST(ln_assessment AS VARCHAR)), '') <> '') AS ln_assessment_all,
           STRING_AGG(DISTINCT COALESCE(recommendation, ''), ' ||| ')
             FILTER (WHERE COALESCE(TRIM(CAST(recommendation AS VARCHAR)), '') <> '') AS recommendation_all
    FROM raw_us_tirads_excel_v1
    GROUP BY 1, 2, 3
    """
    return con.execute(q).fetchdf()


def load_scored_nodule_text(con) -> pd.DataFrame:
    q = """
    SELECT research_id,
           TRY_CAST(us_date AS DATE) AS us_date,
           us_report_number,
           STRING_AGG(DISTINCT COALESCE(nodule_description, ''), ' \n ')
             FILTER (WHERE COALESCE(TRIM(CAST(nodule_description AS VARCHAR)), '') <> '') AS nodule_text_agg
    FROM raw_us_tirads_scored_v1
    GROUP BY 1, 2, 3
    """
    return con.execute(q).fetchdf()


def load_imaging12_exams(con) -> pd.DataFrame:
    q = """
    SELECT research_id,
           exam_date_norm,
           us_report_number,
           MAX(LENGTH(COALESCE(aggregate_exam_text_excerpt,''))) AS _mx,
           STRING_AGG(DISTINCT COALESCE(aggregate_exam_text_excerpt, ''), ' \n ')
             FILTER (WHERE COALESCE(TRIM(aggregate_exam_text_excerpt), '') <> '') AS aggregate_exam_text_excerpt
    FROM raw_imaging_12_slots_v1
    GROUP BY 1, 2, 3
    """
    return con.execute(q).fetchdf()


def load_ultrasound_reports(con) -> pd.DataFrame:
    cols = [
        "research_id",
        "ultrasound_date",
        "us_report_number",
        "lymph_node_assessment",
        "clinical_impression",
        "source_us_impression",
        "recommendation",
    ]
    # tolerate missing columns
    have = {
        r[0]
        for r in con.execute(
            "SELECT DISTINCT column_name FROM information_schema.columns "
            "WHERE table_catalog = current_database() AND table_schema = 'main' "
            "AND table_name = 'ultrasound_reports'"
        ).fetchall()
    }
    use = [c for c in cols if c in have]
    return con.execute(f"SELECT {', '.join(use)} FROM ultrasound_reports").fetchdf()


def optional_excel_layers() -> list[dict]:
    """If raw Excel files exist, add rows (same schema as source_us_ln_inventory)."""
    rows: list[dict] = []
    raw_dir = ROOT / "raw"
    complete = raw_dir / "COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx"
    imaging12 = raw_dir / "Imaging_12_1_25.xlsx"
    if complete.is_file():
        df = pd.read_excel(str(complete), sheet_name="All_Ultrasound_Reports")
        for row_ix, report in df.iterrows():
            rid = report.get("Research_ID")
            if pd.isna(rid):
                continue
            rid = int(rid)
            us_date = norm_date_iso(report.get("Ultrasound_Date"))
            us_num = report.get("US_Report_Number")
            usn = int(us_num) if not pd.isna(us_num) else None
            ln_a = report.get("Lymph_Node_Assessment", "")
            rec = report.get("Recommendation", "")
            imp = report.get("Source_US_Impression", "")
            clin = report.get("Clinical_Impression", "") if "Clinical_Impression" in df.columns else ""
            blob = " ".join(
                str(x)
                for x in (ln_a, rec, imp, clin)
                if x is not None and not (isinstance(x, float) and np.isnan(x))
            )
            st, ex = classify_ln_narrative(blob)
            rows.append(
                {
                    "source_layer": "excel_file_COMPLETE_MULTI_SHEET",
                    "source_exam_uid": hashlib.sha256(
                        f"XLSX|COMPLETE|{row_ix}|{rid}|{us_date}|{usn}".encode()
                    ).hexdigest()[:16],
                    "research_id": rid,
                    "exam_date_norm": us_date,
                    "us_report_number": usn,
                    "ln_assessment_field": str(ln_a)[:4000] if ln_a is not None else "",
                    "recommendation_field": str(rec)[:4000] if rec is not None else "",
                    "scored_nodule_text_excerpt": "",
                    "combined_source_text": blob[:8000],
                    "ln_state_source": st,
                    "source_excerpt": ex[:2000],
                    "detail_flags": extract_detail_flags(blob),
                }
            )
    if imaging12.is_file():
        from utils.imaging_12_slots import parse_imaging_12_exam_slots

        slot_df = parse_imaging_12_exam_slots(imaging12)
        if len(slot_df):
            g = (
                slot_df.groupby(["research_id", "exam_date_norm", "us_report_number"], dropna=False)[
                    "aggregate_exam_text_excerpt"
                ]
                .apply(lambda s: " \n ".join(str(x) for x in s if str(x).strip()))
                .reset_index()
            )
            for _, r in g.iterrows():
                blob = str(r.get("aggregate_exam_text_excerpt", "") or "")
                st, ex = classify_ln_narrative(blob)
                rows.append(
                    {
                        "source_layer": "excel_file_IMAGING_12_1_25",
                        "source_exam_uid": hashlib.sha256(
                            f"XLSX|I12|{r['research_id']}|{r['exam_date_norm']}|{r['us_report_number']}".encode()
                        ).hexdigest()[:16],
                        "research_id": int(r["research_id"]),
                        "exam_date_norm": r.get("exam_date_norm"),
                        "us_report_number": int(r["us_report_number"])
                        if pd.notna(r["us_report_number"])
                        else None,
                        "ln_assessment_field": "",
                        "recommendation_field": "",
                        "scored_nodule_text_excerpt": "",
                        "combined_source_text": blob[:8000],
                        "ln_state_source": st,
                        "source_excerpt": ex[:2000],
                        "detail_flags": extract_detail_flags(blob),
                    }
                )
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true", help="Use thyroid_master.duckdb")
    args = ap.parse_args()
    _log("start us_lymph_node_audit")

    db_path = ROOT / "thyroid_master.duckdb"
    con = connect_md_or_file(db_path, md=not args.local, fail_closed=not args.local)

    # --- Build source inventory from DB layers
    complete = load_complete_exams_from_raw(con)
    scored = load_scored_nodule_text(con)
    img12 = load_imaging12_exams(con)
    us_rep = load_ultrasound_reports(con)
    con.close()

    # Merge complete + scored + imaging12 on keys
    complete["exam_date_norm"] = complete["us_date"].map(norm_date_iso)
    scored["exam_date_norm"] = scored["us_date"].map(norm_date_iso)

    src_rows: list[dict] = []

    for _, r in complete.iterrows():
        rid = int(r["research_id"])
        d = r["exam_date_norm"]
        usn = int(r["us_report_number"]) if pd.notna(r["us_report_number"]) else None
        ln_a = str(r.get("ln_assessment_all") or r.get("ln_assessment_src") or "")
        rec = str(r.get("recommendation_all") or r.get("recommendation_src") or "")
        # merge scored nodule text
        m = scored[
            (scored["research_id"] == rid)
            & (scored["exam_date_norm"] == d)
            & (scored["us_report_number"] == r["us_report_number"])
        ]
        nod_txt = str(m["nodule_text_agg"].iloc[0]) if len(m) else ""
        blob = " ".join(x for x in (ln_a, rec, nod_txt) if x)
        st, ex = classify_ln_narrative(blob)
        src_rows.append(
            {
                "source_layer": "raw_us_tirads_excel_v1+scored",
                "source_exam_uid": hashlib.sha256(
                    f"MD|COMPLETE|{rid}|{d}|{usn}".encode()
                ).hexdigest()[:20],
                "research_id": rid,
                "exam_date_norm": d,
                "us_report_number": usn,
                "ln_assessment_field": ln_a[:4000],
                "recommendation_field": rec[:4000],
                "scored_nodule_text_excerpt": nod_txt[:4000],
                "combined_source_text": blob[:8000],
                "ln_state_source": st,
                "source_excerpt": ex[:2000],
                "detail_flags": extract_detail_flags(blob),
            }
        )

    for _, r in img12.iterrows():
        rid = int(r["research_id"])
        d = r["exam_date_norm"]
        usn = int(r["us_report_number"]) if pd.notna(r["us_report_number"]) else None
        blob = str(r.get("aggregate_exam_text_excerpt") or "")
        st, ex = classify_ln_narrative(blob)
        src_rows.append(
            {
                "source_layer": "raw_imaging_12_slots_v1",
                "source_exam_uid": hashlib.sha256(
                    f"MD|I12|{rid}|{d}|{usn}".encode()
                ).hexdigest()[:20],
                "research_id": rid,
                "exam_date_norm": d,
                "us_report_number": usn,
                "ln_assessment_field": "",
                "recommendation_field": "",
                "scored_nodule_text_excerpt": "",
                "combined_source_text": blob[:8000],
                "ln_state_source": st,
                "source_excerpt": ex[:2000],
                "detail_flags": extract_detail_flags(blob),
            }
        )

    src_df = pd.DataFrame(src_rows)
    xlsx_extra = optional_excel_layers()
    if xlsx_extra:
        src_df = pd.concat([src_df, pd.DataFrame(xlsx_extra)], ignore_index=True)

    src_df.to_csv(OUT / "source_us_ln_inventory.csv", index=False)

    # --- Structured inventory from ultrasound_reports
    us_rep["exam_date_norm"] = us_rep["ultrasound_date"].map(norm_date_iso)
    struct_rows = []
    for _, r in us_rep.iterrows():
        rid = int(r["research_id"])
        d = r["exam_date_norm"]
        usn = int(r["us_report_number"]) if pd.notna(r["us_report_number"]) else None
        stxt = structured_ln_text(r)
        st_class, ex = classify_ln_narrative(stxt)
        struct_rows.append(
            {
                "research_id": rid,
                "exam_date_norm": d,
                "us_report_number": usn,
                "lymph_node_assessment": r.get("lymph_node_assessment"),
                "clinical_impression": r.get("clinical_impression"),
                "source_us_impression": r.get("source_us_impression"),
                "recommendation": r.get("recommendation"),
                "structured_combined_text": stxt[:8000],
                "ln_state_structured_combined": st_class,
                "structured_positive_ok": structured_positive_ok(stxt),
                "structured_negative_ok": structured_negative_ok(stxt),
            }
        )
    struct_df = pd.DataFrame(struct_rows)
    struct_df.to_csv(OUT / "structured_us_ln_inventory.csv", index=False)

    # --- Join source (MD layers) to structured on key (dict: last row wins if dupes)
    struct_lookup: dict[tuple[int, object, int | None], pd.Series] = {}
    for _, row in struct_df.iterrows():
        k = (
            int(row["research_id"]),
            row["exam_date_norm"],
            int(row["us_report_number"]) if pd.notna(row["us_report_number"]) else None,
        )
        struct_lookup[k] = row

    audit_rows = []
    positive_misses = []
    negative_gaps = []

    for _, s in src_df.iterrows():
        rid = int(s["research_id"])
        d = s["exam_date_norm"]
        usn = int(s["us_report_number"]) if pd.notna(s["us_report_number"]) else None
        key = (rid, d, usn)
        src_state = s["ln_state_source"]
        comb = str(s.get("combined_source_text") or "")

        match = struct_lookup.get(key)

        st_combined = ""
        s_pos = False
        s_neg = False
        if match is not None:
            st_combined = str(match.get("structured_combined_text") or "")
            s_pos = bool(match.get("structured_positive_ok"))
            s_neg = bool(match.get("structured_negative_ok"))

        # capture classification
        if src_state == "no_ln_content":
            cap = (
                "fully_captured"
                if (not st_combined.strip() or not _LN_ANY.search(st_combined.lower()))
                else "source_ambiguous"
            )
        elif src_state == "indeterminate_reference":
            cap = "text_only_not_structured" if not st_combined.strip() else "partially_captured"
        elif src_state == "explicit_positive_or_suspicious":
            if s_pos:
                cap = "fully_captured"
            elif st_combined.strip() and not s_pos:
                cap = "partially_captured"
            else:
                cap = "text_only_not_structured"
            if not s_pos:
                positive_misses.append(
                    {
                        "research_id": rid,
                        "exam_date_norm": d,
                        "us_report_number": usn,
                        "source_layer": s.get("source_layer"),
                        "ln_state_source": src_state,
                        "source_excerpt": s.get("source_excerpt"),
                        "combined_source_snippet": comb[:1200],
                        "structured_combined_snippet": st_combined[:1200],
                        "failure_reason": "positive_or_suspicious_not_in_structured_fields",
                    }
                )
        elif src_state == "explicit_negative":
            if s_neg:
                cap = "fully_captured"
            elif st_combined.strip():
                cap = "partially_captured"
            else:
                cap = "text_only_not_structured"
            if not s_neg:
                negative_gaps.append(
                    {
                        "research_id": rid,
                        "exam_date_norm": d,
                        "us_report_number": usn,
                        "source_layer": s.get("source_layer"),
                        "ln_state_source": src_state,
                        "source_excerpt": s.get("source_excerpt"),
                        "combined_source_snippet": comb[:1200],
                        "structured_combined_snippet": st_combined[:1200],
                        "failure_reason": "explicit_negative_not_preserved_in_structured",
                    }
                )
        else:
            cap = "source_ambiguous"

        audit_rows.append(
            {
                "research_id": rid,
                "exam_date_norm": d,
                "us_report_number": usn,
                "source_layer": s.get("source_layer"),
                "source_exam_uid": s.get("source_exam_uid"),
                "ln_state_source": src_state,
                "capture_class": cap,
                "detail_flags": s.get("detail_flags"),
                "structured_match_found": match is not None,
                "structured_combined_snippet": st_combined[:1500],
            }
        )

    audit_df = pd.DataFrame(audit_rows)
    audit_df.to_csv(OUT / "us_ln_capture_audit.csv", index=False)

    pd.DataFrame(positive_misses).to_csv(OUT / "positive_ln_misses.csv", index=False)
    pd.DataFrame(negative_gaps).to_csv(OUT / "negative_ln_capture_gaps.csv", index=False)

    # --- Verdict aggregates (source MD layers only for exam counts — dedupe)
    src_core = src_df[
        src_df["source_layer"].str.startswith("raw_")
        | src_df["source_layer"].str.startswith("excel_file")
    ].copy()
    if len(src_core) == 0:
        src_core = src_df.copy()
    rank = {
        "explicit_positive_or_suspicious": 4,
        "explicit_negative": 3,
        "indeterminate_reference": 2,
        "no_ln_content": 0,
    }
    src_core["ek"] = src_core.apply(
        lambda r: exam_key(int(r["research_id"]), r["exam_date_norm"], r["us_report_number"]),
        axis=1,
    )
    by_exam = (
        src_core.groupby("ek")["ln_state_source"]
        .agg(lambda s: max(s, key=lambda t: rank.get(t, -1)))
        .reset_index()
    )
    by_exam.columns = ["ek", "worst_ln_state"]

    n_any = int((by_exam["worst_ln_state"] != "no_ln_content").sum())
    n_neg = int((by_exam["worst_ln_state"] == "explicit_negative").sum())
    n_pos = int((by_exam["worst_ln_state"] == "explicit_positive_or_suspicious").sum())

    cap_counts = audit_df["capture_class"].value_counts().to_dict()
    verdict = f"""# Ultrasound lymph-node extraction audit — {DATESTAMP}

## Scope

- **Primary sources:** `raw_us_tirads_excel_v1` + `raw_us_tirads_scored_v1` (COMPLETE + scored workbooks ingested to MotherDuck), `raw_imaging_12_slots_v1` (Imaging_12_1_25.xlsx ingest).
- **Structured target:** `ultrasound_reports` (`lymph_node_assessment` plus `clinical_impression`, `source_us_impression`, `recommendation` combined for classification).
- **Not used as proof of US completeness:** `cervical_ln_detail` / pathology-linked NLP (per audit brief).
- **serial_imaging_us:** table not present on connected database — if your environment materializes it, re-run after ingest.
- **Local Excel:** optional pass-through when `raw/COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx` and/or `raw/Imaging_12_1_25.xlsx` exist (gitignored in many setups).

## Counts (deduped exams from source inventory layers)

| Metric | Count |
|--------|------:|
| Total US exams (rows in source inventory, all layers) | {len(src_df)} |
| Exams with **any** LN-related narrative (state ≠ `no_ln_content`, deduped by exam key) | {n_any} |
| Exams with **explicit negative** LN statements (best state per exam) | {n_neg} |
| Exams with **positive/suspicious** LN findings (best state per exam) | {n_pos} |
| **fully_captured** (row-level audit) | {cap_counts.get("fully_captured", 0)} |
| **partially_captured** | {cap_counts.get("partially_captured", 0)} |
| **text_only_not_structured** | {cap_counts.get("text_only_not_structured", 0)} |
| **absent_but_should_exist** | {cap_counts.get("absent_but_should_exist", 0)} |
| **source_ambiguous** | {cap_counts.get("source_ambiguous", 0)} |

**Note:** `source_ambiguous` rows are usually cross-source tension (e.g. Imaging_12 slot text has no LN keywords while `ultrasound_reports` narrative for the same key mentions lymph nodes) or empty Imaging_12 excerpts paired with richer structured rows. Count **physical lines ≠ row count** in CSVs when fields contain embedded newlines; use pandas `len(read_csv(...))` for exact row counts.

## Strict criteria result

- **Positive/suspicious misses (rows):** {len(positive_misses)} — see `positive_ln_misses.csv`
- **Negative preservation gaps (rows):** {len(negative_gaps)} — see `negative_ln_capture_gaps.csv`

## Miss lists (identifiers)

### Positive / suspicious not fully represented in structured fields
{chr(10).join(f"- research_id={m['research_id']} date={m['exam_date_norm']} us#={m['us_report_number']} layer={m.get('source_layer')}" for m in positive_misses) or "- (none)"}

### Explicit negative not preserved in structured combined text
{chr(10).join(f"- research_id={m['research_id']} date={m['exam_date_norm']} us#={m['us_report_number']} layer={m.get('source_layer')}" for m in negative_gaps) or "- (none)"}

## Verdict

"""
    if len(positive_misses) == 0 and len(negative_gaps) == 0:
        verdict += (
            "**PASS (heuristic):** No source-derived positive/suspicious LN statements lacked structured representation; "
            "no explicit-negative gap detected in `ultrasound_reports` combined text vs source layers.\n"
        )
    else:
        verdict += (
            "**FAIL / REVIEW:** One or more exams show source LN content not fully reflected in structured columns. "
            "Review CSVs for exact excerpts.\n"
        )
    verdict += (
        "\n---\n"
        f"Generated UTC: {datetime.now(timezone.utc).isoformat()}\n"
        f"Database: `{'local file' if args.local else 'MotherDuck (md:Thyroid 2026)'}`\n"
    )
    (OUT / "verdict.md").write_text(verdict, encoding="utf-8")
    _log("done")
    print(f"Wrote outputs to {OUT}")


if __name__ == "__main__":
    main()
