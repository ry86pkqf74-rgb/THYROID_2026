#!/usr/bin/env python3
"""
Offline lymph-node data audit for proposal2_ete_staging.

Quantifies coverage and logical checks using frozen repo exports (no local DuckDB required).
Optional: set LOCAL_DB_PATH and pass --md to append clinical_notes_long narrative sample
(imports duckdb + uses local DuckDB_client if available).

Outputs:
  studies/proposal2_ete_staging/outputs/pathology_ln_audit_summary.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.pathology_ln_narrative_extract import extract_pathology_ln_from_text

DEFAULT_MANUSCRIPT = (
    ROOT / "exports" / "FINAL_PUBLICATION_BUNDLE_20260313" / "manuscript_cohort_v1.csv"
)
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"


def _safe_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def audit_manuscript_cohort(path: Path) -> dict:
    df = pd.read_csv(path, low_memory=False)
    n = len(df)
    le = _safe_num(df.get("path_ln_examined_raw"))
    lp = _safe_num(df.get("path_ln_positive_raw"))
    lpf = _safe_num(df.get("ln_positive_final"))

    ajcc_n = df.get("ajcc8_n_stage")
    mask_n1 = (
        ajcc_n.fillna("")
        .astype(str)
        .str.upper()
        .str.match(r"N1[A-Z]?", na=False)
    )

    both = le.notna() & lp.notna()
    summary = {
        "artifact": str(path),
        "n_rows": n,
        "path_ln_examined_raw_nonnull": int(le.notna().sum()),
        "path_ln_examined_raw_pct": round(100 * le.notna().mean(), 2),
        "path_ln_positive_raw_nonnull": int(lp.notna().sum()),
        "path_ln_positive_raw_pct": round(100 * lp.notna().mean(), 2),
        "ln_positive_final_nonnull": int(lpf.notna().sum()),
        "ln_positive_final_pct": round(100 * lpf.notna().mean(), 2),
        "both_path_raw_nonnull": int(both.sum()),
        "ajcc8_n1_family_count": int(mask_n1.sum()),
        "n1_family_ln_positive_final_null_or_zero": int(
            (mask_n1 & (lpf.isna() | (lpf == 0))).sum()
        ),
        "positive_gt_examined_path_raw": int(
            (both & (lp > le)).sum()
        ),
        "examined_eq_zero_count": int((le == 0).sum()),
    }
    return summary


def sample_narrative_from_motherduck(limit: int) -> dict:
    token = os.environ.get("LOCAL_DB_PATH", "")
    if not token:
        return {"status": "skipped", "reason": "LOCAL_DB_PATH unset"}

    try:
        import duckdb
    except ImportError:
        return {"status": "skipped", "reason": "duckdb not installed"}

    try:
        con = duckdb.connect(f"thyroid_master.duckdb")
        con.execute("USE thyroid_master.duckdb")
    except Exception as e:
        return {"status": "error", "reason": str(e)}

    q = f"""
    SELECT note_row_id, research_id, note_type,
           LEFT(note_text, 8000) AS note_text
    FROM clinical_notes_long
    WHERE note_type ILIKE '%path%'
      AND note_text IS NOT NULL
      AND LENGTH(note_text) > 80
    LIMIT {int(limit)}
    """
    try:
        df = con.execute(q).fetchdf()
    except Exception as e:
        return {"status": "error", "reason": str(e)}

    parsed_pair = 0
    no_match = 0
    for text in df["note_text"].astype(str):
        r = extract_pathology_ln_from_text(text, source_type="clinical_notes_path")
        if r.ln_parse_status == "parsed_pair":
            parsed_pair += 1
        elif r.ln_parse_status == "no_match":
            no_match += 1

    return {
        "status": "ok",
        "notes_sampled": len(df),
        "narrative_parsed_pair": parsed_pair,
        "narrative_no_match": no_match,
        "note": "Sample only; not row-complete for all pathology reports.",
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Pathology LN audit (export-based).")
    ap.add_argument(
        "--cohort",
        type=Path,
        default=DEFAULT_MANUSCRIPT,
        help="manuscript_cohort_v1.csv path",
    )
    ap.add_argument("--md", action="store_true", help="Optional local DuckDB narrative sample")
    ap.add_argument("--md-limit", type=int, default=500)
    args = ap.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "manuscript_cohort_audit": audit_manuscript_cohort(args.cohort),
    }
    if args.md:
        out["motherduck_narrative_sample"] = sample_narrative_from_motherduck(args.md_limit)

    outp = OUTPUT_DIR / "pathology_ln_audit_summary.json"
    outp.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    print(f"\nWrote {outp}")


if __name__ == "__main__":
    main()
