"""
Direct-promote structured LN columns from TumorPath to a long-form LN fact table.

TumorPath (FINAL_UPDATE_TumorPath_12_8_CLEANED.xlsx) already contains ~92
pre-structured LN / histology columns — no LLM needed. This is ground-truth
data that closes the LN-acquisition gap for ~3,986 patients.

Outputs two parquets:
  processed/structured/ln_summary_tumorpath.parquet   (wide, one row per RID/tumor)
  processed/structured/ln_levels_tumorpath.parquet    (long, one row per RID × LN level)

Wide summary columns (per RID):
  - primary_ln_total_examined, primary_ln_total_positive, primary_ln_any_positive
  - primary_ln_central_positive, primary_ln_lateral_positive
  - primary_ln_largest_deposit_cm, primary_ln_extranodal_extension
  - primary_ln_levels_examined, primary_ln_levels_involved
  - ln_mets_{ptc, ptc_variant, ftc, hurthle, mtc, atc, pdtc, micrometastasis,
             extranodal_extension, cystic}
  - histology_{1..5}_ln_examined, histology_{1..5}_ln_positive,
    histology_{1..5}_ln_any_positive, histology_{1..5}_ln_largest_deposit_cm,
    histology_{1..5}_ln_extranodal_extension,
    histology_{1..5}_ln_central_positive, histology_{1..5}_ln_lateral_positive

Long per-level table columns:
  - research_id, surgery_date, ln_level (I..VII, unspecified),
    ln_region (central, lateral_left, lateral_right, bilateral_lateral, other),
    n_examined, n_positive, source_workbook, source='tumorpath_structured'

Usage:
    python scripts/promote_structured_ln_tumorpath.py
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pandas as pd

RAW = Path("raw/FINAL_UPDATE_TumorPath_12_8_CLEANED.xlsx")
OUT_DIR = Path("processed/structured")
OUT_WIDE = OUT_DIR / "ln_summary_tumorpath.parquet"
OUT_LONG = OUT_DIR / "ln_levels_tumorpath.parquet"

LEVELS = ["I", "II", "III", "IV", "V", "VI", "VII", "unspecified"]
REGIONS = ["central", "lateral_left", "lateral_right", "bilateral_lateral", "other"]


def _norm_rid(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none"}:
        return None
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except ValueError:
        pass
    return s


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_excel(RAW)
    print(f"Loaded {len(df):,} rows / {df['research_id'].nunique():,} unique RIDs")

    df["research_id"] = df["research_id"].apply(_norm_rid)
    df = df[df["research_id"].notna()].copy()

    # --- wide summary (one row per input row) ---
    wide_cols = ["research_id"]
    if "surgery_date" in df.columns:
        wide_cols.append("surgery_date")

    keep = [
        "primary_ln_ln_total_examined", "primary_ln_ln_total_positive",
        "primary_ln_ln_any_positive", "primary_ln_ln_central_positive",
        "primary_ln_ln_lateral_positive", "primary_ln_ln_largest_deposit_cm",
        "primary_ln_ln_extranodal_extension", "primary_ln_ln_levels_examined",
        "primary_ln_ln_levels_involved", "primary_ln_ln_ratio",
        "primary_ln_ln_location_detail", "primary_ln_ln_comment",
        "ln_locations_parsed_count", "ln_total_examined_from_locations",
        "ln_total_positive_from_locations", "ln_total_levels_involved",
        "ln_total_locations_parsed", "ln_histology_source",
    ]
    keep += [c for c in df.columns if c.startswith("ln_mets_")]
    keep += [c for c in df.columns if c.startswith("histology_") and "_ln_" in c]
    keep = [c for c in keep if c in df.columns]
    wide_cols += keep
    wide = df[wide_cols].copy()
    wide.to_parquet(OUT_WIDE, index=False)
    print(f"wrote wide  -> {OUT_WIDE}  ({len(wide):,} rows, {len(wide.columns)} cols)")

    # --- long per-level table ---
    long_rows = []
    surg_col = "surgery_date" if "surgery_date" in df.columns else None
    for _, row in df.iterrows():
        rid = row["research_id"]
        sdate = str(row[surg_col]) if surg_col and pd.notna(row[surg_col]) else ""
        for lvl in LEVELS:
            ex_col = f"ln_level_{lvl}_examined"
            po_col = f"ln_level_{lvl}_positive"
            if ex_col in df.columns or po_col in df.columns:
                ex = row.get(ex_col) if ex_col in df.columns else None
                po = row.get(po_col) if po_col in df.columns else None
                if pd.notna(ex) or pd.notna(po):
                    long_rows.append({
                        "research_id": rid,
                        "surgery_date": sdate,
                        "grouping": "level",
                        "ln_key": f"level_{lvl}",
                        "n_examined": ex,
                        "n_positive": po,
                        "source": "tumorpath_structured",
                    })
        for reg in REGIONS:
            ex_col = f"ln_region_{reg}_examined"
            po_col = f"ln_region_{reg}_positive"
            if ex_col in df.columns or po_col in df.columns:
                ex = row.get(ex_col) if ex_col in df.columns else None
                po = row.get(po_col) if po_col in df.columns else None
                if pd.notna(ex) or pd.notna(po):
                    long_rows.append({
                        "research_id": rid,
                        "surgery_date": sdate,
                        "grouping": "region",
                        "ln_key": f"region_{reg}",
                        "n_examined": ex,
                        "n_positive": po,
                        "source": "tumorpath_structured",
                    })

    long_df = pd.DataFrame(long_rows)
    long_df.to_parquet(OUT_LONG, index=False)
    print(f"wrote long  -> {OUT_LONG}  ({len(long_df):,} rows, "
          f"{long_df['research_id'].nunique():,} RIDs)")

    # summary
    print(f"\npromoted at {_dt.datetime.utcnow().isoformat()}Z")
    by_group = long_df.groupby("grouping")["ln_key"].value_counts().head(30)
    print(by_group.to_string())


if __name__ == "__main__":
    main()
