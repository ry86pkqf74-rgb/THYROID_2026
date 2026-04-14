"""
Build targeted extraction queues for TI-RADS, cervical LN, and FNA/Bethesda reruns.

Reads the four source workbooks in raw/ and emits three research_id lists:
  - queues/ids_tirads.txt   - research_ids where at least one nodule is described
                              but its TR is blank / Not_Scored / mismatched
  - queues/ids_ln.txt       - research_ids with any non-empty cervical LN US text
  - queues/ids_fna.txt      - research_ids with null category_num, validator/parser
                              errors, or FNA episodes present in the wide workbook
                              but absent from the rescored long-format file

Also writes queues/queue_audit.json with counts and breakdowns so we can track
before/after recovery.

Usage:
    python scripts/build_extraction_queues.py
    python scripts/build_extraction_queues.py --raw-dir raw --out-dir queues
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd

# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────
RID_CANDIDATES = [
    "research_id",
    "Research ID number",
    "Research_ID",
    "Research_ID#",
    "Research_ID #",
]


def _rid_col(df: pd.DataFrame) -> str:
    for c in RID_CANDIDATES:
        if c in df.columns:
            return c
    # fallback: anything containing "research" and "id"
    for c in df.columns:
        if "research" in str(c).lower() and "id" in str(c).lower():
            return c
    raise KeyError(f"no research_id column found in {list(df.columns)[:8]}")


def _nonempty(v) -> bool:
    if v is None:
        return False
    if isinstance(v, float) and pd.isna(v):
        return False
    s = str(v).strip()
    return s != "" and s.lower() not in {"nan", "none", "null"}


def _norm_rid(v) -> str | None:
    """Normalize research_id so int 8, float 8.0, string '8', 'R_0008' all match."""
    if not _nonempty(v):
        return None
    s = str(v).strip()
    # drop trailing ".0" from float stringification
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    # Try to cast numeric strings to int form
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except ValueError:
        pass
    return s


def _tr_unscored(v) -> bool:
    """True if TR value is blank, 'not_scored', or otherwise not a TR1..TR5."""
    if not _nonempty(v):
        return True
    s = str(v).strip().lower().replace(" ", "_")
    if "not_scored" in s or s in {"unscored", "pending"}:
        return True
    # Accept TR1..TR5, tr 1, 1, 2, 3, 4, 5
    if re.search(r"\btr\s*[1-5]\b", s):
        return False
    if re.fullmatch(r"[1-5]", s):
        return False
    return True


# ──────────────────────────────────────────────────────────────────────────
# Queue builders
# ──────────────────────────────────────────────────────────────────────────
def build_tirads_queue(raw: Path) -> tuple[set[str], dict]:
    """From US_NODULES + COMPLETE_US, find research_ids with unscored nodule text."""
    stats: dict = {"sources": {}}
    rids: set[str] = set()

    # Source A: US Nodules TIRADS 12_1_25.xlsx — 14 per-US sheets with N1..N14 + Nn TR
    path = raw / "US Nodules TIRADS 12_1_25.xlsx"
    if path.exists():
        xls = pd.ExcelFile(path)
        per_sheet: dict[str, int] = {}
        sheet_rids: set[str] = set()
        for sn in xls.sheet_names:
            df = pd.read_excel(path, sheet_name=sn)
            if df.empty:
                continue
            rid_col = _rid_col(df)
            # find (nodule, tr) column pairs (column name starts with nodule/N\d
            # and is followed by an 'N<i> TR' column)
            cols = list(df.columns)
            pairs: list[tuple[str, str]] = []
            for i, c in enumerate(cols):
                cl = str(c).strip().lower()
                if re.fullmatch(r"(nodule\s*\d+|n\d+)", cl):
                    # look for TR column within next 2 positions
                    for j in (i + 1, i + 2):
                        if j < len(cols) and "tr" in str(cols[j]).strip().lower():
                            pairs.append((c, cols[j]))
                            break
            sheet_hit = 0
            for _, row in df.iterrows():
                rid = _norm_rid(row.get(rid_col))
                if not rid:
                    continue
                for nod_col, tr_col in pairs:
                    if _nonempty(row.get(nod_col)) and _tr_unscored(row.get(tr_col)):
                        sheet_rids.add(rid)
                        sheet_hit += 1
                        break
            per_sheet[sn] = sheet_hit
        stats["sources"]["US_Nodules_TIRADS"] = {
            "rids_with_unscored_nodule": len(sheet_rids),
            "per_sheet_hits": per_sheet,
        }
        rids |= sheet_rids

    # Source B: COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx
    path = raw / "COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx"
    if path.exists():
        df = pd.read_excel(path, sheet_name="All_Ultrasound_Reports")
        rid_col = _rid_col(df)
        src_rids: set[str] = set()
        hit_rows = 0
        for _, row in df.iterrows():
            rid = _norm_rid(row.get(rid_col))
            if not rid:
                continue
            for i in range(1, 15):
                desc = row.get(f"Nodule_{i}_Source_Description")
                tirads = row.get(f"Nodule_{i}_TI_RADS")
                loc = row.get(f"Nodule_{i}_Location")
                comp = row.get(f"Nodule_{i}_Composition")
                described = any(_nonempty(x) for x in (desc, loc, comp))
                if described and _tr_unscored(tirads):
                    src_rids.add(rid)
                    hit_rows += 1
                    break
        stats["sources"]["COMPLETE_MULTI_SHEET_ULTRASOUND"] = {
            "rids_with_unscored_nodule": len(src_rids),
            "rows_flagged": hit_rows,
        }
        rids |= src_rids

    stats["final_unique_rids"] = len(rids)
    return rids, stats


def build_ln_queue(raw: Path) -> tuple[set[str], dict]:
    """Two sources:
    - LN US sheet with dedicated per-episode LN text
    - Thyroid US sheet free text mentioning lymph/level/cervical (the bulk signal)
    """
    import re as _re

    stats: dict = {"sources": {}}
    rids: set[str] = set()

    path = raw / "Imaging_12_1_25.xlsx"
    if path.exists():
        df = pd.read_excel(path, sheet_name="LN US")
        rid_col = _rid_col(df)
        ln_cols = [c for c in df.columns if str(c).lower().startswith("ln_us")]
        ln_us_rids: set[str] = set()
        for _, row in df.iterrows():
            rid = _norm_rid(row.get(rid_col))
            if not rid:
                continue
            if any(_nonempty(row.get(c)) for c in ln_cols):
                ln_us_rids.add(rid)
        stats["sources"]["Imaging_LN_US"] = {
            "rids_with_any_ln_text": len(ln_us_rids),
            "ln_columns": ln_cols,
        }
        rids |= ln_us_rids

        # Thyroid US free-text mention of LN/cervical/level
        tus = pd.read_excel(path, sheet_name="Thyroid US")
        t_rid = _rid_col(tus)
        text_cols = [
            c for c in tus.columns
            if any(k in str(c).lower() for k in ["finding", "nodule_detail", "impression"])
        ]
        combined = tus[text_cols].astype(str).agg(" ".join, axis=1).str.lower()
        pattern = _re.compile(r"\blymph|\blevel\s*[ivx]+\b|\bcervical\s+node|\bll?n\b", _re.IGNORECASE)
        mask = combined.apply(lambda s: bool(pattern.search(s)))
        tus_rids = set(
            _norm_rid(v) for v in tus.loc[mask, t_rid].dropna().tolist()
        ) - {None}
        stats["sources"]["Thyroid_US_freetext_LN"] = {
            "rids_with_ln_mention": len(tus_rids),
            "text_cols_scanned": len(text_cols),
        }
        rids |= tus_rids

    stats["final_unique_rids"] = len(rids)
    return rids, stats


def build_fna_queue(raw: Path) -> tuple[set[str], dict]:
    stats: dict = {"sources": {}}
    rids: set[str] = set()

    # Source A: long-format rescored — null category_num or non-empty error
    long_path = raw / "FNAs_Rescored_Long_Format.xlsx"
    long_df: pd.DataFrame | None = None
    if long_path.exists():
        long_df = pd.read_excel(long_path)
        null_cat = long_df["category_num"].isna()
        err = long_df["error"].apply(_nonempty) if "error" in long_df.columns else pd.Series([False] * len(long_df))
        flagged = long_df[null_cat | err]
        long_rids = {
            r for r in (_norm_rid(v) for v in flagged["research_id"].dropna().tolist()) if r
        }
        stats["sources"]["FNA_long_format"] = {
            "total_rows": len(long_df),
            "null_category_num": int(null_cat.sum()),
            "non_empty_error": int(err.sum()),
            "flagged_rids": len(long_rids),
        }
        rids |= long_rids

    # Source B: FNA wide workbook — episodes that actually have a populated FNA
    # but are missing from long-format.
    wide_path = raw / "FNAs 12_5_2025.xlsx"
    if wide_path.exists() and long_df is not None:
        wide_df = pd.read_excel(wide_path)
        rid_col = _rid_col(wide_df)
        # detect FNA episode columns (date or path populated)
        fna_date_cols = [c for c in wide_df.columns if "FNA" in str(c) and "Date" in str(c)]
        fna_path_cols = [c for c in wide_df.columns if "FNA" in str(c) and "path" in str(c).lower()]
        # RIDs with at least one populated FNA in the wide workbook
        populated_mask = pd.Series(False, index=wide_df.index)
        for c in fna_date_cols + fna_path_cols:
            populated_mask |= wide_df[c].apply(_nonempty)
        wide_active = wide_df.loc[populated_mask, rid_col].dropna().tolist()
        wide_rids = {r for r in (_norm_rid(v) for v in wide_active) if r}
        long_rids_all = {
            r for r in (_norm_rid(v) for v in long_df["research_id"].dropna().tolist()) if r
        }
        missing = wide_rids - long_rids_all
        stats["sources"]["FNA_wide_missing_from_long"] = {
            "wide_rids_with_populated_fna": len(wide_rids),
            "long_unique_rids": len(long_rids_all),
            "missing_in_long": len(missing),
        }
        rids |= missing

    stats["final_unique_rids"] = len(rids)
    return rids, stats


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", default="raw")
    ap.add_argument("--out-dir", default="queues")
    args = ap.parse_args()

    raw = Path(args.raw_dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    audit: dict = {}

    tirads_rids, tirads_stats = build_tirads_queue(raw)
    (out / "ids_tirads.txt").write_text("\n".join(sorted(tirads_rids)) + "\n")
    audit["tirads"] = tirads_stats

    ln_rids, ln_stats = build_ln_queue(raw)
    (out / "ids_ln.txt").write_text("\n".join(sorted(ln_rids)) + "\n")
    audit["ln"] = ln_stats

    fna_rids, fna_stats = build_fna_queue(raw)
    (out / "ids_fna.txt").write_text("\n".join(sorted(fna_rids)) + "\n")
    audit["fna"] = fna_stats

    audit["summary"] = {
        "ids_tirads": len(tirads_rids),
        "ids_ln": len(ln_rids),
        "ids_fna": len(fna_rids),
        "union_all": len(tirads_rids | ln_rids | fna_rids),
    }
    (out / "queue_audit.json").write_text(json.dumps(audit, indent=2, default=str))

    print(json.dumps(audit["summary"], indent=2))
    print(f"\nQueue files written to: {out.resolve()}")


if __name__ == "__main__":
    main()
