"""
Imaging_12_1_25.xlsx — inferred per-nodule rows from exam-slot text.

Same logic as ``studies/20260413_us_nodule_tirads_linkage_audit`` so deterministic keys
(``research_id|exam_date|nodule_number``) match the audit and ``script 50`` supplements.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd


def stable_key(rid: int, d: str | None, nod: int) -> str:
    """Same as audit: ``research_id|YYYY-MM-DD|nodule_number`` (empty segment if no date)."""
    # DuckDB NULL dates → pandas NaT; map(norm_date_str) may yield float nan — must not
    # use ``d or ''`` alone (np.nan is truthy and stringifies to "nan").
    if d is None or pd.isna(d):
        ds = ""
    else:
        ds = str(d).strip()
        if not ds or ds.lower() == "nan":
            ds = ""
    return f"{rid}|{ds}|{nod}"


def norm_date_str(v) -> str | None:
    """Normalize to ISO ``YYYY-MM-DD`` for stable keys (matches DuckDB DATE + script 50)."""
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
    if not s or s.lower() == "nan":
        return None
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def _excel_row_ix(ix: object) -> int:
    if isinstance(ix, (int, np.integer)):
        return int(ix)
    return int(str(ix))


def _imaging12_slot_columns(df: pd.DataFrame, k: int) -> tuple[str | None, str | None]:
    date_c = None
    nod_c = None
    for c in df.columns:
        cs = str(c)
        if re.search(rf"US[_\s-]*{k}[_\s-]*Date", cs, re.I):
            date_c = c
        if re.search(rf"US[_\s-]*{k}.*nodule", cs, re.I):
            nod_c = c
    return date_c, nod_c


def parse_imaging_12_exam_slots(path: Path) -> pd.DataFrame:
    """Imaging_12_1_25: one aggregate row per (patient, US slot) with a date."""
    df = pd.read_excel(str(path), sheet_name=0)
    rid_col = "Research ID number"
    if rid_col not in df.columns:
        return pd.DataFrame()
    rows = []
    for row_ix, r in df.iterrows():
        rxi = _excel_row_ix(row_ix)
        rid = r.get(rid_col)
        if pd.isna(rid):
            continue
        rid = int(rid)
        for k in range(1, 15):
            dc, nc = _imaging12_slot_columns(df, k)
            if not dc:
                continue
            raw_d = r.get(dc)
            if pd.isna(raw_d) or raw_d is None:
                continue
            dnorm = norm_date_str(raw_d)
            nod_txt = str(r.get(nc))[:2000] if nc and not pd.isna(r.get(nc)) else ""
            n_sub = max(
                1,
                len(re.findall(r"\b\d+\.?\d*\s*(?:cm|mm)\b", nod_txt, flags=re.I)),
            )
            for sub in range(1, n_sub + 1):
                src_uid = hashlib.sha256(
                    f"IMAGING12|{path.name}|row{rxi}|US{k}|sub{sub}|{rid}".encode()
                ).hexdigest()[:20]
                rows.append(
                    {
                        "source_system": "IMAGING_12_1_25",
                        "source_workbook": path.name,
                        "source_sheet": "Sheet1",
                        "excel_row_index": rxi,
                        "source_cell_region": f"row {rxi + 2} / US-{k} / inferred nodule {sub} of {n_sub}",
                        "source_nodule_uid": src_uid,
                        "research_id": rid,
                        "us_report_number": k,
                        "exam_date_norm": dnorm,
                        "nodule_number": sub,
                        "tirads_reported": None,
                        "tirads_recalculated": None,
                        "n_criteria_available": 0,
                        "aggregate_exam_text_excerpt": nod_txt[:300],
                        "inferred_nodule_split_from_measurements": n_sub > 1,
                        "deterministic_key": stable_key(rid, dnorm, sub),
                    }
                )
    return pd.DataFrame(rows)


def imaging_12_supplement_for_master_from_parsed(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Dated + undated slim frames for ``imaging_nodule_master_v1`` (one parse pass).

    Rows with unparseable US dates still appear in the audit with keys ``rid||nod``;
    the undated frame carries ``exam_date`` as NaT so script 50 can INSERT NULL dates.
    """
    empty_cols = ["research_id", "exam_date", "nodule_number", "location_raw"]
    if df.empty:
        return (
            pd.DataFrame(columns=empty_cols),
            pd.DataFrame(columns=empty_cols),
        )

    def _loc_cell(x: object) -> str | None:
        if x is None:
            return None
        if isinstance(x, float) and np.isnan(x):
            return None
        try:
            if bool(pd.isna(cast(Any, x))):
                return None
        except (ValueError, TypeError):
            pass
        return str(x)[:500]

    locs = [_loc_cell(v) for v in df["aggregate_exam_text_excerpt"]]
    ed = pd.to_datetime(df["exam_date_norm"], errors="coerce")
    exam_dates: list[object] = []
    for ts in ed:
        if pd.isna(ts):
            exam_dates.append(None)
        else:
            exam_dates.append(ts.date())
    out = pd.DataFrame(
        {
            "research_id": df["research_id"].astype(int),
            "exam_date": exam_dates,
            "nodule_number": df["nodule_number"].astype(int),
            "location_raw": locs,
        }
    )
    mask = np.array([x is not None for x in exam_dates], dtype=bool)
    dated = out.loc[mask].copy()
    undated = out.loc[~mask].copy()
    return dated, undated


def imaging_12_supplement_for_master(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convenience: parse file then build dated + undated slim frames."""
    return imaging_12_supplement_for_master_from_parsed(parse_imaging_12_exam_slots(path))
