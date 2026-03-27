#!/usr/bin/env python3
"""
Reconcile lymph-node columns between the canonical synoptic Excel workbook
and MotherDuck `path_synoptics` (Excel = source-of-truth snapshot in `raw/`).

Usage:
  MOTHERDUCK_TOKEN=... .venv/bin/python studies/proposal2_ete_staging/run_excel_vs_motherduck_ln_reconcile.py
  MOTHERDUCK_TOKEN=... .venv/bin/python .../run_excel_vs_motherduck_ln_reconcile.py --env prod

Optional local DuckDB (no cloud):
  .venv/bin/python .../run_excel_vs_motherduck_ln_reconcile.py --local thyroid_master_local.duckdb

Outputs (studies/proposal2_ete_staging/audit_excel_vs_md_ln/):
  - excel_md_ln_summary.json
  - excel_md_ln_discordant.csv
  - excel_md_ln_unmatched_excel.csv
  - excel_md_ln_unmatched_md.csv
  - excel_md_ln_ambiguous_keys.csv

Also writes: studies/proposal2_ete_staging/EXCEL_VS_MOTHERDUCK_LN_RECONCILE.md
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from motherduck_client import get_token, resolve_database_for_env  # noqa: E402
from utils.text_helpers import clean_research_id, standardize_columns  # noqa: E402

STUDY_DIR = Path(__file__).resolve().parent
OUT_DIR = STUDY_DIR / "audit_excel_vs_md_ln"
REPORT_MD = STUDY_DIR / "EXCEL_VS_MOTHERDUCK_LN_RECONCILE.md"

DEFAULT_XLSX = ROOT / "raw" / "All Diagnoses & synoptic 12_1_2025.xlsx"
DEFAULT_SHEET = "synoptics + Dx merged"

LN_EXAMINED = "tumor_1_ln_examined"
LN_INVOLVED = "tumor_1_ln_involved"


def _connect(*, local: str | None, use_sa: bool):
    import duckdb

    if local:
        p = Path(local).expanduser()
        if not p.is_file():
            raise FileNotFoundError(f"Local DuckDB not found: {p}")
        os.environ["USE_LOCAL_DUCKDB"] = "true"
        return duckdb.connect(str(p)), f"local:{p}"

    for k in ("USE_LOCAL_DUCKDB", "use_local_duckdb"):
        os.environ.pop(k, None)
    token = get_token(prefer_service_account=use_sa)
    if not token:
        raise RuntimeError(
            "No MotherDuck token. Set MOTHERDUCK_TOKEN or MD_SA_TOKEN, or use --local."
        )
    db = resolve_database_for_env(os.getenv("MOTHERDUCK_ENV", "prod"))
    uri = f"md:{db}?motherduck_token={token}"
    return duckdb.connect(uri), f"md:{db}"


def _sql_clean_expr(col: str) -> str:
    """Mirror run_motherduck_ln_completeness_audit specimen cleaning (VARCHAR path)."""
    return f"""TRY_CAST(
        REPLACE(REPLACE(TRIM(CAST({col} AS VARCHAR)), ';', ''), 'x', '') AS DOUBLE
    )"""


def _load_excel(path: Path, sheet: str) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Excel not found: {path}")
    df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
    df = standardize_columns(df)
    if "research_id" not in df.columns:
        raise RuntimeError("Excel missing research_id after standardize_columns")
    df = clean_research_id(df)
    need = [LN_EXAMINED, LN_INVOLVED, "surg_date"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise RuntimeError(f"Excel missing columns: {missing}")
    df["excel_row_order"] = np.arange(1, len(df) + 1, dtype=np.int64)
    return df


def _surgery_key(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce", utc=False)
    d = dt.dt.normalize()

    def _to_key(x: Any) -> date | None:
        if pd.isna(x):
            return None
        if isinstance(x, pd.Timestamp):
            return x.date()
        if hasattr(x, "date"):
            return x.date()
        return None

    return d.map(_to_key)


def _py_clean_ln(series: pd.Series) -> pd.Series:
    """Match DuckDB: trim varchar, strip ';', strip 'x'/'X', then float."""

    def one(v: Any) -> float | None:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        s = str(v).strip()
        if s == "" or s.lower() in ("nan", "none"):
            return None
        # Mirror DuckDB specimen audit: REPLACE(..., 'x', '') — lowercase x only.
        s = s.replace(";", "").replace("x", "")
        s = s.strip()
        if s == "":
            return None
        try:
            return float(s)
        except ValueError:
            return None

    return series.map(one)


def _collapse_duplicate_keys(
    df: pd.DataFrame,
    key_cols: list[str],
    label: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """One row per key; flag internal disagreements on LN columns."""

    rows = []
    amb = []
    for key, sub in df.groupby(key_cols, dropna=False):
        if sub.shape[0] == 1:
            r = sub.iloc[0].to_dict()
            r["internal_ln_raw_disagree"] = False
            r["internal_ln_clean_disagree"] = False
            r["n_rows"] = 1
            rows.append(r)
            continue
        e_u = sub[f"{LN_EXAMINED}_v"].astype(str).unique()
        i_u = sub[f"{LN_INVOLVED}_v"].astype(str).unique()
        c1_u = pd.Series(sub[f"{LN_EXAMINED}_clean"].dropna().unique())
        c2_u = pd.Series(sub[f"{LN_INVOLVED}_clean"].dropna().unique())
        raw_d = len(e_u) > 1 or len(i_u) > 1
        cl_d = len(c1_u) > 1 or len(c2_u) > 1
        r0 = sub.iloc[0].to_dict()
        r0["internal_ln_raw_disagree"] = bool(raw_d)
        r0["internal_ln_clean_disagree"] = bool(cl_d)
        r0["n_rows"] = int(sub.shape[0])
        rows.append(r0)
        if raw_d or cl_d:
            amb.append(
                {
                    "source": label,
                    "research_id": key[0] if isinstance(key, tuple) else key,
                    "surgery_date_key": key[1] if isinstance(key, tuple) and len(key) > 1 else None,
                    "n_rows": int(sub.shape[0]),
                    "internal_ln_raw_disagree": bool(raw_d),
                    "internal_ln_clean_disagree": bool(cl_d),
                }
            )

    out = pd.DataFrame(rows)
    amb_df = (
        pd.DataFrame(amb)
        if amb
        else pd.DataFrame(
            columns=[
                "source",
                "research_id",
                "surgery_date_key",
                "n_rows",
                "internal_ln_raw_disagree",
                "internal_ln_clean_disagree",
            ]
        )
    )
    return out, amb_df


def main() -> int:
    ap = argparse.ArgumentParser(description="Excel vs MotherDuck LN reconcile")
    ap.add_argument("--excel", type=Path, default=DEFAULT_XLSX)
    ap.add_argument("--sheet", default=DEFAULT_SHEET)
    ap.add_argument("--local", type=str, default=None, help="Local DuckDB path (skip MotherDuck)")
    ap.add_argument("--sa", action="store_true", help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()

    ex = _load_excel(args.excel, args.sheet)
    ex["surgery_date_key"] = _surgery_key(ex["surg_date"])
    ex[f"{LN_EXAMINED}_v"] = ex[LN_EXAMINED]
    ex[f"{LN_INVOLVED}_v"] = ex[LN_INVOLVED]
    ex[f"{LN_EXAMINED}_clean"] = _py_clean_ln(ex[LN_EXAMINED])
    ex[f"{LN_INVOLVED}_clean"] = _py_clean_ln(ex[LN_INVOLVED])

    key_cols = ["research_id", "surgery_date_key"]
    ex_one, ex_amb = _collapse_duplicate_keys(ex, key_cols, "excel")

    con, conn_label = _connect(local=args.local, use_sa=args.sa)

    exam_sql = _sql_clean_expr("tumor_1_ln_examined")
    inv_sql = _sql_clean_expr("tumor_1_ln_involved")
    md_sql = f"""
    SELECT
      research_id,
      surg_date,
      {LN_EXAMINED} AS {LN_EXAMINED}_v,
      {LN_INVOLVED} AS {LN_INVOLVED}_v,
      {exam_sql} AS {LN_EXAMINED}_clean_sql,
      {inv_sql} AS {LN_INVOLVED}_clean_sql
    FROM path_synoptics
    WHERE research_id IS NOT NULL
    """
    md = con.execute(md_sql).df()
    md["surgery_date_key"] = _surgery_key(md["surg_date"])
    md[f"{LN_EXAMINED}_clean"] = md[f"{LN_EXAMINED}_clean_sql"]
    md[f"{LN_INVOLVED}_clean"] = md[f"{LN_INVOLVED}_clean_sql"]

    md_one, md_amb = _collapse_duplicate_keys(md, key_cols, "motherduck")

    merged = ex_one.merge(
        md_one,
        on=key_cols,
        how="outer",
        suffixes=("_excel", "_md"),
        indicator=True,
    )

    unmatched_ex = merged[merged["_merge"] == "left_only"].copy()
    unmatched_md = merged[merged["_merge"] == "right_only"].copy()
    both = merged[merged["_merge"] == "both"].copy()

    cex_e, cex_m = f"{LN_EXAMINED}_clean_excel", f"{LN_EXAMINED}_clean_md"
    cin_e, cin_m = f"{LN_INVOLVED}_clean_excel", f"{LN_INVOLVED}_clean_md"
    for c in (cex_e, cex_m, cin_e, cin_m):
        if c not in both.columns:
            raise RuntimeError(f"Expected merged column missing: {c}; have: {sorted(both.columns)[:40]}...")

    both = both.assign(
        _ex_exam_clean=pd.to_numeric(both[cex_e], errors="coerce"),
        _ex_inv_clean=pd.to_numeric(both[cin_e], errors="coerce"),
        _md_exam_clean=pd.to_numeric(both[cex_m], errors="coerce"),
        _md_inv_clean=pd.to_numeric(both[cin_m], errors="coerce"),
    )

    tol = 1e-6
    disc = both[
        (both["_ex_exam_clean"].sub(both["_md_exam_clean"]).abs() > tol)
        | (both["_ex_inv_clean"].sub(both["_md_inv_clean"]).abs() > tol)
        | (
            both["_ex_exam_clean"].isna() != both["_md_exam_clean"].isna()
        )
        | (
            both["_ex_inv_clean"].isna() != both["_md_inv_clean"].isna()
        )
    ].copy()

    summary: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "excel_path": str(args.excel.resolve()),
        "sheet": args.sheet,
        "connection": conn_label,
        "excel_rows": int(len(ex)),
        "excel_unique_keys": int(ex_one.shape[0]),
        "md_rows_raw": int(len(md)),
        "md_unique_keys": int(md_one.shape[0]),
        "matched_keys": int(len(both)),
        "unmatched_excel_keys": int(len(unmatched_ex)),
        "unmatched_md_keys": int(len(unmatched_md)),
        "discordant_clean_ln_keys": int(len(disc)),
        "excel_internal_ambiguous_keys": int(ex_amb.shape[0]),
        "md_internal_ambiguous_keys": int(md_amb.shape[0]),
        "runtime_seconds": round(time.perf_counter() - t0, 4),
    }

    unmatched_ex[[c for c in unmatched_ex.columns if c != "_merge"]].to_csv(
        OUT_DIR / "excel_md_ln_unmatched_excel.csv", index=False
    )
    unmatched_md[[c for c in unmatched_md.columns if c != "_merge"]].to_csv(
        OUT_DIR / "excel_md_ln_unmatched_md.csv", index=False
    )
    disc_out = disc.drop(columns=["_merge"], errors="ignore")
    disc_out.to_csv(OUT_DIR / "excel_md_ln_discordant.csv", index=False)
    amb_all = pd.concat([ex_amb, md_amb], ignore_index=True)
    if not amb_all.empty:
        amb_all.to_csv(OUT_DIR / "excel_md_ln_ambiguous_keys.csv", index=False)

    verdict = (
        "PASS"
        if summary["discordant_clean_ln_keys"] == 0
        and summary["unmatched_excel_keys"] == 0
        and summary["unmatched_md_keys"] == 0
        and summary["excel_internal_ambiguous_keys"] == 0
        and summary["md_internal_ambiguous_keys"] == 0
        else "FAIL"
    )
    summary["verdict"] = verdict

    (OUT_DIR / "excel_md_ln_summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )

    lines = [
        "# Excel vs MotherDuck — lymph node reconciliation",
        "",
        f"- **Generated (UTC)**: {summary['generated_at_utc']}",
        f"- **Excel**: `{summary['excel_path']}` sheet `{args.sheet}`",
        f"- **Database**: `{summary['connection']}`",
        f"- **Verdict**: **{verdict}** (cleaned `tumor_1_ln_examined` / `tumor_1_ln_involved` vs SQL-cleaned `path_synoptics`)",
        "",
        "## Counts",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Excel rows | {summary['excel_rows']} |",
        f"| Excel unique (research_id, surgery_date) | {summary['excel_unique_keys']} |",
        f"| MotherDuck path_synoptics rows | {summary['md_rows_raw']} |",
        f"| MD unique (research_id, surgery_date) | {summary['md_unique_keys']} |",
        f"| Matched keys | {summary['matched_keys']} |",
        f"| Unmatched keys (Excel only) | {summary['unmatched_excel_keys']} |",
        f"| Unmatched keys (MD only) | {summary['unmatched_md_keys']} |",
        f"| Discordant cleaned LN (matched keys) | {summary['discordant_clean_ln_keys']} |",
        f"| Excel duplicate-key internal ambiguity | {summary['excel_internal_ambiguous_keys']} |",
        f"| MD duplicate-key internal ambiguity | {summary['md_internal_ambiguous_keys']} |",
        "",
        "## Outputs",
        "",
        "- Summary JSON: `audit_excel_vs_md_ln/excel_md_ln_summary.json`",
        "- Discordant rows: `audit_excel_vs_md_ln/excel_md_ln_discordant.csv`",
        "- Unmatched Excel keys: `audit_excel_vs_md_ln/excel_md_ln_unmatched_excel.csv`",
        "- Unmatched MD keys: `audit_excel_vs_md_ln/excel_md_ln_unmatched_md.csv`",
        "",
        "## Cleaning rule",
        "",
        "Cleaned values mirror the specimen audit SQL on varchar: `TRIM`, remove `;`, "
        "remove literal `x` placeholders, then `TRY_CAST`/`float` (see "
        "`run_motherduck_ln_completeness_audit.py` `_ln_specimen`).",
        "",
        "## Method",
        "",
        "Rows are matched on `(research_id, surgery_date)` after `pandas.to_datetime(..., errors='coerce').normalize()`. "
        "Duplicate keys on a side are collapsed to a single row; if duplicate rows disagree on LN fields, "
        "they are listed in `excel_md_ln_ambiguous_keys.csv`.",
        "",
    ]
    REPORT_MD.write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps(summary, indent=2, default=str))
    return 0 if verdict == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
