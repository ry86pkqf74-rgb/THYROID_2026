#!/usr/bin/env python3
"""
61_table1_preview.py -- Table 1 preview for manuscript

Generates a descriptive Table 1 stratified by analysis_eligible_flag.
Uses pandas only (no ThyroidStatisticalAnalyzer dependency).

Outputs:
  exports/manuscript_analysis/table1_preview.csv
  exports/manuscript_analysis/table1_preview.md
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
OUT_DIR = os.path.join("exports", "manuscript_analysis")


def get_connection(md: bool):
    import duckdb
    if md:
        token = os.environ.get("MOTHERDUCK_TOKEN") or ""
        if not token:
            try:
                import toml
                token = toml.load(".streamlit/secrets.toml").get("MOTHERDUCK_TOKEN", "")
            except Exception:
                pass
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(os.environ.get("LOCAL_DUCKDB_PATH", "thyroid_master.duckdb"))


def _resolve_table(con, preferred: str, fallbacks: list[str]) -> str:
    for tbl in [preferred] + fallbacks:
        try:
            con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
            return tbl
        except Exception:
            continue
    return preferred


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _continuous_row(series: pd.Series, label: str) -> dict:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return {"variable": label, "value": "no data", "n": 0}
    return {
        "variable": label,
        "value": f"{s.mean():.1f} +/- {s.std():.1f}",
        "n": len(s),
        "mean": round(float(s.mean()), 2),
        "sd": round(float(s.std()), 2),
        "median": round(float(s.median()), 2),
        "q25": round(float(s.quantile(0.25)), 2),
        "q75": round(float(s.quantile(0.75)), 2),
    }


def _median_iqr_row(series: pd.Series, label: str) -> dict:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return {"variable": label, "value": "no data", "n": 0}
    return {
        "variable": label,
        "value": f"{s.median():.1f} [{s.quantile(0.25):.1f}-{s.quantile(0.75):.1f}]",
        "n": len(s),
        "median": round(float(s.median()), 2),
        "q25": round(float(s.quantile(0.25)), 2),
        "q75": round(float(s.quantile(0.75)), 2),
    }


def _categorical_rows(series: pd.Series, label: str, total_n: int) -> list[dict]:
    vc = series.dropna().value_counts()
    rows = []
    for val, count in vc.items():
        pct = round(100.0 * count / total_n, 1) if total_n > 0 else 0
        rows.append({
            "variable": f"  {label}: {val}",
            "value": f"{count} ({pct}%)",
            "n": int(count),
            "pct": pct,
        })
    missing = int(series.isna().sum())
    if missing > 0:
        rows.append({
            "variable": f"  {label}: missing",
            "value": f"{missing} ({round(100*missing/total_n, 1)}%)",
            "n": missing,
            "pct": round(100 * missing / total_n, 1) if total_n > 0 else 0,
        })
    return rows


def _bool_rate_row(series: pd.Series, label: str, total_n: int) -> dict:
    try:
        n_true = int(series.apply(lambda x: x is True or str(x).lower() == "true").sum())
    except Exception:
        n_true = 0
    pct = round(100.0 * n_true / total_n, 1) if total_n > 0 else 0
    return {"variable": label, "value": f"{n_true} ({pct}%)", "n": n_true, "pct": pct}


def _build_table1(df: pd.DataFrame, stratum: str) -> list[dict]:
    N = len(df)
    rows: list[dict] = [{"variable": "N", "value": str(N), "n": N}]

    age_col = _find_col(df, ["demo_age_final", "demo_age_raw", "age_at_diagnosis", "age"])
    if age_col:
        rows.append(_continuous_row(df[age_col], "Age, mean +/- SD"))

    sex_col = _find_col(df, ["demo_sex_final", "demo_sex_raw", "sex"])
    if sex_col:
        rows.append({"variable": "Sex", "value": "", "n": N})
        rows.extend(_categorical_rows(df[sex_col], "Sex", N))

    hist_col = _find_col(df, ["path_histology_final", "path_histology_raw", "histology"])
    if hist_col:
        rows.append({"variable": "Histology", "value": "", "n": N})
        rows.extend(_categorical_rows(df[hist_col], "Histology", N))

    stage_col = _find_col(df, ["staging_ajcc8_stage_group_final", "ajcc8_stage_group", "overall_stage_ajcc8"])
    if stage_col:
        rows.append({"variable": "AJCC8 Stage", "value": "", "n": N})
        rows.extend(_categorical_rows(df[stage_col], "AJCC8", N))

    ata_col = _find_col(df, ["staging_ata_risk_final", "ata_risk", "recurrence_risk_band"])
    if ata_col:
        rows.append({"variable": "ATA Risk", "value": "", "n": N})
        rows.extend(_categorical_rows(df[ata_col], "ATA", N))

    proc_col = _find_col(df, ["treatment_procedure_type_final", "procedure_type", "procedure_normalized"])
    if proc_col:
        rows.append({"variable": "Procedure Type", "value": "", "n": N})
        rows.extend(_categorical_rows(df[proc_col], "Procedure", N))

    size_col = _find_col(df, ["path_tumor_size_cm_final", "path_tumor_size_cm_raw",
                               "tumor_size_cm", "max_tumor_size_cm_v10"])
    if size_col:
        rows.append(_median_iqr_row(df[size_col], "Tumor size (cm), median [IQR]"))

    ln_col = _find_col(df, ["staging_ln_positive_final", "ln_positive_v6", "ln_positive"])
    if ln_col:
        rows.append(_bool_rate_row(df[ln_col], "LN positive rate", N))

    recur_col = _find_col(df, ["outcome_recurrence_flag", "recurrence_confirmed", "recurrence_flag"])
    if recur_col:
        rows.append(_bool_rate_row(df[recur_col], "Recurrence rate", N))

    tirads_col = _find_col(df, ["imaging_tirads_best_score", "tirads_best_score_v12", "tirads_score_v11"])
    if tirads_col:
        rows.append({"variable": "TIRADS", "value": "", "n": N})
        rows.extend(_categorical_rows(df[tirads_col], "TIRADS", N))

    for r in rows:
        r["stratum"] = stratum
    return rows


def main():
    ap = argparse.ArgumentParser(description="Table 1 preview for manuscript cohort")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--md", action="store_true", help="Use MotherDuck")
    grp.add_argument("--local", action="store_true", help="Use local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Print plan only")
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    con = get_connection(args.md)

    print(f"[61] Table 1 preview  ts={TIMESTAMP}")
    print(f"     target={'MotherDuck' if args.md else 'local DuckDB'}")

    src = _resolve_table(con, "manuscript_cohort_v1", [
        "md_manuscript_cohort_v1",
        "patient_analysis_resolved_v1",
        "md_patient_analysis_resolved_v1",
    ])
    df = con.execute(f"SELECT * FROM {src}").df()
    N = len(df)
    print(f"  source: {src}  rows={N}")

    # ── Stratify ─────────────────────────────────────────────────────
    elig_col = _find_col(df, ["analysis_eligible_flag"])
    if elig_col:
        try:
            df["_elig"] = df[elig_col].apply(
                lambda x: x is True or str(x).lower() == "true"
            )
        except Exception:
            df["_elig"] = True
    else:
        df["_elig"] = True

    overall_rows = _build_table1(df, "Overall")
    eligible_rows = _build_table1(df[df["_elig"]], "Eligible")
    ineligible_rows = _build_table1(df[~df["_elig"]], "Not Eligible")

    all_rows = overall_rows + eligible_rows + ineligible_rows
    result_df = pd.DataFrame(all_rows)

    # ── Print ────────────────────────────────────────────────────────
    for stratum in ["Overall", "Eligible", "Not Eligible"]:
        sub = result_df[result_df["stratum"] == stratum]
        print(f"\n  --- {stratum} ---")
        for _, r in sub.iterrows():
            print(f"    {r['variable']:<45s}  {r['value']}")

    # ── Export ───────────────────────────────────────────────────────
    if not args.dry_run:
        csv_path = os.path.join(OUT_DIR, "table1_preview.csv")
        result_df.to_csv(csv_path, index=False)
        print(f"\n  Exported: {csv_path}")

        md_path = os.path.join(OUT_DIR, "table1_preview.md")
        with open(md_path, "w") as f:
            f.write(f"# Table 1 Preview\n\n")
            f.write(f"**Source:** `{src}` | **N:** {N:,} | **Generated:** {TIMESTAMP}\n\n")

            pivot_rows: dict[str, dict[str, str]] = {}
            for _, r in result_df.iterrows():
                var = r["variable"]
                stratum = r["stratum"]
                if var not in pivot_rows:
                    pivot_rows[var] = {}
                pivot_rows[var][stratum] = str(r["value"])

            f.write("| Variable | Overall | Eligible | Not Eligible |\n")
            f.write("|----------|---------|----------|--------------|\n")
            for var, strata in pivot_rows.items():
                ov = strata.get("Overall", "")
                el = strata.get("Eligible", "")
                ne = strata.get("Not Eligible", "")
                f.write(f"| {var} | {ov} | {el} | {ne} |\n")

        print(f"  Exported: {md_path}")

    print(f"\n[61] Done.  Table 1 with {len(pivot_rows)} rows across 3 strata.")
    con.close()


if __name__ == "__main__":
    main()
