#!/usr/bin/env python3
"""
59_dataset_summary.py -- Dataset summary statistics for manuscript

Computes distributions, cross-tabulations, and scoring calculability
rates from the frozen manuscript cohort.

Outputs:
  exports/manuscript_analysis/dataset_summary.json
  exports/manuscript_analysis/dataset_summary.md
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime

import pandas as pd
import numpy as np

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


def _safe_dist(df: pd.DataFrame, col: str) -> dict:
    """Value counts as {value: count} dict, handling missing columns."""
    if col not in df.columns:
        return {}
    vc = df[col].dropna().value_counts()
    return {str(k): int(v) for k, v in vc.items()}


def _safe_crosstab(df: pd.DataFrame, row_col: str, col_col: str) -> dict:
    """Cross-tabulation as nested dict, handling missing columns."""
    if row_col not in df.columns or col_col not in df.columns:
        return {}
    ct = pd.crosstab(df[row_col].fillna("missing"), df[col_col].fillna("missing"))
    return {str(k): {str(k2): int(v2) for k2, v2 in row.items()} for k, row in ct.iterrows()}


def main():
    ap = argparse.ArgumentParser(description="Dataset summary for manuscript cohort")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--md", action="store_true", help="Use MotherDuck")
    grp.add_argument("--local", action="store_true", help="Use local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Print plan only")
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    con = get_connection(args.md)

    print(f"[59] Dataset summary  ts={TIMESTAMP}")
    print(f"     target={'MotherDuck' if args.md else 'local DuckDB'}")

    src = _resolve_table(con, "manuscript_cohort_v1", [
        "md_manuscript_cohort_v1",
        "patient_analysis_resolved_v1",
        "md_patient_analysis_resolved_v1",
    ])

    df = con.execute(f"SELECT * FROM {src}").df()
    N = len(df)
    print(f"  source: {src}  rows={N}")

    result: dict = {"source": src, "N": N, "timestamp": TIMESTAMP}

    # ── 1. Distributions ─────────────────────────────────────────────
    print("\n  Computing distributions ...")
    dist_cols = {
        "histology": "path_histology_final",
        "ajcc8_stage": "staging_ajcc8_stage_group_final",
        "ata_risk": "staging_ata_risk_final",
        "procedure_type": "treatment_procedure_type_final",
    }
    result["distributions"] = {}
    for label, col in dist_cols.items():
        d = _safe_dist(df, col)
        if not d:
            for alt in [col.replace("_final", "_raw"), col.replace("_final", "")]:
                d = _safe_dist(df, alt)
                if d:
                    break
        result["distributions"][label] = d
        top3 = sorted(d.items(), key=lambda x: -x[1])[:3]
        top_str = ", ".join(f"{k}={v}" for k, v in top3)
        print(f"    {label}: {len(d)} categories  top: {top_str}")

    # ── 2. Cross-tabulations ─────────────────────────────────────────
    print("\n  Computing cross-tabulations ...")
    result["cross_tabs"] = {}

    tirads_col = None
    for c in ("imaging_tirads_best_score", "tirads_best_score_v12", "tirads_score_v11"):
        if c in df.columns:
            tirads_col = c
            break

    bethesda_col = None
    for c in ("bethesda_num", "bethesda_final", "fna_bethesda_final"):
        if c in df.columns:
            bethesda_col = c
            break

    if tirads_col and bethesda_col:
        ct = _safe_crosstab(df, tirads_col, bethesda_col)
        result["cross_tabs"]["tirads_vs_bethesda"] = ct
        print(f"    tirads_vs_bethesda: {len(ct)} rows")
    else:
        print(f"    tirads_vs_bethesda: skipped (tirads={tirads_col}, bethesda={bethesda_col})")

    mol_col = None
    for c in ("molecular_braf_positive_final", "molecular_ras_positive_final",
              "braf_positive_final", "ras_positive_final"):
        if c in df.columns:
            mol_col = c
            break

    if bethesda_col and mol_col:
        ct = _safe_crosstab(df, bethesda_col, mol_col)
        result["cross_tabs"]["bethesda_vs_molecular"] = ct
        print(f"    bethesda_vs_molecular: {len(ct)} rows")
    else:
        print(f"    bethesda_vs_molecular: skipped")

    # ── 3. Scoring calculability ─────────────────────────────────────
    print("\n  Scoring calculability ...")
    scoring_flags = {
        "ajcc8": "scoring_ajcc8_flag",
        "ata": "scoring_ata_flag",
        "macis": "scoring_macis_flag",
        "ages": "scoring_ages_flag",
        "ames": "scoring_ames_flag",
    }
    result["scoring_calculability"] = {}
    for label, col in scoring_flags.items():
        if col in df.columns:
            try:
                calc_n = int(df[col].apply(lambda x: x is True or str(x).lower() == "true").sum())
            except Exception:
                calc_n = 0
            pct = round(100.0 * calc_n / N, 1) if N > 0 else 0
            result["scoring_calculability"][label] = {"calculable": calc_n, "total": N, "pct": pct}
            print(f"    {label}: {calc_n:,}/{N:,} ({pct}%)")
        else:
            print(f"    {label}: column {col} not found")

    # ── 4. Eligibility summary ───────────────────────────────────────
    print("\n  Eligibility flags ...")
    elig_flags = [
        "analysis_eligible_flag", "molecular_eligible_flag",
        "rai_eligible_flag", "survival_eligible_flag",
    ]
    result["eligibility"] = {}
    for col in elig_flags:
        if col in df.columns:
            try:
                n_true = int(df[col].apply(lambda x: x is True or str(x).lower() == "true").sum())
            except Exception:
                n_true = 0
            pct = round(100.0 * n_true / N, 1) if N > 0 else 0
            result["eligibility"][col] = {"n": n_true, "pct": pct}
            print(f"    {col}: {n_true:,} ({pct}%)")

    # ── 5. Export ────────────────────────────────────────────────────
    if not args.dry_run:
        json_path = os.path.join(OUT_DIR, "dataset_summary.json")
        with open(json_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\n  Exported: {json_path}")

        md_path = os.path.join(OUT_DIR, "dataset_summary.md")
        with open(md_path, "w") as f:
            f.write(f"# Dataset Summary\n\n")
            f.write(f"**Source:** `{src}` | **N:** {N:,} | **Generated:** {TIMESTAMP}\n\n")

            f.write("## Distributions\n\n")
            for label, d in result["distributions"].items():
                f.write(f"### {label.replace('_', ' ').title()}\n\n")
                f.write(f"| Value | N | % |\n|-------|--:|---:|\n")
                for k, v in sorted(d.items(), key=lambda x: -x[1]):
                    f.write(f"| {k} | {v:,} | {100*v/N:.1f}% |\n")
                f.write("\n")

            if result["cross_tabs"]:
                f.write("## Cross-Tabulations\n\n")
                for label, ct in result["cross_tabs"].items():
                    f.write(f"### {label.replace('_', ' ').title()}\n\n")
                    if ct:
                        all_cols_set: set[str] = set()
                        for row_vals in ct.values():
                            all_cols_set.update(row_vals.keys())
                        all_cols = sorted(all_cols_set)
                        f.write(f"| | " + " | ".join(all_cols) + " |\n")
                        f.write(f"|---" + "|---:" * len(all_cols) + "|\n")
                        for rk, rv in sorted(ct.items()):
                            vals = " | ".join(str(rv.get(c, 0)) for c in all_cols)
                            f.write(f"| {rk} | {vals} |\n")
                    f.write("\n")

            f.write("## Scoring Calculability\n\n")
            f.write("| System | Calculable | Total | % |\n|--------|----------:|------:|---:|\n")
            for label, info in result["scoring_calculability"].items():
                f.write(f"| {label.upper()} | {info['calculable']:,} | {info['total']:,} | {info['pct']}% |\n")
            f.write("\n")

            f.write("## Eligibility Flags\n\n")
            f.write("| Flag | N | % |\n|------|--:|---:|\n")
            for col, info in result["eligibility"].items():
                f.write(f"| {col} | {info['n']:,} | {info['pct']}% |\n")

        print(f"  Exported: {md_path}")

    print(f"\n[59] Done.  {len(result.get('distributions', {}))} distributions, "
          f"{len(result.get('cross_tabs', {}))} cross-tabs, "
          f"{len(result.get('scoring_calculability', {}))} scoring systems.")
    con.close()


if __name__ == "__main__":
    main()
