#!/usr/bin/env python3
"""
37_publication_export.py — Publication-Ready Export Pipeline

Generates the final export bundle for manuscript submission:
  1. Table 1 demographics from advanced_features_sorted
  2. Mutation/molecular summary from genetic_testing
  3. Manuscript-ready CSVs in exports/FINAL_RELEASE/

Wraps outputs from scripts 31 (analytic_models) + 33 (manuscript_tables)
and adds a cohort export from the materialized survival_cohort table.

Usage:
    .venv/bin/python scripts/37_publication_export.py
    .venv/bin/python scripts/37_publication_export.py --md
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from motherduck_client import MotherDuckClient, MotherDuckConfig

OUT_DIR = ROOT / "exports" / "FINAL_RELEASE"


def _get_con(use_md: bool):
    if use_md:
        return MotherDuckClient().connect_rw()
    return __import__("duckdb").connect(str(ROOT / "thyroid_master.duckdb"))


def _tbl_exists(con, name: str) -> bool:
    try:
        return bool(
            con.execute(
                f"SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE table_name='{name}'"
            ).fetchone()[0]
        )
    except Exception:
        return False


def _safe_export(con, table: str, label: str, out_dir: Path) -> int:
    """Export a table to CSV and Parquet; return row count or 0 on skip."""
    if not _tbl_exists(con, table):
        print(f"  SKIP  {label} ({table} not found)")
        return 0
    df = con.execute(f"SELECT * FROM {table}").fetchdf()
    df.to_csv(out_dir / f"{label}.csv", index=False)
    df.to_parquet(out_dir / f"{label}.parquet", index=False)
    print(f"  OK    {label:40s} {len(df):>8,} rows")
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="Publication export pipeline")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck")
    args = parser.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"{'=' * 72}")
    print(f"Publication Export Pipeline — {ts}")
    print(f"Output: {OUT_DIR}")
    print(f"{'=' * 72}")

    con = _get_con(args.md)

    exports = {}
    for table, label in [
        ("advanced_features_sorted", "cohort_full"),
        ("survival_cohort", "survival_cohort"),
        ("publication_kpis", "publication_kpis"),
        ("overview_kpis", "overview_kpis"),
        ("streamlit_cohort_qc_summary_v", "qc_summary"),
        ("streamlit_patient_header_v", "patient_headers"),
    ]:
        n = _safe_export(con, table, label, OUT_DIR)
        exports[label] = n

    # Table 1: demographics summary
    if _tbl_exists(con, "advanced_features_sorted"):
        df = con.execute("SELECT * FROM advanced_features_sorted").fetchdf()
        demo = {
            "total_patients": len(df),
            "mean_age": float(df["age_at_surgery"].mean())
            if "age_at_surgery" in df.columns
            else None,
            "median_age": float(df["age_at_surgery"].median())
            if "age_at_surgery" in df.columns
            else None,
            "female_pct": round(
                100
                * len(df[df["sex"].str.lower() == "female"])
                / max(len(df), 1),
                1,
            )
            if "sex" in df.columns
            else None,
        }
        if "histology_1_type" in df.columns:
            demo["histology_distribution"] = (
                df["histology_1_type"].value_counts().head(10).to_dict()
            )
        pd.DataFrame([demo]).to_csv(
            OUT_DIR / "table1_demographics.csv", index=False
        )
        print(f"  OK    {'table1_demographics':40s} summary")
        exports["table1_demographics"] = 1

    # Mutation summary
    if _tbl_exists(con, "advanced_features_sorted"):
        mut_cols = [
            c
            for c in df.columns
            if c.endswith("_mutation_mentioned") and df[c].dtype == "bool"
        ]
        if mut_cols:
            mut_summary = {
                c.replace("_mutation_mentioned", "").upper(): int(df[c].sum())
                for c in mut_cols
            }
            pd.DataFrame(
                [{"mutation": k, "count": v} for k, v in mut_summary.items()]
            ).to_csv(OUT_DIR / "mutation_summary.csv", index=False)
            print(f"  OK    {'mutation_summary':40s} {len(mut_summary)} genes")
            exports["mutation_summary"] = len(mut_summary)

    # Manifest
    manifest = {
        "generated_at": ts,
        "source": "thyroid_research_2026",
        "exports": exports,
        "git_sha": "see CITATION.cff",
    }
    (OUT_DIR / "manifest.json").write_text(
        json.dumps(manifest, indent=2, default=str)
    )
    print(f"\n{'=' * 72}")
    print(f"DONE — {sum(exports.values()):,} total rows across {len(exports)} exports")
    print(f"{'=' * 72}")

    con.close()


if __name__ == "__main__":
    main()
