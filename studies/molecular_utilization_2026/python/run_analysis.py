#!/usr/bin/env python3
"""Run molecular utilization study: deploy SQL views, export CSV/MD, plots.

Usage (from repo root THYROID_2026):
  .venv/bin/python studies/molecular_utilization_2026/python/run_analysis.py
  .venv/bin/python studies/molecular_utilization_2026/python/run_analysis.py --local /path/to/thyroid_master_local.duckdb

Requires: duckdb, pandas, matplotlib (seaborn optional)
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

STUDY = Path(__file__).resolve().parents[1]
ROOT = STUDY.parents[1]
SQL_DIR = STUDY / "sql"
OUT = STUDY / "outputs"
PLOTS = OUT / "plots"

for p in (OUT, PLOTS):
    p.mkdir(parents=True, exist_ok=True)


def connect_md() -> "duckdb.DuckDBPyConnection":
    import duckdb
    import toml

    tok = os.environ.get("MOTHERDUCK_TOKEN", "")
    if not tok:
        for candidate in (
            ROOT / ".streamlit" / "secrets.toml",
            Path.home() / ".streamlit" / "secrets.toml",
        ):
            if candidate.exists():
                tok = toml.load(str(candidate)).get("MOTHERDUCK_TOKEN", "")
                if tok:
                    break
    if not tok:
        raise SystemExit("MOTHERDUCK_TOKEN not set and not found in secrets.toml")
    return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={tok}")


def connect_local(path: str) -> "duckdb.DuckDBPyConnection":
    import duckdb

    return duckdb.connect(path)


def main() -> None:
    import pandas as pd

    ap = argparse.ArgumentParser()
    ap.add_argument("--local", metavar="PATH", help="Local DuckDB file instead of MotherDuck")
    args = ap.parse_args()

    if args.local:
        con = connect_local(args.local)
    else:
        con = connect_md()

    sql_main = (SQL_DIR / "01_views_and_cohort.sql").read_text()
    con.execute(sql_main)

    exports = [
        ("table1_year", "SELECT * FROM table1_utilization_by_surgery_year_v1 ORDER BY surgery_year, bethesda_category"),
        ("table1_size", "SELECT * FROM table1_utilization_by_size_v1"),
        ("table1_era", "SELECT * FROM table1_utilization_by_surgery_era_v1"),
        ("table1_test_year", "SELECT * FROM table1_molecular_tests_by_year_v1"),
        ("table2", "SELECT * FROM table2_platform_bethesda_result_v1"),
        ("table3", "SELECT * FROM table3_rom_by_platform_result_v1"),
        ("bonus_surgery", "SELECT * FROM bonus_surgery_extent_by_result_v1"),
        ("cohort", "SELECT * FROM indeterminate_molecular_cohort_v1"),
    ]

    frames: dict[str, pd.DataFrame] = {}
    for name, query in exports:
        df = con.execute(query).fetchdf()
        frames[name] = df
        df.to_csv(OUT / f"{name}.csv", index=False)

    _write_md_table(frames["table1_year"], OUT / "table1_utilization.md", "Table 1a. Utilization by surgery year × Bethesda category")
    _write_md_table(frames["table1_size"], OUT / "table1_utilization_by_size.md", "Table 1b. Utilization by size stratum")
    _write_md_table(frames["table1_era"], OUT / "table1_utilization_by_era.md", "Table 1c. Utilization by surgery era (pre-2021 vs 2021+)")
    _write_md_table(frames["table2"], OUT / "table2_platform_result.md", "Table 2. Platform × Bethesda × result (tested patients only; column % within Bethesda)")
    _write_md_table(frames["table3"], OUT / "table3_rom.md", "Table 3. ROM by platform × result (histology malignant keyword on histology_final)")

    con.execute(
        "SELECT * FROM sankey_flow_edges_v1 ORDER BY source, target"
    ).fetchdf().to_csv(OUT / "sankey_edges.csv", index=False)

    _plots(frames)


def _write_md_table(df, path: Path, title: str) -> None:
    import math

    lines = [f"### {title}", ""]
    if df.empty:
        lines.append("_No rows._")
    else:
        lines.append("| " + " | ".join(df.columns) + " |")
        lines.append("| " + " | ".join(["---"] * len(df.columns)) + " |")
        for _, row in df.iterrows():
            cells = []
            for c in df.columns:
                v = row[c]
                if v is None or (isinstance(v, float) and math.isnan(v)):
                    cells.append("")
                elif isinstance(v, float) and v == int(v):
                    cells.append(str(int(v)))
                else:
                    cells.append(str(v).replace("|", "\\|"))
            lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    path.write_text("\n".join(lines))


def _plots(frames: dict) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    t1 = frames.get("table1_year")
    if t1 is None or t1.empty:
        return

    # Line: pct_tested by surgery_year, one line per bethesda
    fig, ax = plt.subplots(figsize=(9, 5))
    for bet in sorted(t1["bethesda_category"].dropna().unique()):
        sub = t1[t1["bethesda_category"] == bet].sort_values("surgery_year")
        ax.plot(sub["surgery_year"], sub["pct_tested"], marker="o", label=f"Bethesda {int(bet)}")
    ax.set_xlabel("Surgery year")
    ax.set_ylabel("% preoperative molecular tested")
    ax.legend(title="Cytology")
    ax.set_title("Molecular utilization (ThyroSeq/Afirma) among operated Bethesda III–V patients")
    fig.tight_layout()
    fig.savefig(PLOTS / "utilization_trend_by_year.png", dpi=150)
    plt.close(fig)

    # Stacked-style bar: Table 2 platforms × result_bucket (summed over bethesda)
    t2 = frames.get("table2")
    if t2 is not None and not t2.empty:
        agg = t2.groupby(["platform", "result_bucket"], as_index=False)["n"].sum()
        pivot = agg.pivot(index="platform", columns="result_bucket", values="n").fillna(0)
        fig2, ax2 = plt.subplots(figsize=(10, 5))
        pivot.plot(kind="bar", stacked=True, ax=ax2)
        ax2.set_ylabel("Patients (tested cohort)")
        ax2.set_xlabel("Platform")
        ax2.legend(title="Result class", bbox_to_anchor=(1.02, 1), loc="upper left")
        ax2.set_title("Molecular result distribution (tested patients only)")
        fig2.tight_layout()
        fig2.savefig(PLOTS / "stacked_result_by_platform.png", dpi=150)
        plt.close(fig2)

    t3 = frames.get("table3")
    if t3 is not None and not t3.empty:
        fig3, ax3 = plt.subplots(figsize=(9, 5))
        labs = [f"{r['platform']}\n{r['result_bucket']}" for _, r in t3.iterrows()]
        ax3.barh(labs, t3["pct_rom"].astype(float))
        ax3.set_xlabel("% ROM (keyword malignant histology)")
        ax3.set_title("Rate of malignancy on surgical pathology (descriptive)")
        fig3.tight_layout()
        fig3.savefig(PLOTS / "rom_by_platform_result.png", dpi=150)
        plt.close(fig3)


if __name__ == "__main__":
    main()
