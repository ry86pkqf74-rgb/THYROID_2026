#!/usr/bin/env python3
"""Run molecular utilization study: deploy SQL views, export CSV/MD, plots.

Usage (from repo root THYROID_2026):
  .venv/bin/python studies/molecular_utilization_2026/python/run_analysis.py
  .venv/bin/python studies/molecular_utilization_2026/python/run_analysis.py --v2
  .venv/bin/python studies/molecular_utilization_2026/python/run_analysis.py --local /path/to/thyroid_master_local.duckdb

Requires: duckdb, pandas, matplotlib (seaborn optional)
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

STUDY = Path(__file__).resolve().parents[1]
ROOT = STUDY.parents[1]
SQL_DIR = STUDY / "sql"
OUT = STUDY / "outputs"
PLOTS = OUT / "plots"
OUT_V2 = OUT / "v2"
PLOTS_V2 = OUT_V2 / "plots"

for p in (OUT, PLOTS, OUT_V2, PLOTS_V2):
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


def _git_sha_short() -> str:
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def main() -> None:
    import pandas as pd

    ap = argparse.ArgumentParser()
    ap.add_argument("--local", metavar="PATH", help="Local DuckDB file instead of MotherDuck")
    ap.add_argument("--v2", action="store_true", help="Run V2 manuscript refresh pipeline only")
    ap.add_argument("--all", action="store_true", help="Run V1 then V2")
    args = ap.parse_args()

    if args.all:
        _run_v1(args.local)
        _run_v2(args.local)
        return

    if args.v2:
        _run_v2(args.local)
        return

    _run_v1(args.local)


def _run_v1(local_path: str | None) -> None:
    import pandas as pd

    if local_path:
        con = connect_local(local_path)
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


def _run_v2(local_path: str | None) -> None:
    import pandas as pd

    if local_path:
        con = connect_local(local_path)
    else:
        con = connect_md()

    sql_v2 = (SQL_DIR / "01_views_and_cohort_v2.sql").read_text()
    con.execute(sql_v2)

    exports = [
        ("cohort_all_eligible_v2", "SELECT * FROM all_eligible_indeterminate_v2"),
        ("cohort_tested_v2", "SELECT * FROM tested_indeterminate_v2"),
        ("cohort_operated_v2", "SELECT * FROM operated_indeterminate_v2"),
        ("cohort_bethesda5_secondary_v2", "SELECT * FROM bethesdaV_secondary_v2"),
        ("episode_sensitivity_v2", "SELECT * FROM episode_sensitivity_v2"),
        ("manual_review_molecular_path_v2", "SELECT * FROM manual_review_molecular_path_mismatch_v2"),
        ("qa_small_cells_v2", "SELECT * FROM qa_small_cell_strata_v2"),
        ("ana_v2_uptake_year_bethesda", "SELECT * FROM ana_v2_testing_uptake_by_year_bethesda"),
        ("ana_v2_platform_mix_year", "SELECT * FROM ana_v2_platform_mix_by_year"),
        ("ana_v2_result_mix_platform", "SELECT * FROM ana_v2_result_mix_by_platform"),
        ("ana_v2_surgery_rates", "SELECT * FROM ana_v2_surgery_rates_tested_vs_never"),
        ("ana_v2_pathology_operated", "SELECT * FROM ana_v2_pathology_among_operated"),
        ("ana_v2_surgery_extent", "SELECT * FROM ana_v2_surgery_extent_operated"),
        ("ana_v2_mutations", "SELECT * FROM ana_v2_mutation_families_tested"),
        ("ana_v2_uptake_bethesda3", "SELECT * FROM ana_v2_testing_uptake_by_year_bethesda3"),
        ("ana_v2_uptake_bethesda4", "SELECT * FROM ana_v2_testing_uptake_by_year_bethesda4"),
        ("ana_v2_path_bethesda3", "SELECT * FROM ana_v2_pathology_among_operated_bethesda3"),
        ("ana_v2_path_bethesda4", "SELECT * FROM ana_v2_pathology_among_operated_bethesda4"),
        ("ana_v2_uptake_sens2018", "SELECT * FROM ana_v2_testing_uptake_by_year_bethesda_sens2018"),
        ("ana_v2_bethesda5_uptake", "SELECT * FROM ana_v2_bethesda5_uptake_by_year"),
        ("ana_v2_bethesda5_platform_year", "SELECT * FROM ana_v2_bethesda5_platform_by_year"),
    ]

    frames: dict[str, pd.DataFrame] = {}
    for name, query in exports:
        df = con.execute(query).fetchdf()
        frames[name] = df
        df.to_csv(OUT_V2 / f"{name}.csv", index=False)

    # Figure-ready (duplicate key analytic tables with explicit naming)
    for fn, key in [
        ("fig_v2_uptake_index_year_bethesda.csv", "ana_v2_uptake_year_bethesda"),
        ("fig_v2_platform_mix_year.csv", "ana_v2_platform_mix_year"),
        ("fig_v2_result_mix_platform.csv", "ana_v2_result_mix_platform"),
        ("fig_v2_surgery_rates.csv", "ana_v2_surgery_rates"),
        ("fig_v2_pathology_operated.csv", "ana_v2_pathology_operated"),
        ("fig_v2_bethesda5_uptake.csv", "ana_v2_bethesda5_uptake"),
    ]:
        frames[key].to_csv(OUT_V2 / fn, index=False)

    _write_md_table(
        frames["ana_v2_uptake_year_bethesda"],
        OUT_V2 / "table_v2_01_uptake_index_year_bethesda.md",
        "Table V2-1. Testing uptake by index FNA year × Bethesda (III/IV eligible, 2015+ era)",
    )
    _write_md_table(
        frames["ana_v2_platform_mix_year"],
        OUT_V2 / "table_v2_02_platform_mix_year.md",
        "Table V2-2. Platform mix by test year (tested patients)",
    )
    _write_md_table(
        frames["ana_v2_result_mix_platform"],
        OUT_V2 / "table_v2_03_result_mix_platform.md",
        "Table V2-3. Result-class mix by platform (column % within platform)",
    )
    _write_md_table(
        frames["ana_v2_surgery_rates"],
        OUT_V2 / "table_v2_04_surgery_rates.md",
        "Table V2-4. Surgery rates: tested vs never-tested (descriptive)",
    )
    _write_md_table(
        frames["ana_v2_pathology_operated"],
        OUT_V2 / "table_v2_05_pathology_operated.md",
        "Table V2-5. Pathology buckets among operated patients (structured + keyword fallback)",
    )
    _write_md_table(
        frames["ana_v2_surgery_extent"],
        OUT_V2 / "table_v2_06_surgery_extent.md",
        "Table V2-6. First procedure & completion thyroidectomy (operated cohort)",
    )
    _write_md_table(
        frames["ana_v2_mutations"],
        OUT_V2 / "table_v2_07_mutations.md",
        "Table V2-7. Mutation-family flags (latest test, tested cohort)",
    )
    _write_md_table(
        frames["ana_v2_uptake_bethesda3"],
        OUT_V2 / "table_v2_08a_uptake_bethesda3.md",
        "Table V2-8a. Uptake — Bethesda III only",
    )
    _write_md_table(
        frames["ana_v2_uptake_bethesda4"],
        OUT_V2 / "table_v2_08b_uptake_bethesda4.md",
        "Table V2-8b. Uptake — Bethesda IV only",
    )
    _write_md_table(
        frames["ana_v2_path_bethesda3"],
        OUT_V2 / "table_v2_09a_path_bethesda3.md",
        "Table V2-9a. Pathology — operated Bethesda III index",
    )
    _write_md_table(
        frames["ana_v2_path_bethesda4"],
        OUT_V2 / "table_v2_09b_path_bethesda4.md",
        "Table V2-9b. Pathology — operated Bethesda IV index",
    )
    _write_md_table(
        frames["ana_v2_bethesda5_uptake"],
        OUT_V2 / "table_v2_10_bethesda5_uptake.md",
        "Table V2-10 (secondary). Bethesda V uptake by index year",
    )
    _write_md_table(
        frames["ana_v2_bethesda5_platform_year"],
        OUT_V2 / "table_v2_11_bethesda5_platform_year.md",
        "Table V2-11 (secondary). Bethesda V platform × year (tested)",
    )

    # QA markdown report
    n_review = len(frames["manual_review_molecular_path_v2"])
    n_small = len(frames["qa_small_cells_v2"])
    (OUT_V2 / "qa_report_v2.md").write_text(
        "\n".join(
            [
                "# QA report — molecular utilization V2",
                "",
                f"- Generated (UTC): {datetime.now(timezone.utc).isoformat()}",
                f"- Git short SHA: {_git_sha_short()}",
                f"- Manual review queue rows: **{n_review}**",
                f"- Small-cell strata rows (n<10 rule): **{n_small}**",
                "",
                "See `manual_review_molecular_path_v2.csv` and `qa_small_cells_v2.csv`.",
                "",
            ]
        )
    )

    counts_sql = """
    SELECT 'all_eligible_indeterminate_v2' AS k, COUNT(*)::BIGINT AS n FROM all_eligible_indeterminate_v2
    UNION ALL SELECT 'tested_indeterminate_v2', COUNT(*)::BIGINT FROM tested_indeterminate_v2
    UNION ALL SELECT 'operated_indeterminate_v2', COUNT(*)::BIGINT FROM operated_indeterminate_v2
    UNION ALL SELECT 'bethesdaV_secondary_v2', COUNT(*)::BIGINT FROM bethesdaV_secondary_v2
    """
    counts_df = con.execute(counts_sql).fetchdf()
    manifest = {
        "freeze_label": "molecular_utilization_manuscript_v2",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_sha_short": _git_sha_short(),
        "database": "md:thyroid_research_2026" if not local_path else local_path,
        "sql_scripts": [
            str(SQL_DIR / "01_views_and_cohort_v2.sql"),
            str(SQL_DIR / "02_motherduck_verification_v2.sql"),
        ],
        "source_tables": [
            "manuscript_cohort_v1",
            "fna_episode_master_v2",
            "molecular_test_episode_v2",
            "tumor_pathology",
            "tumor_episode_master_v2",
            "operative_episode_detail_v2",
            "fna_molecular_linkage_v3",
        ],
        "cohort_row_counts": {row["k"]: int(row["n"]) for _, row in counts_df.iterrows()},
    }
    (OUT_V2 / "freeze_manifest_v2.json").write_text(json.dumps(manifest, indent=2))
    _plots_v2(frames)


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


def _plots_v2(frames: dict) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    t1 = frames.get("ana_v2_uptake_year_bethesda")
    if t1 is not None and not t1.empty:
        fig, ax = plt.subplots(figsize=(9, 5))
        for bet in sorted(t1["bethesda_category"].dropna().unique()):
            sub = t1[t1["bethesda_category"] == bet].sort_values("index_year")
            ax.plot(sub["index_year"], sub["pct_tested"], marker="o", label=f"Bethesda {int(bet)}")
        ax.set_xlabel("Index indeterminate FNA year (2015+ era)")
        ax.set_ylabel("% molecular tested (ThyroSeq/Afirma after index)")
        ax.legend(title="Index cytology")
        ax.set_title("V2 uptake — Bethesda III/IV eligible cohort")
        fig.tight_layout()
        fig.savefig(PLOTS_V2 / "v2_utilization_trend_by_index_year.png", dpi=150)
        plt.close(fig)

    t2 = frames.get("ana_v2_result_mix_platform")
    if t2 is not None and not t2.empty:
        pivot = t2.pivot(index="platform", columns="result_bucket", values="n").fillna(0)
        fig2, ax2 = plt.subplots(figsize=(10, 5))
        pivot.plot(kind="bar", stacked=True, ax=ax2)
        ax2.set_ylabel("Patients (tested cohort)")
        ax2.set_xlabel("Platform")
        ax2.legend(title="Result class", bbox_to_anchor=(1.02, 1), loc="upper left")
        ax2.set_title("V2 molecular result distribution by platform")
        fig2.tight_layout()
        fig2.savefig(PLOTS_V2 / "v2_stacked_result_by_platform.png", dpi=150)
        plt.close(fig2)


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
