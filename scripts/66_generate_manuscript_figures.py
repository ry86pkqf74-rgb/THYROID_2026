#!/usr/bin/env python3
"""
66_generate_manuscript_figures.py -- Publication-quality manuscript figures

Figures:
  1. Cohort flow diagram
  2. KM recurrence-free survival by AJCC8 stage
  3. AJCC8 + ATA risk distribution bar charts
  4. Mutation spectrum (BRAF/RAS/TERT positivity rates)
  5. Complication rates by procedure type

Uses matplotlib for publication quality.
300 DPI PNG + SVG to exports/manuscript_figures/.
Supports --md, --local, --dry-run.
"""
from __future__ import annotations

import argparse
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

FIGURES_DIR = ROOT / "exports" / "manuscript_figures"
TABLES_DIR = ROOT / "exports" / "manuscript_tables"
ANALYSIS_DIR = ROOT / "exports" / "manuscript_analysis"
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

TODAY = datetime.now().strftime("%Y%m%d_%H%M")

warnings.filterwarnings("ignore", category=UserWarning)

# ---------------------------------------------------------------------------
# Connection (for figures that need raw data)
# ---------------------------------------------------------------------------

def _get_token() -> str:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if token:
        return token
    secrets = ROOT / ".streamlit" / "secrets.toml"
    if secrets.exists():
        try:
            import toml
            return toml.load(str(secrets))["MOTHERDUCK_TOKEN"]
        except Exception:
            pass
    raise RuntimeError(
        "MOTHERDUCK_TOKEN not set. Export it or add to .streamlit/secrets.toml."
    )


def get_connection(md: bool):
    import duckdb
    if md:
        token = _get_token()
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(
        os.environ.get("LOCAL_DUCKDB_PATH", str(ROOT / "thyroid_master.duckdb"))
    )


def resolve_table(con, preferred: str, fallback: str) -> str:
    for name in (preferred, fallback, f"md_{preferred}", f"md_{fallback}"):
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            if n > 0:
                return name
        except Exception:
            continue
    raise RuntimeError(f"Neither {preferred} nor {fallback} found.")


def section(title: str) -> None:
    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print(f"{'=' * 72}\n")


# ---------------------------------------------------------------------------
# Style configuration
# ---------------------------------------------------------------------------

PALETTE = {
    "primary": "#2C3E50",
    "accent1": "#E74C3C",
    "accent2": "#3498DB",
    "accent3": "#2ECC71",
    "accent4": "#F39C12",
    "accent5": "#9B59B6",
    "gray": "#95A5A6",
    "light_gray": "#ECF0F1",
}

STAGE_COLORS = {
    "I/II": "#3498DB",
    "III/IV": "#E74C3C",
}

ATA_COLORS = {
    "low": "#2ECC71",
    "intermediate": "#F39C12",
    "high": "#E74C3C",
}


def setup_style():
    import matplotlib
    matplotlib.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 10,
        "axes.titlesize": 12,
        "axes.labelsize": 11,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "figure.dpi": 300,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.3,
        "grid.linestyle": "--",
    })


def save_figure(fig, name: str):
    for ext in ("png", "svg"):
        out = FIGURES_DIR / f"{name}.{ext}"
        fig.savefig(out, dpi=300, bbox_inches="tight")
    print(f"    -> {FIGURES_DIR / name}.png/svg")
    import matplotlib.pyplot as plt
    plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 1: Cohort flow diagram
# ---------------------------------------------------------------------------

def figure_cohort_flow():
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches

    section("Figure 1: Cohort Flow")
    fp = TABLES_DIR / "cohort_flow.csv"
    if not fp.exists():
        print("  [SKIP] cohort_flow.csv not found")
        return

    df = pd.read_csv(fp)
    fig, ax = plt.subplots(figsize=(6, 8))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, len(df) + 1)
    ax.axis("off")

    box_width, box_height = 6, 0.7
    x_center = 5

    for i, (_, row) in enumerate(df.iterrows()):
        y = len(df) - i
        color = PALETTE["accent2"] if i == 0 else (PALETTE["accent3"] if "eligible" in str(row["Step"]).lower() else PALETTE["light_gray"])
        rect = mpatches.FancyBboxPatch(
            (x_center - box_width / 2, y - box_height / 2),
            box_width, box_height,
            boxstyle="round,pad=0.1",
            facecolor=color, edgecolor=PALETTE["primary"], linewidth=1.5, alpha=0.85,
        )
        ax.add_patch(rect)
        label = f"{row['Step']}\nN = {int(row['N']):,}"
        if row.get("Pct_of_total") and str(row["Pct_of_total"]).strip():
            label += f" ({row['Pct_of_total']})"
        ax.text(x_center, y, label, ha="center", va="center",
                fontsize=9, fontweight="bold", color=PALETTE["primary"])

        if i < len(df) - 1:
            ax.annotate("", xy=(x_center, y - box_height / 2 - 0.05),
                        xytext=(x_center, y - box_height / 2 - 0.25),
                        arrowprops=dict(arrowstyle="->", color=PALETTE["primary"], lw=1.5))

    ax.set_title("Figure 1. Cohort Inclusion Flow", fontsize=13, fontweight="bold",
                 color=PALETTE["primary"], pad=10)
    save_figure(fig, "fig1_cohort_flow")


# ---------------------------------------------------------------------------
# Figure 2: KM survival by AJCC8 stage
# ---------------------------------------------------------------------------

def figure_km_survival():
    import matplotlib.pyplot as plt

    section("Figure 2: KM Recurrence-Free Survival by AJCC8 Stage")
    fp = ANALYSIS_DIR / "km_curve_data.csv"
    if not fp.exists():
        print("  [SKIP] km_curve_data.csv not found")
        return

    df = pd.read_csv(fp)
    stage_data = df[df["Stratifier"] == "AJCC8 Stage"]
    if stage_data.empty:
        print("  [SKIP] No AJCC8 Stage KM data")
        return

    fig, ax = plt.subplots(figsize=(7, 5))

    for group in sorted(stage_data["Group"].unique()):
        gd = stage_data[stage_data["Group"] == group].sort_values("Time_years")
        color = STAGE_COLORS.get(group, PALETTE["gray"])
        ax.step(gd["Time_years"], gd["Survival"], where="post", label=group,
                color=color, linewidth=2)
        if "CI_lower" in gd.columns and "CI_upper" in gd.columns:
            ax.fill_between(gd["Time_years"], gd["CI_lower"], gd["CI_upper"],
                            step="post", alpha=0.15, color=color)

    ax.set_xlabel("Time (years)")
    ax.set_ylabel("Recurrence-Free Survival")
    ax.set_title("Figure 2. Recurrence-Free Survival by AJCC 8th Edition Stage",
                 fontweight="bold", color=PALETTE["primary"])
    ax.set_ylim(0, 1.05)
    ax.set_xlim(left=0)
    ax.legend(title="AJCC8 Stage", loc="lower left")
    save_figure(fig, "fig2_km_ajcc8")


# ---------------------------------------------------------------------------
# Figure 3: AJCC8 + ATA distribution
# ---------------------------------------------------------------------------

def figure_stage_distribution(df: pd.DataFrame | None):
    import matplotlib.pyplot as plt

    section("Figure 3: Stage & Risk Distribution")
    if df is None or df.empty:
        fp = TABLES_DIR / "table2_tumor_treatment.csv"
        if not fp.exists():
            print("  [SKIP] No data available for stage distribution")
            return
        df = pd.read_csv(fp)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    if "ajcc8_stage_group" in (df.columns if not isinstance(df, pd.DataFrame) else []):
        pass

    if "Variable" in df.columns:
        stage_data = df[df["Variable"].astype(str).str.contains("AJCC", case=False, na=False)]
        ata_data = df[df["Variable"].astype(str).str.contains("ATA", case=False, na=False)]
    else:
        stage_data = pd.DataFrame()
        ata_data = pd.DataFrame()

    if len(stage_data) > 0 and "Category" in stage_data.columns:
        cats = stage_data["Category"].values
        vals = pd.to_numeric(stage_data["N"], errors="coerce").fillna(0).values
        colors = [STAGE_COLORS.get(str(c).strip(), PALETTE["gray"]) for c in cats]
        axes[0].barh(range(len(cats)), vals, color=colors, edgecolor="white", height=0.6)
        axes[0].set_yticks(range(len(cats)))
        axes[0].set_yticklabels(cats)
        axes[0].set_xlabel("Number of Patients")
        axes[0].set_title("AJCC 8th Edition Stage", fontweight="bold", color=PALETTE["primary"])
        for i, v in enumerate(vals):
            if v > 0:
                axes[0].text(v + max(vals) * 0.01, i, f"{int(v):,}", va="center", fontsize=8)
    else:
        axes[0].text(0.5, 0.5, "Stage data not available", ha="center", va="center", transform=axes[0].transAxes)
        axes[0].set_title("AJCC 8th Edition Stage", fontweight="bold", color=PALETTE["primary"])

    if len(ata_data) > 0 and "Category" in ata_data.columns:
        cats = ata_data["Category"].values
        vals = pd.to_numeric(ata_data["N"], errors="coerce").fillna(0).values
        colors = [ATA_COLORS.get(str(c).strip().lower(), PALETTE["gray"]) for c in cats]
        axes[1].barh(range(len(cats)), vals, color=colors, edgecolor="white", height=0.6)
        axes[1].set_yticks(range(len(cats)))
        axes[1].set_yticklabels(cats)
        axes[1].set_xlabel("Number of Patients")
        axes[1].set_title("ATA Risk Category", fontweight="bold", color=PALETTE["primary"])
        for i, v in enumerate(vals):
            if v > 0:
                axes[1].text(v + max(vals) * 0.01, i, f"{int(v):,}", va="center", fontsize=8)
    else:
        axes[1].text(0.5, 0.5, "ATA risk data not available", ha="center", va="center", transform=axes[1].transAxes)
        axes[1].set_title("ATA Risk Category", fontweight="bold", color=PALETTE["primary"])

    fig.suptitle("Figure 3. Stage and Risk Stratification Distribution",
                 fontweight="bold", color=PALETTE["primary"], fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    save_figure(fig, "fig3_stage_risk_distribution")


# ---------------------------------------------------------------------------
# Figure 4: Mutation spectrum
# ---------------------------------------------------------------------------

def figure_mutation_spectrum(df: pd.DataFrame | None):
    import matplotlib.pyplot as plt

    section("Figure 4: Mutation Spectrum")
    if df is None or df.empty:
        print("  [SKIP] No cohort data for mutation spectrum")
        return

    eligible = df[df.get("analysis_eligible_flag", pd.Series(False)) == True]  # noqa: E712
    total = len(eligible)
    if total == 0:
        print("  [SKIP] No eligible patients")
        return

    def _bool_rate(col_name):
        if col_name not in eligible.columns:
            return 0, 0.0
        vals = eligible[col_name]
        n_pos = int((vals == True).sum() | (vals.astype(str).str.lower() == "true").sum())  # noqa: E712
        return n_pos, 100 * n_pos / total

    markers = [
        ("BRAF", "mol_braf_positive_final"),
        ("RAS", "mol_ras_positive_final"),
        ("TERT", "mol_tert_positive_final"),
    ]

    names, rates, counts = [], [], []
    for label, col in markers:
        n, pct = _bool_rate(col)
        names.append(label)
        rates.append(pct)
        counts.append(n)

    if all(r == 0 for r in rates):
        print("  [SKIP] All mutation rates are 0")
        return

    fig, ax = plt.subplots(figsize=(7, 4))
    colors = [PALETTE["accent1"], PALETTE["accent2"], PALETTE["accent5"]]
    bars = ax.bar(names, rates, color=colors[:len(names)], edgecolor="white", width=0.5)

    for bar, n, r in zip(bars, counts, rates):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{n:,}\n({r:.1f}%)", ha="center", va="bottom", fontsize=9)

    ax.set_ylabel("Positivity Rate (%)")
    ax.set_title("Figure 4. Molecular Marker Positivity Rates",
                 fontweight="bold", color=PALETTE["primary"])
    ax.set_ylim(0, max(rates) * 1.3 if max(rates) > 0 else 10)
    save_figure(fig, "fig4_mutation_spectrum")


# ---------------------------------------------------------------------------
# Figure 5: Complication rates by procedure type
# ---------------------------------------------------------------------------

def figure_complication_rates(df: pd.DataFrame | None):
    import matplotlib.pyplot as plt

    section("Figure 5: Complication Rates by Procedure Type")
    if df is None or df.empty:
        print("  [SKIP] No cohort data for complication rates")
        return

    eligible = df[df.get("analysis_eligible_flag", pd.Series(False)) == True]  # noqa: E712
    if "surg_procedure_type" not in eligible.columns:
        print("  [SKIP] surg_procedure_type not found")
        return

    proc_col = eligible["surg_procedure_type"].astype(str).str.lower()
    procedures = proc_col.value_counts()
    top_procs = [p for p in procedures.index[:5] if p != "nan"]

    if not top_procs:
        print("  [SKIP] No procedure types found")
        return

    comp_cols = [
        ("hypocalcemia_status", "Hypocalcemia"),
        ("rln_status", "RLN Injury"),
        ("rln_permanent_flag", "Permanent RLN"),
    ]
    available_comps = [(c, l) for c, l in comp_cols if c in eligible.columns]
    if not available_comps:
        print("  [SKIP] No complication columns found")
        return

    data = {}
    for proc in top_procs:
        mask = proc_col == proc
        n_proc = int(mask.sum())
        rates = {}
        for col, label in available_comps:
            vals = eligible.loc[mask, col]
            if vals.dtype == object:
                n_pos = int(vals.str.lower().isin(["true", "yes", "confirmed", "confirmed_permanent", "confirmed_transient"]).sum())
            else:
                n_pos = int((vals == True).sum())  # noqa: E712
            rates[label] = 100 * n_pos / n_proc if n_proc else 0
        data[proc] = rates

    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(top_procs))
    width = 0.8 / len(available_comps)
    comp_colors = [PALETTE["accent1"], PALETTE["accent4"], PALETTE["accent5"]]

    for i, (col, label) in enumerate(available_comps):
        vals = [data[p].get(label, 0) for p in top_procs]
        bars = ax.bar(x + i * width - 0.4 + width / 2, vals, width,
                      label=label, color=comp_colors[i % len(comp_colors)], edgecolor="white")
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.2,
                        f"{v:.1f}%", ha="center", va="bottom", fontsize=7)

    proc_labels = [p.replace("_", " ").title() for p in top_procs]
    ax.set_xticks(x)
    ax.set_xticklabels(proc_labels, rotation=15, ha="right")
    ax.set_ylabel("Rate (%)")
    ax.set_title("Figure 5. Complication Rates by Procedure Type",
                 fontweight="bold", color=PALETTE["primary"])
    ax.legend(loc="upper right")
    ax.set_ylim(bottom=0)
    save_figure(fig, "fig5_complication_rates")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate manuscript figures")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--md", action="store_true", help="Use MotherDuck (for raw data figures)")
    grp.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print plan only")
    args = parser.parse_args()

    use_md = args.md
    if not args.md and not args.local:
        use_md = False

    section("66 · Generate Manuscript Figures")

    if args.dry_run:
        print("[DRY-RUN] Would generate 5 manuscript figures:")
        print("  Fig 1: Cohort flow diagram")
        print("  Fig 2: KM survival by AJCC8 stage")
        print("  Fig 3: Stage & ATA risk distribution")
        print("  Fig 4: Mutation spectrum")
        print("  Fig 5: Complication rates by procedure type")
        print(f"  Output: {FIGURES_DIR} (PNG 300dpi + SVG)")
        return

    try:
        import matplotlib
        matplotlib.use("Agg")
    except ImportError:
        print("[ERROR] matplotlib is required for figure generation.")
        print("  Install: pip install matplotlib")
        return

    setup_style()

    cohort_df = None
    try:
        con = get_connection(use_md)
        table = resolve_table(con, "manuscript_cohort_v1", "patient_analysis_resolved_v1")
        cohort_df = con.execute(f"SELECT * FROM {table}").fetchdf()
        print(f"  Loaded cohort: {len(cohort_df):,} rows")
        con.close()
    except Exception as e:
        print(f"  [WARN] Could not load cohort from DB: {e}")
        print("  Figures 3-5 may be skipped. Figures 1-2 use CSV data.")

    try:
        figure_cohort_flow()
    except Exception as e:
        print(f"  [ERROR] Figure 1 failed: {e}")

    try:
        figure_km_survival()
    except Exception as e:
        print(f"  [ERROR] Figure 2 failed: {e}")

    try:
        figure_stage_distribution(cohort_df)
    except Exception as e:
        print(f"  [ERROR] Figure 3 failed: {e}")

    try:
        figure_mutation_spectrum(cohort_df)
    except Exception as e:
        print(f"  [ERROR] Figure 4 failed: {e}")

    try:
        figure_complication_rates(cohort_df)
    except Exception as e:
        print(f"  [ERROR] Figure 5 failed: {e}")

    print(f"\n  All figures saved to {FIGURES_DIR}")


if __name__ == "__main__":
    main()
