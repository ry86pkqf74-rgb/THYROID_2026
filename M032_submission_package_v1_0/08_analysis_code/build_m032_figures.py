#!/usr/bin/env python3
"""
build_m032_figures.py
=====================
Generates 4 manuscript figures (300 DPI PNG + CSV data source).

Figures:
  1. Cohort flow diagram (CONSORT-style)
  2. Era × malignancy rate (line chart with Wilson CI bands)
  3. TNM stage distribution by era (stacked bar)
  4. Smoking prevalence trend by era

Run from repo root:
    .venv/bin/python M032_submission_package_v1_0/08_analysis_code/build_m032_figures.py

Output dir: M032_submission_package_v1_0/06_figures/
"""
import sys, os, datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import duckdb
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
from scipy import stats

from motherduck_client import get_token

FIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "06_figures")
os.makedirs(FIG_DIR, exist_ok=True)

# Brand palette (colorblind-safe)
C = {
    'navy':    '#1F4E79',
    'blue':    '#2E75B6',
    'ltblue':  '#9DC3E6',
    'teal':    '#00B0F0',
    'green':   '#70AD47',
    'orange':  '#ED7D31',
    'red':     '#FF0000',
    'gray':    '#595959',
    'ltgray':  '#CCCCCC',
}
ERA_LABELS = {
    'A_1999_2004': '1999–2004',
    'B_2005_2009': '2005–2009',
    'C_2010_2014': '2010–2014',
    'D_2015_2019': '2015–2019',
    'E_2020_2025': '2020–2025',
}
ERA_ORDER = list(ERA_LABELS.keys())
ERA_CASE = """
CASE
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 1999 AND 2004 THEN 'A_1999_2004'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2005 AND 2009 THEN 'B_2005_2009'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2010 AND 2014 THEN 'C_2010_2014'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2015 AND 2019 THEN 'D_2015_2019'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2020 AND 2025 THEN 'E_2020_2025'
  ELSE 'F_unknown'
END AS surgery_era
"""

def connect():
    tok = get_token()
    return duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={tok}")


def wilson_ci(k, n, z=1.96):
    """Wilson score confidence interval."""
    if n == 0:
        return 0.0, 0.0
    p = k / n
    denom = 1 + z**2 / n
    center = (p + z**2 / (2*n)) / denom
    margin = z * np.sqrt(p*(1-p)/n + z**2/(4*n**2)) / denom
    return max(0, center - margin), min(1, center + margin)


def save(fig, name, dpi=300):
    path = os.path.join(FIG_DIR, name)
    fig.savefig(path, dpi=dpi, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f"[OK] {path}")
    return path


# ── Figure 1: Cohort Flow ─────────────────────────────────────────────────────
def fig1_cohort_flow(con):
    # Locked numbers
    boxes = [
        (0.5, 0.92, "All thyroid surgery patients\nn = 10,871"),
        (0.5, 0.72, "Non-malignant pathology excluded\nn = 6,758  (62.1%)"),
        (0.5, 0.52, "Malignant indication\nn = 4,113"),
        (0.5, 0.32, "NIFTP/UMP-only excluded (mig_186b)\nn = 95  (2.3%)"),
        (0.5, 0.12, "Analytic malignant cohort\nn = 4,018  (37.0% of total)"),
    ]
    fig, ax = plt.subplots(figsize=(7, 10))
    ax.axis('off')

    colors = [C['navy'], C['ltgray'], C['blue'], C['ltgray'], C['green']]
    txt_colors = ['white', C['gray'], 'white', C['gray'], 'white']

    for i, (x, y, txt) in enumerate(boxes):
        is_exclusion = i in (1, 3)
        xoff = 0.28 if is_exclusion else 0.0
        bx = x + xoff - 0.5 if is_exclusion else x - 0.35
        bw = 0.52 if is_exclusion else 0.7
        fc = colors[i]
        rect = FancyBboxPatch((bx, y-0.07), bw, 0.12,
                               boxstyle="round,pad=0.01", linewidth=1.5,
                               edgecolor=C['navy'], facecolor=fc, zorder=2)
        ax.add_patch(rect)
        ax.text(bx + bw/2, y-0.01, txt, ha='center', va='center',
                fontsize=10, color=txt_colors[i], fontweight='bold' if not is_exclusion else 'normal',
                zorder=3, wrap=True)

    # Arrows
    arrow_x = 0.5
    for i, (_, y, _) in enumerate(boxes[:-1]):
        next_y = boxes[i+1][1]
        if i not in (0, 2):  # skip exclusion boxes
            continue
        ax.annotate('', xy=(arrow_x, next_y+0.07), xytext=(arrow_x, y-0.07),
                    arrowprops=dict(arrowstyle='->', color=C['navy'], lw=2), zorder=4)
        # Side arrows to exclusion boxes
        if i == 0:
            ax.annotate('', xy=(0.64, boxes[1][1]), xytext=(arrow_x, (y + boxes[1][1])/2),
                        arrowprops=dict(arrowstyle='->', color=C['gray'], lw=1.5, linestyle='dashed'), zorder=4)
        if i == 2:
            ax.annotate('', xy=(0.64, boxes[3][1]), xytext=(arrow_x, (y + boxes[3][1])/2),
                        arrowprops=dict(arrowstyle='->', color=C['gray'], lw=1.5, linestyle='dashed'), zorder=4)

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_title("Figure 1 — M032 Cohort Flow\n25-Year Single-Institution Thyroid Surgery Cohort (1999–2025)",
                 fontsize=12, fontweight='bold', color=C['navy'], pad=12)
    ax.text(0.5, 0.01, f"Generated: {datetime.date.today()} | mig_290 | thyroid_canonical_publication_v1_0",
            ha='center', va='bottom', fontsize=7, color=C['gray'])
    return save(fig, "Figure1_CohortFlow.png")


# ── Figure 2: Malignancy Rate by Era with CI ──────────────────────────────────
def fig2_malignancy_rate(con):
    sql = f"""
    WITH b AS (SELECT *, {ERA_CASE}
               FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
               WHERE surgery_era != 'F_unknown')
    SELECT surgery_era,
           COUNT(*) AS n_total,
           COUNT(*) FILTER (WHERE is_malignant=TRUE) AS n_malig
    FROM b GROUP BY surgery_era ORDER BY surgery_era
    """
    df = con.execute(sql).fetchdf()
    df['pct'] = df['n_malig'] / df['n_total'] * 100
    df['ci_lo'] = df.apply(lambda r: wilson_ci(r.n_malig, r.n_total)[0]*100, axis=1)
    df['ci_hi'] = df.apply(lambda r: wilson_ci(r.n_malig, r.n_total)[1]*100, axis=1)
    df['era_label'] = df['surgery_era'].map(ERA_LABELS)

    df.to_csv(os.path.join(FIG_DIR, "Fig2_malignancy_rate_data.csv"), index=False)

    fig, ax = plt.subplots(figsize=(9, 5.5))
    xs = range(len(df))
    ax.fill_between(xs, df['ci_lo'], df['ci_hi'], alpha=0.18, color=C['blue'], label='95% Wilson CI')
    ax.plot(xs, df['pct'], 'o-', color=C['navy'], linewidth=2.5, markersize=8, zorder=3, label='Malignancy rate (%)')
    for i, row in df.iterrows():
        ax.annotate(f"{row.pct:.1f}%\n(n={row.n_malig:,})",
                    (i, row.pct), textcoords='offset points', xytext=(0, 12),
                    ha='center', fontsize=9, color=C['navy'], fontweight='bold')
    ax.bar(xs, df['n_total'] / df['n_total'].max() * 15, color=C['ltblue'], alpha=0.35,
           label='Relative cohort size')

    ax.set_xticks(xs)
    ax.set_xticklabels(df['era_label'], fontsize=10)
    ax.set_ylabel("Malignancy Rate (%)", fontsize=11)
    ax.set_ylim(0, 55)
    ax.set_title("Figure 2 — Malignancy Rate by Surgical Era (1999–2025)\nn = 10,871 total; M032 Descriptive Cohort",
                 fontsize=12, fontweight='bold', color=C['navy'])
    ax.legend(fontsize=9, loc='upper left')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.text(0.99, 0.01, f"Generated: {datetime.date.today()} | mig_290",
            ha='right', va='bottom', transform=ax.transAxes, fontsize=7, color=C['gray'])
    return save(fig, "Figure2_MalignancyRateByEra.png")


# ── Figure 3: Stage Distribution by Era ──────────────────────────────────────
def fig3_stage_distribution(con):
    sql = f"""
    WITH b AS (SELECT *,
                 {ERA_CASE},
                 CASE
                   WHEN ajcc8_stage_group='I'   THEN 'Stage I'
                   WHEN ajcc8_stage_group='II'  THEN 'Stage II'
                   WHEN ajcc8_stage_group='III' THEN 'Stage III'
                   WHEN ajcc8_stage_group IN ('IVA','IVB','IVC') OR ajcc8_stage_group LIKE 'IV%' THEN 'Stage IV'
                   ELSE 'Unknown/Unstaged'
                 END AS stage_group
               FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
               WHERE is_malignant=TRUE AND surgery_era != 'F_unknown')
    SELECT surgery_era, stage_group, COUNT(*) AS n
    FROM b GROUP BY surgery_era, stage_group ORDER BY surgery_era
    """
    df = con.execute(sql).fetchdf()
    pivot = df.pivot(index='surgery_era', columns='stage_group', values='n').fillna(0)
    pivot = pivot.reindex(ERA_ORDER, fill_value=0)
    # Normalize to %
    pct = pivot.div(pivot.sum(axis=1), axis=0) * 100

    df.to_csv(os.path.join(FIG_DIR, "Fig3_stage_distribution_data.csv"), index=False)

    stage_colors = {'Stage I': C['green'], 'Stage II': C['blue'],
                    'Stage III': C['orange'], 'Stage IV': C['navy'], 'Unknown/Unstaged': C['ltgray']}
    stage_order = ['Stage I', 'Stage II', 'Stage III', 'Stage IV', 'Unknown/Unstaged']

    fig, ax = plt.subplots(figsize=(10, 6))
    bottoms = np.zeros(len(pct))
    xs = np.arange(len(pct))
    for stage in stage_order:
        if stage not in pct.columns:
            continue
        vals = pct[stage].values
        bars = ax.bar(xs, vals, bottom=bottoms, color=stage_colors[stage],
                      label=stage, edgecolor='white', linewidth=0.5)
        for bi, (bar, val) in enumerate(zip(bars, vals)):
            if val > 4:
                ax.text(bar.get_x() + bar.get_width()/2,
                        bottoms[bi] + val/2, f"{val:.0f}%",
                        ha='center', va='center', fontsize=8.5,
                        color='white', fontweight='bold')
        bottoms += vals

    ax.set_xticks(xs)
    ax.set_xticklabels([ERA_LABELS[e] for e in ERA_ORDER], fontsize=10)
    ax.set_ylabel("Proportion of Malignant Cohort (%)", fontsize=11)
    ax.set_ylim(0, 110)
    ax.set_title("Figure 3 — AJCC 8th Ed. Stage Distribution by Surgical Era\nMalignant cohort (N=4,018); mig_290",
                 fontsize=12, fontweight='bold', color=C['navy'])
    ax.legend(loc='upper right', fontsize=9, framealpha=0.9)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.text(0.99, 0.01, f"Generated: {datetime.date.today()} | mig_290",
            ha='right', va='bottom', transform=ax.transAxes, fontsize=7, color=C['gray'])
    return save(fig, "Figure3_StageDistributionByEra.png")


# ── Figure 4: Smoking prevalence trend ───────────────────────────────────────
def fig4_smoking_trend(con):
    sql = f"""
    WITH b AS (SELECT *, {ERA_CASE}
               FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
               WHERE surgery_era != 'F_unknown')
    SELECT
      surgery_era,
      COUNT(*) AS n_total,
      COUNT(*) FILTER (WHERE LOWER(smoking_status_combined)='current') AS n_current,
      COUNT(*) FILTER (WHERE LOWER(smoking_status_combined)='former')  AS n_former,
      COUNT(*) FILTER (WHERE LOWER(smoking_status_combined)='never')   AS n_never,
      COUNT(*) FILTER (WHERE smoking_status_combined IS NOT NULL)       AS n_known
    FROM b GROUP BY surgery_era ORDER BY surgery_era
    """
    df = con.execute(sql).fetchdf()
    for status in ('current', 'former', 'never'):
        df[f'pct_{status}'] = df[f'n_{status}'] / df['n_known'].replace(0, np.nan) * 100
    df['era_label'] = df['surgery_era'].map(ERA_LABELS)
    df['pct_known'] = df['n_known'] / df['n_total'] * 100

    df.to_csv(os.path.join(FIG_DIR, "Fig4_smoking_trend_data.csv"), index=False)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.5))

    xs = range(len(df))
    # Left: smoking by status
    ax1.plot(xs, df['pct_current'], 'o-', color=C['red'],    lw=2.5, ms=7, label='Current')
    ax1.plot(xs, df['pct_former'],  's-', color=C['orange'], lw=2.5, ms=7, label='Former')
    ax1.plot(xs, df['pct_never'],   '^-', color=C['green'],  lw=2.5, ms=7, label='Never')
    ax1.set_xticks(xs)
    ax1.set_xticklabels(df['era_label'], fontsize=9, rotation=15)
    ax1.set_ylabel("% of Patients with Known Status", fontsize=10)
    ax1.set_title("Smoking Status Proportions\n(% of known)", fontsize=11, fontweight='bold', color=C['navy'])
    ax1.legend(fontsize=9)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)

    # Right: NLP coverage
    ax2.bar(xs, df['pct_known'], color=C['blue'], alpha=0.8)
    ax2.set_xticks(xs)
    ax2.set_xticklabels(df['era_label'], fontsize=9, rotation=15)
    ax2.set_ylabel("% Cohort with Known Smoking Status", fontsize=10)
    ax2.set_title("NLP Smoking Coverage by Era\n(post-mig_281)", fontsize=11, fontweight='bold', color=C['navy'])
    for i, row in df.iterrows():
        ax2.text(i, row['pct_known'] + 0.5, f"{row.pct_known:.1f}%\n(n={row.n_known:,})",
                 ha='center', fontsize=8.5, color=C['navy'])
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)

    fig.suptitle("Figure 4 — Smoking Prevalence Trend by Era (post-mig_281 NLP)\nM032 25-yr Descriptive Cohort",
                 fontsize=12, fontweight='bold', color=C['navy'], y=1.02)
    fig.text(0.99, -0.01, f"Generated: {datetime.date.today()} | mig_290",
             ha='right', fontsize=7, color=C['gray'])
    fig.tight_layout()
    return save(fig, "Figure4_SmokingTrendByEra.png")


def main():
    print("Connecting to MotherDuck…")
    con = connect()

    print("Generating Figure 1 — Cohort Flow…")
    fig1_cohort_flow(con)

    print("Generating Figure 2 — Malignancy Rate by Era…")
    fig2_malignancy_rate(con)

    print("Generating Figure 3 — Stage Distribution by Era…")
    fig3_stage_distribution(con)

    print("Generating Figure 4 — Smoking Trend by Era…")
    fig4_smoking_trend(con)

    print(f"\n[DONE] All figures saved to: {FIG_DIR}")
    con.close()


if __name__ == "__main__":
    main()
