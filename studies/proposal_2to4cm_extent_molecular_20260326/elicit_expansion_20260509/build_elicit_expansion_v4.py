#!/usr/bin/env python3
"""
build_elicit_expansion_v4.py — cohort flow artifact for EXT2-4 v4 (2026-05-13).

Outputs:
  cohort_flow_bq.csv  (conceptual SSOT aligned to BQ-derived counts from ext2_4_v4_derive_tables.py)
  figures/fig1_cohort_flow_v4.png
"""
import json
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
FIG = ROOT / "figures"
FIG.mkdir(exist_ok=True)

cohort_flow = [
    ("All patients spine (canonical / manuscript cohort v1 rollup)", 10_871),
    ("Patients with first surgery resolved (study spine)", 8_731),
    ("Initial hemithyroidectomy or total thyroidectomy; surgery year 1999–2025", 8_368),
    ("    initial hemithyroidectomy only", 3_809),
    ("    initial total thyroidectomy", 4_559),
    (
        "EXT2-4 v4 primary: ≥1 US nodule 2.0–4.0 cm (canonical_us_nodule_v2.size_cm_max) "
        "with exam_date ≤ DATE(surg_first_date)",
        765,
    ),
    (
        "v4 STRICT sensitivity arm — exclude suspicious CT/MRI neck nodes "
        "+ Bethesda VI lymph-node-directed FNA (pre-operative)",
        654,
    ),
]


def write_cohort_flow_csv() -> None:
    outp = ROOT / "cohort_flow_bq.csv"
    with outp.open("w", newline="", encoding="utf-8") as fh:
        import csv

        w = csv.writer(fh)
        w.writerow(["Step", "n"])
        for label, n in cohort_flow:
            w.writerow([label, n])


def make_flow_figure() -> None:
    fig, ax = plt.subplots(figsize=(11, 7), dpi=200)
    labels = [c[0] for c in cohort_flow]
    counts = [c[1] for c in cohort_flow]
    y_pos = list(range(len(labels)))[::-1]
    bars = ax.barh(y_pos, counts, color="#34495e", edgecolor="white")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xscale("log")
    ax.set_xlabel("n (log scale)")
    ax.set_title(
        "EXT2-4 v4 cohort flow (BigQuery-derived counts; Logan decision 2026-05-14)",
        fontsize=10,
    )
    for bar, n in zip(bars, counts):
        ax.text(
            n * 1.05,
            bar.get_y() + bar.get_height() / 2,
            f"n={n:,}",
            va="center",
            fontsize=7,
        )
    plt.tight_layout()
    fig.savefig(FIG / "fig1_cohort_flow_v4.png", dpi=200, bbox_inches="tight")
    fig.savefig(FIG / "fig1_cohort_flow_v4.pdf", bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    write_cohort_flow_csv()
    make_flow_figure()
    print(json.dumps({"wrote": ["cohort_flow_bq.csv", "figures/fig1_cohort_flow_v4.png"]}, indent=2))
