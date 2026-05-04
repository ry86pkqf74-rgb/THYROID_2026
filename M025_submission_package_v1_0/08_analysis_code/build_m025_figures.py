#!/usr/bin/env python3
"""Generate 06_figures/ for M025 (300 DPI PNG + CSV sidecars)."""

from __future__ import annotations

import json
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIG = os.path.join(PKG, "06_figures")
OUT_ANALYSIS = os.path.join(PKG, "08_analysis_outputs")
PARQUET = os.path.join(OUT_ANALYSIS, "m025_analytic_spine.parquet")

C1 = "#009E73"
C2 = "#56B4E9"
C3 = "#E69F00"


def wilson_ci(x: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n <= 0:
        return (float("nan"), float("nan"))
    phat = x / n
    denom = 1 + z**2 / n
    center = phat + z**2 / (2 * n)
    margin = z * np.sqrt((phat * (1 - phat) / n) + z**2 / (4 * n**2))
    return float((center - margin) / denom), float((center + margin) / denom)


def main():
    if not os.path.isfile(PARQUET):
        raise SystemExit(f"Missing {PARQUET}; run build_m025_tables.py first.")
    df = pd.read_parquet(PARQUET)
    os.makedirs(FIG, exist_ok=True)

    order_tr = ["TR1", "TR2", "TR3", "TR4", "TR5"]

    snap_path = os.path.join(OUT_ANALYSIS, "m025_run_snapshot.json")
    snap = json.loads(open(snap_path, encoding="utf-8").read())

    # Fig 1 — cohort flow schematic
    n_all = int(snap["n_cohort_view"])
    n_known = int(df["tr_rank"].notna().sum())
    mal = int(df["y_mal"].astype(bool).sum())
    flow_rows = pd.DataFrame(
        {
            "step": [
                "Cohort filter (cohort_m025)",
                "Patients with ordinal TR rank (resolved+worst fallback)",
                "Malignancies (gold: CPM.is_malignant)",
            ],
            "n": [n_all, n_known, mal],
        }
    )
    flow_rows.to_csv(os.path.join(FIG, "m025_fig1_flow_data.csv"), index=False)

    fig, ax = plt.subplots(figsize=(7.8, 2.9))
    ax.axis("off")
    for i, (s, nn) in enumerate(zip(flow_rows["step"], flow_rows["n"], strict=True)):
        y = 0.88 - i * 0.3
        ax.text(0.04, y, f"{i+1}. {s}", fontsize=10.8, va="top")
        ax.text(0.94, y, f"n={nn:,}", fontsize=11, va="top", ha="right", fontweight="bold")
    ax.set_title("M025 — Cohort assembly (operative TI-RADS performance)")
    fig.savefig(os.path.join(FIG, "m025_fig1_cohort_flow.png"), dpi=300, bbox_inches="tight")
    plt.close(fig)

    # Fig 3 — ROM by TR with Wilson 95% CI
    pts = []
    for lab in order_tr:
        s = df[df["tr_label"] == lab]
        if s.empty:
            continue
        n = len(s)
        nm = int(s["y_mal"].astype(bool).sum())
        lo, hi = wilson_ci(nm, n)
        pts.append({"TIRADS": lab, "n": n, "ROM": nm / n, "lo": lo, "hi": hi})
    g = pd.DataFrame(pts)
    if not g.empty:
        g["ROM_pct"] = 100 * g["ROM"]
        g["ci_lo_pct"] = 100 * g["lo"]
        g["ci_hi_pct"] = 100 * g["hi"]
        g.to_csv(os.path.join(FIG, "m025_fig3_rom_by_bucket_data.csv"), index=False)

        x = np.arange(len(g))
        fig, ax = plt.subplots(figsize=(7.2, 4.2))
        y = g["ROM_pct"].values
        err_lo = y - g["ci_lo_pct"].values
        err_hi = g["ci_hi_pct"].values - y
        ax.bar(x, y, color=C1, edgecolor="black", linewidth=0.55, label="Observed ROM (operative cohort)")
        ax.errorbar(x, y, yerr=[err_lo, err_hi], fmt="none", color="black", capsize=4)
        ax.set_xticks(x)
        ax.set_xticklabels(list(g["TIRADS"]))
        ax.set_xlabel("TI-RADS category (patient-max ordinal)")
        ax.set_ylabel("Malignancy rate % (Wilson 95% CI)")
        ax.set_title("M025 — Malignancy rate by TI-RADS bucket")
        for i, r in enumerate(g.itertuples(), 0):
            ax.text(i, min(103, float(r.ci_hi_pct) + 2.0), f"n={int(r.n)}", ha="center", fontsize=8)
        fig.tight_layout()
        fig.savefig(os.path.join(FIG, "m025_fig3_rom_by_bucket.png"), dpi=300)
        plt.close(fig)

    # Fig 4 — Bethesda × TR heatmap (counts + text)
    use = df[df["tr_label"].isin(order_tr)]
    ct = pd.crosstab(use["bethesda_bucket"], use["tr_label"])
    cols = [c for c in order_tr if c in ct.columns]
    ct = ct[cols].reindex(sorted(ct.index, key=lambda s: str(s)))
    ct.to_csv(os.path.join(FIG, "m025_fig4_heatmap_counts_data.csv"))
    vals = ct.to_numpy(dtype=float)
    fig, ax = plt.subplots(figsize=(7.5, 4.8))
    im = ax.imshow(vals, cmap="Blues", aspect="auto")
    ax.set_xticks(np.arange(len(cols)), labels=cols)
    ax.set_yticks(np.arange(len(ct.index)), labels=list(ct.index))
    ax.set_xlabel("TI-RADS")
    ax.set_ylabel("Bethesda bucket (numeric map)")
    for i in range(vals.shape[0]):
        for j in range(vals.shape[1]):
            ax.text(j, i, int(vals[i, j]), ha="center", va="center", color="black" if vals[i, j] < vals.max() / 2 else "white", fontsize=9)
    fig.colorbar(im, ax=ax, fraction=0.046)
    ax.set_title("M025 — Cross-stratification (counts)")
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "m025_fig4_tr_bethesda_heatmap.png"), dpi=300)
    plt.close(fig)

    # Fig 2 — ROC from CSV outputs
    roc_path = os.path.join(OUT_ANALYSIS, "m025_supp_ROC_curve_points.csv")
    summ_path = os.path.join(OUT_ANALYSIS, "m025_supp_ROC_summary.csv")
    if os.path.isfile(roc_path):
        roc = pd.read_csv(roc_path)
        roc.to_csv(os.path.join(FIG, "m025_fig2_roc_curve_data.csv"), index=False)
        auc_txt = ""
        if os.path.isfile(summ_path):
            s = pd.read_csv(summ_path)
            auc_txt = "%.4f" % float(s.loc[s["metric"] == "roc_auc_ordinal_rank", "value"].iloc[0])
        fig, ax = plt.subplots(figsize=(5.8, 5.2))
        ax.plot(roc["fpr"], roc["tpr"], color=C2, linewidth=2)
        ax.plot([0, 1], [0, 1], color="gray", linestyle="--", linewidth=1)
        ax.set_xlabel("False positive rate")
        ax.set_ylabel("True positive rate")
        ax.set_title("M025 — ROC (ordinal TI-RADS rank predicting malignancy)" + (f" AUC={auc_txt}" if auc_txt else ""))
        ax.axis("square")
        fig.tight_layout()
        fig.savefig(os.path.join(FIG, "m025_fig2_roc_curve.png"), dpi=300)
        plt.close(fig)
        print(f"Wrote ROC figure (+ AUC {'=' + auc_txt if auc_txt else 'n/a'})")
    else:
        stub = pd.DataFrame([{"note": "ROC skipped — rerun tables with sklearn metrics available"}])
        stub.to_csv(os.path.join(FIG, "m025_fig2_roc_curve_data.csv"), index=False)
        print("ROC figure skipped")

    print("M025 figures done.")


if __name__ == "__main__":
    main()
