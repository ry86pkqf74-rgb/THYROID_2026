#!/usr/bin/env python3
"""Generate 06_figures/ for M025 v2.0 (nodule-level, 300 DPI PNG + CSV sidecars)."""

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
ROM_CSV = os.path.join(OUT_ANALYSIS, "m025v2_per_tr_rom_with_ci.csv")

C1 = "#009E73"
C2 = "#56B4E9"
C3 = "#E69F00"
C_PAT = "#BBBBBB"


def _strict_mask(df: pd.DataFrame) -> pd.Series:
    col = "analytic_eligible_strict_acr_pernodule"
    if col not in df.columns:
        raise SystemExit(f"Missing {col} on parquet; run build_m025_tables.py first.")
    return df[col].map(lambda v: v is True or str(v).lower() in ("true", "t", "1", "yes"))


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
    strict = _strict_mask(df)
    dfx = df.loc[strict].copy()
    os.makedirs(FIG, exist_ok=True)

    snap_path = os.path.join(OUT_ANALYSIS, "m025v2_run_snapshot.json")
    snap = json.loads(open(snap_path, encoding="utf-8").read()) if os.path.isfile(snap_path) else {}

    order_tr = ["TR1", "TR2", "TR3", "TR4", "TR5"]

    n_all = int(snap.get("n_total_rows", len(df)))
    n_strict = int(snap.get("n_strict_acr_analytic_eligible", strict.sum()))
    n_tr = int(dfx["tr_rank"].notna().sum())
    n_mal = int(dfx["y_mal"].astype(bool).sum()) if "y_mal" in dfx.columns else 0

    flow_rows = pd.DataFrame(
        {
            "step": [
                "All rows in cohort_m025_nodule_level_v1",
                "Strict ACR analytic-eligible nodules",
                "Strict nodules with ordinal TR (ACR2017 category)",
                "Strict nodules path-malignant (nodule grain)",
            ],
            "n": [n_all, n_strict, n_tr, n_mal],
        }
    )
    flow_rows.to_csv(os.path.join(FIG, "m025v2_fig1_flow_data.csv"), index=False)

    fig, ax = plt.subplots(figsize=(7.8, 3.2))
    ax.axis("off")
    for i, (s, nn) in enumerate(zip(flow_rows["step"], flow_rows["n"], strict=True)):
        y = 0.88 - i * 0.22
        ax.text(0.04, y, f"{i + 1}. {s}", fontsize=10.8, va="top")
        ax.text(0.94, y, f"n={nn:,}", fontsize=11, va="top", ha="right", fontweight="bold")
    ax.set_title("M025 v2.0 — Nodule-level cohort assembly (strict ACR subset)")
    fig.savefig(os.path.join(FIG, "m025v2_fig1_cohort_flow.png"), dpi=300, bbox_inches="tight")
    plt.close(fig)

    # Fig 3 — nodule ROM by TR (strict)
    pts = []
    for lab in order_tr:
        s = dfx[dfx["tr_label"] == lab] if "tr_label" in dfx.columns else pd.DataFrame()
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
        g.to_csv(os.path.join(FIG, "m025v2_fig3_rom_by_bucket_data.csv"), index=False)

        x = np.arange(len(g))
        fig, ax = plt.subplots(figsize=(7.2, 4.2))
        y = g["ROM_pct"].values
        err_lo = y - g["ci_lo_pct"].values
        err_hi = g["ci_hi_pct"].values - y
        ax.bar(x, y, color=C1, edgecolor="black", linewidth=0.55, label="Nodule-level ROM (strict)")
        ax.errorbar(x, y, yerr=[err_lo, err_hi], fmt="none", color="black", capsize=4)
        ax.set_xticks(x)
        ax.set_xticklabels(list(g["TIRADS"]))
        ax.set_xlabel("TI-RADS category (per-nodule ACR 2017)")
        ax.set_ylabel("Nodule-level malignancy rate % (Wilson 95% CI)")
        ax.set_title("M025 v2.0 — Path-proven malignancy rate by TI-RADS (nodule grain)")
        for i, r in enumerate(g.itertuples(), 0):
            ax.text(i, min(103, float(r.ci_hi_pct) + 2.0), f"n={int(r.n)}", ha="center", fontsize=8)
        fig.tight_layout()
        fig.savefig(os.path.join(FIG, "m025v2_fig3_rom_by_bucket.png"), dpi=300)
        plt.close(fig)

    # Fig 3b — patient vs nodule ROM (grouped bars)
    if os.path.isfile(ROM_CSV):
        rom = pd.read_csv(ROM_CSV)
        plot_tr = ["TR2", "TR3", "TR4", "TR5"]
        sub = rom[rom["tirads"].isin(plot_tr)].copy()
        if not sub.empty:
            sub.to_csv(os.path.join(FIG, "m025v2_fig3b_patient_vs_nodule_rom_data.csv"), index=False)
            x = np.arange(len(sub))
            w = 0.35
            fig, ax = plt.subplots(figsize=(8.2, 4.5))
            p_rom = sub["patient_rom_pct"].fillna(0).values
            n_rom = sub["nodule_rom_pct"].fillna(0).values
            ax.bar(x - w / 2, p_rom, width=w, label="Patient-level (v1.0 spine)", color=C_PAT, edgecolor="black", linewidth=0.4)
            ax.bar(x + w / 2, n_rom, width=w, label="Nodule-level strict", color=C1, edgecolor="black", linewidth=0.4)
            if "patient_lo_95" in sub.columns:
                p_lo = sub["patient_rom_pct"] - sub["patient_lo_95"]
                p_hi = sub["patient_hi_95"] - sub["patient_rom_pct"]
                n_lo = sub["nodule_rom_pct"] - sub["nodule_lo_95"]
                n_hi = sub["nodule_hi_95"] - sub["nodule_rom_pct"]
                ax.errorbar(x - w / 2, p_rom, yerr=[p_lo, p_hi], fmt="none", color="dimgray", capsize=3)
                ax.errorbar(x + w / 2, n_rom, yerr=[n_lo, n_hi], fmt="none", color="black", capsize=3)
            ax.set_xticks(x)
            ax.set_xticklabels(list(sub["tirads"]))
            ax.set_xlabel("TI-RADS category")
            ax.set_ylabel("Risk of malignancy % (Wilson 95% CI)")
            ax.set_title("Patient-level vs nodule-level ROM (operative cohort)")
            ax.legend(loc="upper left")
            fig.tight_layout()
            fig.savefig(os.path.join(FIG, "m025v2_fig3b_patient_vs_nodule_rom.png"), dpi=300)
            plt.close(fig)

    # Fig 4 — Bethesda × TR heatmap (counts)
    if "tr_label" in dfx.columns and "bethesda_bucket" in dfx.columns:
        use = dfx[dfx["tr_label"].isin(order_tr)]
        ct = pd.crosstab(use["bethesda_bucket"], use["tr_label"])
        cols = [c for c in order_tr if c in ct.columns]
        ct = ct[[c for c in cols]]
        ct.to_csv(os.path.join(FIG, "m025v2_fig4_heatmap_counts_data.csv"))
        vals = ct.to_numpy(dtype=float)
        fig, ax = plt.subplots(figsize=(7.5, 4.8))
        im = ax.imshow(vals, cmap="Blues", aspect="auto")
        ax.set_xticks(np.arange(len(cols)), labels=cols)
        ax.set_yticks(np.arange(len(ct.index)), labels=list(ct.index))
        ax.set_xlabel("TI-RADS (nodule ACR 2017)")
        ax.set_ylabel("Bethesda bucket")
        for i in range(vals.shape[0]):
            for j in range(vals.shape[1]):
                ax.text(
                    j,
                    i,
                    int(vals[i, j]),
                    ha="center",
                    va="center",
                    color="black" if vals[i, j] < vals.max() / 2 else "white",
                    fontsize=9,
                )
        fig.colorbar(im, ax=ax, fraction=0.046)
        ax.set_title("M025 v2.0 — Bethesda × TI-RADS (strict nodule grain, counts)")
        fig.tight_layout()
        fig.savefig(os.path.join(FIG, "m025v2_fig4_tr_bethesda_heatmap.png"), dpi=300)
        plt.close(fig)

    # Fig 2 — ROC (nodule strict); optional patient-level reference curve
    roc_n = os.path.join(OUT_ANALYSIS, "m025v2_supp_ROC_curve_points.csv")
    summ_n = os.path.join(OUT_ANALYSIS, "m025v2_supp_ROC_summary.csv")
    roc_p_path = os.path.join(OUT_ANALYSIS, "m025v2_supp_ROC_patient_curve_points.csv")
    auc_n_txt = ""
    auc_p_txt = ""
    if os.path.isfile(summ_n):
        su = pd.read_csv(summ_n)
        row = su[su["metric"] == "roc_auc_ordinal_rank_nodule_strict"]
        if not row.empty:
            auc_n_txt = "%.4f" % float(row["value"].iloc[0])
    summ_p = os.path.join(OUT_ANALYSIS, "m025v2_supp_ROC_patient_summary.csv")
    if os.path.isfile(summ_p):
        su = pd.read_csv(summ_p)
        row = su[su["metric"] == "roc_auc_ordinal_rank_patient_level"]
        if not row.empty:
            auc_p_txt = "%.4f" % float(row["value"].iloc[0])

    if os.path.isfile(roc_n):
        roc = pd.read_csv(roc_n)
        roc.to_csv(os.path.join(FIG, "m025v2_fig2_roc_curve_data.csv"), index=False)
        fig, ax = plt.subplots(figsize=(5.8, 5.2))
        if os.path.isfile(roc_p_path):
            rocp = pd.read_csv(roc_p_path)
            ax.plot(rocp["fpr"], rocp["tpr"], color=C_PAT, linewidth=1.8, linestyle="--", label="Patient-level (TR rank)")
        ax.plot(roc["fpr"], roc["tpr"], color=C2, linewidth=2, label="Nodule-level strict")
        ax.plot([0, 1], [0, 1], color="gray", linestyle="--", linewidth=1)
        ax.set_xlabel("False positive rate")
        ax.set_ylabel("True positive rate")
        title = "M025 v2.0 — ROC (ordinal TI-RADS rank, nodule grain)"
        if auc_n_txt:
            title += f" AUC={auc_n_txt}"
        if auc_p_txt:
            title += f" | patient AUC={auc_p_txt}"
        ax.set_title(title)
        ax.legend(loc="lower right", fontsize=8)
        ax.axis("square")
        fig.tight_layout()
        fig.savefig(os.path.join(FIG, "m025v2_fig2_roc_curve.png"), dpi=300)
        plt.close(fig)
        print("Wrote ROC figure(s).")
    else:
        print("ROC figure skipped (no curve CSV).")

    print("M025 v2.0 figures done.")


if __name__ == "__main__":
    main()
