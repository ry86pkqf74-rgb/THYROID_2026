#!/usr/bin/env python3
"""
mig_274 — Render M044 Figures 2–3 from Snowflake-derived CSV extracts.

Inputs (repo-relative):
  snowflake_trial/reports/m044_km_curves_data.csv
  snowflake_trial/reports/m044_forest_plot_data.csv

Outputs:
  M044_submission_package_v1_0/06_figures/m044_fig2_km_by_ete.{png,svg}
  M044_submission_package_v1_0/06_figures/m044_fig3_forest_cox_multivariable.{png,svg}

Idempotent: overwriting figures on each run.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.transforms as mtransforms
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_KM = REPO_ROOT / "snowflake_trial/reports/m044_km_curves_data.csv"
DEFAULT_FP = REPO_ROOT / "snowflake_trial/reports/m044_forest_plot_data.csv"
OUT_DIR = REPO_ROOT / "M044_submission_package_v1_0/06_figures"

_STRATA_ORDER = ["none", "microscopic", "gross"]
_STRATA_DISPLAY = {
    "none": "No / negative ETE",
    "microscopic": "Microscopic ETE",
    "gross": "Gross ETE",
}
_COL = {"none": "#999999", "microscopic": "#1F77B4", "gross": "#D62728"}

LOG_RANK_P_LABEL = r"Log-rank $p$ = 0.001"


def _configure_matplotlib() -> None:
    plt.rcParams["figure.dpi"] = 300
    plt.rcParams["savefig.dpi"] = 300
    plt.rcParams["font.family"] = "sans-serif"
    plt.rcParams["font.size"] = 11


def _num(v) -> float:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return float("nan")
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.lower() in {"nan", "none"}:
            return float("nan")
        try:
            return float(s)
        except ValueError:
            return float("nan")
    return float(v)


def _prepend_t0_curve(sub: pd.DataFrame) -> pd.DataFrame:
    """Kaplan–Meier convention: survival 1 at t=0; n_at_risk = cohort size."""
    if sub.empty:
        return sub
    row0 = sub.iloc[0]
    n0 = int(row0["n_total"])
    pre = pd.DataFrame(
        [
            {
                "ete_strata": row0["ete_strata"],
                "n_total": n0,
                "time_years": 0.0,
                "survival": 1.0,
                "ci_lo": np.nan,
                "ci_hi": np.nan,
                "n_at_risk": n0,
                "cumulative_events": 0,
            }
        ]
    )
    sub2 = pd.concat([pre, sub], ignore_index=True)
    return sub2.sort_values("time_years")


def _n_at_grid(sub: pd.DataFrame, yr: float) -> int:
    """Latest n_at_risk at or before yr (yr=0 uses first row post-prepend => n_total)."""
    past = sub[sub["time_years"] <= yr + 1e-9]
    if past.empty:
        return int(sub.iloc[0]["n_total"])
    return int(past.iloc[-1]["n_at_risk"])


def plot_fig2_km(km_csv: Path, out_dir: Path, log_rank_label: str) -> tuple[Path, Path]:
    km = pd.read_csv(km_csv)
    km["ete_strata"] = km["ete_strata"].astype(str).str.strip().str.lower()
    for c in ["survival", "ci_lo", "ci_hi", "time_years", "n_at_risk", "n_total"]:
        km[c] = km[c].apply(_num)

    fig = plt.figure(figsize=(7.25, 6.25))
    gs = fig.add_gridspec(2, 1, height_ratios=[4.3, 1.15], hspace=0.08)
    ax = fig.add_subplot(gs[0, 0])
    ax_tbl = fig.add_subplot(gs[1, 0])
    ax_tbl.axis("off")

    grids = [0.0, 2.0, 5.0, 10.0]
    table_rows = []

    has_ci = km["ci_lo"].notna().any() and km["ci_hi"].notna().any()

    for key in _STRATA_ORDER:
        raw = km[km["ete_strata"] == key].sort_values("time_years")
        if raw.empty:
            continue
        sub = _prepend_t0_curve(raw)
        color = _COL[key]
        n_tot = int(sub.iloc[0]["n_total"])
        label = f"{_STRATA_DISPLAY[key]} (n={n_tot})"

        ts = sub["time_years"].to_numpy(dtype=float)
        sv = sub["survival"].to_numpy(dtype=float)
        ax.step(ts, sv, where="post", label=label, color=color, linewidth=2)

        lo = sub["ci_lo"].to_numpy(dtype=float)
        hi = sub["ci_hi"].to_numpy(dtype=float)
        if has_ci and np.isfinite(lo).any():
            finite = np.isfinite(lo) & np.isfinite(hi)
            if finite.any():
                ax.fill_between(
                    ts,
                    np.where(finite, lo, np.nan),
                    np.where(finite, hi, np.nan),
                    step="post",
                    alpha=0.15,
                    color=color,
                    linewidth=0,
                )

        row = [_STRATA_DISPLAY[key]] + [_n_at_grid(sub, g) for g in grids]
        table_rows.append(row)

    ax.set_xlim(0, 15)
    ax.set_ylim(0.45, 1.02)
    ax.set_xlabel("Time (years)")
    ax.set_ylabel("Recurrence-free survival")
    ax.set_title("Figure 2. Recurrence-free survival by ETE stratum")
    ax.grid(alpha=0.3)
    ax.legend(loc="lower left", framealpha=0.95, fontsize=9)
    ax.text(
        0.98,
        0.04,
        log_rank_label,
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=10,
    )

    col_labels = ["Stratum"] + [f"n at risk ({int(g)} y)" if g > 0 else "n at risk (0 y)" for g in grids]
    tbl = ax_tbl.table(
        cellText=table_rows,
        colLabels=col_labels,
        loc="upper center",
        cellLoc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1.05, 1.55)

    out_png = out_dir / "m044_fig2_km_by_ete.png"
    out_svg = out_dir / "m044_fig2_km_by_ete.svg"
    fig.savefig(out_png, dpi=300, bbox_inches="tight", facecolor="white")
    fig.savefig(out_svg, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_png, out_svg


def _fmt_hr(ci_lo: float, hr: float, ci_hi: float) -> str:
    return f"{hr:.3f} ({ci_lo:.3f}–{ci_hi:.3f})"


def _fmt_p_display(raw) -> str:
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return ""
    if isinstance(raw, str):
        s = raw.strip()
        return s if s else ""
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return str(raw)
    if v < 0.0001:
        return "<0.0001"
    if v < 0.001:
        return f"{v:.3f}".rstrip("0").rstrip(".")
    if v < 0.01:
        return f"{v:.4f}".rstrip("0").rstrip(".")
    return f"{v:.4f}".rstrip("0").rstrip(".")


def plot_fig3_forest(fp_csv: Path, out_dir: Path) -> tuple[Path, Path]:
    fp = pd.read_csv(fp_csv)
    fp["predictor"] = fp["predictor"].astype(str)
    for c in ["hr", "ci_lo", "ci_hi"]:
        fp[c] = fp[c].apply(_num)
    fp["p_value_disp"] = fp["p_value"].map(_fmt_p_display)

    def _sig_mask(row) -> bool:
        p_raw = row["p_value"]
        if isinstance(p_raw, str):
            if "<" in p_raw.replace(" ", ""):
                return True
            try:
                p_raw = float(p_raw.replace("<", "").strip())
            except ValueError:
                return False
        try:
            return float(p_raw) < 0.05
        except (TypeError, ValueError):
            return False

    fp["_sig"] = fp.apply(_sig_mask, axis=1)

    n = len(fp)
    fig_height = max(6.0, 0.55 * float(n) + 2.0)
    fig, (ax, ax_ann) = plt.subplots(
        1,
        2,
        figsize=(11.25, fig_height),
        gridspec_kw={"width_ratios": [2.15, 1.35], "wspace": 0.04},
    )

    x_lo = np.clip(fp["ci_lo"].to_numpy(), 1e-3, np.inf)
    x_hi = np.clip(fp["ci_hi"].to_numpy(), 1e-3, np.inf)
    x_pt = np.clip(fp["hr"].to_numpy(), 1e-3, np.inf)

    y = np.arange(n)
    for i in range(n):
        color = "#1F77B4" if fp.iloc[i]["_sig"] else "#444444"
        ax.plot(
            [x_lo[i], x_hi[i]],
            [i, i],
            color=color,
            linewidth=1.85,
            solid_capstyle="round",
        )
        ax.scatter(
            x_pt[i],
            i,
            s=90,
            color=color,
            zorder=3,
            edgecolors="black",
            linewidths=0.75,
            marker="s",
        )

    ax.axvline(1.0, color="#C0392B", linestyle="--", linewidth=1.0, alpha=0.72)

    ax.set_yticks(y)
    lbls = list(fp["predictor"])
    ax.set_yticklabels(lbls, fontsize=10)
    for i in range(len(lbls)):
        ax.get_yticklabels()[i].set_fontweight("bold" if fp.iloc[i]["_sig"] else "normal")

    ax.set_xticks([0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0])
    ax.set_xlim(0.1, 10.0)
    ax.set_xscale("log")
    ax.invert_yaxis()

    ax.set_xlabel("Hazard ratio (95% CI, log scale)")
    ax.set_title("Figure 3. Multivariable Cox model (hazard ratios with 95% CI)")
    ax.grid(axis="x", alpha=0.28, which="major")

    ax_ann.sharey(ax)
    ax_ann.tick_params(axis="y", length=0, labelleft=False, left=False)
    for spine in ax_ann.spines.values():
        spine.set_visible(False)
    ax_ann.set_xticks([])
    ax_ann.set_xlim(0, 1)

    ax_ann.text(
        0.02,
        1.01,
        "HR (95% CI)",
        transform=ax_ann.transAxes,
        fontsize=10,
        fontweight="bold",
        va="bottom",
        ha="left",
    )
    ax_ann.text(
        0.62,
        1.01,
        r"$p$-value",
        transform=ax_ann.transAxes,
        fontsize=10,
        fontweight="bold",
        va="bottom",
        ha="left",
    )

    trans_y = mtransforms.blended_transform_factory(ax_ann.transAxes, ax.transData)

    for i in range(n):
        r = fp.iloc[i]
        sig = bool(r["_sig"])
        clr = "#1F77B4" if sig else "#444444"
        wt = "bold" if sig else "normal"
        hr_part = _fmt_hr(float(r["ci_lo"]), float(r["hr"]), float(r["ci_hi"]))
        pv_disp = str(r["p_value_disp"])
        ax_ann.text(
            0.02,
            i,
            hr_part,
            transform=trans_y,
            fontsize=9,
            va="center",
            ha="left",
            color=clr,
            fontweight=wt,
        )
        ax_ann.text(
            0.62,
            i,
            pv_disp,
            transform=trans_y,
            fontsize=9,
            va="center",
            ha="left",
            color=clr,
            fontweight=wt,
        )

    out_png = out_dir / "m044_fig3_forest_cox_multivariable.png"
    out_svg = out_dir / "m044_fig3_forest_cox_multivariable.svg"
    fig.savefig(out_png, dpi=300, bbox_inches="tight", facecolor="white")
    fig.savefig(out_svg, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_png, out_svg


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="M044 mig_274 KM + forest figures from CSV extracts.")
    p.add_argument("--km-csv", type=Path, default=DEFAULT_KM)
    p.add_argument("--forest-csv", type=Path, default=DEFAULT_FP)
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    p.add_argument(
        "--log-rank-label",
        type=str,
        default=LOG_RANK_P_LABEL,
        help="Exact annotation string for KM panel.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    _configure_matplotlib()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    km_csv = Path(args.km_csv)
    fp_csv = Path(args.forest_csv)
    if not km_csv.is_file():
        print(f"ERROR: missing KM CSV: {km_csv}", file=sys.stderr)
        return 1
    if not fp_csv.is_file():
        print(f"ERROR: missing forest CSV: {fp_csv}", file=sys.stderr)
        return 1

    p2, s2 = plot_fig2_km(km_csv, out_dir, args.log_rank_label)
    p3, s3 = plot_fig3_forest(fp_csv, out_dir)
    print("Wrote:")
    for p in (p2, s2, p3, s3):
        print(f"  {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
