"""
M025 v2.0 — sensitivity-arm statistics (mig_307c tables → mig_307d publication outputs).

Wilson 95% CIs for binomial proportions; per-era diagnostic metrics from analytic masters;
per-era ROC AUC (ordinal TR score, equivalent to Mann–Whitney U / rank-sum AUC).
"""

from __future__ import annotations

import os
import re
from math import sqrt

import duckdb
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

ERA_BOUNDARY_SQL = "DATE '2017-05-01'"

WINDOW_COLS = [
    ("m_w365", "rom_w365_pct", "rom_w365_lo_95", "rom_w365_hi_95"),
    ("m_w180", "rom_w180_pct", "rom_w180_lo_95", "rom_w180_hi_95"),
    ("m_w90", "rom_w90_pct", "rom_w90_lo_95", "rom_w90_hi_95"),
    ("m_w30", "rom_w30_pct", "rom_w30_lo_95", "rom_w30_hi_95"),
]


def wilson_ci(k: float | int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Return Wilson 95% CI for binomial proportion k/n on [0,1]."""
    kf = float(k)
    if n <= 0 or kf < 0:
        return (float("nan"), float("nan"))
    p = min(1.0, kf / n)
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    halfw = (z / denom) * sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (max(0.0, centre - halfw), min(1.0, centre + halfw))


def pct_ci_tuple(k: float | int, n: int, z: float = 1.96) -> tuple[float, float]:
    lo, hi = wilson_ci(k, n, z=z)
    return (round(100 * lo, 2), round(100 * hi, 2))


def add_rom_ci_lo_hi(
    df: pd.DataFrame,
    k_col: str = "n_malignant",
    n_col: str = "n_total",
) -> pd.DataFrame:
    """Append lo_95 / hi_95 (percent scale) for ROM = k_col / n_col."""

    def _row(r: pd.Series) -> pd.Series:
        n = int(r[n_col]) if pd.notna(r[n_col]) else 0
        k = float(r[k_col]) if pd.notna(r[k_col]) else 0.0
        lo, hi = pct_ci_tuple(k, n)
        return pd.Series({"lo_95": lo, "hi_95": hi})

    extra = df.apply(_row, axis=1)
    return pd.concat([df, extra], axis=1)


def augment_window_table(df: pd.DataFrame) -> pd.DataFrame:
    """Wilson CIs for each window ROM column (m_w* / n_total)."""
    out = df.copy()
    for m_col, pct_col, lo_col, hi_col in WINDOW_COLS:
        los, his = [], []
        for _, r in out.iterrows():
            n = int(r["n_total"]) if pd.notna(r["n_total"]) else 0
            k = float(r[m_col]) if pd.notna(r[m_col]) else 0.0
            lo, hi = pct_ci_tuple(k, n)
            los.append(lo)
            his.append(hi)
        out[lo_col] = los
        out[hi_col] = his
    return out


def tr_rank_from_category(cat: object) -> float:
    if cat is None or (isinstance(cat, float) and np.isnan(cat)):
        return np.nan
    s = str(cat).strip().upper()
    m = re.search(r"TR\s*(\d+)", s)
    if m:
        return float(int(m.group(1)))
    m2 = re.search(r"(\d+)", s)
    return float(int(m2.group(1))) if m2 else np.nan


def fetch_diagnostic_by_era(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Sens / Spec / PPV / NPV with Wilson CIs at TR>=TR3, TR>=TR4, TR>=TR5
    for pre_ vs post-2017, patient and nodule_strict grains.
    """
    q_pat = f"""
    WITH base AS (
      SELECT
        CASE WHEN surg_first_date < {ERA_BOUNDARY_SQL} THEN 'pre_2017' ELSE 'post_2017' END AS era,
        COALESCE(is_malignant, FALSE) AS y,
        COALESCE(predicted_pos_TR3, FALSE) AS t3,
        COALESCE(predicted_pos_TR4, FALSE) AS t4,
        COALESCE(predicted_pos_TR5, FALSE) AS t5
      FROM manuscript_workspace.m025_analytic_master_patient_v1
      WHERE surg_first_date IS NOT NULL
    ),
    long AS (
      SELECT era, 'TR>=TR3' AS threshold, y, t3 AS pred FROM base
      UNION ALL SELECT era, 'TR>=TR4', y, t4 FROM base
      UNION ALL SELECT era, 'TR>=TR5', y, t5 FROM base
    )
    SELECT
      'patient' AS grain,
      era,
      threshold,
      COUNT(*)::BIGINT AS n,
      SUM(CASE WHEN y AND pred THEN 1 ELSE 0 END)::BIGINT AS tp,
      SUM(CASE WHEN NOT y AND pred THEN 1 ELSE 0 END)::BIGINT AS fp,
      SUM(CASE WHEN y AND NOT pred THEN 1 ELSE 0 END)::BIGINT AS fn,
      SUM(CASE WHEN NOT y AND NOT pred THEN 1 ELSE 0 END)::BIGINT AS tn
    FROM long
    GROUP BY 1, 2, 3
    """
    q_nod = f"""
    WITH base AS (
      SELECT
        CASE WHEN exam_date < {ERA_BOUNDARY_SQL} THEN 'pre_2017' ELSE 'post_2017' END AS era,
        COALESCE(nodule_path_proven_malignant, FALSE) AS y,
        COALESCE(predicted_pos_TR3, FALSE) AS t3,
        COALESCE(predicted_pos_TR4, FALSE) AS t4,
        COALESCE(predicted_pos_TR5, FALSE) AS t5
      FROM manuscript_workspace.m025_analytic_master_nodule_v1
      WHERE analytic_eligible_strict_acr_pernodule = TRUE
        AND exam_date IS NOT NULL
    ),
    long AS (
      SELECT era, 'TR>=TR3' AS threshold, y, t3 AS pred FROM base
      UNION ALL SELECT era, 'TR>=TR4', y, t4 FROM base
      UNION ALL SELECT era, 'TR>=TR5', y, t5 FROM base
    )
    SELECT
      'nodule_strict' AS grain,
      era,
      threshold,
      COUNT(*)::BIGINT AS n,
      SUM(CASE WHEN y AND pred THEN 1 ELSE 0 END)::BIGINT AS tp,
      SUM(CASE WHEN NOT y AND pred THEN 1 ELSE 0 END)::BIGINT AS fp,
      SUM(CASE WHEN y AND NOT pred THEN 1 ELSE 0 END)::BIGINT AS fn,
      SUM(CASE WHEN NOT y AND NOT pred THEN 1 ELSE 0 END)::BIGINT AS tn
    FROM long
    GROUP BY 1, 2, 3
    """
    df = pd.concat(
        [con.execute(q_pat).df(), con.execute(q_nod).df()],
        ignore_index=True,
    )

    def add_metrics(row: pd.Series) -> pd.Series:
        tp, fp, fn, tn = int(row["tp"]), int(row["fp"]), int(row["fn"]), int(row["tn"])
        sens = 100.0 * tp / (tp + fn) if (tp + fn) > 0 else float("nan")
        spec = 100.0 * tn / (tn + fp) if (tn + fp) > 0 else float("nan")
        ppv = 100.0 * tp / (tp + fp) if (tp + fp) > 0 else float("nan")
        npv = 100.0 * tn / (tn + fn) if (tn + fn) > 0 else float("nan")
        sl, sh = pct_ci_tuple(tp, tp + fn)
        pl, ph = pct_ci_tuple(tn, tn + fp)
        ppl, pph = pct_ci_tuple(tp, tp + fp)
        nl, nh = pct_ci_tuple(tn, tn + fn)
        return pd.Series(
            {
                "sensitivity_pct": round(sens, 2) if sens == sens else None,
                "sensitivity_lo_95": sl,
                "sensitivity_hi_95": sh,
                "specificity_pct": round(spec, 2) if spec == spec else None,
                "specificity_lo_95": pl,
                "specificity_hi_95": ph,
                "ppv_pct": round(ppv, 2) if ppv == ppv else None,
                "ppv_lo_95": ppl,
                "ppv_hi_95": pph,
                "npv_pct": round(npv, 2) if npv == npv else None,
                "npv_lo_95": nl,
                "npv_hi_95": nh,
            }
        )

    met = df.apply(add_metrics, axis=1)
    return pd.concat([df, met], axis=1).sort_values(["grain", "era", "threshold"]).reset_index(drop=True)


def fetch_per_era_auc(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """ROC AUC using ordinal TR 1–5 as score (rank-sum / Mann–Whitney equivalent)."""
    dfp = con.execute(
        f"""
        SELECT
          CASE WHEN surg_first_date < {ERA_BOUNDARY_SQL} THEN 'pre_2017' ELSE 'post_2017' END AS era,
          max_tirads_category_ever,
          is_malignant
        FROM manuscript_workspace.m025_analytic_master_patient_v1
        WHERE surg_first_date IS NOT NULL
        """
    ).df()
    dfp["score"] = dfp["max_tirads_category_ever"].map(tr_rank_from_category)
    dfp["y"] = dfp["is_malignant"].map(
        lambda v: v is True or str(v).lower() in ("true", "t", "1")
    )

    dfn = con.execute(
        f"""
        SELECT
          CASE WHEN exam_date < {ERA_BOUNDARY_SQL} THEN 'pre_2017' ELSE 'post_2017' END AS era,
          acr2017_tirads_category,
          nodule_path_proven_malignant
        FROM manuscript_workspace.m025_analytic_master_nodule_v1
        WHERE analytic_eligible_strict_acr_pernodule = TRUE
          AND exam_date IS NOT NULL
        """
    ).df()
    dfn["score"] = dfn["acr2017_tirads_category"].map(tr_rank_from_category)
    dfn["y"] = dfn["nodule_path_proven_malignant"].map(
        lambda v: v is True or str(v).lower() in ("true", "t", "1")
    )

    rows = []
    for grain, df0 in ("patient", dfp), ("nodule_strict", dfn):
        for era in ("pre_2017", "post_2017"):
            sub = df0[df0["era"] == era].dropna(subset=["score"])
            y = sub["y"].astype(int)
            x = sub["score"].astype(float)
            n = len(sub)
            n_pos = int(y.sum())
            n_neg = n - n_pos
            if n < 2 or n_pos == 0 or n_neg == 0:
                auc = float("nan")
            else:
                auc = float(roc_auc_score(y, x))
            rows.append(
                {
                    "grain": grain,
                    "era": era,
                    "n": n,
                    "n_positive": n_pos,
                    "n_negative": n_neg,
                    "roc_auc_tr_ordinal": round(auc, 4) if auc == auc else None,
                }
            )
    return pd.DataFrame(rows)


def render_forest_rom_by_era(
    df_patient: pd.DataFrame,
    df_nodule: pd.DataFrame,
    out_png: str,
    dpi: int = 300,
) -> None:
    """Forest plot: TR4 and TR5 ROM by era, patient and nodule grains + ACR bands."""
    blocks: list[tuple[str, str, float, float, float]] = []
    for label, df in (("Patient", df_patient), ("Nodule strict", df_nodule)):
        for tr in ("TR4", "TR5"):
            for era, elab in (("pre_2017", "Pre-2017"), ("post_2017", "Post-2017")):
                sel = df[(df["tr_category"] == tr) & (df["era"] == era)]
                if sel.empty:
                    continue
                r = sel.iloc[0]
                rom_pct = float(r["rom_pct"])
                lo = float(r["lo_95"])
                hi = float(r["hi_95"])
                blocks.append((f"{label} | {tr} | {elab}", tr, rom_pct, lo, hi))

    if not blocks:
        return

    labels = [b[0] for b in blocks]
    trs = [b[1] for b in blocks]
    rom = np.array([b[2] for b in blocks], dtype=float)
    err_lo_arr = np.array([b[2] - b[3] for b in blocks], dtype=float)
    err_hi_arr = np.array([b[4] - b[2] for b in blocks], dtype=float)
    y_pos = np.arange(len(labels))[::-1]

    fig, ax = plt.subplots(figsize=(9.5, max(4.0, 0.45 * len(labels) + 1)))
    # Reference regions on the ROM (% x-axis)
    ax.axvspan(5, 20, color="#90C978", alpha=0.15, zorder=0)
    ax.axvspan(20, 65, color="#56B4E9", alpha=0.08, zorder=0)

    colors = ["#0173B2" if t == "TR4" else "#DE8F05" for t in trs]
    ax.errorbar(
        rom,
        y_pos,
        xerr=np.vstack([err_lo_arr, err_hi_arr]),
        fmt="none",
        ecolor="#333333",
        capsize=3,
        zorder=2,
    )
    for i, y in enumerate(y_pos):
        ax.plot(rom[i], y, "o", color=colors[i], markersize=8, zorder=3)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel("Risk of malignancy (%) with Wilson 95% CI", fontsize=11)
    ax.set_title(
        "M025 sensitivity — ROM by ACR-2017 era (TR4 & TR5)\n"
        "Shaded x-regions: ACR 2017 expected ROM ranges",
        fontsize=12,
    )
    xmax = float(np.nanmax(rom + err_hi_arr))
    ax.set_xlim(0, min(75.0, xmax + 8.0))
    ax.grid(axis="x", linestyle=":", alpha=0.5)
    band_leg = [
        Patch(facecolor="#90C978", alpha=0.35, label="ACR TR4 ROM band (5–20%)"),
        Patch(facecolor="#56B4E9", alpha=0.28, label="ACR TR5 ROM ref (>20%)"),
    ]
    ax.legend(handles=band_leg, loc="lower right", fontsize=8)
    fig.tight_layout()
    out_dir = os.path.dirname(out_png)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    fig.savefig(out_png, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def render_linechart_match_window(dfw: pd.DataFrame, out_png: str, dpi: int = 300) -> None:
    """TR3 / TR4 / TR5 ROM vs US-to-surgery match window."""
    windows = [365, 180, 90, 30]
    rows = {}
    for tr in ("TR3", "TR4", "TR5"):
        sub = dfw[dfw["tr_category"] == tr]
        if sub.empty:
            continue
        r0 = sub.iloc[0]
        ys = [
            float(r0["rom_w365_pct"]),
            float(r0["rom_w180_pct"]),
            float(r0["rom_w90_pct"]),
            float(r0["rom_w30_pct"]),
        ]
        rows[tr] = ys

    fig, ax = plt.subplots(figsize=(8, 5))
    palette = {"TR3": "#029E73", "TR4": "#0173B2", "TR5": "#DE8F05"}
    for tr, ys in rows.items():
        ax.plot(windows, ys, "o-", label=tr, color=palette.get(tr, "#333"), linewidth=2)

    ax.set_xticks(windows)
    ax.set_xticklabels([str(w) for w in windows])
    ax.set_xlabel("US-to-malignancy-linked surgery window (days)", fontsize=11)
    ax.set_ylabel("ROM (%), nodule strict cohort", fontsize=11)
    ax.set_title(
        "M025 sensitivity — ROM by match window (365 / 180 / 90 / 30 d)\n"
        "Denominator = all strict-eligible nodules in TI-RADS stratum",
        fontsize=12,
    )
    ax.legend(title="TI-RADS")
    ax.grid(True, linestyle=":", alpha=0.5)
    fig.tight_layout()
    os.makedirs(os.path.dirname(out_png) or ".", exist_ok=True)
    fig.savefig(out_png, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def export_sensitivity_csv_bundle(
    outdir: str,
    df_era_p: pd.DataFrame,
    df_era_n: pd.DataFrame,
    df_win: pd.DataFrame,
    df_diag: pd.DataFrame,
    df_auc: pd.DataFrame,
) -> None:
    os.makedirs(outdir, exist_ok=True)
    df_era_p.to_csv(os.path.join(outdir, "m025_sensitivity_era_patient.csv"), index=False)
    df_era_n.to_csv(os.path.join(outdir, "m025_sensitivity_era_nodule.csv"), index=False)
    df_win.to_csv(os.path.join(outdir, "m025_sensitivity_match_window.csv"), index=False)
    df_diag.to_csv(os.path.join(outdir, "m025_sensitivity_era_diagnostics_tr_thresholds.csv"), index=False)
    df_auc.to_csv(os.path.join(outdir, "m025_sensitivity_per_era_roc_auc.csv"), index=False)
