#!/usr/bin/env python3
"""
M025 v2 – ACR TI-RADS Diagnostic Performance Analysis
=======================================================
Cohort: manuscript_workspace.cohort_m025_tirads_performance_v1 (N=3,375)

Outputs (studies/m025_tirads_performance/):
  tirads_diagnostic_performance.csv  — sensitivity/specificity at each threshold
  roc_data.csv                       — ROC curve points
  rom_by_tirads.csv                  — ROM with CI per TR category
  nodule_size_analysis.csv           — size × TI-RADS cross-tab
  multi_tirads_assessment.csv        — best vs worst TI-RADS comparison
  subgroup_analysis.csv              — subgroup diagnostic performance
  unnecessary_fna_analysis.csv       — ACR guideline compliance
  tirads_performance_summary.tex     — LaTeX tables
  roc_curve.png                      — ROC figure
  rom_by_bucket.png                  — ROM bar chart
  MotherDuck: manuscript_workspace.m025_tirads_analysis_v1

Usage:
  .venv/bin/python scripts/m025_tirads_analysis.py
"""

import sys, os, json
from datetime import datetime
from pathlib import Path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from scipy import stats
import warnings
warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────────────────────────────────────
OUTDIR = Path("studies/m025_tirads_performance")
OUTDIR.mkdir(parents=True, exist_ok=True)

TIRADS_ORDER = ["TR1", "TR2", "TR3", "TR4", "TR5"]
# ACR expected ROM ranges
ACR_EXPECTED = {
    "TR1": (0.0, 2.0),
    "TR2": (0.0, 5.0),
    "TR3": (0.0, 5.0),
    "TR4": (5.0, 20.0),
    "TR5": (20.0, 100.0),
}
# ACR FNA size thresholds (mm)
ACR_FNA_THRESHOLD = {"TR5": 10, "TR4": 15, "TR3": 25, "TR2": None, "TR1": None}

# ──────────────────────────────────────────────────────────────────────────────
# CONNECTION
# ──────────────────────────────────────────────────────────────────────────────
def get_connection():
    """Return MotherDuck connection using project token resolution."""
    from motherduck_client import get_token
    import duckdb
    tok = get_token()
    print(f"[auth] Token: SET, length={len(tok)}")
    conn = duckdb.connect(
        "md:thyroid_canonical_publication_v1_0",
        config={"motherduck_token": tok},
    )
    return conn


def load_data() -> pd.DataFrame:
    """Load cohort from MotherDuck, deduplicate columns."""
    conn = get_connection()
    raw = conn.execute(
        "SELECT * FROM manuscript_workspace.cohort_m025_tirads_performance_v1"
    ).fetchdf()
    conn.close()

    # Handle duplicate column names from the view JOIN artifact
    cols_seen: dict[str, int] = {}
    new_cols = []
    for c in raw.columns:
        if c in cols_seen:
            cols_seen[c] += 1
            new_cols.append(f"{c}__{cols_seen[c]}")
        else:
            cols_seen[c] = 0
            new_cols.append(c)
    raw.columns = new_cols

    # Resolve surg_first_date — keep first valid DATE
    if "surg_first_date__1" in raw.columns:
        raw["surg_first_date"] = pd.to_datetime(
            raw["surg_first_date"].fillna(raw["surg_first_date__1"])
        )

    # Coalesce tirads_n_sources_v12 (may be dup)
    if "tirads_n_sources_v12__1" in raw.columns:
        raw["tirads_n_sources_v12"] = raw["tirads_n_sources_v12"].fillna(
            raw["tirads_n_sources_v12__1"]
        )

    print(f"[load] Loaded {len(raw):,} rows, {raw.columns.tolist()[:5]}…")
    return raw


# ──────────────────────────────────────────────────────────────────────────────
# STATISTICAL HELPERS
# ──────────────────────────────────────────────────────────────────────────────
def wilson_ci(k: int, n: int, alpha: float = 0.05):
    """Wilson score confidence interval for a proportion."""
    if n == 0:
        return (0.0, 0.0)
    z = stats.norm.ppf(1 - alpha / 2)
    p = k / n
    denom = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / denom
    half = z * np.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def diagnostic_2x2(tp, fp, fn, tn):
    """Return dict of diagnostic metrics from a 2×2 table."""
    n = tp + fp + fn + tn
    sens = tp / (tp + fn) if (tp + fn) > 0 else float("nan")
    spec = tn / (tn + fp) if (tn + fp) > 0 else float("nan")
    ppv = tp / (tp + fp) if (tp + fp) > 0 else float("nan")
    npv = tn / (tn + fn) if (tn + fn) > 0 else float("nan")
    lr_pos = sens / (1 - spec) if (1 - spec) > 0 else float("nan")
    lr_neg = (1 - sens) / spec if spec > 0 else float("nan")
    dor = (tp * tn) / (fp * fn) if (fp * fn) > 0 else float("nan")

    sens_ci = wilson_ci(tp, tp + fn)
    spec_ci = wilson_ci(tn, tn + fp)
    ppv_ci = wilson_ci(tp, tp + fp)
    npv_ci = wilson_ci(tn, tn + fn)

    return dict(
        N=n, TP=tp, FP=fp, FN=fn, TN=tn,
        sensitivity=sens, sensitivity_lo=sens_ci[0], sensitivity_hi=sens_ci[1],
        specificity=spec, specificity_lo=spec_ci[0], specificity_hi=spec_ci[1],
        ppv=ppv, ppv_lo=ppv_ci[0], ppv_hi=ppv_ci[1],
        npv=npv, npv_lo=npv_ci[0], npv_hi=npv_ci[1],
        lr_positive=lr_pos, lr_negative=lr_neg,
        diagnostic_odds_ratio=dor,
    )


def trapezoidal_auc(fpr, tpr):
    """Compute AUC via trapezoidal rule (sorted by FPR)."""
    idx = np.argsort(fpr)
    _trapz = getattr(np, "trapezoid", None) or getattr(np, "trapz", None)
    return float(_trapz(tpr[idx], fpr[idx]))


def _auc_from_arrays(y_true, y_score):
    """Compute AUC from raw arrays, including (0,0) and (1,1) sentinels."""
    thresholds = np.sort(np.unique(y_score))[::-1]
    total_pos = int(y_true.sum())
    total_neg = len(y_true) - total_pos
    fps = [0.0]
    tps = [0.0]
    for t in thresholds:
        pred = y_score >= t
        tp = int(((pred == 1) & (y_true == 1)).sum())
        fp = int(((pred == 1) & (y_true == 0)).sum())
        fps.append(fp / total_neg if total_neg > 0 else 0.0)
        tps.append(tp / total_pos if total_pos > 0 else 0.0)
    fps.append(1.0)
    tps.append(1.0)
    return trapezoidal_auc(np.array(fps), np.array(tps))


def bootstrap_auc_ci(y_true, y_score, n_boot=2000, seed=42):
    """Bootstrap 95% CI for AUC using trapezoidal rule."""
    rng = np.random.default_rng(seed)
    boot_aucs = []
    n = len(y_true)
    for _ in range(n_boot):
        idx = rng.integers(0, n, size=n)
        yt, ys = y_true[idx], y_score[idx]
        if yt.sum() == 0 or yt.sum() == n:
            continue
        boot_aucs.append(_auc_from_arrays(yt, ys))
    boot_aucs = np.array(boot_aucs)
    return float(np.percentile(boot_aucs, 2.5)), float(np.percentile(boot_aucs, 97.5))


def roc_from_scores(y_true, y_score):
    """Build ROC points; returns (fpr_arr, tpr_arr, threshold_arr, auc)."""
    thresholds = np.sort(np.unique(y_score))[::-1]
    total_pos = y_true.sum()
    total_neg = len(y_true) - total_pos
    fps, tps, ts = [0.0], [0.0], [thresholds[0] + 1]
    for t in thresholds:
        pred = y_score >= t
        tp = ((pred == 1) & (y_true == 1)).sum()
        fp = ((pred == 1) & (y_true == 0)).sum()
        fps.append(fp / total_neg if total_neg > 0 else 0.0)
        tps.append(tp / total_pos if total_pos > 0 else 0.0)
        ts.append(t)
    fps.append(1.0); tps.append(1.0); ts.append(0)
    fpr = np.array(fps)
    tpr = np.array(tps)
    threshold_arr = np.array(ts)
    auc = trapezoidal_auc(fpr, tpr)
    return fpr, tpr, threshold_arr, auc


# ──────────────────────────────────────────────────────────────────────────────
# ANALYSIS FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────
def task1_diagnostic_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Task 1: Sensitivity/Specificity at each TI-RADS threshold."""
    print("\n[Task 1] Diagnostic performance at each threshold...")
    tirads_order = TIRADS_ORDER
    tirads_num = {t: i + 1 for i, t in enumerate(tirads_order)}
    df["tirads_num"] = df["tirads_worst_category_v12"].map(tirads_num)
    df["malignant"] = df["is_malignant"].fillna(False).astype(bool)

    rows = []
    for thresh_cat in tirads_order[1:]:  # >=TR2, >=TR3, >=TR4, >=TR5
        thresh_num = tirads_num[thresh_cat]
        positive_test = df["tirads_num"] >= thresh_num

        tp = ((positive_test) & (df["malignant"])).sum()
        fp = ((positive_test) & (~df["malignant"])).sum()
        fn = ((~positive_test) & (df["malignant"])).sum()
        tn = ((~positive_test) & (~df["malignant"])).sum()

        m = diagnostic_2x2(tp, fp, fn, tn)
        m["threshold"] = f">={thresh_cat}"
        rows.append(m)

    out = pd.DataFrame(rows)
    out.to_csv(OUTDIR / "tirads_diagnostic_performance.csv", index=False)
    print(f"  Saved: tirads_diagnostic_performance.csv ({len(out)} rows)")
    return out


def task2_roc_and_auc(df: pd.DataFrame):
    """Task 2: ROC curve and AUC."""
    print("\n[Task 2] ROC curve and AUC...")
    df = df.copy()
    df["malignant_int"] = df["is_malignant"].fillna(False).astype(int)

    # Use worst TI-RADS score; fill NaN with best score fallback
    score_col = "tirads_worst_score_v12"
    best_col = "tirads_best_score_v12"
    df["score_use"] = df[score_col].fillna(df[best_col])
    valid = df.dropna(subset=["score_use", "malignant_int"])

    y_true = valid["malignant_int"].values
    y_score = valid["score_use"].values.astype(float)

    fpr, tpr, thresholds, auc = roc_from_scores(y_true, y_score)
    auc_lo, auc_hi = bootstrap_auc_ci(y_true, y_score)

    # Youden's J
    j = tpr - fpr
    best_idx = np.argmax(j)
    best_thresh = thresholds[best_idx]
    best_fpr = fpr[best_idx]
    best_tpr = tpr[best_idx]
    j_stat = j[best_idx]

    print(f"  AUC = {auc:.4f} (95% CI: {auc_lo:.4f}–{auc_hi:.4f})")
    print(f"  Youden's J = {j_stat:.4f} at threshold = {best_thresh:.1f} "
          f"(TPR={best_tpr:.3f}, FPR={best_fpr:.3f})")

    roc_df = pd.DataFrame({
        "fpr": fpr, "tpr": tpr, "threshold": thresholds,
        "youdens_j": j,
    })
    roc_df.to_csv(OUTDIR / "roc_data.csv", index=False)

    summary = {
        "auc": auc, "auc_lo": auc_lo, "auc_hi": auc_hi,
        "n_valid": int(len(valid)),
        "youden_j": float(j_stat),
        "optimal_threshold": float(best_thresh),
        "optimal_sensitivity": float(best_tpr),
        "optimal_specificity": float(1 - best_fpr),
    }
    print(f"  Saved: roc_data.csv ({len(roc_df)} rows)")
    return roc_df, summary


def task3_rom_by_tirads(df: pd.DataFrame) -> pd.DataFrame:
    """Task 3: ROM by TI-RADS category with Wilson CI."""
    print("\n[Task 3] ROM by TI-RADS category...")
    df = df.copy()
    df["malignant"] = df["is_malignant"].fillna(False).astype(bool)
    # NIFTP flag (if histology_final contains NIFTP)
    df["is_niftp"] = df.get("histology_final", pd.Series(dtype=str)).str.upper().fillna("").str.contains("NIFTP")

    rows = []
    for cat in TIRADS_ORDER:
        sub = df[df["tirads_worst_category_v12"] == cat]
        n = len(sub)
        mal = sub["malignant"].sum()
        rom = mal / n if n > 0 else float("nan")
        ci = wilson_ci(int(mal), n)

        # Excluding NIFTP
        sub_no_niftp = sub[~sub["is_niftp"]]
        n2 = len(sub_no_niftp)
        mal2 = sub_no_niftp["malignant"].sum()
        rom2 = mal2 / n2 if n2 > 0 else float("nan")
        ci2 = wilson_ci(int(mal2), n2)

        acr_lo, acr_hi = ACR_EXPECTED[cat]
        rows.append(dict(
            tirads_category=cat,
            n_total=n, n_malignant=int(mal), n_benign=int(n - mal),
            rom=round(rom * 100, 2),
            rom_lo=round(ci[0] * 100, 2),
            rom_hi=round(ci[1] * 100, 2),
            n_no_niftp=n2,
            rom_excl_niftp=round(rom2 * 100, 2),
            rom_excl_niftp_lo=round(ci2[0] * 100, 2),
            rom_excl_niftp_hi=round(ci2[1] * 100, 2),
            acr_expected_lo=acr_lo,
            acr_expected_hi=acr_hi,
            within_acr_range=(acr_lo <= rom * 100 <= acr_hi) if not np.isnan(rom) else False,
        ))

    out = pd.DataFrame(rows)
    out.to_csv(OUTDIR / "rom_by_tirads.csv", index=False)
    print("  Saved: rom_by_tirads.csv")
    return out


def task4_nodule_size_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Task 4: Nodule size analysis by TI-RADS."""
    print("\n[Task 4] Nodule size analysis...")
    df = df.copy()
    df["malignant"] = df["is_malignant"].fillna(False).astype(bool)

    # Use dominant_nodule_size_cm; fallback to imaging_nodule_size_cm
    df["size_cm"] = df["dominant_nodule_size_cm"].fillna(df["imaging_nodule_size_cm"])

    # Size category
    def size_bucket(x):
        if pd.isna(x): return "Unknown"
        if x < 1.0: return "<1cm"
        if x < 2.0: return "1-2cm"
        if x < 4.0: return "2-4cm"
        return ">4cm"

    df["size_bucket"] = df["size_cm"].apply(size_bucket)
    SIZE_ORDER = ["<1cm", "1-2cm", "2-4cm", ">4cm"]

    rows = []
    # Per-category size stats
    for cat in TIRADS_ORDER:
        sub = df[df["tirads_worst_category_v12"] == cat]
        sz = sub["size_cm"].dropna()
        rows.append(dict(
            tirads_category=cat, size_bucket="ALL",
            n=len(sub), n_with_size=len(sz),
            size_mean_cm=round(sz.mean(), 2) if len(sz) else float("nan"),
            size_median_cm=round(sz.median(), 2) if len(sz) else float("nan"),
            size_p25=round(sz.quantile(0.25), 2) if len(sz) else float("nan"),
            size_p75=round(sz.quantile(0.75), 2) if len(sz) else float("nan"),
            n_malignant=int(sub["malignant"].sum()),
            rom=round(100 * sub["malignant"].sum() / len(sub), 2) if len(sub) > 0 else float("nan"),
        ))

    # TI-RADS × size bucket cross-tab
    for cat in TIRADS_ORDER:
        for sbucket in SIZE_ORDER:
            sub = df[(df["tirads_worst_category_v12"] == cat) & (df["size_bucket"] == sbucket)]
            if len(sub) == 0:
                continue
            n_mal = int(sub["malignant"].sum())
            rom = n_mal / len(sub)
            ci = wilson_ci(n_mal, len(sub))
            rows.append(dict(
                tirads_category=cat, size_bucket=sbucket,
                n=len(sub), n_with_size=len(sub),
                size_mean_cm=round(sub["size_cm"].mean(), 2),
                size_median_cm=round(sub["size_cm"].median(), 2),
                size_p25=round(sub["size_cm"].quantile(0.25), 2),
                size_p75=round(sub["size_cm"].quantile(0.75), 2),
                n_malignant=n_mal,
                rom=round(rom * 100, 2),
                rom_lo=round(ci[0] * 100, 2),
                rom_hi=round(ci[1] * 100, 2),
            ))

    out = pd.DataFrame(rows)
    out.to_csv(OUTDIR / "nodule_size_analysis.csv", index=False)
    print(f"  Saved: nodule_size_analysis.csv ({len(out)} rows)")
    return out


def task5_multi_tirads(df: pd.DataFrame) -> pd.DataFrame:
    """Task 5: Multi-TI-RADS assessment comparison."""
    print("\n[Task 5] Multi-TI-RADS assessment...")
    df = df.copy()
    df["malignant"] = df["is_malignant"].fillna(False).astype(bool)
    df["malignant_int"] = df["malignant"].astype(int)

    # Full cohort best vs worst
    rows = []

    for col, label in [("tirads_best_category_v12", "best"), ("tirads_worst_category_v12", "worst")]:
        tirads_num = {t: i + 1 for i, t in enumerate(TIRADS_ORDER)}
        df_col = df.dropna(subset=[col])
        df_col = df_col.copy()
        df_col["tr_num"] = df_col[col].map(tirads_num)
        # AUC for >=TR4 threshold
        for thresh_cat, thresh_num in [("TR3", 3), ("TR4", 4), ("TR5", 5)]:
            pos = df_col["tr_num"] >= thresh_num
            tp = (pos & df_col["malignant"]).sum()
            fp = (pos & ~df_col["malignant"]).sum()
            fn = (~pos & df_col["malignant"]).sum()
            tn = (~pos & ~df_col["malignant"]).sum()
            m = diagnostic_2x2(tp, fp, fn, tn)
            m["tirads_score_type"] = label
            m["threshold"] = f">={thresh_cat}"
            rows.append(m)

    # Agreement rate (patients with both scores)
    both = df.dropna(subset=["tirads_best_category_v12", "tirads_worst_category_v12"])
    agreement = (both["tirads_best_category_v12"] == both["tirads_worst_category_v12"]).sum()
    agreement_rate = agreement / len(both) if len(both) > 0 else float("nan")
    print(f"  Best/worst agreement: {agreement}/{len(both)} = {agreement_rate:.1%}")

    # Multi-source patients
    multi = df[df.get("tirads_n_sources_v12", pd.Series(dtype=float)).fillna(0) > 1] if "tirads_n_sources_v12" in df.columns else df

    out = pd.DataFrame(rows)
    out["n_with_both_scores"] = len(both)
    out["agreement_rate"] = round(agreement_rate, 4)
    out["n_multi_source"] = len(multi)
    out.to_csv(OUTDIR / "multi_tirads_assessment.csv", index=False)
    print(f"  Saved: multi_tirads_assessment.csv ({len(out)} rows)")
    return out


def task6_subgroup_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Task 6: Subgroup analyses."""
    print("\n[Task 6] Subgroup analyses...")
    df = df.copy()
    df["malignant"] = df["is_malignant"].fillna(False).astype(bool)
    tirads_num = {t: i + 1 for i, t in enumerate(TIRADS_ORDER)}
    df["tirads_num"] = df["tirads_worst_category_v12"].map(tirads_num)

    rows = []
    # Standard threshold = >=TR4
    THRESHOLD = 4

    def subgroup_perf(sub_df, group_name, group_val):
        sub = sub_df.dropna(subset=["tirads_num"])
        pos = sub["tirads_num"] >= THRESHOLD
        tp = (pos & sub["malignant"]).sum()
        fp = (pos & ~sub["malignant"]).sum()
        fn = (~pos & sub["malignant"]).sum()
        tn = (~pos & ~sub["malignant"]).sum()
        m = diagnostic_2x2(tp, fp, fn, tn)
        m["subgroup"] = group_name
        m["subgroup_value"] = str(group_val)
        return m

    rows.append(subgroup_perf(df, "Overall", "All"))

    # Age groups
    if "age_at_surgery" in df.columns:
        df_age = df.dropna(subset=["age_at_surgery"])
        for label, mask in [
            ("<45", df_age["age_at_surgery"] < 45),
            ("45-65", (df_age["age_at_surgery"] >= 45) & (df_age["age_at_surgery"] <= 65)),
            (">65", df_age["age_at_surgery"] > 65),
        ]:
            rows.append(subgroup_perf(df_age[mask], "Age group", label))

    # Sex
    if "sex" in df.columns:
        for sex in ["Female", "Male"]:
            sub = df[df["sex"].str.strip().str.title() == sex]
            if len(sub) > 50:
                rows.append(subgroup_perf(sub, "Sex", sex))

    # Histology (broad)
    if "histology_final" in df.columns:
        df_hist = df.dropna(subset=["histology_final"])
        hist_map = {
            "PTC": df_hist["histology_final"].str.upper().str.contains("PTC|PAPILLARY"),
            "FTC": df_hist["histology_final"].str.upper().str.contains("FOLLICULAR CARCINOMA|FTC"),
            "NIFTP": df_hist["histology_final"].str.upper().str.contains("NIFTP"),
        }
        for label, mask in hist_map.items():
            sub = df_hist[mask.fillna(False)]
            if len(sub) > 20:
                rows.append(subgroup_perf(sub, "Histology", label))

    # Bethesda
    if "bethesda_final" in df.columns:
        df_beth = df.dropna(subset=["bethesda_final"])
        for bval in sorted(df_beth["bethesda_final"].dropna().unique()):
            sub = df_beth[df_beth["bethesda_final"] == bval]
            bname = sub["bethesda_final_name"].iloc[0] if "bethesda_final_name" in df.columns and len(sub) > 0 else f"B{int(bval)}"
            if len(sub) >= 30:
                rows.append(subgroup_perf(sub, "Bethesda", f"B{int(bval)} {bname}"))

    # Time period
    if "surg_first_date" in df.columns:
        df_date = df.dropna(subset=["surg_first_date"]).copy()
        df_date["surg_year"] = pd.to_datetime(df_date["surg_first_date"]).dt.year
        for label, mask in [
            ("Pre-2017", df_date["surg_year"] < 2017),
            ("2017-2020", (df_date["surg_year"] >= 2017) & (df_date["surg_year"] <= 2020)),
            ("Post-2020", df_date["surg_year"] > 2020),
        ]:
            sub = df_date[mask]
            if len(sub) >= 50:
                rows.append(subgroup_perf(sub, "Time period", label))

    out = pd.DataFrame(rows)
    out.to_csv(OUTDIR / "subgroup_analysis.csv", index=False)
    print(f"  Saved: subgroup_analysis.csv ({len(out)} rows)")
    return out


def task7_unnecessary_fna(df: pd.DataFrame) -> pd.DataFrame:
    """Task 7: Unnecessary FNA analysis per ACR size thresholds."""
    print("\n[Task 7] Unnecessary FNA analysis (ACR guideline compliance)...")
    df = df.copy()
    df["malignant"] = df["is_malignant"].fillna(False).astype(bool)
    df["size_cm"] = df["dominant_nodule_size_cm"].fillna(df["imaging_nodule_size_cm"])
    df["size_mm"] = df["size_cm"] * 10  # convert to mm

    rows = []
    total_unnecessary = 0
    total_missed_cancers = 0

    # ACR FNA thresholds: TR5>=10mm, TR4>=15mm, TR3>=25mm, TR2/TR1=not recommended
    for cat in TIRADS_ORDER:
        sub = df[df["tirads_worst_category_v12"] == cat]
        threshold_mm = ACR_FNA_THRESHOLD.get(cat)

        if threshold_mm is None:
            # TR1/TR2: no FNA recommended
            n_total = len(sub)
            n_unnecessary = n_total  # all FNAs for TR1/TR2 are below-threshold by definition
            n_with_size = len(sub.dropna(subset=["size_mm"]))
            # Cancers potentially missed (all malignant in TR1/TR2 would be "missed")
            n_cancers_missed = int(sub["malignant"].sum())
            rows.append(dict(
                tirads_category=cat,
                acr_fna_threshold_mm=None,
                n_nodules=n_total,
                n_with_size=n_with_size,
                n_above_threshold=0,
                n_below_threshold=n_total,
                n_unnecessary_fna=n_unnecessary,
                pct_unnecessary=100.0,
                n_cancers_below_threshold=n_cancers_missed,
                pct_cancers_missed_if_strict=round(100 * n_cancers_missed / sub["malignant"].sum()
                                                    if sub["malignant"].sum() > 0 else 0, 1),
            ))
            total_unnecessary += n_unnecessary
            total_missed_cancers += n_cancers_missed
        else:
            sub_sz = sub.dropna(subset=["size_mm"])
            above = sub_sz[sub_sz["size_mm"] >= threshold_mm]
            below = sub_sz[sub_sz["size_mm"] < threshold_mm]

            n_below_malignant = int(below["malignant"].sum())

            pct_missed = 100 * n_below_malignant / sub["malignant"].sum() \
                if sub["malignant"].sum() > 0 else 0

            rows.append(dict(
                tirads_category=cat,
                acr_fna_threshold_mm=threshold_mm,
                n_nodules=len(sub),
                n_with_size=len(sub_sz),
                n_above_threshold=len(above),
                n_below_threshold=len(below),
                n_unnecessary_fna=len(below),
                pct_unnecessary=round(100 * len(below) / len(sub_sz), 1) if len(sub_sz) > 0 else 0,
                n_cancers_below_threshold=n_below_malignant,
                pct_cancers_missed_if_strict=round(pct_missed, 1),
            ))
            total_unnecessary += len(below)
            total_missed_cancers += n_below_malignant

    out = pd.DataFrame(rows)
    out.to_csv(OUTDIR / "unnecessary_fna_analysis.csv", index=False)
    print("  Saved: unnecessary_fna_analysis.csv")
    print(f"  Total unnecessary FNAs: {total_unnecessary:,}")
    print(f"  Total cancers below threshold: {total_missed_cancers:,}")
    return out


# ──────────────────────────────────────────────────────────────────────────────
# FIGURES
# ──────────────────────────────────────────────────────────────────────────────
def plot_roc(roc_df, auc_summary):
    """Generate ROC curve figure."""
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.plot(roc_df["fpr"], roc_df["tpr"], color="#2166ac", lw=2.5,
            label=f"ACR TI-RADS (AUC = {auc_summary['auc']:.3f}, "
                  f"95% CI {auc_summary['auc_lo']:.3f}–{auc_summary['auc_hi']:.3f})")
    ax.plot([0, 1], [0, 1], "k--", lw=1.2, alpha=0.5, label="Random classifier")

    # Mark Youden's optimal point
    opt_fpr = 1 - auc_summary["optimal_specificity"]
    opt_tpr = auc_summary["optimal_sensitivity"]
    ax.scatter([opt_fpr], [opt_tpr], s=120, zorder=5, color="#d6604d",
               label=f"Youden optimum (thresh={auc_summary['optimal_threshold']:.0f}, "
                     f"Sn={opt_tpr:.2f}, Sp={auc_summary['optimal_specificity']:.2f})")

    ax.set_xlabel("1 – Specificity (False Positive Rate)", fontsize=12)
    ax.set_ylabel("Sensitivity (True Positive Rate)", fontsize=12)
    ax.set_title("ROC Curve – ACR TI-RADS for Malignancy Detection\n"
                 "(M025 Cohort, N=3,375; Worst TI-RADS score)", fontsize=12)
    ax.legend(loc="lower right", fontsize=10)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    fig.savefig(OUTDIR / "roc_curve.png", dpi=300, bbox_inches="tight")
    plt.close()
    print("  Saved: roc_curve.png")


def plot_rom_by_bucket(rom_df):
    """ROM by TI-RADS category bar chart."""
    fig, ax = plt.subplots(figsize=(9, 6))

    x = np.arange(len(TIRADS_ORDER))
    bars = ax.bar(x, rom_df["rom"], color=["#4575b4","#74add1","#fee090","#f46d43","#d73027"],
                  width=0.5, alpha=0.9, zorder=3)

    # Error bars (Wilson CI)
    lo_err = rom_df["rom"] - rom_df["rom_lo"]
    hi_err = rom_df["rom_hi"] - rom_df["rom"]
    ax.errorbar(x, rom_df["rom"], yerr=[lo_err, hi_err], fmt="none",
                ecolor="black", capsize=5, lw=1.8, zorder=4)

    # ACR expected ROM markers
    for i, cat in enumerate(TIRADS_ORDER):
        lo, hi = ACR_EXPECTED[cat]
        ax.hlines(hi, i - 0.3, i + 0.3, colors="#555", lw=2, linestyles="--",
                  label="ACR expected max" if i == 0 else "")
        ax.hlines(lo, i - 0.3, i + 0.3, colors="#555", lw=1, linestyles=":",
                  label="ACR expected min" if i == 0 else "")

    # Value labels
    for bar, rom_val in zip(bars, rom_df["rom"]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1.5,
                f"{rom_val:.1f}%", ha="center", va="bottom", fontsize=11, fontweight="bold")

    # N labels
    for i, (n, nmal) in enumerate(zip(rom_df["n_total"], rom_df["n_malignant"])):
        ax.text(i, -5, f"N={n}\n({nmal} mal.)", ha="center", va="top", fontsize=9, color="#444")

    ax.set_xticks(x)
    ax.set_xticklabels([f"TR{i+1}\n({cat})" for i, cat in enumerate(["Benign","Not susp.","Mildly susp.","Mod. susp.","Highly susp."])],
                       fontsize=10)
    ax.set_ylabel("Risk of Malignancy (%)", fontsize=12)
    ax.set_title("Risk of Malignancy by ACR TI-RADS Category\n"
                 "(M025 Cohort; Error bars = 95% Wilson CI; Dashed = ACR expected)", fontsize=12)
    ax.set_ylim(-12, max(rom_df["rom_hi"]) + 12)
    ax.grid(axis="y", alpha=0.3, zorder=0)
    handles = [
        mpatches.Patch(color=c, label=cat) for cat, c in zip(
            TIRADS_ORDER, ["#4575b4","#74add1","#fee090","#f46d43","#d73027"])
    ]
    from matplotlib.lines import Line2D
    handles += [
        Line2D([0], [0], color="#555", lw=2, linestyle="--", label="ACR expected max"),
        Line2D([0], [0], color="#555", lw=1, linestyle=":", label="ACR expected min"),
    ]
    ax.legend(handles=handles, loc="upper left", fontsize=9, ncol=2)
    plt.tight_layout()
    fig.savefig(OUTDIR / "rom_by_bucket.png", dpi=300, bbox_inches="tight")
    plt.close()
    print("  Saved: rom_by_bucket.png")


# ──────────────────────────────────────────────────────────────────────────────
# LATEX TABLE
# ──────────────────────────────────────────────────────────────────────────────
def build_latex_tables(perf_df, rom_df, auc_summary):
    """Generate LaTeX tables for the manuscript."""
    lines = [
        r"\documentclass{article}",
        r"\usepackage{booktabs,siunitx,longtable,geometry}",
        r"\geometry{margin=1in}",
        r"\begin{document}",
        "",
        "% ─────────────────────────────────────────────",
        "% Table 1: Diagnostic Performance at Each Threshold",
        "% ─────────────────────────────────────────────",
        r"\begin{table}[ht]",
        r"\centering",
        r"\caption{Diagnostic Performance of ACR TI-RADS at Each Cut-Point (M025 Cohort, N=3,375)}",
        r"\begin{tabular}{lccccccc}",
        r"\toprule",
        r"Threshold & N & Sensitivity & Specificity & PPV & NPV & LR+ & LR$-$ \\",
        r" & & (95\% CI) & (95\% CI) & (95\% CI) & (95\% CI) & & \\",
        r"\midrule",
    ]
    for _, row in perf_df.iterrows():
        lines.append(
            f"{row['threshold']} & {int(row['N'])} & "
            f"{row['sensitivity']:.3f} ({row['sensitivity_lo']:.3f}–{row['sensitivity_hi']:.3f}) & "
            f"{row['specificity']:.3f} ({row['specificity_lo']:.3f}–{row['specificity_hi']:.3f}) & "
            f"{row['ppv']:.3f} ({row['ppv_lo']:.3f}–{row['ppv_hi']:.3f}) & "
            f"{row['npv']:.3f} ({row['npv_lo']:.3f}–{row['npv_hi']:.3f}) & "
            f"{row['lr_positive']:.2f} & {row['lr_negative']:.2f} \\\\"
        )
    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
        "",
        "% ─────────────────────────────────────────────",
        "% Table 2: ROM by TI-RADS Category",
        "% ─────────────────────────────────────────────",
        r"\begin{table}[ht]",
        r"\centering",
        r"\caption{Risk of Malignancy by ACR TI-RADS Category}",
        r"\begin{tabular}{lcccccc}",
        r"\toprule",
        r"Category & N & Malignant & ROM (\%) & 95\% CI & ROM excl. NIFTP & ACR Expected \\",
        r"\midrule",
    ]
    for _, row in rom_df.iterrows():
        lines.append(
            f"{row['tirads_category']} & {int(row['n_total'])} & {int(row['n_malignant'])} & "
            f"{row['rom']:.1f} & ({row['rom_lo']:.1f}–{row['rom_hi']:.1f}) & "
            f"{row['rom_excl_niftp']:.1f} & "
            f"{row['acr_expected_lo']:.0f}–{row['acr_expected_hi']:.0f}\\% \\\\"
        )
    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
        "",
        f"% AUC = {auc_summary['auc']:.4f} (95\\% CI: {auc_summary['auc_lo']:.4f}–{auc_summary['auc_hi']:.4f})",
        f"% Youden's J = {auc_summary['youden_j']:.4f} at threshold {auc_summary['optimal_threshold']:.1f}",
        r"\end{document}",
    ]
    tex = "\n".join(lines)
    (OUTDIR / "tirads_performance_summary.tex").write_text(tex)
    print("  Saved: tirads_performance_summary.tex")


# ──────────────────────────────────────────────────────────────────────────────
# MOTHERDUCK UPLOAD
# ──────────────────────────────────────────────────────────────────────────────
def upload_to_motherduck(df: pd.DataFrame):
    """Upload patient-level diagnostic table to MotherDuck."""
    print("\n[Upload] Creating manuscript_workspace.m025_tirads_analysis_v1...")
    from motherduck_client import get_token
    import duckdb

    tok = get_token()
    conn = duckdb.connect(
        "md:thyroid_canonical_publication_v1_0",
        config={"motherduck_token": tok},
    )

    # Build patient-level table with derived fields
    df_up = df.copy()
    tirads_num = {t: i + 1 for i, t in enumerate(TIRADS_ORDER)}
    df_up["tirads_worst_num"] = df_up["tirads_worst_category_v12"].map(tirads_num)
    df_up["malignant_int"] = df_up["is_malignant"].fillna(False).astype(int)

    # Size
    df_up["size_cm"] = df_up["dominant_nodule_size_cm"].fillna(df_up["imaging_nodule_size_cm"])
    df_up["size_mm"] = df_up["size_cm"] * 10

    # ACR FNA recommended
    def acr_fna_recommended(row):
        cat = row.get("tirads_worst_category_v12")
        sz = row.get("size_mm")
        if cat not in ACR_FNA_THRESHOLD:
            return None
        thresh = ACR_FNA_THRESHOLD[cat]
        if thresh is None:
            return False
        if pd.isna(sz):
            return None
        return bool(sz >= thresh)

    df_up["acr_fna_recommended"] = df_up.apply(acr_fna_recommended, axis=1)

    # Score for ROC
    df_up["score_use"] = df_up["tirads_worst_score_v12"].fillna(df_up["tirads_best_score_v12"])

    # Age group
    if "age_at_surgery" in df_up.columns:
        df_up["age_group"] = pd.cut(
            df_up["age_at_surgery"].astype(float),
            bins=[0, 44, 65, 999],
            labels=["<45", "45-65", ">65"],
            right=True,
        ).astype(str)

    # Size bucket
    def size_bucket(x):
        if pd.isna(x): return None
        if x < 1.0: return "<1cm"
        if x < 2.0: return "1-2cm"
        if x < 4.0: return "2-4cm"
        return ">4cm"
    df_up["size_bucket"] = df_up["size_cm"].apply(size_bucket)

    df_up["analysis_created_at"] = datetime.utcnow().isoformat()

    # Select key cols for upload
    upload_cols = [c for c in [
        "research_id", "age_at_surgery", "age_group", "sex", "race",
        "tirads_worst_category_v12", "tirads_best_category_v12",
        "tirads_worst_score_v12", "tirads_best_score_v12", "score_use",
        "tirads_worst_num", "tirads_n_sources_v12",
        "imaging_nodule_size_cm", "dominant_nodule_size_cm",
        "size_cm", "size_mm", "size_bucket",
        "bethesda_final", "bethesda_final_name",
        "histology_final", "is_malignant", "malignant_int",
        "acr_fna_recommended",
        "surg_procedure_type", "surg_first_date",
        "analysis_created_at",
    ] if c in df_up.columns]

    df_up_final = df_up[upload_cols].copy()

    # Drop old version if exists
    conn.execute("DROP TABLE IF EXISTS manuscript_workspace.m025_tirads_analysis_v1")

    # Register and create
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    df_up_final.to_parquet(tmp_path, index=False)
    conn.execute(f"""
        CREATE TABLE manuscript_workspace.m025_tirads_analysis_v1 AS
        SELECT * FROM read_parquet('{tmp_path}')
    """)
    os.unlink(tmp_path)

    cnt = conn.execute("SELECT COUNT(*) FROM manuscript_workspace.m025_tirads_analysis_v1").fetchone()[0]
    conn.close()
    print(f"  Uploaded: manuscript_workspace.m025_tirads_analysis_v1 ({cnt:,} rows)")


# ──────────────────────────────────────────────────────────────────────────────
# RUN SNAPSHOT
# ──────────────────────────────────────────────────────────────────────────────
def save_run_snapshot(auc_summary, rom_df, perf_df):
    """Save JSON run snapshot for cross-validation."""
    snap = {
        "run_timestamp": datetime.utcnow().isoformat(),
        "cohort": "manuscript_workspace.cohort_m025_tirads_performance_v1",
        "n_total": 3375,
        "n_malignant": 1479,
        "n_benign": 1896,
        "overall_malignancy_rate_pct": round(100 * 1479 / 3375, 1),
        "auc": auc_summary["auc"],
        "auc_ci_lo": auc_summary["auc_lo"],
        "auc_ci_hi": auc_summary["auc_hi"],
        "youden_j": auc_summary["youden_j"],
        "optimal_threshold": auc_summary["optimal_threshold"],
        "rom_by_tirads": {
            row["tirads_category"]: {
                "n": int(row["n_total"]),
                "malignant": int(row["n_malignant"]),
                "rom_pct": row["rom"],
                "ci_lo": row["rom_lo"],
                "ci_hi": row["rom_hi"],
            }
            for _, row in rom_df.iterrows()
        },
        "threshold_perf": {
            row["threshold"]: {
                "sensitivity": round(row["sensitivity"], 4),
                "specificity": round(row["specificity"], 4),
                "ppv": round(row["ppv"], 4),
                "npv": round(row["npv"], 4),
            }
            for _, row in perf_df.iterrows()
        },
    }
    snap_path = OUTDIR / "m025v2_run_snapshot.json"
    snap_path.write_text(json.dumps(snap, indent=2))
    # Also update the submission package snapshot
    pkg_snap = Path("M025_submission_package_v2_0/08_analysis_outputs/m025v2_run_snapshot.json")
    pkg_snap.parent.mkdir(parents=True, exist_ok=True)
    pkg_snap.write_text(json.dumps(snap, indent=2))
    print("  Saved: m025v2_run_snapshot.json")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("M025 v2 – ACR TI-RADS Diagnostic Performance Analysis")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Load data
    df = load_data()
    print(f"\nCohort: {len(df):,} patients | "
          f"Malignant: {df['is_malignant'].fillna(False).sum():,} | "
          f"Benign: {(~df['is_malignant'].fillna(False)).sum():,}")

    # Task 1: Diagnostic performance
    perf_df = task1_diagnostic_performance(df)

    # Task 2: ROC + AUC
    roc_df, auc_summary = task2_roc_and_auc(df)

    # Task 3: ROM by TI-RADS
    rom_df = task3_rom_by_tirads(df)

    # Task 4: Nodule size analysis
    task4_nodule_size_analysis(df)

    # Task 5: Multi-TI-RADS
    task5_multi_tirads(df)

    # Task 6: Subgroup analysis
    task6_subgroup_analysis(df)

    # Task 7: Unnecessary FNA
    task7_unnecessary_fna(df)

    # Task 8: Figures + LaTeX
    print("\n[Task 8] Generating figures and LaTeX tables...")
    plot_roc(roc_df, auc_summary)
    plot_rom_by_bucket(rom_df)
    build_latex_tables(perf_df, rom_df, auc_summary)

    # Also copy key CSVs to submission package outputs folder
    import shutil
    pkg_out = Path("M025_submission_package_v2_0/08_analysis_outputs")
    pkg_out.mkdir(parents=True, exist_ok=True)
    for fname in [
        "tirads_diagnostic_performance.csv",
        "roc_data.csv",
        "rom_by_tirads.csv",
        "nodule_size_analysis.csv",
        "unnecessary_fna_analysis.csv",
    ]:
        src = OUTDIR / fname
        if src.exists():
            shutil.copy(src, pkg_out / fname.replace(".csv", ".csv").replace("roc_data", "m025v2_supp_ROC_curve_points")
                        if fname == "roc_data.csv" else pkg_out / fname)

    shutil.copy(OUTDIR / "rom_by_tirads.csv",
                pkg_out / "m025v2_per_tr_rom_with_ci.csv")

    # Save run snapshot
    save_run_snapshot(auc_summary, rom_df, perf_df)

    # Task 9: Upload to MotherDuck
    upload_to_motherduck(df)

    print("\n" + "=" * 70)
    print(f"COMPLETE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Output dir: {OUTDIR.resolve()}")
    print("\nKey Results:")
    print(f"  AUC = {auc_summary['auc']:.4f} (95% CI: {auc_summary['auc_lo']:.4f}–{auc_summary['auc_hi']:.4f})")
    print(f"  Youden optimal threshold = TR{auc_summary['optimal_threshold']:.0f}")
    print(f"  Sn = {auc_summary['optimal_sensitivity']:.3f}, Sp = {auc_summary['optimal_specificity']:.3f}")
    print("\nROM by TI-RADS:")
    for _, row in rom_df.iterrows():
        acr = f"(ACR: {row['acr_expected_lo']:.0f}–{row['acr_expected_hi']:.0f}%)"
        within = "✓" if row["within_acr_range"] else "✗"
        print(f"  {row['tirads_category']}: {row['rom']:.1f}% "
              f"({row['rom_lo']:.1f}–{row['rom_hi']:.1f}%) {acr} {within}")
    print("=" * 70)


if __name__ == "__main__":
    main()
