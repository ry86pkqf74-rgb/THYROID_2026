#!/usr/bin/env python3
"""
M019 Radioactive Iodine (RAI) Outcomes Analysis
Analyzes RAI utilization, Tg response, dose-response, and outcomes in thyroid cancer cohort.

Output: studies/m019_rai_outcomes/
"""

import sys
import os
import warnings
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
from scipy import stats
import duckdb

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).parent.parent))
from motherduck_client import get_token

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
OUT_DIR = Path("studies/m019_rai_outcomes")
OUT_DIR.mkdir(parents=True, exist_ok=True)

token = get_token()
if not token:
    raise RuntimeError("No MotherDuck token found")

conn = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}")
print(f"[{datetime.now():%H:%M:%S}] Connected to MotherDuck")

# ---------------------------------------------------------------------------
# Load analytic view
# ---------------------------------------------------------------------------
rai = conn.execute("SELECT * FROM manuscript_workspace.m019_rai_outcomes_analytic_v1").df()
print(f"[{datetime.now():%H:%M:%S}] Loaded RAI analytic view: {len(rai):,} rows")

# Normalize booleans from DuckDB (can be None/pd.NA)
bool_cols = [
    "multifocal_flag_path", "is_malignant", "rai_received_reconciled",
    "rai_dose_data_available", "rai_avid_flag", "tg_data_available",
    "tg_rising_flag", "tg_below_threshold_ever", "tgab_interference_flag",
    "any_recurrence_flag", "any_confirmed_complication_flag", "braf_positive_final",
    "ras_positive_final", "ln_positive_final"
]
def _safe_bool_coerce(x):
    """Safely coerce a possibly-NA value to True/False/None."""
    try:
        if x is None or (hasattr(x, '__class__') and x.__class__.__name__ == 'NAType'):
            return None
        b = bool(x)
        return b
    except (TypeError, ValueError):
        return None

for c in bool_cols:
    if c in rai.columns:
        rai[c] = rai[c].apply(_safe_bool_coerce)


def safe_bool(series):
    """Convert boolean-ish series to numeric 0/1, None=NaN."""
    return series.apply(lambda x: 1 if x is True else (0 if x is False else np.nan))


rai["recurrence_int"] = safe_bool(rai["any_recurrence_flag"])
rai["dose_available_int"] = safe_bool(rai["rai_dose_data_available"])
rai["tg_data_int"] = safe_bool(rai["tg_data_available"])
rai["tgab_flag_int"] = safe_bool(rai["tgab_interference_flag"])

print(f"  Recurrence: {int(rai['recurrence_int'].sum())} / {int(rai['recurrence_int'].notna().sum())}")
print(f"  Dose available: {int(rai['dose_available_int'].sum())}")
print(f"  Tg data: {int(rai['tg_data_int'].sum())}")

# ---------------------------------------------------------------------------
# Load full malignant cohort from CPM for RAI vs No-RAI comparison
# ---------------------------------------------------------------------------
cpm_sql = """
SELECT 
    research_id,
    age_at_surgery,
    sex,
    race,
    ajcc8_stage_group,
    ajcc8_t_stage,
    ajcc8_n_stage,
    ajcc8_m_stage,
    ete_grade_final_v2,
    tumor_size_cm_max,
    ln_positive_final AS ln_positive,
    braf_positive_final,
    ras_positive_final,
    rai_received_reconciled,
    any_recurrence_flag,
    histology_final,
    molecular_risk_tier,
    surg_first_date
FROM main.canonical_patient_master
WHERE is_malignant IS TRUE
"""
cpm = conn.execute(cpm_sql).df()
print(f"[{datetime.now():%H:%M:%S}] Loaded CPM malignant: {len(cpm):,} rows")

# Normalize booleans
for c in ["rai_received_reconciled", "any_recurrence_flag", "braf_positive_final",
          "ras_positive_final", "ln_positive"]:
    if c in cpm.columns:
        cpm[c] = cpm[c].apply(_safe_bool_coerce)

cpm["recurrence_int"] = safe_bool(cpm["any_recurrence_flag"])
# NULL rai_received_reconciled = not received RAI; treat as 0
cpm["rai_received_int"] = cpm["rai_received_reconciled"].apply(
    lambda x: 1 if _safe_bool_coerce(x) is True else 0
)

# Surgery year
if "surg_first_date" in cpm.columns:
    cpm["surgery_year"] = pd.to_datetime(cpm["surg_first_date"], errors="coerce").dt.year

n_malignant = len(cpm)
n_rai = int(cpm["rai_received_int"].sum())
print(f"  Malignant: {n_malignant:,}, RAI: {n_rai:,} ({100*n_rai/n_malignant:.1f}%)")

# ---------------------------------------------------------------------------
# SECTION 1: RAI Utilization Patterns
# ---------------------------------------------------------------------------
print(f"\n[{datetime.now():%H:%M:%S}] Section 1: RAI Utilization Patterns")

util_rows = []

# 1a. Overall RAI receipt
util_rows.append({
    "metric": "RAI receipt rate (malignant cohort)",
    "numerator": n_rai,
    "denominator": n_malignant,
    "pct": f"{100*n_rai/n_malignant:.1f}%"
})

# 1b. RAI by AJCC stage
stage_util = (cpm.groupby("ajcc8_stage_group")
              .agg(n_total=("research_id", "count"),
                   n_rai=("rai_received_int", "sum"))
              .reset_index())
stage_util["pct_rai"] = (100 * stage_util["n_rai"] / stage_util["n_total"]).round(1)

# 1c. RAI by histology
hist_util = (cpm.groupby("histology_final")
             .agg(n_total=("research_id", "count"),
                  n_rai=("rai_received_int", "sum"))
             .reset_index())
hist_util["pct_rai"] = (100 * hist_util["n_rai"] / hist_util["n_total"]).round(1)
hist_util = hist_util.sort_values("n_total", ascending=False).head(15)

# 1d. RAI by ATA risk category (from RAI cohort)
ata_util = (rai.groupby("ata_risk_category")
            .agg(n=("research_id", "count"),
                 n_recurred=("recurrence_int", "sum"))
            .reset_index())
ata_util["pct_recurred"] = (100 * ata_util["n_recurred"] / ata_util["n"]).round(1)

# 1e. RAI intent distribution
intent_dist = rai["rai_intent_v9"].value_counts(dropna=False).reset_index()
intent_dist.columns = ["rai_intent", "n"]
intent_dist["pct"] = (100 * intent_dist["n"] / len(rai)).round(1)

# 1f. N RAI episodes distribution
ep_dist = rai["n_rai_episodes"].value_counts().reset_index()
ep_dist.columns = ["n_episodes", "n_patients"]
ep_dist = ep_dist.sort_values("n_episodes")

# 1g. Temporal trends (if surgery_year available)
if "surgery_year" in cpm.columns:
    yr_util = (cpm[cpm["surgery_year"].notna()]
               .groupby("surgery_year")
               .agg(n_total=("research_id", "count"),
                    n_rai=("rai_received_int", "sum"))
               .reset_index())
    yr_util["pct_rai"] = (100 * yr_util["n_rai"] / yr_util["n_total"]).round(1)
    yr_util = yr_util[yr_util["n_total"] >= 10]  # only stable years

# Build combined utilization CSV
util_out = pd.DataFrame(util_rows)
stage_util["category"] = "ajcc_stage"
stage_util.rename(columns={"ajcc8_stage_group": "value"}, inplace=True)

hist_util["category"] = "histology"
hist_util.rename(columns={"histology_final": "value"}, inplace=True)

ata_util["category"] = "ata_risk"
ata_util.rename(columns={"ata_risk_category": "value"}, inplace=True)
ata_util["n_total"] = ata_util["n"]
ata_util["n_rai"] = ata_util["n"]

util_combined = pd.concat([
    stage_util[["category", "value", "n_total", "n_rai", "pct_rai"]],
    hist_util[["category", "value", "n_total", "n_rai", "pct_rai"]],
], ignore_index=True)

util_combined.to_csv(OUT_DIR / "rai_utilization_patterns.csv", index=False)
intent_dist.to_csv(OUT_DIR / "rai_intent_distribution.csv", index=False)
ep_dist.to_csv(OUT_DIR / "rai_episodes_distribution.csv", index=False)

if "surgery_year" in cpm.columns and len(yr_util) > 0:
    yr_util.to_csv(OUT_DIR / "rai_temporal_trends.csv", index=False)

print(f"  Utilization by AJCC stage:\n{stage_util[['value','n_total','n_rai','pct_rai']].to_string(index=False)}")

# ---------------------------------------------------------------------------
# SECTION 2: Thyroglobulin Response Analysis
# ---------------------------------------------------------------------------
print(f"\n[{datetime.now():%H:%M:%S}] Section 2: Tg Response Analysis")

# Subset: has Tg data and not TgAb interference
tg_df = rai[rai["tg_data_int"] == 1].copy()
tg_clean = tg_df[tg_df["tgab_flag_int"] != 1].copy()  # exclude TgAb interference
print(f"  Tg available: {len(tg_df):,}, after TgAb exclusion: {len(tg_clean):,}")

# 2a. Recurrence by trajectory class
traj_rec = (tg_clean.groupby("tg_trajectory_class")
            .agg(n=("research_id", "count"),
                 n_recurred=("recurrence_int", "sum"))
            .reset_index())
traj_rec["pct_recurred"] = (100 * traj_rec["n_recurred"] / traj_rec["n"]).round(1)

# Order by expected recurrence
traj_order = ["suppressed", "low_stable", "detectable_stable", "rising", "insufficient_data"]
traj_rec["order"] = traj_rec["tg_trajectory_class"].apply(
    lambda x: traj_order.index(x) if x in traj_order else 99)
traj_rec = traj_rec.sort_values("order").drop("order", axis=1)

print(f"  Tg trajectory vs recurrence:\n{traj_rec.to_string(index=False)}")

# 2b. OR for each trajectory vs suppressed (logistic regression)
try:
    from scipy.stats import chi2_contingency
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    tg_model = tg_clean[tg_clean["tg_trajectory_class"].isin(
        ["suppressed", "low_stable", "detectable_stable", "rising"])].copy()
    tg_model = tg_model[tg_model["recurrence_int"].notna()].copy()

    tg_model["traj_code"] = tg_model["tg_trajectory_class"].map({
        "suppressed": 0, "low_stable": 1, "detectable_stable": 2, "rising": 3
    })

    # Unadjusted ORs vs suppressed using chi2 or logistic
    or_rows = []
    ref_data = tg_model[tg_model["traj_code"] == 0]
    ref_rec = ref_data["recurrence_int"].sum()
    ref_n = len(ref_data)

    for tclass, tcode in [("low_stable", 1), ("detectable_stable", 2), ("rising", 3)]:
        comp = tg_model[tg_model["traj_code"] == tcode]
        if len(comp) < 5:
            continue
        comp_rec = comp["recurrence_int"].sum()
        comp_n = len(comp)
        # 2x2 table: [[rec, no_rec], [rec, no_rec]]
        a, b = int(comp_rec), int(comp_n - comp_rec)
        c, d = int(ref_rec), int(ref_n - ref_rec)
        if c == 0 or d == 0:
            or_val = np.nan
            ci_low, ci_high = np.nan, np.nan
            p = np.nan
        else:
            or_val = (a / b) / (c / d) if b > 0 and d > 0 else np.nan
            # Woolf logit CI
            se = np.sqrt(1/a + 1/b + 1/c + 1/d) if (a > 0 and b > 0 and c > 0 and d > 0) else np.nan
            ci_low = np.exp(np.log(or_val) - 1.96 * se) if not np.isnan(or_val) and se is not np.nan else np.nan
            ci_high = np.exp(np.log(or_val) + 1.96 * se) if not np.isnan(or_val) and se is not np.nan else np.nan
            _, p, _, _ = chi2_contingency([[a, b], [c, d]])
        or_rows.append({
            "trajectory": tclass, "n": comp_n, "n_recurred": int(comp_rec),
            "pct_recurred": round(100*comp_rec/comp_n, 1),
            "or": round(or_val, 2) if not np.isnan(or_val) else "N/A",
            "ci_low": round(ci_low, 2) if not np.isnan(ci_low) else "N/A",
            "ci_high": round(ci_high, 2) if not np.isnan(ci_high) else "N/A",
            "p_value": round(p, 4) if not np.isnan(p) else "N/A"
        })
    # Add reference row
    or_rows.insert(0, {
        "trajectory": "suppressed (ref)", "n": int(ref_n), "n_recurred": int(ref_rec),
        "pct_recurred": round(100*ref_rec/ref_n, 1),
        "or": "1.00", "ci_low": "--", "ci_high": "--", "p_value": "--"
    })
    tg_or_df = pd.DataFrame(or_rows)
    print(f"  Tg trajectory ORs:\n{tg_or_df.to_string(index=False)}")

except Exception as e:
    print(f"  Logistic regression error: {e}")
    tg_or_df = traj_rec.copy()

traj_rec.to_csv(OUT_DIR / "tg_trajectory_recurrence.csv", index=False)
tg_or_df.to_csv(OUT_DIR / "tg_trajectory_or.csv", index=False)

# 2c. ROC analysis for post_rai_tg_nadir
roc_rows = []
tg_roc = tg_clean[
    tg_clean["post_rai_tg_nadir"].notna() &
    tg_clean["recurrence_int"].notna()
].copy()

if len(tg_roc) > 20:
    try:
        from sklearn.metrics import roc_curve, roc_auc_score
        y = tg_roc["recurrence_int"].astype(int)
        scores = tg_roc["post_rai_tg_nadir"].astype(float)

        fpr, tpr, thresholds = roc_curve(y, scores)
        auc = roc_auc_score(y, scores)
        # Youden's J
        youden = tpr - fpr
        best_idx = np.argmax(youden)
        best_thresh = thresholds[best_idx]
        best_sens = tpr[best_idx]
        best_spec = 1 - fpr[best_idx]

        roc_df = pd.DataFrame({"fpr": fpr, "tpr": tpr, "threshold": thresholds})
        roc_df.to_csv(OUT_DIR / "tg_threshold_roc.csv", index=False)

        print(f"  ROC AUC (post-RAI Tg nadir): {auc:.3f}")
        print(f"  Optimal threshold (Youden): Tg={best_thresh:.2f} ng/mL, Sens={best_sens:.2f}, Spec={best_spec:.2f}")

        # Sensitivity analysis at clinical thresholds
        for thr in [0.1, 0.2, 0.5, 1.0, 2.0, 5.0]:
            pos = (scores >= thr)
            tp = ((pos) & (y == 1)).sum()
            fp = ((pos) & (y == 0)).sum()
            tn = ((~pos) & (y == 0)).sum()
            fn = ((~pos) & (y == 1)).sum()
            sens = tp / (tp + fn) if (tp + fn) > 0 else np.nan
            spec = tn / (tn + fp) if (tn + fp) > 0 else np.nan
            ppv = tp / (tp + fp) if (tp + fp) > 0 else np.nan
            roc_rows.append({
                "threshold_ng_ml": thr, "tp": int(tp), "fp": int(fp),
                "tn": int(tn), "fn": int(fn),
                "sensitivity": round(sens, 3), "specificity": round(spec, 3),
                "ppv": round(ppv, 3)
            })
        pd.DataFrame(roc_rows).to_csv(OUT_DIR / "tg_threshold_clinical.csv", index=False)
        pd.DataFrame([{"auc": auc, "optimal_threshold": best_thresh,
                       "sensitivity": best_sens, "specificity": best_spec,
                       "n_analyzed": len(tg_roc)}]
                     ).to_csv(OUT_DIR / "tg_roc_summary.csv", index=False)
    except Exception as e:
        print(f"  ROC error: {e}")
        pd.DataFrame({"note": [f"ROC failed: {e}"]}).to_csv(OUT_DIR / "tg_threshold_roc.csv", index=False)
else:
    print(f"  Insufficient data for ROC (n={len(tg_roc)})")
    pd.DataFrame({"note": [f"Insufficient data: n={len(tg_roc)}"]}).to_csv(OUT_DIR / "tg_threshold_roc.csv", index=False)

# 2d. Full Tg response CSV
traj_rec.to_csv(OUT_DIR / "tg_response_analysis.csv", index=False)

# ---------------------------------------------------------------------------
# SECTION 3: TgAb Interference Analysis
# ---------------------------------------------------------------------------
print(f"\n[{datetime.now():%H:%M:%S}] Section 3: TgAb Interference Analysis")

tgab_pos = rai[rai["tgab_flag_int"] == 1]
tgab_neg = rai[rai["tgab_flag_int"] == 0]

tgab_rows = [
    {"group": "TgAb positive", "n": len(tgab_pos),
     "pct_of_rai_cohort": round(100*len(tgab_pos)/len(rai), 1),
     "n_recurred": int(tgab_pos["recurrence_int"].sum()),
     "pct_recurred": round(100*tgab_pos["recurrence_int"].mean()*100/100, 1) if len(tgab_pos) > 0 else 0,
     "median_tgab_last": round(tgab_pos["tgab_last_value"].median(), 1) if "tgab_last_value" in tgab_pos else None,
     "has_dose_data": int(tgab_pos["dose_available_int"].sum())},
    {"group": "TgAb negative", "n": len(tgab_neg),
     "pct_of_rai_cohort": round(100*len(tgab_neg)/len(rai), 1),
     "n_recurred": int(tgab_neg["recurrence_int"].sum()),
     "pct_recurred": round(100*tgab_neg["recurrence_int"].mean()*100/100, 1) if len(tgab_neg) > 0 else 0,
     "median_tgab_last": None,
     "has_dose_data": int(tgab_neg["dose_available_int"].sum())},
]
# Fix pct_recurred
for row in tgab_rows:
    grp = tgab_pos if row["group"] == "TgAb positive" else tgab_neg
    if len(grp) > 0:
        row["pct_recurred"] = round(100 * grp["recurrence_int"].mean(), 1)

tgab_df = pd.DataFrame(tgab_rows)

# Chi-sq for recurrence difference
if len(tgab_pos) > 5 and len(tgab_neg) > 5:
    from scipy.stats import chi2_contingency
    a = int(tgab_pos["recurrence_int"].sum())
    b = int(len(tgab_pos) - a)
    c = int(tgab_neg["recurrence_int"].sum())
    d = int(len(tgab_neg) - c)
    if min(a, b, c, d) > 0:
        _, p_tgab, _, _ = chi2_contingency([[a, b], [c, d]])
        tgab_df["p_vs_other_group"] = ["--", f"{p_tgab:.4f}"]
    else:
        tgab_df["p_vs_other_group"] = ["--", "N/A"]

tgab_df.to_csv(OUT_DIR / "tgab_interference.csv", index=False)
print(f"  TgAb positive: {len(tgab_pos)}, recurrence {round(100*tgab_pos['recurrence_int'].mean(), 1):.1f}%")
print(f"  TgAb negative: {len(tgab_neg)}, recurrence {round(100*tgab_neg['recurrence_int'].mean(), 1):.1f}%")

# ---------------------------------------------------------------------------
# SECTION 4: Dose-Response Analysis (N=214)
# ---------------------------------------------------------------------------
print(f"\n[{datetime.now():%H:%M:%S}] Section 4: Dose-Response Analysis")

dose_df = rai[rai["dose_available_int"] == 1].copy()
print(f"  Dose-available patients: {len(dose_df)}")

# Dose categories
def dose_category(d):
    if pd.isna(d):
        return "unknown"
    elif d < 30:
        return "<30 mCi (low)"
    elif d < 100:
        return "30-100 mCi (standard)"
    elif d <= 150:
        return "100-150 mCi (high)"
    else:
        return ">150 mCi (therapeutic)"

dose_df["dose_category"] = dose_df["rai_max_dose_mci"].apply(dose_category)

dose_rec = (dose_df.groupby("dose_category")
            .agg(n=("research_id", "count"),
                 n_recurred=("recurrence_int", "sum"),
                 median_dose=("rai_max_dose_mci", "median"),
                 mean_dose=("rai_max_dose_mci", "mean"))
            .reset_index())
dose_rec["pct_recurred"] = (100 * dose_rec["n_recurred"] / dose_rec["n"]).round(1)
dose_rec["median_dose"] = dose_rec["median_dose"].round(1)
dose_rec["mean_dose"] = dose_rec["mean_dose"].round(1)

cat_order = ["<30 mCi (low)", "30-100 mCi (standard)", "100-150 mCi (high)", ">150 mCi (therapeutic)", "unknown"]
dose_rec["order"] = dose_rec["dose_category"].apply(lambda x: cat_order.index(x) if x in cat_order else 99)
dose_rec = dose_rec.sort_values("order").drop("order", axis=1)

print(f"  Dose categories:\n{dose_rec[['dose_category','n','median_dose','n_recurred','pct_recurred']].to_string(index=False)}")

# Sensitivity: high-confidence doses only
dose_hq = dose_df[dose_df["rai_dose_confidence_worst"] == "confirmed_with_dose"].copy() \
    if "rai_dose_confidence_worst" in dose_df.columns else pd.DataFrame()
print(f"  High-confidence dose subset: {len(dose_hq)} patients")

# Logistic regression dose vs recurrence
dose_logit_rows = []
try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import LabelEncoder

    dose_sub = dose_df[
        dose_df["rai_max_dose_mci"].notna() &
        dose_df["recurrence_int"].notna()
    ].copy()

    if len(dose_sub) > 20:
        # Simple univariate dose (log-transformed) vs recurrence
        dose_sub["log_dose"] = np.log1p(dose_sub["rai_max_dose_mci"])
        from scipy.stats import logistic
        import statsmodels.api as sm

        X = sm.add_constant(dose_sub["log_dose"])
        y = dose_sub["recurrence_int"].astype(int)
        model = sm.Logit(y, X).fit(disp=0)
        coef = model.params[1]
        pval = model.pvalues[1]
        ci_low = model.conf_int().iloc[1, 0]
        ci_high = model.conf_int().iloc[1, 1]
        dose_logit_rows.append({
            "predictor": "log(dose+1)",
            "OR": round(np.exp(coef), 3),
            "CI_low": round(np.exp(ci_low), 3),
            "CI_high": round(np.exp(ci_high), 3),
            "p_value": round(pval, 4),
            "n": len(dose_sub)
        })
        print(f"  Dose logit: OR={np.exp(coef):.3f} (p={pval:.4f})")
except Exception as e:
    print(f"  Dose logistic error: {e}")

dose_out = {
    "dose_by_category": dose_rec,
    "dose_logit": pd.DataFrame(dose_logit_rows)
}
dose_rec.to_csv(OUT_DIR / "dose_response.csv", index=False)
if dose_logit_rows:
    pd.DataFrame(dose_logit_rows).to_csv(OUT_DIR / "dose_logit.csv", index=False)
if len(dose_hq) > 0:
    dose_hq["dose_category"] = dose_hq["rai_max_dose_mci"].apply(dose_category)
    dose_hq_rec = (dose_hq.groupby("dose_category")
                   .agg(n=("research_id","count"), n_recurred=("recurrence_int","sum"))
                   .reset_index())
    dose_hq_rec["pct_recurred"] = (100*dose_hq_rec["n_recurred"]/dose_hq_rec["n"]).round(1)
    dose_hq_rec.to_csv(OUT_DIR / "dose_response_hq_only.csv", index=False)

# ---------------------------------------------------------------------------
# SECTION 5: RAI vs No-RAI Comparison (Propensity Analysis)
# ---------------------------------------------------------------------------
print(f"\n[{datetime.now():%H:%M:%S}] Section 5: RAI vs No-RAI Propensity Analysis")

# Use rai_received_int which treats NULL as 0 (not received)
cpm["rai_received_01"] = cpm["rai_received_int"]

rai_group = cpm[cpm["rai_received_01"] == 1].copy()
no_rai_group = cpm[cpm["rai_received_01"] == 0].copy()
print(f"  RAI: {len(rai_group):,}, No-RAI: {len(no_rai_group):,}")

# Demographics comparison
compare_rows = []
for var, label in [
    ("age_at_surgery", "Age (mean ± SD)"),
    ("tumor_size_cm_max", "Tumor size cm (mean ± SD)"),
]:
    r_vals = rai_group[var].dropna()
    nr_vals = no_rai_group[var].dropna()
    if len(r_vals) > 5 and len(nr_vals) > 5:
        t, p = stats.ttest_ind(r_vals, nr_vals)
        compare_rows.append({
            "variable": label,
            "rai_mean": round(r_vals.mean(), 2),
            "rai_sd": round(r_vals.std(), 2),
            "no_rai_mean": round(nr_vals.mean(), 2),
            "no_rai_sd": round(nr_vals.std(), 2),
            "p_value": round(p, 4)
        })

for var, label in [
    ("sex", "Female sex"),
    ("braf_positive_final", "BRAF+"),
    ("ln_positive", "LN positive"),
]:
    if var == "sex":
        r_n = (rai_group["sex"] == "female").sum()
        nr_n = (no_rai_group["sex"] == "female").sum()
        r_tot = rai_group["sex"].notna().sum()
        nr_tot = no_rai_group["sex"].notna().sum()
    else:
        r_n = int(rai_group[var].apply(lambda x: x is True).sum())
        nr_n = int(no_rai_group[var].apply(lambda x: x is True).sum())
        r_tot = rai_group[var].notna().sum()
        nr_tot = no_rai_group[var].notna().sum()

    if r_tot > 0 and nr_tot > 0:
        try:
            _, p = stats.chi2_contingency([[r_n, r_tot - r_n], [nr_n, nr_tot - nr_n]])[:2]
        except Exception:
            p = np.nan
        compare_rows.append({
            "variable": label,
            "rai_mean": f"{r_n}/{r_tot} ({100*r_n/r_tot:.1f}%)",
            "rai_sd": "",
            "no_rai_mean": f"{nr_n}/{nr_tot} ({100*nr_n/nr_tot:.1f}%)",
            "no_rai_sd": "",
            "p_value": round(p, 4) if not np.isnan(p) else "N/A"
        })

# Stage comparison
for stage in ["I", "II", "III", "IVA", "IVB"]:
    r_n = (rai_group["ajcc8_stage_group"] == stage).sum()
    nr_n = (no_rai_group["ajcc8_stage_group"] == stage).sum()
    r_tot = rai_group["ajcc8_stage_group"].notna().sum()
    nr_tot = no_rai_group["ajcc8_stage_group"].notna().sum()
    if r_tot > 0 and nr_tot > 0:
        compare_rows.append({
            "variable": f"AJCC Stage {stage}",
            "rai_mean": f"{r_n}/{r_tot} ({100*r_n/r_tot:.1f}%)",
            "rai_sd": "",
            "no_rai_mean": f"{nr_n}/{nr_tot} ({100*nr_n/nr_tot:.1f}%)",
            "no_rai_sd": "",
            "p_value": ""
        })

# Crude recurrence comparison
r_rec = rai_group["recurrence_int"].mean()
nr_rec = no_rai_group["recurrence_int"].mean()
r_rec_n = int(rai_group["recurrence_int"].sum())
nr_rec_n = int(no_rai_group["recurrence_int"].sum())
r_rec_tot = int(rai_group["recurrence_int"].notna().sum())
nr_rec_tot = int(no_rai_group["recurrence_int"].notna().sum())
try:
    _, p_rec = stats.chi2_contingency([
        [r_rec_n, r_rec_tot - r_rec_n],
        [nr_rec_n, nr_rec_tot - nr_rec_n]
    ])[:2]
except Exception:
    p_rec = np.nan

compare_rows.append({
    "variable": "Recurrence (any)",
    "rai_mean": f"{r_rec_n}/{r_rec_tot} ({100*r_rec:.1f}%)",
    "rai_sd": "",
    "no_rai_mean": f"{nr_rec_n}/{nr_rec_tot} ({100*nr_rec:.1f}%)",
    "no_rai_sd": "",
    "p_value": round(p_rec, 4) if not np.isnan(p_rec) else "N/A"
})

compare_df = pd.DataFrame(compare_rows)
compare_df.columns = ["variable", "rai_value", "rai_sd", "no_rai_value", "no_rai_sd", "p_value"]
compare_df.to_csv(OUT_DIR / "rai_vs_no_rai_descriptive.csv", index=False)
print(f"  Recurrence: RAI {100*r_rec:.1f}% vs No-RAI {100*nr_rec:.1f}% (p={p_rec:.4f})")

# Propensity Score Analysis (IPTW)
try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    ps_vars = ["age_at_surgery", "tumor_size_cm_max"]
    # stage dummies
    cpm_ps = cpm[
        ["research_id", "rai_received_01", "recurrence_int"] +
        ps_vars + ["ajcc8_stage_group", "ete_grade_final_v2", "ln_positive"]
    ].copy()

    cpm_ps["stage_iii"] = (cpm_ps["ajcc8_stage_group"].isin(["III", "IVA", "IVB"])).astype(float)
    cpm_ps["ln_pos_int"] = cpm_ps["ln_positive"].apply(lambda x: 1.0 if x is True else (0.0 if x is False else np.nan))

    ps_features = ["age_at_surgery", "tumor_size_cm_max", "stage_iii", "ln_pos_int"]
    cpm_ps_cc = cpm_ps.dropna(subset=ps_features + ["rai_received_01", "recurrence_int"]).copy()
    print(f"  PS complete cases: {len(cpm_ps_cc):,}")

    if len(cpm_ps_cc) > 100:
        X_ps = cpm_ps_cc[ps_features].astype(float)
        y_ps = cpm_ps_cc["rai_received_01"].astype(int)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_ps)

        ps_model = LogisticRegression(max_iter=500, random_state=42)
        ps_model.fit(X_scaled, y_ps)
        ps = ps_model.predict_proba(X_scaled)[:, 1]
        cpm_ps_cc = cpm_ps_cc.copy()
        cpm_ps_cc["propensity_score"] = ps

        # Trim extreme PS
        ps_trim = ps.clip(0.01, 0.99)

        # IPTW weights
        treat = cpm_ps_cc["rai_received_01"].values
        cpm_ps_cc["iptw_ate"] = np.where(treat == 1, 1 / ps_trim, 1 / (1 - ps_trim))
        cpm_ps_cc["iptw_att"] = np.where(treat == 1, 1.0, ps_trim / (1 - ps_trim))

        # Weighted recurrence rates
        iptw_rai = cpm_ps_cc[cpm_ps_cc["rai_received_01"] == 1].copy()
        iptw_no = cpm_ps_cc[cpm_ps_cc["rai_received_01"] == 0].copy()

        w_rec_rai = np.average(iptw_rai["recurrence_int"], weights=iptw_rai["iptw_att"])
        w_rec_no = np.average(iptw_no["recurrence_int"], weights=iptw_no["iptw_att"])
        rd = w_rec_rai - w_rec_no

        ps_summary = pd.DataFrame([{
            "n_analyzed": len(cpm_ps_cc),
            "n_rai": int(treat.sum()),
            "n_no_rai": int((1 - treat).sum()),
            "crude_recurrence_rai": round(100 * float(iptw_rai["recurrence_int"].mean()), 2),
            "crude_recurrence_no_rai": round(100 * float(iptw_no["recurrence_int"].mean()), 2),
            "iptw_att_recurrence_rai": round(100 * float(w_rec_rai), 2),
            "iptw_att_recurrence_no_rai": round(100 * float(w_rec_no), 2),
            "att_risk_difference_pct": round(100 * float(rd), 2),
            "ps_mean_rai": round(float(ps[treat == 1].mean()), 3),
            "ps_mean_no_rai": round(float(ps[treat == 0].mean()), 3),
            "note": "IPTW-ATT: treat=RAI group, covariates=age/tumor_size/stage_III/LN_pos (complete-case)"
        }])
        ps_summary.to_csv(OUT_DIR / "propensity_score_summary.csv", index=False)
        cpm_ps_cc[["research_id","rai_received_01","propensity_score","iptw_ate","iptw_att"]].to_csv(
            OUT_DIR / "propensity_scores_patient.csv", index=False)
        print(f"  IPTW-ATT: RAI {100*w_rec_rai:.1f}% vs No-RAI {100*w_rec_no:.1f}% (RD={100*rd:.1f}%)")

except Exception as e:
    print(f"  Propensity analysis error: {e}")
    pd.DataFrame([{"note": f"Error: {e}"}]).to_csv(OUT_DIR / "propensity_score_summary.csv", index=False)

pd.concat([compare_df], ignore_index=True).to_csv(OUT_DIR / "rai_vs_no_rai.csv", index=False)

# ---------------------------------------------------------------------------
# SECTION 6: RAI Timing Analysis
# ---------------------------------------------------------------------------
print(f"\n[{datetime.now():%H:%M:%S}] Section 6: RAI Timing Analysis")

timing_df = rai[rai["rai_first_days_from_surg"].notna()].copy()
timing_df["days_to_rai"] = pd.to_numeric(timing_df["rai_first_days_from_surg"], errors="coerce")
timing_df = timing_df[timing_df["days_to_rai"] > 0].copy()
print(f"  Patients with timing data: {len(timing_df):,}")

timing_stats = pd.DataFrame([{
    "n": len(timing_df),
    "median_days": round(timing_df["days_to_rai"].median(), 0),
    "mean_days": round(timing_df["days_to_rai"].mean(), 1),
    "q25": round(timing_df["days_to_rai"].quantile(0.25), 0),
    "q75": round(timing_df["days_to_rai"].quantile(0.75), 0),
    "pct_lt90d": round(100 * (timing_df["days_to_rai"] < 90).mean(), 1),
    "pct_90_180d": round(100 * ((timing_df["days_to_rai"] >= 90) & (timing_df["days_to_rai"] <= 180)).mean(), 1),
    "pct_gt180d": round(100 * (timing_df["days_to_rai"] > 180).mean(), 1),
}])

# Early vs late RAI
timing_df["timing_group"] = timing_df["days_to_rai"].apply(
    lambda d: "early_lt90d" if d < 90 else ("mid_90_180d" if d <= 180 else "late_gt180d"))

timing_rec = (timing_df.groupby("timing_group")
              .agg(n=("research_id","count"),
                   n_recurred=("recurrence_int","sum"),
                   median_days=("days_to_rai","median"))
              .reset_index())
timing_rec["pct_recurred"] = (100 * timing_rec["n_recurred"] / timing_rec["n"]).round(1)

timing_stats.to_csv(OUT_DIR / "rai_timing_stats.csv", index=False)
timing_rec.to_csv(OUT_DIR / "rai_timing.csv", index=False)
print(f"  Timing groups:\n{timing_rec.to_string(index=False)}")

# ---------------------------------------------------------------------------
# SECTION 7: ATA Dynamic Risk Assessment
# ---------------------------------------------------------------------------
print(f"\n[{datetime.now():%H:%M:%S}] Section 7: ATA Dynamic Risk Assessment")

ata_df = rai[rai["ata_response_category"].notna()].copy()
print(f"  Patients with ATA response: {len(ata_df):,}")

ata_rec = (ata_df.groupby("ata_response_category")
           .agg(n=("research_id","count"),
                n_recurred=("recurrence_int","sum"))
           .reset_index())
ata_rec["pct_recurred"] = (100 * ata_rec["n_recurred"] / ata_rec["n"]).round(1)
ata_rec.to_csv(OUT_DIR / "ata_response_recurrence.csv", index=False)

# Concordance between Tg trajectory and ATA response (where both available)
both_df = ata_df[ata_df["tg_trajectory_class"].notna()].copy()
if len(both_df) > 5:
    concordance = pd.crosstab(both_df["tg_trajectory_class"], both_df["ata_response_category"])
    concordance.to_csv(OUT_DIR / "tg_ata_concordance.csv")
    print(f"  Tg-ATA concordance table (N={len(both_df)}) saved")

# ---------------------------------------------------------------------------
# SECTION 8: LaTeX Summary Tables
# ---------------------------------------------------------------------------
print(f"\n[{datetime.now():%H:%M:%S}] Section 8: LaTeX Tables")

latex_lines = [
    r"\documentclass{article}",
    r"\usepackage{booktabs,longtable,geometry,array}",
    r"\geometry{margin=1in}",
    r"\begin{document}",
    "",
    r"\section*{M019: RAI Outcomes Analysis --- Summary Tables}",
    rf"\textit{{Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | N=862 RAI recipients}}",
    "",
    r"\subsection*{Table 1: RAI Utilization by AJCC Stage (Malignant Cohort, N=" + str(n_malignant) + r")}",
    r"\begin{tabular}{lrrr}",
    r"\toprule",
    r"AJCC Stage & Total & Received RAI & \% RAI \\",
    r"\midrule",
]
for _, row in stage_util.iterrows():
    n_r = int(row["n_rai"]) if not pd.isna(row["n_rai"]) else 0
    latex_lines.append(rf"{row.get('value', row.get('ajcc8_stage_group','?'))} & {int(row['n_total'])} & {n_r} & {row['pct_rai']}\% \\")
latex_lines += [r"\bottomrule", r"\end{tabular}", ""]

# Tg trajectory table
latex_lines += [
    r"\subsection*{Table 2: Recurrence by Tg Trajectory Class (N=" + str(int(traj_rec["n"].sum())) + r")}",
    r"\textit{Excludes TgAb interference patients. Suppressed = reference.}",
    r"\begin{tabular}{lrrr}",
    r"\toprule",
    r"Tg Trajectory & N & Recurred (\%) & OR (vs suppressed) \\",
    r"\midrule",
]
or_lookup = {}
if "trajectory" in tg_or_df.columns:
    for _, r in tg_or_df.iterrows():
        key = r["trajectory"].replace(" (ref)", "")
        or_lookup[key] = r
for _, row in traj_rec.iterrows():
    tclass = row["tg_trajectory_class"]
    or_info = or_lookup.get(tclass, {})
    if tclass == "suppressed":
        or_str = "1.00 (ref)"
    elif isinstance(or_info.get("or"), str) or or_info.get("or") == "N/A":
        or_str = "N/A"
    else:
        or_v = or_info.get("or", "")
        ci_l = or_info.get("ci_low", "")
        ci_h = or_info.get("ci_high", "")
        or_str = f"{or_v} ({ci_l}--{ci_h})" if or_v else ""
    latex_lines.append(rf"{tclass} & {int(row['n'])} & {int(row['n_recurred'])} ({row['pct_recurred']}\%) & {or_str} \\")
latex_lines += [r"\bottomrule", r"\end{tabular}", ""]

# Dose response table
latex_lines += [
    r"\subsection*{Table 3: Recurrence by Dose Category (N=214 with dose data)}",
    r"\textit{CAUTION: Only 214/862 (24.8\%) have dose data. Quality is variable.}",
    r"\begin{tabular}{lrrrr}",
    r"\toprule",
    r"Dose Category & N & Median Dose (mCi) & Recurred & \% \\",
    r"\midrule",
]
for _, row in dose_rec.iterrows():
    dc = row["dose_category"]
    latex_lines.append(rf"{dc} & {int(row['n'])} & {row['median_dose']} & {int(row['n_recurred'])} & {row['pct_recurred']}\% \\")
latex_lines += [r"\bottomrule", r"\end{tabular}", ""]

# Timing table
latex_lines += [
    r"\subsection*{Table 4: Time from Surgery to RAI}",
    r"\begin{tabular}{lrrrr}",
    r"\toprule",
    r"Timing Group & N & Median Days & Recurred & \% \\",
    r"\midrule",
]
for _, row in timing_rec.iterrows():
    latex_lines.append(rf"{row['timing_group']} & {int(row['n'])} & {row['median_days']} & {int(row['n_recurred'])} & {row['pct_recurred']}\% \\")
latex_lines += [r"\bottomrule", r"\end{tabular}", ""]

# Data limitations note
latex_lines += [
    r"\subsection*{Data Limitations}",
    r"\begin{itemize}",
    r"\item 75.2\% of RAI recipients lack dose data (\textit{n}=648/862)",
    r"\item 34.3\% lack Tg trajectory data (\textit{n}=296/862)",
    r"\item 95.9\% lack formal ATA response category (\textit{n}=827/862)",
    r"\item 88.5\% have NULL or unknown RAI intent (613 NULL + 150 unknown = 763/862)",
    r"\item Only 35 patients are `confirmed\_with\_dose' (high-confidence tier)",
    r"\item TgAb interference affects 58 patients (6.7\%) --- Tg unreliable in these patients",
    r"\end{itemize}",
    "",
    r"\end{document}",
]

with open(OUT_DIR / "rai_outcomes_summary.tex", "w") as f:
    f.write("\n".join(latex_lines))

print("  LaTeX saved")

# ---------------------------------------------------------------------------
# SECTION 9: Upload to MotherDuck
# ---------------------------------------------------------------------------
print(f"\n[{datetime.now():%H:%M:%S}] Section 9: Uploading m019_rai_analysis_v1 to MotherDuck")

# Build patient-level analysis table with propensity scores and classifications
m019_upload = rai.copy()

# Add dose category
m019_upload["dose_category"] = m019_upload["rai_max_dose_mci"].apply(dose_category)

# Add timing group
if "rai_first_days_from_surg" in m019_upload.columns:
    def timing_grp(d):
        try:
            d = float(d)
            if d <= 0 or np.isnan(d):
                return None
            elif d < 90:
                return "early_lt90d"
            elif d <= 180:
                return "mid_90_180d"
            else:
                return "late_gt180d"
        except Exception:
            return None
    m019_upload["timing_group"] = m019_upload["rai_first_days_from_surg"].apply(timing_grp)

# Add tg_trajectory_numeric for ordering
traj_map = {"suppressed": 0, "low_stable": 1, "detectable_stable": 2, "rising": 3, "insufficient_data": 4}
m019_upload["tg_trajectory_numeric"] = m019_upload["tg_trajectory_class"].map(traj_map)

# Add propensity scores if computed
if (OUT_DIR / "propensity_scores_patient.csv").exists():
    ps_load = pd.read_csv(OUT_DIR / "propensity_scores_patient.csv", dtype={"research_id": str})
    m019_upload = m019_upload.merge(ps_load[["research_id", "propensity_score", "iptw_att"]], on="research_id", how="left")

# Select key columns for upload
upload_cols = [
    "research_id", "age_at_surgery", "sex", "ajcc8_stage_group", "histology_pub_category",
    "rai_received_reconciled", "rai_validation_tier", "n_rai_episodes", "rai_max_dose_mci",
    "rai_dose_data_available", "rai_dose_confidence_worst", "dose_category",
    "rai_first_days_from_surg", "timing_group", "rai_intent_v9",
    "rai_avid_flag", "rai_avidity",
    "tg_data_available", "tg_trajectory_class", "tg_trajectory_numeric",
    "tg_nadir", "tg_peak", "tg_last_value", "tg_rising_flag",
    "post_rai_tg_nadir", "post_rai_tg_last", "post_rai_tg_count",
    "max_stimulated_tg",
    "tgab_interference_flag", "tgab_last_value",
    "ata_risk_category", "ata_response_category",
    "any_recurrence_flag", "braf_positive_final", "ras_positive_final",
    "molecular_risk_tier", "tumor_size_cm_dominant",
    "ete_grade_clean", "vascular_invasion_final", "ln_positive_final",
]
# Add PS columns if present
if "propensity_score" in m019_upload.columns:
    upload_cols += ["propensity_score", "iptw_att"]

upload_cols = [c for c in upload_cols if c in m019_upload.columns]
m019_out = m019_upload[upload_cols].copy()

try:
    conn.execute("DROP TABLE IF EXISTS manuscript_workspace.m019_rai_analysis_v1")
    conn.register("m019_temp", m019_out)
    conn.execute("""
        CREATE TABLE manuscript_workspace.m019_rai_analysis_v1 AS
        SELECT * FROM m019_temp
    """)
    count = conn.execute("SELECT COUNT(*) FROM manuscript_workspace.m019_rai_analysis_v1").fetchone()[0]
    print(f"  Uploaded m019_rai_analysis_v1: {count:,} rows, {len(upload_cols)} columns")
except Exception as e:
    print(f"  Upload error: {e}")

conn.close()

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
print(f"\n[{datetime.now():%H:%M:%S}] COMPLETE — Output files in {OUT_DIR}/:")
for f in sorted(OUT_DIR.iterdir()):
    print(f"  {f.name} ({f.stat().st_size:,} bytes)")
