#!/usr/bin/env python3
"""
Proposal 2 — Statistical Audit & Deterministic Reproduction Pipeline
=====================================================================

Audits and reproduces all analyses for the ETE extension manuscript using
the expanded cohort (N=3,278 all PTC) with the original classic cohort
(N=596) as a sensitivity comparison.

Audit scope
-----------
1. AJCC 7th derivation correctness (T3b mapping fix, N substaging gap)
2. Ordinal logistic regression recomputation + proportional odds check
3. AJCC7 → AJCC8 stage migration validation
4. Cross-validated AUC (vs original apparent performance)
5. LN ratio data quality flag
6. Outcome circularity documentation

Outputs
-------
audit_tables/   — CSV tables (demographics, migration, regression, sensitivity)
audit_figures/  — Deterministic PNG + PDF figures (all 9)
audit_report.md — Structured audit narrative
analysis_metadata.yaml — Full provenance

Reproducibility: every stochastic operation uses SEED = 42.
"""

import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
import hashlib
import json
import sys
import time

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import yaml
from scipy import stats
from scipy.stats import chi2_contingency, kruskal
from statsmodels.stats.contingency_tables import mcnemar
from statsmodels.miscmodels.ordinal_model import OrderedModel
from sklearn.metrics import roc_auc_score, roc_curve, confusion_matrix
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import label_binarize
from lifelines import KaplanMeierFitter
from lifelines.statistics import logrank_test

SEED = 42
np.random.seed(SEED)

STUDY_DIR = Path(__file__).resolve().parent
ROOT = STUDY_DIR.parent.parent
FIG_DIR = STUDY_DIR / "audit_figures"
TBL_DIR = STUDY_DIR / "audit_tables"
FIG_DIR.mkdir(exist_ok=True)
TBL_DIR.mkdir(exist_ok=True)

AGGRESSIVE_VARIANTS = [
    "Tall cell", "Columnar cell", "Solid variant", "Diffuse sclerosing",
]

sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)
plt.rcParams.update({
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "font.family": "sans-serif",
})

audit_findings = []


def finding(severity, tag, message):
    """Register an audit finding."""
    audit_findings.append({"severity": severity, "tag": tag, "message": message})
    marker = {"CRITICAL": "🔴", "IMPORTANT": "🟠", "MINOR": "🟡", "INFO": "🟢"}
    print(f"  {marker.get(severity, '·')} [{severity}] {tag}: {message}")


def pval_str(p):
    if pd.isna(p):
        return "—"
    return "<0.001" if p < 0.001 else f"{p:.3f}"


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


# ═══════════════════════════════════════════════════════════════════════
# 0. DATA LOADING
# ═══════════════════════════════════════════════════════════════════════

def load_expanded_cohort():
    """Build expanded PTC dataset (mirrors proposal2_expanded_cohort.py)."""
    rec = pd.read_csv(ROOT / "exports" / "recurrence_full.csv")
    img = pd.read_csv(ROOT / "exports" / "imaging_correlation.csv")
    ptc_orig = pd.read_csv(ROOT / "exports" / "ptc_full.csv")

    ptc_rec = rec[rec["histology_1_type"] == "PTC"].copy()
    ptc_rec = ptc_rec.drop_duplicates(subset=["research_id"], keep="first")

    img_dedup = img[["research_id", "largest_tumor_cm"]].drop_duplicates(
        subset=["research_id"], keep="first"
    )
    ptc_all = ptc_rec.merge(img_dedup, on="research_id", how="left",
                            suffixes=("", "_img"))
    if "largest_tumor_cm_img" in ptc_all.columns:
        ptc_all["largest_tumor_cm"] = ptc_all["largest_tumor_cm"].fillna(
            ptc_all["largest_tumor_cm_img"]
        )
        ptc_all.drop(columns=["largest_tumor_cm_img"], inplace=True)

    orig_cols = ["research_id", "ln_examined", "ln_positive", "m_stage_ajcc8",
                 "tumor_1_ete_microscopic_only"]
    orig_dedup = ptc_orig[orig_cols].drop_duplicates(
        subset=["research_id"], keep="first"
    )
    ptc_all = ptc_all.merge(orig_dedup, on="research_id", how="left")

    ptc_all["is_in_original_cohort"] = ptc_all["research_id"].isin(
        set(ptc_orig["research_id"])
    )
    ptc_all["variant_label"] = ptc_all["variant_standardized"].fillna(
        "Classic/Unspecified"
    )
    ptc_all["is_aggressive"] = ptc_all["variant_standardized"].isin(
        AGGRESSIVE_VARIANTS
    )

    return ptc_all, ptc_orig


# ═══════════════════════════════════════════════════════════════════════
# 1. ETE CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════

def classify_ete(df):
    df = df.copy()
    ete_any = df["tumor_1_extrathyroidal_ext"].astype(str).str.lower().isin(
        ["true", "1", "yes"]
    )
    gross = df["tumor_1_gross_ete"].fillna(0).astype(float) == 1
    conditions = [gross, ete_any & ~gross, ~ete_any]
    choices = ["Gross ETE", "Microscopic ETE", "No ETE"]
    df["ete_group"] = np.select(conditions, choices, default="Unknown")
    df["ete_group"] = pd.Categorical(
        df["ete_group"],
        categories=["No ETE", "Microscopic ETE", "Gross ETE"],
        ordered=True,
    )
    df["ete_micro"] = (df["ete_group"] == "Microscopic ETE").astype(int)
    df["ete_gross"] = (df["ete_group"] == "Gross ETE").astype(int)
    df["female"] = (df["sex"] == "Female").astype(int)
    df["ln_ratio"] = (
        df["ln_positive"].fillna(0) /
        df["ln_examined"].replace(0, np.nan)
    ).fillna(np.nan)

    risk_map = {"low": 0, "intermediate": 1, "high": 2}
    df["risk_ord"] = df["recurrence_risk_band"].map(risk_map)
    return df


# ═══════════════════════════════════════════════════════════════════════
# 2. AJCC 7th DERIVATION — CORRECTED
# ═══════════════════════════════════════════════════════════════════════

def derive_ajcc7_corrected(df):
    """
    Derive AJCC 7th T-stage and overall stage.

    CORRECTION vs original: AJCC 8th T3b (strap muscle invasion) maps to
    AJCC 7th T3, NOT T4a.  In the 7th edition, gross ETE limited to strap
    muscles was T3; T4a required invasion of subcutaneous soft tissues,
    larynx, trachea, esophagus, or recurrent laryngeal nerve.
    """
    df = df.copy()
    size = df["largest_tumor_cm"].fillna(0)
    ete_g = df["ete_group"].astype(str)
    t8 = df["t_stage_ajcc8"].fillna("")

    ajcc7_t = []
    for i in range(len(df)):
        s, eg, t = size.iloc[i], ete_g.iloc[i], t8.iloc[i]
        if t == "T4b":
            ajcc7_t.append("T4b")
        elif t == "T4a":
            ajcc7_t.append("T4a")
        elif t == "T3b":
            # CORRECTED: strap-muscle-only invasion was T3 in AJCC 7th
            ajcc7_t.append("T3")
        elif eg == "Microscopic ETE":
            # Any mETE ≤4 cm → T3 in AJCC 7th
            ajcc7_t.append("T3")
        elif s > 4:
            ajcc7_t.append("T3")
        elif s > 2:
            ajcc7_t.append("T2")
        elif s > 1:
            ajcc7_t.append("T1b")
        elif s > 0:
            ajcc7_t.append("T1a")
        else:
            ajcc7_t.append("Unknown")

    df["t_stage_ajcc7"] = ajcc7_t

    age = df["age_at_surgery"].fillna(45)
    n = df["n_stage_ajcc8"].fillna("NX")
    m = df.get("m_stage_ajcc8", pd.Series("M0", index=df.index)).fillna("M0")

    stage7 = []
    for i in range(len(df)):
        a = age.iloc[i]
        t7 = ajcc7_t[i]
        ni = n.iloc[i]
        mi = m.iloc[i] if isinstance(m.iloc[i], str) else "M0"

        if a < 45:
            stage7.append("II" if mi == "M1" else "I")
        elif t7 in ("T4a", "T4b"):
            if mi == "M1":
                stage7.append("IVC")
            elif t7 == "T4b":
                stage7.append("IVB")
            else:
                stage7.append("IVA")
        elif t7 == "T3":
            stage7.append("IVA" if ni.startswith("N1") else "III")
        elif ni.startswith("N1"):
            # N substaging (N1a vs N1b) not reliably available; treat
            # all N1 with T1/T2 as Stage III (correct for N1a;
            # N1b should be IVA — documented as limitation)
            stage7.append("III")
        elif t7 in ("T1a", "T1b"):
            stage7.append("I")
        elif t7 == "T2":
            stage7.append("II")
        else:
            stage7.append("I")

    df["overall_stage_ajcc7"] = stage7
    return df


def derive_ajcc7_original(df):
    """Original (uncorrected) AJCC 7th derivation for comparison."""
    df = df.copy()
    size = df["largest_tumor_cm"].fillna(0)
    ete_g = df["ete_group"].astype(str)
    t8 = df["t_stage_ajcc8"].fillna("")

    ajcc7_t = []
    for i in range(len(df)):
        s, eg, t = size.iloc[i], ete_g.iloc[i], t8.iloc[i]
        if t in ("T4a", "T4b"):
            ajcc7_t.append(t)
        elif t == "T3b":
            ajcc7_t.append("T4a")  # ORIGINAL (incorrect) mapping
        elif eg == "Microscopic ETE":
            ajcc7_t.append("T3")
        elif s > 4:
            ajcc7_t.append("T3")
        elif s > 2:
            ajcc7_t.append("T2")
        elif s > 1:
            ajcc7_t.append("T1b")
        elif s > 0:
            ajcc7_t.append("T1a")
        else:
            ajcc7_t.append("Unknown")

    df["t_stage_ajcc7_orig"] = ajcc7_t

    age = df["age_at_surgery"].fillna(45)
    n = df["n_stage_ajcc8"].fillna("NX")
    m = df.get("m_stage_ajcc8", pd.Series("M0", index=df.index)).fillna("M0")

    stage7 = []
    for i in range(len(df)):
        a = age.iloc[i]
        t7 = ajcc7_t[i]
        ni = n.iloc[i]
        mi = m.iloc[i] if isinstance(m.iloc[i], str) else "M0"
        if a < 45:
            stage7.append("II" if mi == "M1" else "I")
        elif t7 in ("T4a", "T4b"):
            if mi == "M1":
                stage7.append("IVC")
            else:
                stage7.append("IVA")
        elif t7 == "T3":
            stage7.append("IVA" if ni.startswith("N1") else "III")
        elif ni.startswith("N1"):
            stage7.append("III")
        elif t7 in ("T1a", "T1b"):
            stage7.append("I")
        elif t7 == "T2":
            stage7.append("II")
        else:
            stage7.append("I")

    df["overall_stage_ajcc7_orig"] = stage7
    return df


# ═══════════════════════════════════════════════════════════════════════
# 3. AUDIT: COMPARE CORRECTED VS ORIGINAL AJCC 7th
# ═══════════════════════════════════════════════════════════════════════

def audit_ajcc7_correction(df):
    """Quantify the impact of the T3b mapping correction."""
    results = {}

    t3b_patients = df[df["t_stage_ajcc8"] == "T3b"]
    n_t3b = len(t3b_patients)
    results["n_T3b_patients"] = n_t3b

    if n_t3b > 0:
        n_changed_t = (
            t3b_patients["t_stage_ajcc7"] != t3b_patients["t_stage_ajcc7_orig"]
        ).sum()
        results["n_T_stage_changed"] = int(n_changed_t)

        n_changed_overall = (
            t3b_patients["overall_stage_ajcc7"] !=
            t3b_patients["overall_stage_ajcc7_orig"]
        ).sum()
        results["n_overall_stage_changed"] = int(n_changed_overall)

        finding(
            "CRITICAL", "AJCC7_T3b_MAP",
            f"T3b→T4a mapping affected {n_t3b} patients. "
            f"Corrected to T3b→T3: {n_changed_t} T-stage and "
            f"{n_changed_overall} overall-stage reclassifications."
        )
    else:
        finding("INFO", "AJCC7_T3b_MAP", "No T3b patients in this cohort.")

    return results


# ═══════════════════════════════════════════════════════════════════════
# 4. TABLE 1: DEMOGRAPHICS
# ═══════════════════════════════════════════════════════════════════════

def compute_table1(df):
    groups = ["No ETE", "Microscopic ETE", "Gross ETE"]
    n_total = len(df)
    table_rows = []

    counts = df["ete_group"].value_counts()
    table_rows.append({
        "Variable": "N (%)",
        "No ETE": f'{counts.get("No ETE", 0)} ({100*counts.get("No ETE", 0)/n_total:.1f}%)',
        "Microscopic ETE": f'{counts.get("Microscopic ETE", 0)} ({100*counts.get("Microscopic ETE", 0)/n_total:.1f}%)',
        "Gross ETE": f'{counts.get("Gross ETE", 0)} ({100*counts.get("Gross ETE", 0)/n_total:.1f}%)',
        "p-value": "",
    })

    # Age
    age_data = {g: df.loc[df["ete_group"] == g, "age_at_surgery"].dropna()
                for g in groups}
    _, p_k = kruskal(*[age_data[g] for g in groups if len(age_data[g]) > 0])
    row_age = {"Variable": "Age, mean +/- SD"}
    for g in groups:
        s = age_data[g]
        row_age[g] = f"{s.mean():.1f} +/- {s.std():.1f}" if len(s) > 0 else "—"
    row_age["p-value"] = pval_str(p_k)
    table_rows.append(row_age)

    # Age >= 55
    for g in groups:
        sub = df[df["ete_group"] == g]
    ct_age = pd.crosstab(df["ete_group"], (df["age_at_surgery"] >= 55).astype(int))
    try:
        _, p_age55, _, _ = chi2_contingency(ct_age)
    except ValueError:
        p_age55 = np.nan
    row_age55 = {"Variable": "Age >= 55, n (%)"}
    for g in groups:
        sub = df[df["ete_group"] == g]
        n_ge55 = (sub["age_at_surgery"] >= 55).sum()
        row_age55[g] = f"{n_ge55} ({100*n_ge55/len(sub):.1f}%)" if len(sub) > 0 else "—"
    row_age55["p-value"] = pval_str(p_age55)
    table_rows.append(row_age55)

    # Sex
    ct_sex = pd.crosstab(df["ete_group"], df["sex"])
    _, p_sex, _, _ = chi2_contingency(ct_sex)
    row_sex = {"Variable": "Female sex, n (%)"}
    for g in groups:
        sub = df[df["ete_group"] == g]
        n_f = (sub["sex"] == "Female").sum()
        row_sex[g] = f"{n_f} ({100*n_f/len(sub):.1f}%)" if len(sub) > 0 else "—"
    row_sex["p-value"] = pval_str(p_sex)
    table_rows.append(row_sex)

    # Tumor size
    size_data = {g: df.loc[df["ete_group"] == g, "largest_tumor_cm"].dropna()
                 for g in groups}
    valid_sizes = [size_data[g] for g in groups if len(size_data[g]) > 0]
    _, p_size = kruskal(*valid_sizes) if len(valid_sizes) >= 2 else (np.nan, np.nan)
    row_size = {"Variable": "Tumor size (cm), median [IQR]"}
    for g in groups:
        s = size_data[g]
        if len(s) > 0:
            row_size[g] = f"{s.median():.1f} [{s.quantile(0.25):.1f}-{s.quantile(0.75):.1f}]"
        else:
            row_size[g] = "—"
    row_size["p-value"] = pval_str(p_size)
    table_rows.append(row_size)

    # Tumor size categories
    df["size_cat"] = pd.cut(
        df["largest_tumor_cm"], bins=[0, 1, 2, 4, np.inf],
        labels=["<=1 cm", "1.1-2 cm", "2.1-4 cm", ">4 cm"], right=True,
    )
    ct_sc = pd.crosstab(df["ete_group"], df["size_cat"])
    try:
        _, p_sc, _, _ = chi2_contingency(ct_sc)
    except ValueError:
        p_sc = np.nan
    for i, cat in enumerate(["<=1 cm", "1.1-2 cm", "2.1-4 cm", ">4 cm"]):
        row = {"Variable": f"  {cat}, n (%)"}
        for g in groups:
            sub = df[df["ete_group"] == g]
            n_cat = (sub["size_cat"] == cat).sum()
            row[g] = f"{n_cat} ({100*n_cat/len(sub):.1f}%)" if len(sub) > 0 else "—"
        row["p-value"] = pval_str(p_sc) if i == 0 else ""
        table_rows.append(row)

    # T-stage (AJCC8)
    ct_ts = pd.crosstab(df["ete_group"], df["t_stage_ajcc8"])
    try:
        _, p_ts, _, _ = chi2_contingency(ct_ts)
    except ValueError:
        p_ts = np.nan
    for i, ts in enumerate(["T1a", "T1b", "T2", "T3a", "T3b", "T4a"]):
        row = {"Variable": f"  {ts}, n (%)"}
        for g in groups:
            sub = df[df["ete_group"] == g]
            n_ts = (sub["t_stage_ajcc8"] == ts).sum()
            row[g] = f"{n_ts} ({100*n_ts/len(sub):.1f}%)" if len(sub) > 0 else "—"
        row["p-value"] = pval_str(p_ts) if i == 0 else ""
        table_rows.append(row)

    # Overall stage (AJCC8)
    ct_os = pd.crosstab(df["ete_group"], df["overall_stage_ajcc8"])
    try:
        _, p_os, _, _ = chi2_contingency(ct_os)
    except ValueError:
        p_os = np.nan
    for i, stg in enumerate(["I", "II", "III", "IVB"]):
        row = {"Variable": f"  Stage {stg}, n (%)"}
        for g in groups:
            sub = df[df["ete_group"] == g]
            n_os = (sub["overall_stage_ajcc8"] == stg).sum()
            row[g] = f"{n_os} ({100*n_os/len(sub):.1f}%)" if len(sub) > 0 else "—"
        row["p-value"] = pval_str(p_os) if i == 0 else ""
        table_rows.append(row)

    # N1
    df["n_positive_flag"] = df["n_stage_ajcc8"].fillna("NX").str.startswith("N1").astype(int)
    ct_ns = pd.crosstab(df["ete_group"], df["n_positive_flag"])
    try:
        _, p_ns, _, _ = chi2_contingency(ct_ns)
    except ValueError:
        p_ns = np.nan
    row_ns = {"Variable": "N1 (any), n (%)"}
    for g in groups:
        sub = df[df["ete_group"] == g]
        n_n1 = sub["n_positive_flag"].sum()
        row_ns[g] = f"{n_n1} ({100*n_n1/len(sub):.1f}%)" if len(sub) > 0 else "—"
    row_ns["p-value"] = pval_str(p_ns)
    table_rows.append(row_ns)

    # Recurrence risk band
    sub_rrb = df[df["recurrence_risk_band"].notna()]
    ct_rrb = pd.crosstab(sub_rrb["ete_group"], sub_rrb["recurrence_risk_band"])
    try:
        _, p_rrb, _, _ = chi2_contingency(ct_rrb)
    except ValueError:
        p_rrb = np.nan
    for i, band in enumerate(["low", "intermediate", "high"]):
        row = {"Variable": f"  Risk: {band}, n (%)"}
        for g in groups:
            sub = df[(df["ete_group"] == g) & (df["recurrence_risk_band"].notna())]
            n_b = (sub["recurrence_risk_band"] == band).sum()
            row[g] = f"{n_b} ({100*n_b/len(sub):.1f}%)" if len(sub) > 0 else "—"
        row["p-value"] = pval_str(p_rrb) if i == 0 else ""
        table_rows.append(row)

    return pd.DataFrame(table_rows)


# ═══════════════════════════════════════════════════════════════════════
# 5. STAGE MIGRATION
# ═══════════════════════════════════════════════════════════════════════

def stage_migration_analysis(df, version="corrected"):
    """Quantify downstaging from AJCC7 → AJCC8."""
    t7_col = "t_stage_ajcc7" if version == "corrected" else "t_stage_ajcc7_orig"
    s7_col = "overall_stage_ajcc7" if version == "corrected" else "overall_stage_ajcc7_orig"

    results = {}
    df_valid = df[df[t7_col].notna() & df["t_stage_ajcc8"].notna()].copy()

    mete = df_valid[df_valid["ete_group"] == "Microscopic ETE"]
    n_downstaged_t = (
        (mete[t7_col] == "T3") &
        (mete["t_stage_ajcc8"].isin(["T1a", "T1b", "T2"]))
    ).sum()
    n_mete = len(mete)
    results["n_mete_total"] = n_mete
    results["n_downstaged_t"] = int(n_downstaged_t)
    results["pct_downstaged_t"] = 100 * n_downstaged_t / n_mete if n_mete > 0 else 0

    stage_order = ["I", "II", "III", "IVA", "IVB", "IVC"]
    stage_map = {s: i for i, s in enumerate(stage_order)}

    df_valid["stage7_num"] = df_valid[s7_col].map(stage_map)
    df_valid["stage8_num"] = df_valid["overall_stage_ajcc8"].map(stage_map)
    df_stg = df_valid.dropna(subset=["stage7_num", "stage8_num"])

    n_down = int((df_stg["stage8_num"] < df_stg["stage7_num"]).sum())
    n_up = int((df_stg["stage8_num"] > df_stg["stage7_num"]).sum())
    n_same = int((df_stg["stage8_num"] == df_stg["stage7_num"]).sum())
    results["n_overall_downstaged"] = n_down
    results["n_overall_upstaged"] = n_up
    results["n_overall_same"] = n_same
    results["n_staged"] = len(df_stg)
    results["pct_overall_downstaged"] = 100 * n_down / len(df_stg) if len(df_stg) > 0 else 0

    # McNemar test on stage >= III
    high7 = (df_stg["stage7_num"] >= stage_map["III"]).astype(int)
    high8 = (df_stg["stage8_num"] >= stage_map["III"]).astype(int)
    try:
        table_mc = pd.crosstab(high7, high8)
        mc_result = mcnemar(table_mc, exact=True)
        results["mcnemar_stat"] = float(mc_result.statistic)
        results["mcnemar_pval"] = float(mc_result.pvalue)
    except Exception as e:
        results["mcnemar_stat"] = np.nan
        results["mcnemar_pval"] = np.nan
        finding("MINOR", "MCNEMAR", f"McNemar test failed: {e}")

    # Cross-tab
    migration_table = pd.crosstab(
        df_stg[s7_col], df_stg["overall_stage_ajcc8"], margins=True,
    )
    results["migration_table"] = migration_table

    # mETE-specific overall migration
    mete_stg = df_stg[df_stg["ete_group"] == "Microscopic ETE"]
    results["mete_downstaged_overall"] = int(
        (mete_stg["stage8_num"] < mete_stg["stage7_num"]).sum()
    )
    results["mete_total_staged"] = len(mete_stg)

    return results, df_stg


# ═══════════════════════════════════════════════════════════════════════
# 6. ORDINAL LOGISTIC REGRESSION + PROPORTIONAL ODDS CHECK
# ═══════════════════════════════════════════════════════════════════════

def run_ordinal_regression(df_model, exog_vars, label=""):
    X = df_model[exog_vars].astype(float)
    y = df_model["risk_ord"].astype(float)

    try:
        model = OrderedModel(y, X, distr="logit")
        res = model.fit(method="bfgs", disp=False)
        params = res.params
        ci = res.conf_int()
        rows = []
        for var in exog_vars:
            idx = list(X.columns).index(var)
            coef = params.iloc[idx]
            rows.append({
                "Subgroup": label,
                "Variable": var,
                "Coef": float(coef),
                "OR": float(np.exp(coef)),
                "CI_lo": float(np.exp(ci.iloc[idx, 0])),
                "CI_hi": float(np.exp(ci.iloc[idx, 1])),
                "p_value": float(res.pvalues.iloc[idx]),
            })
        return pd.DataFrame(rows), res
    except Exception as e:
        finding("IMPORTANT", "ORDINAL_REG",
                f"Ordinal regression failed for {label}: {e}")
        return None, None


def check_proportional_odds(df_model, exog_vars):
    """
    Approximate proportional odds test: fit separate binary logistic
    regressions at each cut-point and compare coefficients.  A formal
    Brant test requires R; this provides an equivalent diagnostic.
    """
    X = df_model[exog_vars].astype(float)
    y = df_model["risk_ord"].astype(float)

    cut_results = {}
    for cut_label, threshold in [("low|mid+high", 0.5), ("low+mid|high", 1.5)]:
        y_bin = (y > threshold).astype(int)
        if y_bin.nunique() < 2:
            continue
        import statsmodels.api as sm
        X_const = sm.add_constant(X)
        try:
            logit_res = sm.Logit(y_bin, X_const).fit(disp=False)
            cut_results[cut_label] = {
                var: float(logit_res.params[var])
                for var in exog_vars if var in logit_res.params.index
            }
        except Exception:
            pass

    if len(cut_results) == 2:
        cuts = list(cut_results.keys())
        max_diff = 0
        max_var = ""
        for var in exog_vars:
            if var in cut_results[cuts[0]] and var in cut_results[cuts[1]]:
                diff = abs(cut_results[cuts[0]][var] - cut_results[cuts[1]][var])
                if diff > max_diff:
                    max_diff = diff
                    max_var = var

        if max_diff > 1.0:
            finding(
                "IMPORTANT", "PROP_ODDS",
                f"Proportional odds assumption may be violated: largest "
                f"coefficient difference across cut-points is {max_diff:.2f} "
                f"for '{max_var}'. Consider partial proportional odds model."
            )
        else:
            finding(
                "INFO", "PROP_ODDS",
                f"Proportional odds check: max coefficient difference "
                f"across cut-points = {max_diff:.2f} ('{max_var}'). "
                f"Assumption appears reasonable."
            )

    return cut_results


# ═══════════════════════════════════════════════════════════════════════
# 7. CROSS-VALIDATED AUC
# ═══════════════════════════════════════════════════════════════════════

def cross_validated_auc(df_eval, exog_vars_base, exog_vars_full, n_folds=5):
    """5-fold stratified CV for apparent vs held-out AUC comparison."""
    df_cv = df_eval.dropna(subset=["risk_ord", "age_at_surgery",
                                    "largest_tumor_cm"]).copy()
    df_cv["ln_ratio"] = df_cv["ln_ratio"].fillna(0)
    df_cv["high_risk"] = (df_cv["risk_ord"] == 2).astype(int)
    df_cv = df_cv.dropna(subset=["high_risk"])

    if len(df_cv) < 50 or df_cv["high_risk"].nunique() < 2:
        return {}

    results = {}
    skf = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=SEED)

    for name, feats in [("Base", exog_vars_base), ("Full", exog_vars_full)]:
        X = df_cv[feats].values
        y = df_cv["high_risk"].values

        # Apparent AUC
        lr_app = LogisticRegression(max_iter=1000, random_state=SEED)
        lr_app.fit(X, y)
        y_prob_app = lr_app.predict_proba(X)[:, 1]
        try:
            results[f"AUC_{name}_apparent"] = float(roc_auc_score(y, y_prob_app))
        except Exception:
            results[f"AUC_{name}_apparent"] = np.nan

        # CV AUC
        cv_aucs = []
        for train_idx, test_idx in skf.split(X, y):
            X_train, X_test = X[train_idx], X[test_idx]
            y_train, y_test = y[train_idx], y[test_idx]
            lr_cv = LogisticRegression(max_iter=1000, random_state=SEED)
            lr_cv.fit(X_train, y_train)
            y_prob_cv = lr_cv.predict_proba(X_test)[:, 1]
            try:
                cv_aucs.append(roc_auc_score(y_test, y_prob_cv))
            except Exception:
                pass
        if cv_aucs:
            results[f"AUC_{name}_CV_mean"] = float(np.mean(cv_aucs))
            results[f"AUC_{name}_CV_std"] = float(np.std(cv_aucs))
            optimism = results[f"AUC_{name}_apparent"] - results[f"AUC_{name}_CV_mean"]
            results[f"AUC_{name}_optimism"] = float(optimism)

    if "AUC_Full_CV_mean" in results and "AUC_Base_CV_mean" in results:
        results["delta_AUC_CV"] = float(
            results["AUC_Full_CV_mean"] - results["AUC_Base_CV_mean"]
        )
        results["delta_AUC_apparent"] = float(
            results["AUC_Full_apparent"] - results["AUC_Base_apparent"]
        )

    return results


# ═══════════════════════════════════════════════════════════════════════
# 8. MULTIPLE IMPUTATION (Rubin's Rules)
# ═══════════════════════════════════════════════════════════════════════

def _pmm_impute_once(df, rng, cols_to_impute):
    imp = df.copy()
    for col in cols_to_impute:
        if col not in imp.columns:
            continue
        mask = imp[col].isna()
        if mask.sum() == 0:
            continue
        observed = imp.loc[~mask, col]
        if len(observed) == 0:
            continue
        drawn = rng.choice(observed.values, size=mask.sum(), replace=True)
        jitter = rng.normal(0, observed.std() * 0.05, size=mask.sum())
        imp.loc[mask, col] = np.clip(drawn + jitter, 0, None)
    return imp


def run_mi_pipeline(df, exog_vars, m=20):
    impute_cols = ["ln_ratio", "largest_tumor_cm", "tg_max"]
    rng = np.random.RandomState(SEED)
    coefs_list, se_list = [], []

    for _ in range(m):
        imp = _pmm_impute_once(df, rng, impute_cols)
        imp_model = imp.dropna(
            subset=["risk_ord", "age_at_surgery"] + exog_vars
        ).copy()
        if len(imp_model) < 50:
            continue
        X = imp_model[exog_vars].astype(float)
        y = imp_model["risk_ord"].astype(float)
        try:
            model = OrderedModel(y, X, distr="logit")
            res = model.fit(method="bfgs", disp=False)
            coefs_list.append(res.params[:len(exog_vars)].values)
            se_list.append(np.sqrt(np.diag(res.cov_params())[:len(exog_vars)]))
        except Exception:
            continue

    if not coefs_list:
        return None

    Q_bar = np.mean(coefs_list, axis=0)
    U_bar = np.mean(np.array(se_list) ** 2, axis=0)
    B = np.var(coefs_list, axis=0, ddof=1)
    T = U_bar + (1 + 1 / m) * B

    pooled_se = np.sqrt(T)
    pooled_or = np.exp(Q_bar)
    ci_lo = np.exp(Q_bar - 1.96 * pooled_se)
    ci_hi = np.exp(Q_bar + 1.96 * pooled_se)
    z = Q_bar / pooled_se
    p_vals = 2 * (1 - stats.norm.cdf(np.abs(z)))

    rows = []
    for j, var in enumerate(exog_vars):
        rows.append({
            "Subgroup": "MI (m=20, Rubin's rules)",
            "Variable": var,
            "Coef": float(Q_bar[j]),
            "OR": float(pooled_or[j]),
            "CI_lo": float(ci_lo[j]),
            "CI_hi": float(ci_hi[j]),
            "p_value": float(p_vals[j]),
        })
    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════
# 9. DATA QUALITY AUDIT
# ═══════════════════════════════════════════════════════════════════════

def audit_data_quality(df):
    results = {}

    # LN ratio quality
    ln_avail = df["ln_examined"].notna().sum()
    ln_binary = ((df["ln_examined"] == 1) | (df["ln_examined"] == 0)).sum()
    results["ln_examined_available"] = int(ln_avail)
    results["ln_examined_binary_pct"] = float(
        100 * ln_binary / ln_avail if ln_avail > 0 else 0
    )

    if results["ln_examined_binary_pct"] > 80:
        finding(
            "CRITICAL", "LN_RATIO_QUALITY",
            f"ln_examined is effectively binary for "
            f"{results['ln_examined_binary_pct']:.0f}% of available values "
            f"({ln_avail}/{len(df)}). LN ratio acts as a binary variable, "
            f"not continuous. Verify whether this column represents total "
            f"nodes examined or a binary indicator."
        )

    # Outcome circularity
    if "recurrence_risk_band" in df.columns:
        n_gross = (df["ete_group"] == "Gross ETE").sum()
        n_gross_high = (
            (df["ete_group"] == "Gross ETE") &
            (df["recurrence_risk_band"] == "high")
        ).sum()
        pct_gross_high = 100 * n_gross_high / n_gross if n_gross > 0 else 0
        results["gross_ete_high_risk_pct"] = float(pct_gross_high)

        if pct_gross_high > 90:
            finding(
                "IMPORTANT", "OUTCOME_CIRCULARITY",
                f"{pct_gross_high:.1f}% of gross ETE patients are classified "
                f"as high risk. The recurrence_risk_band includes gross ETE "
                f"in its derivation, inflating the gross ETE OR. The mETE OR "
                f"is the clinically meaningful coefficient."
            )

    # m_stage availability
    m_avail = df["m_stage_ajcc8"].notna().sum()
    results["m_stage_available"] = int(m_avail)
    results["m_stage_pct"] = float(100 * m_avail / len(df))
    if results["m_stage_pct"] < 25:
        finding(
            "MINOR", "M_STAGE_MISSING",
            f"m_stage_ajcc8 available for only {results['m_stage_pct']:.1f}% "
            f"of the expanded cohort. Missing M-stage defaulted to M0."
        )

    return results


# ═══════════════════════════════════════════════════════════════════════
# 10. FIGURES (deterministic)
# ═══════════════════════════════════════════════════════════════════════

def plot_fig1_ete_distribution(df):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    counts = df["ete_group"].value_counts().reindex(
        ["No ETE", "Microscopic ETE", "Gross ETE"]
    )
    colors = ["#2ecc71", "#f39c12", "#e74c3c"]
    counts.plot.bar(ax=axes[0], color=colors, edgecolor="black", linewidth=0.5)
    axes[0].set_title("A. ETE Classification Distribution")
    axes[0].set_ylabel("Number of Patients")
    axes[0].set_xlabel("")
    axes[0].tick_params(axis="x", rotation=0)
    for i, v in enumerate(counts):
        axes[0].text(i, v + len(df)*0.005, f"{v}\n({100*v/len(df):.1f}%)",
                     ha="center", fontsize=9)

    axes[1].pie(counts, labels=counts.index, autopct="%1.1f%%", colors=colors,
                startangle=90, textprops={"fontsize": 10})
    axes[1].set_title("B. ETE Group Proportions")

    fig.suptitle(
        f"Figure 1. ETE Classification (N={len(df):,} PTC)",
        fontsize=13, fontweight="bold", y=1.02,
    )
    plt.tight_layout()
    fig.savefig(FIG_DIR / "fig1_ete_distribution.png")
    fig.savefig(FIG_DIR / "fig1_ete_distribution.pdf")
    plt.close(fig)


def plot_fig2_stage_migration(df):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    w = 0.35

    t7_counts = df["t_stage_ajcc7"].value_counts()
    t8_counts = df["t_stage_ajcc8"].value_counts()
    stages_t = ["T1a", "T1b", "T2", "T3", "T3a", "T3b", "T4a"]
    t7_vals = [t7_counts.get(s, 0) for s in stages_t]
    t8_vals = [t8_counts.get(s, 0) for s in stages_t]
    x = np.arange(len(stages_t))
    axes[0].bar(x - w/2, t7_vals, w, label="AJCC 7th (corrected)",
                color="#3498db", edgecolor="black", linewidth=0.5)
    axes[0].bar(x + w/2, t8_vals, w, label="AJCC 8th",
                color="#e74c3c", edgecolor="black", linewidth=0.5)
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(stages_t, rotation=45)
    axes[0].set_ylabel("Number of Patients")
    axes[0].set_title("A. T-Stage Distribution: AJCC 7th (corrected) vs 8th")
    axes[0].legend()

    stages_o = ["I", "II", "III", "IVA", "IVB", "IVC"]
    s7_counts = df["overall_stage_ajcc7"].value_counts()
    s8_counts = df["overall_stage_ajcc8"].value_counts()
    s7_vals = [s7_counts.get(s, 0) for s in stages_o]
    s8_vals = [s8_counts.get(s, 0) for s in stages_o]
    x2 = np.arange(len(stages_o))
    axes[1].bar(x2 - w/2, s7_vals, w, label="AJCC 7th (corrected)",
                color="#3498db", edgecolor="black", linewidth=0.5)
    axes[1].bar(x2 + w/2, s8_vals, w, label="AJCC 8th",
                color="#e74c3c", edgecolor="black", linewidth=0.5)
    axes[1].set_xticks(x2)
    axes[1].set_xticklabels(stages_o, rotation=45)
    axes[1].set_ylabel("Number of Patients")
    axes[1].set_title("B. Overall Stage: AJCC 7th (corrected) vs 8th")
    axes[1].legend()

    fig.suptitle("Figure 2. Stage Migration (Corrected AJCC 7th Derivation)",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(FIG_DIR / "fig2_stage_migration.png")
    fig.savefig(FIG_DIR / "fig2_stage_migration.pdf")
    plt.close(fig)


def plot_fig3_risk_by_ete(df):
    df_plot = df.dropna(subset=["recurrence_risk_band"]).copy()
    risk_order = ["low", "intermediate", "high"]
    risk_colors = {"low": "#2ecc71", "intermediate": "#f39c12", "high": "#e74c3c"}
    ete_order = ["No ETE", "Microscopic ETE", "Gross ETE"]

    ct = pd.crosstab(df_plot["ete_group"], df_plot["recurrence_risk_band"])
    ct = ct.reindex(index=ete_order, columns=risk_order, fill_value=0)
    ct_pct = ct.div(ct.sum(axis=1), axis=0) * 100

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    ct.plot.bar(stacked=True, ax=axes[0],
                color=[risk_colors[r] for r in risk_order],
                edgecolor="black", linewidth=0.5)
    axes[0].set_title("A. Recurrence Risk by ETE (Counts)")
    axes[0].set_ylabel("Number of Patients")
    axes[0].set_xlabel("")
    axes[0].tick_params(axis="x", rotation=0)
    axes[0].legend(title="Risk Band")

    ct_pct.plot.bar(stacked=True, ax=axes[1],
                    color=[risk_colors[r] for r in risk_order],
                    edgecolor="black", linewidth=0.5)
    axes[1].set_title("B. Recurrence Risk by ETE (%)")
    axes[1].set_ylabel("Percentage")
    axes[1].set_xlabel("")
    axes[1].tick_params(axis="x", rotation=0)
    axes[1].legend(title="Risk Band")
    axes[1].yaxis.set_major_formatter(mticker.PercentFormatter())

    fig.suptitle("Figure 3. ATA Recurrence Risk by ETE Type",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(FIG_DIR / "fig3_risk_by_ete.png")
    fig.savefig(FIG_DIR / "fig3_risk_by_ete.pdf")
    plt.close(fig)


def plot_fig4_roc_curves(df):
    df_eval = df.dropna(subset=["recurrence_risk_band", "age_at_surgery",
                                 "largest_tumor_cm"]).copy()
    risk_map = {"low": 0, "intermediate": 1, "high": 2}
    df_eval["risk_num"] = df_eval["recurrence_risk_band"].map(risk_map)
    df_eval["high_risk"] = (df_eval["risk_num"] == 2).astype(int)
    df_eval["ln_ratio"] = df_eval["ln_ratio"].fillna(0)

    fig, ax = plt.subplots(figsize=(8, 8))
    models = {
        "Base (size+age+sex+LN)": ["age_at_surgery", "female",
                                    "largest_tumor_cm", "ln_ratio"],
        "+ Gross ETE only": ["ete_gross", "age_at_surgery", "female",
                             "largest_tumor_cm", "ln_ratio"],
        "+ Gross & Microscopic ETE": ["ete_micro", "ete_gross", "age_at_surgery",
                                      "female", "largest_tumor_cm", "ln_ratio"],
    }
    colors_roc = ["#3498db", "#e67e22", "#e74c3c"]

    for (name, feats), color in zip(models.items(), colors_roc):
        X = df_eval[feats].values
        y = df_eval["high_risk"].values
        lr = LogisticRegression(max_iter=1000, random_state=SEED)
        lr.fit(X, y)
        y_prob = lr.predict_proba(X)[:, 1]
        try:
            auc = roc_auc_score(y, y_prob)
            fpr, tpr, _ = roc_curve(y, y_prob)
            ax.plot(fpr, tpr, color=color, lw=2,
                    label=f"{name} (AUC={auc:.3f})")
        except Exception:
            pass

    ax.plot([0, 1], [0, 1], "k--", lw=1, alpha=0.5)
    ax.set_xlabel("1 - Specificity (FPR)")
    ax.set_ylabel("Sensitivity (TPR)")
    ax.set_title("Figure 4. ROC Curves — High Risk Prediction\n"
                 "(Apparent performance; see audit for CV AUC)",
                 fontweight="bold")
    ax.legend(loc="lower right", fontsize=9)
    ax.set_xlim([-0.01, 1.01])
    ax.set_ylim([-0.01, 1.01])
    plt.tight_layout()
    fig.savefig(FIG_DIR / "fig4_roc_curves.png")
    fig.savefig(FIG_DIR / "fig4_roc_curves.pdf")
    plt.close(fig)


def plot_fig5_tg_trajectory(df):
    df_tg = df.dropna(subset=["tg_first_value", "tg_last_value"]).copy()
    if len(df_tg) < 10:
        return

    ete_order = ["No ETE", "Microscopic ETE", "Gross ETE"]
    colors = {"No ETE": "#2ecc71", "Microscopic ETE": "#f39c12",
              "Gross ETE": "#e74c3c"}

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    data_boxes = [df_tg.loc[df_tg["ete_group"] == g, "tg_max"].dropna()
                  for g in ete_order]
    bp = axes[0].boxplot(data_boxes, labels=ete_order, patch_artist=True,
                         showfliers=False)
    for patch, g in zip(bp["boxes"], ete_order):
        patch.set_facecolor(colors[g])
    axes[0].set_ylabel("Maximum Thyroglobulin (ng/mL)")
    axes[0].set_title("A. Peak Tg by ETE Group")

    data_delta = [df_tg.loc[df_tg["ete_group"] == g,
                             "tg_delta_per_measurement"].dropna()
                  for g in ete_order]
    bp2 = axes[1].boxplot(data_delta, labels=ete_order, patch_artist=True,
                          showfliers=False)
    for patch, g in zip(bp2["boxes"], ete_order):
        patch.set_facecolor(colors[g])
    axes[1].set_ylabel("Tg delta per Measurement (ng/mL)")
    axes[1].set_title("B. Tg Trend by ETE Group")

    fig.suptitle("Figure 5. Thyroglobulin Dynamics by ETE Type",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(FIG_DIR / "fig5_tg_trajectory.png")
    fig.savefig(FIG_DIR / "fig5_tg_trajectory.pdf")
    plt.close(fig)


def plot_fig6_forest(sensitivity_df):
    from matplotlib.lines import Line2D
    plot_df = sensitivity_df[
        sensitivity_df["Variable"].isin(["ete_micro", "ete_gross"])
    ].copy()
    plot_df["label"] = (
        plot_df["Subgroup"] + "\n" + plot_df["Variable"].map(
            {"ete_micro": "Microscopic ETE", "ete_gross": "Gross ETE"}
        )
    )
    plot_df = plot_df.sort_values(["Variable", "Subgroup"])

    fig, ax = plt.subplots(figsize=(10, max(6, len(plot_df) * 0.55)))
    color_map = {"ete_micro": "#f39c12", "ete_gross": "#e74c3c"}

    for i, (_, row) in enumerate(plot_df.iterrows()):
        color = color_map.get(row["Variable"], "#333")
        display_or = min(row["OR"], 50)
        display_lo = row["CI_lo"]
        display_hi = min(row["CI_hi"], 50)
        ax.errorbar(
            display_or, i,
            xerr=[[display_or - display_lo], [display_hi - display_or]],
            fmt="o", color=color, markersize=8, capsize=4, linewidth=1.5,
            markeredgecolor="black", markeredgewidth=0.5,
        )
        ax.text(display_hi + 0.05, i, f"OR={row['OR']:.2f}", va="center",
                fontsize=8, color=color)

    ax.set_yticks(range(len(plot_df)))
    ax.set_yticklabels(plot_df["label"].values, fontsize=9)
    ax.axvline(x=1, color="black", linestyle="--", linewidth=1, alpha=0.6)
    ax.set_xscale("log")
    ax.set_xlabel("Odds Ratio (log scale)")
    ax.set_title("Figure 6. Forest Plot — ETE ORs Across Sensitivity Analyses",
                 fontsize=13, fontweight="bold")
    legend_elements = [
        Line2D([0], [0], marker="o", color="w", label="Microscopic ETE",
               markerfacecolor="#f39c12", markersize=10),
        Line2D([0], [0], marker="o", color="w", label="Gross ETE",
               markerfacecolor="#e74c3c", markersize=10),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize=9)
    ax.invert_yaxis()
    plt.tight_layout()
    fig.savefig(FIG_DIR / "fig6_forest_plot.png")
    fig.savefig(FIG_DIR / "fig6_forest_plot.pdf")
    plt.close(fig)


def plot_fig7_kaplan_meier(df):
    df_km = df.copy()
    for col in ["tg_first_date", "tg_last_date"]:
        if col not in df_km.columns:
            return False
        df_km[col] = pd.to_datetime(df_km[col], errors="coerce")

    df_km = df_km.dropna(subset=["tg_first_date", "tg_last_date"])
    df_km["follow_up_days"] = (
        df_km["tg_last_date"] - df_km["tg_first_date"]
    ).dt.days
    df_km = df_km[df_km["follow_up_days"] > 0]

    if len(df_km) < 30:
        return False

    df_km["event"] = (df_km["recurrence_risk_band"] == "high").astype(int)

    fig, ax = plt.subplots(figsize=(10, 7))
    kmf = KaplanMeierFitter()
    colors = {"No ETE": "#2ecc71", "Microscopic ETE": "#f39c12",
              "Gross ETE": "#e74c3c"}

    for group in ["No ETE", "Microscopic ETE", "Gross ETE"]:
        mask = df_km["ete_group"] == group
        sub = df_km[mask]
        if len(sub) < 5:
            continue
        kmf.fit(sub["follow_up_days"] / 365.25, event_observed=sub["event"],
                label=f"{group} (n={len(sub)})")
        kmf.plot_survival_function(ax=ax, color=colors.get(group, "#333"),
                                   linewidth=2)

    no_ete = df_km[df_km["ete_group"] == "No ETE"]
    gross = df_km[df_km["ete_group"] == "Gross ETE"]
    if len(no_ete) >= 5 and len(gross) >= 5:
        lr_test = logrank_test(
            no_ete["follow_up_days"] / 365.25,
            gross["follow_up_days"] / 365.25,
            event_observed_A=no_ete["event"],
            event_observed_B=gross["event"],
        )
        ax.annotate(
            f"Log-rank (No vs Gross): p = {lr_test.p_value:.4f}",
            xy=(0.02, 0.02), xycoords="axes fraction", fontsize=9,
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray",
                      alpha=0.8),
        )

    ax.set_xlabel("Follow-up (years)")
    ax.set_ylabel("Event-Free Proportion")
    ax.set_title("Figure 7. Kaplan-Meier by ETE Group\n"
                 "(Proxy: Tg follow-up; event = high risk)",
                 fontsize=12, fontweight="bold")
    ax.legend(loc="lower left", fontsize=10)
    plt.tight_layout()
    fig.savefig(FIG_DIR / "fig7_kaplan_meier.png")
    fig.savefig(FIG_DIR / "fig7_kaplan_meier.pdf")
    plt.close(fig)
    return True


def plot_fig8_cohort_flow(cohort_sizes):
    labels = [c["label"] for c in cohort_sizes]
    ns = [c["N"] for c in cohort_sizes]
    cc_ns = [c["cc_n"] for c in cohort_sizes]

    fig, ax = plt.subplots(figsize=(12, 7))
    x = np.arange(len(labels))
    w = 0.35
    bars1 = ax.bar(x - w/2, ns, w, label="Total cohort",
                   color="#2ecc71", edgecolor="black", linewidth=0.5)
    bars2 = ax.bar(x + w/2, cc_ns, w, label="Complete-case N",
                   color="#3498db", edgecolor="black", linewidth=0.5)
    for bar, val in zip(bars1, ns):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 30,
                f"N={val}", ha="center", va="bottom", fontsize=9,
                fontweight="bold")
    for bar, val in zip(bars2, cc_ns):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 30,
                f"n={val}", ha="center", va="bottom", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=15, ha="right", fontsize=10)
    ax.set_ylabel("Number of Patients")
    ax.set_title("Figure 8. Cohort Size Comparison",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=10)
    ax.set_ylim(0, max(ns) * 1.25)
    plt.tight_layout()
    fig.savefig(FIG_DIR / "fig8_cohort_flow.png")
    fig.savefig(FIG_DIR / "fig8_cohort_flow.pdf")
    plt.close(fig)


def plot_fig9_forest_expanded(cohort_results):
    from matplotlib.lines import Line2D
    rows = []
    for cr in cohort_results:
        if cr.get("mi_or") is not None:
            mete = cr["mi_or"][cr["mi_or"]["Variable"] == "ete_micro"]
            if len(mete) > 0:
                r = mete.iloc[0]
                rows.append({
                    "label": f"{cr['label']}\n(MI, N={cr['N']})",
                    "OR": r["OR"], "CI_lo": r["CI_lo"], "CI_hi": r["CI_hi"],
                    "source": "MI",
                })
        if cr.get("cc_or") is not None:
            mete = cr["cc_or"][cr["cc_or"]["Variable"] == "ete_micro"]
            if len(mete) > 0:
                r = mete.iloc[0]
                rows.append({
                    "label": f"{cr['label']}\n(CC, n={cr['cc_n']})",
                    "OR": r["OR"], "CI_lo": r["CI_lo"], "CI_hi": r["CI_hi"],
                    "source": "CC",
                })

    if not rows:
        return

    plot_df = pd.DataFrame(rows)
    fig, ax = plt.subplots(figsize=(11, max(6, len(plot_df) * 0.6)))
    colors = {"MI": "#e67e22", "CC": "#3498db"}

    for i, (_, row) in enumerate(plot_df.iterrows()):
        color = colors.get(row["source"], "#333")
        display_or = min(row["OR"], 50)
        display_hi = min(row["CI_hi"], 50)
        ax.errorbar(
            display_or, i,
            xerr=[[display_or - row["CI_lo"]], [display_hi - display_or]],
            fmt="o", color=color, markersize=8, capsize=4, linewidth=1.5,
            markeredgecolor="black", markeredgewidth=0.5,
        )
        ax.text(display_hi + 0.05, i, f"OR={row['OR']:.2f}", va="center",
                fontsize=8, color=color)

    ax.set_yticks(range(len(plot_df)))
    ax.set_yticklabels(plot_df["label"].values, fontsize=9)
    ax.axvline(x=1, color="black", linestyle="--", linewidth=1, alpha=0.6)
    ax.set_xscale("log")
    ax.set_xlabel("Odds Ratio (log scale)")
    ax.set_title("Figure 9. mETE ORs Across Expanded Cohorts",
                 fontsize=12, fontweight="bold")
    legend_elements = [
        Line2D([0], [0], marker="o", color="w", label="Multiple Imputation",
               markerfacecolor="#e67e22", markersize=10),
        Line2D([0], [0], marker="o", color="w", label="Complete Case",
               markerfacecolor="#3498db", markersize=10),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize=9)
    ax.invert_yaxis()
    plt.tight_layout()
    fig.savefig(FIG_DIR / "fig9_forest_expanded.png")
    fig.savefig(FIG_DIR / "fig9_forest_expanded.pdf")
    plt.close(fig)


# ═══════════════════════════════════════════════════════════════════════
# 11. REPORT + METADATA
# ═══════════════════════════════════════════════════════════════════════

def write_audit_report(
    df, table1, mig_corrected, mig_original, ajcc7_audit,
    regression_results, sensitivity_df, cv_auc, dq_results,
    prop_odds, cohort_flow, expanded_results
):
    lines = []
    lines.append("# Statistical Audit Report — Proposal 2 ETE Staging Manuscript")
    lines.append(f"\n*Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append(f"\n*Cohort: N={len(df):,} all PTC (expanded)*")
    lines.append(f"\n*Seed: {SEED} (all stochastic operations)*")
    lines.append("")

    # Audit findings
    lines.append("## Audit Findings Summary")
    lines.append("")
    for f in audit_findings:
        sev = f["severity"]
        lines.append(f"- **[{sev}] {f['tag']}**: {f['message']}")
    lines.append("")

    # Table 1
    lines.append("## Table 1. Cohort Demographics by ETE Group")
    lines.append("")
    lines.append(table1.to_markdown(index=False))
    lines.append("")

    # Stage migration
    lines.append("## Stage Migration (Corrected AJCC 7th Derivation)")
    lines.append("")
    lines.append(
        f"- mETE T-stage downstaged: {mig_corrected['n_downstaged_t']}/"
        f"{mig_corrected['n_mete_total']} "
        f"({mig_corrected['pct_downstaged_t']:.1f}%)"
    )
    lines.append(
        f"- Overall downstaged: {mig_corrected['n_overall_downstaged']}/"
        f"{mig_corrected['n_staged']} "
        f"({mig_corrected['pct_overall_downstaged']:.1f}%)"
    )
    lines.append(
        f"- Upstaged: {mig_corrected['n_overall_upstaged']}"
    )
    if not np.isnan(mig_corrected.get("mcnemar_pval", np.nan)):
        lines.append(
            f"- McNemar (stage>=III): stat={mig_corrected['mcnemar_stat']:.1f}, "
            f"p={pval_str(mig_corrected['mcnemar_pval'])}"
        )
    lines.append("")

    if ajcc7_audit.get("n_T3b_patients", 0) > 0:
        lines.append("### Impact of T3b Mapping Correction")
        lines.append("")
        lines.append(
            f"- Original analysis mapped AJCC 8th T3b -> AJCC 7th T4a "
            f"(incorrect; strap muscle = T3 in AJCC 7th)"
        )
        lines.append(
            f"- Correction: T3b -> T3 (AJCC 7th). "
            f"Affected {ajcc7_audit['n_T3b_patients']} patients."
        )
        lines.append(
            f"- T-stage reclassified: {ajcc7_audit['n_T_stage_changed']}, "
            f"Overall stage reclassified: {ajcc7_audit['n_overall_stage_changed']}"
        )
        lines.append(
            f"- Original downstaging: {mig_original['pct_overall_downstaged']:.1f}% | "
            f"Corrected: {mig_corrected['pct_overall_downstaged']:.1f}%"
        )
        lines.append("")

    lines.append("### Migration Cross-Tabulation (Corrected)")
    lines.append("")
    lines.append(mig_corrected["migration_table"].to_markdown())
    lines.append("")

    # Regression
    lines.append("## Ordinal Logistic Regression")
    lines.append("")
    if regression_results is not None:
        or_display = regression_results.copy()
        or_display["OR_fmt"] = or_display["OR"].apply(lambda x: f"{x:.2f}")
        or_display["CI_fmt"] = or_display.apply(
            lambda r: f"({r['CI_lo']:.2f}-{r['CI_hi']:.2f})", axis=1
        )
        or_display["p_fmt"] = or_display["p_value"].apply(pval_str)
        lines.append(
            or_display[["Variable", "OR_fmt", "CI_fmt", "p_fmt"]].rename(
                columns={"OR_fmt": "OR", "CI_fmt": "95% CI", "p_fmt": "p-value"}
            ).to_markdown(index=False)
        )
        lines.append("")

    # Proportional odds
    if prop_odds:
        lines.append("### Proportional Odds Diagnostic")
        lines.append("")
        lines.append("Binary logistic coefficients at each cut-point:")
        lines.append("")
        lines.append("| Variable | low\\|mid+high | low+mid\\|high | Difference |")
        lines.append("|----------|-------------|-------------|------------|")
        if len(prop_odds) == 2:
            cuts = list(prop_odds.keys())
            all_vars = set()
            for c in cuts:
                all_vars.update(prop_odds[c].keys())
            for var in sorted(all_vars):
                v1 = prop_odds[cuts[0]].get(var, np.nan)
                v2 = prop_odds[cuts[1]].get(var, np.nan)
                diff = abs(v1 - v2) if not (np.isnan(v1) or np.isnan(v2)) else np.nan
                lines.append(
                    f"| {var} | {v1:.3f} | {v2:.3f} | {diff:.3f} |"
                )
        lines.append("")

    # Cross-validated AUC
    lines.append("## Model Discrimination (AUC)")
    lines.append("")
    if cv_auc:
        lines.append("| Model | Apparent AUC | CV AUC (5-fold) | Optimism |")
        lines.append("|-------|-------------|-----------------|----------|")
        for mdl in ["Base", "Full"]:
            app = cv_auc.get(f"AUC_{mdl}_apparent", np.nan)
            cv = cv_auc.get(f"AUC_{mdl}_CV_mean", np.nan)
            opt = cv_auc.get(f"AUC_{mdl}_optimism", np.nan)
            lines.append(
                f"| {mdl} | {app:.4f} | {cv:.4f} +/- "
                f"{cv_auc.get(f'AUC_{mdl}_CV_std', 0):.4f} | {opt:.4f} |"
            )
        if "delta_AUC_CV" in cv_auc:
            lines.append(
                f"\ndelta AUC (mETE contribution): "
                f"Apparent={cv_auc['delta_AUC_apparent']:.4f}, "
                f"CV={cv_auc['delta_AUC_CV']:.4f}"
            )
    lines.append("")

    # Sensitivity table
    if sensitivity_df is not None and not sensitivity_df.empty:
        lines.append("## Sensitivity Analyses")
        lines.append("")
        mete_only = sensitivity_df[sensitivity_df["Variable"] == "ete_micro"]
        if not mete_only.empty:
            disp = mete_only.copy()
            disp["OR_fmt"] = disp["OR"].apply(lambda x: f"{x:.2f}")
            disp["CI_fmt"] = disp.apply(
                lambda r: f"({r['CI_lo']:.2f}-{r['CI_hi']:.2f})", axis=1
            )
            disp["p_fmt"] = disp["p_value"].apply(pval_str)
            lines.append(
                disp[["Subgroup", "OR_fmt", "CI_fmt", "p_fmt"]].rename(
                    columns={"OR_fmt": "mETE OR", "CI_fmt": "95% CI",
                             "p_fmt": "p-value"}
                ).to_markdown(index=False)
            )
        lines.append("")

    # Data quality
    lines.append("## Data Quality Flags")
    lines.append("")
    for k, v in dq_results.items():
        if isinstance(v, float):
            lines.append(f"- {k}: {v:.1f}")
        else:
            lines.append(f"- {k}: {v}")
    lines.append("")

    report_text = "\n".join(lines)
    (STUDY_DIR / "audit_report.md").write_text(report_text)
    return report_text


def write_metadata(
    df, mig_results, cv_auc, regression_results, sensitivity_df,
    expanded_results, start_time
):
    hashes = {}
    for export in ["ptc_full.csv", "recurrence_full.csv",
                    "imaging_correlation.csv"]:
        p = ROOT / "exports" / export
        if p.exists():
            hashes[export] = sha256_file(p)

    meta = {
        "study": "Proposal 2 — AJCC 8th Edition Staging & ETE",
        "audit_generated": pd.Timestamp.now().isoformat(),
        "runtime_seconds": round(time.time() - start_time, 1),
        "seed": SEED,
        "python_version": sys.version,
        "packages": {},
        "data": {
            "source_hashes": hashes,
            "expanded_cohort_N": int(len(df)),
            "original_classic_N": 596,
            "unique_patients_recurrence_PTC": 3278,
        },
        "cohort": {
            "N_total": int(len(df)),
            "N_no_ete": int((df["ete_group"] == "No ETE").sum()),
            "N_micro_ete": int((df["ete_group"] == "Microscopic ETE").sum()),
            "N_gross_ete": int((df["ete_group"] == "Gross ETE").sum()),
        },
        "stage_migration": {
            "mete_t_downstaged_n": int(mig_results["n_downstaged_t"]),
            "mete_t_downstaged_pct": float(round(mig_results["pct_downstaged_t"], 1)),
            "overall_downstaged_n": int(mig_results["n_overall_downstaged"]),
            "overall_downstaged_pct": float(round(
                mig_results["pct_overall_downstaged"], 1
            )),
        },
        "auc": {
            k: round(v, 4) for k, v in cv_auc.items()
        } if cv_auc else {},
        "audit_findings": [
            {"severity": f["severity"], "tag": f["tag"], "message": f["message"]}
            for f in audit_findings
        ],
        "outputs": {
            "tables": sorted(str(p.name) for p in TBL_DIR.glob("*.csv")),
            "figures": sorted(str(p.name) for p in FIG_DIR.glob("*.png")),
            "report": "audit_report.md",
            "metadata": "analysis_metadata.yaml",
        },
    }

    for pkg_name in ["numpy", "pandas", "scipy", "statsmodels", "sklearn",
                     "matplotlib", "lifelines"]:
        try:
            mod = __import__(pkg_name)
            meta["packages"][pkg_name] = mod.__version__
        except Exception:
            pass

    if regression_results is not None:
        mete_row = regression_results[
            regression_results["Variable"] == "ete_micro"
        ]
        if not mete_row.empty:
            r = mete_row.iloc[0]
            meta["primary_result"] = {
                "variable": "ete_micro",
                "OR": round(float(r["OR"]), 4),
                "CI_lo": round(float(r["CI_lo"]), 4),
                "CI_hi": round(float(r["CI_hi"]), 4),
                "p_value": round(float(r["p_value"]), 6),
            }

    if expanded_results:
        meta["expanded_cohorts"] = []
        for cr in expanded_results:
            entry = {
                "label": cr["label"],
                "N": cr["N"],
                "cc_n": cr["cc_n"],
            }
            mi = cr.get("mi_or")
            cc = cr.get("cc_or")
            or_df = mi if mi is not None and not mi.empty else cc
            if or_df is not None:
                mete = or_df[or_df["Variable"] == "ete_micro"]
                if len(mete) > 0:
                    r = mete.iloc[0]
                    entry["mETE_OR"] = round(float(r["OR"]), 4)
                    entry["mETE_CI"] = f"({r['CI_lo']:.2f}-{r['CI_hi']:.2f})"
                    entry["mETE_p"] = pval_str(r["p_value"])
            meta["expanded_cohorts"].append(entry)

    yaml_path = STUDY_DIR / "analysis_metadata.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(meta, f, default_flow_style=False, sort_keys=False,
                  allow_unicode=True)

    return meta


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    start_time = time.time()

    print("=" * 72)
    print("PROPOSAL 2 — STATISTICAL AUDIT & DETERMINISTIC REPRODUCTION")
    print("Expanded Cohort (N=3,278 all PTC)")
    print("=" * 72)

    # ── Load ──
    print("\n[1/12] Loading expanded cohort...")
    df_all, ptc_orig = load_expanded_cohort()
    df_all = classify_ete(df_all)
    print(f"  Expanded cohort: {len(df_all)} patients")
    print(f"  ETE: {df_all['ete_group'].value_counts().to_dict()}")

    # ── Data quality audit ──
    print("\n[2/12] Data quality audit...")
    dq = audit_data_quality(df_all)

    # ── AJCC7 derivation (both versions) ──
    print("\n[3/12] Deriving AJCC 7th staging (corrected + original)...")
    df_all = derive_ajcc7_corrected(df_all)
    df_all = derive_ajcc7_original(df_all)

    # ── Audit T3b correction impact ──
    print("\n[4/12] Auditing AJCC 7th T3b correction...")
    ajcc7_audit = audit_ajcc7_correction(df_all)

    # ── Table 1 ──
    print("\n[5/12] Computing Table 1 demographics...")
    table1 = compute_table1(df_all)
    table1.to_csv(TBL_DIR / "table1_demographics.csv", index=False)
    print(f"  Saved audit_tables/table1_demographics.csv")

    # ── Stage migration (corrected) ──
    print("\n[6/12] Stage migration analysis (corrected)...")
    mig_corrected, mig_df_corr = stage_migration_analysis(df_all, "corrected")
    mig_corrected["migration_table"].to_csv(
        TBL_DIR / "table2_migration.csv"
    )
    print(f"  mETE T-downstaged: {mig_corrected['n_downstaged_t']}/"
          f"{mig_corrected['n_mete_total']} "
          f"({mig_corrected['pct_downstaged_t']:.1f}%)")
    print(f"  Overall downstaged: {mig_corrected['n_overall_downstaged']}/"
          f"{mig_corrected['n_staged']} "
          f"({mig_corrected['pct_overall_downstaged']:.1f}%)")

    # ── Stage migration (original, for comparison) ──
    mig_original, _ = stage_migration_analysis(df_all, "original")
    print(f"  [cf. original derivation: "
          f"{mig_original['pct_overall_downstaged']:.1f}% downstaged]")

    # ── Ordinal regression (primary complete-case) ──
    print("\n[7/12] Ordinal logistic regression...")
    exog_vars = ["ete_micro", "ete_gross", "age_at_surgery", "female",
                 "largest_tumor_cm", "ln_ratio"]
    exog_vars_base = ["ete_gross", "age_at_surgery", "female",
                      "largest_tumor_cm", "ln_ratio"]

    df_cc = df_all.dropna(
        subset=["risk_ord", "age_at_surgery", "largest_tumor_cm"]
    ).copy()
    df_cc["ln_ratio"] = df_cc["ln_ratio"].fillna(0)
    df_cc = df_cc.dropna(subset=["risk_ord"])
    print(f"  Complete-case N: {len(df_cc)}")

    primary_or, primary_model = run_ordinal_regression(
        df_cc, exog_vars, "Primary (CC, expanded)"
    )
    if primary_or is not None:
        primary_or.to_csv(TBL_DIR / "table3_ordinal_regression.csv", index=False)
        print(f"  Saved audit_tables/table3_ordinal_regression.csv")
        mete_row = primary_or[primary_or["Variable"] == "ete_micro"].iloc[0]
        print(f"  mETE OR = {mete_row['OR']:.2f} "
              f"({mete_row['CI_lo']:.2f}-{mete_row['CI_hi']:.2f}) "
              f"p={pval_str(mete_row['p_value'])}")

    # ── Proportional odds check ──
    print("\n[8/12] Proportional odds diagnostic...")
    prop_odds = check_proportional_odds(df_cc, exog_vars)

    # ── Cross-validated AUC ──
    print("\n[9/12] Cross-validated AUC (5-fold)...")
    cv_auc = cross_validated_auc(df_all, exog_vars_base, exog_vars)
    for k, v in cv_auc.items():
        print(f"  {k}: {v:.4f}")

    # ── Sensitivity analyses ──
    print("\n[10/12] Sensitivity analyses...")
    all_sensitivity = []

    if primary_or is not None:
        primary_or["N"] = len(df_cc)
        all_sensitivity.append(primary_or)

    # MI
    print("  Running MI (m=20)...")
    mi_or = run_mi_pipeline(df_all, exog_vars, m=20)
    if mi_or is not None:
        mi_or["N"] = len(df_all)
        all_sensitivity.append(mi_or)
        mete_mi = mi_or[mi_or["Variable"] == "ete_micro"].iloc[0]
        print(f"  MI mETE OR = {mete_mi['OR']:.2f} "
              f"({mete_mi['CI_lo']:.2f}-{mete_mi['CI_hi']:.2f})")

    # Subgroups
    for label, mask_fn in [
        ("Age >= 55", lambda d: d["age_at_surgery"] >= 55),
        ("Age < 55", lambda d: d["age_at_surgery"] < 55),
        ("Tumor <= 4 cm", lambda d: d["largest_tumor_cm"] <= 4),
        ("Original classic (N=596)", lambda d: d["is_in_original_cohort"]),
    ]:
        sub = df_all[mask_fn(df_all)].copy()
        sub_cc = sub.dropna(
            subset=["risk_ord", "age_at_surgery", "largest_tumor_cm"]
        ).copy()
        sub_cc["ln_ratio"] = sub_cc["ln_ratio"].fillna(0)
        sub_cc = sub_cc.dropna(subset=["risk_ord"])
        if len(sub_cc) >= 30:
            sub_or, _ = run_ordinal_regression(sub_cc, exog_vars, label)
            if sub_or is not None:
                sub_or["N"] = len(sub_cc)
                all_sensitivity.append(sub_or)
                mete_sub = sub_or[sub_or["Variable"] == "ete_micro"]
                if len(mete_sub) > 0:
                    r = mete_sub.iloc[0]
                    print(f"  {label}: mETE OR={r['OR']:.2f} "
                          f"({r['CI_lo']:.2f}-{r['CI_hi']:.2f}) "
                          f"p={pval_str(r['p_value'])} [N={len(sub_cc)}]")

    sensitivity_df = pd.concat(all_sensitivity, ignore_index=True) if all_sensitivity else pd.DataFrame()
    if not sensitivity_df.empty:
        sens_out = sensitivity_df.copy()
        sens_out["OR_fmt"] = sens_out["OR"].apply(lambda x: f"{x:.2f}")
        sens_out["CI_fmt"] = sens_out.apply(
            lambda r: f"({r['CI_lo']:.2f}-{r['CI_hi']:.2f})", axis=1
        )
        sens_out["p_fmt"] = sens_out["p_value"].apply(pval_str)
        sens_out[["Subgroup", "Variable", "OR_fmt", "CI_fmt", "p_fmt", "N"]].rename(
            columns={"OR_fmt": "OR", "CI_fmt": "95% CI", "p_fmt": "p-value"}
        ).to_csv(TBL_DIR / "table4_sensitivity.csv", index=False)
        print(f"  Saved audit_tables/table4_sensitivity.csv")

    # ── Expanded cohort results for fig 8/9 ──
    expanded_results = []
    ptc_orig_ids = set(ptc_orig["research_id"])
    cohort_defs = [
        ("A: All PTC", df_all),
        ("B: Classic/Unspecified",
         df_all[df_all["variant_standardized"].isna()].copy()),
        ("C: Original Classic",
         df_all[df_all["research_id"].isin(ptc_orig_ids)].copy()),
    ]
    for label, sub in cohort_defs:
        sub = classify_ete(sub)
        sub_cc = sub.dropna(
            subset=["risk_ord", "age_at_surgery", "largest_tumor_cm"]
        ).copy()
        sub_cc["ln_ratio"] = sub_cc["ln_ratio"].fillna(0)
        sub_cc = sub_cc.dropna(subset=["risk_ord"])

        mi_sub = run_mi_pipeline(sub, exog_vars, m=20) if len(sub) > 100 else None
        cc_sub, _ = run_ordinal_regression(
            sub_cc, exog_vars, label
        ) if len(sub_cc) >= 30 else (None, None)

        expanded_results.append({
            "label": label, "N": len(sub), "cc_n": len(sub_cc),
            "mi_or": mi_sub, "cc_or": cc_sub,
        })

    cohort_flow = [
        {"label": cr["label"], "N": cr["N"], "cc_n": cr["cc_n"]}
        for cr in expanded_results
    ]

    # ── Figures ──
    print("\n[11/12] Generating figures (deterministic, seed=42)...")
    plot_fig1_ete_distribution(df_all)
    print("  fig1_ete_distribution")
    plot_fig2_stage_migration(df_all)
    print("  fig2_stage_migration")
    plot_fig3_risk_by_ete(df_all)
    print("  fig3_risk_by_ete")
    plot_fig4_roc_curves(df_all)
    print("  fig4_roc_curves")
    plot_fig5_tg_trajectory(df_all)
    print("  fig5_tg_trajectory")
    if not sensitivity_df.empty:
        plot_fig6_forest(sensitivity_df)
        print("  fig6_forest_plot")
    km_done = plot_fig7_kaplan_meier(df_all)
    print(f"  fig7_kaplan_meier {'(done)' if km_done else '(skipped)'}")
    plot_fig8_cohort_flow(cohort_flow)
    print("  fig8_cohort_flow")
    plot_fig9_forest_expanded(expanded_results)
    print("  fig9_forest_expanded")

    # ── Report + metadata ──
    print("\n[12/12] Writing audit report and metadata...")
    write_audit_report(
        df_all, table1, mig_corrected, mig_original, ajcc7_audit,
        primary_or, sensitivity_df, cv_auc, dq, prop_odds,
        cohort_flow, expanded_results,
    )
    print("  Saved audit_report.md")

    meta = write_metadata(
        df_all, mig_corrected, cv_auc, primary_or, sensitivity_df,
        expanded_results, start_time,
    )
    print("  Saved analysis_metadata.yaml")

    # Save analytic cohort
    df_all.to_csv(TBL_DIR / "analytic_cohort_expanded.csv", index=False)
    print("  Saved audit_tables/analytic_cohort_expanded.csv")

    # ── Summary ──
    elapsed = time.time() - start_time
    print(f"\n{'=' * 72}")
    print(f"AUDIT COMPLETE ({elapsed:.1f}s)")
    print(f"{'=' * 72}")
    print(f"\nFindings: {len(audit_findings)} total")
    for sev in ["CRITICAL", "IMPORTANT", "MINOR", "INFO"]:
        n = sum(1 for f in audit_findings if f["severity"] == sev)
        if n > 0:
            print(f"  {sev}: {n}")

    print(f"\nOutputs:")
    print(f"  audit_tables/  — {len(list(TBL_DIR.glob('*.csv')))} CSV files")
    print(f"  audit_figures/ — {len(list(FIG_DIR.glob('*.png')))} PNG + PDF pairs")
    print(f"  audit_report.md")
    print(f"  analysis_metadata.yaml")


if __name__ == "__main__":
    main()
