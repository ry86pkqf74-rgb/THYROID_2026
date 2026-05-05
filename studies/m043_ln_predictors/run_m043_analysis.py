"""
M043: Lymph Node Metastasis Predictors — Multivariate Analysis
Analytic view: manuscript_workspace.m043_ln_predictors_analytic_v1 (N=4,019)
Output: studies/m043_ln_predictors/
"""

import os
import sys
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
import duckdb
import toml
from scipy import stats
from scipy.special import expit
import statsmodels.api as sm
from statsmodels.formula.api import logit
from statsmodels.stats.proportion import proportion_confint
import sklearn.metrics as skmetrics
from sklearn.utils import resample

warnings.filterwarnings("ignore")
np.random.seed(42)

OUT = Path(__file__).parent
OUT.mkdir(parents=True, exist_ok=True)

# ── Connection ────────────────────────────────────────────────────────────────
def get_conn():
    cfg = toml.load(Path(__file__).parent.parent.parent / "motherduck.local.toml")
    token = cfg.get("MD_SA_TOKEN") or cfg.get("MOTHERDUCK_TOKEN") or cfg.get("motherduck_token")
    return duckdb.connect("md:thyroid_canonical_publication_v1_0",
                          config={"motherduck_token": token})

conn = get_conn()
print("Connected to MotherDuck")

# ── Load data ─────────────────────────────────────────────────────────────────
df = conn.execute("SELECT * FROM manuscript_workspace.m043_ln_predictors_analytic_v1").df()
print(f"Loaded {len(df):,} rows × {len(df.columns)} columns")

# Ensure boolean columns are proper Python booleans
bool_cols = ["multifocal_flag_path", "gross_ete_flag", "ln_positive_flag",
             "cnd_performed", "braf_positive_final", "ras_positive_final",
             "mol_has_fusion", "any_recurrence_flag", "any_confirmed_complication_flag",
             "rai_received_reconciled", "ln_rollup_has_per_level_data"]
for c in bool_cols:
    if c in df.columns:
        # Use `is True` pattern to safely handle pd.NA from DuckDB nullable booleans
        df[c] = df[c].apply(lambda x: True if x is True else (False if x is False else None))

# Outcome
df["ln_pos"] = df["ln_positive_flag"].astype(float)

print(f"LN positive: {df['ln_pos'].sum():.0f} ({df['ln_pos'].mean()*100:.1f}%)")
print(f"Recurred: {df['any_recurrence_flag'].sum():.0f}")


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def or_ci_binary(col, df, outcome="ln_pos"):
    """Binary predictor OR via 2×2 table."""
    sub = df[[col, outcome]].dropna()
    sub[col] = sub[col].astype(float)
    if sub[col].nunique() < 2:
        return dict(n=len(sub), or_=np.nan, ci_lo=np.nan, ci_hi=np.nan, pvalue=np.nan)
    ct = pd.crosstab(sub[col], sub[outcome])
    if ct.shape != (2, 2):
        return dict(n=len(sub), or_=np.nan, ci_lo=np.nan, ci_hi=np.nan, pvalue=np.nan)
    a, b, c, d = ct.iloc[1, 1], ct.iloc[1, 0], ct.iloc[0, 1], ct.iloc[0, 0]
    OR = (a * d) / (b * c) if (b * c) > 0 else np.nan
    se = np.sqrt(1/a + 1/b + 1/c + 1/d) if all(x > 0 for x in [a, b, c, d]) else np.nan
    z = stats.norm.ppf(0.975)
    ci_lo = np.exp(np.log(OR) - z * se) if not np.isnan(se) else np.nan
    ci_hi = np.exp(np.log(OR) + z * se) if not np.isnan(se) else np.nan
    chi2, pval, _, _ = stats.chi2_contingency(ct)
    return dict(n=len(sub), or_=OR, ci_lo=ci_lo, ci_hi=ci_hi, pvalue=pval)


def or_ci_continuous(col, df, outcome="ln_pos"):
    """Continuous predictor: per-unit OR from logistic regression."""
    sub = df[[col, outcome]].dropna()
    if len(sub) < 30:
        return dict(n=len(sub), or_=np.nan, ci_lo=np.nan, ci_hi=np.nan, pvalue=np.nan)
    x = sm.add_constant(sub[col].astype(float))
    y = sub[outcome].astype(float)
    try:
        res = sm.Logit(y, x).fit(disp=False)
        coef = res.params[col]
        ci = res.conf_int().loc[col]
        return dict(n=len(sub), or_=np.exp(coef), ci_lo=np.exp(ci[0]),
                    ci_hi=np.exp(ci[1]), pvalue=res.pvalues[col])
    except Exception:
        return dict(n=len(sub), or_=np.nan, ci_lo=np.nan, ci_hi=np.nan, pvalue=np.nan)


def mean_diff_test(col, df, outcome="ln_pos"):
    """Mean comparison for continuous variable by LN status."""
    sub = df[[col, outcome]].dropna()
    g0 = sub.loc[sub[outcome] == 0, col].astype(float)
    g1 = sub.loc[sub[outcome] == 1, col].astype(float)
    stat, pval = stats.mannwhitneyu(g0, g1, alternative="two-sided")
    return dict(n=len(sub), mean_neg=g0.mean(), mean_pos=g1.mean(), pvalue=pval)


def fmt_or(r):
    if pd.isna(r.get("or_")):
        return "—"
    return f"{r['or_']:.2f} ({r['ci_lo']:.2f}–{r['ci_hi']:.2f})"


def fmt_p(p):
    if pd.isna(p):
        return "—"
    if p < 0.001:
        return "<0.001"
    return f"{p:.3f}"


def bootstrap_auc(y_true, y_prob, n_boot=1000, seed=42):
    rng = np.random.RandomState(seed)
    aucs = []
    for _ in range(n_boot):
        idx = rng.choice(len(y_true), len(y_true), replace=True)
        if len(np.unique(y_true[idx])) < 2:
            continue
        aucs.append(skmetrics.roc_auc_score(y_true[idx], y_prob[idx]))
    return np.percentile(aucs, [2.5, 97.5])


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1: UNIVARIATE PREDICTORS
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== SECTION 1: UNIVARIATE PREDICTORS ===")

rows = []

# Age continuous
r = or_ci_continuous("age_at_surgery", df)
rows.append({"predictor": "Age (continuous, per year)", "type": "continuous", **r})

# Age categorical
df["age_cat"] = pd.cut(df["age_at_surgery"], bins=[0, 44, 65, 120],
                       labels=["<45", "45–65", ">65"])
for cat, ref in [("45–65", "<45"), (">65", "<45")]:
    sub = df[df["age_cat"].isin([ref, cat])].copy()
    sub["dummy"] = (sub["age_cat"] == cat).astype(float)
    r = or_ci_binary("dummy", sub)
    rows.append({"predictor": f"Age {cat} vs {ref}", "type": "categorical", **r})

# Sex
df["sex_male"] = (df["sex"] == "male").astype(float)
r = or_ci_binary("sex_male", df)
rows.append({"predictor": "Sex (male vs female)", "type": "binary", **r})

# Tumor size continuous
r = or_ci_continuous("tumor_size_cm_dominant", df)
rows.append({"predictor": "Tumor size (continuous, per cm)", "type": "continuous", **r})

# Tumor size categorical
df["size_cat"] = pd.cut(df["tumor_size_cm_dominant"],
                        bins=[0, 1, 2, 4, 99],
                        labels=["<1 cm", "1–2 cm", "2–4 cm", ">4 cm"])
for cat in ["1–2 cm", "2–4 cm", ">4 cm"]:
    sub = df[df["size_cat"].isin(["<1 cm", cat])].copy()
    sub["dummy"] = (sub["size_cat"] == cat).astype(float)
    r = or_ci_binary("dummy", sub)
    rows.append({"predictor": f"Tumor size {cat} vs <1 cm", "type": "categorical", **r})

# Histology
df["is_ptc"] = (df["histology_pub_category"] == "PTC").astype(float)
r = or_ci_binary("is_ptc", df)
rows.append({"predictor": "PTC vs non-PTC", "type": "binary", **r})

# Multifocal
df["mf"] = df["multifocal_flag_path"].apply(lambda x: 1.0 if x is True else (0.0 if x is False else np.nan))
r = or_ci_binary("mf", df)
rows.append({"predictor": "Multifocal (yes vs no)", "type": "binary", **r})

# ETE grade
for cat in ["microscopic", "gross"]:
    sub = df[df["ete_grade_clean"].isin(["none", cat])].copy()
    sub["dummy"] = (sub["ete_grade_clean"] == cat).astype(float)
    r = or_ci_binary("dummy", sub)
    rows.append({"predictor": f"ETE {cat} vs none", "type": "categorical", **r})

# Vascular invasion
df["vi_any"] = (~df["vascular_invasion_final"].isin(["none", None]) &
               df["vascular_invasion_final"].notna()).astype(float)
r = or_ci_binary("vi_any", df)
rows.append({"predictor": "Vascular invasion (any vs none)", "type": "binary", **r})

# BRAF (among tested)
braf_sub = df[df["braf_positive_final"].notna()].copy()
braf_sub["braf"] = braf_sub["braf_positive_final"].apply(lambda x: 1.0 if x is True else 0.0)
r = or_ci_binary("braf", braf_sub)
r["n_tested"] = len(braf_sub)
rows.append({"predictor": "BRAF+ vs BRAF− (among tested)", "type": "binary", **r})

# RAS
ras_sub = df[df["ras_positive_final"].notna()].copy()
ras_sub["ras"] = ras_sub["ras_positive_final"].apply(lambda x: 1.0 if x is True else 0.0)
r = or_ci_binary("ras", ras_sub)
rows.append({"predictor": "RAS+ vs RAS− (among tested)", "type": "binary", **r})

# Fusion
fus_sub = df[df["mol_has_fusion"].notna()].copy()
fus_sub["fus"] = fus_sub["mol_has_fusion"].apply(lambda x: 1.0 if x is True else 0.0)
r = or_ci_binary("fus", fus_sub)
rows.append({"predictor": "Gene fusion (yes vs no)", "type": "binary", **r})

# Molecular risk tier
mol_sub = df[df["molecular_risk_tier"].notna()].copy()
for cat in ["intermediate", "high"]:
    sub2 = mol_sub[mol_sub["molecular_risk_tier"].isin(["low", "wild_type", cat])].copy()
    sub2["dummy"] = (sub2["molecular_risk_tier"] == cat).astype(float)
    r = or_ci_binary("dummy", sub2)
    rows.append({"predictor": f"Molecular risk {cat} vs low/wild-type", "type": "categorical", **r})

# TIRADS
tir_sub = df[df["tirads_resolved"].notna()].copy()
for cat in ["TR3", "TR4", "TR5"]:
    sub2 = tir_sub[tir_sub["tirads_resolved"].isin(["TR1", "TR2", cat])].copy()
    sub2["dummy"] = (sub2["tirads_resolved"] == cat).astype(float)
    r = or_ci_binary("dummy", sub2)
    rows.append({"predictor": f"TIRADS {cat} vs TR1/TR2", "type": "categorical", **r})

# Bethesda
beth_sub = df[df["bethesda_final"].notna()].copy()
r = or_ci_continuous("bethesda_final", beth_sub)
rows.append({"predictor": "Bethesda category (per unit)", "type": "continuous", **r})

# Imaging nodule size
r = or_ci_continuous("imaging_nodule_size_cm", df)
rows.append({"predictor": "Imaging nodule size (per cm)", "type": "continuous", **r})

uni_df = pd.DataFrame(rows)
uni_df["OR_fmt"] = uni_df.apply(lambda r: fmt_or(r), axis=1)
uni_df["p_fmt"] = uni_df["pvalue"].apply(fmt_p)
uni_df.to_csv(OUT / "univariate_predictors.csv", index=False)
print(f"Univariate: {len(uni_df)} predictors tested")
print(uni_df[["predictor", "n", "OR_fmt", "p_fmt"]].to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2: MULTIVARIATE LOGISTIC REGRESSION
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== SECTION 2: MULTIVARIATE LOGISTIC REGRESSION ===")

# Prepare features
df["ete_micro"] = (df["ete_grade_clean"] == "microscopic").astype(float)
df["ete_gross"] = (df["ete_grade_clean"] == "gross").astype(float)
df["vi_binary"] = df["vi_any"].copy()
df["ptc_flag"] = df["is_ptc"].copy()

primary_vars = ["age_at_surgery", "sex_male", "tumor_size_cm_dominant",
                "ptc_flag", "mf", "ete_micro", "ete_gross", "vi_binary"]
primary_labels = {
    "age_at_surgery": "Age (per year)",
    "sex_male": "Male sex",
    "tumor_size_cm_dominant": "Tumor size (per cm)",
    "ptc_flag": "PTC histology",
    "mf": "Multifocal",
    "ete_micro": "ETE microscopic",
    "ete_gross": "ETE gross",
    "vi_binary": "Vascular invasion"
}

# Primary model — complete cases on primary vars
primary_df = df[primary_vars + ["ln_pos"]].dropna()
print(f"\nPrimary model N={len(primary_df):,}")

X_primary = sm.add_constant(primary_df[primary_vars].astype(float))
y_primary = primary_df["ln_pos"].astype(float)

mod_primary = sm.Logit(y_primary, X_primary).fit(disp=False, maxiter=100)
print(mod_primary.summary2())

# AUC
y_pred_primary = mod_primary.predict(X_primary)
auc_primary = skmetrics.roc_auc_score(y_primary, y_pred_primary)
auc_ci = bootstrap_auc(y_primary.values, y_pred_primary.values, n_boot=1000)
print(f"\nPrimary model AUC: {auc_primary:.3f} (95% CI: {auc_ci[0]:.3f}–{auc_ci[1]:.3f})")

# Hosmer-Lemeshow
def hosmer_lemeshow(y_true, y_prob, g=10):
    df_hl = pd.DataFrame({"y": y_true, "p": y_prob})
    df_hl["decile"] = pd.qcut(df_hl["p"], g, labels=False, duplicates="drop")
    obs = df_hl.groupby("decile")["y"].agg(["sum", "count"])
    exp = df_hl.groupby("decile")["p"].sum()
    hl = ((obs["sum"] - exp)**2 / (exp * (1 - exp/obs["count"]))).sum()
    pval = 1 - stats.chi2.cdf(hl, df=g - 2)
    return hl, pval

hl_stat, hl_p = hosmer_lemeshow(y_primary.values, y_pred_primary.values)
print(f"Hosmer-Lemeshow: χ²={hl_stat:.2f}, p={fmt_p(hl_p)}")

# Build primary results table
primary_rows = []
for var in primary_vars:
    coef = mod_primary.params[var]
    ci = mod_primary.conf_int().loc[var]
    pval = mod_primary.pvalues[var]
    primary_rows.append({
        "predictor": primary_labels[var],
        "variable": var,
        "adj_or": np.exp(coef),
        "ci_lo": np.exp(ci[0]),
        "ci_hi": np.exp(ci[1]),
        "pvalue": pval,
        "model": "primary"
    })

# Secondary model — molecular enriched
mol_vars = primary_vars + ["mol_risk_high", "mol_risk_inter", "fus_flag", "ras_flag"]
df["mol_risk_high"] = (df["molecular_risk_tier"] == "high").astype(float)
df["mol_risk_inter"] = (df["molecular_risk_tier"] == "intermediate").astype(float)
df["fus_flag"] = df["fus"].copy() if "fus" in df.columns else np.nan
df["ras_flag"] = df["ras"].copy() if "ras" in df.columns else np.nan

# Merge fus/ras if not already set
fus_sub2 = df[df["mol_has_fusion"].notna()].copy()
fus_sub2["fus_flag"] = fus_sub2["mol_has_fusion"].apply(lambda x: 1.0 if x is True else 0.0)
df.loc[fus_sub2.index, "fus_flag"] = fus_sub2["fus_flag"]

ras_sub2 = df[df["ras_positive_final"].notna()].copy()
ras_sub2["ras_flag"] = ras_sub2["ras_positive_final"].apply(lambda x: 1.0 if x is True else 0.0)
df.loc[ras_sub2.index, "ras_flag"] = ras_sub2["ras_flag"]

secondary_df = df[mol_vars + ["ln_pos"]].dropna()
print(f"\nSecondary (molecular) model N={len(secondary_df):,}")

mol_labels = {
    **primary_labels,
    "mol_risk_high": "Molecular risk: high",
    "mol_risk_inter": "Molecular risk: intermediate",
    "fus_flag": "Gene fusion",
    "ras_flag": "RAS mutation"
}

X_secondary = sm.add_constant(secondary_df[mol_vars].astype(float))
y_secondary = secondary_df["ln_pos"].astype(float)

try:
    mod_secondary = sm.Logit(y_secondary, X_secondary).fit(disp=False, maxiter=100)
    y_pred_secondary = mod_secondary.predict(X_secondary)
    auc_secondary = skmetrics.roc_auc_score(y_secondary, y_pred_secondary)
    auc_ci_sec = bootstrap_auc(y_secondary.values, y_pred_secondary.values, n_boot=1000)
    print(f"Secondary model AUC: {auc_secondary:.3f} (95% CI: {auc_ci_sec[0]:.3f}–{auc_ci_sec[1]:.3f})")
    hl_s, hl_ps = hosmer_lemeshow(y_secondary.values, y_pred_secondary.values)
    print(f"Secondary H-L: χ²={hl_s:.2f}, p={fmt_p(hl_ps)}")

    # LR test: primary (on same subset) vs secondary
    prim_sub_df = secondary_df[primary_vars + ["ln_pos"]].dropna()
    X_prim_sub = sm.add_constant(prim_sub_df[primary_vars].astype(float))
    mod_prim_sub = sm.Logit(prim_sub_df["ln_pos"].astype(float), X_prim_sub).fit(disp=False, maxiter=100)
    lr_stat = 2 * (mod_secondary.llf - mod_prim_sub.llf)
    lr_df = len(mol_vars) - len(primary_vars)
    lr_p = 1 - stats.chi2.cdf(lr_stat, df=lr_df)
    print(f"LR test (molecular enrichment): χ²={lr_stat:.2f}, df={lr_df}, p={fmt_p(lr_p)}")

    for var in mol_vars:
        coef = mod_secondary.params[var]
        ci = mod_secondary.conf_int().loc[var]
        pval = mod_secondary.pvalues[var]
        primary_rows.append({
            "predictor": mol_labels.get(var, var),
            "variable": var,
            "adj_or": np.exp(coef),
            "ci_lo": np.exp(ci[0]),
            "ci_hi": np.exp(ci[1]),
            "pvalue": pval,
            "model": "secondary_molecular"
        })
    sec_auc_str = f"{auc_secondary:.3f} ({auc_ci_sec[0]:.3f}–{auc_ci_sec[1]:.3f})"
    sec_hl_str = f"χ²={hl_s:.2f}, p={fmt_p(hl_ps)}"
    sec_lr_str = f"χ²={lr_stat:.2f}, df={lr_df}, p={fmt_p(lr_p)}"
except Exception as e:
    print(f"Secondary model error: {e}")
    sec_auc_str = "—"
    sec_hl_str = "—"
    sec_lr_str = "—"
    y_pred_secondary = None

mv_df = pd.DataFrame(primary_rows)
mv_df["OR_fmt"] = mv_df.apply(lambda r: f"{r['adj_or']:.2f} ({r['ci_lo']:.2f}–{r['ci_hi']:.2f})", axis=1)
mv_df["p_fmt"] = mv_df["pvalue"].apply(fmt_p)
mv_df.to_csv(OUT / "multivariate_model.csv", index=False)

# Summary stats
summary_stats = {
    "primary_n": len(primary_df),
    "primary_auc": f"{auc_primary:.3f} ({auc_ci[0]:.3f}–{auc_ci[1]:.3f})",
    "primary_hl": f"χ²={hl_stat:.2f}, p={fmt_p(hl_p)}",
    "secondary_n": len(secondary_df),
    "secondary_auc": sec_auc_str,
    "secondary_hl": sec_hl_str,
    "lr_test": sec_lr_str
}
print("\nMultivariate results saved")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3: LN BURDEN ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== SECTION 3: LN BURDEN ANALYSIS ===")

ln_pos_df = df[df["ln_pos"] == 1].copy()
print(f"LN positive patients: {len(ln_pos_df):,}")

# Distribution of positive LN count
ln_pos_df["ln_count_cat"] = pd.cut(
    ln_pos_df["ln_rollup_total_positive"],
    bins=[-1, 1, 4, 9, 9999],
    labels=["1", "2–4", "5–9", "≥10"]
)
burden_dist = ln_pos_df["ln_count_cat"].value_counts().sort_index()
print("LN burden distribution:")
print(burden_dist)

# LN ratio
examined = pd.to_numeric(ln_pos_df["ln_rollup_total_examined"], errors="coerce")
positive = pd.to_numeric(ln_pos_df["ln_rollup_total_positive"], errors="coerce")
ln_pos_df["ln_ratio"] = np.where(
    (examined > 0) & examined.notna(),
    positive / examined,
    np.nan
).astype(float)

ratio_stats = ln_pos_df["ln_ratio"].describe(percentiles=[.25, .5, .75])
print(f"\nLN ratio stats:\n{ratio_stats}")

# High burden predictors (≥5 positive)
ln_pos_df["high_burden"] = (ln_pos_df["ln_rollup_total_positive"] >= 5).astype(float)
print(f"High burden (≥5 LN+): {ln_pos_df['high_burden'].sum():.0f} ({ln_pos_df['high_burden'].mean()*100:.1f}%)")

hb_vars = ["age_at_surgery", "sex_male", "tumor_size_cm_dominant",
           "ptc_flag", "ete_gross", "vi_binary"]
hb_df = ln_pos_df[hb_vars + ["high_burden"]].dropna()
print(f"High burden model N={len(hb_df):,}")

hb_rows = []
if len(hb_df) >= 30 and hb_df["high_burden"].sum() >= 10:
    X_hb = sm.add_constant(hb_df[hb_vars].astype(float))
    y_hb = hb_df["high_burden"].astype(float)
    try:
        mod_hb = sm.Logit(y_hb, X_hb).fit(disp=False, maxiter=100)
        for var in hb_vars:
            coef = mod_hb.params[var]
            ci = mod_hb.conf_int().loc[var]
            pval = mod_hb.pvalues[var]
            hb_rows.append({
                "predictor": primary_labels.get(var, var),
                "variable": var,
                "adj_or": np.exp(coef),
                "ci_lo": np.exp(ci[0]),
                "ci_hi": np.exp(ci[1]),
                "pvalue": pval
            })
        hb_auc = skmetrics.roc_auc_score(y_hb, mod_hb.predict(X_hb))
        print(f"High burden model AUC: {hb_auc:.3f}")
    except Exception as e:
        print(f"High burden model error: {e}")

# LN ratio vs recurrence in LN+ patients
ratio_rec_sub = ln_pos_df[["ln_ratio", "any_recurrence_flag"]].dropna()
ratio_rec_sub["recurred"] = ratio_rec_sub["any_recurrence_flag"].apply(
    lambda x: 1.0 if x is True else 0.0)
ratio_rec_r = or_ci_continuous("ln_ratio", ratio_rec_sub, outcome="recurred")
print(f"\nLN ratio vs recurrence (among LN+ pts): OR={ratio_rec_r['or_']:.2f}, p={fmt_p(ratio_rec_r['pvalue'])}")

burden_rows = []
for cat, grp in ln_pos_df.groupby("ln_count_cat", observed=True):
    rec_rate = grp["any_recurrence_flag"].apply(lambda x: x is True).mean()
    burden_rows.append({
        "ln_burden_cat": str(cat),
        "n": len(grp),
        "pct_of_ln_pos": len(grp) / len(ln_pos_df) * 100,
        "recurrence_rate": rec_rate * 100,
        "mean_ln_positive": grp["ln_rollup_total_positive"].mean(),
        "mean_ln_examined": grp["ln_rollup_total_examined"].mean(),
        "mean_ln_ratio": grp["ln_ratio"].mean()
    })

burden_df = pd.DataFrame(burden_rows)
# Add high burden predictors
if hb_rows:
    hb_out = pd.DataFrame(hb_rows)
    hb_out["section"] = "high_burden_predictors"
    hb_out["OR_fmt"] = hb_out.apply(lambda r: f"{r['adj_or']:.2f} ({r['ci_lo']:.2f}–{r['ci_hi']:.2f})", axis=1)
    hb_out["p_fmt"] = hb_out["pvalue"].apply(fmt_p)
else:
    hb_out = pd.DataFrame()

# Ratio quartile analysis
ratio_q = pd.qcut(ln_pos_df["ln_ratio"].dropna(), 4, labels=["Q1", "Q2", "Q3", "Q4"])
ratio_rec_q = pd.DataFrame({
    "quartile": ratio_q,
    "recurred": ln_pos_df.loc[ratio_q.index, "any_recurrence_flag"].apply(
        lambda x: x is True)
}).groupby("quartile", observed=True).agg(
    n=("recurred", "count"),
    n_recurred=("recurred", "sum"),
    recurrence_rate=("recurred", "mean")
).reset_index()
ratio_rec_q["recurrence_rate"] *= 100

ln_burden_full = pd.concat([
    burden_df,
    ratio_rec_q.rename(columns={"quartile": "ln_burden_cat"}).assign(section="ln_ratio_quartile"),
    hb_out
], ignore_index=True)
ln_burden_full.to_csv(OUT / "ln_burden_analysis.csv", index=False)
print("LN burden analysis saved")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4: CND IMPACT
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== SECTION 4: CND IMPACT ===")

# NULL cnd_performed = no formal CND documented (same as False); treat both as "no CND"
cnd_yes = df[df["cnd_performed"] == True].copy()
cnd_no = df[df["cnd_performed"] != True].copy()  # includes False and NULL
print(f"CND performed: {len(cnd_yes):,} | No formal CND: {len(cnd_no):,}")

cnd_rows = []
for grp_name, grp in [("CND", cnd_yes), ("No CND", cnd_no)]:
    ln_pos_rate = grp["ln_pos"].mean()
    ln_pos_ci = proportion_confint(int(grp["ln_pos"].sum()), len(grp), method="wilson")
    rec_rate = grp["any_recurrence_flag"].apply(lambda x: x is True).mean()
    cnd_rows.append({
        "group": grp_name,
        "n": len(grp),
        "ln_positive_n": int(grp["ln_pos"].sum()),
        "ln_positive_pct": ln_pos_rate * 100,
        "ln_pos_ci_lo": ln_pos_ci[0] * 100,
        "ln_pos_ci_hi": ln_pos_ci[1] * 100,
        "mean_ln_examined": grp["ln_rollup_total_examined"].mean(),
        "mean_ln_positive": grp["ln_rollup_total_positive"].mean(),
        "recurrence_pct": rec_rate * 100,
        "n_recurred": grp["any_recurrence_flag"].apply(lambda x: x is True).sum()
    })

# Chi-square for LN+ rate — use binary group label so NULL patients are "No CND"
df["cnd_group"] = df["cnd_performed"].apply(lambda x: "CND" if x is True else "No CND")
chi2_cnd, p_cnd, _, _ = stats.chi2_contingency(pd.crosstab(df["cnd_group"], df["ln_pos"]))
print(f"CND vs LN+ rate: p={fmt_p(p_cnd)}")

# Stage migration: N-stage by CND
stage_by_cnd = pd.crosstab(df["cnd_performed"], df["ajcc8_n_stage"])
print("N-stage by CND:")
print(stage_by_cnd)

# Adjusted recurrence comparison
rec_cnd_df = df[["cnd_performed", "any_recurrence_flag", "age_at_surgery",
                  "tumor_size_cm_dominant", "ete_micro", "ete_gross", "vi_binary", "ln_pos"]].dropna()
rec_cnd_df["cnd"] = rec_cnd_df["cnd_performed"].apply(lambda x: 1.0 if x is True else 0.0)  # NULL treated as 0
rec_cnd_df["recurred"] = rec_cnd_df["any_recurrence_flag"].apply(lambda x: 1.0 if x is True else 0.0)
cnd_rec_vars = ["cnd", "age_at_surgery", "tumor_size_cm_dominant",
                "ete_micro", "ete_gross", "vi_binary", "ln_pos"]
X_cnd = sm.add_constant(rec_cnd_df[cnd_rec_vars].astype(float))
y_cnd = rec_cnd_df["recurred"].astype(float)
try:
    mod_cnd_rec = sm.Logit(y_cnd, X_cnd).fit(disp=False, maxiter=100)
    cnd_or = np.exp(mod_cnd_rec.params["cnd"])
    cnd_ci = np.exp(mod_cnd_rec.conf_int().loc["cnd"])
    cnd_p = mod_cnd_rec.pvalues["cnd"]
    print(f"CND adjusted OR for recurrence: {cnd_or:.2f} ({cnd_ci[0]:.2f}–{cnd_ci[1]:.2f}), p={fmt_p(cnd_p)}")
    cnd_adj_str = f"{cnd_or:.2f} ({cnd_ci[0]:.2f}–{cnd_ci[1]:.2f}), p={fmt_p(cnd_p)}"
except Exception as e:
    print(f"CND recurrence model error: {e}")
    cnd_adj_str = "—"

cnd_df_out = pd.DataFrame(cnd_rows)
cnd_df_out["chi2_p_ln_positive"] = fmt_p(p_cnd)
cnd_df_out["adjusted_or_recurrence"] = cnd_adj_str
cnd_df_out.to_csv(OUT / "cnd_impact.csv", index=False)
print("CND impact saved")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5: LND TYPE AND OUTCOMES
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== SECTION 5: LND OUTCOMES ===")

lnd_df = df[df["lnd_type"].notna()].copy()
print(f"Formal LND patients: {len(lnd_df):,}")
print(lnd_df["lnd_type"].value_counts())

lnd_rows = []
for ltype in lnd_df["lnd_type"].unique():
    grp = lnd_df[lnd_df["lnd_type"] == ltype]
    ln_pos_rate = grp["ln_pos"].mean()
    rec_rate = grp["any_recurrence_flag"].apply(lambda x: x is True).mean()
    comp_rate = grp["any_confirmed_complication_flag"].apply(lambda x: x is True).mean()
    lnd_rows.append({
        "lnd_type": ltype,
        "n": len(grp),
        "ln_positive_n": int(grp["ln_pos"].sum()),
        "ln_positive_pct": ln_pos_rate * 100,
        "mean_ln_examined": grp["ln_rollup_total_examined"].mean(),
        "mean_ln_positive": grp["ln_rollup_total_positive"].mean(),
        "recurrence_n": int(grp["any_recurrence_flag"].apply(lambda x: x is True).sum()),
        "recurrence_pct": rec_rate * 100,
        "complication_n": int(grp["any_confirmed_complication_flag"].apply(lambda x: x is True).sum()),
        "complication_pct": comp_rate * 100
    })

lnd_out = pd.DataFrame(lnd_rows).sort_values("n", ascending=False)

# Lateral neck dissection vs MRND comparison
lat = lnd_df[lnd_df["lnd_type"] == "lateral_neck_dissection"]
mrnd = lnd_df[lnd_df["lnd_type"] == "mrnd"]
if len(lat) > 0 and len(mrnd) > 0:
    # LN+ rate comparison
    lat_mrnd_df = lnd_df[lnd_df["lnd_type"].isin(["lateral_neck_dissection", "mrnd"])].copy()
    lat_mrnd_df["is_mrnd"] = (lat_mrnd_df["lnd_type"] == "mrnd").astype(float)
    r_lm = or_ci_binary("is_mrnd", lat_mrnd_df)
    print(f"MRND vs lateral LN+ OR: {r_lm['or_']:.2f}, p={fmt_p(r_lm['pvalue'])}")
    lnd_out.loc[len(lnd_out)] = {
        "lnd_type": "MRND_vs_lateral_OR",
        "n": r_lm["n"],
        "ln_positive_pct": r_lm["or_"],
        "recurrence_pct": r_lm["pvalue"],
        **{k: np.nan for k in lnd_out.columns if k not in ["lnd_type", "n", "ln_positive_pct", "recurrence_pct"]}
    }

lnd_out.to_csv(OUT / "lnd_outcomes.csv", index=False)
print("LND outcomes saved")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6: PREDICTIVE NOMOGRAM
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== SECTION 6: NOMOGRAM AND CALIBRATION ===")

# Use primary model predictions
primary_df_full = df[primary_vars + ["ln_pos", "research_id"]].dropna().copy()
X_nom = sm.add_constant(primary_df_full[primary_vars].astype(float))
y_nom = primary_df_full["ln_pos"].astype(float)

# Refit on full primary complete-case population
mod_nom = sm.Logit(y_nom, X_nom).fit(disp=False, maxiter=100)
predicted_probs = mod_nom.predict(X_nom)

primary_df_full["predicted_prob"] = predicted_probs
primary_df_full["risk_group"] = pd.cut(
    predicted_probs,
    bins=[0, 0.15, 0.40, 1.0],
    labels=["Low (<15%)", "Moderate (15–40%)", "High (>40%)"]
)

# Calibration by decile
primary_df_full["decile"] = pd.qcut(predicted_probs, 10, labels=False, duplicates="drop")
calib = primary_df_full.groupby("decile").agg(
    n=("ln_pos", "count"),
    mean_predicted=("predicted_prob", "mean"),
    observed_rate=("ln_pos", "mean")
).reset_index()
calib["observed_pct"] = calib["observed_rate"] * 100
calib["predicted_pct"] = calib["mean_predicted"] * 100

print("Calibration by decile:")
print(calib[["decile", "n", "predicted_pct", "observed_pct"]].round(1).to_string(index=False))

# Risk group distribution
rg_summary = primary_df_full.groupby("risk_group", observed=True).agg(
    n=("ln_pos", "count"),
    ln_pos_n=("ln_pos", "sum"),
    ln_pos_rate=("ln_pos", "mean")
).reset_index()
rg_summary["ln_pos_pct"] = rg_summary["ln_pos_rate"] * 100
print("\nRisk group summary:")
print(rg_summary.to_string(index=False))

# Bootstrap optimism-corrected C-statistic
print("\nBootstrap validation (1000 replicates)...")
apparent_auc = skmetrics.roc_auc_score(y_nom, predicted_probs)
optimism_list = []
rng = np.random.RandomState(42)
for i in range(1000):
    idx = rng.choice(len(primary_df_full), len(primary_df_full), replace=True)
    bt_X = X_nom.iloc[idx]
    bt_y = y_nom.iloc[idx]
    if bt_y.nunique() < 2:
        continue
    try:
        bt_mod = sm.Logit(bt_y, bt_X).fit(disp=False, maxiter=50)
        bt_auc = skmetrics.roc_auc_score(bt_y, bt_mod.predict(bt_X))
        orig_auc = skmetrics.roc_auc_score(y_nom, bt_mod.predict(X_nom))
        optimism_list.append(bt_auc - orig_auc)
    except Exception:
        continue

mean_optimism = np.mean(optimism_list)
corrected_auc = apparent_auc - mean_optimism
print(f"Apparent C-statistic: {apparent_auc:.3f}")
print(f"Mean optimism: {mean_optimism:.4f}")
print(f"Optimism-corrected C-statistic: {corrected_auc:.3f}")

# Save nomogram predictions
nom_out = primary_df_full[["research_id", "predicted_prob", "risk_group", "ln_pos"] + primary_vars].copy()
nom_out["optimism_corrected_auc"] = corrected_auc
nom_out["calibration_calib_decile"] = primary_df_full["decile"]
nom_out.to_csv(OUT / "nomogram_predictions.csv", index=False)
print("Nomogram predictions saved")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7: RECURRENCE ANALYSIS BY LN STATUS
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== SECTION 7: RECURRENCE BY LN STATUS ===")

# LN+ vs LN- recurrence
rec_rows = []
for group_label, mask in [("LN negative", df["ln_pos"] == 0),
                           ("LN positive", df["ln_pos"] == 1)]:
    grp = df[mask]
    n = len(grp)
    n_rec = grp["any_recurrence_flag"].apply(lambda x: x is True).sum()
    rate = n_rec / n
    ci = proportion_confint(n_rec, n, method="wilson")
    rec_rows.append({
        "group": group_label,
        "n": n,
        "n_recurred": n_rec,
        "recurrence_rate": rate * 100,
        "ci_lo": ci[0] * 100,
        "ci_hi": ci[1] * 100
    })

# Chi-square
ct_rec = pd.crosstab(df["ln_pos"], df["any_recurrence_flag"].apply(lambda x: x is True))
chi2_rec, p_rec, _, _ = stats.chi2_contingency(ct_rec)
print(f"LN+ vs LN- recurrence: p={fmt_p(p_rec)}")

# Recurrence by LN burden
df["ln_burden_cat"] = "0 (LN negative)"
df.loc[df["ln_rollup_total_positive"].between(1, 4), "ln_burden_cat"] = "1–4"
df.loc[df["ln_rollup_total_positive"] >= 5, "ln_burden_cat"] = "≥5"
df.loc[df["ln_rollup_total_positive"] == 0, "ln_burden_cat"] = "0 (LN negative)"

for cat in ["0 (LN negative)", "1–4", "≥5"]:
    grp = df[df["ln_burden_cat"] == cat]
    n = len(grp)
    n_rec = grp["any_recurrence_flag"].apply(lambda x: x is True).sum()
    rate = n_rec / n if n > 0 else 0
    ci = proportion_confint(n_rec, n, method="wilson") if n > 0 else (0, 0)
    rec_rows.append({
        "group": f"LN burden: {cat}",
        "n": n,
        "n_recurred": n_rec,
        "recurrence_rate": rate * 100,
        "ci_lo": ci[0] * 100,
        "ci_hi": ci[1] * 100
    })

# Multivariate recurrence model
rec_mv_vars = ["ln_pos", "age_at_surgery", "sex_male", "tumor_size_cm_dominant",
               "ete_micro", "ete_gross", "vi_binary", "rai_flag"]
df["rai_flag"] = df["rai_received_reconciled"].apply(lambda x: 1.0 if x is True else (0.0 if x is False else np.nan))
rec_mv_df = df[rec_mv_vars + ["any_recurrence_flag"]].dropna().copy()
rec_mv_df["recurred"] = rec_mv_df["any_recurrence_flag"].apply(lambda x: 1.0 if x is True else 0.0)

rec_mv_labels = {
    "ln_pos": "LN positive",
    "age_at_surgery": "Age (per year)",
    "sex_male": "Male sex",
    "tumor_size_cm_dominant": "Tumor size (per cm)",
    "ete_micro": "ETE microscopic",
    "ete_gross": "ETE gross",
    "vi_binary": "Vascular invasion",
    "rai_flag": "RAI received"
}

print(f"\nRecurrence multivariate model N={len(rec_mv_df):,}")
X_rec = sm.add_constant(rec_mv_df[rec_mv_vars].astype(float))
y_rec = rec_mv_df["recurred"].astype(float)
try:
    mod_rec = sm.Logit(y_rec, X_rec).fit(disp=False, maxiter=100)
    print(mod_rec.summary2())
    for var in rec_mv_vars:
        coef = mod_rec.params[var]
        ci = mod_rec.conf_int().loc[var]
        pval = mod_rec.pvalues[var]
        rec_rows.append({
            "group": f"MV_{rec_mv_labels.get(var, var)}",
            "n": len(rec_mv_df),
            "adj_or": np.exp(coef),
            "ci_lo": np.exp(ci[0]),
            "ci_hi": np.exp(ci[1]),
            "pvalue": pval,
            "OR_fmt": f"{np.exp(coef):.2f} ({np.exp(ci[0]):.2f}–{np.exp(ci[1]):.2f})",
            "p_fmt": fmt_p(pval)
        })
    rec_auc = skmetrics.roc_auc_score(y_rec, mod_rec.predict(X_rec))
    print(f"Recurrence model AUC: {rec_auc:.3f}")
except Exception as e:
    print(f"Recurrence model error: {e}")

# LN ratio vs recurrence
ex_all = pd.to_numeric(df["ln_rollup_total_examined"], errors="coerce")
pos_all = pd.to_numeric(df["ln_rollup_total_positive"], errors="coerce")
df["ln_ratio_all"] = np.where(
    (ex_all > 0) & ex_all.notna(),
    pos_all / ex_all,
    np.nan
).astype(float)
ratio_rec_all = df[["ln_ratio_all", "any_recurrence_flag"]].dropna().copy()
ratio_rec_all["recurred"] = ratio_rec_all["any_recurrence_flag"].apply(lambda x: 1.0 if x is True else 0.0)
ratio_or = or_ci_continuous("ln_ratio_all", ratio_rec_all, outcome="recurred")
count_or = or_ci_continuous("ln_rollup_total_positive", df[["ln_rollup_total_positive", "any_recurrence_flag"]].assign(
    recurred=lambda x: x["any_recurrence_flag"].apply(lambda v: 1.0 if v is True else 0.0)
).dropna().rename(columns={"recurred": "recurred_col"}).copy(), outcome="recurred_col")
# Handle column name
tmp_df = df[["ln_rollup_total_positive", "any_recurrence_flag"]].dropna().copy()
tmp_df["recurred"] = tmp_df["any_recurrence_flag"].apply(lambda x: 1.0 if x is True else 0.0)
count_or = or_ci_continuous("ln_rollup_total_positive", tmp_df, outcome="recurred")

print(f"\nLN ratio OR for recurrence: {ratio_or['or_']:.3f}, p={fmt_p(ratio_or['pvalue'])}")
print(f"LN count OR for recurrence: {count_or['or_']:.3f}, p={fmt_p(count_or['pvalue'])}")

rec_rows.append({
    "group": "LN_ratio_vs_recurrence (per unit ratio)",
    "n": ratio_or["n"],
    "adj_or": ratio_or["or_"],
    "ci_lo": ratio_or["ci_lo"],
    "ci_hi": ratio_or["ci_hi"],
    "pvalue": ratio_or["pvalue"],
    "OR_fmt": fmt_or(ratio_or),
    "p_fmt": fmt_p(ratio_or["pvalue"])
})
rec_rows.append({
    "group": "LN_count_vs_recurrence (per additional LN+)",
    "n": count_or["n"],
    "adj_or": count_or["or_"],
    "ci_lo": count_or["ci_lo"],
    "ci_hi": count_or["ci_hi"],
    "pvalue": count_or["pvalue"],
    "OR_fmt": fmt_or(count_or),
    "p_fmt": fmt_p(count_or["pvalue"])
})

rec_out = pd.DataFrame(rec_rows)
rec_out.to_csv(OUT / "recurrence_by_ln.csv", index=False)
print("Recurrence analysis saved")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 8: LaTeX TABLES
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== SECTION 8: LaTeX TABLES ===")

latex_lines = []
latex_lines.append(r"\documentclass{article}")
latex_lines.append(r"\usepackage{booktabs,longtable,array,geometry,caption}")
latex_lines.append(r"\geometry{margin=1in}")
latex_lines.append(r"\begin{document}")

# Table 1: Selected univariate results
latex_lines.append(r"\begin{table}[h]")
latex_lines.append(r"\caption{Univariate Predictors of Lymph Node Metastasis (M043, N=4,019)}")
latex_lines.append(r"\centering")
latex_lines.append(r"\begin{tabular}{llrrr}")
latex_lines.append(r"\toprule")
latex_lines.append(r"Predictor & N & OR & 95\% CI & p-value \\")
latex_lines.append(r"\midrule")
key_preds = [
    "Age (continuous, per year)",
    "Sex (male vs female)",
    "Tumor size (continuous, per cm)",
    "PTC vs non-PTC",
    "Multifocal (yes vs no)",
    "ETE microscopic vs none",
    "ETE gross vs none",
    "Vascular invasion (any vs none)",
    "BRAF+ vs BRAF− (among tested)",
    "RAS+ vs RAS− (among tested)",
]
for _, row in uni_df[uni_df["predictor"].isin(key_preds)].iterrows():
    p_str = row["p_fmt"].replace("<", r"$<$")
    latex_lines.append(
        f"{row['predictor']} & {int(row['n']) if not pd.isna(row['n']) else '—'} "
        f"& {row['OR_fmt'].split(' ')[0] if row['OR_fmt'] != '—' else '—'} "
        f"& {' '.join(row['OR_fmt'].split(' ')[1:]) if row['OR_fmt'] != '—' else '—'} "
        f"& {p_str} \\\\"
    )
latex_lines.append(r"\bottomrule")
latex_lines.append(r"\end{tabular}")
latex_lines.append(r"\end{table}")
latex_lines.append(r"\vspace{1em}")

# Table 2: Multivariate primary model
latex_lines.append(r"\begin{table}[h]")
latex_lines.append(r"\caption{Multivariate Logistic Regression: Predictors of LN Metastasis}")
latex_lines.append(r"\centering")
latex_lines.append(r"\begin{tabular}{lrrrr}")
latex_lines.append(r"\toprule")
latex_lines.append(r"Predictor & N & Adj.\ OR & 95\% CI & p-value \\")
latex_lines.append(r"\midrule")
prim_mv = mv_df[mv_df["model"] == "primary"]
for _, row in prim_mv.iterrows():
    p_str = row["p_fmt"].replace("<", r"$<$")
    latex_lines.append(
        f"{row['predictor']} & {len(primary_df)} & "
        f"{row['adj_or']:.2f} & ({row['ci_lo']:.2f}--{row['ci_hi']:.2f}) & {p_str} \\\\"
    )
latex_lines.append(r"\midrule")
latex_lines.append(
    f"\\multicolumn{{5}}{{l}}{{C-statistic: {auc_primary:.3f} (95\\% CI: {auc_ci[0]:.3f}--{auc_ci[1]:.3f}); "
    f"Optimism-corrected: {corrected_auc:.3f}}} \\\\"
)
latex_lines.append(
    f"\\multicolumn{{5}}{{l}}{{Hosmer--Lemeshow: $\\chi^2$={hl_stat:.2f}, p={fmt_p(hl_p)}}} \\\\"
)
latex_lines.append(r"\bottomrule")
latex_lines.append(r"\end{tabular}")
latex_lines.append(r"\end{table}")
latex_lines.append(r"\vspace{1em}")

# Table 3: CND Impact
latex_lines.append(r"\begin{table}[h]")
latex_lines.append(r"\caption{Impact of Central Neck Dissection on LN Detection}")
latex_lines.append(r"\centering")
latex_lines.append(r"\begin{tabular}{lrrrr}")
latex_lines.append(r"\toprule")
latex_lines.append(r"Characteristic & CND (n=" + str(len(cnd_yes)) + r") & No CND (n=" + str(len(cnd_no)) + r") & p-value \\")
latex_lines.append(r"\midrule")
for field, label in [
    ("ln_positive_pct", "LN positive, \\%"),
    ("mean_ln_examined", "Mean LN examined"),
    ("mean_ln_positive", "Mean LN positive"),
    ("recurrence_pct", "Recurrence, \\%")
]:
    yes_val = cnd_df_out[cnd_df_out["group"] == "CND"][field].values[0]
    no_val = cnd_df_out[cnd_df_out["group"] == "No CND"][field].values[0]
    latex_lines.append(f"{label} & {yes_val:.1f} & {no_val:.1f} & — \\\\")
latex_lines.append(r"\bottomrule")
latex_lines.append(r"\end{tabular}")
latex_lines.append(r"\end{table}")

latex_lines.append(r"\end{document}")

with open(OUT / "ln_predictors_summary.tex", "w") as f:
    f.write("\n".join(latex_lines))
print("LaTeX tables saved")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 9: UPLOAD TO MOTHERDUCK
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== SECTION 9: UPLOAD TO MOTHERDUCK ===")

# Build the patient-level analysis table
upload_df = df[["research_id", "ln_pos", "any_recurrence_flag",
                "ln_rollup_total_positive", "ln_rollup_total_examined", "ln_ratio_all",
                "ln_burden_cat"]].copy()

# Add predicted probabilities where available
nom_merge = nom_out[["research_id", "predicted_prob", "risk_group"]].copy()
nom_merge["research_id"] = nom_merge["research_id"].astype(str)
upload_df["research_id"] = upload_df["research_id"].astype(str)
upload_df = upload_df.merge(nom_merge, on="research_id", how="left")

upload_df["analysis_version"] = "m043_v1"
upload_df["created_at"] = pd.Timestamp.now()

print(f"Upload table: {len(upload_df):,} rows × {len(upload_df.columns)} columns")

try:
    conn2 = get_conn()
    conn2.execute("DROP TABLE IF EXISTS manuscript_workspace.m043_ln_analysis_v1")
    conn2.execute("""
        CREATE TABLE manuscript_workspace.m043_ln_analysis_v1 AS
        SELECT * FROM upload_df
    """)
    n_check = conn2.execute("SELECT COUNT(*) FROM manuscript_workspace.m043_ln_analysis_v1").fetchone()[0]
    print(f"Uploaded {n_check:,} rows to manuscript_workspace.m043_ln_analysis_v1")
    conn2.close()
except Exception as e:
    print(f"Upload error: {e}")
    # Try alternative approach
    try:
        conn3 = get_conn()
        conn3.register("upload_df", upload_df)
        conn3.execute("DROP TABLE IF EXISTS manuscript_workspace.m043_ln_analysis_v1")
        conn3.execute("CREATE TABLE manuscript_workspace.m043_ln_analysis_v1 AS SELECT * FROM upload_df")
        n_check = conn3.execute("SELECT COUNT(*) FROM manuscript_workspace.m043_ln_analysis_v1").fetchone()[0]
        print(f"Uploaded {n_check:,} rows via register method")
        conn3.close()
    except Exception as e2:
        print(f"Upload error (method 2): {e2}")

conn.close()
print("\n=== M043 ANALYSIS COMPLETE ===")
print(f"Output files in: {OUT}")
for f in sorted(OUT.glob("*")):
    print(f"  {f.name}")
