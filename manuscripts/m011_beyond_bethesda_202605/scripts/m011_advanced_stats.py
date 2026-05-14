#!/usr/bin/env python3
"""
M011 "Beyond Bethesda?" — advanced statistics (Python alternative to the R script).

Computes what BigQuery ML / SQL cannot do natively:
  - bootstrap 95% CI for AUROC and delta-AUC
  - DeLong paired test for nested-model AUC differences
  - logistic calibration slope & intercept
  - likelihood-ratio tests for nested logistic models
  - adjusted odds ratios (Model D) for the forest plot

Inputs: pulls m011_predictions and m011_model_data from BigQuery
        (built by sql/m011_models.sql).
Usage:  python3 m011_advanced_stats.py
Requires: google-cloud-bigquery, pandas, numpy, scipy, statsmodels, scikit-learn
"""
import numpy as np, pandas as pd, os
from google.cloud import bigquery
from sklearn.metrics import roc_auc_score
import statsmodels.api as sm
import statsmodels.formula.api as smf

PROJECT = "thyroid-canonical-pub-2026"
OUT = os.path.join(os.path.dirname(__file__), "..", "tables")
os.makedirs(OUT, exist_ok=True)
bq = bigquery.Client(project=PROJECT)

pred = bq.query("SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_predictions`").to_dataframe()
md   = bq.query("SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data`").to_dataframe()

# ---- 1. Bootstrap AUROC 95% CI -------------------------------------
def boot_auc(d, B=2000, seed=11):
    rng = np.random.default_rng(seed)
    y, p = d["label"].to_numpy(), d["prob"].to_numpy()
    a = roc_auc_score(y, p)
    bs = []
    for _ in range(B):
        i = rng.integers(0, len(y), len(y))
        if len(np.unique(y[i])) < 2:
            continue
        bs.append(roc_auc_score(y[i], p[i]))
    return a, np.percentile(bs, 2.5), np.percentile(bs, 97.5)

rows = []
for (m, c), g in pred.groupby(["model", "cohort"]):
    a, lo, hi = boot_auc(g)
    rows.append(dict(model=m, cohort=c, n=len(g), auc=a, ci_lo=lo, ci_hi=hi))
pd.DataFrame(rows).to_csv(os.path.join(OUT, "m011_auc_bootstrap_ci.csv"), index=False)

# ---- 2. DeLong paired test -----------------------------------------
def delong_var(y, p):
    """Fast DeLong variance/covariance for a single classifier; see Sun & Xu 2014."""
    pos = p[y == 1]; neg = p[y == 0]
    m, n = len(pos), len(neg)
    def midrank(x):
        order = np.argsort(x); ranked = np.empty(len(x)); x = x[order]
        i = 0
        while i < len(x):
            j = i
            while j < len(x) and x[j] == x[i]:
                j += 1
            ranked[order[i:j]] = 0.5 * (i + j - 1) + 1
            i = j
        return ranked
    tx, ty, tz = midrank(pos), midrank(neg), midrank(np.r_[pos, neg])
    auc = (tz[:m].sum() / m - (m + 1) / 2) / n
    v01 = (tz[:m] - tx) / n
    v10 = 1 - (tz[m:] - ty) / m
    s01 = np.var(v01, ddof=1) / m
    s10 = np.var(v10, ddof=1) / n
    return auc, v01, v10, s01 + s10

def delong_test(y, p1, p2):
    """Paired DeLong test (two correlated AUCs, same subjects)."""
    pos = y == 1
    a1, v01a, v10a, _ = delong_var(y, p1)
    a2, v01b, v10b, _ = delong_var(y, p2)
    m = pos.sum(); n = (~pos).sum()
    s = (np.cov(np.c_[v01a, v01b].T) / m + np.cov(np.c_[v10a, v10b].T) / n)
    var = s[0, 0] + s[1, 1] - 2 * s[0, 1]
    from scipy.stats import norm
    z = (a1 - a2) / np.sqrt(var) if var > 0 else 0.0
    return a1, a2, a2 - a1, 2 * (1 - norm.cdf(abs(z)))

pairs = [("A_Bethesda_only", "C_Bethesda_TIRADS"),
         ("A_Bethesda_only", "D_Bethesda_TIRADS_clinical"),
         ("A_Bethesda_only", "E_Bethesda_USfeatures"),
         ("F0_Bethesda_only_molcohort", "F1_Bethesda_TIRADS_molcohort"),
         ("F1_Bethesda_TIRADS_molcohort", "F_Bethesda_TIRADS_molecular"),
         ("F0_Bethesda_only_molcohort", "F_Bethesda_TIRADS_molecular"),
         ("SUB_Bethesda_ref", "SUB_TIRADS_only"),
         ("SUB_Bethesda_ref", "SUB_USfeatures")]
dl = []
for ref, test in pairs:
    a = pred[pred.model == ref][["research_id", "label", "prob"]].rename(columns={"prob": "p_ref"})
    b = pred[pred.model == test][["research_id", "prob"]].rename(columns={"prob": "p_test"})
    d = a.merge(b, on="research_id")
    ar, at, delta, pv = delong_test(d.label.to_numpy(), d.p_ref.to_numpy(), d.p_test.to_numpy())
    dl.append(dict(ref=ref, test=test, auc_ref=ar, auc_test=at, delta=delta, p_value=pv))
pd.DataFrame(dl).to_csv(os.path.join(OUT, "m011_delong_tests.csv"), index=False)

# ---- 3. Calibration slope & intercept ------------------------------
cal = []
for m, g in pred.groupby("model"):
    lp = np.log(np.clip(g.prob, 1e-6, 1 - 1e-6) / (1 - np.clip(g.prob, 1e-6, 1 - 1e-6)))
    slope = sm.GLM(g.label, sm.add_constant(lp), family=sm.families.Binomial()).fit().params[1]
    inter = sm.GLM(g.label, np.ones(len(g)), family=sm.families.Binomial(), offset=lp).fit().params[0]
    cal.append(dict(model=m, calib_slope=slope, calib_intercept=inter))
pd.DataFrame(cal).to_csv(os.path.join(OUT, "m011_calibration_slope_intercept.csv"), index=False)

# ---- 4. Likelihood-ratio tests + adjusted ORs ----------------------
main = md[md.cc_main].copy()
def lrt(small, big):
    return 2 * (big.llf - small.llf), big.df_model - small.df_model
fA = smf.glm("label ~ C(beth_cat)", main, family=sm.families.Binomial()).fit()
fC = smf.glm("label ~ C(beth_cat) + C(acr_cat)", main, family=sm.families.Binomial()).fit()
fD = smf.glm("label ~ C(beth_cat) + C(acr_cat) + age_at_surgery + C(sex) + nodule_size_cm + surgery_year",
             main, family=sm.families.Binomial()).fit()
fE = smf.glm("label ~ C(beth_cat) + f_taller + f_marked_hypo + f_microcalc + f_susp_ln + "
             "f_irreg_margin + f_solid + f_ete + nodule_size_cm", main, family=sm.families.Binomial()).fit()
mol = md[(md.cc_main) & (md.molecular_tested)].copy()
fF0 = smf.glm("label ~ C(beth_cat)", mol, family=sm.families.Binomial()).fit()
fF1 = smf.glm("label ~ C(beth_cat) + C(acr_cat)", mol, family=sm.families.Binomial()).fit()
fF  = smf.glm("label ~ C(beth_cat) + C(acr_cat) + mol_positive", mol, family=sm.families.Binomial()).fit()
from scipy.stats import chi2
lr_rows = []
for name, s, b in [("A->C add TI-RADS", fA, fC), ("A->E add US features", fA, fE),
                   ("C->D add clinical", fC, fD), ("F0->F1 add TI-RADS (mol)", fF0, fF1),
                   ("F1->F add molecular", fF1, fF)]:
    stat, dfree = lrt(s, b)
    lr_rows.append(dict(comparison=name, lr_stat=stat, df=dfree, p_value=1 - chi2.cdf(stat, dfree)))
pd.DataFrame(lr_rows).to_csv(os.path.join(OUT, "m011_likelihood_ratio_tests.csv"), index=False)

orD = pd.DataFrame({"term": fD.params.index, "OR": np.exp(fD.params),
                    "ci_lo": np.exp(fD.conf_int()[0]), "ci_hi": np.exp(fD.conf_int()[1]),
                    "p_value": fD.pvalues})
orD.to_csv(os.path.join(OUT, "m011_adjusted_odds_ratios_modelD.csv"), index=False)
print("M011 advanced stats complete. CSVs written to", os.path.abspath(OUT))
