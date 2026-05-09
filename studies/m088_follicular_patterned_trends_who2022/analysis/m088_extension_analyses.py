#!/usr/bin/env python3
"""
M088 — Extension analyses (post-v0.1):

Reads deliverables/m088_per_research_id.csv and produces extension result CSVs in
analysis/output/extension/ for incorporation into Table 1b, Table 4 expansion,
new Tables S1-S5, and supplemental figures.

Sections:
  A. Family comparison (Oncocytic vs Conventional follicular)
  B. Era-stratified migration + management
  C. Age/sex stratification of management
  D. Tumor size associations
  E. Invasion-field validation
  F. Multivariable logistic regression for definitive total thyroidectomy
  G. Sensitivity analyses (drop 2025; drop multi-primary; drop small-n)
  H. Cochran-Armitage trend test for era × family
"""
from __future__ import annotations
import csv
import math
import json
from pathlib import Path
from collections import defaultdict, Counter

import numpy as np
import pandas as pd
from scipy import stats

ROOT  = Path(__file__).resolve().parent.parent
PER   = ROOT / "deliverables" / "m088_per_research_id.csv"
OUT   = ROOT / "analysis" / "output" / "extension"
OUT.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------------------- #
# Load per-patient data and coerce types

df = pd.read_csv(PER)
for c in ["age_yr", "dom_tumor_size_cm"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
for c in ["h2_fa_strict","h2_fa_broad","h3_strict","h3_moderate","h3_broad",
          "capsular_strict_eq","capsular_broad_eq","vascular_documented",
          "vascular_quantified_ge1","definitive_total","rai_likely_received"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df["surgery_year"] = pd.to_numeric(df["surgery_year"], errors="coerce")
df["age_ge55"] = (df["age_yr"] >= 55).astype("Int64")
df["era_post2017"] = df["era_5yr"].isin(["2015-2019","2020-2025"]).astype(int)

# Identify multi-primary patients (research_id appearing twice)
counts = df["research_id"].value_counts()
multi_pri = set(counts[counts > 1].index)
df["is_multi_primary"] = df["research_id"].isin(multi_pri).astype(int)

print(f"Loaded {len(df)} rows / {df['research_id'].nunique()} distinct research_ids")
print(f"Multi-primary patients: {len(multi_pri)}")

# Patient-level (deduplicated) table — keep first row per research_id for stratified summaries
# (multi-primary handled separately)
df_pt = df.sort_values(["research_id","historical_group"]).drop_duplicates("research_id", keep="first").copy()
print(f"Patient-level rows: {len(df_pt)}")

def wilson(k, n, z=1.96):
    if n == 0: return (np.nan, np.nan, np.nan)
    p = k/n
    denom = 1 + z*z/n
    centre = (p + z*z/(2*n)) / denom
    half = z*math.sqrt(p*(1-p)/n + z*z/(4*n*n)) / denom
    return p, max(0.0, centre-half), min(1.0, centre+half)

def fmt_pct(p, lo, hi):
    if any(np.isnan([p, lo, hi])): return "—"
    return f"{p*100:.1f}% ({lo*100:.1f}–{hi*100:.1f})"

def fdr_bh(pvals):
    p = np.array([np.nan if v is None else v for v in pvals], dtype=float)
    valid = ~np.isnan(p)
    n = valid.sum()
    out = np.full_like(p, np.nan)
    if n == 0: return out.tolist()
    order = np.argsort(p[valid])
    sorted_p = p[valid][order]
    ranks = np.arange(1, n+1)
    qvals = sorted_p * n / ranks
    qvals = np.minimum.accumulate(qvals[::-1])[::-1]
    qvals = np.minimum(qvals, 1.0)
    res_valid = np.empty(n)
    res_valid[order] = qvals
    out[valid] = res_valid
    return out.tolist()

# -------------------------------------------------------------------------- #
# A. Family comparison: Oncocytic vs Conventional follicular

def family_compare():
    f1 = df_pt[df_pt.who2022_family == "Oncocytic"]
    f2 = df_pt[df_pt.who2022_family == "Conventional follicular"]
    rows = []

    # Age
    a1 = f1.age_yr.dropna(); a2 = f2.age_yr.dropna()
    t = stats.ttest_ind(a1, a2, equal_var=False)
    u = stats.mannwhitneyu(a1, a2, alternative="two-sided")
    rows.append({"metric":"Age, mean (SD), years",
                 "oncocytic":f"{a1.mean():.1f} ({a1.std():.1f}) n={len(a1)}",
                 "conventional":f"{a2.mean():.1f} ({a2.std():.1f}) n={len(a2)}",
                 "test":"Welch t-test",
                 "stat":f"t={t.statistic:.2f}",
                 "p":f"{t.pvalue:.2e}"})
    rows.append({"metric":"Age, median (IQR), years",
                 "oncocytic":f"{a1.median():.1f} ({a1.quantile(.25):.1f}-{a1.quantile(.75):.1f})",
                 "conventional":f"{a2.median():.1f} ({a2.quantile(.25):.1f}-{a2.quantile(.75):.1f})",
                 "test":"Mann-Whitney U",
                 "stat":f"U={u.statistic:.0f}",
                 "p":f"{u.pvalue:.2e}"})

    # Sex
    s1 = (f1.sex=="female").sum(); s2 = (f2.sex=="female").sum()
    n1 = (f1.sex.isin(["female","male"])).sum(); n2 = (f2.sex.isin(["female","male"])).sum()
    chi2, p, dof, _ = stats.chi2_contingency([[s1, n1-s1],[s2, n2-s2]])
    rows.append({"metric":"Female, n (%)",
                 "oncocytic":f"{s1}/{n1} ({s1/n1*100:.1f}%)" if n1 else "—",
                 "conventional":f"{s2}/{n2} ({s2/n2*100:.1f}%)" if n2 else "—",
                 "test":"chi-square",
                 "stat":f"χ²={chi2:.2f}",
                 "p":f"{p:.2e}"})

    # Dominant tumor size (where available)
    sz1 = f1.dom_tumor_size_cm.dropna(); sz2 = f2.dom_tumor_size_cm.dropna()
    if len(sz1) >= 5 and len(sz2) >= 5:
        u = stats.mannwhitneyu(sz1, sz2, alternative="two-sided")
        rows.append({"metric":"Dominant tumor size, median (IQR), cm",
                     "oncocytic":f"{sz1.median():.2f} ({sz1.quantile(.25):.2f}-{sz1.quantile(.75):.2f}) n={len(sz1)}",
                     "conventional":f"{sz2.median():.2f} ({sz2.quantile(.25):.2f}-{sz2.quantile(.75):.2f}) n={len(sz2)}",
                     "test":"Mann-Whitney U",
                     "stat":f"U={u.statistic:.0f}",
                     "p":f"{u.pvalue:.2e}"})

    # Definitive total + pure hemi
    for label, k in [("Definitive total thyroidectomy at index", "definitive_total"),
                     ("Pure hemi (initial hemi without completion)", None)]:
        if k:
            d1 = f1[k].sum(); d2 = f2[k].sum()
        else:
            d1 = ((f1.extent_at_index=="hemi") & (f1.definitive_total==0)).sum()
            d2 = ((f2.extent_at_index=="hemi") & (f2.definitive_total==0)).sum()
        n1 = len(f1); n2 = len(f2)
        chi2, p, dof, _ = stats.chi2_contingency([[d1, n1-d1],[d2, n2-d2]])
        p1, lo1, hi1 = wilson(d1, n1); p2, lo2, hi2 = wilson(d2, n2)
        rows.append({"metric": label,
                     "oncocytic": fmt_pct(p1, lo1, hi1) + f" ({d1}/{n1})",
                     "conventional": fmt_pct(p2, lo2, hi2) + f" ({d2}/{n2})",
                     "test":"chi-square",
                     "stat":f"χ²={chi2:.2f}",
                     "p":f"{p:.2e}"})

    pd.DataFrame(rows).to_csv(OUT/"table_1b_family_compare.csv", index=False)
    print("Wrote table_1b_family_compare.csv")
    return rows

family_compare()

# -------------------------------------------------------------------------- #
# B. Era-stratified migration and management

def era_strat():
    eras = ["1990-1994","1995-1999","2000-2004","2005-2009","2010-2014","2015-2019","2020-2025"]
    out_rows = []

    for era in eras:
        sub = df_pt[df_pt.era_5yr==era]
        if len(sub)==0: continue
        # Family migration
        n_total = len(sub)
        n_onco = (sub.who2022_family=="Oncocytic").sum()
        p, lo, hi = wilson(n_onco, n_total)
        # Definitive total
        n_dt = sub.definitive_total.sum()
        pdt, lodt, hidt = wilson(n_dt, n_total)
        # H2 strict (FA only)
        fa = sub[sub.diagnosis_primary=="follicular_adenoma"]
        n_fa = len(fa)
        n_h2s = fa.h2_fa_strict.sum() if n_fa>0 else 0
        # H3 strict (MI-FTC only)
        mi = sub[(sub.diagnosis_primary=="FTC") & (sub.diagnosis_variant=="minimally_invasive")]
        n_mi = len(mi)
        n_h3s = mi.h3_strict.sum() if n_mi>0 else 0

        out_rows.append({
            "era":era,
            "n_cohort":int(n_total),
            "n_onco":int(n_onco),
            "pct_onco":round(p*100,1),
            "ci_onco":f"({lo*100:.1f}-{hi*100:.1f})",
            "n_def_total":int(n_dt),
            "pct_def_total":round(pdt*100,1),
            "ci_def_total":f"({lodt*100:.1f}-{hidt*100:.1f})",
            "n_fa":int(n_fa),
            "h2_strict_n":int(n_h2s),
            "h2_strict_pct":round(100*n_h2s/n_fa,2) if n_fa else np.nan,
            "n_mi_ftc":int(n_mi),
            "h3_strict_n":int(n_h3s),
            "h3_strict_pct":round(100*n_h3s/n_mi,2) if n_mi else np.nan,
        })
    pd.DataFrame(out_rows).to_csv(OUT/"table_s2_era_stratified.csv", index=False)
    print("Wrote table_s2_era_stratified.csv")

    # Era × historical_group interaction test for definitive_total (logistic)
    try:
        import statsmodels.formula.api as smf
        sub = df_pt[df_pt.surgery_year.notna() & df_pt.definitive_total.notna()].copy()
        sub["era_post2017"] = sub.era_5yr.isin(["2015-2019","2020-2025"]).astype(int)
        # Limit to groups with reasonable n
        keep = ["follicular_adenoma","FTC_minimally_invasive","FTC_oncocytic_warthin",
                "hurthle_cell_adenoma","NIFTP","FTC_NOS","FTUMP","atypical_follicular_adenoma"]
        sub = sub[sub.historical_group.isin(keep)]
        m_main = smf.logit("definitive_total ~ C(historical_group) + era_post2017", data=sub).fit(disp=0)
        m_int = smf.logit("definitive_total ~ C(historical_group) * era_post2017", data=sub).fit(disp=0)
        lr = 2*(m_int.llf - m_main.llf)
        df_diff = m_int.df_model - m_main.df_model
        p_int = 1 - stats.chi2.cdf(lr, df_diff)
        with open(OUT/"era_interaction_test.json","w") as f:
            json.dump({"lr_stat":float(lr),"df":int(df_diff),"p_interaction":float(p_int),
                       "n":int(len(sub))}, f, indent=2)
        print(f"Era×group interaction p={p_int:.3e}")
    except Exception as e:
        print(f"Interaction test failed: {e}")

era_strat()

# -------------------------------------------------------------------------- #
# C. Age/sex-stratified definitive total rates by historical group

def age_sex_strat():
    rows = []
    keep = ["follicular_adenoma","FTC_minimally_invasive","FTC_oncocytic_warthin",
            "hurthle_cell_adenoma","NIFTP","FTC_NOS","FTUMP","atypical_follicular_adenoma"]
    for grp in keep:
        sub = df_pt[df_pt.historical_group==grp]
        if len(sub) < 10: continue
        for stratum_col, stratum_label in [("age_ge55", "Age"), ("sex", "Sex")]:
            for val in sub[stratum_col].dropna().unique():
                ss = sub[sub[stratum_col]==val]
                if len(ss) < 5: continue
                n = len(ss); k = ss.definitive_total.sum()
                p, lo, hi = wilson(k, n)
                rows.append({
                    "historical_group":grp,
                    "stratum":stratum_label,
                    "value":(">=55y" if (stratum_col=="age_ge55" and val==1) else
                             ("<55y" if stratum_col=="age_ge55" else val)),
                    "n":int(n),
                    "n_def_total":int(k),
                    "pct_def_total":round(p*100,1),
                    "ci":f"({lo*100:.1f}-{hi*100:.1f})",
                })
    pd.DataFrame(rows).to_csv(OUT/"table_s3_age_sex_stratified.csv", index=False)
    print("Wrote table_s3_age_sex_stratified.csv")

age_sex_strat()

# -------------------------------------------------------------------------- #
# D. Tumor size by group

def size_by_group():
    rows = []
    grps = df_pt.groupby("historical_group")
    for grp, sub in grps:
        sz = sub.dom_tumor_size_cm.dropna()
        if len(sz) < 3: continue
        rows.append({
            "historical_group":grp,
            "n_with_size":int(len(sz)),
            "n_total":int(len(sub)),
            "pct_with_size":round(100*len(sz)/len(sub),1),
            "mean_cm":round(sz.mean(),2),
            "sd_cm":round(sz.std(),2),
            "median_cm":round(sz.median(),2),
            "iqr":f"{sz.quantile(.25):.2f}-{sz.quantile(.75):.2f}",
            "min_cm":round(sz.min(),2),
            "max_cm":round(sz.max(),2),
        })
    pd.DataFrame(rows).to_csv(OUT/"table_s1_size_by_group.csv", index=False)
    print("Wrote table_s1_size_by_group.csv")

size_by_group()

# -------------------------------------------------------------------------- #
# E. Invasion-field validation: capsular qualifier × historical group

def invasion_validation():
    rows = []
    for grp, sub in df_pt.groupby("historical_group"):
        n = len(sub)
        rows.append({
            "historical_group":grp,
            "n":n,
            "n_strict_eq":int(sub.capsular_strict_eq.sum()),
            "pct_strict_eq":round(100*sub.capsular_strict_eq.sum()/n,1) if n else np.nan,
            "n_broad_eq":int(sub.capsular_broad_eq.sum()),
            "pct_broad_eq":round(100*sub.capsular_broad_eq.sum()/n,1) if n else np.nan,
            "n_vascular_doc":int(sub.vascular_documented.sum()),
            "pct_vascular_doc":round(100*sub.vascular_documented.sum()/n,1) if n else np.nan,
            "n_vascular_quant_ge1":int(sub.vascular_quantified_ge1.sum()),
            "pct_vascular_quant_ge1":round(100*sub.vascular_quantified_ge1.sum()/n,1) if n else np.nan,
        })
    pd.DataFrame(rows).to_csv(OUT/"table_s4_invasion_validation.csv", index=False)
    print("Wrote table_s4_invasion_validation.csv")

invasion_validation()

# -------------------------------------------------------------------------- #
# F. Multivariable logistic regression for definitive total thyroidectomy

def multivar_logistic():
    try:
        import statsmodels.formula.api as smf
        d = df_pt[df_pt.definitive_total.notna() & df_pt.age_yr.notna() & df_pt.sex.isin(["female","male"])].copy()
        # Limit to groups with adequate n
        keep = ["follicular_adenoma","FTC_minimally_invasive","FTC_oncocytic_warthin",
                "hurthle_cell_adenoma","NIFTP","FTC_NOS","FTUMP","atypical_follicular_adenoma"]
        d = d[d.historical_group.isin(keep)]
        d["era_post2017"] = d.era_5yr.isin(["2015-2019","2020-2025"]).astype(int)
        d["size_known"] = d.dom_tumor_size_cm.notna().astype(int)
        d["size_imputed"] = d.dom_tumor_size_cm.fillna(d.dom_tumor_size_cm.median())
        d["age_decade"] = d.age_yr/10
        d["male"] = (d.sex=="male").astype(int)
        d["onco"] = (d.who2022_family=="Oncocytic").astype(int)

        # Reference category: follicular_adenoma
        d["historical_group"] = pd.Categorical(d.historical_group, categories=keep)
        m = smf.logit(
            "definitive_total ~ C(historical_group, Treatment(reference='follicular_adenoma')) + "
            "age_decade + male + size_imputed + size_known + era_post2017",
            data=d).fit(disp=0)
        rows = []
        ci = m.conf_int()
        for term in m.params.index:
            beta = m.params[term]
            lo, hi = ci.loc[term]
            rows.append({
                "term":term,
                "OR":round(math.exp(beta),3),
                "OR_lo":round(math.exp(lo),3),
                "OR_hi":round(math.exp(hi),3),
                "p":f"{m.pvalues[term]:.3e}",
            })
        pd.DataFrame(rows).to_csv(OUT/"table_5_multivariable_logistic.csv", index=False)
        with open(OUT/"multivariable_summary.txt","w") as f:
            f.write(str(m.summary()))
        print(f"Wrote table_5_multivariable_logistic.csv (n={len(d)}, llf={m.llf:.1f})")
    except Exception as e:
        print(f"Multivariable logistic failed: {e}")

multivar_logistic()

# -------------------------------------------------------------------------- #
# G. Sensitivity analyses

def sensitivity():
    base = df_pt.copy()
    rows = []

    def summarize(d, label):
        n = len(d)
        n_onco = (d.who2022_family=="Oncocytic").sum()
        p_onco, lo_o, hi_o = wilson(n_onco, n)
        # MI-FTC -> FT-UMP (strict)
        mi = d[(d.diagnosis_primary=="FTC") & (d.diagnosis_variant=="minimally_invasive")]
        n_mi = len(mi)
        n_h3s = int(mi.h3_strict.sum()) if n_mi else 0
        p_h3, lo_h3, hi_h3 = wilson(n_h3s, n_mi) if n_mi else (np.nan,np.nan,np.nan)
        # H4 MI-FTC vs FT-UMP definitive total
        mift = d[(d.diagnosis_primary=="FTC") & (d.diagnosis_variant=="minimally_invasive")]
        ftump = d[d.diagnosis_primary=="FTUMP"]
        if len(mift)>=5 and len(ftump)>=5:
            k1, n1 = int(mift.definitive_total.sum()), len(mift)
            k2, n2 = int(ftump.definitive_total.sum()), len(ftump)
            p1, p2 = k1/n1, k2/n2
            delta_pp = (p2-p1)*100
            # bootstrap CI
            rng = np.random.default_rng(42)
            deltas = []
            for _ in range(1000):
                d1 = rng.binomial(n1, p1)/n1
                d2 = rng.binomial(n2, p2)/n2
                deltas.append((d2-d1)*100)
            deltas = sorted(deltas)
            lo_d, hi_d = deltas[24], deltas[974]
            h4_str = f"{delta_pp:+.1f} pp ({lo_d:+.1f} to {hi_d:+.1f})"
        else:
            h4_str = "—"
        rows.append({
            "scenario":label,
            "n_cohort":n,
            "h1_pct_onco":round(p_onco*100,1),
            "h1_ci":f"({lo_o*100:.1f}-{hi_o*100:.1f})",
            "h3_strict_pct":round(p_h3*100,1) if not np.isnan(p_h3) else np.nan,
            "h3_strict_ci":f"({lo_h3*100:.1f}-{hi_h3*100:.1f})" if not np.isnan(p_h3) else "—",
            "h4_mi_to_ftump_delta":h4_str,
        })

    summarize(base, "Primary (full cohort, n=1,542)")
    summarize(base[base.surgery_year != 2025], "Excl. 2025 partial-year")
    summarize(base[base.is_multi_primary==0], "Excl. 5 multi-primary patients")
    summarize(base[~base.diagnosis_primary.isin(["DHGTC","HCC","PDTC","hyalinizing_trabecular_tumor"])],
              "Excl. small-n entities (DHGTC/HCC/PDTC/HTT)")
    summarize(base[(base.surgery_year != 2025) & (base.is_multi_primary==0) &
                   (~base.diagnosis_primary.isin(["DHGTC","HCC","PDTC","hyalinizing_trabecular_tumor"]))],
              "All sensitivity exclusions combined")

    pd.DataFrame(rows).to_csv(OUT/"table_s5_sensitivity.csv", index=False)
    print("Wrote table_s5_sensitivity.csv")

sensitivity()

# -------------------------------------------------------------------------- #
# H. Cochran-Armitage trend test for era × oncocytic family proportion

def trend_test():
    eras = ["1990-1994","1995-1999","2000-2004","2005-2009","2010-2014","2015-2019","2020-2025"]
    counts = []
    for i, era in enumerate(eras):
        sub = df_pt[df_pt.era_5yr==era]
        n = len(sub)
        if n == 0: continue
        n_onco = int((sub.who2022_family=="Oncocytic").sum())
        counts.append((i, n_onco, n))
    if not counts: return
    # Cochran-Armitage Z = sum(t_i*(2*x_i - n_i)) / sqrt( ... )
    ts = np.array([c[0] for c in counts])
    xs = np.array([c[1] for c in counts])
    ns = np.array([c[2] for c in counts])
    N = ns.sum(); X = xs.sum()
    p = X/N
    num = np.sum(ts * (xs - ns*p))
    var = p*(1-p) * (np.sum(ns*ts**2) - (np.sum(ns*ts)**2)/N)
    z = num/math.sqrt(var) if var>0 else np.nan
    pval = 2*(1 - stats.norm.cdf(abs(z))) if not np.isnan(z) else np.nan
    with open(OUT/"cochran_armitage_trend.json","w") as f:
        json.dump({"z":float(z),"p":float(pval),"eras_used":[c for c in counts]}, f, indent=2)
    print(f"Cochran-Armitage trend Z={z:.2f}, p={pval:.3e}")

trend_test()

# -------------------------------------------------------------------------- #
# I. FDR adjustment over the H1/H2/H3/H4 family

def fdr_family():
    pvals = []
    labels = []
    # H1: oncocytic vs conventional (family proportion vs 0.5? we'll compare oncocytic age vs conventional age)
    f1 = df_pt[df_pt.who2022_family=="Oncocytic"].age_yr.dropna()
    f2 = df_pt[df_pt.who2022_family=="Conventional follicular"].age_yr.dropna()
    pvals.append(stats.ttest_ind(f1, f2, equal_var=False).pvalue); labels.append("H1 age-shift Oncocytic vs Conventional")
    # H2 strict vs broad — proportion difference (FA cohort)
    fa = df_pt[df_pt.diagnosis_primary=="follicular_adenoma"]
    n_fa = len(fa); k_strict = int(fa.h2_fa_strict.sum()); k_broad = int(fa.h2_fa_broad.sum())
    chi2, p, _, _ = stats.chi2_contingency([[k_strict, n_fa-k_strict],[k_broad, n_fa-k_broad]])
    pvals.append(p); labels.append("H2 FA: strict vs broad proportion")
    # H3 strict vs broad — MI-FTC cohort
    mi = df_pt[(df_pt.diagnosis_primary=="FTC") & (df_pt.diagnosis_variant=="minimally_invasive")]
    n_mi = len(mi); k_strict = int(mi.h3_strict.sum()); k_broad = int(mi.h3_broad.sum())
    chi2, p, _, _ = stats.chi2_contingency([[k_strict, n_mi-k_strict],[k_broad, n_mi-k_broad]])
    pvals.append(p); labels.append("H3 MI-FTC: strict vs broad proportion")
    # H4 MI-FTC vs FTUMP definitive total
    mift = df_pt[(df_pt.diagnosis_primary=="FTC") & (df_pt.diagnosis_variant=="minimally_invasive")]
    ftump = df_pt[df_pt.diagnosis_primary=="FTUMP"]
    table = [[int(mift.definitive_total.sum()), int(len(mift)-mift.definitive_total.sum())],
             [int(ftump.definitive_total.sum()), int(len(ftump)-ftump.definitive_total.sum())]]
    chi2, p, _, _ = stats.chi2_contingency(table)
    pvals.append(p); labels.append("H4 MI-FTC vs FT-UMP definitive total")

    qvals = fdr_bh(pvals)
    rows = [{"hypothesis":l, "p_value":f"{p:.3e}", "q_value_BH":f"{q:.3e}"}
            for l,p,q in zip(labels, pvals, qvals)]
    pd.DataFrame(rows).to_csv(OUT/"fdr_adjusted_family.csv", index=False)
    print("Wrote fdr_adjusted_family.csv")

fdr_family()

print("\nAll extension analyses complete. Outputs in", OUT)
