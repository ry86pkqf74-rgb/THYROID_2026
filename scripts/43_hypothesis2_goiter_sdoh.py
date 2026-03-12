#!/usr/bin/env python3
"""
Hypothesis 2: Goiter Presentation — Social Determinants of Health
=================================================================
Presentation of goiter by age/race/gender by size and complications.
A social determinant of health evaluation.

Outputs → studies/hypothesis2_goiter_sdoh/
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy import stats
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

STUDY_DIR = Path(__file__).resolve().parent.parent / "studies" / "hypothesis2_goiter_sdoh"
STUDY_DIR.mkdir(parents=True, exist_ok=True)

np.random.seed(42)


def get_connection(use_md: bool) -> duckdb.DuckDBPyConnection:
    if use_md:
        try:
            import toml
            token = toml.load(".streamlit/secrets.toml")["MOTHERDUCK_TOKEN"]
        except Exception:
            token = os.getenv("MOTHERDUCK_TOKEN", "")
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect("thyroid_master_local.duckdb")


RACE_NORM = """
CASE
    WHEN race LIKE '%African%' OR race LIKE '%Black%' THEN 'Black'
    WHEN race LIKE '%Caucasian%' OR race = 'White' THEN 'White'
    WHEN race LIKE '%Asian%' OR race LIKE '%Korean%' OR race LIKE '%Chinese%'
         OR race LIKE '%Vietnamese%' OR race LIKE '%Filipino%'
         OR race LIKE '%Japanese%' OR race LIKE '%Indian%' THEN 'Asian'
    WHEN race LIKE '%Hispanic%' OR race LIKE '%Latino%' THEN 'Hispanic'
    WHEN race LIKE '%Pacific%' OR race LIKE '%Hawaiian%' THEN 'NHPI'
    WHEN race LIKE '%American Indian%' OR race LIKE '%Alaskan%' THEN 'AI/AN'
    WHEN race IN ('Unknown', 'Unknown, Unavailable or Unreported', 'Not Recorded',
                  'Patient Declines', 'Decline to Answer', 'n/s', 'Multiple', 'Other')
         OR race IS NULL THEN 'Unknown/Other'
    ELSE 'Unknown/Other'
END
"""

GOITER_COHORT_SQL = f"""
SELECT
    p.research_id,
    p.age,
    CASE WHEN LOWER(p.gender) = 'male' THEN 'Male' ELSE 'Female' END AS sex,
    p.race AS race_raw,
    {RACE_NORM} AS race_group,
    p.thyroid_procedure,
    CASE
        WHEN LOWER(COALESCE(p.substernal_multinodular_goiter, '')) = 'x' THEN 'Substernal'
        ELSE 'Cervical'
    END AS goiter_type,
    TRY_CAST(REPLACE(p.weight_total, ';', '') AS DOUBLE) AS specimen_weight_g,
    TRY_CAST(REPLACE(p.tumor_1_size_greatest_dimension_cm, ';', '') AS DOUBLE) AS dominant_nodule_cm,
    p.tumor_1_histologic_type AS histology,
    CASE WHEN p.tumor_1_histologic_type IS NOT NULL AND p.tumor_1_histologic_type != '' THEN 1 ELSE 0 END AS incidental_cancer,
    CASE WHEN LOWER(COALESCE(p.graves, '')) = 'x' THEN 1 ELSE 0 END AS graves_flag,
    CASE WHEN LOWER(COALESCE(p.hashimoto_thyroiditis, '')) = 'x'
              OR LOWER(COALESCE(p.chronic_lymphocytic_thyroiditis, '')) = 'x'
              OR LOWER(COALESCE(p.lymphocytic_thyroiditis, '')) = 'x' THEN 1 ELSE 0 END AS thyroiditis_flag,
    CASE
        WHEN LOWER(p.thyroid_procedure) LIKE '%total%' THEN 'Total Thyroidectomy'
        WHEN LOWER(p.thyroid_procedure) LIKE '%lobectomy%'
             OR LOWER(p.thyroid_procedure) LIKE '%lobe%' THEN 'Lobectomy'
        ELSE 'Other'
    END AS surgery_extent
FROM path_synoptics p
WHERE LOWER(COALESCE(p.multinodular_goiter, '')) = 'x'
   OR LOWER(COALESCE(p.substernal_multinodular_goiter, '')) = 'x'
"""


def load_goiter_cohort(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute(GOITER_COHORT_SQL).fetchdf()
    return df


def add_complications(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> pd.DataFrame:
    """Phase 3: Join refined complication flags from patient_refined_complication_flags_v2."""
    pcf_sql = """
    SELECT
        research_id,
        COALESCE(confirmed_rln_injury, 0) AS rln_injury,
        COALESCE(refined_rln_injury, 0) AS rln_injury_refined,
        COALESCE(refined_hypocalcemia, 0) AS refined_hypocalcemia,
        COALESCE(refined_hypoparathyroidism, 0) AS refined_hypoparathyroidism,
        COALESCE(refined_hematoma, 0) AS refined_hematoma,
        COALESCE(refined_seroma, 0) AS refined_seroma,
        COALESCE(refined_chyle_leak, 0) AS refined_chyle_leak,
        COALESCE(refined_wound_infection, 0) AS refined_wound_infection,
        COALESCE(has_confirmed_complication, 0) AS has_any_confirmed_complication
    FROM patient_refined_complication_flags_v2
    """
    pcf_df = con.execute(pcf_sql).fetchdf()
    df = df.merge(pcf_df, on="research_id", how="left")
    fill_cols = [c for c in pcf_df.columns if c != "research_id"]
    df[fill_cols] = df[fill_cols].fillna(0)
    return df


def load_comparison_cohort(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Non-goiter patients for comparison."""
    sql = f"""
    SELECT
        p.research_id,
        p.age,
        CASE WHEN LOWER(p.gender) = 'male' THEN 'Male' ELSE 'Female' END AS sex,
        {RACE_NORM} AS race_group,
        TRY_CAST(REPLACE(p.weight_total, ';', '') AS DOUBLE) AS specimen_weight_g,
        TRY_CAST(REPLACE(p.tumor_1_size_greatest_dimension_cm, ';', '') AS DOUBLE) AS dominant_nodule_cm,
        'Non-Goiter' AS cohort_label
    FROM path_synoptics p
    WHERE LOWER(COALESCE(p.multinodular_goiter, '')) != 'x'
      AND LOWER(COALESCE(p.substernal_multinodular_goiter, '')) != 'x'
    """
    return con.execute(sql).fetchdf()


def table1_goiter_demographics(df: pd.DataFrame) -> pd.DataFrame:
    """Table 1: Goiter demographics by race."""
    rows = []
    race_order = ["Black", "White", "Asian", "Hispanic", "NHPI", "AI/AN", "Unknown/Other"]
    for race in race_order:
        sub = df[df["race_group"] == race]
        if len(sub) == 0:
            continue
        age_vals = sub["age"].dropna()
        wt_vals = sub["specimen_weight_g"].dropna()
        nod_vals = sub["dominant_nodule_cm"].dropna()
        rows.append({
            "Race": race,
            "N": len(sub),
            "Pct_of_cohort": round(100 * len(sub) / len(df), 1),
            "Female_pct": round(100 * (sub["sex"] == "Female").sum() / max(len(sub), 1), 1),
            "Age_mean_SD": f"{age_vals.mean():.1f} ({age_vals.std():.1f})" if len(age_vals) > 1 else "—",
            "Weight_g_median_IQR": f"{wt_vals.median():.0f} [{wt_vals.quantile(.25):.0f}-{wt_vals.quantile(.75):.0f}]" if len(wt_vals) > 5 else "—",
            "Nodule_cm_median_IQR": f"{nod_vals.median():.1f} [{nod_vals.quantile(.25):.1f}-{nod_vals.quantile(.75):.1f}]" if len(nod_vals) > 5 else "—",
            "Substernal_pct": round(100 * (sub["goiter_type"] == "Substernal").sum() / max(len(sub), 1), 1),
            "Incidental_cancer_pct": round(100 * sub["incidental_cancer"].mean(), 1),
            "Graves_pct": round(100 * sub["graves_flag"].mean(), 1),
            "Thyroiditis_pct": round(100 * sub["thyroiditis_flag"].mean(), 1),
        })

    total = df.copy()
    age_all = total["age"].dropna()
    wt_all = total["specimen_weight_g"].dropna()
    nod_all = total["dominant_nodule_cm"].dropna()
    rows.append({
        "Race": "TOTAL",
        "N": len(total),
        "Pct_of_cohort": 100.0,
        "Female_pct": round(100 * (total["sex"] == "Female").sum() / max(len(total), 1), 1),
        "Age_mean_SD": f"{age_all.mean():.1f} ({age_all.std():.1f})",
        "Weight_g_median_IQR": f"{wt_all.median():.0f} [{wt_all.quantile(.25):.0f}-{wt_all.quantile(.75):.0f}]" if len(wt_all) > 5 else "—",
        "Nodule_cm_median_IQR": f"{nod_all.median():.1f} [{nod_all.quantile(.25):.1f}-{nod_all.quantile(.75):.1f}]" if len(nod_all) > 5 else "—",
        "Substernal_pct": round(100 * (total["goiter_type"] == "Substernal").sum() / max(len(total), 1), 1),
        "Incidental_cancer_pct": round(100 * total["incidental_cancer"].mean(), 1),
        "Graves_pct": round(100 * total["graves_flag"].mean(), 1),
        "Thyroiditis_pct": round(100 * total["thyroiditis_flag"].mean(), 1),
    })
    return pd.DataFrame(rows)


def analyze_size_by_demographics(df: pd.DataFrame) -> dict:
    """Specimen weight and nodule size by race/sex/age."""
    results = {}

    wt_by_race = {}
    for race in df["race_group"].dropna().unique():
        vals = df.loc[df["race_group"] == race, "specimen_weight_g"].dropna()
        if len(vals) > 5:
            wt_by_race[race] = vals.tolist()

    if len(wt_by_race) >= 2:
        groups = list(wt_by_race.values())
        h_stat, p_kw = stats.kruskal(*groups)
        results["weight_by_race_kruskal"] = {"H": round(h_stat, 3), "p": round(p_kw, 6), "n_groups": len(groups)}

    wt_male = df.loc[df["sex"] == "Male", "specimen_weight_g"].dropna()
    wt_female = df.loc[df["sex"] == "Female", "specimen_weight_g"].dropna()
    if len(wt_male) > 5 and len(wt_female) > 5:
        u_stat, p_u = stats.mannwhitneyu(wt_male, wt_female, alternative="two-sided")
        results["weight_by_sex"] = {
            "male_median": round(wt_male.median(), 1),
            "female_median": round(wt_female.median(), 1),
            "U": round(u_stat, 1), "p": round(p_u, 6),
        }

    age_wt = df[["age", "specimen_weight_g"]].dropna()
    if len(age_wt) > 20:
        r_coeff, p_corr = stats.spearmanr(age_wt["age"], age_wt["specimen_weight_g"])
        results["age_weight_correlation"] = {"spearman_r": round(r_coeff, 4), "p": round(p_corr, 6)}

    return results


def analyze_complications_by_demographics(df: pd.DataFrame) -> pd.DataFrame:
    """Complication rates by race and sex (Phase 3: refined flags)."""
    comps = ["rln_injury", "refined_hypocalcemia", "refined_hypoparathyroidism",
             "refined_hematoma", "refined_seroma", "refined_chyle_leak"]
    comp_labels = ["RLN Injury", "Hypocalcemia", "Hypoparathyroidism",
                   "Hematoma", "Seroma", "Chyle Leak"]

    rows = []
    race_order = ["Black", "White", "Asian", "Unknown/Other"]
    for race in race_order:
        sub = df[df["race_group"] == race]
        if len(sub) < 10:
            continue
        row = {"Group": race, "N": len(sub)}
        for comp, label in zip(comps, comp_labels):
            if comp in sub.columns:
                n_pos = int(sub[comp].sum())
                row[label] = f"{n_pos} ({100*n_pos/max(len(sub),1):.1f}%)"
        rows.append(row)

    for sex_val in ["Male", "Female"]:
        sub = df[df["sex"] == sex_val]
        row = {"Group": sex_val, "N": len(sub)}
        for comp, label in zip(comps, comp_labels):
            if comp in sub.columns:
                n_pos = int(sub[comp].sum())
                row[label] = f"{n_pos} ({100*n_pos/max(len(sub),1):.1f}%)"
        rows.append(row)

    for gtype in ["Cervical", "Substernal"]:
        sub = df[df["goiter_type"] == gtype]
        row = {"Group": f"Goiter: {gtype}", "N": len(sub)}
        for comp, label in zip(comps, comp_labels):
            if comp in sub.columns:
                n_pos = int(sub[comp].sum())
                row[label] = f"{n_pos} ({100*n_pos/max(len(sub),1):.1f}%)"
        rows.append(row)

    return pd.DataFrame(rows)


def statistical_tests_complications(df: pd.DataFrame) -> pd.DataFrame:
    """Chi-square / Fisher tests for complication differences (Phase 3: refined)."""
    comps = ["rln_injury", "refined_hypocalcemia", "refined_hypoparathyroidism",
             "refined_hematoma", "refined_seroma"]
    comp_labels = ["RLN Injury", "Hypocalcemia", "Hypoparathyroidism", "Hematoma", "Seroma"]

    rows = []

    analytic_races = df[df["race_group"].isin(["Black", "White", "Asian"])].copy()

    for comp, label in zip(comps, comp_labels):
        if comp not in df.columns:
            continue

        ct_race = pd.crosstab(analytic_races["race_group"], analytic_races[comp].astype(int))
        if ct_race.shape[0] >= 2 and ct_race.shape[1] == 2:
            chi2, p_race, _, _ = stats.chi2_contingency(ct_race)
        else:
            chi2, p_race = np.nan, np.nan

        ct_sex = pd.crosstab(df["sex"], df[comp].astype(int))
        if ct_sex.shape[0] >= 2 and ct_sex.shape[1] == 2:
            _, p_sex, _, _ = stats.chi2_contingency(ct_sex)
        else:
            p_sex = np.nan

        ct_goiter = pd.crosstab(df["goiter_type"], df[comp].astype(int))
        if ct_goiter.shape[0] >= 2 and ct_goiter.shape[1] == 2:
            _, p_goiter, _, _ = stats.chi2_contingency(ct_goiter)
        else:
            p_goiter = np.nan

        rows.append({
            "Complication": label,
            "p_by_race": round(p_race, 4) if not np.isnan(p_race) else "—",
            "p_by_sex": round(p_sex, 4) if not np.isnan(p_sex) else "—",
            "p_cervical_vs_substernal": round(p_goiter, 4) if not np.isnan(p_goiter) else "—",
        })
    return pd.DataFrame(rows)


def logistic_regression_complications(df: pd.DataFrame) -> pd.DataFrame:
    """Multivariable logistic regression for RLN injury in goiter patients."""
    import statsmodels.api as sm

    model_df = df[["rln_injury", "age", "sex", "race_group",
                    "specimen_weight_g", "goiter_type", "surgery_extent"]].dropna(subset=["age", "specimen_weight_g"]).copy()
    model_df["female"] = (model_df["sex"] == "Female").astype(int)
    model_df["black"] = (model_df["race_group"] == "Black").astype(int)
    model_df["asian"] = (model_df["race_group"] == "Asian").astype(int)
    model_df["substernal"] = (model_df["goiter_type"] == "Substernal").astype(int)
    model_df["total_thyroidectomy"] = (model_df["surgery_extent"] == "Total Thyroidectomy").astype(int)

    X_cols = ["age", "female", "black", "asian", "specimen_weight_g", "substernal", "total_thyroidectomy"]
    X = model_df[X_cols].astype(float)
    y = model_df["rln_injury"].astype(float)

    if y.sum() < 10:
        return pd.DataFrame({"note": ["Insufficient events for logistic regression"]})

    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    X_scaled = pd.DataFrame(scaler.fit_transform(X), columns=X_cols)
    X_sm = sm.add_constant(X_scaled)

    try:
        logit = sm.Logit(y.values, X_sm.values).fit(disp=0, maxiter=100)
        results = []
        for i, col in enumerate(X_cols):
            coef = logit.params[i + 1]
            se = logit.bse[i + 1]
            pval = logit.pvalues[i + 1]
            or_val = np.exp(coef)
            or_lo = np.exp(coef - 1.96 * se)
            or_hi = np.exp(coef + 1.96 * se)
            results.append({
                "Variable": col,
                "Coefficient": round(coef, 4),
                "SE": round(se, 4),
                "OR": round(or_val, 3),
                "OR_95CI_low": round(or_lo, 3),
                "OR_95CI_high": round(or_hi, 3),
                "p_value": round(pval, 6),
            })
        res_df = pd.DataFrame(results)
        res_df.attrs["n_obs"] = len(model_df)
        res_df.attrs["pseudo_r2"] = round(logit.prsquared, 4)
        res_df.attrs["aic"] = round(logit.aic, 1)
        res_df.attrs["n_events"] = int(y.sum())
        return res_df
    except Exception as e:
        return pd.DataFrame({"note": [f"Model failed: {e}"]})


def plot_demographics_by_race(df: pd.DataFrame) -> None:
    """Goiter demographics summary by race."""
    race_order = ["Black", "White", "Asian", "Unknown/Other"]
    race_df = df[df["race_group"].isin(race_order)].copy()

    fig, axes = plt.subplots(2, 2, figsize=(12, 10))

    counts = race_df.groupby("race_group").size().reindex(race_order).fillna(0)
    colors_map = {"Black": "#2C5F8A", "White": "#8CB4D5", "Asian": "#E8A838", "Unknown/Other": "#AAAAAA"}
    bar_colors = [colors_map.get(r, "#CCCCCC") for r in race_order]
    axes[0, 0].bar(race_order, counts, color=bar_colors, edgecolor="black", linewidth=0.5)
    axes[0, 0].set_title("Cohort Size by Race", fontweight="bold")
    axes[0, 0].set_ylabel("N Patients")
    for i, (r, c) in enumerate(zip(race_order, counts)):
        axes[0, 0].text(i, c + 20, str(int(c)), ha="center", fontsize=10, fontweight="bold")

    age_data = [race_df.loc[race_df["race_group"] == r, "age"].dropna().tolist() for r in race_order]
    bp = axes[0, 1].boxplot(age_data, labels=race_order, patch_artist=True, widths=0.5)
    for patch, color in zip(bp["boxes"], bar_colors):
        patch.set_facecolor(color)
    axes[0, 1].set_title("Age Distribution by Race", fontweight="bold")
    axes[0, 1].set_ylabel("Age (years)")

    wt_data = [race_df.loc[race_df["race_group"] == r, "specimen_weight_g"].dropna().tolist() for r in race_order]
    wt_data_clipped = [[min(x, 500) for x in d] for d in wt_data]
    bp2 = axes[1, 0].boxplot(wt_data_clipped, labels=race_order, patch_artist=True, widths=0.5)
    for patch, color in zip(bp2["boxes"], bar_colors):
        patch.set_facecolor(color)
    axes[1, 0].set_title("Specimen Weight by Race (capped 500g)", fontweight="bold")
    axes[1, 0].set_ylabel("Weight (grams)")

    sex_pcts = race_df.groupby("race_group")["sex"].apply(lambda x: 100 * (x == "Female").sum() / len(x)).reindex(race_order)
    axes[1, 1].bar(race_order, sex_pcts, color=bar_colors, edgecolor="black", linewidth=0.5)
    axes[1, 1].set_title("Female Proportion by Race", fontweight="bold")
    axes[1, 1].set_ylabel("Female (%)")
    axes[1, 1].set_ylim(0, 100)
    for i, pct in enumerate(sex_pcts):
        axes[1, 1].text(i, pct + 1, f"{pct:.0f}%", ha="center", fontsize=10, fontweight="bold")

    for ax in axes.flat:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle("Goiter Presentation by Race — Social Determinants of Health", fontsize=14, fontweight="bold", y=1.01)
    plt.tight_layout()
    fig.savefig(STUDY_DIR / "fig_demographics_by_race.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_complications_by_race_sex(df: pd.DataFrame) -> None:
    """Complication rates by race and sex (Phase 3: refined flags)."""
    comps = ["rln_injury", "refined_hypocalcemia", "refined_hypoparathyroidism"]
    labels = ["RLN Injury", "Hypocalcemia", "Hypoparathyroidism"]
    race_order = ["Black", "White", "Asian"]

    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    colors_map = {"Black": "#2C5F8A", "White": "#8CB4D5", "Asian": "#E8A838"}

    for idx, (comp, label) in enumerate(zip(comps, labels)):
        if comp not in df.columns:
            continue
        rates = []
        for race in race_order:
            sub = df[df["race_group"] == race]
            rates.append(100 * sub[comp].mean() if len(sub) > 0 else 0)

        bars = axes[idx].bar(race_order, rates, color=[colors_map[r] for r in race_order],
                             edgecolor="black", linewidth=0.5, width=0.5)
        for bar, rate in zip(bars, rates):
            axes[idx].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                           f"{rate:.1f}%", ha="center", fontsize=10, fontweight="bold")
        axes[idx].set_title(label, fontweight="bold")
        axes[idx].set_ylabel("Rate (%)")
        axes[idx].spines["top"].set_visible(False)
        axes[idx].spines["right"].set_visible(False)

    fig.suptitle("Complication Rates by Race in Goiter Patients", fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(STUDY_DIR / "fig_complications_by_race.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_goiter_vs_nongoiter(goiter_df: pd.DataFrame, nongoiter_df: pd.DataFrame) -> None:
    """Goiter vs non-goiter comparison."""
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))

    age_g = goiter_df["age"].dropna()
    age_ng = nongoiter_df["age"].dropna()
    axes[0].hist(age_g, bins=30, alpha=0.6, color="#D65F5F", label=f"Goiter (n={len(goiter_df)})", density=True)
    axes[0].hist(age_ng, bins=30, alpha=0.6, color="#5B8DB8", label=f"Non-Goiter (n={len(nongoiter_df)})", density=True)
    axes[0].set_xlabel("Age (years)")
    axes[0].set_ylabel("Density")
    axes[0].set_title("Age Distribution", fontweight="bold")
    axes[0].legend(frameon=False, fontsize=9)

    wt_g = goiter_df["specimen_weight_g"].dropna().clip(upper=500)
    wt_ng = nongoiter_df["specimen_weight_g"].dropna().clip(upper=500)
    axes[1].hist(wt_g, bins=30, alpha=0.6, color="#D65F5F", label="Goiter", density=True)
    axes[1].hist(wt_ng, bins=30, alpha=0.6, color="#5B8DB8", label="Non-Goiter", density=True)
    axes[1].set_xlabel("Specimen Weight (g, capped 500)")
    axes[1].set_ylabel("Density")
    axes[1].set_title("Specimen Weight", fontweight="bold")
    axes[1].legend(frameon=False, fontsize=9)

    race_order = ["Black", "White", "Asian", "Unknown/Other"]
    g_pcts = goiter_df["race_group"].value_counts(normalize=True).reindex(race_order, fill_value=0) * 100
    ng_pcts = nongoiter_df["race_group"].value_counts(normalize=True).reindex(race_order, fill_value=0) * 100
    x = np.arange(len(race_order))
    w = 0.35
    axes[2].bar(x - w/2, g_pcts, w, color="#D65F5F", label="Goiter", edgecolor="black", linewidth=0.5)
    axes[2].bar(x + w/2, ng_pcts, w, color="#5B8DB8", label="Non-Goiter", edgecolor="black", linewidth=0.5)
    axes[2].set_xticks(x)
    axes[2].set_xticklabels(race_order, rotation=15, ha="right")
    axes[2].set_ylabel("Proportion (%)")
    axes[2].set_title("Racial Composition", fontweight="bold")
    axes[2].legend(frameon=False, fontsize=9)

    for ax in axes:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle("Goiter vs Non-Goiter: Demographic Comparison", fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(STUDY_DIR / "fig_goiter_vs_nongoiter.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_substernal_comparison(df: pd.DataFrame) -> None:
    """Cervical vs substernal goiter comparison."""
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))

    for gtype, color in [("Cervical", "#5B8DB8"), ("Substernal", "#D65F5F")]:
        sub = df[df["goiter_type"] == gtype]
        axes[0].hist(sub["age"].dropna(), bins=25, alpha=0.6, color=color,
                     label=f"{gtype} (n={len(sub)})", density=True)
    axes[0].set_xlabel("Age (years)")
    axes[0].set_title("Age Distribution", fontweight="bold")
    axes[0].legend(frameon=False, fontsize=9)

    wt_cerv = df.loc[df["goiter_type"] == "Cervical", "specimen_weight_g"].dropna().clip(upper=500)
    wt_sub = df.loc[df["goiter_type"] == "Substernal", "specimen_weight_g"].dropna().clip(upper=500)
    axes[1].boxplot([wt_cerv, wt_sub], labels=["Cervical", "Substernal"], patch_artist=True,
                    boxprops=dict(facecolor="#5B8DB8"), medianprops=dict(color="black"))
    axes[1].set_ylabel("Weight (g)")
    axes[1].set_title("Specimen Weight", fontweight="bold")

    comps = ["rln_injury", "refined_hypocalcemia"]
    labels_c = ["RLN Injury", "Hypocalcemia"]
    for ci, (comp, lab) in enumerate(zip(comps, labels_c)):
        if comp in df.columns:
            cerv_rate = 100 * df.loc[df["goiter_type"] == "Cervical", comp].mean()
            sub_rate = 100 * df.loc[df["goiter_type"] == "Substernal", comp].mean()
            x_pos = ci * 2
            axes[2].bar([x_pos, x_pos + 0.8], [cerv_rate, sub_rate], width=0.7,
                        color=["#5B8DB8", "#D65F5F"], edgecolor="black", linewidth=0.5)
            axes[2].text(x_pos, cerv_rate + 0.3, f"{cerv_rate:.1f}%", ha="center", fontsize=9)
            axes[2].text(x_pos + 0.8, sub_rate + 0.3, f"{sub_rate:.1f}%", ha="center", fontsize=9)

    axes[2].set_xticks([0.4, 2.4])
    axes[2].set_xticklabels(labels_c)
    axes[2].set_ylabel("Rate (%)")
    axes[2].set_title("Complications", fontweight="bold")
    custom_handles = [plt.Rectangle((0,0),1,1, facecolor="#5B8DB8"), plt.Rectangle((0,0),1,1, facecolor="#D65F5F")]
    axes[2].legend(custom_handles, ["Cervical", "Substernal"], frameon=False, fontsize=9)

    for ax in axes:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle("Cervical vs Substernal Goiter Comparison", fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(STUDY_DIR / "fig_substernal_comparison.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_weight_by_race_sex(df: pd.DataFrame) -> None:
    """Specimen weight heatmap by race and sex."""
    race_order = ["Black", "White", "Asian", "Unknown/Other"]
    pivot = df[df["race_group"].isin(race_order)].groupby(["race_group", "sex"])["specimen_weight_g"].median().unstack(fill_value=0)
    pivot = pivot.reindex(race_order)

    fig, ax = plt.subplots(figsize=(6, 4))
    im = ax.imshow(pivot.values, cmap="YlOrRd", aspect="auto")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.values[i, j]
            ax.text(j, i, f"{val:.0f}g", ha="center", va="center", fontsize=11,
                    color="white" if val > pivot.values.mean() else "black", fontweight="bold")
    ax.set_title("Median Specimen Weight (g) by Race and Sex", fontweight="bold")
    plt.colorbar(im, ax=ax, label="Weight (g)")
    plt.tight_layout()
    fig.savefig(STUDY_DIR / "fig_weight_heatmap.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Hypothesis 2: Goiter SDOH Analysis")
    parser.add_argument("--md", action="store_true", default=True, help="Use MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only")
    args = parser.parse_args()

    use_md = not args.local
    if args.dry_run:
        print(GOITER_COHORT_SQL)
        return

    print("=" * 70)
    print("HYPOTHESIS 2: Goiter Presentation — SDOH Evaluation")
    print("=" * 70)

    con = get_connection(use_md)
    print(f"\n[1/8] Loading goiter cohort ({'MotherDuck' if use_md else 'local'})...")
    df = load_goiter_cohort(con)
    print(f"  → {len(df)} goiter patients")
    print(f"  → Cervical: {(df['goiter_type']=='Cervical').sum()}, Substernal: {(df['goiter_type']=='Substernal').sum()}")

    print("\n[2/8] Adding refined complication data (Phase 3: patient_refined_complication_flags_v2)...")
    df = add_complications(con, df)
    rln_n = int(df["rln_injury"].sum())
    print(f"  → RLN confirmed: {rln_n}, refined: {int(df['rln_injury_refined'].sum())}")

    print("\n[3/8] Loading non-goiter comparison cohort...")
    nongoiter = load_comparison_cohort(con)
    print(f"  → {len(nongoiter)} non-goiter patients")

    print("\n[4/8] Table 1: Demographics by race...")
    t1 = table1_goiter_demographics(df)
    t1.to_csv(STUDY_DIR / "table1_demographics_by_race.csv", index=False)
    print(t1.to_string(index=False))

    print("\n[5/8] Size analysis by demographics...")
    size_results = analyze_size_by_demographics(df)
    print(f"  Specimen weight by race (Kruskal-Wallis): H={size_results.get('weight_by_race_kruskal',{}).get('H','—')}, p={size_results.get('weight_by_race_kruskal',{}).get('p','—')}")
    print(f"  Weight by sex (Mann-Whitney): Male median={size_results.get('weight_by_sex',{}).get('male_median','—')}g, Female median={size_results.get('weight_by_sex',{}).get('female_median','—')}g, p={size_results.get('weight_by_sex',{}).get('p','—')}")
    print(f"  Age-weight correlation (Spearman): r={size_results.get('age_weight_correlation',{}).get('spearman_r','—')}, p={size_results.get('age_weight_correlation',{}).get('p','—')}")

    print("\n[6/8] Complication rates by demographics...")
    comp_df = analyze_complications_by_demographics(df)
    comp_df.to_csv(STUDY_DIR / "complications_by_demographics.csv", index=False)
    print(comp_df.to_string(index=False))

    print("\n  Statistical tests for complications:")
    stat_df = statistical_tests_complications(df)
    stat_df.to_csv(STUDY_DIR / "complication_statistical_tests.csv", index=False)
    print(stat_df.to_string(index=False))

    print("\n  Multivariable logistic regression for RLN injury:")
    lr_df = logistic_regression_complications(df)
    lr_df.to_csv(STUDY_DIR / "logistic_regression_rln_goiter.csv", index=False)
    print(lr_df.to_string(index=False))
    if hasattr(lr_df, 'attrs') and 'n_obs' in lr_df.attrs:
        print(f"  N={lr_df.attrs['n_obs']}, events={lr_df.attrs.get('n_events','—')}, pseudo-R²={lr_df.attrs.get('pseudo_r2','—')}, AIC={lr_df.attrs.get('aic','—')}")

    print("\n[7/8] Goiter vs Non-Goiter comparison tests...")
    age_g = df["age"].dropna()
    age_ng = nongoiter["age"].dropna()
    u_age, p_age = stats.mannwhitneyu(age_g, age_ng, alternative="two-sided")
    print(f"  Age: Goiter mean={age_g.mean():.1f}, Non-Goiter mean={age_ng.mean():.1f}, p={p_age:.2e}")

    wt_g = df["specimen_weight_g"].dropna()
    wt_ng = nongoiter["specimen_weight_g"].dropna()
    if len(wt_g) > 5 and len(wt_ng) > 5:
        u_wt, p_wt = stats.mannwhitneyu(wt_g, wt_ng, alternative="two-sided")
        print(f"  Weight: Goiter median={wt_g.median():.0f}g, Non-Goiter median={wt_ng.median():.0f}g, p={p_wt:.2e}")

    combined = pd.concat([
        df[["race_group"]].assign(cohort="Goiter"),
        nongoiter[["race_group"]].assign(cohort="Non-Goiter"),
    ], ignore_index=True)
    ct_race = pd.crosstab(combined["cohort"], combined["race_group"])
    chi2_race, p_race, _, _ = stats.chi2_contingency(ct_race)
    print(f"  Race distribution (chi-square): χ²={chi2_race:.1f}, p={p_race:.2e}")

    print("\n[8/8] Generating figures...")
    plot_demographics_by_race(df)
    plot_complications_by_race_sex(df)
    plot_goiter_vs_nongoiter(df, nongoiter)
    plot_substernal_comparison(df)
    plot_weight_by_race_sex(df)
    print("  → 5 figures saved to", STUDY_DIR)

    summary = {
        "hypothesis": "Goiter presentation by age/race/gender/size/complications — SDOH evaluation",
        "cohort": {
            "total_goiter": len(df),
            "cervical": int((df["goiter_type"] == "Cervical").sum()),
            "substernal": int((df["goiter_type"] == "Substernal").sum()),
            "nongoiter_comparison": len(nongoiter),
        },
        "race_distribution": df["race_group"].value_counts().to_dict(),
        "sex_distribution": df["sex"].value_counts().to_dict(),
        "size_analysis": size_results,
        "goiter_vs_nongoiter": {
            "age_p": round(p_age, 6),
            "weight_p": round(p_wt, 6) if len(wt_g) > 5 and len(wt_ng) > 5 else "—",
            "race_chi2": round(chi2_race, 1),
            "race_p": round(p_race, 6),
        },
        "complication_source": "patient_refined_complication_flags_v2 (Phase 3 refined, not raw NLP)",
        "logistic_regression_rln": {
            "n_obs": lr_df.attrs.get("n_obs", "—") if hasattr(lr_df, 'attrs') else "—",
            "n_events": lr_df.attrs.get("n_events", "—") if hasattr(lr_df, 'attrs') else "—",
            "pseudo_r2": lr_df.attrs.get("pseudo_r2", "—") if hasattr(lr_df, 'attrs') else "—",
        },
        "sdoh_data_limitations": [
            "No insurance/payer data available",
            "No zip code or area deprivation index",
            "No income/education/employment data",
            "No ethnicity/Hispanic identifier (separate from race)",
            "Race serves as only available SDOH proxy variable",
        ],
        "data_source": "MotherDuck" if use_md else "local DuckDB",
        "generated_at": datetime.now().isoformat(),
        "random_seed": 42,
    }
    with open(STUDY_DIR / "analysis_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    df.to_csv(STUDY_DIR / "goiter_cohort.csv", index=False)

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE — All outputs saved to:")
    print(f"  {STUDY_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    main()
