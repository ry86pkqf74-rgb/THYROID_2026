#!/usr/bin/env python3
"""
Hypothesis 1: Central Lymph Node Dissection in Lobectomy
========================================================
Were central lymph nodes predictive for recurrence when lobectomy performed?
Association with central LNs and complications / RLN injury in lobectomies.

Outputs → studies/hypothesis1_cln_lobectomy/
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

STUDY_DIR = Path(__file__).resolve().parent.parent / "studies" / "hypothesis1_cln_lobectomy"
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


LOBECTOMY_COHORT_SQL = """
WITH lobectomy_base AS (
    SELECT
        p.research_id,
        p.thyroid_procedure,
        p.age,
        p.gender,
        p.race,
        TRY_CAST(REPLACE(p.tumor_1_size_greatest_dimension_cm, ';', '') AS DOUBLE) AS tumor_size_cm,
        TRY_CAST(REPLACE(p.weight_total, ';', '') AS DOUBLE) AS specimen_weight_g,
        p.tumor_1_histologic_type AS histology,
        p.tumor_1_extrathyroidal_extension AS ete_raw,
        p.tumor_1_angioinvasion AS angioinvasion,
        p.central_compartment_dissection,
        p.tumor_1_level_examined,
        p.tumor_1_level_involved,
        p.other_ln_dissection,
        p.tumor_1_ln_location,
        TRY_CAST(REPLACE(REPLACE(p.tumor_1_ln_examined, ';', ''), 'x', '') AS INT) AS ln_examined,
        TRY_CAST(REPLACE(REPLACE(p.tumor_1_ln_involved, ';', ''), 'x', '') AS INT) AS ln_positive,
        COALESCE(p.completion, '') AS completion,
        p.reop
    FROM path_synoptics p
    WHERE LOWER(p.thyroid_procedure) LIKE '%lobectomy%'
       OR LOWER(p.thyroid_procedure) LIKE '%lobe%'
),
lobectomy_eligible AS (
    SELECT *,
        CASE
            WHEN central_compartment_dissection IS NOT NULL
                 OR LOWER(COALESCE(tumor_1_level_examined, '')) LIKE '%6%'
                 OR LOWER(COALESCE(other_ln_dissection, '')) LIKE '%central%'
                 OR LOWER(COALESCE(other_ln_dissection, '')) LIKE '%level 6%'
                 OR LOWER(COALESCE(tumor_1_ln_location, '')) LIKE '%perithyroidal%'
                 OR LOWER(COALESCE(tumor_1_ln_location, '')) LIKE '%pretracheal%'
                 OR LOWER(COALESCE(tumor_1_ln_location, '')) LIKE '%paratracheal%'
                 OR LOWER(COALESCE(tumor_1_ln_location, '')) LIKE '%delphian%'
                 OR LOWER(COALESCE(tumor_1_ln_location, '')) LIKE '%prelaryngeal%'
            THEN 1 ELSE 0
        END AS central_lnd_flag,
        CASE
            WHEN COALESCE(ln_positive, 0) > 0 THEN 'therapeutic'
            WHEN ln_examined IS NOT NULL AND ln_examined > 0 AND COALESCE(ln_positive, 0) = 0 THEN 'prophylactic'
            ELSE 'none'
        END AS lnd_intent
    FROM lobectomy_base
    WHERE LOWER(completion) NOT IN ('yes', 'y', 'completion')
)
SELECT
    e.*,
    CASE WHEN LOWER(CAST(r.recurrence_flag AS VARCHAR)) = 'true' THEN 1 ELSE 0 END AS recurrence,
    r.recurrence_risk_band,
    r.first_recurrence_date,
    CASE WHEN LOWER(CAST(r.braf_positive AS VARCHAR)) = 'true' THEN 1 ELSE 0 END AS braf_positive,
    CASE WHEN LOWER(CAST(r.ras_positive AS VARCHAR)) = 'true' THEN 1 ELSE 0 END AS ras_positive,
    -- Phase 3: refined complication flags (patient_refined_complication_flags_v2)
    COALESCE(pcf.confirmed_rln_injury, 0) AS rln_injury,
    COALESCE(pcf.refined_rln_injury, 0) AS rln_injury_refined,
    COALESCE(pcf.refined_hypocalcemia, 0) AS refined_hypocalcemia,
    COALESCE(pcf.confirmed_hypocalcemia, 0) AS confirmed_hypocalcemia,
    COALESCE(pcf.refined_hypoparathyroidism, 0) AS refined_hypoparathyroidism,
    COALESCE(pcf.refined_hematoma, 0) AS refined_hematoma,
    COALESCE(pcf.refined_seroma, 0) AS refined_seroma,
    COALESCE(pcf.refined_chyle_leak, 0) AS refined_chyle_leak,
    COALESCE(pcf.refined_wound_infection, 0) AS refined_wound_infection,
    COALESCE(pcf.has_confirmed_complication, 0) AS has_any_confirmed_complication
FROM lobectomy_eligible e
LEFT JOIN recurrence_risk_features_mv r ON e.research_id = r.research_id
LEFT JOIN patient_refined_complication_flags_v2 pcf ON e.research_id = pcf.research_id
"""


def load_cohort(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute(LOBECTOMY_COHORT_SQL).fetchdf()
    df.columns = pd.Index([c if not c.endswith("_1") else c[:-2] for c in df.columns])
    dupes = df.columns[df.columns.duplicated()].unique().tolist()
    if dupes:
        for col in dupes:
            mask = df.columns == col
            idxs = np.where(mask)[0]
            for i, idx in enumerate(idxs):
                if i > 0:
                    df.columns = df.columns[:idx].tolist() + [f"{col}__dup{i}"] + df.columns[idx+1:].tolist()
    return df


def add_nlp_complications(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> pd.DataFrame:
    """Phase 3: Complications now sourced from patient_refined_complication_flags_v2
    via the main cohort SQL JOIN. No separate NLP query needed."""
    return df


def table1_demographics(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Table 1: Demographics by central LND status."""
    rows = []

    def add_row(label, cln_vals, no_cln_vals, p_val):
        rows.append({
            "Variable": label,
            "CentralLND (n={})".format(len(cln_vals) if hasattr(cln_vals, '__len__') else "—"): "",
            "No_CentralLND (n={})".format(len(no_cln_vals) if hasattr(no_cln_vals, '__len__') else "—"): "",
            "p_value": p_val,
        })

    cln = df[df["central_lnd_flag"] == 1]
    no_cln = df[df["central_lnd_flag"] == 0]

    n_cln, n_no = len(cln), len(no_cln)
    header = {
        "Variable": "N",
        f"CentralLND (n={n_cln})": str(n_cln),
        f"No_CentralLND (n={n_no})": str(n_no),
        "p_value": "",
    }
    rows.append(header)

    col_cln = f"CentralLND (n={n_cln})"
    col_no = f"No_CentralLND (n={n_no})"

    age_c, age_n = cln["age"].dropna(), no_cln["age"].dropna()
    _, p_age = stats.mannwhitneyu(age_c, age_n, alternative="two-sided")
    rows.append({"Variable": "Age, mean (SD)", col_cln: f"{age_c.mean():.1f} ({age_c.std():.1f})",
                 col_no: f"{age_n.mean():.1f} ({age_n.std():.1f})", "p_value": f"{p_age:.4f}"})

    for sex_val in ["Female", "Male"]:
        n_c = (cln["gender"].str.lower() == sex_val.lower()).sum()
        n_n = (no_cln["gender"].str.lower() == sex_val.lower()).sum()
        rows.append({"Variable": f"  {sex_val}, n (%)", col_cln: f"{n_c} ({100*n_c/n_cln:.1f}%)",
                     col_no: f"{n_n} ({100*n_n/n_no:.1f}%)", "p_value": ""})
    ct_sex = pd.crosstab(df["gender"].str.lower().isin(["female"]), df["central_lnd_flag"])
    _, p_sex, _, _ = stats.chi2_contingency(ct_sex)
    rows[-2]["p_value"] = f"{p_sex:.4f}"

    size_c = cln["tumor_size_cm"].dropna()
    size_n = no_cln["tumor_size_cm"].dropna()
    if len(size_c) > 5 and len(size_n) > 5:
        _, p_sz = stats.mannwhitneyu(size_c, size_n, alternative="two-sided")
        rows.append({"Variable": "Tumor size (cm), median [IQR]",
                     col_cln: f"{size_c.median():.1f} [{size_c.quantile(.25):.1f}-{size_c.quantile(.75):.1f}]",
                     col_no: f"{size_n.median():.1f} [{size_n.quantile(.25):.1f}-{size_n.quantile(.75):.1f}]",
                     "p_value": f"{p_sz:.4f}"})

    ln_c = cln["ln_examined"].dropna()
    ln_n = no_cln["ln_examined"].dropna()
    if len(ln_c) > 5 and len(ln_n) > 5:
        _, p_ln = stats.mannwhitneyu(ln_c, ln_n, alternative="two-sided")
        rows.append({"Variable": "LN examined, median [IQR]",
                     col_cln: f"{ln_c.median():.0f} [{ln_c.quantile(.25):.0f}-{ln_c.quantile(.75):.0f}]",
                     col_no: f"{ln_n.median():.0f} [{ln_n.quantile(.25):.0f}-{ln_n.quantile(.75):.0f}]",
                     "p_value": f"{p_ln:.4f}"})

    lnp_c = cln["ln_positive"].dropna()
    lnp_n = no_cln["ln_positive"].dropna()
    if len(lnp_c) > 5 and len(lnp_n) > 5:
        _, p_lnp = stats.mannwhitneyu(lnp_c, lnp_n, alternative="two-sided")
        rows.append({"Variable": "LN positive, median [IQR]",
                     col_cln: f"{lnp_c.median():.0f} [{lnp_c.quantile(.25):.0f}-{lnp_c.quantile(.75):.0f}]",
                     col_no: f"{lnp_n.median():.0f} [{lnp_n.quantile(.25):.0f}-{lnp_n.quantile(.75):.0f}]",
                     "p_value": f"{p_lnp:.4f}"})

    for hist_val in df["histology"].dropna().value_counts().head(5).index:
        n_c = (cln["histology"] == hist_val).sum()
        n_n = (no_cln["histology"] == hist_val).sum()
        rows.append({"Variable": f"  {hist_val}, n (%)",
                     col_cln: f"{n_c} ({100*n_c/max(n_cln,1):.1f}%)",
                     col_no: f"{n_n} ({100*n_n/max(n_no,1):.1f}%)", "p_value": ""})

    return pd.DataFrame(rows)


def analyze_recurrence(df: pd.DataFrame) -> dict:
    """Primary outcome: recurrence by central LND status."""
    cln = df[df["central_lnd_flag"] == 1]
    no_cln = df[df["central_lnd_flag"] == 0]

    ct = pd.crosstab(df["central_lnd_flag"], df["recurrence"])
    chi2, p_chi2, dof, expected = stats.chi2_contingency(ct)

    a = cln["recurrence"].sum()
    b = len(cln) - a
    c = no_cln["recurrence"].sum()
    d = len(no_cln) - c

    or_val = (a * d) / max(b * c, 1)
    se_log_or = np.sqrt(1/max(a,1) + 1/max(b,1) + 1/max(c,1) + 1/max(d,1))
    or_ci_low = np.exp(np.log(max(or_val, 1e-10)) - 1.96 * se_log_or)
    or_ci_high = np.exp(np.log(max(or_val, 1e-10)) + 1.96 * se_log_or)

    results = {
        "cln_n": len(cln), "cln_recurrence": int(a),
        "cln_recurrence_rate": round(100 * a / max(len(cln), 1), 2),
        "no_cln_n": len(no_cln), "no_cln_recurrence": int(c),
        "no_cln_recurrence_rate": round(100 * c / max(len(no_cln), 1), 2),
        "chi2": round(chi2, 3), "p_value": round(p_chi2, 6),
        "odds_ratio": round(or_val, 3),
        "or_95ci_low": round(or_ci_low, 3),
        "or_95ci_high": round(or_ci_high, 3),
    }
    return results


def logistic_regression_recurrence(df: pd.DataFrame) -> pd.DataFrame:
    """Multivariable logistic regression for recurrence."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    model_df = df[["recurrence", "central_lnd_flag", "age", "tumor_size_cm",
                    "ln_positive", "braf_positive"]].dropna().copy()
    if len(model_df) < 50:
        return pd.DataFrame({"note": ["Insufficient complete cases for multivariable model"]})

    X_cols = ["central_lnd_flag", "age", "tumor_size_cm", "ln_positive", "braf_positive"]
    X = model_df[X_cols].values.astype(float)
    y = model_df["recurrence"].values.astype(float)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    import statsmodels.api as sm
    X_sm = sm.add_constant(X_scaled)
    try:
        logit = sm.Logit(y, X_sm).fit(disp=0, maxiter=100)
        coefs = logit.params[1:]
        se = logit.bse[1:]
        pvals = logit.pvalues[1:]

        results = []
        for i, col in enumerate(X_cols):
            or_val = np.exp(coefs[i])
            or_lo = np.exp(coefs[i] - 1.96 * se[i])
            or_hi = np.exp(coefs[i] + 1.96 * se[i])
            results.append({
                "Variable": col,
                "Coefficient": round(coefs[i], 4),
                "SE": round(se[i], 4),
                "OR": round(or_val, 3),
                "OR_95CI_low": round(or_lo, 3),
                "OR_95CI_high": round(or_hi, 3),
                "p_value": round(pvals[i], 6),
            })
        result_df = pd.DataFrame(results)
        result_df.attrs["n_obs"] = len(model_df)
        result_df.attrs["pseudo_r2"] = round(logit.prsquared, 4)
        result_df.attrs["aic"] = round(logit.aic, 1)
        return result_df
    except Exception as e:
        return pd.DataFrame({"note": [f"Model failed: {e}"]})


def analyze_rln_injury(df: pd.DataFrame) -> dict:
    """RLN injury by central LND status."""
    cln = df[df["central_lnd_flag"] == 1]
    no_cln = df[df["central_lnd_flag"] == 0]

    ct = pd.crosstab(df["central_lnd_flag"], df["rln_injury"])
    chi2, p_chi2, dof, expected = stats.chi2_contingency(ct)

    a = cln["rln_injury"].sum()
    b = len(cln) - a
    c = no_cln["rln_injury"].sum()
    d = len(no_cln) - c

    or_val = (a * d) / max(b * c, 1)
    se_log_or = np.sqrt(1/max(a,1) + 1/max(b,1) + 1/max(c,1) + 1/max(d,1))
    or_ci_low = np.exp(np.log(max(or_val, 1e-10)) - 1.96 * se_log_or)
    or_ci_high = np.exp(np.log(max(or_val, 1e-10)) + 1.96 * se_log_or)

    return {
        "cln_n": len(cln), "cln_rln": int(a),
        "cln_rln_rate": round(100 * a / max(len(cln), 1), 2),
        "no_cln_n": len(no_cln), "no_cln_rln": int(c),
        "no_cln_rln_rate": round(100 * c / max(len(no_cln), 1), 2),
        "chi2": round(chi2, 3), "p_value": round(p_chi2, 6),
        "odds_ratio": round(or_val, 3),
        "or_95ci_low": round(or_ci_low, 3),
        "or_95ci_high": round(or_ci_high, 3),
    }


def analyze_all_complications(df: pd.DataFrame) -> pd.DataFrame:
    """All complications by central LND status (Phase 3: refined flags)."""
    comps = ["rln_injury", "refined_hypocalcemia", "refined_hypoparathyroidism",
             "refined_hematoma", "refined_seroma", "refined_chyle_leak"]
    rows = []
    cln = df[df["central_lnd_flag"] == 1]
    no_cln = df[df["central_lnd_flag"] == 0]

    for comp in comps:
        if comp not in df.columns:
            continue
        n_c = int(cln[comp].sum())
        n_n = int(no_cln[comp].sum())
        pct_c = round(100 * n_c / max(len(cln), 1), 2)
        pct_n = round(100 * n_n / max(len(no_cln), 1), 2)

        ct = pd.crosstab(df["central_lnd_flag"], df[comp].astype(int))
        if ct.shape == (2, 2):
            chi2, p, _, _ = stats.chi2_contingency(ct)
            a, b = n_c, len(cln) - n_c
            c, d = n_n, len(no_cln) - n_n
            or_val = (a * d) / max(b * c, 1)
        else:
            p, or_val = np.nan, np.nan

        rows.append({
            "Complication": comp.replace("refined_", ""),
            "CentralLND_n": n_c, "CentralLND_pct": pct_c,
            "No_CentralLND_n": n_n, "No_CentralLND_pct": pct_n,
            "OR": round(or_val, 3) if not np.isnan(or_val) else "—",
            "p_value": round(p, 4) if not np.isnan(p) else "—",
        })
    return pd.DataFrame(rows)


def analyze_prophylactic_vs_therapeutic(df: pd.DataFrame) -> pd.DataFrame:
    """Subgroup: prophylactic vs therapeutic central LND outcomes."""
    cln = df[df["central_lnd_flag"] == 1].copy()
    rows = []
    for intent in ["prophylactic", "therapeutic"]:
        sub = cln[cln["lnd_intent"] == intent]
        rows.append({
            "LND_Intent": intent,
            "N": len(sub),
            "Recurrence_n": int(sub["recurrence"].sum()),
            "Recurrence_pct": round(100 * sub["recurrence"].mean(), 2) if len(sub) > 0 else 0,
            "RLN_n": int(sub["rln_injury"].sum()),
            "RLN_pct": round(100 * sub["rln_injury"].mean(), 2) if len(sub) > 0 else 0,
            "Mean_LN_examined": round(sub["ln_examined"].mean(), 1) if sub["ln_examined"].notna().sum() > 0 else "—",
            "Mean_LN_positive": round(sub["ln_positive"].mean(), 1) if sub["ln_positive"].notna().sum() > 0 else "—",
        })
    return pd.DataFrame(rows)


def plot_recurrence_by_cln(df: pd.DataFrame) -> None:
    """Bar chart: recurrence rates by central LND status."""
    cln = df[df["central_lnd_flag"] == 1]
    no_cln = df[df["central_lnd_flag"] == 0]

    groups = ["Central LND\n(n={})".format(len(cln)), "No Central LND\n(n={})".format(len(no_cln))]
    rates = [100 * cln["recurrence"].mean(), 100 * no_cln["recurrence"].mean()]
    colors = ["#D65F5F", "#5B8DB8"]

    fig, ax = plt.subplots(figsize=(6, 5))
    bars = ax.bar(groups, rates, color=colors, width=0.5, edgecolor="black", linewidth=0.8)
    for bar, rate in zip(bars, rates):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f"{rate:.1f}%", ha="center", va="bottom", fontweight="bold", fontsize=12)

    ax.set_ylabel("Recurrence Rate (%)", fontsize=12)
    ax.set_title("Recurrence by Central Lymph Node Dissection\nin Lobectomy Patients", fontsize=13, fontweight="bold")
    ax.set_ylim(0, max(rates) * 1.25)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(STUDY_DIR / "fig_recurrence_by_cln.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_complications_by_cln(df: pd.DataFrame) -> None:
    """Grouped bar chart: complication rates by central LND status (Phase 3: refined)."""
    comps = ["rln_injury", "refined_hypocalcemia", "refined_hypoparathyroidism",
             "refined_hematoma", "refined_seroma", "refined_chyle_leak"]
    labels = ["RLN Injury", "Hypocalcemia", "Hypoparathyroidism",
              "Hematoma", "Seroma", "Chyle Leak"]

    cln = df[df["central_lnd_flag"] == 1]
    no_cln = df[df["central_lnd_flag"] == 0]

    rates_cln = []
    rates_no = []
    valid_labels = []
    for comp, label in zip(comps, labels):
        if comp in df.columns:
            rates_cln.append(100 * cln[comp].mean())
            rates_no.append(100 * no_cln[comp].mean())
            valid_labels.append(label)

    x = np.arange(len(valid_labels))
    w = 0.35

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(x - w/2, rates_cln, w, label=f"Central LND (n={len(cln)})", color="#D65F5F", edgecolor="black", linewidth=0.5)
    ax.bar(x + w/2, rates_no, w, label=f"No Central LND (n={len(no_cln)})", color="#5B8DB8", edgecolor="black", linewidth=0.5)

    ax.set_ylabel("Rate (%)", fontsize=12)
    ax.set_title("Complication Rates by Central LND Status in Lobectomy", fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(valid_labels, rotation=25, ha="right")
    ax.legend(frameon=False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(STUDY_DIR / "fig_complications_by_cln.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_prophylactic_vs_therapeutic(df: pd.DataFrame) -> None:
    """Subgroup outcomes: prophylactic vs therapeutic central LND."""
    cln = df[df["central_lnd_flag"] == 1].copy()
    proph = cln[cln["lnd_intent"] == "prophylactic"]
    ther = cln[cln["lnd_intent"] == "therapeutic"]

    fig, axes = plt.subplots(1, 2, figsize=(10, 5))

    groups = [f"Prophylactic\n(n={len(proph)})", f"Therapeutic\n(n={len(ther)})"]
    rec_rates = [100 * proph["recurrence"].mean() if len(proph) > 0 else 0,
                 100 * ther["recurrence"].mean() if len(ther) > 0 else 0]
    rln_rates = [100 * proph["rln_injury"].mean() if len(proph) > 0 else 0,
                 100 * ther["rln_injury"].mean() if len(ther) > 0 else 0]

    bars1 = axes[0].bar(groups, rec_rates, color=["#7FB3D8", "#D65F5F"], width=0.5, edgecolor="black", linewidth=0.8)
    for bar, rate in zip(bars1, rec_rates):
        axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                     f"{rate:.1f}%", ha="center", fontweight="bold", fontsize=11)
    axes[0].set_ylabel("Rate (%)")
    axes[0].set_title("Recurrence Rate", fontweight="bold")
    axes[0].spines["top"].set_visible(False)
    axes[0].spines["right"].set_visible(False)

    bars2 = axes[1].bar(groups, rln_rates, color=["#7FB3D8", "#D65F5F"], width=0.5, edgecolor="black", linewidth=0.8)
    for bar, rate in zip(bars2, rln_rates):
        axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                     f"{rate:.1f}%", ha="center", fontweight="bold", fontsize=11)
    axes[1].set_ylabel("Rate (%)")
    axes[1].set_title("RLN Injury Rate", fontweight="bold")
    axes[1].spines["top"].set_visible(False)
    axes[1].spines["right"].set_visible(False)

    fig.suptitle("Prophylactic vs Therapeutic Central LND in Lobectomy", fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(STUDY_DIR / "fig_prophylactic_vs_therapeutic.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_forest_or(results: dict) -> None:
    """Forest plot of ORs for recurrence and RLN."""
    labels = ["Recurrence", "RLN Injury"]
    ors = [results["recurrence"]["odds_ratio"], results["rln"]["odds_ratio"]]
    ci_lo = [results["recurrence"]["or_95ci_low"], results["rln"]["or_95ci_low"]]
    ci_hi = [results["recurrence"]["or_95ci_high"], results["rln"]["or_95ci_high"]]
    pvals = [results["recurrence"]["p_value"], results["rln"]["p_value"]]

    fig, ax = plt.subplots(figsize=(7, 3.5))
    y_pos = [1, 0]
    for i, (label, or_v, lo, hi, p) in enumerate(zip(labels, ors, ci_lo, ci_hi, pvals)):
        ax.plot([lo, hi], [y_pos[i], y_pos[i]], color="black", linewidth=2)
        ax.plot(or_v, y_pos[i], "D", color="#D65F5F", markersize=10, zorder=5)
        sig = "*" if p < 0.05 else ""
        ax.text(max(hi, or_v) + 0.05, y_pos[i],
                f"OR={or_v:.2f} [{lo:.2f}-{hi:.2f}] p={p:.4f}{sig}",
                va="center", fontsize=10)

    ax.axvline(1.0, color="gray", linestyle="--", linewidth=0.8)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=12)
    ax.set_xlabel("Odds Ratio (Central LND vs No Central LND)", fontsize=11)
    ax.set_title("Central LND Outcomes in Lobectomy — Odds Ratios", fontsize=12, fontweight="bold")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_xlim(0, max(ci_hi) * 1.8)
    plt.tight_layout()
    fig.savefig(STUDY_DIR / "fig_forest_or.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Hypothesis 1: Central LND in Lobectomy")
    parser.add_argument("--md", action="store_true", default=True, help="Use MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only")
    args = parser.parse_args()

    use_md = not args.local
    if args.dry_run:
        print(LOBECTOMY_COHORT_SQL)
        return

    print("=" * 70)
    print("HYPOTHESIS 1: Central Lymph Node Dissection in Lobectomy")
    print("=" * 70)

    con = get_connection(use_md)
    print(f"\n[1/7] Loading lobectomy cohort ({'MotherDuck' if use_md else 'local'})...")
    df = load_cohort(con)
    print(f"  → {len(df)} lobectomy patients (completion thyroidectomies excluded)")
    print(f"  → Central LND: {(df['central_lnd_flag']==1).sum()}, No Central LND: {(df['central_lnd_flag']==0).sum()}")

    print("\n[2/7] Complication flags (Phase 3: patient_refined_complication_flags_v2)...")
    df = add_nlp_complications(con, df)
    rln_n = int(df["rln_injury"].sum())
    print(f"  → RLN confirmed: {rln_n}, refined: {int(df['rln_injury_refined'].sum())}")
    for c in ["refined_hypocalcemia", "refined_hematoma", "refined_seroma", "refined_chyle_leak"]:
        if c in df.columns:
            print(f"  → {c}: {int(df[c].sum())}")

    print("\n[3/7] Table 1: Demographics...")
    t1 = table1_demographics(df)
    t1.to_csv(STUDY_DIR / "table1_demographics.csv", index=False)
    print(t1.to_string(index=False))

    print("\n[4/7] Primary outcome: Recurrence by central LND...")
    rec_results = analyze_recurrence(df)
    print(f"  Central LND: {rec_results['cln_recurrence']}/{rec_results['cln_n']} ({rec_results['cln_recurrence_rate']}%)")
    print(f"  No Central LND: {rec_results['no_cln_recurrence']}/{rec_results['no_cln_n']} ({rec_results['no_cln_recurrence_rate']}%)")
    print(f"  OR = {rec_results['odds_ratio']} (95% CI {rec_results['or_95ci_low']}–{rec_results['or_95ci_high']})")
    print(f"  p = {rec_results['p_value']}")

    print("\n  Multivariable logistic regression (recurrence)...")
    lr_df = logistic_regression_recurrence(df)
    lr_df.to_csv(STUDY_DIR / "logistic_regression_recurrence.csv", index=False)
    print(lr_df.to_string(index=False))
    if hasattr(lr_df, 'attrs') and 'n_obs' in lr_df.attrs:
        print(f"  N={lr_df.attrs['n_obs']}, pseudo-R²={lr_df.attrs.get('pseudo_r2','—')}, AIC={lr_df.attrs.get('aic','—')}")

    print("\n[5/7] RLN Injury by central LND...")
    rln_results = analyze_rln_injury(df)
    print(f"  Central LND: {rln_results['cln_rln']}/{rln_results['cln_n']} ({rln_results['cln_rln_rate']}%)")
    print(f"  No Central LND: {rln_results['no_cln_rln']}/{rln_results['no_cln_n']} ({rln_results['no_cln_rln_rate']}%)")
    print(f"  OR = {rln_results['odds_ratio']} (95% CI {rln_results['or_95ci_low']}–{rln_results['or_95ci_high']})")
    print(f"  p = {rln_results['p_value']}")

    print("\n  All complications by central LND status:")
    comp_df = analyze_all_complications(df)
    comp_df.to_csv(STUDY_DIR / "complications_by_cln.csv", index=False)
    print(comp_df.to_string(index=False))

    print("\n[6/7] Prophylactic vs Therapeutic central LND...")
    pt_df = analyze_prophylactic_vs_therapeutic(df)
    pt_df.to_csv(STUDY_DIR / "prophylactic_vs_therapeutic.csv", index=False)
    print(pt_df.to_string(index=False))

    print("\n[7/7] Generating figures...")
    plot_recurrence_by_cln(df)
    plot_complications_by_cln(df)
    plot_prophylactic_vs_therapeutic(df)
    all_results = {"recurrence": rec_results, "rln": rln_results}
    plot_forest_or(all_results)
    print("  → 4 figures saved to", STUDY_DIR)

    summary = {
        "hypothesis": "Central LND predictive for recurrence in lobectomy; CLN-complication/RLN association",
        "complication_source": "patient_refined_complication_flags_v2 (Phase 3 refined, not raw NLP)",
        "cohort": {
            "total_lobectomy": len(df),
            "central_lnd": int((df["central_lnd_flag"] == 1).sum()),
            "no_central_lnd": int((df["central_lnd_flag"] == 0).sum()),
            "completion_excluded": "yes",
        },
        "recurrence_analysis": rec_results,
        "rln_injury_analysis": rln_results,
        "logistic_regression": {
            "n_obs": lr_df.attrs.get("n_obs", "—") if hasattr(lr_df, 'attrs') else "—",
            "pseudo_r2": lr_df.attrs.get("pseudo_r2", "—") if hasattr(lr_df, 'attrs') else "—",
        },
        "data_source": "MotherDuck" if use_md else "local DuckDB",
        "generated_at": datetime.now().isoformat(),
        "random_seed": 42,
    }
    with open(STUDY_DIR / "analysis_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    df.to_csv(STUDY_DIR / "lobectomy_cohort.csv", index=False)

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE — All outputs saved to:")
    print(f"  {STUDY_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    main()
