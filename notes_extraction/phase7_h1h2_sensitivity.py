#!/usr/bin/env python3
"""
Phase 7 Sensitivity Analyses — H1 (CLN/Lobectomy) and H2 (Goiter/SDOH)
=======================================================================
Tests whether Phase 7 variables (bethesda_final, braf_positive_v7,
molecular_risk_category, preop_tirads_score, preop_imaging_size_cm)
materially change the primary effect estimates from Hypotheses 1 and 2.

Usage:
    .venv/bin/python notes_extraction/phase7_h1h2_sensitivity.py --md
    .venv/bin/python notes_extraction/phase7_h1h2_sensitivity.py --local
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
import statsmodels.api as sm

STUDY_DIR = Path(__file__).resolve().parent.parent / "studies" / "phase7_sensitivity"
STUDY_DIR.mkdir(parents=True, exist_ok=True)

np.random.seed(42)

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


def get_connection(use_md: bool) -> duckdb.DuckDBPyConnection:
    if use_md:
        try:
            import toml
            token = toml.load(".streamlit/secrets.toml")["MOTHERDUCK_TOKEN"]
        except Exception:
            token = os.getenv("MOTHERDUCK_TOKEN", "")
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect("thyroid_master_local.duckdb")


# ---------------------------------------------------------------------------
# H1: CLN / Lobectomy cohort with Phase 7 molecular covariates
# ---------------------------------------------------------------------------
H1_SQL = """
WITH lobectomy_base AS (
    SELECT
        p.research_id,
        p.age,
        TRY_CAST(REPLACE(p.tumor_1_size_greatest_dimension_cm, ';', '') AS DOUBLE) AS tumor_size_cm,
        TRY_CAST(REPLACE(REPLACE(p.tumor_1_ln_examined, ';', ''), 'x', '') AS INT) AS ln_examined,
        TRY_CAST(REPLACE(REPLACE(p.tumor_1_ln_involved, ';', ''), 'x', '') AS INT) AS ln_positive,
        p.central_compartment_dissection,
        p.tumor_1_level_examined,
        p.other_ln_dissection,
        p.tumor_1_ln_location,
        COALESCE(p.completion, '') AS completion
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
        END AS central_lnd_flag
    FROM lobectomy_base
    WHERE LOWER(completion) NOT IN ('yes', 'y', 'completion')
),
recurrence_dedup AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        BOOL_OR(LOWER(CAST(recurrence_flag AS VARCHAR)) = 'true') AS recurrence_flag
    FROM recurrence_risk_features_mv
    GROUP BY CAST(research_id AS INTEGER)
)
SELECT
    e.research_id,
    e.age,
    e.tumor_size_cm,
    e.ln_examined,
    e.ln_positive,
    e.central_lnd_flag,
    CASE WHEN r.recurrence_flag THEN 1 ELSE 0 END AS recurrence,
    -- Phase 7 molecular
    v6.bethesda_final,
    CASE WHEN LOWER(CAST(v6.braf_positive_v7 AS VARCHAR)) = 'true' THEN 1 ELSE 0 END AS braf_v7,
    v6.molecular_risk_category
FROM lobectomy_eligible e
LEFT JOIN recurrence_dedup r ON e.research_id = r.research_id
LEFT JOIN patient_refined_master_clinical_v6 v6 ON e.research_id = v6.research_id
"""


def load_h1(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(H1_SQL).fetchdf()


def _run_logit(y: np.ndarray, X: np.ndarray, col_names: list[str]) -> pd.DataFrame | None:
    """Fit a statsmodels Logit and return an OR summary table."""
    X_c = sm.add_constant(X)
    try:
        fit = sm.Logit(y, X_c).fit(disp=0, maxiter=200)
    except Exception as exc:
        print(f"  [warn] model failed: {exc}")
        return None
    rows = []
    for i, col in enumerate(col_names):
        coef = fit.params[i + 1]
        se = fit.bse[i + 1]
        p = fit.pvalues[i + 1]
        or_val = np.exp(coef)
        rows.append({
            "Variable": col,
            "Coef": round(float(coef), 4),
            "SE": round(float(se), 4),
            "OR": round(float(or_val), 3),
            "OR_95CI_low": round(float(np.exp(coef - 1.96 * se)), 3),
            "OR_95CI_high": round(float(np.exp(coef + 1.96 * se)), 3),
            "p_value": round(float(p), 6),
        })
    result = pd.DataFrame(rows)
    result.attrs["n"] = int(len(y))
    result.attrs["pseudo_r2"] = round(float(fit.prsquared), 4)
    result.attrs["aic"] = round(float(fit.aic), 1)
    return result


def h1_sensitivity(df: pd.DataFrame) -> dict:
    """Run H1 crude, base-adjusted, and Phase-7-adjusted models."""
    from sklearn.preprocessing import StandardScaler

    results: dict = {"cohort_n": len(df)}

    cln = df["central_lnd_flag"] == 1
    rec = df["recurrence"] == 1
    a = (cln & rec).sum()
    b = (cln & ~rec).sum()
    c = (~cln & rec).sum()
    d = (~cln & ~rec).sum()
    crude_or = (a * d) / max(b * c, 1)
    results["crude_or"] = round(float(crude_or), 3)
    results["cln_n"] = int(cln.sum())
    results["no_cln_n"] = int((~cln).sum())
    results["cln_recurrence_rate"] = round(100 * a / max(int(cln.sum()), 1), 2)
    results["no_cln_recurrence_rate"] = round(100 * c / max(int((~cln).sum()), 1), 2)

    scaler = StandardScaler()

    # Model 1: base (CLN + age + tumor_size + ln_positive)
    base_cols = ["central_lnd_flag", "age", "tumor_size_cm", "ln_positive"]
    m1_df = df[["recurrence"] + base_cols].dropna()
    if len(m1_df) >= 50:
        X1 = scaler.fit_transform(m1_df[base_cols].values.astype(float))
        y1 = m1_df["recurrence"].values.astype(float)
        r1 = _run_logit(y1, X1, base_cols)
        if r1 is not None:
            results["model1_base"] = r1.to_dict("records")
            results["model1_n"] = r1.attrs["n"]
            results["model1_aic"] = r1.attrs["aic"]
            r1.to_csv(STUDY_DIR / "h1_model1_base.csv", index=False)

    # Model 2: base + Phase 7 (bethesda + braf_v7 + molecular_risk_category)
    df2 = df.copy()
    df2["mol_high_risk"] = (df2["molecular_risk_category"] == "high_risk_molecular").astype(int)
    df2["mol_tested_neg"] = (df2["molecular_risk_category"] == "tested_negative").astype(int)
    phase7_cols = base_cols + ["bethesda_final", "braf_v7", "mol_high_risk", "mol_tested_neg"]
    m2_df = df2[["recurrence"] + phase7_cols].dropna()
    if len(m2_df) >= 50:
        X2 = scaler.fit_transform(m2_df[phase7_cols].values.astype(float))
        y2 = m2_df["recurrence"].values.astype(float)
        r2 = _run_logit(y2, X2, phase7_cols)
        if r2 is not None:
            results["model2_phase7"] = r2.to_dict("records")
            results["model2_n"] = r2.attrs["n"]
            results["model2_aic"] = r2.attrs["aic"]
            r2.to_csv(STUDY_DIR / "h1_model2_phase7.csv", index=False)

    return results


# ---------------------------------------------------------------------------
# H2: Goiter / SDOH cohort with Phase 7 preop imaging covariates
# ---------------------------------------------------------------------------
H2_SQL = f"""
SELECT
    p.research_id,
    p.age,
    CASE WHEN LOWER(p.gender) = 'male' THEN 'Male' ELSE 'Female' END AS sex,
    {RACE_NORM} AS race_group,
    CASE
        WHEN LOWER(COALESCE(p.substernal_multinodular_goiter, '')) = 'x' THEN 'Substernal'
        ELSE 'Cervical'
    END AS goiter_type,
    TRY_CAST(REPLACE(p.weight_total, ';', '') AS DOUBLE) AS specimen_weight_g,
    TRY_CAST(REPLACE(p.tumor_1_size_greatest_dimension_cm, ';', '') AS DOUBLE) AS dominant_nodule_cm,
    CASE
        WHEN LOWER(p.thyroid_procedure) LIKE '%total%' THEN 'Total Thyroidectomy'
        WHEN LOWER(p.thyroid_procedure) LIKE '%lobectomy%'
             OR LOWER(p.thyroid_procedure) LIKE '%lobe%' THEN 'Lobectomy'
        ELSE 'Other'
    END AS surgery_extent,
    -- Phase 7 preop imaging
    v6.preop_tirads_score,
    v6.preop_imaging_size_cm,
    v6.preop_tirads_category
FROM path_synoptics p
LEFT JOIN patient_refined_master_clinical_v6 v6 ON p.research_id = v6.research_id
WHERE LOWER(COALESCE(p.multinodular_goiter, '')) = 'x'
   OR LOWER(COALESCE(p.substernal_multinodular_goiter, '')) = 'x'
"""


def load_h2(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(H2_SQL).fetchdf()


def h2_sensitivity(df: pd.DataFrame) -> dict:
    """Run H2 race-disparity models with and without preop imaging adjustment."""
    from sklearn.preprocessing import StandardScaler

    results: dict = {"cohort_n": len(df)}
    scaler = StandardScaler()

    df = df.copy()
    df["log_weight"] = np.log1p(df["specimen_weight_g"])
    df["female"] = (df["sex"] == "Female").astype(int)
    df["black"] = (df["race_group"] == "Black").astype(int)
    df["white"] = (df["race_group"] == "White").astype(int)
    df["substernal"] = (df["goiter_type"] == "Substernal").astype(int)

    bw = df[df["race_group"].isin(["Black", "White"])].copy()
    results["black_n"] = int(bw["black"].sum())
    results["white_n"] = int(bw["white"].sum())
    wt_b = bw.loc[bw["black"] == 1, "specimen_weight_g"].dropna()
    wt_w = bw.loc[bw["black"] == 0, "specimen_weight_g"].dropna()
    results["black_wt_median"] = round(float(wt_b.median()), 1) if len(wt_b) else None
    results["white_wt_median"] = round(float(wt_w.median()), 1) if len(wt_w) else None

    # Model 1: log(weight) ~ black + age + female + substernal (base)
    base_cols = ["black", "age", "female", "substernal"]
    m1_df = bw[["log_weight"] + base_cols].dropna()
    if len(m1_df) >= 50:
        X1 = sm.add_constant(m1_df[base_cols].values.astype(float))
        y1 = m1_df["log_weight"].values.astype(float)
        try:
            ols1 = sm.OLS(y1, X1).fit()
            rows1 = []
            for i, col in enumerate(base_cols):
                rows1.append({
                    "Variable": col,
                    "Coef": round(float(ols1.params[i + 1]), 4),
                    "SE": round(float(ols1.bse[i + 1]), 4),
                    "t": round(float(ols1.tvalues[i + 1]), 3),
                    "p_value": round(float(ols1.pvalues[i + 1]), 6),
                    "95CI_low": round(float(ols1.conf_int()[i + 1, 0]), 4),
                    "95CI_high": round(float(ols1.conf_int()[i + 1, 1]), 4),
                })
            r1 = pd.DataFrame(rows1)
            r1.attrs["n"] = int(len(m1_df))
            r1.attrs["r2"] = round(float(ols1.rsquared), 4)
            results["model1_base"] = r1.to_dict("records")
            results["model1_n"] = r1.attrs["n"]
            results["model1_r2"] = r1.attrs["r2"]
            r1.to_csv(STUDY_DIR / "h2_model1_base.csv", index=False)
        except Exception as exc:
            print(f"  [warn] H2 model 1 failed: {exc}")

    # Model 2: base + preop_tirads_score + preop_imaging_size_cm
    phase7_cols = base_cols + ["preop_tirads_score", "preop_imaging_size_cm"]
    m2_df = bw[["log_weight"] + phase7_cols].dropna()
    if len(m2_df) >= 50:
        X2 = sm.add_constant(m2_df[phase7_cols].values.astype(float))
        y2 = m2_df["log_weight"].values.astype(float)
        try:
            ols2 = sm.OLS(y2, X2).fit()
            rows2 = []
            for i, col in enumerate(phase7_cols):
                rows2.append({
                    "Variable": col,
                    "Coef": round(float(ols2.params[i + 1]), 4),
                    "SE": round(float(ols2.bse[i + 1]), 4),
                    "t": round(float(ols2.tvalues[i + 1]), 3),
                    "p_value": round(float(ols2.pvalues[i + 1]), 6),
                    "95CI_low": round(float(ols2.conf_int()[i + 1, 0]), 4),
                    "95CI_high": round(float(ols2.conf_int()[i + 1, 1]), 4),
                })
            r2 = pd.DataFrame(rows2)
            r2.attrs["n"] = int(len(m2_df))
            r2.attrs["r2"] = round(float(ols2.rsquared), 4)
            results["model2_phase7"] = r2.to_dict("records")
            results["model2_n"] = r2.attrs["n"]
            results["model2_r2"] = r2.attrs["r2"]
            r2.to_csv(STUDY_DIR / "h2_model2_phase7.csv", index=False)
        except Exception as exc:
            print(f"  [warn] H2 model 2 failed: {exc}")

    return results


# ---------------------------------------------------------------------------
# Pretty-print summary
# ---------------------------------------------------------------------------
def _or_str(row: dict) -> str:
    return f"{row['OR']:.3f} ({row['OR_95CI_low']:.3f}–{row['OR_95CI_high']:.3f})"


def _coef_str(row: dict) -> str:
    return f"{row['Coef']:.4f} ({row['95CI_low']:.4f}–{row['95CI_high']:.4f})"


def print_summary(h1: dict, h2: dict) -> None:
    sep = "=" * 78
    print(f"\n{sep}")
    print("  PHASE 7 SENSITIVITY ANALYSIS — SUMMARY")
    print(sep)

    # ---- H1 ----
    print("\n  H1: Central LND → Recurrence in Lobectomy")
    print(f"  Cohort: {h1['cohort_n']} lobectomies | CLN={h1['cln_n']} | No-CLN={h1['no_cln_n']}")
    print(f"  Recurrence rate: CLN {h1['cln_recurrence_rate']}% vs No-CLN {h1['no_cln_recurrence_rate']}%")
    print(f"  Crude OR = {h1['crude_or']}")
    print()

    header = f"  {'Model':<35} {'CLN OR (95% CI)':<28} {'p':<10} {'N':<8} {'AIC':<10}"
    print(header)
    print("  " + "-" * 76)

    if "model1_base" in h1:
        cln_row = next((r for r in h1["model1_base"] if r["Variable"] == "central_lnd_flag"), None)
        if cln_row:
            print(f"  {'Base (age+size+LN)':<35} {_or_str(cln_row):<28} {cln_row['p_value']:<10.4f} {h1['model1_n']:<8} {h1['model1_aic']:<10}")

    if "model2_phase7" in h1:
        cln_row = next((r for r in h1["model2_phase7"] if r["Variable"] == "central_lnd_flag"), None)
        if cln_row:
            print(f"  {'+ Bethesda+BRAF_v7+MolRisk':<35} {_or_str(cln_row):<28} {cln_row['p_value']:<10.4f} {h1['model2_n']:<8} {h1['model2_aic']:<10}")

    if "model1_base" in h1 and "model2_phase7" in h1:
        or1 = next((r["OR"] for r in h1["model1_base"] if r["Variable"] == "central_lnd_flag"), None)
        or2 = next((r["OR"] for r in h1["model2_phase7"] if r["Variable"] == "central_lnd_flag"), None)
        if or1 and or2:
            pct_change = 100 * (or2 - or1) / or1
            direction = "attenuated" if pct_change < 0 else "amplified"
            print(f"\n  Phase 7 adjustment: CLN OR changed {pct_change:+.1f}% ({direction})")

    if "model2_phase7" in h1:
        print("\n  Phase 7 covariate effects:")
        for row in h1["model2_phase7"]:
            if row["Variable"] not in ("central_lnd_flag", "age", "tumor_size_cm", "ln_positive"):
                sig = "*" if row["p_value"] < 0.05 else ""
                print(f"    {row['Variable']:<25} OR={row['OR']:.3f} ({row['OR_95CI_low']:.3f}–{row['OR_95CI_high']:.3f})  p={row['p_value']:.4f} {sig}")

    # ---- H2 ----
    print(f"\n{sep}")
    print("  H2: Race Disparity in Goiter Specimen Weight")
    print(f"  Cohort: {h2['cohort_n']} goiter patients")
    print(f"  Black (n={h2['black_n']}) median weight: {h2.get('black_wt_median','—')}g")
    print(f"  White (n={h2['white_n']}) median weight: {h2.get('white_wt_median','—')}g")
    print()

    header2 = f"  {'Model':<40} {'Black coef (95% CI)':<30} {'p':<10} {'N':<8} {'R²':<8}"
    print(header2)
    print("  " + "-" * 76)

    if "model1_base" in h2:
        black_row = next((r for r in h2["model1_base"] if r["Variable"] == "black"), None)
        if black_row:
            print(f"  {'Base (age+sex+substernal)':<40} {_coef_str(black_row):<30} {black_row['p_value']:<10.4f} {h2['model1_n']:<8} {h2['model1_r2']:<8}")

    if "model2_phase7" in h2:
        black_row = next((r for r in h2["model2_phase7"] if r["Variable"] == "black"), None)
        if black_row:
            print(f"  {'+ TI-RADS + imaging_size':<40} {_coef_str(black_row):<30} {black_row['p_value']:<10.4f} {h2['model2_n']:<8} {h2['model2_r2']:<8}")

    if "model1_base" in h2 and "model2_phase7" in h2:
        c1 = next((r["Coef"] for r in h2["model1_base"] if r["Variable"] == "black"), None)
        c2 = next((r["Coef"] for r in h2["model2_phase7"] if r["Variable"] == "black"), None)
        if c1 and c2:
            pct = 100 * (c2 - c1) / abs(c1) if c1 != 0 else 0
            direction = "attenuated" if abs(c2) < abs(c1) else "amplified"
            print(f"\n  Phase 7 adjustment: Black coef changed {pct:+.1f}% ({direction})")

    if "model2_phase7" in h2:
        print("\n  Phase 7 covariate effects:")
        for row in h2["model2_phase7"]:
            if row["Variable"] in ("preop_tirads_score", "preop_imaging_size_cm"):
                sig = "*" if row["p_value"] < 0.05 else ""
                print(f"    {row['Variable']:<30} coef={row['Coef']:.4f} ({row['95CI_low']:.4f}–{row['95CI_high']:.4f})  p={row['p_value']:.4f} {sig}")

    print(f"\n{sep}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 7 H1/H2 sensitivity analyses")
    parser.add_argument("--md", action="store_true", default=True,
                        help="Use MotherDuck (default)")
    parser.add_argument("--local", action="store_true",
                        help="Use local DuckDB instead of MotherDuck")
    args = parser.parse_args()
    use_md = not args.local

    print(f"[Phase 7 Sensitivity] connecting to {'MotherDuck' if use_md else 'local DuckDB'}...")
    con = get_connection(use_md)

    # ---- H1 ----
    print("\n[1/4] Loading H1 lobectomy cohort with Phase 7 molecular variables...")
    h1_df = load_h1(con)
    print(f"  Loaded {len(h1_df)} lobectomy rows")
    print(f"  CLN=1: {(h1_df['central_lnd_flag']==1).sum()}, CLN=0: {(h1_df['central_lnd_flag']==0).sum()}")
    print(f"  Phase 7 coverage: bethesda_final {h1_df['bethesda_final'].notna().sum()}, "
          f"braf_v7 {(h1_df['braf_v7']==1).sum()}, "
          f"molecular_risk_category {h1_df['molecular_risk_category'].notna().sum()}")

    print("\n[2/4] Running H1 sensitivity models...")
    h1_results = h1_sensitivity(h1_df)

    # ---- H2 ----
    print("\n[3/4] Loading H2 goiter cohort with Phase 7 preop imaging...")
    h2_df = load_h2(con)
    print(f"  Loaded {len(h2_df)} goiter rows")
    print(f"  Phase 7 coverage: preop_tirads {h2_df['preop_tirads_score'].notna().sum()}, "
          f"preop_imaging_size {h2_df['preop_imaging_size_cm'].notna().sum()}")

    print("\n[4/4] Running H2 sensitivity models...")
    h2_results = h2_sensitivity(h2_df)

    con.close()

    # ---- Summary ----
    print_summary(h1_results, h2_results)

    summary = {
        "h1": h1_results,
        "h2": h2_results,
        "data_source": "MotherDuck" if use_md else "local DuckDB",
        "generated_at": datetime.now().isoformat(),
        "random_seed": 42,
    }
    with open(STUDY_DIR / "phase7_sensitivity_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"  Results saved to {STUDY_DIR}/")


if __name__ == "__main__":
    main()
