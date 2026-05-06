"""
Prompt 4 — Cox Proportional Hazards Model on Recurrence Cohort
DFL: DFL-20260506-086
Migration log: mig_086_cox_recurrence_v1 (logged to bqml_eval_log_v1)

Uses: cohort_m044_ajcc_ete_v1 from pub_workspace
Outcome: any_recurrence_flag (event), followup_years * 365.25 (duration in days)
Model: CoxPHFitter (lifelines)
Competitor: recurrence_5y_baseline_v1 (BQML logistic, AUC 0.738)
"""

import json
import os
import warnings
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from lifelines import CoxPHFitter
from lifelines.utils import concordance_index

warnings.filterwarnings("ignore")

PROJECT = "thyroid-canonical-pub-2026"
CREDS_PATH = "/Users/loganglosser/Desktop/Thyroid Motherduck To GC migration/_creds/thyroid-pub-loader-key.json"
STUDY_DIR = Path("/Users/loganglosser/THYROID_2026/studies/cox_recurrence_v1")
STUDY_DIR.mkdir(parents=True, exist_ok=True)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDS_PATH


def pull_cohort(client: bigquery.Client) -> pd.DataFrame:
    query = """
    SELECT
        research_id,
        age_at_surgery,
        sex,
        histology_final,
        ata_risk_category,
        ajcc8_stage_group,
        ete_grade_final,
        ln_positive_flag,
        tumor_size_cm,
        CAST(any_recurrence_flag AS INT64) AS recurrence_event,
        CAST(structural_recurrence_flag AS INT64) AS structural_recurrence_event,
        followup_years,
        followup_years * 365.25 AS followup_days,
        death_occurred
    FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_m044_ajcc_ete_v1`
    WHERE followup_years IS NOT NULL
      AND followup_years > 0
    """
    df = client.query(query).to_dataframe()
    print(f"Pulled {len(df):,} rows, {df['recurrence_event'].sum():,} recurrence events")
    return df


def prepare_for_cox(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Binary encode sex
    df["sex_male"] = (df["sex"].str.lower() == "male").astype(int)

    # ETE: gross vs not
    df["gross_ete"] = df["ete_grade_final"].str.lower().str.contains("gross", na=False).astype(int)

    # Stage: III/IV vs I/II
    df["stage_iii_iv"] = df["ajcc8_stage_group"].isin(["III", "IVA", "IVB", "IVC"]).astype(int)

    # ATA risk: high vs not
    df["ata_high_risk"] = (df["ata_risk_category"].str.lower() == "high").astype(int)
    df["ata_intermediate_risk"] = (df["ata_risk_category"].str.lower() == "intermediate").astype(int)

    # Histology: PTC vs other
    df["histology_ptc"] = df["histology_final"].str.upper().str.startswith("PTC", na=False).astype(int)

    # LN positive flag
    df["ln_positive"] = pd.to_numeric(df["ln_positive_flag"], errors="coerce").fillna(0).astype(int)

    # Drop rows with missing duration or key covariates
    features = [
        "followup_days", "recurrence_event",
        "age_at_surgery", "sex_male", "gross_ete", "stage_iii_iv",
        "ata_high_risk", "ata_intermediate_risk", "histology_ptc",
        "ln_positive", "tumor_size_cm",
    ]
    before = len(df)
    df = df[features].dropna()
    print(f"After NA drop: {len(df):,} rows (dropped {before - len(df):,})")
    return df


def fit_cox(df: pd.DataFrame) -> dict:
    cph = CoxPHFitter(penalizer=0.1)
    duration_col = "followup_days"
    event_col = "recurrence_event"

    cph.fit(df, duration_col=duration_col, event_col=event_col, show_progress=False)
    cph.print_summary()

    c_idx = concordance_index(df[duration_col], -cph.predict_partial_hazard(df), df[event_col])
    print(f"\nC-index (harrell): {c_idx:.4f}")

    # Compute Brier scores at 1y/3y/5y
    from lifelines import KaplanMeierFitter

    kmf = KaplanMeierFitter()
    kmf.fit(df[duration_col], event_observed=df[event_col])

    # IBS approximation: Brier at each time point
    brier_results = {}
    for years, label in [(365, "1y"), (3 * 365, "3y"), (5 * 365, "5y")]:
        mask_alive = df[duration_col] >= years
        if mask_alive.sum() < 10:
            brier_results[label] = None
            continue
        sub = df[mask_alive].copy()
        pred_surv = cph.predict_survival_function(sub, times=[years]).iloc[0]
        actual = (sub[event_col] == 1).astype(int)
        brier_t = ((pred_surv.values - (1 - actual.values)) ** 2).mean()
        brier_results[label] = round(float(brier_t), 4)

    # Coefficients table
    coeff_df = cph.summary[["coef", "exp(coef)", "p", "coef lower 95%", "coef upper 95%"]].copy()
    coeff_df.columns = ["coef", "HR", "p_value", "HR_lower_95", "HR_upper_95"]
    coeff_df = coeff_df.round(4).reset_index().rename(columns={"covariate": "feature"})

    return {
        "c_index": round(c_idx, 4),
        "brier_scores": brier_results,
        "coeff_table": coeff_df,
        "n_train": int(len(df)),
        "n_events": int(df[event_col].sum()),
        "concordance_pvalue": round(cph.log_likelihood_ratio_test().p_value, 6),
        "log_likelihood": round(float(cph.log_likelihood_), 4),
    }


def main():
    client = bigquery.Client(project=PROJECT)

    print("=== Pulling cohort from BQ ===")
    raw_df = pull_cohort(client)

    print("\n=== Preparing features ===")
    df = prepare_for_cox(raw_df)

    print("\n=== Fitting Cox PH ===")
    results = fit_cox(df)

    print(f"\nC-index: {results['c_index']}")
    print(f"Brier scores: {results['brier_scores']}")
    print(f"LLR p-value: {results['concordance_pvalue']}")

    # Save coefficient table
    coeff_path = STUDY_DIR / "cox_recurrence_v1_coefficients.csv"
    results["coeff_table"].to_csv(coeff_path, index=False)
    print(f"\nSaved coefficients to {coeff_path}")

    # Persist results JSON (aggregate only — no per-patient data)
    results_out = {
        "model_id": "cox_recurrence_v1",
        "model_type": "CoxPHFitter",
        "library": "lifelines 0.30.3",
        "cohort": "cohort_m044_ajcc_ete_v1 (BQ pub_workspace)",
        "n_train": results["n_train"],
        "n_events": results["n_events"],
        "c_index": results["c_index"],
        "brier_1y": results["brier_scores"].get("1y"),
        "brier_3y": results["brier_scores"].get("3y"),
        "brier_5y": results["brier_scores"].get("5y"),
        "llr_pvalue": results["concordance_pvalue"],
        "log_likelihood": results["log_likelihood"],
        "dfl_id": "DFL-20260506-086",
        "run_ts": datetime.now(timezone.utc).isoformat(),
    }
    results_json = STUDY_DIR / "cox_recurrence_v1_results.json"
    with open(results_json, "w") as f:
        json.dump(results_out, f, indent=2)
    print(f"Saved results JSON to {results_json}")

    # Insert into bqml_eval_log_v1
    print("\n=== Logging to bqml_eval_log_v1 ===")
    # Check schema first — no 'c_index' column; use notes field
    insert_sql = f"""
    INSERT INTO `thyroid-canonical-pub-2026.pub_workspace.bqml_eval_log_v1`
      (model_id, model_type, trained_at, training_rows, eval_rows, auc,
       accuracy, f1_score, feature_count, notes)
    VALUES
      ('cox_recurrence_v1',
       'CoxPHFitter',
       CURRENT_TIMESTAMP(),
       {results['n_train']},
       NULL,
       NULL,
       NULL,
       NULL,
       9,
       'lifelines CoxPHFitter; penalizer=0.1; C-index={results["c_index"]}; '
       'Brier 1y={results["brier_scores"].get("1y")} 3y={results["brier_scores"].get("3y")} '
       '5y={results["brier_scores"].get("5y")}; LLR p={results["concordance_pvalue"]}; '
       'not a BQML model — Python lifelines; DFL=DFL-20260506-086')
    """
    client.query(insert_sql).result()
    print("Inserted cox_recurrence_v1 into bqml_eval_log_v1")

    return results_out


if __name__ == "__main__":
    results = main()
    print("\n=== DONE ===")
    print(json.dumps(results, indent=2))
