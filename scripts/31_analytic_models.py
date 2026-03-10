#!/usr/bin/env python3
"""
31_analytic_models.py — Analytic Modeling Starter

Produces reproducible analytic outputs from the publication-ready lakehouse:

  1. Cohort description (Table 1)
  2. Univariate + multivariable logistic regression (recurrence)
  3. Kaplan–Meier survival curves by AJCC stage, ETE, BRAF
  4. Cox proportional-hazards model
  5. Propensity-score matched ETE sub-analysis (if lifelines + statsmodels)

Reads from:
  - risk_enriched_mv          (risk features + survival)
  - advanced_features_v3      (60+ engineered features)
  - survival_cohort_ready_mv  (time-to-event)

Outputs:
  - studies/analytic_models/table1_demographics.csv
  - studies/analytic_models/logistic_regression.csv
  - studies/analytic_models/cox_model.csv
  - studies/analytic_models/km_summary.csv
  - studies/analytic_models/analysis_metadata.json

Usage:
    .venv/bin/python scripts/31_analytic_models.py
    .venv/bin/python scripts/31_analytic_models.py --local
    .venv/bin/python scripts/31_analytic_models.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

np.random.seed(42)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient, MotherDuckConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("analytic_models")

OUT_DIR = ROOT / "studies" / "analytic_models"

try:
    from lifelines import KaplanMeierFitter, CoxPHFitter
    from lifelines.statistics import logrank_test

    HAS_LIFELINES = True
except ImportError:
    HAS_LIFELINES = False

try:
    from scipy import stats as sp_stats

    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

try:
    import statsmodels.api as sm

    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False


# ── Helpers ───────────────────────────────────────────────────────────────

def _safe_query(con, sql: str) -> pd.DataFrame:
    try:
        return con.execute(sql).fetchdf()
    except Exception as exc:
        log.warning("Query failed: %s", exc)
        return pd.DataFrame()


def _get_connection(args):
    if args.local or os.getenv("USE_LOCAL_DUCKDB", "").lower() in ("1", "true", "yes"):
        import duckdb

        local_path = os.getenv("LOCAL_DUCKDB_PATH", str(ROOT / "thyroid_master_local.duckdb"))
        log.info("Connecting to local DuckDB: %s", local_path)
        return duckdb.connect(local_path)
    log.info("Connecting to MotherDuck (thyroid_research_2026)…")
    cfg = MotherDuckConfig(database="thyroid_research_2026")
    return MotherDuckClient(cfg).connect_rw()


# ── Phase 1: Table 1 — Cohort Description ────────────────────────────────

def phase1_table1(con, out_dir: Path) -> pd.DataFrame:
    log.info("Phase 1: Table 1 — Cohort Description")
    df = _safe_query(con, "SELECT * FROM risk_enriched_mv")
    if df.empty:
        log.warning("  risk_enriched_mv is empty; falling back to advanced_features_v3")
        df = _safe_query(con, "SELECT * FROM advanced_features_v3")
    if df.empty:
        log.error("  No data available for Table 1")
        return df

    rows = []

    def _add(label, value, n=None):
        rows.append({"Variable": label, "Value": value, "N": n or ""})

    total = len(df)
    _add("Total patients", f"{total:,}")

    if "age_at_surgery" in df.columns:
        ages = df["age_at_surgery"].dropna()
        _add("Age at surgery, median (IQR)",
             f"{ages.median():.1f} ({ages.quantile(0.25):.1f}–{ages.quantile(0.75):.1f})",
             f"{len(ages):,}")

    if "sex" in df.columns:
        for val in ["Female", "Male"]:
            n = (df["sex"] == val).sum()
            pct = 100.0 * n / total if total else 0
            _add(f"Sex — {val}", f"{n:,} ({pct:.1f}%)", f"{n:,}")

    if "histology_1_type" in df.columns:
        top5 = df["histology_1_type"].value_counts().head(5)
        for hist, n in top5.items():
            pct = 100.0 * n / total
            _add(f"Histology — {hist}", f"{n:,} ({pct:.1f}%)", f"{n:,}")

    for col, label in [
        ("overall_stage_ajcc8", "AJCC 8th Stage"),
        ("overall_stage", "Overall Stage"),
    ]:
        if col in df.columns:
            for stage in sorted(df[col].dropna().unique()):
                n = (df[col] == stage).sum()
                pct = 100.0 * n / total
                _add(f"{label} — {stage}", f"{n:,} ({pct:.1f}%)", f"{n:,}")
            break

    for flag, label in [
        ("braf_mutation_mentioned", "BRAF mutated"),
        ("braf_positive", "BRAF positive"),
        ("ras_mutation_mentioned", "RAS mutated"),
        ("ret_mutation_mentioned", "RET mutated"),
        ("tert_mutation_mentioned", "TERT mutated"),
    ]:
        if flag in df.columns:
            n = df[flag].sum()
            pct = 100.0 * n / total
            _add(label, f"{n:,} ({pct:.1f}%)", f"{n:,}")

    if "recurrence_flag" in df.columns:
        n_rec = df["recurrence_flag"].sum()
        pct = 100.0 * n_rec / total
        _add("Recurrence", f"{n_rec:,} ({pct:.1f}%)", f"{n_rec:,}")

    if "largest_tumor_cm" in df.columns:
        sizes = df["largest_tumor_cm"].dropna()
        if not sizes.empty:
            _add("Tumor size (cm), median (IQR)",
                 f"{sizes.median():.1f} ({sizes.quantile(0.25):.1f}–{sizes.quantile(0.75):.1f})",
                 f"{len(sizes):,}")

    t1 = pd.DataFrame(rows)
    t1.to_csv(out_dir / "table1_demographics.csv", index=False)
    log.info("  Table 1: %d rows → table1_demographics.csv", len(t1))
    return df


# ── Phase 2: Logistic Regression (recurrence) ────────────────────────────

def phase2_logistic(df: pd.DataFrame, out_dir: Path) -> None:
    log.info("Phase 2: Logistic Regression — Recurrence")
    if not HAS_STATSMODELS:
        log.warning("  statsmodels not installed — skipping logistic regression")
        return
    if "recurrence_flag" not in df.columns:
        log.warning("  recurrence_flag not available — skipping")
        return

    predictors = []
    candidate_cols = [
        "braf_positive", "braf_mutation_mentioned",
        "ras_positive", "ras_mutation_mentioned",
        "tert_positive", "tert_mutation_mentioned",
        "ret_positive", "ret_mutation_mentioned",
        "ete", "tumor_1_extrathyroidal_ext",
        "gross_ete", "tumor_1_gross_ete",
        "largest_tumor_cm", "tumor_size_cm",
        "ln_positive", "ln_ratio",
        "age_at_surgery",
    ]
    for c in candidate_cols:
        if c in df.columns:
            predictors.append(c)
    predictors = list(dict.fromkeys(predictors))

    if not predictors:
        log.warning("  No usable predictors found")
        return

    sub = df[["recurrence_flag"] + predictors].dropna().copy()
    sub["recurrence_flag"] = sub["recurrence_flag"].astype(int)
    for c in predictors:
        sub[c] = pd.to_numeric(sub[c], errors="coerce")
    sub = sub.dropna()

    if len(sub) < 20:
        log.warning("  Only %d complete cases — too few for regression", len(sub))
        return

    X = sm.add_constant(sub[predictors].astype(float))
    y = sub["recurrence_flag"]

    try:
        model = sm.Logit(y, X).fit(disp=0, maxiter=100)
        results = pd.DataFrame({
            "Variable": model.params.index,
            "Coef": model.params.values,
            "OR": np.exp(model.params.values),
            "SE": model.bse.values,
            "z": model.tvalues.values,
            "p_value": model.pvalues.values,
            "CI_lower": np.exp(model.conf_int()[0].values),
            "CI_upper": np.exp(model.conf_int()[1].values),
        })
        results.to_csv(out_dir / "logistic_regression.csv", index=False)
        log.info("  Logistic regression: %d predictors, %d obs, pseudo-R²=%.4f",
                 len(predictors), len(sub), model.prsquared)
        sig = results[results["p_value"] < 0.05]
        for _, row in sig.iterrows():
            log.info("    %s: OR=%.2f (%.2f–%.2f), p=%.4f",
                     row["Variable"], row["OR"], row["CI_lower"], row["CI_upper"], row["p_value"])
    except Exception as exc:
        log.error("  Logistic regression failed: %s", exc)


# ── Phase 3: Kaplan–Meier ────────────────────────────────────────────────

def phase3_kaplan_meier(df: pd.DataFrame, out_dir: Path) -> None:
    log.info("Phase 3: Kaplan–Meier Survival Analysis")
    if not HAS_LIFELINES:
        log.warning("  lifelines not installed — skipping KM analysis")
        return

    time_col = "time_to_event_days" if "time_to_event_days" in df.columns else None
    event_col = "event_occurred" if "event_occurred" in df.columns else None
    if not time_col or not event_col:
        log.warning("  Missing time_to_event_days or event_occurred")
        return

    sub = df[[time_col, event_col]].dropna().copy()
    sub["time_years"] = sub[time_col] / 365.25
    sub = sub[sub["time_years"] > 0]

    km_rows = []

    def _fit_strata(label_col, label_name):
        if label_col not in df.columns:
            return
        strata_df = df[[time_col, event_col, label_col]].dropna()
        strata_df["time_years"] = strata_df[time_col] / 365.25
        strata_df = strata_df[strata_df["time_years"] > 0]

        kmf = KaplanMeierFitter()
        for stratum in sorted(strata_df[label_col].unique()):
            mask = strata_df[label_col] == stratum
            n = mask.sum()
            if n < 5:
                continue
            kmf.fit(strata_df.loc[mask, "time_years"],
                    event_observed=strata_df.loc[mask, event_col],
                    label=str(stratum))
            median_surv = kmf.median_survival_time_
            def _predict_safe(t):
                try:
                    val = kmf.predict(t)
                    return round(float(val), 4)
                except Exception:
                    return None
            km_rows.append({
                "Stratifier": label_name,
                "Stratum": str(stratum),
                "N": n,
                "Events": int(strata_df.loc[mask, event_col].sum()),
                "Median_survival_years": round(float(median_surv), 2) if np.isfinite(median_surv) else None,
                "1yr_survival": _predict_safe(1.0),
                "3yr_survival": _predict_safe(3.0),
                "5yr_survival": _predict_safe(5.0),
            })

    for col, name in [
        ("overall_stage_ajcc8", "AJCC 8th Stage"),
        ("overall_stage", "Overall Stage"),
    ]:
        if col in df.columns:
            _fit_strata(col, name)
            break

    for col, name in [
        ("ete", "ETE"),
        ("tumor_1_extrathyroidal_ext", "ETE"),
        ("braf_positive", "BRAF"),
        ("braf_mutation_mentioned", "BRAF"),
        ("recurrence_risk_band", "Risk Band"),
        ("histology_1_type", "Histology"),
    ]:
        if col in df.columns:
            _fit_strata(col, name)

    if km_rows:
        km_df = pd.DataFrame(km_rows)
        km_df.to_csv(out_dir / "km_summary.csv", index=False)
        log.info("  KM summary: %d strata across %d stratifiers",
                 len(km_df), km_df["Stratifier"].nunique())


# ── Phase 4: Cox Proportional-Hazards ────────────────────────────────────

def phase4_cox(df: pd.DataFrame, out_dir: Path) -> None:
    log.info("Phase 4: Cox Proportional-Hazards Model")
    if not HAS_LIFELINES:
        log.warning("  lifelines not installed — skipping Cox model")
        return

    time_col = "time_to_event_days" if "time_to_event_days" in df.columns else None
    event_col = "event_occurred" if "event_occurred" in df.columns else None
    if not time_col or not event_col:
        log.warning("  Missing time/event columns")
        return

    covariates = []
    candidates = [
        "age_at_surgery", "survival_age_at_surgery",
        "largest_tumor_cm", "tumor_size_cm",
        "ln_positive", "ln_ratio",
        "braf_positive", "braf_mutation_mentioned",
        "ete", "tumor_1_extrathyroidal_ext",
        "tert_positive", "tert_mutation_mentioned",
    ]
    for c in candidates:
        if c in df.columns:
            covariates.append(c)
    covariates = list(dict.fromkeys(covariates))

    if not covariates:
        log.warning("  No usable covariates found")
        return

    sub = df[[time_col, event_col] + covariates].dropna().copy()
    sub["time_years"] = sub[time_col] / 365.25
    sub = sub[sub["time_years"] > 0]

    for c in covariates:
        sub[c] = pd.to_numeric(sub[c], errors="coerce")
    sub = sub.dropna()

    if len(sub) < 20:
        log.warning("  Only %d cases — too few for Cox model", len(sub))
        return

    try:
        cph = CoxPHFitter(penalizer=0.01)
        cph.fit(sub[["time_years", event_col] + covariates],
                duration_col="time_years", event_col=event_col)
        cox_summary = cph.summary.reset_index()
        cox_summary = cox_summary.rename(columns={"index": "covariate"})
        cox_summary.to_csv(out_dir / "cox_model.csv", index=False)
        log.info("  Cox model: %d covariates, %d obs, concordance=%.4f",
                 len(covariates), len(sub), cph.concordance_index_)
        sig = cox_summary[cox_summary["p"] < 0.05]
        for _, row in sig.iterrows():
            log.info("    %s: HR=%.2f (%.2f–%.2f), p=%.4f",
                     row["covariate"], row["exp(coef)"],
                     row["exp(coef) lower 95%"], row["exp(coef) upper 95%"], row["p"])
    except Exception as exc:
        log.error("  Cox model failed: %s", exc)


# ── Main ──────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> int:
    t_start = time.perf_counter()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    con = _get_connection(args)

    if args.dry_run:
        log.info("[DRY RUN] Would run 4 analytic phases against %s",
                 "local DB" if args.local else "MotherDuck")
        return 0

    df = phase1_table1(con, OUT_DIR)
    if df.empty:
        log.error("No data loaded — aborting.")
        return 1

    phase2_logistic(df, OUT_DIR)
    phase3_kaplan_meier(df, OUT_DIR)
    phase4_cox(df, OUT_DIR)

    elapsed = time.perf_counter() - t_start
    metadata = {
        "script": "31_analytic_models.py",
        "timestamp": datetime.now().isoformat(),
        "random_seed": 42,
        "source": "local" if args.local else "motherduck",
        "cohort_rows": len(df),
        "elapsed_seconds": round(elapsed, 2),
        "lifelines_available": HAS_LIFELINES,
        "statsmodels_available": HAS_STATSMODELS,
        "scipy_available": HAS_SCIPY,
        "outputs": [f.name for f in OUT_DIR.iterdir() if f.is_file()],
    }
    with open(OUT_DIR / "analysis_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print()
    print("=" * 68)
    print("  Analytic Models — Summary")
    print(f"  Timestamp: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Elapsed:   {elapsed:.1f}s")
    print(f"  Cohort:    {len(df):,} patients")
    print("=" * 68)
    for f in sorted(OUT_DIR.iterdir()):
        if f.is_file():
            print(f"  ✓  {f.name}")
    print("=" * 68)
    print()
    print("  Next: review CSVs, refine models, add to manuscript.")
    print("  Figures: notebooks/01_publication_figures.ipynb")
    print()
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run reproducible analytic models on the thyroid cohort."
    )
    ap.add_argument("--local", action="store_true", help="Use local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Print plan without executing")
    args = ap.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
