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


# ── Phase 5: Subgroup Analyses ────────────────────────────────────────────

def phase5_subgroups(df: pd.DataFrame, out_dir: Path) -> None:
    log.info("Phase 5: Subgroup Analyses")
    if not HAS_LIFELINES:
        log.warning("  lifelines not installed — skipping subgroup analyses")
        return

    time_col = "time_to_event_days" if "time_to_event_days" in df.columns else None
    event_col = "event_occurred" if "event_occurred" in df.columns else None
    if not time_col or not event_col:
        log.warning("  Missing time/event columns")
        return

    age_col = None
    for c in ("age_at_surgery", "survival_age_at_surgery"):
        if c in df.columns:
            age_col = c
            break

    subgroup_rows = []

    def _cox_subgroup(label: str, mask):
        sub = df.loc[mask].copy()
        sub = sub[[time_col, event_col]].dropna()
        sub["time_years"] = sub[time_col] / 365.25
        sub = sub[sub["time_years"] > 0]
        n = len(sub)
        events = int(sub[event_col].sum())
        subgroup_rows.append({
            "Subgroup": label,
            "N": n,
            "Events": events,
            "Event_rate_pct": round(100.0 * events / n, 2) if n else None,
        })

    _cox_subgroup("All patients", df.index >= 0)

    for hist_col in ("histology_1_type",):
        if hist_col in df.columns:
            for hist in ("PTC", "FTC", "MTC"):
                mask = df[hist_col] == hist
                if mask.sum() >= 10:
                    _cox_subgroup(f"Histology={hist}", mask)

    if age_col:
        ages = pd.to_numeric(df[age_col], errors="coerce")
        _cox_subgroup("Age < 45", ages < 45)
        _cox_subgroup("Age >= 45", ages >= 45)

    for stage_col in ("overall_stage_ajcc8", "overall_stage"):
        if stage_col in df.columns:
            for s in ("I", "II", "III"):
                mask = df[stage_col] == s
                if mask.sum() >= 10:
                    _cox_subgroup(f"Stage {s}", mask)
            break

    for ete_col in ("ete", "tumor_1_extrathyroidal_ext"):
        if ete_col in df.columns:
            _cox_subgroup("ETE present", df[ete_col] == True)
            _cox_subgroup("ETE absent", df[ete_col] == False)
            break

    for braf_col in ("braf_positive", "braf_mutation_mentioned"):
        if braf_col in df.columns:
            _cox_subgroup("BRAF+", df[braf_col] == True)
            _cox_subgroup("BRAF-", df[braf_col] == False)
            break

    if subgroup_rows:
        sg_df = pd.DataFrame(subgroup_rows)
        sg_df.to_csv(out_dir / "subgroup_event_rates.csv", index=False)
        log.info("  Subgroup analyses: %d subgroups → subgroup_event_rates.csv", len(sg_df))


# ── Phase 6: Interaction Terms ────────────────────────────────────────────

def phase6_interactions(df: pd.DataFrame, out_dir: Path) -> None:
    log.info("Phase 6: Interaction Tests")
    if not HAS_LIFELINES:
        log.warning("  lifelines not installed — skipping interaction tests")
        return

    time_col = "time_to_event_days" if "time_to_event_days" in df.columns else None
    event_col = "event_occurred" if "event_occurred" in df.columns else None
    if not time_col or not event_col:
        return

    ete_col = braf_col = None
    for c in ("ete", "tumor_1_extrathyroidal_ext"):
        if c in df.columns:
            ete_col = c
            break
    for c in ("braf_positive", "braf_mutation_mentioned"):
        if c in df.columns:
            braf_col = c
            break

    interactions = []

    def _test_interaction(name, col_a, col_b):
        if col_a is None or col_b is None:
            return
        sub = df[[time_col, event_col, col_a, col_b]].dropna().copy()
        for c in (col_a, col_b):
            sub[c] = pd.to_numeric(sub[c], errors="coerce")
        sub = sub.dropna()
        sub["time_years"] = sub[time_col] / 365.25
        sub = sub[sub["time_years"] > 0]
        if len(sub) < 30:
            return

        interaction_col = f"{col_a}_x_{col_b}"
        sub[interaction_col] = sub[col_a] * sub[col_b]

        try:
            cph_base = CoxPHFitter(penalizer=0.01)
            cph_base.fit(sub[["time_years", event_col, col_a, col_b]],
                         duration_col="time_years", event_col=event_col)

            cph_int = CoxPHFitter(penalizer=0.01)
            cph_int.fit(sub[["time_years", event_col, col_a, col_b, interaction_col]],
                        duration_col="time_years", event_col=event_col)

            int_row = cph_int.summary.loc[interaction_col]
            interactions.append({
                "Interaction": name,
                "Term": interaction_col,
                "HR": round(float(np.exp(int_row["coef"])), 3),
                "CI_lower": round(float(int_row["exp(coef) lower 95%"]), 3),
                "CI_upper": round(float(int_row["exp(coef) upper 95%"]), 3),
                "p_value": round(float(int_row["p"]), 4),
                "N": len(sub),
                "AIC_base": round(float(cph_base.AIC_partial_), 2),
                "AIC_interaction": round(float(cph_int.AIC_partial_), 2),
                "AIC_improved": cph_int.AIC_partial_ < cph_base.AIC_partial_,
            })
        except Exception as exc:
            log.warning("  Interaction %s failed: %s", name, exc)

    ln_col = "ln_positive" if "ln_positive" in df.columns else None

    _test_interaction("ETE x BRAF", ete_col, braf_col)
    _test_interaction("ETE x LN_positive", ete_col, ln_col)
    _test_interaction("BRAF x LN_positive", braf_col, ln_col)

    age_col = None
    for c in ("age_at_surgery", "survival_age_at_surgery"):
        if c in df.columns:
            age_col = c
            break
    _test_interaction("ETE x Age", ete_col, age_col)

    if interactions:
        int_df = pd.DataFrame(interactions)
        int_df.to_csv(out_dir / "interaction_tests.csv", index=False)
        log.info("  Interaction tests: %d tested → interaction_tests.csv", len(int_df))
        for _, row in int_df.iterrows():
            sig = "*" if row["p_value"] < 0.05 else ""
            log.info("    %s: HR=%.3f (%.3f–%.3f), p=%.4f %s",
                     row["Interaction"], row["HR"], row["CI_lower"], row["CI_upper"],
                     row["p_value"], sig)


# ── Phase 7: Propensity-Score Matching (ETE) ──────────────────────────────

def phase7_psm(df: pd.DataFrame, out_dir: Path) -> None:
    log.info("Phase 7: Propensity-Score Matching — ETE")
    if not HAS_STATSMODELS or not HAS_LIFELINES:
        log.warning("  statsmodels + lifelines required — skipping PSM")
        return

    ete_col = None
    for c in ("ete", "tumor_1_extrathyroidal_ext"):
        if c in df.columns:
            ete_col = c
            break
    if ete_col is None:
        log.warning("  No ETE column found")
        return

    time_col = "time_to_event_days" if "time_to_event_days" in df.columns else None
    event_col = "event_occurred" if "event_occurred" in df.columns else None
    if not time_col or not event_col:
        log.warning("  Missing time/event columns")
        return

    confounder_candidates = [
        "age_at_surgery", "survival_age_at_surgery",
        "largest_tumor_cm", "tumor_size_cm",
        "ln_positive", "ln_examined",
    ]
    confounders = []
    for c in confounder_candidates:
        if c in df.columns:
            confounders.append(c)
    confounders = list(dict.fromkeys(confounders))

    # Drop near-zero-variance columns (< 1% prevalence causes singularity)
    sub_check = df[confounders].dropna()
    confounders = [c for c in confounders
                   if sub_check[c].nunique() > 1
                   and pd.to_numeric(sub_check[c], errors="coerce").std() > 0.01]

    if len(confounders) < 2:
        log.warning("  Need >= 2 confounders, found %d", len(confounders))
        return

    sub = df[[ete_col, time_col, event_col] + confounders].dropna().copy()
    sub[ete_col] = pd.to_numeric(sub[ete_col], errors="coerce").astype(int)
    for c in confounders:
        sub[c] = pd.to_numeric(sub[c], errors="coerce")
    sub = sub.dropna()
    sub["time_years"] = sub[time_col] / 365.25
    sub = sub[sub["time_years"] > 0]

    if len(sub) < 50:
        log.warning("  Only %d complete cases — too few for PSM", len(sub))
        return

    log.info("  Confounders used (%d): %s", len(confounders), ", ".join(confounders))

    # Propensity score via logistic regression
    X = sm.add_constant(sub[confounders].astype(float))
    y = sub[ete_col]

    try:
        ps_model = sm.Logit(y, X).fit(disp=0, maxiter=100)
        sub["propensity_score"] = ps_model.predict(X)
    except Exception as exc:
        log.error("  Propensity score model failed: %s", exc)
        return

    # Nearest-neighbor matching (1:1, caliper=0.25*SD — slightly relaxed)
    treated = sub[sub[ete_col] == 1].copy()
    control = sub[sub[ete_col] == 0].copy()
    caliper = 0.25 * sub["propensity_score"].std()

    matched_pairs = []
    used_controls = set()
    for idx, row in treated.iterrows():
        ps = row["propensity_score"]
        candidates = control[~control.index.isin(used_controls)]
        if candidates.empty:
            break
        diffs = (candidates["propensity_score"] - ps).abs()
        best_idx = diffs.idxmin()
        if diffs[best_idx] <= caliper:
            matched_pairs.append((idx, best_idx))
            used_controls.add(best_idx)

    if len(matched_pairs) < 10:
        log.warning("  Only %d matched pairs — too few", len(matched_pairs))
        return

    t_idx = [p[0] for p in matched_pairs]
    c_idx = [p[1] for p in matched_pairs]
    matched = pd.concat([sub.loc[t_idx], sub.loc[c_idx]])

    # Balance check
    balance_rows = []
    for conf in confounders:
        m_treated = matched.loc[t_idx, conf].mean()
        m_control = matched.loc[c_idx, conf].mean()
        pooled_sd = matched[conf].std()
        smd = abs(m_treated - m_control) / pooled_sd if pooled_sd > 0 else 0
        balance_rows.append({
            "Variable": conf,
            "Mean_ETE": round(float(m_treated), 3),
            "Mean_NoETE": round(float(m_control), 3),
            "SMD": round(float(smd), 4),
            "Balanced": smd < 0.1,
        })
    balance_df = pd.DataFrame(balance_rows)
    balance_df.to_csv(out_dir / "psm_balance.csv", index=False)

    # Cox on matched cohort
    try:
        cph = CoxPHFitter(penalizer=0.01)
        cph.fit(matched[["time_years", event_col, ete_col]],
                duration_col="time_years", event_col=event_col)
        cox_row = cph.summary.loc[ete_col]
        psm_result = {
            "N_matched_pairs": len(matched_pairs),
            "N_total": len(matched),
            "Caliper": round(float(caliper), 4),
            "HR_ETE": round(float(np.exp(cox_row["coef"])), 3),
            "CI_lower": round(float(cox_row["exp(coef) lower 95%"]), 3),
            "CI_upper": round(float(cox_row["exp(coef) upper 95%"]), 3),
            "p_value": round(float(cox_row["p"]), 4),
            "Concordance": round(float(cph.concordance_index_), 4),
        }
        with open(out_dir / "psm_result.json", "w") as f:
            json.dump(psm_result, f, indent=2)
        log.info("  PSM: %d matched pairs, HR=%.3f (%.3f–%.3f), p=%.4f",
                 psm_result["N_matched_pairs"], psm_result["HR_ETE"],
                 psm_result["CI_lower"], psm_result["CI_upper"], psm_result["p_value"])
    except Exception as exc:
        log.error("  PSM Cox model failed: %s", exc)

    # KM on matched cohort
    try:
        kmf = KaplanMeierFitter()
        fig_rows = []
        for label, mask_val in [("ETE+", 1), ("ETE-", 0)]:
            mask = matched[ete_col] == mask_val
            n = mask.sum()
            kmf.fit(matched.loc[mask, "time_years"],
                    event_observed=matched.loc[mask, event_col], label=label)
            def _ps(t):
                try:
                    return round(float(kmf.predict(t)), 4)
                except Exception:
                    return None
            fig_rows.append({
                "Group": label,
                "N": n,
                "Events": int(matched.loc[mask, event_col].sum()),
                "1yr": _ps(1.0),
                "3yr": _ps(3.0),
                "5yr": _ps(5.0),
            })
        pd.DataFrame(fig_rows).to_csv(out_dir / "psm_km_summary.csv", index=False)
        log.info("  PSM KM summary saved → psm_km_summary.csv")
    except Exception as exc:
        log.error("  PSM KM failed: %s", exc)

    log.info("  Balance table: %d variables, %d balanced (SMD < 0.1)",
             len(balance_df), balance_df["Balanced"].sum())

    # Sensitivity analysis: vary caliper multiplier
    sensitivity_rows = []
    for mult in (0.1, 0.15, 0.2, 0.25, 0.3, 0.5):
        cal = mult * sub["propensity_score"].std()
        pairs = []
        used = set()
        for idx2, row2 in treated.iterrows():
            cands = control[~control.index.isin(used)]
            if cands.empty:
                break
            dists = (cands["propensity_score"] - row2["propensity_score"]).abs()
            best = dists.idxmin()
            if dists[best] <= cal:
                pairs.append((idx2, best))
                used.add(best)
        if len(pairs) < 10:
            continue
        t_i = [p[0] for p in pairs]
        c_i = [p[1] for p in pairs]
        m_df = pd.concat([sub.loc[t_i], sub.loc[c_i]])
        try:
            cx = CoxPHFitter(penalizer=0.01)
            cx.fit(m_df[["time_years", event_col, ete_col]],
                   duration_col="time_years", event_col=event_col)
            cr = cx.summary.loc[ete_col]
            sensitivity_rows.append({
                "Caliper_mult": mult,
                "Caliper": round(float(cal), 5),
                "Matched_pairs": len(pairs),
                "HR": round(float(np.exp(cr["coef"])), 3),
                "CI_lower": round(float(cr["exp(coef) lower 95%"]), 3),
                "CI_upper": round(float(cr["exp(coef) upper 95%"]), 3),
                "p_value": round(float(cr["p"]), 4),
                "Concordance": round(float(cx.concordance_index_), 4),
            })
        except Exception:
            pass

    if sensitivity_rows:
        sens_df = pd.DataFrame(sensitivity_rows)
        sens_df.to_csv(out_dir / "psm_sensitivity.csv", index=False)
        log.info("  PSM sensitivity: %d caliper levels → psm_sensitivity.csv", len(sens_df))
        for _, r in sens_df.iterrows():
            log.info("    caliper=%.1f×SD: %d pairs, HR=%.3f (%.3f–%.3f), p=%.4f",
                     r["Caliper_mult"], r["Matched_pairs"], r["HR"],
                     r["CI_lower"], r["CI_upper"], r["p_value"])


# ── Main ──────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> int:
    t_start = time.perf_counter()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    con = _get_connection(args)

    if args.dry_run:
        log.info("[DRY RUN] Would run 7 analytic phases against %s",
                 "local DB" if args.local else "MotherDuck")
        return 0

    df = phase1_table1(con, OUT_DIR)
    if df.empty:
        log.error("No data loaded — aborting.")
        return 1

    phase2_logistic(df, OUT_DIR)
    phase3_kaplan_meier(df, OUT_DIR)
    phase4_cox(df, OUT_DIR)
    phase5_subgroups(df, OUT_DIR)
    phase6_interactions(df, OUT_DIR)
    phase7_psm(df, OUT_DIR)

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
