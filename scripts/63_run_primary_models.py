#!/usr/bin/env python3
"""
63_run_primary_models.py -- Primary logistic regression models for manuscript

Models:
  A) Nodal metastasis ~ age + tumor_size + ete + multifocality + BRAF
  B) Gross ETE ~ age + tumor_size + LN + BRAF + TERT
  C) RAI receipt ~ AJCC8 stage + ATA risk + age + tumor_size
  D) Any complication ~ procedure_type + age + tumor_size

Complete-case analysis. Reports OR, 95% CI, p-value, VIF.
Warns if events < 10*predictors.
Outputs to exports/manuscript_analysis/.
Supports --md, --local, --dry-run.
"""
from __future__ import annotations

import argparse
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

ANALYSIS_DIR = ROOT / "exports" / "manuscript_analysis"
ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

TODAY = datetime.now().strftime("%Y%m%d_%H%M")

warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _get_token() -> str:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if token:
        return token
    secrets = ROOT / ".streamlit" / "secrets.toml"
    if secrets.exists():
        try:
            import toml
            return toml.load(str(secrets))["MOTHERDUCK_TOKEN"]
        except Exception:
            pass
    raise RuntimeError(
        "MOTHERDUCK_TOKEN not set. Export it or add to .streamlit/secrets.toml."
    )


def get_connection(md: bool):
    import duckdb
    if md:
        token = _get_token()
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(
        os.environ.get("LOCAL_DUCKDB_PATH", str(ROOT / "thyroid_master.duckdb"))
    )


def resolve_table(con, preferred: str, fallback: str) -> str:
    for name in (preferred, fallback, f"md_{preferred}", f"md_{fallback}"):
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            if n > 0:
                print(f"  [source] Using table '{name}' ({n:,} rows)")
                return name
        except Exception:
            continue
    raise RuntimeError(f"Neither {preferred} nor {fallback} found.")


def section(title: str) -> None:
    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print(f"{'=' * 72}\n")


# ---------------------------------------------------------------------------
# Model helpers
# ---------------------------------------------------------------------------

def _to_bool_int(series: pd.Series) -> pd.Series:
    """Convert various boolean representations to 0/1."""
    def _conv(v):
        if v is True or str(v).lower() in ("true", "yes", "1"):
            return 1
        if v is False or str(v).lower() in ("false", "no", "0"):
            return 0
        return np.nan
    return series.map(_conv)


def _compute_vif(X: pd.DataFrame) -> pd.DataFrame:
    from statsmodels.stats.outliers_influence import variance_inflation_factor
    rows = []
    X_arr = X.values.astype(float)
    for i, col in enumerate(X.columns):
        try:
            vif = variance_inflation_factor(X_arr, i)
        except Exception:
            vif = np.nan
        rows.append({"Variable": col, "VIF": round(vif, 2)})
    return pd.DataFrame(rows)


def run_logistic(y: pd.Series, X: pd.DataFrame, model_name: str) -> pd.DataFrame | None:
    """Fit logistic regression, return OR table or None on failure."""
    try:
        import statsmodels.api as sm
    except ImportError:
        print("  [WARN] statsmodels not installed, skipping model")
        return None

    mask = y.notna() & X.notna().all(axis=1)
    y_cc, X_cc = y[mask].astype(float), X[mask].astype(float)
    n_events = int(y_cc.sum())
    n_predictors = X_cc.shape[1]
    n_obs = len(y_cc)

    print(f"  N={n_obs:,}  events={n_events}  predictors={n_predictors}")
    if n_events < 10 * n_predictors:
        print(f"  [WARN] Events ({n_events}) < 10 * predictors ({10*n_predictors}). "
              f"Results may be unreliable.")
    if n_events < 5:
        print(f"  [SKIP] Too few events ({n_events}) to fit model.")
        return None

    X_const = sm.add_constant(X_cc)
    try:
        model = sm.Logit(y_cc, X_const).fit(disp=0, maxiter=100)
    except Exception as e:
        print(f"  [ERROR] Model failed: {e}")
        return None

    params = model.params
    conf = model.conf_int()
    pvals = model.pvalues

    rows = []
    for var in params.index:
        or_val = np.exp(params[var])
        ci_lo = np.exp(conf.loc[var, 0])
        ci_hi = np.exp(conf.loc[var, 1])
        rows.append({
            "Model": model_name,
            "Variable": var,
            "OR": round(or_val, 3),
            "CI_lower": round(ci_lo, 3),
            "CI_upper": round(ci_hi, 3),
            "p_value": round(pvals[var], 4),
            "N": n_obs,
            "Events": n_events,
            "AIC": round(model.aic, 1),
            "Pseudo_R2": round(model.prsquared, 4),
        })

    vif_df = _compute_vif(X_cc)
    high_vif = vif_df[vif_df["VIF"] > 5]
    if len(high_vif):
        print("  [WARN] High VIF detected:")
        print(high_vif.to_string(index=False))

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Model specifications
# ---------------------------------------------------------------------------

def model_a_nodal(df: pd.DataFrame) -> pd.DataFrame | None:
    """Nodal metastasis ~ age + tumor_size + ete + multifocality + BRAF."""
    section("Model A: Nodal Metastasis")
    sub = df[df["analysis_eligible_flag"] == True].copy()  # noqa: E712
    y = _to_bool_int(sub.get("path_ln_positive_raw", pd.Series(dtype=float)))

    predictors = {}
    if "path_age_at_surgery_raw" in sub.columns:
        predictors["age"] = pd.to_numeric(sub["path_age_at_surgery_raw"], errors="coerce")
    elif "age_at_surgery" in sub.columns:
        predictors["age"] = pd.to_numeric(sub["age_at_surgery"], errors="coerce")
    if "path_tumor_size_cm" in sub.columns:
        predictors["tumor_size_cm"] = pd.to_numeric(sub["path_tumor_size_cm"], errors="coerce")
    for col, name in [("path_ete_final", "ete"), ("path_multifocal_final", "multifocal"),
                       ("mol_braf_positive_final", "braf_positive")]:
        if col in sub.columns:
            predictors[name] = _to_bool_int(sub[col])

    if len(predictors) < 2:
        print("  [SKIP] Insufficient predictor columns")
        return None
    X = pd.DataFrame(predictors, index=sub.index)
    return run_logistic(y, X, "A_nodal_metastasis")


def model_b_ete(df: pd.DataFrame) -> pd.DataFrame | None:
    """Gross ETE ~ age + tumor_size + LN + BRAF + TERT."""
    section("Model B: Gross ETE")
    sub = df[df["analysis_eligible_flag"] == True].copy()  # noqa: E712

    if "path_ete_final" in sub.columns:
        y = (sub["path_ete_final"].astype(str).str.lower() == "gross").astype(float)
    else:
        print("  [SKIP] No ETE column found")
        return None

    predictors = {}
    if "path_age_at_surgery_raw" in sub.columns:
        predictors["age"] = pd.to_numeric(sub["path_age_at_surgery_raw"], errors="coerce")
    elif "age_at_surgery" in sub.columns:
        predictors["age"] = pd.to_numeric(sub["age_at_surgery"], errors="coerce")
    if "path_tumor_size_cm" in sub.columns:
        predictors["tumor_size_cm"] = pd.to_numeric(sub["path_tumor_size_cm"], errors="coerce")
    if "path_ln_positive_raw" in sub.columns:
        predictors["ln_positive"] = _to_bool_int(sub["path_ln_positive_raw"])
    for col, name in [("mol_braf_positive_final", "braf"), ("mol_tert_positive_final", "tert")]:
        if col in sub.columns:
            predictors[name] = _to_bool_int(sub[col])

    if len(predictors) < 2:
        print("  [SKIP] Insufficient predictor columns")
        return None
    X = pd.DataFrame(predictors, index=sub.index)
    return run_logistic(y, X, "B_gross_ete")


def model_c_rai(df: pd.DataFrame) -> pd.DataFrame | None:
    """RAI receipt ~ AJCC8 stage + ATA risk + age + tumor_size."""
    section("Model C: RAI Receipt")
    sub = df[df["analysis_eligible_flag"] == True].copy()  # noqa: E712
    y = _to_bool_int(sub.get("rai_received_flag", pd.Series(dtype=float)))

    predictors = {}
    if "ajcc8_stage_group" in sub.columns:
        stage = sub["ajcc8_stage_group"].astype(str).str.upper()
        predictors["stage_III_IV"] = stage.isin(["III", "IVA", "IVB", "IVC", "IV", "III/IV"]).astype(float)
    if "ata_risk_category" in sub.columns:
        ata = sub["ata_risk_category"].astype(str).str.lower()
        predictors["ata_high"] = (ata == "high").astype(float)
        predictors["ata_intermediate"] = (ata == "intermediate").astype(float)
    if "path_age_at_surgery_raw" in sub.columns:
        predictors["age"] = pd.to_numeric(sub["path_age_at_surgery_raw"], errors="coerce")
    elif "age_at_surgery" in sub.columns:
        predictors["age"] = pd.to_numeric(sub["age_at_surgery"], errors="coerce")
    if "path_tumor_size_cm" in sub.columns:
        predictors["tumor_size_cm"] = pd.to_numeric(sub["path_tumor_size_cm"], errors="coerce")

    if len(predictors) < 2:
        print("  [SKIP] Insufficient predictor columns")
        return None
    X = pd.DataFrame(predictors, index=sub.index)
    return run_logistic(y, X, "C_rai_receipt")


def model_d_complications(df: pd.DataFrame, con) -> pd.DataFrame | None:
    """Any complication ~ procedure_type + age + tumor_size."""
    section("Model D: Any Complication")

    episode_table = None
    for t in ("episode_analysis_resolved_v1", "md_episode_analysis_resolved_v1"):
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            if n > 0:
                episode_table = t
                print(f"  [source] Using episode table '{t}' ({n:,} rows)")
                break
        except Exception:
            continue

    if episode_table:
        sub = con.execute(f"SELECT * FROM {episode_table}").fetchdf()
    else:
        print("  [NOTE] episode_analysis_resolved_v1 not found; using patient table")
        sub = df[df["analysis_eligible_flag"] == True].copy()  # noqa: E712

    has_complication_cols = [c for c in sub.columns
                            if "complication" in c.lower() or c in ("hypocalcemia_status", "rln_status")]
    if has_complication_cols:
        any_comp = pd.Series(0, index=sub.index, dtype=float)
        for c in has_complication_cols:
            any_comp = any_comp | _to_bool_int(sub[c]).fillna(0)
        y = any_comp.clip(0, 1)
    else:
        print("  [SKIP] No complication columns found")
        return None

    predictors = {}
    if "surg_procedure_type" in sub.columns:
        proc = sub["surg_procedure_type"].astype(str).str.lower()
        predictors["total_thyroidectomy"] = proc.isin(["total_thyroidectomy", "total thyroidectomy"]).astype(float)
    elif "procedure_normalized" in sub.columns:
        proc = sub["procedure_normalized"].astype(str).str.lower()
        predictors["total_thyroidectomy"] = (proc == "total_thyroidectomy").astype(float)

    for age_col in ("path_age_at_surgery_raw", "age_at_surgery", "age"):
        if age_col in sub.columns:
            predictors["age"] = pd.to_numeric(sub[age_col], errors="coerce")
            break
    for size_col in ("path_tumor_size_cm", "tumor_size_cm"):
        if size_col in sub.columns:
            predictors["tumor_size_cm"] = pd.to_numeric(sub[size_col], errors="coerce")
            break

    if len(predictors) < 1:
        print("  [SKIP] Insufficient predictor columns")
        return None
    X = pd.DataFrame(predictors, index=sub.index)
    return run_logistic(y, X, "D_any_complication")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run primary logistic regression models")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--md", action="store_true", help="Use MotherDuck")
    grp.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print plan only")
    args = parser.parse_args()

    use_md = args.md
    if not args.md and not args.local:
        use_md = False

    section("63 · Primary Logistic Regression Models")

    if args.dry_run:
        print("[DRY-RUN] Would run 4 logistic regression models:")
        print("  A) Nodal metastasis ~ age + tumor_size + ete + multifocal + BRAF")
        print("  B) Gross ETE ~ age + tumor_size + LN + BRAF + TERT")
        print("  C) RAI receipt ~ AJCC8 stage + ATA risk + age + tumor_size")
        print("  D) Any complication ~ procedure_type + age + tumor_size")
        print(f"  Output: {ANALYSIS_DIR}")
        return

    con = get_connection(use_md)
    table = resolve_table(con, "manuscript_cohort_v1", "patient_analysis_resolved_v1")

    print("  Loading cohort ...")
    df = con.execute(f"SELECT * FROM {table}").fetchdf()
    print(f"  Loaded {len(df):,} rows")

    all_results = []

    for model_fn in [model_a_nodal, model_b_ete, model_c_rai]:
        try:
            result = model_fn(df)
            if result is not None:
                all_results.append(result)
        except Exception as e:
            print(f"  [ERROR] {model_fn.__name__}: {e}")

    try:
        result = model_d_complications(df, con)
        if result is not None:
            all_results.append(result)
    except Exception as e:
        print(f"  [ERROR] model_d_complications: {e}")

    if all_results:
        combined = pd.concat(all_results, ignore_index=True)
        out = ANALYSIS_DIR / "primary_logistic_models.csv"
        combined.to_csv(out, index=False)
        print(f"\n  Combined model results ({len(combined)} rows) -> {out}")

        section("Summary")
        for model_name in combined["Model"].unique():
            sub = combined[combined["Model"] == model_name]
            print(f"  {model_name}: N={sub['N'].iloc[0]:,}, events={sub['Events'].iloc[0]}, "
                  f"AIC={sub['AIC'].iloc[0]:.1f}")
            sig = sub[sub["p_value"] < 0.05]
            if len(sig):
                for _, r in sig.iterrows():
                    if r["Variable"] == "const":
                        continue
                    print(f"    {r['Variable']}: OR={r['OR']:.2f} "
                          f"({r['CI_lower']:.2f}\u2013{r['CI_upper']:.2f}), p={r['p_value']:.4f}")
    else:
        print("\n  [WARN] No models produced results.")

    con.close()
    print(f"\n  Analysis outputs saved to {ANALYSIS_DIR}")


if __name__ == "__main__":
    main()
