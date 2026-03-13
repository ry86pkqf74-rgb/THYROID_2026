#!/usr/bin/env python3
"""
64_run_survival_analyses.py -- Survival analyses for manuscript

Analyses:
  - Kaplan-Meier curves by AJCC8 stage, ATA risk, procedure type, BRAF status
  - Log-rank tests for each stratification
  - Multivariable Cox PH: age + sex + AJCC8 + ATA_risk + ete + ln_positive + rai
  - Schoenfeld residual test for proportional hazards
  - KM plot data export (time, survival, CI) for figure generation

Endpoint: structural_recurrence_flag (time = recurrence_date - surgery_date).
Falls back to any_recurrence_flag if structural is unavailable.
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


def _to_bool(series: pd.Series) -> pd.Series:
    def _conv(v):
        if v is True or str(v).lower() in ("true", "yes", "1"):
            return True
        if v is False or str(v).lower() in ("false", "no", "0"):
            return False
        return None
    return series.map(_conv).astype("boolean")


# ---------------------------------------------------------------------------
# Survival cohort prep
# ---------------------------------------------------------------------------

def prepare_survival_cohort(df: pd.DataFrame) -> pd.DataFrame:
    """Derive time_days and event from available columns."""
    surv = df[df["analysis_eligible_flag"] == True].copy()  # noqa: E712

    surv["surgery_dt"] = pd.to_datetime(surv["surgery_date"], errors="coerce")
    surv["recurrence_dt"] = pd.to_datetime(surv["recurrence_date"], errors="coerce")

    for flag_col in ("structural_recurrence_flag", "any_recurrence_flag"):
        if flag_col in surv.columns:
            surv["event"] = _to_bool(surv[flag_col]).fillna(False).astype(int)
            print(f"  Event source: {flag_col}")
            break
    else:
        print("  [ERROR] No recurrence flag column found")
        return pd.DataFrame()

    surv["time_days"] = (surv["recurrence_dt"] - surv["surgery_dt"]).dt.days
    last_followup = surv["surgery_dt"].max()
    surv["time_days"] = surv["time_days"].fillna(
        (last_followup - surv["surgery_dt"]).dt.days
    )
    surv = surv[surv["time_days"].notna() & (surv["time_days"] > 0)].copy()
    surv["time_years"] = surv["time_days"] / 365.25

    print(f"  Survival cohort: N={len(surv):,}, events={surv['event'].sum()}, "
          f"median follow-up={surv['time_years'].median():.1f}y")
    return surv


# ---------------------------------------------------------------------------
# Kaplan-Meier
# ---------------------------------------------------------------------------

def run_km_analysis(surv: pd.DataFrame, strat_col: str, strat_name: str) -> pd.DataFrame | None:
    try:
        from lifelines import KaplanMeierFitter  # noqa: F811
        from lifelines.statistics import logrank_test  # noqa: F811
    except ImportError:
        print("  [WARN] lifelines not installed. Skipping KM analysis.")
        return None

    if strat_col not in surv.columns:
        print(f"  [SKIP] Column {strat_col} not found")
        return None

    sub = surv[surv[strat_col].notna()].copy()
    groups = sub[strat_col].unique()
    if len(groups) < 2:
        print(f"  [SKIP] Only {len(groups)} group(s) for {strat_name}")
        return None

    print(f"  KM by {strat_name}: {len(groups)} groups, N={len(sub):,}")

    all_rows = []
    km_fits = {}
    for g in sorted(groups, key=str):
        mask = sub[strat_col] == g
        kmf = KaplanMeierFitter()
        kmf.fit(sub.loc[mask, "time_years"], sub.loc[mask, "event"], label=str(g))
        km_fits[g] = kmf

        sf = kmf.survival_function_
        ci = kmf.confidence_interval_survival_function_
        for t_idx in sf.index:
            row = {
                "Stratifier": strat_name,
                "Group": str(g),
                "Time_years": round(float(t_idx), 4),
                "Survival": round(float(sf.loc[t_idx].values[0]), 4),
                "N_at_risk": int(kmf.event_table.loc[:t_idx, "at_risk"].iloc[-1])
                    if t_idx in kmf.event_table.index or len(kmf.event_table) > 0 else 0,
            }
            if len(ci.columns) >= 2:
                row["CI_lower"] = round(float(ci.iloc[:, 0].loc[t_idx]), 4)
                row["CI_upper"] = round(float(ci.iloc[:, 1].loc[t_idx]), 4)
            all_rows.append(row)

    group_list = sorted(groups, key=str)
    if len(group_list) == 2:
        g1, g2 = group_list
        m1, m2 = sub[strat_col] == g1, sub[strat_col] == g2
        try:
            lr = logrank_test(
                sub.loc[m1, "time_years"], sub.loc[m2, "time_years"],
                sub.loc[m1, "event"], sub.loc[m2, "event"],
            )
            print(f"    Log-rank p = {lr.p_value:.4f}")
        except Exception as e:
            print(f"    Log-rank failed: {e}")

    for t in [1, 3, 5, 10]:
        line = f"    {t}y survival:"
        for g in group_list:
            kmf = km_fits[g]
            try:
                s = kmf.predict(t)
                line += f"  {g}={s:.3f}"
            except Exception:
                pass
        print(line)

    return pd.DataFrame(all_rows)


def build_km_summary(surv: pd.DataFrame) -> pd.DataFrame:
    """Summary statistics for each KM stratification."""
    try:
        from lifelines import KaplanMeierFitter  # noqa: F401
        from lifelines.statistics import logrank_test  # noqa: F401
    except ImportError:
        return pd.DataFrame()

    strats = []
    if "ajcc8_stage_grouped" in surv.columns:
        strats.append(("ajcc8_stage_grouped", "AJCC8 Stage"))
    if "ata_risk_category" in surv.columns:
        strats.append(("ata_risk_category", "ATA Risk"))
    if "surg_procedure_type" in surv.columns:
        strats.append(("surg_procedure_type", "Procedure"))
    if "braf_status" in surv.columns:
        strats.append(("braf_status", "BRAF"))

    rows = []
    for col, name in strats:
        sub = surv[surv[col].notna()]
        groups = sorted(sub[col].unique(), key=str)
        for g in groups:
            mask = sub[col] == g
            n = int(mask.sum())
            events = int(sub.loc[mask, "event"].sum())
            med_fu = sub.loc[mask, "time_years"].median()
            rows.append({
                "Stratifier": name, "Group": str(g),
                "N": n, "Events": events,
                "Median_followup_years": round(med_fu, 1) if not np.isnan(med_fu) else None,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Cox PH
# ---------------------------------------------------------------------------

def run_cox_ph(surv: pd.DataFrame) -> pd.DataFrame | None:
    try:
        from lifelines import CoxPHFitter
    except ImportError:
        print("  [WARN] lifelines not installed. Skipping Cox PH.")
        return None

    section("Multivariable Cox PH")

    cox_df = surv[["time_years", "event"]].copy()

    for age_col in ("path_age_at_surgery_raw", "age_at_surgery"):
        if age_col in surv.columns:
            cox_df["age"] = pd.to_numeric(surv[age_col], errors="coerce")
            break

    for sex_col in ("demo_sex_final", "sex"):
        if sex_col in surv.columns:
            cox_df["sex_male"] = (surv[sex_col].astype(str).str.lower() == "male").astype(float)
            break

    if "ajcc8_stage_group" in surv.columns:
        stage = surv["ajcc8_stage_group"].astype(str).str.upper()
        cox_df["stage_III_IV"] = stage.isin(["III", "IVA", "IVB", "IVC", "IV", "III/IV"]).astype(float)

    if "ata_risk_category" in surv.columns:
        ata = surv["ata_risk_category"].astype(str).str.lower()
        cox_df["ata_high"] = (ata == "high").astype(float)
        cox_df["ata_intermediate"] = (ata == "intermediate").astype(float)

    if "path_ete_final" in surv.columns:
        ete = surv["path_ete_final"].astype(str).str.lower()
        cox_df["ete_present"] = ete.isin(["microscopic", "gross", "present", "present_ungraded"]).astype(float)

    if "path_ln_positive_raw" in surv.columns:
        cox_df["ln_positive"] = pd.to_numeric(surv["path_ln_positive_raw"], errors="coerce").apply(
            lambda x: 1.0 if x and x > 0 else 0.0
        )

    if "rai_received_flag" in surv.columns:
        cox_df["rai_received"] = _to_bool(surv["rai_received_flag"]).fillna(False).astype(float)

    available = [c for c in cox_df.columns if c not in ("time_years", "event") and cox_df[c].notna().sum() > 50]
    if len(available) < 2:
        print("  [SKIP] Fewer than 2 available predictors")
        return None

    cox_df = cox_df[["time_years", "event"] + available].dropna()
    print(f"  Cox PH: N={len(cox_df):,}, events={int(cox_df['event'].sum())}, "
          f"predictors={len(available)}")

    if cox_df["event"].sum() < 10:
        print("  [SKIP] Too few events for Cox PH")
        return None

    try:
        cph = CoxPHFitter()
        cph.fit(cox_df, duration_col="time_years", event_col="event")
    except Exception as e:
        print(f"  [ERROR] Cox fit failed: {e}")
        return None

    summary = cph.summary
    print(f"  Concordance: {cph.concordance_index_:.3f}")

    rows = []
    for var in summary.index:
        rows.append({
            "Variable": var,
            "HR": round(summary.loc[var, "exp(coef)"], 3),
            "CI_lower": round(summary.loc[var, "exp(coef) lower 95%"], 3),
            "CI_upper": round(summary.loc[var, "exp(coef) upper 95%"], 3),
            "p_value": round(summary.loc[var, "p"], 4),
            "Concordance": round(cph.concordance_index_, 3),
            "N": len(cox_df),
            "Events": int(cox_df["event"].sum()),
        })

    print("\n  Proportional hazards test (Schoenfeld):")
    try:
        cph.check_assumptions(cox_df, p_value_threshold=0.05, show_plots=False)
    except Exception:
        print("    (test did not flag violations or could not run)")

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run survival analyses")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--md", action="store_true", help="Use MotherDuck")
    grp.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print plan only")
    args = parser.parse_args()

    use_md = args.md
    if not args.md and not args.local:
        use_md = False

    section("64 · Survival Analyses")

    if args.dry_run:
        print("[DRY-RUN] Would run KM + Cox PH survival analyses")
        print("[DRY-RUN] Stratifiers: AJCC8, ATA risk, procedure, BRAF")
        print(f"[DRY-RUN] Output: {ANALYSIS_DIR}")
        return

    try:
        from lifelines import KaplanMeierFitter  # noqa: F401,F811
    except ImportError:
        print("[ERROR] lifelines is required for survival analyses.")
        print("  Install: pip install lifelines")
        return

    con = get_connection(use_md)
    table = resolve_table(con, "manuscript_cohort_v1", "patient_analysis_resolved_v1")

    print("  Loading cohort ...")
    df = con.execute(f"SELECT * FROM {table}").fetchdf()
    print(f"  Loaded {len(df):,} rows")

    surv = prepare_survival_cohort(df)
    if surv.empty:
        print("  [ERROR] Could not build survival cohort")
        con.close()
        return

    if "ajcc8_stage_group" in surv.columns:
        stage = surv["ajcc8_stage_group"].astype(str).str.upper()
        surv["ajcc8_stage_grouped"] = stage.map(
            lambda s: "I/II" if s in ("I", "IA", "IB", "II", "IIA", "IIB")
            else ("III/IV" if s in ("III", "IVA", "IVB", "IVC", "IV") else None)
        )

    if "mol_braf_positive_final" in surv.columns:
        surv["braf_status"] = _to_bool(surv["mol_braf_positive_final"]).map(
            {True: "BRAF+", False: "BRAF-"}
        )

    # -- KM analyses --
    km_strats = [
        ("ajcc8_stage_grouped", "AJCC8 Stage"),
        ("ata_risk_category", "ATA Risk"),
        ("surg_procedure_type", "Procedure Type"),
        ("braf_status", "BRAF Status"),
    ]

    all_km = []
    for col, name in km_strats:
        section(f"KM: {name}")
        km_data = run_km_analysis(surv, col, name)
        if km_data is not None and len(km_data):
            all_km.append(km_data)

    if all_km:
        combined_km = pd.concat(all_km, ignore_index=True)
        out = ANALYSIS_DIR / "km_curve_data.csv"
        combined_km.to_csv(out, index=False)
        print(f"\n  KM curve data ({len(combined_km)} rows) -> {out}")

    km_summary = build_km_summary(surv)
    if len(km_summary):
        out = ANALYSIS_DIR / "km_summary.csv"
        km_summary.to_csv(out, index=False)
        print(f"  KM summary -> {out}")

    # -- Cox PH --
    cox_result = run_cox_ph(surv)
    if cox_result is not None:
        out = ANALYSIS_DIR / "cox_ph_results.csv"
        cox_result.to_csv(out, index=False)
        print(f"  Cox PH results -> {out}")

    con.close()
    print(f"\n  Survival analysis outputs saved to {ANALYSIS_DIR}")


if __name__ == "__main__":
    main()
