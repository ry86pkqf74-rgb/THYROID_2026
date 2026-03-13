#!/usr/bin/env python3
"""
62_run_primary_descriptives.py -- Primary descriptive statistics for manuscript

Generates:
  - Cohort flow cascade (total -> eligible -> cancer -> molecular -> RAI -> Tg -> TIRADS)
  - Table 1: Demographics stratified by analysis_eligible_flag
  - Table 2: Tumor/treatment characteristics
  - Table 3: Outcomes (recurrence, complications, Tg)
  - Supplementary missingness table

All outputs saved as CSV to exports/manuscript_tables/.
Supports --md, --local, --dry-run.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TABLES_DIR = ROOT / "exports" / "manuscript_tables"
TABLES_DIR.mkdir(parents=True, exist_ok=True)

TODAY = datetime.now().strftime("%Y%m%d_%H%M")

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def section(title: str) -> None:
    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print(f"{'=' * 72}\n")


def _safe_float(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return np.nan


def continuous_summary(series: pd.Series, name: str) -> dict:
    s = series.dropna()
    return {
        "Variable": name,
        "N": len(s),
        "Mean": f"{s.mean():.1f}",
        "SD": f"{s.std():.1f}",
        "Median": f"{s.median():.1f}",
        "IQR": f"[{s.quantile(0.25):.1f}\u2013{s.quantile(0.75):.1f}]",
        "Missing_n": int(series.isna().sum()),
        "Missing_pct": f"{100 * series.isna().mean():.1f}%",
    }


def categorical_summary(series: pd.Series, name: str) -> list[dict]:
    total = len(series)
    counts = series.fillna("Missing").value_counts()
    rows = []
    for val, n in counts.items():
        rows.append({
            "Variable": name if len(rows) == 0 else "",
            "Category": str(val),
            "N": int(n),
            "Pct": f"{100 * n / total:.1f}%" if total else "0.0%",
        })
    return rows


def _chi2_p(col: pd.Series, group: pd.Series) -> float:
    from scipy.stats import chi2_contingency
    ct = pd.crosstab(col.fillna("Missing"), group)
    if ct.shape[0] < 2 or ct.shape[1] < 2:
        return np.nan
    try:
        return chi2_contingency(ct)[1]
    except Exception:
        return np.nan


def _continuous_p(col: pd.Series, group: pd.Series) -> float:
    from scipy.stats import mannwhitneyu
    groups = group.dropna().unique()
    if len(groups) != 2:
        return np.nan
    a = col[group == groups[0]].dropna()
    b = col[group == groups[1]].dropna()
    if len(a) < 2 or len(b) < 2:
        return np.nan
    try:
        return mannwhitneyu(a, b, alternative="two-sided").pvalue
    except Exception:
        return np.nan


# ---------------------------------------------------------------------------
# Main analysis functions
# ---------------------------------------------------------------------------

def build_cohort_flow(df: pd.DataFrame) -> pd.DataFrame:
    """Cascade of inclusion counts."""
    total = len(df)
    eligible = int((df["analysis_eligible_flag"] == True).sum())  # noqa: E712
    cancer = int(df["path_histology_raw"].notna().sum())
    molecular = int((df["molecular_eligible_flag"] == True).sum())  # noqa: E712
    rai = int((df["rai_received_flag"] == True).sum())  # noqa: E712
    tg = int(df["tg_nadir"].notna().sum())
    tirads = int(df["imaging_tirads_best"].notna().sum())

    rows = [
        ("Total patients", total, ""),
        ("Analysis-eligible", eligible, f"{100*eligible/total:.1f}%" if total else ""),
        ("With histology (cancer)", cancer, f"{100*cancer/total:.1f}%" if total else ""),
        ("Molecular-eligible", molecular, f"{100*molecular/total:.1f}%" if total else ""),
        ("RAI received", rai, f"{100*rai/total:.1f}%" if total else ""),
        ("Tg available", tg, f"{100*tg/total:.1f}%" if total else ""),
        ("TIRADS available", tirads, f"{100*tirads/total:.1f}%" if total else ""),
    ]
    return pd.DataFrame(rows, columns=["Step", "N", "Pct_of_total"])


def build_table1(df: pd.DataFrame) -> pd.DataFrame:
    """Demographics stratified by analysis_eligible_flag."""
    df = df.copy()
    df["age"] = df.get("path_age_at_surgery_raw", df.get("age_at_surgery", pd.Series(dtype=float)))
    df["sex"] = df.get("demo_sex_final", df.get("sex", pd.Series(dtype=object)))
    df["race"] = df.get("demo_race_final", df.get("race", pd.Series(dtype=object)))

    group_col = "analysis_eligible_flag"
    rows = []

    for col_name, display in [("age", "Age at surgery"), ("path_tumor_size_cm", "Tumor size (cm)")]:
        if col_name not in df.columns:
            continue
        s = pd.to_numeric(df[col_name], errors="coerce")
        p = _continuous_p(s, df[group_col])
        for g in [True, False]:
            sub = s[df[group_col] == g].dropna()
            rows.append({
                "Variable": display,
                "Group": "Eligible" if g else "Not eligible",
                "Summary": f"{sub.mean():.1f} ({sub.std():.1f})" if len(sub) else "N/A",
                "N": int(len(sub)),
                "p_value": f"{p:.4f}" if not np.isnan(p) else "",
            })

    for col_name, display in [("sex", "Sex"), ("race", "Race"), ("path_histology_raw", "Histology")]:
        if col_name not in df.columns:
            continue
        p = _chi2_p(df[col_name], df[group_col])
        for g in [True, False]:
            sub = df[df[group_col] == g][col_name].fillna("Missing")
            total_g = len(sub)
            vc = sub.value_counts()
            for cat, n in vc.items():
                rows.append({
                    "Variable": display,
                    "Group": "Eligible" if g else "Not eligible",
                    "Summary": f"{cat}: {n} ({100*n/total_g:.1f}%)" if total_g else "N/A",
                    "N": int(n),
                    "p_value": f"{p:.4f}" if not np.isnan(p) else "",
                })

    return pd.DataFrame(rows)


def build_table2(df: pd.DataFrame) -> pd.DataFrame:
    """Tumor and treatment characteristics among analysis-eligible."""
    eligible = df[df["analysis_eligible_flag"] == True].copy()  # noqa: E712
    rows = []

    cat_vars = [
        ("path_histology_raw", "Histology type"),
        ("ajcc8_stage_group", "AJCC 8th stage group"),
        ("ata_risk_category", "ATA risk category"),
        ("surg_procedure_type", "Procedure type"),
    ]
    for col, label in cat_vars:
        if col not in eligible.columns:
            continue
        for r in categorical_summary(eligible[col], label):
            rows.append(r)

    cont_vars = [
        ("path_tumor_size_cm", "Tumor size (cm)"),
        ("path_ln_positive_raw", "LN positive"),
        ("path_ln_examined_raw", "LN examined"),
        ("macis_score", "MACIS score"),
    ]
    for col, label in cont_vars:
        if col not in eligible.columns:
            continue
        s = pd.to_numeric(eligible[col], errors="coerce")
        rows.append(continuous_summary(s, label))

    bool_vars = [
        ("rai_received_flag", "RAI received"),
    ]
    for col, label in bool_vars:
        if col not in eligible.columns:
            continue
        n_true = int((eligible[col] == True).sum())  # noqa: E712
        total = len(eligible)
        rows.append({
            "Variable": label,
            "Category": "Yes",
            "N": n_true,
            "Pct": f"{100*n_true/total:.1f}%" if total else "0.0%",
        })

    return pd.DataFrame(rows)


def build_table3(df: pd.DataFrame) -> pd.DataFrame:
    """Outcomes among analysis-eligible patients."""
    eligible = df[df["analysis_eligible_flag"] == True].copy()  # noqa: E712
    total = len(eligible)
    rows = []

    outcome_flags = [
        ("any_recurrence_flag", "Any recurrence"),
        ("structural_recurrence_flag", "Structural recurrence"),
        ("biochemical_recurrence_flag", "Biochemical recurrence"),
        ("hypocalcemia_status", "Hypocalcemia (any)"),
        ("rln_permanent_flag", "RLN injury (permanent)"),
        ("rln_transient_flag", "RLN injury (transient)"),
    ]
    for col, label in outcome_flags:
        if col not in eligible.columns:
            continue
        if eligible[col].dtype == object:
            n_pos = int(eligible[col].str.lower().isin(["true", "yes", "confirmed", "confirmed_permanent", "confirmed_transient"]).sum())
        else:
            n_pos = int((eligible[col] == True).sum())  # noqa: E712
        rows.append({
            "Outcome": label,
            "N_events": n_pos,
            "Rate_pct": f"{100*n_pos/total:.1f}%" if total else "0.0%",
            "Total_at_risk": total,
        })

    tg_cols = [("tg_nadir", "Tg nadir (ng/mL)"), ("tg_last_value", "Tg last value (ng/mL)")]
    for col, label in tg_cols:
        if col not in eligible.columns:
            continue
        s = pd.to_numeric(eligible[col], errors="coerce").dropna()
        rows.append({
            "Outcome": label,
            "N_events": len(s),
            "Rate_pct": f"Median {s.median():.2f} [IQR {s.quantile(0.25):.2f}\u2013{s.quantile(0.75):.2f}]",
            "Total_at_risk": total,
        })

    return pd.DataFrame(rows)


def build_missingness_table(df: pd.DataFrame) -> pd.DataFrame:
    """Supplementary missingness table across key variables."""
    key_cols = [
        "path_histology_raw", "path_tumor_size_cm", "ajcc8_t_stage",
        "ajcc8_stage_group", "ata_risk_category", "macis_score",
        "imaging_tirads_best", "any_recurrence_flag", "structural_recurrence_flag",
        "biochemical_recurrence_flag", "recurrence_date", "surg_procedure_type",
        "rai_received_flag", "hypocalcemia_status", "rln_status",
        "tg_nadir", "tg_last_value", "path_ln_positive_raw",
        "path_ln_examined_raw", "molecular_eligible_flag", "surgery_date",
        "ata_response_category",
    ]
    rows = []
    total = len(df)
    for col in key_cols:
        if col not in df.columns:
            rows.append({"Variable": col, "Present_n": 0, "Missing_n": total,
                         "Missing_pct": "100.0%", "Note": "Column not in dataset"})
            continue
        missing = int(df[col].isna().sum())
        present = total - missing
        rows.append({
            "Variable": col,
            "Present_n": present,
            "Missing_n": missing,
            "Missing_pct": f"{100*missing/total:.1f}%" if total else "0.0%",
            "Note": "",
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run primary descriptive statistics")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--md", action="store_true", help="Use MotherDuck")
    grp.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print plan only")
    args = parser.parse_args()

    use_md = args.md
    if not args.md and not args.local:
        use_md = False

    section("62 · Primary Descriptive Statistics")

    if args.dry_run:
        print("[DRY-RUN] Would connect to", "MotherDuck" if use_md else "local DuckDB")
        print("[DRY-RUN] Would produce Tables 1-3, cohort flow, missingness")
        print("[DRY-RUN] Output dir:", TABLES_DIR)
        return

    con = get_connection(use_md)
    table = resolve_table(con, "manuscript_cohort_v1", "patient_analysis_resolved_v1")

    print("  Loading cohort ...")
    df = con.execute(f"SELECT * FROM {table}").fetchdf()
    print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")

    for col in df.select_dtypes(include=["object"]).columns:
        mask = df[col].apply(lambda x: x is True or x is False if not isinstance(x, str) else False)
        if mask.any():
            df[col] = df[col].map(lambda x: True if x is True or str(x).lower() == "true"
                                  else (False if x is False or str(x).lower() == "false" else x))

    # -- Cohort flow --
    section("Cohort Flow")
    flow = build_cohort_flow(df)
    print(flow.to_string(index=False))
    out = TABLES_DIR / "cohort_flow.csv"
    flow.to_csv(out, index=False)
    print(f"  -> {out}")

    # -- Table 1 --
    section("Table 1: Demographics")
    t1 = build_table1(df)
    print(f"  {len(t1)} rows")
    out = TABLES_DIR / "table1_demographics.csv"
    t1.to_csv(out, index=False)
    print(f"  -> {out}")

    # -- Table 2 --
    section("Table 2: Tumor & Treatment")
    t2 = build_table2(df)
    print(f"  {len(t2)} rows")
    out = TABLES_DIR / "table2_tumor_treatment.csv"
    t2.to_csv(out, index=False)
    print(f"  -> {out}")

    # -- Table 3 --
    section("Table 3: Outcomes")
    t3 = build_table3(df)
    print(f"  {len(t3)} rows")
    out = TABLES_DIR / "table3_outcomes.csv"
    t3.to_csv(out, index=False)
    print(f"  -> {out}")

    # -- Missingness --
    section("Supplementary: Missingness")
    miss = build_missingness_table(df)
    print(miss.to_string(index=False))
    out = TABLES_DIR / "supplementary_missingness.csv"
    miss.to_csv(out, index=False)
    print(f"  -> {out}")

    con.close()
    print(f"\n  All descriptive tables saved to {TABLES_DIR}")


if __name__ == "__main__":
    main()
