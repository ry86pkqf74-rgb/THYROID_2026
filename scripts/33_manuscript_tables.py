#!/usr/bin/env python3
"""
33_manuscript_tables.py — Formatted Manuscript Tables

Reads the analytic model outputs from studies/analytic_models/ and
produces publication-ready formatted tables in Word-compatible format
(CSV with clean headers) and LaTeX.

Outputs: studies/manuscript_tables/
  - Table1_Demographics.csv
  - Table2_Cox_Multivariable.csv
  - Table3_KaplanMeier_Summary.csv
  - Table4_Subgroup_Event_Rates.csv
  - Table5_Interaction_Tests.csv
  - Table6_PSM_ETE.csv
  - all_tables.tex  (LaTeX for direct manuscript inclusion)

Usage:
    .venv/bin/python scripts/33_manuscript_tables.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = ROOT / "studies" / "analytic_models"
OUTPUT_DIR = ROOT / "studies" / "manuscript_tables"


def _read_csv(name: str) -> pd.DataFrame:
    path = INPUT_DIR / name
    if not path.exists():
        print(f"  [SKIP] {name} — not found")
        return pd.DataFrame()
    return pd.read_csv(path)


def _fmt_p(p) -> str:
    if pd.isna(p):
        return ""
    p = float(p)
    if p < 0.001:
        return "<0.001"
    if p < 0.01:
        return f"{p:.3f}"
    return f"{p:.2f}"


def _fmt_ci(hr, lo, hi) -> str:
    return f"{hr:.2f} ({lo:.2f}–{hi:.2f})"


def table1() -> pd.DataFrame | None:
    df = _read_csv("table1_demographics.csv")
    if df.empty:
        return None
    df.columns = ["Characteristic", "Result", "n"]
    print(f"  Table 1: {len(df)} rows")
    return df


def table2_cox() -> pd.DataFrame | None:
    df = _read_csv("cox_model.csv")
    if df.empty:
        return None
    out = pd.DataFrame({
        "Variable": df["covariate"],
        "HR (95% CI)": [_fmt_ci(r["exp(coef)"], r["exp(coef) lower 95%"],
                                r["exp(coef) upper 95%"])
                        for _, r in df.iterrows()],
        "p-value": df["p"].apply(_fmt_p),
    })
    print(f"  Table 2: {len(out)} covariates")
    return out


def table3_km() -> pd.DataFrame | None:
    df = _read_csv("km_summary.csv")
    if df.empty:
        return None
    out = pd.DataFrame({
        "Stratifier": df["Stratifier"],
        "Group": df["Stratum"],
        "N": df["N"],
        "Events": df["Events"],
        "1-yr (%)": (df["1yr_survival"] * 100).round(1),
        "3-yr (%)": (df["3yr_survival"] * 100).round(1),
        "5-yr (%)": (df["5yr_survival"] * 100).round(1),
    })
    print(f"  Table 3: {len(out)} strata")
    return out


def table4_subgroups() -> pd.DataFrame | None:
    df = _read_csv("subgroup_event_rates.csv")
    if df.empty:
        return None
    df.columns = ["Subgroup", "N", "Events", "Event Rate (%)"]
    print(f"  Table 4: {len(df)} subgroups")
    return df


def table5_interactions() -> pd.DataFrame | None:
    df = _read_csv("interaction_tests.csv")
    if df.empty:
        return None
    out = pd.DataFrame({
        "Interaction": df["Interaction"],
        "HR (95% CI)": [_fmt_ci(r["HR"], r["CI_lower"], r["CI_upper"])
                        for _, r in df.iterrows()],
        "p-value": df["p_value"].apply(_fmt_p),
        "AIC improved": df.get("AIC_improved", "").astype(str),
    })
    print(f"  Table 5: {len(out)} interactions")
    return out


def table6_psm() -> pd.DataFrame | None:
    psm_path = INPUT_DIR / "psm_result.json"
    balance_path = INPUT_DIR / "psm_balance.csv"
    km_path = INPUT_DIR / "psm_km_summary.csv"

    rows = []

    if psm_path.exists():
        with open(psm_path) as f:
            psm = json.load(f)
        rows.append(("Matched pairs", str(psm.get("N_matched_pairs", ""))))
        rows.append(("HR ETE (95% CI)",
                      _fmt_ci(psm["HR_ETE"], psm["CI_lower"], psm["CI_upper"])))
        rows.append(("p-value", _fmt_p(psm["p_value"])))
        rows.append(("Concordance", f'{psm.get("Concordance", ""):.4f}'))

    if balance_path.exists():
        bal = pd.read_csv(balance_path)
        for _, r in bal.iterrows():
            status = "balanced" if r.get("Balanced") else "IMBALANCED"
            rows.append((f"Balance: {r['Variable']}",
                          f"SMD={r['SMD']:.3f} ({status})"))

    if km_path.exists():
        km = pd.read_csv(km_path)
        for _, r in km.iterrows():
            rows.append((f"KM {r['Group']}: 5-yr survival",
                          f"{r.get('5yr', '')}" if pd.notna(r.get('5yr')) else "N/A"))

    if not rows:
        print("  [SKIP] Table 6 — no PSM results")
        return None

    out = pd.DataFrame(rows, columns=["Measure", "Value"])
    print(f"  Table 6: {len(out)} rows")
    return out


def _to_latex(tables: dict[str, pd.DataFrame], out_path: Path) -> None:
    lines = [
        "% Auto-generated manuscript tables",
        f"% Generated: {datetime.now():%Y-%m-%d %H:%M}",
        "% Script: 33_manuscript_tables.py",
        "",
    ]
    for title, df in tables.items():
        safe_title = title.replace("_", r"\_")
        lines.append(f"\\begin{{table}}[htbp]")
        lines.append(f"\\centering")
        lines.append(f"\\caption{{{safe_title}}}")
        cols = "l" + "r" * (len(df.columns) - 1)
        lines.append(f"\\begin{{tabular}}{{{cols}}}")
        lines.append("\\toprule")
        lines.append(" & ".join(str(c) for c in df.columns) + " \\\\")
        lines.append("\\midrule")
        for _, row in df.iterrows():
            lines.append(" & ".join(str(v) for v in row.values) + " \\\\")
        lines.append("\\bottomrule")
        lines.append(f"\\end{{tabular}}")
        lines.append(f"\\end{{table}}")
        lines.append("")

    out_path.write_text("\n".join(lines))


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print("  Manuscript Tables Generator")
    print(f"  Input:  {INPUT_DIR.relative_to(ROOT)}")
    print(f"  Output: {OUTPUT_DIR.relative_to(ROOT)}")
    print("=" * 68)

    tables: dict[str, pd.DataFrame] = {}

    for name, func in [
        ("Table1_Demographics", table1),
        ("Table2_Cox_Multivariable", table2_cox),
        ("Table3_KaplanMeier_Summary", table3_km),
        ("Table4_Subgroup_Event_Rates", table4_subgroups),
        ("Table5_Interaction_Tests", table5_interactions),
        ("Table6_PSM_ETE", table6_psm),
    ]:
        result = func()
        if result is not None and not result.empty:
            result.to_csv(OUTPUT_DIR / f"{name}.csv", index=False)
            tables[name] = result

    if tables:
        _to_latex(tables, OUTPUT_DIR / "all_tables.tex")
        print(f"\n  LaTeX → all_tables.tex")

    print()
    print("=" * 68)
    for f in sorted(OUTPUT_DIR.iterdir()):
        if f.is_file():
            print(f"  ✓  {f.name}")
    print("=" * 68)
    print()
    print("  Tables ready for manuscript insertion.")
    print("  Copy CSVs into Word/Google Docs, or include all_tables.tex in LaTeX.")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
