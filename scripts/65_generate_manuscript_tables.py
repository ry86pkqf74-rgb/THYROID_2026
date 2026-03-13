#!/usr/bin/env python3
"""
65_generate_manuscript_tables.py -- Format analysis CSVs into manuscript tables

Reads CSVs from exports/manuscript_tables/ and exports/manuscript_analysis/.
Produces:
  - Markdown tables (.md) for each Table 1-3 + supplementary
  - LaTeX tables (.tex) in booktabs style
  - Adds caveat footnotes for provisional fields

No database connection needed (reads CSVs only).
Supports --dry-run.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TABLES_DIR = ROOT / "exports" / "manuscript_tables"
ANALYSIS_DIR = ROOT / "exports" / "manuscript_analysis"
TABLES_DIR.mkdir(parents=True, exist_ok=True)

TODAY = datetime.now().strftime("%Y%m%d_%H%M")

PROVISIONAL_FOOTNOTES = {
    "ata_response_category": "ATA response-to-therapy classification is provisional; "
                             "requires structured Tg + imaging follow-up data.",
    "biochemical_recurrence_flag": "Biochemical recurrence defined as rising Tg > 1.0 ng/mL "
                                   "and > 2x nadir without structural disease; threshold-dependent.",
    "macis_score": "MACIS score calculable only for patients with age, tumor size, "
                   "resection completeness, and metastasis data.",
}


def section(title: str) -> None:
    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print(f"{'=' * 72}\n")


# ---------------------------------------------------------------------------
# Markdown formatting
# ---------------------------------------------------------------------------

def df_to_markdown(df: pd.DataFrame, title: str, footnotes: list[str] | None = None) -> str:
    lines = [f"## {title}", ""]
    if df.empty:
        lines.append("*No data available.*\n")
        return "\n".join(lines)

    headers = list(df.columns)
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for _, row in df.iterrows():
        vals = [str(v) if pd.notna(v) else "" for v in row]
        lines.append("| " + " | ".join(vals) + " |")

    lines.append("")
    if footnotes:
        for fn in footnotes:
            lines.append(f"*{fn}*  ")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LaTeX formatting (booktabs)
# ---------------------------------------------------------------------------

def _escape_latex(s: str) -> str:
    for ch in ("&", "%", "$", "#", "_", "{", "}"):
        s = s.replace(ch, f"\\{ch}")
    s = s.replace("~", r"\textasciitilde{}")
    s = s.replace("\u2013", "--")
    return s


def df_to_latex(df: pd.DataFrame, caption: str, label: str,
                footnotes: list[str] | None = None) -> str:
    if df.empty:
        return f"% Table {label}: no data available\n"

    ncols = len(df.columns)
    col_spec = "l" + "r" * (ncols - 1)

    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        f"\\caption{{{_escape_latex(caption)}}}",
        f"\\label{{tab:{label}}}",
        f"\\begin{{tabular}}{{{col_spec}}}",
        r"\toprule",
    ]

    headers = [_escape_latex(str(c)) for c in df.columns]
    lines.append(" & ".join(headers) + r" \\")
    lines.append(r"\midrule")

    for _, row in df.iterrows():
        vals = [_escape_latex(str(v)) if pd.notna(v) else "" for v in row]
        lines.append(" & ".join(vals) + r" \\")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")

    if footnotes:
        lines.append(r"\begin{tablenotes}\footnotesize")
        for i, fn in enumerate(footnotes, 1):
            lines.append(f"\\item[{i}] {_escape_latex(fn)}")
        lines.append(r"\end{tablenotes}")

    lines.append(r"\end{table}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------------------

def format_cohort_flow(tables_dir: Path) -> tuple[str, str]:
    fp = tables_dir / "cohort_flow.csv"
    if not fp.exists():
        return "", ""
    df = pd.read_csv(fp)
    md = df_to_markdown(df, "Cohort Flow")
    tex = df_to_latex(df, "Cohort inclusion cascade", "cohort_flow")
    return md, tex


def format_table1(tables_dir: Path) -> tuple[str, str]:
    fp = tables_dir / "table1_demographics.csv"
    if not fp.exists():
        return "", ""
    df = pd.read_csv(fp)
    md = df_to_markdown(df, "Table 1: Patient Demographics")
    tex = df_to_latex(df, "Patient demographics stratified by analysis eligibility",
                      "demographics")
    return md, tex


def format_table2(tables_dir: Path) -> tuple[str, str]:
    fp = tables_dir / "table2_tumor_treatment.csv"
    if not fp.exists():
        return "", ""
    df = pd.read_csv(fp)
    fns = [PROVISIONAL_FOOTNOTES["macis_score"]]
    md = df_to_markdown(df, "Table 2: Tumor and Treatment Characteristics", fns)
    tex = df_to_latex(df, "Tumor and treatment characteristics", "tumor_treatment", fns)
    return md, tex


def format_table3(tables_dir: Path) -> tuple[str, str]:
    fp = tables_dir / "table3_outcomes.csv"
    if not fp.exists():
        return "", ""
    df = pd.read_csv(fp)
    fns = [
        PROVISIONAL_FOOTNOTES["biochemical_recurrence_flag"],
        PROVISIONAL_FOOTNOTES["ata_response_category"],
    ]
    md = df_to_markdown(df, "Table 3: Clinical Outcomes", fns)
    tex = df_to_latex(df, "Clinical outcomes", "outcomes", fns)
    return md, tex


def format_missingness(tables_dir: Path) -> tuple[str, str]:
    fp = tables_dir / "supplementary_missingness.csv"
    if not fp.exists():
        return "", ""
    df = pd.read_csv(fp)
    md = df_to_markdown(df, "Supplementary Table: Variable Missingness")
    tex = df_to_latex(df, "Variable missingness across key analysis fields",
                      "missingness")
    return md, tex


def format_logistic_models(analysis_dir: Path) -> tuple[str, str]:
    fp = analysis_dir / "primary_logistic_models.csv"
    if not fp.exists():
        return "", ""
    df = pd.read_csv(fp)
    display = df[["Model", "Variable", "OR", "CI_lower", "CI_upper", "p_value"]].copy()
    display["OR_CI"] = display.apply(
        lambda r: f"{r['OR']:.2f} ({r['CI_lower']:.2f}\u2013{r['CI_upper']:.2f})", axis=1
    )
    display = display[["Model", "Variable", "OR_CI", "p_value"]]
    md = df_to_markdown(display, "Supplementary Table: Logistic Regression Models")
    tex = df_to_latex(display, "Multivariable logistic regression models", "logistic_models")
    return md, tex


def format_cox_results(analysis_dir: Path) -> tuple[str, str]:
    fp = analysis_dir / "cox_ph_results.csv"
    if not fp.exists():
        return "", ""
    df = pd.read_csv(fp)
    display = df[["Variable", "HR", "CI_lower", "CI_upper", "p_value"]].copy()
    display["HR_CI"] = display.apply(
        lambda r: f"{r['HR']:.2f} ({r['CI_lower']:.2f}\u2013{r['CI_upper']:.2f})", axis=1
    )
    display = display[["Variable", "HR_CI", "p_value"]]
    md = df_to_markdown(display, "Table 4: Cox Proportional Hazards Model")
    tex = df_to_latex(display, "Multivariable Cox proportional hazards model", "cox_ph")
    return md, tex


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate formatted manuscript tables")
    parser.add_argument("--dry-run", action="store_true", help="Print plan only")
    args = parser.parse_args()

    section("65 · Generate Manuscript Tables")

    if args.dry_run:
        print("[DRY-RUN] Would format CSVs into markdown + LaTeX tables")
        print(f"  Input:  {TABLES_DIR} + {ANALYSIS_DIR}")
        print(f"  Output: {TABLES_DIR}")
        return

    formatters = [
        ("cohort_flow", format_cohort_flow, TABLES_DIR),
        ("table1_demographics", format_table1, TABLES_DIR),
        ("table2_tumor_treatment", format_table2, TABLES_DIR),
        ("table3_outcomes", format_table3, TABLES_DIR),
        ("supplementary_missingness", format_missingness, TABLES_DIR),
        ("logistic_models", format_logistic_models, ANALYSIS_DIR),
        ("cox_ph", format_cox_results, ANALYSIS_DIR),
    ]

    all_md = [
        "# THYROID 2026 Manuscript Tables",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "> **Denominator standards:** All rates reference explicit denominators per "
        "`manuscript_metrics_v2`. Molecular prevalences (BRAF, RAS) use the molecular-tested "
        "denominator (N = 10,025). All other rates use the full surgical cohort (N = 10,871) "
        "unless otherwise noted. Cancer-specific outcomes use the analysis-eligible denominator "
        "(N = 4,136). See `docs/manuscript_metric_reconciliation_20260313.md` for definitions.",
        "",
    ]
    all_tex = [
        r"% THYROID 2026 Manuscript Tables",
        f"% Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        r"% Requires: booktabs, threeparttable packages",
        "",
    ]

    for name, formatter, source_dir in formatters:
        print(f"  Formatting {name} ...")
        md_text, tex_text = formatter(source_dir)

        if md_text:
            all_md.append(md_text)
            md_path = TABLES_DIR / f"{name}.md"
            md_path.write_text(md_text, encoding="utf-8")
            print(f"    -> {md_path}")

        if tex_text:
            all_tex.append(tex_text)
            tex_path = TABLES_DIR / f"{name}.tex"
            tex_path.write_text(tex_text, encoding="utf-8")
            print(f"    -> {tex_path}")

        if not md_text and not tex_text:
            print("    [SKIP] Source CSV not found")

    combined_md = TABLES_DIR / "all_tables.md"
    combined_md.write_text("\n\n".join(all_md), encoding="utf-8")
    print(f"\n  Combined markdown -> {combined_md}")

    combined_tex = TABLES_DIR / "all_tables.tex"
    combined_tex.write_text("\n\n".join(all_tex), encoding="utf-8")
    print(f"  Combined LaTeX    -> {combined_tex}")

    print(f"\n  All manuscript tables saved to {TABLES_DIR}")


if __name__ == "__main__":
    main()
