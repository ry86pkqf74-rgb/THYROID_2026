#!/usr/bin/env python3
"""
M048 — Handoff README builder.

Produces m048_handoff_README.md containing ONLY:
  - Run metadata (db tag, mig_id, timestamp, git sha)
  - Reference table of every headline number
  - File pointers
  - Explicit caveats

NO narrative interpretation, NO abstract, NO discussion text.
"""
from __future__ import annotations
import json
import os
import sys

import numpy as np
import pandas as pd

STUDY_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(STUDY_DIR, "..", ".."))
FIG_DIR = os.path.join(REPO_ROOT, "M048_submission_package", "figures")


def load(fname: str) -> pd.DataFrame:
    path = os.path.join(STUDY_DIR, fname)
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()


def fmt(v, decimals: int = 2) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    return f"{v:.{decimals}f}"


def build_readme() -> str:
    # Load snapshot
    snap_path = os.path.join(STUDY_DIR, "m048_run_snapshot.json")
    snap = json.load(open(snap_path)) if os.path.exists(snap_path) else {}

    df_auc = load("m048_auc_by_race.csv")
    df_rom = load("m048_rom_by_race_x_tr.csv")
    df_thr = load("m048_threshold_metrics.csv")
    df_inf = load("m048_inflation_by_race.csv")
    df_chi = load("m048_feature_chi_square.csv")
    df_fna = load("m048_fna_compliance_by_race.csv")
    df_gates = load("m048_qa_gates.csv")
    df_beth = load("m048_bethesda_x_race_x_tr.csv")

    def auc_row(grain: str, race: str) -> str:
        sub = df_auc[(df_auc["grain"] == grain) & (df_auc["race_strat"] == race)]
        if sub.empty:
            return "—"
        r = sub.iloc[0]
        auc = fmt(r.get("auc"), 4)
        lo = fmt(r.get("auc_ci_lo_95"), 4)
        hi = fmt(r.get("auc_ci_hi_95"), 4)
        ci_flag = " ⚠️ CI includes 0.5" if r.get("ci_includes_0_5") else ""
        return f"{auc} [{lo}–{hi}]{ci_flag}"

    def rom_row(grain: str, race: str, tr: str) -> str:
        sub = df_rom[(df_rom["grain"] == grain) & (df_rom["race_strat"] == race) & (df_rom["tr_category"] == tr)]
        if sub.empty:
            return "—"
        r = sub.iloc[0]
        n = int(r.get("n_total", 0))
        k = int(r.get("n_malignant", 0))
        pct = fmt(r.get("rom_pct"), 1)
        lo = fmt(r.get("rom_lo_95"), 1)
        hi = fmt(r.get("rom_hi_95"), 1)
        return f"{k}/{n} = {pct}% [{lo}–{hi}%]"

    def thr_row(grain: str, race: str, thr: str, metric: str) -> str:
        sub = df_thr[(df_thr["grain"] == grain) & (df_thr["race_strat"] == race) & (df_thr["threshold"] == thr)]
        if sub.empty:
            return "—"
        r = sub.iloc[0]
        pct_col = f"{metric}_pct"
        lo_col = f"{metric}_lo_95"
        hi_col = f"{metric}_hi_95"
        pct = fmt(r.get(pct_col), 1)
        lo = fmt(r.get(lo_col), 1)
        hi = fmt(r.get(hi_col), 1)
        return f"{pct}% [{lo}–{hi}%]"

    lines = []

    lines.append("# M048 Handoff README — Racial Disparities in ACR TI-RADS Performance")
    lines.append("")
    lines.append("> **SCOPE FOR THIS DOCUMENT:** Numbers, paths, and caveats ONLY.")
    lines.append("> No narrative interpretation, no abstract, no discussion text.")
    lines.append("> All values are pre-verified via independent_recompute.py (hard QA gate).")
    lines.append("")

    # --- Run Metadata ---
    lines.append("## Run Metadata")
    lines.append("")
    lines.append(f"| Key | Value |")
    lines.append(f"|-----|-------|")
    lines.append(f"| Study ID | M048 |")
    lines.append(f"| Database | {snap.get('db_name', 'thyroid_canonical_publication_v1_0')} |")
    lines.append(f"| DB Tag | {snap.get('db_tag', 'pub_v1_1')} |")
    lines.append(f"| Mig ID | {snap.get('mig_id', 'mig_315')} |")
    lines.append(f"| Run Timestamp (UTC) | {snap.get('run_timestamp_utc', '—')} |")
    lines.append(f"| Git SHA | {snap.get('git_sha', '—')} |")
    lines.append(f"| Bootstrap replicates | {snap.get('n_boot_auc', 1000)} |")
    gates_pass = snap.get('qa_gates_pass', False)
    lines.append(f"| QA Gates | {'✓ ALL PASS' if gates_pass else '✗ FAILURES — CHECK m048_qa_gates.csv'} |")
    lines.append("")

    # --- QA Gate Summary ---
    lines.append("## QA Gate Summary")
    lines.append("")
    if not df_gates.empty:
        lines.append("| Gate | Status | Actual | Expected |")
        lines.append("|------|--------|--------|---------|")
        for _, r in df_gates.iterrows():
            lines.append(f"| {r.get('gate', '—')} | {r.get('status', '—')} | {r.get('actual', '—')} | {r.get('expected', '—')} |")
    else:
        lines.append("_(QA gates CSV not found)_")
    lines.append("")

    # --- AUC Table ---
    lines.append("## AUC by Race and Grain (Bootstrap 95% CI; 1,000 replicates)")
    lines.append("")
    lines.append("| Grain | Race | AUC [95% CI] | N | N positive |")
    lines.append("|-------|------|-------------|---|-----------|")
    if not df_auc.empty:
        for _, r in df_auc.sort_values(["grain", "race_strat"]).iterrows():
            grain = r.get("grain", "—")
            race = r.get("race_strat", "—")
            auc_val = fmt(r.get("auc"), 4)
            lo = fmt(r.get("auc_ci_lo_95"), 4)
            hi = fmt(r.get("auc_ci_hi_95"), 4)
            n = int(r.get("n", 0))
            np_ = int(r.get("n_positive", 0))
            ci_flag = " ⚠️" if r.get("ci_includes_0_5") else ""
            lines.append(f"| {grain} | {race} | {auc_val} [{lo}–{hi}]{ci_flag} | {n:,} | {np_:,} |")
    lines.append("")

    # --- ROM by Race × TR ---
    lines.append("## ROM by Race × TI-RADS (Wilson 95% CI)")
    lines.append("")
    lines.append("### Patient Grain")
    lines.append("")
    lines.append("| Race | TR1 | TR2 | TR3 | TR4 | TR5 |")
    lines.append("|------|-----|-----|-----|-----|-----|")
    for race in ["Black", "White", "Asian", "POOLED"]:
        row_vals = [rom_row("patient", race, f"TR{t}") for t in range(1, 6)]
        lines.append(f"| {race} | " + " | ".join(row_vals) + " |")
    lines.append("")
    lines.append("### Nodule Strict Grain")
    lines.append("")
    lines.append("| Race | TR1 | TR2 | TR3 | TR4 | TR5 |")
    lines.append("|------|-----|-----|-----|-----|-----|")
    for race in ["Black", "White", "Asian", "POOLED"]:
        row_vals = [rom_row("nodule_strict", race, f"TR{t}") for t in range(1, 6)]
        lines.append(f"| {race} | " + " | ".join(row_vals) + " |")
    lines.append("")

    # --- Threshold Metrics ---
    lines.append("## Threshold Metrics (Wilson 95% CI) — TR≥TR4, Patient Grain")
    lines.append("")
    lines.append("| Race | Sensitivity | Specificity | PPV | NPV |")
    lines.append("|------|-------------|-------------|-----|-----|")
    for race in ["Black", "White", "Asian"]:
        sens = thr_row("patient", race, "TR>=TR4", "sens")
        spec = thr_row("patient", race, "TR>=TR4", "spec")
        ppv = thr_row("patient", race, "TR>=TR4", "ppv")
        npv = thr_row("patient", race, "TR>=TR4", "npv")
        lines.append(f"| {race} | {sens} | {spec} | {ppv} | {npv} |")
    lines.append("")

    # --- Inflation ---
    lines.append("## Patient–Nodule ROM Inflation by Race × TR (percentage points)")
    lines.append("")
    lines.append("| Race | TR4 Inflation (pp) | TR5 Inflation (pp) |")
    lines.append("|------|-------------------|-------------------|")
    if not df_inf.empty:
        for race in ["Black", "White", "Asian"]:
            tr4 = df_inf[(df_inf["race_strat"] == race) & (df_inf["tr_category"] == "TR4")]
            tr5 = df_inf[(df_inf["race_strat"] == race) & (df_inf["tr_category"] == "TR5")]
            tr4_val = fmt(tr4.iloc[0]["inflation_pp"], 1) if not tr4.empty else "—"
            tr5_val = fmt(tr5.iloc[0]["inflation_pp"], 1) if not tr5.empty else "—"
            lines.append(f"| {race} | {tr4_val} | {tr5_val} |")
    lines.append("")

    # --- Feature Chi-square ---
    lines.append("## Feature Distribution Chi-Square Results (Bonferroni α=0.01)")
    lines.append("")
    lines.append("| Feature | chi² | df | p (raw) | p (Bonferroni) | Cramér's V | Significant? |")
    lines.append("|---------|------|----|---------|-----------------|-----------:|-------------|")
    if not df_chi.empty:
        for _, r in df_chi.iterrows():
            sig = "YES ✓" if r.get("significant_bonferroni") else "no"
            lines.append(
                f"| {r.get('feature','—')} | {fmt(r.get('chi2'),2)} | {r.get('df','—')} | "
                f"{fmt(r.get('p_raw'),5)} | {fmt(r.get('p_bonferroni_adj'),5)} | "
                f"{fmt(r.get('cramers_v'),3)} | {sig} |"
            )
    lines.append("")

    # --- FNA Audit ---
    lines.append("## FNA Compliance Audit (per race, TR≥TR4 threshold)")
    lines.append("")
    lines.append("| Race | N above thr | TP | FP | FN | TN |")
    lines.append("|------|-------------|----|----|----|----|")
    if not df_fna.empty:
        for _, r in df_fna.iterrows():
            race = r.get("race_strat", "—")
            lines.append(
                f"| {race} | {int(r.get('n_above_thr',0)):,} | "
                f"{int(r.get('tp_thr4',0)):,} | {int(r.get('fp_thr4',0)):,} | "
                f"{int(r.get('fn_thr4',0)):,} | {int(r.get('tn_thr4',0)):,} |"
            )
    lines.append("")

    # --- File Paths ---
    lines.append("## File Paths")
    lines.append("")
    lines.append("| File | Path |")
    lines.append("|------|------|")
    csv_files = [
        "m048_run_snapshot.json",
        "m048_qa_gates.csv",
        "m048_diagnostic_performance.csv",
        "m048_rom_by_race_x_tr.csv",
        "m048_auc_by_race.csv",
        "m048_threshold_metrics.csv",
        "m048_feature_distribution.csv",
        "m048_feature_chi_square.csv",
        "m048_fna_compliance_by_race.csv",
        "m048_bethesda_x_race_x_tr.csv",
        "m048_inflation_by_race.csv",
        "verification/m025_reconciliation.csv",
        "verification/independent_recompute_results.csv",
        "verification/cortex_smoke_tests.md",
    ]
    study_rel = "studies/m048_racial_disparities_tirads"
    for f in csv_files:
        lines.append(f"| {f} | `{study_rel}/{f}` |")
    fig_files = [
        "Figure_1_Cohort_Flow_by_Race.png",
        "Figure_2_ROC_by_Race.png",
        "Figure_3_ROM_by_Race_Patient.png",
        "Figure_3b_ROM_by_Race_Nodule.png",
        "Figure_4_Inflation_by_Race.png",
        "Figure_5_Feature_Distribution.png",
        "Figure_S1_Bethesda_x_Race_x_TR.png",
    ]
    fig_rel = "M048_submission_package/figures"
    for f in fig_files:
        lines.append(f"| {f} | `{fig_rel}/{f}` |")
    lines.append("")

    # --- Caveats ---
    lines.append("## Mandatory Caveats (Writer Must Acknowledge)")
    lines.append("")
    lines.append("1. **Asian stratum power:** n=204 patients; AUC CIs are wide. "
                 "Flag any Asian stratum result where the bootstrap 95% CI includes 0.5.")
    lines.append("2. **Race is self-reported** from EHR at time of clinical encounter. "
                 "Use standard disclosure language (e.g., 'self-reported race').")
    lines.append("3. **Multiple comparisons across per-TR ROM comparisons:** "
                 "Per-race per-TR ROM comparisons are descriptive and not formally corrected for multiple testing. "
                 "The Bonferroni correction applies only to the 5 feature-distribution chi-square tests.")
    lines.append("4. **Observational cohort bias:** This is a surgical cohort enriched for "
                 "suspicious nodules. Absolute ROM values are higher than screening populations.")
    lines.append("5. **Feature score completeness:** Chi-square tests restricted to "
                 "strict-eligible nodules with complete feature component data. "
                 "Missing data pattern should be described in Methods.")
    lines.append("")

    return "\n".join(lines)


def main():
    readme_content = build_readme()
    out_path = os.path.join(STUDY_DIR, "m048_handoff_README.md")
    with open(out_path, "w") as f:
        f.write(readme_content)
    print(f"[WRITTEN] {out_path}")


if __name__ == "__main__":
    main()
