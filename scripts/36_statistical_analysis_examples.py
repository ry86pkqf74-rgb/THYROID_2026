#!/usr/bin/env python3
"""
36_statistical_analysis_examples.py — Statistical Analysis Toolkit Demo

Demonstrates all ThyroidStatisticalAnalyzer capabilities against the
publication-ready lakehouse:

  1. Table 1 (grouped by sex and BRAF)
  2. Hypothesis test battery with FDR correction
  3. Logistic regression (recurrence ~ clinical predictors)
  4. Cox proportional hazards (time-to-event ~ ETE + confounders)
  5. Forest plot export
  6. Correlation matrix

Outputs to studies/statistical_analysis_examples/

Usage:
    .venv/bin/python scripts/36_statistical_analysis_examples.py
    .venv/bin/python scripts/36_statistical_analysis_examples.py --local
    .venv/bin/python scripts/36_statistical_analysis_examples.py --md
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
from typing import Any

import numpy as np
import pandas as pd

np.random.seed(42)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient, MotherDuckConfig
from utils.statistical_analysis import (
    ThyroidStatisticalAnalyzer,
    THYROID_PREDICTORS,
    THYROID_SURVIVAL,
    THYROID_NSQIP_OUTCOMES,
    THYROID_NSQIP_PREDICTORS,
    LONGITUDINAL_MARKERS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stat_examples")

OUT_DIR = ROOT / "studies" / "statistical_analysis_examples"


def _get_connection(args):
    if args.local or os.getenv("USE_LOCAL_DUCKDB", "").lower() in ("1", "true", "yes"):
        import duckdb
        local_path = os.getenv("LOCAL_DUCKDB_PATH", str(ROOT / "thyroid_master_local.duckdb"))
        log.info("Connecting to local DuckDB: %s", local_path)
        return duckdb.connect(local_path)
    log.info("Connecting to MotherDuck (thyroid_research_2026)...")
    cfg = MotherDuckConfig(database="thyroid_research_2026")
    return MotherDuckClient(cfg).connect_rw()


def _safe_query(con, sql: str) -> pd.DataFrame:
    try:
        return con.execute(sql).fetchdf()
    except Exception as exc:
        log.warning("Query failed: %s", exc)
        return pd.DataFrame()


def phase1_table1(analyzer: ThyroidStatisticalAnalyzer, df: pd.DataFrame, out_dir: Path) -> None:
    log.info("Phase 1: Table 1 — Cohort Description")

    for group_col in [None, "sex", "braf_positive"]:
        suffix = f"_by_{group_col}" if group_col else ""
        t1_df, meta = analyzer.generate_table_one(
            data=df, groupby_col=group_col,
        )
        if "error" in meta:
            log.warning("  Table 1%s: %s", suffix, meta["error"])
            continue

        out_file = out_dir / f"table1{suffix}.csv"
        export_df = t1_df.reset_index() if hasattr(t1_df.index, "names") and len(t1_df.index.names) > 1 else t1_df
        export_df.to_csv(out_file, index=True)
        log.info("  Table 1%s: %d rows -> %s", suffix, len(t1_df), out_file.name)


def phase2_hypothesis_tests(analyzer: ThyroidStatisticalAnalyzer, df: pd.DataFrame, out_dir: Path) -> None:
    log.info("Phase 2: Hypothesis Testing Battery")

    target = "event_occurred" if "event_occurred" in df.columns else "braf_positive"
    if target not in df.columns:
        log.warning("  No suitable target variable found")
        return

    features = [p for p in THYROID_PREDICTORS if p in df.columns and p != target]
    if not features:
        log.warning("  No testable features found")
        return

    results = analyzer.run_hypothesis_tests(df, target, features, correction="fdr_bh")
    if results.empty or "error" in results.columns:
        log.warning("  Hypothesis tests failed")
        return

    results.to_csv(out_dir / "hypothesis_tests.csv", index=False)
    n_sig = int(results["significant"].sum()) if "significant" in results.columns else 0
    log.info("  %d tests, %d significant (FDR-corrected) -> hypothesis_tests.csv", len(results), n_sig)

    for _, row in results[results.get("significant", False) == True].iterrows():
        log.info("    %s: %s, p_adj=%.4f, effect=%s",
                 row["variable"], row["test_used"],
                 row.get("p_adjusted", row["p_value"]), row.get("effect_label", ""))


def phase3_logistic(analyzer: ThyroidStatisticalAnalyzer, df: pd.DataFrame, out_dir: Path) -> None:
    log.info("Phase 3: Logistic Regression")

    outcome = "event_occurred" if "event_occurred" in df.columns else "recurrence_flag"
    if outcome not in df.columns:
        log.warning("  No binary outcome found")
        return

    predictors = [p for p in THYROID_PREDICTORS if p in df.columns and p != outcome
                  and pd.api.types.is_numeric_dtype(df[p])][:8]
    if not predictors:
        log.warning("  No usable predictors")
        return

    result = analyzer.fit_logistic_regression(outcome=outcome, predictors=predictors, data=df)
    if "error" in result:
        log.error("  Logistic regression: %s", result["error"])
        return

    or_table = result["or_table"]
    or_table.to_csv(out_dir / "logistic_or_table.csv", index=False)
    log.info("  Logistic: %d obs, pseudo-R²=%.4f, AUC=%s -> logistic_or_table.csv",
             result["n_obs"], result["pseudo_r2"], result.get("auc", "N/A"))

    if not result.get("vif", pd.DataFrame()).empty:
        result["vif"].to_csv(out_dir / "logistic_vif.csv", index=False)

    for w in result.get("warnings", []):
        log.warning("  %s", w)

    sig = or_table[(or_table["p_value"] < 0.05) & (or_table["predictor"] != "const")]
    for _, row in sig.iterrows():
        log.info("    %s: OR=%.2f (%.2f-%.2f), p=%.4f",
                 row["predictor"], row["OR"], row["CI_lower"], row["CI_upper"], row["p_value"])


def phase4_cox(analyzer: ThyroidStatisticalAnalyzer, df: pd.DataFrame, out_dir: Path) -> None:
    log.info("Phase 4: Cox Proportional Hazards")

    time_col = THYROID_SURVIVAL["time_col"]
    event_col = THYROID_SURVIVAL["event_col"]
    if time_col not in df.columns or event_col not in df.columns:
        log.warning("  Missing survival columns (%s, %s)", time_col, event_col)
        return

    predictors = [p for p in THYROID_PREDICTORS if p in df.columns
                  and p not in (time_col, event_col)
                  and pd.api.types.is_numeric_dtype(df[p])][:8]
    if not predictors:
        log.warning("  No usable covariates")
        return

    result = analyzer.fit_cox_ph(time_col=time_col, event_col=event_col, predictors=predictors, data=df)
    if "error" in result:
        log.error("  Cox PH: %s", result["error"])
        return

    hr_table = result["hr_table"]
    hr_table.to_csv(out_dir / "cox_hr_table.csv", index=False)
    log.info("  Cox PH: %d obs, %d events, concordance=%.4f -> cox_hr_table.csv",
             result["n_obs"], result["n_events"], result["concordance"])

    for w in result.get("warnings", []):
        log.warning("  %s", w)

    sig = hr_table[hr_table["p_value"] < 0.05]
    for _, row in sig.iterrows():
        log.info("    %s: HR=%.2f (%.2f-%.2f), p=%.4f",
                 row["covariate"], row["HR"], row["CI_lower"], row["CI_upper"], row["p_value"])


def phase5_forest_plot(analyzer: ThyroidStatisticalAnalyzer, out_dir: Path) -> None:
    log.info("Phase 5: Forest Plot Export")

    hr_path = out_dir / "cox_hr_table.csv"
    or_path = out_dir / "logistic_or_table.csv"

    for src, est_col, label_col, fname in [
        (hr_path, "HR", "covariate", "forest_plot_cox.html"),
        (or_path, "OR", "predictor", "forest_plot_logistic.html"),
    ]:
        if not src.exists():
            continue
        df = pd.read_csv(src)
        df = df[df[label_col] != "const"] if label_col == "predictor" else df
        forest_df = df.rename(columns={
            label_col: "label", est_col: "estimate",
            "CI_lower": "ci_lower", "CI_upper": "ci_upper",
        })
        if forest_df.empty:
            continue
        try:
            fig = analyzer.create_forest_plot(forest_df, title=f"Forest Plot ({est_col})")
            fig.write_html(str(out_dir / fname))
            log.info("  %s -> %s", est_col, fname)
        except Exception as exc:
            log.warning("  Forest plot (%s) failed: %s", est_col, exc)


def phase6_correlation(analyzer: ThyroidStatisticalAnalyzer, df: pd.DataFrame, out_dir: Path) -> None:
    log.info("Phase 6: Correlation Matrix")

    numeric_predictors = [p for p in THYROID_PREDICTORS if p in df.columns
                          and pd.api.types.is_numeric_dtype(df[p])]
    if len(numeric_predictors) < 2:
        log.warning("  Fewer than 2 numeric predictors")
        return

    corr, pval = analyzer.correlation_matrix_with_pvalues(df, numeric_predictors, method="spearman")
    if corr.empty:
        log.warning("  Correlation computation failed")
        return

    corr.to_csv(out_dir / "correlation_matrix.csv")
    pval.to_csv(out_dir / "correlation_pvalues.csv")
    log.info("  %dx%d correlation matrix -> correlation_matrix.csv", *corr.shape)

    try:
        fig = analyzer.create_correlation_heatmap(corr, pval)
        fig.write_html(str(out_dir / "correlation_heatmap.html"))
        log.info("  Heatmap -> correlation_heatmap.html")
    except Exception as exc:
        log.warning("  Heatmap export failed: %s", exc)


def phase7_longitudinal(
    analyzer: ThyroidStatisticalAnalyzer,
    con: Any,
    out_dir: Path,
) -> None:
    """Phase 7: Longitudinal Tg/TSH mixed-effects trajectory analysis."""
    log.info("Phase 7: Longitudinal Biomarker Analysis (Mixed-Effects)")

    for marker in ["tg", "tsh"]:
        label = LONGITUDINAL_MARKERS[marker]["label"]
        log.info("  Fitting mixed-effects model for %s...", label)
        result = analyzer.longitudinal_summary(marker=marker)

        if "error" in result:
            log.warning("  %s: %s", label, result["error"])
            continue

        for w in result.get("warnings", []):
            log.warning("  %s", w)

        log.info(
            "  %s: n=%d patients, %d obs, slope=%.4f/yr, p=%s",
            label,
            result["n_patients"],
            result["n_obs"],
            result["slope"],
            f"{result['p_value']:.4f}" if result.get("p_value") else "N/A",
        )
        log.info("  → %s", result["clinical_note"])

        pp = result.get("per_patient_summary", pd.DataFrame())
        if not pp.empty:
            out_file = out_dir / f"longitudinal_{marker}_per_patient.csv"
            pp.to_csv(out_file, index=False)
            log.info("  Per-patient summary -> %s", out_file.name)

        summary_path = out_dir / f"longitudinal_{marker}_summary.txt"
        summary_path.write_text(result["model_summary"])
        log.info("  Model summary -> %s", summary_path.name)


def phase8_power_analysis(out_dir: Path) -> None:
    """Phase 8: Power and sample-size calculations for thyroid-specific hypotheses."""
    log.info("Phase 8: Power & Sample-Size Analysis")

    rows = []

    # H1: BRAF+ vs BRAF- recurrence rate difference
    r1 = ThyroidStatisticalAnalyzer.power_two_proportions(
        p1=0.15, p2=0.05, alpha=0.05, power=0.80
    )
    rows.append({
        "hypothesis": "BRAF+ vs BRAF− recurrence (15% vs 5%)",
        "formula": r1["formula"],
        "n_per_group": r1["n_per_group"],
        "n_total": r1["n_total"],
        "effect_size": r1["effect_size_h"],
        "alpha": r1["alpha"],
        "power": r1["power"],
    })
    log.info(
        "  H1 BRAF recurrence: n_per_group=%d, n_total=%d (h=%.3f)",
        r1["n_per_group"], r1["n_total"], r1["effect_size_h"],
    )

    # H2: OR=2.0 for ETE in logistic regression (event rate 10%)
    r2 = ThyroidStatisticalAnalyzer.power_logistic(
        p_event=0.10, or_detect=2.0, alpha=0.05, power=0.80
    )
    rows.append({
        "hypothesis": "ETE vs recurrence (OR=2.0, baseline 10%)",
        "formula": r2["formula"],
        "n_per_group": None,
        "n_total": r2["n_total"],
        "effect_size": None,
        "alpha": r2["alpha"],
        "power": r2["power"],
    })
    log.info("  H2 ETE logistic: n_total=%s", r2["n_total"])

    # H3: Log-rank for HR=1.8, event rate 10%
    r3 = ThyroidStatisticalAnalyzer.sample_size_km(
        hr=1.8, alpha=0.05, power=0.80, event_rate=0.10
    )
    rows.append({
        "hypothesis": "Cox log-rank: HR=1.8, 10% event rate",
        "formula": r3["formula"],
        "n_per_group": r3.get("n_group1"),
        "n_total": r3["n_total"],
        "effect_size": None,
        "alpha": r3["alpha"],
        "power": r3["power"],
    })
    log.info(
        "  H3 KM log-rank: events_required=%d, n_total=%s",
        r3["events_required"], r3["n_total"],
    )

    # H4: Hypocalcemia complication rate (NSQIP — 20% vs 10%)
    r4 = ThyroidStatisticalAnalyzer.power_two_proportions(
        p1=0.20, p2=0.10, alpha=0.05, power=0.80
    )
    rows.append({
        "hypothesis": "Hypocalcemia rate total vs completion thyroid (20% vs 10%)",
        "formula": r4["formula"],
        "n_per_group": r4["n_per_group"],
        "n_total": r4["n_total"],
        "effect_size": r4["effect_size_h"],
        "alpha": r4["alpha"],
        "power": r4["power"],
    })
    log.info("  H4 Hypocalcemia: n_per_group=%d", r4["n_per_group"])

    out_file = out_dir / "power_analysis.csv"
    pd.DataFrame(rows).to_csv(out_file, index=False)
    log.info("  Power analysis -> %s", out_file.name)


def run(args: argparse.Namespace) -> int:
    t_start = time.perf_counter()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    con = _get_connection(args)
    analyzer = ThyroidStatisticalAnalyzer(con)

    resolved = analyzer.resolve_view()
    if resolved is None:
        log.error("No analytic views available. Run materialization scripts first.")
        return 1

    log.info("Using view: %s", resolved)
    df = _safe_query(con, f"SELECT * FROM {resolved}")
    if df.empty:
        log.error("No data returned from %s", resolved)
        return 1

    log.info("Loaded %d rows x %d columns from %s", *df.shape, resolved)

    phase1_table1(analyzer, df, OUT_DIR)
    phase2_hypothesis_tests(analyzer, df, OUT_DIR)
    phase3_logistic(analyzer, df, OUT_DIR)
    phase4_cox(analyzer, df, OUT_DIR)
    phase5_forest_plot(analyzer, OUT_DIR)
    phase6_correlation(analyzer, df, OUT_DIR)
    phase7_longitudinal(analyzer, con, OUT_DIR)
    phase8_power_analysis(OUT_DIR)

    elapsed = time.perf_counter() - t_start
    metadata = {
        "script": "36_statistical_analysis_examples.py",
        "timestamp": datetime.now().isoformat(),
        "random_seed": 42,
        "source_view": resolved,
        "cohort_rows": len(df),
        "elapsed_seconds": round(elapsed, 2),
        "outputs": sorted(f.name for f in OUT_DIR.iterdir() if f.is_file()),
    }
    with open(OUT_DIR / "analysis_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print()
    print("=" * 68)
    print("  Statistical Analysis Examples — Summary")
    print(f"  Timestamp: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Elapsed:   {elapsed:.1f}s")
    print(f"  Cohort:    {len(df):,} patients from {resolved}")
    print("=" * 68)
    for f in sorted(OUT_DIR.iterdir()):
        if f.is_file():
            print(f"  +  {f.name}")
    print("=" * 68)
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run statistical analysis examples on the thyroid cohort."
    )
    ap.add_argument("--local", action="store_true", help="Use local DuckDB")
    ap.add_argument("--md", action="store_true", help="Use MotherDuck (default)")
    args = ap.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
