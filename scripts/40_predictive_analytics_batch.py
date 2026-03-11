#!/usr/bin/env python3
"""
40_predictive_analytics_batch.py -- Batch runner for predictive analytics.

Runs the full predictive analytics pipeline:
  1. Multi-model comparison (KM, Cox PH, PTCM, Mixture Cure, Penalized Cox, RSF)
  2. Competing risks (Aalen-Johansen CIF + cause-specific Cox)
  3. Batch PTCM cure scoring for all patients
  4. Manuscript report generation

Outputs to exports/predictive_analytics/.

Usage:
    python scripts/40_predictive_analytics_batch.py [--md] [--local] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

EXPORT_DIR = ROOT / "exports" / "predictive_analytics"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def _get_con(use_md: bool, use_local: bool):
    import duckdb
    if use_local:
        return duckdb.connect(str(ROOT / "thyroid_master.duckdb")), False
    if use_md:
        token = os.environ.get("MOTHERDUCK_TOKEN", "")
        if not token:
            try:
                import toml
                token = toml.load(str(ROOT / ".streamlit" / "secrets.toml")).get(
                    "MOTHERDUCK_TOKEN", ""
                )
            except Exception:
                pass
        if not token:
            print("  ERROR: MOTHERDUCK_TOKEN not set.")
            sys.exit(1)
        con = duckdb.connect(f"md:?motherduck_token={token}")
        con.execute("USE thyroid_research_2026;")
        return con, True
    return duckdb.connect(str(ROOT / "thyroid_master.duckdb")), False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true", help="Read from MotherDuck")
    parser.add_argument("--local", action="store_true", help="Force local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Skip heavy operations")
    args = parser.parse_args()

    print("\n" + "=" * 80)
    print("  40 -- Predictive Analytics Batch Runner")
    print("=" * 80 + "\n")

    con, is_md = _get_con(args.md, args.local)

    from utils.predictive_analytics import (
        ThyroidPredictiveAnalyzer,
        PREDICTIVE_PRESETS,
    )

    pa = ThyroidPredictiveAnalyzer(con)
    print(f"  PTCM available: {pa.ptcm_available}")
    print(f"  Source: {'MotherDuck' if is_md else 'local DuckDB'}\n")

    ts = datetime.now().strftime("%Y%m%d_%H%M")

    # ── 1. Model comparison ──────────────────────────────────────────────
    print("  [1/4] Multi-model comparison…")
    t0 = time.time()
    comp = pa.compare_survival_models()
    if "error" not in comp:
        df = comp["comparison_table"]
        out = EXPORT_DIR / f"model_comparison_{ts}.csv"
        df.to_csv(out, index=False)
        print(f"        {len(df)} models compared → {out.name} ({time.time() - t0:.1f}s)")
        for _, row in df.iterrows():
            print(f"        {row['Model']:<28} C={row['Concordance']!s:<8} AIC={row['AIC']!s:<10} {row['Notes']}")
    else:
        print(f"        FAILED: {comp['error']}")
    print()

    # ── 2. Competing risks ───────────────────────────────────────────────
    print("  [2/4] Competing risks (recurrence vs death)…")
    t0 = time.time()
    preset = PREDICTIVE_PRESETS["recurrence"]
    cr = pa.fit_competing_risks(
        time_col=preset["time_col"],
        event_col=preset["event_col"],
        competing_event_col=preset["competing_event_col"],
        predictors=preset["predictors"],
    )
    if "error" not in cr:
        out = EXPORT_DIR / f"competing_risks_summary_{ts}.csv"
        cr["summary_table"].to_csv(out, index=False)
        print(f"        N={cr['n_obs']:,} | Primary={cr['n_events']} | Competing={cr['n_competing']}")
        print(f"        → {out.name} ({time.time() - t0:.1f}s)")
        for label, hr in cr.get("cause_specific_hrs", {}).items():
            print(f"        Cause-specific Cox ({label}): C-index={hr['concordance']}")
    else:
        print(f"        FAILED: {cr['error']}")
    print()

    # ── 3. Batch PTCM cure scoring ───────────────────────────────────────
    if pa.ptcm_available and not args.dry_run:
        print("  [3/4] Batch PTCM cure scoring…")
        t0 = time.time()
        try:
            cohort = con.execute("SELECT * FROM promotion_cure_cohort").fetchdf()
            scored = pa.predict_cure_batch(cohort)
            if not scored.empty:
                out = EXPORT_DIR / f"ptcm_batch_scores_{ts}.csv"
                scored.to_csv(out, index=False)
                print(f"        Scored {len(scored):,} patients → {out.name} ({time.time() - t0:.1f}s)")
                tiers = scored["cure_tier"].value_counts()
                for tier, n in tiers.items():
                    print(f"        {tier}: {n:,}")
        except Exception as exc:
            print(f"        FAILED: {exc}")
    else:
        print("  [3/4] Batch scoring skipped (PTCM not available or --dry-run)")
    print()

    # ── 4. Manuscript report ─────────────────────────────────────────────
    if not args.dry_run:
        print("  [4/4] Manuscript report…")
        t0 = time.time()
        report = pa.generate_manuscript_report(
            sections=["PTCM", "CompetingRisks", "Comparison"],
        )
        if "error" not in report:
            print(f"        → {report['path']} ({time.time() - t0:.1f}s)")
        else:
            print(f"        FAILED: {report['error']}")
    else:
        print("  [4/4] Report skipped (--dry-run)")

    # ── Summary ──────────────────────────────────────────────────────────
    con.close()
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "source": "MotherDuck" if is_md else "local",
        "ptcm_available": pa.ptcm_available,
        "models_compared": comp.get("n_models", 0) if "error" not in comp else 0,
        "competing_risks_n": cr.get("n_obs", 0) if "error" not in cr else 0,
        "dry_run": args.dry_run,
    }
    manifest_path = EXPORT_DIR / f"manifest_{ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("\n" + "=" * 80)
    print("  PREDICTIVE ANALYTICS BATCH COMPLETE")
    print(f"  Outputs: {EXPORT_DIR}")
    print(f"  Manifest: {manifest_path.name}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
