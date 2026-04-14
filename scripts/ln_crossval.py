#!/usr/bin/env python3
"""
LN Cross-Validation — compare lymph node counts across 3 independent sources.

Sources:
  1. tumor_pathology   (primary, richest — 249 cols, 4,290 rows)
  2. path_synoptics    (fallback, VARCHAR LN fields — 11,688 rows)
  3. patient_refined_master_clinical_v12 (aggregated rollup — 12,886 rows)

Outputs:
  output/ln_crossval.parquet   — per-patient discordance report
  Console summary              — discordance rates and internal consistency stats

Usage:
  .venv/bin/python scripts/ln_crossval.py          # parquet backup (local)
  .venv/bin/python scripts/ln_crossval.py --md     # MotherDuck cloud
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

OUTPUT_DIR = REPO / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def _connect(use_md: bool) -> duckdb.DuckDBPyConnection:
    if use_md:
        from motherduck_client import MotherDuckClient
        client = MotherDuckClient.for_env("prod")
        return client.connect_rw()
    return duckdb.connect()


def _register_parquets(con: duckdb.DuckDBPyConnection) -> None:
    """Register local parquet backups as views so SQL works identically."""
    backup = REPO / "output" / "parquet_backup"
    for name in ("tumor_pathology", "path_synoptics", "patient_refined_master_clinical_v12"):
        pq = backup / f"{name}.parquet"
        if not pq.exists():
            raise FileNotFoundError(f"Missing parquet backup: {pq}")
        con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{pq}')")


CROSSVAL_SQL = """
WITH tp AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        primary_ln_ln_total_examined   AS tp_ln_examined,
        primary_ln_ln_total_positive   AS tp_ln_positive,
        ln_total_examined_from_locations AS tp_ln_exam_from_locs,
        ln_total_positive_from_locations AS tp_ln_pos_from_locs,
        histology_1_ln_examined        AS tp_h1_ln_examined,
        histology_1_ln_positive        AS tp_h1_ln_positive
    FROM tumor_pathology
),
ps AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        TRY_CAST(tumor_1_ln_examined AS INTEGER) AS ps_ln_examined,
        TRY_CAST(tumor_1_ln_involved AS INTEGER) AS ps_ln_positive
    FROM path_synoptics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS VARCHAR) ORDER BY ps_ln_examined DESC NULLS LAST) = 1
),
mc AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        TRY_CAST(ln_total_examined AS INTEGER) AS mc_ln_examined,
        TRY_CAST(ln_total_positive AS INTEGER) AS mc_ln_positive
    FROM patient_refined_master_clinical_v12
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS VARCHAR) ORDER BY mc_ln_examined DESC NULLS LAST) = 1
)
SELECT
    tp.research_id,

    -- Source values
    tp.tp_ln_examined,
    tp.tp_ln_positive,
    tp.tp_ln_exam_from_locs,
    tp.tp_ln_pos_from_locs,
    tp.tp_h1_ln_examined,
    tp.tp_h1_ln_positive,

    ps.ps_ln_examined,
    ps.ps_ln_positive,

    mc.mc_ln_examined,
    mc.mc_ln_positive,

    -- Cross-source discordance (examined): tp vs ps
    CASE
        WHEN tp.tp_ln_examined IS NULL OR ps.ps_ln_examined IS NULL THEN 'insufficient_data'
        WHEN ABS(tp.tp_ln_examined - ps.ps_ln_examined) <= 1 THEN 'agree'
        ELSE 'discordant'
    END AS tp_vs_ps_examined,

    -- Cross-source discordance (examined): tp vs mc
    CASE
        WHEN tp.tp_ln_examined IS NULL OR mc.mc_ln_examined IS NULL THEN 'insufficient_data'
        WHEN ABS(tp.tp_ln_examined - mc.mc_ln_examined) <= 1 THEN 'agree'
        ELSE 'discordant'
    END AS tp_vs_mc_examined,

    -- Cross-source discordance (positive): tp vs ps
    CASE
        WHEN tp.tp_ln_positive IS NULL OR ps.ps_ln_positive IS NULL THEN 'insufficient_data'
        WHEN ABS(tp.tp_ln_positive - ps.ps_ln_positive) <= 1 THEN 'agree'
        ELSE 'discordant'
    END AS tp_vs_ps_positive,

    -- Cross-source discordance (positive): tp vs mc
    CASE
        WHEN tp.tp_ln_positive IS NULL OR mc.mc_ln_positive IS NULL THEN 'insufficient_data'
        WHEN ABS(tp.tp_ln_positive - mc.mc_ln_positive) <= 1 THEN 'agree'
        ELSE 'discordant'
    END AS tp_vs_mc_positive,

    -- Internal consistency: does per-location sum match reported total?
    CASE
        WHEN tp.tp_ln_examined IS NULL AND tp.tp_ln_exam_from_locs IS NULL THEN 'insufficient_data'
        WHEN tp.tp_ln_exam_from_locs IS NULL THEN 'no_location_breakdown'
        WHEN tp.tp_ln_examined IS NULL THEN 'location_only'
        WHEN ABS(tp.tp_ln_examined - tp.tp_ln_exam_from_locs) <= 1 THEN 'ok'
        ELSE 'mismatch'
    END AS internal_examined_consistency,

    CASE
        WHEN tp.tp_ln_positive IS NULL AND tp.tp_ln_pos_from_locs IS NULL THEN 'insufficient_data'
        WHEN tp.tp_ln_pos_from_locs IS NULL THEN 'no_location_breakdown'
        WHEN tp.tp_ln_positive IS NULL THEN 'location_only'
        WHEN ABS(tp.tp_ln_positive - tp.tp_ln_pos_from_locs) <= 1 THEN 'ok'
        ELSE 'mismatch'
    END AS internal_positive_consistency,

    -- Histology-1 vs primary LN consistency
    CASE
        WHEN tp.tp_ln_examined IS NULL OR tp.tp_h1_ln_examined IS NULL THEN 'insufficient_data'
        WHEN ABS(tp.tp_ln_examined - tp.tp_h1_ln_examined) <= 1 THEN 'ok'
        ELSE 'mismatch'
    END AS h1_vs_primary_examined,

    -- Overall cross-val verdict
    CASE
        WHEN (tp.tp_ln_examined IS NOT NULL AND ps.ps_ln_examined IS NOT NULL
              AND ABS(tp.tp_ln_examined - ps.ps_ln_examined) > 1)
          OR (tp.tp_ln_examined IS NOT NULL AND mc.mc_ln_examined IS NOT NULL
              AND ABS(tp.tp_ln_examined - mc.mc_ln_examined) > 1)
          OR (tp.tp_ln_positive IS NOT NULL AND ps.ps_ln_positive IS NOT NULL
              AND ABS(tp.tp_ln_positive - ps.ps_ln_positive) > 1)
          OR (tp.tp_ln_positive IS NOT NULL AND mc.mc_ln_positive IS NOT NULL
              AND ABS(tp.tp_ln_positive - mc.mc_ln_positive) > 1)
        THEN 'discordant'
        WHEN tp.tp_ln_examined IS NOT NULL
             AND (ps.ps_ln_examined IS NOT NULL OR mc.mc_ln_examined IS NOT NULL)
        THEN 'agree'
        WHEN tp.tp_ln_examined IS NOT NULL
        THEN 'single_source_only'
        ELSE 'no_data'
    END AS crossval_status

FROM tp
LEFT JOIN ps ON tp.research_id = ps.research_id
LEFT JOIN mc ON tp.research_id = mc.research_id
ORDER BY tp.research_id
"""


def run(use_md: bool = False) -> pd.DataFrame:
    con = _connect(use_md)
    if not use_md:
        _register_parquets(con)

    print("Running LN cross-validation query...")
    df = con.execute(CROSSVAL_SQL).fetchdf()
    print(f"  Total rows: {len(df)}")

    out_path = OUTPUT_DIR / "ln_crossval.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  Saved: {out_path}")

    # --- Summary stats ---
    print("\n" + "=" * 70)
    print("CROSS-VALIDATION SUMMARY")
    print("=" * 70)

    print(f"\nTotal patients (tumor_pathology): {len(df)}")

    for label, col in [
        ("TP vs PS (examined)", "tp_vs_ps_examined"),
        ("TP vs MC (examined)", "tp_vs_mc_examined"),
        ("TP vs PS (positive)", "tp_vs_ps_positive"),
        ("TP vs MC (positive)", "tp_vs_mc_positive"),
    ]:
        vc = df[col].value_counts()
        total_comparable = vc.get("agree", 0) + vc.get("discordant", 0)
        discordant = vc.get("discordant", 0)
        rate = f"{discordant/total_comparable*100:.1f}%" if total_comparable > 0 else "N/A"
        print(f"\n{label}:")
        for k, v in sorted(vc.items()):
            print(f"    {k:25s}  {v:>5d}")
        print(f"    Discordance rate: {rate} ({discordant}/{total_comparable})")

    print("\n--- Internal Consistency (per-location sum vs reported total) ---")
    for label, col in [
        ("Examined", "internal_examined_consistency"),
        ("Positive", "internal_positive_consistency"),
    ]:
        vc = df[col].value_counts()
        print(f"\n  {label}:")
        for k, v in sorted(vc.items()):
            print(f"    {k:25s}  {v:>5d}")

    print("\n--- Histology-1 vs Primary LN (examined) ---")
    vc = df["h1_vs_primary_examined"].value_counts()
    for k, v in sorted(vc.items()):
        print(f"    {k:25s}  {v:>5d}")

    print("\n--- Overall Cross-Val Verdict ---")
    vc = df["crossval_status"].value_counts()
    for k, v in sorted(vc.items()):
        print(f"    {k:25s}  {v:>5d}")

    # Show examples of discordant rows
    disc = df[df["crossval_status"] == "discordant"]
    if len(disc) > 0:
        print(f"\n--- Sample discordant rows (first 10 of {len(disc)}) ---")
        sample_cols = [
            "research_id",
            "tp_ln_examined", "ps_ln_examined", "mc_ln_examined",
            "tp_ln_positive", "ps_ln_positive", "mc_ln_positive",
        ]
        print(disc[sample_cols].head(10).to_string(index=False))

    con.close()
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LN cross-validation across 3 sources")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck instead of local parquet")
    args = parser.parse_args()
    run(use_md=args.md)
