#!/usr/bin/env python3
"""
mig_307d — sign off M025 v2.0 sensitivity publication outputs (post mig_307c).

Run after regenerating package workbooks:

  .venv/bin/python M025_FINAL_PACKAGE/build_m025_final_xlsx.py
  .venv/bin/python M025_FINAL_PACKAGE/m025_sensitivity_mig_307d.py

Safe to re-run: skips INSERT if mig_307d already exists.
"""

from __future__ import annotations

import os
import sys

_PKG_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _PKG_DIR)
sys.path.insert(0, os.path.dirname(_PKG_DIR))

import duckdb  # noqa: E402

from motherduck_client import get_token  # noqa: E402

DB = "thyroid_canonical_publication_v1_0"

MIG_SUMMARY = (
    "M025 v2.0 sensitivity-arm publication outputs after mig_307c: (1) Wilson 95% CIs "
    "for all ROM cells — manuscript_workspace exports augmented in "
    "M025_tables_and_summary.xlsx sheets Sensitivity_Era_Patient, "
    "Sensitivity_Era_Nodule, Sensitivity_Match_Window (lo_95/hi_95; window table "
    "rom_w*_lo_95/hi_95). "
    "(2) Sens/Spec/PPV/NPV with Wilson intervals at TR≥TR3, TR≥TR4, TR≥TR5 by "
    "pre_2017 vs post_2017 for patient and nodule_strict grains — "
    "m025_sensitivity_era_diagnostics_tr_thresholds.csv. "
    "(3) Per-era ROC AUC (ordinal TR 1–5; rank-sum) — m025_sensitivity_per_era_roc_auc.csv. "
    "(4) Figures (300 DPI PNG): M025_fig_sens_forest_tr4_tr5_rom_by_era.png, "
    "M025_fig_sens_rom_by_match_window.png under M025_FINAL_PACKAGE/06_figures_sensitivity/. "
    "Builder: M025_FINAL_PACKAGE/build_m025_final_xlsx.py + m025_sensitivity_lib.py."
)


def main() -> None:
    token = get_token()
    con = duckdb.connect(f"md:{DB}?motherduck_token={token}")
    n = con.execute(
        "SELECT COUNT(*) FROM main.signoff_migration WHERE mig_id = 'mig_307d'"
    ).fetchone()[0]
    if int(n) > 0:
        print("signoff_migration.mig_307d already present — skip INSERT")
        return
    con.execute(
        """
        INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
        VALUES ('mig_307d', CURRENT_TIMESTAMP, 'mig_307d_apply', ?)
        """,
        [MIG_SUMMARY],
    )
    print("INSERT signoff_migration mig_307d OK")


if __name__ == "__main__":
    main()
