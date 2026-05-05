#!/usr/bin/env python3
"""Verify mig_319 cohort view + repaired m083_dual_platform analytic view on MotherDuck.

Usage:
  .venv/bin/python scripts/verify_mig319_m083_cohort_view.py

Exits non-zero if hard gates fail. Discordance % vs published cross-lab benchmarks is
informational only (this cohort uses Afirma classifier vs ThyroSeq NGS).
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token


def main() -> int:
    tok = get_token()
    if not tok:
        print("ERROR: No MotherDuck RW token (see motherduck_client.get_token).", file=sys.stderr)
        return 2
    con = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={tok}")

    row_m083 = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.m083_dual_platform_analytic_v1"
    ).fetchone()
    row_cohort = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1"
    ).fetchone()
    if row_m083 is None or row_cohort is None:
        print("FAIL: COUNT query returned no row", file=sys.stderr)
        return 1
    n_m083 = row_m083[0]
    n_cohort = row_cohort[0]
    if n_m083 != n_cohort:
        print(f"FAIL: m083 ({n_m083}) vs cohort_m083 ({n_cohort}) row count mismatch", file=sys.stderr)
        return 1
    if not (130 <= n_cohort <= 200):
        print(f"FAIL: cohort row count {n_cohort} not in [130, 200]", file=sys.stderr)
        return 1

    path_row = con.execute(
        """
        SELECT COUNT(*) AS n_total,
               COUNT(path_braf_status) AS n_path,
               ROUND(100.0 * COUNT(path_braf_status) / COUNT(*), 1) AS pct
        FROM manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1
        """
    ).fetchone()
    assert path_row is not None
    _n_total, n_path, pct_path = path_row
    if pct_path < 40.0:
        print(f"FAIL: path_braf_status coverage {pct_path}% < 40%", file=sys.stderr)
        return 1

    disc = con.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE dual_platform_discordant_flag IS NOT NULL) AS evaluable,
          COUNT(*) FILTER (WHERE dual_platform_discordant_flag IS TRUE) AS discordant_true,
          ROUND(
            100.0 * COUNT(*) FILTER (WHERE dual_platform_discordant_flag IS TRUE)
            / NULLIF(COUNT(*) FILTER (WHERE dual_platform_discordant_flag IS NOT NULL), 0),
            1
          ) AS discordance_pct
        FROM manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1
        """
    ).fetchone()
    assert disc is not None
    evaluable, discordant_true, discordance_pct = disc

    sig = con.execute(
        "SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_319'"
    ).fetchone()
    if sig is None:
        print("WARN: mig_319 not present in main.signoff_migration yet", file=sys.stderr)

    print("mig_319 verification — PASS (hard gates)")
    print(f"  m083_dual_platform_analytic_v1 rows: {n_m083}")
    print(f"  cohort_m083_braf_dual_platform_discordance_v1 rows: {n_cohort}")
    print(f"  path_braf_status filled: {n_path}/{_n_total} ({pct_path}%)")
    print(
        f"  dual_platform discordant (evaluable): {discordant_true}/{evaluable} "
        f"({discordance_pct}%) — expected elevated vs single-assay literature benchmarks"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
