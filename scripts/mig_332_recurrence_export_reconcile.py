#!/usr/bin/env python3
"""
Companion to mig_332 — MotherDuck audit helpers (archive vs CPM, optional parquet export).

BigQuery reload of recurrence_histology / recurrence_evidence_source is **BQ-native** via
``bq_migrations/mig_101_canonical_recurrence_v1_bq_native_histology_evidence_20260514.sql``.
The former parquet → ``stg_canonical_recurrence_v1_mig332`` → mig_100 MERGE path is **deprecated**.

Requires MotherDuck publication DB + attached archive ("Thyroid 2026 UPdated"). Run AFTER:
  .venv/bin/python scripts/mig_332_recurrence_histology_evidence_apply.py --apply

Usage:
  .venv/bin/python scripts/mig_332_recurrence_export_reconcile.py --reconcile-archive-vs-cpm
  .venv/bin/python scripts/mig_332_recurrence_export_reconcile.py --export-parquet exports/stg_canonical_recurrence_v1_mig332.parquet
  .venv/bin/python scripts/mig_332_recurrence_export_reconcile.py --reconcile-archive-vs-cpm --export-parquet out.parquet
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402

ARCHIVE_RECUR = (
    '"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig284_20260503'
)


def reconcile_archive_vs_cpm(con) -> dict:
    """Row-level NULL-safe compare: pre_mig284 archive vs CPM recurrence columns."""
    sql = f"""
    WITH leg AS (
      SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        recurrence_histology AS leg_hist,
        recurrence_evidence_source AS leg_evidence
      FROM {ARCHIVE_RECUR}
    ),
    cpm AS (
      SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        recurrence_histology AS cpm_hist,
        recurrence_evidence_source AS cpm_evidence
      FROM main.canonical_patient_master
    ),
    j AS (
      SELECT
        leg.research_id,
        leg.leg_hist,
        leg.leg_evidence,
        cpm.cpm_hist,
        cpm.cpm_evidence,
        (leg.leg_hist IS NOT DISTINCT FROM cpm.cpm_hist) AS hist_match,
        (leg.leg_evidence IS NOT DISTINCT FROM cpm.cpm_evidence) AS evidence_match
      FROM leg
      INNER JOIN cpm ON leg.research_id = cpm.research_id
    )
    SELECT
      COUNT(*) AS n_joined,
      COUNT(*) FILTER (WHERE hist_match AND evidence_match) AS both_match,
      COUNT(*) FILTER (WHERE NOT hist_match) AS hist_mismatch,
      COUNT(*) FILTER (WHERE NOT evidence_match) AS evidence_mismatch,
      COUNT(*) FILTER (WHERE NOT hist_match OR NOT evidence_match) AS any_mismatch
    FROM j
    """
    row = con.execute(sql).fetchone()
    keys = ["n_joined", "both_match", "hist_mismatch", "evidence_mismatch", "any_mismatch"]
    return dict(zip(keys, [int(x) for x in row]))


def reconcile_feeder_vs_cpm(con) -> dict:
    """After mig_332: canonical_recurrence_v1 (VIEW) vs CPM."""
    sql = """
    WITH cr AS (
      SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        recurrence_histology AS cr_hist,
        recurrence_evidence_source AS cr_evidence
      FROM main.canonical_recurrence_v1
    ),
    cpm AS (
      SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        recurrence_histology AS cpm_hist,
        recurrence_evidence_source AS cpm_evidence
      FROM main.canonical_patient_master
    ),
    j AS (
      SELECT
        cr.research_id,
        (cr.cr_hist IS NOT DISTINCT FROM cpm.cpm_hist) AS hist_match,
        (cr.cr_evidence IS NOT DISTINCT FROM cpm.cpm_evidence) AS evidence_match
      FROM cr
      INNER JOIN cpm ON cr.research_id = cpm.research_id
    )
    SELECT
      COUNT(*) AS n_joined,
      COUNT(*) FILTER (WHERE hist_match AND evidence_match) AS both_match,
      COUNT(*) FILTER (WHERE NOT hist_match) AS hist_mismatch,
      COUNT(*) FILTER (WHERE NOT evidence_match) AS evidence_mismatch,
      COUNT(*) FILTER (WHERE NOT hist_match OR NOT evidence_match) AS any_mismatch
    FROM j
    """
    row = con.execute(sql).fetchone()
    keys = ["n_joined", "both_match", "hist_mismatch", "evidence_mismatch", "any_mismatch"]
    return dict(zip(keys, [int(x) for x in row]))


def export_parquet(con, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    p = str(out_path.resolve())
    # BQ MERGE keys as STRING
    con.execute(
        f"""
        COPY (
          SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            CAST(recurrence_histology AS VARCHAR) AS recurrence_histology,
            CAST(recurrence_evidence_source AS VARCHAR) AS recurrence_evidence_source
          FROM main.canonical_recurrence_v1
        ) TO '{p}' (FORMAT PARQUET)
        """
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reconcile-archive-vs-cpm", action="store_true")
    ap.add_argument("--reconcile-feeder-vs-cpm", action="store_true")
    ap.add_argument("--export-parquet", type=Path, default=None)
    args = ap.parse_args()

    if not args.reconcile_archive_vs_cpm and not args.reconcile_feeder_vs_cpm and not args.export_parquet:
        ap.error("pass at least one of --reconcile-archive-vs-cpm / --reconcile-feeder-vs-cpm / --export-parquet")

    con = connect_locked()
    out: dict = {}

    if args.reconcile_archive_vs_cpm:
        out["reconcile_archive_vs_cpm"] = reconcile_archive_vs_cpm(con)

    if args.reconcile_feeder_vs_cpm:
        try:
            out["reconcile_feeder_vs_cpm"] = reconcile_feeder_vs_cpm(con)
        except Exception as e:
            out["reconcile_feeder_vs_cpm_error"] = str(e)

    if args.export_parquet:
        export_parquet(con, args.export_parquet)
        out["export_parquet"] = str(args.export_parquet.resolve())

    con.close()
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
