#!/usr/bin/env python3
"""mig_329_load_canonical_labs_thyroglobulin_bq — legacy parquet shim (MotherDuck → BigQuery).

DEPRECATED — MotherDuck was retired from the publication build chain (2026-05-14).
Analyst Thyroglobulin refreshes MUST use BigQuery-native mig_340:

  qc_framework_v1/migrations/mig_340_thyroglobulin_analyst_bq_rebuild.py
  qc_framework_v1/migrations/sql/mig_340_thy_canonical_from_analyst_raw.sql

This script remains ONLY for archival parity exercises (reload a parquet export from MotherDuck
into BigQuery).

Historic description (parity load):
Rebuilds thyroglobulin canonical parquet into:

  thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1

Companion view refreshes MUST follow the parquet load (manual or via mig_340 SQL):

  pub_canonical.thyroglobulin_lab_VIEW_v1

Source of historical row contents was MotherDuck ``main.canonical_labs_thyroglobulin_v1``
exported after ``scripts/347_lab_master_canonical_v1_build.py --commit``.

Prereq:
  Export parquet … gcloud auth / ``bq`` CLI for thyroid-canonical-pub-2026

Validation (historical Prompt 1): src vs tgt patients / joins.

Usage:

  .venv/bin/python qc_framework_v1/migrations/mig_329_load_canonical_labs_thyroglobulin_bq.py \\
      --parquet exports/bq_mig329/canonical_labs_thyroglobulin_v1.parquet
"""
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

_PROJECT = "thyroid-canonical-pub-2026"
_DATASET = "pub_canonical"
_TABLE = "canonical_labs_thyroglobulin_v1"
_LOCATION = "us-central1"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--parquet",
        type=Path,
        required=True,
        help="Parquet export of MotherDuck main.canonical_labs_thyroglobulin_v1",
    )
    ap.add_argument("--project", default=_PROJECT)
    ap.add_argument("--dataset", default=_DATASET)
    ap.add_argument("--location", default=_LOCATION)
    args = ap.parse_args()
    pq: Path = args.parquet
    if not pq.is_file():
        raise SystemExit(f"Missing parquet: {pq}")

    print(
        "[mig_329 WARN] Deprecated MotherDuck parity load. Thyroglobulin refresh -> mig_340 "
        "(qc_framework_v1/migrations/mig_340_thyroglobulin_analyst_bq_rebuild.py)."
    )

    dest = f"{args.project}:{args.dataset}.{_TABLE}"
    cmd = [
        "bq",
        f"--location={args.location}",
        "load",
        "--replace",
        "--source_format=PARQUET",
        "--clustering_fields=research_id",
        dest,
        str(pq.resolve()),
    ]
    print("[mig_329]", " ".join(cmd))
    rc = subprocess.call(cmd)
    if rc != 0:
        raise SystemExit(rc)
    print("[mig_329] Load complete. Recreate thyroglobulin_lab_VIEW_v1 in BQ:")
    print(
        "  CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026."
        "pub_canonical.thyroglobulin_lab_VIEW_v1` AS\\n"
        "  SELECT research_id, analyte, assay_method,\\n"
        "         lab_datetime AS specimen_collect_dt,\\n"
        "         value_raw AS result_raw, value_numeric AS result_numeric,\\n"
        "         is_censored, value_correction_note, unit_standardized,\\n"
        "         source AS ingestion_script,\\n"
        "         is_in_canonical_cancer_cohort, ingestion_date,\\n"
        "         analyte_assignment_method\\n"
        "  FROM `thyroid-canonical-pub-2026.pub_canonical."
        "canonical_labs_thyroglobulin_v1`;"
    )
    print("[mig_329] Then run validation SQL (historical Prompt 1). Prefer mig_340 validation SQL thereafter.")


if __name__ == "__main__":
    main()
