#!/usr/bin/env python3
"""mig_329_load_canonical_labs_thyroglobulin_bq — rebuild BQ thyroglobulin canonicals from MotherDuck.

Replaces:
  thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1

And refreshes the legacy-shaped companion view (run separately in BQ or via
MOTHERDUCK parity exports):

  pub_canonical.thyroglobulin_lab_VIEW_v1

Source of truth for row contents: MotherDuck ``main.canonical_labs_thyroglobulin_v1``
rebuilt by ``scripts/347_lab_master_canonical_v1_build.py --commit``
(Script 347 — full-timestamp dedup + full longitudinal Tg/TgAb name coverage).

Prereq:
  1. Re-run Script 347 on MotherDuck with --commit.
  2. Export parquet (example):

       duckdb / MotherDuck: COPY (SELECT * FROM main.canonical_labs_thyroglobulin_v1)
       TO 'exports/bq_mig329/canonical_labs_thyroglobulin_v1.parquet'
       (FORMAT PARQUET, COMPRESSION ZSTD);

  3. gcloud auth / ``bq`` CLI for thyroid-canonical-pub-2026

Validation (BigQuery):

  -- See Prompt 1 acceptance queries (src vs tgt patients = 3258, ~55k rows).

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
    print("[mig_329] Then run validation SQL (Prompt 1).")


if __name__ == "__main__":
    main()
