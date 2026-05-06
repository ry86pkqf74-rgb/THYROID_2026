#!/usr/bin/env python3
"""mig_324_load_ctc_bq — load mig_323 parquet into BigQuery pub_canonical CTC.

Creates/replaces BASE TABLE:
  thyroid-canonical-pub-2026.pub_canonical.canonical_tumor_characteristics_v1

Clustering: research_id

Prereq:
  * gcloud auth application-default (or active creds for thyroid-canonical-pub-2026)
  * bq CLI
  * Parquet from 323_export_ctc_md_to_parquet.py

Usage:
  .venv/bin/python qc_framework_v1/migrations/324_load_ctc_bq.py \\
      --parquet exports/bq_ctc_mig323/canonical_tumor_characteristics_v1.parquet
"""
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

_PROJECT = "thyroid-canonical-pub-2026"
_DATASET = "pub_canonical"
_TABLE = "canonical_tumor_characteristics_v1"
_LOCATION = "us-central1"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--parquet",
        type=Path,
        required=True,
        help="Parquet from mig_323_export_ctc_md_to_parquet.py",
    )
    ap.add_argument("--project", default=_PROJECT)
    ap.add_argument("--dataset", default=_DATASET)
    ap.add_argument("--location", default=_LOCATION)
    args = ap.parse_args()
    pq: Path = args.parquet
    if not pq.is_file():
        raise SystemExit(f"Missing parquet: {pq}")

    dest = f"{args.project}:{args.dataset}.{_TABLE}"
    # bq load positional: destination table, source, schema (omit for autodetect from parquet)
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
    print("[mig_324]", " ".join(cmd))
    rc = subprocess.call(cmd)
    if rc != 0:
        raise SystemExit(rc)
    print("[mig_324] Load complete. Run parity + 325/326.")


if __name__ == "__main__":
    main()
