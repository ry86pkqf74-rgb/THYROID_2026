#!/usr/bin/env python3
"""
mig_340 — BigQuery-native rebuild of canonical_labs_thyroglobulin_v1 from the analyst Thyroglobulin CSV.

Loads the authoritative EHR pull (same row universe as Thyroid Thyroglobulin Lab_20251120.csv) into
``pub_raw.thyroglobulin_analyst_ehr_20251120`` with all columns typed STRING, snapshots the existing
canonical to ``pub_archive.canonical_labs_thyroglobulin_v1_pre_tgrebuild_20260514`` when present, then runs
the SQL bundle in qc_framework_v1/migrations/sql/mig_340_thy_canonical_from_analyst_raw.sql.

This supersedes MotherDuck + Scripts 113 / 127 / 347 / 386 for **Tg refreshes**.

Prereqs: google-cloud-bigquery, gcloud ADC or GOOGLE_APPLICATION_CREDENTIALS, network to BigQuery.

Usage:
  .venv/bin/python qc_framework_v1/migrations/mig_340_thyroglobulin_analyst_bq_rebuild.py \\
      --csv /path/to/Thyroid\\ Thyroglobulin\\ Lab_20251120.csv --dry-run

  .venv/bin/python qc_framework_v1/migrations/mig_340_thyroglobulin_analyst_bq_rebuild.py \\
      --csv /path/to/Thyroid\\ Thyroglobulin\\ Lab_20251120.csv --apply

Options:
  --skip-snapshot   Do not write the pub_archive snapshot (only if intentional / first deploy).

Validation: see qc_framework_v1/migrations/sql/mig_340_validation_queries.sql
"""
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent

BQ_PROJECT = "thyroid-canonical-pub-2026"
RAW_DS = "pub_raw"
CANON_DS = "pub_canonical"
ARCHIVE_DS = "pub_archive"
LOCATION = "us-central1"

RAW_TABLE = "thyroglobulin_analyst_ehr_20251120"
PROVENANCE_TABLE = "thyroglobulin_analyst_ehr_20251120_load_provenance"
CANON_TABLE = "canonical_labs_thyroglobulin_v1"
SNAPSHOT_TABLE = "canonical_labs_thyroglobulin_v1_pre_tgrebuild_20260514"
SQL_TRANSFORM = SCRIPT_DIR / "sql" / "mig_340_thy_canonical_from_analyst_raw.sql"


def _sanitize_field(name: str) -> str:
    x = " ".join((name or "").strip().split())
    x = x.replace("/", "_").replace("-", "_")
    x = re.sub(r"[^0-9A-Za-z_]+", "_", x)
    x = x.strip("_")
    while "__" in x:
        x = x.replace("__", "_")
    if not x:
        raise ValueError(f"cannot sanitize CSV header column name: {name!r}")
    if x[0].isdigit():
        x = "c_" + x
    return x


def _unique_field_names(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for h in headers:
        base = _sanitize_field(h)
        if base not in seen:
            seen[base] = 1
            out.append(base)
            continue
        seen[base] += 1
        out.append(f"{base}_{seen[base]}")
    return out


def csv_column_names_ordered(csv_path: Path) -> list[str]:
    """Return BigQuery-safe field names aligned to CSV column order (physical positions)."""
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header_row = next(reader)
    fields = _unique_field_names(header_row)
    if "research_id_number" not in fields:
        raise SystemExit(
            "Expected sanitized column research_id_number in CSV headers; "
            f"derived fields ({len(fields)}): {fields[:12]}{' ...' if len(fields) > 12 else ''}"
        )
    return fields


def bq_csv_load(csv_path: Path, dry_run: bool) -> None:
    from google.cloud import bigquery as bq

    fields = csv_column_names_ordered(csv_path)
    schema = [
        bq.SchemaField(fname, field_type="STRING", mode="NULLABLE") for fname in fields
    ]
    fq = f"{BQ_PROJECT}.{RAW_DS}.{RAW_TABLE}"
    _print(f"[mig_340] LOAD {fq} from {csv_path.name} ({len(fields)} STRING columns)")
    if dry_run:
        return

    client = bq.Client(project=BQ_PROJECT, location=LOCATION)
    job_config = bq.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bq.SourceFormat.CSV,
        write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
        max_bad_records=0,
        allow_jagged_rows=False,
        allow_quoted_newlines=True,
    )
    _print(f"[mig_340] Waiting for LOAD job into {fq} …")
    with csv_path.open("rb") as fh:
        job = client.load_table_from_file(fh, fq, job_config=job_config)
    job.result(timeout=7200)


def snapshot_canonical(skip: bool, dry_run: bool) -> None:
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery as bq

    fq_canon = f"{BQ_PROJECT}.{CANON_DS}.{CANON_TABLE}"
    fq_snap = f"{BQ_PROJECT}.{ARCHIVE_DS}.{SNAPSHOT_TABLE}"

    client = bq.Client(project=BQ_PROJECT, location=LOCATION)
    if skip:
        _print("[mig_340] Skipping archive snapshot (--skip-snapshot)")
        return
    try:
        client.get_table(fq_canon)
    except NotFound:
        _print(f"[mig_340] No existing {fq_canon}; snapshot omitted (nothing to freeze)")
        return
    sql = f"""
CREATE OR REPLACE TABLE `{fq_snap}` AS
SELECT *, CURRENT_TIMESTAMP() AS _archive_captured_ts FROM `{fq_canon}`
""".strip()
    _print(f"[mig_340] Snapshot -> {fq_snap}")
    if dry_run:
        _print(sql[:500] + ("…" if len(sql) > 500 else ""))
        return
    client.query(sql).result()


def write_provenance(
    *,
    csv_path: Path,
    loaded_table: str,
    dry_run: bool,
    client_row_count_override: int | None = None,
) -> None:
    from google.cloud import bigquery as bq

    fq = f"{BQ_PROJECT}.{RAW_DS}.{PROVENANCE_TABLE}"
    fname = csv_path.name
    if dry_run:
        _print(f"[mig_340] PROVENANCE (dry-run): {fq} row_count from BQ COUNT or override")
        return

    client = bq.Client(project=BQ_PROJECT, location=LOCATION)
    if client_row_count_override is not None:
        n_rows = int(client_row_count_override)
    else:
        q = client.query(f"SELECT COUNT(*) AS n FROM `{loaded_table}`")
        n_rows = int(list(q.result())[0][0])

    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter("source_filename", "STRING", fname),
            bq.ScalarQueryParameter("loaded_table_id", "STRING", loaded_table),
            bq.ScalarQueryParameter("row_count", "INT64", n_rows),
        ],
    )
    sql = f"""
CREATE OR REPLACE TABLE `{fq}` AS
SELECT
  CURRENT_TIMESTAMP() AS load_utc_ts,
  @source_filename AS source_filename,
  @loaded_table_id AS loaded_table_id,
  @row_count AS source_row_count,
  CAST(NULL AS STRING) AS notes,
  'mig_340_thyroglobulin_analyst_bq_rebuild.py' AS load_script_tag
""".strip()
    client.query(sql, job_config=job_config).result()
    _print(f"[mig_340] Provenance refreshed: {fq} (rows={n_rows})")


def dry_run_via_bq_sql_script() -> None:
    _print("[mig_340] Dry-run transform: skipping BigQuery DDL (see qc_framework_v1/migrations/sql/mig_340_thy_canonical_from_analyst_raw.sql)")
    sql = SQL_TRANSFORM.read_text(encoding="utf-8")
    snippet = sql[:720].replace("\n", " ")
    _print(f"  begin: {snippet}…")


def apply_transform_via_client(dry_run: bool) -> None:
    sql = SQL_TRANSFORM.read_text(encoding="utf-8")
    if dry_run:
        dry_run_via_bq_sql_script()
        return
    try:
        from google.cloud import bigquery as bq
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency google-cloud-bigquery. Install with:\n"
            "  pip install google-cloud-bigquery\n"
            "and authenticate with ADC or GOOGLE_APPLICATION_CREDENTIALS."
        ) from exc

    client = bq.Client(project=BQ_PROJECT, location=LOCATION)
    _print(f"[mig_340] Applying transform script ({SQL_TRANSFORM.relative_to(REPO_ROOT)}) …")
    job = client.query(sql)
    job.result(timeout=7200)
    _print("[mig_340] Transform complete.")


def _print(msg: str) -> None:
    print(msg)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--csv",
        type=Path,
        required=True,
        help="Path to Thyroid Thyroglobulin Lab CSV (78312 rows expected).",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Actually load CSV and execute transform (default prints dry-run skeleton).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Echo actions only (no LOAD / no DDL).",
    )
    ap.add_argument(
        "--load-only",
        action="store_true",
        help="STOP after RAW load + provenance (omit canonical rebuild).",
    )
    ap.add_argument(
        "--transform-only",
        action="store_true",
        help="Assume RAW table already refreshed; snapshot + canonical SQL only.",
    )
    ap.add_argument(
        "--skip-snapshot",
        action="store_true",
        help="If set, omit pub_archive canonical snapshot.",
    )
    args = ap.parse_args()
    csv_path: Path = args.csv.expanduser().resolve()
    dry = args.dry_run or not args.apply

    if not csv_path.is_file():
        raise SystemExit(f"missing CSV: {csv_path}")
    if not SQL_TRANSFORM.is_file():
        raise SystemExit(f"missing SQL bundle: {SQL_TRANSFORM}")

    if args.transform_only:
        snapshot_canonical(args.skip_snapshot, dry)
        fq_raw = f"{BQ_PROJECT}.{RAW_DS}.{RAW_TABLE}"
        write_provenance(
            csv_path=csv_path,
            loaded_table=fq_raw,
            dry_run=dry,
            client_row_count_override=None,
        )
        apply_transform_via_client(dry)
        _print("[mig_340] Done (transform-only).")
        return

    bq_csv_load(csv_path, dry)
    fq_raw = f"{BQ_PROJECT}.{RAW_DS}.{RAW_TABLE}"
    write_provenance(csv_path=csv_path, loaded_table=fq_raw, dry_run=dry)
    snapshot_canonical(args.skip_snapshot, dry)
    if args.load_only:
        _print("[mig_340] Done (load-only).")
        return
    apply_transform_via_client(dry)
    _print("[mig_340] Complete. Next: run qc_framework_v1/migrations/sql/mig_340_validation_queries.sql")


if __name__ == "__main__":
    main()
