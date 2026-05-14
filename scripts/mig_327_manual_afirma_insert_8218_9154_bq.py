#!/usr/bin/env python3
"""
mig_327: manual Afirma INSERT for research_id 8218 and 9154 (BigQuery).

Closes mig_325 residual: superseded ThyroSeq rows with no companion Afirma row in CMG.
Source: CURSOR_PROMPT_afirma_insert_8218_9154.md (structured fields from THYROSEQ_AFIRMA_12_5.xlsx).

Fingerprints match mig_324 convention (semantic_test_cluster_key, test_dedup_key with episode).

Usage:
  .venv/bin/python scripts/mig_327_manual_afirma_insert_8218_9154_bq.py --dry-run
  .venv/bin/python scripts/mig_327_manual_afirma_insert_8218_9154_bq.py --apply

Environment:
  GOOGLE_APPLICATION_CREDENTIALS or gcloud auth application-default
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_OUTPUT = REPO_ROOT / "scripts" / "output"
sys.path.insert(0, str(REPO_ROOT))

BQ_PROJECT_DEFAULT = "thyroid-canonical-pub-2026"
CANONICAL_DATASET = "pub_canonical"
ARCHIVE_DATASET = "pub_archive"
CMG_TABLE = "canonical_molecular_genetics_v2"

RUN_DATE = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
RUN_ID_HEX = uuid.uuid4().hex[:10]
MANUAL_INSERT_RUN_ID = f"manual_insert_afirma_8218_9154_{RUN_DATE}_{RUN_ID_HEX}"
ARCHIVE_TABLE = f"canonical_molecular_genetics_v2_pre_manual_insert_{RUN_DATE}"

REPORT_SRC = "manual_insert_from_THYROSEQ_AFIRMA_12_5_xlsx_20260514"


def _bq(project: str):
    from google.cloud import bigquery

    return bigquery.Client(project=project)


def main() -> int:
    SCRIPTS_OUTPUT.mkdir(parents=True, exist_ok=True)
    ap = argparse.ArgumentParser(description="mig_327 manual Afirma INSERT 8218/9154")
    ap.add_argument("--project", default=BQ_PROJECT_DEFAULT)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    if args.apply == args.dry_run:
        print("Specify exactly one of --apply / --dry-run")
        return 2

    dry = args.dry_run
    client = _bq(args.project)

    precheck = f"""
SELECT research_id, platform, overall_result_class
FROM `{args.project}.{CANONICAL_DATASET}.{CMG_TABLE}`
WHERE platform = 'Afirma'
  AND CAST(research_id AS INT64) IN (8218, 9154)
"""
    blocking = list(client.query(precheck).result())
    if blocking:
        print("✗ Existing Afirma rows for target rids — abort:", blocking)
        return 1

    snapshot_sql = f"""
CREATE OR REPLACE TABLE `{args.project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}`
AS SELECT * FROM `{args.project}.{CANONICAL_DATASET}.{CMG_TABLE}`
"""

    ts = datetime.now(tz=timezone.utc).isoformat()

    insert_sql = f"""
INSERT INTO `{args.project}.{CANONICAL_DATASET}.{CMG_TABLE}` (
  research_id,
  molecular_episode_id,
  resolved_test_date,
  resolved_test_date_source,
  test_date_native,
  platform,
  platform_raw,
  bethesda_category,
  overall_result_class,
  rom_descriptor,
  rom_percent_point,
  report_source_table,
  band_backfill_applied_at,
  band_backfill_source,
  band_backfill_run_id,
  molecular_episode_id_v2,
  test_dedup_key,
  semantic_test_cluster_key,
  completeness_pass_run_id,
  built_at,
  builder_version,
  ingestion_source,
  parse_status_v2
)
WITH new_rows AS (
  SELECT *
  FROM UNNEST([
    STRUCT(
      '8218' AS research_id,
      CAST(2 AS INT64) AS molecular_episode_id,
      DATE '2024-05-01' AS resolved_test_date,
      CAST(NULL AS INT64) AS bethesda_category,
      CAST(NULL AS STRING) AS rom_descriptor,
      CAST(NULL AS FLOAT64) AS rom_percent_point
    ),
    STRUCT(
      '9154' AS research_id,
      CAST(1 AS INT64) AS molecular_episode_id,
      DATE '2020-10-28' AS resolved_test_date,
      CAST(3 AS INT64) AS bethesda_category,
      CAST(NULL AS STRING) AS rom_descriptor,
      CAST(NULL AS FLOAT64) AS rom_percent_point
    )
  ])
)
SELECT
  n.research_id,
  n.molecular_episode_id,
  n.resolved_test_date,
  'manual_xlsx_extract' AS resolved_test_date_source,
  n.resolved_test_date AS test_date_native,
  'Afirma' AS platform,
  'Afirma' AS platform_raw,
  n.bethesda_category,
  'suspicious' AS overall_result_class,
  n.rom_descriptor,
  n.rom_percent_point,
  '{REPORT_SRC}' AS report_source_table,
  TIMESTAMP('{ts}') AS band_backfill_applied_at,
  'manual_insert_v1' AS band_backfill_source,
  '{MANUAL_INSERT_RUN_ID}' AS band_backfill_run_id,
  FARM_FINGERPRINT(CONCAT(
    n.research_id, '|',
    CAST(IFNULL(n.resolved_test_date, DATE '1900-01-01') AS STRING), '|',
    IFNULL('Afirma', '_unknown'), '|',
    IFNULL('{REPORT_SRC}', '_unknown')
  )) AS molecular_episode_id_v2,
  FARM_FINGERPRINT(CONCAT(
    n.research_id, '|',
    CAST(IFNULL(n.resolved_test_date, DATE '1900-01-01') AS STRING), '|',
    'Afirma', '|',
    '{REPORT_SRC}', '|',
    CAST(IFNULL(n.molecular_episode_id, -1) AS STRING)
  )) AS test_dedup_key,
  FARM_FINGERPRINT(CONCAT(
    n.research_id, '|',
    CAST(IFNULL(n.resolved_test_date, DATE '1900-01-01') AS STRING), '|',
    'Afirma'
  )) AS semantic_test_cluster_key,
  '{MANUAL_INSERT_RUN_ID}' AS completeness_pass_run_id,
  CURRENT_TIMESTAMP() AS built_at,
    'mig_327_manual_afirma_insert_bq.py' AS builder_version,
  'manual_insert_afirma_xlsx_structured' AS ingestion_source,
  'manual_structured_insert' AS parse_status_v2
FROM new_rows n
"""

    verify_sql = f"""
SELECT research_id, platform, molecular_episode_id,
       overall_result_class, resolved_test_date, bethesda_category,
       report_source_table, band_backfill_source, platform_reclass_status
FROM `{args.project}.{CANONICAL_DATASET}.{CMG_TABLE}`
WHERE CAST(research_id AS INT64) IN (8218, 9154)
ORDER BY research_id, platform
"""

    label = "[DRY-RUN]" if dry else "[APPLY]"
    print(f"{label} run_id={MANUAL_INSERT_RUN_ID} archive={ARCHIVE_TABLE}")

    if dry:
        print("Would snapshot:")
        print(snapshot_sql.strip()[:220], "…")
        print("\nWould insert (preview keys only): 8218 episode 2, 9154 episode 1 …")
        print("\nDry-run executing verify pre-state:")
    else:
        client.query(snapshot_sql).result()
        print("  ✓ Snapshot complete")
        client.query(insert_sql).result()
        print("  ✓ INSERT complete")

    rows = [dict(r) for r in client.query(verify_sql).result()]
    print(json.dumps(rows, indent=2, default=str))

    if not dry:
        afirma_n = sum(
            1
            for r in rows
            if str(r.get("platform")) == "Afirma"
            and int(r["research_id"]) in (8218, 9154)
        )
        if afirma_n != 2:
            print(f"✗ Expected 2 new Afirma rows; got {afirma_n}")
            return 1

        outp = SCRIPTS_OUTPUT / f"mig_327_manual_afirma_insert_verify_{RUN_DATE}.json"
        outp.write_text(json.dumps(rows, indent=2, default=str))
        print(f"  ✓ Wrote {outp}")

    if dry:
        print(f"[mig_327] DONE (dry-run) would archive `{ARCHIVE_TABLE}`")
    else:
        print(f"[mig_327] DONE archive `{ARCHIVE_TABLE}` verify Afirma rows=2 OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
