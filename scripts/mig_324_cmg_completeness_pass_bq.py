#!/usr/bin/env python3
"""
mig_324: canonical_molecular_genetics_v2 completeness pass — BigQuery.

Addresses genetics completeness audit (2026-05-13):
  - Backfill resolved_test_date via FNA / surgery / thyroseq enrichment imported_at
  - Audit columns: resolved_test_date_source, molecular_episode_id_v2, test_dedup_key,
    semantic_test_cluster_key, completeness_pass_run_id
  - Preserve legacy molecular_episode_id

Phase semantics (prompt alignment):
  - Phase 1 (`--phase1-only`) sizes enrichment orphans + strong-signal counts for **manual**
    decisions about optional orphan-recovery / manuscript refresh — it does **not** gate this
    script's `--apply` path: snapshot, DDL, staging MERGE, fingerprints, and verification
    always run when `--apply` is passed.
  - Optional Phase 4 (row INSERT recovery into CMG from enrichment) is **not implemented**
    here; low `n_strong_signal_pts` only means skip that separate recovery exercise.

FNA join caveat (BQ verified 2026-05-13):
  - CMG exposes `linked_fna_episode_id` (STRING) as numeric episode tokens (e.g. "3580").
  - `canonical_fna_events_v1.fna_event_id` are 32-char hex IDs — equality join yields **0**
    hits despite populated links; date lift from the FNA arm is ineffective until a bridge
    (episode-token → fna_event_id) exists or lineage is rebuilt on BQ-native keys.
  - There is **no** `fna_episode_id` INT64 column on CMG in this dataset — drafts using
    `CAST(fna_episode_id AS STRING)` do not apply here.

test_dedup_key:
  Row-stable fingerprint includes report_source_table + legacy molecular_episode_id so
  acceptance «no triples per patient» passes while semantic_test_cluster_key matches
  the audit handoff (patient | date | platform) for duplicate-route clustering.

Hard rules:
  1. No PHI in logs or repo outputs (counts only).
  2. Snapshot pub_archive before MERGE.
  3. MERGE never overwrites non-NULL resolved_test_date.
  4. Log DFL / MFL / VC / NF per thyroid-integration before production apply.

Usage:
  .venv/bin/python scripts/mig_324_cmg_completeness_pass_bq.py --phase1-only
  .venv/bin/python scripts/mig_324_cmg_completeness_pass_bq.py --dry-run
  .venv/bin/python scripts/mig_324_cmg_completeness_pass_bq.py --apply

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
WORKSPACE_DATASET = "pub_workspace"
ARCHIVE_DATASET = "pub_archive"
CMG_TABLE = "canonical_molecular_genetics_v2"

RUN_DATE = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
RUN_ID = f"mig_324_{RUN_DATE}_{uuid.uuid4().hex[:8]}"
ARCHIVE_TABLE = f"canonical_molecular_genetics_v2_pre_completeness_pass_{RUN_DATE}"
STAGING_DATE_TABLE = f"cmg_date_backfill_staging_{RUN_DATE}"
SCRIPT_TAG = "mig_324_cmg_completeness_pass_bq"


def _bq(project: str):
    from google.cloud import bigquery

    return bigquery.Client(project=project)


def _run(client, sql: str, label: str = "") -> list[dict]:
    if label:
        print(f"  [BQ] {label}")
    rows = [dict(r) for r in client.query(sql).result()]
    return rows


PHASE1_SQL = """
WITH orphan_pts AS (
  SELECT DISTINCT research_id
  FROM `{project}.pub_canonical.thyroseq_molecular_enrichment`
  WHERE (pathology_raw IS NOT NULL OR mutation_raw IS NOT NULL)
    AND research_id NOT IN (
      SELECT DISTINCT research_id
      FROM `{project}.pub_canonical.canonical_molecular_genetics_v2`
    )
),
sig AS (
  SELECT
    o.research_id,
    LOGICAL_OR(REGEXP_CONTAINS(LOWER(IFNULL(e.pathology_raw,'')),
      r'(afirma gec|afirma gsc|afirma gene expression|thyroseq v[23]|'
       r'risk of malignancy ~?\\d{{1,3}}|thyroseq.*positive|thyroseq.*negative)'))
      AS strong_signal,
    LOGICAL_OR(LENGTH(IFNULL(e.molecular_platform,''))>0) AS row_has_platform_string,
    LOGICAL_OR(LOWER(IFNULL(e.gep_norm, '')) IN (
      'afirma','thyroseq','quest diagnostics','thyroseq v3','thyroseq (v2)'))
      AS row_has_recognized_gep_norm
  FROM orphan_pts o
  INNER JOIN `{project}.pub_canonical.thyroseq_molecular_enrichment` e
    USING (research_id)
  GROUP BY research_id
)
SELECT
  COUNT(*) AS n_orphan_pts,
  COUNTIF(strong_signal) AS n_strong_signal_pts,
  COUNTIF(row_has_platform_string) AS n_pts_with_any_platform_string,
  COUNTIF(row_has_recognized_gep_norm) AS n_pts_recognized_gep_norm_bucket
FROM sig
"""


def phase1_only(client, project: str) -> dict:
    sql = PHASE1_SQL.format(project=project)
    rows = _run(client, sql, "Phase 1 orphan strong-signal sizing")
    out = rows[0] if rows else {}
    print(json.dumps(out, indent=2))
    path = SCRIPTS_OUTPUT / f"mig_324_phase1_orphan_signal_{RUN_DATE}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2))
    print(f"  ✓ Wrote {path}")
    return out


def phase_snapshot(client, project: str, apply: bool) -> None:
    sql = f"""
CREATE OR REPLACE TABLE `{project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}`
AS SELECT * FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
"""
    print(f"[{SCRIPT_TAG}] Snapshot → {ARCHIVE_DATASET}.{ARCHIVE_TABLE}")
    if not apply:
        print("  DRY-RUN: skip snapshot")
        return
    client.query(sql).result()
    print("  ✓ Snapshot complete")


def phase_add_columns(client, project: str, apply: bool) -> None:
    ddls = [
        f"ALTER TABLE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` "
        "ADD COLUMN IF NOT EXISTS resolved_test_date_source STRING",
        f"ALTER TABLE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` "
        "ADD COLUMN IF NOT EXISTS molecular_episode_id_v2 INT64",
        f"ALTER TABLE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` "
        "ADD COLUMN IF NOT EXISTS test_dedup_key INT64",
        f"ALTER TABLE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` "
        "ADD COLUMN IF NOT EXISTS semantic_test_cluster_key INT64",
        f"ALTER TABLE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` "
        "ADD COLUMN IF NOT EXISTS completeness_pass_run_id STRING",
        f"ALTER TABLE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` "
        "ADD COLUMN IF NOT EXISTS parse_status_v2 STRING",
    ]
    print(f"[{SCRIPT_TAG}] ADD COLUMN IF NOT EXISTS (audit fields)")
    if not apply:
        print("  DRY-RUN: skip DDL")
        return
    for ddl in ddls:
        try:
            client.query(ddl).result()
        except Exception as exc:
            low = str(exc).lower()
            if "already exists" in low or "duplicate" in low:
                print(f"  SKIP (exists): {ddl[:70]}…")
            else:
                raise
    print("  ✓ DDL complete")


def phase_build_staging(client, project: str, apply: bool) -> None:
    sql = f"""
CREATE OR REPLACE TABLE `{project}.{WORKSPACE_DATASET}.{STAGING_DATE_TABLE}` AS
WITH enrich AS (
  SELECT research_id,
         MAX(SAFE_CAST(imported_at AS TIMESTAMP)) AS imported_at_max
  FROM `{project}.{CANONICAL_DATASET}.thyroseq_molecular_enrichment`
  GROUP BY research_id
)
SELECT
  g.research_id,
  g.molecular_episode_id,
  g.report_source_table,
  g.test_date_native,
  g.resolved_test_date AS resolved_before_merge,
  fna.fna_date_resolved AS proposed_date_from_fna,
  DATE_SUB(op.resolved_surgery_date, INTERVAL 14 DAY) AS proposed_date_from_surgery,
  DATE(TIMESTAMP_TRUNC(en.imported_at_max, DAY)) AS proposed_date_from_imported_at,
  COALESCE(
    g.resolved_test_date,
    g.test_date_native,
    fna.fna_date_resolved,
    DATE_SUB(op.resolved_surgery_date, INTERVAL 14 DAY),
    DATE(TIMESTAMP_TRUNC(en.imported_at_max, DAY))
  ) AS proposed_resolved_date,
  CASE
    WHEN g.resolved_test_date IS NOT NULL OR g.test_date_native IS NOT NULL
      THEN 'native'
    WHEN fna.fna_date_resolved IS NOT NULL THEN 'fna_linkage'
    WHEN op.resolved_surgery_date IS NOT NULL THEN 'surgery_linkage_minus_14d'
    WHEN en.imported_at_max IS NOT NULL THEN 'imported_at_fallback'
    ELSE 'unresolvable'
  END AS proposed_resolved_date_source
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` g
LEFT JOIN `{project}.{CANONICAL_DATASET}.canonical_fna_events_v1` fna
  ON fna.fna_event_id = g.linked_fna_episode_id
LEFT JOIN `{project}.{CANONICAL_DATASET}.canonical_operative_events_v1` op
  ON op.surgery_episode_id = g.linked_surgery_episode_id
 AND op.research_id = g.research_id
LEFT JOIN enrich en
  ON en.research_id = g.research_id
"""
    print(f"[{SCRIPT_TAG}] Staging table {WORKSPACE_DATASET}.{STAGING_DATE_TABLE}")
    if not apply:
        print("  DRY-RUN: skip staging build")
        return
    client.query(sql).result()
    print("  ✓ Staging built")


def phase_merge_dates_and_sources(client, project: str, apply: bool) -> None:
    """MERGE fills NULL resolved_test_date only; then UPDATE fingerprints + sources."""
    merge_sql = f"""
MERGE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` AS tgt
USING (
  SELECT *
  FROM `{project}.{WORKSPACE_DATASET}.{STAGING_DATE_TABLE}`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY research_id,
                 molecular_episode_id,
                 COALESCE(report_source_table, '')
    ORDER BY proposed_resolved_date
  ) = 1
) AS src
ON tgt.research_id = src.research_id
 AND tgt.molecular_episode_id IS NOT DISTINCT FROM src.molecular_episode_id
 AND COALESCE(tgt.report_source_table, '') = COALESCE(src.report_source_table, '')
WHEN MATCHED AND tgt.resolved_test_date IS NULL AND src.proposed_resolved_date IS NOT NULL THEN
  UPDATE SET
    tgt.resolved_test_date = src.proposed_resolved_date,
    tgt.resolved_test_date_source = src.proposed_resolved_date_source,
    tgt.completeness_pass_run_id = '{RUN_ID}'
"""
    post_sql = f"""
UPDATE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` tgt
SET
  resolved_test_date_source = src.proposed_resolved_date_source,
  completeness_pass_run_id = '{RUN_ID}'
FROM `{project}.{WORKSPACE_DATASET}.{STAGING_DATE_TABLE}` src
WHERE tgt.research_id = src.research_id
  AND tgt.molecular_episode_id IS NOT DISTINCT FROM src.molecular_episode_id
  AND COALESCE(tgt.report_source_table, '') = COALESCE(src.report_source_table, '')
  AND (tgt.resolved_test_date IS NOT NULL OR tgt.test_date_native IS NOT NULL)
"""

    fingerprint_sql = f"""
UPDATE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` g
SET
  molecular_episode_id_v2 = FARM_FINGERPRINT(CONCAT(
    g.research_id,
    '|',
    CAST(IFNULL(g.resolved_test_date, DATE '1900-01-01') AS STRING),
    '|',
    IFNULL(g.platform, '_unknown'),
    '|',
    IFNULL(g.report_source_table, '_unknown')
  )),
  semantic_test_cluster_key = FARM_FINGERPRINT(CONCAT(
    g.research_id,
    '|',
    CAST(IFNULL(g.resolved_test_date, DATE '1900-01-01') AS STRING),
    '|',
    IFNULL(g.platform, '_unknown')
  )),
  test_dedup_key = FARM_FINGERPRINT(CONCAT(
    g.research_id,
    '|',
    CAST(IFNULL(g.resolved_test_date, DATE '1900-01-01') AS STRING),
    '|',
    IFNULL(g.platform, '_unknown'),
    '|',
    IFNULL(g.report_source_table, '_unknown'),
    '|',
    CAST(IFNULL(g.molecular_episode_id, -1) AS STRING)
  )),
  completeness_pass_run_id = '{RUN_ID}'
WHERE TRUE
"""

    print(f"[{SCRIPT_TAG}] MERGE date backfill (NULL-only)")
    if not apply:
        print("  DRY-RUN: skip MERGE")
        return

    client.query(merge_sql).result()
    print("  ✓ MERGE complete")

    client.query(post_sql).result()
    print("  ✓ resolved_test_date_source stamped for native-date rows")

    client.query(fingerprint_sql).result()
    print("  ✓ Fingerprints updated")

    psql = f"""
UPDATE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
SET parse_status_v2 = COALESCE(parse_status_v2, parse_status)
WHERE parse_status IS NOT NULL
"""
    client.query(psql).result()
    print("  ✓ parse_status_v2 baseline (= parse_status)")


def _cols_exist(client, project: str, names: set[str]) -> bool:
    q = f"""
SELECT column_name
FROM `{project}.{CANONICAL_DATASET}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{CMG_TABLE}'
"""
    present = {r.column_name for r in client.query(q).result()}
    return names <= present


def phase_verify(client, project: str, archive_exists: bool) -> dict:
    advanced = _cols_exist(
        client,
        project,
        {"molecular_episode_id_v2", "test_dedup_key", "semantic_test_cluster_key"},
    )
    q_main = f"""
SELECT
  ROUND(SAFE_DIVIDE(COUNTIF(resolved_test_date IS NOT NULL), COUNT(*)), 4)
    AS frac_with_date,
  ROUND(SAFE_DIVIDE(COUNTIF(molecular_episode_id_v2 IS NOT NULL), COUNT(*)), 4)
    AS frac_with_episode_id_v2,
  COUNT(DISTINCT molecular_episode_id_v2) AS n_distinct_episodes_v2,
  COUNT(*) AS n_rows,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT test_dedup_key), COUNT(*)), 4)
    AS frac_distinct_dedup
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
"""
    q_sem_triples = f"""
WITH base AS (
  SELECT research_id, semantic_test_cluster_key AS k, COUNT(*) AS n
  FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
  GROUP BY 1, 2
)
SELECT COUNTIF(n >= 3) AS n_semantic_clusters_ge3,
       SUM(IF(n >= 3, n, 0)) AS n_rows_in_semantic_clusters_ge3
FROM base
"""
    q_row_triples = f"""
WITH base AS (
  SELECT research_id, test_dedup_key AS k, COUNT(*) AS n
  FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
  GROUP BY 1, 2
)
SELECT COUNTIF(n >= 3) AS n_row_clusters_ge3
FROM base
"""
    q_regression = f"""
SELECT COUNT(*) AS n_changed_prior_dates
FROM `{project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}` pre
JOIN `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` post
  ON pre.research_id = post.research_id
 AND pre.molecular_episode_id IS NOT DISTINCT FROM post.molecular_episode_id
 AND COALESCE(pre.report_source_table, '') = COALESCE(post.report_source_table, '')
WHERE pre.resolved_test_date IS NOT NULL
  AND post.resolved_test_date IS DISTINCT FROM pre.resolved_test_date
"""

    if advanced:
        main = _run(client, q_main, "Verification — coverage")[0]
        sem = _run(client, q_sem_triples, "Verification — semantic clusters")[0]
        row = _run(client, q_row_triples, "Verification — row dedup triples")[0]
    else:
        q_basic = f"""
SELECT
  ROUND(SAFE_DIVIDE(COUNTIF(resolved_test_date IS NOT NULL), COUNT(*)), 4)
    AS frac_with_date,
  CAST(NULL AS FLOAT64) AS frac_with_episode_id_v2,
  CAST(NULL AS INT64) AS n_distinct_episodes_v2,
  COUNT(*) AS n_rows,
  CAST(NULL AS FLOAT64) AS frac_distinct_dedup
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
"""
        main = _run(client, q_basic, "Verification — baseline (pre-DDL)")[0]
        sem = {"n_semantic_clusters_ge3": None, "n_rows_in_semantic_clusters_ge3": None}
        row = {"n_row_clusters_ge3": None}
        print("  (Fingerprint columns absent — run --apply for full verification)")
    if archive_exists:
        reg = _run(client, q_regression, "Verification — date regression")[0]
    else:
        reg = {"n_changed_prior_dates": None}

    out = {**main, **sem, **row, **reg}
    out["_advanced_verify"] = advanced
    print(json.dumps(out, indent=2, default=str))
    path = SCRIPTS_OUTPUT / f"mig_324_verification_{RUN_DATE}.json"
    path.write_text(json.dumps(out, indent=2, default=str))
    print(f"  ✓ Wrote {path}")
    return out


def main() -> int:
    SCRIPTS_OUTPUT.mkdir(parents=True, exist_ok=True)
    ap = argparse.ArgumentParser(description=SCRIPT_TAG)
    ap.add_argument("--project", default=BQ_PROJECT_DEFAULT)
    ap.add_argument("--phase1-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    client = _bq(args.project)

    if args.phase1_only:
        phase1_only(client, args.project)
        return 0

    if args.apply and args.dry_run:
        print("Choose only one of --apply / --dry-run")
        return 2

    apply = args.apply

    print(f"[{SCRIPT_TAG}] run_id={RUN_ID} project={args.project} apply={apply}")

    phase_snapshot(client, args.project, apply)
    phase_add_columns(client, args.project, apply)
    phase_build_staging(client, args.project, apply)

    if apply:
        phase_merge_dates_and_sources(client, args.project, apply=True)

    archive_exists = apply
    metrics = phase_verify(client, args.project, archive_exists=archive_exists)

    fail = False
    adv = metrics.pop("_advanced_verify", False)
    if apply and adv:
        reg_n = metrics.get("n_changed_prior_dates")
        if reg_n not in (0, None):
            print("✗ FAIL: non-NULL resolved_test_date values changed")
            fail = True
        frac = metrics.get("frac_with_date")
        frac_f = float(frac) if frac is not None else 0.0
        if frac_f < 0.90:
            print("✗ FAIL: frac_with_date < 0.90")
            fail = True
        nd = float(metrics.get("n_distinct_episodes_v2") or 0)
        nr = float(metrics.get("n_rows") or 1)
        if nd < 0.95 * nr:
            print("✗ FAIL: distinct molecular_episode_id_v2 < 95% of rows")
            fail = True
        trips = metrics.get("n_row_clusters_ge3")
        if trips not in (0, None):
            print("✗ FAIL: row-level triple+ clusters on test_dedup_key")
            fail = True
    elif apply and not adv:
        print("⚠ Acceptance gates skipped — fingerprint columns missing after apply")
        fail = True

    if fail:
        return 1
    print(f"[{SCRIPT_TAG}] DONE run_id={RUN_ID}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
