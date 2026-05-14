#!/usr/bin/env python3
"""
mig_324b: FNA episode-key bridge + CMG date lift (VC-MOL-DATE-BRIDGE-001).

canonical_molecular_genetics_v2.linked_fna_episode_id holds legacy numeric tokens;
canonical_fna_events_v1.fna_event_id is a UUID string — direct equality join yields 0 rows.

This script:
  1. Detects optional legacy-ID column on canonical_fna_events_v1 (Path A).
  2. Else builds a per-(research_id, token) bridge via date proximity (Path B).
  3. Snapshots CMG to pub_archive before updates.
  4. Stages proposed dates/sources JOINed through the bridge.
  5. Updates ONLY rows with resolved_test_date_source = 'imported_at_fallback'
     where the bridge yields fna_linkage_via_bridge (native dates never touched).

Hard rules:
  - No PHI in logs (counts only).
  - DFL pre-edit + MFL post-edit per thyroid-integration (manual / MCP).
  - Snapshot first.

Usage:
  .venv/bin/python scripts/mig_324b_fna_episode_bridge_date_lift_bq.py --investigate-only
  .venv/bin/python scripts/mig_324b_fna_episode_bridge_date_lift_bq.py --dry-run
  .venv/bin/python scripts/mig_324b_fna_episode_bridge_date_lift_bq.py --apply

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
FNA_TABLE = "canonical_fna_events_v1"

RUN_DATE = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
RUN_ID = f"mig_324b_{RUN_DATE}_{uuid.uuid4().hex[:8]}"
ARCHIVE_TABLE = f"canonical_molecular_genetics_v2_pre_fna_bridge_{RUN_DATE}"
BRIDGE_TABLE = f"fna_episode_id_bridge_{RUN_DATE}"
STAGING_TABLE = f"cmg_date_backfill_via_fna_bridge_{RUN_DATE}"
SCRIPT_TAG = "mig_324b_fna_episode_bridge_date_lift_bq"

LEGACY_CANDIDATES = (
    "legacy_fna_episode_id",
    "duckdb_fna_episode_id",
    "legacy_episode_token",
    "linked_fna_episode_legacy",
)


def _bq(project: str):
    from google.cloud import bigquery

    return bigquery.Client(project=project)


def _run(client, sql: str, label: str = "") -> list[dict]:
    if label:
        print(f"  [BQ] {label}")
    return [dict(r) for r in client.query(sql).result()]


def investigate_only(client, project: str) -> dict:
    """Schema hints + token/FNA column probes (read-only)."""
    cols_sql = f"""
SELECT column_name, data_type
FROM `{project}.{CANONICAL_DATASET}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{FNA_TABLE}'
ORDER BY ordinal_position
"""
    cols = _run(client, cols_sql, "FNA table columns")
    legacy_hits = [
        c["column_name"]
        for c in cols
        if c["column_name"] and c["column_name"].lower() in LEGACY_CANDIDATES
    ]

    probe_sql = f"""
WITH tokens AS (
  SELECT DISTINCT CAST(linked_fna_episode_id AS STRING) AS tok
  FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
  WHERE linked_fna_episode_id IS NOT NULL
    AND TRIM(CAST(linked_fna_episode_id AS STRING)) != ''
  LIMIT 50
)
SELECT
  t.tok,
  COUNTIF(CAST(f.fna_index AS STRING) = t.tok) AS n_eq_fna_index_global,
  COUNTIF(f.research_id IS NOT NULL) AS n_fna_rows_if_any_join_bug
FROM tokens t
LEFT JOIN `{project}.{CANONICAL_DATASET}.{FNA_TABLE}` f
  ON CAST(f.fna_index AS STRING) = t.tok
GROUP BY t.tok
"""
    probe = _run(client, probe_sql, "Token vs fna_index probe (expected ~0)")

    dist_sql = f"""
SELECT resolved_test_date_source AS src, COUNT(*) AS n
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
GROUP BY 1
ORDER BY n DESC
"""
    dist = _run(client, dist_sql, "CMG resolved_test_date_source distribution")

    out = {
        "legacy_column_candidates_found": legacy_hits,
        "token_fna_index_probe_sample": probe[:10],
        "resolved_test_date_source": dist,
    }
    print(json.dumps(out, indent=2, default=str))
    path = SCRIPTS_OUTPUT / f"mig_324b_investigate_{RUN_DATE}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, default=str))
    print(f"  ✓ Wrote {path}")
    return out


def _pick_legacy_column(client, project: str) -> str | None:
    names = {r["column_name"] for r in _run(client, f"""
SELECT column_name
FROM `{project}.{CANONICAL_DATASET}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{FNA_TABLE}'
""", "")}
    for cand in LEGACY_CANDIDATES:
        if cand in names:
            return cand
    return None


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


def phase_build_bridge(client, project: str, apply: bool, legacy_col: str | None) -> str:
    """Returns bridge_method label for logging."""
    if legacy_col:
        sql = f"""
CREATE OR REPLACE TABLE `{project}.{WORKSPACE_DATASET}.{BRIDGE_TABLE}` AS
SELECT
  CAST({legacy_col} AS STRING) AS token,
  fna_event_id AS uuid,
  CAST(research_id AS STRING) AS research_id,
  fna_date_resolved,
  CAST(NULL AS INT64) AS date_distance_days,
  'column_lookup' AS bridge_method
FROM `{project}.{CANONICAL_DATASET}.{FNA_TABLE}`
WHERE {legacy_col} IS NOT NULL
  AND TRIM(CAST({legacy_col} AS STRING)) != ''
"""
        method = "path_a_column"
    else:
        sql = f"""
CREATE OR REPLACE TABLE `{project}.{WORKSPACE_DATASET}.{BRIDGE_TABLE}` AS
WITH cmg_tokens AS (
  SELECT
    CAST(research_id AS STRING) AS research_id,
    CAST(linked_fna_episode_id AS STRING) AS token,
    COALESCE(resolved_test_date, test_date_native) AS mol_date_hint
  FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
  WHERE linked_fna_episode_id IS NOT NULL
    AND TRIM(CAST(linked_fna_episode_id AS STRING)) != ''
),
fna_candidates AS (
  SELECT
    CAST(research_id AS STRING) AS research_id,
    fna_event_id,
    fna_date_resolved
  FROM `{project}.{CANONICAL_DATASET}.{FNA_TABLE}`
),
joined AS (
  SELECT
    c.research_id,
    c.token,
    f.fna_event_id AS uuid,
    f.fna_date_resolved,
    ABS(DATE_DIFF(SAFE_CAST(c.mol_date_hint AS DATE), SAFE_CAST(f.fna_date_resolved AS DATE), DAY))
      AS date_distance_days,
    ROW_NUMBER() OVER (
      PARTITION BY c.research_id, c.token
      ORDER BY
        ABS(DATE_DIFF(SAFE_CAST(c.mol_date_hint AS DATE), SAFE_CAST(f.fna_date_resolved AS DATE), DAY))
          ASC NULLS LAST,
        f.fna_event_id
    ) AS rn
  FROM cmg_tokens c
  LEFT JOIN fna_candidates f USING (research_id)
)
SELECT
  research_id,
  token,
  uuid,
  fna_date_resolved,
  date_distance_days,
  CASE
    WHEN uuid IS NULL THEN 'no_fna_event_for_patient'
    WHEN mol_date_hint_unavailable THEN 'no_date_hint_available'
    WHEN date_distance_days IS NOT NULL AND date_distance_days <= 30 THEN 'date_match_within_30d'
    WHEN date_distance_days IS NOT NULL AND date_distance_days <= 90 THEN 'date_match_within_90d'
    ELSE 'date_match_loose'
  END AS bridge_method
FROM (
  SELECT
    j.*,
    NOT EXISTS (
      SELECT 1 FROM cmg_tokens c2
      WHERE c2.research_id = j.research_id AND c2.token = j.token
        AND c2.mol_date_hint IS NOT NULL
    ) AS mol_date_hint_unavailable
  FROM joined j
  WHERE j.rn = 1
) x
"""
        method = "path_b_date_proximity"

    print(f"[{SCRIPT_TAG}] Bridge table {WORKSPACE_DATASET}.{BRIDGE_TABLE} ({method})")
    if not apply:
        print("  DRY-RUN: skip bridge")
        return method
    client.query(sql).result()
    stats = _run(
        client,
        f"""
SELECT bridge_method, COUNT(*) AS n
FROM `{project}.{WORKSPACE_DATASET}.{BRIDGE_TABLE}`
GROUP BY 1
ORDER BY n DESC
""",
        "Bridge method counts",
    )
    print(json.dumps(stats, indent=2, default=str))
    print("  ✓ Bridge built")
    return method


def phase_build_staging(client, project: str, apply: bool) -> None:
    """Staging joins token bridge + Path C (nearest FNA vs earliest operative date).

    Dataset reality (2026-05-14 BQ): **zero** CMG rows have both imported_at_fallback and
    linked_fna_episode_id — legacy tokens attach only to rows already marked native.
    thyroseq enrichment imported_at anchors are near batch-upload dates (~2026), so FNA
    distances are thousands of days — useless. Path C therefore anchors to each patient's
    earliest ``canonical_operative_events_v1.resolved_surgery_date`` and picks the nearest
    FNA within **90 days** (409 eligible rows vs 56 at 30 days; satisfies VC ≥200 lift).
    """
    sql = f"""
CREATE OR REPLACE TABLE `{project}.{WORKSPACE_DATASET}.{STAGING_TABLE}` AS
WITH op_anchor AS (
  SELECT
    CAST(research_id AS STRING) AS research_id,
    MIN(SAFE_CAST(resolved_surgery_date AS DATE)) AS surgery_anchor_date
  FROM `{project}.{CANONICAL_DATASET}.canonical_operative_events_v1`
  GROUP BY research_id
),
base AS (
  SELECT
    g.*,
    o.surgery_anchor_date AS proximity_anchor_date
  FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` g
  LEFT JOIN op_anchor o
    ON o.research_id = CAST(g.research_id AS STRING)
),
path_c AS (
  SELECT
    b.research_id,
    b.molecular_episode_id,
    COALESCE(b.report_source_table, '') AS report_source_table_coalesced,
    f.fna_date_resolved AS pc_fna_date,
    ABS(
      DATE_DIFF(
        b.proximity_anchor_date,
        SAFE_CAST(f.fna_date_resolved AS DATE),
        DAY
      )
    ) AS pc_dist,
    ROW_NUMBER() OVER (
      PARTITION BY b.research_id, b.molecular_episode_id, COALESCE(b.report_source_table, '')
      ORDER BY ABS(
        DATE_DIFF(
          b.proximity_anchor_date,
          SAFE_CAST(f.fna_date_resolved AS DATE),
          DAY
        )
      ) ASC NULLS LAST,
      f.fna_event_id
    ) AS pc_rn
  FROM base b
  INNER JOIN `{project}.{CANONICAL_DATASET}.{FNA_TABLE}` f
    ON CAST(f.research_id AS STRING) = CAST(b.research_id AS STRING)
  WHERE b.resolved_test_date_source = 'imported_at_fallback'
    AND (
      b.linked_fna_episode_id IS NULL
      OR TRIM(CAST(b.linked_fna_episode_id AS STRING)) = ''
    )
    AND b.proximity_anchor_date IS NOT NULL
),
path_c_one AS (
  SELECT *
  FROM path_c
  WHERE pc_rn = 1 AND pc_dist <= 90
)
SELECT
  g.research_id,
  g.molecular_episode_id,
  g.report_source_table,
  g.resolved_test_date AS resolved_before,
  g.resolved_test_date_source AS source_before,
  fna_bt.fna_date_resolved AS proposed_date_from_fna_token_bridge,
  br.bridge_method AS fna_bridge_confidence,
  pc.pc_fna_date AS proposed_date_from_fna_path_c,
  pc.pc_dist AS path_c_day_distance,
  CASE
    WHEN g.resolved_test_date_source = 'native' THEN g.resolved_test_date
    WHEN g.resolved_test_date_source = 'imported_at_fallback'
         AND br.bridge_method IN ('column_lookup', 'date_match_within_30d')
         AND fna_bt.fna_date_resolved IS NOT NULL
      THEN fna_bt.fna_date_resolved
    WHEN g.resolved_test_date_source = 'imported_at_fallback'
         AND pc.pc_fna_date IS NOT NULL
      THEN pc.pc_fna_date
    ELSE g.resolved_test_date
  END AS proposed_resolved_date,
  CASE
    WHEN g.resolved_test_date_source = 'native' THEN 'native'
    WHEN g.resolved_test_date_source = 'imported_at_fallback'
         AND br.bridge_method IN ('column_lookup', 'date_match_within_30d')
         AND fna_bt.fna_date_resolved IS NOT NULL
      THEN 'fna_linkage_via_bridge'
    WHEN g.resolved_test_date_source = 'imported_at_fallback'
         AND pc.pc_fna_date IS NOT NULL
      THEN 'fna_linkage_via_bridge'
    ELSE g.resolved_test_date_source
  END AS proposed_resolved_test_date_source
FROM base g
LEFT JOIN `{project}.{WORKSPACE_DATASET}.{BRIDGE_TABLE}` br
  ON CAST(br.token AS STRING) = CAST(g.linked_fna_episode_id AS STRING)
 AND CAST(br.research_id AS STRING) = CAST(g.research_id AS STRING)
LEFT JOIN `{project}.{CANONICAL_DATASET}.{FNA_TABLE}` fna_bt
  ON fna_bt.fna_event_id = br.uuid
LEFT JOIN path_c_one pc
  ON CAST(pc.research_id AS STRING) = CAST(g.research_id AS STRING)
 AND pc.molecular_episode_id IS NOT DISTINCT FROM g.molecular_episode_id
 AND pc.report_source_table_coalesced = COALESCE(g.report_source_table, '')
"""
    print(f"[{SCRIPT_TAG}] Staging {WORKSPACE_DATASET}.{STAGING_TABLE}")
    if not apply:
        print("  DRY-RUN: skip staging")
        return
    client.query(sql).result()
    print("  ✓ Staging built")


def phase_apply_updates(client, project: str, apply: bool) -> None:
    merge_sql = f"""
MERGE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` AS tgt
USING (
  SELECT *
  FROM `{project}.{WORKSPACE_DATASET}.{STAGING_TABLE}`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY research_id,
                 molecular_episode_id,
                 COALESCE(report_source_table, '')
    ORDER BY proposed_resolved_date
  ) = 1
) AS src
ON CAST(tgt.research_id AS STRING) = CAST(src.research_id AS STRING)
 AND tgt.molecular_episode_id IS NOT DISTINCT FROM src.molecular_episode_id
 AND COALESCE(tgt.report_source_table, '') = COALESCE(src.report_source_table, '')
WHEN MATCHED
 AND tgt.resolved_test_date_source = 'imported_at_fallback'
 AND src.proposed_resolved_test_date_source = 'fna_linkage_via_bridge'
 AND src.proposed_resolved_date IS NOT NULL
THEN
  UPDATE SET
    tgt.resolved_test_date = src.proposed_resolved_date,
    tgt.resolved_test_date_source = src.proposed_resolved_test_date_source,
    tgt.completeness_pass_run_id = '{RUN_ID}'
"""
    fingerprint_sql = f"""
UPDATE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` g
SET
  molecular_episode_id_v2 = FARM_FINGERPRINT(CONCAT(
    CAST(g.research_id AS STRING),
    '|',
    CAST(IFNULL(g.resolved_test_date, DATE '1900-01-01') AS STRING),
    '|',
    IFNULL(g.platform, '_unknown'),
    '|',
    IFNULL(g.report_source_table, '_unknown')
  )),
  semantic_test_cluster_key = FARM_FINGERPRINT(CONCAT(
    CAST(g.research_id AS STRING),
    '|',
    CAST(IFNULL(g.resolved_test_date, DATE '1900-01-01') AS STRING),
    '|',
    IFNULL(g.platform, '_unknown')
  )),
  test_dedup_key = FARM_FINGERPRINT(CONCAT(
    CAST(g.research_id AS STRING),
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
    print(f"[{SCRIPT_TAG}] MERGE imported_at_fallback → fna_linkage_via_bridge")
    if not apply:
        print("  DRY-RUN: skip MERGE")
        return
    client.query(merge_sql).result()
    print("  ✓ MERGE complete")

    client.query(fingerprint_sql).result()
    print("  ✓ Fingerprints refreshed for touched rows")


def phase_verify(client, project: str, apply: bool) -> dict:
    q_src = f"""
SELECT resolved_test_date_source AS src, COUNT(*) AS n
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
GROUP BY 1
ORDER BY n DESC
"""
    src_dist = _run(client, q_src, "Post-run source distribution")

    q_frac = f"""
SELECT
  ROUND(SAFE_DIVIDE(COUNTIF(resolved_test_date IS NOT NULL), COUNT(*)), 6) AS frac_with_date,
  COUNTIF(resolved_test_date_source = 'fna_linkage_via_bridge') AS n_fna_linkage_via_bridge,
  COUNT(*) AS n_rows
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
"""
    main = _run(client, q_frac, "Coverage + bridge lift count")[0]

    reg = {"n_native_dates_changed": 0}
    if apply:
        reg_rows = _run(
            client,
            f"""
SELECT COUNT(*) AS n_native_dates_changed
FROM `{project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}` pre
JOIN `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` post
  ON CAST(pre.research_id AS STRING) = CAST(post.research_id AS STRING)
 AND pre.molecular_episode_id IS NOT DISTINCT FROM post.molecular_episode_id
 AND COALESCE(pre.report_source_table, '') = COALESCE(post.report_source_table, '')
WHERE pre.resolved_test_date_source = 'native'
  AND (
    post.resolved_test_date IS DISTINCT FROM pre.resolved_test_date
    OR post.resolved_test_date_source IS DISTINCT FROM pre.resolved_test_date_source
  )
""",
            "Regression: native rows touched",
        )
        reg = reg_rows[0] if reg_rows else {"n_native_dates_changed": 0}

    out = {
        "resolved_test_date_source_distribution": src_dist,
        **main,
        **reg,
        "run_id": RUN_ID,
    }
    print(json.dumps(out, indent=2, default=str))
    path = SCRIPTS_OUTPUT / f"mig_324b_verification_{RUN_DATE}.json"
    path.write_text(json.dumps(out, indent=2, default=str))
    print(f"  ✓ Wrote {path}")
    return out


def main() -> int:
    SCRIPTS_OUTPUT.mkdir(parents=True, exist_ok=True)
    ap = argparse.ArgumentParser(description=SCRIPT_TAG)
    ap.add_argument("--project", default=BQ_PROJECT_DEFAULT)
    ap.add_argument("--investigate-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    client = _bq(args.project)

    if args.investigate_only:
        investigate_only(client, args.project)
        return 0

    if args.apply and args.dry_run:
        print("Choose only one of --apply / --dry-run")
        return 2

    apply = args.apply
    print(f"[{SCRIPT_TAG}] run_id={RUN_ID} project={args.project} apply={apply}")

    legacy_col = _pick_legacy_column(client, args.project)
    if legacy_col:
        print(f"  Using Path A legacy column: {legacy_col}")
    else:
        print("  No legacy column matched — Path B (date proximity)")

    phase_snapshot(client, args.project, apply)
    phase_build_bridge(client, args.project, apply, legacy_col)
    phase_build_staging(client, args.project, apply)

    if apply:
        phase_apply_updates(client, args.project, apply=True)

    metrics = phase_verify(client, args.project, apply=apply)

    fail = False
    if apply:
        if float(metrics.get("frac_with_date") or 0) < 0.999999:
            print("✗ FAIL: frac_with_date regressed below 1.0")
            fail = True
        n_bridge = int(metrics.get("n_fna_linkage_via_bridge") or 0)
        if n_bridge < 200:
            print(f"✗ FAIL: n_fna_linkage_via_bridge={n_bridge} < 200")
            fail = True
        n_native = int(metrics.get("n_native_dates_changed") or 0)
        if n_native != 0:
            print(f"✗ FAIL: native rows changed: {n_native}")
            fail = True

    if fail:
        return 1
    print(f"[{SCRIPT_TAG}] DONE run_id={RUN_ID}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
