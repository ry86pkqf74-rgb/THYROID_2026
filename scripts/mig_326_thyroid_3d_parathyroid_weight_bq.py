#!/usr/bin/env python3
"""
mig_326: BigQuery additive schema + backfill for pathology thyroid 3D dimensions and
parathyroid weight (mg) from LLM evidence text.

Source prompt:
  studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/
  CURSOR_PROMPT_thyroid_size_3D_and_parathyroid_weight.md

Governance (thyroid-integration):
  1) Append Data Feedback Log row (Base B, Data Feedback Log) BEFORE --apply.
  2) Snapshot pub_archive tables before ALTER/UPDATE.
  3) Append Manuscript Feedback Log after --apply with verification metrics (no PHI in notes).

Targets:
  - pub_canonical.thyroid_sizes — new *_cm_path columns + dim_parse_status
  - pub_canonical.canonical_parathyroid_events_v1 — parathyroid_weight_mg + provenance

Usage:
  .venv/bin/python scripts/mig_326_thyroid_3d_parathyroid_weight_bq.py --dry-run
  .venv/bin/python scripts/mig_326_thyroid_3d_parathyroid_weight_bq.py --apply
  .venv/bin/python scripts/mig_326_thyroid_3d_parathyroid_weight_bq.py --verify-only

Environment:
  GOOGLE_APPLICATION_CREDENTIALS or gcloud application-default login
"""
from __future__ import annotations

import argparse
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from google.cloud import bigquery  # noqa: E402

BQ_PROJECT_DEFAULT = "thyroid-canonical-pub-2026"
CANONICAL = "pub_canonical"
ARCHIVE = "pub_archive"
THYROID_SIZES = "thyroid_sizes"
PARA_EVENTS = "canonical_parathyroid_events_v1"
CPM_TABLE = "canonical_patient_master_v1_6"
CPM_SURG_FILTER = "surg_first_date IS NOT NULL"


def _client(project: str) -> bigquery.Client:
    return bigquery.Client(project=project)


def _run(client: bigquery.Client, sql: str, dry_run: bool, label: str) -> None:
    if dry_run:
        print(f"  [DRY-RUN] {label}")
        return
    client.query(sql).result()
    print(f"  ✓ {label}")


# --- Phase 2 extraction (prompt + conservative keyword gate) ---
# Find numeric weight + unit, then verify a weight-ish keyword lies just before it.
_MEASURE_RX = re.compile(
    r"(\d+(?:\.\d+)?)\s*(milligrams?|mg\.?|grams?|\bg\b|gm)\b",
    re.IGNORECASE,
)


def _normalize_unit(unit: str) -> float:
    u = unit.lower().strip().rstrip("s").rstrip(".")
    if u.startswith("milli") or u == "mg":
        return 1.0
    if u in ("g", "gm", "gram"):
        return 1000.0
    return float("nan")


def extract_weight_mg(text: str | None) -> float | None:
    """First weight-ish measurement gated by contextual keywords within 48 chars."""
    if not text or not str(text).strip():
        return None
    blob = str(text)
    keys = ("weight", "wt", "weighed", "weighing", "weighted", "specimen wt")
    for m in _MEASURE_RX.finditer(blob):
        start = m.start()
        window = blob[max(0, start - 48) : start].lower()
        if not any(k in window for k in keys):
            continue
        mult = _normalize_unit(m.group(2))
        if mult != mult:
            continue
        out = float(m.group(1)) * mult
        if out <= 0:
            continue
        return float(out)
    return None


def _has_column(client: bigquery.Client, project: str, table: str, column: str) -> bool:
    sql = f"""
SELECT 1
FROM `{project}.{CANONICAL}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table}' AND column_name = '{column}'
LIMIT 1
"""
    return bool(list(client.query(sql).result()))


def _snapshot_tables(
    client: bigquery.Client, project: str, run_date: str, dry_run: bool
) -> tuple[str, str]:
    tsnap = f"{THYROID_SIZES}_pre_3d_parse_{run_date}"
    psnap = f"{PARA_EVENTS}_pre_weight_extract_{run_date}"
    for label, fq_dest, fq_src in (
        ("Snapshot thyroid_sizes", f"{project}.{ARCHIVE}.{tsnap}", f"{project}.{CANONICAL}.{THYROID_SIZES}"),
        ("Snapshot parathyroid events", f"{project}.{ARCHIVE}.{psnap}", f"{project}.{CANONICAL}.{PARA_EVENTS}"),
    ):
        sql = f"CREATE OR REPLACE TABLE `{fq_dest}` AS SELECT * FROM `{fq_src}`"
        _run(client, sql, dry_run, label)
    return tsnap, psnap


def _alter_thyroid_sizes(client: bigquery.Client, project: str, dry_run: bool) -> None:
    sql = f"""
ALTER TABLE `{project}.{CANONICAL}.{THYROID_SIZES}`
  ADD COLUMN IF NOT EXISTS rl_length_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS rl_width_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS rl_depth_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS rl_largest_dim_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS ll_length_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS ll_width_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS ll_depth_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS ll_largest_dim_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS total_length_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS total_width_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS total_depth_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS isthmus_length_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS isthmus_width_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS isthmus_depth_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS dim_parse_status STRING,
  ADD COLUMN IF NOT EXISTS dim_parse_at TIMESTAMP;
"""
    _run(client, sql, dry_run, "ALTER thyroid_sizes (additive columns)")


def _phase1_updates(
    client: bigquery.Client, project: str, dry_run: bool, has_isthmus: bool
) -> None:
    """Parse L×W×H from formatted strings using BigQuery regexp."""
    d3 = r"\d+(?:\.\d+)?\s*[x×]\s*\d+(?:\.\d+)?\s*[x×]\s*\d+(?:\.\d+)?\s*cm"
    d2 = r"\d+(?:\.\d+)?\s*[x×]\s*\d+(?:\.\d+)?\s*cm"
    d1 = r"\d+(?:\.\d+)?\s*cm"

    def dims_extract(col: str) -> tuple[str, str, str]:
        """Return SQL expressions for length, width, depth from `col`."""
        p_l = r"(\d+(?:\.\d+)?)\s*[x×]\s*\d+(?:\.\d+)?\s*[x×]\s*\d+(?:\.\d+)?\s*cm"
        p_w = r"\d+(?:\.\d+)?\s*[x×]\s*(\d+(?:\.\d+)?)\s*[x×]\s*\d+(?:\.\d+)?\s*cm"
        p_d = r"\d+(?:\.\d+)?\s*[x×]\s*\d+(?:\.\d+)?\s*[x×]\s*(\d+(?:\.\d+)?)\s*cm"
        return (
            f"SAFE_CAST(REGEXP_EXTRACT({col}, r'{p_l}') AS FLOAT64)",
            f"SAFE_CAST(REGEXP_EXTRACT({col}, r'{p_w}') AS FLOAT64)",
            f"SAFE_CAST(REGEXP_EXTRACT({col}, r'{p_d}') AS FLOAT64)",
        )

    def stat(col: str) -> str:
        return f"""CASE
    WHEN {col} IS NULL OR LENGTH(TRIM({col})) = 0 THEN NULL
    WHEN REGEXP_CONTAINS({col}, r'{d3}') THEN '3d_parsed'
    WHEN REGEXP_CONTAINS({col}, r'{d2}') THEN '2d_only'
    WHEN REGEXP_CONTAINS({col}, r'{d1}') THEN '1d_only'
    ELSE 'unparseable'
  END"""

    rl_l, rl_w, rl_d = dims_extract("rl_formatted")
    ll_l, ll_w, ll_d = dims_extract("ll_formatted")
    tt_l, tt_w, tt_d = dims_extract("total_formatted")

    isthmus_frag = ""
    if has_isthmus:
        is_l, is_w, is_d = dims_extract("isthmus_formatted")
        isthmus_frag = f"""
  isthmus_length_cm_path = {is_l},
  isthmus_width_cm_path = {is_w},
  isthmus_depth_cm_path = {is_d},"""

    segs = [
        "IF(LENGTH(IFNULL(TRIM(rl_formatted), '')) > 0, CONCAT('rl:', (" + stat("rl_formatted") + ")), NULL)",
        "IF(LENGTH(IFNULL(TRIM(ll_formatted), '')) > 0, CONCAT('ll:', (" + stat("ll_formatted") + ")), NULL)",
        "IF(LENGTH(IFNULL(TRIM(total_formatted), '')) > 0, CONCAT('total:', (" + stat("total_formatted") + ")), NULL)",
    ]
    if has_isthmus:
        segs.append(
            "IF(LENGTH(IFNULL(TRIM(isthmus_formatted), '')) > 0, CONCAT('isthmus:', ("
            + stat("isthmus_formatted")
            + ")), NULL)"
        )
    seg_list_sql = "[" + ",".join(segs) + "]"

    sql_phase1 = f"""
UPDATE `{project}.{CANONICAL}.{THYROID_SIZES}` SET
  rl_length_cm_path = {rl_l},
  rl_width_cm_path = {rl_w},
  rl_depth_cm_path = {rl_d},
  ll_length_cm_path = {ll_l},
  ll_width_cm_path = {ll_w},
  ll_depth_cm_path = {ll_d},
  total_length_cm_path = {tt_l},
  total_width_cm_path = {tt_w},
  total_depth_cm_path = {tt_d},{isthmus_frag}
  dim_parse_status = ARRAY_TO_STRING(
    ARRAY(SELECT seg FROM UNNEST({seg_list_sql}) AS seg WHERE seg IS NOT NULL),
    '|'
  ),
  dim_parse_at = CURRENT_TIMESTAMP()
WHERE TRUE;
"""
    _run(client, sql_phase1, dry_run, "Phase 1: parse 3D columns + dim_parse_status")

    sql_max = f"""
UPDATE `{project}.{CANONICAL}.{THYROID_SIZES}` SET
  rl_largest_dim_cm_path = (
    SELECT MAX(v) FROM UNNEST([rl_length_cm_path, rl_width_cm_path, rl_depth_cm_path]) AS v
    WHERE v IS NOT NULL
  ),
  ll_largest_dim_cm_path = (
    SELECT MAX(v) FROM UNNEST([ll_length_cm_path, ll_width_cm_path, ll_depth_cm_path]) AS v
    WHERE v IS NOT NULL
  )
WHERE TRUE;
"""
    _run(client, sql_max, dry_run, "Phase 1b: rl/ll largest dimension helpers")


def _alter_parathyroid_events(client: bigquery.Client, project: str, dry_run: bool) -> None:
    sql = f"""
ALTER TABLE `{project}.{CANONICAL}.{PARA_EVENTS}`
  ADD COLUMN IF NOT EXISTS parathyroid_weight_mg FLOAT64,
  ADD COLUMN IF NOT EXISTS parathyroid_weight_source STRING,
  ADD COLUMN IF NOT EXISTS parathyroid_weight_extracted_at TIMESTAMP;
"""
    _run(client, sql, dry_run, "ALTER canonical_parathyroid_events_v1 (weight columns)")


def _phase2_python_backfill(
    client: bigquery.Client, project: str, run_id: str, dry_run: bool
) -> int:
    import pandas as pd

    sql_pull = f"""
SELECT
  parathyroid_event_id,
  research_id,
  CONCAT(
    COALESCE(evidence_quote, ''),
    ' ',
    COALESCE(reasoning, ''),
    ' ',
    COALESCE(parathyroid_pathology, '')
  ) AS evidence_blob
FROM `{project}.{CANONICAL}.{PARA_EVENTS}`
WHERE evidence_quote IS NOT NULL
   OR reasoning IS NOT NULL
   OR parathyroid_pathology IS NOT NULL
"""
    if dry_run:
        print(f"  [DRY-RUN] Phase 2: would pull evidence + regex backfill ({run_id})")
        return 0

    df = client.query(sql_pull).result().to_dataframe(create_bqstorage_client=False)
    if df.empty:
        print("  ! Phase 2: no rows to scan")
        return 0

    weights = [extract_weight_mg(row.get("evidence_blob")) for _, row in df.iterrows()]
    out = pd.DataFrame(
        {
            "parathyroid_event_id": df["parathyroid_event_id"],
            "research_id": df["research_id"],
            "parathyroid_weight_mg": weights,
        }
    )
    out = out[out["parathyroid_weight_mg"].notna()].copy()
    n = len(out)
    if n == 0:
        print("  ! Phase 2: regex produced zero weights")
        return 0

    out["parathyroid_weight_source"] = "llm_evidence_regex_v1"

    stag = f"_staging_mig326_para_weight_{uuid.uuid4().hex[:10]}"
    fq = f"{project}.{CANONICAL}.{stag}"

    client.delete_table(fq, not_found_ok=True)

    load_job = client.load_table_from_dataframe(
        out,
        destination=fq,
        job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE),
    )
    load_job.result()

    merge_sql = f"""
MERGE `{project}.{CANONICAL}.{PARA_EVENTS}` T
USING `{project}.{CANONICAL}.{stag}` S
ON T.parathyroid_event_id = S.parathyroid_event_id
WHEN MATCHED AND S.parathyroid_weight_mg IS NOT NULL THEN
  UPDATE SET
    T.parathyroid_weight_mg = S.parathyroid_weight_mg,
    T.parathyroid_weight_source = S.parathyroid_weight_source,
    T.parathyroid_weight_extracted_at = CURRENT_TIMESTAMP()
"""
    client.query(merge_sql).result()
    client.delete_table(fq, not_found_ok=True)
    print(f"  ✓ Phase 2: MERGE parathyroid weights (staging rows: {n})")
    return n


def _verify(client: bigquery.Client, project: str, dry_run: bool) -> None:
    if dry_run:
        print("  [DRY-RUN] Verification thyroid_sizes / parathyroid_events")
        return

    if not _has_column(client, project, THYROID_SIZES, "rl_length_cm_path"):
        print(
            "\n[!] Verification skipped: thyroid_sizes.rl_length_cm_path missing — "
            "run `--apply` first (or DDL not yet promoted)."
        )
        return

    sql_ts = f"""
SELECT
  COUNTIF(rl_formatted IS NOT NULL AND LENGTH(TRIM(rl_formatted))>0) AS n_rl_formatted,
  COUNTIF(rl_length_cm_path IS NOT NULL) AS n_rl_length_parsed,
  ROUND(SAFE_DIVIDE(
    COUNTIF(rl_length_cm_path IS NOT NULL),
    NULLIF(COUNTIF(rl_formatted IS NOT NULL AND LENGTH(TRIM(rl_formatted))>0), 0)
  ), 4) AS frac_rl_parsed,
  COUNTIF(ll_formatted IS NOT NULL AND LENGTH(TRIM(ll_formatted))>0) AS n_ll_formatted,
  COUNTIF(ll_length_cm_path IS NOT NULL) AS n_ll_length_parsed,
  ROUND(SAFE_DIVIDE(
    COUNTIF(ll_length_cm_path IS NOT NULL),
    NULLIF(COUNTIF(ll_formatted IS NOT NULL AND LENGTH(TRIM(ll_formatted))>0), 0)
  ), 4) AS frac_ll_parsed,
  COUNTIF(total_formatted IS NOT NULL AND LENGTH(TRIM(total_formatted))>0) AS n_total_formatted,
  COUNTIF(total_length_cm_path IS NOT NULL) AS n_total_length_parsed,
  ROUND(SAFE_DIVIDE(
    COUNTIF(total_length_cm_path IS NOT NULL),
    NULLIF(COUNTIF(total_formatted IS NOT NULL AND LENGTH(TRIM(total_formatted))>0), 0)
  ), 4) AS frac_total_parsed,
  COUNT(DISTINCT dim_parse_status) AS n_parse_statuses
FROM `{project}.{CANONICAL}.{THYROID_SIZES}`;
"""

    sql_para = f"""
WITH surg AS (
  SELECT DISTINCT CAST(cpm.research_id AS STRING) AS rid
  FROM `{project}.{CANONICAL}.{CPM_TABLE}` cpm
  WHERE {CPM_SURG_FILTER}
)
SELECT
  COUNT(*) AS n_rows_total,
  COUNTIF(e.parathyroid_weight_mg IS NOT NULL) AS n_with_weight,
  COUNTIF(e.parathyroid_weight_mg IS NOT NULL AND CAST(e.research_id AS STRING) IN (SELECT rid FROM surg)
  ) AS n_with_weight_surgical,
  ROUND(AVG(e.parathyroid_weight_mg), 1) AS mean_weight_mg,
  APPROX_QUANTILES(e.parathyroid_weight_mg, 100)[OFFSET(50)] AS median_weight_mg,
  MIN(e.parathyroid_weight_mg) AS min_weight_mg,
  MAX(e.parathyroid_weight_mg) AS max_weight_mg
FROM `{project}.{CANONICAL}.{PARA_EVENTS}` e
"""

    sql_cpm_exists = f"""
SELECT 1 FROM `{project}.{CANONICAL}.INFORMATION_SCHEMA.TABLES`
WHERE table_name = '{CPM_TABLE}' LIMIT 1
"""

    print("\n=== Verification: thyroid_sizes ===")
    for r in client.query(sql_ts).result():
        print(dict(r.items()))
    if list(client.query(sql_cpm_exists).result()):
        print("\n=== Verification: canonical_parathyroid_events_v1 ===")
        for r in client.query(sql_para).result():
            print(dict(r.items()))
    else:
        print(f"\n[SKIP] {CPM_TABLE} missing — parathyroid surgical subset not evaluated.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=BQ_PROJECT_DEFAULT)
    ap.add_argument("--dry-run", action="store_true", help="Print steps only")
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Snapshots + DDL + Phase1/2 backfills + verification",
    )
    ap.add_argument(
        "--verify-only",
        action="store_true",
        help="Only run verification SQL (reads live canonical)",
    )
    args = ap.parse_args()
    if not args.verify_only and not args.dry_run and not args.apply:
        print("Specify --dry-run, --verify-only, or --apply")
        return 2

    run_date = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
    run_id = f"mig_326_{run_date}_{uuid.uuid4().hex[:8]}"
    print(f"[{run_id}] project={args.project}")

    client = _client(args.project)

    if args.verify_only:
        _verify(client, args.project, dry_run=False)
        return 0

    has_isthmus = _has_column(client, args.project, THYROID_SIZES, "isthmus_formatted")
    print(f"  isthmus_formatted present: {has_isthmus}")

    _snapshot_tables(client, args.project, run_date, args.dry_run)
    _alter_thyroid_sizes(client, args.project, args.dry_run)
    _phase1_updates(client, args.project, args.dry_run, has_isthmus)
    _alter_parathyroid_events(client, args.project, args.dry_run)
    _phase2_python_backfill(client, args.project, run_id, args.dry_run)
    _verify(client, args.project, dry_run=args.dry_run)

    if args.apply and not args.dry_run:
        print("\nNext (manual):\n")
        print("  1) Confirm Data Feedback Log row exists for this canonical schema extension.")
        print(
            "  2) Append Manuscript Feedback Log "
            "`MFL-<DATE>-EXT2-4-WEIGHT-SIZE-EXTENSION` linking EXT2-4 + M084 (recx6Jr6WFtF2hZxb)."
        )
        print("  3) Manual-review CSV export: gs internal only — do not commit evidence text.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
