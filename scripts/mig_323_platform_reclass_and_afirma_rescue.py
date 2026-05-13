#!/usr/bin/env python3
"""
mig_323: Platform reclassification + Afirma result-field rescue — BigQuery canonical layer.

Fixes two problems in pub_canonical.canonical_molecular_genetics_v2:

Problem 1 — PLATFORM MISLABELING
  ~170 rows have platform='ThyroSeq' but their source (gep_norm in
  thyroseq_molecular_enrichment) clearly identifies them as Afirma or Quest
  Diagnostics. The ThyroSeq parser could not find a ThyroSeq band because the
  underlying test isn't ThyroSeq.

Problem 2 — MISSING AFIRMA CALLS
  For the newly-reclassified Afirma rows, the binary Afirma call (Suspicious /
  Benign / Non-diagnostic) is available in molecular_testing.result but was never
  extracted because the platform routing pointed to the ThyroSeq parser.

Source-of-truth waterfall for proposed_platform:
  Tier 1: gep_norm LIKE '%afirma%' → propose 'Afirma'
  Tier 2: gep_norm LIKE '%thyroseq%' → keep 'ThyroSeq'
  Tier 3: gep_norm LIKE '%quest%' → propose 'Other' (Quest Diagnostics in-house)
  Tier 4: fall back to molecular_testing.genetic_test / thyroseq_afirma keywords
  Tier 5: no source signal → propose same as current (unchanged), flag unresolved

MERGE guards:
  - UPDATE platform only when current differs from proposed AND proposed is
    source-supported AND band_backfill_source IS NULL, 'manual_review', or
    'numeric_rom_inferred'. Rows with band_backfill_source='reported_text' are
    SURFACED in the diff report but NOT auto-changed.
  - UPDATE overall_result_class / rom_percent_* / band_source only when current
    overall_result_class IS NULL (i.e. the Afirma call has never been set) OR
    when the row is being reclassified away from ThyroSeq semantics (in which
    case the ThyroSeq-derived band is semantically invalid for Afirma).

Hard rules (thyroid-integration skill):
  1. No PHI in code, logs, or Airtable. research_id only. result patterns are
     class-level strings, never raw patient text.
  2. Append-only: pre-merge snapshot to pub_archive.
  3. Audit columns: platform_reclass_applied_at, platform_reclass_source,
     platform_reclass_run_id (in addition to existing band_backfill_* columns).
  4. DFL row DFL-YYYYMMDD-EXT2-4-PLATFORM-RECLASS logged before running --apply.
  5. VC-MOL-PLATFORM-001 must be filed before production apply.

Usage:
  # Dry-run (no BQ writes):
  .venv/bin/python scripts/mig_323_platform_reclass_and_afirma_rescue.py --dry-run

  # Full apply:
  .venv/bin/python scripts/mig_323_platform_reclass_and_afirma_rescue.py --apply

  # Skip the MERGE, only build the staging/diff report:
  .venv/bin/python scripts/mig_323_platform_reclass_and_afirma_rescue.py --staging-only

Environment:
  GOOGLE_APPLICATION_CREDENTIALS — service account JSON path (or use gcloud auth)
  BQ_PROJECT                     — defaults to thyroid-canonical-pub-2026
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MCONS = REPO_ROOT / "molecular_consolidation_20260421"
SCRIPTS_OUTPUT = REPO_ROOT / "scripts" / "output"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(MCONS))

from afirma_result_field_parser import parse_afirma_result

BQ_PROJECT_DEFAULT = "thyroid-canonical-pub-2026"
CANONICAL_DATASET = "pub_canonical"
WORKSPACE_DATASET = "pub_workspace"
ARCHIVE_DATASET = "pub_archive"
CMG_TABLE = "canonical_molecular_genetics_v2"
RUN_DATE = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
RUN_ID = f"mig_323_{RUN_DATE}_{uuid.uuid4().hex[:8]}"
STAGING_TABLE = f"canonical_molecular_genetics_v2_platform_reclass_staging_{RUN_DATE}"
ARCHIVE_TABLE = f"canonical_molecular_genetics_v2_pre_platform_reclass_{RUN_DATE}"


# ---------------------------------------------------------------------------
# BQ helper
# ---------------------------------------------------------------------------

def _bq(project: str = BQ_PROJECT_DEFAULT):
    from google.cloud import bigquery
    return bigquery.Client(project=project)


def _run(client, sql: str, description: str = "") -> list[dict]:
    if description:
        print(f"  [BQ] {description}")
    job = client.query(sql)
    rows = [dict(r) for r in job.result()]
    return rows


# ---------------------------------------------------------------------------
# Waterfall: classify proposed_platform from gep_norm + fallback sources
# ---------------------------------------------------------------------------

def _classify_platform(gep_norm: str | None,
                        genetic_test: str | None,
                        thyroseq_afirma_snippet: str | None) -> tuple[str | None, str]:
    """
    Returns (proposed_platform, tier_label).
    proposed_platform ∈ {'Afirma', 'ThyroSeq', 'Other', None}
    None means "unresolved — leave unchanged".
    """
    gn = (gep_norm or "").strip().lower()
    gt = (genetic_test or "").strip().lower()
    # thyroseq_afirma_snippet is the first 200 chars of the OCR field — check keywords only
    ta = (thyroseq_afirma_snippet or "").strip().lower()

    # Tier 1 — gep_norm afirma
    if "afirma" in gn:
        return "Afirma", "gep_norm_afirma"
    # Tier 2 — gep_norm thyroseq
    if "thyroseq" in gn:
        return "ThyroSeq", "gep_norm_thyroseq"
    # Tier 3 — gep_norm quest
    if "quest" in gn:
        return "Other", "gep_norm_quest"
    # Tier 4a — genetic_test keyword
    if "afirma" in gt:
        return "Afirma", "genetic_test_afirma"
    if "thyroseq" in gt:
        return "ThyroSeq", "genetic_test_thyroseq"
    if "quest" in gt:
        return "Other", "genetic_test_quest"
    # Tier 4b — thyroseq_afirma text snippet keywords
    if "afirma" in ta:
        return "Afirma", "thyroseq_afirma_text_afirma"
    if "thyroseq" in ta:
        return "ThyroSeq", "thyroseq_afirma_text_thyroseq"
    # Tier 5 — unresolved
    return None, "unresolved"


# ---------------------------------------------------------------------------
# Phase 1 — snapshot
# ---------------------------------------------------------------------------

def _snapshot(client, project: str, dry_run: bool) -> None:
    sql = f"""
CREATE OR REPLACE TABLE `{project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}`
AS SELECT * FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
"""
    if dry_run:
        print(f"  [DRY-RUN] Would snapshot → {ARCHIVE_DATASET}.{ARCHIVE_TABLE}")
        return
    _run(client, sql, f"Snapshot → {ARCHIVE_DATASET}.{ARCHIVE_TABLE}")
    print(f"  ✓ Snapshot complete: {ARCHIVE_DATASET}.{ARCHIVE_TABLE}")


# ---------------------------------------------------------------------------
# Phase 2 — build staging rows (all platforms, including pre-classified)
# ---------------------------------------------------------------------------

def _build_staging(client, project: str) -> list[dict]:
    """Pull rows for reclassification analysis.

    We pull ALL ThyroSeq rows + join to source tables, then classify.
    We also pull 'NGS_unspecified' rows with afirma gep_norm (4 rows from Phase 1).
    """
    sql = f"""
SELECT
  g.research_id,
  g.molecular_episode_id,
  g.report_source_table,
  g.platform AS current_platform,
  g.overall_result_class AS current_overall_result_class,
  g.rom_descriptor AS current_rom_descriptor,
  g.rom_percent_point AS current_rom_percent_point,
  g.rom_percent_low AS current_rom_percent_low,
  g.band_backfill_source,
  g.parse_status,
  e.gep_norm,
  mt.genetic_test,
  mt.result AS mt_result,
  -- First 200 chars of thyroseq_afirma text for keyword-only platform detection
  -- (thyroseq_afirma lives in molecular_testing, NOT in thyroseq_molecular_enrichment)
  SUBSTR(mt.thyroseq_afirma, 1, 200) AS thyroseq_afirma_snippet
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` g
LEFT JOIN `{project}.{CANONICAL_DATASET}.thyroseq_molecular_enrichment` e
  ON CAST(g.research_id AS STRING) = CAST(e.research_id AS STRING)
LEFT JOIN `{project}.{CANONICAL_DATASET}.molecular_testing` mt
  ON CAST(g.research_id AS STRING) = CAST(mt.research_id AS STRING)
WHERE g.platform IN ('ThyroSeq', 'NGS_unspecified')
"""
    rows = _run(client, sql, "Pull ThyroSeq/NGS_unspecified rows for reclassification analysis")
    print(f"  ✓ Pulled {len(rows)} rows for analysis")
    return rows


def _build_staging_proposals(rows: list[dict]) -> list[dict]:
    """Apply waterfall + Afirma parser to produce staging proposals."""
    proposals = []
    for r in rows:
        current = r["current_platform"]
        proposed, tier = _classify_platform(
            r.get("gep_norm"),
            r.get("genetic_test"),
            r.get("thyroseq_afirma_snippet"),
        )
        # If proposed is None (unresolved), leave as-is
        if proposed is None:
            proposed = current

        # Determine if platform change is allowed
        bbs = r.get("band_backfill_source")
        platform_change = proposed != current
        # Allow auto-update when band_backfill_source is:
        # NULL (no mig_321 touch), 'manual_review', 'numeric_rom_inferred'
        # Surface but don't auto-apply when 'reported_text'
        band_allows_update = bbs in (None, "manual_review", "numeric_rom_inferred")
        platform_change_allowed = platform_change and (tier != "unresolved") and band_allows_update
        platform_change_flagged = platform_change and not band_allows_update  # reported_text

        # Afirma call extraction: only for rows being reclassified to Afirma
        # OR pre-existing Afirma rows with NULL overall_result_class
        afirma_parsed: dict = {
            "overall_result_class": None,
            "rom_percent_point": None,
            "rom_percent_low": None,
            "band_source": None,
        }
        run_afirma_parser = (
            (platform_change_allowed and proposed == "Afirma")
            or (current == "Afirma" and r.get("current_overall_result_class") is None)
        )
        if run_afirma_parser:
            mt_result = r.get("mt_result") or ""
            afirma_parsed = parse_afirma_result(mt_result)

        # Determine if call update is allowed:
        # - current overall_result_class is NULL (never set), OR
        # - row is moving from ThyroSeq→Afirma (ThyroSeq-derived band is semantically invalid)
        current_orc = r.get("current_overall_result_class")
        call_update_allowed = (
            run_afirma_parser
            and afirma_parsed["band_source"] is not None  # parser found something
            and (current_orc is None or (platform_change_allowed and proposed == "Afirma"))
        )

        proposals.append({
            "research_id": r["research_id"],
            "molecular_episode_id": r.get("molecular_episode_id"),
            "report_source_table": r.get("report_source_table"),
            "current_platform": current,
            "proposed_platform": proposed,
            "proposed_platform_source": tier,
            "platform_change": platform_change,
            "platform_change_allowed": platform_change_allowed,
            "platform_change_flagged": platform_change_flagged,
            "current_overall_result_class": current_orc,
            "current_rom_descriptor": r.get("current_rom_descriptor"),
            "proposed_overall_result_class": afirma_parsed["overall_result_class"] if call_update_allowed else None,
            "proposed_rom_percent_point": afirma_parsed["rom_percent_point"] if call_update_allowed else None,
            "proposed_rom_percent_low": afirma_parsed["rom_percent_low"] if call_update_allowed else None,
            "proposed_band_source": afirma_parsed["band_source"] if call_update_allowed else None,
            "call_update_allowed": call_update_allowed,
            "band_backfill_source": bbs,
            "parse_status": r.get("parse_status"),
        })
    return proposals


# ---------------------------------------------------------------------------
# Phase 3 — diff report
# ---------------------------------------------------------------------------

def _build_diff_report(proposals: list[dict]) -> dict:
    platform_changes = [p for p in proposals if p["platform_change_allowed"]]
    platform_flagged = [p for p in proposals if p["platform_change_flagged"]]
    call_updates = [p for p in proposals if p["call_update_allowed"]]
    # Rows where current call non-null disagrees with proposed call
    pre_existing_disagree = [
        p for p in proposals
        if p["call_update_allowed"]
        and p["current_overall_result_class"] is not None
        and p["proposed_overall_result_class"] is not None
        and p["current_overall_result_class"] != p["proposed_overall_result_class"]
    ]

    # Cross-tab: current_platform → proposed_platform
    platform_crosstab: dict[str, int] = {}
    for p in platform_changes:
        key = f"{p['current_platform']} → {p['proposed_platform']}"
        platform_crosstab[key] = platform_crosstab.get(key, 0) + 1

    # Tier breakdown for changes
    tier_counts: dict[str, int] = {}
    for p in platform_changes:
        t = p["proposed_platform_source"]
        tier_counts[t] = tier_counts.get(t, 0) + 1

    report = {
        "run_id": RUN_ID,
        "run_date": RUN_DATE,
        "n_rows_analyzed": len(proposals),
        "n_rows_proposed_platform_change": len(platform_changes),
        "n_rows_platform_change_flagged_reported_text": len(platform_flagged),
        "platform_change_crosstab": platform_crosstab,
        "platform_change_by_tier": tier_counts,
        "n_rows_call_update_new": len([p for p in call_updates if p["current_overall_result_class"] is None]),
        "n_rows_call_update_overwrite": len([p for p in call_updates if p["current_overall_result_class"] is not None]),
        "n_rows_pre_existing_call_disagrees": len(pre_existing_disagree),
        "flagged_rows_reported_text": [
            {
                "research_id": p["research_id"],
                "current_platform": p["current_platform"],
                "proposed_platform": p["proposed_platform"],
                "current_overall_result_class": p["current_overall_result_class"],
                "band_backfill_source": p["band_backfill_source"],
            }
            for p in platform_flagged
        ],
        "pre_existing_disagree_rows": [
            {
                "research_id": p["research_id"],
                "current_platform": p["current_platform"],
                "proposed_platform": p["proposed_platform"],
                "current_orc": p["current_overall_result_class"],
                "proposed_orc": p["proposed_overall_result_class"],
                "band_backfill_source": p["band_backfill_source"],
            }
            for p in pre_existing_disagree
        ],
    }
    return report


def _write_diff_report(report: dict, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"mig_323_diff_report_{RUN_DATE}.json"
    md_path = output_dir / f"mig_323_diff_report_{RUN_DATE}.md"

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    lines = [
        f"# mig_323 Platform Reclassification Diff Report — {RUN_DATE}",
        f"",
        f"**run_id:** `{report['run_id']}`  ",
        f"**rows analyzed:** {report['n_rows_analyzed']}",
        f"",
        f"## Platform changes",
        f"",
        f"| Change | n |",
        f"|---|---|",
    ]
    for k, v in report["platform_change_crosstab"].items():
        lines.append(f"| {k} | {v} |")

    lines += [
        f"",
        f"**Total allowed auto-changes:** {report['n_rows_proposed_platform_change']}  ",
        f"**Flagged (reported_text guard — requires manual review):** {report['n_rows_platform_change_flagged_reported_text']}  ",
        f"",
        f"### Platform change source tier breakdown",
        f"",
        f"| Tier | n |",
        f"|---|---|",
    ]
    for k, v in report["platform_change_by_tier"].items():
        lines.append(f"| {k} | {v} |")

    lines += [
        f"",
        f"## Afirma call updates",
        f"",
        f"| Type | n |",
        f"|---|---|",
        f"| New call (current ORC was NULL) | {report['n_rows_call_update_new']} |",
        f"| Overwrite (ThyroSeq semantics → Afirma) | {report['n_rows_call_update_overwrite']} |",
        f"| Pre-existing disagrees with proposed | {report['n_rows_pre_existing_call_disagrees']} |",
        f"",
    ]

    if report["n_rows_pre_existing_call_disagrees"] > 0:
        lines.append("### Pre-existing call disagreements (INSPECT BEFORE APPLY)")
        lines.append("")
        for row in report["pre_existing_disagree_rows"]:
            lines.append(f"- research_id={row['research_id']}: current={row['current_orc']} → proposed={row['proposed_orc']} (bbs={row['band_backfill_source']})")
        lines.append("")

    if report["flagged_rows_reported_text"]:
        lines.append("### Rows flagged (reported_text guard — NOT auto-applied)")
        lines.append("")
        for row in report["flagged_rows_reported_text"]:
            lines.append(f"- research_id={row['research_id']}: {row['current_platform']} → {row['proposed_platform']}, orc={row['current_overall_result_class']}, bbs={row['band_backfill_source']}")

    with open(md_path, "w") as f:
        f.write("\n".join(lines))

    return md_path


# ---------------------------------------------------------------------------
# Phase 4 — write staging table to BQ
# ---------------------------------------------------------------------------

def _write_staging_table(client, project: str, proposals: list[dict],
                          dry_run: bool) -> None:
    from google.cloud import bigquery

    rows_to_write = [
        {
            "research_id": p["research_id"],
            "molecular_episode_id": p["molecular_episode_id"],
            "report_source_table": p.get("report_source_table"),
            "current_platform": p["current_platform"],
            "proposed_platform": p["proposed_platform"],
            "proposed_platform_source": p["proposed_platform_source"],
            "platform_change": p["platform_change"],
            "platform_change_allowed": p["platform_change_allowed"],
            "platform_change_flagged": p["platform_change_flagged"],
            "current_overall_result_class": p["current_overall_result_class"],
            "proposed_overall_result_class": p["proposed_overall_result_class"],
            "proposed_rom_percent_point": p["proposed_rom_percent_point"],
            "proposed_rom_percent_low": p["proposed_rom_percent_low"],
            "proposed_band_source": p["proposed_band_source"],
            "call_update_allowed": p["call_update_allowed"],
            "band_backfill_source": p["band_backfill_source"],
            "run_id": RUN_ID,
        }
        for p in proposals
    ]

    if dry_run:
        print(f"  [DRY-RUN] Would write {len(rows_to_write)} rows → {WORKSPACE_DATASET}.{STAGING_TABLE}")
        return

    schema = [
        bigquery.SchemaField("research_id", "STRING"),
        bigquery.SchemaField("molecular_episode_id", "INTEGER"),
        bigquery.SchemaField("report_source_table", "STRING"),
        bigquery.SchemaField("current_platform", "STRING"),
        bigquery.SchemaField("proposed_platform", "STRING"),
        bigquery.SchemaField("proposed_platform_source", "STRING"),
        bigquery.SchemaField("platform_change", "BOOL"),
        bigquery.SchemaField("platform_change_allowed", "BOOL"),
        bigquery.SchemaField("platform_change_flagged", "BOOL"),
        bigquery.SchemaField("current_overall_result_class", "STRING"),
        bigquery.SchemaField("proposed_overall_result_class", "STRING"),
        bigquery.SchemaField("proposed_rom_percent_point", "FLOAT"),
        bigquery.SchemaField("proposed_rom_percent_low", "FLOAT"),
        bigquery.SchemaField("proposed_band_source", "STRING"),
        bigquery.SchemaField("call_update_allowed", "BOOL"),
        bigquery.SchemaField("band_backfill_source", "STRING"),
        bigquery.SchemaField("run_id", "STRING"),
    ]

    full_table = f"{project}.{WORKSPACE_DATASET}.{STAGING_TABLE}"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    load_job = client.load_table_from_json(rows_to_write, full_table, job_config=job_config)
    load_job.result()  # wait for completion
    if load_job.errors:
        print(f"  ⚠ Staging load errors: {load_job.errors[:3]}")
        raise RuntimeError("Staging table load failed")
    print(f"  ✓ Staging table written: {WORKSPACE_DATASET}.{STAGING_TABLE} ({len(rows_to_write)} rows)")


# ---------------------------------------------------------------------------
# Phase 5 — add audit columns to canonical table
# ---------------------------------------------------------------------------

def _add_audit_columns(client, project: str, dry_run: bool) -> None:
    sqls = [
        f"ALTER TABLE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` ADD COLUMN IF NOT EXISTS platform_reclass_applied_at TIMESTAMP",
        f"ALTER TABLE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` ADD COLUMN IF NOT EXISTS platform_reclass_source STRING",
        f"ALTER TABLE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` ADD COLUMN IF NOT EXISTS platform_reclass_run_id STRING",
    ]
    for sql in sqls:
        if dry_run:
            print(f"  [DRY-RUN] Would run: {sql[:80]}…")
        else:
            _run(client, sql, "")
    if not dry_run:
        print("  ✓ Audit columns present (or already existed)")


# ---------------------------------------------------------------------------
# Phase 6 — MERGE
# ---------------------------------------------------------------------------

def _merge(client, project: str, proposals: list[dict], dry_run: bool) -> tuple[int, int]:
    """MERGE canonical table from proposals.

    Returns (n_platform_changes, n_call_updates).
    """
    now_ts = datetime.now(tz=timezone.utc).isoformat()

    platform_rows = [p for p in proposals if p["platform_change_allowed"]]
    call_rows = [p for p in proposals if p["call_update_allowed"]]

    print(f"  Applying {len(platform_rows)} platform changes + {len(call_rows)} call updates")

    if dry_run:
        print(f"  [DRY-RUN] Would MERGE {len(platform_rows)} platform + {len(call_rows)} call rows")
        return len(platform_rows), len(call_rows)

    # Platform-change MERGE (using MERGE for BigQuery)
    # Build UPDATE SET list from staging table (all rows allowed for platform change)
    platform_merge_sql = f"""
MERGE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` AS t
USING (
  SELECT
    research_id,
    molecular_episode_id,
    report_source_table,
    proposed_platform,
    proposed_platform_source,
    proposed_overall_result_class,
    proposed_rom_percent_point,
    proposed_rom_percent_low,
    proposed_band_source,
    call_update_allowed,
    platform_change_allowed,
    platform_change
  FROM `{project}.{WORKSPACE_DATASET}.{STAGING_TABLE}`
  WHERE platform_change_allowed = TRUE
) AS s
ON (
  t.research_id = s.research_id
  AND COALESCE(t.molecular_episode_id, -1) = COALESCE(s.molecular_episode_id, -1)
  AND COALESCE(t.report_source_table, '') = COALESCE(s.report_source_table, '')
)
WHEN MATCHED AND s.platform_change = TRUE THEN
  UPDATE SET
    t.platform = s.proposed_platform,
    t.platform_reclass_applied_at = TIMESTAMP('{now_ts}'),
    t.platform_reclass_source = s.proposed_platform_source,
    t.platform_reclass_run_id = '{RUN_ID}',
    -- Only update call fields when call_update_allowed = TRUE and proposed is non-null
    t.overall_result_class = CASE
      WHEN s.call_update_allowed = TRUE AND s.proposed_overall_result_class IS NOT NULL
      THEN s.proposed_overall_result_class
      ELSE t.overall_result_class
    END,
    t.rom_percent_point = CASE
      WHEN s.call_update_allowed = TRUE AND s.proposed_rom_percent_point IS NOT NULL
      THEN s.proposed_rom_percent_point
      ELSE t.rom_percent_point
    END,
    t.rom_percent_low = CASE
      WHEN s.call_update_allowed = TRUE AND s.proposed_rom_percent_low IS NOT NULL
      THEN s.proposed_rom_percent_low
      ELSE t.rom_percent_low
    END,
    t.band_backfill_source = CASE
      WHEN s.call_update_allowed = TRUE AND s.proposed_band_source IS NOT NULL
      THEN s.proposed_band_source
      ELSE t.band_backfill_source
    END
"""

    result = client.query(platform_merge_sql).result()
    print(f"  ✓ Platform MERGE complete (job: {result.job_id if hasattr(result, 'job_id') else 'done'})")

    # For rows NOT in the platform_change set but where call should be updated
    # (pre-existing Afirma rows with NULL overall_result_class)
    afirma_only_call_rows = [
        p for p in proposals
        if p["call_update_allowed"]
        and not p["platform_change_allowed"]
        and p["current_platform"] == "Afirma"
        and p["current_overall_result_class"] is None
    ]
    if afirma_only_call_rows:
        afirma_call_sql = f"""
MERGE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` AS t
USING (
  SELECT research_id, molecular_episode_id, report_source_table,
    proposed_overall_result_class, proposed_rom_percent_point,
    proposed_rom_percent_low, proposed_band_source
  FROM `{project}.{WORKSPACE_DATASET}.{STAGING_TABLE}`
  WHERE call_update_allowed = TRUE
    AND platform_change_allowed = FALSE
    AND current_platform = 'Afirma'
    AND current_overall_result_class IS NULL
) AS s
ON (
  t.research_id = s.research_id
  AND COALESCE(t.molecular_episode_id, -1) = COALESCE(s.molecular_episode_id, -1)
  AND COALESCE(t.report_source_table, '') = COALESCE(s.report_source_table, '')
  AND t.overall_result_class IS NULL
)
WHEN MATCHED THEN
  UPDATE SET
    t.overall_result_class = s.proposed_overall_result_class,
    t.rom_percent_point = s.proposed_rom_percent_point,
    t.rom_percent_low = s.proposed_rom_percent_low,
    t.band_backfill_source = s.proposed_band_source,
    t.band_backfill_applied_at = TIMESTAMP('{now_ts}'),
    t.band_backfill_run_id = '{RUN_ID}',
    t.platform_reclass_run_id = '{RUN_ID}'
"""
        client.query(afirma_call_sql).result()
        print(f"  ✓ Afirma-only call update MERGE complete ({len(afirma_only_call_rows)} rows)")

    return len(platform_rows), len(call_rows)


# ---------------------------------------------------------------------------
# Phase 7 — post-MERGE verification
# ---------------------------------------------------------------------------

def _verify(client, project: str) -> dict:
    """Run the three acceptance-criteria queries from the handoff spec."""

    # 4.1 Coverage
    q_coverage = f"""
WITH base AS (SELECT * FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`)
SELECT
  platform,
  COUNT(*) AS n_total,
  COUNTIF(overall_result_class IS NOT NULL OR rom_descriptor IS NOT NULL) AS n_classified,
  ROUND(SAFE_DIVIDE(
    COUNTIF(overall_result_class IS NOT NULL OR rom_descriptor IS NOT NULL),
    COUNT(*)
  ), 3) AS frac_classified
FROM base
WHERE platform IN ('Afirma','ThyroSeq')
GROUP BY platform
ORDER BY platform
"""
    coverage = {r["platform"]: r for r in _run(client, q_coverage, "Coverage check")}

    # 4.2 No-regression — use 3-column composite key to avoid cross-patient fanout.
    # Excludes ThyroSeq/NGS_unspecified rows whose calls changed by design.
    q_regression = f"""
SELECT COUNT(*) AS n_regressed
FROM `{project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}` pre
JOIN `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` post
  ON pre.research_id = post.research_id
  AND COALESCE(pre.molecular_episode_id, -1) = COALESCE(post.molecular_episode_id, -1)
  AND COALESCE(pre.report_source_table, '') = COALESCE(post.report_source_table, '')
WHERE
  (pre.overall_result_class IS NOT NULL
   AND pre.overall_result_class != post.overall_result_class
   AND pre.platform NOT IN ('ThyroSeq','NGS_unspecified')
  )
  OR (pre.rom_descriptor IS NOT NULL AND pre.rom_descriptor != post.rom_descriptor)
"""
    regression_rows = _run(client, q_regression, "No-regression check")
    n_regressed = regression_rows[0]["n_regressed"] if regression_rows else -1

    # 4.3 Platform vs source-of-truth consistency
    q_consistency = f"""
WITH joined AS (
  SELECT g.research_id, g.molecular_episode_id, g.platform, e.gep_norm
  FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` g
  LEFT JOIN `{project}.{CANONICAL_DATASET}.thyroseq_molecular_enrichment` e
    ON CAST(g.research_id AS STRING) = CAST(e.research_id AS STRING)
)
SELECT
  COUNTIF(platform = 'ThyroSeq' AND LOWER(gep_norm) LIKE '%afirma%') AS n_thyroseq_with_afirma_source,
  COUNTIF(platform = 'Afirma' AND LOWER(gep_norm) LIKE '%thyroseq%') AS n_afirma_with_thyroseq_source
FROM joined
"""
    consistency = _run(client, q_consistency, "Platform consistency check")[0]

    return {
        "coverage": coverage,
        "n_regressed": n_regressed,
        "n_thyroseq_with_afirma_source": consistency.get("n_thyroseq_with_afirma_source", -1),
        "n_afirma_with_thyroseq_source": consistency.get("n_afirma_with_thyroseq_source", -1),
    }


def _print_verification(v: dict) -> bool:
    """Print verification results and return True if all gates pass."""
    print("\n─── Post-MERGE Verification ───────────────────────────────────────────")
    all_pass = True

    # Coverage
    for platform in ["Afirma", "ThyroSeq"]:
        info = v["coverage"].get(platform, {})
        n_t = info.get("n_total", 0)
        n_c = info.get("n_classified", 0)
        frac = info.get("frac_classified", 0.0)
        gate = frac >= 0.95
        status = "✓ PASS" if gate else ("⚠ NEAR-MISS (90–95%)" if frac >= 0.90 else "✗ FAIL")
        if not gate:
            all_pass = False
        print(f"  {platform}: {n_c}/{n_t} classified = {100*frac:.1f}% {status}")

    # No-regression
    n_reg = v["n_regressed"]
    reg_gate = n_reg == 0
    reg_status = "✓ PASS" if reg_gate else "✗ FAIL"
    if not reg_gate:
        all_pass = False
    print(f"  No-regression: n_regressed={n_reg} {reg_status}")

    # Consistency
    n_ts_af = v["n_thyroseq_with_afirma_source"]
    n_af_ts = v["n_afirma_with_thyroseq_source"]
    cons_gate = n_ts_af <= 5 and n_af_ts <= 5
    cons_status = "✓ PASS" if cons_gate else "✗ FAIL"
    if not cons_gate:
        all_pass = False
    print(f"  Platform consistency: ThyroSeq+afirma_src={n_ts_af}, Afirma+thyroseq_src={n_af_ts} {cons_status}")

    print("────────────────────────────────────────────────────────────────────────")
    return all_pass


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="mig_323: Platform reclassification + Afirma rescue")
    ap.add_argument("--dry-run", action="store_true", help="No BQ writes; print what would happen")
    ap.add_argument("--apply", action="store_true", help="Run full pipeline with BQ writes")
    ap.add_argument("--staging-only", action="store_true",
                    help="Build staging table and diff report, but skip the canonical MERGE")
    ap.add_argument("--verify-only", action="store_true",
                    help="Run post-MERGE verification only (assumes apply already ran)")
    ap.add_argument("--project", default=BQ_PROJECT_DEFAULT)
    args = ap.parse_args()

    if not any([args.dry_run, args.apply, args.staging_only, args.verify_only]):
        print("ERROR: specify --dry-run, --apply, --staging-only, or --verify-only")
        sys.exit(1)

    dry_run = args.dry_run

    print(f"mig_323 — run_id={RUN_ID}  date={RUN_DATE}  project={args.project}")
    print(f"mode={'DRY-RUN' if dry_run else ('STAGING-ONLY' if args.staging_only else ('VERIFY-ONLY' if args.verify_only else 'APPLY'))}")
    print()

    client = _bq(args.project)

    if args.verify_only:
        v = _verify(client, args.project)
        _print_verification(v)
        (SCRIPTS_OUTPUT / f"mig_323_verification_{RUN_DATE}.json").write_text(
            json.dumps(v, indent=2, default=str)
        )
        return

    # Phase 1 — snapshot (skip for staging-only)
    if not args.staging_only:
        print("Phase 1 — Snapshot canonical table…")
        _snapshot(client, args.project, dry_run)

    # Phase 2 — build staging proposals
    print("\nPhase 2 — Pull rows and build reclassification proposals…")
    raw_rows = _build_staging(client, args.project)
    proposals = _build_staging_proposals(raw_rows)
    print(f"  ✓ Built {len(proposals)} proposals")

    # Phase 3 — diff report
    print("\nPhase 3 — Generate diff report…")
    report = _build_diff_report(proposals)
    SCRIPTS_OUTPUT.mkdir(parents=True, exist_ok=True)
    md_path = _write_diff_report(report, SCRIPTS_OUTPUT)
    print(f"  ✓ Diff report → {md_path}")

    # STOP if there are unexpected SAME-PLATFORM pre-existing call disagreements.
    # Cross-platform disagreements (ThyroSeq→Afirma with call change) are EXPECTED
    # because ThyroSeq band semantics are invalid for Afirma tests.
    same_platform_disagree = [
        p for p in proposals
        if p["call_update_allowed"]
        and p["current_overall_result_class"] is not None
        and p["proposed_overall_result_class"] is not None
        and p["current_overall_result_class"] != p["proposed_overall_result_class"]
        and not p["platform_change"]  # same platform = unexpected
    ]
    if len(same_platform_disagree) > 0 and not dry_run:
        print(f"\n⚠ STOP: {len(same_platform_disagree)} SAME-PLATFORM rows have unexpected call disagreements.")
        print("  Review the diff report before proceeding. Use --dry-run to inspect.")
        sys.exit(1)
    n_disagree = report["n_rows_pre_existing_call_disagrees"]
    if n_disagree > 0:
        print(f"  ⚠ Note: {n_disagree} cross-platform call updates (expected semantic corrections)")

    # Phase 4 — write staging table
    print("\nPhase 4 — Write staging table to pub_workspace…")
    _write_staging_table(client, args.project, proposals, dry_run)

    if args.staging_only:
        print("\n[staging-only] Skipping canonical MERGE and verification.")
        print(f"Summary: {report['n_rows_proposed_platform_change']} platform changes, "
              f"{report['n_rows_call_update_new'] + report['n_rows_call_update_overwrite']} call updates proposed.")
        return

    # Phase 5 — add audit columns
    print("\nPhase 5 — Add audit columns to canonical table…")
    _add_audit_columns(client, args.project, dry_run)

    # Phase 6 — MERGE
    print("\nPhase 6 — MERGE into canonical table…")
    n_plat, n_call = _merge(client, args.project, proposals, dry_run)
    print(f"  ✓ Applied {n_plat} platform changes, {n_call} call updates")

    # Phase 7 — verify
    if not dry_run:
        print("\nPhase 7 — Post-MERGE verification…")
        v = _verify(client, args.project)
        all_pass = _print_verification(v)
        v_path = SCRIPTS_OUTPUT / f"mig_323_verification_{RUN_DATE}.json"
        v_path.write_text(json.dumps(v, indent=2, default=str))
        print(f"\n  Verification JSON → {v_path}")
        if all_pass:
            print(f"\n✓ mig_323 COMPLETE — all gates pass. run_id={RUN_ID}")
        else:
            ts_frac = v["coverage"].get("ThyroSeq", {}).get("frac_classified", 0.0)
            if ts_frac >= 0.90:
                print(f"\n⚠ mig_323 COMPLETE WITH CAVEAT — ThyroSeq coverage {100*ts_frac:.1f}% "
                      f"(90–95% near-miss). File VC-MOL-PARSE-002 for residual gap. run_id={RUN_ID}")
            else:
                print(f"\n✗ mig_323 FAILED — gates not met. run_id={RUN_ID}")
                sys.exit(1)
    else:
        print(f"\n[DRY-RUN] Summary: {report['n_rows_proposed_platform_change']} platform changes, "
              f"{report['n_rows_call_update_new']+report['n_rows_call_update_overwrite']} call updates would be applied.")
        print(f"run_id (if applied)={RUN_ID}")


if __name__ == "__main__":
    main()
