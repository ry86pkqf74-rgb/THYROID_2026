#!/usr/bin/env python3
"""
mig_321: ThyroSeq ROM-band backfill — BigQuery canonical layer.

Fixes 647 ThyroSeq rows in pub_canonical.canonical_molecular_genetics_v2 where
rom_descriptor IS NULL, using the updated thyroseq_detailed_parser v4 fallbacks:
  Fallback A — numeric rom_percent_point → band (deterministic thresholds)
  Fallback B — full-text scan for band keywords / ROM% near malignancy language

Hard rules per the thyroid-integration skill:
  1. No PHI committed anywhere. research_id only.
  2. Append-only: uses MERGE … WHEN MATCHED AND target.rom_descriptor IS NULL.
  3. Pre-merge snapshot to pub_archive before any write.
  4. Audit columns: band_backfill_applied_at, band_backfill_source, band_backfill_run_id.
  5. DFL row DFL-20260509-EXT2-4-THYROSEQ-BAND-BACKFILL logged before running.
  6. VC-MOL-PARSE-001 must be filed and linked before production apply.

Usage:
  # Dry-run (no BQ writes):
  .venv/bin/python scripts/mig_321_thyroseq_band_backfill_bq.py --dry-run

  # Full apply:
  .venv/bin/python scripts/mig_321_thyroseq_band_backfill_bq.py --apply

Environment:
  GOOGLE_APPLICATION_CREDENTIALS  — service account JSON path (or use gcloud auth)
  BQ_PROJECT                       — defaults to thyroid-canonical-pub-2026
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MCONS = REPO_ROOT / "molecular_consolidation_20260421"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(MCONS))

BQ_PROJECT_DEFAULT = "thyroid-canonical-pub-2026"
CANONICAL_DATASET = "pub_canonical"
WORKSPACE_DATASET = "pub_workspace"
ARCHIVE_DATASET = "pub_archive"
CMG_TABLE = "canonical_molecular_genetics_v2"
RUN_DATE = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
RUN_ID = f"mig_321_{RUN_DATE}_{uuid.uuid4().hex[:8]}"
STAGING_TABLE = f"canonical_molecular_genetics_v2_band_backfill_{RUN_DATE}"
ARCHIVE_TABLE = f"canonical_molecular_genetics_v2_pre_band_backfill_{RUN_DATE}"
PARSER_VERSION = "thyroseq_detailed_parser_v4_mig321"
SCRIPT_TAG = "mig_321_thyroseq_band_backfill_bq"

# Composite key columns (no surrogate genomic_assay_id exists in this table)
_KEY_COLS = ("research_id", "molecular_episode_id", "report_source_table")


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def _src_query(project: str) -> str:
    """Pull unclassified ThyroSeq rows + report text.

    Composite key: (research_id, molecular_episode_id, report_source_table).
    Source tables: pub_canonical.thyroseq_molecular_enrichment + molecular_testing.
    """
    return f"""
SELECT
    cmg.research_id,
    cmg.molecular_episode_id,
    cmg.report_source_table,
    cmg.rom_percent_point,
    cmg.parse_status,
    cmg.report_text_ref,
    cmg.report_text_length,
    COALESCE(
        NULLIF(TRIM(tme.pathology_raw), ''),
        NULLIF(TRIM(mt.detailed_findings), '')
    ) AS report_text_joined
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` cmg
LEFT JOIN `{project}.{CANONICAL_DATASET}.thyroseq_molecular_enrichment` tme
       ON tme.research_id = cmg.research_id
LEFT JOIN (
    SELECT research_id,
           (ARRAY_AGG(
               detailed_findings
               ORDER BY CHAR_LENGTH(COALESCE(detailed_findings, '')) DESC
               LIMIT 1
           ))[OFFSET(0)] AS detailed_findings
    FROM `{project}.{CANONICAL_DATASET}.molecular_testing`
    WHERE detailed_findings IS NOT NULL
      AND CHAR_LENGTH(TRIM(detailed_findings)) > 50
    GROUP BY research_id
) mt ON mt.research_id = cmg.research_id
WHERE cmg.platform = 'ThyroSeq'
  AND cmg.rom_descriptor IS NULL
  AND (
      cmg.overall_result_class NOT IN ('positive', 'negative')
      OR cmg.overall_result_class IS NULL
  )
ORDER BY cmg.research_id, COALESCE(CAST(cmg.molecular_episode_id AS STRING), '')
"""


def _add_audit_columns_ddl(project: str) -> list[str]:
    """DDL to add band_backfill audit columns if they don't exist.

    BigQuery supports ADD COLUMN on existing tables (idempotent if column already exists
    only via IF NOT EXISTS syntax, available since 2022).
    """
    base = f"`{project}.{CANONICAL_DATASET}.{CMG_TABLE}`"
    return [
        f"ALTER TABLE {base} ADD COLUMN IF NOT EXISTS band_backfill_applied_at TIMESTAMP",
        f"ALTER TABLE {base} ADD COLUMN IF NOT EXISTS band_backfill_source STRING",
        f"ALTER TABLE {base} ADD COLUMN IF NOT EXISTS band_backfill_run_id STRING",
    ]


def _merge_sql(project: str) -> str:
    """MERGE staging → canonical using composite key.

    Guard: only when tgt.rom_descriptor IS NULL (idempotent).
    Composite key: research_id + molecular_episode_id + report_source_table.
    """
    stg = f"`{project}.{WORKSPACE_DATASET}.{STAGING_TABLE}`"
    cmg = f"`{project}.{CANONICAL_DATASET}.{CMG_TABLE}`"
    return f"""
MERGE {cmg} AS tgt
USING (
    SELECT * FROM {stg}
    -- Deduplicate staging to prevent BQ MERGE "target row matched multiple times" error
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id,
                     CAST(molecular_episode_id AS STRING),
                     COALESCE(report_source_table, '')
        ORDER BY band_backfill_run_id
    ) = 1
) AS src
ON tgt.research_id = src.research_id
   AND tgt.molecular_episode_id IS NOT DISTINCT FROM src.molecular_episode_id
   AND tgt.report_source_table IS NOT DISTINCT FROM src.report_source_table
WHEN MATCHED AND tgt.rom_descriptor IS NULL THEN
  UPDATE SET
    tgt.rom_descriptor          = src.rom_descriptor_new,
    tgt.rom_percent_point       = COALESCE(tgt.rom_percent_point, src.rom_percent_point_new),
    tgt.overall_result_class    = CASE
        WHEN tgt.overall_result_class NOT IN ('positive', 'negative', 'intermediate')
             OR tgt.overall_result_class IS NULL
        THEN src.overall_result_class_inferred
        ELSE tgt.overall_result_class
    END,
    tgt.band_backfill_applied_at = CURRENT_TIMESTAMP(),
    tgt.band_backfill_source     = src.band_source,
    tgt.band_backfill_run_id     = src.band_backfill_run_id
"""


def _validation_query(project: str) -> str:
    return f"""
SELECT
    COUNTIF(rom_descriptor IS NOT NULL) AS n_with_band,
    COUNTIF(rom_descriptor IS NULL)     AS n_still_null,
    COUNT(*)                            AS n_thyroseq,
    SAFE_DIVIDE(COUNTIF(rom_descriptor IS NOT NULL), COUNT(*)) AS frac_with_band,
    COUNTIF(band_backfill_source = 'numeric_rom_inferred') AS n_inferred_numeric,
    COUNTIF(band_backfill_source = 'reported_text')         AS n_from_text,
    COUNTIF(band_backfill_source = 'manual_review')         AS n_manual_review
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
WHERE platform = 'ThyroSeq'
"""


def _no_overwrite_check(project: str) -> str:
    """Check that no pre-existing non-NULL rom_descriptor was changed."""
    return f"""
SELECT COUNT(*) AS n_overwritten_bands
FROM `{project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}` pre
JOIN `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` post
  ON pre.research_id = post.research_id
  AND pre.molecular_episode_id IS NOT DISTINCT FROM post.molecular_episode_id
  AND pre.report_source_table IS NOT DISTINCT FROM post.report_source_table
WHERE pre.rom_descriptor IS NOT NULL
  AND pre.rom_descriptor != post.rom_descriptor
"""


# ---------------------------------------------------------------------------
# Parser helpers
# ---------------------------------------------------------------------------

def _load_parser():
    """Dynamically load thyroseq_detailed_parser from the molecular_consolidation dir."""
    spec = importlib.util.spec_from_file_location(
        "thyroseq_detailed_parser",
        MCONS / "thyroseq_detailed_parser.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _pick_text(row_dict: dict) -> tuple[str, str]:
    """Return (report_text, source_label) from a BQ row dict."""
    joined = (row_dict.get("report_text_joined") or "").strip()
    if joined:
        tbl = row_dict.get("report_source_table") or "joined"
        return joined, tbl
    # Fall back to any pre-existing numeric ROM% (Fallback A will handle it)
    return "", "no_text"


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def phase_archive(client, project: str, apply: bool) -> None:
    """Snapshot the full CMG table to pub_archive before any writes."""
    archive_ref = f"`{project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}`"
    src_ref = f"`{project}.{CANONICAL_DATASET}.{CMG_TABLE}`"
    print(f"[{SCRIPT_TAG}] Phase archive → {archive_ref}")
    if not apply:
        print(f"[{SCRIPT_TAG}]   DRY-RUN: skipping archive snapshot")
        return
    ddl = f"CREATE OR REPLACE TABLE {archive_ref} AS SELECT * FROM {src_ref}"
    job = client.query(ddl)
    job.result()
    print(f"[{SCRIPT_TAG}]   Snapshot complete.")


def phase_add_audit_columns(client, project: str, apply: bool) -> None:
    """Add band_backfill audit columns to CMG (idempotent — IF NOT EXISTS)."""
    print(f"[{SCRIPT_TAG}] Phase add_audit_columns: band_backfill_{{applied_at,source,run_id}}")
    if not apply:
        print(f"[{SCRIPT_TAG}]   DRY-RUN: skipping DDL")
        return
    for ddl in _add_audit_columns_ddl(project):
        try:
            client.query(ddl).result()
            print(f"[{SCRIPT_TAG}]   OK: {ddl[:80]}")
        except Exception as exc:
            # Column may already exist; treat duplicate-column error as non-fatal
            if "already exists" in str(exc).lower() or "duplicate" in str(exc).lower():
                print(f"[{SCRIPT_TAG}]   SKIP (already exists): {ddl[:80]}")
            else:
                raise


def phase_parse(client, project: str, _apply: bool, parser_mod) -> list[dict]:
    """Fetch unclassified rows, re-parse, return staging records."""
    from google.cloud import bigquery as bq_module

    print(f"[{SCRIPT_TAG}] Phase parse: fetching unclassified ThyroSeq rows…")
    rows = list(client.query(_src_query(project)).result())
    print(f"[{SCRIPT_TAG}]   {len(rows)} unclassified rows fetched")

    staging: list[dict] = []
    n_fallback_a = n_fallback_b = n_text_parsed = n_manual = 0

    for row in rows:
        rd = dict(row)
        text, src_label = _pick_text(rd)

        # Pre-populate with existing numeric ROM% so Fallback A can fire even with no text
        existing_rom = rd.get("rom_percent_point")

        if text:
            parsed = parser_mod.parse(text, platform="ThyroSeq")
            # If parser didn't find a numeric ROM but CMG already has one, inject for Fallback A
            if parsed.get("rom_percent_point") is None and existing_rom is not None:
                parsed["rom_percent_point"] = float(existing_rom)
                parser_mod._apply_band_fallbacks(parsed, text)
        else:
            # No text at all — only Fallback A possible via existing rom_percent_point
            parsed = {"parse_status": "no_text", "parser": "thyroseq"}
            if existing_rom is not None:
                parsed["rom_percent_point"] = float(existing_rom)
            parser_mod._apply_band_fallbacks(parsed, "")

        rom_desc = parsed.get("rom_descriptor")
        band_src = parsed.get("band_source", "manual_review")
        inferred_cls = parsed.get("overall_result_class_inferred")

        if band_src == "numeric_rom_inferred":
            n_fallback_a += 1
        elif band_src == "reported_text":
            n_text_parsed += 1
        else:
            n_manual += 1

        staging.append({
            "research_id": str(rd["research_id"]),
            "molecular_episode_id": rd.get("molecular_episode_id"),
            "report_source_table": rd.get("report_source_table"),
            "rom_descriptor_new": rom_desc,
            "rom_percent_point_new": parsed.get("rom_percent_point"),
            "overall_result_class_inferred": inferred_cls,
            "band_source": band_src,
            "parse_status_new": parsed.get("parse_status"),
            "parser_version": PARSER_VERSION,
            "report_text_source": src_label,
            "band_backfill_run_id": RUN_ID,
        })

    print(
        f"[{SCRIPT_TAG}]   Parsed: reported_text={n_text_parsed}, "
        f"numeric_inferred={n_fallback_a}, manual_review={n_manual}"
    )
    return staging


def phase_write_staging(client, project: str, staging: list[dict], apply: bool) -> None:
    """Write staging records to pub_workspace."""
    from google.cloud import bigquery as bq_module

    stg_ref = f"{project}.{WORKSPACE_DATASET}.{STAGING_TABLE}"
    print(f"[{SCRIPT_TAG}] Phase staging → {stg_ref}  ({len(staging)} rows)")
    if not apply:
        print(f"[{SCRIPT_TAG}]   DRY-RUN: skipping staging write")
        # Print a sample
        for r in staging[:3]:
            print(f"    sample: {json.dumps({k: v for k, v in r.items() if k != 'research_id'})}")
        return

    schema = [
        bq_module.SchemaField("research_id", "STRING"),
        bq_module.SchemaField("molecular_episode_id", "INTEGER"),
        bq_module.SchemaField("report_source_table", "STRING"),
        bq_module.SchemaField("rom_descriptor_new", "STRING"),
        bq_module.SchemaField("rom_percent_point_new", "FLOAT64"),
        bq_module.SchemaField("overall_result_class_inferred", "STRING"),
        bq_module.SchemaField("band_source", "STRING"),
        bq_module.SchemaField("parse_status_new", "STRING"),
        bq_module.SchemaField("parser_version", "STRING"),
        bq_module.SchemaField("report_text_source", "STRING"),
        bq_module.SchemaField("band_backfill_run_id", "STRING"),
    ]

    table_ref = bq_module.Table(stg_ref, schema=schema)
    table = client.create_table(table_ref, exists_ok=True)
    errors = client.insert_rows_json(table, staging)
    if errors:
        raise RuntimeError(f"BQ insert errors: {errors[:5]}")
    print(f"[{SCRIPT_TAG}]   Staging write complete.")


def phase_merge(client, project: str, apply: bool) -> int:
    """MERGE staging → canonical; only when rom_descriptor IS NULL on target."""
    sql = _merge_sql(project)
    print(f"[{SCRIPT_TAG}] Phase MERGE (idempotent guard: tgt.rom_descriptor IS NULL)")
    if not apply:
        print(f"[{SCRIPT_TAG}]   DRY-RUN: MERGE not executed")
        return 0

    job = client.query(sql)
    job.result()
    n_affected = job.num_dml_affected_rows or 0
    print(f"[{SCRIPT_TAG}]   MERGE affected rows: {n_affected}")
    return n_affected


def phase_verify(client, project: str) -> dict:
    """Run the three acceptance-criteria queries."""
    from google.cloud import bigquery as bq_module

    print(f"[{SCRIPT_TAG}] Phase verify…")

    coverage = list(client.query(_validation_query(project)).result())[0]
    cov = dict(coverage)

    no_overwrite = list(client.query(_no_overwrite_check(project)).result())[0][0]

    print(f"[{SCRIPT_TAG}]   frac_with_band     = {cov.get('frac_with_band', '?'):.4f}")
    print(f"[{SCRIPT_TAG}]   n_inferred_numeric = {cov.get('n_inferred_numeric')}")
    print(f"[{SCRIPT_TAG}]   n_from_text        = {cov.get('n_from_text')}")
    print(f"[{SCRIPT_TAG}]   n_manual_review    = {cov.get('n_manual_review')}")
    print(f"[{SCRIPT_TAG}]   no-overwrite check = {no_overwrite} rows changed (must be 0)")

    frac = cov.get("frac_with_band") or 0.0
    if frac < 0.95:
        print(
            f"[{SCRIPT_TAG}]   *** ASSERTION FAIL: frac_with_band={frac:.4f} < 0.95 ***"
        )
        print(f"[{SCRIPT_TAG}]   *** DO NOT bump skill version. Surface diff and stop. ***")
    else:
        print(f"[{SCRIPT_TAG}]   ✓ Coverage gate PASS ({frac:.1%})")

    if no_overwrite != 0:
        print(
            f"[{SCRIPT_TAG}]   *** ASSERTION FAIL: {no_overwrite} pre-existing bands changed ***"
        )
    else:
        print(f"[{SCRIPT_TAG}]   ✓ No-overwrite gate PASS")

    return {**cov, "n_overwritten": no_overwrite, "run_id": RUN_ID}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Write to BQ (archive + staging + MERGE). Default: dry-run.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Explicit dry-run (same as omitting --apply).")
    ap.add_argument("--project", default=BQ_PROJECT_DEFAULT,
                    help=f"GCP project (default: {BQ_PROJECT_DEFAULT})")
    ap.add_argument("--skip-verify", action="store_true",
                    help="Skip post-merge verification (useful for debugging staging)")
    args = ap.parse_args()

    apply = bool(args.apply and not args.dry_run)
    project = args.project

    print(f"[{SCRIPT_TAG}] run_id={RUN_ID}  apply={apply}  project={project}")

    # Smoke-test parser import
    parser_mod = _load_parser()
    smoke = parser_mod.parse(
        "TEST RESULTS\nThyroSeq GC: NEGATIVE - LOW (3%)\nDETAILED RESULTS\n"
        "Gene mutations: Not detected\n",
        platform="ThyroSeq",
    )
    assert smoke.get("parser") == "thyroseq", "Parser smoke test failed"
    assert smoke.get("rom_descriptor") == "LOW", f"Expected LOW, got {smoke.get('rom_descriptor')}"
    assert smoke.get("band_source") == "reported_text", f"band_source={smoke.get('band_source')}"
    print(f"[{SCRIPT_TAG}] Parser smoke test PASS")

    if not apply:
        print(f"[{SCRIPT_TAG}] DRY-RUN mode — fetching and parsing only (no BQ writes)")

    try:
        from google.cloud import bigquery as bq_module
    except ImportError:
        print(
            f"[{SCRIPT_TAG}] google-cloud-bigquery not installed. "
            "Install with: pip install google-cloud-bigquery"
        )
        if not apply:
            # Still useful to run parser locally in dry-run mode
            print(f"[{SCRIPT_TAG}] Skipping BQ phase (no bigquery package); dry-run parser only")
            return
        sys.exit(1)

    client = bq_module.Client(project=project)

    phase_archive(client, project, apply)
    phase_add_audit_columns(client, project, apply)

    staging = phase_parse(client, project, apply, parser_mod)

    if not staging:
        print(f"[{SCRIPT_TAG}] No unclassified rows found — nothing to do. Exiting.")
        return

    phase_write_staging(client, project, staging, apply)

    n_merged = phase_merge(client, project, apply)

    if apply and not args.skip_verify:
        metrics = phase_verify(client, project)
        # Write metrics to pub_workspace for traceability
        metrics_table = f"{project}.{WORKSPACE_DATASET}.mig_321_verification_{RUN_DATE}"
        schema = [bq_module.SchemaField(k, "STRING") for k in metrics]
        tbl = bq_module.Table(metrics_table,
                              schema=[bq_module.SchemaField(k, "STRING") for k in metrics])
        client.create_table(tbl, exists_ok=True)
        client.insert_rows_json(tbl, [{k: str(v) for k, v in metrics.items()}])
        print(f"[{SCRIPT_TAG}] Metrics written to {metrics_table}")
    elif not apply:
        print(f"[{SCRIPT_TAG}] DRY-RUN complete. Re-run with --apply to execute BQ writes.")

    print(f"[{SCRIPT_TAG}] Done. run_id={RUN_ID}")


if __name__ == "__main__":
    main()
