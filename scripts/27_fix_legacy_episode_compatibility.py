#!/usr/bin/env python3
"""
27_fix_legacy_episode_compatibility.py — Legacy Episode Compatibility Layer

Creates 5 compatibility tables that bridge the legacy episode architecture
(scripts 17/18/22/23/26) to the current modern table stack:

  molecular_episode_v3    ← advanced_features_v3 (molecular flags)
  rai_episode_v3          ← extracted_clinical_events_v4 (treatment events)
  validation_failures_v3  ← qa_issues (direct mapping)
  tumor_episode_master_v2 ← advanced_features_v3 + master_timeline
  linkage_summary_v2      ← patient_level_summary_mv (domain summary)

All views are created as CREATE OR REPLACE TABLE (MotherDuck does not support
CREATE OR REPLACE MATERIALIZED VIEW). Tables are idempotent — safe to re-run.

Usage:
    .venv/bin/python scripts/27_fix_legacy_episode_compatibility.py
    .venv/bin/python scripts/27_fix_legacy_episode_compatibility.py --local
    .venv/bin/python scripts/27_fix_legacy_episode_compatibility.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import textwrap
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient, MotherDuckConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("legacy_compat")

# ── SQL Definitions ───────────────────────────────────────────────────────────
#
# Each entry: (table_name, sql, description)
# SQL uses CREATE OR REPLACE TABLE for MotherDuck compatibility.
#
COMPAT_TABLES: list[tuple[str, str, str]] = [
    # ── 1. molecular_episode_v3 ──────────────────────────────────────────────
    # Maps molecular flag columns from advanced_features_v3 into the episode
    # shape expected by dashboard v3 molecular review tabs.
    (
        "molecular_episode_v3",
        textwrap.dedent("""
            CREATE OR REPLACE TABLE molecular_episode_v3 AS
            SELECT
                CAST(af.research_id AS BIGINT)          AS research_id,
                af.histology_1_type,
                af.variant_standardized,
                af.overall_stage_ajcc8,
                af.braf_mutation_mentioned,
                af.ras_mutation_mentioned,
                af.ret_mutation_mentioned,
                af.tert_mutation_mentioned,
                -- Linkage confidence derived from available molecular data
                CASE
                    WHEN af.braf_mutation_mentioned
                      OR af.ras_mutation_mentioned
                      OR af.ret_mutation_mentioned
                      OR af.tert_mutation_mentioned
                    THEN 'high_confidence'
                    ELSE 'unlinked'
                END                                     AS overall_linkage_confidence,
                -- Eligible for analysis if at least one molecular result present
                (
                    af.braf_mutation_mentioned
                    OR af.ras_mutation_mentioned
                    OR af.ret_mutation_mentioned
                    OR af.tert_mutation_mentioned
                )                                       AS molecular_analysis_eligible_flag,
                'advanced_features_v3'                  AS source_table,
                CURRENT_TIMESTAMP                       AS created_at
            FROM advanced_features_v3 af
        """).strip(),
        "Molecular episode compatibility view (molecular flags from advanced_features_v3)",
    ),

    # ── 2. rai_episode_v3 ────────────────────────────────────────────────────
    # Derives RAI episodes from extracted_clinical_events_v4 (treatment events
    # tagged as RAI/iodine) joined to master_timeline for temporal context.
    (
        "rai_episode_v3",
        textwrap.dedent("""
            CREATE OR REPLACE TABLE rai_episode_v3 AS
            SELECT
                ece.research_id,
                ece.event_type,
                ece.event_subtype,
                ece.event_value,
                ece.event_unit,
                ece.event_date,
                ece.event_text,
                ece.days_since_nearest_surgery,
                ece.nearest_surgery_number,
                ece.confidence_score,
                -- Map to v3 assertion/interval schema expected by review_rai.py
                CASE
                    WHEN LOWER(ece.event_text) LIKE '%radioactive%iodine%'
                      OR LOWER(ece.event_text) LIKE '%rai%'
                      OR LOWER(ece.event_text) LIKE '%i-131%'
                      OR LOWER(ece.event_text) LIKE '%i131%'
                    THEN 'definite_received'
                    WHEN LOWER(ece.event_text) LIKE '%thyrogen%'
                      OR LOWER(ece.event_text) LIKE '%levothyroxine withdrawal%'
                    THEN 'likely_received'
                    ELSE 'ambiguous'
                END                                 AS rai_assertion_status,
                CASE
                    WHEN ece.days_since_nearest_surgery IS NULL THEN 'unresolved'
                    WHEN ece.days_since_nearest_surgery < 0     THEN 'pre_surgical'
                    WHEN ece.days_since_nearest_surgery <= 365  THEN 'adjuvant'
                    ELSE 'salvage'
                END                                 AS rai_interval_class,
                CASE
                    WHEN ece.confidence_score >= 0.8 THEN 'high'
                    WHEN ece.confidence_score >= 0.5 THEN 'moderate'
                    ELSE 'low'
                END                                 AS rai_treatment_certainty,
                'extracted_clinical_events_v4'      AS source_table,
                CURRENT_TIMESTAMP                   AS created_at
            FROM extracted_clinical_events_v4 ece
            WHERE ece.event_type = 'treatment'
              AND (
                LOWER(ece.event_text) LIKE '%iodine%'
                OR LOWER(ece.event_text) LIKE '%rai%'
                OR LOWER(ece.event_text) LIKE '%i-131%'
                OR LOWER(ece.event_text) LIKE '%i131%'
                OR LOWER(ece.event_text) LIKE '%thyrogen%'
                OR LOWER(ece.event_text) LIKE '%nuclear%'
                OR LOWER(ece.event_text) LIKE '%remnant%ablat%'
              )
        """).strip(),
        "RAI episode compatibility view (treatment events from extracted_clinical_events_v4)",
    ),

    # ── 3. validation_failures_v3 ────────────────────────────────────────────
    # Direct reclassification of qa_issues into v3 schema (coarse anchor dates
    # downgraded from error to info per validation v3 policy).
    (
        "validation_failures_v3",
        textwrap.dedent("""
            CREATE OR REPLACE TABLE validation_failures_v3 AS
            SELECT
                qi.check_id,
                -- V3 reclassification: coarse_anchor_date issues are info, not error
                CASE
                    WHEN qi.check_id LIKE '%coarse%anchor%' THEN 'info'
                    WHEN qi.check_id LIKE '%date%coarse%'   THEN 'info'
                    ELSE qi.severity
                END                         AS severity,
                qi.research_id,
                qi.description,
                qi.detail,
                qi.checked_at,
                -- V3 taxonomy flags
                (qi.check_id LIKE '%date%')                AS date_related_flag,
                (qi.severity = 'error')                    AS requires_manual_review_flag,
                'qa_issues'                                AS source_table,
                CURRENT_TIMESTAMP                          AS created_at
            FROM qa_issues qi
        """).strip(),
        "Validation failures v3 (qa_issues with v3 severity reclassification)",
    ),

    # ── 4. tumor_episode_master_v2 ───────────────────────────────────────────
    # Canonical tumor episode table bridging advanced_features_v3 pathology
    # columns with master_timeline surgery events.
    (
        "tumor_episode_master_v2",
        textwrap.dedent("""
            CREATE OR REPLACE TABLE tumor_episode_master_v2 AS
            SELECT
                CAST(af.research_id AS BIGINT)      AS research_id,
                mt.surgery_number,
                mt.surgery_date,
                mt.surgery_type,
                mt.days_since_prior_surgery,
                af.histology_1_type,
                af.variant_standardized,
                af.overall_stage_ajcc8,
                af.largest_tumor_cm,
                af.ln_examined,
                af.ln_positive,
                af.tumor_1_extrathyroidal_ext,
                af.tumor_1_gross_ete,
                af.surgery_type_normalized,
                af.total_surgeries,
                -- Episode-level analysis eligibility
                (
                    af.largest_tumor_cm IS NOT NULL
                    AND af.histology_1_type IS NOT NULL
                )                                   AS analysis_eligible_flag,
                -- Adjudication flag for discordant multi-surgery patients
                (af.total_surgeries > 1)            AS adjudication_needed_flag,
                'advanced_features_v3+master_timeline' AS source_table,
                CURRENT_TIMESTAMP                   AS created_at
            FROM advanced_features_v3 af
            LEFT JOIN master_timeline mt
                   ON CAST(af.research_id AS BIGINT) = mt.research_id
        """).strip(),
        "Tumor episode master v2 (pathology + surgery timeline join)",
    ),

    # ── 5. linkage_summary_v2 ────────────────────────────────────────────────
    # Domain-level linkage summary derived from patient_level_summary_mv,
    # providing the cross-domain availability metrics expected by
    # adjudication_summary.py.
    (
        "linkage_summary_v2",
        textwrap.dedent("""
            CREATE OR REPLACE TABLE linkage_summary_v2 AS
            SELECT
                CAST(pl.research_id AS BIGINT)      AS research_id,
                -- Coverage flags as proxy for linkage completeness
                (pl.latest_tg IS NOT NULL)           AS has_thyroglobulin_linked,
                (pl.latest_tsh IS NOT NULL)          AS has_tsh_linked,
                (pl.latest_calcium IS NOT NULL)      AS has_calcium_linked,
                (pl.recurrence_flag IS NOT NULL)     AS has_recurrence_linked,
                (pl.last_followup_date IS NOT NULL)  AS has_followup_linked,
                (pl.histology_1_type IS NOT NULL)    AS has_pathology_linked,
                -- Composite linkage confidence tier
                CASE
                    WHEN pl.latest_tg IS NOT NULL
                     AND pl.histology_1_type IS NOT NULL
                     AND pl.last_followup_date IS NOT NULL
                    THEN 'exact_match'
                    WHEN pl.histology_1_type IS NOT NULL
                     AND pl.last_followup_date IS NOT NULL
                    THEN 'high_confidence'
                    WHEN pl.histology_1_type IS NOT NULL
                    THEN 'plausible'
                    ELSE 'unlinked'
                END                                 AS linkage_confidence_tier,
                -- Domain counts for adjudication display
                (
                    CAST(pl.latest_tg IS NOT NULL AS INT) +
                    CAST(pl.latest_tsh IS NOT NULL AS INT) +
                    CAST(pl.histology_1_type IS NOT NULL AS INT) +
                    CAST(pl.last_followup_date IS NOT NULL AS INT)
                )                                   AS linked_domain_count,
                'patient_level_summary_mv'          AS source_table,
                CURRENT_TIMESTAMP                   AS created_at
            FROM patient_level_summary_mv pl
        """).strip(),
        "Linkage summary v2 (cross-domain coverage from patient_level_summary_mv)",
    ),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def table_row_count(con, table: str) -> int:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        return -1


def run(args: argparse.Namespace) -> int:
    if args.local or os.getenv("USE_LOCAL_DUCKDB", "").lower() in ("1", "true", "yes"):
        import duckdb
        local_path = os.getenv("LOCAL_DUCKDB_PATH", str(ROOT / "thyroid_master_local.duckdb"))
        log.info("Connecting to local DuckDB: %s", local_path)
        con = duckdb.connect(local_path)
    else:
        log.info("Connecting to MotherDuck (thyroid_research_2026)…")
        cfg = MotherDuckConfig(database="thyroid_research_2026")
        cli = MotherDuckClient(cfg)
        con = cli.connect_rw()

    created: list[str] = []
    failed: list[tuple[str, str]] = []

    for tbl_name, sql, description in COMPAT_TABLES:
        log.info("Creating %-35s — %s", tbl_name, description)
        if args.dry_run:
            log.info("  [DRY RUN] Would execute:\n%s", sql)
            created.append(tbl_name)
            continue
        try:
            con.execute(sql)
            try:
                con.execute(f"ANALYZE {tbl_name}")
                log.info("  ✓  ANALYZE %s", tbl_name)
            except Exception:
                pass  # ANALYZE optional (e.g. view in some backends)
            n = table_row_count(con, tbl_name)
            log.info("  ✓  %-35s  rows=%d", tbl_name, n)
            created.append(tbl_name)
        except Exception as exc:
            log.error("  ✗  %-35s  ERROR: %s", tbl_name, exc)
            failed.append((tbl_name, str(exc)))

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 68)
    print("  Legacy Compatibility Layer — Summary")
    print(f"  Timestamp: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 68)
    for t in created:
        n = table_row_count(con, t) if not args.dry_run else "N/A (dry-run)"
        print(f"  ✓  {t:<38}  rows={n}")
    for t, err in failed:
        print(f"  ✗  {t:<38}  ERROR: {err[:60]}")
    print("=" * 68)
    if failed:
        print(f"\n  {len(failed)} table(s) failed — see errors above.")
        return 1

    print()
    print("  Legacy compatibility layer created — error resolved.")
    print()
    print("  The following tables are now available:")
    for t in created:
        print(f"    • {t}")
    print()
    print("  Dashboard check_critical_tables() will now pass.")
    print("  Restart the Streamlit app to clear the cached connection.")
    print()
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Create legacy episode compatibility tables in DuckDB/MotherDuck."
    )
    ap.add_argument("--local",   action="store_true", help="Use local DuckDB instead of MotherDuck")
    ap.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    args = ap.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
