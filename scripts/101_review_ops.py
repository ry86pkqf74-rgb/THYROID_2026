#!/usr/bin/env python3
"""
101_review_ops.py  —  Unified Manual Review Queue & Review Ops Governance

Consolidates 46 review queue tables into a single governed schema:
  unified_review_queue_v1

The governed schema:
  review_domain        — histology|molecular|rai|timeline|linkage|recurrence|
                          staging|operative|imaging|complication|demographics|thyroseq
  review_priority      — 0-100 integer score (higher = more urgent)
  research_id          — patient key
  surgery_episode_id   — episode key (NULL if patient-level)
  source_table         — originating table name
  source_artifact_ids  — pipe-delimited IDs from source table
  review_reason        — short free-text reason
  confidence_tier      — exact_match|high_confidence|plausible|weak|unlinked|unknown
  recommended_evidence — suggested evidence to resolve (table/column pointers)
  reviewer_status      — queued|in_progress|resolved|deferred|permanently_source_limited
  reviewed_by          — reviewer handle or NULL
  reviewed_at          — review timestamp or NULL
  final_resolution     — free-text resolution summary or NULL
  limitation_type      — source_feed|template|pipeline|review|none

Also creates:
  review_ops_progress_v1   — per-domain aggregated progress metrics
  review_ops_kpi_v1        — single-row overall KPI summary

Usage:
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/101_review_ops.py --md
  .venv/bin/python scripts/101_review_ops.py --local
  .venv/bin/python scripts/101_review_ops.py --dry-run

Exit codes:
  0  Success
  1  Error
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ── Source table registry ───────────────────────────────────────────────────
# Each entry: (source_table, domain, priority_base, limitation_type, sql_template)
# sql_template yields: research_id, surgery_episode_id, review_reason, confidence_tier,
#                      recommended_evidence, source_artifact_ids

REVIEW_QUEUE_SOURCES = [
    # ── Histology ──
    {
        "source_table": "histology_manual_review_queue_v",
        "domain": "histology",
        "priority_base": 70,
        "limitation_type": "review",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   COALESCE(unresolved_reason, 'histology discordance') AS review_reason,
                   'plausible' AS confidence_tier,
                   'path_synoptics, tumor_pathology' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) AS source_artifact_ids
            FROM histology_manual_review_queue_v
        """,
    },
    # ── Molecular ──
    {
        "source_table": "md_molecular_manual_review_queue",
        "domain": "molecular",
        "priority_base": 65,
        "limitation_type": "review",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   'molecular linkage/discordance review' AS review_reason,
                   'plausible' AS confidence_tier,
                   'molecular_test_episode_v2, fna_molecular_linkage_v3' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) AS source_artifact_ids
            FROM md_molecular_manual_review_queue
        """,
    },
    # ── RAI ──
    {
        "source_table": "md_rai_manual_review_queue",
        "domain": "rai",
        "priority_base": 60,
        "limitation_type": "review",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   'RAI treatment review' AS review_reason,
                   'plausible' AS confidence_tier,
                   'rai_treatment_episode_v2, pathology_rai_linkage_v3' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) AS source_artifact_ids
            FROM md_rai_manual_review_queue
        """,
    },
    # ── Timeline ──
    {
        "source_table": "md_timeline_manual_review_queue",
        "domain": "timeline",
        "priority_base": 55,
        "limitation_type": "review",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   'timeline inconsistency' AS review_reason,
                   'unknown' AS confidence_tier,
                   'patient_master_timeline_v2, extracted_clinical_events_v4' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) AS source_artifact_ids
            FROM md_timeline_manual_review_queue
        """,
    },
    # ── Linkage ambiguity ──
    {
        "source_table": "linkage_ambiguity_review_v1",
        "domain": "linkage",
        "priority_base": 75,
        "limitation_type": "pipeline",
        "sql": """
            SELECT research_id,
                   COALESCE(TRY_CAST(episode_id AS BIGINT), NULL) AS surgery_episode_id,
                   'multi-candidate linkage ambiguity (' || COALESCE(domain,'unknown') || ')' AS review_reason,
                   CASE WHEN n_candidates >= 3 THEN 'weak'
                        WHEN n_candidates = 2 THEN 'plausible'
                        ELSE 'unknown' END AS confidence_tier,
                   'preop_surgery_linkage_v3, surgery_pathology_linkage_v3' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) || '|' || COALESCE(CAST(episode_id AS VARCHAR),'') AS source_artifact_ids
            FROM linkage_ambiguity_review_v1
        """,
    },
    # ── Recurrence ──
    {
        "source_table": "recurrence_manual_review_queue_v1",
        "domain": "recurrence",
        "priority_base": 80,
        "limitation_type": "source_feed",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   'unresolved recurrence date (' || COALESCE(detection_category,'unknown') || ')' AS review_reason,
                   CASE WHEN COALESCE(priority_score,0) >= 80 THEN 'high_confidence'
                        WHEN COALESCE(priority_score,0) >= 50 THEN 'plausible'
                        ELSE 'weak' END AS confidence_tier,
                   'extracted_recurrence_refined_v1, thyroglobulin_labs' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) AS source_artifact_ids
            FROM recurrence_manual_review_queue_v1
        """,
    },
    # ── Episode linkage v2 ──
    {
        "source_table": "val_episode_linkage_v2_review_queue",
        "domain": "linkage",
        "priority_base": 72,
        "limitation_type": "pipeline",
        "sql": """
            SELECT research_id,
                   COALESCE(TRY_CAST(original_ep_id AS BIGINT), NULL) AS surgery_episode_id,
                   'episode linkage v2 review (' || COALESCE(domain,'') || ')' AS review_reason,
                   COALESCE(v2_confidence_tier, 'unknown') AS confidence_tier,
                   'episode_linkage_v2_sp_rescored, surgery_pathology_linkage_v3' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) || '|' || COALESCE(CAST(original_ep_id AS VARCHAR),'') AS source_artifact_ids
            FROM val_episode_linkage_v2_review_queue
        """,
    },
    # ── Multi-surgery review ──
    {
        "source_table": "val_multi_surgery_review_queue_v3",
        "domain": "linkage",
        "priority_base": 68,
        "limitation_type": "pipeline",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   'multi-surgery routing review' AS review_reason,
                   'plausible' AS confidence_tier,
                   'tumor_episode_master_v2, operative_episode_detail_v2' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) AS source_artifact_ids
            FROM val_multi_surgery_review_queue_v3
        """,
    },
    # ── QA high priority ──
    {
        "source_table": "qa_high_priority_review_v2",
        "domain": "staging",
        "priority_base": 60,
        "limitation_type": "pipeline",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   COALESCE(check_id, 'qa issue') || ': ' || COALESCE(LEFT(description,80), '') AS review_reason,
                   'unknown' AS confidence_tier,
                   'qa_issues_v2' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) || '|' || COALESCE(check_id,'') AS source_artifact_ids
            FROM qa_high_priority_review_v2
        """,
    },
    # ── Hardening review ──
    {
        "source_table": "hardening_review_queue",
        "domain": "staging",
        "priority_base": 65,
        "limitation_type": "pipeline",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   COALESCE(domain, 'hardening') || ': ' || COALESCE(LEFT(detail,80), '') AS review_reason,
                   'unknown' AS confidence_tier,
                   '' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) || '|' || COALESCE(check_id,'') AS source_artifact_ids
            FROM hardening_review_queue
        """,
    },
    # ── Manuscript review ──
    {
        "source_table": "manuscript_review_queue_v2",
        "domain": "staging",
        "priority_base": 85,
        "limitation_type": "review",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   COALESCE(issue_type, 'manuscript discrepancy') || ': ' || COALESCE(LEFT(detail,60),'') AS review_reason,
                   'plausible' AS confidence_tier,
                   'manuscript_cohort_v1' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) AS source_artifact_ids
            FROM manuscript_review_queue_v2
        """,
    },
    # ── ThyroSeq ──
    {
        "source_table": "thyroseq_review_queue",
        "domain": "thyroseq",
        "priority_base": 50,
        "limitation_type": "review",
        "sql": """
            SELECT TRY_CAST(suspected_research_ids AS INTEGER) AS research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   COALESCE(issue_type, 'thyroseq integration review') || ': ' || COALESCE(LEFT(issue_detail,60),'') AS review_reason,
                   'plausible' AS confidence_tier,
                   'thyroseq_molecular_enrichment, stg_thyroseq_match_results' AS recommended_evidence,
                   COALESCE(source_row_hash,'') AS source_artifact_ids
            FROM thyroseq_review_queue
            WHERE suspected_research_ids IS NOT NULL
        """,
    },
    # ── Episode duplicate ──
    {
        "source_table": "episode_duplicate_review_v1",
        "domain": "linkage",
        "priority_base": 55,
        "limitation_type": "pipeline",
        "sql": """
            SELECT research_id,
                   COALESCE(TRY_CAST(surgery_episode_id AS BIGINT), NULL) AS surgery_episode_id,
                   'episode duplicate: ' || COALESCE(review_reason, 'unknown') AS review_reason,
                   'plausible' AS confidence_tier,
                   'episode_analysis_resolved_v1' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) || '|' || COALESCE(CAST(surgery_episode_id AS VARCHAR),'') AS source_artifact_ids
            FROM episode_duplicate_review_v1
        """,
    },
    # ── Canonical backfill ambiguity ──
    {
        "source_table": "canonical_backfill_ambiguity_review",
        "domain": "linkage",
        "priority_base": 58,
        "limitation_type": "pipeline",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   'canonical backfill ambiguity' AS review_reason,
                   'weak' AS confidence_tier,
                   'extracted_rai_dose_refined_v1, fna_molecular_linkage_v3' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) AS source_artifact_ids
            FROM canonical_backfill_ambiguity_review
        """,
    },
    # ── Imaging-pathology concordance ──
    {
        "source_table": "imaging_pathology_concordance_review_v2",
        "domain": "imaging",
        "priority_base": 45,
        "limitation_type": "source_feed",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   'imaging-pathology concordance review' AS review_reason,
                   'plausible' AS confidence_tier,
                   'imaging_nodule_long_v2, path_synoptics' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) AS source_artifact_ids
            FROM imaging_pathology_concordance_review_v2
        """,
    },
    # ── Operative-pathology reconciliation ──
    {
        "source_table": "operative_pathology_reconciliation_review_v2",
        "domain": "operative",
        "priority_base": 50,
        "limitation_type": "review",
        "sql": """
            SELECT research_id,
                   COALESCE(TRY_CAST(surgery_episode_id AS BIGINT), NULL) AS surgery_episode_id,
                   COALESCE(review_reason, 'operative-pathology reconciliation') AS review_reason,
                   'plausible' AS confidence_tier,
                   'operative_episode_detail_v2, path_synoptics' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) || '|' || COALESCE(CAST(surgery_episode_id AS VARCHAR),'') AS source_artifact_ids
            FROM operative_pathology_reconciliation_review_v2
        """,
    },
    # ── Provenance gaps ──
    {
        "source_table": "review_provenance_gaps_v1",
        "domain": "timeline",
        "priority_base": 40,
        "limitation_type": "source_feed",
        "sql": """
            SELECT NULL::INTEGER AS research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   'provenance gap: ' || COALESCE(severity,'') || ' (' || COALESCE(CAST(n_gaps AS VARCHAR),'0') || ' gaps)' AS review_reason,
                   'unknown' AS confidence_tier,
                   COALESCE(affected_tables, '') AS recommended_evidence,
                   COALESCE(gap_types, '') AS source_artifact_ids
            FROM review_provenance_gaps_v1
        """,
    },
    # ── Surgery-path discordance ──
    {
        "source_table": "review_surgery_path_discordance_v1",
        "domain": "staging",
        "priority_base": 62,
        "limitation_type": "review",
        "sql": """
            SELECT research_id,
                   NULL::BIGINT AS surgery_episode_id,
                   'surgery-path discordance' AS review_reason,
                   'plausible' AS confidence_tier,
                   'path_synoptics, operative_episode_detail_v2' AS recommended_evidence,
                   CAST(research_id AS VARCHAR) AS source_artifact_ids
            FROM review_surgery_path_discordance_v1
        """,
    },
]


def safe_exec(con, sql: str, label: str, dry_run: bool = False) -> int:
    """Execute SQL and return row count; on error print and return -1."""
    if dry_run:
        print(f"  [DRY-RUN] Would execute: {label}")
        return 0
    try:
        con.execute(sql)
        # Get count from the table just created
        tbl_match = None
        for kw in ("CREATE OR REPLACE TABLE ", "CREATE TABLE "):
            if kw in sql.upper():
                start = sql.upper().index(kw) + len(kw)
                rest = sql[start:].strip().split()[0]
                tbl_match = rest
                break
        if tbl_match:
            try:
                cnt = con.execute(f"SELECT COUNT(*) FROM {tbl_match}").fetchone()[0]
                return int(cnt)
            except Exception:
                pass
        return 0
    except Exception as e:
        print(f"  ERROR [{label}]: {e}")
        return -1


def build_unified_queue(con, dry_run: bool = False) -> int:
    """Build unified_review_queue_v1 by querying all source tables."""
    print("\n  Phase A: Building unified_review_queue_v1")
    print(f"  Scanning {len(REVIEW_QUEUE_SOURCES)} source table definitions...\n")

    # Probe which tables exist
    available = []
    for src in REVIEW_QUEUE_SOURCES:
        tbl = src["source_table"]
        try:
            con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
            available.append(src)
            print(f"    ✓ {tbl}")
        except Exception:
            print(f"    ✗ {tbl} (not found — skipped)")

    if not available:
        print("  ERROR: No review queue tables found.")
        return -1

    # Build UNION ALL
    union_parts = []
    for src in available:
        part = f"""
        SELECT
            '{src["domain"]}' AS review_domain,
            LEAST(GREATEST(COALESCE(0, 0) + {src["priority_base"]}, 0), 100)::INTEGER AS review_priority,
            sub.research_id::INTEGER AS research_id,
            sub.surgery_episode_id::BIGINT AS surgery_episode_id,
            '{src["source_table"]}' AS source_table,
            CAST(sub.source_artifact_ids AS VARCHAR) AS source_artifact_ids,
            CAST(sub.review_reason AS VARCHAR) AS review_reason,
            CAST(sub.confidence_tier AS VARCHAR) AS confidence_tier,
            CAST(sub.recommended_evidence AS VARCHAR) AS recommended_evidence,
            'queued' AS reviewer_status,
            NULL::VARCHAR AS reviewed_by,
            NULL::TIMESTAMP AS reviewed_at,
            NULL::VARCHAR AS final_resolution,
            '{src["limitation_type"]}' AS limitation_type
        FROM ({src["sql"]}) sub
        """
        union_parts.append(part)

    union_sql = "\n        UNION ALL\n".join(union_parts)

    create_sql = f"""
    CREATE OR REPLACE TABLE unified_review_queue_v1 AS
    WITH raw_union AS (
        {union_sql}
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY review_priority DESC, research_id) AS review_item_id,
        review_domain,
        review_priority,
        research_id,
        surgery_episode_id,
        source_table,
        source_artifact_ids,
        review_reason,
        confidence_tier,
        recommended_evidence,
        reviewer_status,
        reviewed_by,
        reviewed_at,
        final_resolution,
        limitation_type,
        CURRENT_TIMESTAMP AS created_at
    FROM raw_union
    WHERE research_id IS NOT NULL
    """

    cnt = safe_exec(con, create_sql, "unified_review_queue_v1", dry_run)
    if cnt >= 0:
        print(f"\n  ✓ unified_review_queue_v1: {cnt:,} rows from {len(available)} source tables")
    return cnt


def build_progress_table(con, dry_run: bool = False) -> int:
    """Build review_ops_progress_v1 — per-domain aggregation."""
    print("\n  Phase B: Building review_ops_progress_v1")

    sql = """
    CREATE OR REPLACE TABLE review_ops_progress_v1 AS
    SELECT
        review_domain,
        COUNT(*) AS total_items,
        COUNT(*) FILTER (WHERE reviewer_status = 'queued') AS queued,
        COUNT(*) FILTER (WHERE reviewer_status = 'in_progress') AS in_progress,
        COUNT(*) FILTER (WHERE reviewer_status = 'resolved') AS resolved,
        COUNT(*) FILTER (WHERE reviewer_status = 'deferred') AS deferred,
        COUNT(*) FILTER (WHERE reviewer_status = 'permanently_source_limited') AS permanently_source_limited,
        COUNT(DISTINCT research_id) AS unique_patients,
        ROUND(AVG(review_priority), 1) AS avg_priority,
        MAX(review_priority) AS max_priority,
        COUNT(*) FILTER (WHERE limitation_type = 'source_feed') AS source_feed_limited,
        COUNT(*) FILTER (WHERE limitation_type = 'template') AS template_limited,
        COUNT(*) FILTER (WHERE limitation_type = 'pipeline') AS pipeline_limited,
        COUNT(*) FILTER (WHERE limitation_type = 'review') AS review_actionable,
        ROUND(100.0 * COUNT(*) FILTER (WHERE reviewer_status = 'resolved')
              / NULLIF(COUNT(*), 0), 1) AS resolution_rate_pct,
        CURRENT_TIMESTAMP AS computed_at
    FROM unified_review_queue_v1
    GROUP BY review_domain
    ORDER BY total_items DESC
    """
    cnt = safe_exec(con, sql, "review_ops_progress_v1", dry_run)
    if cnt >= 0:
        print(f"  ✓ review_ops_progress_v1: {cnt:,} domain rows")
    return cnt


def build_kpi_table(con, dry_run: bool = False) -> int:
    """Build review_ops_kpi_v1 — single-row overall summary."""
    print("\n  Phase C: Building review_ops_kpi_v1")

    sql = """
    CREATE OR REPLACE TABLE review_ops_kpi_v1 AS
    SELECT
        COUNT(*) AS total_items,
        COUNT(DISTINCT review_domain) AS n_domains,
        COUNT(DISTINCT source_table) AS n_source_tables,
        COUNT(DISTINCT research_id) AS unique_patients,
        COUNT(*) FILTER (WHERE reviewer_status = 'queued') AS queued,
        COUNT(*) FILTER (WHERE reviewer_status = 'resolved') AS resolved,
        COUNT(*) FILTER (WHERE reviewer_status = 'deferred') AS deferred,
        COUNT(*) FILTER (WHERE reviewer_status = 'permanently_source_limited') AS permanently_source_limited,
        ROUND(100.0 * COUNT(*) FILTER (WHERE reviewer_status = 'resolved')
              / NULLIF(COUNT(*), 0), 1) AS overall_resolution_rate_pct,
        COUNT(*) FILTER (WHERE limitation_type = 'source_feed') AS total_source_feed_limited,
        COUNT(*) FILTER (WHERE limitation_type = 'pipeline') AS total_pipeline_limited,
        COUNT(*) FILTER (WHERE limitation_type = 'review') AS total_review_actionable,
        COUNT(*) FILTER (WHERE review_priority >= 80) AS critical_items,
        COUNT(*) FILTER (WHERE review_priority >= 60 AND review_priority < 80) AS high_priority_items,
        COUNT(*) FILTER (WHERE review_priority >= 40 AND review_priority < 60) AS medium_priority_items,
        COUNT(*) FILTER (WHERE review_priority < 40) AS low_priority_items,
        CURRENT_TIMESTAMP AS computed_at
    FROM unified_review_queue_v1
    """
    cnt = safe_exec(con, sql, "review_ops_kpi_v1", dry_run)
    if cnt >= 0:
        print(f"  ✓ review_ops_kpi_v1: 1 row")
    return cnt


def export_artifacts(con, dry_run: bool = False) -> Path:
    """Export unified queue and progress to CSV."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    export_dir = ROOT / "exports" / f"review_ops_{ts}"
    if dry_run:
        print(f"\n  [DRY-RUN] Would export to {export_dir}")
        return export_dir

    export_dir.mkdir(parents=True, exist_ok=True)

    for tbl, fname in [
        ("unified_review_queue_v1", "unified_review_queue.csv"),
        ("review_ops_progress_v1", "review_ops_progress.csv"),
        ("review_ops_kpi_v1", "review_ops_kpi.csv"),
    ]:
        try:
            df = con.execute(f"SELECT * FROM {tbl}").fetchdf()
            df.to_csv(export_dir / fname, index=False)
            print(f"  Exported {fname}: {len(df):,} rows")
        except Exception as e:
            print(f"  WARN: Could not export {tbl}: {e}")

    # Manifest
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "script": "101_review_ops.py",
        "tables_created": [
            "unified_review_queue_v1",
            "review_ops_progress_v1",
            "review_ops_kpi_v1",
        ],
        "source_tables_scanned": len(REVIEW_QUEUE_SOURCES),
    }
    (export_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"  Manifest: {export_dir / 'manifest.json'}")
    return export_dir


def connect(args) -> "duckdb.DuckDBPyConnection":
    """Connect to MotherDuck or local DuckDB."""
    import duckdb

    if args.md:
        token = os.environ.get("MOTHERDUCK_TOKEN", "")
        if not token:
            print("ERROR: MOTHERDUCK_TOKEN not set")
            sys.exit(2)
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    else:
        local_path = ROOT / "thyroid_master.duckdb"
        if not local_path.exists():
            local_path = ROOT / "thyroid_master_local.duckdb"
        return duckdb.connect(str(local_path))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--md", action="store_true", help="Target MotherDuck (requires MOTHERDUCK_TOKEN)")
    ap.add_argument("--local", action="store_true", help="Target local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Print SQL but do not execute")
    args = ap.parse_args()

    if not args.md and not args.local and not args.dry_run:
        args.local = True

    print("\n" + "=" * 72)
    print("  101_review_ops.py  —  Unified Review Queue Governance")
    print(f"  Target: {'MotherDuck' if args.md else 'Local DuckDB'}")
    print(f"  Mode:   {'DRY-RUN' if args.dry_run else 'LIVE'}")
    print("=" * 72)

    t0 = time.time()

    if args.dry_run:
        con = None
        build_unified_queue(con, dry_run=True)
        build_progress_table(con, dry_run=True)
        build_kpi_table(con, dry_run=True)
        export_artifacts(con, dry_run=True)
    else:
        con = connect(args)
        queue_cnt = build_unified_queue(con, dry_run=False)
        if queue_cnt < 0:
            print("\n  FATAL: unified queue build failed.")
            con.close()
            sys.exit(1)
        build_progress_table(con, dry_run=False)
        build_kpi_table(con, dry_run=False)
        export_dir = export_artifacts(con, dry_run=False)
        con.close()
        print(f"\n  Exports: {export_dir.relative_to(ROOT)}")

    elapsed = time.time() - t0
    print(f"\n  Completed in {elapsed:.1f}s\n")


if __name__ == "__main__":
    main()
