#!/usr/bin/env python3
"""125_master_verified_views.py — Analyst-facing verified presentation layer.

Creates three views in the main schema that serve as the canonical analyst
surface for release-signed data.  Every row exposes the six required provenance
fields: research_id, source domain, source object id, extraction_run_id,
reviewer_status, and release_tag.

Reviewer columns are **not** per-fact human validation unless policy says so:
``reviewer_status`` is joined from ``qa.manual_review_queue`` at
**(research_id, domain)** grain only. See ``review_grain``, ``review_status_source``,
and ``review_join_key`` on ``master_fact_long_verified_v1``.

Views created:
  main.master_fact_long_verified_v1
      One row per extracted entity fact.  Joins canonical facts with reviewer
      status from qa.manual_review_queue and the latest release tag from
      qa.release_manifest.

  main.master_patient_rollup_verified_v1
      Per-patient summary over master_fact_long_verified_v1.  Counts by
      linkage family, review coverage percentage, and release tag.

  main.master_source_lineage_v1
      Full provenance chain: extraction run → fact → reviewer decision →
      release tag.  Source object identity is preserved as note_row_id.

Usage:
  .venv/bin/python scripts/125_master_verified_views.py
  .venv/bin/python scripts/125_master_verified_views.py --md
  .venv/bin/python scripts/125_master_verified_views.py --md --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_or_file  # noqa: E402

DB_PATH = ROOT / "thyroid_master.duckdb"

# ---------------------------------------------------------------------------
# View DDL
# ---------------------------------------------------------------------------

MASTER_FACT_LONG_DDL = """\
-- Reviewer fields: propagated from qa.manual_review_queue at (research_id, domain) grain — not per-fact adjudication unless queue stores finer grain.
CREATE OR REPLACE VIEW main.master_fact_long_verified_v1 AS
WITH latest_release AS (
    SELECT release_tag
    FROM   qa.release_manifest
    ORDER  BY TRY_CAST(release_tag AS BIGINT) DESC NULLS LAST, created_at DESC
    LIMIT  1
),
review_lookup AS (
    SELECT
        research_id,
        reviewer_domain,
        reviewer_status,
        reviewer_verified_by,
        reviewer_decision_at,
        reviewer_notes
    FROM (
        SELECT
            research_id,
            domain                                AS reviewer_domain,
            verification_status                   AS reviewer_status,
            reviewer                              AS reviewer_verified_by,
            reviewed_at                           AS reviewer_decision_at,
            trim(both ' ' FROM concat_ws(
                ' | ',
                NULLIF(trim(COALESCE(reviewer_comment, '')), ''),
                NULLIF(trim(COALESCE(reason_code, '')), ''),
                NULLIF(trim(COALESCE(promotion_approved, '')), '')
            ))                                    AS reviewer_notes,
            ROW_NUMBER() OVER (
                PARTITION BY research_id, domain
                ORDER BY reviewed_at DESC NULLS LAST, loaded_at DESC
            )                                     AS _rn
        FROM qa.manual_review_queue
    ) _mrq
    WHERE _rn = 1
),
fact_core AS (
    SELECT
        f.*,
        COALESCE(
            NULLIF(trim(CAST(f.extraction_run_id AS VARCHAR)), ''),
            (SELECT run_id FROM main.note_extraction_runs r1
             WHERE r1.success = true
               AND try_cast(r1.started_at AS TIMESTAMPTZ)
                   <= try_cast(f.extracted_at AS TIMESTAMPTZ)
             ORDER BY try_cast(r1.started_at AS TIMESTAMPTZ) DESC
             LIMIT 1),
            (SELECT run_id FROM main.note_extraction_runs r2
             WHERE r2.success = true
             ORDER BY try_cast(r2.started_at AS TIMESTAMPTZ) ASC
             LIMIT 1)
        ) AS _resolved_extraction_run_id
    FROM main.canonical_extracted_fact_long_v2 f
)
SELECT
    f.research_id,
    f.fact_id,
    -- source domain
    f.fact_domain                           AS source_domain,
    -- source object id
    f.note_row_id                           AS source_object_id,
    -- extraction run linkage (row id; else latest successful run <= extracted_at;
    -- else earliest successful run for pre-telemetry timestamps)
    f._resolved_extraction_run_id            AS extraction_run_id,
    r.extractor_build_version,
    CAST(NULL AS VARCHAR)                  AS llm_model,
    r.started_at                            AS extraction_started_at,
    -- entity fields
    f.entity_type,
    f.entity_value_norm,
    f.entity_value_raw,
    f.entity_date,
    f.present_or_negated,
    f.confidence,
    -- episode linkage
    f.linkage_anchor_family,
    f.inferred_surgery_episode_id,
    f.ep_source_table,
    f.ep_distance_days,
    f.linkage_confidence,
    -- provenance
    f.extraction_method,
    f.extracted_at,
    f.source_file_id,
    f.date_source_type,
    -- reviewer status
    rv.reviewer_status,
    rv.reviewer_verified_by,
    rv.reviewer_decision_at,
    rv.reviewer_notes,
    CAST('research_id_domain' AS VARCHAR)              AS review_grain,
    CAST('qa.manual_review_queue' AS VARCHAR)          AS review_status_source,
    concat(
        cast(f.research_id AS VARCHAR),
        '|',
        cast(f.fact_domain AS VARCHAR)
    )                                                  AS review_join_key,
    -- release tag (largest numeric tag in manifest; tie-break created_at)
    (SELECT release_tag FROM latest_release) AS release_tag
FROM  fact_core f
LEFT  JOIN main.note_extraction_runs r
      ON  r.run_id = f._resolved_extraction_run_id
LEFT  JOIN review_lookup rv
      ON  rv.research_id  = f.research_id
      AND rv.reviewer_domain = f.fact_domain
"""

MASTER_PATIENT_ROLLUP_DDL = """\
CREATE OR REPLACE VIEW main.master_patient_rollup_verified_v1 AS
SELECT
    f.research_id,
    COUNT(*)                                                    AS total_facts,
    COUNT(DISTINCT f.source_domain)                             AS domains_covered,
    COUNT(DISTINCT f.entity_type)                               AS unique_entity_types,
    SUM(CASE WHEN f.linkage_anchor_family = 'pathology'    THEN 1 ELSE 0 END) AS pathology_facts,
    SUM(CASE WHEN f.linkage_anchor_family = 'operative'    THEN 1 ELSE 0 END) AS operative_facts,
    SUM(CASE WHEN f.linkage_anchor_family = 'imaging'      THEN 1 ELSE 0 END) AS imaging_facts,
    SUM(CASE WHEN f.linkage_anchor_family = 'molecular'    THEN 1 ELSE 0 END) AS molecular_facts,
    SUM(CASE WHEN f.linkage_anchor_family = 'followup'     THEN 1 ELSE 0 END) AS followup_facts,
    SUM(CASE WHEN f.linkage_anchor_family = 'rai'          THEN 1 ELSE 0 END) AS rai_facts,
    SUM(CASE WHEN f.linkage_anchor_family = 'demographics' THEN 1 ELSE 0 END) AS demographics_facts,
    COUNT(f.inferred_surgery_episode_id)                        AS episode_linked_facts,
    ROUND(
        100.0 * COUNT(f.inferred_surgery_episode_id) / NULLIF(COUNT(*), 0),
        1
    )                                                           AS pct_episode_linked,
    COUNT(f.reviewer_status)                                    AS reviewed_facts,
    ROUND(
        100.0 * COUNT(f.reviewer_status) / NULLIF(COUNT(*), 0),
        1
    )                                                           AS pct_reviewed,
    f.release_tag
FROM  main.master_fact_long_verified_v1 f
GROUP BY f.research_id, f.release_tag
ORDER BY f.research_id
"""

MASTER_SOURCE_LINEAGE_DDL = """\
CREATE OR REPLACE VIEW main.master_source_lineage_v1 AS
SELECT
    f.research_id,
    -- source domain and object identity
    f.source_domain,
    f.source_object_id,
    f.entity_type,
    f.entity_date,
    -- extraction run provenance
    f.extraction_run_id,
    f.extractor_build_version,
    f.llm_model,
    f.extraction_started_at,
    f.extraction_method,
    f.extracted_at,
    -- episode linkage
    f.linkage_anchor_family,
    f.inferred_surgery_episode_id,
    f.ep_source_table,
    -- reviewer chain
    f.reviewer_status,
    f.reviewer_verified_by,
    f.reviewer_decision_at,
    f.reviewer_notes,
    f.review_grain,
    f.review_status_source,
    f.review_join_key,
    -- release provenance
    f.release_tag
FROM  main.master_fact_long_verified_v1 f
ORDER BY f.research_id, f.source_domain, f.extracted_at
"""

VIEWS: list[tuple[str, str]] = [
    ("master_fact_long_verified_v1", MASTER_FACT_LONG_DDL),
    ("master_patient_rollup_verified_v1", MASTER_PATIENT_ROLLUP_DDL),
    ("master_source_lineage_v1", MASTER_SOURCE_LINEAGE_DDL),
]

VIEW_DESCRIPTIONS = {
    "master_fact_long_verified_v1": (
        "One row per extracted entity fact; joins canonical_extracted_fact_long_v2 "
        "with reviewer status (MRQ grain: research_id+domain) and latest release tag."
    ),
    "master_patient_rollup_verified_v1": (
        "Per-patient summary: fact counts by linkage family, review coverage, release tag."
    ),
    "master_source_lineage_v1": (
        "Full provenance chain from extraction run to reviewer decision to release tag."
    ),
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument(
        "--md-sa",
        action="store_true",
        help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN.",
    )
    p.add_argument("--md-user-agent", default=None, help="MotherDuck custom_user_agent.")
    p.add_argument("--md-session-hint", default=None, help="SET motherduck_session_hint.")
    p.add_argument("--dry-run", action="store_true", help="Show DDL without executing.")
    p.add_argument("--db-path", default=str(DB_PATH), help="Local DuckDB path.")
    return p.parse_args()


def main() -> None:
    import os

    args = parse_args()

    print("=" * 70)
    print("  125 — master verified views (analyst presentation layer)")
    print("=" * 70)

    ua = (
        args.md_user_agent
        or os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT")
        or "THYROID_2026_master_verified_views/1.0"
    )
    con = connect_md_or_file(
        Path(args.db_path),
        md=args.md,
        fail_closed=args.md,
        prefer_service_account=args.md_sa,
        custom_user_agent=ua,
        motherduck_session_hint=args.md_session_hint,
    )

    failed = 0
    for name, ddl in VIEWS:
        desc = VIEW_DESCRIPTIONS.get(name, "")
        if args.dry_run:
            print(f"\n  [dry-run] {name}")
            print(f"            {desc}")
            print("  DDL preview (first 4 lines):")
            for line in ddl.strip().splitlines()[:4]:
                print(f"    {line}")
            print("    ...")
            continue

        try:
            con.execute(ddl)
            cnt = con.execute(f"SELECT COUNT(*) FROM main.{name}").fetchone()[0]
            print(f"  [OK] {name}: {cnt:,} rows — {desc}")
        except Exception as exc:
            print(f"  [WARN] {name}: {exc}")
            print("         This view may require prerequisite tables to exist first.")
            print("         Re-run after 103 (canonical facts) and 114 (qa schema) complete.")
            failed += 1

    con.close()

    print("=" * 70)
    if args.dry_run:
        print("  DONE (dry-run — no views created)")
    elif failed:
        print(f"  DONE with {failed} warning(s). Re-run after prerequisites are in place.")
    else:
        print("  DONE — all 3 master verified views created")
    print("=" * 70)

    if failed and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    main()
