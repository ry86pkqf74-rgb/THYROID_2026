#!/usr/bin/env python3
"""123_presentation_views.py — Create analyst and manuscript presentation-layer views.

Creates 6 views in the main schema for analysts and manuscript writers:
  1. v_patient_entity_summary — per-patient entity count pivot by domain family
  2. v_domain_completeness_matrix — domain x entity_type coverage
  3. v_manuscript_cohort_v2_enriched — manuscript cohort joined with v2 entity pivots
  4. v_entity_type_normalized — wrapper applying normalization to canonical facts
  5. v_tg_longitudinal_clean — deduplicated Tg trajectory surface
  6. v_release_audit_trail — full lineage from extraction run to release

Usage:
  .venv/bin/python scripts/123_presentation_views.py
  .venv/bin/python scripts/123_presentation_views.py --md
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_or_file  # noqa: E402

DB_PATH = ROOT / "thyroid_master.duckdb"

VIEWS = {
    "v_patient_entity_summary": """
CREATE OR REPLACE VIEW main.v_patient_entity_summary AS
SELECT
    f.research_id,
    COUNT(*) AS total_entities,
    COUNT(DISTINCT f.entity_type) AS unique_entity_types,
    COUNT(DISTINCT f.fact_domain) AS domains_covered,
    SUM(CASE WHEN f.linkage_anchor_family = 'pathology' THEN 1 ELSE 0 END) AS pathology_count,
    SUM(CASE WHEN f.linkage_anchor_family = 'operative' THEN 1 ELSE 0 END) AS operative_count,
    SUM(CASE WHEN f.linkage_anchor_family = 'followup' THEN 1 ELSE 0 END) AS followup_count,
    SUM(CASE WHEN f.linkage_anchor_family = 'imaging' THEN 1 ELSE 0 END) AS imaging_count,
    SUM(CASE WHEN f.linkage_anchor_family = 'demographics' THEN 1 ELSE 0 END) AS demographics_count,
    SUM(CASE WHEN f.linkage_anchor_family = 'rai' THEN 1 ELSE 0 END) AS rai_count,
    SUM(CASE WHEN f.linkage_anchor_family = 'molecular' THEN 1 ELSE 0 END) AS molecular_count
FROM canonical_extracted_fact_long_v2 f
GROUP BY f.research_id
""",

    "v_domain_completeness_matrix": """
CREATE OR REPLACE VIEW main.v_domain_completeness_matrix AS
SELECT
    f.fact_domain,
    f.linkage_anchor_family,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT f.research_id) AS unique_patients,
    COUNT(DISTINCT f.entity_type) AS unique_entity_types,
    SUM(CASE WHEN f.entity_date IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_date,
    ROUND(100.0 * SUM(CASE WHEN f.entity_date IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS date_coverage_pct,
    SUM(CASE WHEN f.inferred_surgery_episode_id IS NOT NULL THEN 1 ELSE 0 END) AS linked_rows,
    ROUND(100.0 * SUM(CASE WHEN f.inferred_surgery_episode_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS linkage_pct
FROM canonical_extracted_fact_long_v2 f
GROUP BY f.fact_domain, f.linkage_anchor_family
ORDER BY unique_patients DESC
""",

    "v_entity_type_normalized": """
CREATE OR REPLACE VIEW main.v_entity_type_normalized AS
SELECT
    f.*,
    COALESCE(f.entity_type_raw, f.entity_type) AS original_entity_type
FROM canonical_extracted_fact_long_v2 f
WHERE f.entity_type IS NOT NULL
""",

    "v_tg_longitudinal_clean": """
CREATE OR REPLACE VIEW main.v_tg_longitudinal_clean AS
SELECT DISTINCT
    l.research_id,
    l.lab_date,
    l.analyte_group,
    l.lab_name_standardized,
    l.value_numeric,
    l.unit_standardized,
    l.source_table,
    l.provenance_note
FROM longitudinal_lab_canonical_v1 l
WHERE l.analyte_group IN ('tg', 'tg_ab', 'tsh')
ORDER BY l.research_id, l.lab_date
""",

    "v_release_audit_trail": """
CREATE OR REPLACE VIEW main.v_release_audit_trail AS
SELECT
    r.run_id,
    r.extractor_build_version,
    r.started_at,
    r.completed_at,
    r.success,
    r.output_record_count,
    r.domains_requested,
    r.hostname,
    r.git_commit
FROM note_extraction_runs r
ORDER BY r.started_at DESC
""",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 70)
    print("  123 — presentation-layer views")
    print("=" * 70)

    con = connect_md_or_file(DB_PATH, md=args.md, fail_closed=args.md)

    for name, ddl in VIEWS.items():
        if args.dry_run:
            print(f"  [dry-run] {name}")
            continue
        try:
            con.execute(ddl)
            cnt = con.execute(f"SELECT COUNT(*) FROM main.{name}").fetchone()[0]
            print(f"  [OK] {name}: {cnt:,} rows")
        except Exception as e:
            print(f"  [WARN] {name}: {e}")

    con.close()
    print("=" * 70)
    print("  DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
