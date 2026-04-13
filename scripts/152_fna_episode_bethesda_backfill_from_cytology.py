#!/usr/bin/env python3
"""
Backfill fna_episode_master_v2.bethesda_category from fna_cytology.category_num
when the episode column is NULL and cytology has a deduplicated category.

Deterministic join: (research_id, fna_episode_id) == (research_id, fna_index).
Deduplication matches v_fna_episode_bethesda_resolved_v1 (confidence, ingested_at).

Run:
  .venv/bin/python scripts/152_fna_episode_bethesda_backfill_from_cytology.py --md
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402

BACKFILL_SQL = """
WITH cy AS (
    SELECT
        research_id,
        fna_index,
        category_num,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, fna_index
            ORDER BY COALESCE(confidence, 0) DESC NULLS LAST,
                     ingested_at_utc DESC NULLS LAST
        ) AS rn
    FROM fna_cytology
    WHERE category_num IS NOT NULL
)
UPDATE fna_episode_master_v2 AS e
SET bethesda_category = cy.category_num
FROM cy
WHERE cy.rn = 1
  AND CAST(e.research_id AS BIGINT) = CAST(cy.research_id AS BIGINT)
  AND CAST(e.fna_episode_id AS BIGINT) = CAST(cy.fna_index AS BIGINT)
  AND e.bethesda_category IS NULL
"""


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--md", action="store_true", help="MotherDuck (fail-closed)")
    p.add_argument("--dry-run", action="store_true", help="Show counts only, no UPDATE")
    args = p.parse_args()
    if not args.md:
        args.md = True

    con = connect_md_fail_closed(ROOT / "thyroid_master.duckdb")

    before_r = con.execute(
        "SELECT COUNT(*) FROM fna_episode_master_v2 "
        "WHERE bethesda_category IS NULL"
    ).fetchone()
    before = int(before_r[0]) if before_r else 0

    preview_r = con.execute(
        """
        WITH cy AS (
            SELECT
                research_id,
                fna_index,
                category_num,
                ROW_NUMBER() OVER (
                    PARTITION BY research_id, fna_index
                    ORDER BY COALESCE(confidence, 0) DESC NULLS LAST,
                             ingested_at_utc DESC NULLS LAST
                ) AS rn
            FROM fna_cytology
            WHERE category_num IS NOT NULL
        )
        SELECT COUNT(*) FROM fna_episode_master_v2 e
        INNER JOIN cy ON cy.rn = 1
          AND CAST(e.research_id AS BIGINT) = CAST(cy.research_id AS BIGINT)
          AND CAST(e.fna_episode_id AS BIGINT) = CAST(cy.fna_index AS BIGINT)
        WHERE e.bethesda_category IS NULL
        """
    ).fetchone()
    preview = int(preview_r[0]) if preview_r else 0

    print(f"[152] fna_episode_master_v2 bethesda_category IS NULL before: {before}")
    print(f"[152] rows matchable from fna_cytology (preview): {preview}")

    if args.dry_run:
        con.close()
        return 0

    con.execute(BACKFILL_SQL)
    after_r = con.execute(
        "SELECT COUNT(*) FROM fna_episode_master_v2 "
        "WHERE bethesda_category IS NULL"
    ).fetchone()
    after = int(after_r[0]) if after_r else 0
    print(f"[152] bethesda_category IS NULL after: {after}")
    print(f"[152] rows updated (approx): {before - after}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
