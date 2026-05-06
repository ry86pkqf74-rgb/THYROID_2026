#!/usr/bin/env python3
"""
TGDC race coverage verification vs pub_workspace.emr_demographics_v1 (MotherDuck).

Joins TGDC manuscript cohort (thyroglossal evidence in path_synoptics UNION synoptic_tumor_long)
to emr_demographics_v1, prints coverage % and race bucket distributions.
Exit 1 if overall race coverage < 99.5%%.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402


def race_bucket(raw: object) -> str:
    if raw is None:
        return "Unknown"
    s = str(raw).strip().lower()
    if not s:
        return "Unknown"
    if any(x in s for x in ("white", "caucasian", "european")):
        return "White"
    if any(
        x in s
        for x in (
            "black",
            "african american",
            "african-american",
            "african",
        )
    ):
        return "Black"
    if any(
        x in s
        for x in (
            "asian",
            "chinese",
            "korean",
            "vietnamese",
            "filipino",
            "asian indian",
            "japanese",
        )
    ):
        return "Asian"
    if any(x in s for x in ("hispanic", "latino", "latina")):
        return "Hispanic"
    if "pacific" in s or "hawaiian" in s or "nhpi" in s:
        return "NHPI"
    if any(x in s for x in ("american indian", "alaska native", "native american", "ai/an")):
        return "AI/AN"
    return "Other/Unknown"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--md-database", default="thyroid_canonical_publication_v1_0")
    p.add_argument(
        "--coverage-gate",
        type=float,
        default=99.5,
        help="Minimum percent of cohort with non-empty race (default 99.5).",
    )
    args = p.parse_args()

    cfg = MotherDuckConfig(database=args.md_database)
    con = MotherDuckClient(cfg).connect_rw()
    con.execute(f'USE "{args.md_database}"')

    cohort_sql = """
    CREATE OR REPLACE TEMP TABLE _tgdc_cohort AS
    WITH u AS (
      SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
      FROM main.path_synoptics
      WHERE lower(coalesce(path_diagnosis_summary, '')) LIKE '%thyroglossal%'
         OR lower(coalesce(clinical_information_pre_op_diagnosis, '')) LIKE '%thyroglossal%'
      UNION
      SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
      FROM readonly_share.synoptic_tumor_long_v1
      WHERE lower(coalesce(histologic_type, '')) LIKE '%thyroglossal%'
    )
    SELECT research_id FROM u;
    """

    con.execute(cohort_sql)
    n_cohort = con.execute("SELECT COUNT(*) FROM _tgdc_cohort").fetchone()[0]

    res = con.execute(
        """
        SELECT
          c.research_id,
          e.race,
          CASE
            WHEN e.race IS NULL OR trim(CAST(e.race AS VARCHAR)) = '' THEN 0
            ELSE 1
          END AS has_race
        FROM _tgdc_cohort c
        LEFT JOIN pub_workspace.emr_demographics_v1 e
          ON c.research_id = CAST(e.research_id AS VARCHAR)
        """
    ).fetchall()

    if len(res) != n_cohort:
        print("WARN: cohort join row count mismatch", len(res), "vs", n_cohort)

    covered = sum(1 for _, __, has in res if has)
    cov_pct = 100.0 * covered / n_cohort if n_cohort else 0.0
    print("TGDC cohort n (union path + STL):", n_cohort)
    print(f"Race non-null: {covered} ({cov_pct:.2f}%)")

    buckets: dict[str, int] = {}
    for _, race, has in res:
        if not has:
            b = "Unknown"
        else:
            b = race_bucket(race)
        buckets[b] = buckets.get(b, 0) + 1

    print("Race bucket counts (cohort):")
    for k in sorted(buckets.keys()):
        pct = 100.0 * buckets[k] / n_cohort if n_cohort else 0.0
        print(f"  {k}: {buckets[k]} ({pct:.1f}%)")

    con.close()

    if cov_pct + 1e-9 < args.coverage_gate:
        print(
            f"FAIL: coverage {cov_pct:.2f}% < gate {args.coverage_gate}%",
            file=sys.stderr,
        )
        return 1
    print(f"PASS: coverage gate {args.coverage_gate}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
