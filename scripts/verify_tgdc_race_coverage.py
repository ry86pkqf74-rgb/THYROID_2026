#!/usr/bin/env python3
"""
TGDC race coverage verification vs pub_workspace.emr_demographics_v1.

Default engine is BigQuery (thyroid-canonical-pub-2026). Use --engine md for
MotherDuck (legacy). Cohort: thyroglossal in path_synoptics text fields; when
using BigQuery, synoptic_tumor_long is not joined (table not migrated) — n=214
matches the path-only arm of the MotherDuck cohort for this definition.

Exit 1 if overall race coverage < gate (default 99.5%%).
"""

from __future__ import annotations

import argparse
import csv
import io
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from google.auth.exceptions import DefaultCredentialsError  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

BQ_PROJECT_DEFAULT = "thyroid-canonical-pub-2026"


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


def _bq_verify_sql(project_id: str) -> str:
    return f"""
    WITH cohort AS (
      SELECT DISTINCT CAST(research_id AS STRING) AS research_id
      FROM `{project_id}.pub_canonical.path_synoptics`
      WHERE LOWER(COALESCE(path_diagnosis_summary, '')) LIKE '%thyroglossal%'
         OR LOWER(COALESCE(clinical_information_pre_op_diagnosis, '')) LIKE '%thyroglossal%'
    ),
    joined AS (
      SELECT
        c.research_id,
        e.race,
        CASE
          WHEN e.race IS NOT NULL AND TRIM(CAST(e.race AS STRING)) != '' THEN 1
          ELSE 0
        END AS has_race
      FROM cohort c
      LEFT JOIN `{project_id}.pub_workspace.emr_demographics_v1` e
        ON c.research_id = e.research_id
    )
    SELECT research_id, race, has_race FROM joined
    """


def run_bigquery(project_id: str, coverage_gate: float) -> int:
    q = _bq_verify_sql(project_id)
    rows: list[dict[str, object]] = []
    try:
        client = bigquery.Client(project=project_id)
        rows = [dict(r.items()) for r in client.query(q).result()]
    except DefaultCredentialsError:
        proc = subprocess.run(
            [
                "bq",
                "query",
                "--use_legacy_sql=false",
                f"--project_id={project_id}",
                "--format=csv",
                "--max_rows",
                "100000",
                q,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        buf = io.StringIO(proc.stdout)
        rdr = csv.DictReader(buf)
        for row in rdr:
            rid = row.get("research_id", "")
            race = row.get("race")
            has_race = int(row.get("has_race", 0) or 0)
            rows.append({"research_id": rid, "race": race, "has_race": has_race})

    n_cohort = len(rows)
    covered = sum(1 for r in rows if r["has_race"])
    cov_pct = 100.0 * covered / n_cohort if n_cohort else 0.0
    print("Engine: BigQuery")
    print("TGDC cohort n (path_synoptics thyroglossal text; no STL union in BQ):", n_cohort)
    print(f"Race non-null: {covered} ({cov_pct:.2f}%)")

    buckets: dict[str, int] = {}
    for r in rows:
        if not r["has_race"]:
            b = "Unknown"
        else:
            b = race_bucket(r["race"])
        buckets[b] = buckets.get(b, 0) + 1

    print("Race bucket counts (cohort):")
    for k in sorted(buckets.keys()):
        pct = 100.0 * buckets[k] / n_cohort if n_cohort else 0.0
        print(f"  {k}: {buckets[k]} ({pct:.1f}%)")

    if cov_pct + 1e-9 < coverage_gate:
        print(
            f"FAIL: coverage {cov_pct:.2f}% < gate {coverage_gate}%",
            file=sys.stderr,
        )
        return 1
    print(f"PASS: coverage gate {coverage_gate}%")
    return 0


def run_motherduck(md_database: str, coverage_gate: float) -> int:
    cfg = MotherDuckConfig(database=md_database)
    con = MotherDuckClient(cfg).connect_rw()
    con.execute(f'USE "{md_database}"')

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
    print("Engine: MotherDuck")
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

    if cov_pct + 1e-9 < coverage_gate:
        print(
            f"FAIL: coverage {cov_pct:.2f}% < gate {coverage_gate}%",
            file=sys.stderr,
        )
        return 1
    print(f"PASS: coverage gate {coverage_gate}%")
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--engine",
        choices=("bq", "md"),
        default="bq",
        help="Data warehouse to verify against (default: BigQuery).",
    )
    p.add_argument("--bq-project", default=BQ_PROJECT_DEFAULT)
    p.add_argument("--md-database", default="thyroid_canonical_publication_v1_0")
    p.add_argument(
        "--coverage-gate",
        type=float,
        default=99.5,
        help="Minimum percent of cohort with non-empty race (default 99.5).",
    )
    args = p.parse_args()

    if args.engine == "bq":
        return run_bigquery(args.bq_project, args.coverage_gate)
    return run_motherduck(args.md_database, args.coverage_gate)


if __name__ == "__main__":
    raise SystemExit(main())
