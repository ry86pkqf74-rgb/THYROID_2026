#!/usr/bin/env python3
"""
TGDC primary cohort rebuild (THY-2).

Materializes:
  * pub_workspace.tgdc_manual_addons_v1 — from sources/tgdc_manual_addons_v1.csv
  * pub_workspace.cohort_tgdc_primary_v1 — path_synoptics primary text arm ∪ manual
    addons (manual rows that are not already in primary).

Hard gate: COUNT(DISTINCT research_id) == 227 for cohort_tgdc_primary_v1.

Usage (from repo root, token via motherduck.local.toml or env):
  .venv/bin/python studies/tgdc_reconciliation/build_cohort.py --apply
  .venv/bin/python studies/tgdc_reconciliation/build_cohort.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

PUB_DB = "thyroid_canonical_publication_v1_0"
CSV_PATH = Path(__file__).resolve().parent / "sources" / "tgdc_manual_addons_v1.csv"
_EXPECTED_N = 227


PRIMARY_SQL = """
CREATE OR REPLACE TEMP TABLE _tgdc_primary_path_text AS
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM main.path_synoptics
  WHERE LOWER(COALESCE(CAST(path_diagnosis_summary AS VARCHAR), '')) LIKE '%thyroglossal%'
     OR LOWER(COALESCE(CAST(clinical_information_pre_op_diagnosis AS VARCHAR), '')) LIKE '%thyroglossal%';
"""


def main() -> int:
    p = argparse.ArgumentParser(description="Rebuild TGDC cohort tables on MotherDuck.")
    p.add_argument(
        "--md-database",
        default=PUB_DB,
        help=f"MotherDuck database name (default {PUB_DB}).",
    )
    p.add_argument(
        "--expected-n",
        type=int,
        default=_EXPECTED_N,
        help=f"Distinct research_id gate (default {_EXPECTED_N}).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print counts only (still connects; does not write pub_workspace).",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Create/replace pub_workspace.tgdc_manual_addons_v1 and cohort_tgdc_primary_v1.",
    )
    args = p.parse_args()

    if not args.apply and not args.dry_run:
        print("Specify --apply and/or --dry-run", file=sys.stderr)
        return 2

    if not CSV_PATH.is_file():
        print(f"Missing CSV: {CSV_PATH}", file=sys.stderr)
        return 1

    cfg = MotherDuckConfig(database=args.md_database)
    con = MotherDuckClient(cfg).connect_rw()
    con.execute(f'USE "{args.md_database}"')
    con.execute(PRIMARY_SQL)
    n_primary = con.execute("SELECT COUNT(*) FROM _tgdc_primary_path_text").fetchone()[0]

    csv_sql = str(CSV_PATH).replace("'", "''")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _tgdc_manual_csv AS
        SELECT
          TRIM(CAST(research_id AS VARCHAR)) AS research_id,
          TRIM(CAST(evidence_source AS VARCHAR)) AS evidence_source,
          TRIM(CAST(evidence_summary AS VARCHAR)) AS evidence_summary,
          CAST(STRPTIME(concat(TRIM(CAST(added_at AS VARCHAR)), ' 00:00:00'), '%Y-%m-%d %H:%M:%S') AS TIMESTAMP) AS added_at
        FROM read_csv_auto('{csv_sql}', header := true, all_varchar := true);
        """
    )
    n_csv = con.execute("SELECT COUNT(*) FROM _tgdc_manual_csv").fetchone()[0]
    n_manual_only = con.execute(
        """
        SELECT COUNT(*) FROM _tgdc_manual_csv m
        WHERE m.research_id NOT IN (SELECT research_id FROM _tgdc_primary_path_text);
        """
    ).fetchone()[0]

    union_n = con.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT research_id FROM _tgdc_primary_path_text
          UNION
          SELECT research_id FROM _tgdc_manual_csv
        ) u;
        """
    ).fetchone()[0]
    dist_n = con.execute(
        """
        SELECT COUNT(DISTINCT research_id) FROM (
          SELECT research_id FROM _tgdc_primary_path_text
          UNION
          SELECT research_id FROM _tgdc_manual_csv
        ) u;
        """
    ).fetchone()[0]

    print(f"path-text primary distinct: {n_primary}")
    print(f"manual CSV rows: {n_csv}")
    print(f"manual rows not in primary: {n_manual_only}")
    print(f"union rows (with dupes if overlap): {union_n}")
    print(f"COUNT DISTINCT research_id (gate): {dist_n}")

    if dist_n != args.expected_n:
        print(
            f"FAIL: expected {args.expected_n} distinct research_id, got {dist_n}",
            file=sys.stderr,
        )
        con.close()
        return 1

    if args.dry_run and not args.apply:
        print("DRY-RUN complete (gate PASS).")
        con.close()
        return 0

    if args.apply:
        con.execute("CREATE SCHEMA IF NOT EXISTS pub_workspace;")
        con.execute(
            """
            CREATE OR REPLACE TABLE pub_workspace.tgdc_manual_addons_v1 AS
            SELECT
              research_id,
              evidence_source,
              evidence_summary,
              added_at,
              'studies/tgdc_reconciliation/sources/tgdc_manual_addons_v1.csv' AS loaded_from,
              CURRENT_TIMESTAMP AS loaded_at
            FROM _tgdc_manual_csv;
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE pub_workspace.cohort_tgdc_primary_v1 AS
            SELECT
              p.research_id,
              'primary_path_text_v1'::VARCHAR AS cohort_member_source,
              NULL::VARCHAR AS evidence_source,
              NULL::VARCHAR AS evidence_summary,
              NULL::TIMESTAMP AS addon_added_at
            FROM _tgdc_primary_path_text p
            UNION ALL
            SELECT
              m.research_id,
              'manual_addon_v1'::VARCHAR,
              m.evidence_source,
              m.evidence_summary,
              m.added_at
            FROM _tgdc_manual_csv m
            WHERE m.research_id NOT IN (SELECT research_id FROM _tgdc_primary_path_text);
            """
        )
        final_d = con.execute(
            """
            SELECT COUNT(DISTINCT research_id)
            FROM pub_workspace.cohort_tgdc_primary_v1;
            """
        ).fetchone()[0]
        final_r = con.execute(
            "SELECT COUNT(*) FROM pub_workspace.cohort_tgdc_primary_v1;"
        ).fetchone()[0]
        print(f"Applied cohort_tgdc_primary_v1 rows={final_r} distinct_id={final_d}")
        if final_d != args.expected_n:
            print(
                f"FAIL post-apply: distinct_id={final_d} expected {args.expected_n}",
                file=sys.stderr,
            )
            con.close()
            return 1

    con.close()
    print("PASS: TGDC cohort gate")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
