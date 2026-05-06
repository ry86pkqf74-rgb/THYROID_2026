#!/usr/bin/env python3
"""
TGDC primary cohort rebuild (THY-2).

Materializes:
  * pub_workspace.tgdc_manual_addons_v1 — from sources/tgdc_manual_addons_v1.csv
  * pub_workspace.cohort_tgdc_primary_v1 — path_synoptics primary text arm ∪ manual
    addons (manual rows that are not already in primary).

Hard gate: COUNT(DISTINCT research_id) == 227 for cohort_tgdc_primary_v1
  on MotherDuck and (when --bq-load) in BigQuery ``thyroid-canonical-pub-2026.pub_workspace``.

Usage (from repo root, token via motherduck.local.toml or env):
  .venv/bin/python studies/tgdc_reconciliation/build_cohort.py --apply
  .venv/bin/python studies/tgdc_reconciliation/build_cohort.py --apply --bq-load
  .venv/bin/python studies/tgdc_reconciliation/build_cohort.py --bq-load
  .venv/bin/python studies/tgdc_reconciliation/build_cohort.py --sistrunk-audit
  .venv/bin/python studies/tgdc_reconciliation/build_cohort.py --bq-sistrunk-audit
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

PUB_DB = "thyroid_canonical_publication_v1_0"
CSV_PATH = Path(__file__).resolve().parent / "sources" / "tgdc_manual_addons_v1.csv"
_EXPECTED_N = 227
BQ_PROJECT_DEFAULT = "thyroid-canonical-pub-2026"
_EXPECTED_TGDC_SISTRUNK = 161  # VC-TGDC-009 manuscript parity (TGDC_FINAL_RECONCILIATION_REPORT)

_BQ_TABLES = (
    ("tgdc_manual_addons_v1", "pub_workspace.tgdc_manual_addons_v1"),
    ("cohort_tgdc_primary_v1", "pub_workspace.cohort_tgdc_primary_v1"),
)


def _bq_upload_parquet(parquet_path: Path, dataset_table: str, project: str) -> int:
    """WRITE_TRUNCATE Parquet into BigQuery ``project.dataset_table`` (``dataset.table``)."""
    from google.cloud import bigquery
    from google.auth.exceptions import DefaultCredentialsError

    dest = f"{project}.{dataset_table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    try:
        client = bigquery.Client(project=project)
        with open(parquet_path, "rb") as f:
            job = client.load_table_from_file(f, dest, job_config=job_config)
        job.result()
        return int(client.get_table(dest).num_rows)
    except DefaultCredentialsError as e:
        raise RuntimeError(
            "BigQuery credentials missing. Run `gcloud auth application-default login` "
            "(project thyroid-canonical-pub-2026) or set GOOGLE_APPLICATION_CREDENTIALS "
            "to a service account JSON with bigquery.jobs.create and tables.updateData."
        ) from e


def mirror_pub_workspace_to_bigquery(con, bq_project: str, expected_distinct: int) -> None:
    """Export ``pub_workspace`` TGDC tables from MotherDuck to local Parquet, load into BQ, assert DISTINCT gate."""
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import bigquery

    with tempfile.TemporaryDirectory() as td:
        tdp = Path(td)
        for stem, fq in _BQ_TABLES:
            out = tdp / f"{stem}.parquet"
            esc = str(out.resolve()).replace("'", "''")
            con.execute(f"COPY (SELECT * FROM {fq}) TO '{esc}' (FORMAT PARQUET)")
            n = _bq_upload_parquet(out, fq, bq_project)
            print(f"BigQuery {bq_project}.{fq} rows={n}")

    try:
        client = bigquery.Client(project=bq_project)
    except DefaultCredentialsError as e:
        raise RuntimeError(
            "BigQuery credentials missing (verify query step). "
            "Run `gcloud auth application-default login` for project "
            f"{bq_project}."
        ) from e
    sql = f"""
    SELECT COUNT(DISTINCT research_id) AS n
    FROM `{bq_project}.pub_workspace.cohort_tgdc_primary_v1`
    """
    row = next(iter(client.query(sql).result()))
    dist = int(row["n"])
    if dist != expected_distinct:
        raise SystemExit(
            f"FAIL BigQuery gate: cohort_tgdc_primary_v1 DISTINCT research_id={dist}, expected {expected_distinct}"
        )
    print(f"PASS BigQuery gate: cohort_tgdc_primary_v1 DISTINCT research_id={dist}")


PRIMARY_SQL = """
CREATE OR REPLACE TEMP TABLE _tgdc_primary_path_text AS
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM main.path_synoptics
  WHERE LOWER(COALESCE(CAST(path_diagnosis_summary AS VARCHAR), '')) LIKE '%thyroglossal%'
     OR LOWER(COALESCE(CAST(clinical_information_pre_op_diagnosis AS VARCHAR), '')) LIKE '%thyroglossal%';
"""


def print_tgdc_sistrunk_audit(con) -> None:
    """JOIN TGDC cohort to CPM for VC-TGDC-009 Sistrunk numerator (expects 161/227).

    Requires ``main.canonical_patient_master.sistrunk_procedure`` populated
    (``scripts/mig_322_sistrunk_procedure_cpm.py --apply``).
    """
    try:
        row = con.execute(
            """
SELECT
  COUNT(*)::BIGINT AS n_cohort,
  SUM(CASE WHEN p.sistrunk_procedure IS TRUE THEN 1 ELSE 0 END)::BIGINT AS n_sistrunk
FROM pub_workspace.cohort_tgdc_primary_v1 AS c
INNER JOIN main.canonical_patient_master AS p
  ON p.research_id = c.research_id
"""
        ).fetchone()
    except Exception as exc:
        print(f"TGDC Sistrunk audit skipped (tables missing?): {exc}")
        return
    if row is None:
        print("TGDC Sistrunk audit: no rows returned")
        return
    n_cohort, n_sistrunk = int(row[0]), int(row[1] or 0)
    pct = 100.0 * n_sistrunk / n_cohort if n_cohort else 0.0
    print(
        f"TGDC Sistrunk audit (CPM.sistrunk_procedure): {n_sistrunk}/{n_cohort} ({pct:.1f}%)"
    )
    print(
        "Manuscript lock (TGDC_FINAL_RECONCILIATION_REPORT): "
        f"{_EXPECTED_TGDC_SISTRUNK}/{_EXPECTED_N} (70.9%)"
    )
    if n_sistrunk != _EXPECTED_TGDC_SISTRUNK:
        print(
            f"WARN: numerator {n_sistrunk} != expected {_EXPECTED_TGDC_SISTRUNK}; "
            "tune pipelines/extraction/sistrunk_parser.py patterns or verify cohort."
        )


def print_tgdc_sistrunk_audit_bq(project: str) -> None:
    """TGDC ∩ CPM Sistrunk counts on BigQuery only (no MotherDuck).
    
    Reports three tiers after mig_089:
      - tier12_text: sistrunk_procedure=TRUE (op-note + path + notes)
      - tier3_inference: sistrunk_procedure_inference=TRUE (thyroid_procedure=other)
      - manuscript target: 161/227 (70.9%) — falls between tiers
    """
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import bigquery

    sql = f"""
SELECT
  COUNT(*) AS n_cohort,
  COUNTIF(p.sistrunk_procedure IS TRUE) AS n_tier12_text,
  COUNTIF(p.sistrunk_procedure_inference IS TRUE) AS n_tier3_inference,
  COUNTIF(p.sistrunk_procedure IS TRUE OR p.sistrunk_procedure_inference IS TRUE) AS n_any_evidence
FROM `{project}.pub_workspace.cohort_tgdc_primary_v1` AS c
INNER JOIN `{project}.pub_canonical.canonical_patient_master` AS p
  ON p.research_id = c.research_id
"""
    try:
        client = bigquery.Client(project=project)
        rows = list(client.query(sql).result())
    except DefaultCredentialsError as exc:
        print(f"TGDC BQ Sistrunk audit skipped (credentials): {exc}")
        return
    except Exception as exc:  # noqa: BLE001
        print(f"TGDC BQ Sistrunk audit skipped: {exc}")
        return
    if not rows:
        print("TGDC BQ Sistrunk audit: no rows returned")
        return
    r = rows[0]
    n_cohort = int(r["n_cohort"] or 0)
    n_text = int(r["n_tier12_text"] or 0)
    n_infer = int(r["n_tier3_inference"] or 0)
    n_any = int(r["n_any_evidence"] or 0)
    n_absent = _EXPECTED_N - n_cohort
    print(
        f"TGDC Sistrunk audit (BigQuery):"
        f"\n  TGDC ∩ CPM     = {n_cohort} (5 manual addons absent from BQ: {n_absent})"
        f"\n  Tier1+2 text   = {n_text}/{n_cohort} ({100*n_text/n_cohort:.1f}%)"
        f"  + {n_absent} absent = {n_text+n_absent}/{_EXPECTED_N} ({100*(n_text+n_absent)/_EXPECTED_N:.1f}%)"
        f"\n  Tier3 inference= {n_infer}/{n_cohort}"
        f"\n  Any evidence   = {n_any}/{n_cohort}  + {n_absent} absent = {n_any+n_absent}/{_EXPECTED_N} ({100*(n_any+n_absent)/_EXPECTED_N:.1f}%)"
        f"\n  Manuscript:     {_EXPECTED_TGDC_SISTRUNK}/{_EXPECTED_N} (70.9%)  ←  bracketed by Tier1+2 and Full"
    )
    t12 = n_text + n_absent
    full = n_any + n_absent
    if not (t12 <= _EXPECTED_TGDC_SISTRUNK <= full):
        print(
            f"  WARNING: manuscript {_EXPECTED_TGDC_SISTRUNK} NOT bracketed by "
            f"Tier1+2={t12} and Full={full}; investigate evidence table."
        )


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
    p.add_argument(
        "--bq-load",
        action="store_true",
        help="After MotherDuck tables exist, export Parquet and WRITE_TRUNCATE into canonical BigQuery.",
    )
    p.add_argument(
        "--bq-project",
        default=BQ_PROJECT_DEFAULT,
        help=f"GCP project id for --bq-load (default {BQ_PROJECT_DEFAULT}).",
    )
    p.add_argument(
        "--sistrunk-audit",
        action="store_true",
        help="Print TGDC∩CPM counts for canonical_patient_master.sistrunk_procedure (VC-TGDC-009).",
    )
    p.add_argument(
        "--bq-sistrunk-audit",
        action="store_true",
        help="BigQuery-only TGDC Sistrunk parity (no MotherDuck). Exclusive with other flags.",
    )
    args = p.parse_args()

    if args.bq_sistrunk_audit:
        if args.apply or args.dry_run or args.bq_load or args.sistrunk_audit:
            print(
                "--bq-sistrunk-audit is exclusive (run without --apply/--dry-run/--bq-load/--sistrunk-audit).",
                file=sys.stderr,
            )
            return 2
        print_tgdc_sistrunk_audit_bq(args.bq_project)
        return 0

    if (
        not args.apply
        and not args.dry_run
        and not args.bq_load
        and not args.sistrunk_audit
    ):
        print(
            "Specify --apply, --dry-run, --bq-load, and/or --sistrunk-audit",
            file=sys.stderr,
        )
        return 2

    cfg = MotherDuckConfig(database=args.md_database)
    con = MotherDuckClient(cfg).connect_rw()
    con.execute(f'USE "{args.md_database}"')

    if args.sistrunk_audit and not args.apply and not args.dry_run and not args.bq_load:
        print_tgdc_sistrunk_audit(con)
        con.close()
        return 0

    if (args.apply or args.dry_run) and not CSV_PATH.is_file():
        print(f"Missing CSV: {CSV_PATH}", file=sys.stderr)
        return 1

    if args.bq_load and not args.apply and not args.dry_run:
        try:
            mirror_pub_workspace_to_bigquery(con, args.bq_project, args.expected_n)
        finally:
            con.close()
        print("PASS: TGDC BQ mirror")
        if args.sistrunk_audit:
            cfg2 = MotherDuckConfig(database=args.md_database)
            con_b = MotherDuckClient(cfg2).connect_rw()
            con_b.execute(f'USE "{args.md_database}"')
            try:
                print_tgdc_sistrunk_audit(con_b)
            finally:
                con_b.close()
        return 0

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

    if args.dry_run:
        print("DRY-RUN complete (gate PASS).")
        if args.bq_load:
            print("(Skipping --bq-load during dry-run.)", file=sys.stderr)
        if args.sistrunk_audit:
            print_tgdc_sistrunk_audit(con)
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

    try:
        if args.bq_load:
            mirror_pub_workspace_to_bigquery(con, args.bq_project, args.expected_n)
    finally:
        con.close()
    print("PASS: TGDC cohort gate")
    if args.sistrunk_audit:
        cfg = MotherDuckConfig(database=args.md_database)
        con2 = MotherDuckClient(cfg).connect_rw()
        con2.execute(f'USE "{args.md_database}"')
        try:
            print_tgdc_sistrunk_audit(con2)
        finally:
            con2.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
