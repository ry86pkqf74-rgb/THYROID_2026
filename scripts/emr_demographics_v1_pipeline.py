#!/usr/bin/env python3
"""
Build PHI-scrubbed emr_demographics_v1 parquet and load into BigQuery pub_workspace.

THY-1: optional restricted EMR CSV (research_id, race, ethnicity, sex, dob|dob_year)
produces row-level demographics with dob reduced to dob_year only. Without CSV,
bootstrap from canonical_patient_master + path_synoptics race fallback (explicit
source_table labels).

Default bootstrap and load targets are BigQuery (thyroid-canonical-pub-2026).
Use --bootstrap md only when BQ is unavailable. MotherDuck load is opt-in (--md-load).

Auth: Application Default Credentials for BigQuery; motherduck.local.toml for --bootstrap md.
"""

from __future__ import annotations

import argparse
import io
import json
import subprocess
import sys
from datetime import timezone
from pathlib import Path

import duckdb
import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from google.auth.exceptions import DefaultCredentialsError  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

BQ_PROJECT_DEFAULT = "thyroid-canonical-pub-2026"
BQ_TABLE_DEFAULT = "pub_workspace.emr_demographics_v1"
SCHEMA_JSON = _REPO / "schemas" / "emr_demographics_v1_bigquery.json"


def _norm_sex(val: object) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    sl = s.lower()
    if sl in ("m", "male"):
        return "Male"
    if sl in ("f", "female"):
        return "Female"
    return s


def _dob_to_year(val: object) -> int | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, int):
        y = val
        return y if 1900 <= y <= 2100 else None
    s = str(val).strip()
    if not s:
        return None
    if len(s) >= 4 and s[:4].isdigit():
        y = int(s[:4])
        if 1900 <= y <= 2100:
            return y
    return None


def read_emr_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    cols = {c.lower().strip(): c for c in df.columns}
    rid_c = cols.get("research_id")
    if not rid_c:
        raise ValueError("CSV must contain research_id column")
    out = pd.DataFrame()
    out["research_id"] = df[rid_c].astype("Int64").astype(str)
    if "race" in cols:
        out["race"] = df[cols["race"]].apply(
            lambda x: None if pd.isna(x) or str(x).strip() == "" else str(x).strip()
        )
    else:
        out["race"] = None
    if "ethnicity" in cols:
        out["ethnicity"] = df[cols["ethnicity"]].apply(
            lambda x: None if pd.isna(x) or str(x).strip() == "" else str(x).strip()
        )
    else:
        out["ethnicity"] = None
    if "sex" in cols:
        out["sex"] = df[cols["sex"]].map(_norm_sex)
    else:
        out["sex"] = None
    dob_y = None
    if "dob_year" in cols:
        dob_y = df[cols["dob_year"]].map(_dob_to_year)
    elif "dob" in cols:
        dob_y = df[cols["dob"]].map(_dob_to_year)
    out["dob_year"] = dob_y
    out["source_table"] = "emr_restricted_export"
    out["extracted_at"] = pd.Timestamp.now(tz=timezone.utc).tz_localize(None)
    out = out.drop_duplicates(subset=["research_id"], keep="last")
    return out


def _bootstrap_from_bigquery_sql(project_id: str) -> str:
    return f"""
    WITH ranked AS (
      SELECT
        CAST(research_id AS STRING) AS research_id,
        race,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(research_id AS STRING)
          ORDER BY
            CASE
              WHEN race IS NULL OR TRIM(CAST(race AS STRING)) = '' THEN 0
              ELSE LENGTH(TRIM(CAST(race AS STRING)))
            END DESC
        ) AS rn
      FROM `{project_id}.pub_canonical.path_synoptics`
    ),
    ps_race AS (
      SELECT research_id, race AS path_race
      FROM ranked
      WHERE rn = 1
    )
    SELECT
      CAST(c.research_id AS STRING) AS research_id,
      NULLIF(
        TRIM(
          COALESCE(
            NULLIF(TRIM(CAST(c.race AS STRING)), ''),
            NULLIF(TRIM(CAST(p.path_race AS STRING)), '')
          )
        ),
        ''
      ) AS race,
      CAST(NULL AS STRING) AS ethnicity,
      CAST(NULL AS INT64) AS dob_year,
      CAST(c.sex AS STRING) AS sex,
      CASE
        WHEN c.race IS NOT NULL AND TRIM(CAST(c.race AS STRING)) <> '' THEN 'canonical_patient_master'
        WHEN p.path_race IS NOT NULL AND TRIM(CAST(p.path_race AS STRING)) <> '' THEN 'path_synoptics'
        ELSE 'unresolved'
      END AS source_table,
      CURRENT_TIMESTAMP() AS extracted_at
    FROM `{project_id}.pub_canonical.canonical_patient_master` c
    LEFT JOIN ps_race p ON c.research_id = p.research_id
    ORDER BY 1
    """


def bootstrap_from_bigquery(project_id: str) -> pd.DataFrame:
    q = _bootstrap_from_bigquery_sql(project_id)
    try:
        client = bigquery.Client(project=project_id)
        return client.query(q).to_dataframe()
    except DefaultCredentialsError:
        pass
    proc = subprocess.run(
        [
            "bq",
            "query",
            "--use_legacy_sql=false",
            f"--project_id={project_id}",
            "--format=csv",
            "--max_rows",
            "200000",
            q,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return pd.read_csv(io.StringIO(proc.stdout))


def bootstrap_from_motherduck(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    q = """
    CREATE OR REPLACE TEMP TABLE _ps_race AS
    SELECT
      CAST(research_id AS VARCHAR) AS research_id,
      arg_max(
        race,
        (CASE
          WHEN race IS NULL OR trim(CAST(race AS VARCHAR)) = '' THEN 0
          ELSE length(trim(CAST(race AS VARCHAR)))
        END)
      ) AS path_race
    FROM main.path_synoptics
    GROUP BY 1;

    SELECT
      CAST(c.research_id AS VARCHAR) AS research_id,
      trim(COALESCE(
        NULLIF(trim(CAST(c.race AS VARCHAR)), ''),
        NULLIF(trim(CAST(p.path_race AS VARCHAR)), '')
      )) AS race,
      CAST(NULL AS VARCHAR) AS ethnicity,
      CAST(NULL AS INTEGER) AS dob_year,
      CAST(c.sex AS VARCHAR) AS sex,
      CASE
        WHEN c.race IS NOT NULL AND trim(CAST(c.race AS VARCHAR)) <> '' THEN 'canonical_patient_master'
        WHEN p.path_race IS NOT NULL AND trim(CAST(p.path_race AS VARCHAR)) <> '' THEN 'path_synoptics'
        ELSE 'unresolved'
      END AS source_table,
      CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS extracted_at
    FROM main.canonical_patient_master AS c
    LEFT JOIN _ps_race AS p ON CAST(c.research_id AS VARCHAR) = p.research_id
    ORDER BY 1;
    """
    return con.execute(q).df()


def _bq_schema() -> list[bigquery.SchemaField]:
    raw = json.loads(SCHEMA_JSON.read_text(encoding="utf-8"))
    return [
        bigquery.SchemaField(
            e["name"],
            e["type"],
            mode=e.get("mode", "NULLABLE"),
        )
        for e in raw
    ]


def load_bigquery(parquet_path: Path, project_id: str, table_id: str) -> int:
    """Load parquet into BigQuery with explicit schema (stable types for all-NULL columns)."""
    full = f"{project_id}.{table_id}"
    try:
        client = bigquery.Client(project=project_id)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=_bq_schema(),
        )
        with open(parquet_path, "rb") as f:
            job = client.load_table_from_file(f, full, job_config=job_config)
        job.result()
        return int(client.get_table(full).num_rows)
    except DefaultCredentialsError:
        load_bigquery_cli(parquet_path, project_id, table_id)
        cnt = subprocess.run(
            [
                "bq",
                "query",
                "--use_legacy_sql=false",
                f"--project_id={project_id}",
                "--format=csv",
                "--quiet",
                f"SELECT COUNT(*) AS n FROM `{full}`",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        lines = [ln for ln in cnt.stdout.strip().splitlines() if ln.strip()]
        return int(lines[-1]) if lines else 0


def load_bigquery_cli(parquet_path: Path, project_id: str, table_id: str) -> None:
    """Fallback load via `bq` CLI + schema JSON (no extra Py deps beyond CLI)."""
    fq = f"{project_id}.{table_id}"
    cmd = [
        "bq",
        "load",
        f"--project_id={project_id}",
        "--source_format=PARQUET",
        f"--schema={SCHEMA_JSON.resolve()}",
        "--replace",
        fq,
        str(parquet_path.resolve()),
    ]
    subprocess.run(cmd, check=True)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--emr-csv",
        type=Path,
        default=None,
        help="Restricted EMR export (not committed). Columns: research_id, race, ethnicity, sex, dob or dob_year.",
    )
    p.add_argument(
        "--parquet-out",
        type=Path,
        default=_REPO / "studies/tgdc_reconciliation/sources/emr_demographics_v1.parquet",
        help="Output parquet path.",
    )
    p.add_argument(
        "--bootstrap",
        choices=("bq", "md"),
        default="bq",
        help="Where to bootstrap cohort rows when --emr-csv is omitted (default: BigQuery).",
    )
    p.add_argument("--md-database", default="thyroid_canonical_publication_v1_0", help="MotherDuck database (for --bootstrap md).")
    p.add_argument("--bq-project", default=BQ_PROJECT_DEFAULT, help="GCP project for BigQuery bootstrap/load.")
    p.add_argument(
        "--bq-table",
        default=BQ_TABLE_DEFAULT,
        help="Dataset.table for load (no project prefix).",
    )
    p.add_argument(
        "--bq-load",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Load parquet into BigQuery after write (default: true).",
    )
    p.add_argument(
        "--bq-load-via-cli",
        action="store_true",
        help="Use `bq load` subprocess instead of google-cloud-bigquery client.",
    )
    p.add_argument(
        "--md-load",
        action="store_true",
        help="Also CREATE OR REPLACE pub_workspace.emr_demographics_v1 in MotherDuck (optional).",
    )
    args = p.parse_args()

    args.parquet_out.parent.mkdir(parents=True, exist_ok=True)

    if args.emr_csv:
        df = read_emr_csv(args.emr_csv)
    elif args.bootstrap == "bq":
        df = bootstrap_from_bigquery(args.bq_project)
    else:
        cfg = MotherDuckConfig(database=args.md_database)
        con = MotherDuckClient(cfg).connect_rw()
        con.execute(f'USE "{args.md_database}"')
        df = bootstrap_from_motherduck(con)
        con.close()

    # Ensure ethnicity remains string-like for parquet (avoids INT inference on all-null).
    if "ethnicity" in df.columns:
        df["ethnicity"] = df["ethnicity"].astype(object)
        df["ethnicity"] = df["ethnicity"].where(pd.notna(df["ethnicity"]), None)

    df.to_parquet(args.parquet_out, index=False)
    print(f"Wrote {len(df)} rows to {args.parquet_out}")

    if args.bq_load:
        if args.bq_load_via_cli:
            load_bigquery_cli(args.parquet_out, args.bq_project, args.bq_table)
            print(f"BigQuery {args.bq_project}.{args.bq_table} loaded via bq CLI")
        else:
            n = load_bigquery(args.parquet_out, args.bq_project, args.bq_table)
            print(f"BigQuery {args.bq_project}.{args.bq_table} row count = {n}")

    if args.md_load:
        cfg = MotherDuckConfig(database=args.md_database)
        con = MotherDuckClient(cfg).connect_rw()
        con.execute(f'USE "{args.md_database}"')
        con.execute("CREATE SCHEMA IF NOT EXISTS pub_workspace")
        path_esc = str(args.parquet_out.resolve()).replace("'", "''")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE pub_workspace.emr_demographics_v1 AS
            SELECT * FROM read_parquet('{path_esc}')
            """
        )
        n = con.execute("SELECT COUNT(*) FROM pub_workspace.emr_demographics_v1").fetchone()[0]
        con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_emr_demographics_v1_research_id "
            "ON pub_workspace.emr_demographics_v1(research_id)"
        )
        print(f"MotherDuck pub_workspace.emr_demographics_v1 row count = {n}")
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
