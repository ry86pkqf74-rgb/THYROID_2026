#!/usr/bin/env python3
"""
Build PHI-scrubbed emr_demographics_v1 parquet and load into MotherDuck pub_workspace.

THY-1: optional restricted EMR CSV (research_id, race, ethnicity, sex, dob|dob_year)
produces row-level demographics with dob reduced to dob_year only. Without CSV,
bootstrap from canonical_patient_master + path_synoptics race fallback (explicit
source_table labels; not a substitute for a true EMR export when governance requires it).

Auth: motherduck.local.toml via motherduck_client.get_token — never print tokens.
"""

from __future__ import annotations

import argparse
import sys
from datetime import timezone
from pathlib import Path

import duckdb
import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402


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
    # ISO YYYY-MM-DD
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
    p.add_argument("--skip-md-load", action="store_true", help="Write parquet only; do not CREATE TABLE in MotherDuck.")
    p.add_argument("--md-database", default="thyroid_canonical_publication_v1_0", help="MotherDuck database name.")
    args = p.parse_args()

    args.parquet_out.parent.mkdir(parents=True, exist_ok=True)

    if args.emr_csv:
        df = read_emr_csv(args.emr_csv)
    else:
        cfg = MotherDuckConfig(database=args.md_database)
        con = MotherDuckClient(cfg).connect_rw()
        con.execute(f'USE "{args.md_database}"')
        df = bootstrap_from_motherduck(con)
        con.close()

    df.to_parquet(args.parquet_out, index=False)
    print(f"Wrote {len(df)} rows to {args.parquet_out}")

    if args.skip_md_load:
        return 0

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
