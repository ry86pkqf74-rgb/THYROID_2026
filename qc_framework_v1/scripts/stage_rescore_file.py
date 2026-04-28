"""
qc_framework_v1/scripts/stage_rescore_file.py
=============================================

Loads raw/FNAs_Rescored_Long_Format.xlsx into MotherDuck as
manuscript_workspace.fna_bethesda_rescore_staging_v1 so we can
verify canonical_fna_events_v1.bethesda_calculated_num against
the rescored categories.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import openpyxl

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_XLSX = REPO_ROOT / "raw" / "FNAs_Rescored_Long_Format.xlsx"
DEST_DB = "thyroid_canonical_publication_v1_0"
DEST_TABLE = "manuscript_workspace.fna_bethesda_rescore_staging_v1"


def main() -> None:
    if not SRC_XLSX.exists():
        print(f"ERROR: rescore file not found at {SRC_XLSX}", file=sys.stderr)
        sys.exit(2)

    print(f"[rescore_stage] reading {SRC_XLSX}")
    df = pd.read_excel(SRC_XLSX, sheet_name=0)
    print(f"[rescore_stage] rows: {len(df):,} cols: {len(df.columns)}")
    print(f"[rescore_stage] cols: {list(df.columns)}")

    # Normalize research_id to string (DB-side is VARCHAR)
    df["research_id"] = df["research_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    # fna_index to int
    df["fna_index"] = df["fna_index"].astype("Int64")
    # category_num to int (could be NaN)
    df["category_num"] = pd.to_numeric(df["category_num"], errors="coerce").astype("Int64")
    df["original_bethesda"] = pd.to_numeric(df["original_bethesda"], errors="coerce").astype("Int64")
    df["bethesda_2010_num"] = pd.to_numeric(df["bethesda_2010_num"], errors="coerce").astype("Int64")
    df["bethesda_2015_num"] = pd.to_numeric(df["bethesda_2015_num"], errors="coerce").astype("Int64")
    df["bethesda_2023_num"] = pd.to_numeric(df["bethesda_2023_num"], errors="coerce").astype("Int64")
    df["rules_category"] = pd.to_numeric(df["rules_category"], errors="coerce").astype("Int64")

    # Also stringify path_text + reasoning (large)
    df["path_text"] = df["path_text"].astype(str)
    df["evidence"] = df["evidence"].astype(str)
    df["reasoning"] = df["reasoning"].astype(str)

    print(f"[rescore_stage] connecting MD ({DEST_DB})")
    con = duckdb.connect("md:")
    con.execute(f"USE {DEST_DB}")

    # Drop + create
    con.execute(f"DROP TABLE IF EXISTS {DEST_TABLE}")
    con.execute(f"""
        CREATE TABLE {DEST_TABLE} AS
        SELECT * FROM df
    """)

    n = con.execute(f"SELECT COUNT(*) FROM {DEST_TABLE}").fetchone()[0]
    print(f"[rescore_stage] {DEST_TABLE} rowcount: {n:,}")
    print("[rescore_stage] columns:")
    for c in con.execute(f"DESCRIBE {DEST_TABLE}").fetchall():
        print(f"  {c[0]:30s} {c[1]}")


if __name__ == "__main__":
    main()
