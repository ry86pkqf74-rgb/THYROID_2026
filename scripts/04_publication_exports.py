#!/usr/bin/env python3
"""
04_publication_exports.py

Phase 3 publication exports:
- Generates core study CSVs from research views
- Generates matching data dictionary slices for each export
- Provides export_study_cohort(view_name, filename) for rapid reuse
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
MD_DATABASE = "thyroid_research_2026"
EXPORT_DIR = ROOT / "exports"
GLOBAL_DICTIONARY_CSV = ROOT / "data_dictionary.csv"

EXPORTS: list[tuple[str, str]] = [
    ("ptc_cohort", "ptc_full.csv"),
    ("recurrence_risk_cohort", "recurrence_full.csv"),
    ("fna_accuracy_view", "fna_accuracy_full.csv"),
    ("imaging_pathology_correlation", "imaging_correlation.csv"),
]


def load_global_dictionary() -> pd.DataFrame:
    if GLOBAL_DICTIONARY_CSV.exists():
        return pd.read_csv(GLOBAL_DICTIONARY_CSV)
    return pd.DataFrame(
        columns=["object_type", "object_name", "column_name", "data_type", "description", "phase"]
    )


def _table_columns(con: duckdb.DuckDBPyConnection, view_name: str) -> pd.DataFrame:
    cols = con.execute(f"DESCRIBE {view_name}").fetchdf()
    return cols.rename(columns={"column_name": "column_name", "column_type": "data_type"})[
        ["column_name", "data_type"]
    ]


def export_study_cohort(
    con: duckdb.DuckDBPyConnection,
    dictionary_df: pd.DataFrame,
    view_name: str,
    filename: str,
) -> tuple[Path, Path, int]:
    """
    Export any DuckDB view/table to CSV and generate a matching dictionary slice.

    Returns:
        csv_path, dictionary_path, row_count
    """
    EXPORT_DIR.mkdir(exist_ok=True)
    csv_path = EXPORT_DIR / filename
    dict_path = EXPORT_DIR / f"{csv_path.stem}_data_dictionary.csv"

    row_count = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
    csv_sql_path = str(csv_path).replace("'", "''")
    con.execute(
        f"COPY (SELECT * FROM {view_name}) TO '{csv_sql_path}' (HEADER, DELIMITER ',')"
    )

    desc = _table_columns(con, view_name)

    if not dictionary_df.empty:
        subset = dictionary_df[dictionary_df["object_name"] == view_name].copy()
    else:
        subset = pd.DataFrame(
            columns=["object_type", "object_name", "column_name", "data_type", "description", "phase"]
        )

    if subset.empty:
        subset = pd.DataFrame(
            {
                "object_type": ["view"] * len(desc),
                "object_name": [view_name] * len(desc),
                "column_name": desc["column_name"].tolist(),
                "data_type": desc["data_type"].tolist(),
                "description": [f"Column `{c}` from `{view_name}` export" for c in desc["column_name"]],
                "phase": ["phase3"] * len(desc),
            }
        )
    else:
        # Ensure schema parity and include new columns not yet in global dictionary
        present = set(subset["column_name"].tolist())
        missing = desc[~desc["column_name"].isin(present)].copy()
        if not missing.empty:
            missing_rows = pd.DataFrame(
                {
                    "object_type": ["view"] * len(missing),
                    "object_name": [view_name] * len(missing),
                    "column_name": missing["column_name"].tolist(),
                    "data_type": missing["data_type"].tolist(),
                    "description": [f"Column `{c}` from `{view_name}` export" for c in missing["column_name"]],
                    "phase": ["phase3"] * len(missing),
                }
            )
            subset = pd.concat([subset, missing_rows], ignore_index=True)

        # normalize dtype from live view
        dtype_map = dict(zip(desc["column_name"], desc["data_type"]))
        subset["data_type"] = subset["column_name"].map(dtype_map).fillna(subset["data_type"])

    subset = subset.sort_values(["object_name", "column_name"]).reset_index(drop=True)
    subset.to_csv(dict_path, index=False)

    return csv_path, dict_path, row_count


def _connect(use_md: bool) -> duckdb.DuckDBPyConnection:
    if use_md:
        token = os.environ.get("MOTHERDUCK_TOKEN") or ""
        if not token:
            try:
                import tomllib
            except ImportError:
                import tomli as tomllib  # type: ignore
            secrets_path = ROOT / ".streamlit" / "secrets.toml"
            with open(secrets_path, "rb") as f:
                token = tomllib.load(f).get("MOTHERDUCK_TOKEN", "")
        con = duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={token}")
        con.execute(f"USE {MD_DATABASE}")
        print(f"Connected to MotherDuck: {MD_DATABASE}")
        return con
    con = duckdb.connect(str(DB_PATH), read_only=True)
    print(f"Connected to local: {DB_PATH}")
    return con


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true", help="Read from MotherDuck instead of local DuckDB")
    args = parser.parse_args()

    print("=" * 72)
    print("PHASE 3: Publication exports")
    print("=" * 72)

    con = _connect(args.md)
    dictionary_df = load_global_dictionary()

    for view_name, filename in EXPORTS:
        csv_path, dict_path, n = export_study_cohort(con, dictionary_df, view_name, filename)
        print(f"✅ {view_name:30s} -> {csv_path.name:28s} rows={n:,}")
        print(f"   dictionary -> {dict_path.name}")

    con.close()
    print("-" * 72)
    print(f"Export folder: {EXPORT_DIR}")
    print("Done.")


if __name__ == "__main__":
    main()
