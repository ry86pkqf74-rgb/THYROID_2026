#!/usr/bin/env python3
"""Backfill missing provenance columns onto completed extraction artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-parquet",
        type=Path,
        required=True,
        help="Canonical source note parquet containing provenance columns.",
    )
    parser.add_argument(
        "artifacts",
        nargs="+",
        type=Path,
        help="Completed extraction parquet artifacts to patch in place.",
    )
    return parser.parse_args()


def load_source(source_parquet: Path) -> pd.DataFrame:
    source_df = pd.read_parquet(
        source_parquet,
        columns=["note_row_id", "preprocessed_at_utc"],
    ).copy()
    source_df["note_row_id"] = source_df["note_row_id"].astype(str)
    if source_df["note_row_id"].duplicated().any():
        raise ValueError("Source parquet has duplicate note_row_id values")
    return source_df.set_index("note_row_id")


def backfill_artifact(path: Path, source_indexed: pd.DataFrame) -> None:
    df = pd.read_parquet(path).copy()
    if "note_row_id" not in df.columns:
        raise ValueError(f"{path} is missing note_row_id")

    df["note_row_id"] = df["note_row_id"].astype(str)
    missing_before = [column for column in ["preprocessed_at_utc"] if column not in df.columns]

    if "preprocessed_at_utc" not in df.columns:
        df = df.merge(
            source_indexed[["preprocessed_at_utc"]],
            left_on="note_row_id",
            right_index=True,
            how="left",
            validate="many_to_one",
        )
    else:
        source_values = df["note_row_id"].map(source_indexed["preprocessed_at_utc"])
        df["preprocessed_at_utc"] = df["preprocessed_at_utc"].fillna(source_values)

    unmatched = int(df["preprocessed_at_utc"].isna().sum())
    if unmatched:
        raise ValueError(f"{path} still has {unmatched} rows without preprocessed_at_utc after backfill")

    df.to_parquet(path, index=False)
    print(f"PATCHED {path.name} missing_before={missing_before} rows={len(df)}")


def main() -> int:
    args = parse_args()
    source_indexed = load_source(args.source_parquet)
    for artifact in args.artifacts:
        backfill_artifact(artifact, source_indexed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())