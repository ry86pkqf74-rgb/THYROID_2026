#!/usr/bin/env python3
"""Validate extraction checkpoints/parquets against the source note corpus."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

REQUIRED_COLUMNS = [
    "research_id",
    "note_row_id",
    "note_type",
    "note_date",
    "linkage_date",
    "source_workbook",
    "source_sheet",
    "source_column",
    "result_json",
]

COMPARE_COLUMNS = [
    "research_id",
    "note_type",
    "note_date",
    "source_workbook",
    "source_sheet",
    "source_column",
]


def _normalize(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "nat", "none"} else text


def _load_artifact(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.name.endswith(".jsonl"):
        return pd.read_json(path, lines=True)
    raise ValueError(f"Unsupported artifact type: {path}")


def _validate_artifact(path: Path, source_df: pd.DataFrame, require_complete: bool, expected_rows: int | None) -> dict[str, Any]:
    df = _load_artifact(path)
    summary: dict[str, Any] = {
        "artifact": str(path),
        "rows": int(len(df)),
        "missing_columns": [column for column in REQUIRED_COLUMNS if column not in df.columns],
        "duplicate_note_row_id": None,
        "unmatched_note_row_id": None,
        "source_mismatches": {},
        "result_json_parse_errors": 0,
        "status": "ok",
    }

    if expected_rows is not None:
        summary["expected_rows"] = expected_rows
        summary["row_count_matches_expected"] = int(len(df)) == expected_rows
        if require_complete and len(df) != expected_rows:
            summary["status"] = "error"

    if "note_row_id" not in df.columns:
        summary["status"] = "error"
        return summary

    df = df.copy()
    df["note_row_id"] = df["note_row_id"].astype(str)
    summary["duplicate_note_row_id"] = int(len(df) - df["note_row_id"].nunique())
    if summary["duplicate_note_row_id"]:
        summary["status"] = "error"

    join_cols = ["note_row_id", *[column for column in COMPARE_COLUMNS if column in df.columns]]
    merged = df[join_cols].merge(source_df, on="note_row_id", how="left", suffixes=("_artifact", "_source"))
    summary["unmatched_note_row_id"] = int(merged["research_id_source"].isna().sum())
    if summary["unmatched_note_row_id"]:
        summary["status"] = "error"

    for column in COMPARE_COLUMNS:
        artifact_col = f"{column}_artifact"
        source_col = f"{column}_source"
        if artifact_col not in merged.columns:
            continue
        mismatches = int(
            (
                merged[artifact_col].map(_normalize)
                != merged[source_col].map(_normalize)
            ).sum()
        )
        summary["source_mismatches"][column] = mismatches
        if column != "note_date" and mismatches:
            summary["status"] = "error"

    if "result_json" in df.columns:
        parse_errors = 0
        for value in df["result_json"]:
            try:
                json.loads(value)
            except Exception:
                parse_errors += 1
        summary["result_json_parse_errors"] = parse_errors
        if parse_errors:
            summary["status"] = "error"

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate extraction checkpoints/parquets against source notes")
    parser.add_argument("artifacts", nargs="+", type=Path, help="Checkpoint (.jsonl) or parquet artifacts to validate")
    parser.add_argument("--source-parquet", type=Path, required=True, help="Source note parquet used for extraction")
    parser.add_argument("--expected-rows", type=int, default=None, help="Expected note count for complete artifacts")
    parser.add_argument("--require-complete", action="store_true", help="Fail if row count does not match expected rows")
    args = parser.parse_args()

    source_df = pd.read_parquet(
        args.source_parquet,
        columns=["note_row_id", *COMPARE_COLUMNS],
    ).copy()
    source_df["note_row_id"] = source_df["note_row_id"].astype(str)

    results = [
        _validate_artifact(path, source_df, args.require_complete, args.expected_rows)
        for path in args.artifacts
    ]
    print(json.dumps(results, indent=2))

    return 1 if any(result["status"] != "ok" for result in results) else 0


if __name__ == "__main__":
    sys.exit(main())