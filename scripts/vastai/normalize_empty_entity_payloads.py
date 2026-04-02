#!/usr/bin/env python3
"""Normalize exact empty JSON payloads to an explicit empty entities list."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "artifacts",
        nargs="+",
        type=Path,
        help="Completed extraction parquet artifacts to patch in place.",
    )
    return parser.parse_args()


def normalize_artifact(path: Path) -> None:
    df = pd.read_parquet(path).copy()
    if "result_json" not in df.columns:
        raise ValueError(f"{path} is missing result_json")

    replacements = 0
    normalized_values: list[str] = []
    for value in df["result_json"]:
        obj = json.loads(value)
        if obj == {}:
            normalized_values.append(json.dumps({"entities": []}, separators=(",", ":")))
            replacements += 1
        else:
            normalized_values.append(value)

    if replacements:
        df["result_json"] = normalized_values
        df.to_parquet(path, index=False)

    print(f"NORMALIZED {path.name} replacements={replacements} rows={len(df)}")


def main() -> int:
    args = parse_args()
    for artifact in args.artifacts:
        normalize_artifact(artifact)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())