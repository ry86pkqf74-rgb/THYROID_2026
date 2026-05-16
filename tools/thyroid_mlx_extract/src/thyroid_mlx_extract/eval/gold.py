"""Gold-set utilities."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

GOLD_SCHEMA_COLS = ["source_pk", "field_path", "gold_value", "gold_evidence_substring"]


def validate_gold(gold_csv: Path | str) -> tuple[bool, list[str]]:
    """Check that a gold CSV has the expected columns and no obvious errors."""
    errors: list[str] = []
    try:
        df = pd.read_csv(gold_csv)
    except Exception as e:
        return False, [f"Could not read CSV: {e}"]

    for col in GOLD_SCHEMA_COLS:
        if col not in df.columns:
            errors.append(f"Missing column: {col}")
    if errors:
        return False, errors

    n_dupes = df.duplicated(subset=["source_pk", "field_path"]).sum()
    if n_dupes:
        errors.append(f"{n_dupes} duplicate (source_pk, field_path) rows")
    n_missing_pk = df["source_pk"].isna().sum()
    if n_missing_pk:
        errors.append(f"{n_missing_pk} rows missing source_pk")

    return not errors, errors
