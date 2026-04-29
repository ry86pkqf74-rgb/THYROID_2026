#!/usr/bin/env python3
"""Rewrite mig_168 ratified histology enum CSV for mig_172b.

This is an authoring helper only. It does not connect to MotherDuck and does
not mutate database state.

Rules implemented for the post-mig_178 lane:
  * Keep only the recurrence/completion histology enum columns that mig_178 did
    not already normalize.
  * Remove the rejected ``mtc_ptc_mixed`` canonical code by remapping any such
    raw value to the mig_178 mixed-label convention ``MTC | PTC``.
  * Emit a deterministic CSV plus a stdout row-delta/checksum log.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = (
    REPO_ROOT
    / "exports"
    / "mig168_pm_vocab_audit_20260429_175417"
    / "pm_ssot_enum_dictionary_draft_ratified.csv"
)
DEFAULT_OUTPUT = (
    REPO_ROOT
    / "exports"
    / "mig168_pm_vocab_audit_20260429_175417"
    / "pm_ssot_enum_dictionary_post_mig178_v1.csv"
)

SCOPE_COLS = {
    "recurrence_histology",
    "recurrence_histology_v2",
    "completion_prior_histology",
    "completion_histology_type",
}

# The ratified CSV normalized embedded newlines to spaces for two observed raw
# values, while MotherDuck still stores the exact newline-bearing strings. Keep
# explicit aliases so the apply SQL has complete exact-match coverage without
# mutating any database state in this authoring step.
RAW_VALUE_ALIASES = {
    (
        "recurrence_histology",
        "metastatic PTC classic subtype with tall cell component ~25%",
    ): "metastatic PTC\nclassic subtype with tall cell component ~25%",
    (
        "completion_prior_histology",
        "MTC PTC mixed composit",
    ): "MTC\nPTC mixed composit",
}

REJECTED_CODE = "mtc_ptc_mixed"
MIG178_MIXED_LABEL = "MTC | PTC"


def _norm(value: object) -> str:
    return str(value or "").strip().lower()


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rewrite_rows(rows: Iterable[dict[str, str]]) -> tuple[list[dict[str, str]], dict[str, int]]:
    out: list[dict[str, str]] = []
    stats = {
        "input_rows": 0,
        "rows_out": 0,
        "exact_match_alias_rows_added": 0,
        "rows_filtered_to_other_cols": 0,
        "mtc_ptc_mixed_code_rows_rewritten": 0,
        "mtc_ptc_mixed_code_rows_remaining": 0,
    }

    for row in rows:
        stats["input_rows"] += 1
        source_col = row.get("source_col", "")
        if source_col not in SCOPE_COLS:
            stats["rows_filtered_to_other_cols"] += 1
            continue

        new_row = dict(row)
        if _norm(new_row.get("canonical_code")) == REJECTED_CODE:
            stats["mtc_ptc_mixed_code_rows_rewritten"] += 1
            new_row["canonical_code"] = MIG178_MIXED_LABEL
            new_row["display_label"] = MIG178_MIXED_LABEL
            existing_note = str(new_row.get("ratification_notes") or "").strip()
            suffix = (
                "mig_172b post-mig_178 rewrite: Logan rejected mtc_ptc_mixed; "
                "remapped to MTC | PTC convention"
            )
            new_row["ratification_notes"] = f"{existing_note}; {suffix}" if existing_note else suffix

        if _norm(new_row.get("canonical_code")) == REJECTED_CODE:
            stats["mtc_ptc_mixed_code_rows_remaining"] += 1
        out.append(new_row)

        alias_value = RAW_VALUE_ALIASES.get((source_col, row.get("raw_value", "")))
        if alias_value:
            alias_row = dict(new_row)
            alias_row["raw_value"] = alias_value
            existing_note = str(alias_row.get("ratification_notes") or "").strip()
            suffix = "mig_172b exact-match alias for live MotherDuck newline-bearing raw value"
            alias_row["ratification_notes"] = f"{existing_note}; {suffix}" if existing_note else suffix
            out.append(alias_row)
            stats["exact_match_alias_rows_added"] += 1

    stats["rows_out"] = len(out)
    return out, stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input CSV not found: {args.input}")

    with args.input.open("r", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise SystemExit(f"Input CSV has no header: {args.input}")
        required = {"source_col", "raw_value", "canonical_code", "display_label"}
        missing = sorted(required - set(reader.fieldnames))
        if missing:
            raise SystemExit(f"Input CSV missing required columns: {missing}")
        rows, stats = rewrite_rows(reader)
        fieldnames = list(reader.fieldnames)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    stats.update(
        {
            "input_csv": str(args.input.relative_to(REPO_ROOT)),
            "output_csv": str(args.output.relative_to(REPO_ROOT)),
            "output_sha256": _sha256(args.output),
            "scoped_columns": sorted(SCOPE_COLS),
        }
    )
    print(json.dumps(stats, indent=2, sort_keys=True))
    if stats["mtc_ptc_mixed_code_rows_remaining"] != 0:
        raise SystemExit("Rejected mtc_ptc_mixed canonical code remains after rewrite")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())