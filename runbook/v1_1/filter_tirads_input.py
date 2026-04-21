#!/usr/bin/env python3
"""Filter clinical_notes_long.parquet down to the subset likely to contain
TI-RADS / ultrasound thyroid nodule content. Runs on the compute node before
vLLM spins up.

Input  : clinical_notes_long.parquet  (all ~11k notes)
Output : clinical_notes_tirads_input.parquet  (subset, ~2-3k notes)

Keyword filter is intentionally broad: a note containing any of the
ultrasound/TIRADS markers is kept. The LLM (not the filter) decides which
notes have real entities vs empty `entities: []`. This just drops the bulk
of clearly-irrelevant operative / H&P / death-summary notes to keep wall
clock inside the Slurm budget.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

# Strong US/TIRADS markers: a note must match at least one of these to qualify.
# These markers are specific to an actual ultrasound report body (not merely a
# passing "thyroid nodule" mention in an H&P or op-note indication).
STRONG_PATTERNS = [
    re.compile(r"\bti[\s\-]?rads\b", re.IGNORECASE),
    re.compile(r"\btr[1-5]\b"),  # TR1..TR5 — case sensitive to avoid false hits on words starting with 'tr'
    re.compile(r"\bsonogra(phic|m|phy)", re.IGNORECASE),  # sonographic / sonogram / sonography
    re.compile(r"\bisoechoic\b|\bhypoechoic\b|\bhyperechoic\b|\banechoic\b", re.IGNORECASE),
    re.compile(r"\becho(genic|texture)\b", re.IGNORECASE),
    re.compile(r"\btaller\s+than\s+wide\b", re.IGNORECASE),
    re.compile(r"\bpunctate\s+echogenic", re.IGNORECASE),
    re.compile(r"\bULTRASOUND\s+THYROID\b"),   # section header, typical of ACC/ACR reports
    re.compile(r"\bTHYROID\s+ULTRASOUND\b"),
    re.compile(r"\bTHYROID,?\s*\n\s*(Date|Clinical\s+Indication)", re.IGNORECASE),
    re.compile(r"\bUS\s+THYROID\b"),
    re.compile(r"\bTHYROID\s+US\b"),
]


def note_matches(text: str) -> bool:
    if not text:
        return False
    return any(pat.search(text) for pat in STRONG_PATTERNS)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        default="/scratch/lglosse/thyroid_repo/processed/remaining/clinical_notes_long.parquet",
        help="Path to full clinical_notes_long.parquet",
    )
    ap.add_argument(
        "--output",
        default="/scratch/lglosse/thyroid_repo/processed/remaining/clinical_notes_tirads_input.parquet",
        help="Path to write filtered parquet",
    )
    ap.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="If >0, truncate output to first N rows (useful for smoke tests).",
    )
    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        print(f"ERROR: input parquet not found: {in_path}", file=sys.stderr)
        sys.exit(2)

    df = pd.read_parquet(in_path)
    n_in = len(df)
    print(f"[filter] input:  {n_in:,} rows from {in_path}")

    # Ensure note_row_id exists (required by run_extraction_concurrent.py)
    if "note_row_id" not in df.columns:
        df["note_row_id"] = df.index.astype(str)

    # Ensure note_date exists (the driver reads it for provenance + the prompt
    # needs it for date attribution). Derive from ingested_at_utc or leave blank
    # — the prompt handles null dates gracefully.
    if "note_date" not in df.columns:
        df["note_date"] = ""

    mask = df["note_text"].astype(str).map(note_matches)
    filtered = df[mask].copy()
    n_out = len(filtered)
    print(f"[filter] kept:   {n_out:,} rows ({100 * n_out / max(n_in, 1):.1f}%) match TIRADS/US patterns")

    if args.max_rows and n_out > args.max_rows:
        filtered = filtered.head(args.max_rows).copy()
        print(f"[filter] truncated to first {args.max_rows:,} rows for smoke test")

    # Breakdown by note_type so we can sanity-check
    if "note_type" in filtered.columns:
        print("[filter] note_type distribution in kept rows:")
        print(filtered["note_type"].value_counts().head(15).to_string())

    filtered.to_parquet(out_path, index=False)
    print(f"[filter] wrote:  {out_path}  ({out_path.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
