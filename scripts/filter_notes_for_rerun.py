"""
Filter clinical_notes_long.parquet to the union of research_ids in our
extraction queues, writing a smaller parquet that the Vast.ai concurrent
runner can consume via --input-parquet.

Usage:
    python scripts/filter_notes_for_rerun.py \
        --queues queues/ids_tirads.txt queues/ids_ln.txt queues/ids_fna.txt \
        --input processed/remaining/clinical_notes_long.parquet \
        --output processed/remaining/clinical_notes_long_rerun.parquet
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _read_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip()
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--queues", nargs="+", required=True, type=Path)
    ap.add_argument(
        "--input", type=Path,
        default=Path("processed/remaining/clinical_notes_long.parquet"),
    )
    ap.add_argument(
        "--output", type=Path,
        default=Path("processed/remaining/clinical_notes_long_rerun.parquet"),
    )
    ap.add_argument("--audit", type=Path, default=Path("queues/filter_audit.json"))
    args = ap.parse_args()

    union: set[str] = set()
    per_queue: dict[str, int] = {}
    for q in args.queues:
        ids = _read_ids(q)
        per_queue[q.name] = len(ids)
        union |= ids

    notes = pd.read_parquet(args.input)
    notes["research_id"] = notes["research_id"].astype(str).str.strip()
    # normalize float-ish ids
    notes["research_id"] = notes["research_id"].str.replace(r"\.0$", "", regex=True)

    notes_rids = set(notes["research_id"].unique())
    intersect = union & notes_rids
    missing = sorted(union - notes_rids, key=lambda x: (len(x), x))

    filtered = notes[notes["research_id"].isin(intersect)].reset_index(drop=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    filtered.to_parquet(args.output, index=False)

    audit = {
        "queues": per_queue,
        "union_queue_rids": len(union),
        "notes_parquet_rids": len(notes_rids),
        "intersect_rids": len(intersect),
        "missing_rids_count": len(missing),
        "missing_rids_sample": missing[:50],
        "input_notes": len(notes),
        "filtered_notes": len(filtered),
        "output_parquet": str(args.output),
    }
    args.audit.write_text(json.dumps(audit, indent=2))
    print(json.dumps({k: v for k, v in audit.items() if k != "missing_rids_sample"}, indent=2))
    print(f"\nWrote {len(filtered):,} notes for {len(intersect):,} research_ids -> {args.output}")


if __name__ == "__main__":
    main()
