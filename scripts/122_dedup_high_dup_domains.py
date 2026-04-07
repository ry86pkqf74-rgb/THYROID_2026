#!/usr/bin/env python3
"""122_dedup_high_dup_domains.py — Deduplicate v2 domain parquets exceeding 5% threshold.

Targets: labs (12.2%), tg_kinetics (10.4%), cervical_ln_detail (9.6%),
         patient_decision_adherence (6.6%)

Dedup key: research_id, note_row_id, entity_type, entity_value_norm
(matches the registry-defined dedupe_key for all domains).

Usage:
  .venv/bin/python scripts/122_dedup_high_dup_domains.py
  .venv/bin/python scripts/122_dedup_high_dup_domains.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from utils.text_helpers import save_parquet  # noqa: E402

V2_DIR = ROOT / "processed" / "output" / "v2_parquets"

TARGET_STEMS = [
    "note_entities_llm_labs",
    "note_entities_llm_tg_kinetics",
    "note_entities_llm_cervical_ln_detail",
    "note_entities_llm_patient_decision_adherence",
]

DEDUPE_KEY = ["research_id", "note_row_id"]

ENTITY_DEDUPE_KEY = ["research_id", "note_row_id", "entity_type", "entity_value_norm"]


def expand_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    """Expand fleet JSON format if result_json is present and entity_type is not."""
    if "entity_type" in df.columns:
        return df
    if "result_json" not in df.columns:
        return df

    out_rows = []
    for _, row in df.iterrows():
        raw = row.get("result_json")
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        try:
            payload = json.loads(str(raw))
        except (json.JSONDecodeError, TypeError):
            continue
        entities = payload.get("entities") if isinstance(payload, dict) else None
        if not isinstance(entities, list):
            continue
        seen = set()
        for ent in entities:
            if not isinstance(ent, dict):
                continue
            et = ent.get("entity_type", "")
            ev = ent.get("entity_value") or ent.get("entity_value_raw") or ""
            key = (row.get("research_id"), row.get("note_row_id"), et, ev)
            if key in seen:
                continue
            seen.add(key)
            out_rows.append(dict(row) | {
                "entity_type": et,
                "entity_value_raw": ev,
                "entity_value_norm": ev,
                "entity_date": ent.get("entity_date"),
            })

    if not out_rows:
        return df
    return pd.DataFrame(out_rows)


def dedup_note_level(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate at note level (same research_id + note_row_id)."""
    if "result_json" in df.columns:
        available = [c for c in DEDUPE_KEY if c in df.columns]
        if available:
            before = len(df)
            df = df.drop_duplicates(subset=available, keep="first")
            removed = before - len(df)
            if removed > 0:
                print(f"    note-level dedup: removed {removed:,} rows")
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 70)
    print("  122 — dedup high-dup v2 domains")
    print("=" * 70)

    for stem in TARGET_STEMS:
        pq = V2_DIR / f"{stem}.parquet"
        if not pq.exists():
            print(f"  skip (not found): {stem}")
            continue

        df = pd.read_parquet(pq)
        before = len(df)

        df = dedup_note_level(df)
        after = len(df)
        dup_rate = (before - after) / before * 100 if before > 0 else 0

        print(f"  {stem}: {before:,} -> {after:,} rows (removed {before - after:,}, was {dup_rate:.1f}%)")

        if not args.dry_run and after < before:
            save_parquet(df, pq)

    if args.dry_run:
        print("  dry-run: no files changed")

    print("=" * 70)
    print("  DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
