#!/usr/bin/env python3
"""
run_extraction.py — Run all entity extractors on clinical_notes_long

Loads processed/clinical_notes_long.parquet, applies regex (and optionally
LLM) extractors, and writes one parquet per entity domain:

  processed/note_entities_staging.parquet
  processed/note_entities_genetics.parquet
  processed/note_entities_procedures.parquet
  processed/note_entities_complications.parquet
  processed/note_entities_medications.parquet
  processed/note_entities_problem_list.parquet

Usage:
  python notes_extraction/run_extraction.py
      --target medications        # re-run only the medications domain
      --research-ids ids.txt      # re-run only notes for listed research_ids
      --target genetics --research-ids ids.txt  # combined filter
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notes_extraction.base import BaseExtractor, EntityMatch
from notes_extraction.extract_llm import LLMExtractor
from notes_extraction.extract_regex import ALL_REGEX_EXTRACTORS
from notes_extraction.vocab import ENTITY_SCHEMA_COLUMNS
from utils.text_helpers import save_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("extraction")

PROCESSED = ROOT / "processed"
np.random.seed(42)

PHI_SNIPPET_LEN = 80

# Map extractor class entity_domain -> output parquet stem
DOMAIN_TO_FILE = {
    "staging": "note_entities_staging",
    "genetics": "note_entities_genetics",
    "procedures": "note_entities_procedures",
    "complications": "note_entities_complications",
    "medications": "note_entities_medications",
    "problem_list": "note_entities_problem_list",
}


def run_extractors(
    notes_df: pd.DataFrame,
    extractors: list[BaseExtractor],
) -> dict[str, list[dict]]:
    """Run all extractors across every note row, grouping results by domain."""
    domain_results: dict[str, list[dict]] = {}

    has_note_date = "note_date" in notes_df.columns

    total = len(notes_df)
    for i, (_, row) in enumerate(notes_df.iterrows()):
        note_row_id = row["note_row_id"]
        research_id = int(row["research_id"])
        note_type = row["note_type"]
        note_text = str(row["note_text"])
        note_date = row.get("note_date") if has_note_date else None
        if pd.isna(note_date):
            note_date = None

        for ext in extractors:
            matches = ext.extract(note_row_id, research_id, note_type, note_text, note_date=note_date)
            if matches:
                domain = ext.entity_domain
                domain_results.setdefault(domain, [])
                for m in matches:
                    domain_results[domain].append(m.to_dict())

        if (i + 1) % 2000 == 0:
            log.info(f"  Processed {i+1:,}/{total:,} notes ...")

    return domain_results


def results_to_dataframes(domain_results: dict[str, list[dict]]) -> dict[str, pd.DataFrame]:
    """Convert domain results to validated DataFrames."""
    dfs: dict[str, pd.DataFrame] = {}
    for domain, records in domain_results.items():
        if not records:
            continue
        df = pd.DataFrame(records)
        for col in ENTITY_SCHEMA_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[ENTITY_SCHEMA_COLUMNS]
        BaseExtractor.validate_output(df)
        dfs[domain] = df
    return dfs


def _merge_into_existing(
    domain: str,
    new_df: pd.DataFrame,
    replace_research_ids: set[int] | None,
) -> pd.DataFrame:
    """Merge new extraction results into an existing parquet (for targeted runs).

    Replaces rows for the given research_ids (or all rows if replace_research_ids
    is None, i.e., full run) and appends any new rows not previously in the file.
    """
    out_path = PROCESSED / f"{DOMAIN_TO_FILE[domain]}.parquet"
    if not out_path.exists() or replace_research_ids is None:
        return new_df  # Full run: just return new_df as-is

    existing_df = pd.read_parquet(out_path)
    if "research_id" not in existing_df.columns:
        return new_df

    # Drop rows for the targeted research_ids from the existing parquet
    mask = existing_df["research_id"].astype(int).isin(replace_research_ids)
    kept_existing = existing_df[~mask].copy()

    # Concatenate kept rows + new extractions
    merged = pd.concat([kept_existing, new_df], ignore_index=True)
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run entity extractors on clinical_notes_long"
    )
    parser.add_argument(
        "--target",
        metavar="DOMAIN",
        help=(
            "Re-extract only a specific entity domain "
            f"(choices: {', '.join(sorted(DOMAIN_TO_FILE))}). "
            "Merges results with existing parquet for unaffected research_ids."
        ),
    )
    parser.add_argument(
        "--research-ids",
        metavar="FILE",
        help=(
            "Path to a text file with one research_id per line. "
            "Only notes for those patients will be (re-)extracted. "
            "Combines with --target if both are given."
        ),
    )
    args = parser.parse_args()

    # Validate --target
    target_domain: str | None = args.target
    if target_domain and target_domain not in DOMAIN_TO_FILE:
        parser.error(
            f"Unknown domain '{target_domain}'. "
            f"Valid domains: {', '.join(sorted(DOMAIN_TO_FILE))}"
        )

    # Load research_id filter
    research_id_filter: set[int] | None = None
    if args.research_ids:
        ids_path = Path(args.research_ids)
        if not ids_path.exists():
            log.error(f"research-ids file not found: {ids_path}")
            sys.exit(1)
        research_id_filter = {
            int(line.strip())
            for line in ids_path.read_text().splitlines()
            if line.strip().isdigit()
        }
        log.info(f"  Research-id filter: {len(research_id_filter):,} patients")

    log.info("=" * 70)
    log.info("  ENTITY EXTRACTION PIPELINE")
    log.info("=" * 70)

    notes_path = PROCESSED / "clinical_notes_long.parquet"
    if not notes_path.exists():
        log.error(f"Input not found: {notes_path}")
        log.error("Run scripts/build_clinical_notes_long.py first.")
        sys.exit(1)

    notes_df = pd.read_parquet(notes_path)
    log.info(f"  Loaded {len(notes_df):,} notes from {notes_path.name}")

    # Apply research_id filter
    if research_id_filter:
        before = len(notes_df)
        notes_df = notes_df[
            notes_df["research_id"].astype(int).isin(research_id_filter)
        ].reset_index(drop=True)
        log.info(
            f"  Filtered to {len(notes_df):,} notes "
            f"({before - len(notes_df):,} excluded by research-id filter)"
        )

    # Select extractors
    all_extractors: list[BaseExtractor] = [cls() for cls in ALL_REGEX_EXTRACTORS]

    llm = LLMExtractor()
    if llm.available:
        all_extractors.append(llm)
        log.info("  LLM extractor enabled")
    else:
        log.info("  LLM extractor disabled (no API key)")

    # Filter to target domain if specified
    if target_domain:
        extractors = [
            e for e in all_extractors
            if e.entity_domain == target_domain or e.entity_domain == "llm"
        ]
        log.info(f"  Target domain filter: '{target_domain}' "
                 f"({len(extractors)} extractor(s) active)")
    else:
        extractors = all_extractors

    domain_results = run_extractors(notes_df, extractors)

    dfs = results_to_dataframes(domain_results)

    # Determine which domains to write
    if target_domain:
        domains_to_write = {target_domain} | ({"llm"} if "llm" in dfs else set())
    else:
        domains_to_write = set(DOMAIN_TO_FILE.keys()) | ({"llm"} if "llm" in dfs else set())

    log.info("\n  Results summary:")
    for domain, df in dfs.items():
        if domain not in domains_to_write:
            continue

        file_stem = DOMAIN_TO_FILE.get(domain, f"note_entities_{domain}")
        out_path = PROCESSED / f"{file_stem}.parquet"

        # Merge with existing parquet on targeted re-runs
        final_df = _merge_into_existing(domain, df, research_id_filter)
        save_parquet(final_df, out_path)

        n_present = (df["present_or_negated"] == "present").sum()
        n_negated = (df["present_or_negated"] == "negated").sum()
        n_patients = df["research_id"].nunique()
        n_entity_dated = df["entity_date"].notna().sum()
        n_note_dated = df["note_date"].notna().sum()

        log.info(
            f"    {domain:20s}  {len(df):>6,} entities  "
            f"({n_present:,} present, {n_negated:,} negated)  "
            f"{n_patients:,} patients"
        )
        log.info(
            f"      dates: {n_entity_dated:,} entity_date, "
            f"{n_note_dated:,} note_date"
        )

        top = df["entity_value_norm"].value_counts().head(5)
        for val, cnt in top.items():
            log.info(f"      {val}: {cnt:,}")

    if not dfs:
        log.warning("  No entities extracted!")

    log.info("\n" + "=" * 70)
    log.info("  EXTRACTION COMPLETE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
