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
"""

from __future__ import annotations

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


def run_extractors(
    notes_df: pd.DataFrame,
    extractors: list[BaseExtractor],
) -> dict[str, list[dict]]:
    """Run all extractors across every note row, grouping results by domain."""
    domain_results: dict[str, list[dict]] = {}

    total = len(notes_df)
    for i, (_, row) in enumerate(notes_df.iterrows()):
        note_row_id = row["note_row_id"]
        research_id = int(row["research_id"])
        note_type = row["note_type"]
        note_text = str(row["note_text"])

        for ext in extractors:
            matches = ext.extract(note_row_id, research_id, note_type, note_text)
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


def main() -> None:
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

    extractors: list[BaseExtractor] = [cls() for cls in ALL_REGEX_EXTRACTORS]

    llm = LLMExtractor()
    if llm.available:
        extractors.append(llm)
        log.info("  LLM extractor enabled")
    else:
        log.info("  LLM extractor disabled (no API key)")

    domain_results = run_extractors(notes_df, extractors)

    dfs = results_to_dataframes(domain_results)

    log.info("\n  Results summary:")
    for domain, df in dfs.items():
        out_path = PROCESSED / f"note_entities_{domain}.parquet"
        save_parquet(df, out_path)

        n_present = (df["present_or_negated"] == "present").sum()
        n_negated = (df["present_or_negated"] == "negated").sum()
        n_patients = df["research_id"].nunique()

        log.info(
            f"    {domain:20s}  {len(df):>6,} entities  "
            f"({n_present:,} present, {n_negated:,} negated)  "
            f"{n_patients:,} patients"
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
