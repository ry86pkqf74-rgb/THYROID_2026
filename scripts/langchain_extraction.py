#!/usr/bin/env python3
"""
langchain_extraction.py -- LangChain-powered LLM extraction with Pydantic schema
enforcement, LangSmith tracing, and batch processing for THYROID_2026.

Replaces/supplements run_extraction_split.py with:
  * Pydantic models enforcing the exact 9-column entity schema
  * LangChain LCEL chains with structured output parsing
  * LangSmith tracing for observability (optional, auto-detected)
  * Async batch processing with configurable concurrency
  * Per-note JSONL checkpointing (crash-safe, same format as original)
  * Docker-ready (all config via env vars)

Usage:
  # Standard (uses env vars for config):
  python scripts/langchain_extraction.py --domains imaging pathology

  # Override model/endpoint:
  python scripts/langchain_extraction.py --url http://localhost:11434 --model qwen3:14b --domains labs

  # With LangSmith tracing (set env vars):
  LANGCHAIN_TRACING_V2=true LANGCHAIN_API_KEY=ls__... python scripts/langchain_extraction.py

  # Docker:
  docker compose run thyroid-extractor --domains imaging pathology labs

Pip: langchain-core langchain-ollama pandas pyarrow pydantic tenacity
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Path resolution (same as run_extraction_split.py for compatibility)
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

PROCESSED_REMAINING = ROOT / "processed" / "remaining"
PROMPTS_DIR = ROOT / "notes_extraction_new" / "prompts"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(processName)s] %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("langchain_extraction")


# ---------------------------------------------------------------------------
# Pydantic schema -- enforces the exact entity structure across all domains
# ---------------------------------------------------------------------------
class ExtractedEntity(BaseModel):
    """Single clinical entity extracted from a note."""
    entity_type: str = Field(description="Domain-specific entity type (e.g. ultrasound_thyroid, staging_ajcc)")
    entity_value: str = Field(description="Extracted value or description")
    entity_date: Optional[str] = Field(default=None, description="ISO date (YYYY-MM-DD) or null")
    date_confidence: float = Field(default=0.0, ge=0.0, le=1.0,
                                   description="1.0=explicit date, 0.85=same-day, 0.7=relative, 0.0=unknown")
    date_source_keyword: Optional[str] = Field(default=None,
                                               description="The keyword or phrase that anchored the date")
    present_or_negated: str = Field(default="present", description="'present' or 'negated'")
    confidence: float = Field(default=0.8, ge=0.0, le=1.0, description="Extraction confidence")
    evidence_text: str = Field(default="", description="Supporting text snippet from the note")
    source_line: Optional[int] = Field(default=None, description="Approximate line number in note")

    @field_validator("present_or_negated")
    @classmethod
    def validate_assertion(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in ("present", "negated"):
            return "present"
        return v


class ExtractionResult(BaseModel):
    """Container for all entities extracted from a single note."""
    entities: list[ExtractedEntity] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Domain -> prompt file mapping (identical to run_extraction_split.py)
# ---------------------------------------------------------------------------
DOMAIN_PROMPT = {
    "complications":  "complications_extraction_v1.txt",
    "staging":        "staging_extraction_v1.txt",
    "genetics":       "genetics_extraction_v1.txt",
    "recurrence":     "recurrence_extraction_v1.txt",
    "medications":    "medications_extraction_v1.txt",
    "procedures":     "procedures_extraction_v1.txt",
    "problem_list":   "problem_list_extraction_v1.txt",
    "imaging":        "imaging_extraction_v1.txt",
    "pathology":      "pathology_extraction_v1.txt",
    "labs":           "labs_extraction_v1.txt",
    "physical_exam":  "physical_exam_extraction_v1.txt",
    "rad_treatment":  "rad_treatment_extraction_v1.txt",
    "past_medical_hx": "past_medical_hx_extraction_v1.txt",
    "past_surgical_hx": "past_surgical_hx_extraction_v1.txt",
    "operative_details": "operative_details_extraction_v1.txt",
    "presenting_symptoms": "presenting_symptoms_extraction_v1.txt",
    "dynamic_risk_response": "dynamic_risk_response_extraction_v1.txt",
    "survival_followup": "survival_followup_extraction_v1.txt",
    "vascular_invasion": "vascular_invasion_extraction_v1.txt",
    "rai_detailed": "rai_detailed_extraction_v1.txt",
    "recurrence_detailed": "recurrence_detailed_extraction_v1.txt",
    "medication_management": "medication_management_extraction_v1.txt",
    "functional_outcomes": "functional_outcomes_extraction_v1.txt",
    "tg_kinetics": "tg_kinetics_extraction_v1.txt",
    "parathyroid_detail": "parathyroid_detail_extraction_v1.txt",
    "airway_invasion": "airway_invasion_extraction_v1.txt",
    "frozen_section_detail": "frozen_section_detail_extraction_v1.txt",
    "us_nodule_dynamics": "us_nodule_dynamics_extraction_v1.txt",
    "cervical_ln_detail": "cervical_ln_detail_extraction_v1.txt",
    "patient_decision_adherence": "patient_decision_adherence_extraction_v1.txt",
}

ALL_DOMAINS = list(DOMAIN_PROMPT.keys())


def _load_prompt(domain: str) -> str:
    fname = DOMAIN_PROMPT.get(domain)
    if not fname:
        return f"Extract {domain} entities from the following clinical note. Return JSON."
    path = PROMPTS_DIR / fname
    if path.exists():
        return path.read_text(encoding="utf-8")
    log.warning(f"Prompt file not found: {path} -- using fallback.")
    return f"Extract {domain} entities from the following clinical note. Return JSON."


# ---------------------------------------------------------------------------
# LangChain chain builder
# ---------------------------------------------------------------------------
def build_extraction_chain(
    base_url: str,
    model: str,
    domain: str,
    temperature: float = 0.0,
):
    """Build a LangChain LCEL chain with structured Pydantic output.

    Uses RunnableLambda to inject the system prompt safely (no template
    escaping issues with JSON examples in prompts).
    """
    from langchain_core.messages import SystemMessage, HumanMessage
    from langchain_core.output_parsers import JsonOutputParser
    from langchain_core.runnables import RunnableLambda

    # Try langchain-ollama first, fall back to langchain-community
    try:
        from langchain_ollama import ChatOllama
        llm = ChatOllama(
            base_url=base_url.replace("/v1", ""),  # Ollama native URL
            model=model,
            temperature=temperature,
            num_predict=2000,
            format="json",
        )
    except ImportError:
        from langchain_community.chat_models import ChatOllama as CommunityChatOllama
        llm = CommunityChatOllama(
            base_url=base_url.replace("/v1", ""),
            model=model,
            temperature=temperature,
            num_predict=2000,
            format="json",
        )

    system_prompt_text = _load_prompt(domain)
    parser = JsonOutputParser(pydantic_object=ExtractionResult)
    format_instructions = parser.get_format_instructions()

    # Build messages directly (avoids ChatPromptTemplate escaping issues
    # with { } in prompt JSON examples)
    def _build_messages(inputs: dict) -> list:
        return [
            SystemMessage(content=system_prompt_text + "\n\n" + format_instructions),
            HumanMessage(content=inputs["note_text"]),
        ]

    chain = RunnableLambda(_build_messages) | llm | parser

    return chain, parser


# ---------------------------------------------------------------------------
# Extraction with retries and Pydantic validation
# ---------------------------------------------------------------------------
def extract_single_note(
    chain,
    parser,
    note_text: str,
    domain: str,
    max_retries: int = 3,
) -> dict:
    """Extract entities from a single note with retry logic and schema validation."""
    import re

    for attempt in range(max_retries):
        try:
            result = chain.invoke({
                "note_text": note_text[:6000],
            })

            # Validate through Pydantic
            if isinstance(result, dict):
                validated = ExtractionResult(**result)
                return validated.model_dump()
            else:
                return {"entities": []}

        except Exception as exc:
            if attempt < max_retries - 1:
                log.debug(f"Retry {attempt+1}/{max_retries} for {domain}: {exc}")
                time.sleep(2 ** attempt)
            else:
                log.warning(f"All retries exhausted for {domain}: {exc}")
                return {"entities": [], "parse_error": True, "error": str(exc)[:200]}

    return {"entities": []}


# ---------------------------------------------------------------------------
# Domain extraction with JSONL checkpointing (compatible with original)
# ---------------------------------------------------------------------------
def extract_domain(
    notes_df: pd.DataFrame,
    domain: str,
    base_url: str,
    model: str,
    output_dir: Path,
    batch_size: int = 50,
) -> Path:
    """Run LangChain extraction for one domain with per-note checkpointing.

    Output format is identical to run_extraction_split.py for downstream
    compatibility.
    """
    ckpt_path = output_dir / f"note_entities_llm_{domain}.ckpt.jsonl"
    out_path = output_dir / f"note_entities_llm_{domain}.parquet"

    # -- Load existing checkpoint (resume support) -------------------------
    done_ids: set = set()
    existing_rows: list[dict] = []
    if ckpt_path.exists():
        with open(ckpt_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    existing_rows.append(row)
                    done_ids.add(str(row.get("note_row_id", "")))
                except json.JSONDecodeError:
                    pass
        if done_ids:
            log.info(f"  [{domain}] RESUME -- {len(done_ids):,} notes already done, "
                     f"{len(notes_df) - len(done_ids):,} remaining")

    remaining_df = notes_df[~notes_df["note_row_id"].astype(str).isin(done_ids)]
    n_total = len(notes_df)
    n_remaining = len(remaining_df)

    if n_remaining == 0:
        log.info(f"  [{domain}] already complete ({n_total:,} notes) -- rebuilding parquet")
        _flush_parquet(existing_rows, out_path, domain)
        return out_path

    log.info(f"  [{domain}] starting -- {n_remaining:,}/{n_total:,} notes | "
             f"model={model} | LangChain+Pydantic")

    # Build chain
    chain, parser = build_extraction_chain(base_url, model, domain)

    new_rows: list[dict] = []
    t0 = time.time()

    with open(ckpt_path, "a", encoding="utf-8") as ckpt_fh:
        for i, (_, note_row) in enumerate(remaining_df.iterrows()):
            n_done_so_far = len(done_ids) + i
            if i % batch_size == 0:
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed > 0 and i > 0 else 0
                log.info(f"  [{domain}] {n_done_so_far}/{n_total} notes done "
                         f"({100*n_done_so_far/n_total:.1f}%) "
                         f"[{rate:.1f} notes/min]")

            note_text = str(note_row.get("note_text", ""))
            result = extract_single_note(chain, parser, note_text, domain)

            row = {
                "note_row_id":  str(note_row.get("note_row_id", "")),
                "research_id":  str(note_row.get("research_id", "")),
                "note_type":    str(note_row.get("note_type", "")),
                "note_date":    str(note_row.get("note_date", "") or ""),
                "domain":       domain,
                "llm_model":    model,
                "llm_base_url": base_url,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "result_json":  json.dumps(result),
                "pipeline":     "langchain",  # Tag for provenance
            }
            # -- Flush each row immediately -- crash-safe ------------------
            ckpt_fh.write(json.dumps(row) + "\n")
            ckpt_fh.flush()
            new_rows.append(row)

    # -- Rebuild parquet from full checkpoint ------------------------------
    all_rows = existing_rows + new_rows
    _flush_parquet(all_rows, out_path, domain)
    log.info(f"  [{domain}] DONE -- {len(all_rows):,} rows -> {out_path.name}")
    return out_path


def _flush_parquet(rows: list[dict], out_path: Path, domain: str) -> None:
    if not rows:
        log.warning(f"  [{domain}] no rows to write")
        return
    tmp = out_path.with_suffix(".tmp.parquet")
    pd.DataFrame(rows).to_parquet(tmp, index=False)
    tmp.replace(out_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="LangChain LLM extraction with Pydantic schema enforcement")

    parser.add_argument("--url", default=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1"),
                        help="Ollama base URL")
    parser.add_argument("--model", default=os.getenv("OLLAMA_MODEL", "qwen3:14b"),
                        help="Model name")
    parser.add_argument("--domains", nargs="+", default=None,
                        help="Domains to extract (default: all)")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="Output directory for checkpoints and parquet")
    parser.add_argument("--input-parquet", type=Path, default=None,
                        help="Input parquet file")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Logging batch size")

    args = parser.parse_args()

    # Resolve paths
    output_dir = args.output_dir or PROCESSED_REMAINING
    input_parquet = args.input_parquet or (PROCESSED_REMAINING / "clinical_notes_long.parquet")
    output_dir.mkdir(parents=True, exist_ok=True)

    domains = args.domains or ALL_DOMAINS

    # Banner
    log.info("=" * 70)
    log.info("  LLM EXTRACTION  (LangChain + Pydantic)")
    log.info("=" * 70)
    log.info(f"  Input: {input_parquet.name}")
    log.info(f"  Model: {args.model} @ {args.url}")
    log.info(f"  Domains: {domains}")
    log.info(f"  Output: {output_dir}")

    # LangSmith tracing detection
    if os.getenv("LANGCHAIN_TRACING_V2", "").lower() == "true":
        log.info(f"  LangSmith: ENABLED (project={os.getenv('LANGCHAIN_PROJECT', 'default')})")
    else:
        log.info("  LangSmith: disabled (set LANGCHAIN_TRACING_V2=true to enable)")

    log.info("=" * 70)

    # Load data
    notes_df = pd.read_parquet(input_parquet)
    log.info(f"  Loaded {len(notes_df):,} notes from {input_parquet.name}")

    # Process each domain
    for domain in domains:
        if domain not in DOMAIN_PROMPT:
            log.warning(f"  Unknown domain: {domain} -- skipping")
            continue
        log.info(f"\n{'='*60}\n  DOMAIN: {domain}\n{'='*60}")
        extract_domain(
            notes_df=notes_df,
            domain=domain,
            base_url=args.url,
            model=args.model,
            output_dir=output_dir,
            batch_size=args.batch_size,
        )

    log.info("\n" + "=" * 70)
    log.info("  ALL REQUESTED DOMAINS COMPLETE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
