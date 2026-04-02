#!/usr/bin/env python3
"""
run_extraction_local.py — Local OpenAI-compatible vLLM wrapper for the shared
entity extraction pipeline.

This wrapper preserves the standard llm_extraction CLI while adding:
  - Pydantic validation for structured JSON output
  - a deterministic row contract for local vLLM parquet outputs
  - 50-note batching for qwen-30b-class local runs
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import llm_extraction.extract_llm as _ell  # noqa: E402

VLLM_BATCH_SIZE = int(os.environ.get("VLLM_BATCH_SIZE", "50"))
VLLM_OUTPUT_MODEL_NAME = os.environ.get("VLLM_OUTPUT_MODEL_NAME", "qwen-30b-instruct-vLLM")
CONTRACT_PROMPT_PATH = ROOT / "prompts" / "vllm_output_contract_v1.txt"


def _normalize_iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        year = value.get("year")
        month = value.get("month")
        day = value.get("day")
        if year is not None and month is not None and day is not None:
            try:
                return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
            except (TypeError, ValueError):
                return None
        for key in ("date", "entity_date", "value", "raw", "text"):
            normalized = _normalize_iso_date(value.get(key))
            if normalized:
                return normalized
        return None
    if isinstance(value, (list, tuple, set)):
        for item in value:
            normalized = _normalize_iso_date(item)
            if normalized:
                return normalized
        return None

    text = str(value).strip()
    if not text or text.lower() in {"null", "none", "nat"}:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


class VLLMEntityPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    entity_type: str = ""
    entity_value: str = ""
    entity_date: str | None = None
    date_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    date_source_keyword: str | None = None
    present_or_negated: str = "present"
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    confidence_score: float | None = Field(default=None, ge=0.0, le=1.0)
    evidence_text: str = ""
    evidence_span: str | None = None
    source_line: int | None = None
    research_id: int | None = None
    episode_id: str | None = None
    note_row_id: str | None = None
    extraction_timestamp_utc: str | None = None
    llm_model: str | None = None
    llm_prompt_version: str | None = None
    verification_status: str = "pending"

    @field_validator("entity_date", mode="before")
    @classmethod
    def _coerce_entity_date(cls, value: Any) -> str | None:
        return _normalize_iso_date(value)

    @field_validator("present_or_negated", mode="before")
    @classmethod
    def _normalize_negation(cls, value: Any) -> str:
        normalized = str(value or "present").strip().lower()
        return normalized if normalized in {"present", "negated"} else "present"

    @model_validator(mode="after")
    def _sync_alias_fields(self) -> "VLLMEntityPayload":
        confidence = self.confidence if self.confidence is not None else self.confidence_score
        if confidence is None:
            confidence = 0.0
        self.confidence = confidence
        self.confidence_score = confidence
        evidence = self.evidence_text or self.evidence_span or ""
        self.evidence_text = evidence
        self.evidence_span = evidence
        self.verification_status = "pending"
        return self


class VLLMResponsePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    entities: list[VLLMEntityPayload] = Field(default_factory=list)


class VLLMLocalExtractor(_ell.LLMExtractor):
    """LLMExtractor wired to a local vLLM OpenAI-compatible endpoint."""

    force_pending_verification = True

    def __init__(self, telemetry=None) -> None:
        self._vllm_base = os.environ.get(
            "VLLM_OPENAI_BASE_URL", "http://localhost:8000/v1"
        ).rstrip("/")
        self._vllm_key = os.environ.get("VLLM_API_KEY", "EMPTY")
        self._served_model_name = os.environ.get("VLLM_MODEL", VLLM_OUTPUT_MODEL_NAME)
        super().__init__(telemetry=telemetry)
        self._api_key = self._vllm_key
        self._base_url = self._vllm_base
        self._model_id = VLLM_OUTPUT_MODEL_NAME
        self._provider = "vllm_local"

    @property
    def available(self) -> bool:
        return bool(self._served_model_name)

    def _load_contract_prompt(self) -> str:
        if CONTRACT_PROMPT_PATH.exists():
            return CONTRACT_PROMPT_PATH.read_text(encoding="utf-8").strip()
        return (
            "Final parquet rows must include research_id, episode_id, note_row_id, evidence_span, "
            "extraction_timestamp_utc, llm_model, llm_prompt_version, confidence_score, "
            "and verification_status='pending'."
        )

    def _prompt_version(self, *, operative: bool) -> str:
        base_version = super()._prompt_version(operative=operative)
        if CONTRACT_PROMPT_PATH.exists():
            digest = hashlib.sha256(CONTRACT_PROMPT_PATH.read_bytes()).hexdigest()[:12]
            contract_version = f"{CONTRACT_PROMPT_PATH.name}|{digest}"
        else:
            contract_version = "vllm_output_contract_v1.txt|missing"
        return f"{base_version}||{contract_version}"

    def _build_prompt(
        self,
        note_type: str,
        note_text: str,
        note_date: str | None,
        *,
        operative: bool = False,
    ) -> list[dict]:
        messages = super()._build_prompt(
            note_type,
            note_text,
            note_date,
            operative=operative,
        )
        messages[0]["content"] = f"{self._load_contract_prompt()}\n\n{messages[0]['content']}"
        return messages

    def _validate_payload_json(self, raw_json: str) -> str:
        try:
            payload = VLLMResponsePayload.model_validate_json(raw_json)
        except ValidationError:
            payload = VLLMResponsePayload.model_validate(json.loads(raw_json or "{}"))
        return payload.model_dump_json()

    def _call_llm(
        self,
        note_row_id: str,
        research_id: int,
        note_type: str,
        text: str,
        note_date: str | None = None,
        *,
        full_note_text: str,
        chunk_char_start: int,
        chunk_char_end: int,
        chunk_index: int = 0,
        operative: bool = False,
    ):
        try:
            import openai
        except ImportError:
            _ell.log.error("openai package not installed. Run: pip install openai")
            return []

        if not self._served_model_name:
            return []

        client = openai.OpenAI(base_url=self._vllm_base, api_key=self._vllm_key)
        messages = self._build_prompt(note_type, text, note_date, operative=operative)

        try:
            response = client.chat.completions.create(
                model=self._served_model_name,
                messages=messages,
                temperature=0,
                response_format={"type": "json_object"},
                max_tokens=2000,
            )
        except Exception as exc:
            _ell.log.error("vLLM API call failed: %s", exc)
            return []

        raw_json = response.choices[0].message.content or "{}"
        try:
            validated_json = self._validate_payload_json(raw_json)
        except (ValidationError, json.JSONDecodeError) as exc:
            _ell.log.error("vLLM structured payload validation failed for %s: %s", note_row_id, exc)
            return []

        matches = self._parse_llm_response(
            validated_json,
            note_row_id,
            research_id,
            note_type,
            chunk_text=text,
            full_note_text=full_note_text,
            note_date=note_date,
            chunk_char_start=chunk_char_start,
            chunk_char_end=chunk_char_end,
            chunk_index=chunk_index,
            llm_operative=operative,
        )
        prompt_version = self._prompt_version(operative=operative)
        for match in matches:
            match.verification_status = "pending"
            match.verification_step = "awaiting_review"
            match.model_name = VLLM_OUTPUT_MODEL_NAME
            match.model_version = self._served_model_name
            match.prompt_version = prompt_version
        return matches


_ell.LLMExtractor = VLLMLocalExtractor  # type: ignore[misc, assignment]

import llm_extraction.run_extraction as _run  # noqa: E402

_ORIGINAL_RUN_EXTRACTORS = _run.run_extractors


def _merge_domain_results(target: dict[str, list[dict]], batch: dict[str, list[dict]]) -> None:
    for domain, records in batch.items():
        target.setdefault(domain, []).extend(records)


def _batched_run_extractors(
    notes_df,
    extractors,
    max_workers: int = 1,
    *,
    extraction_run_id: str,
):
    llm_enabled = any(getattr(ext, "entity_domain", None) == "llm" for ext in extractors)
    if not llm_enabled or len(notes_df) <= VLLM_BATCH_SIZE:
        return _ORIGINAL_RUN_EXTRACTORS(
            notes_df,
            extractors,
            max_workers=max_workers,
            extraction_run_id=extraction_run_id,
        )

    domain_results: dict[str, list[dict]] = {}
    total_batches = (len(notes_df) + VLLM_BATCH_SIZE - 1) // VLLM_BATCH_SIZE
    for batch_index, start in enumerate(range(0, len(notes_df), VLLM_BATCH_SIZE), start=1):
        stop = min(start + VLLM_BATCH_SIZE, len(notes_df))
        batch_df = notes_df.iloc[start:stop].reset_index(drop=True)
        logging.info(
            "  vLLM batch %s/%s -- notes %s-%s of %s",
            batch_index,
            total_batches,
            start + 1,
            stop,
            len(notes_df),
        )
        batch_results = _ORIGINAL_RUN_EXTRACTORS(
            batch_df,
            extractors,
            max_workers=max_workers,
            extraction_run_id=extraction_run_id,
        )
        _merge_domain_results(domain_results, batch_results)
    return domain_results


_run.run_extractors = _batched_run_extractors
main = _run.main

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

if __name__ == "__main__":
    main()
