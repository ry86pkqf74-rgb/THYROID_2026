"""
Optional LLM-based entity extractor.

Gated behind the OPENAI_API_KEY environment variable.
If the key is not set, all methods return empty results with a warning.

Output schema (JSON per entity):
  {
    "entity_type":   "complication|staging|genetics|procedure|medication|problem",
    "entity_value":  "<raw text from note>",
    "entity_date":   "YYYY-MM-DD or null",
    "date_confidence": 0.0-1.0,  // 1.0=explicit lab date, 0.7=note body, 0.0=none
    "present_or_negated": "present|negated",
    "confidence": 0.0-1.0,
    "evidence_text": "<exact substring from source note>",
    "source_line": <integer line number in note, 1-based>
  }

Lab date precedence rule enforced in prompts:
  specimen_collect_dt / "collected on" / "drawn on" / "specimen date" / "result date"
  ALWAYS take precedence over note encounter date.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from threading import local
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from llm_extraction.base import BaseExtractor, EntityMatch
from llm_extraction.run_telemetry import RunTelemetryContext

log = logging.getLogger(__name__)

VERIFIER_NAME = "evidence_substring_verifier"
VERIFIER_VERSION = "1.0"

MAX_CHUNK_CHARS = 6000
# Operative dictation is long; use a larger prefix when OPENAI is enabled (cost/latency tradeoff).
OP_NOTE_CHUNK_CHARS = 24_000
ROOT = Path(__file__).resolve().parent.parent
PROMPT_DIR = ROOT / "prompts"
_OP_NOTE_TYPES = frozenset({"op_note", "OPNOTE"})


# GitHub Models endpoint (free tier, same openai SDK)
_GITHUB_MODELS_BASE_URL = "https://models.github.ai/inference/"
# Model IDs: GitHub Models uses "openai/gpt-4o-mini" format
_GITHUB_MODEL_ID = "openai/gpt-4o-mini"
_OPENAI_MODEL_ID = "gpt-4o-mini"


class LLMExtractor(BaseExtractor):
    """Structured LLM extraction with evidence-span enforcement and lab-date priority.

    Provider priority:
      1. GITHUB_TOKEN  → GitHub Models (free, rate-limited)
      2. OPENAI_API_KEY → OpenAI API (paid)
    """

    entity_domain = "llm"

    def __init__(self, telemetry: RunTelemetryContext | None = None) -> None:
        self._telemetry = telemetry
        self._api_key: str | None = None
        self._base_url: str | None = None
        self._model_id: str = _OPENAI_MODEL_ID
        self._provider: str = "none"
        self._thread_local = local()

        # 1. Prefer GitHub Models (free)
        gh_token = os.getenv("GITHUB_TOKEN")
        if gh_token:
            self._api_key = gh_token
            self._base_url = _GITHUB_MODELS_BASE_URL
            self._model_id = _GITHUB_MODEL_ID
            self._provider = "github_models"
            log.info("LLMExtractor: using GitHub Models (free tier) — model %s", self._model_id)
        else:
            # 2. Fall back to OpenAI API
            oai_key = os.getenv("OPENAI_API_KEY")
            if oai_key:
                self._api_key = oai_key
                self._base_url = None  # default openai endpoint
                self._model_id = _OPENAI_MODEL_ID
                self._provider = "openai"
                log.info("LLMExtractor: using OpenAI API — model %s", self._model_id)
            else:
                log.warning(
                    "LLMExtractor: neither GITHUB_TOKEN nor OPENAI_API_KEY set — "
                    "LLM extraction disabled. Set one to enable."
                )

    @property
    def available(self) -> bool:
        return self._api_key is not None

    def extract(
        self,
        note_row_id: str,
        research_id: int,
        note_type: str,
        note_text: str,
        note_date: str | None = None,
    ) -> list[EntityMatch]:
        if not self.available:
            return []

        is_op = note_type in _OP_NOTE_TYPES
        limit = OP_NOTE_CHUNK_CHARS if is_op else MAX_CHUNK_CHARS
        chunk = note_text[:limit]
        chunk_char_start = 0
        chunk_char_end = len(chunk)

        try:
            return self._call_llm(
                note_row_id,
                research_id,
                note_type,
                chunk,
                note_date,
                full_note_text=note_text,
                chunk_char_start=chunk_char_start,
                chunk_char_end=chunk_char_end,
                chunk_index=0,
                operative=is_op,
            )
        except Exception as exc:
            log.error(
                "LLM extraction failed for %s (research_id=%s): %s — returning no entities",
                note_row_id,
                research_id,
                exc,
            )
            if self._telemetry:
                self._telemetry.record_api_failure()
            return []

    # ── Prompt building ──────────────────────────────────────────────────────

    def _load_system_prompt(
        self, *, operative: bool = False, domain: str | None = None
    ) -> str:
        """Load system prompt: domain-specific → operative → general fallback.

        When *domain* is provided, the registry is consulted for a matching prompt
        file.  If the file exists it is used; otherwise the operative / general
        fallback chain runs as before.
        """
        if domain:
            try:
                from llm_extraction.registry import load_registry

                spec = load_registry().prompt_for_domain(domain)
                if spec and spec.absolute_path.exists():
                    return spec.absolute_path.read_text(encoding="utf-8")
            except Exception:
                pass
        if operative:
            op_path = PROMPT_DIR / "operative_note_extraction_v1.txt"
            if op_path.exists():
                return op_path.read_text(encoding="utf-8")
        prompt_path = PROMPT_DIR / "lab_date_extraction_v1.txt"
        if prompt_path.exists() and not operative:
            return prompt_path.read_text(encoding="utf-8")
        return _DEFAULT_SYSTEM_PROMPT

    def _prompt_version(self, *, operative: bool, domain: str | None = None) -> str:
        if domain:
            try:
                from llm_extraction.registry import load_registry

                spec = load_registry().prompt_for_domain(domain)
                if spec and spec.absolute_path.exists():
                    digest = hashlib.sha256(
                        spec.absolute_path.read_bytes()
                    ).hexdigest()[:12]
                    return f"{spec.repo_path}|{digest}"
            except Exception:
                pass
        rel = (
            "operative_note_extraction_v1.txt" if operative else "lab_date_extraction_v1.txt"
        )
        path = PROMPT_DIR / rel
        if path.exists():
            digest = hashlib.sha256(path.read_bytes()).hexdigest()[:12]
            return f"{rel}|{digest}"
        return "embedded_fallback|0"

    def _build_prompt(
        self,
        note_type: str,
        note_text: str,
        note_date: str | None,
        *,
        operative: bool = False,
    ) -> list[dict]:
        """Build the OpenAI messages list for entity + date extraction."""
        system = self._load_system_prompt(operative=operative)
        if operative:
            user_content = (
                f"NOTE TYPE: {note_type} (operative dictation)\n"
                f"ANCHOR ENCOUNTER DATE (clinical_notes_long.note_date): {note_date or 'unknown'}\n"
                "Use this only when no explicit operative/procedure date exists in the excerpt.\n\n"
                f"OPERATIVE NOTE TEXT:\n{note_text}"
            )
        else:
            user_content = (
                f"NOTE TYPE: {note_type}\n"
                f"ENCOUNTER DATE (note_date): {note_date or 'unknown'}\n\n"
                "IMPORTANT: Lab-specific dates (collected on, drawn on, specimen date, "
                "result date) ALWAYS take precedence over the encounter date above.\n\n"
                f"NOTE TEXT:\n{note_text}"
            )
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user_content},
        ]

    # ── LLM API call ─────────────────────────────────────────────────────────

    def _get_client(self):
        client = getattr(self._thread_local, "client", None)
        if client is not None:
            return client

        import openai

        client_kwargs: dict = {"api_key": self._api_key, "max_retries": 0}
        if self._base_url:
            client_kwargs["base_url"] = self._base_url
        client = openai.OpenAI(**client_kwargs)
        self._thread_local.client = client
        return client

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
    ) -> list[EntityMatch]:
        """Call the OpenAI API and parse structured JSON output."""
        try:
            client = self._get_client()
        except ImportError:
            log.error("openai package not installed. Run: pip install openai")
            if self._telemetry:
                self._telemetry.record_api_failure()
            return []

        messages = cast(Any, self._build_prompt(note_type, text, note_date, operative=operative))
        max_out = 4096 if operative else 2000

        try:
            response = None
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    response = client.chat.completions.create(
                        model=self._model_id,
                        messages=messages,
                        temperature=0,
                        response_format={"type": "json_object"},
                        max_tokens=max_out,
                    )
                    break
                except Exception as retry_exc:
                    err_str = str(retry_exc).lower()
                    status_code = getattr(retry_exc, "status_code", None)
                    if status_code == 429 or "rate" in err_str or "429" in err_str or "limit" in err_str:
                        if self._telemetry:
                            self._telemetry.record_retry(1)
                        wait = min(60, 5 * (2**attempt))
                        log.warning(
                            f"Rate limited (attempt {attempt+1}/{max_retries}), "
                            f"waiting {wait}s ..."
                        )
                        time.sleep(wait)
                    else:
                        raise retry_exc
            if response is None:
                log.error(f"LLM API exhausted {max_retries} retries for {note_row_id}")
                if self._telemetry:
                    self._telemetry.record_api_failure()
                return []
        except Exception as exc:
            log.error(f"LLM API call failed ({self._provider}): {exc}")
            if self._telemetry:
                self._telemetry.record_api_failure()
            return []

        raw_json = response.choices[0].message.content or "{}"
        return self._parse_llm_response(
            raw_json,
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

    # ── Response parsing ─────────────────────────────────────────────────────

    def _parse_llm_response(
        self,
        raw_json: str,
        note_row_id: str,
        research_id: int,
        note_type: str,
        *,
        chunk_text: str,
        full_note_text: str,
        note_date: str | None,
        chunk_char_start: int,
        chunk_char_end: int,
        chunk_index: int,
        llm_operative: bool = False,
    ) -> list[EntityMatch]:
        """Parse LLM JSON output into EntityMatch objects."""
        response_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            log.warning(
                "LLM returned invalid JSON for %s (research_id=%s): %s",
                note_row_id,
                research_id,
                exc,
            )
            if self._telemetry:
                self._telemetry.record_parse_failure()
            return []

        entities = data.get("entities", [])
        if not isinstance(entities, list):
            log.warning(
                "LLM response missing 'entities' list for %s (research_id=%s)",
                note_row_id,
                research_id,
            )
            if self._telemetry:
                self._telemetry.record_parse_failure()
            return []

        prompt_ver = self._prompt_version(operative=llm_operative)
        results: list[EntityMatch] = []
        for item in entities:
            if not isinstance(item, dict):
                continue

            evidence_text = str(item.get("evidence_text", ""))
            entity_value = str(item.get("entity_value", ""))
            entity_type = str(item.get("entity_type", "unknown"))
            present_or_negated = str(item.get("present_or_negated", "present"))
            confidence = float(item.get("confidence", 0.8))
            raw_date = item.get("entity_date")
            date_confidence = float(item.get("date_confidence", 0.0))
            try:
                sl = item.get("source_line")
                source_line = int(sl) if sl is not None and str(sl).strip() != "" else None
            except (TypeError, ValueError):
                source_line = None

            g_start = full_note_text.find(evidence_text) if evidence_text else -1
            if evidence_text and g_start < 0:
                log.debug(
                    "Evidence not found in full note for %s; marking rejected",
                    note_row_id,
                )
                verification_status = "rejected"
                verification_step = "substring_check"
                evidence_text = evidence_text[:500]
                g_start, g_end = 0, 0
                c_start, c_end = 0, 0
            elif evidence_text and g_start >= 0:
                verification_status = "verified_substring"
                verification_step = "substring_ok"
                g_end = g_start + len(evidence_text)
                c_start = (
                    g_start - chunk_char_start
                    if chunk_char_start <= g_start < chunk_char_end
                    else chunk_text.find(evidence_text)
                )
                c_end = c_start + len(evidence_text) if c_start >= 0 else 0
                if c_start < 0:
                    c_start, c_end = 0, 0
            else:
                verification_status = "unverified"
                verification_step = "no_evidence_text"
                g_start, g_end, c_start, c_end = 0, 0, 0, 0

            if verification_step in ("substring_ok", "substring_check"):
                v_name, v_ver = VERIFIER_NAME, VERIFIER_VERSION
            else:
                v_name, v_ver = None, None

            # Validate and normalise date
            entity_date: str | None = None
            if raw_date:
                try:
                    dt = datetime.strptime(str(raw_date), "%Y-%m-%d")
                    if 1990 <= dt.year <= 2030:
                        entity_date = raw_date
                except ValueError:
                    pass

            provider_tag = self._provider  # "github_models" or "openai"
            ext_method = f"llm_{provider_tag}_operative" if llm_operative else f"llm_{provider_tag}"
            match = EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type=entity_type,
                entity_value_raw=entity_value,
                entity_value_norm=entity_value.lower().strip(),
                present_or_negated=present_or_negated,
                confidence=confidence,
                evidence_span=evidence_text[:500],
                evidence_start=c_start if c_start >= 0 else 0,
                evidence_end=c_end if c_end >= 0 else 0,
                entity_date=entity_date,
                note_date=note_date,
                extraction_method=ext_method,
                extracted_at=datetime.now(timezone.utc).isoformat(),
                date_confidence=date_confidence,
                source_line=source_line,
                chunk_index=chunk_index,
                chunk_char_start=chunk_char_start,
                chunk_char_end=chunk_char_end,
                evidence_global_start=g_start,
                evidence_global_end=g_end,
                raw_response_sha256=response_hash,
                verification_status=verification_status,
                verification_step=verification_step,
                extractor_name="LLMExtractor",
                model_name=self._model_id,
                model_version=self._model_id,
                prompt_version=prompt_ver,
                verifier_name=v_name,
                verifier_version=v_ver,
            )
            results.append(match)

        log.debug(
            f"LLM extracted {len(results)} entities from {note_row_id} (note_type={note_type})"
        )
        return results

    @staticmethod
    def _validate_evidence_span(span: str, full_text: str) -> bool:
        """Evidence span must be an exact substring of the source text."""
        return span in full_text


# ── Default system prompt (fallback if prompt file missing) ──────────────────

_DEFAULT_SYSTEM_PROMPT = """You are a clinical NLP system extracting structured medical entities
from thyroid surgery clinical notes.

CRITICAL DATE RULE:
Lab-specific dates ALWAYS take precedence over the note encounter date.
Look for these patterns near any lab value:
- "collected on <date>"
- "drawn on <date>"
- "specimen date: <date>"
- "result date: <date>"
- "received: <date>"
- "reported on <date>"
When found, use that date as entity_date with date_confidence=1.0.
When only an encounter/note date is available, use date_confidence=0.0.

ENTITY TYPES TO EXTRACT:
- complication: RLN injury, hypocalcemia, chyle leak, hematoma, seroma, hypoparathyroidism
- staging: T/N/M stage, overall AJCC stage
- genetics: BRAF, TERT, NTRK, RAS, RET mutations
- procedure: thyroidectomy, neck dissection, RAI
- medication: levothyroxine dose (mcg), RAI dose (mCi)
- problem: comorbidities (diabetes, hypertension, etc.)

OUTPUT FORMAT (strict JSON):
{
  "entities": [
    {
      "entity_type": "<type>",
      "entity_value": "<exact value from text>",
      "entity_date": "YYYY-MM-DD or null",
      "date_confidence": 0.0,
      "present_or_negated": "present or negated",
      "confidence": 0.9,
      "evidence_text": "<exact substring from note>",
      "source_line": 1
    }
  ]
}

RULES:
1. entity_date must be YYYY-MM-DD format or null.
2. evidence_text MUST be an exact substring of the input note.
3. date_confidence: 1.0=explicit lab-collection date, 0.7=note body date, 0.0=no date.
4. Return empty entities array if nothing found; never hallucinate entities.
5. For negated entities (e.g., "no hypocalcemia"), set present_or_negated="negated".
"""
