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

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from notes_extraction.base import BaseExtractor, EntityMatch

log = logging.getLogger(__name__)

MAX_CHUNK_CHARS = 6000
# Operative dictation is long; use a larger prefix when OPENAI is enabled (cost/latency tradeoff).
OP_NOTE_CHUNK_CHARS = 24_000
ROOT = Path(__file__).resolve().parent.parent
PROMPT_DIR = ROOT / "prompts"
_OP_NOTE_TYPES = frozenset({"op_note", "OPNOTE"})


class LLMExtractor(BaseExtractor):
    """Structured LLM extraction with evidence-span enforcement and lab-date priority."""

    entity_domain = "llm"

    def __init__(self, api_key_env: str = "OPENAI_API_KEY") -> None:
        self._api_key = os.getenv(api_key_env)
        if not self._api_key:
            log.warning(
                f"LLMExtractor: {api_key_env} not set — LLM extraction disabled. "
                "Set the env var to enable."
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

        try:
            return self._call_llm(
                note_row_id,
                research_id,
                note_type,
                chunk,
                note_date,
                operative=is_op,
            )
        except Exception as exc:
            log.error(f"LLM extraction failed for {note_row_id}: {exc}")
            return []

    # ── Prompt building ──────────────────────────────────────────────────────

    def _load_system_prompt(self, *, operative: bool = False) -> str:
        """Load system prompt: operative dictation vs general / lab-aware default."""
        if operative:
            op_path = PROMPT_DIR / "operative_note_extraction_v1.txt"
            if op_path.exists():
                return op_path.read_text(encoding="utf-8")
        prompt_path = PROMPT_DIR / "lab_date_extraction_v1.txt"
        if prompt_path.exists() and not operative:
            return prompt_path.read_text(encoding="utf-8")
        return _DEFAULT_SYSTEM_PROMPT

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

    def _call_llm(
        self,
        note_row_id: str,
        research_id: int,
        note_type: str,
        text: str,
        note_date: str | None = None,
        *,
        operative: bool = False,
    ) -> list[EntityMatch]:
        """Call the OpenAI API and parse structured JSON output."""
        try:
            import openai
        except ImportError:
            log.error("openai package not installed. Run: pip install openai")
            return []

        client = openai.OpenAI(api_key=self._api_key)
        messages = self._build_prompt(note_type, text, note_date, operative=operative)
        max_out = 4096 if operative else 2000

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                temperature=0,
                response_format={"type": "json_object"},
                max_tokens=max_out,
            )
        except Exception as exc:
            log.error(f"OpenAI API call failed: {exc}")
            return []

        raw_json = response.choices[0].message.content or "{}"
        return self._parse_llm_response(
            raw_json,
            note_row_id,
            research_id,
            note_type,
            text,
            note_date,
            llm_operative=operative,
        )

    # ── Response parsing ─────────────────────────────────────────────────────

    def _parse_llm_response(
        self,
        raw_json: str,
        note_row_id: str,
        research_id: int,
        note_type: str,
        source_text: str,
        note_date: str | None,
        *,
        llm_operative: bool = False,
    ) -> list[EntityMatch]:
        """Parse LLM JSON output into EntityMatch objects."""
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            log.warning(f"LLM returned invalid JSON for {note_row_id}: {exc}")
            return []

        entities = data.get("entities", [])
        if not isinstance(entities, list):
            log.warning(f"LLM response missing 'entities' list for {note_row_id}")
            return []

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

            # Validate evidence span
            if evidence_text and not self._validate_evidence_span(evidence_text, source_text):
                log.debug(
                    f"Evidence span not found in source text for {note_row_id}; truncating"
                )
                evidence_text = evidence_text[:120]

            # Resolve character offsets from evidence span
            ev_start = source_text.find(evidence_text) if evidence_text else 0
            ev_end = ev_start + len(evidence_text) if ev_start >= 0 else 0
            if ev_start < 0:
                ev_start = 0
                ev_end = 0

            # Validate and normalise date
            entity_date: str | None = None
            if raw_date:
                try:
                    dt = datetime.strptime(str(raw_date), "%Y-%m-%d")
                    if 1990 <= dt.year <= 2030:
                        entity_date = raw_date
                except ValueError:
                    pass

            ext_method = "llm_gpt4o_mini_operative" if llm_operative else "llm_gpt4o_mini"
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
                evidence_start=ev_start,
                evidence_end=ev_end,
                entity_date=entity_date,
                note_date=note_date,
                extraction_method=ext_method,
                extracted_at=datetime.now(timezone.utc).isoformat(),
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
