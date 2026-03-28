#!/usr/bin/env python3
"""
run_extraction_local.py — Same CLI and outputs as notes_extraction/run_extraction.py
but uses an OpenAI-compatible vLLM server (default http://localhost:8000/v1).

extract_llm.py is unchanged; this module replaces notes_extraction.extract_llm.LLMExtractor
with a subclass before importing run_extraction.main.

Environment:
  VLLM_OPENAI_BASE_URL   default http://localhost:8000/v1
  VLLM_API_KEY           default EMPTY
  VLLM_MODEL             should match vLLM --served-model-name (empty disables LLM)

Usage (repo root):
  export VLLM_MODEL=your-model-id
  python notes_extraction_new/run_extraction_local.py
  python notes_extraction_new/run_extraction_local.py --target llm --research-ids ids.txt
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import notes_extraction.extract_llm as _ell  # noqa: E402


class VLLMLocalExtractor(_ell.LLMExtractor):
    """LLMExtractor wired to a local vLLM OpenAI-compatible endpoint."""

    def __init__(self) -> None:
        self._vllm_base = os.environ.get(
            "VLLM_OPENAI_BASE_URL", "http://localhost:8000/v1"
        ).rstrip("/")
        self._vllm_key = os.environ.get("VLLM_API_KEY", "EMPTY")
        self._vllm_model = os.environ.get("VLLM_MODEL", "")
        super().__init__(api_key_env="OPENAI_API_KEY")
        if not self._vllm_model:
            _ell.log.warning(
                "VLLM_MODEL is empty — LLM extraction disabled. "
                "Set VLLM_MODEL to your --served-model-name."
            )

    @property
    def available(self) -> bool:
        return bool(self._vllm_model)

    def _call_llm(
        self,
        note_row_id: str,
        research_id: int,
        note_type: str,
        text: str,
        note_date: str | None = None,
    ):
        try:
            import openai
        except ImportError:
            _ell.log.error("openai package not installed. Run: pip install openai")
            return []

        if not self._vllm_model:
            return []

        client = openai.OpenAI(base_url=self._vllm_base, api_key=self._vllm_key)
        messages = self._build_prompt(note_type, text, note_date)

        try:
            response = client.chat.completions.create(
                model=self._vllm_model,
                messages=messages,
                temperature=0,
                response_format={"type": "json_object"},
                max_tokens=2000,
            )
        except Exception as exc:
            _ell.log.error(f"vLLM API call failed: {exc}")
            return []

        raw_json = response.choices[0].message.content or "{}"
        return self._parse_llm_response(
            raw_json, note_row_id, research_id, note_type, text, note_date
        )


_ell.LLMExtractor = VLLMLocalExtractor  # type: ignore[misc, assignment]

from notes_extraction.run_extraction import main  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

if __name__ == "__main__":
    main()
