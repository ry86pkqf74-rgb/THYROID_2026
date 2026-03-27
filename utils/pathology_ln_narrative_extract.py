"""
Deterministic lymph-node burden extraction from pathology narrative text.

Used for audit/remediation when structured path_synoptics / tumor_pathology
columns are NULL but the report body states counts or explicit absence of nodal disease.

This module does NOT call LLMs. All patterns are regex with explicit capture groups.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# Positive patterns: numerator/denominator with lymph node context
_PATTERNS_POSITIVE: list[tuple[str, re.Pattern[str]]] = [
    (
        "x_of_y_ln_positive_classic",
        re.compile(
            r"\b(\d+)\s+(?:of|out\s+of|/)\s*(\d+)\s+"
            r"(?:lymph\s+)?nodes?\b"
            r".{0,60}?\b(?:positive|involved|contain(?:s|ing)?\s+(?:metasta|carcinoma|tumor|deposit))",
            re.I | re.DOTALL,
        ),
    ),
    (
        "metastatic_in_x_of_y_ln",
        re.compile(
            r"\b(?:metastatic\s+(?:carcinoma|disease)|carcinoma)\b"
            r".{0,40}?"
            r"\b(\d+)\s+(?:of|out\s+of)\s*(\d+)\s+(?:lymph\s+)?nodes?",
            re.I | re.DOTALL,
        ),
    ),
    (
        "involving_x_of_y_ln",
        re.compile(
            r"\b(?:involv\w+|present\w*)\b"
            r".{0,20}?"
            r"\b(\d+)\s+(?:of|out\s+of)\s*(\d+)\s+(?:lymph\s+)?nodes?",
            re.I,
        ),
    ),
]

# Fraction near "node" language (e.g. "2/12 lymph nodes")
_PATTERN_SLASH: list[tuple[str, re.Pattern[str]]] = [
    (
        "slash_ln",
        re.compile(
            r"\b(\d+)\s*/\s*(\d+)\s+(?:lymph\s+)?nodes?\b",
            re.I,
        ),
    ),
]

# Explicit zero / absence (sets ln_no_nodes_flag when appropriate)
_PATTERNS_NEGATION: list[tuple[str, re.Pattern[str]]] = [
    (
        "no_ln_identified_excised",
        re.compile(
            r"\bno\s+(?:lymph\s+nodes?|nodes?)\s+(?:identified|found|recovered|"
            r"submitted|present|included|seen)\b",
            re.I,
        ),
    ),
    (
        "no_nodal_metastasis",
        re.compile(
            r"\bno\s+(?:regional\s+)?(?:nodal\s+metastasis|lymph\s+node\s+metastasis|"
            r"positive\s+lymph\s+nodes?)\b",
            re.I,
        ),
    ),
    (
        "all_ln_negative",
        re.compile(
            r"\b(?:all|each)\s+(?:\d+\s+)?(?:lymph\s+)?nodes?\s+(?:are\s+)?(?:negative|benign|"
            r"free\s+of\s+(?:tumor|carcinoma|metasta))\b",
            re.I,
        ),
    ),
    (
        "zero_of_y_negative",
        re.compile(
            r"\b0\s+(?:of|out\s+of)\s*(\d+)\s+(?:lymph\s+)?nodes?\s+"
            r"(?:negative|without\s+metasta|free\s+of)",
            re.I,
        ),
    ),
]


@dataclass
class PathologyLNParseResult:
    ln_positive_path: int | None
    ln_examined_path: int | None
    ln_no_nodes_flag: bool | None
    ln_parse_status: str
    ln_source_type: str
    evidence_snippet: str | None
    pattern_id: str | None

    def to_row(self) -> dict[str, Any]:
        return {
            "ln_positive_path": self.ln_positive_path,
            "ln_examined_path": self.ln_examined_path,
            "ln_no_nodes_flag": self.ln_no_nodes_flag,
            "ln_parse_status": self.ln_parse_status,
            "ln_source_type": self.ln_source_type,
            "ln_evidence_snippet": self.evidence_snippet,
            "ln_pattern_id": self.pattern_id,
        }


def _snippet(text: str, start: int, end: int, max_len: int = 120) -> str:
    frag = text[max(0, start - 20) : min(len(text), end + 40)]
    frag = " ".join(frag.split())
    if len(frag) > max_len:
        frag = frag[: max_len - 3] + "..."
    return frag


def extract_pathology_ln_from_text(
    text: str | None,
    *,
    source_type: str = "pathology_narrative",
) -> PathologyLNParseResult:
    """
    Parse lymph node examined/positive counts from free-text pathology content.

    ln_parse_status:
      - parsed_pair: both positive and examined integers extracted
      - parsed_positive_only: positive known, examined unknown
      - parsed_examined_only: examined known from 0/Y negative pattern
      - explicit_no_nodal_disease: negation phrase matched (0 positive implied)
      - no_nodes_submitted: no nodes in specimen / not identified
      - no_match: no deterministic pattern
      - empty_input: no text
    """
    if not text or not str(text).strip():
        return PathologyLNParseResult(
            None,
            None,
            None,
            "empty_input",
            source_type,
            None,
            None,
        )

    t = str(text)

    # 1) Positives with X of Y
    for pid, rx in _PATTERNS_POSITIVE + _PATTERN_SLASH:
        m = rx.search(t)
        if m:
            pos_s, exam_s = m.group(1), m.group(2)
            try:
                pos_i, exam_i = int(pos_s), int(exam_s)
            except ValueError:
                continue
            if exam_i < 0 or pos_i < 0 or pos_i > exam_i:
                continue
            return PathologyLNParseResult(
                pos_i,
                exam_i,
                False,
                "parsed_pair",
                source_type,
                _snippet(t, m.start(), m.end()),
                pid,
            )

    # 2) 0 of Y negative
    for pid, rx in _PATTERNS_NEGATION:
        if pid == "zero_of_y_negative":
            m = rx.search(t)
            if m:
                try:
                    y = int(m.group(1))
                except ValueError:
                    continue
                return PathologyLNParseResult(
                    0,
                    y,
                    False,
                    "parsed_pair",
                    source_type,
                    _snippet(t, m.start(), m.end()),
                    pid,
                )
        else:
            m = rx.search(t)
            if m:
                if pid in ("no_ln_identified_excised",):
                    return PathologyLNParseResult(
                        None,
                        None,
                        True,
                        "no_nodes_submitted",
                        source_type,
                        _snippet(t, m.start(), m.end()),
                        pid,
                    )
                return PathologyLNParseResult(
                    0,
                    None,
                    False,
                    "explicit_no_nodal_disease",
                    source_type,
                    _snippet(t, m.start(), m.end()),
                    pid,
                )

    return PathologyLNParseResult(
        None,
        None,
        None,
        "no_match",
        source_type,
        None,
        None,
    )
