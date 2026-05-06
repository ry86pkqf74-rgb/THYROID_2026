"""Operative-note Sistrunk keyword pass (TGDC / M025 lineage).

Parses LOCAL operative documentation only. Outputs are PHI-safe: no raw note
text is returned—only paraphrased one-line evidence templates keyed to the
matched rule.

Match tiers
-----------
* ``regex`` — high-specificity phrases and word boundaries (preferred).
* ``fuzzy`` — conservative OCR/typo variants (e.g. *sustrunk*).

Author-knowledge synonyms are aligned with ``TGDC_VERIFICATION_REPORT.md``,
``TGDC_FINAL_RECONCILIATION_REPORT.md``, and representative operative phrasing
observed in LLM staging exports (thyroglossal duct cyst excision + hyoid).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Literal, Optional

MatchKind = Literal["regex", "fuzzy"]


@dataclass(frozen=True)
class SistrunkParseResult:
    """One successful hit inside a note (no raw PHI)."""

    research_id: str
    sistrunk_text_evidence: str
    sistrunk_match_kind: MatchKind
    match_offset: int
    rule_id: str
    note_row_id: Optional[str] = None

    def to_row_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "research_id": self.research_id,
            "sistrunk_text_evidence": self.sistrunk_text_evidence,
            "sistrunk_match_kind": self.sistrunk_match_kind,
            "match_offset": int(self.match_offset),
            "rule_id": self.rule_id,
        }
        if self.note_row_id is not None:
            d["note_row_id"] = self.note_row_id
        return d


# Ordered: longer / more specific phrases first (first winning regex by offset
# still respects loop order for same offset — list priority).
_REGEX_SPECS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    (
        "phrase_tgdc_excision_hyoid",
        re.compile(
            r"thyroglossal\s+duct\s+cyst\s+excision\s+with\s+hyoid\s+resection",
            re.IGNORECASE,
        ),
        "Operative documentation describes thyroglossal duct cyst excision with hyoid resection, matching a Sistrunk-type operation.",
    ),
    (
        "phrase_tgdc_excision_hyoid_bone",
        re.compile(
            r"thyroglossal\s+duct\s+cyst.{0,48}hyoid\s+bone\s+excision|"
            r"hyoid\s+bone\s+excision.{0,48}thyroglossal",
            re.IGNORECASE | re.DOTALL,
        ),
        "Operative documentation links thyroglossal duct cyst treatment to hyoid bone excision, consistent with a Sistrunk procedure.",
    ),
    (
        "phrase_sistrunk_procedure",
        re.compile(r"\bsistrunk\s+procedure\b", re.IGNORECASE),
        "Operative documentation explicitly names a Sistrunk procedure.",
    ),
    (
        "phrase_sistrunk_possessive",
        re.compile(r"\bsistrunk'?s\s+procedure\b", re.IGNORECASE),
        "Operative documentation uses a possessive Sistrunk procedure label.",
    ),
    (
        "phrase_excision_sistrunk",
        re.compile(
            r"(?:wide\s+)?excision.{0,64}\bsistrunk\b|\bsistrunk\b.{0,64}excision",
            re.IGNORECASE | re.DOTALL,
        ),
        "Operative documentation pairs excision language with a Sistrunk procedure reference.",
    ),
    (
        "phrase_tgdc_and_sistrunk",
        re.compile(
            r"thyroglossal.{0,80}\bsistrunk\b|\bsistrunk\b.{0,80}thyroglossal",
            re.IGNORECASE | re.DOTALL,
        ),
        "Operative documentation connects thyroglossal duct context to a Sistrunk procedure.",
    ),
    (
        "word_sistrunk",
        re.compile(r"\bsistrunk\b", re.IGNORECASE),
        "Operative documentation references Sistrunk in a surgical context.",
    ),
)

_FUZZY_SPECS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    (
        "typo_sustrunk",
        re.compile(r"\bsustrunk\b", re.IGNORECASE),
        "Operative documentation contains a near-spell match to Sistrunk (likely typo of Sistrunk).",
    ),
    (
        "typo_sinstrunk",
        re.compile(r"\bsinstrunk\b", re.IGNORECASE),
        "Operative documentation contains a near-spell match to Sistrunk (likely typo of Sistrunk).",
    ),
)


def parse_sistrunk_in_note(
    note_text: str,
    *,
    research_id: str,
    note_row_id: Optional[str] = None,
) -> Optional[SistrunkParseResult]:
    """Return first structured hit in ``note_text``, or ``None``.

    Prefer regex tier over fuzzy tier. Within a tier choose the earliest
    ``match.start()``; ties break by rule declaration order above.
    """
    if note_text is None:
        return None
    text = str(note_text)
    if not text.strip():
        return None

    best: Optional[SistrunkParseResult] = None

    def consider(kind: MatchKind, rule_id: str, rx: re.Pattern[str], evidence: str) -> None:
        nonlocal best
        m = rx.search(text)
        if m is None:
            return
        off = int(m.start())
        cand = SistrunkParseResult(
            research_id=str(research_id),
            sistrunk_text_evidence=evidence,
            sistrunk_match_kind=kind,
            match_offset=off,
            rule_id=rule_id,
            note_row_id=note_row_id,
        )
        if best is None:
            best = cand
            return
        if off < best.match_offset:
            best = cand
            return
        if off == best.match_offset and kind == "regex" and best.sistrunk_match_kind == "fuzzy":
            best = cand

    for rule_id, rx, evid in _REGEX_SPECS:
        consider("regex", rule_id, rx, evid)
    if best is not None:
        return best

    for rule_id, rx, evid in _FUZZY_SPECS:
        consider("fuzzy", rule_id, rx, evid)
    return best


def iter_matches_from_notes(
    rows: Iterable[tuple[Any, Any, Any]],
) -> Iterator[SistrunkParseResult]:
    """Yield hits for iterable of (research_id, note_row_id, note_text)."""
    for tup in rows:
        if tup is None or len(tup) < 3:
            continue
        rid, nrid, ntext = tup[0], tup[1], tup[2]
        if rid is None:
            continue
        parsed = parse_sistrunk_in_note(
            str(ntext) if ntext is not None else "",
            research_id=str(rid).strip(),
            note_row_id=None if nrid is None else str(nrid),
        )
        if parsed is not None:
            yield parsed


def pick_best_per_patient(
    hits: Iterable[SistrunkParseResult],
) -> dict[str, SistrunkParseResult]:
    """Choose a single strongest hit per ``research_id`` (regex beats fuzzy).

    Tie-break: lower ``match_offset``, then lexicographic ``rule_id`` for
    stability.
    """
    by_rid: dict[str, SistrunkParseResult] = {}

    def rank(h: SistrunkParseResult) -> tuple[int, int, str]:
        tier = 0 if h.sistrunk_match_kind == "regex" else 1
        return (tier, h.match_offset, h.rule_id)

    for h in hits:
        cur = by_rid.get(h.research_id)
        if cur is None or rank(h) < rank(cur):
            by_rid[h.research_id] = h
    return by_rid
