"""
Targeted H&P extractors for smoking status and BMI.

Follows BaseExtractor pattern. Consent-boilerplate aware: smoking and BMI
are NOT typically listed in surgical risk boilerplate, but the extractors
include section-awareness to avoid extracting from risk-listing paragraphs.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from notes_extraction.base import BaseExtractor, EntityMatch
from utils.text_helpers import extract_nearby_date

# ---------------------------------------------------------------------------
# Consent / risk-discussion section detector
# ---------------------------------------------------------------------------
_CONSENT_SECTION_RE = re.compile(
    r"(?:risks?\s+(?:of|for|include)|informed\s+consent|"
    r"complications?\s+(?:include|may|could|such\s+as)|"
    r"discussed\s+(?:the\s+)?(?:patient|pt).*?(?:risks|complications)|"
    r"consented\s+(?:for|to))",
    re.IGNORECASE,
)

_CONSENT_WINDOW = 300


def _in_consent_section(text: str, match_start: int) -> bool:
    """Return True if the match sits inside a consent/risk-discussion block."""
    window_start = max(0, match_start - _CONSENT_WINDOW)
    window = text[window_start:match_start]
    return bool(_CONSENT_SECTION_RE.search(window))


# ---------------------------------------------------------------------------
# Smoking Status Extractor
# ---------------------------------------------------------------------------

_SMOKING_NORM: dict[str, str] = {
    "current": "current_smoker",
    "active": "current_smoker",
    "currently smokes": "current_smoker",
    "currently smoking": "current_smoker",
    "smoker": "current_smoker",
    "current smoker": "current_smoker",
    "current every day smoker": "current_smoker",
    "current every day": "current_smoker",
    "current some day smoker": "current_smoker",
    "current some day": "current_smoker",
    "every day smoker": "current_smoker",
    "everyday smoker": "current_smoker",
    "former": "former_smoker",
    "former smoker": "former_smoker",
    "ex-smoker": "former_smoker",
    "ex smoker": "former_smoker",
    "quit": "former_smoker",
    "quit smoking": "former_smoker",
    "stopped smoking": "former_smoker",
    "remote": "former_smoker",
    "remote history": "former_smoker",
    "previously": "former_smoker",
    "past smoker": "former_smoker",
    "never": "never_smoker",
    "never smoker": "never_smoker",
    "never smoked": "never_smoker",
    "non-smoker": "never_smoker",
    "nonsmoker": "never_smoker",
    "non smoker": "never_smoker",
    "no tobacco": "never_smoker",
    "no smoking": "never_smoker",
    "denies tobacco": "never_smoker",
    "denies smoking": "never_smoker",
    "does not smoke": "never_smoker",
    "doesn't smoke": "never_smoker",
    "negative": "never_smoker",
    "no history of tobacco": "never_smoker",
    "no history of smoking": "never_smoker",
    "passive": "passive_exposure",
    "secondhand": "passive_exposure",
    "second hand": "passive_exposure",
    "unknown": "unknown",
    "not asked": "unknown",
}

_PACK_YEAR_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:pack[\s-]*year|py|pky)",
    re.IGNORECASE,
)

_PPD_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:ppd|pack(?:s)?\s*(?:per|/)\s*day)",
    re.IGNORECASE,
)


class SmokingStatusExtractor(BaseExtractor):
    """Extract smoking status from H&P social history sections."""

    entity_domain = "smoking_status"

    _SECTION_RE = re.compile(
        r"(?:social\s+history|tobacco|smoking|cigarette|nicotine)",
        re.IGNORECASE,
    )

    _STATUS_RE = re.compile(
        r"(?:(?:tobacco|smoking|cigarette|nicotine)\s+(?:use|status|history)"
        r"[:\s]*"
        r"(current(?:\s+every\s+day)?(?:\s+some\s+day)?(?:\s+smoker)?|"
        r"former(?:\s+smoker)?|"
        r"never(?:\s+smoker)?|"
        r"(?:ex|past)[\s-]*smoker|"
        r"quit(?:\s+smoking)?|"
        r"active|"
        r"non[\s-]*smoker|"
        r"nonsmoker|"
        r"unknown|"
        r"not\s+asked|"
        r"negative))"
        r"|"
        r"(?:(current(?:\s+every\s+day)?(?:\s+some\s+day)?|former|never|past)"
        r"\s+(?:tobacco|smoker|smoking))"
        r"|"
        r"((?:denies|no\s+history\s+of|no)\s+(?:tobacco|smoking))"
        r"|"
        r"((?:does(?:n'?t)?|do(?:es)?\s+not)\s+smoke)"
        r"|"
        r"((?:currently\s+smok(?:es|ing))\b)"
        r"|"
        r"((?:stopped|quit)\s+smoking\b)"
        r"|"
        r"((?:remote\s+(?:history(?:\s+of)?|tobacco))\b)",
        re.IGNORECASE,
    )

    def _normalize(self, raw: str) -> str:
        key = raw.strip().lower()
        key = re.sub(r"\s+", " ", key)
        if key in _SMOKING_NORM:
            return _SMOKING_NORM[key]
        for k, v in _SMOKING_NORM.items():
            if k in key:
                return v
        return "unknown"

    def extract(
        self,
        note_row_id: str,
        research_id: int,
        note_type: str,
        note_text: str,
        note_date: str | None = None,
    ) -> list[EntityMatch]:
        if not note_text or len(note_text) < 20:
            return []

        results: list[EntityMatch] = []
        seen_norms: set[str] = set()

        for m in self._STATUS_RE.finditer(note_text):
            raw = next(
                (g for g in m.groups() if g is not None),
                m.group(0),
            )
            if _in_consent_section(note_text, m.start()):
                continue

            norm = self._normalize(raw)
            if norm in seen_norms:
                continue
            seen_norms.add(norm)

            ctx_start = max(0, m.start() - 40)
            ctx_end = min(len(note_text), m.end() + 40)
            evidence = note_text[ctx_start:ctx_end].replace("\n", " ").strip()

            conf = 0.90 if norm != "unknown" else 0.50

            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="smoking_status",
                entity_value_raw=raw.strip(),
                entity_value_norm=norm,
                present_or_negated="present",
                confidence=conf,
                evidence_span=evidence[:200],
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
            ))

        for m in _PACK_YEAR_RE.finditer(note_text):
            if _in_consent_section(note_text, m.start()):
                continue
            val = m.group(1)
            try:
                py_val = float(val)
                if py_val < 0 or py_val > 300:
                    continue
            except ValueError:
                continue

            ctx_start = max(0, m.start() - 40)
            ctx_end = min(len(note_text), m.end() + 40)
            evidence = note_text[ctx_start:ctx_end].replace("\n", " ").strip()

            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="pack_years",
                entity_value_raw=m.group(0).strip(),
                entity_value_norm=val,
                present_or_negated="present",
                confidence=0.85,
                evidence_span=evidence[:200],
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
            ))

        for m in _PPD_RE.finditer(note_text):
            if _in_consent_section(note_text, m.start()):
                continue
            val = m.group(1)
            try:
                ppd_val = float(val)
                if ppd_val < 0 or ppd_val > 10:
                    continue
            except ValueError:
                continue

            ctx_start = max(0, m.start() - 40)
            ctx_end = min(len(note_text), m.end() + 40)
            evidence = note_text[ctx_start:ctx_end].replace("\n", " ").strip()

            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="packs_per_day",
                entity_value_raw=m.group(0).strip(),
                entity_value_norm=val,
                present_or_negated="present",
                confidence=0.85,
                evidence_span=evidence[:200],
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
            ))

        return results


# ---------------------------------------------------------------------------
# BMI Extractor
# ---------------------------------------------------------------------------

class BMIExtractor(BaseExtractor):
    """Extract numeric BMI values from H&P vitals/assessment sections."""

    entity_domain = "bmi"

    _BMI_RE = re.compile(
        r"\bBMI\s*[:=]?\s*(\d{2}(?:\.\d{1,2})?)\b",
        re.IGNORECASE,
    )

    _BMI_LONG_RE = re.compile(
        r"\bbody\s+mass\s+index\s*[:=]?\s*(\d{2}(?:\.\d{1,2})?)\b",
        re.IGNORECASE,
    )

    _BMI_CATEGORY_RE = re.compile(
        r"\b(morbid(?:ly)?\s+obes\w*|"
        r"obes\w*\s+class\s+(?:I{1,3}|[1-3])|"
        r"overweight|"
        r"normal\s+(?:weight|bmi)|"
        r"underweight)\b",
        re.IGNORECASE,
    )

    _BMI_CATEGORY_NORM: dict[str, str] = {
        "underweight": "underweight",
        "normal weight": "normal_weight",
        "normal bmi": "normal_weight",
        "overweight": "overweight",
        "obese": "obese",
        "obesity": "obese",
        "obese class i": "obese_class_i",
        "obese class ii": "obese_class_ii",
        "obese class iii": "obese_class_iii",
        "obesity class 1": "obese_class_i",
        "obesity class 2": "obese_class_ii",
        "obesity class 3": "obese_class_iii",
        "morbid obesity": "morbid_obesity",
        "morbidly obese": "morbid_obesity",
    }

    def _norm_category(self, raw: str) -> str:
        key = raw.strip().lower()
        key = re.sub(r"\s+", " ", key)
        for k, v in self._BMI_CATEGORY_NORM.items():
            if k in key:
                return v
        return "obese" if "obes" in key else key

    def extract(
        self,
        note_row_id: str,
        research_id: int,
        note_type: str,
        note_text: str,
        note_date: str | None = None,
    ) -> list[EntityMatch]:
        if not note_text or len(note_text) < 20:
            return []

        results: list[EntityMatch] = []
        seen_values: set[str] = set()

        for pat in (self._BMI_RE, self._BMI_LONG_RE):
            for m in pat.finditer(note_text):
                if _in_consent_section(note_text, m.start()):
                    continue

                val = m.group(1)
                try:
                    bmi_f = float(val)
                    if bmi_f < 10.0 or bmi_f > 80.0:
                        continue
                except ValueError:
                    continue

                if val in seen_values:
                    continue
                seen_values.add(val)

                ctx_start = max(0, m.start() - 30)
                ctx_end = min(len(note_text), m.end() + 30)
                evidence = note_text[ctx_start:ctx_end].replace("\n", " ").strip()

                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type="bmi_value",
                    entity_value_raw=m.group(0).strip(),
                    entity_value_norm=val,
                    present_or_negated="present",
                    confidence=0.95,
                    evidence_span=evidence[:200],
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                ))

        for m in self._BMI_CATEGORY_RE.finditer(note_text):
            if _in_consent_section(note_text, m.start()):
                continue

            raw = m.group(0)
            norm = self._norm_category(raw)

            if norm in seen_values:
                continue
            seen_values.add(norm)

            ctx_start = max(0, m.start() - 30)
            ctx_end = min(len(note_text), m.end() + 30)
            evidence = note_text[ctx_start:ctx_end].replace("\n", " ").strip()

            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="bmi_category",
                entity_value_raw=raw.strip(),
                entity_value_norm=norm,
                present_or_negated="present",
                confidence=0.75,
                evidence_span=evidence[:200],
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
            ))

        return results


HP_TARGETED_EXTRACTORS: list[type[BaseExtractor]] = [
    SmokingStatusExtractor,
    BMIExtractor,
]
