"""
Deep RAI (radioactive iodine) treatment parser.

Extends BaseExtractor to deeply parse clinical notes and nuclear medicine
reports, extracting dose, intent, completion status, scan findings,
iodine avidity, stimulated lab values, and uptake percentages.

The existing MedicationExtractor only captures basic RAI dose mentions;
this extractor covers the full treatment context.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from notes_extraction.base import BaseExtractor, EntityMatch
from utils.text_helpers import extract_nearby_date, safe_float

GBQ_TO_MCI = 27.027


# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------

_DOSE = re.compile(
    r"\b(\d{1,4}(?:\.\d{1,2})?)\s*(mCi|GBq)\b",
    re.IGNORECASE,
)

_DOSE_CONTEXT = re.compile(
    r"(?:radioactive\s+iodine|RAI|I[\s-]*131|131[\s-]*I|ablation\s+dose|"
    r"therapeutic\s+dose|treatment\s+dose|administered\s+dose|received\s+dose)"
    r"\s+(?:of\s+)?(\d{1,4}(?:\.\d{1,2})?)\s*(mCi|GBq)",
    re.IGNORECASE,
)

_INTENT_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(
        r"\b(?:remnant\s+ablation|thyroid\s+remnant\s+ablat\w*|"
        r"ablat(?:e|ion|ive)\s+(?:of\s+)?(?:the\s+)?(?:thyroid\s+)?remnant)\b",
        re.I),
     "remnant_ablation"),
    (re.compile(
        r"\b(?:adjuvant\s+(?:RAI|radioactive\s+iodine|I[\s-]*131)\s*(?:therap\w*|treatment)?|"
        r"(?:RAI|radioactive\s+iodine)\s+(?:as\s+)?adjuvant)\b",
        re.I),
     "adjuvant_therapy"),
    (re.compile(
        r"\b(?:treat(?:ment|ing)?\s+(?:of\s+)?(?:persistent|recurrent)\s+(?:disease|cancer|carcinoma)|"
        r"(?:persistent|recurrent)\s+(?:disease|cancer)\s+treat\w*)\b",
        re.I),
     "treatment_persistent_recurrent"),
    (re.compile(
        r"\b(?:treat(?:ment|ing)?\s+(?:of\s+)?metasta\w+\s+(?:disease|cancer|carcinoma)|"
        r"metasta\w+\s+(?:disease\s+)?treat\w*)\b",
        re.I),
     "treatment_metastatic"),
]

_COMPLETION_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(
        r"\b(?:(?:RAI|radioactive\s+iodine|I[\s-]*131)\s+(?:was\s+)?(?:received|completed|administered|given)|"
        r"(?:received|completed|underwent)\s+(?:RAI|radioactive\s+iodine|I[\s-]*131))\b",
        re.I),
     "received"),
    (re.compile(
        r"\b(?:(?:RAI|radioactive\s+iodine|I[\s-]*131)\s+(?:is\s+)?(?:recommended|planned|will\s+(?:be\s+)?(?:receive|undergo))|"
        r"(?:recommend(?:ed)?|plan(?:ned)?)\s+(?:for\s+)?(?:RAI|radioactive\s+iodine|I[\s-]*131))\b",
        re.I),
     "recommended"),
    (re.compile(
        r"\b(?:(?:RAI|radioactive\s+iodine|I[\s-]*131)\s+(?:is\s+)?scheduled|"
        r"scheduled\s+(?:for\s+)?(?:RAI|radioactive\s+iodine|I[\s-]*131))\b",
        re.I),
     "scheduled"),
    (re.compile(
        r"\b(?:(?:RAI|radioactive\s+iodine|I[\s-]*131)\s+(?:was\s+)?(?:declined|refused)|"
        r"(?:declined?|refused?)\s+(?:RAI|radioactive\s+iodine|I[\s-]*131))\b",
        re.I),
     "declined"),
    (re.compile(
        r"\b(?:(?:RAI|radioactive\s+iodine|I[\s-]*131)\s+(?:is\s+)?(?:not\s+(?:indicated|necessary|required|applicable))|"
        r"no\s+(?:indication|need)\s+for\s+(?:RAI|radioactive\s+iodine|I[\s-]*131))\b",
        re.I),
     "not_applicable"),
]

_PRE_SCAN = re.compile(
    r"\b(?:pre[\s-]*(?:treatment|therapy|ablation|RAI)\s+(?:whole\s+body\s+)?scan|"
    r"diagnostic\s+(?:whole\s+body\s+)?(?:I[\s-]*(?:123|131)\s+)?scan|"
    r"I[\s-]*123\s+(?:whole\s+body\s+)?scan|"
    r"(?:whole\s+body|WBS)\s+(?:diagnostic\s+)?scan\s+(?:prior\s+to|before)\s+"
    r"(?:RAI|ablation|treatment))\b",
    re.IGNORECASE,
)

_POST_SCAN = re.compile(
    r"\b(?:post[\s-]*(?:treatment|therapy|ablation|RAI|dose)\s+(?:whole\s+body\s+)?scan|"
    r"post[\s-]*(?:RAI|treatment|therapy|ablation)\s+WBS|"
    r"(?:whole\s+body|WBS)\s+scan\s+(?:after|post|following)\s+"
    r"(?:RAI|ablation|treatment|I[\s-]*131)|"
    r"(?:Rx|treatment)\s+WBS|RxWBS|"
    r"post[\s-]*(?:RAI|treatment)\s+(?:I[\s-]*131\s+)?(?:whole\s+body\s+)?imag\w*)\b",
    re.IGNORECASE,
)

_AVIDITY_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(
        r"\b(?:iodine[\s-]*avid|radioiodine[\s-]*avid|RAI[\s-]*avid|"
        r"(?:avid\s+(?:for|to)\s+)?(?:iodine|radioiodine|RAI|I[\s-]*131)\s+uptake|"
        r"demonstrates?\s+(?:iodine|radioiodine|RAI)\s+uptake)\b",
        re.I),
     "avid"),
    (re.compile(
        r"\b(?:non[\s-]*(?:iodine|radioiodine|RAI)[\s-]*avid|"
        r"(?:iodine|radioiodine|RAI)[\s-]*(?:non[\s-]*)?refractor\w*|"
        r"no\s+(?:significant\s+)?(?:iodine|radioiodine|RAI)\s+uptake|"
        r"(?:iodine|radioiodine|RAI)[\s-]*non[\s-]*avid)\b",
        re.I),
     "non_avid"),
]

_SCAN_FINDING_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(
        r"\b(?:uptake\s+(?:in\s+)?(?:the\s+)?thyroid\s+bed|"
        r"thyroid\s+bed\s+(?:uptake|activity|accumulation)|"
        r"activity\s+(?:in\s+)?(?:the\s+)?thyroid\s+bed)\b",
        re.I),
     "thyroid_bed_uptake"),
    (re.compile(
        r"\b(?:(?:cervical|neck)\s+(?:lymph\s+node\s+)?uptake|"
        r"uptake\s+(?:in\s+)?(?:the\s+)?(?:cervical|neck)\s+(?:lymph\s+nodes?|region)|"
        r"(?:cervical|neck)\s+(?:nodal\s+)?activity)\b",
        re.I),
     "cervical_uptake"),
    (re.compile(
        r"\b(?:distant\s+(?:metasta\w+\s+)?uptake|"
        r"uptake\s+(?:in\s+)?distant\s+(?:sites?|metastas\w*)|"
        r"distant\s+(?:iodine|radioiodine)\s+(?:avid\s+)?(?:foci|lesions?|disease))\b",
        re.I),
     "distant_uptake"),
    (re.compile(
        r"\b(?:(?:pulmonary|lung)\s+(?:metasta\w+\s+)?uptake|"
        r"uptake\s+(?:in\s+)?(?:the\s+)?(?:lungs?|pulmonary)|"
        r"(?:lung|pulmonary)\s+(?:iodine\s+)?(?:avid\s+)?(?:foci|lesions?|metastas\w*|activity))\b",
        re.I),
     "lung_uptake"),
    (re.compile(
        r"\b(?:(?:bone|osseous|skeletal)\s+(?:metasta\w+\s+)?uptake|"
        r"uptake\s+(?:in\s+)?(?:the\s+)?(?:bones?|skeleton|osseous)|"
        r"(?:bone|osseous)\s+(?:iodine\s+)?(?:avid\s+)?(?:foci|lesions?|metastas\w*|activity))\b",
        re.I),
     "bone_uptake"),
]

_STIM_TG = re.compile(
    r"\b(?:stimulated\s+)?(?:thyroglobulin|Tg)\s*"
    r"(?:level\s*)?(?:(?:was|of|is|=|:)\s*)?(\d{1,6}(?:\.\d{1,3})?)\s*"
    r"(?:ng/mL|ng/ml|mcg/L|ug/L|µg/L)?\b",
    re.IGNORECASE,
)

_STIM_TG_CONTEXT = re.compile(
    r"(?:stimulated|pre[\s-]*(?:RAI|ablation|treatment))\s+"
    r"(?:thyroglobulin|Tg)\s*(?:level\s*)?(?:(?:was|of|is|=|:)\s*)?"
    r"(\d{1,6}(?:\.\d{1,3})?)",
    re.IGNORECASE,
)

_STIM_TSH = re.compile(
    r"\b(?:stimulated\s+)?TSH\s*"
    r"(?:level\s*)?(?:(?:was|of|is|=|:)\s*)?(\d{1,4}(?:\.\d{1,3})?)\s*"
    r"(?:mIU/L|mU/L|uIU/mL|µIU/mL)?\b",
    re.IGNORECASE,
)

_STIM_TSH_CONTEXT = re.compile(
    r"(?:stimulated|pre[\s-]*(?:RAI|ablation|treatment))\s+"
    r"TSH\s*(?:level\s*)?(?:(?:was|of|is|=|:)\s*)?"
    r"(\d{1,4}(?:\.\d{1,3})?)",
    re.IGNORECASE,
)

_UPTAKE_PCT = re.compile(
    r"\b(?:uptake|avidity)\s+(?:of\s+)?(\d{1,3}(?:\.\d{1,2})?)\s*%|"
    r"(\d{1,3}(?:\.\d{1,2})?)\s*%\s+(?:uptake|avidity)\b",
    re.IGNORECASE,
)

_RAI_CONTEXT_WINDOW = 600


def _in_rai_context(text: str, pos: int, window: int = _RAI_CONTEXT_WINDOW) -> bool:
    """Return True if `pos` falls near an RAI / nuclear medicine mention."""
    start = max(0, pos - window)
    end = min(len(text), pos + window)
    region = text[start:end].lower()
    return bool(re.search(
        r"(?:radioactive\s+iodine|rai\b|i[\s-]*131|131[\s-]*i|ablation|"
        r"nuclear\s+medicine|thyroid\s+scan|whole\s+body\s+scan|wbs|"
        r"remnant|post[\s-]*treatment|post[\s-]*therapy)",
        region,
    ))


class RAIDetailExtractor(BaseExtractor):
    """Deep parser for RAI treatment context in clinical / nuclear-medicine notes."""

    entity_domain = "rai_detail"

    def extract(self, note_row_id, research_id, note_type, note_text, note_date=None):
        results: list[EntityMatch] = []
        if not note_text:
            return results

        self._extract_dose(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_intent(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_completion(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_pre_scan(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_post_scan(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_avidity(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_scan_findings(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_stim_tg(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_stim_tsh(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_uptake_pct(results, note_row_id, research_id, note_type, note_text, note_date)

        return results

    # ----- dose -----

    def _extract_dose(self, results, note_row_id, research_id, note_type, note_text, note_date):
        seen_starts: set[int] = set()

        for m in _DOSE_CONTEXT.finditer(note_text):
            seen_starts.add(m.start())
            self._append_dose_match(
                results, m, note_row_id, research_id, note_type, note_text, note_date,
                group_val=1, group_unit=2, confidence=0.95,
            )

        for m in _DOSE.finditer(note_text):
            if m.start() in seen_starts:
                continue
            if not _in_rai_context(note_text, m.start()):
                continue
            self._append_dose_match(
                results, m, note_row_id, research_id, note_type, note_text, note_date,
                group_val=1, group_unit=2, confidence=0.85,
            )

    def _append_dose_match(self, results, m, note_row_id, research_id, note_type,
                           note_text, note_date, *, group_val, group_unit, confidence):
        raw_val = safe_float(m.group(group_val))
        if raw_val is None:
            return
        unit = m.group(group_unit)
        if unit.lower() == "gbq":
            mci_val = round(raw_val * GBQ_TO_MCI, 1)
            norm = f"{mci_val} mCi"
        else:
            norm = f"{raw_val:g} mCi"
        results.append(EntityMatch(
            research_id=research_id,
            note_row_id=note_row_id,
            note_type=note_type,
            entity_type="rai_dose",
            entity_value_raw=m.group(0),
            entity_value_norm=norm,
            present_or_negated=self.check_negation(note_text, m.start()),
            confidence=confidence,
            evidence_span=m.group(0),
            evidence_start=m.start(),
            evidence_end=m.end(),
            entity_date=extract_nearby_date(note_text, m.start(), m.end()),
            note_date=note_date,
            extraction_method="regex_rai_v2",
        ))

    # ----- intent -----

    def _extract_intent(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _INTENT_MAP:
            for m in pat.finditer(note_text):
                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type="rai_intent",
                    entity_value_raw=m.group(0),
                    entity_value_norm=norm,
                    present_or_negated=self.check_negation(note_text, m.start()),
                    confidence=0.85,
                    evidence_span=m.group(0),
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                    extraction_method="regex_rai_v2",
                ))

    # ----- completion -----

    def _extract_completion(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _COMPLETION_MAP:
            for m in pat.finditer(note_text):
                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type="rai_completion",
                    entity_value_raw=m.group(0),
                    entity_value_norm=norm,
                    present_or_negated=self.check_negation(note_text, m.start()),
                    confidence=0.85,
                    evidence_span=m.group(0),
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                    extraction_method="regex_rai_v2",
                ))

    # ----- pre-treatment scan -----

    def _extract_pre_scan(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for m in _PRE_SCAN.finditer(note_text):
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="rai_pre_scan",
                entity_value_raw=m.group(0),
                entity_value_norm="pre_treatment_scan",
                present_or_negated=self.check_negation(note_text, m.start()),
                confidence=0.95,
                evidence_span=m.group(0),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
                extraction_method="regex_rai_v2",
            ))

    # ----- post-therapy scan -----

    def _extract_post_scan(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for m in _POST_SCAN.finditer(note_text):
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="rai_post_scan",
                entity_value_raw=m.group(0),
                entity_value_norm="post_treatment_scan",
                present_or_negated=self.check_negation(note_text, m.start()),
                confidence=0.95,
                evidence_span=m.group(0),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
                extraction_method="regex_rai_v2",
            ))

    # ----- iodine avidity -----

    def _extract_avidity(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _AVIDITY_MAP:
            for m in pat.finditer(note_text):
                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type="rai_avidity",
                    entity_value_raw=m.group(0),
                    entity_value_norm=norm,
                    present_or_negated=self.check_negation(note_text, m.start()),
                    confidence=0.95,
                    evidence_span=m.group(0),
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                    extraction_method="regex_rai_v2",
                ))

    # ----- scan findings -----

    def _extract_scan_findings(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _SCAN_FINDING_MAP:
            for m in pat.finditer(note_text):
                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type="rai_scan_finding",
                    entity_value_raw=m.group(0),
                    entity_value_norm=norm,
                    present_or_negated=self.check_negation(note_text, m.start()),
                    confidence=0.95,
                    evidence_span=m.group(0),
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                    extraction_method="regex_rai_v2",
                ))

    # ----- stimulated thyroglobulin -----

    def _extract_stim_tg(self, results, note_row_id, research_id, note_type, note_text, note_date):
        seen_starts: set[int] = set()

        for m in _STIM_TG_CONTEXT.finditer(note_text):
            seen_starts.add(m.start())
            val = safe_float(m.group(1))
            if val is None:
                continue
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="rai_stimulated_tg",
                entity_value_raw=m.group(0),
                entity_value_norm=f"{val:g} ng/mL",
                present_or_negated=self.check_negation(note_text, m.start()),
                confidence=0.95,
                evidence_span=m.group(0),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
                extraction_method="regex_rai_v2",
            ))

        for m in _STIM_TG.finditer(note_text):
            if m.start() in seen_starts:
                continue
            if not _in_rai_context(note_text, m.start()):
                continue
            val = safe_float(m.group(1))
            if val is None:
                continue
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="rai_stimulated_tg",
                entity_value_raw=m.group(0),
                entity_value_norm=f"{val:g} ng/mL",
                present_or_negated=self.check_negation(note_text, m.start()),
                confidence=0.7,
                evidence_span=m.group(0),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
                extraction_method="regex_rai_v2",
            ))

    # ----- stimulated TSH -----

    def _extract_stim_tsh(self, results, note_row_id, research_id, note_type, note_text, note_date):
        seen_starts: set[int] = set()

        for m in _STIM_TSH_CONTEXT.finditer(note_text):
            seen_starts.add(m.start())
            val = safe_float(m.group(1))
            if val is None:
                continue
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="rai_stimulated_tsh",
                entity_value_raw=m.group(0),
                entity_value_norm=f"{val:g} mIU/L",
                present_or_negated=self.check_negation(note_text, m.start()),
                confidence=0.95,
                evidence_span=m.group(0),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
                extraction_method="regex_rai_v2",
            ))

        for m in _STIM_TSH.finditer(note_text):
            if m.start() in seen_starts:
                continue
            if not _in_rai_context(note_text, m.start()):
                continue
            val = safe_float(m.group(1))
            if val is None:
                continue
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="rai_stimulated_tsh",
                entity_value_raw=m.group(0),
                entity_value_norm=f"{val:g} mIU/L",
                present_or_negated=self.check_negation(note_text, m.start()),
                confidence=0.7,
                evidence_span=m.group(0),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
                extraction_method="regex_rai_v2",
            ))

    # ----- uptake percentage -----

    def _extract_uptake_pct(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for m in _UPTAKE_PCT.finditer(note_text):
            val = m.group(1) or m.group(2)
            pct = safe_float(val)
            if pct is None or pct > 100 or pct < 0:
                continue
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="rai_uptake_pct",
                entity_value_raw=m.group(0),
                entity_value_norm=f"{pct:g}%",
                present_or_negated=self.check_negation(note_text, m.start()),
                confidence=0.95,
                evidence_span=m.group(0),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
                extraction_method="regex_rai_v2",
            ))
