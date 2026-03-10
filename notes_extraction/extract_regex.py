"""
High-precision regex-based entity extractors.

Each extractor subclasses BaseExtractor and produces EntityMatch objects
with evidence_span (exact substring), character offsets, negation status,
entity_date (date found near the match), and note_date (encounter date
from the note header, passed through as fallback).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from notes_extraction.base import BaseExtractor, EntityMatch
from notes_extraction.vocab import (
    COMPLICATION_NORM,
    GENE_NORM,
    MEDICATION_NORM,
    PROCEDURE_NORM,
)
from utils.text_helpers import extract_nearby_date


class StagingExtractor(BaseExtractor):
    """Extract AJCC T/N/M and overall stage mentions."""

    entity_domain = "staging"

    _T_STAGE = re.compile(
        r"\b(p?T[0-4][ab]?(?:is)?)\b", re.IGNORECASE
    )
    _N_STAGE = re.compile(
        r"\b(p?N[0-2][ab]?)\b", re.IGNORECASE
    )
    _M_STAGE = re.compile(
        r"\b(p?M[01x])\b", re.IGNORECASE
    )
    _OVERALL = re.compile(
        r"\b[Ss]tage\s+(I{1,3}V?|IV[ABC]?|[1-4][ABC]?)\b"
    )

    _PATTERNS: list[tuple[re.Pattern, str]] = [
        (_T_STAGE, "T_stage"),
        (_N_STAGE, "N_stage"),
        (_M_STAGE, "M_stage"),
        (_OVERALL, "overall_stage"),
    ]

    def extract(self, note_row_id, research_id, note_type, note_text, note_date=None):
        results: list[EntityMatch] = []
        for pat, component in self._PATTERNS:
            for m in pat.finditer(note_text):
                raw = m.group(1) if m.lastindex else m.group(0)
                norm = raw.upper() if component != "overall_stage" else f"Stage {raw}"
                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type=component,
                    entity_value_raw=raw,
                    entity_value_norm=norm,
                    present_or_negated=self.check_negation(note_text, m.start()),
                    evidence_span=m.group(0),
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                ))
        return results


class GeneticsExtractor(BaseExtractor):
    """Extract gene / mutation mentions."""

    entity_domain = "genetics"

    _PATTERNS: list[tuple[re.Pattern, str, str]] = [
        (re.compile(r"\bBRAF\s*V600E?\b", re.I), "BRAF V600E", "BRAF"),
        (re.compile(r"\bBRAF\b(?!\s*V600)", re.I), "BRAF", "BRAF"),
        (re.compile(r"\bNRAS\b", re.I), "NRAS", "NRAS"),
        (re.compile(r"\bHRAS\b", re.I), "HRAS", "HRAS"),
        (re.compile(r"\bKRAS\b", re.I), "KRAS", "KRAS"),
        (re.compile(r"\bRAS\b(?![\w])", re.I), "RAS", "RAS"),
        (re.compile(r"\bRET(?:/PTC)?\b", re.I), "RET", "RET"),
        (re.compile(r"\bTERT\s*(?:promoter)?\b", re.I), "TERT", "TERT"),
        (re.compile(r"\bNTRK\s*(?:fusion)?\b", re.I), "NTRK", "NTRK"),
        (re.compile(r"\bALK\s*(?:fusion)?\b", re.I), "ALK", "ALK"),
    ]

    def extract(self, note_row_id, research_id, note_type, note_text, note_date=None):
        results: list[EntityMatch] = []
        seen: set[tuple[str, int]] = set()
        for pat, raw_label, norm_gene in self._PATTERNS:
            for m in pat.finditer(note_text):
                key = (norm_gene, m.start())
                if key in seen:
                    continue
                seen.add(key)
                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type="gene",
                    entity_value_raw=m.group(0),
                    entity_value_norm=norm_gene,
                    present_or_negated=self.check_negation(note_text, m.start()),
                    evidence_span=m.group(0),
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                ))
        return results


class ProcedureExtractor(BaseExtractor):
    """Extract surgical procedure mentions."""

    entity_domain = "procedures"

    _PATTERNS: list[tuple[re.Pattern, str]] = [
        (re.compile(
            r"\b(total\s+thyroidectom\w*)\b", re.I),
            "total_thyroidectomy"),
        (re.compile(
            r"\b(near[\s-]*total\s+thyroidectom\w*)\b", re.I),
            "total_thyroidectomy"),
        (re.compile(
            r"\b(bilateral\s+thyroidectom\w*)\b", re.I),
            "total_thyroidectomy"),
        (re.compile(
            r"\b(hemithyroidectom\w*)\b", re.I),
            "hemithyroidectomy"),
        (re.compile(
            r"\b(thyroid\s+lobectom\w*)\b", re.I),
            "hemithyroidectomy"),
        (re.compile(
            r"\b((?:right|left)\s+(?:thyroid\s+)?lobectom\w*)\b", re.I),
            "hemithyroidectomy"),
        (re.compile(
            r"\b(completion\s+thyroidectom\w*)\b", re.I),
            "completion_thyroidectomy"),
        (re.compile(
            r"\b(central\s+(?:compartment\s+)?neck\s+dissection)\b", re.I),
            "central_neck_dissection"),
        (re.compile(
            r"\b(level\s+VI\s+(?:neck\s+)?dissection)\b", re.I),
            "central_neck_dissection"),
        (re.compile(
            r"\b(lateral\s+neck\s+dissection)\b", re.I),
            "lateral_neck_dissection"),
        (re.compile(
            r"\b(modified\s+radical\s+neck\s+dissection)\b", re.I),
            "modified_radical_neck_dissection"),
        (re.compile(
            r"\b(MRND)\b"),
            "modified_radical_neck_dissection"),
        (re.compile(
            r"\b(parathyroid\s+auto\s*transplant\w*)\b", re.I),
            "parathyroid_autotransplant"),
        (re.compile(
            r"\b(tracheostom\w*)\b", re.I),
            "tracheostomy"),
        (re.compile(
            r"\b((?:flex(?:ible)?\s+)?laryngoscop\w*)\b", re.I),
            "laryngoscopy"),
    ]

    def extract(self, note_row_id, research_id, note_type, note_text, note_date=None):
        results: list[EntityMatch] = []
        for pat, norm_proc in self._PATTERNS:
            for m in pat.finditer(note_text):
                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type="procedure",
                    entity_value_raw=m.group(1),
                    entity_value_norm=norm_proc,
                    present_or_negated=self.check_negation(note_text, m.start()),
                    evidence_span=m.group(0),
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                ))
        return results


class ComplicationExtractor(BaseExtractor):
    """Extract post-operative complication mentions."""

    entity_domain = "complications"

    _PATTERNS: list[tuple[re.Pattern, str]] = [
        (re.compile(
            r"\b((?:recurrent\s+laryngeal\s+nerve|RLN)\s+injur\w*)\b", re.I),
            "rln_injury"),
        (re.compile(
            r"\b(vocal\s+cord\s+(?:paralys\w+|palsy))\b", re.I),
            "vocal_cord_paralysis"),
        (re.compile(
            r"\b(VCP)\b"),
            "vocal_cord_paralysis"),
        (re.compile(
            r"\b(vocal\s+cord\s+(?:pares\w+|weakness|hypomobil\w+|immobil\w+))\b", re.I),
            "vocal_cord_paresis"),
        (re.compile(
            r"\b(hypocalcemi\w*)\b", re.I),
            "hypocalcemia"),
        (re.compile(
            r"\b(hypoparathyroidism)\b", re.I),
            "hypoparathyroidism"),
        (re.compile(
            r"\b((?:neck\s+)?hematoma)\b", re.I),
            "hematoma"),
        (re.compile(
            r"\b(seroma)\b", re.I),
            "seroma"),
        (re.compile(
            r"\b(wound\s+infection|surgical\s+site\s+infection|SSI)\b", re.I),
            "wound_infection"),
        (re.compile(
            r"\b(chyle\s+leak|chylous\s+fistula)\b", re.I),
            "chyle_leak"),
    ]

    def extract(self, note_row_id, research_id, note_type, note_text, note_date=None):
        results: list[EntityMatch] = []
        for pat, norm_comp in self._PATTERNS:
            for m in pat.finditer(note_text):
                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type="complication",
                    entity_value_raw=m.group(1) if m.lastindex else m.group(0),
                    entity_value_norm=norm_comp,
                    present_or_negated=self.check_negation(note_text, m.start()),
                    evidence_span=m.group(0),
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                ))
        return results


class MedicationExtractor(BaseExtractor):
    """Extract medication mentions with optional dose."""

    entity_domain = "medications"

    _PATTERNS: list[tuple[re.Pattern, str]] = [
        (re.compile(
            r"\b((?:levothyroxine|synthroid|levoxyl|l[\s-]*thyroxine)"
            r"(?:\s+(\d+\.?\d*)\s*(mcg|mg|ug|µg))?)\b", re.I),
            "levothyroxine"),
        (re.compile(
            r"\b((?:calcium\s+(?:carbonate|citrate)|caltrate|tums|oscal|citracal)"
            r"(?:\s+(\d+\.?\d*)\s*(mg))?)\b", re.I),
            "calcium_supplement"),
        (re.compile(
            r"\b((?:calcitriol|rocaltrol)"
            r"(?:\s+(\d+\.?\d*)\s*(mcg|mg|ug|µg))?)\b", re.I),
            "calcitriol"),
        (re.compile(
            r"\b((?:radioactive\s+iodine|RAI|I[\s-]*131|131[\s-]*I)"
            r"(?:\s+(\d+\.?\d*)\s*(mCi|GBq))?)\b", re.I),
            "rai_dose"),
    ]

    def extract(self, note_row_id, research_id, note_type, note_text, note_date=None):
        results: list[EntityMatch] = []
        for pat, norm_med in self._PATTERNS:
            for m in pat.finditer(note_text):
                raw = m.group(1)
                dose_part = ""
                if m.lastindex and m.lastindex >= 2 and m.group(2):
                    dose_part = f" {m.group(2)}"
                    if m.lastindex >= 3 and m.group(3):
                        dose_part += f" {m.group(3)}"
                norm_val = norm_med + dose_part.strip()

                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type="medication",
                    entity_value_raw=raw,
                    entity_value_norm=norm_val,
                    present_or_negated=self.check_negation(note_text, m.start()),
                    evidence_span=m.group(0),
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                ))
        return results


class ProblemListExtractor(BaseExtractor):
    """Extract comorbidity / diagnosis mentions from the problem list."""

    entity_domain = "problem_list"

    _PATTERNS: list[tuple[re.Pattern, str]] = [
        (re.compile(r"\b(hypertension|HTN)\b", re.I), "hypertension"),
        (re.compile(r"\b(type\s*2\s*diabet\w*|DM\s*2|T2DM|NIDDM)\b", re.I), "diabetes_type2"),
        (re.compile(r"\b(diabet\w+|IDDM)\b", re.I), "diabetes"),
        (re.compile(r"\b(obes\w+)\b", re.I), "obesity"),
        (re.compile(r"\b(coronary\s+artery\s+disease|CAD)\b", re.I), "CAD"),
        (re.compile(r"\b(atrial\s+fibrillat\w+|a[\s-]*fib)\b", re.I), "atrial_fibrillation"),
        (re.compile(r"\b(hypothyroid\w+)\b", re.I), "hypothyroidism"),
        (re.compile(r"\b(hyperthyroid\w+)\b", re.I), "hyperthyroidism"),
        (re.compile(r"\b(breast\s+(?:cancer|carcinoma))\b", re.I), "breast_cancer"),
        (re.compile(r"\b(lung\s+(?:cancer|carcinoma))\b", re.I), "lung_cancer"),
        (re.compile(r"\b(GERD|gastroesophageal\s+reflux)\b", re.I), "GERD"),
        (re.compile(r"\b(chronic\s+kidney|CKD|renal\s+insufficiency)\b", re.I), "CKD"),
        (re.compile(r"\b(depression|MDD)\b", re.I), "depression"),
        (re.compile(r"\b(asthma)\b", re.I), "asthma"),
        (re.compile(r"\b(COPD|chronic\s+obstructive)\b", re.I), "COPD"),
    ]

    def extract(self, note_row_id, research_id, note_type, note_text, note_date=None):
        results: list[EntityMatch] = []
        seen_norms: set[str] = set()
        for pat, norm_problem in self._PATTERNS:
            m = pat.search(note_text)
            if m and norm_problem not in seen_norms:
                seen_norms.add(norm_problem)
                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type="problem",
                    entity_value_raw=m.group(0),
                    entity_value_norm=norm_problem,
                    present_or_negated=self.check_negation(note_text, m.start()),
                    evidence_span=m.group(0),
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                ))
        return results


ALL_REGEX_EXTRACTORS: list[type[BaseExtractor]] = [
    StagingExtractor,
    GeneticsExtractor,
    ProcedureExtractor,
    ComplicationExtractor,
    MedicationExtractor,
    ProblemListExtractor,
]
