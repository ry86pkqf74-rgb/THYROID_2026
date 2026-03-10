"""
Deep imaging / nodule parser for ultrasound, CT, and MRI reports.

Extends BaseExtractor to extract nodule-level and exam-level findings
from radiology and clinical notes: size, composition, echogenicity,
shape, margins, calcifications, vascularity, TI-RADS, lymph nodes,
interval change, multinodular goiter, thyroiditis, extrathyroidal
extension, dominant nodule designation, and laterality.
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
# Compiled patterns
# ---------------------------------------------------------------------------

# --- nodule size ---

_SIZE_3AXIS = re.compile(
    r"(\d{1,3}(?:\.\d{1,2})?)\s*[xX×]\s*"
    r"(\d{1,3}(?:\.\d{1,2})?)\s*[xX×]\s*"
    r"(\d{1,3}(?:\.\d{1,2})?)\s*"
    r"(cm|mm)\b",
    re.IGNORECASE,
)

_SIZE_2AXIS = re.compile(
    r"(\d{1,3}(?:\.\d{1,2})?)\s*[xX×]\s*"
    r"(\d{1,3}(?:\.\d{1,2})?)\s*"
    r"(cm|mm)\b",
    re.IGNORECASE,
)

_SIZE_1AXIS = re.compile(
    r"\b(\d{1,3}(?:\.\d{1,2})?)\s*(cm|mm)\s+"
    r"(?:thyroid\s+)?(?:nodule|mass|lesion)\b",
    re.IGNORECASE,
)

# --- composition ---

_COMPOSITION_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bspongiform\b", re.I), "spongiform"),
    (re.compile(r"\bmixed\s+cystic\s+and\s+solid\b", re.I), "mixed_cystic_and_solid"),
    (re.compile(
        r"\b(?:mixed\s+(?:solid\s+and\s+cystic|cystic[\s/]+solid|echogenicity))\b",
        re.I),
     "mixed_cystic_and_solid"),
    (re.compile(r"\bpredominantly\s+solid\b", re.I), "predominantly_solid"),
    (re.compile(r"\bpredominantly\s+cystic\b", re.I), "predominantly_cystic"),
    (re.compile(
        r"\b(?:solid\s+(?:nodule|mass|lesion|component|appearance))\b",
        re.I),
     "solid"),
    (re.compile(
        r"\b(?:(?:completely|entirely|purely)\s+(?:cystic|anechoic)|cystic\s+(?:nodule|mass|lesion))\b",
        re.I),
     "cystic"),
]

# --- echogenicity ---

_ECHOGENICITY_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(?:very|markedly)\s+hypoechoic\b", re.I), "markedly_hypoechoic"),
    (re.compile(r"\bhypoechoic\b", re.I), "hypoechoic"),
    (re.compile(r"\bisoechoic\b", re.I), "isoechoic"),
    (re.compile(r"\bhyperechoic\b", re.I), "hyperechoic"),
    (re.compile(r"\banechoic\b", re.I), "anechoic"),
]

# --- shape ---

_SHAPE_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\btaller\s+than\s+wide\b", re.I), "taller_than_wide"),
    (re.compile(r"\bwider\s+than\s+tall\b", re.I), "wider_than_tall"),
    (re.compile(
        r"\b(?:AP\s*(?:dimension|diameter)\s*(?:>|greater\s+than)\s*transverse)\b",
        re.I),
     "taller_than_wide"),
]

# --- margins ---

_MARGINS_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bmicrolobulated\b", re.I), "microlobulated"),
    (re.compile(r"\bspiculated\b", re.I), "spiculated"),
    (re.compile(r"\b(?:ill[\s-]*defined|poorly[\s-]*defined)\b", re.I), "ill_defined"),
    (re.compile(r"\birregular\s+(?:margin|border|contour)\w*\b", re.I), "irregular"),
    (re.compile(r"\birregular\b", re.I), "irregular"),
    (re.compile(r"\blobulated\b", re.I), "lobulated"),
    (re.compile(
        r"\b(?:well[\s-]*defined|smooth|well[\s-]*circumscribed|sharp)\s*(?:margin|border|contour)?\w*\b",
        re.I),
     "well_defined"),
]

# --- calcifications ---

_CALCIFICATION_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bmicrocalcification\w*\b", re.I), "microcalcifications"),
    (re.compile(r"\bmacrocalcification\w*\b", re.I), "macrocalcifications"),
    (re.compile(
        r"\b(?:peripheral|rim)\s+calcification\w*\b", re.I),
     "peripheral_calcifications"),
    (re.compile(r"\bcoarse\s+calcification\w*\b", re.I), "coarse_calcifications"),
    (re.compile(r"\beggshell\s+calcification\w*\b", re.I), "eggshell_calcifications"),
    (re.compile(
        r"\bpunctate\s+echogenic\s+foci\b", re.I),
     "punctate_echogenic_foci"),
    (re.compile(r"\bcalcification\w*\b", re.I), "calcifications"),
]

# --- vascularity ---

_VASCULARITY_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bhypervascular\w*\b", re.I), "hypervascular"),
    (re.compile(r"\bavascular\b", re.I), "avascular"),
    (re.compile(
        r"\b(?:peripheral\s+(?:vascularity|blood\s+flow|flow))\b", re.I),
     "peripheral_vascularity"),
    (re.compile(
        r"\b(?:intranodular|internal|central)\s+(?:vascularity|blood\s+flow|flow)\b",
        re.I),
     "intranodular_vascularity"),
    (re.compile(
        r"\b(?:increased|marked)\s+(?:vascularity|blood\s+flow|flow)\b",
        re.I),
     "increased_vascularity"),
]

# --- TI-RADS ---

_TIRADS = re.compile(
    r"\b(?:TI[\s-]*RADS|ACR\s+TI[\s-]*RADS)\s*(?:category\s*)?:?\s*"
    r"(?:TR)?([1-5])\b",
    re.IGNORECASE,
)

_TIRADS_TR = re.compile(
    r"\bTR([1-5])\b",
    re.IGNORECASE,
)

# --- suspicious lymph nodes ---

_SUSP_LN_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(
        r"\b(?:suspicious|pathologic(?:al)?|abnormal|atypical)\s+"
        r"(?:cervical\s+)?(?:lymph\s+node|lymphadenopathy)\w*\b",
        re.I),
     "suspicious_lymph_node"),
    (re.compile(
        r"\b(?:cervical\s+)?lymphadenopathy\b", re.I),
     "lymphadenopathy"),
    (re.compile(
        r"\bcystic\s+(?:cervical\s+)?lymph\s+node\w*\b", re.I),
     "cystic_lymph_node"),
    (re.compile(
        r"\b(?:microcalcification|calcification)\w*\s+"
        r"(?:in|within)\s+(?:(?:the\s+)?(?:cervical\s+)?lymph\s+node\w*)\b",
        re.I),
     "calcified_lymph_node"),
    (re.compile(
        r"\blymph\s+node\w*\s+(?:with|containing|demonstrating)\s+"
        r"(?:microcalcification|calcification)\w*\b",
        re.I),
     "calcified_lymph_node"),
]

# --- interval change ---

_INTERVAL_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(
        r"\b(?:new\s+(?:thyroid\s+)?(?:nodule|mass|lesion))\b", re.I),
     "new_nodule"),
    (re.compile(
        r"\b(?:interval\s+(?:growth|increase|enlargement)|enlarging|growing|"
        r"increased\s+in\s+size|interval\s+increase\s+in\s+size)\b",
        re.I),
     "increased"),
    (re.compile(
        r"\b(?:decreased\s+in\s+size|interval\s+decrease|"
        r"smaller|shrunk|regress\w*)\b",
        re.I),
     "decreased"),
    (re.compile(
        r"\b(?:stable\s+(?:in\s+size|appearance|thyroid)?|"
        r"unchanged|no\s+(?:significant\s+)?(?:change|interval\s+change))\b",
        re.I),
     "stable"),
]

# --- multinodular goiter ---

_MNG = re.compile(
    r"\b(?:multinodular\s+goiter|MNG|multi[\s-]*nodular\s+goitre?)\b",
    re.IGNORECASE,
)

# --- thyroiditis ---

_THYROIDITIS_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(
        r"\b(?:Hashimoto(?:'?s)?(?:\s+thyroiditis)?)\b", re.I),
     "hashimoto_thyroiditis"),
    (re.compile(
        r"\bheterogeneous\s+echo(?:texture|pattern)\s+"
        r"(?:consistent\s+with|suggestive\s+of|compatible\s+with)\s+thyroiditis\b",
        re.I),
     "thyroiditis_imaging"),
    (re.compile(
        r"\bdiffuse\s+thyroiditis\b", re.I),
     "diffuse_thyroiditis"),
    (re.compile(
        r"\bthyroiditis\b", re.I),
     "thyroiditis"),
]

# --- extrathyroidal extension on imaging ---

_IMAGING_ETE_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(
        r"\b(?:extrathyroidal\s+extension|extra[\s-]*thyroidal\s+extension|ETE)\b",
        re.I),
     "extrathyroidal_extension"),
    (re.compile(
        r"\b(?:extension\s+beyond\s+(?:the\s+)?(?:thyroid\s+)?capsule|"
        r"capsular\s+(?:invasion|breach|disruption))\b",
        re.I),
     "capsular_invasion"),
    (re.compile(
        r"\b(?:invad(?:es?|ing|ed)\s+(?:the\s+)?(?:strap\s+muscle|trachea|esophagus|"
        r"recurrent\s+laryngeal|surrounding\s+(?:tissue|structure)))\b",
        re.I),
     "local_invasion"),
]

# --- dominant nodule ---

_DOMINANT = re.compile(
    r"\b(?:dominant|largest|index)\s+(?:thyroid\s+)?(?:nodule|mass|lesion)\b",
    re.IGNORECASE,
)

# --- laterality ---

_LATERALITY_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(?:right\s+(?:thyroid\s+)?lobe)\b", re.I), "right_lobe"),
    (re.compile(r"\b(?:left\s+(?:thyroid\s+)?lobe)\b", re.I), "left_lobe"),
    (re.compile(r"\bisthmus\b", re.I), "isthmus"),
    (re.compile(r"\bbilateral\b", re.I), "bilateral"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_cm(value: float, unit: str) -> float:
    """Normalize a measurement to centimetres."""
    if unit.lower() == "mm":
        return round(value / 10.0, 2)
    return round(value, 2)


def _fmt_cm(val: float) -> str:
    """Format a cm value, stripping unnecessary trailing zeros."""
    return f"{val:g}"


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------

class ImagingNoduleExtractor(BaseExtractor):
    """Deep parser for imaging / nodule findings in radiology and clinical notes."""

    entity_domain = "imaging_detail"

    def extract(self, note_row_id, research_id, note_type, note_text, note_date=None):
        results: list[EntityMatch] = []
        if not note_text:
            return results

        self._extract_size(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_composition(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_echogenicity(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_shape(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_margins(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_calcifications(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_vascularity(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_tirads(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_suspicious_ln(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_interval_change(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_mng(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_thyroiditis(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_imaging_ete(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_dominant(results, note_row_id, research_id, note_type, note_text, note_date)
        self._extract_laterality(results, note_row_id, research_id, note_type, note_text, note_date)

        return results

    # ----- helpers -----

    def _make(self, note_row_id, research_id, note_type, note_text, note_date,
              m, *, entity_type, raw, norm, confidence=0.85):
        return EntityMatch(
            research_id=research_id,
            note_row_id=note_row_id,
            note_type=note_type,
            entity_type=entity_type,
            entity_value_raw=raw,
            entity_value_norm=norm,
            present_or_negated=self.check_negation(note_text, m.start()),
            confidence=confidence,
            evidence_span=m.group(0),
            evidence_start=m.start(),
            evidence_end=m.end(),
            entity_date=extract_nearby_date(note_text, m.start(), m.end()),
            note_date=note_date,
            extraction_method="regex_imaging_v2",
        )

    # ----- nodule size -----

    def _extract_size(self, results, note_row_id, research_id, note_type, note_text, note_date):
        seen_starts: set[int] = set()

        for m in _SIZE_3AXIS.finditer(note_text):
            seen_starts.add(m.start())
            a = _to_cm(float(m.group(1)), m.group(4))
            b = _to_cm(float(m.group(2)), m.group(4))
            c = _to_cm(float(m.group(3)), m.group(4))
            norm = f"{_fmt_cm(a)}x{_fmt_cm(b)}x{_fmt_cm(c)}cm"
            results.append(self._make(
                note_row_id, research_id, note_type, note_text, note_date, m,
                entity_type="nodule_size", raw=m.group(0), norm=norm,
                confidence=0.95,
            ))

        for m in _SIZE_2AXIS.finditer(note_text):
            if m.start() in seen_starts:
                continue
            seen_starts.add(m.start())
            a = _to_cm(float(m.group(1)), m.group(3))
            b = _to_cm(float(m.group(2)), m.group(3))
            norm = f"{_fmt_cm(a)}x{_fmt_cm(b)}cm"
            results.append(self._make(
                note_row_id, research_id, note_type, note_text, note_date, m,
                entity_type="nodule_size", raw=m.group(0), norm=norm,
                confidence=0.95,
            ))

        for m in _SIZE_1AXIS.finditer(note_text):
            if m.start() in seen_starts:
                continue
            a = _to_cm(float(m.group(1)), m.group(2))
            norm = f"{_fmt_cm(a)}cm"
            results.append(self._make(
                note_row_id, research_id, note_type, note_text, note_date, m,
                entity_type="nodule_size", raw=m.group(0), norm=norm,
                confidence=0.85,
            ))

    # ----- composition -----

    def _extract_composition(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _COMPOSITION_MAP:
            for m in pat.finditer(note_text):
                results.append(self._make(
                    note_row_id, research_id, note_type, note_text, note_date, m,
                    entity_type="composition", raw=m.group(0), norm=norm,
                    confidence=0.85,
                ))

    # ----- echogenicity -----

    def _extract_echogenicity(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _ECHOGENICITY_MAP:
            for m in pat.finditer(note_text):
                results.append(self._make(
                    note_row_id, research_id, note_type, note_text, note_date, m,
                    entity_type="echogenicity", raw=m.group(0), norm=norm,
                    confidence=0.85,
                ))

    # ----- shape -----

    def _extract_shape(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _SHAPE_MAP:
            for m in pat.finditer(note_text):
                results.append(self._make(
                    note_row_id, research_id, note_type, note_text, note_date, m,
                    entity_type="nodule_shape", raw=m.group(0), norm=norm,
                    confidence=0.85,
                ))

    # ----- margins -----

    def _extract_margins(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _MARGINS_MAP:
            for m in pat.finditer(note_text):
                results.append(self._make(
                    note_row_id, research_id, note_type, note_text, note_date, m,
                    entity_type="nodule_margins", raw=m.group(0), norm=norm,
                    confidence=0.85,
                ))

    # ----- calcifications -----

    def _extract_calcifications(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _CALCIFICATION_MAP:
            for m in pat.finditer(note_text):
                results.append(self._make(
                    note_row_id, research_id, note_type, note_text, note_date, m,
                    entity_type="calcifications", raw=m.group(0), norm=norm,
                    confidence=0.85,
                ))

    # ----- vascularity -----

    def _extract_vascularity(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _VASCULARITY_MAP:
            for m in pat.finditer(note_text):
                results.append(self._make(
                    note_row_id, research_id, note_type, note_text, note_date, m,
                    entity_type="vascularity", raw=m.group(0), norm=norm,
                    confidence=0.85,
                ))

    # ----- TI-RADS -----

    def _extract_tirads(self, results, note_row_id, research_id, note_type, note_text, note_date):
        seen_starts: set[int] = set()

        for m in _TIRADS.finditer(note_text):
            seen_starts.add(m.start())
            score = m.group(1)
            results.append(self._make(
                note_row_id, research_id, note_type, note_text, note_date, m,
                entity_type="tirads_score", raw=m.group(0),
                norm=f"TR{score}",
                confidence=0.95,
            ))

        for m in _TIRADS_TR.finditer(note_text):
            if m.start() in seen_starts:
                continue
            score = m.group(1)
            results.append(self._make(
                note_row_id, research_id, note_type, note_text, note_date, m,
                entity_type="tirads_score", raw=m.group(0),
                norm=f"TR{score}",
                confidence=0.85,
            ))

    # ----- suspicious lymph nodes -----

    def _extract_suspicious_ln(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _SUSP_LN_MAP:
            for m in pat.finditer(note_text):
                results.append(self._make(
                    note_row_id, research_id, note_type, note_text, note_date, m,
                    entity_type="suspicious_lymph_node", raw=m.group(0), norm=norm,
                    confidence=0.85,
                ))

    # ----- interval change -----

    def _extract_interval_change(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _INTERVAL_MAP:
            for m in pat.finditer(note_text):
                results.append(self._make(
                    note_row_id, research_id, note_type, note_text, note_date, m,
                    entity_type="interval_change", raw=m.group(0), norm=norm,
                    confidence=0.85,
                ))

    # ----- multinodular goiter -----

    def _extract_mng(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for m in _MNG.finditer(note_text):
            results.append(self._make(
                note_row_id, research_id, note_type, note_text, note_date, m,
                entity_type="multinodular_goiter", raw=m.group(0),
                norm="multinodular_goiter",
                confidence=0.95,
            ))

    # ----- thyroiditis -----

    def _extract_thyroiditis(self, results, note_row_id, research_id, note_type, note_text, note_date):
        seen_starts: set[int] = set()
        for pat, norm in _THYROIDITIS_MAP:
            for m in pat.finditer(note_text):
                if m.start() in seen_starts:
                    continue
                seen_starts.add(m.start())
                conf = 0.95 if norm == "hashimoto_thyroiditis" else 0.85
                results.append(self._make(
                    note_row_id, research_id, note_type, note_text, note_date, m,
                    entity_type="thyroiditis", raw=m.group(0), norm=norm,
                    confidence=conf,
                ))

    # ----- extrathyroidal extension on imaging -----

    def _extract_imaging_ete(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _IMAGING_ETE_MAP:
            for m in pat.finditer(note_text):
                results.append(self._make(
                    note_row_id, research_id, note_type, note_text, note_date, m,
                    entity_type="imaging_ete", raw=m.group(0), norm=norm,
                    confidence=0.85,
                ))

    # ----- dominant nodule -----

    def _extract_dominant(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for m in _DOMINANT.finditer(note_text):
            results.append(self._make(
                note_row_id, research_id, note_type, note_text, note_date, m,
                entity_type="dominant_nodule", raw=m.group(0),
                norm="dominant_nodule",
                confidence=0.85,
            ))

    # ----- laterality -----

    def _extract_laterality(self, results, note_row_id, research_id, note_type, note_text, note_date):
        for pat, norm in _LATERALITY_MAP:
            for m in pat.finditer(note_text):
                results.append(self._make(
                    note_row_id, research_id, note_type, note_text, note_date, m,
                    entity_type="nodule_laterality", raw=m.group(0), norm=norm,
                    confidence=0.95,
                ))
