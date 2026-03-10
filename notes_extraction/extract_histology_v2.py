"""
Deep histology/pathology detail extractor (v2).

Parses pathology reports, synoptic text, and clinical notes for granular
histopathologic findings not covered by the base StagingExtractor.

Entity types produced:
    capsular_invasion, perineural_invasion, extranodal_extension,
    vascular_invasion_detail, lymphatic_invasion_detail, margin_status,
    consult_diagnosis, histology_subtype, multifocality, niftp,
    pdtc_features, aggressive_features, tumor_count,
    lymph_node_count, extrathyroidal_extension_detail
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

# -- Capsular invasion -------------------------------------------------------
_CAPSULAR_INV = re.compile(
    r"\b(capsul(?:ar|e)\s+(?:invasion|invad\w+|penetrat\w+|breach\w*|infiltrat\w+))\b",
    re.I,
)
_CAPSULAR_INV_PRESENT = re.compile(
    r"\b(capsul(?:ar|e)\s+invasion\s+(?:present|identified|noted))\b", re.I,
)
_CAPSULAR_INV_ABSENT = re.compile(
    r"\b(no\s+capsul(?:ar|e)\s+invasion)\b", re.I,
)

# -- Perineural invasion -----------------------------------------------------
_PERINEURAL = re.compile(
    r"\b(perineural\s+(?:invasion|invad\w+|infiltrat\w+))\b", re.I,
)

# -- Extranodal extension ----------------------------------------------------
_EXTRANODAL = re.compile(
    r"\b(extranodal\s+(?:extension|spread|involvement))\b", re.I,
)
_ENE_ABBREV = re.compile(r"\b(ENE)\b")

# -- Vascular invasion detail ------------------------------------------------
_VASCULAR_INV = re.compile(
    r"\b((?:lymph)?vascular\s+(?:invasion|invad\w+)|"
    r"angioinvasion|angiolymphatic\s+invasion)\b", re.I,
)
_VASC_MINIMAL = re.compile(
    r"\b(focal\s+vascular\s+invasion|minimal\s+vascular\s+invasion)\b", re.I,
)
_VASC_EXTENSIVE = re.compile(
    r"\b(extensive\s+vascular\s+invasion|"
    r"(?:widely|extensively)\s+(?:angio)?invasive)\b", re.I,
)

# -- Lymphatic invasion -------------------------------------------------------
_LYMPHATIC = re.compile(
    r"\b(lymphatic\s+(?:invasion|invad\w+|channel\s+invasion|infiltrat\w+))\b", re.I,
)

# -- Margin status ------------------------------------------------------------
_MARGIN_POS = re.compile(
    r"\b(positive\s+margin|margin\s*(?:is|are)?\s*positive|"
    r"margin\s+involved|margin\s+status\s*:?\s*positive)\b", re.I,
)
_MARGIN_NEG = re.compile(
    r"\b(negative\s+margin|margin\s*(?:is|are)?\s*(?:negative|clear|free|uninvolved)|"
    r"margin\s+status\s*:?\s*negative)\b", re.I,
)
_MARGIN_CLOSE = re.compile(
    r"\b(close\s+margin|margin\s*(?:is|are)?\s*close|"
    r"margin\s*<?\s*[012]\s*mm)\b", re.I,
)

# -- Consult / expert diagnosis -----------------------------------------------
_CONSULT = re.compile(
    r"\b(consult(?:ation)?\s+(?:diagnosis|opinion|review)|"
    r"(?:expert|outside|second\s+opinion)\s+(?:review|diagnosis|patholog\w+)|"
    r"reviewed\s+by\s+(?:expert|outside|consulting)\s+patholog\w+)\b", re.I,
)

# -- Histology subtypes / variants -------------------------------------------
_NIFTP = re.compile(
    r"\b(NIFTP|noninvasive\s+follicular\s+thyroid\s+neoplasm\s+"
    r"with\s+papillary[\s-]*like\s+nuclear\s+features)\b", re.I,
)
_PDTC = re.compile(
    r"\b(poorly\s+differentiated\s+(?:thyroid\s+)?(?:carcinoma|component)|PDTC)\b",
    re.I,
)
_AGGRESSIVE = re.compile(
    r"\b(anaplastic|undifferentiated|high[\s-]*grade|"
    r"(?:tall|columnar|hobnail|diffuse\s+sclerosing|insular)\s+"
    r"(?:cell\s+)?(?:variant|pattern|component|features))\b", re.I,
)
_FOLLICULAR_INVASION = re.compile(
    r"\b(minimally\s+invasive|widely\s+invasive|encapsulated)\b", re.I,
)

# -- Multifocality / tumor count ---------------------------------------------
_MULTIFOCAL = re.compile(
    r"\b(multifocal(?:ity)?|multi[\s-]*focal|multiple\s+foci|"
    r"(?:bilateral|bilobar)\s+(?:disease|tumors?))\b", re.I,
)
_TUMOR_COUNT = re.compile(
    r"\b(\d+)\s+(?:separate\s+)?(?:tumor|foc[iu]s|nodule|lesion)(?:s|es)?\s+"
    r"(?:of\s+)?(?:papillary|follicular|carcinoma|cancer)?\b", re.I,
)

# -- Lymph node counts -------------------------------------------------------
_LN_COUNT = re.compile(
    r"\b(\d+)\s+(?:of|out\s+of|/)\s*(\d+)\s+"
    r"(?:lymph\s+nodes?|nodes?)\s+"
    r"(?:positive|involved|with\s+(?:metasta|tumor|carcinoma))", re.I,
)

# -- ETE detail ---------------------------------------------------------------
_ETE_DETAIL = re.compile(
    r"\b((?:minimal|microscopic|gross|macroscopic|extensive)\s+"
    r"extrathyroidal\s+extension)\b", re.I,
)
_ETE_GENERIC = re.compile(
    r"\b(extrathyroidal\s+extension)\b", re.I,
)


class HistologyDetailExtractor(BaseExtractor):
    """Extract granular histopathologic findings from pathology text."""

    entity_domain = "histology_detail"

    def extract(
        self,
        note_row_id: str,
        research_id: int,
        note_type: str,
        note_text: str,
        note_date: str | None = None,
    ) -> list[EntityMatch]:
        results: list[EntityMatch] = []
        seen: set[tuple[str, int]] = set()
        if not note_text:
            return results

        def _add(
            m: re.Match,
            entity_type: str,
            norm: str,
            conf: float = 0.95,
        ) -> None:
            key = (entity_type, m.start())
            if key in seen:
                return
            seen.add(key)
            start = max(0, m.start() - 30)
            end = min(len(note_text), m.end() + 30)
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type=entity_type,
                entity_value_raw=m.group(0),
                entity_value_norm=norm,
                present_or_negated=self.check_negation(note_text, m.start()),
                confidence=conf,
                evidence_span=note_text[start:end],
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
                extraction_method="regex_histology_v2",
            ))

        # capsular invasion
        for m in _CAPSULAR_INV_PRESENT.finditer(note_text):
            _add(m, "capsular_invasion", "present")
        for m in _CAPSULAR_INV_ABSENT.finditer(note_text):
            _add(m, "capsular_invasion", "absent")
        for m in _CAPSULAR_INV.finditer(note_text):
            _add(m, "capsular_invasion", "mentioned")

        # perineural invasion
        for m in _PERINEURAL.finditer(note_text):
            _add(m, "perineural_invasion", "mentioned")

        # extranodal extension
        for m in _EXTRANODAL.finditer(note_text):
            _add(m, "extranodal_extension", "mentioned")
        for m in _ENE_ABBREV.finditer(note_text):
            _add(m, "extranodal_extension", "ENE", 0.85)

        # vascular invasion detail
        for m in _VASC_EXTENSIVE.finditer(note_text):
            _add(m, "vascular_invasion_detail", "extensive")
        for m in _VASC_MINIMAL.finditer(note_text):
            _add(m, "vascular_invasion_detail", "focal")
        for m in _VASCULAR_INV.finditer(note_text):
            _add(m, "vascular_invasion_detail", "mentioned")

        # lymphatic invasion
        for m in _LYMPHATIC.finditer(note_text):
            _add(m, "lymphatic_invasion_detail", "mentioned")

        # margin status
        for m in _MARGIN_POS.finditer(note_text):
            _add(m, "margin_status", "positive")
        for m in _MARGIN_NEG.finditer(note_text):
            _add(m, "margin_status", "negative")
        for m in _MARGIN_CLOSE.finditer(note_text):
            _add(m, "margin_status", "close")

        # consult / expert diagnosis
        for m in _CONSULT.finditer(note_text):
            start_ctx = max(0, m.end())
            end_ctx = min(len(note_text), m.end() + 200)
            snippet = note_text[start_ctx:end_ctx]
            _add(m, "consult_diagnosis", snippet.strip()[:120], 0.90)

        # NIFTP
        for m in _NIFTP.finditer(note_text):
            _add(m, "niftp", "NIFTP")

        # PDTC
        for m in _PDTC.finditer(note_text):
            _add(m, "pdtc_features", "PDTC")

        # aggressive features
        for m in _AGGRESSIVE.finditer(note_text):
            norm = m.group(0).lower().strip()
            _add(m, "aggressive_features", norm)

        # follicular invasion pattern
        for m in _FOLLICULAR_INVASION.finditer(note_text):
            _add(m, "histology_subtype", m.group(0).lower().strip(), 0.90)

        # multifocality
        for m in _MULTIFOCAL.finditer(note_text):
            _add(m, "multifocality", "multifocal")

        # tumor count
        for m in _TUMOR_COUNT.finditer(note_text):
            _add(m, "tumor_count", m.group(1), 0.90)

        # lymph node counts (positive/total)
        for m in _LN_COUNT.finditer(note_text):
            _add(m, "lymph_node_count", f"{m.group(1)}/{m.group(2)}", 0.95)

        # ETE detail
        for m in _ETE_DETAIL.finditer(note_text):
            norm = m.group(0).lower().strip()
            _add(m, "extrathyroidal_extension_detail", norm)
        for m in _ETE_GENERIC.finditer(note_text):
            _add(m, "extrathyroidal_extension_detail", "mentioned", 0.85)

        return results
