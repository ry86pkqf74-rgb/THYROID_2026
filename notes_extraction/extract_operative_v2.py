"""
Deep operative-note parser for thyroid surgery detail extraction.

Extends BaseExtractor to capture operative findings beyond basic procedure
mentions: RLN status, nerve monitoring, parathyroid management, gross
invasion, EBL, drain placement, specimen handling, and intraoperative
complications.  Complements ProcedureExtractor from extract_regex.py.
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

# ── confidence tiers ─────────────────────────────────────────────
CONF_EXPLICIT = 0.95
CONF_CONTEXTUAL = 0.85
CONF_INFERRED = 0.70

# ── pattern type alias ───────────────────────────────────────────
# (compiled regex, normalised value, entity_type, confidence)
_PatternRow = tuple[re.Pattern[str], str, str, float]


def _ctx(text: str, start: int, end: int, margin: int = 60) -> str:
    """Return a context window around a match for evidence_span."""
    lo = max(0, start - margin)
    hi = min(len(text), end + margin)
    return text[lo:hi]


# =====================================================================
#  Pattern banks – one list per clinical domain
# =====================================================================

_RLN_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b((?:recurrent\s+laryngeal\s+nerve|RLN)\s+"
        r"(?:was\s+)?(?:identified|visuali[sz]ed|preserved|intact|protected))\b",
        re.I),
     "rln_preserved", "rln_finding", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:recurrent\s+laryngeal\s+nerve|RLN)\s+"
        r"(?:was\s+)?(?:injure?d|sacrifice?d|transect\w*|divided|not\s+identified))\b",
        re.I),
     "rln_injured", "rln_finding", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:recurrent\s+laryngeal\s+nerve|RLN)\s+"
        r"(?:was\s+)?(?:stretch\w*|attenuated|thinned|adherent))\b",
        re.I),
     "rln_stretched", "rln_finding", CONF_CONTEXTUAL),
    (re.compile(
        r"\b((?:bilateral|right|left)\s+(?:recurrent\s+laryngeal\s+nerves?|RLNs?)\s+"
        r"(?:were\s+)?(?:identified|preserved|intact))\b",
        re.I),
     "rln_bilateral_preserved", "rln_finding", CONF_EXPLICIT),
    (re.compile(
        r"\b(stimulation\s+threshold\s*(?:of\s*)?\d+\s*(?:mA|milliamps?))\b",
        re.I),
     "stimulation_threshold", "rln_finding", CONF_EXPLICIT),
]

_NERVE_MONITOR_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b((?:intraoperative\s+)?nerve\s+(?:integrity\s+)?monit\w+)\b",
        re.I),
     "nerve_monitoring_used", "nerve_monitoring", CONF_EXPLICIT),
    (re.compile(r"\b(IONM)\b"),
     "ionm", "nerve_monitoring", CONF_EXPLICIT),
    (re.compile(r"\b(NIM\s+(?:3\.0|monitor\w*|system|device))\b", re.I),
     "nim_device", "nerve_monitoring", CONF_EXPLICIT),
    (re.compile(r"\b(NIM)\b"),
     "nim", "nerve_monitoring", CONF_CONTEXTUAL),
    (re.compile(
        r"\b(EMG\s+(?:endotracheal|ET)\s+tube)\b",
        re.I),
     "emg_tube", "nerve_monitoring", CONF_EXPLICIT),
    (re.compile(
        r"\b(nerve\s+stimulat(?:or|ion)\s+(?:was\s+)?used)\b",
        re.I),
     "nerve_stimulator_used", "nerve_monitoring", CONF_EXPLICIT),
]

_PARATHYROID_AUTOGRAFT_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b(parathyroid\s+(?:gland\s+)?(?:was\s+)?"
        r"auto\s*(?:transplant|graft)\w*)\b",
        re.I),
     "parathyroid_autotransplant", "parathyroid_autograft", CONF_EXPLICIT),
    (re.compile(
        r"\b(auto\s*(?:transplant|graft)\w*\s+(?:of\s+)?parathyroid)\b",
        re.I),
     "parathyroid_autotransplant", "parathyroid_autograft", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:reimplant|autoimplant)\w*\s+(?:in(?:to)?|to)\s+"
        r"(?:the\s+)?(?:sternocleidomastoid|SCM|forearm|strap\s+muscle|"
        r"brachioradialis))\b",
        re.I),
     "autograft_site", "parathyroid_autograft", CONF_EXPLICIT),
    (re.compile(
        r"\b((\d)\s+(?:parathyroid\s+)?glands?\s+"
        r"(?:were\s+)?auto\s*(?:transplant|graft)\w*)\b",
        re.I),
     "autograft_count", "parathyroid_autograft", CONF_EXPLICIT),
]

_PARATHYROID_MGMT_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b(parathyroid\s+glands?\s+(?:were\s+)?(?:identified|visuali[sz]ed|"
        r"preserved|dissected\s+free))\b",
        re.I),
     "parathyroid_identified", "parathyroid_management", CONF_EXPLICIT),
    (re.compile(
        r"\b((\d)\s+parathyroid\s+glands?\s+(?:were\s+)?"
        r"(?:identified|preserved|visuali[sz]ed))\b",
        re.I),
     "parathyroid_count_identified", "parathyroid_management", CONF_EXPLICIT),
    (re.compile(
        r"\b(parathyroid\s+(?:gland\s+)?(?:was\s+)?"
        r"(?:inadvertent\w*\s+)?(?:removed|excised|resected))\b",
        re.I),
     "parathyroid_removed", "parathyroid_management", CONF_EXPLICIT),
    (re.compile(
        r"\b(parathyroid\s+(?:gland\s+)?(?:was\s+)?devasculariz\w*)\b",
        re.I),
     "parathyroid_devascularized", "parathyroid_management", CONF_EXPLICIT),
    (re.compile(
        r"\b(parathyroid\s+(?:gland\s+)?(?:was\s+)?reimplant\w*)\b",
        re.I),
     "parathyroid_reimplanted", "parathyroid_management", CONF_EXPLICIT),
    (re.compile(
        r"\b(parathyroid\s+(?:gland\s+)?(?:appear\w*|seem\w*)\s+"
        r"(?:viable|well[\s-]*perfused|healthy))\b",
        re.I),
     "parathyroid_viable", "parathyroid_management", CONF_CONTEXTUAL),
]

_GROSS_INVASION_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b((?:tumor|mass|lesion|nodule)\s+(?:was\s+)?"
        r"(?:adherent|invading|infiltrating|abutting|inseparable)\s+"
        r"(?:to\s+|into\s+)?(?:the\s+)?"
        r"(?:trachea|esophag\w+|strap\s+muscles?|RLN|recurrent\s+laryngeal"
        r"|carotid|jugular|prevertebral|larynx|mediastin\w+))\b",
        re.I),
     "gross_invasion", "gross_invasion", CONF_EXPLICIT),
    (re.compile(
        r"\b(gross\s+extrathyroidal\s+extension)\b",
        re.I),
     "gross_ete", "gross_invasion", CONF_EXPLICIT),
    (re.compile(
        r"\b(extrathyroidal\s+extension\s+(?:was\s+)?(?:present|noted|seen|"
        r"identified|grossly\s+apparent))\b",
        re.I),
     "ete_present", "gross_invasion", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:invad\w+|infiltrat\w+)\s+(?:the\s+)?(?:trachea|esophag\w+|"
        r"strap\s+muscles?|RLN|recurrent\s+laryngeal|carotid|jugular"
        r"|prevertebral|larynx))\b",
        re.I),
     "structure_invasion", "gross_invasion", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:tumor|mass)\s+(?:was\s+)?(?:densely\s+)?adherent\s+to\s+"
        r"(?:surrounding|adjacent)\s+(?:structures?|tissue))\b",
        re.I),
     "adherent_to_structures", "gross_invasion", CONF_CONTEXTUAL),
]

_STRAP_MUSCLE_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b(strap\s+muscles?\s+(?:were?\s+)?(?:invaded|resected|excised"
        r"|removed|sacrificed|divided|taken))\b",
        re.I),
     "strap_resected", "strap_muscle", CONF_EXPLICIT),
    (re.compile(
        r"\b(strap\s+muscles?\s+(?:were?\s+)?(?:adherent|involved|"
        r"infiltrated))\b",
        re.I),
     "strap_invaded", "strap_muscle", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:tumor|mass)\s+(?:was\s+)?(?:adherent|invading)\s+"
        r"(?:to\s+|into\s+)?(?:the\s+)?strap\s+muscles?)\b",
        re.I),
     "strap_invaded", "strap_muscle", CONF_EXPLICIT),
    (re.compile(
        r"\b(strap\s+muscles?\s+(?:were?\s+)?(?:preserved|retracted|"
        r"dissected\s+free))\b",
        re.I),
     "strap_preserved", "strap_muscle", CONF_CONTEXTUAL),
]

_TRACHEAL_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b(trache(?:a|al)\s+(?:was\s+)?(?:invaded|infiltrat\w+|"
        r"involved))\b",
        re.I),
     "tracheal_invasion", "tracheal_involvement", CONF_EXPLICIT),
    (re.compile(
        r"\b(trache?al\s+(?:shav\w+|peel\w*|window\s+resect\w*))\b",
        re.I),
     "tracheal_shave", "tracheal_involvement", CONF_EXPLICIT),
    (re.compile(
        r"\b(trache?al\s+(?:resect\w+|segmental\s+resect\w+))\b",
        re.I),
     "tracheal_resection", "tracheal_involvement", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:tumor|mass)\s+(?:was\s+)?(?:shaved|dissected)\s+"
        r"(?:off|from|away\s+from)\s+(?:the\s+)?trache?a)\b",
        re.I),
     "tracheal_shave", "tracheal_involvement", CONF_EXPLICIT),
    (re.compile(
        r"\b(trache?a\s+(?:was\s+)?(?:adherent|intact|uninvolved))\b",
        re.I),
     "trachea_intact", "tracheal_involvement", CONF_CONTEXTUAL),
]

_ESOPHAGEAL_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b(esophag\w+\s+(?:was\s+)?(?:invaded|infiltrat\w+|involved))\b",
        re.I),
     "esophageal_invasion", "esophageal_involvement", CONF_EXPLICIT),
    (re.compile(
        r"\b(esophag\w+\s+(?:was\s+)?(?:adherent|abutting))\b",
        re.I),
     "esophageal_adherent", "esophageal_involvement", CONF_CONTEXTUAL),
    (re.compile(
        r"\b((?:tumor|mass)\s+(?:was\s+)?(?:adherent|invading)\s+"
        r"(?:to\s+|into\s+)?(?:the\s+)?esophag\w+)\b",
        re.I),
     "esophageal_invasion", "esophageal_involvement", CONF_EXPLICIT),
    (re.compile(
        r"\b(esophag\w+\s+(?:was\s+)?(?:intact|uninvolved|preserved))\b",
        re.I),
     "esophagus_intact", "esophageal_involvement", CONF_CONTEXTUAL),
]

_REOPERATIVE_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b(re[\s-]*operat\w+\s+(?:field|neck|case|exploration))\b",
        re.I),
     "reoperative", "reoperative_field", CONF_EXPLICIT),
    (re.compile(
        r"\b(revision\s+(?:thyroidectom\w*|neck\s+dissect\w*|surgery))\b",
        re.I),
     "revision_surgery", "reoperative_field", CONF_EXPLICIT),
    (re.compile(
        r"\b(redo\s+(?:thyroidectom\w*|neck\s+dissect\w*|surgery"
        r"|exploration))\b",
        re.I),
     "redo_surgery", "reoperative_field", CONF_EXPLICIT),
    (re.compile(
        r"\b(scarred\s+(?:operative\s+)?field)\b",
        re.I),
     "scarred_field", "reoperative_field", CONF_CONTEXTUAL),
    (re.compile(
        r"\b(previous\s+(?:thyroid\s+)?surgery)\b",
        re.I),
     "previous_surgery", "reoperative_field", CONF_CONTEXTUAL),
    (re.compile(
        r"\b(prior\s+(?:thyroidectomy|neck\s+dissection|surgery))\b",
        re.I),
     "prior_surgery", "reoperative_field", CONF_CONTEXTUAL),
    (re.compile(
        r"\b(significant\s+(?:scarring|adhesions|fibrosis)\s+"
        r"(?:from|due\s+to|related\s+to)\s+(?:prior|previous)\s+surgery)\b",
        re.I),
     "post_surgical_scarring", "reoperative_field", CONF_INFERRED),
]

_EBL_PATTERN = re.compile(
    r"\b(?:(?:estimated\s+)?blood\s+loss|EBL)\s*"
    r"(?:was\s+|of\s+|:?\s*(?:approximately\s+|approx\.?\s+)?)"
    r"(\d{1,5})\s*(?:mL|cc|ml)\b",
    re.I,
)

_DRAIN_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b((?:Jackson[\s-]*Pratt|JP|Penrose|Blake|closed[\s-]*suction)"
        r"\s+drain\s+(?:was\s+)?(?:placed|left|inserted))\b",
        re.I),
     "drain_placed", "drain_placement", CONF_EXPLICIT),
    (re.compile(
        r"\b(drain\s+(?:was\s+)?(?:placed|left|inserted)\s+"
        r"(?:in|within|through)\s+(?:the\s+)?(?:wound|neck|operative\s+"
        r"bed|thyroid\s+bed))\b",
        re.I),
     "drain_placed", "drain_placement", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:a|one|two|1|2)\s+(?:Jackson[\s-]*Pratt|JP|Penrose|Blake)"
        r"\s+drains?)\b",
        re.I),
     "drain_placed", "drain_placement", CONF_CONTEXTUAL),
    (re.compile(
        r"\b(no\s+drain\s+(?:was\s+)?(?:placed|left|used))\b",
        re.I),
     "no_drain", "drain_placement", CONF_EXPLICIT),
]

_SPECIMEN_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b(specimen\s+(?:was\s+)?(?:sent|submitted)\s+"
        r"(?:to|for)\s+(?:patholog\w+|permanent\s+section))\b",
        re.I),
     "specimen_to_pathology", "specimen_detail", CONF_EXPLICIT),
    (re.compile(
        r"\b(frozen\s+section\s+(?:was\s+)?(?:sent|performed|obtained"
        r"|submitted|requested))\b",
        re.I),
     "frozen_section_sent", "specimen_detail", CONF_EXPLICIT),
    (re.compile(
        r"\b(frozen\s+section\s+(?:result|showed|revealed|demonstrated"
        r"|confirmed|returned|was\s+consistent))\b",
        re.I),
     "frozen_section_result", "specimen_detail", CONF_EXPLICIT),
    (re.compile(
        r"\b(specimen\s+(?:was\s+)?(?:oriented|marked|labeled|tagged))\b",
        re.I),
     "specimen_oriented", "specimen_detail", CONF_CONTEXTUAL),
    (re.compile(
        r"\b((?:right|left|superior|inferior)\s+(?:lobe|thyroid)\s+"
        r"specimen)\b",
        re.I),
     "specimen_laterality", "specimen_detail", CONF_CONTEXTUAL),
]

_BERRY_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b(Berry(?:'?s)?\s+ligament\s+(?:was\s+)?"
        r"(?:dissected|divided|ligated|carefully\s+dissected"
        r"|taken\s+down|freed))\b",
        re.I),
     "berry_ligament_dissected", "berry_ligament", CONF_EXPLICIT),
    (re.compile(
        r"\b(ligament\s+of\s+Berry\s+(?:was\s+)?"
        r"(?:dissected|divided|ligated|taken\s+down))\b",
        re.I),
     "berry_ligament_dissected", "berry_ligament", CONF_EXPLICIT),
    (re.compile(
        r"\b(Berry(?:'?s)?\s+ligament)\b",
        re.I),
     "berry_ligament_mentioned", "berry_ligament", CONF_INFERRED),
]

_INTRAOP_COMPLICATION_PATTERNS: list[_PatternRow] = [
    (re.compile(
        r"\b((?:significant|brisk|uncontrolled|unexpected|arterial|venous)"
        r"\s+(?:bleeding|hemorrhag\w+))\b",
        re.I),
     "intraop_bleeding", "intraop_complication", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:inadvertent|accidental|unintentional|iatrogenic)\s+"
        r"(?:injury|damage|laceration|transection)\s+"
        r"(?:to\s+|of\s+)?(?:the\s+)?"
        r"(?:RLN|recurrent\s+laryngeal|trachea|esophag\w+|thoracic\s+duct"
        r"|jugular|carotid|parathyroid))\b",
        re.I),
     "inadvertent_injury", "intraop_complication", CONF_EXPLICIT),
    (re.compile(
        r"\b(conver(?:ted|sion)\s+(?:to|from)\s+"
        r"(?:open|total\s+thyroidectom\w*|bilateral))\b",
        re.I),
     "conversion", "intraop_complication", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:pneumothorax|air\s+leak)\s+"
        r"(?:was\s+)?(?:noted|identified|occurred))\b",
        re.I),
     "pneumothorax", "intraop_complication", CONF_EXPLICIT),
    (re.compile(
        r"\b(thoracic\s+duct\s+(?:injury|leak|transection))\b",
        re.I),
     "thoracic_duct_injury", "intraop_complication", CONF_EXPLICIT),
]


# =====================================================================
#  Main extractor
# =====================================================================

class OperativeDetailExtractor(BaseExtractor):
    """Deep parser for operative-note findings beyond procedure names."""

    entity_domain = "operative_detail"

    _DOMAIN_PATTERNS: list[list[_PatternRow]] = [
        _RLN_PATTERNS,
        _NERVE_MONITOR_PATTERNS,
        _PARATHYROID_AUTOGRAFT_PATTERNS,
        _PARATHYROID_MGMT_PATTERNS,
        _GROSS_INVASION_PATTERNS,
        _STRAP_MUSCLE_PATTERNS,
        _TRACHEAL_PATTERNS,
        _ESOPHAGEAL_PATTERNS,
        _REOPERATIVE_PATTERNS,
        _DRAIN_PATTERNS,
        _SPECIMEN_PATTERNS,
        _BERRY_PATTERNS,
        _INTRAOP_COMPLICATION_PATTERNS,
    ]

    def extract(self, note_row_id, research_id, note_type, note_text, note_date=None):
        results: list[EntityMatch] = []
        seen: set[tuple[str, int]] = set()
        if not note_text:
            return results

        for bank in self._DOMAIN_PATTERNS:
            for pat, norm_val, etype, conf in bank:
                for m in pat.finditer(note_text):
                    key = (etype + ":" + norm_val, m.start())
                    if key in seen:
                        continue
                    seen.add(key)
                    raw = m.group(1) if m.lastindex else m.group(0)
                    results.append(EntityMatch(
                        research_id=research_id,
                        note_row_id=note_row_id,
                        note_type=note_type,
                        entity_type=etype,
                        entity_value_raw=raw,
                        entity_value_norm=norm_val,
                        present_or_negated=self.check_negation(note_text, m.start()),
                        confidence=conf,
                        evidence_span=_ctx(note_text, m.start(), m.end()),
                        evidence_start=m.start(),
                        evidence_end=m.end(),
                        entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                        note_date=note_date,
                        extraction_method="regex_operative_v2",
                    ))

        self._extract_ebl(note_text, note_row_id, research_id, note_type,
                          note_date, results, seen)

        return results

    # ── EBL sub-extractor ────────────────────────────────────────
    def _extract_ebl(
        self,
        note_text: str,
        note_row_id: str,
        research_id: int,
        note_type: str,
        note_date: str | None,
        results: list[EntityMatch],
        seen: set[tuple[str, int]],
    ) -> None:
        for m in _EBL_PATTERN.finditer(note_text):
            key = ("ebl:ebl_value", m.start())
            if key in seen:
                continue
            seen.add(key)
            volume = m.group(1)
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="ebl",
                entity_value_raw=m.group(0),
                entity_value_norm=f"{volume} mL",
                present_or_negated="present",
                confidence=CONF_EXPLICIT,
                evidence_span=_ctx(note_text, m.start(), m.end()),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
                extraction_method="regex_operative_v2",
            ))
