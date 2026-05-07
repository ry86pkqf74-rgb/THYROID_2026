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

from llm_extraction.base import BaseExtractor, EntityMatch
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
    # ── Existing patterns (v2.0) ─────────────────────────────────
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

    # ── F3 expansion (v2.1, 2026-05-06; M038-AUDIT-F3) ───────────
    # 1. Continuous IONM / cIONM
    (re.compile(
        r"\b(c[\.\s-]?IONM|continuous\s+IONM|continuous\s+intraoperative\s+(?:neuro)?monitoring)\b",
        re.I),
     "continuous_ionm", "nerve_monitoring", CONF_EXPLICIT),
    # 2. Intermittent IONM / iIONM
    (re.compile(
        r"\b(i[\.\s-]?IONM|intermittent\s+IONM|intermittent\s+stimulation)\b",
        re.I),
     "intermittent_ionm", "nerve_monitoring", CONF_EXPLICIT),
    # 3. RLN identified AND stimulated (intraoperative monitoring evidence)
    (re.compile(
        r"\b((?:recurrent\s+laryngeal\s+nerve|RLN)\s+(?:was\s+)?(?:identified[,\s]+(?:and\s+)?(?:was\s+)?)?stimulated)\b",
        re.I),
     "nerve_monitoring_used", "nerve_monitoring", CONF_EXPLICIT),
    # 4. Verified intact response to stimulation / response confirmed
    (re.compile(
        r"\b((?:verified\s+)?intact\s+response\s+to\s+stimulation|stimulation\s+response\s+(?:was\s+)?(?:verified|confirmed|intact))\b",
        re.I),
     "nerve_monitoring_used", "nerve_monitoring", CONF_EXPLICIT),
    # 5. EMG response confirmed / positive EMG response / EMG signal
    (re.compile(
        r"\b((?:positive\s+)?EMG\s+(?:response|signal|tracing|amplitude)\s+(?:was\s+)?(?:confirmed|preserved|maintained|intact|noted|obtained|present))\b",
        re.I),
     "nerve_monitoring_used", "nerve_monitoring", CONF_EXPLICIT),
    # 6. NIM ETT (endotracheal tube variants beyond the existing emg_tube pattern)
    (re.compile(
        r"\b(NIM(?:\s+(?:3\.0|2\.0|TriVantage|standard))?\s+ETTs?\b)",
        re.I),
     "nim_etts", "nerve_monitoring", CONF_EXPLICIT),
    # 7. Nerve integrity monitor (broader than monit-stem)
    (re.compile(
        r"\b(nerve\s+integrity\s+monitor(?:ing|s)?)\b",
        re.I),
     "nerve_monitoring_used", "nerve_monitoring", CONF_EXPLICIT),
    # 8. Continuous vagal monitoring / APS / vagal stimulator
    (re.compile(
        r"\b(continuous\s+vagal\s+(?:nerve\s+)?(?:monitoring|stimulation)|APS\s+(?:electrode|probe|monitor)|vagal\s+nerve\s+stimulator)\b",
        re.I),
     "vagal_continuous", "nerve_monitoring", CONF_EXPLICIT),
    # 9. Randolph protocol amplitudes — V1/V2/R1/R2 stimulation
    (re.compile(
        r"\b((?:V1|V2|R1|R2)(?:[/\s\-]+(?:V1|V2|R1|R2))*\s+(?:stimulation|amplitudes?|signals?|recorded|obtained))\b",
        re.I),
     "nerve_monitoring_used", "nerve_monitoring", CONF_EXPLICIT),
    # 10. Signal preserved / maintained (positive monitoring outcome)
    (re.compile(
        r"\b((?:nerve\s+)?signal\s+(?:was\s+)?(?:preserved|maintained|present|intact|robust))\b",
        re.I),
     "nerve_monitoring_used", "nerve_monitoring", CONF_EXPLICIT),
    # 11. Device brand → monitoring (Inomed, Medtronic, Magstim, Checkpoint)
    (re.compile(
        r"\b((?:Inomed|Medtronic|Magstim|Checkpoint|Xomed|C2)\s+(?:NIM|stimulator|nerve\s+monitor|neuromonitor\w*|probe))\b",
        re.I),
     "nim_device", "nerve_monitoring", CONF_EXPLICIT),
    # 12. Looser nerve stimulator phrasing
    (re.compile(
        r"\b((?:electrical\s+)?nerve\s+stimulation\s+(?:was\s+)?(?:applied|performed|utilized|employed)|nerve\s+stimulator\s+probe)\b",
        re.I),
     "nerve_stimulator_used", "nerve_monitoring", CONF_EXPLICIT),
    # 13. Laryngeal EMG / electromyography
    (re.compile(
        r"\b(laryngeal\s+(?:nerve\s+)?(?:EMG|electromyograph\w+))\b",
        re.I),
     "nerve_monitoring_used", "nerve_monitoring", CONF_EXPLICIT),
    # 14. Stimulation amplitudes recorded — broad monitoring evidence
    (re.compile(
        r"\b(stimulation\s+(?:was\s+)?(?:performed|carried\s+out|done)\s+(?:at|with|to)\s+\d+(?:\.\d+)?\s*(?:mA|microV|µV|uV))\b",
        re.I),
     "nerve_monitoring_used", "nerve_monitoring", CONF_EXPLICIT),
    # 15. Compound 'neuromonitor*' word (no preceding 'nerve' required) —
    #     catches Saunders-style 'Intraoperative recurrent laryngeal neuromonitoring',
    #     'neuromonitoring endotracheal tube', 'continuous neuromonitoring'.
    #     This was the v2.0 blind spot recovered during F3 validation.
    (re.compile(r"\b(neuromonitor\w+)\b", re.I),
     "nerve_monitoring_used", "nerve_monitoring", CONF_EXPLICIT),
    # 16. Neuromonitoring endotracheal tube (specific common phrasing)
    (re.compile(
        r"\b(neuromonitor\w+\s+(?:endotracheal|ET)\s+tube)\b",
        re.I),
     "nim_etts", "nerve_monitoring", CONF_EXPLICIT),
    # 17. RLN nerves identified WITH stimulation noted in same clause
    (re.compile(
        r"\b((?:recurrent\s+laryngeal\s+nerves?|RLNs?)\s+(?:were\s+)?identified\s+(?:and\s+)?(?:stimulated|monitored|protected)\s+(?:via|with|using)\s+(?:the\s+)?(?:NIM|nerve\s+monitor|stimulator|neuromonitor\w*))\b",
        re.I),
     "nerve_monitoring_used", "nerve_monitoring", CONF_EXPLICIT),
]

_NECK_DISSECTION_PATTERNS: list[_PatternRow] = [
    # ── F4 (v2.2, 2026-05-06; M038-AUDIT-F4-NeckDissection-NLPRules) ────
    # Central neck dissection patterns
    (re.compile(
        r"\b((?:right\s+|left\s+|bilateral\s+)?central\s+neck\s+(?:lymph\s+node\s+|lymph(?:adenectomy)?|compartment\s+)?dissection)\b",
        re.I),
     "central_neck_dissection", "neck_dissection", CONF_EXPLICIT),
    (re.compile(
        r"\b(level\s+(?:VI|6)\s+(?:lymph(?:adenectomy)?|dissection|lymph\s+node\s+dissection))\b",
        re.I),
     "central_neck_dissection", "neck_dissection", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:right\s+|left\s+|bilateral\s+)?central\s+compartment\s+(?:lymph(?:adenectomy)?|dissection|lymph\s+node\s+dissection))\b",
        re.I),
     "central_neck_dissection", "neck_dissection", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:right\s+|left\s+|bilateral\s+)?(?:para|pre)tracheal\s+(?:lymph(?:adenectomy)?|dissection|lymph\s+node\s+dissection))\b",
        re.I),
     "central_neck_dissection", "neck_dissection", CONF_CONTEXTUAL),
    (re.compile(
        r"\b(prelaryngeal\s+(?:lymph(?:adenectomy)?|dissection|node)|delphian\s+(?:node\s+)?(?:dissection|excision))\b",
        re.I),
     "central_neck_dissection", "neck_dissection", CONF_CONTEXTUAL),
    # Lateral neck dissection patterns
    (re.compile(
        r"\b((?:right\s+|left\s+|bilateral\s+)?lateral\s+neck\s+(?:lymph\s+node\s+|lymph(?:adenectomy)?|compartment\s+)?dissection)\b",
        re.I),
     "lateral_neck_dissection", "neck_dissection", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:modified\s+)?radical\s+neck\s+(?:lymph\s+node\s+)?dissection)\b",
        re.I),
     "lateral_neck_dissection", "neck_dissection", CONF_EXPLICIT),
    (re.compile(r"\b(MRND)\b"),
     "lateral_neck_dissection", "neck_dissection", CONF_EXPLICIT),
    # Multi-level dissection (II-V or 2-5 ranges) — implies lateral
    (re.compile(
        r"\b(levels?\s+(?:II|2)\s*(?:[-–—]\s*|\s+through\s+|\s+to\s+|\s*and\s+|\s+,\s*)*\s*(?:III|IV|V|3|4|5)(?:\s*[-–—,]\s*(?:III|IV|V|3|4|5))*\s+(?:lymph(?:adenectomy)?|dissection|lymph\s+node\s+dissection))\b",
        re.I),
     "lateral_neck_dissection", "neck_dissection", CONF_EXPLICIT),
    # Single-lateral-level dissection (II/III/IV/V) — implies lateral
    (re.compile(
        r"\b(level\s+(?:II|III|IV|V|2|3|4|5)\s+(?:lymph(?:adenectomy)?|dissection|lymph\s+node\s+dissection))\b",
        re.I),
     "lateral_neck_dissection", "neck_dissection", CONF_CONTEXTUAL),
    # Jugular chain dissection — lateral neck
    (re.compile(
        r"\b((?:right\s+|left\s+|bilateral\s+)?jugular\s+(?:chain|node)\s+(?:lymph(?:adenectomy)?|dissection|lymph\s+node\s+dissection))\b",
        re.I),
     "lateral_neck_dissection", "neck_dissection", CONF_CONTEXTUAL),
]


_OP_TIME_PATTERNS: list[_PatternRow] = [
    # ── F6 (v2.3, 2026-05-06; M038-FOLLOWUP-F6-OpDuration) ───────────────
    # Operative time / case time / OR time — capture minutes (numeric is in
    # the regex group 1; downstream parser converts H:MM and hh hours mm min)
    (re.compile(
        r"(?<!time\s)(?<!preoperative\s)"  # don't match 'time out' adjacents
        r"\b(?:operative\s+time|operating\s+time|OR\s+time|procedure\s+time|"
        r"case\s+time|operation\s+(?:total\s+)?time|total\s+operation\s+time|"
        r"duration\s+of\s+(?:the\s+)?(?:procedure|operation|surgery))"
        r"\s*(?:was\s+|of\s+|:?\s*)"
        r"(\d{1,3})\s*(?:minutes?|min|mins)\b",
        re.I),
     "op_time_minutes_explicit", "op_time", CONF_EXPLICIT),
    (re.compile(
        r"\b(?:operative\s+time|operating\s+time|OR\s+time|procedure\s+time|"
        r"case\s+time|duration\s+of\s+(?:the\s+)?(?:procedure|operation|surgery))"
        r"\s*(?:was\s+|of\s+|:?\s*)"
        r"(\d{1,2})\s*(?:hours?|hrs?|h)\s+(?:and\s+)?(\d{1,2})\s*(?:minutes?|min|mins)\b",
        re.I),
     "op_time_hours_minutes", "op_time", CONF_EXPLICIT),
    (re.compile(
        r"\b(?:start|incision)\s+time\s*[:=]\s*(\d{1,2}:\d{2})\s*"
        r"(?:.{1,80})?\b(?:end|closure|stop)\s+time\s*[:=]\s*(\d{1,2}:\d{2})\b",
        re.I),
     "op_time_start_end", "op_time", CONF_CONTEXTUAL),
]


_LOS_PATTERNS: list[_PatternRow] = [
    # ── F7 (v2.3, 2026-05-06; M038-FOLLOWUP-F7-LengthOfStay) ─────────────
    (re.compile(
        r"\b(?:discharged?|d/c'?d?)\s+(?:home\s+)?(?:on\s+)?(?:POD|postoperative\s+day|post[\s-]?op\s+day)\s*(\d{1,2})\b",
        re.I),
     "los_pod_discharge", "length_of_stay", CONF_EXPLICIT),
    (re.compile(
        r"\b(?:length\s+of\s+stay|LOS|hospital\s+stay|inpatient\s+stay)"
        r"\s*(?:was\s+|of\s+|:?\s*)"
        r"(\d{1,3})\s*(?:days?|d)\b",
        re.I),
     "los_days_explicit", "length_of_stay", CONF_EXPLICIT),
    (re.compile(
        r"\b(?:patient\s+(?:was\s+)?)?(?:discharged|sent\s+home|went\s+home)"
        r"\s+(?:the\s+)?(?:same\s+day|day\s+of\s+surgery|same[\s-]day)\b",
        re.I),
     "los_zero_same_day", "length_of_stay", CONF_EXPLICIT),
    (re.compile(
        r"\b(?:overnight\s+stay|admitted\s+overnight|kept\s+overnight)\b",
        re.I),
     "los_one_overnight", "length_of_stay", CONF_CONTEXTUAL),
    (re.compile(
        r"\b(?:POD|postoperative\s+day)\s*(\d{1,2})\s*(?:to|->|→)\s*(?:home|discharge)\b",
        re.I),
     "los_pod_discharge", "length_of_stay", CONF_EXPLICIT),
    # DC summary date-pair pattern (very common in dc_sum notes):
    #   "Date of admission\n3/30/2021"  +  "Date of discharge\n4/2/2021"
    # Captures the two dates so the rollup can compute LOS = discharge - admission
    (re.compile(
        r"Date\s+of\s+admission\s*[\n\r:.\s]*?(\d{1,2}/\d{1,2}/\d{2,4})"
        r"[\s\S]{0,400}?"
        r"Date\s+of\s+discharge\s*[\n\r:.\s]*?(\d{1,2}/\d{1,2}/\d{2,4})",
        re.I),
     "los_admission_discharge_pair", "length_of_stay", CONF_EXPLICIT),
    # Inverse order (some templates swap)
    (re.compile(
        r"(?:admit(?:ted)?|admission)\s+date\s*[:\-]\s*(\d{1,2}/\d{1,2}/\d{2,4})"
        r"[\s\S]{0,400}?"
        r"(?:discharge|dc)\s+date\s*[:\-]\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        re.I),
     "los_admission_discharge_pair", "length_of_stay", CONF_EXPLICIT),
]


_ENERGY_DEVICE_PATTERNS: list[_PatternRow] = [
    # ── F9 (v2.3, 2026-05-06; M038-FOLLOWUP-F9-VesselSealant -> LigaSure) ──
    # Device-specific entity_types so the canonical can expose op_nlp_ligasure_used
    # and op_nlp_harmonic_used as separate BOOLs (per PI direction: LigaSure is the
    # practice's standard since ~2017; Harmonic n=153 cases mostly 2019-2020 era).
    # LigaSure family
    (re.compile(
        r"\b(LigaSure(?:\s+(?:small\s+jaw|exact|impact|maryland|advance|atlas|dolphin))?)\b",
        re.I),
     "ligasure", "ligasure", CONF_EXPLICIT),
    # Harmonic family
    (re.compile(
        r"\b(Harmonic\s+(?:scalpel|focus|ace|hd1000i|shears?|device)|Harmonic\b)",
        re.I),
     "harmonic_scalpel", "harmonic", CONF_EXPLICIT),
    # Other modern energy devices — kept separately so they don't pollute either
    # primary BOOL. Currently zero cases in cohort.
    (re.compile(
        r"\b(EnSeal(?:\s+(?:trio|G2|tissue\s+sealer))?)\b",
        re.I),
     "enseal", "energy_device_other", CONF_EXPLICIT),
    (re.compile(r"\b(ThunderBeat|Thunder[\s-]?Beat)\b", re.I),
     "thunderbeat", "energy_device_other", CONF_EXPLICIT),
    (re.compile(r"\b(Caiman(?:\s+\d+)?)\b", re.I),
     "caiman", "energy_device_other", CONF_EXPLICIT),
    (re.compile(
        r"\b(?:bipolar\s+)?(?:vessel[\s-]?sealing\s+(?:device|system)|"
        r"electrothermal\s+bipolar\s+(?:vessel\s+)?(?:sealer|sealing)|"
        r"advanced\s+bipolar\s+(?:device|sealer))\b",
        re.I),
     "vessel_sealing_device_generic", "energy_device_other", CONF_CONTEXTUAL),
    # Negative / suture-only documentation — separate entity_type so the
    # combined energy-device rollup doesn't accidentally count these as positive.
    (re.compile(
        r"\b(?:vessels?\s+(?:were\s+)?(?:tied|ligated|suture[\s-]?ligated)\s+with(?:out|\s+only)\s+"
        r"(?:silk|vicryl|chromic|prolene)|conventional\s+suture\s+ligation)\b",
        re.I),
     "suture_ligation_only", "suture_ligation", CONF_CONTEXTUAL),
]


_TRACHEOSTOMY_TEMPORAL_PATTERNS: list[_PatternRow] = [
    # ── F2 (v2.2, 2026-05-06; M038-AUDIT-F2-Tracheostomy-Perioperative) ───
    # CONCURRENT/PERIOPERATIVE — high confidence the trach is THIS admission
    (re.compile(
        r"\b(tracheo(?:s|t)tomy\s+(?:tube\s+)?(?:was\s+)?(?:placed|performed|created|fashioned|inserted)\s+(?:through|in|during|today|now|concurrently))\b",
        re.I),
     "tracheostomy_concurrent", "tracheostomy", CONF_EXPLICIT),
    (re.compile(
        r"\b(concurrent(?:ly)?\s+tracheo(?:s|t)tomy|tracheo(?:s|t)tomy\s+(?:was\s+)?performed\s+(?:concurrently|at\s+the\s+same\s+(?:time|setting)|in\s+the\s+same\s+(?:procedure|setting)))\b",
        re.I),
     "tracheostomy_concurrent", "tracheostomy", CONF_EXPLICIT),
    (re.compile(
        r"\b((?:emergent|emergency|urgent)\s+tracheo(?:s|t)tomy)\b",
        re.I),
     "tracheostomy_concurrent", "tracheostomy", CONF_EXPLICIT),
    # CPT-coded concurrent tracheostomy (31600 = elective; 31603 = emergency
    # transtracheal; 31605 = emergency cricothyroid)
    (re.compile(r"\b(?:CPT\s*[:\-]?\s*)?(3160[035])\b"),
     "tracheostomy_concurrent_cpt", "tracheostomy", CONF_EXPLICIT),
    # POD-N timing reference ("tracheostomy on postoperative day 4")
    (re.compile(
        r"\b(tracheo(?:s|t)tomy\s+(?:on\s+|at\s+)?(?:POD\s*\d+|postoperative\s+day\s+\d+|day\s+\d+\s+post\s*-?\s*op))\b",
        re.I),
     "tracheostomy_pod", "tracheostomy", CONF_EXPLICIT),
    # PRE-EXISTING / HISTORICAL — these should DEMOTE proc_nlp_tracheostomy
    (re.compile(
        r"\b((?:history\s+of|prior|previous|past\s+(?:medical\s+)?history\s+of|s/p|status\s+post|h/o)\s+tracheo(?:s|t)tomy)\b",
        re.I),
     "tracheostomy_history", "tracheostomy", CONF_EXPLICIT),
    (re.compile(
        r"\b(tracheo(?:s|t)tomy\s+(?:tract|tube|stoma|site|scar)|tracheostoma\b)",
        re.I),
     "tracheostomy_anatomy_only", "tracheostomy", CONF_CONTEXTUAL),
    (re.compile(
        r"\b(no\s+tracheo(?:s|t)tomy\s+(?:required|needed|necessary|performed))\b",
        re.I),
     "tracheostomy_negated", "tracheostomy", CONF_EXPLICIT),
]


_RLN_SIGNAL_STATUS_PATTERNS: list[_PatternRow] = [
    # ── F5 (v2.2, 2026-05-06; M038-AUDIT-F5-NerveSignal-AbnormalVsVerified) ──
    # Verified / preserved / intact signal
    (re.compile(
        r"\b(signal\s+(?:was\s+)?(?:preserved|maintained|intact|robust|present|verified))\b",
        re.I),
     "signal_verified", "rln_signal_status", CONF_EXPLICIT),
    (re.compile(
        r"\b(intact\s+stimulation|positive\s+EMG\s+response|amplitude\s+(?:was\s+)?maintained|continuous\s+response\s+(?:was\s+)?confirmed)\b",
        re.I),
     "signal_verified", "rln_signal_status", CONF_EXPLICIT),
    # Diminished / reduced
    (re.compile(
        r"\b(amplitude\s+(?:was\s+)?(?:decreased|reduced|attenuated|diminished)|reduced\s+response|attenuated\s+signal|signal\s+(?:was\s+)?weakened)\b",
        re.I),
     "signal_diminished", "rln_signal_status", CONF_EXPLICIT),
    # Loss of signal / absent
    (re.compile(
        r"\b(no\s+response\s+to\s+stimulation|signal\s+(?:was\s+)?lost|loss\s+of\s+signal|failure\s+to\s+elicit|flatline|no\s+EMG\s+response|absent\s+(?:nerve\s+)?signal|LOS\b(?!\s+\d))",
        re.I),
     "loss_of_signal_los", "rln_signal_status", CONF_EXPLICIT),
    # Amplitude < 100µV LOS-threshold (per North American Loss-of-Signal definition)
    (re.compile(
        r"\b(amplitude\s*(?:<|less\s+than|under)\s*100\s*(?:µV|microV|uV)|under\s+100\s+microvolts?)\b",
        re.I),
     "loss_of_signal_los", "rln_signal_status", CONF_EXPLICIT),
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
        _NECK_DISSECTION_PATTERNS,
        _TRACHEOSTOMY_TEMPORAL_PATTERNS,
        _RLN_SIGNAL_STATUS_PATTERNS,
        _OP_TIME_PATTERNS,
        _LOS_PATTERNS,
        _ENERGY_DEVICE_PATTERNS,
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
        # Avoid H&P / consent false positives (operative patterns appear in risk discussions).
        if (note_type or "").strip().lower() not in ("op_note", "opnote"):
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
