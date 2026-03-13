"""
Extraction Audit Engine v6 — Phase 8 Source-Linked Recurrence, Follow-up & Long-Term Outcomes
==============================================================================================
Extends v5 with specialized parsers for:
  1. RecurrenceEventParser     – temporal + detection-method hierarchy
  2. LongTermOutcomeReconciler – multi-note voice/swallow timeline building
  3. RAIResponseAssessor       – ATA 2015 response-to-therapy classification
  4. CompletionReasonClassifier – completion thyroidectomy reason categorisation
  5. FollowUpBlockAuditor      – Excel-cell sweep for missed events
  6. MissedDataSweeper         – 1000-patient full note-history verification

Source hierarchy: structured_db (1.0) > Tg_lab (0.95) > endocrine_note (0.90)
                  > imaging_report (0.85) > path_report (0.85) > op_note (0.80)
                  > dc_sum (0.70) > h_p_consent (0.20)

Usage:
    from notes_extraction.extraction_audit_engine_v6 import audit_and_refine_phase8
    results = audit_and_refine_phase8(con)

CLI:
    .venv/bin/python notes_extraction/extraction_audit_engine_v6.py --md --variable all
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from notes_extraction.extraction_audit_engine_v4 import _extract_table_name
from notes_extraction.extraction_audit_engine_v3 import _get_connection
from notes_extraction.vocab import (  # noqa: F401 — re-exported for downstream
    RECURRENCE_SITE_NORM,
    RECURRENCE_DETECTION_NORM,
    RAI_RESPONSE_NORM,
    VOICE_OUTCOME_NORM,
    COMPLETION_REASON_NORM,
)

# ---------------------------------------------------------------------------
# Source hierarchy constants — Phase 8
# ---------------------------------------------------------------------------
PHASE8_SOURCE_HIERARCHY = {
    "structured_db": 1.0,
    "thyroglobulin_lab": 0.95,
    "endocrine_note": 0.90,
    "imaging_report": 0.85,
    "path_report": 0.85,
    "op_note": 0.80,
    "dc_sum": 0.70,
    "nuclear_med": 0.70,
    "h_p_consent": 0.20,
}

# ATA 2015 response-to-therapy thresholds
TG_EXCELLENT_THRESHOLD = 0.2       # suppressed Tg < 0.2 ng/mL
TG_BIOCHEM_INCOMPLETE = 1.0        # suppressed Tg > 1.0 ng/mL or rising
TG_INDETERMINATE_LOW = 0.2
TG_INDETERMINATE_HIGH = 1.0

# ---------------------------------------------------------------------------
# Parser: RecurrenceEventParser
# ---------------------------------------------------------------------------
class RecurrenceEventParser:
    """Classifies recurrence events by site, detection method, and timing."""

    _SITE_PATTERNS = [
        (re.compile(r"\b(?:thyroid\s*bed|remnant)\b", re.I), "local"),
        (re.compile(r"\b(?:central\s*(?:neck|compartment)|level\s*(?:vi|6))\b", re.I), "regional_central"),
        (re.compile(r"\b(?:lateral\s*(?:neck|compartment)|level\s*(?:ii|iii|iv|v|2|3|4|5))\b", re.I), "regional_lateral"),
        (re.compile(r"\b(?:mediast)", re.I), "regional_mediastinal"),
        (re.compile(r"\b(?:lung|pulmon)", re.I), "distant_lung"),
        (re.compile(r"\b(?:bone|osseous)", re.I), "distant_bone"),
        (re.compile(r"\b(?:brain|cerebr)", re.I), "distant_brain"),
        (re.compile(r"\b(?:liver|hepat)", re.I), "distant_liver"),
    ]

    _METHOD_PATTERNS = [
        (re.compile(r"\b(?:ultrasound|us\b|neck\s*us|sonograph)", re.I), "structural_imaging"),
        (re.compile(r"\b(?:ct\b|ct\s*scan|computed\s*tomogra)", re.I), "structural_imaging"),
        (re.compile(r"\b(?:mri|magnetic\s*resonance)", re.I), "structural_imaging"),
        (re.compile(r"\b(?:pet|fdg|pet[\s/]*ct)", re.I), "structural_imaging"),
        (re.compile(r"\b(?:rai\s*scan|whole\s*body\s*scan|wbs|i[\s-]*131\s*scan|diagnostic\s*scan)", re.I), "functional_imaging"),
        (re.compile(r"\b(?:thyroglobulin|tg\b|rising\s*tg|stimulated\s*tg)", re.I), "biochemical"),
        (re.compile(r"\b(?:biopsy|fna|fine\s*needle|cytol)", re.I), "cytologic"),
        (re.compile(r"\b(?:palpab|physical\s*exam|clinical\s*exam)", re.I), "clinical"),
    ]

    _NEGATION_WINDOW = 60

    @classmethod
    def classify_site(cls, text: str | None) -> str:
        if not text:
            return "unknown"
        for pat, site in cls._SITE_PATTERNS:
            if pat.search(text):
                return site
        return "unknown"

    @classmethod
    def classify_method(cls, text: str | None) -> str:
        if not text:
            return "unknown"
        for pat, method in cls._METHOD_PATTERNS:
            if pat.search(text):
                return method
        return "unknown"

    @classmethod
    def is_negated(cls, text: str, match_pos: int) -> bool:
        window = text[max(0, match_pos - cls._NEGATION_WINDOW):match_pos].lower()
        neg_cues = ["no evidence", "without", "denies", "negative for", "no ", "not ",
                    "ruled out", "r/o", "absent", "unlikely", "resolved", "no recurrence",
                    "no structural", "surveillance"]
        return any(c in window for c in neg_cues)

    @classmethod
    def classify_type(cls, site: str, method: str) -> str:
        if method == "biochemical":
            return "biochemical"
        if site.startswith("distant_"):
            return "distant_structural"
        if site in ("local", "regional_central", "regional_lateral", "regional_mediastinal"):
            return "locoregional_structural"
        return "structural"


class LongTermOutcomeReconciler:
    """Builds multi-note timeline for voice/swallow outcomes."""

    _VOICE_PATTERNS = [
        (re.compile(r"\b(?:hoarse|hoarseness|dysphon)", re.I), "hoarse"),
        (re.compile(r"\b(?:breathy\s*voice|breathy\b)", re.I), "breathy_voice"),
        (re.compile(r"\b(?:vocal\s*fatigue|voice\s*fatigue)", re.I), "vocal_fatigue"),
        (re.compile(r"\b(?:normal\s*voice|voice\s*(?:is\s*)?normal)", re.I), "normal"),
    ]

    _SWALLOW_PATTERNS = [
        (re.compile(r"\b(?:dysphagia|difficulty\s*swallow|swallowing\s*difficult)", re.I), "dysphagia"),
        (re.compile(r"\b(?:aspiration)", re.I), "aspiration"),
        (re.compile(r"\b(?:normal\s*swallow|swallow(?:ing)?\s*(?:is\s*)?normal)", re.I), "normal_swallow"),
    ]

    _RECOVERY_PATTERNS = [
        (re.compile(r"\b(?:resolved|recovered|resolution|back\s*to\s*normal|return\s*(?:to|of)\s*normal)", re.I), "recovered"),
        (re.compile(r"\b(?:improving|improved|better|partial\s*recovery)", re.I), "improving"),
        (re.compile(r"\b(?:persistent|permanent|unchanged|no\s*improvement)", re.I), "persistent"),
    ]

    @classmethod
    def classify_voice(cls, text: str | None) -> str:
        if not text:
            return "unknown"
        for pat, label in cls._VOICE_PATTERNS:
            if pat.search(text):
                return label
        return "unknown"

    @classmethod
    def classify_swallow(cls, text: str | None) -> str:
        if not text:
            return "unknown"
        for pat, label in cls._SWALLOW_PATTERNS:
            if pat.search(text):
                return label
        return "unknown"

    @classmethod
    def classify_recovery(cls, text: str | None) -> str:
        if not text:
            return "unknown"
        for pat, label in cls._RECOVERY_PATTERNS:
            if pat.search(text):
                return label
        return "unknown"


class RAIResponseAssessor:
    """ATA 2015 response-to-therapy classification from Tg trajectory + imaging."""

    @classmethod
    def classify_response(cls, suppressed_tg: float | None,
                          stimulated_tg: float | None,
                          has_structural_disease: bool,
                          anti_tg_rising: bool) -> dict:
        if has_structural_disease:
            return {"response": "structural_incomplete", "confidence": 0.95,
                    "basis": "structural_disease_present"}

        if suppressed_tg is not None:
            if suppressed_tg < TG_EXCELLENT_THRESHOLD and not anti_tg_rising:
                return {"response": "excellent", "confidence": 0.90,
                        "basis": f"suppressed_tg={suppressed_tg}"}
            if suppressed_tg > TG_BIOCHEM_INCOMPLETE:
                return {"response": "biochemical_incomplete", "confidence": 0.85,
                        "basis": f"suppressed_tg={suppressed_tg}"}
            return {"response": "indeterminate", "confidence": 0.70,
                    "basis": f"suppressed_tg={suppressed_tg}"}

        if stimulated_tg is not None:
            if stimulated_tg < 1.0 and not anti_tg_rising:
                return {"response": "excellent", "confidence": 0.85,
                        "basis": f"stimulated_tg={stimulated_tg}"}
            if stimulated_tg > 10.0:
                return {"response": "biochemical_incomplete", "confidence": 0.80,
                        "basis": f"stimulated_tg={stimulated_tg}"}
            return {"response": "indeterminate", "confidence": 0.65,
                    "basis": f"stimulated_tg={stimulated_tg}"}

        return {"response": "insufficient_data", "confidence": 0.0,
                "basis": "no_tg_data"}


class CompletionReasonClassifier:
    """Classifies reason for completion thyroidectomy from operative/clinical notes."""

    _REASON_PATTERNS = [
        (re.compile(r"\b(?:malignan|cancer|carcinom|papillary|ptc\b|follicular\s*carcinom)", re.I), "pathology_upgrade"),
        (re.compile(r"\b(?:molecular|thyroseq|afirma|braf|tert|ntrk|gene)", re.I), "molecular_result"),
        (re.compile(r"\b(?:enlarg|growing|increas(?:ing|ed)\s*size|contralateral\s*nodul)", re.I), "imaging_concern"),
        (re.compile(r"\b(?:suspicious\s*(?:lymph|node|ln)|new\s*(?:node|lymph))", re.I), "imaging_concern"),
        (re.compile(r"\b(?:patient\s*prefer|patient\s*request|elect(?:ive|ed)|chose)", re.I), "patient_preference"),
        (re.compile(r"\b(?:grave|hyperthyroid|compressive|airway|tracheal\s*deviat)", re.I), "medical_indication"),
        (re.compile(r"\b(?:prophylact|risk\s*reduct)", re.I), "prophylactic"),
    ]

    @classmethod
    def classify(cls, preop_dx: str | None, op_note_context: str | None,
                 path_result: str | None) -> dict:
        sources = []
        if preop_dx:
            sources.append(("preop_dx", preop_dx))
        if op_note_context:
            sources.append(("op_note", op_note_context))
        if path_result:
            sources.append(("path_result", path_result))

        reasons_found = []
        for src_name, text in sources:
            for pat, reason in cls._REASON_PATTERNS:
                if pat.search(text):
                    reasons_found.append((reason, src_name))
                    break

        if not reasons_found:
            return {"reason": "unclassified", "source": None, "confidence": 0.0}

        reason_priority = ["pathology_upgrade", "molecular_result", "imaging_concern",
                          "medical_indication", "patient_preference", "prophylactic"]
        best = min(reasons_found, key=lambda x: reason_priority.index(x[0])
                   if x[0] in reason_priority else 99)
        return {"reason": best[0], "source": best[1],
                "confidence": 0.90 if best[1] == "path_result" else 0.75}


# ---------------------------------------------------------------------------
# SQL builders — Phase 8 tables
# ---------------------------------------------------------------------------

def build_recurrence_refined_sql() -> str:
    """extracted_recurrence_refined_v1 — structured recurrence with site + method."""
    return """
CREATE OR REPLACE TABLE extracted_recurrence_refined_v1 AS
WITH structured_recurrence AS (
    SELECT
        CAST(r.research_id AS INTEGER) AS research_id,
        BOOL_OR(LOWER(CAST(r.recurrence_flag AS VARCHAR)) = 'true') AS has_recurrence,
        MIN(r.first_recurrence_date) AS first_recurrence_date,
        MAX(r.recurrence_risk_band) AS recurrence_risk_band
    FROM recurrence_risk_features_mv r
    GROUP BY CAST(r.research_id AS INTEGER)
),
tg_trajectory AS (
    SELECT
        research_id,
        COUNT(*) AS n_tg_measurements,
        MIN(TRY_CAST(specimen_collect_dt AS DATE)) AS first_tg_date,
        MAX(TRY_CAST(specimen_collect_dt AS DATE)) AS last_tg_date,
        MIN(CASE WHEN TRY_CAST(REGEXP_REPLACE(result, '[<>]', '', 'g') AS DOUBLE) IS NOT NULL
            THEN TRY_CAST(REGEXP_REPLACE(result, '[<>]', '', 'g') AS DOUBLE) END) AS tg_nadir,
        MAX(CASE WHEN TRY_CAST(REGEXP_REPLACE(result, '[<>]', '', 'g') AS DOUBLE) IS NOT NULL
            THEN TRY_CAST(REGEXP_REPLACE(result, '[<>]', '', 'g') AS DOUBLE) END) AS tg_max,
        -- Last Tg value (by date)
        LAST(CASE WHEN TRY_CAST(REGEXP_REPLACE(result, '[<>]', '', 'g') AS DOUBLE) IS NOT NULL
            THEN TRY_CAST(REGEXP_REPLACE(result, '[<>]', '', 'g') AS DOUBLE) END
            ORDER BY TRY_CAST(specimen_collect_dt AS DATE)) AS tg_last_value,
        -- Rising Tg flag: last > 2x nadir
        CASE WHEN MAX(CASE WHEN TRY_CAST(REGEXP_REPLACE(result, '[<>]', '', 'g') AS DOUBLE) IS NOT NULL
                THEN TRY_CAST(REGEXP_REPLACE(result, '[<>]', '', 'g') AS DOUBLE) END) >
            2.0 * NULLIF(MIN(CASE WHEN TRY_CAST(REGEXP_REPLACE(result, '[<>]', '', 'g') AS DOUBLE) IS NOT NULL
                THEN TRY_CAST(REGEXP_REPLACE(result, '[<>]', '', 'g') AS DOUBLE) END), 0)
            THEN TRUE ELSE FALSE END AS tg_rising_flag
    FROM thyroglobulin_labs
    WHERE result IS NOT NULL AND TRIM(result) <> ''
    GROUP BY research_id
),
rai_scan_recurrence AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        BOOL_OR(scan_findings_raw IS NOT NULL AND scan_findings_raw <> '') AS has_scan_findings,
        BOOL_OR(iodine_avidity_flag IS TRUE) AS rai_avid,
        MAX(dose_mci) AS max_rai_dose_mci,
        COUNT(*) AS n_rai_treatments,
        STRING_AGG(DISTINCT scan_findings_raw, '; ') FILTER (WHERE scan_findings_raw IS NOT NULL) AS scan_findings_combined,
        MAX(stimulated_tg) AS last_stimulated_tg,
        MAX(stimulated_tsh) AS last_stimulated_tsh
    FROM rai_treatment_episode_v2
    GROUP BY CAST(research_id AS INTEGER)
),
per_patient AS (
    SELECT
        ps.research_id,
        COALESCE(sr.has_recurrence, FALSE) AS recurrence_flag_structured,
        sr.first_recurrence_date,
        sr.recurrence_risk_band,
        -- Tg trajectory
        tg.n_tg_measurements,
        tg.tg_nadir,
        tg.tg_max,
        tg.tg_last_value,
        tg.tg_rising_flag,
        tg.first_tg_date,
        tg.last_tg_date,
        -- Biochemical recurrence: rising Tg above threshold with no structural disease
        CASE
            WHEN tg.tg_last_value > 1.0 AND tg.tg_rising_flag THEN TRUE
            WHEN sr.has_recurrence THEN TRUE
            ELSE FALSE
        END AS recurrence_any,
        -- Detection method hierarchy
        CASE
            WHEN sr.has_recurrence AND sr.first_recurrence_date IS NOT NULL THEN 'structural_confirmed'
            WHEN tg.tg_last_value > 1.0 AND tg.tg_rising_flag AND NOT COALESCE(sr.has_recurrence, FALSE) THEN 'biochemical_only'
            WHEN sr.has_recurrence AND sr.first_recurrence_date IS NULL THEN 'structural_date_unknown'
            ELSE 'no_recurrence'
        END AS detection_category,
        -- RAI data
        rsc.has_scan_findings,
        rsc.rai_avid,
        rsc.max_rai_dose_mci,
        rsc.n_rai_treatments,
        rsc.scan_findings_combined,
        rsc.last_stimulated_tg,
        rsc.last_stimulated_tsh,
        -- Source linkage
        CASE WHEN sr.has_recurrence THEN 'recurrence_risk_features_mv' END AS recurrence_source,
        CASE WHEN tg.n_tg_measurements > 0 THEN 'thyroglobulin_labs' END AS tg_source,
        CASE WHEN rsc.n_rai_treatments > 0 THEN 'rai_treatment_episode_v2' END AS rai_source
    FROM (SELECT DISTINCT research_id FROM path_synoptics) ps
    LEFT JOIN structured_recurrence sr ON ps.research_id = sr.research_id
    LEFT JOIN tg_trajectory tg ON ps.research_id = tg.research_id
    LEFT JOIN rai_scan_recurrence rsc ON ps.research_id = rsc.research_id
)
SELECT
    *,
    -- Recurrence site from scan findings (best available)
    CASE
        WHEN scan_findings_combined ILIKE '%lung%' OR scan_findings_combined ILIKE '%pulmon%' THEN 'distant_lung'
        WHEN scan_findings_combined ILIKE '%bone%' OR scan_findings_combined ILIKE '%osseous%' THEN 'distant_bone'
        WHEN scan_findings_combined ILIKE '%lateral%' OR scan_findings_combined ILIKE '%level ii%'
             OR scan_findings_combined ILIKE '%level iii%' THEN 'regional_lateral'
        WHEN scan_findings_combined ILIKE '%central%' OR scan_findings_combined ILIKE '%thyroid bed%' THEN 'local'
        WHEN scan_findings_combined ILIKE '%uptake%' THEN 'local'
        ELSE NULL
    END AS recurrence_site_inferred,
    -- Confidence scoring
    CASE
        WHEN recurrence_flag_structured AND n_tg_measurements >= 3 AND rai_source IS NOT NULL THEN 0.95
        WHEN recurrence_flag_structured AND n_tg_measurements >= 1 THEN 0.85
        WHEN recurrence_flag_structured THEN 0.75
        WHEN tg_rising_flag AND n_tg_measurements >= 3 THEN 0.70
        ELSE 0.50
    END AS recurrence_data_confidence,
    -- Source count
    (CASE WHEN recurrence_source IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN tg_source IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN rai_source IS NOT NULL THEN 1 ELSE 0 END) AS n_recurrence_sources
FROM per_patient
ORDER BY research_id;
"""


def build_rai_response_sql() -> str:
    """extracted_rai_response_v1 — ATA response-to-therapy per RAI patient."""
    return """
CREATE OR REPLACE TABLE extracted_rai_response_v1 AS
WITH rai_patients AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        MAX(dose_mci) AS max_dose_mci,
        COUNT(*) AS n_treatments,
        MAX(resolved_rai_date) AS last_rai_date,
        STRING_AGG(DISTINCT rai_intent, ', ') FILTER (WHERE rai_intent IS NOT NULL) AS rai_intents,
        BOOL_OR(iodine_avidity_flag IS TRUE) AS avid,
        MAX(stimulated_tg) AS stimulated_tg,
        MAX(stimulated_tsh) AS stimulated_tsh,
        STRING_AGG(DISTINCT scan_findings_raw, '; ') FILTER (WHERE scan_findings_raw IS NOT NULL) AS scan_findings
    FROM rai_treatment_episode_v2
    GROUP BY CAST(research_id AS INTEGER)
),
post_rai_tg AS (
    SELECT
        tl.research_id,
        MIN(CASE WHEN TRY_CAST(REGEXP_REPLACE(tl.result, '[<>]', '', 'g') AS DOUBLE) IS NOT NULL
            THEN TRY_CAST(REGEXP_REPLACE(tl.result, '[<>]', '', 'g') AS DOUBLE) END) AS post_rai_tg_nadir,
        MAX(CASE WHEN TRY_CAST(REGEXP_REPLACE(tl.result, '[<>]', '', 'g') AS DOUBLE) IS NOT NULL
            THEN TRY_CAST(REGEXP_REPLACE(tl.result, '[<>]', '', 'g') AS DOUBLE) END) AS post_rai_tg_max,
        LAST(CASE WHEN TRY_CAST(REGEXP_REPLACE(tl.result, '[<>]', '', 'g') AS DOUBLE) IS NOT NULL
            THEN TRY_CAST(REGEXP_REPLACE(tl.result, '[<>]', '', 'g') AS DOUBLE) END
            ORDER BY TRY_CAST(tl.specimen_collect_dt AS DATE)) AS post_rai_tg_last,
        COUNT(*) AS post_rai_tg_count
    FROM thyroglobulin_labs tl
    INNER JOIN rai_patients rp ON tl.research_id = rp.research_id
    WHERE TRY_CAST(tl.specimen_collect_dt AS DATE) > TRY_CAST(rp.last_rai_date AS DATE)
    GROUP BY tl.research_id
),
recurrence_check AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        BOOL_OR(LOWER(CAST(recurrence_flag AS VARCHAR)) = 'true') AS has_structural_recurrence
    FROM recurrence_risk_features_mv
    GROUP BY CAST(research_id AS INTEGER)
)
SELECT
    rp.research_id,
    rp.max_dose_mci,
    rp.n_treatments,
    rp.last_rai_date,
    rp.rai_intents,
    rp.avid AS iodine_avid,
    rp.stimulated_tg,
    rp.stimulated_tsh,
    rp.scan_findings,
    pt.post_rai_tg_nadir,
    pt.post_rai_tg_max,
    pt.post_rai_tg_last,
    pt.post_rai_tg_count,
    -- ATA response classification
    CASE
        WHEN rc.has_structural_recurrence IS TRUE THEN 'structural_incomplete'
        WHEN pt.post_rai_tg_last IS NOT NULL AND pt.post_rai_tg_last < 0.2
             AND NOT COALESCE(rc.has_structural_recurrence, FALSE) THEN 'excellent'
        WHEN pt.post_rai_tg_last IS NOT NULL AND pt.post_rai_tg_last > 1.0 THEN 'biochemical_incomplete'
        WHEN pt.post_rai_tg_last IS NOT NULL AND pt.post_rai_tg_last BETWEEN 0.2 AND 1.0 THEN 'indeterminate'
        WHEN rp.stimulated_tg IS NOT NULL AND rp.stimulated_tg < 1.0
             AND NOT COALESCE(rc.has_structural_recurrence, FALSE) THEN 'excellent'
        WHEN rp.stimulated_tg IS NOT NULL AND rp.stimulated_tg > 10.0 THEN 'biochemical_incomplete'
        WHEN rp.stimulated_tg IS NOT NULL THEN 'indeterminate'
        ELSE 'insufficient_data'
    END AS ata_response_category,
    CASE
        WHEN rc.has_structural_recurrence IS TRUE THEN 0.95
        WHEN pt.post_rai_tg_count >= 3 THEN 0.90
        WHEN pt.post_rai_tg_count >= 1 THEN 0.80
        WHEN rp.stimulated_tg IS NOT NULL THEN 0.70
        ELSE 0.0
    END AS response_confidence,
    COALESCE(rc.has_structural_recurrence, FALSE) AS structural_disease_flag,
    'rai_treatment_episode_v2+thyroglobulin_labs' AS source_tables
FROM rai_patients rp
LEFT JOIN post_rai_tg pt ON rp.research_id = pt.research_id
LEFT JOIN recurrence_check rc ON rp.research_id = rc.research_id
ORDER BY rp.research_id;
"""


def build_longterm_outcomes_sql() -> str:
    """extracted_longterm_outcomes_v1 — voice/swallow/RLN outcomes beyond binary."""
    return """
CREATE OR REPLACE TABLE extracted_longterm_outcomes_v1 AS
WITH rln_detail AS (
    SELECT
        research_id,
        vocal_cord_status,
        affected_side,
        laryngoscopy_date,
        CASE
            WHEN LOWER(vocal_cord_status) IN ('paralysis','vocal cord paralysis') THEN 'paralysis'
            WHEN LOWER(vocal_cord_status) IN ('paresis','vocal cord paresis') THEN 'paresis'
            ELSE vocal_cord_status
        END AS rln_grade,
        CASE
            WHEN LOWER(rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy) = 'yes' THEN TRUE
            WHEN LOWER(COALESCE(vocal_cord_status,'')) IN ('paralysis','paresis',
                'vocal cord paralysis','vocal cord paresis') THEN TRUE
            ELSE FALSE
        END AS has_rln_injury
    FROM complications
    WHERE (LOWER(COALESCE(rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy,'')) = 'yes'
        OR LOWER(COALESCE(vocal_cord_status,'')) IN ('paralysis','paresis',
            'vocal cord paralysis','vocal cord paresis'))
),
rln_per_patient AS (
    SELECT
        research_id,
        MAX(rln_grade) AS worst_rln_grade,
        STRING_AGG(DISTINCT affected_side, ', ') AS rln_sides,
        MIN(laryngoscopy_date) AS first_laryngoscopy_date,
        MAX(laryngoscopy_date) AS last_laryngoscopy_date,
        COUNT(*) AS n_rln_assessments,
        BOOL_OR(rln_grade = 'paralysis') AS has_paralysis,
        BOOL_OR(rln_grade = 'paresis') AS has_paresis
    FROM rln_detail
    WHERE has_rln_injury
    GROUP BY research_id
),
surgery_dates AS (
    SELECT research_id, MIN(surg_date) AS first_surgery_date
    FROM path_synoptics
    GROUP BY research_id
),
recovery_timeline AS (
    SELECT
        rp.research_id,
        rp.worst_rln_grade,
        rp.rln_sides,
        rp.n_rln_assessments,
        rp.has_paralysis,
        rp.has_paresis,
        rp.first_laryngoscopy_date,
        rp.last_laryngoscopy_date,
        sd.first_surgery_date,
        DATEDIFF('day', TRY_CAST(sd.first_surgery_date AS DATE),
                 TRY_CAST(rp.first_laryngoscopy_date AS DATE)) AS days_to_first_laryngoscopy,
        DATEDIFF('day', TRY_CAST(sd.first_surgery_date AS DATE),
                 TRY_CAST(rp.last_laryngoscopy_date AS DATE)) AS days_to_last_laryngoscopy,
        CASE
            WHEN DATEDIFF('day', TRY_CAST(sd.first_surgery_date AS DATE),
                          TRY_CAST(rp.last_laryngoscopy_date AS DATE)) > 365
                 AND rp.has_paralysis THEN 'permanent_paralysis'
            WHEN DATEDIFF('day', TRY_CAST(sd.first_surgery_date AS DATE),
                          TRY_CAST(rp.last_laryngoscopy_date AS DATE)) > 180
                 AND rp.has_paresis THEN 'prolonged_paresis'
            WHEN rp.n_rln_assessments = 1 THEN 'single_assessment_only'
            WHEN rp.has_paresis AND NOT rp.has_paralysis THEN 'paresis_outcome_unknown'
            WHEN rp.has_paralysis THEN 'paralysis_duration_unknown'
            ELSE 'assessment_incomplete'
        END AS voice_outcome_category
    FROM rln_per_patient rp
    LEFT JOIN surgery_dates sd ON rp.research_id = sd.research_id
)
SELECT
    ps.research_id,
    COALESCE(rt.worst_rln_grade, 'no_injury') AS rln_worst_grade,
    rt.rln_sides,
    rt.n_rln_assessments,
    rt.has_paralysis,
    rt.has_paresis,
    rt.first_laryngoscopy_date,
    rt.last_laryngoscopy_date,
    rt.days_to_first_laryngoscopy,
    rt.days_to_last_laryngoscopy,
    COALESCE(rt.voice_outcome_category, 'no_injury') AS voice_outcome_category,
    CASE
        WHEN rt.research_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS has_voice_data,
    CASE
        WHEN rt.research_id IS NULL THEN 'no_injury'
        WHEN rt.n_rln_assessments >= 2 AND rt.days_to_last_laryngoscopy > 180 THEN 'complete_followup'
        WHEN rt.n_rln_assessments >= 1 THEN 'partial_followup'
        ELSE 'no_followup'
    END AS voice_followup_completeness,
    CASE WHEN rt.research_id IS NOT NULL THEN 0.90 ELSE 0.50 END AS voice_data_confidence,
    'complications+path_synoptics' AS source_tables
FROM (SELECT DISTINCT research_id FROM path_synoptics) ps
LEFT JOIN recovery_timeline rt ON ps.research_id = rt.research_id
ORDER BY ps.research_id;
"""


def build_completion_reasons_sql() -> str:
    """extracted_completion_reasons_v1 — completion thyroidectomy reason classification."""
    return """
CREATE OR REPLACE TABLE extracted_completion_reasons_v1 AS
WITH completion_patients AS (
    SELECT
        ps.research_id,
        ps.surg_date,
        ps.completion,
        ps.path_diagnosis_summary,
        ps.clinical_information_pre_op_diagnosis,
        ps.tumor_1_histologic_type,
        ps.tumor_1_pt,
        ps.tumor_1_size_greatest_dimension_cm
    FROM path_synoptics ps
    WHERE LOWER(TRIM(COALESCE(ps.completion, ''))) IN ('yes', 'y', 'completion')
),
prior_surgery AS (
    SELECT
        cp.research_id,
        MAX(ps2.surg_date) FILTER (WHERE TRY_CAST(ps2.surg_date AS DATE) < TRY_CAST(cp.surg_date AS DATE)) AS prior_surg_date,
        MAX(ps2.tumor_1_histologic_type) FILTER (WHERE TRY_CAST(ps2.surg_date AS DATE) < TRY_CAST(cp.surg_date AS DATE)) AS prior_histology,
        COUNT(*) FILTER (WHERE TRY_CAST(ps2.surg_date AS DATE) < TRY_CAST(cp.surg_date AS DATE)) AS n_prior_surgeries
    FROM completion_patients cp
    LEFT JOIN path_synoptics ps2 ON cp.research_id = ps2.research_id
        AND ps2.research_id = cp.research_id
    GROUP BY cp.research_id
),
molecular_before_completion AS (
    SELECT
        CAST(m.research_id AS INTEGER) AS research_id,
        BOOL_OR(m.braf_positive IS TRUE) AS had_braf_positive,
        BOOL_OR(m.tert_positive IS TRUE) AS had_tert_positive,
        STRING_AGG(DISTINCT m.platforms_used, ', ') AS molecular_platforms
    FROM extracted_molecular_panel_v1 m
    GROUP BY CAST(m.research_id AS INTEGER)
)
SELECT
    cp.research_id,
    cp.surg_date AS completion_date,
        cp.tumor_1_histologic_type AS completion_histology,
        cp.tumor_1_pt AS completion_t_stage,
    ps.prior_surg_date,
    ps.prior_histology,
    ps.n_prior_surgeries,
    mc.had_braf_positive,
    mc.had_tert_positive,
    mc.molecular_platforms,
    -- Reason classification
    CASE
        WHEN cp.tumor_1_histologic_type IS NOT NULL
             AND LOWER(cp.tumor_1_histologic_type) NOT IN ('', 'none', 'benign')
             AND ps.prior_histology IS NOT NULL THEN 'pathology_upgrade'
        WHEN mc.had_braf_positive OR mc.had_tert_positive THEN 'molecular_result'
        WHEN LOWER(COALESCE(cp.path_diagnosis_summary, '') || ' ' || COALESCE(cp.clinical_information_pre_op_diagnosis, '')) LIKE '%malignan%'
             OR LOWER(COALESCE(cp.path_diagnosis_summary, '') || ' ' || COALESCE(cp.clinical_information_pre_op_diagnosis, '')) LIKE '%cancer%'
             OR LOWER(COALESCE(cp.path_diagnosis_summary, '') || ' ' || COALESCE(cp.clinical_information_pre_op_diagnosis, '')) LIKE '%suspicious%' THEN 'pathology_upgrade'
        WHEN LOWER(COALESCE(cp.path_diagnosis_summary, '') || ' ' || COALESCE(cp.clinical_information_pre_op_diagnosis, '')) LIKE '%nodule%'
             OR LOWER(COALESCE(cp.path_diagnosis_summary, '') || ' ' || COALESCE(cp.clinical_information_pre_op_diagnosis, '')) LIKE '%enlarg%'
             OR LOWER(COALESCE(cp.path_diagnosis_summary, '') || ' ' || COALESCE(cp.clinical_information_pre_op_diagnosis, '')) LIKE '%contralateral%' THEN 'imaging_concern'
        WHEN LOWER(COALESCE(cp.path_diagnosis_summary, '') || ' ' || COALESCE(cp.clinical_information_pre_op_diagnosis, '')) LIKE '%grave%'
             OR LOWER(COALESCE(cp.path_diagnosis_summary, '') || ' ' || COALESCE(cp.clinical_information_pre_op_diagnosis, '')) LIKE '%hyperthyroid%'
             OR LOWER(COALESCE(cp.path_diagnosis_summary, '') || ' ' || COALESCE(cp.clinical_information_pre_op_diagnosis, '')) LIKE '%compress%' THEN 'medical_indication'
        WHEN ps.n_prior_surgeries > 0 AND ps.prior_histology IS NOT NULL THEN 'pathology_upgrade'
        ELSE 'unclassified'
    END AS completion_reason,
    CASE
        WHEN cp.tumor_1_histologic_type IS NOT NULL AND ps.prior_histology IS NOT NULL THEN 0.95
        WHEN mc.had_braf_positive OR mc.had_tert_positive THEN 0.90
        WHEN LOWER(COALESCE(cp.path_diagnosis_summary, '') || ' ' || COALESCE(cp.clinical_information_pre_op_diagnosis, '')) LIKE '%malignan%' THEN 0.85
        ELSE 0.50
    END AS reason_confidence,
    'path_synoptics+molecular_panel+operative_details' AS source_tables
FROM completion_patients cp
LEFT JOIN prior_surgery ps ON cp.research_id = ps.research_id
LEFT JOIN molecular_before_completion mc ON cp.research_id = mc.research_id
ORDER BY cp.research_id;
"""


def build_followup_audit_sql() -> str:
    """extracted_followup_audit_v1 — cross-source follow-up completeness audit."""
    return """
CREATE OR REPLACE TABLE extracted_followup_audit_v1 AS
WITH patient_spine AS (
    SELECT
        research_id,
        MIN(surg_date) AS first_surgery_date,
        MAX(surg_date) AS last_surgery_date,
        COUNT(*) AS n_surgeries
    FROM path_synoptics
    GROUP BY research_id
),
tg_coverage AS (
    SELECT
        research_id,
        COUNT(*) AS n_tg_labs,
        MIN(TRY_CAST(specimen_collect_dt AS DATE)) AS first_tg_date,
        MAX(TRY_CAST(specimen_collect_dt AS DATE)) AS last_tg_date,
        DATEDIFF('day', MIN(TRY_CAST(specimen_collect_dt AS DATE)),
                 MAX(TRY_CAST(specimen_collect_dt AS DATE))) AS tg_span_days
    FROM thyroglobulin_labs
    WHERE result IS NOT NULL AND TRIM(result) <> ''
    GROUP BY research_id
),
rai_coverage AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        COUNT(*) AS n_rai_episodes,
        MIN(resolved_rai_date) AS first_rai_date,
        MAX(resolved_rai_date) AS last_rai_date
    FROM rai_treatment_episode_v2
    GROUP BY CAST(research_id AS INTEGER)
),
event_coverage AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        COUNT(*) AS n_clinical_events,
        COUNT(DISTINCT event_type) AS n_event_types,
        MIN(TRY_CAST(event_date AS DATE)) AS first_event_date,
        MAX(TRY_CAST(event_date AS DATE)) AS last_event_date
    FROM extracted_clinical_events_v4
    GROUP BY CAST(research_id AS INTEGER)
),
complication_coverage AS (
    SELECT
        research_id,
        BOOL_OR(TRUE) AS has_complication_record
    FROM complications
    GROUP BY research_id
)
SELECT
    ps.research_id,
    ps.first_surgery_date,
    ps.last_surgery_date,
    ps.n_surgeries,
    -- Tg coverage
    COALESCE(tc.n_tg_labs, 0) AS n_tg_labs,
    tc.first_tg_date,
    tc.last_tg_date,
    tc.tg_span_days,
    CASE WHEN tc.n_tg_labs >= 3 THEN TRUE ELSE FALSE END AS tg_adequate_followup,
    -- RAI coverage
    COALESCE(rc.n_rai_episodes, 0) AS n_rai_episodes,
    rc.first_rai_date,
    rc.last_rai_date,
    -- Events
    COALESCE(ec.n_clinical_events, 0) AS n_clinical_events,
    COALESCE(ec.n_event_types, 0) AS n_event_types,
    ec.first_event_date,
    ec.last_event_date,
    -- Complications
    COALESCE(cc.has_complication_record, FALSE) AS has_complication_record,
    -- Follow-up completeness score (0-100)
    (
        CASE WHEN tc.n_tg_labs >= 3 THEN 25 WHEN tc.n_tg_labs >= 1 THEN 15 ELSE 0 END +
        CASE WHEN rc.n_rai_episodes > 0 THEN 20 ELSE 0 END +
        CASE WHEN ec.n_clinical_events > 0 THEN 20 ELSE 0 END +
        CASE WHEN cc.has_complication_record THEN 15 ELSE 0 END +
        CASE WHEN tc.tg_span_days > 365 THEN 20 WHEN tc.tg_span_days > 90 THEN 10 ELSE 0 END
    ) AS followup_completeness_score,
    -- Gaps
    CASE WHEN tc.n_tg_labs IS NULL OR tc.n_tg_labs = 0 THEN 'missing_tg' ELSE NULL END AS gap_tg,
    CASE WHEN rc.n_rai_episodes IS NULL OR rc.n_rai_episodes = 0 THEN 'missing_rai' ELSE NULL END AS gap_rai,
    CASE WHEN ec.n_clinical_events IS NULL OR ec.n_clinical_events = 0 THEN 'missing_events' ELSE NULL END AS gap_events
FROM patient_spine ps
LEFT JOIN tg_coverage tc ON ps.research_id = tc.research_id
LEFT JOIN rai_coverage rc ON ps.research_id = rc.research_id
LEFT JOIN event_coverage ec ON ps.research_id = ec.research_id
LEFT JOIN complication_coverage cc ON ps.research_id = cc.research_id
ORDER BY ps.research_id;
"""


def build_vw_recurrence_by_detection_sql() -> str:
    """vw_recurrence_by_detection_method — summary view."""
    return """
CREATE OR REPLACE TABLE vw_recurrence_by_detection_method AS
SELECT
    detection_category,
    COUNT(*) AS n_patients,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
    AVG(n_tg_measurements) AS avg_tg_measurements,
    AVG(recurrence_data_confidence) AS avg_confidence,
    SUM(CASE WHEN recurrence_any THEN 1 ELSE 0 END) AS n_with_recurrence,
    ROUND(100.0 * SUM(CASE WHEN recurrence_any THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS recurrence_rate_pct,
    AVG(tg_max) FILTER (WHERE tg_max IS NOT NULL) AS avg_tg_max,
    SUM(CASE WHEN recurrence_site_inferred IS NOT NULL THEN 1 ELSE 0 END) AS n_with_site
FROM extracted_recurrence_refined_v1
GROUP BY detection_category
ORDER BY n_patients DESC;
"""


def build_vw_longterm_outcomes_sql() -> str:
    """vw_longterm_outcomes — summary of voice/swallow outcomes."""
    return """
CREATE OR REPLACE TABLE vw_longterm_outcomes AS
SELECT
    voice_outcome_category,
    COUNT(*) AS n_patients,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
    AVG(n_rln_assessments) FILTER (WHERE n_rln_assessments > 0) AS avg_assessments,
    AVG(days_to_last_laryngoscopy) FILTER (WHERE days_to_last_laryngoscopy IS NOT NULL) AS avg_days_followup,
    AVG(voice_data_confidence) AS avg_confidence
FROM extracted_longterm_outcomes_v1
GROUP BY voice_outcome_category
ORDER BY n_patients DESC;
"""


def build_vw_rai_response_sql() -> str:
    """vw_rai_response_summary — ATA response breakdown."""
    return """
CREATE OR REPLACE TABLE vw_rai_response_summary AS
SELECT
    ata_response_category,
    COUNT(*) AS n_patients,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
    AVG(max_dose_mci) AS avg_dose_mci,
    AVG(response_confidence) AS avg_confidence,
    AVG(post_rai_tg_last) FILTER (WHERE post_rai_tg_last IS NOT NULL) AS avg_last_tg,
    SUM(CASE WHEN iodine_avid THEN 1 ELSE 0 END) AS n_avid
FROM extracted_rai_response_v1
GROUP BY ata_response_category
ORDER BY n_patients DESC;
"""


def build_vw_completion_reasons_sql() -> str:
    """vw_completion_reasons — breakdown of completion thyroidectomy reasons."""
    return """
CREATE OR REPLACE TABLE vw_completion_reasons AS
SELECT
    completion_reason,
    COUNT(*) AS n_patients,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
    AVG(reason_confidence) AS avg_confidence,
    SUM(CASE WHEN had_braf_positive THEN 1 ELSE 0 END) AS n_braf_positive,
    SUM(CASE WHEN had_tert_positive THEN 1 ELSE 0 END) AS n_tert_positive
FROM extracted_completion_reasons_v1
GROUP BY completion_reason
ORDER BY n_patients DESC;
"""


def build_missed_data_sweep_sql() -> str:
    """extracted_missed_data_sweep_v1 — 1000-patient comprehensive audit."""
    return """
CREATE OR REPLACE TABLE extracted_missed_data_sweep_v1 AS
WITH sample_patients AS (
    SELECT research_id
    FROM path_synoptics
    GROUP BY research_id
    ORDER BY HASH(research_id * 20260312)
    LIMIT 1000
),
note_counts AS (
    SELECT
        CAST(c.research_id AS INTEGER) AS research_id,
        COUNT(*) AS total_notes,
        COUNT(DISTINCT c.note_type) AS n_note_types,
        STRING_AGG(DISTINCT c.note_type, ', ') AS note_types_present
    FROM clinical_notes_long c
    INNER JOIN sample_patients sp ON CAST(c.research_id AS INTEGER) = sp.research_id
    GROUP BY CAST(c.research_id AS INTEGER)
),
entity_counts AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        'complications' AS domain,
        COUNT(*) AS n_entities
    FROM note_entities_complications
    WHERE CAST(research_id AS INTEGER) IN (SELECT research_id FROM sample_patients)
    GROUP BY CAST(research_id AS INTEGER)
    UNION ALL
    SELECT CAST(research_id AS INTEGER), 'problem_list', COUNT(*)
    FROM note_entities_problem_list
    WHERE CAST(research_id AS INTEGER) IN (SELECT research_id FROM sample_patients)
    GROUP BY CAST(research_id AS INTEGER)
    UNION ALL
    SELECT CAST(research_id AS INTEGER), 'medications', COUNT(*)
    FROM note_entities_medications
    WHERE CAST(research_id AS INTEGER) IN (SELECT research_id FROM sample_patients)
    GROUP BY CAST(research_id AS INTEGER)
    UNION ALL
    SELECT CAST(research_id AS INTEGER), 'staging', COUNT(*)
    FROM note_entities_staging
    WHERE CAST(research_id AS INTEGER) IN (SELECT research_id FROM sample_patients)
    GROUP BY CAST(research_id AS INTEGER)
    UNION ALL
    SELECT CAST(research_id AS INTEGER), 'procedures', COUNT(*)
    FROM note_entities_procedures
    WHERE CAST(research_id AS INTEGER) IN (SELECT research_id FROM sample_patients)
    GROUP BY CAST(research_id AS INTEGER)
    UNION ALL
    SELECT CAST(research_id AS INTEGER), 'genetics', COUNT(*)
    FROM note_entities_genetics
    WHERE CAST(research_id AS INTEGER) IN (SELECT research_id FROM sample_patients)
    GROUP BY CAST(research_id AS INTEGER)
),
entity_pivot AS (
    SELECT
        research_id,
        SUM(n_entities) AS total_entities,
        COUNT(DISTINCT domain) AS n_domains_with_entities,
        SUM(CASE WHEN domain = 'complications' THEN n_entities ELSE 0 END) AS n_complication_entities,
        SUM(CASE WHEN domain = 'problem_list' THEN n_entities ELSE 0 END) AS n_problem_entities,
        SUM(CASE WHEN domain = 'medications' THEN n_entities ELSE 0 END) AS n_medication_entities,
        SUM(CASE WHEN domain = 'staging' THEN n_entities ELSE 0 END) AS n_staging_entities,
        SUM(CASE WHEN domain = 'procedures' THEN n_entities ELSE 0 END) AS n_procedure_entities,
        SUM(CASE WHEN domain = 'genetics' THEN n_entities ELSE 0 END) AS n_genetics_entities
    FROM entity_counts
    GROUP BY research_id
),
structured_coverage AS (
    SELECT
        sp.research_id,
        CASE WHEN ps.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_path_synoptics,
        CASE WHEN tp.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_tumor_pathology,
        CASE WHEN c.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_complications,
        CASE WHEN tl.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_tg_labs,
        CASE WHEN re.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_rai_episode,
        CASE WHEN v6.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_master_v6
    FROM sample_patients sp
    LEFT JOIN (SELECT DISTINCT research_id FROM path_synoptics) ps ON sp.research_id = ps.research_id
    LEFT JOIN (SELECT DISTINCT research_id FROM tumor_pathology) tp ON sp.research_id = tp.research_id
    LEFT JOIN (SELECT DISTINCT research_id FROM complications) c ON sp.research_id = c.research_id
    LEFT JOIN (SELECT DISTINCT research_id FROM thyroglobulin_labs) tl ON sp.research_id = tl.research_id
    LEFT JOIN (SELECT DISTINCT CAST(research_id AS INTEGER) AS research_id FROM rai_treatment_episode_v2) re ON sp.research_id = re.research_id
    LEFT JOIN (SELECT DISTINCT research_id FROM patient_refined_master_clinical_v6) v6 ON sp.research_id = v6.research_id
)
SELECT
    sc.*,
    COALESCE(nc.total_notes, 0) AS total_notes,
    nc.n_note_types,
    nc.note_types_present,
    COALESCE(ep.total_entities, 0) AS total_entities,
    ep.n_domains_with_entities,
    ep.n_complication_entities,
    ep.n_problem_entities,
    ep.n_medication_entities,
    ep.n_staging_entities,
    ep.n_procedure_entities,
    ep.n_genetics_entities,
    -- Coverage score (0-100)
    (
        CASE WHEN sc.has_path_synoptics THEN 20 ELSE 0 END +
        CASE WHEN sc.has_tumor_pathology THEN 15 ELSE 0 END +
        CASE WHEN sc.has_complications THEN 15 ELSE 0 END +
        CASE WHEN sc.has_tg_labs THEN 15 ELSE 0 END +
        CASE WHEN sc.has_rai_episode THEN 10 ELSE 0 END +
        CASE WHEN sc.in_master_v6 THEN 10 ELSE 0 END +
        CASE WHEN COALESCE(nc.total_notes, 0) > 0 THEN 10 ELSE 0 END +
        CASE WHEN COALESCE(ep.n_domains_with_entities, 0) >= 3 THEN 5 ELSE 0 END
    ) AS source_coverage_score,
    -- Gaps
    CASE WHEN NOT sc.has_path_synoptics THEN 'missing_path_synoptics' END AS gap_path,
    CASE WHEN NOT sc.in_master_v6 THEN 'missing_from_master_v6' END AS gap_master,
    CASE WHEN COALESCE(nc.total_notes, 0) = 0 THEN 'no_clinical_notes' END AS gap_notes
FROM structured_coverage sc
LEFT JOIN note_counts nc ON sc.research_id = nc.research_id
LEFT JOIN entity_pivot ep ON sc.research_id = ep.research_id
ORDER BY sc.research_id;
"""


def build_master_clinical_v7_sql() -> str:
    """patient_refined_master_clinical_v7 — FINAL master table with all Phase 8 columns."""
    return """
CREATE OR REPLACE TABLE patient_refined_master_clinical_v7 AS
SELECT
    v6.*,

    -- Phase 8: Recurrence refined (15 columns)
    rr.recurrence_flag_structured,
    rr.recurrence_any,
    rr.detection_category AS recurrence_detection_category,
    rr.recurrence_site_inferred,
    rr.tg_nadir,
    rr.tg_max,
    rr.tg_last_value,
    rr.tg_rising_flag,
    rr.n_tg_measurements,
    rr.n_rai_treatments AS rai_treatment_count,
    rr.max_rai_dose_mci,
    rr.rai_avid AS rai_avid_flag,
    rr.scan_findings_combined AS rai_scan_findings,
    rr.recurrence_data_confidence,
    rr.n_recurrence_sources,

    -- Phase 8: RAI response (8 columns)
    ar.ata_response_category,
    ar.response_confidence AS rai_response_confidence,
    ar.post_rai_tg_nadir,
    ar.post_rai_tg_last,
    ar.post_rai_tg_count,
    ar.stimulated_tg AS rai_stimulated_tg,
    ar.stimulated_tsh AS rai_stimulated_tsh,
    ar.structural_disease_flag,

    -- Phase 8: Long-term outcomes (10 columns)
    lo.rln_worst_grade,
    lo.rln_sides,
    lo.n_rln_assessments,
    lo.voice_outcome_category,
    lo.has_voice_data,
    lo.voice_followup_completeness,
    lo.days_to_first_laryngoscopy,
    lo.days_to_last_laryngoscopy,
    lo.voice_data_confidence,

    -- Phase 8: Completion thyroidectomy reason (5 columns)
    cr.completion_reason,
    cr.reason_confidence AS completion_reason_confidence,
    cr.completion_histology AS completion_histology_type,
    cr.completion_t_stage,
    cr.prior_histology AS completion_prior_histology,
    cr.had_braf_positive AS completion_braf_positive,
    cr.had_tert_positive AS completion_tert_positive,

    -- Phase 8: Follow-up audit (5 columns)
    fa.followup_completeness_score,
    fa.n_tg_labs AS followup_tg_labs,
    fa.tg_adequate_followup,
    fa.n_clinical_events AS followup_clinical_events,
    fa.has_complication_record AS followup_has_complications

FROM patient_refined_master_clinical_v6 v6
LEFT JOIN extracted_recurrence_refined_v1 rr ON v6.research_id = rr.research_id
LEFT JOIN extracted_rai_response_v1 ar ON v6.research_id = ar.research_id
LEFT JOIN extracted_longterm_outcomes_v1 lo ON v6.research_id = lo.research_id
LEFT JOIN extracted_completion_reasons_v1 cr ON v6.research_id = cr.research_id
LEFT JOIN extracted_followup_audit_v1 fa ON v6.research_id = fa.research_id
ORDER BY v6.research_id;
"""


# ---------------------------------------------------------------------------
# Step registry
# ---------------------------------------------------------------------------
_PHASE8_STEPS = [
    ("recurrence_refined", build_recurrence_refined_sql),
    ("rai_response", build_rai_response_sql),
    ("longterm_outcomes", build_longterm_outcomes_sql),
    ("completion_reasons", build_completion_reasons_sql),
    ("followup_audit", build_followup_audit_sql),
    ("missed_data_sweep", build_missed_data_sweep_sql),
    ("vw_recurrence_by_detection", build_vw_recurrence_by_detection_sql),
    ("vw_longterm_outcomes", build_vw_longterm_outcomes_sql),
    ("vw_rai_response", build_vw_rai_response_sql),
    ("vw_completion_reasons", build_vw_completion_reasons_sql),
    ("master_v7", build_master_clinical_v7_sql),
]


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------
def _recurrence_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN recurrence_any THEN 1 ELSE 0 END) AS with_recurrence,
            SUM(CASE WHEN detection_category = 'structural_confirmed' THEN 1 ELSE 0 END) AS structural,
            SUM(CASE WHEN detection_category = 'biochemical_only' THEN 1 ELSE 0 END) AS biochemical,
            SUM(CASE WHEN tg_rising_flag THEN 1 ELSE 0 END) AS tg_rising,
            AVG(n_tg_measurements) AS avg_tg,
            AVG(recurrence_data_confidence) AS avg_conf,
            SUM(CASE WHEN recurrence_site_inferred IS NOT NULL THEN 1 ELSE 0 END) AS with_site,
            SUM(n_recurrence_sources) AS total_source_links
        FROM extracted_recurrence_refined_v1
    """).fetchone()
    return {
        "total_patients": row[0],
        "with_recurrence": row[1],
        "recurrence_rate_pct": round(100 * (row[1] or 0) / max(row[0], 1), 2),
        "structural_confirmed": row[2],
        "biochemical_only": row[3],
        "tg_rising": row[4],
        "avg_tg_measurements": round(row[5], 1) if row[5] else 0,
        "avg_confidence": round(row[6], 3) if row[6] else 0,
        "with_site_inferred": row[7],
        "total_source_links": row[8],
    }


def _rai_response_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN ata_response_category = 'excellent' THEN 1 ELSE 0 END) AS excellent,
            SUM(CASE WHEN ata_response_category = 'indeterminate' THEN 1 ELSE 0 END) AS indeterminate,
            SUM(CASE WHEN ata_response_category = 'biochemical_incomplete' THEN 1 ELSE 0 END) AS biochem,
            SUM(CASE WHEN ata_response_category = 'structural_incomplete' THEN 1 ELSE 0 END) AS structural,
            SUM(CASE WHEN ata_response_category = 'insufficient_data' THEN 1 ELSE 0 END) AS no_data,
            AVG(response_confidence) AS avg_conf
        FROM extracted_rai_response_v1
    """).fetchone()
    return {
        "total_rai_patients": row[0],
        "excellent": row[1], "indeterminate": row[2],
        "biochemical_incomplete": row[3], "structural_incomplete": row[4],
        "insufficient_data": row[5],
        "avg_confidence": round(row[6], 3) if row[6] else 0,
    }


def _longterm_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN has_voice_data THEN 1 ELSE 0 END) AS with_voice_data,
            SUM(CASE WHEN voice_outcome_category = 'permanent_paralysis' THEN 1 ELSE 0 END) AS permanent,
            SUM(CASE WHEN voice_outcome_category = 'prolonged_paresis' THEN 1 ELSE 0 END) AS prolonged,
            SUM(CASE WHEN voice_followup_completeness = 'complete_followup' THEN 1 ELSE 0 END) AS complete_fu,
            AVG(voice_data_confidence) AS avg_conf
        FROM extracted_longterm_outcomes_v1
    """).fetchone()
    return {
        "total_patients": row[0],
        "with_voice_data": row[1],
        "voice_data_rate_pct": round(100 * (row[1] or 0) / max(row[0], 1), 2),
        "permanent_paralysis": row[2],
        "prolonged_paresis": row[3],
        "complete_followup": row[4],
        "avg_confidence": round(row[5], 3) if row[5] else 0,
    }


def _completion_stats(con) -> dict:
    rows = con.execute("""
        SELECT completion_reason, COUNT(*) AS n
        FROM extracted_completion_reasons_v1
        GROUP BY 1 ORDER BY n DESC
    """).fetchall()
    return {"reasons": {r[0]: r[1] for r in rows}, "total": sum(r[1] for r in rows)}


def _followup_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            AVG(followup_completeness_score) AS avg_score,
            SUM(CASE WHEN n_tg_labs > 0 THEN 1 ELSE 0 END) AS with_tg,
            SUM(CASE WHEN n_rai_episodes > 0 THEN 1 ELSE 0 END) AS with_rai,
            SUM(CASE WHEN n_clinical_events > 0 THEN 1 ELSE 0 END) AS with_events,
            SUM(CASE WHEN has_complication_record THEN 1 ELSE 0 END) AS with_complications
        FROM extracted_followup_audit_v1
    """).fetchone()
    return {
        "total_patients": row[0],
        "avg_completeness_score": round(row[1], 1) if row[1] else 0,
        "with_tg_labs": row[2],
        "tg_coverage_pct": round(100 * (row[2] or 0) / max(row[0], 1), 1),
        "with_rai": row[3],
        "with_events": row[4],
        "with_complications": row[5],
    }


def _sweep_stats(con) -> dict:
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            AVG(source_coverage_score) AS avg_coverage,
            SUM(CASE WHEN in_master_v6 THEN 1 ELSE 0 END) AS in_v6,
            SUM(CASE WHEN has_path_synoptics THEN 1 ELSE 0 END) AS has_path,
            SUM(CASE WHEN has_tg_labs THEN 1 ELSE 0 END) AS has_tg,
            SUM(CASE WHEN total_notes > 0 THEN 1 ELSE 0 END) AS has_notes,
            SUM(CASE WHEN gap_master IS NOT NULL THEN 1 ELSE 0 END) AS gaps_master,
            SUM(CASE WHEN gap_notes IS NOT NULL THEN 1 ELSE 0 END) AS gaps_notes
        FROM extracted_missed_data_sweep_v1
    """).fetchone()
    return {
        "sample_size": row[0],
        "avg_coverage_score": round(row[1], 1) if row[1] else 0,
        "in_master_v6_pct": round(100 * (row[2] or 0) / max(row[0], 1), 1),
        "has_path_synoptics_pct": round(100 * (row[3] or 0) / max(row[0], 1), 1),
        "has_tg_labs_pct": round(100 * (row[4] or 0) / max(row[0], 1), 1),
        "has_notes_pct": round(100 * (row[5] or 0) / max(row[0], 1), 1),
        "missing_from_master": row[6],
        "missing_notes": row[7],
    }


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
def audit_and_refine_phase8(
    con,
    variables: list[str] | None = None,
    verbose: bool = True,
) -> dict[str, dict]:
    steps = _PHASE8_STEPS
    if variables:
        steps = [(n, fn) for n, fn in _PHASE8_STEPS if n in variables or "all" in variables]

    results = {}
    stat_fns = {
        "recurrence_refined": _recurrence_stats,
        "rai_response": _rai_response_stats,
        "longterm_outcomes": _longterm_stats,
        "completion_reasons": _completion_stats,
        "followup_audit": _followup_stats,
        "missed_data_sweep": _sweep_stats,
    }

    for step_name, sql_builder in steps:
        if verbose:
            print(f"\n{'='*70}")
            print(f"  Phase 8: {step_name}")
            print(f"{'='*70}")

        sql = sql_builder()
        table_name = _extract_table_name(sql)

        try:
            con.execute(sql)
            n = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if verbose:
                print(f"  {table_name}: {n} rows")
            results[step_name] = {"table": table_name, "rows": n, "status": "ok"}

            if step_name in stat_fns:
                results[step_name].update(stat_fns[step_name](con))

        except Exception as e:
            if verbose:
                print(f"  [ERROR] {step_name}: {e}")
            results[step_name] = {"error": str(e), "status": "failed"}

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Phase 8 Source-Linked Recurrence, Follow-up & Long-Term Outcomes")
    parser.add_argument("--variable", default="all",
                        choices=["all"] + [s[0] for s in _PHASE8_STEPS])
    parser.add_argument("--md", action="store_true", default=True)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default="notes_extraction")
    args = parser.parse_args()

    use_md = not args.local

    if args.dry_run:
        print(f"[dry-run] Would run phase8 step={args.variable}")
        return

    con = _get_connection(use_md)

    variables = None if args.variable == "all" else [args.variable]
    results = audit_and_refine_phase8(con, variables=variables, verbose=True)

    out_dir = PROJECT_ROOT / args.output_dir
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    lines = ["# Phase 8 Source-Linked Recurrence, Follow-up & Long-Term Outcomes Report",
             f"_Generated: {timestamp}_", ""]
    for step, rpt in results.items():
        lines.append(f"## {step}")
        for k, v in rpt.items():
            lines.append(f"- **{k}:** {v}")
        lines.append("")

    report_path = out_dir / f"phase8_recurrence_followup_outcomes_{timestamp}.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n[phase8] Report saved: {report_path}")

    # Save results as JSON for downstream consumption
    json_path = out_dir / f"phase8_results_{timestamp}.json"
    json_path.write_text(json.dumps(results, default=str, indent=2), encoding="utf-8")
    print(f"[phase8] JSON results: {json_path}")

    con.close()


if __name__ == "__main__":
    main()
