"""
Controlled vocabularies and normalisation maps for entity extraction.
"""

from __future__ import annotations

NOTE_TYPES: set[str] = {
    "h_p",
    "op_note",
    "dc_sum",
    "ed_note",
    "endocrine_note",
    "history_summary",
    "other_history",
    "other_notes",
}

ENTITY_SCHEMA_COLUMNS: list[str] = [
    "research_id",
    "note_row_id",
    "note_type",
    "entity_type",
    "entity_value_raw",
    "entity_value_norm",
    "present_or_negated",
    "confidence",
    "evidence_span",
    "evidence_start",
    "evidence_end",
    "extraction_method",
    "extracted_at",
]

# ── Procedures ───────────────────────────────────────────────────

PROCEDURE_NORM: dict[str, str] = {
    "total thyroidectomy": "total_thyroidectomy",
    "tt": "total_thyroidectomy",
    "bilateral thyroidectomy": "total_thyroidectomy",
    "near-total thyroidectomy": "total_thyroidectomy",
    "hemithyroidectomy": "hemithyroidectomy",
    "thyroid lobectomy": "hemithyroidectomy",
    "lobectomy": "hemithyroidectomy",
    "right lobectomy": "hemithyroidectomy",
    "left lobectomy": "hemithyroidectomy",
    "right thyroid lobectomy": "hemithyroidectomy",
    "left thyroid lobectomy": "hemithyroidectomy",
    "completion thyroidectomy": "completion_thyroidectomy",
    "completion": "completion_thyroidectomy",
    "central neck dissection": "central_neck_dissection",
    "cnd": "central_neck_dissection",
    "level vi dissection": "central_neck_dissection",
    "level vi": "central_neck_dissection",
    "lateral neck dissection": "lateral_neck_dissection",
    "lnd": "lateral_neck_dissection",
    "modified radical neck dissection": "modified_radical_neck_dissection",
    "mrnd": "modified_radical_neck_dissection",
    "modified radical": "modified_radical_neck_dissection",
    "parathyroid autotransplant": "parathyroid_autotransplant",
    "parathyroid autotransplantation": "parathyroid_autotransplant",
    "autotransplantation": "parathyroid_autotransplant",
    "tracheostomy": "tracheostomy",
    "laryngoscopy": "laryngoscopy",
    "flex laryngoscopy": "laryngoscopy",
    "flexible laryngoscopy": "laryngoscopy",
    "direct laryngoscopy": "laryngoscopy",
}

PROCEDURE_TYPES: set[str] = set(PROCEDURE_NORM.values())

# ── Complications ────────────────────────────────────────────────

COMPLICATION_NORM: dict[str, str] = {
    "rln injury": "rln_injury",
    "recurrent laryngeal nerve injury": "rln_injury",
    "recurrent laryngeal nerve": "rln_injury",
    "vocal cord paralysis": "vocal_cord_paralysis",
    "vcp": "vocal_cord_paralysis",
    "cord paralysis": "vocal_cord_paralysis",
    "cord palsy": "vocal_cord_paralysis",
    "vocal cord paresis": "vocal_cord_paresis",
    "cord paresis": "vocal_cord_paresis",
    "cord weakness": "vocal_cord_paresis",
    "hypocalcemia": "hypocalcemia",
    "low calcium": "hypocalcemia",
    "hypoparathyroidism": "hypoparathyroidism",
    "hematoma": "hematoma",
    "neck hematoma": "hematoma",
    "seroma": "seroma",
    "wound infection": "wound_infection",
    "ssi": "wound_infection",
    "surgical site infection": "wound_infection",
    "chyle leak": "chyle_leak",
    "chylous fistula": "chyle_leak",
}

COMPLICATION_TYPES: set[str] = set(COMPLICATION_NORM.values())

# ── Genetics ─────────────────────────────────────────────────────

GENE_NAMES: set[str] = {"BRAF", "NRAS", "HRAS", "KRAS", "RET", "TERT", "NTRK", "ALK"}

GENE_NORM: dict[str, str] = {
    "braf": "BRAF",
    "braf v600e": "BRAF",
    "brafv600e": "BRAF",
    "nras": "NRAS",
    "hras": "HRAS",
    "kras": "KRAS",
    "ras": "RAS",
    "ret": "RET",
    "ret/ptc": "RET",
    "tert": "TERT",
    "tert promoter": "TERT",
    "ntrk": "NTRK",
    "ntrk fusion": "NTRK",
    "alk": "ALK",
    "alk fusion": "ALK",
}

# ── Staging ──────────────────────────────────────────────────────

STAGING_COMPONENTS: set[str] = {"T_stage", "N_stage", "M_stage", "overall_stage"}

# ── Problem list / comorbidities ─────────────────────────────────

PROBLEM_TYPES: set[str] = {
    "hypertension",
    "diabetes_type2",
    "diabetes",
    "obesity",
    "CAD",
    "atrial_fibrillation",
    "hypothyroidism",
    "hyperthyroidism",
    "breast_cancer",
    "lung_cancer",
    "GERD",
    "CKD",
    "depression",
    "asthma",
    "COPD",
}

# ── Medications ──────────────────────────────────────────────────

MEDICATION_TYPES: set[str] = {
    "levothyroxine",
    "calcium_supplement",
    "calcitriol",
    "rai_dose",
}

MEDICATION_NORM: dict[str, str] = {
    "levothyroxine": "levothyroxine",
    "synthroid": "levothyroxine",
    "levoxyl": "levothyroxine",
    "l-thyroxine": "levothyroxine",
    "calcium carbonate": "calcium_supplement",
    "calcium citrate": "calcium_supplement",
    "caltrate": "calcium_supplement",
    "tums": "calcium_supplement",
    "oscal": "calcium_supplement",
    "citracal": "calcium_supplement",
    "calcitriol": "calcitriol",
    "rocaltrol": "calcitriol",
}

# ── Negation cues ────────────────────────────────────────────────

NEGATION_CUES: list[str] = [
    "no ",
    "no evidence of",
    "without ",
    "denies ",
    "denied ",
    "negative for ",
    "not ",
    "ruled out",
    "rules out",
    "r/o ",
    "absent",
    "unlikely",
]
