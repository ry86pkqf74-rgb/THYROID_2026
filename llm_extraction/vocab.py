"""
Controlled vocabularies and normalisation maps for entity extraction.
"""

from __future__ import annotations

import pandas as pd

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
    "entity_domain",
    "episode_id",
    "note_type",
    "note_index",
    "source_sheet",
    "source_column",
    "entity_type",
    "entity_value_raw",
    "entity_value_norm",
    "present_or_negated",
    "confidence",
    "confidence_score",
    "evidence_span",
    "evidence_start",
    "evidence_end",
    "entity_date",
    "note_date",
    "extraction_method",
    "extracted_at",
    "extraction_timestamp_utc",
    # LLM / verification provenance (nullable for regex extractors)
    "date_confidence",
    "source_line",
    "chunk_index",
    "chunk_char_start",
    "chunk_char_end",
    "evidence_global_start",
    "evidence_global_end",
    "raw_response_sha256",
    "verification_status",
    "verification_step",
    # Run / model / verifier provenance (nullable for legacy rows)
    "extraction_run_id",
    "extractor_name",
    "extractor_version",
    "llm_model",
    "model_name",
    "model_version",
    "llm_prompt_version",
    "prompt_version",
    "verifier_name",
    "verifier_version",
    # Nullable LLM transport / provider metadata (older rows omit these)
    "llm_provider",
    "llm_base_url",
    "llm_sdk",
    "llm_sdk_version",
    "provider_returned_model",
    "provider_system_fingerprint",
]

# Nullable pandas dtypes for round-tripped entity DataFrames (explicit, backward-compatible).
ENTITY_SCHEMA_DTYPES: dict[str, str] = {
    "research_id": "Int64",
    "note_row_id": "string",
    "entity_domain": "string",
    "episode_id": "string",
    "note_type": "string",
    "note_index": "Int64",
    "source_sheet": "string",
    "source_column": "string",
    "entity_type": "string",
    "entity_value_raw": "string",
    "entity_value_norm": "string",
    "present_or_negated": "string",
    "confidence": "float64",
    "confidence_score": "float64",
    "evidence_span": "string",
    "evidence_start": "Int64",
    "evidence_end": "Int64",
    "entity_date": "object",
    "note_date": "object",
    "extraction_method": "string",
    "extracted_at": "string",
    "extraction_timestamp_utc": "string",
    "date_confidence": "float64",
    "source_line": "Int64",
    "chunk_index": "Int64",
    "chunk_char_start": "Int64",
    "chunk_char_end": "Int64",
    "evidence_global_start": "Int64",
    "evidence_global_end": "Int64",
    "raw_response_sha256": "string",
    "verification_status": "string",
    "verification_step": "string",
    "extraction_run_id": "string",
    "extractor_name": "string",
    "extractor_version": "string",
    "llm_model": "string",
    "model_name": "string",
    "model_version": "string",
    "llm_prompt_version": "string",
    "prompt_version": "string",
    "verifier_name": "string",
    "verifier_version": "string",
    "llm_provider": "string",
    "llm_base_url": "string",
    "llm_sdk": "string",
    "llm_sdk_version": "string",
    "provider_returned_model": "string",
    "provider_system_fingerprint": "string",
}

# Defaults for new provenance fields when absent (legacy rows, regex-only paths).
PROVENANCE_FIELD_DEFAULTS: dict[str, object | None] = {
    "episode_id": None,
    "note_index": None,
    "source_sheet": None,
    "source_column": None,
    "confidence_score": None,
    "extraction_timestamp_utc": None,
    "date_confidence": None,
    "source_line": None,
    "chunk_index": 0,
    "chunk_char_start": 0,
    "chunk_char_end": 0,
    "evidence_global_start": None,
    "evidence_global_end": None,
    "raw_response_sha256": None,
    "verification_status": "unverified",
    "verification_step": "none",
    "extraction_run_id": None,
    "extractor_name": None,
    "extractor_version": None,
    "llm_model": None,
    "model_name": None,
    "model_version": None,
    "llm_prompt_version": None,
    "prompt_version": None,
    "verifier_name": None,
    "verifier_version": None,
    "entity_domain": None,
    "llm_provider": None,
    "llm_base_url": None,
    "llm_sdk": None,
    "llm_sdk_version": None,
    "provider_returned_model": None,
    "provider_system_fingerprint": None,
}

# Canonical row order before every entity parquet write (stable mergesort; skip missing columns).
ENTITY_SORT_KEY_ORDER: tuple[str, ...] = (
    "research_id",
    "note_row_id",
    "entity_domain",
    "entity_type",
    "entity_date",
    "note_date",
    "entity_value_norm",
    "present_or_negated",
    "chunk_index",
    "source_line",
    "evidence_global_start",
    "evidence_global_end",
    "extraction_method",
    "raw_response_sha256",
)


def sort_entities_deterministic(df: pd.DataFrame) -> pd.DataFrame:
    """Return *df* sorted deterministically for reproducible parquet row order.

    Uses stable mergesort. Only columns present in *df* participate; others are ignored.
    ``entity_date`` / ``note_date`` are ordered via temporary UTC-normalized timestamps so
    ISO strings and ``datetime``/``Timestamp`` cells sort consistently.
    """
    if df.empty:
        return df.copy()
    keys = [c for c in ENTITY_SORT_KEY_ORDER if c in df.columns]
    if not keys:
        return df.copy()
    work = df.copy()
    helpers: list[str] = []
    sort_keys: list[str] = []
    for k in keys:
        if k in ("entity_date", "note_date"):
            h = f"__sort_ts__{k}"
            # UTC-naive ok: keys are date-only strings; mixed tz in cells is coerced best-effort
            work[h] = pd.to_datetime(work[k], errors="coerce", utc=True)
            sort_keys.append(h)
            helpers.append(h)
        else:
            sort_keys.append(k)
    out = work.sort_values(by=sort_keys, kind="mergesort", na_position="last")
    if helpers:
        out = out.drop(columns=helpers)
    return out.reset_index(drop=True)


# Optional casts on canonical long output (post-contract) for stable parquet types.
CANONICAL_FACT_CONTRACT_DTYPES: dict[str, str] = {
    "linkage_confidence": "float64",
    "date_source_type": "string",
    "source_text_hash": "string",
    "source_text_span_start": "float64",
    "source_text_span_end": "float64",
    "source_file_id": "string",
    "canonical_domain": "string",
    "canonical_fact_type": "string",
    "fact_provenance_category": "string",
}

# Aligned with config/extraction_domain_registry.yaml schema_version
EXTRACTOR_BUILD_VERSION = "entity_schema_v2.2_2026-05-06"

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

# ── V2 molecular detail normalisation ─────────────────────────────

MOLECULAR_PLATFORM_NORM: dict[str, str] = {
    "thyroseq": "ThyroSeq",
    "thyroseq v2": "ThyroSeq_v2",
    "thyroseq v3": "ThyroSeq_v3",
    "afirma": "Afirma",
    "afirma gsc": "Afirma_GSC",
    "afirma gec": "Afirma_GEC",
    "thygennext": "ThyGenNEXT",
    "thygenx": "ThyGenX",
    "rosetta gx reveal": "Rosetta_GX",
}

MOLECULAR_RESULT_NORM: dict[str, str] = {
    "positive": "positive",
    "detected": "positive",
    "negative": "negative",
    "not detected": "negative",
    "benign": "negative",
    "suspicious": "suspicious",
    "indeterminate": "indeterminate",
    "inadequate": "non_diagnostic",
    "insufficient": "non_diagnostic",
    "cancelled": "cancelled",
}

# ── V2 RAI treatment normalisation ────────────────────────────────

RAI_INTENT_NORM: dict[str, str] = {
    "remnant ablation": "remnant_ablation",
    "ablation": "remnant_ablation",
    "adjuvant": "adjuvant",
    "metastatic": "metastatic_disease",
    "persistent disease": "persistent_disease",
    "recurrence": "recurrence",
}

RAI_STATUS_NORM: dict[str, str] = {
    "received": "completed",
    "completed": "completed",
    "administered": "completed",
    "recommended": "recommended",
    "planned": "recommended",
    "declined": "declined",
}

# ── V2 imaging normalisation ──────────────────────────────────────

COMPOSITION_NORM: dict[str, str] = {
    "solid": "solid",
    "cystic": "cystic",
    "mixed": "mixed",
    "mixed cystic and solid": "mixed",
    "predominantly solid": "predominantly_solid",
    "predominantly cystic": "predominantly_cystic",
    "spongiform": "spongiform",
}

ECHOGENICITY_NORM: dict[str, str] = {
    "hyperechoic": "hyperechoic",
    "isoechoic": "isoechoic",
    "hypoechoic": "hypoechoic",
    "markedly hypoechoic": "markedly_hypoechoic",
    "very hypoechoic": "markedly_hypoechoic",
    "anechoic": "anechoic",
}

# ── V2 operative normalisation ────────────────────────────────────

OPERATIVE_FINDING_NORM: dict[str, str] = {
    "rln identified": "rln_intact",
    "rln preserved": "rln_intact",
    "rln intact": "rln_intact",
    "rln injury": "rln_injured",
    "nerve monitoring": "ionm_used",
    "ionm": "ionm_used",
    "nim": "ionm_used",
    "parathyroid autotransplant": "parathyroid_autograft",
    "parathyroid reimplant": "parathyroid_autograft",
    "gross ete": "gross_ete",
    "tracheal invasion": "tracheal_involvement",
    "esophageal invasion": "esophageal_involvement",
    "strap muscle invasion": "strap_muscle_involvement",
}

# ── V2 histology detail normalisation ─────────────────────────────

HISTOLOGY_DETAIL_NORM: dict[str, str] = {
    "capsular invasion": "capsular_invasion",
    "perineural invasion": "perineural_invasion",
    "extranodal extension": "extranodal_extension",
    "positive margin": "margin_positive",
    "negative margin": "margin_negative",
    "close margin": "margin_close",
    "niftp": "NIFTP",
    "pdtc": "PDTC",
    "minimally invasive": "minimally_invasive",
    "widely invasive": "widely_invasive",
    "encapsulated": "encapsulated",
}

# ── Additional normalization maps (v2 audit) ───────────────────────────

MARGIN_NORM: dict[str, str] = {
    "positive": "positive",
    "involved": "positive",
    "present at margin": "positive",
    "negative": "negative",
    "free": "negative",
    "uninvolved": "negative",
    "clear": "negative",
    "close": "close",
    "near": "close",
}

ETE_DETAIL_NORM: dict[str, str] = {
    "no": "none",
    "none": "none",
    "absent": "none",
    "not identified": "none",
    "not present": "none",
    "negative": "none",
    "minimal": "microscopic",
    "microscopic": "microscopic",
    "minor": "microscopic",
    "focal": "microscopic",
    "gross": "gross",
    "extensive": "gross",
}

AGGRESSIVE_VARIANT_NORM: dict[str, str] = {
    "tall cell": "tall_cell",
    "tall cell variant": "tall_cell",
    "hobnail": "hobnail",
    "hobnail variant": "hobnail",
    "columnar": "columnar_cell",
    "columnar cell": "columnar_cell",
    "columnar cell variant": "columnar_cell",
    "diffuse sclerosing": "diffuse_sclerosing",
    "insular": "insular",
    "solid": "solid_variant",
    "cribriform": "cribriform_morular",
    "cribriform-morular": "cribriform_morular",
    "warthin-like": "warthin_like",
    "oncocytic": "oncocytic",
}

VASCULAR_INVASION_NORM: dict[str, str] = {
    "present": "present",
    "yes": "present",
    "identified": "present",
    "positive": "present",
    "focal": "focal",
    "rare": "focal",
    "few": "focal",
    "isolated": "focal",
    "extensive": "extensive",
    "multifocal": "extensive",
    "widespread": "extensive",
    "absent": "absent",
    "no": "absent",
    "none": "absent",
    "not identified": "absent",
    "negative": "absent",
}

MOLECULAR_VARIANT_NORM: dict[str, str] = {
    "braf v600e": "BRAF_V600E",
    "braf v600": "BRAF_V600",
    "brafv600e": "BRAF_V600E",
    "nras q61r": "NRAS_Q61R",
    "nras q61k": "NRAS_Q61K",
    "nras": "NRAS",
    "hras q61r": "HRAS_Q61R",
    "hras": "HRAS",
    "kras g12": "KRAS_G12",
    "kras": "KRAS",
    "tert c228t": "TERT_C228T",
    "tert c250t": "TERT_C250T",
    "tert promoter": "TERT_promoter",
    "tp53": "TP53",
    "eif1ax": "EIF1AX",
    "pten": "PTEN",
    "alk": "ALK",
    "ntrk1": "NTRK1",
    "ntrk2": "NTRK2",
    "ntrk3": "NTRK3",
}

GENE_FUSION_NORM: dict[str, str] = {
    "ret/ptc1": "RET_PTC1",
    "ret-ptc1": "RET_PTC1",
    "ret/ptc3": "RET_PTC3",
    "ret-ptc3": "RET_PTC3",
    "ret/ptc": "RET_PTC",
    "ret-ptc": "RET_PTC",
    "ret fusion": "RET_fusion",
    "pax8-pparg": "PAX8_PPARG",
    "pax8/pparg": "PAX8_PPARG",
    "ntrk fusion": "NTRK_fusion",
    "ntrk1 fusion": "NTRK1_fusion",
    "ntrk3 fusion": "NTRK3_fusion",
    "alk fusion": "ALK_fusion",
    "braf fusion": "BRAF_fusion",
}

SHAPE_NORM: dict[str, str] = {
    "wider than tall": "wider_than_tall",
    "taller than wide": "taller_than_wide",
    "round": "round",
    "oval": "oval",
    "irregular": "irregular",
    "lobulated": "lobulated",
}

CALCIFICATION_NORM: dict[str, str] = {
    "microcalcifications": "micro",
    "microcalcification": "micro",
    "micro": "micro",
    "macrocalcifications": "macro",
    "macrocalcification": "macro",
    "macro": "macro",
    "peripheral": "peripheral",
    "rim": "peripheral",
    "eggshell": "peripheral",
    "coarse": "coarse",
    "none": "none",
    "no calcification": "none",
    "no calcifications": "none",
}

VASCULARITY_NORM: dict[str, str] = {
    "avascular": "avascular",
    "hypovascular": "hypovascular",
    "mildly vascular": "mildly_vascular",
    "moderately vascular": "moderately_vascular",
    "hypervascular": "hypervascular",
    "markedly vascular": "hypervascular",
    "peripheral vascularity": "peripheral",
    "intranodular vascularity": "intranodular",
    "mixed vascularity": "mixed",
}

# ── Phase 8: Recurrence & long-term outcome normalisation ──────────

RECURRENCE_SITE_NORM: dict[str, str] = {
    "thyroid bed": "local",
    "central neck": "regional_central",
    "central compartment": "regional_central",
    "level vi": "regional_central",
    "lateral neck": "regional_lateral",
    "lateral compartment": "regional_lateral",
    "cervical lymph node": "regional_lateral",
    "level ii": "regional_lateral",
    "level iii": "regional_lateral",
    "level iv": "regional_lateral",
    "level v": "regional_lateral",
    "mediastinal": "regional_mediastinal",
    "superior mediastinum": "regional_mediastinal",
    "lung": "distant_lung",
    "pulmonary": "distant_lung",
    "lung metastasis": "distant_lung",
    "bone": "distant_bone",
    "osseous": "distant_bone",
    "bone metastasis": "distant_bone",
    "brain": "distant_brain",
    "cerebral": "distant_brain",
    "liver": "distant_liver",
    "hepatic": "distant_liver",
    "distant": "distant_other",
}

RECURRENCE_DETECTION_NORM: dict[str, str] = {
    "ultrasound": "structural_imaging",
    "us": "structural_imaging",
    "neck ultrasound": "structural_imaging",
    "ct": "structural_imaging",
    "ct scan": "structural_imaging",
    "mri": "structural_imaging",
    "pet": "structural_imaging",
    "pet/ct": "structural_imaging",
    "fdg pet": "structural_imaging",
    "rai scan": "functional_imaging",
    "whole body scan": "functional_imaging",
    "wbs": "functional_imaging",
    "i-131 scan": "functional_imaging",
    "diagnostic scan": "functional_imaging",
    "thyroglobulin": "biochemical",
    "tg": "biochemical",
    "tg rise": "biochemical",
    "rising tg": "biochemical",
    "stimulated tg": "biochemical",
    "anti-tg": "biochemical",
    "biopsy": "cytologic",
    "fna": "cytologic",
    "fine needle": "cytologic",
    "pathology": "histologic",
    "surgical pathology": "histologic",
    "clinical exam": "clinical",
    "palpable mass": "clinical",
    "physical exam": "clinical",
}

RAI_RESPONSE_NORM: dict[str, str] = {
    "excellent response": "excellent",
    "excellent": "excellent",
    "complete response": "excellent",
    "no evidence of disease": "excellent",
    "ned": "excellent",
    "indeterminate response": "indeterminate",
    "indeterminate": "indeterminate",
    "biochemical incomplete": "biochemical_incomplete",
    "rising tg": "biochemical_incomplete",
    "elevated tg": "biochemical_incomplete",
    "structural incomplete": "structural_incomplete",
    "structural disease": "structural_incomplete",
    "persistent disease": "structural_incomplete",
    "residual disease": "structural_incomplete",
}

VOICE_OUTCOME_NORM: dict[str, str] = {
    "normal voice": "normal",
    "normal": "normal",
    "no hoarseness": "normal",
    "hoarseness": "hoarse",
    "hoarse": "hoarse",
    "dysphonia": "hoarse",
    "voice change": "hoarse",
    "breathy": "breathy_voice",
    "breathy voice": "breathy_voice",
    "vocal fatigue": "vocal_fatigue",
    "voice fatigue": "vocal_fatigue",
    "dysphagia": "dysphagia",
    "swallowing difficulty": "dysphagia",
    "difficulty swallowing": "dysphagia",
    "aspiration": "aspiration",
    "vocal cord paralysis": "paralysis",
    "cord paralysis": "paralysis",
    "vocal cord paresis": "paresis",
    "cord paresis": "paresis",
    "recovered": "recovered",
    "resolved": "recovered",
    "improving": "improving",
    "persistent": "persistent",
    "permanent": "permanent",
}

COMPLETION_REASON_NORM: dict[str, str] = {
    "malignancy": "pathology_upgrade",
    "malignant": "pathology_upgrade",
    "cancer found": "pathology_upgrade",
    "papillary thyroid cancer": "pathology_upgrade",
    "ptc": "pathology_upgrade",
    "suspicious pathology": "pathology_upgrade",
    "molecular": "molecular_result",
    "molecular testing": "molecular_result",
    "thyroseq": "molecular_result",
    "afirma suspicious": "molecular_result",
    "braf positive": "molecular_result",
    "growing nodule": "imaging_concern",
    "enlarging": "imaging_concern",
    "suspicious node": "imaging_concern",
    "suspicious lymph node": "imaging_concern",
    "contralateral nodule": "imaging_concern",
    "patient preference": "patient_preference",
    "patient request": "patient_preference",
    "elective": "patient_preference",
    "prophylactic": "prophylactic",
    "graves": "medical_indication",
    "hyperthyroidism": "medical_indication",
    "compressive symptoms": "medical_indication",
    "airway": "medical_indication",
}
