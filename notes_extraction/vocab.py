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
    "entity_date",
    "note_date",
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
