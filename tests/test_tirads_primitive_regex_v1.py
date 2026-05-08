"""Unit tests for scripts/411_tirads_primitive_regex_v1.py.

Covers every composition / echogenicity / shape / margins / echogenic-foci
category plus halo / vascularity / ETE-on-US categories and critical
negation cases. Must pass fully before the BQ extraction run.
"""
import importlib, json, sys, types, pathlib

# ---------------------------------------------------------------------------
# Import the module without triggering __main__ or requiring google-cloud-bigquery
# ---------------------------------------------------------------------------
_MOD_PATH = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "411_tirads_primitive_regex_v1.py"
_spec = importlib.util.spec_from_file_location("tirads_regex", _MOD_PATH)
_mod = importlib.util.module_from_spec(_spec)
# Stub out google.cloud so import works without the package
_gc = types.ModuleType("google")
_gc.cloud = types.ModuleType("google.cloud")  # type: ignore
_gc.cloud.bigquery = types.ModuleType("google.cloud.bigquery")  # type: ignore
sys.modules.setdefault("google", _gc)
sys.modules.setdefault("google.cloud", _gc.cloud)
sys.modules.setdefault("google.cloud.bigquery", _gc.cloud.bigquery)
sys.modules.setdefault("google.oauth2", types.ModuleType("google.oauth2"))
sys.modules.setdefault("google.oauth2.credentials", types.ModuleType("google.oauth2.credentials"))
_spec.loader.exec_module(_mod)

X = _mod  # convenience alias

# ===========================================================================
# Composition tests
# ===========================================================================

def test_composition_spongiform():
    result = X.extract_composition(["The nodule is spongiform in appearance."])
    assert result == "spongiform"

def test_composition_purely_cystic():
    result = X.extract_composition(["Purely cystic structure measuring 1.2 cm."])
    assert result == "cystic"

def test_composition_mixed_cystic_solid():
    result = X.extract_composition(["Mixed cystic and solid nodule with echogenic component."])
    assert result == "mixed_cystic_solid"

def test_composition_mixed_complex():
    result = X.extract_composition(["Complex cystic nodule with internal solid component."])
    assert result == "mixed_cystic_solid"

def test_composition_predominantly_solid():
    result = X.extract_composition(["Predominantly solid hypoechoic nodule."])
    assert result == "predominantly_solid"

def test_composition_solid():
    result = X.extract_composition(["Solid isoechoic nodule."])
    assert result == "solid"

def test_composition_negation_no_solid():
    # "No solid component" should not return "solid"
    result = X.extract_composition(["No solid component identified. Entirely cystic lesion."])
    assert result == "cystic"

def test_composition_predominantly_cystic_maps_to_mixed():
    result = X.extract_composition(["Predominantly cystic nodule."])
    assert result == "mixed_cystic_solid"

# ===========================================================================
# Echogenicity tests
# ===========================================================================

def test_echogenicity_anechoic():
    result = X.extract_echogenicity(["Anechoic cyst with thin walls."])
    assert result == "anechoic"

def test_echogenicity_hyperechoic():
    result = X.extract_echogenicity(["Hyperechoic nodule in the right lobe."])
    assert result == "hyperechoic"

def test_echogenicity_isoechoic():
    result = X.extract_echogenicity(["Isoechoic solid nodule."])
    assert result == "isoechoic"

def test_echogenicity_hypoechoic():
    result = X.extract_echogenicity(["Hypoechoic nodule with smooth margins."])
    assert result == "hypoechoic"

def test_echogenicity_very_hypoechoic_markedly():
    result = X.extract_echogenicity(["Markedly hypoechoic nodule with irregular margins."])
    assert result == "very_hypoechoic"

def test_echogenicity_very_hypoechoic_marked_noun():
    result = X.extract_echogenicity(["Marked hypoechogenicity noted."])
    assert result == "very_hypoechoic"

def test_echogenicity_very_wins_over_plain_hypo():
    """very_hypoechoic pattern must fire before plain hypoechoic."""
    result = X.extract_echogenicity(["Very hypoechoic solid lesion."])
    assert result == "very_hypoechoic"

# ===========================================================================
# Shape tests
# ===========================================================================

def test_shape_taller_than_wide():
    result = X.extract_shape(["Taller than wide nodule."])
    assert result == "taller_than_wide"

def test_shape_taller_ap_greater():
    result = X.extract_shape(["AP dimension greater than transverse dimension."])
    assert result == "taller_than_wide"

def test_shape_wider_than_tall():
    result = X.extract_shape(["Wider than tall with smooth margins."])
    assert result == "wider_than_tall"

def test_shape_none_when_absent():
    result = X.extract_shape(["Solid hypoechoic nodule."])
    assert result is None

# ===========================================================================
# Margins tests
# ===========================================================================

def test_margins_smooth_well_defined():
    result = X.extract_margins(["Well-defined, smooth margins."])
    assert result == "smooth"

def test_margins_well_circumscribed():
    result = X.extract_margins(["Well-circumscribed solid nodule."])
    assert result == "smooth"

def test_margins_ill_defined():
    result = X.extract_margins(["Ill-defined hypoechoic nodule."])
    assert result == "ill_defined"

def test_margins_lobulated():
    result = X.extract_margins(["Lobulated contour solid nodule."])
    assert result == "lobulated"

def test_margins_microlobulated():
    result = X.extract_margins(["Microlobulated margins noted."])
    assert result == "microlobulated"

def test_margins_irregular_spiculated():
    result = X.extract_margins(["Spiculated hypoechoic nodule."])
    assert result == "irregular"

def test_margins_ete():
    result = X.extract_margins(["Extrathyroidal extension present."])
    assert result == "extrathyroidal_extension"

# ===========================================================================
# Echogenic foci tests
# ===========================================================================

def test_ef_none_explicit():
    result = X.extract_echogenic_foci(["No echogenic foci identified."])
    assert "none" in result

def test_ef_punctate():
    result = X.extract_echogenic_foci(["Punctate echogenic foci noted."])
    assert "punctate_echogenic_foci" in result

def test_ef_microcalcifications():
    result = X.extract_echogenic_foci(["Multiple microcalcifications seen."])
    assert "punctate_echogenic_foci" in result

def test_ef_macrocalcifications():
    result = X.extract_echogenic_foci(["Coarse calcifications present."])
    assert "macrocalcifications" in result

def test_ef_peripheral_rim():
    result = X.extract_echogenic_foci(["Peripheral rim calcifications."])
    assert "peripheral_rim_calcifications" in result

def test_ef_comet_tail():
    result = X.extract_echogenic_foci(["Large comet-tail artifact noted."])
    assert "large_comet_tail_artifacts" in result

def test_ef_negation_no_microcalc():
    result = X.extract_echogenic_foci(["No microcalcifications identified."])
    # should not contain punctate; could contain 'none'
    assert "punctate_echogenic_foci" not in result

def test_ef_multiple_findings():
    result = X.extract_echogenic_foci(
        ["Punctate echogenic foci and peripheral rim calcifications."]
    )
    assert "punctate_echogenic_foci" in result
    assert "peripheral_rim_calcifications" in result
    assert "none" not in result

# ===========================================================================
# Halo tests
# ===========================================================================

def test_halo_present():
    h = X.extract_halo(["There is a peripheral halo around the nodule."])
    assert h["presence"] == "present"

def test_halo_absent():
    h = X.extract_halo(["No halo is identified."])
    assert h["presence"] == "absent"

def test_halo_complete():
    h = X.extract_halo(["Complete halo present."])
    assert h["presence"] == "present"
    assert h["completeness"] == "complete"

def test_halo_incomplete():
    h = X.extract_halo(["Partial halo present."])
    assert h["presence"] == "present"
    assert h["completeness"] == "incomplete"

def test_halo_hypoechoic_rim():
    h = X.extract_halo(["Hypoechoic rim surrounding the nodule."])
    assert h["presence"] == "present"
    assert h["hypoechoic_rim_wording_present"] is True

def test_halo_doppler_ring():
    h = X.extract_halo(["Ring of vascularity at the periphery."])
    assert h["doppler_ring_present"] == "true"

def test_halo_unstated():
    h = X.extract_halo(["Solid hypoechoic nodule with smooth margins."])
    assert h["presence"] == "unstated"

# ===========================================================================
# Vascularity tests
# ===========================================================================

def test_vasc_inferno():
    v = X.extract_vascularity(["Thyroid inferno pattern."])
    assert v["intensity"] == "thyroid_inferno"

def test_vasc_markedly_increased():
    v = X.extract_vascularity(["Markedly increased flow on Doppler."])
    assert v["intensity"] == "markedly_increased"

def test_vasc_increased():
    v = X.extract_vascularity(["Increased vascularity noted."])
    assert v["intensity"] == "increased"

def test_vasc_normal():
    v = X.extract_vascularity(["Normal flow pattern."])
    assert v["intensity"] == "normal"

def test_vasc_absent():
    v = X.extract_vascularity(["No internal vascularity."])
    assert v["intensity"] == "none"

def test_vasc_peripheral():
    v = X.extract_vascularity(["Peripheral vascularity only."])
    assert v["distribution"] == "peripheral"

def test_vasc_intranodular():
    v = X.extract_vascularity(["Intranodular flow present."])
    assert v["distribution"] == "intranodular"

# ===========================================================================
# ETE-on-US tests
# ===========================================================================

def test_ete_none():
    e = X.extract_ete_us(["No extrathyroidal extension identified."])
    assert e["presence"] == "none"

def test_ete_abutment():
    e = X.extract_ete_us(["Nodule abuts the capsule."])
    assert e["presence"] == "abutment"

def test_ete_bulging():
    e = X.extract_ete_us(["Contour bulging of the thyroid capsule."])
    assert e["presence"] == "bulging"

def test_ete_capsule_loss():
    e = X.extract_ete_us(["Loss of capsule line noted."])
    assert e["presence"] == "capsule_loss"

def test_ete_strap_muscle():
    e = X.extract_ete_us(["Strap muscle invasion present."])
    assert e["presence"] == "strap_muscle_invasion"

def test_ete_percent_grade():
    e = X.extract_ete_us(["30% of perimeter in contact with capsule."])
    assert e["abutment_percent_perimeter"] == 30.0
    assert e["grade"] == 2  # 26-50 → grade 2

def test_ete_explicit_grade():
    e = X.extract_ete_us(["ETE grade 3 per radiologist."])
    assert e["grade"] == 3

# ===========================================================================
# TI-RADS system hint tests
# ===========================================================================

import datetime as _dt

def test_tirads_hint_acr2017_by_tr():
    import datetime
    result = X.extract_tirads_system_hint(
        ["TR4 nodule, recommend FNA."], exam_date=datetime.date(2020, 1, 1)
    )
    assert result == "ACR2017"

def test_tirads_hint_kwak():
    result = X.extract_tirads_system_hint(["K-TIRADS 4 by Kwak classification."])
    assert result in ("Kwak", "KTIRADS")

def test_tirads_hint_pre2017_unspecified():
    import datetime
    result = X.extract_tirads_system_hint(
        ["TI-RADS 3 per standard criteria."], exam_date=datetime.date(2015, 6, 1)
    )
    assert result == "unspecified"

def test_tirads_hint_none():
    result = X.extract_tirads_system_hint(["Solid nodule with smooth margins."])
    assert result is None

# ===========================================================================
# Integration / top-level extract_nodule_features tests
# ===========================================================================

def test_full_extraction_high_confidence():
    text = (
        "Solid hypoechoic nodule in the right lobe. Wider than tall. "
        "Well-defined margins. No echogenic foci. No halo. "
        "Absent internal vascularity."
    )
    result = X.extract_nodule_features(text, {}, {})
    assert result["composition_regex"] == "solid"
    assert result["echogenicity_regex"] == "hypoechoic"
    assert result["shape_regex"] == "wider_than_tall"
    assert result["margins_regex"] == "smooth"
    assert result["confidence_overall_regex"] >= 0.7
    # evidence_short must be ≤ 140 chars
    ev = result["evidence_short_regex"]
    assert ev is None or len(ev) <= 140

def test_full_extraction_known_features_not_overwritten():
    """If composition is already known, regex should return None for it."""
    text = "Solid hypoechoic nodule."
    known = {"composition": "cystic"}  # already populated canonical
    result = X.extract_nodule_features(text, {}, known)
    assert result["composition_regex"] is None  # guarded

def test_full_extraction_empty_text_zero_confidence():
    result = X.extract_nodule_features("", {}, {})
    assert result["confidence_overall_regex"] == 0.0
    assert result["composition_regex"] is None

def test_full_extraction_ef_json_valid():
    text = "Punctate echogenic foci noted. Solid isoechoic nodule."
    result = X.extract_nodule_features(text, {}, {})
    ef = json.loads(result["echogenic_foci_regex_jsonarray"])
    assert "punctate_echogenic_foci" in ef

def test_full_extraction_halo_jsonb_valid_json():
    text = "Complete halo present. Solid nodule."
    result = X.extract_nodule_features(text, {}, {})
    h = json.loads(result["halo_jsonb_regex"])
    assert h["presence"] == "present"
    assert h["completeness"] == "complete"

def test_negation_no_microcalc_not_in_ef():
    text = "No microcalcifications. Solid hypoechoic nodule."
    result = X.extract_nodule_features(text, {}, {})
    ef_raw = result["echogenic_foci_regex_jsonarray"]
    ef = json.loads(ef_raw) if ef_raw else []
    assert "punctate_echogenic_foci" not in ef

def test_evidence_phi_strip():
    text = "Nodule seen on 01/15/2024. Hypoechoic solid lesion."
    result = X.extract_nodule_features(text, {}, {})
    ev = result["evidence_short_regex"]
    assert ev is None or "[date]" in ev or "2024" not in ev

def test_evidence_max_140():
    long_text = "Solid " + "hypoechoic " * 30 + "nodule."
    result = X.extract_nodule_features(long_text, {}, {})
    ev = result["evidence_short_regex"]
    assert ev is None or len(ev) <= 140
