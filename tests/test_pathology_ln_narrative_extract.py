"""Tests for deterministic pathology lymph-node narrative parsing."""

from utils.pathology_ln_narrative_extract import extract_pathology_ln_from_text


def test_classic_two_of_six():
    text = "2 of 6 lymph nodes positive with metastatic carcinoma."
    r = extract_pathology_ln_from_text(text)
    assert r.ln_positive_path == 2
    assert r.ln_examined_path == 6
    assert r.ln_parse_status == "parsed_pair"


def test_metastatic_in_x_of_y():
    text = "Metastatic carcinoma identified in 1 of 8 lymph nodes."
    r = extract_pathology_ln_from_text(text)
    assert r.ln_positive_path == 1
    assert r.ln_examined_path == 8


def test_slash_form():
    text = "Final: 0/12 lymph nodes involved by invasive carcinoma."
    r = extract_pathology_ln_from_text(text)
    assert r.ln_positive_path == 0
    assert r.ln_examined_path == 12


def test_no_nodal_metastasis():
    text = "No lymph node metastasis identified in the central neck specimen."
    r = extract_pathology_ln_from_text(text)
    assert r.ln_positive_path == 0
    assert r.ln_parse_status == "explicit_no_nodal_disease"


def test_zero_of_y_negative():
    text = "0 out of 4 lymph nodes negative for malignancy."
    r = extract_pathology_ln_from_text(text)
    assert r.ln_positive_path == 0
    assert r.ln_examined_path == 4


def test_no_nodes_identified():
    text = "No lymph nodes submitted in the specimen container."
    r = extract_pathology_ln_from_text(text)
    assert r.ln_no_nodes_flag is True
    assert r.ln_parse_status == "no_nodes_submitted"
