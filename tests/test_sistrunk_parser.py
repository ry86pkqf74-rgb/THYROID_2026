"""Unit tests for Sistrunk operative-note keyword parser (THY-4)."""

from __future__ import annotations

from pipelines.extraction.sistrunk_parser import (
    pick_best_per_patient,
    parse_sistrunk_in_note,
)


def test_phrase_sistrunk_procedure() -> None:
    text = (
        "Procedure: Sistrunk procedure for thyroglossal duct cyst. "
        "Specimen taken to pathology."
    )
    r = parse_sistrunk_in_note(text, research_id="1")
    assert r is not None
    assert r.sistrunk_match_kind == "regex"
    assert r.rule_id == "phrase_sistrunk_procedure"
    assert "Sistrunk" in r.sistrunk_text_evidence


def test_fuzzy_typo_prefers_regex_when_both_exist() -> None:
    """Regex tier must win before fuzzy even if typo appears earlier (should not occur)."""
    text = "sustrunk ignored; later we did a Sistrunk procedure for TGDC."
    r = parse_sistrunk_in_note(text, research_id="2")
    assert r is not None
    assert r.sistrunk_match_kind == "regex"


def test_fuzzy_only_when_no_regex() -> None:
    text = "The patient underwent a sustrunk resection yesterday."
    r = parse_sistrunk_in_note(text, research_id="3")
    assert r is not None
    assert r.sistrunk_match_kind == "fuzzy"


def test_hyoid_phrase() -> None:
    text = "Thyroglossal duct cyst excision with hyoid resection was completed."
    r = parse_sistrunk_in_note(text, research_id="4")
    assert r is not None
    assert r.rule_id == "phrase_tgdc_excision_hyoid"


def test_pick_best_prefers_regex() -> None:
    a = parse_sistrunk_in_note(
        "Sistrunk procedure done.", research_id="5", note_row_id="n1"
    )
    b = parse_sistrunk_in_note("sustrunk", research_id="5", note_row_id="n2")
    assert a and b
    m = pick_best_per_patient([a, b])
    assert m["5"].rule_id == "phrase_sistrunk_procedure"


def test_no_match() -> None:
    assert parse_sistrunk_in_note("", research_id="6") is None
    assert parse_sistrunk_in_note("Routine thyroidectomy only.", research_id="7") is None
