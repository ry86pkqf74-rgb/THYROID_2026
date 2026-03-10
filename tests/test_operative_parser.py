"""Tests for OperativeDetailExtractor."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notes_extraction.extract_operative_v2 import OperativeDetailExtractor

NOTE_ROW_ID = "test_001"
RESEARCH_ID = 1
NOTE_TYPE = "op_note"


@pytest.fixture
def ext() -> OperativeDetailExtractor:
    return OperativeDetailExtractor()


class TestRLNFinding:
    def test_rln_identified_and_preserved(self, ext, sample_detailed_op_note):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_detailed_op_note)
        rln = [m for m in matches if m.entity_type == "rln_finding"]
        assert len(rln) >= 1
        norms = [m.entity_value_norm for m in rln]
        assert "rln_preserved" in norms

    def test_rln_injured(self, ext):
        text = "The recurrent laryngeal nerve was injured during dissection."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        rln = [m for m in matches if m.entity_type == "rln_finding"]
        assert any(m.entity_value_norm == "rln_injured" for m in rln)


class TestNerveMonitoring:
    def test_nim_used(self, ext, sample_detailed_op_note):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_detailed_op_note)
        nm = [m for m in matches if m.entity_type == "nerve_monitoring"]
        assert len(nm) >= 1

    def test_ionm_abbreviation(self, ext):
        text = "IONM was used throughout the procedure."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        nm = [m for m in matches if m.entity_type == "nerve_monitoring"]
        assert any(m.entity_value_norm == "ionm" for m in nm)


class TestParathyroidAutograft:
    def test_autotransplant(self, ext):
        text = ("Parathyroid gland was autotransplanted into the right "
                "sternocleidomastoid muscle.")
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        pt = [m for m in matches if m.entity_type == "parathyroid_autograft"]
        assert len(pt) >= 1


class TestParathyroidDevascularization:
    def test_devascularized(self, ext):
        text = "One parathyroid gland was devascularized during the dissection."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        devasc = [m for m in matches if m.entity_type == "parathyroid_management"
                  and m.entity_value_norm == "parathyroid_devascularized"]
        assert len(devasc) >= 1


class TestStrapMuscle:
    def test_strap_muscle_involved(self, ext, sample_detailed_op_note):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_detailed_op_note)
        strap = [m for m in matches if m.entity_type == "strap_muscle"
                 or (m.entity_type == "gross_invasion"
                     and "strap" in m.entity_value_raw.lower())]
        assert len(strap) >= 1


class TestTrachealInvasion:
    def test_no_tracheal_invasion(self, ext, sample_detailed_op_note):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_detailed_op_note)
        trach = [m for m in matches if m.entity_type == "tracheal_involvement"]
        if trach:
            assert any(m.present_or_negated == "negated" for m in trach)

    def test_tracheal_invasion_present(self, ext):
        text = "Trachea was invaded by tumor requiring tracheal shaving."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        trach = [m for m in matches if m.entity_type == "tracheal_involvement"]
        assert any(m.present_or_negated == "present" for m in trach)


class TestEBL:
    def test_ebl_extraction(self, ext, sample_detailed_op_note):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_detailed_op_note)
        ebl = [m for m in matches if m.entity_type == "ebl"]
        assert len(ebl) >= 1
        assert ebl[0].entity_value_norm == "50 mL"

    def test_ebl_different_value(self, ext):
        text = "Estimated blood loss was 200 mL."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        ebl = [m for m in matches if m.entity_type == "ebl"]
        assert len(ebl) >= 1
        assert ebl[0].entity_value_norm == "200 mL"


class TestDrainPlacement:
    def test_jp_drain(self, ext, sample_detailed_op_note):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_detailed_op_note)
        drains = [m for m in matches if m.entity_type == "drain_placement"]
        assert len(drains) >= 1

    def test_no_drain(self, ext):
        text = "No drain was placed."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        drains = [m for m in matches if m.entity_type == "drain_placement"]
        assert any(m.entity_value_norm == "no_drain" for m in drains)


class TestBerryLigament:
    def test_berry_ligament_dissected(self, ext, sample_detailed_op_note):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_detailed_op_note)
        berry = [m for m in matches if m.entity_type == "berry_ligament"]
        assert len(berry) >= 1
        assert any("berry_ligament" in m.entity_value_norm for m in berry)


class TestFrozenSection:
    def test_frozen_section_detected(self, ext):
        text = "Frozen section was performed and showed papillary carcinoma."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        fs = [m for m in matches if m.entity_type == "specimen_detail"
              and "frozen" in m.entity_value_norm]
        assert len(fs) >= 1

    def test_specimen_sent_from_fixture(self, ext, sample_detailed_op_note):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_detailed_op_note)
        spec = [m for m in matches if m.entity_type == "specimen_detail"]
        assert len(spec) >= 1


class TestEntityMetadata:
    def test_fields_populated(self, ext, sample_detailed_op_note):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_detailed_op_note)
        assert len(matches) >= 1
        for m in matches:
            assert m.research_id == RESEARCH_ID
            assert m.note_row_id == NOTE_ROW_ID
            assert m.present_or_negated in ("present", "negated")
            assert 0 < m.confidence <= 1.0


class TestEmptyAndNullInput:
    def test_empty_string_returns_empty(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "") == []

    def test_none_returns_empty(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, None) == []

    def test_whitespace_only(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "   \n\t  ") == []
