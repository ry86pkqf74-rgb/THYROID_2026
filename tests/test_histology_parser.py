"""Tests for HistologyDetailExtractor."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notes_extraction.extract_histology_v2 import HistologyDetailExtractor

NOTE_ROW_ID = "test_001"
RESEARCH_ID = 1
NOTE_TYPE = "path_report"


@pytest.fixture
def ext() -> HistologyDetailExtractor:
    return HistologyDetailExtractor()


class TestCapsularInvasion:
    def test_capsular_invasion_present(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        cap = [m for m in matches if m.entity_type == "capsular_invasion"]
        assert len(cap) >= 1
        assert any(m.entity_value_norm == "present" for m in cap)

    def test_no_capsular_invasion(self, ext):
        text = "No capsular invasion identified."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        cap = [m for m in matches if m.entity_type == "capsular_invasion"]
        assert any(m.entity_value_norm == "absent" for m in cap)


class TestPerineuralInvasion:
    def test_perineural_invasion_negated(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        pni = [m for m in matches if m.entity_type == "perineural_invasion"]
        assert len(pni) >= 1
        assert pni[0].present_or_negated == "negated"


class TestExtranodalExtension:
    def test_extranodal_extension_present(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        ene = [m for m in matches if m.entity_type == "extranodal_extension"]
        assert len(ene) >= 1
        assert any(m.present_or_negated == "present" for m in ene)


class TestVascularInvasion:
    def test_extensive_vascular_invasion(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        vi = [m for m in matches if m.entity_type == "vascular_invasion_detail"]
        assert len(vi) >= 1
        assert any(m.entity_value_norm == "extensive" for m in vi)

    def test_focal_vascular_invasion(self, ext):
        text = "Focal vascular invasion identified."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        vi = [m for m in matches if m.entity_type == "vascular_invasion_detail"]
        assert any(m.entity_value_norm == "focal" for m in vi)


class TestLymphaticInvasion:
    def test_lymphatic_invasion(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        li = [m for m in matches if m.entity_type == "lymphatic_invasion_detail"]
        assert len(li) >= 1
        assert li[0].present_or_negated == "present"


class TestMarginStatus:
    def test_margins_negative(self, ext):
        text = "Margin is negative. All margins are clear."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        mg = [m for m in matches if m.entity_type == "margin_status"]
        assert len(mg) >= 1
        assert any(m.entity_value_norm == "negative" for m in mg)

    def test_margins_positive(self, ext):
        text = "Margin status: positive for carcinoma."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        mg = [m for m in matches if m.entity_type == "margin_status"]
        assert any(m.entity_value_norm == "positive" for m in mg)


class TestNIFTP:
    def test_niftp_mention(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        niftp = [m for m in matches if m.entity_type == "niftp"]
        assert len(niftp) >= 1
        assert niftp[0].entity_value_norm == "NIFTP"


class TestPDTCFeatures:
    def test_pdtc_component(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        pdtc = [m for m in matches if m.entity_type == "pdtc_features"]
        assert len(pdtc) >= 1
        assert pdtc[0].entity_value_norm == "PDTC"


class TestAggressiveFeatures:
    def test_tall_cell_variant(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        agg = [m for m in matches if m.entity_type == "aggressive_features"]
        assert len(agg) >= 1
        assert any("tall" in m.entity_value_norm for m in agg)

    def test_hobnail_variant(self, ext):
        text = "Papillary thyroid carcinoma with hobnail variant features."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        agg = [m for m in matches if m.entity_type == "aggressive_features"]
        assert any("hobnail" in m.entity_value_norm for m in agg)


class TestMultifocality:
    def test_multifocal(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        mf = [m for m in matches if m.entity_type == "multifocality"]
        assert len(mf) >= 1
        assert mf[0].entity_value_norm == "multifocal"


class TestTumorCount:
    def test_three_foci(self, ext):
        text = "3 separate tumors of papillary carcinoma identified."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        tc = [m for m in matches if m.entity_type == "tumor_count"]
        assert len(tc) >= 1
        assert tc[0].entity_value_norm == "3"


class TestLymphNodeCounts:
    def test_four_of_twelve(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        ln = [m for m in matches if m.entity_type == "lymph_node_count"]
        assert len(ln) >= 1
        assert ln[0].entity_value_norm == "4/12"

    def test_different_counts(self, ext):
        text = "2 of 6 lymph nodes positive with metastatic carcinoma."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        ln = [m for m in matches if m.entity_type == "lymph_node_count"]
        assert len(ln) >= 1
        assert ln[0].entity_value_norm == "2/6"


class TestETEDetail:
    def test_minimal_ete(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        ete = [m for m in matches if m.entity_type == "extrathyroidal_extension_detail"]
        assert len(ete) >= 1
        assert any("minimal" in m.entity_value_norm for m in ete)

    def test_gross_ete(self, ext):
        text = "Gross extrathyroidal extension into strap muscles."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        ete = [m for m in matches if m.entity_type == "extrathyroidal_extension_detail"]
        assert any("gross" in m.entity_value_norm for m in ete)


class TestConsultDiagnosis:
    def test_consult_diagnosis_detected(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
        consult = [m for m in matches if m.entity_type == "consult_diagnosis"]
        assert len(consult) >= 1

    def test_consult_in_conflicting_path(self, ext, sample_conflicting_path):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_conflicting_path)
        consult = [m for m in matches if m.entity_type == "consult_diagnosis"]
        assert len(consult) >= 1


class TestEntityMetadata:
    def test_fields_populated(self, ext, sample_path_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_path_report)
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
