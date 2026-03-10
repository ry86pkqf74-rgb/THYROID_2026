"""Tests for ImagingNoduleExtractor."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notes_extraction.extract_imaging_v2 import ImagingNoduleExtractor

NOTE_ROW_ID = "test_001"
RESEARCH_ID = 1
NOTE_TYPE = "us_report"


@pytest.fixture
def ext() -> ImagingNoduleExtractor:
    return ImagingNoduleExtractor()


class TestNoduleSize:
    def test_three_axis_size(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        sizes = [m for m in matches if m.entity_type == "nodule_size"]
        assert len(sizes) >= 1
        assert any("2.3" in m.entity_value_norm for m in sizes)

    def test_single_axis_size(self, ext):
        text = "0.8 cm nodule in the left lobe"
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        sizes = [m for m in matches if m.entity_type == "nodule_size"]
        assert len(sizes) >= 1
        assert any("0.8" in m.entity_value_norm for m in sizes)

    def test_mm_converted_to_cm(self, ext):
        text = "15 x 10 x 8 mm thyroid nodule"
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        sizes = [m for m in matches if m.entity_type == "nodule_size"]
        assert len(sizes) >= 1
        assert "cm" in sizes[0].entity_value_norm


class TestComposition:
    def test_solid_detected(self, ext):
        text = "2.3 cm solid nodule in the right thyroid lobe."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        comp = [m for m in matches if m.entity_type == "composition"]
        assert any(m.entity_value_norm == "solid" for m in comp)

    def test_spongiform(self, ext):
        text = "spongiform nodule in the right lobe"
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        comp = [m for m in matches if m.entity_type == "composition"]
        assert any(m.entity_value_norm == "spongiform" for m in comp)


class TestEchogenicity:
    def test_hypoechoic(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        echo = [m for m in matches if m.entity_type == "echogenicity"]
        assert any(m.entity_value_norm == "hypoechoic" for m in echo)

    def test_isoechoic(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        echo = [m for m in matches if m.entity_type == "echogenicity"]
        assert any(m.entity_value_norm == "isoechoic" for m in echo)


class TestCalcifications:
    def test_microcalcifications(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        calc = [m for m in matches if m.entity_type == "calcifications"]
        assert any(m.entity_value_norm == "microcalcifications" for m in calc)

    def test_no_calcifications_negated(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        calc = [m for m in matches if m.entity_type == "calcifications"
                and m.present_or_negated == "negated"]
        assert len(calc) >= 1


class TestMargins:
    def test_irregular_margins(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        mg = [m for m in matches if m.entity_type == "nodule_margins"]
        assert any(m.entity_value_norm == "irregular" for m in mg)

    def test_well_defined_margins(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        mg = [m for m in matches if m.entity_type == "nodule_margins"
              and m.entity_value_norm == "well_defined"]
        assert len(mg) >= 1


class TestTIRADS:
    def test_tirads_score_5(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        tr = [m for m in matches if m.entity_type == "tirads_score"]
        assert any(m.entity_value_norm == "TR5" for m in tr)

    def test_tirads_score_2(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        tr = [m for m in matches if m.entity_type == "tirads_score"]
        assert any(m.entity_value_norm == "TR2" for m in tr)


class TestSuspiciousLymphNode:
    def test_no_suspicious_ln(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        ln = [m for m in matches if m.entity_type == "suspicious_lymph_node"]
        if ln:
            assert any(m.present_or_negated == "negated" for m in ln)

    def test_suspicious_ln_present(self, ext):
        text = "Suspicious cervical lymphadenopathy in level III"
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        ln = [m for m in matches if m.entity_type == "suspicious_lymph_node"]
        assert any(m.present_or_negated == "present" for m in ln)


class TestIntervalChange:
    def test_stable(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        ic = [m for m in matches if m.entity_type == "interval_change"]
        assert any(m.entity_value_norm == "stable" for m in ic)

    def test_interval_growth(self, ext):
        text = "Interval growth of the right lobe nodule"
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        ic = [m for m in matches if m.entity_type == "interval_change"]
        assert any(m.entity_value_norm == "increased" for m in ic)


class TestMultinodularGoiter:
    def test_mng(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        mng = [m for m in matches if m.entity_type == "multinodular_goiter"]
        assert len(mng) >= 1
        assert mng[0].entity_value_norm == "multinodular_goiter"


class TestLaterality:
    def test_right_lobe(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        lat = [m for m in matches if m.entity_type == "nodule_laterality"]
        assert any(m.entity_value_norm == "right_lobe" for m in lat)

    def test_left_lobe(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        lat = [m for m in matches if m.entity_type == "nodule_laterality"]
        assert any(m.entity_value_norm == "left_lobe" for m in lat)


class TestShape:
    def test_taller_than_wide(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        shape = [m for m in matches if m.entity_type == "nodule_shape"]
        assert any(m.entity_value_norm == "taller_than_wide" for m in shape)


class TestEntityMetadata:
    def test_fields_populated(self, ext, sample_us_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_us_report)
        assert len(matches) >= 1
        for m in matches:
            assert m.research_id == RESEARCH_ID
            assert m.note_row_id == NOTE_ROW_ID
            assert m.present_or_negated in ("present", "negated")

    def test_empty_text_returns_empty(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "") == []


class TestEmptyAndNullInput:
    def test_empty_string_returns_empty(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "") == []

    def test_none_returns_empty(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, None) == []

    def test_whitespace_only(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "   \n\t  ") == []
