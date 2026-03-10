"""Tests for RAIDetailExtractor."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notes_extraction.extract_rai_v2 import RAIDetailExtractor

NOTE_ROW_ID = "test_001"
RESEARCH_ID = 1
NOTE_TYPE = "nuclear_medicine"


@pytest.fixture
def ext() -> RAIDetailExtractor:
    return RAIDetailExtractor()


class TestDoseExtraction:
    def test_dose_150_mci(self, ext, sample_rai_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_rai_report)
        doses = [m for m in matches if m.entity_type == "rai_dose"]
        assert len(doses) >= 1
        assert any("150" in m.entity_value_norm for m in doses)

    def test_dose_gbq_conversion(self, ext):
        text = "Radioactive iodine 5.55 GBq administered for remnant ablation."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        doses = [m for m in matches if m.entity_type == "rai_dose"]
        assert len(doses) >= 1
        assert "mCi" in doses[0].entity_value_norm


class TestTreatmentIntent:
    def test_remnant_ablation(self, ext, sample_rai_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_rai_report)
        intent = [m for m in matches if m.entity_type == "rai_intent"]
        assert len(intent) >= 1
        assert any(m.entity_value_norm == "remnant_ablation" for m in intent)


class TestScanDetection:
    def test_pre_treatment_scan(self, ext, sample_rai_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_rai_report)
        pre = [m for m in matches if m.entity_type == "rai_pre_scan"]
        assert len(pre) >= 1
        assert pre[0].entity_value_norm == "pre_treatment_scan"

    def test_post_therapy_scan(self, ext, sample_rai_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_rai_report)
        post = [m for m in matches if m.entity_type == "rai_post_scan"]
        assert len(post) >= 1
        assert post[0].entity_value_norm == "post_treatment_scan"


class TestIodineAvidity:
    def test_avid_uptake(self, ext, sample_rai_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_rai_report)
        avid = [m for m in matches if m.entity_type == "rai_avidity"]
        assert len(avid) >= 1
        assert any(m.entity_value_norm == "avid" for m in avid)

    def test_non_avid(self, ext):
        text = ("Nuclear Medicine Report: Post-thyroidectomy I-131 therapy. "
                "Non-iodine-avid thyroid bed on post-treatment scan.")
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        non_avid = [m for m in matches if m.entity_type == "rai_avidity"
                    and m.entity_value_norm == "non_avid"]
        assert len(non_avid) >= 1


class TestLabValues:
    def test_stimulated_tg(self, ext, sample_rai_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_rai_report)
        tg = [m for m in matches if m.entity_type == "rai_stimulated_tg"]
        assert len(tg) >= 1
        assert any("2.5" in m.entity_value_norm for m in tg)

    def test_stimulated_tsh(self, ext, sample_rai_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_rai_report)
        tsh = [m for m in matches if m.entity_type == "rai_stimulated_tsh"]
        assert len(tsh) >= 1
        assert any("45" in m.entity_value_norm for m in tsh)


class TestUptakePercentage:
    def test_uptake_pct(self, ext):
        text = ("Nuclear Medicine Report: Post-thyroidectomy I-131 therapy. "
                "24-hour uptake of 3.2% in the thyroid bed.")
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        uptake = [m for m in matches if m.entity_type == "rai_uptake_pct"]
        assert len(uptake) >= 1
        assert any("3.2" in m.entity_value_norm for m in uptake)

    def test_uptake_over_100_ignored(self, ext):
        text = ("Nuclear Medicine Report: Post-thyroidectomy I-131 therapy. "
                "Uptake 150% is erroneous.")
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        uptake = [m for m in matches if m.entity_type == "rai_uptake_pct"]
        assert len(uptake) == 0


class TestNegatedRAI:
    def test_no_rai_given(self, ext):
        text = "No radioactive iodine given. RAI is not indicated for this patient."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        completion = [m for m in matches if m.entity_type == "rai_completion"]
        if completion:
            assert any(m.entity_value_norm == "not_applicable" for m in completion)

    def test_rai_declined(self, ext):
        text = "Patient declined RAI therapy after discussion of risks and benefits."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        completion = [m for m in matches if m.entity_type == "rai_completion"]
        assert any(m.entity_value_norm == "declined" for m in completion)


class TestEntityMetadata:
    def test_fields_populated(self, ext, sample_rai_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_rai_report)
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
