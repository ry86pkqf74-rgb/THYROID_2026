"""Tests for notes_extraction regex extractors."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notes_extraction.extract_regex import (
    ComplicationExtractor,
    GeneticsExtractor,
    MedicationExtractor,
    ProblemListExtractor,
    ProcedureExtractor,
    StagingExtractor,
)

NOTE_ROW_ID = "abc123"
RESEARCH_ID = 42
NOTE_TYPE = "op_note"


class TestStagingExtractor:
    ext = StagingExtractor()

    def test_t_stage(self):
        matches = self.ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "Pathology shows pT1a")
        norms = [m.entity_value_norm for m in matches]
        assert "PT1A" in norms

    def test_n_stage(self):
        matches = self.ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "Final staging N1b")
        norms = [m.entity_value_norm for m in matches]
        assert "N1B" in norms

    def test_overall_stage(self):
        matches = self.ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "AJCC Stage II disease")
        norms = [m.entity_value_norm for m in matches]
        assert "Stage II" in norms

    def test_evidence_span_is_substring(self):
        text = "pT2 N0 M0"
        matches = self.ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        for m in matches:
            assert m.evidence_span in text
            assert text[m.evidence_start:m.evidence_end] == m.evidence_span


class TestGeneticsExtractor:
    ext = GeneticsExtractor()

    def test_braf_v600e(self):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE,
            "BRAF V600E mutation detected"
        )
        norms = [m.entity_value_norm for m in matches]
        assert "BRAF" in norms

    def test_ret_ptc(self):
        matches = self.ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "RET/PTC rearrangement")
        norms = [m.entity_value_norm for m in matches]
        assert "RET" in norms

    def test_negated_braf(self):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE,
            "No evidence of BRAF mutation"
        )
        braf = [m for m in matches if m.entity_value_norm == "BRAF"]
        assert len(braf) == 1
        assert braf[0].present_or_negated == "negated"

    def test_tert_promoter(self):
        matches = self.ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "TERT promoter mutation")
        norms = [m.entity_value_norm for m in matches]
        assert "TERT" in norms


class TestProcedureExtractor:
    ext = ProcedureExtractor()

    def test_total_thyroidectomy(self):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE,
            "Patient underwent total thyroidectomy"
        )
        norms = [m.entity_value_norm for m in matches]
        assert "total_thyroidectomy" in norms

    def test_central_neck_dissection(self):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE,
            "with central neck dissection"
        )
        norms = [m.entity_value_norm for m in matches]
        assert "central_neck_dissection" in norms

    def test_hemithyroidectomy(self):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE,
            "right thyroid lobectomy performed"
        )
        norms = [m.entity_value_norm for m in matches]
        assert "hemithyroidectomy" in norms

    def test_evidence_span_exact(self, sample_op_note):
        matches = self.ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_op_note)
        for m in matches:
            assert m.evidence_span in sample_op_note


class TestComplicationExtractor:
    ext = ComplicationExtractor()

    def test_hypocalcemia(self):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE,
            "Post-op hypocalcemia requiring calcium supplementation"
        )
        norms = [m.entity_value_norm for m in matches]
        assert "hypocalcemia" in norms

    def test_negation_detection(self, sample_negated_note):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_negated_note
        )
        for m in matches:
            assert m.present_or_negated == "negated", (
                f"{m.entity_value_norm} should be negated in: "
                f"'{sample_negated_note[max(0, m.evidence_start-20):m.evidence_end+20]}'"
            )

    def test_rln_injury(self):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE,
            "RLN injury noted on the left side"
        )
        norms = [m.entity_value_norm for m in matches]
        assert "rln_injury" in norms

    def test_chyle_leak(self):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE,
            "Developed chyle leak on POD 2"
        )
        norms = [m.entity_value_norm for m in matches]
        assert "chyle_leak" in norms


class TestMedicationExtractor:
    ext = MedicationExtractor()

    def test_levothyroxine_with_dose(self):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE,
            "levothyroxine 125 mcg daily"
        )
        assert len(matches) >= 1
        levo = [m for m in matches if "levothyroxine" in m.entity_value_norm]
        assert len(levo) >= 1

    def test_rai_mention(self):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE,
            "Received radioactive iodine 150 mCi"
        )
        norms = [m.entity_value_norm for m in matches]
        assert any("rai_dose" in n for n in norms)

    def test_calcium_supplement(self):
        matches = self.ext.extract(
            NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE,
            "taking calcium carbonate 500 mg three times daily"
        )
        norms = [m.entity_value_norm for m in matches]
        assert any("calcium_supplement" in n for n in norms)


class TestProblemListExtractor:
    ext = ProblemListExtractor()

    def test_multiple_problems(self, sample_hp_note):
        matches = self.ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_hp_note)
        norms = {m.entity_value_norm for m in matches}
        assert "hypertension" in norms
        assert "diabetes_type2" in norms
        assert "obesity" in norms
        assert "GERD" in norms

    def test_dedup_within_note(self):
        text = "HTN HTN hypertension"
        matches = self.ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        htn = [m for m in matches if m.entity_value_norm == "hypertension"]
        assert len(htn) == 1


class TestIntegrated:
    """Test that an op note produces entities from multiple extractors."""

    def test_op_note_multi_domain(self, sample_op_note):
        extractors = [
            StagingExtractor(),
            GeneticsExtractor(),
            ProcedureExtractor(),
            ComplicationExtractor(),
        ]
        all_matches = []
        for ext in extractors:
            all_matches.extend(
                ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_op_note)
            )

        domains = {m.entity_type for m in all_matches}
        assert "procedure" in domains
        assert "gene" in domains

        for m in all_matches:
            assert m.evidence_span in sample_op_note
            assert m.note_row_id == NOTE_ROW_ID
            assert m.research_id == RESEARCH_ID
