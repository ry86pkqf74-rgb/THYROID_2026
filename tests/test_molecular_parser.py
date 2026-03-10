"""Tests for MolecularDetailExtractor."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notes_extraction.extract_molecular_v2 import MolecularDetailExtractor

NOTE_ROW_ID = "test_001"
RESEARCH_ID = 1
NOTE_TYPE = "molecular_report"


@pytest.fixture
def ext() -> MolecularDetailExtractor:
    return MolecularDetailExtractor()


class TestMutationDetection:
    def test_braf_v600e_detected(self, ext, sample_molecular_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_molecular_report)
        braf = [m for m in matches if m.entity_type == "result_classification"
                and m.entity_value_norm == "positive"]
        assert len(braf) >= 1

    def test_tert_promoter_detected(self, ext, sample_molecular_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_molecular_report)
        positive = [m for m in matches if m.entity_type == "result_classification"
                    and m.entity_value_norm == "positive"
                    and "TERT" in sample_molecular_report[
                        max(0, m.evidence_start - 30):m.evidence_end]]
        assert len(positive) >= 1

    def test_eif1ax_negated(self, ext, sample_molecular_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_molecular_report)
        eif = [m for m in matches if m.entity_type == "mutation_eif1ax"]
        assert len(eif) >= 1
        assert eif[0].present_or_negated == "negated"

    def test_tp53_detected(self, ext, sample_molecular_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_molecular_report)
        tp53 = [m for m in matches if m.entity_type == "mutation_tp53"]
        assert len(tp53) >= 1
        assert tp53[0].entity_value_norm == "TP53"

    def test_tp53_negated_with_preceding_cue(self, ext):
        text = "Not detected: TP53 mutation."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        tp53 = [m for m in matches if m.entity_type == "mutation_tp53"]
        assert len(tp53) >= 1
        assert tp53[0].present_or_negated == "negated"


class TestPlatformExtraction:
    def test_thyroseq_v3(self, ext, sample_molecular_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_molecular_report)
        plat = [m for m in matches if m.entity_type == "molecular_platform"]
        assert len(plat) >= 1
        assert plat[0].entity_value_norm == "thyroseq_v3"

    def test_afirma_gsc(self, ext):
        text = "Afirma GSC result is suspicious"
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        plat = [m for m in matches if m.entity_type == "molecular_platform"]
        assert any(m.entity_value_norm == "afirma_gsc" for m in plat)


class TestCopyNumberAlteration:
    def test_copy_number_negated(self, ext, sample_molecular_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_molecular_report)
        cna = [m for m in matches if m.entity_type == "copy_number_alteration"]
        if cna:
            assert cna[0].present_or_negated == "negated"

    def test_copy_number_amplification(self, ext):
        text = "copy number amplification in BRAF locus"
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        cna = [m for m in matches if m.entity_type == "copy_number_alteration"]
        assert any(m.entity_value_norm == "amplification" for m in cna)


class TestGeneFusion:
    def test_ret_ptc_fusion(self, ext, sample_molecular_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_molecular_report)
        fusions = [m for m in matches if m.entity_type == "gene_fusion"]
        assert len(fusions) >= 1
        norms = [m.entity_value_norm for m in fusions]
        assert any("RET" in n for n in norms)

    def test_pax8_pparg_fusion(self, ext, sample_molecular_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_molecular_report)
        pax = [m for m in matches if m.entity_type == "mutation_pax8_pparg"
               or (m.entity_type == "gene_fusion" and "PAX8" in m.entity_value_norm)]
        assert len(pax) >= 1


class TestLOH:
    def test_loh_detected(self, ext, sample_molecular_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_molecular_report)
        loh = [m for m in matches if m.entity_type == "loh"]
        assert len(loh) >= 1
        assert loh[0].entity_value_norm == "loh"

    def test_loh_negated_with_preceding_cue(self, ext):
        text = "No evidence of loss of heterozygosity."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        loh = [m for m in matches if m.entity_type == "loh"]
        assert len(loh) >= 1
        assert loh[0].present_or_negated == "negated"


class TestClassifierResult:
    def test_suspicious_classifier(self, ext):
        text = "GEC result is suspicious"
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        clf = [m for m in matches if m.entity_type == "classifier_result"]
        assert any(m.entity_value_norm == "gec_suspicious" for m in clf)


class TestRiskProbability:
    def test_high_risk_malignancy(self, ext):
        text = "High probability of malignancy based on molecular profile."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        risk = [m for m in matches if m.entity_type == "risk_probability"]
        assert len(risk) >= 1
        norms = [m.entity_value_norm for m in risk]
        assert any("high" in n for n in norms)

    def test_percentage_risk(self, ext):
        text = "95% probability of malignancy."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        risk = [m for m in matches if m.entity_type == "risk_probability"]
        assert len(risk) >= 1
        assert any("95%" in m.entity_value_norm for m in risk)


class TestBethesda:
    def test_bethesda_iv(self, ext, sample_molecular_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_molecular_report)
        beth = [m for m in matches if m.entity_type == "bethesda_mention"]
        assert len(beth) >= 1
        assert beth[0].entity_value_norm == "bethesda_4"

    def test_bethesda_vi_roman(self, ext):
        text = "Bethesda category VI"
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        beth = [m for m in matches if m.entity_type == "bethesda_mention"]
        assert len(beth) >= 1
        assert beth[0].entity_value_norm == "bethesda_6"


class TestSpecimenAdequacy:
    def test_insufficient_material(self, ext):
        text = "insufficient material for analysis"
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        spec = [m for m in matches if m.entity_type == "specimen_adequacy"]
        assert len(spec) >= 1
        assert spec[0].entity_value_norm == "insufficient"

    def test_qns(self, ext):
        text = "Specimen QNS, unable to perform molecular testing"
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, text)
        spec = [m for m in matches if m.entity_type == "specimen_adequacy"]
        assert any(m.entity_value_norm == "quantity_not_sufficient" for m in spec)


class TestEntityMetadata:
    def test_fields_populated(self, ext, sample_molecular_report):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, sample_molecular_report)
        assert len(matches) >= 1
        for m in matches:
            assert m.research_id == RESEARCH_ID
            assert m.note_row_id == NOTE_ROW_ID
            assert m.note_type == NOTE_TYPE
            assert m.present_or_negated in ("present", "negated")
            assert 0 < m.confidence <= 1.0


class TestEmptyAndNullInput:
    def test_empty_string_returns_empty(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "") == []

    def test_none_returns_empty(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, None) == []

    def test_whitespace_only(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, NOTE_TYPE, "   \n\t  ") == []
