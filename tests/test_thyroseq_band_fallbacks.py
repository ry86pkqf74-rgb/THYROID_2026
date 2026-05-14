"""
Unit tests for ThyroSeq parser v4 band-fallback logic (mig_321 / EXT2-4).

Five test scenarios per the Phase 2 spec:
  i.   Clean LOW report (3% ROM) — band from reported text
  ii.  INTERMEDIATE report (50%) — third-category semantics preserved
  iii. HIGH report (95%) — positive
  iv.  no_detailed_block where numeric ROM% appears in a comment block
  v.   Fully unparseable report — band_source = manual_review

Tests also cover:
  - Fallback A threshold boundaries (0, 5, 30, 50, 75, 100)
  - overall_result_class_inferred derivation
  - Existing bands NOT overwritten when already present (reported_text wins)
  - band_source always populated by parse_block / parse_afirma routing
"""
import sys
from pathlib import Path

import pytest

# Allow running from repo root or tests/ directory
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "molecular_consolidation_20260421"))

from thyroseq_detailed_parser import (
    _apply_band_fallbacks,
    _numeric_rom_to_band,
    _result_class_from_band,
    _scan_full_text_for_band,
    extract_af_pct,
    normalize,
    parse,
    parse_afirma,
    parse_block,
)


# ---------------------------------------------------------------------------
# Unit tests for helper functions
# ---------------------------------------------------------------------------

class TestNumericRomToBand:
    def test_rom_zero(self):
        assert _numeric_rom_to_band(0.0) == "LOW"

    def test_rom_at_low_upper_boundary(self):
        assert _numeric_rom_to_band(5.0) == "LOW"

    def test_rom_just_above_low(self):
        assert _numeric_rom_to_band(5.1) == "INTERMEDIATE-LOW"

    def test_rom_at_intermediate_low_upper(self):
        assert _numeric_rom_to_band(30.0) == "INTERMEDIATE-LOW"

    def test_rom_just_above_intermediate_low(self):
        assert _numeric_rom_to_band(30.1) == "INTERMEDIATE"

    def test_rom_at_intermediate_upper(self):
        assert _numeric_rom_to_band(50.0) == "INTERMEDIATE"

    def test_rom_just_above_intermediate(self):
        assert _numeric_rom_to_band(50.1) == "INTERMEDIATE-HIGH"

    def test_rom_at_intermediate_high_upper(self):
        assert _numeric_rom_to_band(75.0) == "INTERMEDIATE-HIGH"

    def test_rom_just_above_intermediate_high(self):
        assert _numeric_rom_to_band(75.1) == "HIGH"

    def test_rom_100(self):
        assert _numeric_rom_to_band(100.0) == "HIGH"

    def test_rom_typical_low(self):
        assert _numeric_rom_to_band(3.0) == "LOW"

    def test_rom_typical_intermediate(self):
        assert _numeric_rom_to_band(45.0) == "INTERMEDIATE"

    def test_rom_typical_high(self):
        assert _numeric_rom_to_band(95.0) == "HIGH"


class TestResultClassFromBand:
    def test_low_is_negative(self):
        assert _result_class_from_band("LOW") == "negative"

    def test_intermediate_low_is_negative(self):
        assert _result_class_from_band("INTERMEDIATE-LOW") == "negative"

    def test_intermediate_is_intermediate(self):
        assert _result_class_from_band("INTERMEDIATE") == "intermediate"

    def test_intermediate_high_is_positive(self):
        assert _result_class_from_band("INTERMEDIATE-HIGH") == "positive"

    def test_high_is_positive(self):
        assert _result_class_from_band("HIGH") == "positive"

    def test_very_high_is_positive(self):
        assert _result_class_from_band("VERY-HIGH") == "positive"

    def test_case_insensitive(self):
        assert _result_class_from_band("low") == "negative"
        assert _result_class_from_band("intermediate") == "intermediate"


class TestScanFullTextForBand:
    def test_band_keyword_in_text(self):
        text = "RISK OF MALIGNANCY: LOW (3%)"
        band, rom, source = _scan_full_text_for_band(text)
        assert band == "LOW"
        assert source == "reported_text"

    def test_intermediate_high_keyword(self):
        text = "ROM: INTERMEDIATE HIGH (65%)"
        band, rom, source = _scan_full_text_for_band(text)
        assert band is not None
        assert "INTERMEDIATE" in band
        assert source == "reported_text"

    def test_numeric_rom_without_descriptor(self):
        text = "Risk of malignancy: 22% per published literature."
        band, rom, source = _scan_full_text_for_band(text)
        assert band == "INTERMEDIATE-LOW"
        assert rom == pytest.approx(22.0)
        assert source == "numeric_rom_inferred"

    def test_no_rom_text(self):
        text = "Specimen adequate. Gene expression profile detected."
        band, rom, source = _scan_full_text_for_band(text)
        assert band is None
        assert source == "manual_review"

    def test_empty_text(self):
        band, rom, source = _scan_full_text_for_band("")
        assert band is None
        assert source == "manual_review"


class TestApplyBandFallbacks:
    def test_already_has_descriptor_keeps_reported_text(self):
        out = {"rom_descriptor": "HIGH", "rom_percent_point": 90.0}
        _apply_band_fallbacks(out, "some text")
        assert out["band_source"] == "reported_text"
        assert out["rom_descriptor"] == "HIGH"

    def test_fallback_a_from_numeric_rom(self):
        out = {"rom_percent_point": 3.0}
        _apply_band_fallbacks(out, "")
        assert out["rom_descriptor"] == "LOW"
        assert out["band_source"] == "numeric_rom_inferred"
        assert out["overall_result_class_inferred"] == "negative"

    def test_fallback_a_intermediate(self):
        out = {"rom_percent_point": 45.0}
        _apply_band_fallbacks(out, "")
        assert out["rom_descriptor"] == "INTERMEDIATE"
        assert out["overall_result_class_inferred"] == "intermediate"

    def test_fallback_a_positive(self):
        out = {"rom_percent_point": 80.0}
        _apply_band_fallbacks(out, "")
        assert out["overall_result_class_inferred"] == "positive"

    def test_fallback_b_triggers_when_no_numeric(self):
        out = {}
        full_text = "Risk of malignancy: INTERMEDIATE-HIGH (68%)"
        _apply_band_fallbacks(out, full_text)
        assert out["band_source"] == "reported_text"
        assert "INTERMEDIATE" in out["rom_descriptor"]

    def test_manual_review_when_nothing_found(self):
        out = {}
        _apply_band_fallbacks(out, "Specimen: adequate. Gene mutations: not detected.")
        assert out["band_source"] == "manual_review"
        assert out.get("rom_descriptor") is None


# ---------------------------------------------------------------------------
# Integration tests against parse_block / parse()
# ---------------------------------------------------------------------------

_CLEAN_LOW_REPORT = """\
TEST RESULTS
ThyroSeq GC: NEGATIVE - LOW (3%)
INTERPRETATION:
Low risk of malignancy consistent with benign nodule.

DETAILED RESULTS
Specimen cellularity/adequacy for interpretation: Adequate
Gene mutations: Not detected
Gene fusions: Not detected
Copy number alterations: Not detected
Gene expression profile: Negative
Parathyroid: Not detected
Medullary/C-cells: Not detected
"""

_INTERMEDIATE_REPORT = """\
TEST RESULTS
ThyroSeq GC: INTERMEDIATE (50%)
INTERPRETATION:
Intermediate risk of malignancy.

DETAILED RESULTS
Specimen cellularity/adequacy for interpretation: Adequate
Gene mutations: Not detected
Gene fusions: Not detected
Copy number alterations: Not detected
Gene expression profile: Negative
Parathyroid: Not detected
Medullary/C-cells: Not detected
"""

_HIGH_REPORT = """\
TEST RESULTS
ThyroSeq GC: POSITIVE - HIGH (95%)
INTERPRETATION:
High risk of malignancy.

DETAILED RESULTS
Specimen cellularity/adequacy for interpretation: Adequate
Gene mutations: BRAF p.V600E positive (AF 48%)
Gene fusions: Not detected
Copy number alterations: Not detected
Gene expression profile: Positive
Parathyroid: Not detected
Medullary/C-cells: Not detected
"""

_NO_DETAILED_BLOCK_NUMERIC_COMMENT = """\
Note: Risk of malignancy approximately 22% based on current literature.
BRAF mutation: Not detected. Patient discussed in multidisciplinary conference.
"""

_FULLY_UNPARSEABLE = """\
Specimen cellularity: adequate for interpretation.
No detailed genetic findings available in this summary.
Patient follow-up recommended per clinical guidelines.
"""


class TestScenarioI_CleanLowReport:
    """Scenario i: Clean LOW report (3% ROM) — band from reported text."""
    def test_band_from_reported_text(self):
        result = parse(_CLEAN_LOW_REPORT, platform="ThyroSeq")
        assert result.get("rom_descriptor") == "LOW"
        assert result.get("band_source") == "reported_text"
        assert result.get("overall_result_class_inferred") == "negative"
        assert result.get("parse_status") in ("ok", "partial")


class TestScenarioII_IntermediateReport:
    """Scenario ii: INTERMEDIATE report (50%) — third-category, not pooled with positive."""
    def test_intermediate_is_third_category(self):
        result = parse(_INTERMEDIATE_REPORT, platform="ThyroSeq")
        assert result.get("rom_descriptor") == "INTERMEDIATE"
        # Must be 'intermediate', never 'positive'
        assert result.get("overall_result_class_inferred") == "intermediate"
        assert result.get("band_source") == "reported_text"


class TestScenarioIII_HighReport:
    """Scenario iii: HIGH report (95%) — positive."""
    def test_high_is_positive(self):
        result = parse(_HIGH_REPORT, platform="ThyroSeq")
        assert result.get("rom_descriptor") in ("HIGH", "INTERMEDIATE-HIGH")
        assert result.get("overall_result_class_inferred") == "positive"
        assert result.get("band_source") == "reported_text"
        # BRAF variant should be extracted
        variants = result.get("gene_mutations_variants") or []
        genes = [v.get("gene") for v in variants]
        assert "BRAF" in genes


class TestScenarioIV_NoDetailedBlockNumericComment:
    """Scenario iv: no DETAILED RESULTS block, numeric ROM% in comment block."""
    def test_fallback_b_numeric_in_comment(self):
        result = parse(_NO_DETAILED_BLOCK_NUMERIC_COMMENT, platform="ThyroSeq")
        assert result.get("parse_status") in ("no_detailed_block", "partial", "minimal")
        # Fallback B should find the numeric 22% near "Risk of malignancy"
        # → INTERMEDIATE-LOW band
        assert result.get("rom_descriptor") == "INTERMEDIATE-LOW"
        assert result.get("band_source") in ("numeric_rom_inferred", "reported_text")
        assert result.get("overall_result_class_inferred") == "negative"


class TestScenarioV_FullyUnparseable:
    """Scenario v: fully unparseable — band_source = manual_review, rom_descriptor stays None."""
    def test_manual_review_fallback(self):
        result = parse(_FULLY_UNPARSEABLE, platform="ThyroSeq")
        assert result.get("band_source") == "manual_review"
        assert result.get("rom_descriptor") is None


class TestBandSourceAlwaysSet:
    """band_source must be populated on every ThyroSeq parse result."""
    def test_band_source_on_all_scenarios(self):
        reports = [
            _CLEAN_LOW_REPORT,
            _INTERMEDIATE_REPORT,
            _HIGH_REPORT,
            _NO_DETAILED_BLOCK_NUMERIC_COMMENT,
            _FULLY_UNPARSEABLE,
        ]
        for text in reports:
            result = parse(text, platform="ThyroSeq")
            assert "band_source" in result, f"band_source missing for: {text[:50]!r}"
            assert result["band_source"] in (
                "reported_text", "numeric_rom_inferred", "manual_review"
            ), f"unexpected band_source: {result['band_source']!r}"


class TestAfirmaUnaffected:
    """Afirma parsing should not gain band_source (ROM bands are ThyroSeq-specific)."""
    _AFIRMA_TEXT = (
        "Afirma BRAF: Negative\nAfirma MTC Result: Negative\nParathyroid: Negative"
    )

    def test_afirma_no_band_source(self):
        result = parse(self._AFIRMA_TEXT, platform="Afirma")
        # Afirma path does not call _apply_band_fallbacks; band_source should be absent
        assert result.get("parser") == "afirma"
        assert "band_source" not in result


class TestExtractAfPct:
    def test_labeled_af(self):
        assert extract_af_pct("Allele frequency: 23.6%") == 24

    def test_after_cdna(self):
        assert extract_af_pct("c.1799T>A 11%") == 11

    def test_vaf_suffix(self):
        assert extract_af_pct("45% VAF extra") == 45


class TestOcrNormalizeMutationsHeader:
    def test_inutations_fixed(self):
        n = normalize("Gene Inutations\nNegative\n")
        assert "Gene mutations" in n


class TestThyroSeqDetailedAfExtraction:
    def test_braf_percent_after_hgvs(self):
        txt = """DETAILED RESULTS
Gene mutations
BRAF p.V600E c.1799T>A 11%
Gene fusions
Negative
Copy number alterations
Negative
Gene expression profile
Negative
"""
        r = parse_block(txt)
        by_gene = {v["gene"]: v.get("af_pct") for v in r.get("gene_mutations_variants", [])}
        assert by_gene.get("BRAF") == 11


class TestAfirmaFusionFallback:
    def test_ret_ptc_fusion_not_detected_without_colon_field(self):
        t = (
            "Afirma BRAF: Positive\n"
            "Afirma MTC Result: Negative\n"
            "RET/PTC fusion not detected in this specimen.\n"
        )
        r = parse_afirma(t)
        assert r.get("gene_fusions_status") == "Negative"


class TestAllNegativeFallbackThyroSeq:
    """Issue 2 — header/body lines without ':' before Negative."""

    def test_multiline_negative_sections(self):
        txt = """DETAILED RESULTS
Gene mutations
Negative
Gene fusions
Negative
Copy number alterations
Negative
Gene expression profile
Negative
"""
        r = parse_block(txt)
        assert r["gene_mutations_status"] == "Negative"
        assert r["gene_fusions_status"] == "Negative"
        assert r["cna_status"] == "Negative"
        assert r["gep_status"] == "Negative"
