"""
Unit tests for scripts/_lab_value_normalizer.py — covers every required
case from the Script 347 spec plus additional regression cases.

Run:
    pytest tests/test_lab_value_normalizer.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

import pytest  # noqa: E402

from _lab_value_normalizer import (  # noqa: E402
    convert_to_canonical_unit,
    normalize_lab_value,
)


def _has_note(note: str | None, fragment: str) -> bool:
    return note is not None and fragment in note


# ---------------------------------------------------------------------------
# Required cases from the prompt
# ---------------------------------------------------------------------------

class TestSpecRequiredCases:
    def test_tsh_clean_inrange_no_correction(self):
        v, c, n = normalize_lab_value("0.3", "tsh")
        assert v == pytest.approx(0.3)
        assert c is False
        assert n is None

    def test_thyroglobulin_clean_inrange_no_correction(self):
        v, c, n = normalize_lab_value("4.5", "thyroglobulin")
        assert v == pytest.approx(4.5)
        assert c is False
        assert n is None

    def test_anti_thyroglobulin_clean_inrange_no_correction(self):
        v, c, n = normalize_lab_value("12", "anti_thyroglobulin")
        assert v == pytest.approx(12.0)
        assert c is False
        assert n is None

    def test_pth_clean_inrange_no_correction(self):
        v, c, n = normalize_lab_value("45", "pth")
        assert v == pytest.approx(45.0)
        assert c is False
        assert n is None

    def test_calcium_clean_inrange_no_correction(self):
        v, c, n = normalize_lab_value("9.4", "calcium")
        assert v == pytest.approx(9.4)
        assert c is False
        assert n is None

    def test_vitamin_d_clean_inrange_no_correction(self):
        v, c, n = normalize_lab_value("32", "vitamin_d")
        assert v == pytest.approx(32.0)
        assert c is False
        assert n is None

    def test_tsh_with_unit_suffix(self):
        v, c, n = normalize_lab_value("0.3 mIU/L", "tsh")
        assert v == pytest.approx(0.3)
        assert c is False
        assert _has_note(n, "unit_suffix_stripped")

    def test_tsh_censored_lt(self):
        v, c, n = normalize_lab_value("<0.01", "tsh")
        assert v == pytest.approx(0.01)
        assert c is True
        # No plausibility correction on censored values.

    def test_tsh_plausibility_div10(self):
        v, c, n = normalize_lab_value("151", "tsh")
        assert v == pytest.approx(15.1)
        assert c is False
        assert _has_note(n, "divided_by_10")

    def test_calcium_plausibility_div100(self):
        v, c, n = normalize_lab_value("950", "calcium")
        assert v == pytest.approx(9.5)
        assert c is False
        assert _has_note(n, "divided_by_100")

    def test_tgab_titer_extracted(self):
        v, c, n = normalize_lab_value("1:25600", "anti_thyroglobulin")
        assert v == pytest.approx(25600.0)
        assert c is False
        assert _has_note(n, "titer_denominator_extracted")

    def test_tg_censored_gt_no_plausibility(self):
        # >9000 is at the plausible boundary; censored bypasses correction.
        v, c, n = normalize_lab_value(">9000.0", "thyroglobulin")
        assert v == pytest.approx(9000.0)
        assert c is True

    def test_tg_less_than_phrase(self):
        v, c, n = normalize_lab_value("less than 0.1", "thyroglobulin")
        assert v == pytest.approx(0.1)
        assert c is True

    def test_pth_zero_implausible(self):
        v, c, n = normalize_lab_value("0", "pth")
        assert v is None
        assert c is False
        assert _has_note(n, "nulled_zero_implausible")

    def test_tsh_zero_permitted(self):
        v, c, n = normalize_lab_value("0", "tsh")
        assert v == 0.0
        assert c is False
        # No correction note for clean zero.
        assert n is None

    def test_vitamin_d_negative(self):
        v, c, n = normalize_lab_value("-5", "vitamin_d")
        assert v is None
        assert c is False
        assert _has_note(n, "nulled_negative")

    def test_calcium_unrecoverable(self):
        v, c, n = normalize_lab_value("5e8", "calcium")
        assert v is None
        assert c is False
        assert _has_note(n, "nulled_unrecoverable_implausible")

    def test_garbage_unparseable(self):
        v, c, n = normalize_lab_value("garbage", "tsh")
        assert v is None
        assert c is False
        assert _has_note(n, "unparseable_string")

    def test_pth_with_h_flag(self):
        v, c, n = normalize_lab_value("45 (H)", "pth")
        assert v == pytest.approx(45.0)
        assert c is False
        assert _has_note(n, "unit_suffix_stripped")


# ---------------------------------------------------------------------------
# Additional regression cases (real values from MotherDuck preflight)
# ---------------------------------------------------------------------------

class TestRealWorldRegressions:
    def test_tg_censored_lt_with_decimal(self):
        v, c, n = normalize_lab_value("<0.1", "thyroglobulin")
        assert v == pytest.approx(0.1)
        assert c is True

    def test_tsh_censored_with_unit_then_strip(self):
        v, c, n = normalize_lab_value("<0.01 mIU/L", "tsh")
        assert v == pytest.approx(0.01)
        assert c is True
        assert _has_note(n, "unit_suffix_stripped")

    def test_tsh_censored_with_l_flag(self):
        v, c, n = normalize_lab_value("<0.01 (L)", "tsh")
        assert v == pytest.approx(0.01)
        assert c is True
        assert _has_note(n, "unit_suffix_stripped")

    def test_tsh_space_after_lt(self):
        v, c, n = normalize_lab_value("< 0.01", "tsh")
        assert v == pytest.approx(0.01)
        assert c is True

    def test_calcium_with_unit_suffix(self):
        v, c, n = normalize_lab_value("9.4 mg/dL", "calcium")
        assert v == pytest.approx(9.4)
        assert c is False
        assert _has_note(n, "unit_suffix_stripped")

    def test_calcium_units_correction_for_500(self):
        # 500 is OOR (4..20 plausible, 20..2000 OOR); /100 = 5 -> in plausible.
        v, c, n = normalize_lab_value("500.0", "calcium")
        assert v == pytest.approx(5.0)
        assert _has_note(n, "divided_by_100")

    def test_pth_with_unit_suffix(self):
        v, c, n = normalize_lab_value("31 pg/mL", "pth")
        assert v == pytest.approx(31.0)
        assert _has_note(n, "unit_suffix_stripped")

    def test_vitamin_d_with_unit_suffix(self):
        v, c, n = normalize_lab_value("32 ng/mL", "vitamin_d")
        assert v == pytest.approx(32.0)
        assert _has_note(n, "unit_suffix_stripped")

    def test_tg_censored_gt2500(self):
        v, c, n = normalize_lab_value(">2500", "thyroglobulin")
        assert v == pytest.approx(2500.0)
        assert c is True

    def test_tg_value_with_trailing_decimal_zero(self):
        v, c, n = normalize_lab_value("450.0", "thyroglobulin")
        assert v == pytest.approx(450.0)
        assert c is False
        assert n is None

    def test_tsh_high_value_div10(self):
        v, c, n = normalize_lab_value("151.36", "tsh")
        assert v == pytest.approx(15.136)
        assert _has_note(n, "divided_by_10")

    def test_calcium_low_below_plausible_min_unrecoverable(self):
        # 1.0 mg/dL is below plausible MIN (4). Not negative, not zero.
        # Plausibility correction returns None with unrecoverable note.
        v, c, n = normalize_lab_value("1.0", "calcium")
        assert v is None
        assert _has_note(n, "nulled_unrecoverable_implausible")

    def test_unknown_analyte_no_plausibility(self):
        v, c, n = normalize_lab_value("12345", "some_unknown_lab")
        assert v == pytest.approx(12345.0)
        assert c is False

    def test_none_input(self):
        v, c, n = normalize_lab_value(None, "tsh")
        assert v is None
        assert c is False
        assert _has_note(n, "unparseable_string")

    def test_empty_string(self):
        v, c, n = normalize_lab_value("", "tsh")
        assert v is None
        assert _has_note(n, "unparseable_string")

    def test_nan_input(self):
        v, c, n = normalize_lab_value(float("nan"), "tsh")
        assert v is None
        assert _has_note(n, "unparseable_string")

    def test_tg_div10_correction(self):
        # 12000 ng/mL Tg is OOR (plausible <= 10000); /10 = 1200 is in range.
        v, c, n = normalize_lab_value("12000", "thyroglobulin")
        assert v == pytest.approx(1200.0)
        assert _has_note(n, "divided_by_10")

    def test_tg_exact_at_plausible_max_keep(self):
        v, c, n = normalize_lab_value("10000", "thyroglobulin")
        assert v == pytest.approx(10000.0)
        assert c is False
        assert n is None

    def test_tgab_clean_value(self):
        v, c, n = normalize_lab_value("12", "anti_thyroglobulin")
        assert v == pytest.approx(12.0)

    def test_titer_only_for_tgab(self):
        # If a titer-format string appears for Tg by accident, treat it as
        # a regular numeric parse (1 is the first number).
        v, c, n = normalize_lab_value("1:25600", "thyroglobulin")
        # parsed = 1.0; in plausible; no titer note.
        assert v == pytest.approx(1.0)
        assert not _has_note(n, "titer_denominator_extracted")


# ---------------------------------------------------------------------------
# Unit conversion helper
# ---------------------------------------------------------------------------

class TestUnitConversion:
    def test_identity_no_note(self):
        v, u, note = convert_to_canonical_unit(0.3, "mIU/L", "tsh")
        assert v == 0.3
        assert u == "mIU/L"
        assert note is None

    def test_unknown_unit_raises(self):
        with pytest.raises(ValueError):
            convert_to_canonical_unit(0.3, "GalacticUnits/parsec", "tsh")

    def test_tsh_miu_per_ml_to_miu_per_l_factor_1000(self):
        v, u, note = convert_to_canonical_unit(0.001, "mIU/mL", "tsh")
        assert v == pytest.approx(1.0)
        assert u == "mIU/L"
        assert note is not None and note.startswith("unit_converted_")

    def test_tg_ng_per_dl_to_ng_per_ml(self):
        v, u, note = convert_to_canonical_unit(100.0, "ng/dL", "thyroglobulin")
        assert v == pytest.approx(1.0)
        assert u == "ng/mL"

    def test_null_unit_returns_canonical(self):
        v, u, note = convert_to_canonical_unit(5.5, None, "calcium")
        assert v == 5.5
        assert u == "mg/dL"
        assert note is None

    def test_value_none_passthrough(self):
        v, u, note = convert_to_canonical_unit(None, "mIU/mL", "tsh")
        assert v is None
        assert u == "mIU/L"
        assert note is not None
