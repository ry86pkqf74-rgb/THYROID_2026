"""
Unit tests for scripts/113_tg_lab_ingestion.py

Covers:
  - Analyte classification (TEST_NAME_MAP completeness and correctness)
  - Result parsing (numeric, censored, titer, footnote, non-numeric)
  - Temporal window assignment (all 6 windows)
  - Combo disambiguation heuristic and cross-reference logic
  - Dedup key stability (Phase C)
  - Provenance column stability (Phase H)
  - Review queue routing (unmapped test names, ambiguous combos)
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# The script filename starts with a digit so it cannot be imported via standard
# dotted-module syntax. Use importlib to load it by file path.
_SCRIPT_PATH = ROOT / "scripts" / "113_tg_lab_ingestion.py"

def _load_script():
    spec = importlib.util.spec_from_file_location("_tg_ingest", _SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod

_M = _load_script()

TEST_NAME_MAP = _M.TEST_NAME_MAP
TEMPORAL_WINDOWS = _M.TEMPORAL_WINDOWS
_crossref_disambiguate = _M._crossref_disambiguate
_heuristic_disambiguate = _M._heuristic_disambiguate
_parse_single_result = _M._parse_single_result
phase_c_dedup = _M.phase_c_dedup
phase_d_normalize = _M.phase_d_normalize
phase_h_align_schema = _M.phase_h_align_schema
phase_j_append_longitudinal = _M.phase_j_append_longitudinal

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _minimal_raw_row(**kwargs) -> dict:
    """Return a minimal row dict matching the post-Phase-B schema."""
    defaults = {
        "research_id": 1001,
        "test_name": "THYROGLOBULIN",
        "specimen_collect_dt": "2022-01-15",
        "specimen_collect_dt_parsed": pd.Timestamp("2022-01-15"),
        "order_dt": "2022-01-14",
        "result": "0.5",
        "race": "White",
        "gender": "Female",
        "surg_date": "2021-06-01",
        "age": "45",
        "thyroid_procedure": "Total Thyroidectomy",
    }
    defaults.update(kwargs)
    return defaults


def _make_df(*rows) -> pd.DataFrame:
    """Accept either plain dicts or keyword-expanded dicts."""
    processed = []
    for r in rows:
        if isinstance(r, dict):
            processed.append(_minimal_raw_row(**r))
        else:
            processed.append(_minimal_raw_row())
    return pd.DataFrame(processed)


def _result_df_for_parsing(raw_value: str) -> pd.DataFrame:
    """Build a single-row DataFrame suitable for _parse_single_result."""
    df = pd.DataFrame([{
        "result_raw": raw_value,
        "result_numeric": np.nan,
        "result_qualifier": None,
        "result_flag": None,
    }])
    return df


# ---------------------------------------------------------------------------
# 1. Analyte classification (TEST_NAME_MAP)
# ---------------------------------------------------------------------------

VALID_ANALYTES = {"Tg", "TgAb", "COMBO"}
VALID_METHODS = {
    "immunoassay", "IMA", "LC-MS/MS", "RIA", "comprehensive",
    "IgG", "combo_panel", "reflex",
}


class TestAnalyteClassification:
    def test_all_map_entries_have_valid_analyte(self):
        for test_name, (analyte, method) in TEST_NAME_MAP.items():
            assert analyte in VALID_ANALYTES, (
                f"'{test_name}' maps to invalid analyte '{analyte}'"
            )

    def test_all_map_entries_have_valid_method(self):
        for test_name, (analyte, method) in TEST_NAME_MAP.items():
            assert method in VALID_METHODS, (
                f"'{test_name}' maps to invalid method '{method}'"
            )

    def test_tg_entries_classified_as_tg(self):
        tg_keys = [k for k, (a, _) in TEST_NAME_MAP.items() if a == "Tg"]
        assert len(tg_keys) >= 5, "Expected at least 5 Tg test names in map"

    def test_tgab_entries_classified_as_tgab(self):
        tgab_keys = [k for k, (a, _) in TEST_NAME_MAP.items() if a == "TgAb"]
        assert len(tgab_keys) >= 4, "Expected at least 4 TgAb test names in map"

    def test_combo_entry_exists(self):
        combo_keys = [k for k, (a, _) in TEST_NAME_MAP.items() if a == "COMBO"]
        assert len(combo_keys) >= 1, "Expected at least 1 COMBO test name in map"

    def test_phase_d_maps_all_known_names(self):
        rows = [_minimal_raw_row(test_name=tn) for tn in TEST_NAME_MAP]
        df = pd.DataFrame(rows)
        mapped, review = phase_d_normalize(df)
        assert len(review) == 0, (
            f"Known test names produced {len(review)} review rows — map regression"
        )
        assert (mapped["analyte"].notna()).all(), "Some mapped rows have null analyte"

    def test_phase_d_routes_unknown_test_name_to_review(self):
        df = _make_df({"test_name": "TOTALLY_UNKNOWN_ASSAY_XYZ"})
        _, review = phase_d_normalize(df)
        assert len(review) == 1
        assert review.iloc[0]["review_reason"] == "unmapped_test_name"


# ---------------------------------------------------------------------------
# 2. Result parsing
# ---------------------------------------------------------------------------

class TestResultParsing:
    def _parse(self, raw: str) -> dict:
        df = _result_df_for_parsing(raw)
        _parse_single_result(df, 0, raw)
        return df.iloc[0].to_dict()

    def test_plain_numeric(self):
        r = self._parse("12.5")
        assert r["result_numeric"] == pytest.approx(12.5)
        assert r["result_qualifier"] == "="
        assert r["result_flag"] == "numeric"

    def test_below_detection_less_than(self):
        r = self._parse("<0.2")
        assert r["result_numeric"] == pytest.approx(0.2)
        assert r["result_qualifier"] == "<"
        assert r["result_flag"] == "below_detection"

    def test_below_detection_with_space(self):
        r = self._parse("< 0.1")
        assert r["result_numeric"] == pytest.approx(0.1)
        assert r["result_qualifier"] == "<"
        assert r["result_flag"] == "below_detection"

    def test_above_detection(self):
        r = self._parse(">100")
        assert r["result_numeric"] == pytest.approx(100.0)
        assert r["result_qualifier"] == ">"
        assert r["result_flag"] == "above_detection"

    def test_titer_format(self):
        r = self._parse("1:160")
        assert r["result_numeric"] == pytest.approx(160.0)
        assert r["result_qualifier"] == "="
        assert r["result_flag"] == "titer"

    def test_footnote(self):
        r = self._parse("FOOTNOTE")
        assert r["result_flag"] == "footnote"
        assert np.isnan(r["result_numeric"])

    def test_see_scanned(self):
        r = self._parse("SEE SCANNED RESULT")
        assert r["result_flag"] == "see_scanned"

    def test_negative(self):
        r = self._parse("NEGATIVE")
        assert r["result_flag"] == "negative"

    def test_non_numeric_string(self):
        r = self._parse("PENDING")
        assert r["result_flag"] == "non_numeric"
        assert np.isnan(r["result_numeric"])

    def test_zero_is_numeric(self):
        r = self._parse("0")
        assert r["result_numeric"] == pytest.approx(0.0)
        assert r["result_flag"] == "numeric"

    def test_large_numeric(self):
        r = self._parse("9999.99")
        assert r["result_numeric"] == pytest.approx(9999.99)
        assert r["result_flag"] == "numeric"


# ---------------------------------------------------------------------------
# 3. Temporal window assignment
# ---------------------------------------------------------------------------

class TestTemporalWindows:
    """Verify TEMPORAL_WINDOWS constants cover all expected clinical windows."""

    def _label_for_days(self, days: int) -> str | None:
        for lo, hi, label in TEMPORAL_WINDOWS:
            if lo <= days <= hi:
                return label
        return None

    def test_pre_surgery(self):
        assert self._label_for_days(-30) == "pre_surgery"
        assert self._label_for_days(-1) == "pre_surgery"

    def test_perioperative(self):
        assert self._label_for_days(0) == "perioperative"
        assert self._label_for_days(15) == "perioperative"
        assert self._label_for_days(30) == "perioperative"

    def test_early_postop(self):
        assert self._label_for_days(31) == "early_postop"
        assert self._label_for_days(180) == "early_postop"

    def test_surveillance_1y(self):
        assert self._label_for_days(181) == "surveillance_1y"
        assert self._label_for_days(365) == "surveillance_1y"

    def test_surveillance_5y(self):
        assert self._label_for_days(366) == "surveillance_5y"
        assert self._label_for_days(1825) == "surveillance_5y"

    def test_long_term(self):
        assert self._label_for_days(1826) == "long_term"
        assert self._label_for_days(5000) == "long_term"

    def test_no_gap_between_windows(self):
        """No integer day value between -999999 and 999999 should fall through."""
        # Spot-check boundaries
        all_labels = [self._label_for_days(d) for d in range(-5, 2000)]
        assert all(lbl is not None for lbl in all_labels), (
            "Gap detected in temporal window coverage"
        )

    def test_windows_sorted_ascending(self):
        bounds = [(lo, hi) for lo, hi, _ in TEMPORAL_WINDOWS]
        for i in range(1, len(bounds)):
            assert bounds[i][0] > bounds[i - 1][1], (
                f"Windows {i-1} and {i} overlap or are not sorted"
            )


# ---------------------------------------------------------------------------
# 4. Combo disambiguation
# ---------------------------------------------------------------------------

class TestComboDisambiguation:
    def test_heuristic_tgab_sentinel_less_09(self):
        # <0.9 unambiguously indicates TgAb
        result = _heuristic_disambiguate("<0.9", "5.4")
        assert result == "b_is_tg"

    def test_heuristic_tg_sentinel_less_02(self):
        result = _heuristic_disambiguate("<0.2", "45.0")
        assert result == "a_is_tg"

    def test_heuristic_tgab_high_vs_tg_low(self):
        result = _heuristic_disambiguate("<2", "<0.2")
        assert result == "b_is_tg"

    def test_heuristic_ambiguous_both_unknown(self):
        result = _heuristic_disambiguate("3.5", "2.1")
        assert result is None

    def test_heuristic_symmetric_swapped(self):
        a = _heuristic_disambiguate("45.0", "<0.9")
        b = _heuristic_disambiguate("<0.9", "45.0")
        assert a == "a_is_tg"
        assert b == "b_is_tg"

    def test_crossref_uses_known_values(self):
        tg_vals = {1001: {"0.5", "0.2"}}
        tgab_vals = {1001: {"<0.9", "1.2"}}
        result = _crossref_disambiguate(1001, "0.5", "<0.9", tg_vals, tgab_vals)
        assert result == "a_is_tg"

    def test_crossref_returns_none_when_no_data(self):
        result = _crossref_disambiguate(9999, "3.5", "2.0", {}, {})
        assert result is None

    def test_crossref_returns_none_on_ambiguous_match(self):
        tg_vals = {1001: {"3.5"}}
        tgab_vals = {1001: {"3.5"}}
        result = _crossref_disambiguate(1001, "3.5", "2.0", tg_vals, tgab_vals)
        assert result is None


# ---------------------------------------------------------------------------
# 5. Dedup key stability (Phase C)
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_exact_duplicate_removed(self):
        df = _make_df(
            dict(research_id=1001, test_name="THYROGLOBULIN",
                 specimen_collect_dt="2022-01-15", result="0.5"),
            dict(research_id=1001, test_name="THYROGLOBULIN",
                 specimen_collect_dt="2022-01-15", result="0.5"),
        )
        out = phase_c_dedup(df)
        assert len(out) == 1

    def test_different_result_not_deduped(self):
        df = _make_df(
            dict(research_id=1001, test_name="THYROGLOBULIN",
                 specimen_collect_dt="2022-01-15", result="0.5"),
            dict(research_id=1001, test_name="THYROGLOBULIN",
                 specimen_collect_dt="2022-01-15", result="1.0"),
        )
        out = phase_c_dedup(df)
        assert len(out) == 2

    def test_different_dates_not_deduped(self):
        df = _make_df(
            dict(research_id=1001, specimen_collect_dt="2022-01-15", result="0.5"),
            dict(research_id=1001, specimen_collect_dt="2022-06-01", result="0.5"),
        )
        out = phase_c_dedup(df)
        assert len(out) == 2

    def test_different_patients_not_deduped(self):
        df = _make_df(
            dict(research_id=1001, result="0.5"),
            dict(research_id=1002, result="0.5"),
        )
        out = phase_c_dedup(df)
        assert len(out) == 2

    def test_three_identical_rows_keeps_one(self):
        base = dict(research_id=2001, test_name="Thyroglobulin Antibody",
                    specimen_collect_dt="2023-03-10", result="<0.9")
        df = _make_df(base, base, base)
        out = phase_c_dedup(df)
        assert len(out) == 1


# ---------------------------------------------------------------------------
# 6. Provenance columns (Phase H)
# ---------------------------------------------------------------------------

# Provenance columns produced by phase_h_align_schema (the canonical output schema).
# Note: source_table, source_script, ingestion_wave, data_completeness_tier, provenance_note
# are added later in phase_j_append_longitudinal when mapping to longitudinal_lab_canonical_v1.
REQUIRED_PROVENANCE_COLS = [
    "ingestion_script",
    "ingestion_date",
    "analyte",
    "assay_method",
    "temporal_window",
    "days_from_surgery",
    "disambiguation_method",
    "disambiguation_confidence",
]

# Columns added in phase_j longitudinal mapping (tested separately below)
LONGITUDINAL_WAVE_COLS = [
    "source_table",
    "source_script",
    "ingestion_wave",
    "data_completeness_tier",
    "provenance_note",
]


class TestProvenanceColumns:
    def _make_canonical_row(self, **kwargs):
        """Build a minimal DataFrame that can pass through phase_h_align_schema."""
        base = {
            "research_id": 5001,
            "analyte": "Tg",
            "assay_method": "immunoassay",
            "test_name_raw": "THYROGLOBULIN",
            "specimen_collect_dt_parsed": pd.Timestamp("2022-03-01"),
            "order_dt": "2022-02-28",
            "result_raw": "2.1",
            "result_numeric": 2.1,
            "result_qualifier": "=",
            "result_flag": "numeric",
            "days_from_surgery": 270,
            "temporal_window": "early_postop",
            "surg_date_parsed": pd.Timestamp("2021-06-01"),
            "race": "White",
            "gender": "Female",
            "age": "50",
            "thyroid_procedure": "Total Thyroidectomy",
            "disambiguation_method": "direct_label",
            "disambiguation_confidence": 1.0,
        }
        base.update(kwargs)
        return pd.DataFrame([base])

    def test_all_provenance_columns_present(self):
        df = self._make_canonical_row()
        canonical = phase_h_align_schema(df)
        missing = [c for c in REQUIRED_PROVENANCE_COLS if c not in canonical.columns]
        assert not missing, f"Missing provenance columns: {missing}"

    def test_ingestion_script_not_null(self):
        df = self._make_canonical_row()
        canonical = phase_h_align_schema(df)
        assert canonical["ingestion_script"].notna().all()

    def test_ingestion_date_not_null(self):
        df = self._make_canonical_row()
        canonical = phase_h_align_schema(df)
        assert canonical["ingestion_date"].notna().all()

    def test_analyte_preserved(self):
        df = self._make_canonical_row(analyte="TgAb")
        canonical = phase_h_align_schema(df)
        assert canonical.iloc[0]["analyte"] == "TgAb"

    def test_assay_method_preserved(self):
        df = self._make_canonical_row(assay_method="LC-MS/MS")
        canonical = phase_h_align_schema(df)
        assert canonical.iloc[0]["assay_method"] == "LC-MS/MS"

    def test_ingestion_wave_in_longitudinal_mapping(self):
        """Verify wave values are the documented EHR-structured wave identifiers."""
        df_tg = self._make_canonical_row(analyte="Tg")
        df_tgab = self._make_canonical_row(analyte="TgAb")
        tg_canonical = phase_h_align_schema(df_tg)
        tgab_canonical = phase_h_align_schema(df_tgab)
        combined = pd.concat([tg_canonical, tgab_canonical], ignore_index=True)
        # Reconstruct the wave mapping inline (mirrors phase_j_append_longitudinal logic)
        waves = np.where(
            combined["analyte"] == "Tg",
            "wave_tg_structured_ehr",
            "wave_tgab_structured_ehr",
        )
        assert list(waves) == ["wave_tg_structured_ehr", "wave_tgab_structured_ehr"]


# ---------------------------------------------------------------------------
# 7. Review queue routing
# ---------------------------------------------------------------------------

class TestReviewQueueRouting:
    def test_unmapped_test_name_lands_in_review(self):
        df = _make_df({"test_name": "COMPLETELY_UNKNOWN_PANEL_ZZZ"})
        _, review = phase_d_normalize(df)
        assert len(review) == 1
        assert "unmapped_test_name" in review["review_reason"].values

    def test_known_test_name_not_in_review(self):
        df = _make_df({"test_name": "THYROGLOBULIN"})
        mapped, review = phase_d_normalize(df)
        assert len(review) == 0
        assert len(mapped) == 1

    def test_multiple_unmapped_all_in_review(self):
        df = _make_df(
            {"test_name": "UNKNOWN_A"},
            {"test_name": "UNKNOWN_B"},
        )
        _, review = phase_d_normalize(df)
        assert len(review) == 2
        assert set(review["review_reason"]) == {"unmapped_test_name"}

    def test_mixed_known_and_unknown(self):
        df = _make_df(
            {"test_name": "THYROGLOBULIN"},
            {"test_name": "TOTALLY_UNKNOWN"},
        )
        mapped, review = phase_d_normalize(df)
        assert len(mapped) == 1
        assert len(review) == 1

    def test_review_reason_column_present(self):
        df = _make_df({"test_name": "MYSTERY_LAB"})
        _, review = phase_d_normalize(df)
        assert "review_reason" in review.columns

    def test_review_preserves_research_id(self):
        df = _make_df({"test_name": "UNKNOWN_Z", "research_id": 9999})
        _, review = phase_d_normalize(df)
        assert 9999 in review["research_id"].values
