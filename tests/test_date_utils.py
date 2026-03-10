"""Tests for utils.date_utils functions."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.date_utils import (
    classify_date_status,
    compute_date_confidence,
    compute_temporal_offset,
    find_best_anchor,
    parse_date_safe,
    resolve_event_date,
)


class TestClassifyDateStatus:
    def test_exact_source_date(self):
        assert classify_date_status("2024-01-15", None, None) == "exact_source_date"

    def test_inferred_day_level(self):
        assert classify_date_status(None, "2024-01-15", None) == "inferred_day_level_date"

    def test_coarse_anchor(self):
        assert classify_date_status(None, None, "2024-01-15") == "coarse_anchor_date"

    def test_unresolved(self):
        assert classify_date_status(None, None, None) == "unresolved_date"

    def test_native_takes_priority(self):
        result = classify_date_status("2024-01-01", "2024-02-01", "2024-03-01")
        assert result == "exact_source_date"

    def test_inferred_over_anchor(self):
        result = classify_date_status(None, "2024-02-01", "2024-03-01")
        assert result == "inferred_day_level_date"


class TestComputeDateConfidence:
    def test_exact_source_confidence(self):
        assert compute_date_confidence("exact_source_date") == 100

    def test_inferred_confidence(self):
        assert compute_date_confidence("inferred_day_level_date") == 70

    def test_coarse_anchor_confidence(self):
        assert compute_date_confidence("coarse_anchor_date") == 50

    def test_unresolved_confidence(self):
        assert compute_date_confidence("unresolved_date") == 0

    def test_coarse_anchor_multi_source_boost(self):
        single = compute_date_confidence("coarse_anchor_date", source_count=1)
        multi = compute_date_confidence("coarse_anchor_date", source_count=3)
        assert multi > single
        assert multi <= 60

    def test_coarse_anchor_boost_capped_at_60(self):
        result = compute_date_confidence("coarse_anchor_date", source_count=100)
        assert result == 60

    def test_non_coarse_multi_source_no_boost(self):
        assert compute_date_confidence("exact_source_date", source_count=5) == 100


class TestResolveEventDate:
    def test_native_date_first(self):
        date, status, conf = resolve_event_date(
            ["2024-01-15"], {}, {"note_date": "2024-02-01"},
        )
        assert date == "2024-01-15"
        assert status == "exact_source_date"
        assert conf == 100

    def test_skips_none_in_native(self):
        date, status, conf = resolve_event_date(
            [None, "2024-03-10"], {}, {},
        )
        assert date == "2024-03-10"
        assert status == "exact_source_date"

    def test_falls_to_inferred(self):
        date, status, conf = resolve_event_date(
            [None], {}, {"note_date": "2024-02-01"},
        )
        assert date == "2024-02-01"
        assert status == "inferred_day_level_date"
        assert conf == 70

    def test_falls_to_surgery_anchor(self):
        date, status, conf = resolve_event_date(
            [None], {"surgery": "2024-03-01", "fna": "2024-01-01"}, {},
        )
        assert date == "2024-03-01"
        assert status == "coarse_anchor_date"
        assert conf == 60

    def test_falls_to_fna_anchor(self):
        date, status, conf = resolve_event_date(
            [None], {"fna": "2024-01-01"}, {},
        )
        assert date == "2024-01-01"
        assert status == "coarse_anchor_date"
        assert conf == 45

    def test_falls_to_molecular_anchor(self):
        date, status, conf = resolve_event_date(
            [None], {"molecular": "2024-06-01"}, {},
        )
        assert date == "2024-06-01"
        assert status == "coarse_anchor_date"
        assert conf == 35

    def test_unresolved(self):
        date, status, conf = resolve_event_date([None], {}, {})
        assert date is None
        assert status == "unresolved_date"
        assert conf == 0

    def test_priority_ordering_surgery_over_fna(self):
        date, status, conf = resolve_event_date(
            [None],
            {"surgery": "2024-05-01", "fna": "2024-01-01", "molecular": "2024-02-01"},
            {},
        )
        assert date == "2024-05-01"
        assert conf == 60


class TestComputeTemporalOffset:
    def test_positive_offset(self):
        assert compute_temporal_offset("2024-01-20", "2024-01-15") == 5

    def test_negative_offset(self):
        assert compute_temporal_offset("2024-01-10", "2024-01-15") == -5

    def test_zero_offset(self):
        assert compute_temporal_offset("2024-01-15", "2024-01-15") == 0

    def test_none_event_date(self):
        assert compute_temporal_offset(None, "2024-01-15") is None

    def test_none_surgery_date(self):
        assert compute_temporal_offset("2024-01-15", None) is None

    def test_both_none(self):
        assert compute_temporal_offset(None, None) is None

    def test_invalid_format_returns_none(self):
        assert compute_temporal_offset("not-a-date", "2024-01-15") is None

    def test_large_offset(self):
        result = compute_temporal_offset("2025-01-15", "2024-01-15")
        assert result == 366


class TestParseDateSafe:
    def test_valid_iso_date(self):
        date, status = parse_date_safe("2024-03-15")
        assert date == "2024-03-15"
        assert status == "exact_source_date"

    def test_invalid_date(self):
        date, status = parse_date_safe("not-a-date")
        assert date is None
        assert status == "unresolved_date"

    def test_none_input(self):
        date, status = parse_date_safe(None)
        assert date is None
        assert status == "unresolved_date"

    def test_datetime_object(self):
        from datetime import datetime
        date, status = parse_date_safe(datetime(2024, 6, 15))
        assert date == "2024-06-15"
        assert status == "exact_source_date"

    def test_empty_string(self):
        date, status = parse_date_safe("")
        assert date is None
        assert status == "unresolved_date"


class TestFindBestAnchor:
    def test_surgery_priority(self):
        date, atype, conf = find_best_anchor(
            1,
            surgery_dates=["2024-03-01"],
            fna_dates=["2024-01-15"],
            molecular_dates=["2024-02-01"],
        )
        assert date == "2024-03-01"
        assert atype == "surgery"
        assert conf == 60

    def test_fna_fallback(self):
        date, atype, conf = find_best_anchor(
            1,
            surgery_dates=[None],
            fna_dates=["2024-01-15"],
            molecular_dates=["2024-02-01"],
        )
        assert date == "2024-01-15"
        assert atype == "fna"
        assert conf == 45

    def test_molecular_fallback(self):
        date, atype, conf = find_best_anchor(
            1,
            surgery_dates=[None],
            fna_dates=[None],
            molecular_dates=["2024-02-01"],
        )
        assert date == "2024-02-01"
        assert atype == "molecular"
        assert conf == 35

    def test_no_anchor_available(self):
        date, atype, conf = find_best_anchor(
            1,
            surgery_dates=[None],
            fna_dates=[None],
            molecular_dates=[None],
        )
        assert date is None
        assert atype is None
        assert conf == 0

    def test_earliest_surgery_date_selected(self):
        date, atype, conf = find_best_anchor(
            1,
            surgery_dates=["2024-06-01", "2024-03-01", "2024-09-01"],
            fna_dates=[],
            molecular_dates=[],
        )
        assert date == "2024-03-01"
        assert atype == "surgery"

    def test_skips_invalid_dates(self):
        date, atype, conf = find_best_anchor(
            1,
            surgery_dates=["not-a-date", None, "2024-05-01"],
            fna_dates=[],
            molecular_dates=[],
        )
        assert date == "2024-05-01"
        assert atype == "surgery"
