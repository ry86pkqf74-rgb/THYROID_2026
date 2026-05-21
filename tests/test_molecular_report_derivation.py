from __future__ import annotations

from datetime import date

from utils.molecular_report_derivation import (
    derive_overall_result_class,
    derive_platform_from_report_header,
    parse_native_report_date,
)


def test_parse_native_report_date_handles_6511_digit_transposition() -> None:
    assert parse_native_report_date("12/1/17") == date(2017, 12, 1)


def test_parse_native_report_date_handles_7249_digit_transposition() -> None:
    assert parse_native_report_date("FNA 2/5/19") == date(2019, 2, 5)


def test_derive_overall_result_class_prefers_summary_negative() -> None:
    assert derive_overall_result_class(test_result_summary="CURRENTLY_NEGATIVE") == "negative"


def test_derive_overall_result_class_maps_cancelled_summary() -> None:
    assert derive_overall_result_class(test_result_summary="CANCELLED") == "cancelled"


def test_derive_platform_from_report_header_afirma_wins_over_source_call_label() -> None:
    text = "AFIRMA Thyroid FNA Analysis\nGene mutations source_call=ThyroSeq_FREEFORM_FALLBACK"
    platform, version = derive_platform_from_report_header(text, fallback_platform="ThyroSeq")
    assert platform == "Afirma"
    assert version is None


def test_derive_platform_from_report_header_thyroseq_version() -> None:
    platform, version = derive_platform_from_report_header("ThyroSeq GC v3 Test Result: Positive")
    assert platform == "ThyroSeq"
    assert version == 3
