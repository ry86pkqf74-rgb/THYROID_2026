"""Unit tests for utils/molecular_ingest_common.py."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils import molecular_ingest_common as mic
from utils.molecular_ingest_common import (
    canonicalize_columns_from_map,
    checksum_sorted_json_payload,
    compute_keyed_row_hash,
    molecular_result_id_from_parts,
    molecular_variant_id_afirma,
    molecular_variant_id_thyroseq,
    normalize_header_snake,
    parse_test_date_iso_and_native,
    parse_thyroseq_workbook_test_date_cell,
)


def test_normalize_header_snake() -> None:
    assert normalize_header_snake("Pt. MRN") == "pt_mrn"
    assert normalize_header_snake("Research ID") == "research_id"


def test_canonicalize_columns_from_map() -> None:
    df = pd.DataFrame({"Research_ID": [1], "pt mrn": ["x"]})
    amap = {"research_id": ["research_id"], "mrn": ["pt_mrn", "mrname"]}
    out = canonicalize_columns_from_map(df, amap)
    assert "research_id" in out.columns
    assert "mrn" in out.columns


def test_compute_keyed_row_hash_order_matters() -> None:
    keys = ("a", "b")
    h1 = compute_keyed_row_hash({"a": "1", "b": "2"}, keys)
    h2 = compute_keyed_row_hash({"a": "1", "b": "2"}, keys)
    assert h1 == h2
    h3 = compute_keyed_row_hash({"a": "2", "b": "1"}, keys)
    assert h1 != h3


def test_parse_test_date_iso_and_native() -> None:
    iso, native = parse_test_date_iso_and_native("2024-01-15")
    assert iso == "2024-01-15"
    assert native == "2024-01-15"


def test_parse_thyroseq_workbook_test_date_cell() -> None:
    from datetime import date

    n, d = parse_thyroseq_workbook_test_date_cell(date(2023, 1, 1))
    assert d == date(2023, 1, 1)
    assert n == "2023-01-01"


def test_molecular_result_id_from_parts_matches_raw_sha() -> None:
    import hashlib

    rid = molecular_result_id_from_parts("afirma", "abc", "NA")
    assert rid == hashlib.sha256(b"afirma|abc|NA").hexdigest()[:32]


def test_variant_ids_differ_afirma_vs_thyroseq() -> None:
    spec = {"variant_class": "SNV", "raw_variant_token": "t", "gene_symbol": "B", "cdna_hgvs": "c.1A>T"}
    a = molecular_variant_id_afirma("mr1", 0, spec)
    b = molecular_variant_id_thyroseq("mr1", 0, spec)
    assert a != b


def test_checksum_sorted_json_stable() -> None:
    assert checksum_sorted_json_payload({"b": 1, "a": 2}) == checksum_sorted_json_payload({"a": 2, "b": 1})


def test_config_load_embedded_when_yaml_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(mic, "_DEFAULT_ALIAS_PATH", tmp_path / "nope.yaml")
    mic.clear_molecular_ingest_config_cache()
    cfg = mic.load_molecular_ingest_config(force_reload=True)
    assert "afirma_column_aliases" in cfg
    assert "thyroseq_row_hash_fields" in cfg
    mic.clear_molecular_ingest_config_cache()
