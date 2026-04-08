"""Tests for DICOM flattened-header ingest helpers and deterministic linkage."""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

from utils.dicom_header_helpers import (  # noqa: E402
    build_column_lookup,
    build_enriched_rows,
    fetch_linkage_candidates_union,
    load_alias_config,
    normalize_accession_key,
    optional_attach_dicom_to_imaging_nodule_frame,
    read_input_files,
    resolve_exact_links,
    row_fingerprint_sha256,
    rows_to_study_series,
)

FIXTURES = ROOT / "tests" / "fixtures" / "dicom_headers"


def test_alias_load_and_column_lookup() -> None:
    cfg = load_alias_config()
    assert "canonical_fields" in cfg
    lu = build_column_lookup(cfg["canonical_fields"])
    assert lu["studyinstanceuid"] == "study_instance_uid"
    assert lu["mrn"] == "patient_id"


def test_accession_normalization_matches_specimen_key_rules() -> None:
    assert normalize_accession_key("  ABC-123 ") == "abc123"
    assert normalize_accession_key(None) is None


def test_study_series_dedup_and_row_counts() -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    df = read_input_files([FIXTURES / "study_series_synthetic.csv"], "csv")
    enriched = build_enriched_rows(df, lu)
    prov, study, series = rows_to_study_series(enriched, ingestion_run_id="testrun")
    assert len(study) == 1
    assert study.iloc[0]["study_instance_uid"] == "1.2.840.10008.1.1.1.1"
    assert len(series) == 2
    assert len(prov) == 2


def test_malformed_uid_qc_flags() -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    df = read_input_files([FIXTURES / "malformed_uid.csv"], "csv")
    enriched = build_enriched_rows(df, lu)
    _, study, _ = rows_to_study_series(enriched, ingestion_run_id="t")
    flags = json.loads(study.iloc[0]["qc_flags_json"])
    assert "MALFORMED_STUDY_INSTANCE_UID" in flags


def test_json_input_alias_normalization() -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    df = read_input_files([FIXTURES / "rows.json"], "json")
    enriched = build_enriched_rows(df, lu)
    assert enriched[0]["study_instance_uid"] == "1.2.840.10008.1.4.1.1"
    assert normalize_accession_key(enriched[0].get("accession_number")) == "js1"


def test_parquet_roundtrip(tmp_path: Path) -> None:
    df = read_input_files([FIXTURES / "study_series_synthetic.csv"], "csv")
    pq = tmp_path / "in.parquet"
    df.to_parquet(pq, index=False)
    df2 = read_input_files([pq], "parquet")
    assert len(df2) == len(df)


def test_fingerprint_stable() -> None:
    a = {"b": 1, "a": 2}
    b = {"a": 2, "b": 1}
    assert row_fingerprint_sha256(a) == row_fingerprint_sha256(b)


def test_explicit_research_id_link_priority() -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    df = read_input_files([FIXTURES / "explicit_research_id.csv"], "csv")
    enriched = build_enriched_rows(df, lu)
    _, study, _ = rows_to_study_series(enriched, ingestion_run_id="run")
    cand = pd.DataFrame(
        [
            {
                "research_id": 999999,
                "accession_norm": "expl1",
                "imaging_exam_id": None,
                "imaging_nodule_id": None,
                "specimen_id": None,
                "source_table": "imaging",
                "exam_date_yyyymmdd": None,
            }
        ],
    )
    links, reviews = resolve_exact_links(study, cand, ingestion_run_id="run")
    assert len(links) == 1
    assert links.iloc[0]["linkage_tier"] == "explicit_research_id"
    assert int(links.iloc[0]["research_id"]) == 424242
    assert len(reviews) == 0


def test_ambiguous_accession_multi_research_to_review() -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    df = read_input_files([FIXTURES / "study_series_synthetic.csv"], "csv")
    enriched = build_enriched_rows(df, lu)
    _, study, _ = rows_to_study_series(enriched, ingestion_run_id="r")
    acc = normalize_accession_key("SYN-ACC-1001")
    cand = pd.DataFrame(
        [
            {
                "research_id": 1,
                "accession_norm": acc,
                "imaging_exam_id": "e1",
                "imaging_nodule_id": "n1",
                "specimen_id": None,
                "source_table": "imaging",
                "exam_date_yyyymmdd": "20240115",
            },
            {
                "research_id": 2,
                "accession_norm": acc,
                "imaging_exam_id": "e2",
                "imaging_nodule_id": "n2",
                "specimen_id": None,
                "source_table": "imaging",
                "exam_date_yyyymmdd": "20240115",
            },
        ],
    )
    links, reviews = resolve_exact_links(study, cand, ingestion_run_id="r")
    assert links.empty
    assert not reviews.empty
    assert reviews.iloc[0]["reason_code"] == "AMBIGUOUS_ACCESSION_MULTI_RESEARCH_ID"


def test_exact_accession_single_candidate_link() -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    df = read_input_files([FIXTURES / "study_series_synthetic.csv"], "csv")
    enriched = build_enriched_rows(df, lu)
    _, study, _ = rows_to_study_series(enriched, ingestion_run_id="r")
    acc = normalize_accession_key("SYN-ACC-1001")
    cand = pd.DataFrame(
        [
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": "examA",
                "imaging_nodule_id": "nodA",
                "specimen_id": None,
                "source_table": "imaging",
                "exam_date_yyyymmdd": "20240115",
            },
        ],
    )
    links, reviews = resolve_exact_links(study, cand, ingestion_run_id="r")
    assert len(links) == 1
    assert links.iloc[0]["linkage_tier"] == "exact_accession"
    assert int(links.iloc[0]["research_id"]) == 77
    assert reviews.empty


def test_date_discordant_blocks_auto_link() -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    df = read_input_files([FIXTURES / "study_series_synthetic.csv"], "csv")
    enriched = build_enriched_rows(df, lu)
    _, study, _ = rows_to_study_series(enriched, ingestion_run_id="r")
    acc = normalize_accession_key("SYN-ACC-1001")
    cand = pd.DataFrame(
        [
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": "examA",
                "imaging_nodule_id": "nodA",
                "specimen_id": None,
                "source_table": "imaging",
                "exam_date_yyyymmdd": "20240601",
            },
        ],
    )
    links, reviews = resolve_exact_links(
        study,
        cand,
        ingestion_run_id="r",
        date_skew_days_max=14,
    )
    assert links.empty
    assert reviews.iloc[0]["reason_code"] == "DATE_DISCORDANT_ACCESSION_MATCH"


def test_no_fuzzy_no_mrn_date_auto_link() -> None:
    """MRN + study date alone must never create a link; accession must match exactly."""
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    df = read_input_files([FIXTURES / "mrn_no_accession.csv"], "csv")
    enriched = build_enriched_rows(df, lu)
    _, study, _ = rows_to_study_series(enriched, ingestion_run_id="r")
    cand = pd.DataFrame(
        [
            {
                "research_id": 999,
                "accession_norm": normalize_accession_key("ONLY_IN_CATALOG"),
                "imaging_exam_id": "e",
                "imaging_nodule_id": "n",
                "specimen_id": None,
                "source_table": "imaging",
                "exam_date_yyyymmdd": "20240401",
            },
        ],
    )
    links, reviews = resolve_exact_links(study, cand, ingestion_run_id="r")
    assert links.empty
    assert not reviews.empty
    rc = set(reviews["reason_code"].tolist())
    assert "MISSING_ACCESSION_NO_RESEARCH_ID" in rc


def test_fetch_linkage_candidates_union_empty_db() -> None:
    con = duckdb.connect(database=":memory:")
    df = fetch_linkage_candidates_union(con)
    assert df.empty
    con.close()


def test_optional_attach_is_noop_without_dicom_table() -> None:
    con = duckdb.connect(database=":memory:")
    img = pd.DataFrame({"research_id": [1]})
    out = optional_attach_dicom_to_imaging_nodule_frame(img, con)
    pd.testing.assert_frame_equal(out, img)
    con.close()