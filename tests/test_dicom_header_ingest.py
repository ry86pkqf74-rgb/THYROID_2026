"""Tests for DICOM flattened-header ingest helpers and deterministic linkage."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, SecondaryCaptureImageStorage, generate_uid

ROOT = Path(__file__).resolve().parent.parent

from utils.dicom_header_helpers import (  # noqa: E402
    build_column_lookup,
    build_enriched_rows,
    fetch_linkage_candidates_union,
    load_alias_config,
    normalize_accession_key,
    optional_attach_dicom_to_imaging_nodule_frame,
    read_dicom_metadata_flat_row,
    read_input_files,
    resolve_exact_links,
    row_fingerprint_sha256,
    rows_to_study_series,
)

FIXTURES = ROOT / "tests" / "fixtures" / "dicom_headers"
FIX_TS = datetime(2026, 4, 8, 12, 0, 0, tzinfo=timezone.utc)


def _write_minimal_synthetic_dicom(path: Path, **kwargs: str) -> None:
    """Minimal Secondary Capture instance (single placeholder pixel) for tests only."""
    attrs: dict[str, str] = dict(kwargs)
    sop_raw = attrs.pop("sop_instance_uid", None)
    sop_uid = generate_uid() if not sop_raw else pydicom.uid.UID(sop_raw)
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = sop_uid
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID

    ds = FileDataset(str(path), {}, file_meta=file_meta, preamble=b"\x00" * 128)
    ds.SOPClassUID = SecondaryCaptureImageStorage
    ds.SOPInstanceUID = sop_uid
    mapping = {
        "StudyInstanceUID": "study_instance_uid",
        "SeriesInstanceUID": "series_instance_uid",
        "AccessionNumber": "accession_number",
        "StudyDate": "study_date",
        "SeriesDate": "series_date",
        "Modality": "modality",
        "BodyPartExamined": "body_part_examined",
        "StudyDescription": "study_description",
        "SeriesDescription": "series_description",
        "PatientID": "patient_id",
        "InstitutionName": "institution_name",
    }
    for elem_keyword, attr in mapping.items():
        if attr in attrs and attrs[attr]:
            setattr(ds, elem_keyword, attrs[attr])
    ds.Rows = 1
    ds.Columns = 1
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelRepresentation = 0
    ds.PixelData = bytes([0])
    ds.save_as(str(path), enforce_file_format=True)


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


def test_date_discordance_max_skew_across_multiple_exam_dates() -> None:
    """Do not compare only the first distinct exam_date; worst skew vs study wins."""
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
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": "examA",
                "imaging_nodule_id": "nodB",
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
    assert "max delta" in (reviews.iloc[0]["conflict_note"] or "").lower()


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


def test_synthetic_dcm_extracts_uids_and_accession(tmp_path: Path) -> None:
    path = tmp_path / "one.dcm"
    _write_minimal_synthetic_dicom(
        path,
        study_instance_uid="1.2.840.10008.1.1.1.1",
        series_instance_uid="1.2.840.10008.1.1.1.2",
        accession_number="SYN-ACC-1001",
        study_date="20240115",
    )
    flat = read_dicom_metadata_flat_row(path)
    assert flat["StudyInstanceUID"] == "1.2.840.10008.1.1.1.1"
    assert flat["SeriesInstanceUID"] == "1.2.840.10008.1.1.1.2"
    assert flat["AccessionNumber"] == "SYN-ACC-1001"


def test_parity_csv_vs_raw_dcm_study_series(tmp_path: Path) -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    df_csv = read_input_files([FIXTURES / "study_series_synthetic.csv"], "csv")
    p1 = tmp_path / "s1.dcm"
    p2 = tmp_path / "s2.dcm"
    _write_minimal_synthetic_dicom(
        p1,
        study_instance_uid="1.2.840.10008.1.1.1.1",
        series_instance_uid="1.2.840.10008.1.1.1.2",
        accession_number="SYN-ACC-1001",
        study_date="20240115",
        series_date="20240115",
        modality="US",
        body_part_examined="NECK",
        study_description="Thyroid US",
        series_description="Right lobe nodule",
        patient_id="M00001",
        institution_name="TestOrg",
    )
    _write_minimal_synthetic_dicom(
        p2,
        study_instance_uid="1.2.840.10008.1.1.1.1",
        series_instance_uid="1.2.840.10008.1.1.1.3",
        accession_number="SYN-ACC-1001",
        study_date="20240115",
        series_date="20240115",
        modality="US",
        body_part_examined="NECK",
        study_description="Thyroid US",
        series_description="Left lobe survey",
        patient_id="M00001",
        institution_name="TestOrg",
    )
    df_dcm = read_input_files([p1, p2], "auto")
    enc_c = build_enriched_rows(df_csv, lu)
    enc_d = build_enriched_rows(df_dcm, lu)
    _, study_c, series_c = rows_to_study_series(enc_c, ingestion_run_id="parity", ingestion_ts=FIX_TS)
    _, study_d, series_d = rows_to_study_series(enc_d, ingestion_run_id="parity", ingestion_ts=FIX_TS)
    pd.testing.assert_frame_equal(
        study_c.sort_values("study_instance_uid").reset_index(drop=True),
        study_d.sort_values("study_instance_uid").reset_index(drop=True),
        check_like=True,
    )
    sc = series_c.sort_values("series_instance_uid").reset_index(drop=True)
    sd = series_d.sort_values("series_instance_uid").reset_index(drop=True)
    pd.testing.assert_frame_equal(sc, sd, check_like=True)


def test_dcm_exact_accession_single_candidate_matches_csv_behavior(tmp_path: Path) -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    path = tmp_path / "link.dcm"
    _write_minimal_synthetic_dicom(
        path,
        study_instance_uid="1.2.840.10008.1.1.1.1",
        series_instance_uid="1.2.840.10008.1.1.1.2",
        accession_number="SYN-ACC-1001",
        study_date="20240115",
    )
    df = read_input_files([path], "auto")
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


def test_dcm_malformed_file_routes_to_provenance_qc_not_auto_link(tmp_path: Path) -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    bad = tmp_path / "bad.dcm"
    bad.write_bytes(b"not valid dicom")
    df = read_input_files([bad], "dcm")
    enriched = build_enriched_rows(df, lu)
    prov, study, _ = rows_to_study_series(enriched, ingestion_run_id="qc")
    assert study.empty
    assert len(prov) == 1
    assert prov.iloc[0]["parse_status"] == "error"
    qc = json.loads(prov.iloc[0]["qc_flags_json"])
    assert "MISSING_STUDY_INSTANCE_UID" in qc
    links, reviews = resolve_exact_links(study, pd.DataFrame(), ingestion_run_id="qc")
    assert links.empty
    assert reviews.empty


def test_dcm_missing_accession_no_auto_link_to_review(tmp_path: Path) -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    path = tmp_path / "nacc.dcm"
    _write_minimal_synthetic_dicom(
        path,
        study_instance_uid="1.2.840.10008.1.1.1.9",
        series_instance_uid="1.2.840.10008.1.1.1.99",
        accession_number="",
    )
    df = read_input_files([path], "auto")
    enriched = build_enriched_rows(df, lu)
    _, study, _ = rows_to_study_series(enriched, ingestion_run_id="r")
    cand = pd.DataFrame(
        [
            {
                "research_id": 1,
                "accession_norm": normalize_accession_key("X"),
                "imaging_exam_id": "e",
                "imaging_nodule_id": "n",
                "specimen_id": None,
                "source_table": "imaging",
                "exam_date_yyyymmdd": "20240101",
            },
        ],
    )
    links, reviews = resolve_exact_links(study, cand, ingestion_run_id="r")
    assert links.empty
    assert not reviews.empty
    assert reviews.iloc[0]["reason_code"] == "MISSING_ACCESSION_NO_RESEARCH_ID"


def test_ambiguous_accession_multi_specimen_no_auto_link() -> None:
    """One research_id, one imaging exam id, multiple distinct specimen_id → review, no link."""
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
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": None,
                "imaging_nodule_id": None,
                "specimen_id": "sp1",
                "source_table": "specimen",
                "exam_date_yyyymmdd": None,
            },
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": None,
                "imaging_nodule_id": None,
                "specimen_id": "sp2",
                "source_table": "specimen",
                "exam_date_yyyymmdd": None,
            },
        ],
    )
    links, reviews = resolve_exact_links(study, cand, ingestion_run_id="r")
    assert links.empty
    assert reviews.iloc[0]["reason_code"] == "AMBIGUOUS_ACCESSION_MULTI_SPECIMEN"
    spec_json = json.loads(reviews.iloc[0]["candidate_specimen_ids_json"])
    assert set(spec_json) == {"sp1", "sp2"}


def test_repeated_rows_single_distinct_specimen_still_links() -> None:
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
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": None,
                "imaging_nodule_id": None,
                "specimen_id": "spx",
                "source_table": "specimen",
                "exam_date_yyyymmdd": None,
            },
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": None,
                "imaging_nodule_id": None,
                "specimen_id": "spx",
                "source_table": "specimen",
                "exam_date_yyyymmdd": None,
            },
        ],
    )
    links, reviews = resolve_exact_links(study, cand, ingestion_run_id="r")
    assert len(links) == 1
    assert links.iloc[0]["specimen_id"] == "spx"
    assert reviews.empty


def test_multi_imaging_exam_includes_specimen_candidates_and_no_link() -> None:
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
                "imaging_exam_id": "e1",
                "imaging_nodule_id": "n1",
                "specimen_id": None,
                "source_table": "imaging",
                "exam_date_yyyymmdd": "20240115",
            },
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": "e2",
                "imaging_nodule_id": "n2",
                "specimen_id": None,
                "source_table": "imaging",
                "exam_date_yyyymmdd": "20240115",
            },
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": None,
                "imaging_nodule_id": None,
                "specimen_id": "s1",
                "source_table": "specimen",
                "exam_date_yyyymmdd": None,
            },
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": None,
                "imaging_nodule_id": None,
                "specimen_id": "s2",
                "source_table": "specimen",
                "exam_date_yyyymmdd": None,
            },
        ],
    )
    links, reviews = resolve_exact_links(study, cand, ingestion_run_id="r")
    assert links.empty
    assert reviews.iloc[0]["reason_code"] == "AMBIGUOUS_ACCESSION_MULTI_IMAGING_EXAM"
    assert set(json.loads(reviews.iloc[0]["candidate_specimen_ids_json"])) == {"s1", "s2"}
    assert "specimen" in (reviews.iloc[0]["conflict_note"] or "").lower()


def test_blank_ids_ignored_for_distinct_counts() -> None:
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
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": "   ",
                "imaging_nodule_id": None,
                "specimen_id": None,
                "source_table": "imaging",
                "exam_date_yyyymmdd": "20240115",
            },
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": None,
                "imaging_nodule_id": None,
                "specimen_id": " ",
                "source_table": "specimen",
                "exam_date_yyyymmdd": None,
            },
            {
                "research_id": 77,
                "accession_norm": acc,
                "imaging_exam_id": None,
                "imaging_nodule_id": None,
                "specimen_id": "sp1",
                "source_table": "specimen",
                "exam_date_yyyymmdd": None,
            },
        ],
    )
    links, reviews = resolve_exact_links(study, cand, ingestion_run_id="r")
    assert len(links) == 1
    assert links.iloc[0]["specimen_id"] == "sp1"


def test_flattened_formats_still_run_after_dcm_support() -> None:
    cfg = load_alias_config()
    lu = build_column_lookup(cfg["canonical_fields"])
    df = read_input_files(
        [FIXTURES / "study_series_synthetic.csv", FIXTURES / "rows.json"],
        "auto",
    )
    assert len(df) == 3
    enc = build_enriched_rows(df, lu)
    _, study, series = rows_to_study_series(enc, ingestion_run_id="mix")
    assert len(study) == 2