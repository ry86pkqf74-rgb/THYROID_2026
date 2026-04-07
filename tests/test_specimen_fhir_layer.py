"""Offline tests: specimen fingerprint parity + multi-row synoptic isolation logic."""

from __future__ import annotations

import hashlib

import duckdb

from utils.specimen_fingerprint import (
    specimen_master_fingerprint_input,
    specimen_master_fingerprint_sha256,
    tumor_focus_fingerprint_sha256,
)


def test_specimen_master_fingerprint_stable_normalization() -> None:
    kwargs = dict(
        research_id=" 12 ",
        source_system="PATHOLOGY_SYNOPTIC_ENCOUNTER",
        procedure_date_day="2020-01-15",
        accession_or_source_id=" ACC-1 ",
        specimen_role="Surgical_resection",
        anatomic_site="thyroid",
        laterality="",
        surgery_episode_id=100,
        encounter_synoptic_row_ix=1,
    )
    payload = specimen_master_fingerprint_input(**kwargs)
    h1 = specimen_master_fingerprint_sha256(**kwargs)
    h2 = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    assert h1 == h2


def test_tumor_focus_includes_master_fp_and_slot() -> None:
    master = specimen_master_fingerprint_sha256(
        research_id=1,
        source_system="pathology_synoptic_encounter",
        procedure_date_day="2020-01-15",
        accession_or_source_id="a",
        specimen_role="surgical_resection",
        anatomic_site="thyroid",
        laterality="",
        surgery_episode_id=1,
        encounter_synoptic_row_ix=1,
    )
    f1 = tumor_focus_fingerprint_sha256(
        master_fingerprint_sha256=master,
        synoptic_row_ix=1,
        tumor_index=1,
        site_text="right",
        histologic_type="PTC",
    )
    f2 = tumor_focus_fingerprint_sha256(
        master_fingerprint_sha256=master,
        synoptic_row_ix=1,
        tumor_index=2,
        site_text="right",
        histologic_type="PTC",
    )
    assert f1 != f2


def test_sql_fingerprint_matches_python_case() -> None:
    """DuckDB sha256(concat_ws(...)) matches Python helper for a fixed row."""
    parts_sql = duckdb.connect(":memory:").execute(
        """
        SELECT sha256(concat_ws('|',
          LOWER(TRIM(CAST(12 AS VARCHAR))),
          LOWER(TRIM('pathology_synoptic_encounter')),
          LOWER(TRIM('2020-01-15')),
          LOWER(TRIM('acc1')),
          LOWER(TRIM('surgical_resection')),
          LOWER(TRIM('thyroid')),
          LOWER(TRIM('')),
          LOWER(TRIM(CAST(100 AS VARCHAR))),
          LOWER(TRIM(CAST(1 AS VARCHAR)))
        )) AS h
        """
    ).fetchone()[0]
    parts_py = specimen_master_fingerprint_sha256(
        research_id=12,
        source_system="pathology_synoptic_encounter",
        procedure_date_day="2020-01-15",
        accession_or_source_id="acc1",
        specimen_role="surgical_resection",
        anatomic_site="thyroid",
        laterality="",
        surgery_episode_id=100,
        encounter_synoptic_row_ix=1,
    )
    assert parts_sql == parts_py


def test_distinct_surgery_dates_two_focus_rows() -> None:
    """Two encounters (different days) → two tumor focus rows for same patient."""
    from pathlib import Path

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA qa;")
    con.execute("""
    CREATE TABLE main.synoptic_tumor_long_v1 (
      synoptic_row_ix BIGINT, research_id BIGINT, surg_date VARCHAR, thyroid_procedure VARCHAR,
      tumor_index BIGINT, site VARCHAR, histologic_type VARCHAR
    );
    CREATE TABLE main.path_synoptics_encounter_qc_v1 AS
    SELECT * FROM (VALUES
      (1::BIGINT, '2020-01-15'::VARCHAR, 'PTC'::VARCHAR, DATE '2020-01-15', 1::BIGINT, 'tier1'::VARCHAR),
      (1::BIGINT, '2020-02-01'::VARCHAR, 'PTC'::VARCHAR, DATE '2020-02-01', 1::BIGINT, 'tier1'::VARCHAR)
    ) t(research_id, surg_date, tumor_1_histologic_type, surg_date_canonical, encounter_synoptic_row_ix, surg_date_parse_tier);
    INSERT INTO main.synoptic_tumor_long_v1 VALUES
      (1, 1, '2020-01-15', 'thyroidectomy', 1, 'R', 'PTC'),
      (2, 1, '2020-02-01', 'thyroidectomy', 1, 'L', 'PTC');
    CREATE TABLE main.surgery_pathology_linkage_v3 (
      research_id BIGINT, surgery_episode_id BIGINT, path_surgery_id VARCHAR, tumor_ordinal BIGINT,
      day_gap BIGINT, surg_lat VARCHAR, path_lat VARCHAR, n_candidates BIGINT,
      linkage_score DOUBLE, score_rank BIGINT, linkage_confidence_tier VARCHAR,
      linkage_reason_summary VARCHAR, analysis_eligible_link_flag BOOLEAN
    );
    INSERT INTO main.surgery_pathology_linkage_v3 VALUES
      (1, 100, 'A', 1, 0, 'R', 'R', 1, 0.9, 1, 'high_confidence', '', TRUE),
      (1, 200, 'B', 1, 0, 'L', 'L', 1, 0.9, 1, 'high_confidence', '', TRUE);
    CREATE TABLE main.fna_molecular_linkage_v3 (
      research_id BIGINT, fna_episode_id BIGINT, molecular_episode_id BIGINT,
      fna_date_native DATE, test_date_native DATE, day_gap BIGINT, laterality VARCHAR, platform VARCHAR,
      n_candidates BIGINT, linkage_score DOUBLE, score_rank BIGINT,
      linkage_confidence_tier VARCHAR, linkage_reason_summary VARCHAR, analysis_eligible_link_flag BOOLEAN
    );
    CREATE TABLE main.preop_surgery_linkage_v3 (
      research_id BIGINT, preop_episode_id BIGINT, preop_type VARCHAR, surgery_episode_id BIGINT,
      preop_date DATE, surgery_date DATE, day_gap BIGINT, score_rank BIGINT, n_candidates BIGINT,
      linkage_confidence_tier VARCHAR, linkage_reason_summary VARCHAR, analysis_eligible_link_flag BOOLEAN,
      preop_lat VARCHAR, surg_lat VARCHAR
    );
    CREATE TABLE main.molecular_test_episode_v2 (research_id BIGINT, molecular_episode_id BIGINT,
      platform VARCHAR, test_date_native DATE);
    """)
    ddl = (Path(__file__).resolve().parent.parent / "scripts/sql/138_specimen_fhir_layer_ddl.sql").read_text()
    con.execute(ddl)
    n = con.execute(
        "SELECT COUNT(DISTINCT specimen_focus_id) FROM main.specimen_tumor_focus_v1"
    ).fetchone()[0]
    assert n == 2
