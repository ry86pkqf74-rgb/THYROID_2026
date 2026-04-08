"""Offline tests: specimen fingerprint parity + multi-row synoptic isolation logic."""

from __future__ import annotations

import hashlib
import json

import duckdb

from utils.specimen_fingerprint import (
    specimen_master_fingerprint_input,
    specimen_master_fingerprint_sha256,
    tumor_focus_fingerprint_sha256,
)


def test_specimen_master_fingerprint_stable_normalization() -> None:
    payload = specimen_master_fingerprint_input(
        research_id=" 12 ",
        source_system="PATHOLOGY_SYNOPTIC_ENCOUNTER",
        procedure_date_day="2020-01-15",
        accession_or_source_id=" ACC-1 ",
        specimen_role="Surgical_resection",
        anatomic_site="thyroid",
        laterality="",
        surgery_episode_id=100,
        encounter_synoptic_row_ix=1,
        synoptic_row_ix=9,
    )
    h1 = specimen_master_fingerprint_sha256(
        research_id=" 12 ",
        source_system="PATHOLOGY_SYNOPTIC_ENCOUNTER",
        procedure_date_day="2020-01-15",
        accession_or_source_id=" ACC-1 ",
        specimen_role="Surgical_resection",
        anatomic_site="thyroid",
        laterality="",
        surgery_episode_id=100,
        encounter_synoptic_row_ix=1,
        synoptic_row_ix=9,
    )
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
        synoptic_row_ix=1,
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
    sql_row = duckdb.connect(":memory:").execute(
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
          LOWER(TRIM(CAST(1 AS VARCHAR))),
          LOWER(TRIM(CAST(1 AS VARCHAR)))
        )) AS h
        """
    ).fetchone()
    assert sql_row is not None
    parts_sql = sql_row[0]
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
        synoptic_row_ix=1,
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
      (2, 1, '2020-02-01', 'thyroidectomy', 2, 'L', 'PTC');
    CREATE TABLE main.surgery_pathology_linkage_v3 (
      research_id BIGINT, surgery_episode_id BIGINT, path_surgery_id VARCHAR, tumor_ordinal BIGINT,
      day_gap BIGINT, surg_lat VARCHAR, path_lat VARCHAR, n_candidates BIGINT,
      linkage_score DOUBLE, score_rank BIGINT, linkage_confidence_tier VARCHAR,
      linkage_reason_summary VARCHAR, analysis_eligible_link_flag BOOLEAN
    );
    INSERT INTO main.surgery_pathology_linkage_v3 VALUES
      (1, 100, 'A', 1, 0, 'R', 'R', 1, 0.9, 1, 'high_confidence', '', TRUE),
      (1, 200, 'B', 2, 0, 'L', 'L', 1, 0.9, 1, 'high_confidence', '', TRUE);
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
    CREATE TABLE main.tumor_episode_master_v2 (
      research_id BIGINT, surgery_episode_id BIGINT, surgery_date VARCHAR
    );
    INSERT INTO main.tumor_episode_master_v2 VALUES
      (1, 100, '2020-01-15'),
      (1, 200, '2020-02-01');
    """)
    root = Path(__file__).resolve().parent.parent
    ident = (
        root / "scripts/sql/139_specimen_identity_layer_ddl.sql"
    ).read_text(encoding="utf-8").replace("__BUILD_RUN_ID__", "pytest_fhir_tail")
    tail = (root / "scripts/sql/138_specimen_fhir_tail_ddl.sql").read_text(encoding="utf-8")
    con.execute(ident)
    con.execute(tail)
    nf = con.execute(
        "SELECT COUNT(DISTINCT specimen_focus_id) FROM main.specimen_tumor_focus_v1"
    ).fetchone()
    assert nf is not None
    n = nf[0]
    assert n == 2

    ne = con.execute("SELECT COUNT(*) FROM main.fhir_episode_of_care_v1").fetchone()
    assert ne is not None
    n_eoc = ne[0]
    assert n_eoc == 2

    sr = con.execute(
        "SELECT json_extract_string(resource_json, '$.subject.reference') FROM main.fhir_specimen_v1 LIMIT 1"
    ).fetchone()
    assert sr is not None
    subj = sr[0]
    assert subj.startswith("Patient/")
    assert "Patient/Patient/" not in subj

    br = con.execute(
        "SELECT cast(bundle_json AS VARCHAR) FROM main.fhir_bundle_specimen_export_v1 ORDER BY specimen_id LIMIT 1"
    ).fetchone()
    assert br is not None
    raw = br[0]
    bundle = json.loads(raw)
    assert bundle["resourceType"] == "Bundle"
    assert len(bundle["entry"]) == 4
    types = [e["resource"]["resourceType"] for e in bundle["entry"]]
    assert types == ["Specimen", "Procedure", "Encounter", "EpisodeOfCare"]
    proc_coll = bundle["entry"][0]["resource"]["collection"]["procedure"]["reference"]
    proc_id = bundle["entry"][1]["resource"]["id"]
    assert proc_coll == f"Procedure/{proc_id}"
    enc_ref = bundle["entry"][1]["resource"]["encounter"]["reference"]
    enc_id = bundle["entry"][2]["resource"]["id"]
    assert enc_ref == f"Encounter/{enc_id}"
    eoc_ref = bundle["entry"][2]["resource"]["episodeOfCare"][0]["reference"]
    eoc_id = bundle["entry"][3]["resource"]["id"]
    assert eoc_ref == f"EpisodeOfCare/{eoc_id}"
