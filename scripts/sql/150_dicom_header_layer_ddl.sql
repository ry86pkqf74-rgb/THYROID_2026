-- 150_dicom_header_layer_ddl.sql — governed tables for flattened DICOM header ingest (v1)
-- Companion: scripts/150_ingest_dicom_headers.py
-- Apply with script --execute-ddl --write-db (operator-confirmed environments only).

CREATE TABLE IF NOT EXISTS dicom_header_ingestion_provenance_v1 (
    source_file VARCHAR,
    source_row_number INTEGER,
    raw_payload_json VARCHAR,
    row_fingerprint_sha256 VARCHAR,
    ingestion_ts TIMESTAMP,
    ingestion_run_id VARCHAR,
    parse_status VARCHAR,
    qc_flags_json VARCHAR,
    study_instance_uid VARCHAR,
    series_instance_uid VARCHAR,
    sop_instance_uid VARCHAR
);

CREATE TABLE IF NOT EXISTS dicom_study_header_v1 (
    study_instance_uid VARCHAR PRIMARY KEY,
    study_date_raw VARCHAR,
    study_date_normalized VARCHAR,
    accession_number_raw VARCHAR,
    accession_norm VARCHAR,
    study_description_raw VARCHAR,
    institution_name_raw VARCHAR,
    patient_id_raw VARCHAR,
    research_id_explicit BIGINT,
    modality_summary VARCHAR,
    body_part_examined_summary VARCHAR,
    n_source_rows INTEGER,
    qc_flags_json VARCHAR,
    ingestion_run_id VARCHAR,
    ingestion_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dicom_series_header_v1 (
    series_instance_uid VARCHAR PRIMARY KEY,
    study_instance_uid VARCHAR,
    series_date_raw VARCHAR,
    series_date_normalized VARCHAR,
    modality_raw VARCHAR,
    body_part_examined_raw VARCHAR,
    series_description_raw VARCHAR,
    institution_name_raw VARCHAR,
    patient_id_raw VARCHAR,
    n_source_rows INTEGER,
    qc_flags_json VARCHAR,
    ingestion_run_id VARCHAR,
    ingestion_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dicom_imaging_link_exact_v1 (
    link_id VARCHAR PRIMARY KEY,
    study_instance_uid VARCHAR,
    series_instance_uid VARCHAR,
    linkage_tier VARCHAR,
    research_id BIGINT,
    imaging_exam_id VARCHAR,
    imaging_nodule_id VARCHAR,
    specimen_id VARCHAR,
    accession_norm VARCHAR,
    date_concordance_flag BOOLEAN,
    candidate_digest VARCHAR,
    ingestion_run_id VARCHAR,
    ingestion_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dicom_link_review_queue_v1 (
    review_id VARCHAR PRIMARY KEY,
    reason_code VARCHAR,
    source_file VARCHAR,
    study_instance_uid VARCHAR,
    series_instance_uid VARCHAR,
    accession_raw VARCHAR,
    accession_norm VARCHAR,
    study_date_normalized VARCHAR,
    series_date_normalized VARCHAR,
    modality_raw VARCHAR,
    candidate_research_ids_json VARCHAR,
    candidate_imaging_exam_ids_json VARCHAR,
    candidate_specimen_ids_json VARCHAR,
    conflict_note VARCHAR,
    ingestion_run_id VARCHAR,
    created_ts TIMESTAMP
);
