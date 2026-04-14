-- 131_molecular_results_layer_ddl.sql
-- Governed normalized molecular assay + variant layer (MotherDuck + local DuckDB).
-- Idempotent: CREATE IF NOT EXISTS + seed uses anti-join (DuckLake has no PK/UNIQUE).
-- Does not modify ThyroSeq integration tables or molecular_testing.

-- ═══════════════════════════════════════════════════════════════════════════
-- Ingestion runs (optional metadata for append-only batches)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS main.molecular_ingestion_runs (
    ingestion_run_id VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    source_system VARCHAR,
    runner_script VARCHAR,
    status VARCHAR NOT NULL,
    notes VARCHAR
);

-- ═══════════════════════════════════════════════════════════════════════════
-- Assay / panel reference (curated dictionary; no overwrite of source tables)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS main.molecular_assay_dictionary (
    assay_key VARCHAR NOT NULL,
    assay_name VARCHAR NOT NULL,
    panel_version VARCHAR,
    platform VARCHAR,
    vendor VARCHAR,
    loinc_code VARCHAR,
    loinc_long_name VARCHAR,
    effective_from DATE,
    effective_to DATE,
    source_reference VARCHAR,
    inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════════════════════
-- Code crosswalk (exact source -> target; no fuzzy matching in DB)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS main.molecular_code_crosswalk (
    domain VARCHAR NOT NULL,
    source_code VARCHAR NOT NULL,
    target_code VARCHAR NOT NULL,
    mapping_status VARCHAR NOT NULL DEFAULT 'approved',
    notes VARCHAR,
    inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════════════════════
-- molecular_results: one row per assay/specimen result envelope
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS main.molecular_results (
    molecular_result_id VARCHAR NOT NULL,
    research_id INTEGER NOT NULL,
    source_patient_id VARCHAR,
    source_specimen_id VARCHAR,
    source_accession VARCHAR,
    assay_name VARCHAR,
    panel_version VARCHAR,
    platform VARCHAR,
    vendor VARCHAR,
    loinc_code VARCHAR,
    test_date_native VARCHAR,
    test_date_parsed DATE,
    interpretation_summary VARCHAR,
    risk_call VARCHAR,
    canonical_hgvs VARCHAR,
    raw_payload_json JSON,
    payload_checksum VARCHAR,
    parse_status VARCHAR NOT NULL DEFAULT 'pending',
    normalization_status VARCHAR NOT NULL DEFAULT 'raw',
    qc_flags JSON,
    lineage_id VARCHAR NOT NULL,
    ingestion_ts TIMESTAMP NOT NULL,
    ingestion_run_id VARCHAR,
    source_table VARCHAR,
    source_row_fingerprint VARCHAR,
    molecular_episode_id INTEGER,
    superseded_by_molecular_result_id VARCHAR
);

-- Note: DuckLake (MotherDuck production) does not support secondary indexes; filter on research_id
-- / lineage_id in queries or add indexes only in standalone file-based DuckDB if needed.

-- ═══════════════════════════════════════════════════════════════════════════
-- molecular_variant_long: one row per variant call (long / atomic)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS main.molecular_variant_long (
    molecular_variant_id VARCHAR NOT NULL,
    molecular_result_id VARCHAR NOT NULL,
    research_id INTEGER NOT NULL,
    gene_symbol VARCHAR,
    transcript_id VARCHAR,
    genomic_hgvs VARCHAR,
    cdna_hgvs VARCHAR,
    protein_hgvs VARCHAR,
    canonical_hgvs VARCHAR,
    variant_class VARCHAR NOT NULL,
    allele_fraction DOUBLE,
    zygosity VARCHAR,
    interpretation_text VARCHAR,
    risk_call VARCHAR,
    parse_status VARCHAR NOT NULL DEFAULT 'pending',
    normalization_status VARCHAR NOT NULL DEFAULT 'raw',
    qc_flags JSON,
    lineage_id VARCHAR NOT NULL,
    ingestion_ts TIMESTAMP NOT NULL,
    partner_gene_symbol VARCHAR,
    fusion_partner VARCHAR,
    raw_variant_token VARCHAR
);

-- ═══════════════════════════════════════════════════════════════════════════
-- Seed: canonical variant_class targets (SNV / INDEL / FUSION / CNV / OTHER)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO main.molecular_code_crosswalk (domain, source_code, target_code, mapping_status, notes)
SELECT t.domain, t.source_code, t.target_code, t.mapping_status, t.notes
FROM (
    VALUES
        ('variant_class', 'SNV', 'SNV', 'approved', 'canonical'),
        ('variant_class', 'snv', 'SNV', 'approved', 'case fold'),
        ('variant_class', 'POINT_MUTATION', 'SNV', 'approved', 'synonym'),
        ('variant_class', 'MISSENSE', 'SNV', 'approved', 'synonym'),
        ('variant_class', 'INDEL', 'INDEL', 'approved', 'canonical'),
        ('variant_class', 'indel', 'INDEL', 'approved', 'case fold'),
        ('variant_class', 'FUSION', 'FUSION', 'approved', 'canonical'),
        ('variant_class', 'fusion', 'FUSION', 'approved', 'case fold'),
        ('variant_class', 'REARRANGEMENT', 'FUSION', 'approved', 'synonym'),
        ('variant_class', 'CNV', 'CNV', 'approved', 'canonical'),
        ('variant_class', 'cnv', 'CNV', 'approved', 'case fold'),
        ('variant_class', 'AMPLIFICATION', 'CNV', 'approved', 'synonym'),
        ('variant_class', 'GAIN', 'CNV', 'approved', 'synonym'),
        ('variant_class', 'LOSS', 'CNV', 'approved', 'synonym'),
        ('variant_class', 'OTHER', 'OTHER', 'approved', 'canonical'),
        ('variant_class', 'other', 'OTHER', 'approved', 'case fold'),
        ('variant_class', 'UNKNOWN', 'OTHER', 'provisional', 'bucket')
) AS t(domain, source_code, target_code, mapping_status, notes)
WHERE NOT EXISTS (
    SELECT 1
    FROM main.molecular_code_crosswalk e
    WHERE e.domain = t.domain AND e.source_code = t.source_code
);

-- ═══════════════════════════════════════════════════════════════════════════
-- Seed: Afirma assay dictionary + exact string crosswalks (42_ingest_afirma)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO main.molecular_assay_dictionary (
    assay_key, assay_name, panel_version, platform, vendor, loinc_code, loinc_long_name,
    effective_from, effective_to, source_reference
)
SELECT
    t.assay_key, t.assay_name, t.panel_version, t.platform, t.vendor,
    t.loinc_code, t.loinc_long_name, t.effective_from, t.effective_to, t.source_reference
FROM (
    VALUES
        ('afirma_gec', 'Afirma Gene Expression Classifier', 'GEC', 'Afirma', 'Veracyte',
            CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS DATE), CAST(NULL AS DATE),
            'THYROID_2026 molecular layer seed'),
        ('afirma_gsc', 'Afirma Genomic Sequencing Classifier', 'GSC', 'Afirma', 'Veracyte',
            CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS DATE), CAST(NULL AS DATE),
            'THYROID_2026 molecular layer seed'),
        ('afirma_xpression_atlas', 'Afirma Xpression Atlas', 'Xpression Atlas', 'Afirma', 'Veracyte',
            CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS DATE), CAST(NULL AS DATE),
            'THYROID_2026 molecular layer seed'),
        ('afirma_combined', 'Afirma GEC+GSC', 'GEC+GSC', 'Afirma', 'Veracyte',
            CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS DATE), CAST(NULL AS DATE),
            'THYROID_2026 molecular layer seed'),
        ('thyroseq', 'ThyroSeq', CAST(NULL AS VARCHAR), 'ThyroSeq', 'ThyroSeq / institutional NGS',
            CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS DATE), CAST(NULL AS DATE),
            'THYROID_2026 molecular layer seed — contract dictionary coverage')
) AS t(
    assay_key, assay_name, panel_version, platform, vendor,
    loinc_code, loinc_long_name, effective_from, effective_to, source_reference
)
WHERE NOT EXISTS (
    SELECT 1 FROM main.molecular_assay_dictionary e WHERE e.assay_key = t.assay_key
);

INSERT INTO main.molecular_code_crosswalk (domain, source_code, target_code, mapping_status, notes)
SELECT t.domain, t.source_code, t.target_code, t.mapping_status, t.notes
FROM (
    VALUES
        ('afirma_assay_key', 'GEC', 'afirma_gec', 'approved', 'panel hint token'),
        ('afirma_assay_key', 'gec', 'afirma_gec', 'approved', 'case fold alias'),
        ('afirma_assay_key', 'GSC', 'afirma_gsc', 'approved', 'panel hint token'),
        ('afirma_assay_key', 'gsc', 'afirma_gsc', 'approved', 'case fold alias'),
        ('afirma_assay_key', 'GEC+GSC', 'afirma_combined', 'approved', 'combined classifier run'),
        ('afirma_assay_key', 'BOTH', 'afirma_combined', 'approved', 'synonym'),
        ('afirma_assay_key', 'Xpression Atlas', 'afirma_xpression_atlas', 'approved', 'XA panel'),
        ('afirma_assay_key', 'XA', 'afirma_xpression_atlas', 'approved', 'abbrev'),
        ('afirma_assay_key', 'XPRESSION_ATLAS', 'afirma_xpression_atlas', 'approved', 'abbrev'),
        ('afirma_call', 'Benign', 'benign', 'approved', 'GEC/GSC bucket'),
        ('afirma_call', 'benign', 'benign', 'approved', 'case variant'),
        ('afirma_call', 'BENIGN', 'benign', 'approved', 'case variant'),
        ('afirma_call', 'Suspicious', 'suspicious', 'approved', 'GEC/GSC bucket'),
        ('afirma_call', 'suspicious', 'suspicious', 'approved', 'case variant'),
        ('afirma_call', 'SUSPICIOUS', 'suspicious', 'approved', 'case variant'),
        ('afirma_call', 'Suspicious for malignancy', 'suspicious', 'approved', 'long form'),
        ('afirma_call', 'Indeterminate', 'indeterminate', 'approved', 'GEC/GSC bucket'),
        ('afirma_call', 'indeterminate', 'indeterminate', 'approved', 'case variant'),
        ('afirma_call', 'No result', 'no_result', 'approved', 'explicit none'),
        ('afirma_call', 'QNS', 'no_result', 'approved', 'quantity not sufficient'),
        ('afirma_call', 'Failed', 'failed', 'approved', 'assay failure'),
        ('afirma_call', 'Invalid', 'failed', 'approved', 'specimen/assay invalid'),
        ('afirma_risk_call', 'benign', 'benign', 'approved', 'harmonized risk_call'),
        ('afirma_risk_call', 'suspicious', 'suspicious', 'approved', 'harmonized risk_call'),
        ('afirma_risk_call', 'indeterminate', 'indeterminate', 'approved', 'harmonized risk_call'),
        ('afirma_risk_call', 'no_result', 'no_result', 'approved', 'harmonized risk_call'),
        ('afirma_risk_call', 'failed', 'failed', 'approved', 'harmonized risk_call')
) AS t(domain, source_code, target_code, mapping_status, notes)
WHERE NOT EXISTS (
    SELECT 1
    FROM main.molecular_code_crosswalk e
    WHERE e.domain = t.domain AND e.source_code = t.source_code
);

-- ═══════════════════════════════════════════════════════════════════════════
-- Contract / presentation views (stable names for Streamlit consumers)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW main.molecular_results_contract_v1 AS
SELECT
    molecular_result_id,
    research_id,
    source_patient_id,
    source_specimen_id,
    source_accession,
    assay_name,
    panel_version,
    platform,
    vendor,
    loinc_code,
    test_date_native,
    test_date_parsed,
    interpretation_summary,
    risk_call,
    canonical_hgvs,
    raw_payload_json,
    payload_checksum,
    parse_status,
    normalization_status,
    qc_flags,
    lineage_id,
    ingestion_ts,
    ingestion_run_id,
    source_table,
    source_row_fingerprint,
    molecular_episode_id,
    superseded_by_molecular_result_id
FROM main.molecular_results;

CREATE OR REPLACE VIEW main.molecular_variant_long_contract_v1 AS
SELECT
    molecular_variant_id,
    molecular_result_id,
    research_id,
    gene_symbol,
    transcript_id,
    genomic_hgvs,
    cdna_hgvs,
    protein_hgvs,
    canonical_hgvs,
    variant_class,
    allele_fraction,
    zygosity,
    interpretation_text,
    risk_call,
    parse_status,
    normalization_status,
    qc_flags,
    lineage_id,
    ingestion_ts,
    partner_gene_symbol,
    fusion_partner,
    raw_variant_token
FROM main.molecular_variant_long;

CREATE OR REPLACE VIEW main.molecular_results_enriched_v1 AS
SELECT
    r.*,
    (
        SELECT COUNT(*)::BIGINT
        FROM main.molecular_variant_long v
        WHERE v.molecular_result_id = r.molecular_result_id
    ) AS n_variants_long
FROM main.molecular_results r;

CREATE OR REPLACE VIEW main.molecular_normalization_review_v1 AS
SELECT *
FROM main.molecular_results
WHERE normalization_status IN ('quarantine', 'pending_review')
   OR parse_status IN ('partial', 'failed');
