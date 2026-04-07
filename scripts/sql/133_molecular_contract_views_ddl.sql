-- 133_molecular_contract_views_ddl.sql
-- Stable contract views for the governed molecular normalized layer (main.molecular_*).
-- Applied by scripts/117_md_contract_views.py after core episode contract DDL.
-- Analytic slice: rows missing superseded_by_molecular_result_id only.

-- ═══════════════════════════════════════════════════════════════════════════
-- molecular_results_contract_v — current (non-superseded) result envelopes
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW main.molecular_results_contract_v AS
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
FROM main.molecular_results
WHERE superseded_by_molecular_result_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- molecular_variant_contract_v — variant calls tied to live result rows only
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW main.molecular_variant_contract_v AS
SELECT
    v.molecular_variant_id,
    v.molecular_result_id,
    v.research_id,
    v.gene_symbol,
    v.transcript_id,
    v.genomic_hgvs,
    v.cdna_hgvs,
    v.protein_hgvs,
    v.canonical_hgvs,
    v.variant_class,
    v.allele_fraction,
    v.zygosity,
    v.interpretation_text,
    v.risk_call,
    v.parse_status,
    v.normalization_status,
    v.qc_flags,
    v.lineage_id,
    v.ingestion_ts,
    v.partner_gene_symbol,
    v.fusion_partner,
    v.raw_variant_token
FROM main.molecular_variant_long v
INNER JOIN main.molecular_results_contract_v r
    ON v.molecular_result_id = r.molecular_result_id;

-- ═══════════════════════════════════════════════════════════════════════════
-- molecular_qc_summary_v — parse / normalization / checksum coverage by source
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW main.molecular_qc_summary_v AS
SELECT
    COALESCE(source_table, '(unknown)') AS source_table,
    parse_status,
    normalization_status,
    COUNT(*)::BIGINT AS n_results,
    COUNT(DISTINCT research_id)::BIGINT AS n_patients,
    COUNT(*) FILTER (WHERE payload_checksum IS NULL)::BIGINT AS n_missing_payload_checksum,
    COUNT(*) FILTER (WHERE trim(COALESCE(CAST(lineage_id AS VARCHAR), '')) = '')::BIGINT AS n_blank_lineage_id,
    COUNT(*) FILTER (WHERE qc_flags IS NOT NULL)::BIGINT AS n_with_qc_flags
FROM main.molecular_results
WHERE superseded_by_molecular_result_id IS NULL
GROUP BY 1, 2, 3;

-- ═══════════════════════════════════════════════════════════════════════════
-- molecular_patient_rollup_v — patient-level metrics over live molecular layer
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW main.molecular_patient_rollup_v AS
SELECT
    r.research_id,
    COUNT(DISTINCT r.molecular_result_id)::BIGINT AS n_molecular_results,
    COUNT(DISTINCT v.molecular_variant_id)::BIGINT AS n_variant_calls,
    MAX(r.test_date_parsed) AS latest_test_date_parsed,
    MIN(r.ingestion_ts) AS earliest_result_ingestion_ts,
    MAX(r.ingestion_ts) AS latest_result_ingestion_ts,
    COUNT(DISTINCT r.source_table)::BIGINT AS n_distinct_source_tables,
    COUNT(DISTINCT r.assay_name)::BIGINT AS n_distinct_assay_names,
    MAX(r.panel_version) AS max_panel_version_observed,
    COUNT(*) FILTER (
        WHERE lower(trim(COALESCE(CAST(r.parse_status AS VARCHAR), '')))
            IN ('partial', 'failed', 'pending')
    )::BIGINT AS n_results_parse_review
FROM main.molecular_results_contract_v r
LEFT JOIN main.molecular_variant_long v
    ON v.molecular_result_id = r.molecular_result_id
GROUP BY r.research_id;
