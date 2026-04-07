-- 132_molecular_fact_lineage_views.sql
-- Unifies note-derived genetics molecular facts with governed molecular_results / variant_long.
-- Precedence: structured assay wins for dates, classifier summary, and variants. Matched note rows
-- remain as supporting_note_evidence (not dropped) with included_in_primary_analytics = false.
--
-- Dependencies: main.canonical_extracted_fact_long_v2, main.molecular_results,
--               main.molecular_variant_long, qa.manual_review_queue (genetics review overlay).

CREATE OR REPLACE VIEW main.molecular_fact_long_base_v AS
WITH mr_live AS (
    SELECT *
    FROM main.molecular_results
    WHERE superseded_by_molecular_result_id IS NULL
),
assay_family AS (
    SELECT
        r.*,
        CASE
            WHEN r.source_table = '42_afirma_structured_file'
                OR lower(COALESCE(r.vendor, '')) LIKE '%veracyte%'
                OR COALESCE(r.platform, '') = 'Afirma' THEN 'afirma'
            WHEN r.source_table = '41_thyroseq_excel_workbook'
                OR lower(COALESCE(r.assay_name, '')) LIKE '%thyroseq%' THEN 'thyroseq'
            ELSE 'other_molecular'
        END AS molecular_family
    FROM mr_live r
),
note_molecular AS (
    SELECT
        f.research_id,
        f.fact_id,
        f.note_row_id,
        f.entity_type,
        f.entity_value_raw,
        f.entity_value_norm,
        COALESCE(
            TRY_CAST(f.entity_date AS DATE),
            TRY_CAST(f.clin_note_date AS DATE),
            TRY_CAST(f.note_date AS DATE)
        ) AS event_date,
        f.evidence_span,
        f.extraction_method,
        f.extracted_at,
        f.fact_domain,
        f.linkage_anchor_family,
        f.inferred_surgery_episode_id,
        f.ep_distance_days,
        f.linkage_confidence,
        f.date_source_type,
        f.source_file_id AS source_workbook_or_table,
        'note_derived' AS fact_provenance_category,
        CASE
            WHEN f.entity_type ILIKE 'thyroseq%' THEN 'thyroseq'
            WHEN f.entity_type ILIKE 'afirma%' THEN 'afirma'
            WHEN f.entity_type IN (
                'mutation_panel',
                'molecular_risk_category',
                'serial_molecular_test',
                'bethesda_category',
                'fna_adequacy',
                'fna_nodule_location',
                'fna_cytology_detail'
            ) THEN 'thyroseq'
            ELSE 'other_molecular'
        END AS molecular_family,
        'note_genetics' AS source_stream,
        CAST(NULL AS VARCHAR) AS molecular_result_id,
        CAST(NULL AS VARCHAR) AS molecular_variant_id,
        CAST(NULL AS VARCHAR) AS test_date_native,
        CAST(NULL AS VARCHAR) AS risk_call,
        CAST(NULL AS VARCHAR) AS source_table,
        CAST(NULL AS VARCHAR) AS assay_lineage_id
    FROM main.canonical_extracted_fact_long_v2 f
    WHERE f.fact_domain = 'genetics'
      AND (
            f.entity_type ILIKE 'thyroseq%'
         OR f.entity_type ILIKE 'afirma%'
         OR f.entity_type IN (
                'mutation_panel',
                'molecular_risk_category',
                'serial_molecular_test',
                'bethesda_category',
                'fna_adequacy',
                'fna_nodule_location',
                'fna_cytology_detail'
            )
      )
),
assay_envelope_facts AS (
    SELECT
        r.research_id,
        r.molecular_result_id AS fact_id,
        CAST(NULL AS VARCHAR) AS note_row_id,
        CASE
            WHEN r.molecular_family = 'afirma' THEN 'afirma_result'
            WHEN r.molecular_family = 'thyroseq' THEN 'thyroseq_result'
            ELSE 'assay_envelope'
        END AS entity_type,
        COALESCE(r.interpretation_summary, r.risk_call, r.canonical_hgvs) AS entity_value_raw,
        COALESCE(r.interpretation_summary, r.risk_call, r.canonical_hgvs) AS entity_value_norm,
        r.test_date_parsed AS event_date,
        CAST(NULL AS VARCHAR) AS evidence_span,
        CAST('structured_import' AS VARCHAR) AS extraction_method,
        CAST(r.ingestion_ts AS VARCHAR) AS extracted_at,
        'genetics' AS fact_domain,
        'molecular' AS linkage_anchor_family,
        CAST(NULL AS BIGINT) AS inferred_surgery_episode_id,
        CAST(NULL AS BIGINT) AS ep_distance_days,
        CAST(NULL AS DOUBLE) AS linkage_confidence,
        CAST('assay_vendor_record' AS VARCHAR) AS date_source_type,
        r.source_table AS source_workbook_or_table,
        'assay_structured_import' AS fact_provenance_category,
        r.molecular_family,
        'molecular_results' AS source_stream,
        r.molecular_result_id,
        CAST(NULL AS VARCHAR) AS molecular_variant_id,
        r.test_date_native,
        r.risk_call,
        r.source_table,
        r.lineage_id AS assay_lineage_id
    FROM assay_family r
),
variant_facts AS (
    SELECT
        v.research_id,
        v.molecular_variant_id AS fact_id,
        CAST(NULL AS VARCHAR) AS note_row_id,
        CASE
            WHEN af.molecular_family = 'afirma' THEN 'afirma_xpression_atlas'
            ELSE 'thyroseq_mutations'
        END AS entity_type,
        COALESCE(
            v.protein_hgvs,
            v.canonical_hgvs,
            v.cdna_hgvs,
            v.genomic_hgvs,
            v.raw_variant_token,
            v.gene_symbol
        ) AS entity_value_raw,
        COALESCE(
            v.protein_hgvs,
            v.canonical_hgvs,
            v.cdna_hgvs,
            v.genomic_hgvs,
            v.raw_variant_token,
            v.gene_symbol
        ) AS entity_value_norm,
        af.test_date_parsed AS event_date,
        CAST(NULL AS VARCHAR) AS evidence_span,
        CAST('structured_import' AS VARCHAR) AS extraction_method,
        CAST(v.ingestion_ts AS VARCHAR) AS extracted_at,
        'genetics' AS fact_domain,
        'molecular' AS linkage_anchor_family,
        CAST(NULL AS BIGINT) AS inferred_surgery_episode_id,
        CAST(NULL AS BIGINT) AS ep_distance_days,
        CAST(NULL AS DOUBLE) AS linkage_confidence,
        CAST('assay_vendor_record' AS VARCHAR) AS date_source_type,
        af.source_table AS source_workbook_or_table,
        'assay_structured_import' AS fact_provenance_category,
        af.molecular_family,
        'molecular_variant_long' AS source_stream,
        v.molecular_result_id,
        v.molecular_variant_id,
        af.test_date_native,
        CAST(NULL AS VARCHAR) AS risk_call,
        af.source_table,
        v.lineage_id AS assay_lineage_id
    FROM main.molecular_variant_long v
    INNER JOIN assay_family af
        ON v.molecular_result_id = af.molecular_result_id
),
stacked_full AS (
    SELECT
        research_id,
        fact_id,
        note_row_id,
        entity_type,
        entity_value_raw,
        entity_value_norm,
        event_date,
        evidence_span,
        extraction_method,
        extracted_at,
        fact_domain,
        linkage_anchor_family,
        inferred_surgery_episode_id,
        ep_distance_days,
        linkage_confidence,
        date_source_type,
        source_workbook_or_table,
        fact_provenance_category,
        molecular_family,
        source_stream,
        molecular_result_id,
        molecular_variant_id,
        test_date_native,
        risk_call,
        source_table,
        assay_lineage_id
    FROM note_molecular
    UNION ALL
    SELECT
        research_id,
        fact_id,
        note_row_id,
        entity_type,
        entity_value_raw,
        entity_value_norm,
        event_date,
        evidence_span,
        extraction_method,
        extracted_at,
        fact_domain,
        linkage_anchor_family,
        inferred_surgery_episode_id,
        ep_distance_days,
        linkage_confidence,
        date_source_type,
        source_workbook_or_table,
        fact_provenance_category,
        molecular_family,
        source_stream,
        molecular_result_id,
        molecular_variant_id,
        test_date_native,
        risk_call,
        source_table,
        assay_lineage_id
    FROM assay_envelope_facts
    UNION ALL
    SELECT
        research_id,
        fact_id,
        note_row_id,
        entity_type,
        entity_value_raw,
        entity_value_norm,
        event_date,
        evidence_span,
        extraction_method,
        extracted_at,
        fact_domain,
        linkage_anchor_family,
        inferred_surgery_episode_id,
        ep_distance_days,
        linkage_confidence,
        date_source_type,
        source_workbook_or_table,
        fact_provenance_category,
        molecular_family,
        source_stream,
        molecular_result_id,
        molecular_variant_id,
        test_date_native,
        risk_call,
        source_table,
        assay_lineage_id
    FROM variant_facts
),
pair AS (
    SELECT DISTINCT
        n.fact_id AS note_fact_id,
        a.molecular_result_id,
        n.research_id,
        n.molecular_family,
        n.event_date AS note_event_date,
        a.event_date AS assay_event_date,
        abs(date_diff('day', n.event_date, a.event_date)) AS date_gap_days
    FROM note_molecular n
    INNER JOIN assay_envelope_facts a
        ON n.research_id = a.research_id
        AND n.molecular_family = a.molecular_family
        AND n.molecular_family IN ('thyroseq', 'afirma')
        AND n.event_date IS NOT NULL
        AND a.event_date IS NOT NULL
        AND abs(date_diff('day', n.event_date, a.event_date)) <= 21
),
note_has_assay AS (
    SELECT
        note_fact_id,
        BOOL_OR(TRUE) AS has_structured_assay_match,
        MIN(date_gap_days) AS best_gap_days,
        arg_min(molecular_result_id, date_gap_days) AS best_molecular_result_id
    FROM pair
    GROUP BY note_fact_id
),
assay_has_note AS (
    SELECT
        molecular_result_id,
        BOOL_OR(TRUE) AS has_note_support,
        MIN(date_gap_days) AS best_note_gap_days
    FROM pair
    GROUP BY molecular_result_id
)
SELECT
    s.*,
    nh.has_structured_assay_match AS note_matched_structured_assay,
    nh.best_gap_days AS note_to_assay_gap_days,
    nh.best_molecular_result_id AS matched_molecular_result_id,
    hn.has_note_support AS assay_has_note_support,
    hn.best_note_gap_days AS assay_to_note_gap_days,
    CASE
        WHEN s.source_stream = 'note_genetics' AND COALESCE(nh.has_structured_assay_match, FALSE)
            THEN 'supporting_note_evidence'
        WHEN s.source_stream IN ('molecular_results', 'molecular_variant_long')
            AND COALESCE(hn.has_note_support, FALSE)
            THEN 'primary_assay_record'
        WHEN s.source_stream = 'note_genetics'
            THEN 'standalone_note'
        ELSE 'standalone_assay'
    END AS record_role,
    CASE
        WHEN s.source_stream = 'note_genetics' AND COALESCE(nh.has_structured_assay_match, FALSE)
            THEN FALSE
        ELSE TRUE
    END AS included_in_primary_analytics,
    CAST(
        CASE
            WHEN s.source_stream = 'note_genetics' AND COALESCE(nh.has_structured_assay_match, FALSE)
                THEN 'structured_assay_supersedes_note_row'
            WHEN s.source_stream IN ('molecular_results', 'molecular_variant_long')
                AND COALESCE(hn.has_note_support, FALSE)
                THEN 'note_retained_as_supporting_context'
            ELSE NULL
        END AS VARCHAR
    ) AS precedence_rationale
FROM stacked_full s
LEFT JOIN note_has_assay nh ON s.fact_id = nh.note_fact_id AND s.source_stream = 'note_genetics'
LEFT JOIN assay_has_note hn
    ON s.molecular_result_id = hn.molecular_result_id
    AND s.source_stream IN ('molecular_results', 'molecular_variant_long');

CREATE OR REPLACE VIEW main.molecular_fact_long_v AS
WITH review AS (
    SELECT
        research_id,
        domain,
        verification_status AS genetics_review_status,
        reviewer,
        reviewed_at
    FROM (
        SELECT
            research_id,
            domain,
            verification_status,
            reviewer,
            reviewed_at,
            ROW_NUMBER() OVER (
                PARTITION BY research_id
                ORDER BY reviewed_at DESC NULLS LAST, loaded_at DESC
            ) AS rn
        FROM qa.manual_review_queue
        WHERE lower(COALESCE(domain, '')) = 'genetics'
    ) x
    WHERE rn = 1
)
SELECT
    b.research_id,
    b.fact_id,
    b.note_row_id,
    b.entity_type,
    b.entity_value_raw,
    b.entity_value_norm,
    b.event_date,
    b.evidence_span,
    b.extraction_method,
    b.extracted_at,
    b.fact_domain,
    b.linkage_anchor_family,
    b.inferred_surgery_episode_id,
    b.ep_distance_days,
    b.linkage_confidence,
    b.date_source_type,
    b.source_workbook_or_table,
    b.fact_provenance_category,
    b.molecular_family,
    b.source_stream,
    b.molecular_result_id,
    b.molecular_variant_id,
    b.test_date_native,
    b.risk_call,
    b.source_table,
    b.assay_lineage_id,
    b.note_matched_structured_assay,
    b.note_to_assay_gap_days,
    b.matched_molecular_result_id,
    b.assay_has_note_support,
    b.assay_to_note_gap_days,
    b.record_role,
    b.included_in_primary_analytics,
    b.precedence_rationale,
    r.genetics_review_status,
    r.reviewer AS genetics_reviewer,
    r.reviewed_at AS genetics_reviewed_at,
    CASE
        WHEN r.genetics_review_status ILIKE '%verified%'
            OR r.genetics_review_status ILIKE '%approved%'
            THEN 'manual_adjudicated_effective'
        WHEN r.genetics_review_status IS NOT NULL
            THEN 'manual_review_present'
        ELSE NULL
    END AS human_review_overlay
FROM main.molecular_fact_long_base_v b
LEFT JOIN review r ON b.research_id = r.research_id;

CREATE OR REPLACE VIEW main.molecular_results_unified_v AS
SELECT * FROM main.molecular_fact_long_v;

CREATE OR REPLACE VIEW main.molecular_fact_lineage_qa_duplicate_candidates_v AS
SELECT
    p.note_fact_id,
    p.molecular_result_id,
    p.research_id,
    p.molecular_family,
    p.note_event_date,
    p.assay_event_date,
    p.date_gap_days,
    n.entity_type AS note_entity_type,
    substring(COALESCE(n.entity_value_raw, ''), 1, 200) AS note_value_excerpt,
    a.entity_type AS assay_entity_type,
    substring(COALESCE(a.entity_value_raw, ''), 1, 200) AS assay_value_excerpt
FROM (
    SELECT DISTINCT
        n.fact_id AS note_fact_id,
        a.molecular_result_id,
        n.research_id,
        n.molecular_family,
        n.event_date AS note_event_date,
        a.event_date AS assay_event_date,
        abs(date_diff('day', n.event_date, a.event_date)) AS date_gap_days
    FROM main.molecular_fact_long_base_v n
    INNER JOIN main.molecular_fact_long_base_v a
        ON n.source_stream = 'note_genetics'
        AND a.source_stream = 'molecular_results'
        AND n.research_id = a.research_id
        AND n.molecular_family = a.molecular_family
        AND n.molecular_family IN ('thyroseq', 'afirma')
        AND n.event_date IS NOT NULL
        AND a.event_date IS NOT NULL
        AND abs(date_diff('day', n.event_date, a.event_date)) <= 21
) p
LEFT JOIN main.molecular_fact_long_base_v n
    ON p.note_fact_id = n.fact_id AND n.source_stream = 'note_genetics'
LEFT JOIN main.molecular_fact_long_base_v a
    ON p.molecular_result_id = a.molecular_result_id AND a.source_stream = 'molecular_results';
