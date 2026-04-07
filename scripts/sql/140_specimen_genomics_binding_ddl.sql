-- Specimen ↔ genomics assay binding (hardened v1)
-- Chain: molecular_test_episode_v2 → fna_molecular_linkage_v3 → preop_surgery_linkage_v3
--        → specimen_tumor_focus_v1; optional surgery_pathology_linkage_v3 for pathology keys.
--
-- linkage_confidence_tier: exact | high_confidence | plausible_review | unresolved_review
--
-- Optional blocks (@OPTIONAL_GENETIC_BODY / @OPTIONAL_THYROSEQ_BODY / unions) are stripped
-- by scripts/140_md_specimen_genomics_binding.py when source tables are absent.

-- DuckLake (MotherDuck): no PRIMARY KEY — logical key is review_queue_id.
CREATE TABLE IF NOT EXISTS qa.specimen_genomic_link_review_v1 (
  review_queue_id VARCHAR NOT NULL,
  genomic_assay_id VARCHAR NOT NULL,
  research_id BIGINT NOT NULL,
  molecular_episode_id BIGINT,
  reason_codes VARCHAR NOT NULL,
  conflict_summary VARCHAR,
  source_table VARCHAR,
  source_row_key VARCHAR,
  queued_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
  review_status VARCHAR NOT NULL DEFAULT 'open'
);

CREATE TABLE IF NOT EXISTS qa.val_specimen_genomic_binding_v1 (
  check_name VARCHAR NOT NULL,
  status VARCHAR NOT NULL,
  detail VARCHAR,
  measured_at TIMESTAMP NOT NULL
);

CREATE OR REPLACE TABLE main.specimen_genomic_assay_v1 AS
WITH mol AS (
  SELECT
    CAST(research_id AS BIGINT) AS research_id,
    CAST(molecular_episode_id AS BIGINT) AS molecular_episode_id,
    CAST(platform AS VARCHAR) AS platform,
    test_date_native
  FROM main.molecular_test_episode_v2
),
fm AS (
  SELECT
    research_id,
    molecular_episode_id,
    fna_episode_id,
    linkage_confidence_tier AS fm_tier_raw,
    linkage_score,
    score_rank,
    ROW_NUMBER() OVER (
      PARTITION BY research_id, molecular_episode_id
      ORDER BY score_rank NULLS LAST, linkage_score DESC NULLS LAST
    ) AS _rk
  FROM main.fna_molecular_linkage_v3
),
fm1 AS (SELECT * FROM fm WHERE _rk = 1),
ps AS (
  SELECT
    research_id,
    preop_episode_id,
    surgery_episode_id,
    linkage_confidence_tier AS preop_tier_raw,
    score_rank AS preop_rank,
    ROW_NUMBER() OVER (
      PARTITION BY research_id, preop_episode_id
      ORDER BY score_rank NULLS LAST
    ) AS _pr
  FROM main.preop_surgery_linkage_v3
),
ps1 AS (SELECT * FROM ps WHERE _pr = 1),
sp_path AS (
  SELECT
    research_id,
    surgery_episode_id,
    path_surgery_id,
    tumor_ordinal,
    linkage_confidence_tier AS pathology_tier_raw,
    ROW_NUMBER() OVER (
      PARTITION BY research_id, surgery_episode_id
      ORDER BY score_rank NULLS LAST, linkage_score DESC NULLS LAST
    ) AS _sr
  FROM main.surgery_pathology_linkage_v3
),
sp_path1 AS (SELECT * FROM sp_path WHERE _sr = 1),
spec_agg AS (
  SELECT
    research_id,
    surgery_episode_id,
    COUNT(DISTINCT specimen_focus_id) AS n_focus,
    COUNT(DISTINCT specimen_id) AS n_specimen,
    MIN(specimen_id) AS specimen_id_any,
    MIN(specimen_focus_id) AS focus_any
  FROM main.specimen_tumor_focus_v1
  GROUP BY 1, 2
),
spec_pick AS (
  SELECT
    research_id,
    surgery_episode_id,
    CASE WHEN n_specimen = 1 THEN specimen_id_any ELSE NULL END AS specimen_id,
    CASE WHEN n_focus = 1 THEN focus_any ELSE NULL END AS specimen_focus_id,
    n_focus,
    n_specimen
  FROM spec_agg
),
bound_core AS (
  SELECT
    m.research_id,
    m.molecular_episode_id,
    m.platform,
    m.test_date_native,
    fm1.fna_episode_id,
    fm1.fm_tier_raw,
    ps1.surgery_episode_id,
    ps1.preop_tier_raw,
    ph.path_surgery_id,
    ph.tumor_ordinal,
    ph.pathology_tier_raw,
    sp.specimen_id,
    sp.specimen_focus_id,
    sp.n_focus,
    sp.n_specimen,
    CASE
      WHEN fm1.research_id IS NULL THEN 'NO_FNA_MOLECULAR_LINK'
      WHEN ps1.research_id IS NULL THEN 'NO_PREOP_SURGERY_LINK'
      ELSE 'CHAIN_OK'
    END AS chain_reason,
    CASE
      WHEN sp.n_specimen > 1 THEN 'MULTIPLE_SPECIMEN_AMBIGUOUS'
      WHEN sp.n_focus > 1 THEN 'MULTIFOCAL_FOCUS_AMBIGUOUS'
      ELSE NULL
    END AS focus_ambiguity_reason
  FROM mol m
  LEFT JOIN fm1
    ON m.research_id = fm1.research_id
   AND m.molecular_episode_id = fm1.molecular_episode_id
  LEFT JOIN ps1
    ON fm1.research_id = ps1.research_id
   AND fm1.fna_episode_id = ps1.preop_episode_id
  LEFT JOIN spec_pick sp
    ON sp.research_id = ps1.research_id
   AND COALESCE(CAST(sp.surgery_episode_id AS VARCHAR), '')
     = COALESCE(CAST(ps1.surgery_episode_id AS VARCHAR), '')
  LEFT JOIN sp_path1 ph
    ON ph.research_id = ps1.research_id
   AND COALESCE(CAST(ph.surgery_episode_id AS VARCHAR), '')
     = COALESCE(CAST(ps1.surgery_episode_id AS VARCHAR), '')
),
tier_norm AS (
  SELECT
    research_id,
    molecular_episode_id,
    platform,
    test_date_native,
    fna_episode_id,
    fm_tier_raw,
    preop_tier_raw,
    surgery_episode_id,
    path_surgery_id,
    tumor_ordinal,
    pathology_tier_raw,
    specimen_id,
    specimen_focus_id,
    n_focus,
    n_specimen,
    chain_reason,
    focus_ambiguity_reason,
    CASE
      WHEN fm_tier_raw IS NULL THEN 'unresolved_review'
      WHEN fm_tier_raw IN ('weak', 'unlinked') THEN 'unresolved_review'
      WHEN fm_tier_raw = 'plausible' THEN 'plausible_review'
      WHEN fm_tier_raw = 'high_confidence' THEN 'high_confidence'
      WHEN fm_tier_raw = 'exact_match' THEN 'exact'
      ELSE 'unresolved_review'
    END AS fm_norm,
    'molecular_test_episode_v2' AS source_table,
    concat_ws(':', CAST(research_id AS VARCHAR), CAST(molecular_episode_id AS VARCHAR)) AS source_row_key,
    0::BIGINT AS payload_explode_ord,
    CAST(NULL AS VARCHAR) AS payload_field
  FROM bound_core
),
tier_adj AS (
  SELECT
    *,
    CASE
      WHEN chain_reason <> 'CHAIN_OK' THEN 'unresolved_review'
      WHEN fm_norm IN ('exact', 'high_confidence') AND focus_ambiguity_reason IS NOT NULL
        THEN 'plausible_review'
      WHEN fm_norm IN ('exact', 'high_confidence') AND specimen_id IS NULL
        THEN 'plausible_review'
      ELSE fm_norm
    END AS linkage_confidence_tier,
    concat_ws('|',
      NULLIF(chain_reason, 'CHAIN_OK'),
      focus_ambiguity_reason
    ) AS reason_fragment
  FROM tier_norm
),
molecular_rows AS (
  SELECT
    ('sga_' || sha256(concat_ws(
      '|', CAST(research_id AS VARCHAR), CAST(molecular_episode_id AS VARCHAR),
      'molecular_test_episode_v2', CAST(payload_explode_ord AS VARCHAR), COALESCE(payload_field, '')
    ))) AS genomic_assay_id,
    research_id,
    molecular_episode_id,
    platform,
    test_date_native,
    fna_episode_id,
    surgery_episode_id,
    path_surgery_id,
    tumor_ordinal,
    specimen_id,
    specimen_focus_id,
    CAST(fm_tier_raw AS VARCHAR) AS fm_tier,
    CAST(preop_tier_raw AS VARCHAR) AS preop_tier,
    CAST(pathology_tier_raw AS VARCHAR) AS pathology_linkage_tier_raw,
    linkage_confidence_tier,
    TRIM('|' FROM replace(
      concat_ws('|',
        NULLIF(chain_reason, 'CHAIN_OK'),
        focus_ambiguity_reason,
        CASE WHEN pathology_tier_raw IS NOT NULL AND pathology_tier_raw NOT IN ('exact_match', 'high_confidence')
          THEN 'PATHOLOGY_LINK_NON_STRONG' END
      ),
      '||', '|'
    )) AS linkage_reason_codes,
    CASE
      WHEN linkage_confidence_tier IN ('exact', 'high_confidence')
           AND focus_ambiguity_reason IS NULL
           AND chain_reason = 'CHAIN_OK'
        THEN 'A_exact_high'
      WHEN specimen_id IS NOT NULL AND chain_reason = 'CHAIN_OK'
        THEN 'B_specimen_only'
      WHEN chain_reason <> 'CHAIN_OK' OR fm_tier_raw IS NULL
        THEN 'D_unlinked'
      ELSE 'C_review'
    END AS binding_confidence_tier,
    (
      linkage_confidence_tier NOT IN ('exact', 'high_confidence')
      OR focus_ambiguity_reason IS NOT NULL
      OR chain_reason <> 'CHAIN_OK'
      OR (pathology_tier_raw IS NOT NULL AND pathology_tier_raw NOT IN ('exact_match', 'high_confidence'))
    ) AS review_flag,
    source_table,
    source_row_key,
    payload_explode_ord,
    payload_field,
    'molecular_test_episode_v2+fna_molecular_linkage_v3+preop_surgery_linkage_v3' AS binding_chain,
    current_timestamp AS materialized_at
  FROM tier_adj
)
-- @OPTIONAL_GENETIC_BODY_START
, gtx AS (
  SELECT *, row_number() OVER (ORDER BY research_id, test_platform) AS gt_rn
  FROM main.genetic_testing
),
gt0 AS (
  SELECT
    CAST(gt.research_id AS BIGINT) AS research_id,
    CAST(gt.gt_rn AS BIGINT) AS gt_rn,
    CAST(m.molecular_episode_id AS BIGINT) AS molecular_episode_id,
    CAST(gt.test_platform AS VARCHAR) AS platform,
    m.test_date_native,
    ROW_NUMBER() OVER (
      PARTITION BY gt.research_id, gt.gt_rn,
        LOWER(TRIM(COALESCE(CAST(gt.test_platform AS VARCHAR), '')))
      ORDER BY m.test_date_native NULLS LAST
    ) AS _rk
  FROM gtx gt
  INNER JOIN main.molecular_test_episode_v2 m
    ON gt.research_id = m.research_id
   AND LOWER(TRIM(COALESCE(CAST(gt.test_platform AS VARCHAR), '')))
       = LOWER(TRIM(COALESCE(CAST(m.platform AS VARCHAR), '')))
),
gt1 AS (SELECT * FROM gt0 WHERE _rk = 1),
genetic_bound AS (
  SELECT
    x.research_id,
    x.gt_rn,
    x.molecular_episode_id,
    x.platform,
    x.test_date_native,
    fm1.fna_episode_id,
    fm1.fm_tier_raw AS fm_tier_raw,
    ps1.preop_tier_raw AS preop_tier_raw,
    ps1.surgery_episode_id,
    ph.path_surgery_id,
    ph.tumor_ordinal,
    ph.pathology_tier_raw AS pathology_tier_raw,
    sp.specimen_id,
    sp.specimen_focus_id,
    sp.n_focus,
    sp.n_specimen,
    CASE
      WHEN fm1.research_id IS NULL THEN 'NO_FNA_MOLECULAR_LINK'
      WHEN ps1.research_id IS NULL THEN 'NO_PREOP_SURGERY_LINK'
      ELSE 'CHAIN_OK'
    END AS chain_reason,
    CASE
      WHEN sp.n_specimen > 1 THEN 'MULTIPLE_SPECIMEN_AMBIGUOUS'
      WHEN sp.n_focus > 1 THEN 'MULTIFOCAL_FOCUS_AMBIGUOUS'
      ELSE NULL
    END AS focus_ambiguity_reason
  FROM gt1 x
  LEFT JOIN fm1
    ON x.research_id = fm1.research_id
   AND x.molecular_episode_id = fm1.molecular_episode_id
  LEFT JOIN ps1
    ON fm1.research_id = ps1.research_id
   AND fm1.fna_episode_id = ps1.preop_episode_id
  LEFT JOIN spec_pick sp
    ON sp.research_id = ps1.research_id
   AND COALESCE(CAST(sp.surgery_episode_id AS VARCHAR), '')
     = COALESCE(CAST(ps1.surgery_episode_id AS VARCHAR), '')
  LEFT JOIN sp_path1 ph
    ON ph.research_id = ps1.research_id
   AND COALESCE(CAST(ph.surgery_episode_id AS VARCHAR), '')
     = COALESCE(CAST(ps1.surgery_episode_id AS VARCHAR), '')
),
genetic_tier AS (
  SELECT
    research_id,
    gt_rn,
    molecular_episode_id,
    platform,
    test_date_native,
    fna_episode_id,
    fm_tier_raw,
    preop_tier_raw,
    surgery_episode_id,
    path_surgery_id,
    tumor_ordinal,
    pathology_tier_raw,
    specimen_id,
    specimen_focus_id,
    n_focus,
    n_specimen,
    chain_reason,
    focus_ambiguity_reason,
    CASE
      WHEN fm_tier_raw IS NULL THEN 'unresolved_review'
      WHEN fm_tier_raw IN ('weak', 'unlinked') THEN 'unresolved_review'
      WHEN fm_tier_raw = 'plausible' THEN 'plausible_review'
      WHEN fm_tier_raw = 'high_confidence' THEN 'high_confidence'
      WHEN fm_tier_raw = 'exact_match' THEN 'exact'
      ELSE 'unresolved_review'
    END AS fm_norm,
    'genetic_testing' AS source_table,
    concat_ws(':', CAST(research_id AS VARCHAR), CAST(gt_rn AS VARCHAR)) AS source_row_key,
    0::BIGINT AS payload_explode_ord,
    CAST(NULL AS VARCHAR) AS payload_field
  FROM genetic_bound
),
genetic_adj AS (
  SELECT
    *,
    CASE
      WHEN chain_reason <> 'CHAIN_OK' THEN 'unresolved_review'
      WHEN fm_norm IN ('exact', 'high_confidence') AND focus_ambiguity_reason IS NOT NULL
        THEN 'plausible_review'
      WHEN fm_norm IN ('exact', 'high_confidence') AND specimen_id IS NULL
        THEN 'plausible_review'
      ELSE fm_norm
    END AS linkage_confidence_tier
  FROM genetic_tier
),
genetic_rows AS (
  SELECT
    ('sga_' || sha256(concat_ws(
      '|', CAST(research_id AS VARCHAR), CAST(gt_rn AS VARCHAR),
      CAST(molecular_episode_id AS VARCHAR), 'genetic_testing', '0', ''
    ))) AS genomic_assay_id,
    research_id,
    molecular_episode_id,
    platform,
    test_date_native,
    fna_episode_id,
    surgery_episode_id,
    path_surgery_id,
    tumor_ordinal,
    specimen_id,
    specimen_focus_id,
    CAST(fm_tier_raw AS VARCHAR) AS fm_tier,
    CAST(preop_tier_raw AS VARCHAR) AS preop_tier,
    CAST(pathology_tier_raw AS VARCHAR) AS pathology_linkage_tier_raw,
    linkage_confidence_tier,
    TRIM('|' FROM replace(
      concat_ws('|',
        NULLIF(chain_reason, 'CHAIN_OK'),
        focus_ambiguity_reason,
        'GENETIC_TESTING_EXCEL_ROW',
        CASE WHEN pathology_tier_raw IS NOT NULL AND pathology_tier_raw NOT IN ('exact_match', 'high_confidence')
          THEN 'PATHOLOGY_LINK_NON_STRONG' END
      ),
      '||', '|'
    )) AS linkage_reason_codes,
    CASE
      WHEN linkage_confidence_tier IN ('exact', 'high_confidence')
           AND focus_ambiguity_reason IS NULL
           AND chain_reason = 'CHAIN_OK'
        THEN 'A_exact_high'
      WHEN specimen_id IS NOT NULL AND chain_reason = 'CHAIN_OK'
        THEN 'B_specimen_only'
      WHEN chain_reason <> 'CHAIN_OK' OR fm_tier_raw IS NULL
        THEN 'D_unlinked'
      ELSE 'C_review'
    END AS binding_confidence_tier,
    (
      linkage_confidence_tier NOT IN ('exact', 'high_confidence')
      OR focus_ambiguity_reason IS NOT NULL
      OR chain_reason <> 'CHAIN_OK'
      OR (pathology_tier_raw IS NOT NULL AND pathology_tier_raw NOT IN ('exact_match', 'high_confidence'))
    ) AS review_flag,
    source_table,
    source_row_key,
    payload_explode_ord,
    payload_field,
    'genetic_testing+molecular_test_episode_v2+fna_molecular_linkage_v3+preop_surgery_linkage_v3' AS binding_chain,
    current_timestamp AS materialized_at
  FROM genetic_adj
)
-- @OPTIONAL_GENETIC_BODY_END
-- @OPTIONAL_THYROSEQ_BODY_START
, thy_pick AS (
  SELECT
    t.research_id,
    CAST(t.source_row_hash AS VARCHAR) AS source_row_hash,
    t.fusion_genes_json,
    t.allele_fractions_json,
    m.molecular_episode_id,
    CAST(m.platform AS VARCHAR) AS platform,
    m.test_date_native,
    COUNT(*) OVER (PARTITION BY t.research_id, t.source_row_hash) AS n_episode_candidates
  FROM main.thyroseq_molecular_enrichment t
  INNER JOIN main.molecular_test_episode_v2 m
    ON t.research_id = m.research_id
   AND (
     LOWER(COALESCE(m.platform, '')) LIKE '%thyroseq%'
     OR LOWER(COALESCE(m.platform, '')) = 'thyroseq'
   )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.research_id, t.source_row_hash
    ORDER BY m.test_date_native DESC NULLS LAST, m.molecular_episode_id DESC
  ) = 1
),
thy_fusion AS (
  SELECT
    p.research_id,
    p.molecular_episode_id,
    p.platform,
    p.test_date_native,
    p.source_row_hash,
    p.n_episode_candidates,
    ROW_NUMBER() OVER (
      PARTITION BY p.research_id, p.source_row_hash
      ORDER BY j.key
    ) AS payload_explode_ord,
    'fusion_genes_json' AS payload_field,
    j.value AS payload_json_fragment
  FROM thy_pick p,
       json_each(CAST(p.fusion_genes_json AS VARCHAR)) AS j
  WHERE p.fusion_genes_json IS NOT NULL
    AND LENGTH(TRIM(CAST(p.fusion_genes_json AS VARCHAR))) > 2
    AND json_valid(CAST(p.fusion_genes_json AS VARCHAR))
),
thy_allele AS (
  SELECT
    p.research_id,
    p.molecular_episode_id,
    p.platform,
    p.test_date_native,
    p.source_row_hash,
    p.n_episode_candidates,
    ROW_NUMBER() OVER (
      PARTITION BY p.research_id, p.source_row_hash
      ORDER BY j.key
    ) AS payload_explode_ord,
    'allele_fractions_json' AS payload_field,
    j.value AS payload_json_fragment
  FROM thy_pick p,
       json_each(CAST(p.allele_fractions_json AS VARCHAR)) AS j
  WHERE p.allele_fractions_json IS NOT NULL
    AND LENGTH(TRIM(CAST(p.allele_fractions_json AS VARCHAR))) > 2
    AND json_valid(CAST(p.allele_fractions_json AS VARCHAR))
),
thy_union AS (
  SELECT * FROM thy_fusion
  UNION ALL
  SELECT * FROM thy_allele
),
thy_bind AS (
  SELECT
    tu.research_id,
    tu.molecular_episode_id,
    tu.platform,
    tu.test_date_native,
    tu.source_row_hash,
    tu.payload_explode_ord,
    tu.payload_field,
    tu.n_episode_candidates,
    tu.payload_json_fragment,
    fm1.fna_episode_id,
    fm1.fm_tier_raw AS fm_tier_raw,
    ps1.preop_tier_raw AS preop_tier_raw,
    ps1.surgery_episode_id,
    ph.path_surgery_id,
    ph.tumor_ordinal,
    ph.pathology_tier_raw AS pathology_tier_raw,
    sp.specimen_id,
    sp.specimen_focus_id,
    sp.n_focus,
    sp.n_specimen,
    CASE
      WHEN fm1.research_id IS NULL THEN 'NO_FNA_MOLECULAR_LINK'
      WHEN ps1.research_id IS NULL THEN 'NO_PREOP_SURGERY_LINK'
      ELSE 'CHAIN_OK'
    END AS chain_reason,
    CASE
      WHEN sp.n_specimen > 1 THEN 'MULTIPLE_SPECIMEN_AMBIGUOUS'
      WHEN sp.n_focus > 1 THEN 'MULTIFOCAL_FOCUS_AMBIGUOUS'
      ELSE NULL
    END AS focus_ambiguity_reason
  FROM thy_union tu
  LEFT JOIN fm1
    ON tu.research_id = fm1.research_id
   AND tu.molecular_episode_id = fm1.molecular_episode_id
  LEFT JOIN ps1
    ON fm1.research_id = ps1.research_id
   AND fm1.fna_episode_id = ps1.preop_episode_id
  LEFT JOIN spec_pick sp
    ON sp.research_id = ps1.research_id
   AND COALESCE(CAST(sp.surgery_episode_id AS VARCHAR), '')
     = COALESCE(CAST(ps1.surgery_episode_id AS VARCHAR), '')
  LEFT JOIN sp_path1 ph
    ON ph.research_id = ps1.research_id
   AND COALESCE(CAST(ph.surgery_episode_id AS VARCHAR), '')
     = COALESCE(CAST(ps1.surgery_episode_id AS VARCHAR), '')
),
thy_tier AS (
  SELECT
    b.*,
    CASE
      WHEN fm_tier_raw IS NULL THEN 'unresolved_review'
      WHEN fm_tier_raw IN ('weak', 'unlinked') THEN 'unresolved_review'
      WHEN fm_tier_raw = 'plausible' THEN 'plausible_review'
      WHEN fm_tier_raw = 'high_confidence' THEN 'high_confidence'
      WHEN fm_tier_raw = 'exact_match' THEN 'exact'
      ELSE 'unresolved_review'
    END AS fm_norm
  FROM thy_bind b
),
thy_adj AS (
  SELECT
    *,
    CASE
      WHEN chain_reason <> 'CHAIN_OK' THEN 'unresolved_review'
      WHEN fm_norm IN ('exact', 'high_confidence') AND focus_ambiguity_reason IS NOT NULL
        THEN 'plausible_review'
      WHEN fm_norm IN ('exact', 'high_confidence') AND specimen_id IS NULL
        THEN 'plausible_review'
      WHEN n_episode_candidates > 1 THEN 'plausible_review'
      ELSE fm_norm
    END AS linkage_confidence_tier
  FROM thy_tier
),
thy_rows AS (
  SELECT
    ('sga_' || sha256(concat_ws(
      '|', CAST(research_id AS VARCHAR), CAST(molecular_episode_id AS VARCHAR),
      'thyroseq_json', source_row_hash, payload_field, CAST(payload_explode_ord AS VARCHAR),
      CAST(payload_json_fragment AS VARCHAR)
    ))) AS genomic_assay_id,
    research_id,
    molecular_episode_id,
    platform,
    test_date_native,
    fna_episode_id,
    surgery_episode_id,
    path_surgery_id,
    tumor_ordinal,
    specimen_id,
    specimen_focus_id,
    CAST(fm_tier_raw AS VARCHAR) AS fm_tier,
    CAST(preop_tier_raw AS VARCHAR) AS preop_tier,
    CAST(pathology_tier_raw AS VARCHAR) AS pathology_linkage_tier_raw,
    linkage_confidence_tier,
    TRIM('|' FROM replace(
      concat_ws('|',
        NULLIF(chain_reason, 'CHAIN_OK'),
        focus_ambiguity_reason,
        'THYROSEQ_JSON_EXPLODE',
        payload_field,
        CASE WHEN n_episode_candidates > 1 THEN 'THYROSEQ_MOLECULAR_EPISODE_AMBIGUOUS' END,
        CASE WHEN pathology_tier_raw IS NOT NULL AND pathology_tier_raw NOT IN ('exact_match', 'high_confidence')
          THEN 'PATHOLOGY_LINK_NON_STRONG' END
      ),
      '||', '|'
    )) AS linkage_reason_codes,
    CASE
      WHEN linkage_confidence_tier IN ('exact', 'high_confidence')
           AND focus_ambiguity_reason IS NULL
           AND chain_reason = 'CHAIN_OK'
           AND n_episode_candidates <= 1
        THEN 'A_exact_high'
      WHEN specimen_id IS NOT NULL AND chain_reason = 'CHAIN_OK'
        THEN 'B_specimen_only'
      WHEN chain_reason <> 'CHAIN_OK' OR fm_tier_raw IS NULL
        THEN 'D_unlinked'
      ELSE 'C_review'
    END AS binding_confidence_tier,
    (
      linkage_confidence_tier NOT IN ('exact', 'high_confidence')
      OR focus_ambiguity_reason IS NOT NULL
      OR chain_reason <> 'CHAIN_OK'
      OR n_episode_candidates > 1
      OR (pathology_tier_raw IS NOT NULL AND pathology_tier_raw NOT IN ('exact_match', 'high_confidence'))
    ) AS review_flag,
    'thyroseq_molecular_enrichment+json_each' AS source_table,
    concat_ws(':', CAST(research_id AS VARCHAR), source_row_hash, payload_field, CAST(payload_explode_ord AS VARCHAR)) AS source_row_key,
    payload_explode_ord,
    payload_field,
    'thyroseq_molecular_enrichment+json_each+molecular_test_episode_v2+fna_molecular_linkage_v3+preop_surgery_linkage_v3' AS binding_chain,
    current_timestamp AS materialized_at
  FROM thy_adj
)
-- @OPTIONAL_THYROSEQ_BODY_END
SELECT * FROM molecular_rows
-- @OPTIONAL_UNION_GENETIC
UNION ALL
SELECT * FROM genetic_rows
-- @OPTIONAL_UNION_THYROSEQ
UNION ALL
SELECT * FROM thy_rows
;

DELETE FROM qa.specimen_genomic_link_review_v1 WHERE 1 = 1;

INSERT INTO qa.specimen_genomic_link_review_v1 (
  review_queue_id, genomic_assay_id, research_id, molecular_episode_id,
  reason_codes, conflict_summary, source_table, source_row_key, queued_at, review_status
)
SELECT
  ('sglr_' || substring(sha256(concat_ws('|', genomic_assay_id, CAST(research_id AS VARCHAR))), 1, 40)) AS review_queue_id,
  genomic_assay_id,
  research_id,
  molecular_episode_id,
  COALESCE(nullif(linkage_reason_codes, ''), linkage_confidence_tier) AS reason_codes,
  concat_ws('; ',
    'tier=' || linkage_confidence_tier,
    'specimen_id=' || COALESCE(specimen_id, 'NULL'),
    'focus_id=' || COALESCE(specimen_focus_id, 'NULL'),
    'surgery_episode=' || COALESCE(CAST(surgery_episode_id AS VARCHAR), 'NULL')
  ) AS conflict_summary,
  source_table,
  source_row_key,
  current_timestamp,
  'open'
FROM main.specimen_genomic_assay_v1
WHERE linkage_confidence_tier IN ('plausible_review', 'unresolved_review')
   OR (
     linkage_reason_codes IS NOT NULL AND (
       linkage_reason_codes LIKE '%NO_FNA_MOLECULAR_LINK%'
       OR linkage_reason_codes LIKE '%NO_PREOP_SURGERY_LINK%'
       OR linkage_reason_codes LIKE '%MULTIFOCAL_FOCUS_AMBIGUOUS%'
       OR linkage_reason_codes LIKE '%MULTIPLE_SPECIMEN_AMBIGUOUS%'
       OR linkage_reason_codes LIKE '%THYROSEQ_MOLECULAR_EPISODE_AMBIGUOUS%'
       OR linkage_reason_codes LIKE '%PATHOLOGY_LINK_NON_STRONG%'
     )
   );
