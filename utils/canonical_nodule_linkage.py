"""
Deterministic thyroid nodule cross-domain linkage SQL (study layer).

Anchors on ``imaging_nodule_master_v1``; reuses multimodal imaging↔FNA rows and v3
episode linkages (``scripts/129_imaging_fna_linkage_mm_v1.py``,
``scripts/49_enhanced_linkage_v3.py``). Specimen-key and laterality rules follow
the existing ``imaging_fna_linkage_mm_v1`` primary-link policy.
"""
from __future__ import annotations

_MANUAL_REVIEW_PREDICATE = """
(
    (n_candidates_for_nodule IS NOT NULL AND n_candidates_for_nodule > 1
         AND NOT COALESCE(is_primary_link, FALSE))
    OR (fna_molecular_n_candidates IS NOT NULL AND fna_molecular_n_candidates > 1)
    OR (preop_surgery_n_candidates IS NOT NULL AND preop_surgery_n_candidates > 1)
    OR (surgery_pathology_n_candidates IS NOT NULL AND surgery_pathology_n_candidates > 1)
    OR (
        surgery_episode_id_linked IS NOT NULL
        AND earliest_postfna_surgery_episode_id IS NOT NULL
        AND CAST(surgery_episode_id_linked AS INTEGER)
            <> CAST(earliest_postfna_surgery_episode_id AS INTEGER)
    )
    OR (
        bethesda_category IN (3, 4)
        AND molecular_episode_id IS NULL
    )
)
"""

# CTE chain only (no trailing SELECT). Final SELECT added by canonical_nodule_linkage_sql().
_CANONICAL_LINKAGE_CTES = r"""
spine AS (
    SELECT
        CAST(i.research_id AS BIGINT) AS research_id,
        CAST(i.nodule_id AS VARCHAR) AS canonical_nodule_id,
        CAST(i.exam_id AS VARCHAR) AS imaging_exam_id,
        i.exam_date AS us_exam_date,
        TRY_CAST(i.nodule_number AS INTEGER) AS nodule_ordinal,
        i.laterality AS lobe_laterality,
        i.location_raw AS source_nodule_site_raw,
        i.max_dimension_cm AS us_max_size_cm,
        COALESCE(
            i.tirads_category,
            CASE COALESCE(i.tirads_reported, i.tirads_acr_recalculated)
                WHEN 1 THEN 'TR1'
                WHEN 2 THEN 'TR2'
                WHEN 3 THEN 'TR3'
                WHEN 4 THEN 'TR4'
                WHEN 5 THEN 'TR5'
                WHEN 6 THEN 'TR5'
                ELSE NULL
            END
        ) AS acr_tirads,
        i.source_table AS imaging_source_table
    FROM imaging_nodule_master_v1 i
    WHERE i.exam_date IS NOT NULL
),
ifna AS (
    SELECT
        s.research_id,
        s.canonical_nodule_id,
        s.imaging_exam_id,
        s.us_exam_date,
        s.nodule_ordinal,
        s.lobe_laterality,
        s.source_nodule_site_raw,
        s.us_max_size_cm,
        s.acr_tirads,
        s.imaging_source_table,
        ifn.fna_event_date,
        ifn.fna_episode_id,
        ifn.match_path,
        ifn.specimen_match_flag,
        ifn.n_candidates_for_nodule,
        ifn.is_primary_link,
        ifn.ordinal_in_nodule
    FROM spine s
    LEFT JOIN imaging_fna_linkage_mm_v1 ifn
      ON s.research_id = ifn.research_id
     AND s.canonical_nodule_id = ifn.nodule_id
     AND ifn.is_primary_link IS TRUE
),
fna_row AS (
    SELECT
        x.*,
        fe.fna_date_native AS fna_date,
        fe.bethesda_category,
        fe.bethesda_raw,
        fe.specimen_site_raw AS fna_specimen_site_raw,
        fe.laterality AS fna_laterality
    FROM ifna x
    LEFT JOIN fna_episode_master_v2 fe
      ON x.fna_episode_id IS NOT NULL
     AND CAST(x.research_id AS BIGINT) = CAST(fe.research_id AS BIGINT)
     AND CAST(x.fna_episode_id AS BIGINT) = CAST(fe.fna_episode_id AS BIGINT)
),
fmol AS (
    SELECT
        fr.*,
        fmol.molecular_episode_id,
        fmol.linkage_confidence_tier AS fna_molecular_linkage_tier,
        fmol.linkage_score AS fna_molecular_linkage_score,
        fmol.n_candidates AS fna_molecular_n_candidates,
        mol.test_date_native AS molecular_test_date,
        mol.platform AS molecular_platform,
        mol.result AS molecular_result_raw,
        mol.overall_result_class AS molecular_result_class
    FROM fna_row fr
    LEFT JOIN (
        SELECT * FROM fna_molecular_linkage_v3 WHERE score_rank = 1
    ) fmol
      ON fr.fna_episode_id IS NOT NULL
     AND CAST(fr.research_id AS BIGINT) = CAST(fmol.research_id AS BIGINT)
     AND CAST(fr.fna_episode_id AS BIGINT) = CAST(fmol.fna_episode_id AS BIGINT)
    LEFT JOIN molecular_test_episode_v2 mol
      ON fmol.molecular_episode_id IS NOT NULL
     AND CAST(fmol.research_id AS BIGINT) = CAST(mol.research_id AS BIGINT)
     AND CAST(fmol.molecular_episode_id AS BIGINT) = CAST(mol.molecular_episode_id AS BIGINT)
),
preop AS (
    SELECT
        fm.*,
        ps.surgery_episode_id AS surgery_episode_id_linked,
        ps.surgery_date AS surgery_date_index,
        ps.linkage_confidence_tier AS preop_surgery_linkage_tier,
        ps.linkage_score AS preop_surgery_linkage_score,
        ps.n_candidates AS preop_surgery_n_candidates
    FROM fmol fm
    LEFT JOIN (
        SELECT * FROM preop_surgery_linkage_v3
        WHERE score_rank = 1 AND preop_type = 'fna'
    ) ps
      ON fm.fna_episode_id IS NOT NULL
     AND CAST(fm.research_id AS BIGINT) = CAST(ps.research_id AS BIGINT)
     AND CAST(fm.fna_episode_id AS BIGINT) = CAST(ps.preop_episode_id AS BIGINT)
),
spath AS (
    SELECT
        p.*,
        sp.path_surgery_id,
        sp.tumor_ordinal AS tumor_ordinal_linked,
        sp.linkage_confidence_tier AS surgery_pathology_linkage_tier,
        sp.linkage_score AS surgery_pathology_linkage_score,
        sp.n_candidates AS surgery_pathology_n_candidates
    FROM preop p
    LEFT JOIN (
        SELECT * FROM surgery_pathology_linkage_v3 WHERE score_rank = 1
    ) sp
      ON p.surgery_episode_id_linked IS NOT NULL
     AND CAST(p.research_id AS BIGINT) = CAST(sp.research_id AS BIGINT)
     AND CAST(p.surgery_episode_id_linked AS BIGINT) = CAST(sp.surgery_episode_id AS BIGINT)
),
tumor_joined AS (
    SELECT
        spth.*,
        tum.primary_histology AS final_histology,
        tum.t_stage AS t_stage_final,
        tum.n_stage AS n_stage_final,
        tum.margin_status AS margins_final,
        tum.tumor_size_cm * 10.0 AS tumor_size_mm_path,
        COALESCE(tum.multifocality_flag, tum.number_of_tumors > 1) AS multifocal_flag
    FROM spath spth
    LEFT JOIN tumor_episode_master_v2 tum
      ON spth.surgery_episode_id_linked IS NOT NULL
     AND CAST(spth.research_id AS BIGINT) = CAST(tum.research_id AS BIGINT)
     AND CAST(spth.surgery_episode_id_linked AS BIGINT) = CAST(tum.surgery_episode_id AS BIGINT)
     AND CAST(spth.tumor_ordinal_linked AS INTEGER) = CAST(tum.tumor_ordinal AS INTEGER)
),
earliest_surg AS (
    SELECT
        tj.*,
        (
            SELECT MIN(o.surgery_episode_id)
            FROM operative_episode_detail_v2 o
            WHERE CAST(o.research_id AS BIGINT) = tj.research_id
              AND tj.fna_date IS NOT NULL
              AND o.surgery_date_native IS NOT NULL
              AND o.surgery_date_native >= tj.fna_date
        ) AS earliest_postfna_surgery_episode_id
    FROM tumor_joined tj
)
"""


def canonical_nodule_linkage_sql() -> str:
    """Single DuckDB SELECT: one row per dated imaging nodule (primary FNA link only)."""

    return f"""
WITH {_CANONICAL_LINKAGE_CTES.strip()}
SELECT
    research_id,
    lower(md5(concat(cast(research_id AS VARCHAR), '|', canonical_nodule_id))) AS canonical_row_hash,
    canonical_nodule_id,
    lobe_laterality AS laterality,
    lobe_laterality AS lobe,
    nodule_ordinal,
    source_nodule_site_raw,
    us_exam_date AS us_exam_date_first,
    us_exam_date AS us_exam_date_last,
    us_max_size_cm * 10.0 AS us_max_size_mm,
    acr_tirads,
    fna_episode_id,
    fna_date AS fna_date_first,
    fna_date AS fna_date_last,
    bethesda_category AS bethesda_worst,
    molecular_episode_id,
    molecular_test_date AS molecular_date,
    molecular_platform,
    molecular_result_raw AS molecular_result,
    surgery_episode_id_linked,
    surgery_date_index AS surgery_date_index,
    CASE
        WHEN LOWER(COALESCE(final_histology, '')) LIKE '%carcinoma%'
             OR LOWER(COALESCE(final_histology, '')) LIKE '%cancer%'
             OR LOWER(COALESCE(final_histology, '')) LIKE '%ptc%'
             OR LOWER(COALESCE(final_histology, '')) LIKE '%ftc%'
             OR LOWER(COALESCE(final_histology, '')) LIKE '%mtc%'
             OR LOWER(COALESCE(final_histology, '')) LIKE '%atc%'
             OR LOWER(COALESCE(final_histology, '')) LIKE '%pdtc%'
        THEN 'malignant'
        WHEN final_histology IS NOT NULL THEN 'non_malignant_or_other'
        ELSE NULL
    END AS final_pathology_label,
    final_histology,
    tumor_size_mm_path,
    t_stage_final,
    n_stage_final,
    margins_final,
    multifocal_flag,
    (LOWER(COALESCE(final_histology, '')) LIKE '%niftp%') AS niftp_flag,
    json_object(
        'imaging_source_table', imaging_source_table,
        'img_fna', json_object(
            'match_path', match_path,
            'specimen_match_flag', specimen_match_flag,
            'n_candidates_for_nodule', n_candidates_for_nodule,
            'is_primary_link', is_primary_link
        ),
        'fna_molecular', json_object(
            'tier', fna_molecular_linkage_tier,
            'score', fna_molecular_linkage_score,
            'n_candidates', fna_molecular_n_candidates,
            'molecular_episode_id', molecular_episode_id
        ),
        'preop_surgery', json_object(
            'tier', preop_surgery_linkage_tier,
            'score', preop_surgery_linkage_score,
            'n_candidates', preop_surgery_n_candidates,
            'surgery_episode_id', surgery_episode_id_linked
        ),
        'surgery_pathology', json_object(
            'tier', surgery_pathology_linkage_tier,
            'score', surgery_pathology_linkage_score,
            'n_candidates', surgery_pathology_n_candidates,
            'path_surgery_id', path_surgery_id,
            'tumor_ordinal', tumor_ordinal_linked
        )
    ) AS source_lineage_json,
    LEAST(
        COALESCE(fna_molecular_linkage_score, 1.0),
        COALESCE(preop_surgery_linkage_score, 1.0),
        COALESCE(surgery_pathology_linkage_score, 1.0)
    ) AS deterministic_match_confidence,
    {_MANUAL_REVIEW_PREDICATE.strip()} AS manual_review_needed_flag
FROM earliest_surg
"""


def candidate_pairs_sql() -> str:
    """Imaging↔FNA rows in multi-candidate nodule sets (deterministic audit)."""
    return """
SELECT
    ifn.research_id,
    CAST(ifn.nodule_id AS VARCHAR) AS canonical_nodule_id,
    ifn.fna_episode_id,
    ifn.match_path,
    ifn.n_candidates_for_nodule,
    ifn.is_primary_link,
    ifn.ordinal_in_nodule,
    'imaging_fna_multicandidate'::VARCHAR AS pair_domain
FROM imaging_fna_linkage_mm_v1 ifn
WHERE ifn.n_candidates_for_nodule > 1
"""


def qc_summary_sql() -> str:
    """Aggregate linkage yields."""
    return """
SELECT
    (SELECT COUNT(*) FROM imaging_nodule_master_v1 WHERE exam_date IS NOT NULL)
        AS n_imaging_nodule_rows,
    (SELECT COUNT(DISTINCT concat(
            cast(research_id AS VARCHAR), '|', cast(nodule_id AS VARCHAR)))
        FROM imaging_nodule_master_v1 WHERE exam_date IS NOT NULL)
        AS n_distinct_imaging_nodules,
    (SELECT COUNT(*) FROM imaging_fna_linkage_mm_v1 WHERE is_primary_link)
        AS n_us_fna_primary_links,
    (SELECT COUNT(*) FROM fna_molecular_linkage_v3 WHERE score_rank = 1)
        AS n_fna_molecular_rank1,
    (SELECT COUNT(*) FROM preop_surgery_linkage_v3
        WHERE score_rank = 1 AND preop_type = 'fna') AS n_preop_surgery_fna_rank1,
    (SELECT COUNT(*) FROM surgery_pathology_linkage_v3 WHERE score_rank = 1)
        AS n_surgery_path_rank1
"""


def discordance_sql() -> str:
    """Aggregated discordance flags (no clinical note text)."""
    return f"""
WITH {_CANONICAL_LINKAGE_CTES.strip()}
, base AS (
    SELECT
        bethesda_category,
        molecular_result_class,
        CASE
            WHEN LOWER(COALESCE(final_histology, '')) LIKE '%carcinoma%'
                 OR LOWER(COALESCE(final_histology, '')) LIKE '%cancer%'
                 OR LOWER(COALESCE(final_histology, '')) LIKE '%ptc%'
                 OR LOWER(COALESCE(final_histology, '')) LIKE '%ftc%'
                 OR LOWER(COALESCE(final_histology, '')) LIKE '%mtc%'
                 OR LOWER(COALESCE(final_histology, '')) LIKE '%atc%'
                 OR LOWER(COALESCE(final_histology, '')) LIKE '%pdtc%'
            THEN 'malignant'
            WHEN final_histology IS NOT NULL THEN 'non_malignant_or_other'
            ELSE NULL
        END AS final_pathology_label,
        (LOWER(COALESCE(final_histology, '')) LIKE '%niftp%') AS niftp_hist
    FROM earliest_surg
)
SELECT 'mol_positive_path_not_malignant'::VARCHAR AS discordance_kind, COUNT(*) AS n_rows
FROM base
WHERE LOWER(COALESCE(molecular_result_class, '')) IN ('positive', 'suspicious')
  AND (final_pathology_label IS NULL OR final_pathology_label <> 'malignant')

UNION ALL

SELECT 'bethesda_high_grade_path_benign'::VARCHAR, COUNT(*)
FROM base
WHERE bethesda_category >= 5
  AND final_pathology_label = 'non_malignant_or_other'

UNION ALL

SELECT 'path_malignant_mol_negative'::VARCHAR, COUNT(*)
FROM base
WHERE final_pathology_label = 'malignant'
  AND LOWER(COALESCE(molecular_result_class, '')) = 'negative'

UNION ALL

SELECT 'niftp_rows'::VARCHAR, COUNT(*)
FROM base
WHERE niftp_hist
"""


def manual_review_queue_sql() -> str:
    """Explicit adjudication queue: ambiguous chains and policy triggers."""
    return f"""
WITH {_CANONICAL_LINKAGE_CTES.strip()}
SELECT
    research_id,
    canonical_nodule_id,
    'canonical_nodule_linkage_v1'::VARCHAR AS queue_origin,
    'manual_review_triggered'::VARCHAR AS queue_status,
    json_object(
        'bethesda_category', bethesda_category,
        'fna_date', fna_date,
        'acr_tirads', acr_tirads,
        'match_path', match_path,
        'fna_molecular_n_candidates', fna_molecular_n_candidates,
        'preop_surgery_n_candidates', preop_surgery_n_candidates,
        'surgery_pathology_n_candidates', surgery_pathology_n_candidates,
        'surgery_episode_id_linked', surgery_episode_id_linked,
        'earliest_postfna_surgery_episode_id', earliest_postfna_surgery_episode_id,
        'molecular_episode_id', molecular_episode_id
    ) AS review_context_json
FROM earliest_surg
WHERE {_MANUAL_REVIEW_PREDICATE.strip()}
"""

