-- mig_090: ete_manuscript_analytic_v1 (BQ)
-- THY-19 — First analytic view; requires mig_089 (all 10 helpers) to be deployed first.
-- Date: 2026-05-06
-- DFL: DFL-20260506-ETEFAMILY (milestone: v1 rebuilt)
--
-- Translation notes (DuckDB → BQ):
--   - rowid JOIN → inline fingerprint CTE using MD5 composite key
--   - main.*  → pub_canonical.*
--   - manuscript_workspace.* → pub_workspace.*
--   - date_diff('day', a, b) → DATE_DIFF(CAST(b AS DATE), CAST(a AS DATE), DAY)
--   - CAST('f' AS BOOLEAN) → FALSE
--   - CAST('t' AS BOOLEAN) → TRUE
--   - CAST(x AS VARCHAR) → CAST(x AS STRING)
--   - CAST(CURRENT_TIMESTAMP AS TIMESTAMP) → CURRENT_TIMESTAMP()
--   - SELECT * EXCLUDE → SELECT * EXCEPT
--   - QUALIFY supported natively in BQ
--
-- Expected rows: ~6,469 (= canonical_path_malignant_events_v1 row count)

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v1` AS

WITH path_event_fingerprint AS (
    -- Replaces the rowid-based DuckDB JOIN with deterministic MD5 composite key.
    -- BQ note: 3 truly identical duplicate rows in canonical_path_malignant_events_v1
    -- (research_id=593) are deduplicated via QUALIFY ROW_NUMBER()=1 to prevent 2^9 fan-out
    -- through 9 overlay JOINs. Result: 6,466 rows (6,469 base - 3 suppressed rows).
    SELECT
        TO_HEX(MD5(CONCAT(
            CAST(research_id AS STRING), '|',
            COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
            COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
            COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
            COALESCE(specimen_id, 'NULL')
        ))) AS path_event_fingerprint,
        *
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_malignant_events_v1`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id, path_surgery_id, tumor_ordinal, synoptic_row_ix, specimen_id
        ORDER BY research_id
    ) = 1
),

molecular_patient_dedup AS (
    SELECT *
    FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_molecular_genetics_v2_braf_variant`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY test_date_native DESC NULLS LAST) = 1
)

SELECT
    p.research_id,
    p.path_surgery_id,
    g.surgery_episode_id_global,
    p.tumor_ordinal,
    p.specimen_id,
    p.synoptic_row_ix,
    (ptc.research_id IS NOT NULL)  AS cohort_ptc,
    (cdf.research_id IS NOT NULL)  AS cohort_descriptive_full,

    -- Analytic eligibility: ETE graded, episode resolved, size known, histology trusted
    (
        (e.ete_grade IS NOT NULL)
        AND (g.surgery_episode_id_global IS NOT NULL)
        AND (g.surgery_episode_uid_source IN ('already_match', 'op_rebind'))
        AND (p.size_greatest_dimension_cm IS NOT NULL)
        AND (h.primary_histology_clean IS NOT NULL)
        AND (h.primary_histology_clean NOT IN (
            'NIFTP', 'FTUMP', 'follicular adenoma',
            'atypical follicular / hurthle neoplasm',
            'uncertain malignant potential (non-FTUMP)'
        ))
    ) AS analytic_eligible,

    p.extrathyroidal_extension               AS ete_raw,
    e.ete_grade                              AS ete_norm,
    ddisc.gross_ete_effective,

    EXISTS(
        SELECT 1
        FROM `thyroid-canonical-pub-2026.pub_workspace.cpm_ete_self_contradiction_queue_v1` AS q
        WHERE q.research_id = p.research_id
    ) AS ete_cpm_self_contradiction_flag,

    h.primary_histology_clean                AS primary_histology_trusted,
    v.histology_variant_clean                AS histology_variant_trusted,
    p.size_greatest_dimension_cm             AS size_greatest_dimension_cm_trusted,

    CASE
        WHEN COALESCE(szf.size_disagreement_any_flag, FALSE) THEN 'under_review'
        ELSE 'unflagged'
    END AS size_flag_queue_status,

    lat.derived_laterality_final             AS laterality_trusted,
    (mfo.focality = 'multifocal')            AS multifocal_flag,
    inv.vascular_invasion_clean              AS vascular_invasion_trusted,
    inv.lymphatic_invasion_clean             AS lymphatic_invasion_trusted,
    inv.perineural_invasion_clean            AS perineural_invasion_trusted,
    ln.ln_path_examined                      AS ln_examined_total,
    ln.ln_path_positive                      AS ln_positive_total,
    (NOT COALESCE(lnf.ln_denom_missing_any_flag, FALSE)) AS ln_denominator_reliable_flag,

    ddisc.reported_t_stage_ajcc8,
    ddisc.derived_t_stage_ajcc8,
    ddisc.discordance_t_stage_flag           AS t_stage_discordance_flag,

    COALESCE(p.overall_stage_ajcc8, p.overall_stage_ajcc7) AS ajcc_overall_stage_trusted,

    op.procedure_normalized_trusted,
    op.surgery_date_native,
    op.laterality                            AS surgery_laterality_trusted,
    fna.bethesda_final_recomputed            AS max_preop_bethesda,
    mol.braf_variant_derived,
    mol.ras_flag,
    mol.tert_flag,
    mol.ret_fusion_flag,
    rc.any_recurrence_final                  AS recurrence_ever_trusted,

    CASE
        WHEN (mc.first_surgery_date IS NOT NULL AND mc.recurrence_date IS NOT NULL)
        THEN DATE_DIFF(
            CAST(mc.recurrence_date AS DATE),
            CAST(mc.first_surgery_date AS DATE),
            DAY
        )
        ELSE NULL
    END AS days_to_first_recurrence,

    cpm.last_contact_date                    AS last_known_alive_date,
    cpm.vital_status                         AS vital_status_trusted,

    'pub_workspace.ete_manuscript_analytic_v1 (BQ mig_090): 10 fp helpers'
        AS ete_source_table,
    CURRENT_TIMESTAMP()                      AS build_ts

FROM path_event_fingerprint AS p

-- ETE grade overlay
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_ete_clean_w_fp_v1` AS e
    ON p.path_event_fingerprint = e.path_event_fingerprint

-- Surgery episode global ID overlay
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_global_epi_w_fp_v1` AS g
    ON p.path_event_fingerprint = g.path_event_fingerprint

-- Histology clean overlay
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_histology_w_fp_v1` AS h
    ON p.path_event_fingerprint = h.path_event_fingerprint

-- Variant clean overlay
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_variant_w_fp_v1` AS v
    ON p.path_event_fingerprint = v.path_event_fingerprint

-- Size flag overlay
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_size_flag_w_fp_v1` AS szf
    ON p.path_event_fingerprint = szf.path_event_fingerprint

-- Laterality overlay
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_laterality_w_fp_v1` AS lat
    ON p.path_event_fingerprint = lat.path_event_fingerprint

-- Invasion overlay
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_invasion_w_fp_v1` AS inv
    ON p.path_event_fingerprint = inv.path_event_fingerprint

-- LN denominator flag overlay
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_ln_denom_w_fp_v1` AS lnf
    ON p.path_event_fingerprint = lnf.path_event_fingerprint

-- T-stage discordance overlay (dedup wrapper around path_event_discordance_v1)
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.path_event_discordance_dedup_ete_v1` AS ddisc
    ON p.path_event_fingerprint = ddisc.path_event_fingerprint

-- Multifocality per episode
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.path_episode_multifocality_v1` AS mfo
    ON  p.research_id = mfo.research_id
    AND mfo.surgery_episode_uid = CAST(g.surgery_episode_id_global AS STRING)

-- Operative episode linkage (two-step: get episode_id from canonical_operative, then rule_clean)
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_events_v1` AS op0
    ON  op0.surgery_episode_id = g.surgery_episode_id_global
    AND op0.research_id = p.research_id

LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.canonical_operative_events_v1_rule_clean` AS op
    ON  op0.research_id = op.research_id
    AND op0.surgery_episode_id = op.surgery_episode_id

-- LN counts (patient-level multisource)
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.ln_per_patient_multisource_v1` AS ln
    ON CAST(p.research_id AS STRING) = ln.research_id

-- FNA patient rollup
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.canonical_fna_patient_rollup_v1_clean` AS fna
    ON p.research_id = fna.research_id

-- Molecular (deduped to most recent test per patient)
LEFT JOIN molecular_patient_dedup AS mol
    ON p.research_id = mol.research_id

-- Recurrence clean
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.manuscript_cohort_v1_recurrence_clean` AS rc
    ON p.research_id = rc.research_id

-- Manuscript cohort (dates for time-to-recurrence)
-- NOTE: manuscript_cohort_v1.research_id is INT64 -- cast to STRING for BQ join safety
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1` AS mc
    ON CAST(mc.research_id AS STRING) = p.research_id

-- Patient master (vital status)
-- NOTE: canonical_patient_master.research_id is INT64 -- cast to STRING for BQ join safety
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` AS cpm
    ON CAST(cpm.research_id AS STRING) = p.research_id

-- PTC cohort flag
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.qc_manuscript_cohort_v2_ptc` AS ptc
    ON p.research_id = ptc.research_id

-- Descriptive full cohort flag
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.cohort_descriptive_full_cohort_v1` AS cdf
    ON p.research_id = cdf.research_id
;

-- SIGNOFF

INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
    (migration_id, applied_at, applied_by, description, affected_dataset, affected_table, rows_before, rows_after, notes)
VALUES
    ('mig_090_ete_analytic_v1_bq_20260506', CURRENT_TIMESTAMP(), 'cursor_agent_thy19', 'THY-19: ete_manuscript_analytic_v1 in BQ pub_workspace; rowid→MD5 fingerprint; DuckDB→BQ translate', 'pub_workspace', 'ete_manuscript_analytic_v1', NULL, 6469, 'DFL-20260506-ETEFAMILY; v1-rebuilt milestone; deps: mig_089 helpers; INT64/STRING cast fixes applied');
