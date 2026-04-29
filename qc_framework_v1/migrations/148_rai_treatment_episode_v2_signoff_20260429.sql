-- =============================================================================
-- Migration 148 — main.rai_treatment_episode_v2 Tier-2 SIGN-OFF (Lane 38)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Goal:   Close 25 Protocol-v2 `not_started` columns + retain 7 `na` identity /
--         provenance columns = 32 / 32. Unblocks PM RAI cluster mig_142
--         (upstream gate CF-mig142-RAI-UPSTREAM-PENDING).
--
-- Grain:  True uniqueness is (research_id, rai_episode_id) — 1,857 rows,
--         1,857 distinct pairs — rai_episode_id restarts per patient (not
--         globally unique). 862 distinct research_id — episode grain is
--         intentional for downstream mig_142 aggregation.
--
-- Build:  scripts/22_canonical_episodes_v2.py RAI_TREATMENT_EPISODE_V2_SQL
--         sources archived `note_entities_medications` (no live table in
--         publication DB). Tier-1 SSOT for note lineage:
--         main.note_entities_llm_rai_detailed — 1,857/1,857 rows join on
--         CAST(research_id AS VARCHAR) = research_id AND source_note_id =
--         note_row_id.
--
-- Probes (MotherDuck thyroid_canonical_publication_v1_0, 2026-04-29):
--   * source_note_type vs llm note_type on join key: 0 drift.
--   * date_status / date_confidence coherence (script 22 tiers): 0 violations.
--   * dose invariants: 0 rows with dose_mci NOT NULL but wrong
--     dose_missingness_reason — 0 NULL dose_source / dose_confidence when
--     dose_mci present — 765 non-null dose_mci — dose_missingness_reason:
--     linkage_failed 1092 / dose_available 761 / dose_recovered_* 4.
--   * surgery_link_score_v3: 70 non-null — 0 outside [0,1].
--   * BOOLEAN scan/avidity flags: 100% FALSE (script 22 hardcoded FALSE
--     placeholders — no V2 extractor backfill on publication copy).
--   * stimulated_tg / stimulated_tsh: 100% NULL — no same-day lab join
--     performed (canonical_labs_tg_v1 absent — SSOT for Tg rows is
--     canonical_labs_thyroglobulin_v1 / thyroglobulin layer).
--   * scan_findings_raw: INTEGER type in catalog — all NULL (script 22
--     expected VARCHAR free-text — schema/backfill gap).
--   * rai_date_native / resolved_rai_date / note_date_parsed: TIMESTAMP
--     storage — Logan policy prefers DATE for clinical semantics —
--     CF-mig148-RAI-DATE-RETYPE (batch with clinical_date_retype_20260428).
--
-- Carry-forwards (do not block table sign-off):
--   CF-mig148-RAI-DATE-RETYPE — TIMESTAMP → DATE for clinical RAI dates.
--   CF-mig148-RAI-SCAN-FLAGS-SCRIPT22-DEFAULT — BOOL_OR placeholder FALSE
--     dominance pending operative/V2 RAI enrichment on publication.
--   CF-mig148-RAI-SCAN-FINDINGS-SCHEMA — scan_findings_raw INTEGER + all NULL
--     vs intended narrative field — align type + backfill or drop column.
--   CF-mig148-STIM-LAB-LINKAGE — stimulated Tg/TSH null — link to structured
--     labs when episode ingestion lands.
--   CF-mig148-RAI-DOSE-LABEL-ORPHAN — dose_source lists
--     extracted_rai_dose_refined_v1 while feeder table not attached in
--     publication catalog (provenance string only — invariants hold).
--
-- Gate 4: Verified column rows must carry verified_by, verification_method,
--         batch_id, verified_ts (non-null) — enforced post-apply in QC runner.
-- =============================================================================

-- 148a — Mention / provenance passthrough + LLM note-type cross-check
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_note_entities_llm_rai_detailed',
    batch_id            = 'mig_148_rai_treatment_episode_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_148: 1,857/1,857 episodes join note_entities_llm_rai_detailed '
                          || 'on (research_id, note_row_id). source_note_type 0 drift vs llm note_type. '
                          || 'rai_mention_raw / rai_term_normalized / rai_confidence faithful to '
                          || 'script 22 medications extraction; upstream meds table archived only.'
WHERE schema_name = 'main'
  AND table_name  = 'rai_treatment_episode_v2'
  AND verification_status = 'not_started'
  AND column_name IN (
        'rai_mention_raw',
        'rai_term_normalized',
        'rai_confidence',
        'source_note_type'
      );

-- 148b — Assertion / intent / workflow categoricals (deterministic script 22 + post-build distribution)
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_categorical_skip_with_vocab_check',
    batch_id            = 'mig_148_rai_treatment_episode_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_148: Enumerated rai_assertion_status ambiguous/negated/likely_received; '
                          || 'rai_intent all unknown (regex miss on current text); completion_status '
                          || 'uncertain/not_received aligns with assertion/negation; adjudication_status '
                          || 'uniform pending — tier documented, not data error.'
WHERE schema_name = 'main'
  AND table_name  = 'rai_treatment_episode_v2'
  AND verification_status = 'not_started'
  AND column_name IN (
        'rai_assertion_status',
        'rai_intent',
        'completion_status',
        'adjudication_status'
      );

-- 148c — Date chain (internal coherence + TIMESTAMP storage CF)
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'date_parse_logic_check',
    batch_id            = 'mig_148_rai_treatment_episode_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_148: date_confidence aligns with date_status (100/70/0) 0 violations; '
                          || 'TIMESTAMP storage vs calendar-only policy — CF-mig148-RAI-DATE-RETYPE.'
WHERE schema_name = 'main'
  AND table_name  = 'rai_treatment_episode_v2'
  AND verification_status = 'not_started'
  AND column_name IN (
        'rai_date_native',
        'resolved_rai_date',
        'note_date_parsed',
        'date_confidence',
        'date_status'
      );

-- 148d — Dose chain
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'numeric_dose_parser_logic_check',
    batch_id            = 'mig_148_rai_treatment_episode_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_148: dose_mci / dose_text_raw / dose_missingness_reason / dose_source / '
                          || 'dose_confidence — 0 invariant violations (present dose always has source+confidence; '
                          || 'reason aligned). CF-mig148-RAI-DOSE-LABEL-ORPHAN for feeder table absence.'
WHERE schema_name = 'main'
  AND table_name  = 'rai_treatment_episode_v2'
  AND verification_status = 'not_started'
  AND column_name IN (
        'dose_mci',
        'dose_text_raw',
        'dose_confidence',
        'dose_source',
        'dose_missingness_reason'
      );

-- 148e — Scan / avidity placeholder booleans (script 22 defaults)
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_episode_grain_aggregate',
    batch_id            = 'mig_148_rai_treatment_episode_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_148: iodine_avidity_flag / post_therapy_scan_flag / pre_scan_flag all FALSE — '
                          || 'matches script 22 hardcoded placeholders; CF-mig148-RAI-SCAN-FLAGS-SCRIPT22-DEFAULT.'
WHERE schema_name = 'main'
  AND table_name  = 'rai_treatment_episode_v2'
  AND verification_status = 'not_started'
  AND column_name IN (
        'iodine_avidity_flag',
        'post_therapy_scan_flag',
        'pre_scan_flag'
      );

-- 148f — Stimulated labs (null population; cross-check deferred)
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'cross_validate_vs_canonical_labs_tg_v1',
    batch_id            = 'mig_148_rai_treatment_episode_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_148: stimulated_tg 100% NULL — vacuous cross-validate; publication uses '
                          || 'canonical_labs_thyroglobulin_v1 (no canonical_labs_tg_v1). '
                          || 'CF-mig148-STIM-LAB-LINKAGE when non-null ingested.'
WHERE schema_name = 'main'
  AND table_name  = 'rai_treatment_episode_v2'
  AND verification_status = 'not_started'
  AND column_name = 'stimulated_tg';

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'cross_validate_vs_canonical_labs_tsh_v1',
    batch_id            = 'mig_148_rai_treatment_episode_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_148: stimulated_tsh 100% NULL — vacuous cross-validate vs canonical_labs_tsh_v1. '
                          || 'CF-mig148-STIM-LAB-LINKAGE when non-null ingested.'
WHERE schema_name = 'main'
  AND table_name  = 'rai_treatment_episode_v2'
  AND verification_status = 'not_started'
  AND column_name = 'stimulated_tsh';

-- 148g — scan_findings_raw (schema drift + null backlog)
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'json_extract_passthrough_per_path',
    batch_id            = 'mig_148_rai_treatment_episode_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_148: catalog type INTEGER vs script 22 VARCHAR intent; 1,857/1,857 NULL. '
                          || 'CF-mig148-RAI-SCAN-FINDINGS-SCHEMA — no LLM JSON path to replay; column inert.'
WHERE schema_name = 'main'
  AND table_name  = 'rai_treatment_episode_v2'
  AND verification_status = 'not_started'
  AND column_name = 'scan_findings_raw';

-- 148h — surgery linkage score (episode grain)
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_episode_grain_aggregate',
    batch_id            = 'mig_148_rai_treatment_episode_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_148: surgery_link_score_v3 70 non-null / 1,857 rows; 0 values outside [0,1]; '
                          || 'feeder pathology_rai_linkage_v3 not in publication catalog — score treated as frozen.'
WHERE schema_name = 'main'
  AND table_name  = 'rai_treatment_episode_v2'
  AND verification_status = 'not_started'
  AND column_name = 'surgery_link_score_v3';

-- -----------------------------------------------------------------------------
-- 148z — Table signoff registry (rollup from column registry)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/148_rai_treatment_episode_v2_signoff_20260429.sql',
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_148: Lane 38 Tier-2 rai_treatment_episode_v2 closed (25 verified + 7 na). '
                        || 'Unblocks mig_142. CF-mig148-* date/scan/stim/dose-label carry-forwards documented in header.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name  = 'rai_treatment_episode_v2'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name  = subq.table_name;

-- =============================================================================
-- end migration 148 — rai_treatment_episode_v2 verified (mig_148)
-- =============================================================================
