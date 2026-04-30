-- mig_221 — Lane E6 (Round 2): document acr2017_feature_points_complete semantics
-- run_id: mig_221_acr_completeness_flag_clarification_20260430
-- Source: CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md — E6
-- Target DB: thyroid_canonical_publication_v1_0
-- Companion memory: memory/feedback_acr2017_feature_points_complete_semantic.md
-- Methods paste-up: docs/methods_acr2017_feature_points_complete_20260430.md
-- No recomputation of the flag — documentation + registry alignment only.

USE thyroid_canonical_publication_v1_0;

COMMENT ON COLUMN main.canonical_us_nodule_v2.acr2017_feature_points_complete IS
'TRUE only when all five ACR 2017 **descriptor** fields on the upstream characteristics row (composition, echogenicity, shape, margins, calcifications on canonical_us_nodule_characteristics_v1) were non-NULL when Script 271 populated tirads_score_component_complete — then renamed by Script 374. NOT equivalent to “all five *_pts columns non-NULL”: Script 376 can impute *_pts from normalized feature strings on canonical_us_nodule_v2 after merge, so many rows have full points while this flag remains FALSE. Primary strict ACR 2017 analytic cohort should filter acr2017_feature_points_complete = TRUE (see manuscript_workspace.vw_us_nodule_tirads_strict_acr2017_VIEW_v1, mig_219).';

UPDATE main.canonical_column_verification_registry_v1
SET notes = TRIM(COALESCE(notes, '') || ' ')
    || 'mig_221: acr2017_feature_points_complete = legacy tirads_score_component_complete from CUNC (Script 271): TRUE iff all five feature *descriptors* non-NULL on characteristics table. '
    || 'Does NOT track post-merge / post-376 imputation of *_pts; expect ~5k TRUE vs many more rows with all *_pts filled. '
    || 'Strict manuscript TIRADS cohort uses vw_us_nodule_tirads_strict_acr2017_VIEW_v1. '
    || 'See memory/feedback_acr2017_feature_points_complete_semantic.md.',
    verification_method = COALESCE(verification_method, 'verified')
      || '|mig_221_acr_completeness_flag_documented_20260430'
WHERE schema_name = 'main'
  AND table_name = 'canonical_us_nodule_v2'
  AND column_name = 'acr2017_feature_points_complete';

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_221_acr_completeness_flag_clarification_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'lane_e6_comment_on_column_col_registry_notes_memory_methods_stub',
   '0',
   'acr2017_feature_points_complete_semantic_documentation',
   'companion_md_feedback_acr2017_feature_points_complete_semantic',
   'optional_future_recompute:_relax_flag_to_match_all_pts_nonnull_requires_Logan_ratification');
