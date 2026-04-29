-- Migration: 156b_framework_cleanup_20260429.sql
-- Purpose: Cleanup on canonical_patient_master mig_156 framework cluster (71 cols) post-Cowork
--          independent verification (Path C):
--
--          (A) prm_high_risk_marker_any: reclassify verified -> na (Type-B placeholder).
--              Cohort-uniformity sweep: 0 TRUE / 10,305 FALSE / 566 NULL — degenerate FALSE-only across
--              the entire non-null subset. Same pattern as mig_148b iodine_avidity_flag, mig_151b radtx
--              degenerates, mig_157b high_risk_molecular_v7. The PRM rule yielded zero positives on
--              current build; reclassify per Type-B "upstream/rule not yielding signal" pattern.
--              Will return to verified when PRM high-risk-marker rule is rebuilt with real positive signal.
--
--          (B) any_recurrence_flag: strengthen agent's CF-ANY-RECURRENCE-VS-STRUCTURED-219PT with
--              the canon-only undercount finding. Cowork independent reconcile vs canonical_recurrence_v1
--              (recurrence_confirmed=TRUE) found:
--                  PM 384 T / canonical 514 T / both 165 / PM-only 219 / canon-only 349
--              The PM-only side is OR envelope (OK — agent's framing). The canon-only 349 is a
--              DERIVATION GAP: 349 patients have recurrence_confirmed=TRUE in the canonical that PM
--              any_recurrence_flag does NOT pick up. Same family of finding as mig_138 447-pt undercount
--              that was closed by mig_139 CR-spine resync. Recommend a follow-up CR-spine resync
--              migration for any_recurrence_flag -> canonical_recurrence_v1.recurrence_confirmed=TRUE union.
--
--          (C) Resync canonical_table_signoff_registry_v1 after the verified->na flip.
--
--          Independent verification spot-checks pre-cleanup:
--            - 71 not_started cols confirmed pre-mig_156 apply
--            - SSOT canonical_recurrence_v1, canonical_invasion_events_v1, canonical_fna_events_v1
--              all confirmed live in main
--            - is_malignant: 4137 TRUE / 6734 FALSE confirmed (PM is mixed-cohort, not pure cancer)
--            - any_recurrence_flag: 384 TRUE confirmed; canonical_recurrence_v1.recurrence_confirmed=TRUE: 514
--
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 45b (mig_156 cleanup; registry-only)
-- Effect : PM n_verified -1 / n_na +1; CF appendices on prm_high_risk_marker_any and any_recurrence_flag

-- (A) Reclassify prm_high_risk_marker_any verified -> na
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verification_method = 'helper_placeholder_pending_real_extraction_per_mig148b_pattern',
    batch_id = 'mig_156b_framework_cleanup_20260429',
    verified_by = 'cowork_mig_156b',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'') ||
            ' | mig_156b: Reclassified verified->na. Cohort-uniformity sweep: 0 TRUE / 10305 FALSE / ' ||
            '566 NULL — degenerate FALSE-only across the entire non-null subset. Same pattern as ' ||
            'mig_148b iodine_avidity_flag and mig_151b radtx degenerates. The PRM rule yielded zero ' ||
            'positives on current build; reclassified to na per Type-B "upstream/rule not yielding ' ||
            'signal" pattern. Will return to verified when PRM high-risk-marker rule is rebuilt with ' ||
            'real positive signal.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'prm_high_risk_marker_any';

-- (B) Strengthen any_recurrence_flag CF with canon-only=349 derivation gap
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_156b: CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT — Cowork independent ' ||
            'reconcile vs canonical_recurrence_v1 (recurrence_confirmed=TRUE): PM 384 T / canonical ' ||
            '514 T / both 165 / PM-only 219 / **canon-only 349**. The PM-only side is OR envelope (OK). ' ||
            'The canon-only 349 is a DERIVATION GAP: 349 patients have recurrence_confirmed=TRUE in ' ||
            'the canonical that PM any_recurrence_flag does NOT pick up. Same family of finding as ' ||
            'mig_138 447-pt undercount that was closed by mig_139 CR-spine resync. Recommend follow-up ' ||
            'CR-spine resync for any_recurrence_flag -> canonical_recurrence_v1.recurrence_confirmed=TRUE union.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'any_recurrence_flag';

-- (C) Resync signoff registry
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    signed_off_ts   = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes           = COALESCE(ts.notes,'') ||
                      ' | mig_156b: prm_high_risk_marker_any verified->na (Type-B); CF-ANY-RECURRENCE strengthened with canon_only=349 derivation gap.'
FROM (
  SELECT schema_name, table_name, COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- End of mig_156b. Already applied via query_rw 2026-04-29.
-- Pre-snapshot:
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig156_20260429
