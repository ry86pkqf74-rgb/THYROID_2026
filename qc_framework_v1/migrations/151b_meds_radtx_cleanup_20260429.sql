-- Migration: 151b_meds_radtx_cleanup_20260429.sql
-- Purpose: Cleanup on canonical_patient_master mig_151 cluster (37 cols) post-Cowork
--          independent verification. Three categories of fixes:
--
--          (A) 15 med_* cols — verification_method renamed to LIVE canonical:
--              Original: 'extraction_faithfulness_vs_note_entities_medications_script215'
--              The named table `note_entities_medications` lives ONLY in archive_pub_v1_0
--              (pre-script-251 snapshot 2026-04-17, multiple pre-script-365 snapshots).
--              Live successor SSOT: canonical_medications_events_v1.
--              Renamed to: 'derivation_vs_canonical_medications_events_v1_via_script215'.
--              Cowork spot-check med_nlp_levothyroxine: 1491 PM TRUE / 1351 canonical levothyroxine pts /
--              0 canon-only drift / 140 PM-only (Synthroid/Levoxyl spelling variants in archive snapshot
--              not in current canonical). Values are extraction-faithful from the pre-archive snapshot
--              Script 215 consumed; methodology updated to name the live canonical so verification
--              can be re-run.
--
--          (B) 3 radtx_nlp_* BOOLEANs reclassified verified → na (Type-B placeholder):
--              radtx_nlp_hormone_withdrawal     : 0 TRUE / 210 FALSE / 10661 NULL
--              radtx_nlp_post_tx_scan_negative  : 0 TRUE / 210 FALSE / 10661 NULL
--              radtx_nlp_thyrogen_prep          : 0 TRUE / 210 FALSE / 10661 NULL
--              All three show degenerate FALSE-only across the 210 patients with any radtx data.
--              Pattern matches mig_148b iodine_avidity_flag (script-22 placeholder). Upstream
--              `note_entities_llm_rad_treatment` lives only in archive_pub_v1_0 (pre-script-251).
--              When the radtx LLM extraction is rebuilt and re-canonicalized, these cols can return
--              to verified.
--
--          (C) 5 remaining radtx_* cols — verification_method renamed to explicit archive snapshot:
--              radtx_llm_extraction_method, radtx_llm_mean_confidence, radtx_llm_n_source_notes,
--              radtx_nlp_external_beam_radiation, radtx_nlp_has_data
--              Renamed from 'extraction_faithfulness_vs_note_entities_llm_rad_treatment_script215'
--              to        'extraction_faithfulness_vs_archive_pub_v1_0_note_entities_llm_rad_treatment_pre251_20260417T012311Z_script215'.
--              The original methodology named a non-live table; the explicit archive snapshot
--              reference makes the verification reproducible against the actual upstream Script 215 used.
--
--          (D) CF-mig151-RADTX-DERIVATION-GAP appendix on radtx_nlp_has_data:
--              Tier-1 archive (note_entities_llm_rad_treatment_pre251) has 5,641 distinct patients
--              with rad-treatment LLM extractions; PM radtx_nlp_has_data flags only 210 patients
--              (5,431-pt gap, 96% missing). Likely cause: Script 215 derivation only consumed a
--              filtered subset (high-confidence / specific note types). Investigation deferred until
--              manuscript radtx scope is finalized.
--
--          NOTE on proc_* cluster: The 14 proc_nlp_* verification_method strings name
--          `note_entities_procedures` which DOES live in main schema — no rename needed.
--          Cowork verified procedural BOOLEANs have real distributions (laryngoscopy 270 TRUE,
--          mrnd 186, parathyroid_autotransplant 48, tracheostomy 384) — clean.
--
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 41b (mig_151 cleanup; registry-only data writes + verification_method renames)
-- Effect : PM n_verified -3 (radtx degenerates → na), n_na +3
--          37 mig_151 cols: 15 med methodology renamed, 5 radtx methodology renamed,
--          3 radtx flipped verified→na under mig_151b batch_id, 14 proc unchanged

-- (A) 15 med_* methodology rename
UPDATE main.canonical_column_verification_registry_v1
SET verification_method = 'derivation_vs_canonical_medications_events_v1_via_script215',
    notes = COALESCE(notes,'') ||
            ' | mig_151b: CF-mig151-MED-METHODOLOGY-CORRECTED — original mig_151 verification_method ' ||
            'named "note_entities_medications" which lives ONLY in archive_pub_v1_0 (pre-script-251 ' ||
            'snapshot 2026-04-17). Live SSOT is canonical_medications_events_v1. Cowork spot-check ' ||
            'med_nlp_levothyroxine: 1491 PM TRUE / 1351 canonical levothyroxine pts / 0 canon-only drift / ' ||
            '140 PM-only (Synthroid/Levoxyl spelling variants in archive snapshot not in current canonical). ' ||
            'Values are extraction-faithful from the pre-archive snapshot Script 215 consumed; ' ||
            'verification_method updated to name live canonical for re-runnability.'
WHERE table_name = 'canonical_patient_master'
  AND batch_id = 'mig_151_patient_master_meds_radtx_proc_cluster_20260429'
  AND column_name LIKE 'med\_%' ESCAPE '\';

-- (B) 3 radtx degenerate-FALSE BOOLEANs verified → na
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verification_method = 'placeholder_radtx_pending_real_extraction_per_mig148b_pattern',
    batch_id = 'mig_151b_meds_radtx_cleanup_20260429',
    verified_by = 'cowork_mig_151b',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'') ||
            ' | mig_151b: Reclassified verified->na. Cohort-uniformity sweep: 0 TRUE / 210 FALSE / ' ||
            '10661 NULL — degenerate FALSE-only across the 210 patients with any radtx data. Pattern ' ||
            'matches mig_148b iodine_avidity_flag (script-22 placeholder). Upstream ' ||
            '`note_entities_llm_rad_treatment` lives only in archive_pub_v1_0 (pre-script-251). When ' ||
            'the radtx LLM extraction is rebuilt and re-canonicalized, these cols can return to verified.'
WHERE table_name = 'canonical_patient_master'
  AND column_name IN ('radtx_nlp_hormone_withdrawal','radtx_nlp_post_tx_scan_negative','radtx_nlp_thyrogen_prep');

-- (C) 5 remaining radtx_* methodology rename to archive snapshot
UPDATE main.canonical_column_verification_registry_v1
SET verification_method = 'extraction_faithfulness_vs_archive_pub_v1_0_note_entities_llm_rad_treatment_pre251_20260417T012311Z_script215',
    notes = COALESCE(notes,'') ||
            ' | mig_151b: CF-mig151-RADTX-UPSTREAM-ARCHIVED — verification_method renamed to explicit ' ||
            'archive snapshot. Live `main` schema has no `note_entities_llm_rad_treatment` (archived ' ||
            'pre-script-251). Cols carry data extracted from the pre-archive Tier-1 snapshot.'
WHERE table_name = 'canonical_patient_master'
  AND column_name IN ('radtx_llm_extraction_method','radtx_llm_mean_confidence','radtx_llm_n_source_notes',
                      'radtx_nlp_external_beam_radiation','radtx_nlp_has_data');

-- (D) CF-RADTX-DERIVATION-GAP on radtx_nlp_has_data
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_151b: CF-mig151-RADTX-DERIVATION-GAP — Tier-1 archive ' ||
            '(note_entities_llm_rad_treatment_pre251_20260417T012311Z) has 5,641 distinct patients ' ||
            'with rad-treatment LLM extractions; PM radtx_nlp_has_data flags only 210 patients ' ||
            '(5,431-pt gap, 96% missing). Likely cause: Script 215 derivation only consumed a ' ||
            'filtered subset (high-confidence / specific note types). Investigation deferred until ' ||
            'manuscript radtx scope is finalized; if radtx becomes a primary analytic, rebuild PM ' ||
            'derivation against full Tier-1 archive or restored canonical.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'radtx_nlp_has_data';

-- (E) Resync canonical_table_signoff_registry_v1 for CPM
UPDATE main.canonical_table_signoff_registry_v1 AS ts
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
    notes             = COALESCE(ts.notes,'') ||
                        ' | mig_151b: 3 radtx degenerate-FALSE cols verified->na; ' ||
                        '20 verification_method strings renamed to live/archive-explicit references.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- End of mig_151b. Already applied via query_rw 2026-04-29.
-- Pre-snapshot:
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig151b_20260429
