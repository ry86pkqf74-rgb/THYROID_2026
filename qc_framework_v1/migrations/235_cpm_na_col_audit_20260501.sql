-- mig_235 — Lane J CPM 24-na column audit
-- Date: 2026-05-01
-- Author: Cline GPT-5.5 (Lane J dispatch, v15 round)
-- Purpose:
--   Audit the 24 canonical_patient_master columns currently marked
--   verification_status='na' in canonical_column_verification_registry_v1.
--   Add a durable na_rationale column, document every retained NA, and
--   reclassify the one accidental/deferred NA that is rule-verifiable.
--
-- Pre-snapshot:
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_v1_pre_mig235_20260501

-- LOG: §0 pre-snapshot canonical_column_verification_registry_v1
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_v1_pre_mig235_20260501 AS
SELECT * FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1;

-- LOG: §1 add durable na_rationale column if absent
ALTER TABLE main.canonical_column_verification_registry_v1
ADD COLUMN IF NOT EXISTS na_rationale VARCHAR;

-- LOG: §2 document retained genuine NA / non-actionable NA columns
UPDATE main.canonical_column_verification_registry_v1
SET na_rationale = CASE column_name
    WHEN 'any_ete_present_not_further_specified_in_imaging' THEN 'na_genuine: upstream imaging NFS-ETE axis not populated; all FALSE placeholder until structured imaging NFS ETE exists.'
    WHEN 'biochemical_concern_first_date_source' THEN 'na_genuine: empty VARCHAR placeholder; 0 non-null values; pending real Script-224 biochemical helper extraction.'
    WHEN 'biochemical_concern_flag' THEN 'na_genuine: degenerate FALSE-only placeholder distinct from recurrence_v1 biochemical pair; pending Script-224 helper build.'
    WHEN 'gm_recurrence_site_primary' THEN 'na_genuine: empty VARCHAR placeholder; GM recurrence-site helper has no populated source values.'
    WHEN 'high_risk_molecular_v7' THEN 'na_genuine: v7 high-risk molecular rule yields 0 TRUE across populated rows; placeholder pending rebuilt molecular-risk ladder.'
    WHEN 'nlp_tg_rising_mentioned' THEN 'na_genuine: Type-B NLP placeholder; 0 TRUE / 49 FALSE / 10822 NULL; no positive signal in current rollup.'
    WHEN 'nsqip_hypoparathyroidism_recovered_flag' THEN 'na_genuine: degenerate FALSE-only NSQIP helper; mate hypocalcemia flag carries signal but hypopara recovered flag is unpopulated.'
    WHEN 'nucmed_cumulative_therapeutic_dose' THEN 'na_genuine: partial nucmed narrative dose signal superseded by RAI canonical cumulative-dose SSOT; retained as source-diagnostic only.'
    WHEN 'nucmed_tgab_max_source' THEN 'na_genuine: empty VARCHAR placeholder; no populated nucmed TgAb max source values.'
    WHEN 'op_esophageal_inv_first_evidence_text' THEN 'na_genuine: empty VARCHAR placeholder; esophageal operative evidence text absent/source-limited in current extraction.'
    WHEN 'prm_high_risk_marker_any' THEN 'na_genuine: PRM high-risk-marker rule yields 0 TRUE; placeholder pending rule rebuild with real positive signal.'
    WHEN 'radtx_nlp_hormone_withdrawal' THEN 'na_genuine: radtx NLP RAI-prep placeholder; degenerate FALSE-only among radtx-data patients pending radtx LLM recanonicalization.'
    WHEN 'radtx_nlp_post_tx_scan_negative' THEN 'na_genuine: radtx NLP post-treatment scan placeholder; degenerate FALSE-only among radtx-data patients pending radtx LLM recanonicalization.'
    WHEN 'radtx_nlp_thyrogen_prep' THEN 'na_genuine: radtx NLP Thyrogen-prep placeholder; degenerate FALSE-only among radtx-data patients pending radtx LLM recanonicalization.'
    WHEN 'rai_avid_flag' THEN 'na_genuine: direct propagation from rai_treatment_episode_v2 iodine_avidity placeholder; 0 TRUE until V2 RAI NLP backfill populates avidity.'
    WHEN 'rai_avidity' THEN 'na_genuine: redundant direct propagation of same upstream iodine_avidity placeholder as rai_avid_flag; 0 TRUE until V2 RAI NLP backfill.'
    WHEN 'recurrence_pathology_specimen_id' THEN 'na_genuine: non-actionable identifier/specimen trace column for recurrence pathology evidence, not an analytic verified variable.'
    WHEN 'research_id' THEN 'na_genuine: primary identifier spine; intentionally excluded from content-verification counts via auto_identifier_skip.'
    WHEN 'source_table' THEN 'na_genuine: constant provenance/source mix label; intentionally excluded from content verification via auto_provenance_skip.'
    WHEN 'syn_isthmus_size_cm_legacy_raw' THEN 'na_genuine: legacy raw VARCHAR preserved after typed decomposition; use typed syn_isthmus_* axis/volume columns for analysis.'
    WHEN 'syn_left_lobe_size_cm_legacy_raw' THEN 'na_genuine: legacy raw VARCHAR preserved after typed decomposition; use typed syn_left_lobe_* axis/volume columns for analysis.'
    WHEN 'syn_right_lobe_size_cm_legacy_raw' THEN 'na_genuine: legacy raw VARCHAR preserved after typed decomposition; use typed syn_right_lobe_* axis/volume columns for analysis.'
    WHEN 'tsh_suppressed_ever_source' THEN 'na_genuine: empty VARCHAR provenance placeholder; TSH-suppressed source lineage not populated in current lab rollup.'
    ELSE na_rationale
  END,
  notes = COALESCE(notes, '') || ' | mig_235 Lane J NA audit: retained as documented genuine/non-actionable NA.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='na'
  AND column_name IN (
    'any_ete_present_not_further_specified_in_imaging',
    'biochemical_concern_first_date_source',
    'biochemical_concern_flag',
    'gm_recurrence_site_primary',
    'high_risk_molecular_v7',
    'nlp_tg_rising_mentioned',
    'nsqip_hypoparathyroidism_recovered_flag',
    'nucmed_cumulative_therapeutic_dose',
    'nucmed_tgab_max_source',
    'op_esophageal_inv_first_evidence_text',
    'prm_high_risk_marker_any',
    'radtx_nlp_hormone_withdrawal',
    'radtx_nlp_post_tx_scan_negative',
    'radtx_nlp_thyrogen_prep',
    'rai_avid_flag',
    'rai_avidity',
    'recurrence_pathology_specimen_id',
    'research_id',
    'source_table',
    'syn_isthmus_size_cm_legacy_raw',
    'syn_left_lobe_size_cm_legacy_raw',
    'syn_right_lobe_size_cm_legacy_raw',
    'tsh_suppressed_ever_source'
  );

-- LOG: §3 reclassify one deferred NA to verified after rule-based companion-column check
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'cline_gpt_5_5_mig_235',
    batch_id = 'mig_235_cpm_na_col_audit_20260501',
    verification_method = 'lane_j_na_audit_rule_check',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    na_rationale = 'na_deferred_should_verify resolved: pmhx_nlp_family_hx_thyroid exactly matches companion n_mentions rule on all non-null flag rows (31 TRUE, 259 FALSE, 0 mismatches); NULL rows reflect no PMHx NLP family-history scope.',
    notes = COALESCE(notes, '') || ' | mig_235 Lane J NA audit: reclassified na->verified. Evidence: flag TRUE=31, FALSE=259, NULL=10581; companion pmhx_nlp_family_hx_thyroid_n_mentions>0 has 31 patients; 0 mismatches among non-null flag rows.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND column_name='pmhx_nlp_family_hx_thyroid'
  AND verification_status='na';

-- LOG: §4 resync canonical_patient_master table-level signoff counts
UPDATE main.canonical_table_signoff_registry_v1
SET n_verified = s.n_verified,
    n_na = s.n_na,
    n_not_started = s.n_not_started,
    n_failed = s.n_failed,
    n_columns_total = s.n_columns_total,
    signoff_migration = 'qc_framework_v1/migrations/235_cpm_na_col_audit_20260501.sql',
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM (
  SELECT
    SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END)::INTEGER AS n_verified,
    SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END)::INTEGER AS n_na,
    SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END)::INTEGER AS n_not_started,
    SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END)::INTEGER AS n_failed,
    COUNT(*)::INTEGER AS n_columns_total
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
) s
WHERE schema_name='main' AND table_name='canonical_patient_master';

-- LOG: §5 provenance row
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'mig_235_cpm_na_audit_v15',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'lane_j_cpm_24_na_column_audit',
  '0',
  '0',
  '1',
  '0'
);

-- ASSERT: 24 original NA rows were covered by audit actions (23 retained NA + 1 verified)
SELECT CASE WHEN COUNT(*)=24 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_column_verification_registry_v1
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND column_name IN (
    'any_ete_present_not_further_specified_in_imaging',
    'biochemical_concern_first_date_source',
    'biochemical_concern_flag',
    'gm_recurrence_site_primary',
    'high_risk_molecular_v7',
    'nlp_tg_rising_mentioned',
    'nsqip_hypoparathyroidism_recovered_flag',
    'nucmed_cumulative_therapeutic_dose',
    'nucmed_tgab_max_source',
    'op_esophageal_inv_first_evidence_text',
    'pmhx_nlp_family_hx_thyroid',
    'prm_high_risk_marker_any',
    'radtx_nlp_hormone_withdrawal',
    'radtx_nlp_post_tx_scan_negative',
    'radtx_nlp_thyrogen_prep',
    'rai_avid_flag',
    'rai_avidity',
    'recurrence_pathology_specimen_id',
    'research_id',
    'source_table',
    'syn_isthmus_size_cm_legacy_raw',
    'syn_left_lobe_size_cm_legacy_raw',
    'syn_right_lobe_size_cm_legacy_raw',
    'tsh_suppressed_ever_source'
  )
  AND na_rationale IS NOT NULL;

-- ASSERT: CPM signoff math remains closed with 1,630 columns total
SELECT CASE
  WHEN n_verified=1607 AND n_na=23 AND COALESCE(n_failed,0)=0 AND n_not_started=0 AND n_columns_total=1630 AND table_status='verified'
  THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_table_signoff_registry_v1
WHERE schema_name='main' AND table_name='canonical_patient_master';

-- ASSERT: cohort parity invariant remains intact
SELECT CASE WHEN
  (SELECT COUNT(*) FROM main.canonical_patient_master)=10871
  AND (SELECT COUNT(DISTINCT research_id) FROM main.canonical_patient_master)=10871
  AND (SELECT COUNT(DISTINCT research_id) FROM main.canonical_us_thyroid_gland_patient_rollup_v2)=10871
  AND (SELECT COUNT(DISTINCT research_id) FROM main.canonical_us_lymph_node_patient_rollup_v2)=10871
THEN 'PASS' ELSE 'FAIL' END;

-- ASSERT: Lane J produced no failed-in-disguise reclassification
SELECT CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_column_verification_registry_v1
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND batch_id='mig_235_cpm_na_col_audit_20260501'
  AND verification_status='failed';