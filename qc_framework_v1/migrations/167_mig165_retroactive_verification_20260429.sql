-- =============================================================================
-- Migration 167 — mig_165 RETROACTIVE PATH-C VERIFICATION (notes-only)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Cursor agent (read-only audit) + Logan Glosser <logan.glosser@gmail.com>
--
-- Prompt: cursor_prompts/CURSOR_PROMPT_mig167_mig165_retroactive_verify_20260429.md
-- Lane:   55 / mig_167
-- batch_id audited: mig_165_auxiliary_registry_hygiene_20260429
--
-- EFFECT: Registry notes appendices only.
--         No base-table mutation.
--         No verification_status / table_status mutation in this SQL.
--         No MotherDuck writes from agent — Logan/Cowork applies after review.
--
-- Read-only audit summary:
--   * mig_165 column-registry batch observed: 77 na tables / 1,306 columns.
--   * Method histogram: auto_governance_audit_table_skip 57 tables / 707 cols;
--     auto_tier1_raw_mirror_skip 12 tables / 530 cols;
--     auto_registry_governance_skip 8 tables / 69 cols.
--   * Ten table-registry CF-only rows remain not_started; all are valid analytic
--     / Tier-2 deferred verification targets.
--   * One auto-na table needs follow-up real verification: main.imaging_exam_master_v1
--     was bucketed as auto_tier1_raw_mirror_skip but has analytic per-exam rollup
--     columns (n_nodules, max_tirads, has_suspicious_nodule, largest_nodule_cm).
--     This lane opens a CF only; it intentionally does not flip statuses.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- SECTION A — Pre-snapshots for every registry row this notes-only SQL touches
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig_167_mig165_retroactive_verification_20260429 AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig167_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE batch_id = 'mig_165_auxiliary_registry_hygiene_20260429';

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_pre_mig_167_mig165_retroactive_verification_20260429 AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig167_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1
WHERE (schema_name, table_name) IN (
    SELECT * FROM (VALUES
      ('main', 'imaging_exam_master_v1'),
      ('main', 'imaging_fna_linkage_v3'),
      ('main', 'imaging_patient_summary_v1'),
      ('main', 'manuscript_cohort_v1'),
      ('main', 'patient_cross_domain_timeline_v2'),
      ('main', 'recurrence_event_clean_v1'),
      ('main', 'tumor_stage_heterogeneity_v1'),
      ('manuscript_workspace', 'episode_analysis_resolved_v1_dedup'),
      ('manuscript_workspace', 'lesion_analysis_resolved_v1'),
      ('manuscript_workspace', 'ln_master_rollup_v1'),
      ('manuscript_workspace', 'patient_analysis_resolved_v1')
    ) AS v(schema_name, table_name)
);

-- -----------------------------------------------------------------------------
-- SECTION B — Retroactive status corrections
-- -----------------------------------------------------------------------------
-- None in mig_167. This lane is notes-only by governance request. The read-only
-- audit found one auto-na table that should receive future real verification
-- (main.imaging_exam_master_v1), but this SQL opens a carry-forward instead of
-- changing verification_status/table_status.

-- -----------------------------------------------------------------------------
-- SECTION C — CF appendices / notes-only registry stamps
-- -----------------------------------------------------------------------------

-- C0 — Global mig_167 Path-C stamp on every mig_165 column-registry row.
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: retroactive Path-C read-only verification of mig_165 (2026-04-29). '
            || 'Observed 77 na tables / 1,306 cols; methods: auto_governance_audit_table_skip=57 tables/707 cols, '
            || 'auto_tier1_raw_mirror_skip=12/530, auto_registry_governance_skip=8/69. '
            || 'Ten CF-only tables remain valid deferred real-verification targets. '
            || 'One auto-na follow-up CF opened for imaging_exam_master_v1; no status mutations in notes-only lane.'
WHERE batch_id = 'mig_165_auxiliary_registry_hygiene_20260429';

-- C1 — Auto-na follow-up: imaging_exam_master_v1 was not a plain raw mirror.
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-MIG165-MISCLASSIFIED-AUTO-NA-imaging_exam_master_v1 — '
            || 'sampled in auto_tier1_raw_mirror_skip bucket, but live columns include analytic per-exam rollup fields '
            || '(n_nodules, max_tirads, has_suspicious_nodule, largest_nodule_cm); needs future real verification. '
            || 'No retroactive status flip here because mig_167 is notes-only.'
WHERE schema_name = 'main'
  AND table_name = 'imaging_exam_master_v1'
  AND batch_id = 'mig_165_auxiliary_registry_hygiene_20260429';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-MIG165-MISCLASSIFIED-AUTO-NA-imaging_exam_master_v1 — '
            || 'auto_tier1_raw_mirror_skip classification is not fully defensible for this analytic per-exam rollup; '
            || 'queue future real verification / status reconsideration outside this notes-only retro lane.'
WHERE schema_name = 'main'
  AND table_name = 'imaging_exam_master_v1';

-- C2 — The ten still-not_started CF-only tables were correctly left deferred.
UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-VALID-DEFER-imaging_fna_linkage_v3 — '
            || 'validated as analytic linkage layer (9,911 rows; score/candidate/link columns); not a tier-1 raw mirror.'
WHERE schema_name = 'main'
  AND table_name = 'imaging_fna_linkage_v3'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-VALID-DEFER-imaging_patient_summary_v1 — '
            || 'validated as patient-level imaging analytic rollup (6,126 rows; max_tirads_ever/dominant_nodule_size flags); not auto-na.'
WHERE schema_name = 'main'
  AND table_name = 'imaging_patient_summary_v1'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-VALID-DEFER-manuscript_cohort_v1 — '
            || 'validated as Tier-2 analytic manuscript composite (10,871 rows / 151 cols); requires real replay verification.'
WHERE schema_name = 'main'
  AND table_name = 'manuscript_cohort_v1'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-VALID-DEFER-patient_cross_domain_timeline_v2 — '
            || 'validated as analytic cross-domain event timeline (61,055 rows); not a governance/raw mirror table.'
WHERE schema_name = 'main'
  AND table_name = 'patient_cross_domain_timeline_v2'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-VALID-DEFER-recurrence_event_clean_v1 — '
            || 'validated as recurrence analytic event table (1,946 rows; recurrence_type/date/source flags); defer to recurrence verification lane.'
WHERE schema_name = 'main'
  AND table_name = 'recurrence_event_clean_v1'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-VALID-DEFER-tumor_stage_heterogeneity_v1 — '
            || 'validated as AJCC heterogeneity analytic rollup (8,422 rows; dominant_tumor_* and heterogeneity flags); needs real verification.'
WHERE schema_name = 'main'
  AND table_name = 'tumor_stage_heterogeneity_v1'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-VALID-DEFER-episode_analysis_resolved_v1_dedup — '
            || 'validated as episode-level resolved analytic table (9,368 rows; linkage/confidence fields); correctly deferred.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'episode_analysis_resolved_v1_dedup'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-VALID-DEFER-lesion_analysis_resolved_v1 — '
            || 'validated as lesion-level resolved analytic table (11,851 rows; tumor/pathology fields); correctly deferred.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'lesion_analysis_resolved_v1'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-VALID-DEFER-ln_master_rollup_v1 — '
            || 'validated as lymph-node analytic rollup (4,273 rows; LN counts/levels/ratio); correctly deferred.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'ln_master_rollup_v1'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-VALID-DEFER-patient_analysis_resolved_v1 — '
            || 'validated as patient-level resolved analytic composite (10,871 rows / 146 cols); correctly deferred.'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'patient_analysis_resolved_v1'
  AND table_status = 'not_started';

-- C3 — New Tier-1 LLM mirror registration spot-check.
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_167: CF-mig167-MIG165-PRESENTING-SYMPTOMS-REGISTRATION-CHECK — '
            || '23/23 registry columns match information_schema and use auto_tier1_raw_mirror_skip; classification upheld.'
WHERE schema_name = 'main'
  AND table_name = 'note_entities_llm_presenting_symptoms'
  AND batch_id = 'mig_165_auxiliary_registry_hygiene_20260429';

-- -----------------------------------------------------------------------------
-- SECTION D — Methodology vocabulary additions
-- -----------------------------------------------------------------------------
-- None. No new verification_method string introduced by mig_167.

-- End mig_167. Apply on MotherDuck RW only after Logan/Cowork review.