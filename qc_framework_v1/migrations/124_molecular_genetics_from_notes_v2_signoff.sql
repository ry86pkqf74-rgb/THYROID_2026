-- =============================================================================
-- Migration 124 — canonical_molecular_genetics_from_notes_v2 SIGN-OFF (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29 (UTC — Lane 16 / molecular family close-out)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Scope: Notes-derived molecular **mentions** layer (mention grain), 28 cols.
--   17 not_started columns verified here; 11 remain na (identifiers / linkage /
--   LLM metadata echo / provenance timestamps as tagged in registry).
--
-- Build origin (archival replay):
--   Same column projection as `molecular_consolidation_20260421/08_build_master.py`
--   Step `molecular_genetics_from_notes_v2`: SELECT … FROM note_entities_genetics.
--   Live publication upstream is archived at:
--     "Thyroid 2026 UPdated".molecular_legacy_20260421.note_entities_genetics
--   (1,738 rows). main.* no longer carries note_entities_genetics; verification
--   is extraction-faithfulness vs that archive snapshot (not a live main mirror).
--
-- Pre-signoff probes (MotherDuck thyroid_canonical_publication_v1_0, 2026-04-29):
--   * Row / patient parity: 1,738 rows / 605 patients.
--   * Natural key: COUNT(*) = COUNT(DISTINCT (research_id, note_row_id,
--     evidence_start, entity_value_raw)) = 1,738.
--   * Extraction-faithfulness (CAST(research_id AS VARCHAR) on upstream):
--       - 14-col multiset (entity_type, entity_value_{raw,norm}, present_or_negated,
--         confidence, confidence_score, evidence_{span,start,end}, extraction_method,
--         extractor_{name,version}): EXCEPT ALL both directions = 0.
--       - llm_prompt_version triple + verification_{status,step} triples: 0 drift.
--   * confidence: constant 0.9 on all rows (regex extraction tier).
--   * confidence_score: NULL on all 1,738 rows upstream and canonical (derived
--     placeholder; not equal to confidence — documented, extraction-faithful).
--   * Vocabulary: entity_type = {gene}; present_or_negated = {present, negated};
--     verification_status = {unverified}; extraction_method = {regex}.
--   * Build provenance: builder_version = 'v3_2026-04-21' (1 distinct);
--     built_at single batch stamp (uniform across rows).
--
-- Cross-check vs canonical_molecular_genetics_v2 (mig_116 master):
--   * DISTINCT fn patients: 605. Patients with ≥1 master row sharing research_id: 372.
--   * 233/605 note-mention patients have no structured master row — EXPECTed:
--     README / GEN10: mentions layer is not coextensive with formal test episodes.
--     Not a merge defect; do not join as peer of canonical_molecular_genetics_v2.
--
-- Carry-forwards (non-blocking):
--   CF-mig124-MGFN-BUILT-AT-TZ-RETYPE — built_at is TIMESTAMP WITH TIME ZONE;
--     align to plain TIMESTAMP in a future clinical_date / build_ts cleanup (mig_117
--     audit allowlist already permits gate 5).
--   CF-mig124-MGFN-MASTER-OVERLAP — notes-only patients without structured
--     molecular_genetics_v2 rows; use manuscript_workspace.molecular_mentions_from_notes_v2
--     for mention analytics, not parity with master N.
--
-- Executed against MotherDuck thyroid_canonical_publication_v1_0 via
-- connect_locked() after independent probe run.
-- =============================================================================

-- 124a — Mention-grain source cluster + adjudicated echo columns: archive replay
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_molecular_legacy_note_entities_genetics',
    batch_id            = 'mig_124_molecular_genetics_from_notes_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_124: multiset EXCEPT ALL vs '
                          || '"Thyroid 2026 UPdated".molecular_legacy_20260421.'
                          || 'note_entities_genetics (1,738/1,738 rows; VARCHAR research_id). '
                          || 'Natural key + 14-col source cluster + llm_prompt_version + '
                          || 'verification_status + verification_step: 0 drift both directions. '
                          || 'confidence=0.9 uniform; confidence_score NULL extraction-faithful.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_molecular_genetics_from_notes_v2'
  AND verification_status = 'not_started'
  AND column_name IN (
    'entity_type',
    'entity_value_raw',
    'entity_value_norm',
    'present_or_negated',
    'confidence',
    'confidence_score',
    'evidence_span',
    'evidence_start',
    'evidence_end',
    'extraction_method',
    'extractor_name',
    'extractor_version',
    'llm_prompt_version',
    'verification_status',
    'verification_step'
  );

-- 124b — Batch build provenance
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'build_provenance_consistency',
    batch_id            = 'mig_124_molecular_genetics_from_notes_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_124: Uniform builder_version v3_2026-04-21; '
                          || 'single built_at batch stamp. CF-mig124-MGFN-BUILT-AT-TZ-RETYPE '
                          || 'for TIMESTAMP WITH TIME ZONE → TIMESTAMP housekeeping.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_molecular_genetics_from_notes_v2'
  AND verification_status = 'not_started'
  AND column_name IN ('built_at', 'builder_version');

-- 124c — Table signoff registry (recompute from column registry)
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
    signoff_migration = 'qc_framework_v1/migrations/124_molecular_genetics_from_notes_v2_signoff.sql',
    notes             = 'mig_124: Protocol v2 notes molecular mentions layer closed. '
                        || '1,738 rows / 605 pts; extraction-faithful vs molecular_legacy '
                        || 'note_entities_genetics; 372/605 pts also in canonical_molecular_'
                        || 'genetics_v2 (233 mentions-only, expected). CF TZ built_at; CF master overlap.'
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
    AND table_name = 'canonical_molecular_genetics_from_notes_v2'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- =============================================================================
-- end migration 124 — canonical_molecular_genetics_from_notes_v2 verified
-- =============================================================================
