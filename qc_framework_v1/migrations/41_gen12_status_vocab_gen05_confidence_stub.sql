-- ============================================================================
-- Migration 41 — GEN12 status fields vocab normalization + GEN05 confidence stub
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:
--   GEN12 — multiple *_status columns with mixed case, empty strings, and
--           freetext values ("no result", "no result due to inadequate rn", "N/A")
--   GEN05 — molecular_confidence DOUBLE is NULL on ALL 1,384 rows (stub
--           column, no upstream population). Advisory-only — no per-row queue.
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Normalization applied (view layer, no mutation of canonical):
--   Rule 1: TRIM + empty-string → NULL
--   Rule 2: ILIKE 'positive'  → 'Positive'
--   Rule 3: ILIKE 'negative'  → 'Negative'
--   Rule 4: ILIKE 'failed'    → 'Failed'
--   Rule 5: ILIKE 'inadequate' OR 'no result%' → 'Inadequate'
--   Rule 6: ILIKE 'n/a'       → 'NotApplicable'
--   Rule 7: 'Positive_high' / 'Positive_low' kept as-is (CNA tier detail)
--   Rule 8: specimen_adequacy_norm uppercase tokens kept (ADEQUATE / LIMITED /
--           LOW_THYROID_CELL_CONTENT / DNA_ADEQUATE_RNA_FAILED / INADEQUATE)
-- Rows whose raw value does NOT match any rule keep the original value AND
-- raise gen12_status_nonstandard_flag for hand-review.
--
-- Columns surfaced on the clean view:
--   gene_mutations_status_norm, gene_fusions_status_norm, cna_status_norm,
--   gep_status_norm, parathyroid_status_norm, medullary_status_norm
--   gen12_status_nonstandard_flag   BOOLEAN (any column had a vocab-miss)
--   gen05_molecular_confidence_stub_flag BOOLEAN (always TRUE — advisory)
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_molecular_genetics_v2_status_clean AS
WITH normed AS (
  SELECT
    g.*,
    -- helper normalizer inlined per column
    CASE
      WHEN g.gene_mutations_status IS NULL OR LENGTH(TRIM(g.gene_mutations_status))=0 THEN NULL
      WHEN LOWER(TRIM(g.gene_mutations_status))='positive' THEN 'Positive'
      WHEN LOWER(TRIM(g.gene_mutations_status))='negative' THEN 'Negative'
      WHEN LOWER(TRIM(g.gene_mutations_status))='failed' THEN 'Failed'
      WHEN LOWER(TRIM(g.gene_mutations_status)) LIKE 'no result%' OR LOWER(TRIM(g.gene_mutations_status)) LIKE '%inadequate%' THEN 'Inadequate'
      WHEN LOWER(TRIM(g.gene_mutations_status))='n/a' THEN 'NotApplicable'
      ELSE g.gene_mutations_status
    END AS gene_mutations_status_norm,
    CASE
      WHEN g.gene_fusions_status IS NULL OR LENGTH(TRIM(g.gene_fusions_status))=0 THEN NULL
      WHEN LOWER(TRIM(g.gene_fusions_status))='positive' THEN 'Positive'
      WHEN LOWER(TRIM(g.gene_fusions_status))='negative' THEN 'Negative'
      WHEN LOWER(TRIM(g.gene_fusions_status))='failed' THEN 'Failed'
      WHEN LOWER(TRIM(g.gene_fusions_status)) LIKE 'no result%' OR LOWER(TRIM(g.gene_fusions_status)) LIKE '%inadequate%' THEN 'Inadequate'
      WHEN LOWER(TRIM(g.gene_fusions_status))='n/a' THEN 'NotApplicable'
      ELSE g.gene_fusions_status
    END AS gene_fusions_status_norm,
    CASE
      WHEN g.cna_status IS NULL OR LENGTH(TRIM(g.cna_status))=0 THEN NULL
      WHEN LOWER(TRIM(g.cna_status))='positive' THEN 'Positive'
      WHEN LOWER(TRIM(g.cna_status))='negative' THEN 'Negative'
      WHEN LOWER(TRIM(g.cna_status))='positive_high' THEN 'Positive_high'
      WHEN LOWER(TRIM(g.cna_status))='positive_low'  THEN 'Positive_low'
      WHEN LOWER(TRIM(g.cna_status))='failed' THEN 'Failed'
      WHEN LOWER(TRIM(g.cna_status)) LIKE 'no result%' OR LOWER(TRIM(g.cna_status)) LIKE '%inadequate%' THEN 'Inadequate'
      WHEN LOWER(TRIM(g.cna_status))='n/a' THEN 'NotApplicable'
      ELSE g.cna_status
    END AS cna_status_norm,
    CASE
      WHEN g.gep_status IS NULL OR LENGTH(TRIM(g.gep_status))=0 THEN NULL
      WHEN LOWER(TRIM(g.gep_status))='positive' THEN 'Positive'
      WHEN LOWER(TRIM(g.gep_status))='negative' THEN 'Negative'
      WHEN LOWER(TRIM(g.gep_status))='failed' THEN 'Failed'
      WHEN LOWER(TRIM(g.gep_status)) LIKE 'no result%' OR LOWER(TRIM(g.gep_status)) LIKE '%inadequate%' THEN 'Inadequate'
      WHEN LOWER(TRIM(g.gep_status))='n/a' THEN 'NotApplicable'
      ELSE g.gep_status
    END AS gep_status_norm,
    CASE
      WHEN g.parathyroid_status IS NULL OR LENGTH(TRIM(g.parathyroid_status))=0 THEN NULL
      WHEN LOWER(TRIM(g.parathyroid_status))='positive' THEN 'Positive'
      WHEN LOWER(TRIM(g.parathyroid_status))='negative' THEN 'Negative'
      WHEN LOWER(TRIM(g.parathyroid_status))='failed' THEN 'Failed'
      WHEN LOWER(TRIM(g.parathyroid_status)) LIKE 'no result%' OR LOWER(TRIM(g.parathyroid_status)) LIKE '%inadequate%' THEN 'Inadequate'
      WHEN LOWER(TRIM(g.parathyroid_status))='n/a' THEN 'NotApplicable'
      ELSE g.parathyroid_status
    END AS parathyroid_status_norm,
    CASE
      WHEN g.medullary_status IS NULL OR LENGTH(TRIM(g.medullary_status))=0 THEN NULL
      WHEN LOWER(TRIM(g.medullary_status))='positive' THEN 'Positive'
      WHEN LOWER(TRIM(g.medullary_status))='negative' THEN 'Negative'
      WHEN LOWER(TRIM(g.medullary_status))='failed' THEN 'Failed'
      WHEN LOWER(TRIM(g.medullary_status)) LIKE 'no result%' OR LOWER(TRIM(g.medullary_status)) LIKE '%inadequate%' THEN 'Inadequate'
      WHEN LOWER(TRIM(g.medullary_status))='n/a' THEN 'NotApplicable'
      ELSE g.medullary_status
    END AS medullary_status_norm
  FROM main.canonical_molecular_genetics_v2 g
)
SELECT
  n.*,
  -- non-standard = any normalized value NOT in the canonical vocab (still has
  -- the original freetext because no rule matched)
  (gene_mutations_status_norm  NOT IN ('Positive','Negative','Failed','Inadequate','NotApplicable')
   OR gene_fusions_status_norm NOT IN ('Positive','Negative','Failed','Inadequate','NotApplicable')
   OR cna_status_norm          NOT IN ('Positive','Negative','Positive_high','Positive_low','Failed','Inadequate','NotApplicable')
   OR gep_status_norm          NOT IN ('Positive','Negative','Failed','Inadequate','NotApplicable')
   OR parathyroid_status_norm  NOT IN ('Positive','Negative','Failed','Inadequate','NotApplicable')
   OR medullary_status_norm    NOT IN ('Positive','Negative','Failed','Inadequate','NotApplicable'))
    AS gen12_status_nonstandard_flag,
  (molecular_confidence IS NULL) AS gen05_molecular_confidence_stub_flag
FROM normed n;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='GEN12';

-- GEN12: queue rows with any non-vocab status value post-normalization
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'GEN12',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_molecular_genetics_v2',
  CAST(molecular_episode_id AS VARCHAR),
  TO_JSON(struct_pack(
    platform := platform,
    gene_mutations_status := gene_mutations_status,
    gene_fusions_status := gene_fusions_status,
    cna_status := cna_status,
    gep_status := gep_status,
    parathyroid_status := parathyroid_status,
    medullary_status := medullary_status,
    gene_mutations_status_norm := gene_mutations_status_norm,
    medullary_status_norm := medullary_status_norm
  )),
  'GEN12 status field outside canonical vocab after normalization — hand-map needed',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_molecular_genetics_v2_status_clean
WHERE gen12_status_nonstandard_flag;

-- GEN05: advisory only. No queue (100% stub — writing 1,384 identical queue rows is noise).

COMMENT ON TABLE main.canonical_molecular_genetics_v2 IS
'Molecular genetics master (1,384 rows). Three clean views: ..._platform_clean (mig 39), ..._date_clean (mig 40), ..._status_clean (mig 41). Status clean normalizes 6 *_status columns to {Positive, Negative, Failed, Inadequate, NotApplicable, Positive_high, Positive_low} + surfaces gen12_status_nonstandard_flag. molecular_confidence is STUB (NULL on all 1,384) — gen05_molecular_confidence_stub_flag always TRUE pending scoring model. 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_40';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_molecular_genetics_v2','table',
   'manuscript_workspace.canonical_molecular_genetics_v2_status_clean',
   'GEN12,GEN05','prompt_40','column_only',DATE '2026-04-23',
   'GEN12: empty-string / mixed-case / freetext values across 6 *_status columns — normalized at view layer to canonical vocab. Non-match rows surfaced via gen12_status_nonstandard_flag and queued. GEN05: molecular_confidence NULL on all 1,384 rows (stub, no upstream model output). Advisory flag only — no queue emission (100% uniform).',
   NULL,
   'Vocab = {Positive, Negative, Failed, Inadequate, NotApplicable}; CNA additionally accepts {Positive_high, Positive_low}. specimen_adequacy_norm left as-is (its enum is intentionally different). GEN05 awaits downstream confidence-scoring model; not in scope here.');
