-- ============================================================================
-- Migration 51 — GEN16: braf_variant derivation from gene_mutations_variants
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      GEN16 — 180 molecular rows carry braf_flag=TRUE but
--                braf_variant=NULL, while the structured gene_mutations_variants
--                column already holds the BRAF protein change (mostly p.V600E).
--                Also 2 stored braf_variant values disagree with the STRUCT
--                (V600 stored vs V600L derived; both legit V600-domain hits).
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Probe (2026-04-24):
--   braf_flag=TRUE total:              333
--     braf_variant stored non-null:    153
--       149 agree (V600E + p.V600E)
--         2 stored V600E, STRUCT empty for BRAF protein (struct omission)
--         1 V600 / p.V600L (position partial → V600L derivation is richer)
--         1 V600 / V600    (idem — protein lacks p. prefix)
--     braf_variant stored NULL:        180
--       176 p.V600E + 1 pV600E  → derive 'V600E' (177 recovered V600E)
--         2 p.K601E              → derive 'K601E' (non-V600E BRAF — queue)
--         1 p.V600                → derive 'V600'  (partial; queue non-V600E)
--   Multi-protein BRAF rows: 0 — safe to pick single protein per episode.
--
--   Non-V600E set (queued under GEN16): 2 K601E + 1 V600 recovered + 1 V600
--   stored/V600 derived + 1 V600 stored/V600L derived = 5 rows.
--
-- Derivation rule:
--   For each braf_flag=TRUE row, pick the single BRAF protein from
--   gene_mutations_variants (unique per episode), then normalize:
--     - strip leading 'p.' or 'p'
--     - upper-case
--   Yields braf_variant_derived ∈ {V600E, K601E, V600, V600L, ...}.
--
-- Output:
--   manuscript_workspace.canonical_molecular_genetics_v2_braf_variant (VIEW)
--     + braf_variant_derived              VARCHAR
--     + gen16_braf_variant_recovered_flag BOOLEAN (stored NULL, derived NN)
--     + gen16_braf_variant_disagree_flag  BOOLEAN (both NN, disagree)
--     + gen16_braf_non_v600e_flag         BOOLEAN (derived ∉ {V600E, NULL})
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_molecular_genetics_v2_braf_variant AS
WITH braf_proteins AS (
  SELECT m.research_id, m.molecular_episode_id,
         MAX(v.protein) AS braf_protein_raw  -- single value per episode (probe confirms 0 multi)
  FROM main.canonical_molecular_genetics_v2 m,
       UNNEST(m.gene_mutations_variants) AS t(v)
  WHERE m.braf_flag=TRUE AND UPPER(v.gene)='BRAF' AND v.protein IS NOT NULL
  GROUP BY 1,2
),
normalized AS (
  SELECT research_id, molecular_episode_id,
         braf_protein_raw,
         -- Strip 'p.' or 'p' prefix, uppercase
         UPPER(REGEXP_REPLACE(braf_protein_raw, '^p\.?', '')) AS braf_variant_derived
  FROM braf_proteins
)
SELECT
  m.*,
  n.braf_variant_derived,
  (m.braf_flag AND m.braf_variant IS NULL AND n.braf_variant_derived IS NOT NULL)
    AS gen16_braf_variant_recovered_flag,
  (m.braf_flag AND m.braf_variant IS NOT NULL AND n.braf_variant_derived IS NOT NULL
   AND UPPER(m.braf_variant) <> n.braf_variant_derived)
    AS gen16_braf_variant_disagree_flag,
  (m.braf_flag AND n.braf_variant_derived IS NOT NULL AND n.braf_variant_derived <> 'V600E')
    AS gen16_braf_non_v600e_flag
FROM main.canonical_molecular_genetics_v2 m
LEFT JOIN normalized n
  ON n.research_id = m.research_id
 AND n.molecular_episode_id IS NOT DISTINCT FROM m.molecular_episode_id;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='GEN16';

-- Queue only the non-V600E BRAF hits (2 K601E rows + any V600L-style hits) —
-- these are clinically distinct from canonical V600E and warrant chart review.
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'GEN16', TRY_CAST(research_id AS INTEGER),
  'main.canonical_molecular_genetics_v2',
  CAST(molecular_episode_id AS VARCHAR),
  TO_JSON(struct_pack(
    braf_variant_stored := braf_variant,
    braf_variant_derived := braf_variant_derived
  )),
  'GEN16 non-V600E BRAF variant (K601E/V600L/V600) — clinically distinct from canonical V600E',
  'open', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_molecular_genetics_v2_braf_variant
WHERE gen16_braf_non_v600e_flag;

COMMENT ON TABLE main.canonical_molecular_genetics_v2 IS
'Molecular genetics canonical (1,384 events / 1,151 pts). Clean view manuscript_workspace.canonical_molecular_genetics_v2_fna_rebind surfaces fna_index_rebound + GEN15 rebind confidence. Clean view manuscript_workspace.canonical_molecular_genetics_v2_braf_variant surfaces braf_variant_derived + gen16_braf_variant_recovered_flag (180: 177 V600E + 2 K601E + 1 V600) + gen16_braf_variant_disagree_flag (1) + gen16_braf_non_v600e_flag (5). Non-V600E hits queued. 2026-04-24.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_50';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_molecular_genetics_v2.braf_variant','column',
   'manuscript_workspace.canonical_molecular_genetics_v2_braf_variant.braf_variant_derived',
   'GEN16','prompt_50','column_only',DATE '2026-04-24',
   'GEN16: 180 braf_flag=TRUE rows with braf_variant=NULL recovered via gene_mutations_variants STRUCT (177 V600E incl. pV600E typo + 2 K601E + 1 V600). 1 true stored/derived disagreement (V600 stored, V600L derived) — treat derived as authoritative. 5 non-V600E rows (2 K601E + 1 V600 recovered + 1 V600 stored/V600 derived + 1 V600/V600L disagree) queued under GEN16 as clinically distinct variants.',
   NULL,
   'Downstream BRAF genotype analyses should use braf_variant_derived. V600E-specific cohort filter: braf_variant_derived=''V600E''. Non-V600E cohort (K601E, V600, V600L) available via gen16_braf_non_v600e_flag. Original braf_variant preserved on main for audit.');
