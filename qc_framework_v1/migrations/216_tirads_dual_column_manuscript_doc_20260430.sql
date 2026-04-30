-- mig_216 — Document manuscript-facing semantics for dual TIRADS category columns
-- Target: thyroid_canonical_publication_v1_0
-- Memory: memory/feedback_tirads_category_canonical.md
--
USE thyroid_canonical_publication_v1_0;

-- §A Column comments (DDL truth for analysts)
COMMENT ON COLUMN main.canonical_us_nodule_v2.acr2017_tirads_category IS
'Manuscript PRIMARY surface — strict ACR TI-RADS 2017 category from acr2017_tirads_points (Tessler 2017 JACR): TR1=0 pts, TR2=2, TR3=3, TR4=4-6, TR5>=7. No ACR band for total points=1 (NULL after mig_215).';

COMMENT ON COLUMN main.canonical_us_nodule_v2.updated_tirads_category IS
'Manuscript SENSITIVITY / secondary surface — institutional Emory updated TI-RADS tier (legacy tirads_category_v2 + v2/LLM overlays). Compare to acr2017_tirads_category via acr2017_vs_updated_concordant.';

-- §B Column verification registry — long-form notes (Logan ratified dual reportability)
UPDATE main.canonical_column_verification_registry_v1
SET notes = 'Manuscript primary surface — strict ACR 2017 TI-RADS (Tessler et al. 2017 JACR): '
            || 'TR1 = 0 points, TR2 = 2, TR3 = 3, TR4 = 4-6, TR5 >= 7. '
            || 'Total points = 1 has no ACR 2017 band; category must be NULL (mig_215). '
            || 'Derives from acr2017_tirads_points / feature points; see Script 374 / 376.',
    verification_method = COALESCE(verification_method, 'verified')
                        || '|mig_216_manuscript_primary_acr2017_documented_20260430'
WHERE schema_name = 'main'
  AND table_name = 'canonical_us_nodule_v2'
  AND column_name = 'acr2017_tirads_category';

UPDATE main.canonical_column_verification_registry_v1
SET notes = 'Manuscript sensitivity-analysis surface — institutional / Emory “updated” TI-RADS '
            || 'category (legacy tirads_category_v2 path, tirads_v2_nodules_raw, Script 377/378 absorption). '
            || 'Not interchangeable with acr2017_tirads_category; both reportable in supplementary methods.',
    verification_method = COALESCE(verification_method, 'verified')
                        || '|mig_216_manuscript_sensitivity_updated_tirads_documented_20260430'
WHERE schema_name = 'main'
  AND table_name = 'canonical_us_nodule_v2'
  AND column_name = 'updated_tirads_category';

-- §C Provenance
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_216_tirads_dual_column_manuscript_doc_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'canonical_us_nodule_v2_comment_on_column_acr2017_and_updated_tirads_category_registry_notes',
   'MANUSCRIPT_FACING_DUAL_TIRADS_DOC',
   'column_verification_registry_v1_notes_acr2017_updated_tirads',
   'memory_feedback_tirads_category_canonical_md',
   'none');

-- §D Post-check
SELECT column_name, LEFT(notes, 120) AS notes_preview
FROM main.canonical_column_verification_registry_v1
WHERE table_name = 'canonical_us_nodule_v2'
  AND column_name IN ('acr2017_tirads_category', 'updated_tirads_category');
