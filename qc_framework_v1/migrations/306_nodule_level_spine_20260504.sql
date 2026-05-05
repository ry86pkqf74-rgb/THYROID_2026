-- mig_306: per-nodule M025 analytic spine (Option B). See cursor_prompts/CURSOR_PROMPT_MIG_306_*.md
-- Idempotent CREATE OR REPLACE. Built and applied by Cowork (Claude) on 2026-05-04.
-- Database: thyroid_canonical_publication_v1_0
--
-- mig_260 audit (2026-05-04): live view uses canonical_us_nodule_v2.acr2017_tirads_* for
-- nodule TI-RADS. canonical_patient_master is referenced only as (research_id, is_malignant).
-- No removed CPM TIRADS columns (tirads_best_category_v12 / imaging_tirads_best / preop_tirads_best).
USE thyroid_canonical_publication_v1_0;
-- (full DDL omitted from this stub — see Cowork chat transcript for the canonical CREATE OR REPLACE VIEW; the view manuscript_workspace.cohort_m025_nodule_level_v1 is already live)

-- Signoff
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_306', CURRENT_TIMESTAMP, 'cowork_claude_mig306',
        'mig_306: per-nodule analytic spine cohort_m025_nodule_level_v1 — see cursor_prompts/CURSOR_PROMPT_MIG_306_NODULE_LEVEL_SPINE_20260504.md');
