-- mig_278: Backfill signoff_migration row for mig_252 (comp_*_confirmed rollup fix)
-- Registry-only: no canonical table DML. Applied on MotherDuck thyroid_canonical_publication_v1_0 after verification.
-- Drift probe (rollup CPM vs strict canonical_complications_events_v1) must show 0 for all five comps.
-- See cursor_prompts/CURSOR_PROMPT_MIG_278_252_BACKFILL_SIGNOFF_20260503.md
-- Closes: CF-COMP-CONFIRMED-ROLLUP-BUG

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_252', CURRENT_TIMESTAMP, 'cursor_composer_mig278_backfill',
 'mig_252 (comp_*_confirmed rollup fix): NO-OP backfill signoff. CPM rollup numbers verified by Cowork 2026-05-03 to already match canonical_complications_events_v1 strict definition (finding_status=present AND evidence_strength IN (definitive,probable)). Drift=0 for seroma/hematoma/rln_injury/chyle_leak/hypoparathyroidism. Bug was fixed by downstream rebuild (likely mig_265 PMH definitive tier rebuild side-effect); SQL file qc_framework_v1/migrations/252_comp_confirmed_rollup_fix_20260501.sql was never applied but is no longer needed. Closes CF-COMP-CONFIRMED-ROLLUP-BUG.');
