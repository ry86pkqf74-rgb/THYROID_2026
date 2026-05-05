-- Run on MotherDuck thyroid_canonical_publication_v1_0 after M048 v3.1 pipeline completes.
USE thyroid_canonical_publication_v1_0;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_317b',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig317b',
  'mig_317b: M048 v3.1 surgical fixes — Asian mediation arm, Asian assertion in independent_recompute, Figure 13 subplots, Bethesda x race x TR ROM table + Figure 12b heatmap, Bethesda x TR interaction secondary model. mig_317 primary signoff unchanged.'
);
