-- Run on MotherDuck thyroid_canonical_publication_v1_0 after M048 v3 pipeline completes.
USE thyroid_canonical_publication_v1_0;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_317',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig317',
  'mig_317: M048 v3 racial-disparities full covariate-adjusted analysis (see studies/m048_racial_disparities_tirads/v3/).'
);
