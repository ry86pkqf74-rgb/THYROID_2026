# mig_249 — manuscript_feasibility_v1 re-refresh (post-mig_248)

## Summary

- Rows refreshed: 83 (expected 83)
- Applied to MotherDuck: True
- Broken cohort COUNT after mig_248: 0

## Color transitions

| RED->RED | 46 |
| GREEN->GREEN | 27 |
| YELLOW->YELLOW | 5 |
| RED->YELLOW | 5 |

## Gained GREEN

[]

## Worsened vs prior flag

[]

## Cohort regressions (must be empty)

_(none — all cohort COUNT paths succeeded.)_

## MotherDuck verification (post-apply §4 prompt)

Execute on `thyroid_canonical_publication_v1_0`:

- `canonical_version_at_scoring = 'v1_0_post_mig_248'`: **83 / 83** rows.
- Color distribution vs post-mig_247 baseline (**27 GREEN / 5 YELLOW / 51 RED**):
  **27 GREEN / 10 YELLOW / 46 RED** — five manuscripts moved **RED→YELLOW** via resolved cohort-parent columns (`tumor_size_cm`, `tirads_best_category_v12`, etc.); none met **GREEN** thresholds (still have key_variable coverage \<80%, e.g. histology \(\sim\)38%, TIRADS category \(\sim\)30%).
- Stale-flag audit: `cohort SELECT failed` **0**, isthmus+Binder **0**, `tirads_best_category_v12 MISSING` **0**, `tumor_size_cm MISSING` **0**.
- `manuscript_dashboard_VIEW_v1`: READY_TO_DRAFT stayed **3** (restoration manuscripts improved to YELLOW, not READY_TO_DRAFT under current dashboard rules).

## RED→YELLOW manuscript IDs (**5**)

**25, 29, 37, 45, 75** — aliases on `cohort_descriptive_full_cohort_v1` removed column-MISSING blockers; manuscripts remain YELLOW (not GREEN) because ≥1 key_variable still has \<80% coverage (e.g. `histology_final` \(\sim\)38%, `tirads_best_category_v12` \(\sim\)30%).

**Still RED (examples):** M030 (mol coverage \(\sim\)9%), M043 (`total_ln_positive_v10` \(\sim\)12%).

