# `acr2017_feature_points_complete` — what the flag actually means

**Ratified documentation pass:** `mig_221` (2026-04-30), Lane E6 Round 2.

## Why this flag disagrees with “all five `*_pts` columns are non-NULL”

| Concept | Meaning |
|--------|---------|
| **`acr2017_feature_points_complete`** | Carried from `canonical_us_nodule_characteristics_v1.tirads_score_component_complete`, populated in Script **271** (`scripts/frozen/271_tirads_imaging_finalization.py`). **TRUE iff** all five **ACR 2017 component descriptor** columns were non-NULL on the **characteristics** row: `composition`, `echogenicity`, `shape`, `margins`, `calcifications`. That is a *source-row completeness* gate, not a “sum of points exists” gate. |
| **All five `composition_pts` … `foci_pts` non-NULL on `canonical_us_nodule_v2`** | Often true after Script **376**, which **recomputes** per-feature points from normalized feature strings on the merged v2 row. Imputation can fill points **without** retroactively changing this legacy boolean (Script 376 does not redefine `acr2017_feature_points_complete`). |

So the ~4× gap called out in the ChatGPT TIRADS doc (**~21k rows with full point columns vs ~5.1k with the flag TRUE**) is **expected**: the flag encodes **pre-imputation CUNC descriptor completeness**, while `*_pts` can be completed later from merged / normalized labels.

## Manuscript guidance

- **Primary strict ACR 2017 per-nodule cohort:** filter `acr2017_feature_points_complete = TRUE` **and** require `acr2017_tirads_points` / `acr2017_tirads_category` as needed. The view **`manuscript_workspace.vw_us_nodule_tirads_strict_acr2017_VIEW_v1`** (`mig_219`) encodes that pattern on top of `canonical_us_nodule_v2_filtered`.
- **Sensitivity / broader “any reported TIRADS” surfaces:** use **`vw_us_nodule_tirads_any_reported_VIEW_v1`** or the “reported but not fully parsed” view — do **not** confuse those denominators with the strict completeness flag.

## If Logan wants a second flag

A **new** derived column (e.g. `acr2017_all_pts_columns_nonnull`) could be added in a future migration to mean “five `*_pts` all non-NULL today”—**do not overload** `acr2017_feature_points_complete` without an explicit ratified semantic change.

**Related:** `memory/feedback_tirads_category_canonical.md` (dual `acr2017_*` vs `updated_tirads_category`); `mig_215` / `mig_216` (ACR band + dual-column documentation).
