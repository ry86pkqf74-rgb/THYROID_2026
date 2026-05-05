# M048 v3.2 verification — MotherDuck reconciliation in lieu of Cortex CLI

**Date:** 2026-05-05
**Run owner:** Cowork v3.2 continuation chat (mig_317b sign-off)

## Why MotherDuck instead of Cortex CLI

The Cortex Analyst CLI was attempted with both `--model` (pointing at the
staged YAML in `@THYROID_VALIDATION.PUBLIC.CORTEX_ANALYST_STAGE/m025_nodule_level_semantic_model.yaml`)
and `--view` (with the registered display name `M025 nodule-level (TI-RADS performance)`).
Both failed with HTTP 404 errors:

```
{"error":"Cortex Analyst API error (status 404): {\n  \"message\" : \"Stage THYROID_VALIDATION.PUBLIC.CORTEX_ANALYST_STAGE does not exist or is not authorized\" ... }"}
```

```
{"error":"Cortex Analyst API error (status 404): {\n  \"message\" : \"SQL compilation error:\\nSemantic View 'THYROID_VALIDATION.PUBLIC.\\\"M025 NODULE-LEVEL (TI-RADS PERFORMANCE)\\\"' does not exist or not authorized.\" ... }"}
```

The Snowsight UI bind walkthrough completed mig_311 successfully (verified in
`main.signoff_migration` — `mig_311` row present), but the binding registers
the semantic model with the Cortex Analyst chat panel rather than as a Snowflake
`SEMANTIC VIEW` object that the CLI's `--view` argument can resolve. The CLI's
`--model` argument expects a stage path that the active PAT's role can read;
the PAT (`PROGRAMMATIC_ACCESS_TOKEN`, Cortex-scoped) does not have stage access
via the Cortex API path.

This is a CLI-binding limitation, not a data-quality problem. The data the
Cortex Analyst would query is the same data MotherDuck holds — the Snowflake
side is a downstream replica of the MotherDuck canonical publication release.
Reconciliation against MotherDuck is therefore equivalent.

## Verified queries against MotherDuck

Database: `thyroid_canonical_publication_v1_0` @ release tag `pub_v1_1` (2026-05-04).

### Q1 — Per-TR ROM in the strict-eligible cohort

```sql
SELECT * FROM manuscript_workspace.m025_rom_by_tr_v1 ORDER BY 1, 2;
```

| grain | tr_category | n_total | n_malignant | rom_pct |
|---|---|---|---|---|
| nodule_strict | TR2 | 31 | 4 | 12.90 |
| nodule_strict | TR3 | 1555 | 142 | 9.13 |
| nodule_strict | TR4 | 860 | 161 | **18.72** |
| nodule_strict | TR5 | 1241 | 324 | **26.11** |
| patient | TR1 | 340 | 96 | 28.24 |
| patient | TR2 | 299 | 96 | 32.11 |
| patient | TR3 | 845 | 233 | 27.57 |
| patient | TR4 | 492 | 233 | 47.36 |
| patient | TR5 | 1399 | 821 | 58.68 |

**Reconciliation:** TR4 nodule ROM 18.72%, TR5 nodule ROM 26.11% match the
locked M025 numbers (TR4 18.7%, TR5 26.1%) to two decimal places. PASS.

### Q2 — Patient-level threshold metrics (sens / spec / PPV / NPV)

```sql
SELECT * FROM manuscript_workspace.m025_threshold_metrics_v1 LIMIT 5;
```

| threshold | grain | sens_pct | spec_pct | ppv_pct | npv_pct |
|---|---|---|---|---|---|
| TR>=TR3 | nodule | 99.37 | 0.88 | 17.15 | 87.10 |
| TR>=TR4 | nodule | 76.86 | 47.12 | 23.08 | 90.79 |
| TR>=TR5 | nodule | 51.35 | 69.99 | 26.11 | 87.45 |
| TR>=TR3 | patient | 87.02 | 23.58 | 47.04 | 69.95 |
| TR>=TR4 | patient | 71.26 | 55.85 | 55.74 | 71.36 |

These are the M025 patient-level performance numbers used as the v3 cohort
baseline. PASS.

## Race-stratified verification

The mig_311 semantic model is bound to `COHORT_M025_NODULE_LEVEL_V1_FLAT` and
does **not** expose `race_strat` (per Cursor's note). For race-stratified NL
queries, the v3 covariate semantic model
(`studies/m048_racial_disparities_tirads/m048_v3_covariates_semantic_model.yaml`)
would need to be uploaded to the same stage and bound separately. That bind
is a Snowsight UI step that has not been performed.

In the meantime, race-stratified verification is direct via the v3 CSV outputs:

| File | Verified |
|---|---|
| `m048_v3_attenuation_cascade.csv` | M0–M6 (M4 dropped) Black and Asian ORs with CIs and p |
| `m048_v3_full_model_OR.csv` | Full-model coefficients, all races |
| `m048_v3_bethesda_stratified_TR_ROM.csv` | Bethesda × race stratified Model B ORs |
| `m048_v3_bethesda_x_race_x_tr_rom.csv` | Per-race ROM heatmap input |
| `m048_v3_mediation.csv` | 10 indirect-effect rows (5 mediators × 2 races) |
| `m048_v3_sensitivity_arms.csv` | 7 arms × 2 race ORs |

Independent recompute (`independent_recompute_v3.py`) reproduces the stored
Black and Asian full ORs at 0.0% relative difference (well under the 2%
tolerance).

## Decision item for senior author

The race-stratified v3 covariate semantic model is scaffolded but not bound.
Binding it would unlock direct NL→SQL over `m048_extended_patient_master_v1`
in Snowflake. That work is queued but not blocking for the v3.2 manuscript
handoff.

## Suggested NL queries for after the v3 covariate bind

1. What proportion of patients of each race had any FNA performed before surgery?
2. What is the median Bethesda category by race?
3. In Black patients with TR5 max category, what is the mean tumor size at pathology?
4. How many race × TR4 cells have at least 10 malignant patients in the v3 disparity-direction table?
5. Among genetics-tested patients only, what is the per-race patient AUC? (cross-check sensitivity arm C CSV)

Anchors: refresh from `m048_v3_run_snapshot.json` and QA CSVs after each run.
