# mig_185 — apply rollup-only patch (option B)

**Date:** 2026-04-30  
**Batch:** `mig185_apply_rollup_only_patch_20260430`  
**Author:** Logan Glosser \<logan.glosser@gmail.com\>  
**Predecessor:** mig_185 scoping (commit `5b64cfb`); duplicate scoping report `qc_framework_v1/reports/mig_185_path_malignant_duplicate_scoping_20260430.md`

---

## §1 Logan-ratified rule (verbatim)

**Do NOT dedupe `canonical_path_malignant_events_v1`.** The 533 excess rows are clinically/source-distinct and removing them loses information.

**Patch `canonical_path_malignant_patient_rollup_v1` and any other consumer that uses `COUNT(*)` over event rows where the correct semantics are `COUNT(DISTINCT (research_id, surgery_episode_id, tumor_ordinal))`.**

Document the residual 533 source-distinct rows via a new CF tag and a column on the events table (`is_source_distinct_duplicate_grain BOOLEAN` or similar) that flags them so analyst SQL can choose the right grain explicitly.

---

## §2 affected-column audit (`canonical_path_malignant_patient_rollup_v1`)

Source: `scripts/361_op_path_consolidation.py` Step 5 (`step_5_build_rollups`, malignant rollup).

| column_name | current_logic | inflated_yes_no | proposed_fix |
|-------------|---------------|-----------------|--------------|
| research_id | `GROUP BY research_id` | no | none |
| any_malignant_event | constant `TRUE` in agg | no | none |
| n_malignant_surgeries | `COUNT(DISTINCT surgery_episode_id)` | no | none |
| n_tumors_total | `COUNT(*)` over all event rows | **yes** | `COUNT(DISTINCT (surgery_episode_id, tumor_ordinal))` per patient |
| earliest_malignant_path_date | `MIN(surgery_date)` | no | none |
| latest_malignant_path_date | `MAX(surgery_date)` | no | none |
| highest_stage_ajcc8 | `MAX(stage_group_ajcc8)` | no | none |
| highest_stage_ajcc7 | `MAX(stage_group_ajcc7)` | no | none |
| any_ett | `BOOL_OR` over ETE / gross_ete | no | none |
| any_metastasis | constant `FALSE` | no | none |
| dominant_histology | `mode(primary_histology)` over **all** event rows | **yes** (row-count skew) | `mode()` after collapsing to one row per `(surgery_episode_id, tumor_ordinal)` via `ANY_VALUE(primary_histology)` |
| bethesda_final / bethesda_final_name / regex_path_outcome / poc_tumor_1_histologic_type | merged from `path_outcome_classification_v1` at Script 361 time; **table no longer live** | no | **preserve from §A archive snapshot** on rebuild |
| build_script / build_ts | provenance | no | set by migration |

Full machine-readable table: `exports/mig185_rollup_patch_20260430/rollup_cols_audit.csv`.

---

## §3 inflation impact (pre-fix vs post-fix expectations)

Live MotherDuck read-only probes (`thyroid_canonical_publication_v1_0`, `connect_locked`, 2026-04-30).

### `n_tumors_total`

| Metric | Pre-fix | Post-fix (expected) |
|--------|---------|------------------------|
| Patients compared | 4,137 | 4,137 |
| Patients with `n_tumors_total` > distinct grain count | **466** | **0** |
| Sum of per-patient excess tumor counts | **533** | **0** |
| Max excess on one patient | 5 | 0 |

### `dominant_histology` (mode grain)

- Patients where `mode(primary_histology)` on all rows differs from `mode` after one-row-per-grain collapse: **77** (see `inflation_impact_per_metric.csv`).

### Distribution shift

Histogram of `(n_tumors_pre, n_tumors_post)` pairs across all 4,137 patients: `exports/mig185_rollup_patch_20260430/n_tumors_pre_post_distribution.csv`.  
Most mass is on the diagonal (`pre = post`); off-diagonal pairs are concentrated in `(2,1)`, `(3,2)`, `(4,3)`, etc., consistent with +1 duplicate row per duplicate grain.

---

## §4 cascade scan

Queries: `duckdb_views()` across attached DBs for SQL containing `canonical_path_malignant_patient_rollup_v1` or `n_tumors_total`.

| Consumer | Finding |
|----------|---------|
| `views_readable.path_malignant_patient_rollup_VIEW_v1` | Thin `SELECT *` over `main.canonical_path_malignant_patient_rollup_v1` — inherits fixed columns automatically. |
| `information_schema.views` (`ilike '%n_tumors_total%'`) | **No hits** in `thyroid_canonical_publication_v1_0` (view bodies do not reference the column name textually in catalog). |
| `main.canonical_patient_master` | No column named `n_tumors_total`; pathology tumor counts use distinct feeds (`n_tumors_path`, `n_tumors`, etc.). **No PM rebuild flagged** solely for this rollup patch. |

**Follow-up (process):** Re-align `scripts/361_op_path_consolidation.py` Step 5 with this migration so a future full Script 361 re-run does not regress `n_tumors_total` / `dominant_histology`.

---

## §5 Unblocking checklist — Cowork Path-C apply

1. Confirm MotherDuck RW token / `fail_closed` posture for apply environment.
2. Run §0 pre-flight row-count probes (10,871 CPM; 4,137 malignant rollup).
3. Execute `qc_framework_v1/migrations/185_apply_rollup_only_patch_20260430.sql` in a single transaction window (archive → rebuild → events flag → registry → provenance).
4. Run §F post-state probes: inflated patient count → 0; `is_source_distinct_duplicate_grain = TRUE` row count = **533** (matches duplicate excess).
5. Spot-check sample RIDs: 1294, 593, 8894 (largest pre-fix `n_tumors_total` inflation).
6. Refresh any **downstream ad-hoc** analyses that hard-coded assumptions about `n_tumors_total = COUNT(*)` on events.

---

## §6 Events grain — Logan-ratified preservation

All **6,689** path-malignant event rows in `main.canonical_path_malignant_events_v1` **remain** in `main.canonical_path_malignant_events_v1`. The **533** duplicate-grain rows are **not** deleted; they receive `is_source_distinct_duplicate_grain = TRUE` when not the first row in `(synoptic_row_ix ASC NULLS LAST, build_ts ASC NULLS LAST)` within each `(research_id, surgery_episode_id, tumor_ordinal)` partition.

**CF tags**

- `CF-mig185-EVENT-GRAIN-SOURCE-DISTINCT-PRESERVED` — events column comment / analyst contract.
- `CF-mig185-ROLLUP-GRAIN-DEDUPE` — rollup `n_tumors_total` + `dominant_histology` registry notes.

---

## Deliverables index

| Artifact | Path |
|----------|------|
| Apply SQL (skeleton / READY) | `qc_framework_v1/migrations/185_apply_rollup_only_patch_20260430.sql` |
| This report | `qc_framework_v1/reports/mig_185_apply_rollup_only_patch_20260430.md` |
| Rollup column audit | `exports/mig185_rollup_patch_20260430/rollup_cols_audit.csv` |
| Inflation metrics | `exports/mig185_rollup_patch_20260430/inflation_impact_per_metric.csv` |
| Pre/post `n_tumors` histogram | `exports/mig185_rollup_patch_20260430/n_tumors_pre_post_distribution.csv` |
| Manifest | `exports/mig185_rollup_patch_20260430/manifest.json` |
