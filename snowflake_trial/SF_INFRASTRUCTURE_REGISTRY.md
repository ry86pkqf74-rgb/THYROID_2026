# Snowflake AI Infrastructure — Deployed Components (v2 — round 17)

**Last updated:** 2026-05-04
**Deploy scripts:**
- `snowflake_trial/scripts/sf_infrastructure_deploy_v2.py` — initial deploy
- `snowflake_trial/scripts/sf_baseline_update_and_search_expand.py` — baseline v2 + full-corpus search

**Account:** `qcc02515.us-east-1` / DB `THYROID_VALIDATION` / Schema `PUBLIC`

---

## What's running on Snowflake (16 components)

### Flat-projection views (cohort + CPM)
- `CANONICAL_PATIENT_MASTER_FLAT` — VARIANT $1 → flat (10,871 rows)
- `COHORT_M044_AJCC_ETE_V1_FLAT` — 4,013 / 35 cols
- `COHORT_M037_LN_METASTASIS_V1_FLAT` — 2,234 / 43 cols
- `COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT` — 3,375 / 23 cols
- `COHORT_M032_DESCRIPTIVE_25YR_V1_FLAT` — 10,871 / 37 cols
- `COHORT_M038_MASSIVE_GOITER_V1_FLAT` — 10,871 / 31 cols
- All canonical_*_v1 events tables (8 tables) — see CANONICAL_*_FLAT views

### NLP results (AI_CLASSIFY full corpus)
- `NLP_SMOKING_FULL_RESULTS_v1` — 3,541 notes, 92.7% actionable
- `NLP_FAMILY_HX_THYROID_FULL_RESULTS_v1` — 3,534 notes, 84% actionable
- `NLP_VASC_INVASION_FULL_RESULTS_v1` — 806 notes, 52% actionable

### Validation infrastructure (baseline v2)
- `VALIDATION_RUN_LOG_v1` (table) — audit log; 17 checks per run
- `VALIDATE_ALL_COHORTS()` (procedure) — **17/17 PASS as of 2026-05-04**; INSERT to log + RETURN results
- `COHORT_SUMMARY_DASHBOARD` (view) — cross-manuscript cohort sizes at a glance

### Search + AI
- `THYROID_NOTES_SEARCH` (Cortex Search service) — semantic search over **FULL 11,050-note corpus** (expanded from 1K sample 2026-05-04)
- `CLINICAL_NOTES_SEARCH_V1` (table) — 11,050 notes, full corpus
- `@SEMANTIC_MODELS/thyroid_2026_semantic_model.yaml` — Cortex Analyst semantic model staged; bind via Snowsight UI

### Pipeline manifest
- `COWORK_PIPELINE_REGISTRY_V1` (table) — registry of all Cowork-deployed components

---

## Validation baseline (17 checks all PASS at 2026-05-04)

| Category | Check | Expected | Observed |
|---|---|---:|---:|
| Cohort denominators | M044_cohort_n | 4,013 | 4,013 ✓ |
| | M037_cohort_n | 2,234 | 2,234 ✓ |
| | M025_cohort_n | 3,375 | 3,375 ✓ |
| | M032_cohort_n | 10,871 | 10,871 ✓ |
| | M038_cohort_n | 10,871 | 10,871 ✓ |
| | CPM_cohort_n | 10,871 | 10,871 ✓ |
| | CPM_malig_n | 4,019 | 4,019 ✓ |
| NLP coverage | CPM_smoking_known_n | 3,022 | 3,022 ✓ |
| | CPM_fhx_thy_known_n | 3,018 | 3,018 ✓ |
| | CPM_smoking_clean_enum | YES | YES ✓ |
| | CPM_tirads_resolved_n | 3,382 | 3,382 ✓ |
| Manuscript cells | M044_events_any_recurrence | 499 | 499 ✓ |
| | M037_LN_pos_n | 1,124 | 1,124 ✓ |
| | M025_malig_n | 1,479 | 1,479 ✓ |
| | TIRADS_TR5_n | 1,402 | 1,402 ✓ |
| NLP scale | NLP_smoking_full_results | 3,541 | 3,541 ✓ |
| | NLP_family_hx_full_results | 3,534 | 3,534 ✓ |

---

## How Logan uses these (unchanged from v1; expanded baseline)

### `CALL VALIDATE_ALL_COHORTS()`
Sub-second. Re-runs 17 checks. Returns table of PASS/FAIL + writes to `VALIDATION_RUN_LOG_v1` for audit history. Run anytime as a sanity check after MD migration rounds.

### `SELECT * FROM COHORT_SUMMARY_DASHBOARD`
At-a-glance manuscript denominators + event counts.

### Cortex Analyst (post-Snowsight bind)
Type questions like "What's the malignancy rate by Bethesda category?" → SQL + result table.

### Cortex Search (full 11K corpus)
```sql
SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'THYROID_NOTES_SEARCH',
  '{"query": "lateral neck dissection lymph node positive", "limit": 5}'
));
```
Returns top-N most semantically similar notes from the FULL indexed corpus.

### Pipeline registry
```sql
SELECT * FROM THYROID_VALIDATION.PUBLIC.COWORK_PIPELINE_REGISTRY_V1 ORDER BY KIND, COMPONENT;
```

---

## Maintenance

After any MD migration round that changes cohort views or canonical schemas:
```bash
cd "/Users/ros/THyroid 2026"
source .venv/bin/activate
export SNOWFLAKE_PAT=...
export MOTHERDUCK_TOKEN=$(grep MD_SA_TOKEN motherduck.local.toml | sed 's/.*"\(.*\)".*/\1/')
python snowflake_trial/scripts/01_export_md_to_parquet.py
python snowflake_trial/scripts/02_load_to_snowflake.py
python snowflake_trial/scripts/04_build_flat_views.py
python snowflake_trial/scripts/sf_infrastructure_deploy_v2.py    # re-create flat views, dashboard, SP
# Then sanity check (should be 17/17 PASS):
# CALL VALIDATE_ALL_COHORTS();
```

When new cohort views land in MD, also re-run:
```bash
python snowflake_trial/scripts/sf_baseline_update_and_search_expand.py    # update baseline + Cortex Search refresh
```

---

**Status:** ALL 16 COMPONENTS ACTIVE — 17/17 PASS — full-corpus search live.
